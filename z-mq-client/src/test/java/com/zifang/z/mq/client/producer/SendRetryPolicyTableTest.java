package com.zifang.z.mq.client.producer;

import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.remoting.exception.RemotingConnectException;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.exception.RemotingTooMuchRequestException;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 白名单「是一张表」这件事的机器判：把 {@link SendRetryPolicy#rules()} 逐行走完，
 * 每行按它自己声明的入口形状合成一份输入，再问判据"这一份怎么判"，判决必须与表上那一行一致。
 * <p>
 * 这条用例刻意<b>不</b>按档位写死九个 {@code assertEquals}：如果判据与表是两处真相，
 * 逐行遍历会在第一处不一致就报出"表说什么 / 判据给什么"两栏；写死九个断言则只会在
 * "有人改了表忘了改判据"时红在最不像原因的那一条上。
 * <p>
 * 承重的一条口径在这里钉死：<b>存储侧已经给出结论的那一档（响应体里的任意状态）与
 * "响应码不是 SUCCESS"那一档，重试次数必须是 0</b> —— 一个状态值都不放过。
 */
public class SendRetryPolicyTableTest {

    private final SendRetryPolicy closed = SendRetryPolicy.defaults();
    private final SendRetryPolicy open = SendRetryPolicy.allowingUnknownOutcomeRetries();

    // ==================== 表的形状 ====================

    @Test
    @DisplayName("表是 9 行点名的档 + 1 行兜底；兜底那一行只能是禁重试")
    public void theTableHasNineNamedRowsAndOneFailSafeTail() {
        List<SendRetryPolicy.Rule> rules = closed.rules();
        assertEquals(10, rules.size(), "整张表的行数（9 档 + 兜底）: " + rules);
        assertEquals(9, closed.namedRuleCount(), "被点名的档位数: " + rules);

        SendRetryPolicy.Rule tail = rules.get(rules.size() - 1);
        assertTrue(tail.isCatchAll(), "最后一行必须是兜底行: " + tail);
        assertEquals(SendRetryPolicy.Tier.UNRECOGNISED, tail.getTier());
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, tail.getDecision(),
                "认不出来的形状一律不重试；这一条反过来（兜底允许重试）等于把整个白名单作废: " + tail);

        Set<SendRetryPolicy.Tier> seen = EnumSet.noneOf(SendRetryPolicy.Tier.class);
        for (SendRetryPolicy.Rule rule : rules) {
            assertTrue(seen.add(rule.getTier()), "同一个档位不许出现两行（会有先后歧义）: " + rule);
            assertNotNull(rule.getTriggerShape(), "每行都要说清什么形状落到它: " + rule);
            assertNotNull(rule.getReason(), "每行都要说清为什么这么判: " + rule);
            assertFalse(rule.getTriggerShape().trim().isEmpty(), "形状说明不许是空话: " + rule);
        }
        assertEquals(10, seen.size(), "档位集合与行数对不上: " + seen);
    }

    @Test
    @DisplayName("★ 逐行机器判：按表自己声明的入口形状合成输入，判据给的判决必须等于表上那一行")
    public void everyRowOfTheTableIsActuallyWhatTheJudgeDoes() {
        for (SendRetryPolicy.Rule rule : closed.rules()) {
            // 表上写的是"这一档的判决"，开关只把带条件的那一档落成可执行判决，
            // 所以对照的是"表上的那一档按当前开关落成的判决"。
            SendRetryPolicy.Decision expectedClosed = rule.getDecision().resolve(false);
            SendRetryPolicy.Decision judged = judgeFor(rule, closed);
            assertEquals(expectedClosed, judged,
                    "表与判据不一致（表就是判据这件事被破坏了）: " + rule);

            SendRetryPolicy.Decision expectedOpen = rule.getDecision().resolve(true);
            SendRetryPolicy.Decision judgedOpen = judgeFor(rule, open);
            assertEquals(expectedOpen, judgedOpen,
                    "开关打开后判据与表不一致: " + rule);
            if (rule.getDecision() == SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN) {
                assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER, judgedOpen,
                        "开关打开之后这一档应当真的换机器重试: " + rule);
                assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, judged,
                        "开关没打开时这一档必须是禁重试: " + rule);
            } else {
                assertEquals(judged, judgedOpen,
                        "开关只许影响\"结论未知\"那两档，别的一档都不许被它动到: " + rule);
            }
        }
    }

    // ==================== 禁重试的那几档 ====================

    @Test
    @DisplayName("第 1/2 档（消息不合法 / 未 start）：一条字节都没出进程 ⇒ 禁重试")
    public void deterministicPrecheckFailuresAreNeverRetried() {
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decide(new IllegalArgumentException("topic is null or empty"),
                        SendRetryPolicy.Phase.PRECHECK));
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decide(new IllegalStateException("producer not started"),
                        SendRetryPolicy.Phase.PRECHECK));
        // 开关打开也不许把这两档放进重试
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                open.decide(new IllegalArgumentException("body is null"),
                        SendRetryPolicy.Phase.PRECHECK));
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                open.decide(new IllegalStateException("producer not started"),
                        SendRetryPolicy.Phase.PRECHECK));
        assertEquals(SendRetryPolicy.StoreOutcome.NOT_SENT,
                closed.ruleFor(SendRetryPolicy.Tier.MESSAGE_INVALID).getStoreOutcome());
    }

    /**
     * 「前置校验那一档不参与重试」这句话只由这张表说：三条判据在这里钉死，
     * 生产代码里那句"这一支不会重试"的卫兵因此是<b>结构上进不来</b>的，而不是一句复述。
     * <ol>
     *   <li>表里所有 {@code phase == PRECHECK} 的行，两种开关下都必须是 {@code NEVER_RETRY}，
     *       且这样的行恰好两行；认不出来的形状落到的兜底行同样禁重试；</li>
     *   <li>判据在 PRECHECK 阶段对一根形状任意的输入（含被层层包装的、含 {@code null}）都禁重试；</li>
     *   <li><b>没有换表的缝</b>：策略类不可继承、公开构造器为 0、{@link SendRetryPolicy#rules()}
     *       返回不可变列表、行对象没有 setter，所以调用方无法把 PRECHECK 档判成可重试 ——
     *       这一条才是"结构上进不来"的判据本身，前两条只是它的结果。</li>
     * </ol>
     */
    @Test
    @DisplayName("★ 前置校验那一档：可构造出来的每一张表都判禁重试，且没有换表的缝 ⇒ 卫兵那一支不可达")
    public void noObtainablePolicyMakesAPrecheckFailureRetryable() throws Exception {
        // 判据一：表里挂在 PRECHECK 阶段下的行，两种开关落成的判决都是禁
        int precheckRows = 0;
        for (SendRetryPolicy.Rule rule : closed.rules()) {
            if (rule.getPhase() == SendRetryPolicy.Phase.PRECHECK) {
                precheckRows++;
                assertSame(SendRetryPolicy.Decision.NEVER_RETRY, rule.getDecision().resolve(false),
                        "PRECHECK 档被放开了一半（开关关着）: " + rule);
                assertSame(SendRetryPolicy.Decision.NEVER_RETRY, rule.getDecision().resolve(true),
                        "PRECHECK 档被 at-least-once 那个开关放开了: " + rule);
            }
        }
        assertEquals(2, precheckRows, "点名的前置校验档位必须恰好两行（不合法 / 未 start）: "
                + closed.rules());
        assertSame(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.ruleFor(SendRetryPolicy.Tier.UNRECOGNISED).getDecision(),
                "PRECHECK 阶段认不出的形状全靠兜底行，兜底行放开等于这一档放开");

        // 判据二：合成一批形状任意的输入，PRECHECK 阶段两个开关下都必须禁
        Throwable[] shapes = new Throwable[]{
                new IllegalArgumentException("topic is null or empty"),
                new IllegalStateException("producer not started"),
                new RuntimeException("something else entirely"),
                new InterruptedException("instance start interrupted"),
                new RemotingSendRequestException("No route for topic: T"),
                new RemotingConnectException("127.0.0.1:1"),
                new RemotingTimeoutException("timeout"),
                new RemotingTooMuchRequestException("flow control"),
                new IllegalStateException("wrapped", new IllegalArgumentException("inner")),
                new RuntimeException("twice wrapped",
                        new IllegalStateException("outer", new RemotingConnectException("inner"))),
                null
        };
        for (int i = 0; i < shapes.length; i++) {
            assertSame(SendRetryPolicy.Decision.NEVER_RETRY,
                    closed.decide(shapes[i], SendRetryPolicy.Phase.PRECHECK),
                    "形状 #" + i + " 在 PRECHECK 阶段被判成可重试: " + shapes[i]);
            assertSame(SendRetryPolicy.Decision.NEVER_RETRY,
                    open.decide(shapes[i], SendRetryPolicy.Phase.PRECHECK),
                    "形状 #" + i +  " 在开关打开后被判成可重试: " + shapes[i]);
        }

        // 判据三：换表的缝一处都不许存在（否则前两条只是"默认表恰好如此"，那句卫兵就成了可达）
        assertTrue(Modifier.isFinal(SendRetryPolicy.class.getModifiers()),
                "策略类可被继承 ⇒ 子类就能改写判决");
        assertEquals(0, SendRetryPolicy.class.getConstructors().length,
                "公开构造器必须为 0：否则调用方能拿到一个不经工厂的策略实例");
        int factories = 0;
        List<SendRetryPolicy> obtainable = new ArrayList<SendRetryPolicy>();
        for (Method m : SendRetryPolicy.class.getDeclaredMethods()) {
            if (!Modifier.isStatic(m.getModifiers()) || m.getReturnType() != SendRetryPolicy.class) {
                continue;
            }
            factories++;
            for (Object[] args : argumentShapesOf(m)) {
                obtainable.add((SendRetryPolicy) m.invoke(null, args));
            }
        }
        assertEquals(3, factories, "工厂方法数量（defaults / allowingUnknownOutcomeRetries / of）: "
                + factories);
        assertTrue(obtainable.size() >= 3, "可构造出的策略实例少于 3 份，说明扫错了: " + obtainable.size());
        for (SendRetryPolicy policy : obtainable) {
            assertSame(SendRetryPolicy.Decision.NEVER_RETRY,
                    policy.decide(new IllegalStateException("producer not started"),
                            SendRetryPolicy.Phase.PRECHECK),
                    "这一份策略实例会把前置校验失败判成可重试: " + policy.getClass());
        }

        List<SendRetryPolicy.Rule> rules = closed.rules();
        try {
            rules.add(closed.ruleFor(SendRetryPolicy.Tier.UNRECOGNISED));
            fail("rules() 返回的表可被追加 ⇒ 调用方能往表里加一行可重试的前置校验档");
        } catch (UnsupportedOperationException expected) {
            assertNotNull(expected);
        }
        for (Method m : SendRetryPolicy.Rule.class.getMethods()) {
            assertFalse(m.getName().startsWith("set"),
                    "行对象暴露了 setter，表就能被就地改掉: " + m);
        }
    }

    /** 一个工厂方法的合法入参形状；无参工厂返回一份空数组的数组. */
    private static List<Object[]> argumentShapesOf(Method m) {
        List<Object[]> out = new ArrayList<Object[]>();
        if (m.getParameterTypes().length == 0) {
            out.add(new Object[0]);
            return out;
        }
        if (m.getParameterTypes().length == 1 && m.getParameterTypes()[0] == boolean.class) {
            out.add(new Object[]{Boolean.FALSE});
            out.add(new Object[]{Boolean.TRUE});
            return out;
        }
        if (m.getParameterTypes().length == 1 && m.getParameterTypes()[0] == int.class) {
            out.add(new Object[]{Integer.valueOf(0)});
            out.add(new Object[]{Integer.valueOf(3)});
            return out;
        }
        fail("工厂方法 " + m + " 的入参形状没被覆盖，判据三扫不全");
        return out;
    }

    @Test
    @DisplayName("★ 第 8 档：响应体里出现的<b>每一个</b>状态值都禁重试，一个都不放过")
    public void everyStatusDecodedFromTheResponseBodyIsForbidden() {
        SendStatus[] all = SendStatus.values();
        assertTrue(all.length >= 8, "SendStatus 的取值数与表设计时的口径差太多，先看清再判: "
                + java.util.Arrays.toString(all));
        for (SendStatus status : all) {
            assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, closed.decideForBodyStatus(status),
                    "存储侧已经给出结论，重试它就是写第二遍: " + status);
            assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, open.decideForBodyStatus(status),
                    "第 8 档不许被 at-least-once 那个开关放行（开关管的是\"不知道送没送到\"，"
                            + "而这一档是\"已经知道送到了\"）: " + status);
            assertSame(SendRetryPolicy.Tier.STORE_REPORTED_STATUS,
                    closed.ruleFor(SendRetryPolicy.Tier.STORE_REPORTED_STATUS).getTier());
        }
        // 这一档的理由必须是"已经进了存储"，否则它凭什么压过"多试一次"
        assertEquals(SendRetryPolicy.StoreOutcome.WRITTEN,
                closed.ruleFor(SendRetryPolicy.Tier.STORE_REPORTED_STATUS).getStoreOutcome(),
                "把第 8 档的存储结论改写成 UNKNOWN 就等于悄悄放开它: "
                        + closed.ruleFor(SendRetryPolicy.Tier.STORE_REPORTED_STATUS));
    }

    @Test
    @DisplayName("第 9 档：响应码不是 SUCCESS ⇒ 默认禁（认不出是哪个码，就认不出写没写）")
    public void aRejectedResponseCodeIsForbiddenForEveryNonSuccessCode() {
        int[] nonSuccess = new int[]{
                RemotingSysResponseCode.SYSTEM_ERROR,
                RemotingSysResponseCode.SYSTEM_BUSY,
                RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                Integer.MIN_VALUE,
                77777
        };
        for (int code : nonSuccess) {
            assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, closed.decideForResponseCode(code),
                    "响应码 " + code + " 不许被放行");
            assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, open.decideForResponseCode(code),
                    "开关也不许放行第 9 档: " + code);
        }
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decideForResponseCode(RemotingSysResponseCode.SUCCESS),
                "SUCCESS 根本不是一次失败，不该由第 9 档放行任何重试");
    }

    // ==================== 同一类型、不同阶段：阶段就是判据的一半 ====================

    @Test
    @DisplayName("同一个 RemotingSendRequestException：组包阶段是\"没送出\"，发送阶段是\"未知\" ⇒ 两档判决必须不同")
    public void theSameExceptionTypeSplitsByPhase() {
        RemotingSendRequestException same = new RemotingSendRequestException("No route for topic: T");
        assertEquals(SendRetryPolicy.Decision.RETRY_AFTER_ROUTE_REFRESH,
                closed.decide(same, SendRetryPolicy.Phase.PREPARE),
                "组包阶段的\"路由没解析出来\"必须重试，而且必须先重取路由");
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decide(same, SendRetryPolicy.Phase.INVOKE),
                "同一个类型在发送阶段是\"字节可能已经进了 socket\"，默认禁");
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER,
                open.decide(same, SendRetryPolicy.Phase.INVOKE),
                "开关打开之后发送阶段这一档才换机器");

        // 两档的存储结论也必然不同：一个是 NOT_SENT，一个只能是 UNKNOWN
        assertEquals(SendRetryPolicy.StoreOutcome.NOT_SENT,
                closed.ruleFor(SendRetryPolicy.Tier.ROUTE_UNRESOLVED).getStoreOutcome());
        assertEquals(SendRetryPolicy.StoreOutcome.UNKNOWN,
                closed.ruleFor(SendRetryPolicy.Tier.SEND_OUTCOME_UNKNOWN).getStoreOutcome());
    }

    // ==================== 可以重试的那几档 ====================

    @Test
    @DisplayName("第 3/4/7 档：重试，且第 3 档必须挂\"重取路由\"、第 7 档必须挂\"退避\"")
    public void theRetryableTiersCarryTheirOwnObligations() {
        assertEquals(SendRetryPolicy.Decision.RETRY_AFTER_ROUTE_REFRESH,
                closed.decide(new RemotingSendRequestException("No writable queue for topic: T"),
                        SendRetryPolicy.Phase.PREPARE));
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER,
                closed.decide(new RemotingConnectException("127.0.0.1:1"),
                        SendRetryPolicy.Phase.INVOKE));
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER_WITH_BACKOFF,
                closed.decide(new RemotingTooMuchRequestException("flow control"),
                        SendRetryPolicy.Phase.INVOKE));

        // 路由那一档只许挂重取路由；换机器那一档只许挂排除——两套义务不许互相冒充
        assertSame(SendRetryPolicy.Tier.ROUTE_UNRESOLVED,
                closed.explain(new RemotingSendRequestException("No master addr for broker: b"),
                        SendRetryPolicy.Phase.PREPARE).getTier());
        assertFalse(closed.ruleFor(SendRetryPolicy.Tier.CONNECT_FAILED).getDecision()
                        == SendRetryPolicy.Decision.RETRY_AFTER_ROUTE_REFRESH,
                "连不上那台机器时重取路由没意义：地址就在缓存那份路由里，要的是换一台");
    }

    @Test
    @DisplayName("第 5/6 档（写失败 / 超时）单列一档：默认禁，显式接受 at-least-once 才放开")
    public void unknownOutcomeTiersAreTheirOwnRowsAndAreOptIn() {
        assertFalse(closed.isUnknownOutcomeRetriesAllowed(), "默认口径必须是不接受重复写");
        assertTrue(open.isUnknownOutcomeRetriesAllowed());

        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN,
                closed.ruleFor(SendRetryPolicy.Tier.SEND_OUTCOME_UNKNOWN).getDecision());
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN,
                closed.ruleFor(SendRetryPolicy.Tier.RESPONSE_TIMEOUT).getDecision());
        assertEquals(SendRetryPolicy.StoreOutcome.UNKNOWN,
                closed.ruleFor(SendRetryPolicy.Tier.RESPONSE_TIMEOUT).getStoreOutcome(),
                "\"超时≠没送到\"这件事必须写在表上，不许被实现忘掉");

        // 落成可执行判决：关=0 次重试、开=换机器
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY, closed.decide(
                new RemotingTimeoutException("wait response on the channel <x> timeout 3000ms"),
                SendRetryPolicy.Phase.INVOKE));
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER, open.decide(
                new RemotingTimeoutException("wait response on the channel <x> timeout 3000ms"),
                SendRetryPolicy.Phase.INVOKE));
    }

    // ==================== 次数口径 ====================

    @Test
    @DisplayName("默认 3 次的口径：不含首次那次尝试 ⇒ 预算是 4 趟")
    public void retryTimesExcludeTheFirstAttempt() {
        assertEquals(3, SendRetryPolicy.DEFAULT_RETRY_TIMES_WHEN_SEND_FAILED);
        assertEquals(3, closed.getDefaultRetryTimes(), "producer 的默认值就是从这个常量取的");
        assertEquals(4, closed.attemptBudget(3), "1 次首发 + 3 次重试");
        assertEquals(1, closed.attemptBudget(0), "置 0 就是关掉重试，不是\"一共只许试 0 趟\"");
        assertEquals(1, closed.attemptBudget(-5), "负数按 0 处理");
        assertEquals(2, closed.attemptBudget(1));
    }

    @Test
    @DisplayName("尺自检：兜底方向与开关范围——认不出的形状、null 异常，都只能落在禁重试上")
    public void theJudgeFailsSafeNotFailOpen() {
        RuntimeException nobodyInThisRepoThrowsThisOnTheSendPath = new RuntimeException("java.lang.Error 的兄弟");
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decide(new java.io.NotSerializableException("认不出来的形状"), SendRetryPolicy.Phase.INVOKE));
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decide(null, SendRetryPolicy.Phase.PREPARE));
        assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                closed.decide(nobodyInThisRepoThrowsThisOnTheSendPath, SendRetryPolicy.Phase.PREPARE));
        assertEquals(SendRetryPolicy.Tier.UNRECOGNISED,
                closed.explain(nobodyInThisRepoThrowsThisOnTheSendPath, SendRetryPolicy.Phase.INVOKE).getTier());
        assertEquals(SendRetryPolicy.Tier.UNRECOGNISED,
                closed.explain(nobodyInThisRepoThrowsThisOnTheSendPath, SendRetryPolicy.Phase.PREPARE).getTier());
        // 阴性对照：一根不存在的针式输入也不能被放行
        for (SendRetryPolicy.Phase phase : SendRetryPolicy.Phase.values()) {
            assertEquals(SendRetryPolicy.Decision.NEVER_RETRY,
                    open.decide(new java.io.IOException("w2g-needle-that-cannot-be-whitelisted"), phase),
                    "任何阶段认不出来的形状都不许被放行: " + phase);
        }
        // 认不出的 cause 不许把外面那层认得出来的包装改成别的档
        assertEquals(SendRetryPolicy.Tier.SEND_OUTCOME_UNKNOWN,
                closed.explain(new RemotingSendRequestException("wrapped",
                        new java.io.IOException("w2g-needle-that-cannot-be-whitelisted")),
                        SendRetryPolicy.Phase.INVOKE).getTier());
    }

    @Test
    @DisplayName("★ 因果链：同步出口把流控包进 RemotingSendRequestException，档位必须还是流控那一档")
    public void classificationFollowsTheInnermostRecognisableCause() {
        // 这就是 MQClientInstance#invokeAndTranslateException 最后一个分支的产出形状：
        // 外面一层 RemotingSendRequestException，真正的因在 cause 里
        RemotingSendRequestException wrappedFlowControl = RemotingSendRequestException
                .newSendRequestException("127.0.0.1:10911", new RemotingTooMuchRequestException("semaphore timeout"));
        SendRetryPolicy.Rule rule = closed.explain(wrappedFlowControl, SendRetryPolicy.Phase.INVOKE);
        assertEquals(SendRetryPolicy.Tier.FLOW_CONTROL, rule.getTier(),
                "包装层不该把\"稍后再来\"洗成\"不知道写没写\": " + rule);
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER_WITH_BACKOFF,
                closed.decide(wrappedFlowControl, SendRetryPolicy.Phase.INVOKE),
                "流控那一档的判决不依赖开关，且必须带退避: " + rule);
        assertEquals(SendRetryPolicy.Decision.RETRY_ON_ANOTHER_BROKER_WITH_BACKOFF,
                open.decide(wrappedFlowControl, SendRetryPolicy.Phase.INVOKE));
        // 但同一条规则不能越过阶段：组包阶段的这个形状不是流控
        assertEquals(SendRetryPolicy.Tier.ROUTE_UNRESOLVED,
                closed.explain(wrappedFlowControl, SendRetryPolicy.Phase.PREPARE).getTier(),
                "阶段还是要先于类型：组包阶段拿不到的是路由，不是流控");

        // 三层包装也照样往里找到最里面那一个认得出来的
        RemotingSendRequestException threeLayers = RemotingSendRequestException.newSendRequestException("a",
                new RemotingSendRequestException("b",
                        new RemotingConnectException("127.0.0.1:10911")));
        assertEquals(SendRetryPolicy.Tier.CONNECT_FAILED,
                closed.explain(threeLayers, SendRetryPolicy.Phase.INVOKE).getTier(),
                "最里面的原因才是真因: " + closed.explain(threeLayers, SendRetryPolicy.Phase.INVOKE));

        // 因果环不许把判定转死（这里只能靠"它回来了"来判，不测时间）
        RemotingSendRequestException ringA = new RemotingSendRequestException("ring-a");
        RemotingSendRequestException ringB = new RemotingSendRequestException("ring-b", ringA);
        ringA.initCause(ringB);
        assertEquals(SendRetryPolicy.Tier.SEND_OUTCOME_UNKNOWN,
                closed.explain(ringA, SendRetryPolicy.Phase.INVOKE).getTier(),
                "环状 cause 也必须收得住并给出默认档");
    }

    private static Throwable nobodyInTheSendPath(String why) {
        return new RemotingTimeoutException(why);
    }

    /** 按一行自己声明的入口形状合成一份输入，再交给对应的判据. */
    private static SendRetryPolicy.Decision judgeFor(SendRetryPolicy.Rule rule, SendRetryPolicy policy) {
        SendRetryPolicy.Judge judge = rule.getJudge();
        if (judge == null) {
            // 兜底行没有入口形状（它的定义就是"认不出来"），用一份一定认不出的输入判
            return policy.decide(new java.io.IOException("w2g-needle-that-cannot-be-whitelisted"),
                    SendRetryPolicy.Phase.INVOKE);
        }
        switch (judge) {
            case PRECHECK_FAILURE:
                return policy.decide(sampleFailure(rule.getTier()), SendRetryPolicy.Phase.PRECHECK);
            case PREPARE_FAILURE:
                return policy.decide(sampleFailure(rule.getTier()), SendRetryPolicy.Phase.PREPARE);
            case INVOKE_FAILURE:
                return policy.decide(sampleFailure(rule.getTier()), SendRetryPolicy.Phase.INVOKE);
            case RESPONSE_BODY_STATUS:
                // 这一档的定义是"任意状态值" ⇒ 每一个取值都必须落在同一行判决上
                SendRetryPolicy.Decision bodyVerdict = null;
                for (SendStatus status : SendStatus.values()) {
                    SendRetryPolicy.Decision one = policy.decideForBodyStatus(status);
                    if (bodyVerdict == null) {
                        bodyVerdict = one;
                    } else if (bodyVerdict != one) {
                        fail("第 8 档按状态值分了判决（表上这一行的意思是\"任意状态都不重\"）: "
                                + bodyVerdict + " vs " + one);
                    }
                }
                return bodyVerdict;
            case RESPONSE_CODE_NOT_SUCCESS:
                SendRetryPolicy.Decision codeVerdict = null;
                int[] codes = new int[]{RemotingSysResponseCode.SYSTEM_ERROR,
                        RemotingSysResponseCode.SYSTEM_BUSY,
                        RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, 4242};
                for (int code : codes) {
                    SendRetryPolicy.Decision one = policy.decideForResponseCode(code);
                    if (codeVerdict == null) {
                        codeVerdict = one;
                    } else if (codeVerdict != one) {
                        fail("第 9 档按响应码分了判决，而表上这一行没有这个区分: " + code);
                    }
                }
                return codeVerdict;
            default:
                throw new IllegalStateException("表里出现了一条测试不认识的判据入口: " + judge);
        }
    }

    /** 这一档的合成输入：类型必须与 {@code triggerShape} 里写的那个异常一致. */
    private static Throwable sampleFailure(SendRetryPolicy.Tier tier) {
        switch (tier) {
            case MESSAGE_INVALID:
                return new IllegalArgumentException("table sample");
            case PRODUCER_NOT_STARTED:
                return new IllegalStateException("table sample");
            case ROUTE_UNRESOLVED:
                return new RemotingSendRequestException("table sample");
            case CONNECT_FAILED:
                return new RemotingConnectException("127.0.0.1:1");
            case SEND_OUTCOME_UNKNOWN:
                return new RemotingSendRequestException("table sample");
            case RESPONSE_TIMEOUT:
                return new RemotingTimeoutException("table sample");
            case FLOW_CONTROL:
                return new RemotingTooMuchRequestException("table sample");
            default:
                return null;
        }
    }
}
