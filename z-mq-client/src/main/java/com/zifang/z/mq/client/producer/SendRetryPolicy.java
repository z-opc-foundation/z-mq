package com.zifang.z.mq.client.producer;

import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.remoting.exception.RemotingConnectException;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.exception.RemotingTooMuchRequestException;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 发送重试白名单：一张可机器判的表，加一个按这张表下判决的判据。
 * <p>
 * 三条形状约束：
 * <ol>
 *   <li><b>表就是判据</b>。判决不从 {@code if} 链里读出来，而是从 {@link #rules()} 那张表里读出来：
 *       每一行自带"什么形状落到这一档"（{@link Rule#getJudge()} + {@link Rule#getPhase()}
 *       + {@link Rule#getTriggerShape()}），调用方（{@link DefaultMQProducer} 的发送循环）拿到的
 *       也只是表里那一行的 {@link Decision}。测试可以把整张表逐行走一遍，不需要知道实现细节。</li>
 *   <li><b>同一个异常类型在不同阶段是不同档</b>。{@link RemotingSendRequestException} 在
 *       {@link Phase#PREPARE}（组包阶段，请求还没离开进程）是"路由没解析出来"，重试要<b>先重取路由</b>；
 *       在 {@link Phase#INVOKE}（已经交给 remoting）就意味着"字节可能已经进了 socket"，
 *       存储侧结论未知。因此 {@link #decide(Throwable, Phase)} 必须带阶段，只有类型不够。</li>
 *   <li><b>兜底方向是"不重试"</b>：表里认不出来的任何形状一律 {@link Decision#NEVER_RETRY}。
 *       重试的代价是同一条消息在存储里出现第二遍，而这个 client 的对外口径是
 *       "幂等去重由业务侧按 Key + 业务时间戳负责"，所以默认表偏保守：宁可少试，不可多写。
 *       {@link Decision#RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN} 那一档就是这个"保守"的显式出口：
 *       结论未知的失败默认不重试，接受 at-least-once 的调用方必须自己把它打开。</li>
 * </ol>
 * 另有一条与 {@link DefaultMQProducer#toSendResult} 同源的约束：broker 在响应体里给出的
 * {@link SendStatus} 是<b>存储侧的结论</b>（"这条消息进了/没进存储、落没落住盘"只有 store 知道），
 * client 对这一档没有任何独立信息源，所以 {@link Tier#STORE_REPORTED_STATUS} 覆盖
 * <b>响应体里出现的每一个状态值</b>，一个都不例外地禁重试 —— 这里刻意不按状态值列举，
 * 以免 client 侧出现"凭状态码猜存储结果"的第二个真相来源。
 */
public final class SendRetryPolicy {

    /** 一次发送里，失败发生的阶段（同一个异常类型靠它分档）. */
    public enum Phase {
        /** 前置校验：消息本身不合法、producer 没 start —— 一条字节都没碰 remoting */
        PRECHECK,
        /** 组包：拉路由 → 选队列 → 找 master 地址 → 编码 */
        PREPARE,
        /** 已经把请求交给 remoting 出口（含把响应翻译成结论的那一步） */
        INVOKE
    }

    /** 判决档位. */
    public enum Tier {
        /** 1. 消息不合法：topic/body 为空、message 为 null */
        MESSAGE_INVALID,
        /** 2. producer 没 start */
        PRODUCER_NOT_STARTED,
        /** 3. 路由没解析出来：无路由 / 无可写队列 / 没有 master 地址 */
        ROUTE_UNRESOLVED,
        /** 4. 连不上那台 broker：channel 根本没建起来 */
        CONNECT_FAILED,
        /** 5. 请求已交给 remoting 而写失败：字节可能已经进了 socket */
        SEND_OUTCOME_UNKNOWN,
        /** 6. 等不到响应：超时不等于没送到 */
        RESPONSE_TIMEOUT,
        /** 7. 流控：语义就是"稍后再来" */
        FLOW_CONTROL,
        /** 8. 响应体里解出来的存储侧结论（任何状态值） */
        STORE_REPORTED_STATUS,
        /** 9. 响应码不是 SUCCESS：client 就地造的通用失败 */
        REMOTE_RESPONSE_REJECTED,
        /** 兜底：表里认不出来的形状 */
        UNRECOGNISED
    }

    /** 这一档要不要重试、怎么重试. */
    public enum Decision {
        /** 禁重试：把同一次确定性失败做 N 遍没有收益，或者代价（写第二遍）不可接受 */
        NEVER_RETRY,
        /** 重试，且下一次尝试之前必须先重取路由 —— 不刷路由下一次还是同一个错 */
        RETRY_AFTER_ROUTE_REFRESH,
        /** 重试，且必须换一台 broker：同地址再来一次没有收益 */
        RETRY_ON_ANOTHER_BROKER,
        /** 重试、换 broker，并且先退避（流控的"稍后再来"要真的等一会儿） */
        RETRY_ON_ANOTHER_BROKER_WITH_BACKOFF,
        /** 换 broker 重试，但只在调用方显式接受"可能写第二遍"时打开 */
        RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN;

        /** 把带条件的档位按开关落成可执行的判决. */
        public Decision resolve(boolean unknownOutcomeRetriesAllowed) {
            if (this == RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN) {
                return unknownOutcomeRetriesAllowed ? RETRY_ON_ANOTHER_BROKER : NEVER_RETRY;
            }
            return this;
        }

        public boolean allowsRetry() {
            return this != NEVER_RETRY;
        }
    }

    /** 判决的入口形状：这一行由哪个判据负责. */
    public enum Judge {
        /** 前置校验阶段抛出的异常 */
        PRECHECK_FAILURE,
        /** 组包阶段抛出的异常 */
        PREPARE_FAILURE,
        /** 已交给 remoting 之后抛出的异常 */
        INVOKE_FAILURE,
        /** 响应体里解出来的存储侧结论 */
        RESPONSE_BODY_STATUS,
        /** 响应码不是 SUCCESS */
        RESPONSE_CODE_NOT_SUCCESS
    }

    /** 消息在存储里出现的可能性：这一列才是"禁 8 / 禁 9"的理由所在. */
    public enum StoreOutcome {
        /** 一条都没出去（进程内就失败了） */
        NOT_SENT,
        /** 可能出去了，client 无从知道 */
        UNKNOWN,
        /** 已经进了存储（重试它就是写第二遍） */
        WRITTEN
    }

    /** 表里的一行. */
    public static final class Rule {
        private final int ordinalInTable;
        private final Tier tier;
        private final Judge judge;
        private final Phase phase;
        private final Decision decision;
        private final StoreOutcome storeOutcome;
        private final String triggerShape;
        private final String reason;
        private final boolean catchAll;

        Rule(int ordinalInTable, Tier tier, Judge judge, Phase phase, Decision decision,
             StoreOutcome storeOutcome, String triggerShape, String reason, boolean catchAll) {
            this.ordinalInTable = ordinalInTable;
            this.tier = tier;
            this.judge = judge;
            this.phase = phase;
            this.decision = decision;
            this.storeOutcome = storeOutcome;
            this.triggerShape = triggerShape;
            this.reason = reason;
            this.catchAll = catchAll;
        }

        public int getOrdinalInTable() {
            return ordinalInTable;
        }

        public Tier getTier() {
            return tier;
        }

        public Judge getJudge() {
            return judge;
        }

        /** 仅对 {@code *_FAILURE} 三类判据有意义；其余判据返回 {@code null}. */
        public Phase getPhase() {
            return phase;
        }

        public Decision getDecision() {
            return decision;
        }

        public StoreOutcome getStoreOutcome() {
            return storeOutcome;
        }

        /** 人读的"什么形状落到这一档"，同时是测试合成入参的说明书. */
        public String getTriggerShape() {
            return triggerShape;
        }

        public String getReason() {
            return reason;
        }

        public boolean isCatchAll() {
            return catchAll;
        }

        @Override
        public String toString() {
            return "#" + ordinalInTable + " " + tier + " via " + judge
                    + (phase == null ? "" : "@" + phase) + " => " + decision
                    + " [存储侧=" + storeOutcome + "] " + triggerShape;
        }
    }

    /** 默认重试次数（不含首次那次尝试）. */
    public static final int DEFAULT_RETRY_TIMES_WHEN_SEND_FAILED = 3;

    /** 顺着因果链往里找档位时最多走几层（防御自引用环与包装套娃）. */
    static final int MAX_CAUSE_DEPTH = 16;

    private static final Rule[] DEFAULT_TABLE = new Rule[]{
            new Rule(1, Tier.MESSAGE_INVALID, Judge.PRECHECK_FAILURE, Phase.PRECHECK,
                    Decision.NEVER_RETRY, StoreOutcome.NOT_SENT,
                    "IllegalArgumentException（message 为 null / topic 空 / body 为 null）",
                    "确定性错误：重试只是把同一次失败做 N 遍，一条字节都没出进程", false),
            new Rule(2, Tier.PRODUCER_NOT_STARTED, Judge.PRECHECK_FAILURE, Phase.PRECHECK,
                    Decision.NEVER_RETRY, StoreOutcome.NOT_SENT,
                    "IllegalStateException（producer 尚未 start）",
                    "要的是 start()，不是循环；重试只会把同一个「没启动」重复报三遍", false),
            new Rule(3, Tier.ROUTE_UNRESOLVED, Judge.PREPARE_FAILURE, Phase.PREPARE,
                    Decision.RETRY_AFTER_ROUTE_REFRESH, StoreOutcome.NOT_SENT,
                    "RemotingSendRequestException 于组包阶段：No route / No writable queue / No master addr",
                    "请求根本没送出；下一次之前必须先重取路由，否则拿到的还是同一份陈旧路由、同一个错", false),
            new Rule(4, Tier.CONNECT_FAILED, Judge.INVOKE_FAILURE, Phase.INVOKE,
                    Decision.RETRY_ON_ANOTHER_BROKER, StoreOutcome.NOT_SENT,
                    "RemotingConnectException（channel 建不起来）",
                    "字节没出去，换一个 broker 地址有真实收益", false),
            new Rule(5, Tier.SEND_OUTCOME_UNKNOWN, Judge.INVOKE_FAILURE, Phase.INVOKE,
                    Decision.RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN, StoreOutcome.UNKNOWN,
                    "RemotingSendRequestException 于发送阶段（含响应写回失败、以及\"没拿到响应\"这一支）",
                    "可能已经进了 socket：重试是在\"也许已有第一份\"的前提上再写一份，默认禁，"
                            + "接受 at-least-once 的调用方显式打开", false),
            new Rule(6, Tier.RESPONSE_TIMEOUT, Judge.INVOKE_FAILURE, Phase.INVOKE,
                    Decision.RETRY_ON_ANOTHER_BROKER_ONLY_IF_OPT_IN, StoreOutcome.UNKNOWN,
                    "RemotingTimeoutException（等不到响应）",
                    "超时不等于没送到，而且很可能已经进了存储；与第 5 档同一判法：默认禁、显式才放开", false),
            new Rule(7, Tier.FLOW_CONTROL, Judge.INVOKE_FAILURE, Phase.INVOKE,
                    Decision.RETRY_ON_ANOTHER_BROKER_WITH_BACKOFF, StoreOutcome.NOT_SENT,
                    "RemotingTooMuchRequestException（流控）；异步/单向出口直接抛它，同步出口会把它包进 "
                            + "RemotingSendRequestException 的 cause 里，两种形状都算这一档",
                    "流控本身就是结论、语义是\"稍后再来\"，所以既不包装成发送失败、也不立刻再来一次", false),
            new Rule(8, Tier.STORE_REPORTED_STATUS, Judge.RESPONSE_BODY_STATUS, null,
                    Decision.NEVER_RETRY, StoreOutcome.WRITTEN,
                    "响应体里解出来的任意 SendStatus（成功结论与刷盘/主从结论都算）",
                    "这一档的语义是\"存储侧已经给出了结论\"，重试它等于把同一条消息写第二遍；"
                            + "而存储侧的结论只有 broker 知道，client 不许按状态码再造第二个真相", false),
            new Rule(9, Tier.REMOTE_RESPONSE_REJECTED, Judge.RESPONSE_CODE_NOT_SUCCESS, null,
                    Decision.NEVER_RETRY, StoreOutcome.UNKNOWN,
                    "response.getCode() != SUCCESS（client 就地造的通用失败结论）",
                    "默认禁：响应码到不了\"写之前还是写之后\"这一层信息 —— 发送路径上的 SYSTEM_ERROR "
                            + "确实都产生于落库之前，但那是 free-text remark 区分的，不是稳定的线上契约；"
                            + "拿不准就少试", false),
            new Rule(10, Tier.UNRECOGNISED, null, null,
                    Decision.NEVER_RETRY, StoreOutcome.UNKNOWN,
                    "表里认不出来的任何形状",
                    "兜底方向固定是不重试：认不出来就意味着不知道消息进没进存储", true)
    };

    private final boolean unknownOutcomeRetriesAllowed;
    private final int defaultRetryTimes;

    private SendRetryPolicy(boolean unknownOutcomeRetriesAllowed, int defaultRetryTimes) {
        this.unknownOutcomeRetriesAllowed = unknownOutcomeRetriesAllowed;
        this.defaultRetryTimes = defaultRetryTimes;
    }

    /** 默认表：第 5/6 档（结论未知）关着. */
    public static SendRetryPolicy defaults() {
        return new SendRetryPolicy(false, DEFAULT_RETRY_TIMES_WHEN_SEND_FAILED);
    }

    /** 同一张表，把"结论未知"那一档按 at-least-once 打开. */
    public static SendRetryPolicy allowingUnknownOutcomeRetries() {
        return new SendRetryPolicy(true, DEFAULT_RETRY_TIMES_WHEN_SEND_FAILED);
    }

    public static SendRetryPolicy of(boolean unknownOutcomeRetriesAllowed) {
        return new SendRetryPolicy(unknownOutcomeRetriesAllowed, DEFAULT_RETRY_TIMES_WHEN_SEND_FAILED);
    }

    /** 整张表（含最后那行兜底），顺序即判序. */
    public List<Rule> rules() {
        List<Rule> out = new ArrayList<Rule>(DEFAULT_TABLE.length);
        Collections.addAll(out, DEFAULT_TABLE);
        return Collections.unmodifiableList(out);
    }

    /** 表里被点名的档位数量（不含兜底行）. */
    public int namedRuleCount() {
        int n = 0;
        for (int i = 0; i < DEFAULT_TABLE.length; i++) {
            if (!DEFAULT_TABLE[i].isCatchAll()) {
                n++;
            }
        }
        return n;
    }

    public Rule ruleFor(Tier tier) {
        for (int i = 0; i < DEFAULT_TABLE.length; i++) {
            if (DEFAULT_TABLE[i].tier == tier) {
                return DEFAULT_TABLE[i];
            }
        }
        return null;
    }

    public boolean isUnknownOutcomeRetriesAllowed() {
        return unknownOutcomeRetriesAllowed;
    }

    public int getDefaultRetryTimes() {
        return defaultRetryTimes;
    }

    /**
     * 一次发送的尝试预算。
     * <p>
     * 口径写死在这里：<b>{@code retryTimesWhenSendFailed} 不含首次那次尝试</b>，
     * 所以预算是 {@code 1 + retryTimesWhenSendFailed}；负数按 0 处理（等于关掉重试）。
     */
    public int attemptBudget(int retryTimesWhenSendFailed) {
        return 1 + Math.max(0, retryTimesWhenSendFailed);
    }

    // ==================== 判据 ====================

    /**
     * 表就是判据：按 {@link #rules()} 的顺序找第一条能解释这个失败的行，拿它的判决并按开关落成可执行判决。
     *
     * @param failure 发送过程中抛出的异常，可为 null（视为认不出来）
     * @param phase   失败发生的阶段
     */
    public Decision decide(Throwable failure, Phase phase) {
        return matchFailure(failure, phase).getDecision()
                .resolve(unknownOutcomeRetriesAllowed);
    }

    /** 同 {@link #decide(Throwable, Phase)}，但还告诉你落到哪一行（日志与测试要用）. */
    public Rule explain(Throwable failure, Phase phase) {
        return matchFailure(failure, phase);
    }

    private Rule matchFailure(Throwable failure, Phase phase) {
        if (failure == null) {
            return ruleFor(Tier.UNRECOGNISED);
        }
        // remoting 的同步出口会把认不出来的异常包成 RemotingSendRequestException（见
        // MQClientInstance#invokeAndTranslateException 的最后一个 throw 分支），所以类型要顺着
        // 因果链往里找：外层包装只说"发送这一步失败了"，里面那层才说"为什么失败"。
        // 取最里面一个能被表认出来的档位，是从"包装层层套"这个现实里唯一能稳定分类的读法。
        List<Throwable> chain = causalChainOf(failure);
        for (int depth = chain.size() - 1; depth >= 0; depth--) {
            Throwable candidate = chain.get(depth);
            for (int i = 0; i < DEFAULT_TABLE.length; i++) {
                Rule rule = DEFAULT_TABLE[i];
                if (rule.judge == null || rule.phase != phase || !matches(rule.tier, candidate)) {
                    continue;
                }
                return rule;
            }
        }
        return ruleFor(Tier.UNRECOGNISED);
    }

    /** 因果链（自外向内），遇到自引用环就在这里收住，最多 16 层. */
    private static List<Throwable> causalChainOf(Throwable failure) {
        List<Throwable> chain = new ArrayList<Throwable>();
        Throwable cursor = failure;
        while (cursor != null && chain.size() < MAX_CAUSE_DEPTH) {
            chain.add(cursor);
            Throwable next = cursor.getCause();
            if (next == cursor) {
                break;
            }
            cursor = next;
        }
        return chain;
    }

    private static boolean matches(Tier tier, Throwable failure) {
        if (failure == null) {
            return tier == Tier.UNRECOGNISED;
        }
        switch (tier) {
            case MESSAGE_INVALID:
                return failure instanceof IllegalArgumentException;
            case PRODUCER_NOT_STARTED:
                return failure instanceof IllegalStateException;
            case ROUTE_UNRESOLVED:
                return failure instanceof RemotingSendRequestException;
            case CONNECT_FAILED:
                return failure instanceof RemotingConnectException;
            case SEND_OUTCOME_UNKNOWN:
                return failure instanceof RemotingSendRequestException;
            case RESPONSE_TIMEOUT:
                return failure instanceof RemotingTimeoutException;
            case FLOW_CONTROL:
                return failure instanceof RemotingTooMuchRequestException;
            default:
                return false;
        }
    }

    /**
     * 响应体里解出了存储侧结论：整档禁重试，一个状态值都不例外。
     * <p>
     * 这一档判的是「broker 已经给出结论」这件事本身，不看结论的内容，所以参数只是被记下来、
     * 不参与分支 —— 少一个状态值被漏掉的机会。
     */
    public Decision decideForBodyStatus(SendStatus statusFromResponse) {
        return ruleFor(Tier.STORE_REPORTED_STATUS).getDecision();
    }

    /** 响应码不是 SUCCESS：默认禁重试（第 9 档）；是 SUCCESS 则不构成失败. */
    public Decision decideForResponseCode(int responseCode) {
        if (responseCode == RemotingSysResponseCode.SUCCESS) {
            return Decision.NEVER_RETRY;
        }
        return ruleFor(Tier.REMOTE_RESPONSE_REJECTED).getDecision();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SendRetryPolicy{结论未知档重试=");
        sb.append(unknownOutcomeRetriesAllowed).append(", 默认重试次数=").append(defaultRetryTimes)
                .append(", 表=");
        for (int i = 0; i < DEFAULT_TABLE.length; i++) {
            sb.append("\n  ").append(DEFAULT_TABLE[i]);
        }
        return sb.append("\n}").toString();
    }
}
