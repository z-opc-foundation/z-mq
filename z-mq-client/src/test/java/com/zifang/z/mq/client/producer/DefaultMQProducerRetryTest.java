package com.zifang.z.mq.client.producer;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.QueueData;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.message.Message;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.exception.RemotingConnectException;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.exception.RemotingTooMuchRequestException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.NettyRemotingClient;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * 重试循环的行为尺：白名单九档每一档都从 {@link DefaultMQProducer#send(Message)} 这条路走一遍，
 * 断的是<b>出口读数</b>——RPC 出口被调用了几次、目标地址序列是什么、路由被重取了几次、
 * 退避被调用了几次各自参数是什么、重试计数读到几。
 * <p>
 * 出口是一个覆写了 {@code invokeSync} 的假 {@link MQClientInstance}（同 {@code NullResponseSendTest}
 * 的做法），所以：
 * <ul>
 *   <li>一次 {@code invokeSync} 就是"请求交出去一次"，也就是一次存储写的机会 ⇒ "这条消息只被写了 1 次"
 *       这类断言落在这里；</li>
 *   <li>选队列走的是<b>真</b> {@link MQClientInstance#selectOneMessageQueue(String, TopicRouteData,
 *       java.util.Collection)}（假实例只换 RPC 出口），所以"换没换机器"读的是真实的选择结果序列；</li>
 *   <li>全程不建真连接、不睡真时间：等待走注入的 {@link RecordingSleeper}。</li>
 * </ul>
 */
public class DefaultMQProducerRetryTest {

    private static final String TOPIC = "RetryPolicyTopic";
    private static final String GROUP = "RetryPolicyGroup";

    private FakeInstance instance;
    private DefaultMQProducer producer;
    private RecordingSleeper sleeper;
    private RecordingBackoff backoff;

    @BeforeEach
    public void setUp() {
        instance = new FakeInstance();
        producer = new DefaultMQProducer(GROUP, new NettyClientConfig());
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setClientId("retry-policy-tester");
        producer.mqClientInstance = instance;
        sleeper = new RecordingSleeper();
        backoff = new RecordingBackoff();
        producer.setSendRetrySleeper(sleeper);
        producer.setSendRetryBackoff(backoff);
    }

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            producer.shutdown();
        }
    }

    // ==================== 次数口径 ====================

    @Test
    @DisplayName("默认 3 次的口径钉死：不含首次 ⇒ 持久可重试失败一共走 4 趟 RPC")
    public void defaultThreeRetriesMeansFourAttemptsInTotal() throws Exception {
        assertEquals(3, producer.getRetryTimesWhenSendFailed(), "README 口径的默认值");
        assertEquals(4, producer.getSendAttemptBudget(), "1 次首发 + 3 次重试");

        instance.route(route(4, 1));
        instance.throwEachInvoke(connect("任何一台都连不上"));

        assertThrows(RemotingConnectException.class, () -> producer.send(message()));

        assertEquals(4, instance.invokes, "首发那一趟也算在内，所以是 4 趟");
        assertEquals(3L, producer.getSendRetryCount(), "重试计数只数重试那几趟，必须比 RPC 出口少 1");
        assertEquals(3, sleeper.delays.size(), "每一趟重试之前都问一次退避");
        assertEquals(3, backoff.calls.size());
    }

    @Test
    @DisplayName("可配：retryTimesWhenSendFailed=0 就是一次不重试；=2 就是总共 3 趟")
    public void retryTimesIsConfigurableAndZeroTurnsTheLoopOff() throws Exception {
        instance.route(route(4, 1));
        instance.throwEachInvoke(connect("连不上"));

        producer.setRetryTimesWhenSendFailed(0);
        assertEquals(1, producer.getSendAttemptBudget());
        assertThrows(RemotingConnectException.class, () -> producer.send(message()));
        assertEquals(1, instance.invokes);
        assertEquals(0L, producer.getSendRetryCount());
        assertTrue(sleeper.delays.isEmpty(), "一次都没重试就不该等过");

        instance.invokes = 0;
        producer.setRetryTimesWhenSendFailed(2);
        assertThrows(RemotingConnectException.class, () -> producer.send(message()));
        assertEquals(3, instance.invokes);
        assertEquals(2L, producer.getSendRetryCount());
    }

    // ==================== 第 1/2 档：禁 ====================

    @Test
    @DisplayName("第 1 档（消息不合法）：RPC 出口一次都不许多走，异常原样是 IllegalArgumentException")
    public void illegalArgumentNeverReachesTheRpcExit() {
        instance.route(route(4, 1));
        instance.throwEachInvoke(connect("如果这被走到，说明禁重试那一档漏了"));

        Message badTopic = new Message();
        badTopic.setTopic("");
        badTopic.setBody("x".getBytes(StandardCharsets.UTF_8));
        assertThrows(IllegalArgumentException.class, () -> producer.send(badTopic));

        Message badBody = new Message();
        badBody.setTopic(TOPIC);
        assertThrows(IllegalArgumentException.class, () -> producer.send(badBody));

        assertThrows(IllegalArgumentException.class, () -> producer.send(null));

        assertEquals(0, instance.invokes, "确定性错误重试只是把同一次失败做 N 遍: " + instance.describe());
        assertEquals(0L, producer.getSendRetryCount());
        assertEquals(0, instance.routeFetches, "连路由都不该去拿");
    }

    @Test
    @DisplayName("第 2 档（producer 没 start）：当场抛，不进循环")
    public void notStartedProducerIsNotRetried() throws Exception {
        DefaultMQProducer cold = new DefaultMQProducer("ColdGroup", new NettyClientConfig());
        cold.setNamesrvAddr("127.0.0.1:9876");
        try {
            assertThrows(IllegalStateException.class, () -> cold.send(message()));
        } finally {
            cold.shutdown();
        }
        assertEquals(0, instance.invokes);
        assertEquals(0L, producer.getSendRetryCount());
    }

    // ==================== 第 3 档：可，且必须先重取路由 ====================

    @Test
    @DisplayName("第 3 档（No route）：重试之前必须真的把路由重取一次；不重取就一直拿同一份 null")
    public void routeFailureRetriesOnlyAfterReFetchingTheRoute() throws Exception {
        // NameServer 那一侧：第一趟拿不到路由，第二趟才给
        instance.answerRoutes(null, route(2, 1));

        SendResult result = producer.send(message());

        assertSame(SendStatus.SEND_OK, result.getSendStatus(), "重取到路由之后这一趟就该成: " + result);
        assertEquals(2, instance.routeFetches, "首发一趟、重取一趟");
        assertEquals(1, instance.routeRefreshes, "★ 重试那一趟之前必须走重取入口，而不是再读一次缓存");
        assertEquals(1, instance.invokes, "只有拿到路由的那一趟才允许碰 RPC 出口");
        assertEquals(1L, producer.getSendRetryCount());
    }

    @Test
    @DisplayName("第 3 档另一条形状（No writable queue）：同样必须先重取路由；两形状不许当同一条复制")
    public void noWritableQueueAlsoRefreshesTheRouteFirst() throws Exception {
        TopicRouteData noWritable = route(2, 1);
        for (QueueData qd : noWritable.getQueueDatas()) {
            qd.setWriteQueueNums(0); // 读队列还在，写队列没了
        }
        instance.answerRoutes(noWritable, route(2, 1));

        assertSame(SendStatus.SEND_OK, producer.send(message()).getSendStatus());
        assertEquals(1, instance.routeRefreshes, "路由在但没得写 ⇒ 要的是重取，不是就地再选一次");
        assertEquals(1, instance.invokes);
        assertEquals(1L, producer.getSendRetryCount());
    }

    @Test
    @DisplayName("第 3 档的\"没 master 地址\"形状：也走重取；这条路径上没有\"无可写队列\"那一支")
    public void missingMasterAddrIsAlsoARouteTier() throws Exception {
        TopicRouteData noMaster = route(2, 1);
        for (BrokerData bd : noMaster.getBrokerDatas()) {
            bd.getBrokerAddrs().clear(); // 有队列、有 brokerName，但没有可连地址
        }
        instance.answerRoutes(noMaster, route(2, 1));

        assertSame(SendStatus.SEND_OK, producer.send(message()).getSendStatus());
        assertEquals(1, instance.routeRefreshes);
        assertEquals(1, instance.invokes);
    }

    // ==================== 第 4 档：换机器 ====================

    @Test
    @DisplayName("★ 第 4 档：连接失败后换机重试成功（前两台都是死地址，第三台才收下）")
    public void connectFailureFailsOverToAnotherBrokerAndSucceeds() throws Exception {
        instance.route(route(4, 1));
        instance.throwFirstTwoInvokesWithConnectFailureThenSucceed();

        SendResult result = producer.send(message());

        assertSame(SendStatus.SEND_OK, result.getSendStatus());
        assertEquals(3, instance.invokes, "两次换机 + 一次成功: " + instance.describe());
        assertEquals(2L, producer.getSendRetryCount());
        List<String> targets = new ArrayList<String>(instance.invokedAddrs);
        assertEquals(3, new HashSet<String>(targets).size(),
                "★ 三趟必须落在三台不同的机器上，摇到同一台不叫换机器: " + targets);
        assertEquals(0, instance.routeRefreshes, "地址就在缓存那份路由里，这一档要的是换一台而不是重取");
    }

    @Test
    @DisplayName("第 4 档：持续连不上时四趟的机器互不相同（路由里有四台）")
    public void persistentConnectFailuresWalkDistinctBrokers() throws Exception {
        instance.route(route(4, 1));
        instance.throwEachInvoke(connect("全下线了"));

        assertThrows(RemotingConnectException.class, () -> producer.send(message()));

        assertEquals(4, instance.invokes);
        assertEquals(4, instance.invokedAddrs.size(), "四趟就该留下四次出口记录");
        assertEquals(4, new HashSet<String>(instance.invokedAddrs).size(),
                "★ 路由里四台机器，四趟就该是四台不同的: " + instance.invokedAddrs);
    }

    @Test
    @DisplayName("第 4 档的下限：路由里只剩一台时，换不了机器也要把预算试满（口径写在表上，不是摇出来的）")
    public void singleBrokerRouteStillExhaustsTheBudget() throws Exception {
        instance.route(route(1, 1));
        instance.throwEachInvoke(connect("只有一台，而这台连不上"));

        assertThrows(RemotingConnectException.class, () -> producer.send(message()));

        assertEquals(4, instance.invokes, "换不了机器不等于不必重试: " + instance.describe());
        assertEquals(1, new HashSet<String>(instance.invokedAddrs).size());
        assertEquals(3L, producer.getSendRetryCount());
    }

    // ==================== 第 5/6 档：默认禁，显式开 ====================

    @Test
    @DisplayName("第 5 档（写失败，结论未知）默认禁：RPC 出口只走一次")
    public void sendRequestFailureIsNotRetriedByDefault() throws Exception {
        instance.route(route(4, 1));
        instance.throwEachInvoke(new RemotingSendRequestException("bytes may already be in the socket"));

        assertThrows(RemotingSendRequestException.class, () -> producer.send(message()));
        assertEquals(1, instance.invokes, "默认口径宁可少试不可多写: " + instance.describe());
        assertEquals(0L, producer.getSendRetryCount());
        assertTrue(sleeper.delays.isEmpty());
    }

    @Test
    @DisplayName("第 5 档显式接受 at-least-once 之后：换机器试满，且地址互不相同")
    public void sendRequestFailureRetriesOnlyWhenTheCallerAcceptsAtLeastOnce() throws Exception {
        producer.setRetryWhenSendOutcomeUnknown(true);
        assertTrue(producer.isRetryWhenSendOutcomeUnknown());

        instance.route(route(4, 1));
        instance.throwEachInvoke(new RemotingSendRequestException("bytes may already be in the socket"));

        assertThrows(RemotingSendRequestException.class, () -> producer.send(message()));
        assertEquals(4, instance.invokes);
        assertEquals(4, new HashSet<String>(instance.invokedAddrs).size(),
                "放开之后同样必须换机器，不许再戳同一台: " + instance.invokedAddrs);
        assertEquals(3L, producer.getSendRetryCount());
    }

    @Test
    @DisplayName("第 6 档（超时）默认禁；显式放开后按第 5 档同一形状走")
    public void timeoutIsForbiddenByDefaultAndOptInAfter() throws Exception {
        instance.route(route(4, 1));
        instance.throwEachInvoke(new RemotingTimeoutException("wait response on the channel timeout 3000ms"));

        assertThrows(RemotingTimeoutException.class, () -> producer.send(message()));
        assertEquals(1, instance.invokes, "超时不等于没送到，很可能已经进了存储: " + instance.describe());
        assertEquals(0L, producer.getSendRetryCount());

        instance.invokes = 0;
        instance.invokedAddrs.clear();
        producer.setRetryWhenSendOutcomeUnknown(true);
        assertThrows(RemotingTimeoutException.class, () -> producer.send(message()));
        assertEquals(4, instance.invokes);
        assertEquals(4, new HashSet<String>(instance.invokedAddrs).size());
    }

    @Test
    @DisplayName("第 5 档的\"根本没拿到响应\"那一支（remoting 契约被违反）同样默认禁")
    public void nullResponseIsTreatedAsAnUnknownOutcomeAndNotRetried() throws Exception {
        instance.route(route(4, 1));
        instance.returnNullResponse = true;

        assertThrows(RemotingSendRequestException.class, () -> producer.send(message()));
        assertEquals(1, instance.invokes, "没有响应是\"不知道结果\"，不是\"再写一遍\"的理由");
        assertEquals(0L, producer.getSendRetryCount());
    }

    // ==================== 第 7 档：流控 ⇒ 退避 ====================

    @Test
    @DisplayName("第 7 档（流控）：重试并且每次都问退避；退避参数是被记录的读数，不是挂钟")
    public void flowControlRetriesWithARecordedBackoff() throws Exception {
        instance.route(route(4, 1));
        // 同步出口给回来的就是"RemotingSendRequestException 里包着流控"这个形状，
        // 所以这一档必须靠因果链认出来；把它当成第 5 档（结论未知）就是默认禁 ⇒ 一趟都不许多走
        instance.throwEachInvoke(new RemotingTooMuchRequestException("system busy, flow control"));

        assertThrows(RemotingSendRequestException.class, () -> producer.send(message()));

        assertEquals(4, instance.invokes);
        assertEquals(3L, producer.getSendRetryCount());
        assertEquals(4, new HashSet<String>(instance.invokedAddrs).size(),
                "流控之后也要换一台: " + instance.invokedAddrs);
        // ★ 退避的账：被调用了几次、每次参数是什么
        assertEquals(3, backoff.calls.size(), "退避调用记录=" + backoff.describe());
        assertEquals("1/" + SendRetryPolicy.Tier.FLOW_CONTROL, backoff.calls.get(0),
                "第一次重试传进去的是第 1 趟与流控那一档: " + backoff.describe());
        assertEquals("3/" + SendRetryPolicy.Tier.FLOW_CONTROL, backoff.calls.get(2),
                backoff.describe());
        assertEquals(3, sleeper.delays.size(), "算了退避就必须真的等这一次: " + sleeper.describe());
        assertEquals(20L, sleeper.delays.get(0).longValue(), "默认 20ms 起步: " + sleeper.describe());
        assertEquals(40L, sleeper.delays.get(1).longValue());
        assertEquals(80L, sleeper.delays.get(2).longValue());
        assertEquals(3L, producer.getSendRetryCount(), "三处读数必须互相对上：退避次数=等待次数=重试计数");
    }

    @Test
    @DisplayName("第 4 档不退避：下一台就在同一份路由里，多等一毫秒只是把这次发送拖慢")
    public void connectFailureDoesNotBackOff() throws Exception {
        instance.route(route(4, 1));
        instance.throwEachInvoke(connect("连不上"));

        assertThrows(RemotingConnectException.class, () -> producer.send(message()));

        assertEquals(3, backoff.calls.size(), backoff.describe());
        assertEquals(3, sleeper.delays.size(), sleeper.describe());
        for (Long delay : sleeper.delays) {
            assertEquals(0L, delay.longValue(), "非流控档不退避: " + sleeper.describe());
        }
    }

    // ==================== 第 8 档：响应体结论 ⇒ 绝对禁 ====================

    @Test
    @DisplayName("★ 第 8 档：broker 回了刷盘超时的结论 ⇒ 这条消息只被写了 1 次（RPC 出口只走一趟）")
    public void aStoreVerdictInTheResponseBodyIsNeverRewritten() throws Exception {
        instance.route(route(4, 1));
        instance.respondWithBodyStatus(SendStatus.FLUSH_DISK_TIMEOUT);

        SendResult result = producer.send(message());

        assertSame(SendStatus.FLUSH_DISK_TIMEOUT, result.getSendStatus(),
                "结论必须原样交给调用方，不许被重试洗成 SEND_OK");
        assertEquals(1, instance.invokes,
                "★ 消息已经进了存储，再来一趟就是写第二遍: " + instance.describe());
        assertEquals(0L, producer.getSendRetryCount());
        assertTrue(sleeper.delays.isEmpty());
        assertEquals(0, instance.routeRefreshes);
    }

    @Test
    @DisplayName("★ 第 8 档对每一个非成功结论都禁（把状态枚举整个过一遍，不留\"漏了哪一个\"的缝）")
    public void everyStoreVerdictFromTheBodyIsWrittenOnlyOnce() throws Exception {
        int visited = 0;
        for (SendStatus status : SendStatus.values()) {
            visited++;
            instance.invokes = 0;
            instance.resetScript();
            instance.route(route(4, 1));
            instance.respondWithBodyStatus(status);

            SendResult result = producer.send(message());
            assertSame(status, result.getSendStatus(), "结论原样透传: " + status);
            assertEquals(1, instance.invokes,
                    "★ 存储侧结论一律只写一份，状态=" + status + " 时出口走了 " + instance.invokes + " 趟");
            assertEquals(0L, producer.getSendRetryCount(), "状态=" + status);
        }
        assertEquals(SendStatus.values().length, visited,
                "上面那个遍历必须真的覆盖每一个存储结论: visited=" + visited);
    }

    // ==================== 第 9 档：响应码不是 SUCCESS ⇒ 默认禁 ====================

    @Test
    @DisplayName("★ 第 9 档：响应码 SYSTEM_ERROR ⇒ 就地造 SEND_FAILED 交回，且一次都不许多写")
    public void aRejectedResponseCodeIsReportedAsFailedWithoutAnotherWrite() throws Exception {
        instance.route(route(4, 1));
        instance.respondWithCode(RemotingSysResponseCode.SYSTEM_ERROR, "commitLog not initialized");

        SendResult result = producer.send(message());

        assertSame(SendStatus.SEND_FAILED, result.getSendStatus(), "client 只能报通用失败");
        assertEquals("commitLog not initialized", result.getErrorMsg());
        assertEquals(1, instance.invokes,
                "★ 响应码分不清写前写后 ⇒ 默认不重试: " + instance.describe());
        assertEquals(0L, producer.getSendRetryCount());
    }

    @Test
    @DisplayName("第 9 档的开关免疫：把 at-least-once 打开也不许放行响应码这一档")
    public void theOptInSwitchDoesNotTouchTheResponseCodeTier() throws Exception {
        producer.setRetryWhenSendOutcomeUnknown(true);
        instance.route(route(4, 1));
        instance.respondWithCode(RemotingSysResponseCode.SYSTEM_BUSY, "busy");

        assertSame(SendStatus.SEND_FAILED, producer.send(message()).getSendStatus());
        assertEquals(1, instance.invokes, "开关管的是\"不知道送没送到\"，这一档是 broker 明确回了非 SUCCESS: "
                + instance.describe());
        assertEquals(0L, producer.getSendRetryCount());
    }

    // ==================== 计数出口必须有读者（这一条本身就是那个读者） ====================

    @Test
    @DisplayName("重试计数的起点必须是 0，且它涨的格数等于\"多走的 RPC 趟数\"")
    public void theRetryCounterIsReadAndTracksExtraAttempts() throws Exception {
        assertEquals(0L, producer.getSendRetryCount(), "起点：一位没重试过必须是 0");

        instance.route(route(4, 1));
        instance.throwFirstTwoInvokesWithConnectFailureThenSucceed();
        producer.send(message());

        long retries = producer.getSendRetryCount();
        assertEquals(2L, retries, "三趟 RPC、其中两趟是重试: " + instance.describe());
        assertEquals(instance.invokes - 1, retries,
                "计数与出口读数必须一致（计数不是一句口号）: invokes=" + instance.invokes);
    }

    // ==================== 辅助 ====================

    private static Message message() {
        Message message = new Message();
        message.setTopic(TOPIC);
        message.setBody("retry-me".getBytes(StandardCharsets.UTF_8));
        message.setKeys("k-retry");
        return message;
    }

    private static RemotingConnectException connect(String why) {
        return new RemotingConnectException("scripted:" + why);
    }

    private static List<String> uniqueOf(List<String> in) {
        return new ArrayList<String>(new HashSet<String>(in));
    }

    /** 造一份 N 台 broker、每台 M 个写队列的路由；地址可按机器名对上. */    private static TopicRouteData route(int brokerCount, int writeQueueNums) {
        TopicRouteData route = new TopicRouteData();
        List<QueueData> queueDatas = new ArrayList<QueueData>();
        List<BrokerData> brokerDatas = new ArrayList<BrokerData>();
        for (int i = 0; i < brokerCount; i++) {
            String name = "broker-" + i;
            QueueData qd = new QueueData();
            qd.setBrokerName(name);
            qd.setReadQueueNums(writeQueueNums);
            qd.setWriteQueueNums(writeQueueNums);
            qd.setPerm(6);
            queueDatas.add(qd);

            HashMap<Long, String> addrs = new HashMap<Long, String>();
            addrs.put(BrokerData.MASTER_ID, "127.0.0.2:" + (20000 + i));
            brokerDatas.add(new BrokerData("retry-cluster", name, addrs));
        }
        route.setQueueDatas(queueDatas);
        route.setBrokerDatas(brokerDatas);
        return route;
    }

    /** 记录\"退避算了什么、等了什么\"的假 sleeper：等待时长成为可断言的读数. */
    private static final class RecordingSleeper implements SendRetrySleeper {
        final List<Long> delays = new ArrayList<Long>();

        @Override
        public void await(long delayMillis) {
            delays.add(Long.valueOf(delayMillis));
        }

        String describe() {
            return "await 次数=" + delays.size() + " 参数=" + delays;
        }
    }

    /** 记录\"第几趟、哪一档\"的假退避，时长仍按默认算法算，避免把被测值换成测试值. */
    private static final class RecordingBackoff implements SendRetryBackoff {
        private final SendRetryBackoff delegate = SendRetryBackoff.DEFAULT;
        final List<String> calls = new ArrayList<String>();

        @Override
        public long delayMillisFor(int retryIndex, SendRetryPolicy.Tier tier) {
            calls.add(retryIndex + "/" + tier);
            return delegate.delayMillisFor(retryIndex, tier);
        }

        String describe() {
            return "退避调用次数=" + calls.size() + " 参数(趟/档位)=" + calls;
        }
    }

    /**
     * 只换掉两件事的假实例：路由从脚本回答（可返回 null 以模拟拿不到）、RPC 出口按脚本抛/回。
     * 选队列走父类真实现，所以"换没换机器"读的是真实选择结果。
     */
    private static final class FakeInstance extends MQClientInstance {

        final NettyRemotingClient remotingMock = mock(NettyRemotingClient.class);

        /** 每次"真的去取名址"的回答脚本（含 null，表示这一次问不到路由）. */
        private final List<TopicRouteData> routeAnswers = new ArrayList<TopicRouteData>();
        private int routeAnswerCursor;
        /** 脚本弹空之后的常驻答案（相当于 NameServer 稳定可达）. */
        private TopicRouteData standingRoute;
        /** 客户端本地那份路由缓存. */
        private TopicRouteData cachedRoute;

        private final Deque<Throwable> invokeScript = new ArrayDeque<Throwable>();
        private Throwable everyInvoke;
        private RemotingCommand repeatingResponse;

        volatile boolean returnNullResponse;
        volatile int invokes;
        volatile int routeFetches;
        volatile int routeRefreshes;
        final List<String> invokedAddrs = new ArrayList<String>();

        FakeInstance() {
            super("retry-fake-instance", "127.0.0.1:9876", new NettyClientConfig());
        }

        @Override
        public NettyRemotingClient getRemotingClient() {
            return remotingMock;
        }

        // ---------- 路由 ----------

        /** 名址稳定可取（脚本清空、常驻答案就是它），本地缓存里已经有这份. */
        void route(TopicRouteData good) {
            routeAnswers.clear();
            routeAnswerCursor = 0;
            standingRoute = good;
            cachedRoute = good;
        }

        /** 每次"真的去取"弹一格脚本；弹空之后一直给最后一格. */
        void answerRoutes(TopicRouteData... inOrder) {
            routeAnswers.clear();
            routeAnswerCursor = 0;
            cachedRoute = null;
            standingRoute = null;
            for (TopicRouteData data : inOrder) {
                routeAnswers.add(data);
            }
            if (inOrder.length > 0) {
                standingRoute = inOrder[inOrder.length - 1];
            }
        }

        @Override
        public TopicRouteData getTopicRouteData(String topic) {
            if (cachedRoute != null) {
                return cachedRoute;
            }
            routeFetches++;
            if (routeAnswerCursor < routeAnswers.size()) {
                cachedRoute = routeAnswers.get(routeAnswerCursor++);
            } else {
                cachedRoute = standingRoute;
            }
            return cachedRoute;
        }

        @Override
        public TopicRouteData refreshTopicRouteData(String topic) {
            routeRefreshes++;
            cachedRoute = null;
            return getTopicRouteData(topic);
        }

        // ---------- RPC 出口 ----------

        void throwEachInvoke(Throwable failure) {
            everyInvoke = failure;
        }

        void resetScript() {
            invokeScript.clear();
            everyInvoke = null;
            repeatingResponse = null;
            returnNullResponse = false;
            invokedAddrs.clear();
        }

        void throwFirstTwoInvokesWithConnectFailureThenSucceed() {
            invokeScript.addLast(connect("第 1 趟那台连不上"));
            invokeScript.addLast(connect("第 2 趟那台连不上"));
        }

        void respondWithBodyStatus(SendStatus status) {
            SendResult remote = new SendResult(status);
            remote.setMsgId("store-side-msg-id");
            remote.setTopic(TOPIC);
            remote.setQueueId(0);
            remote.setQueueOffset(11L);
            repeatingResponse = success(JsonCodec.encode(remote));
        }

        void respondWithCode(int code, String remark) {
            RemotingCommand response = RemotingCommand.createResponseCommand(code);
            response.setRemark(remark);
            repeatingResponse = response;
        }

        private static RemotingCommand success(byte[] body) {
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            response.setBody(body);
            return response;
        }

        @Override
        public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
                throws RemotingConnectException, RemotingSendRequestException,
                RemotingTimeoutException, InterruptedException {
            invokes++;
            invokedAddrs.add(addr);
            if (returnNullResponse) {
                return null;
            }
            Throwable pending = invokeScript.isEmpty() ? everyInvoke : invokeScript.pollFirst();
            if (pending != null) {
                rethrow(pending);
            }
            return repeatingResponse != null ? repeatingResponse : success(null);
        }

        /**
         * 按 remoting 同步出口的真实翻译抛出（与
         * {@code MQClientInstance#invokeAndTranslateException} 同一形状）：声明里有的三种
         * 异常原样抛，其余一律包进 {@link RemotingSendRequestException} 的 cause 里。
         */
        private static void rethrow(Throwable failure)
                throws RemotingConnectException, RemotingSendRequestException,
                RemotingTimeoutException, InterruptedException {
            if (failure instanceof RemotingConnectException) {
                throw (RemotingConnectException) failure;
            }
            if (failure instanceof RemotingSendRequestException) {
                throw (RemotingSendRequestException) failure;
            }
            if (failure instanceof RemotingTimeoutException) {
                throw (RemotingTimeoutException) failure;
            }
            if (failure instanceof InterruptedException) {
                throw (InterruptedException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            // 流控（RemotingTooMuchRequestException）走的就是这一支：同步出口把它包起来，
            // 所以循环看到的形状与真机上完全一致
            throw RemotingSendRequestException.newSendRequestException("127.0.0.1:10911", failure);
        }

        String describe() {
            return "invokes=" + invokes + " addrs=" + invokedAddrs
                    + " routeFetches=" + routeFetches + " routeRefreshes=" + routeRefreshes;
        }
    }

    @Test
    @DisplayName("引用自检：本用例真的在用带排除入口的那条选择 API（防止签名漂了而测试还在绿）")
    public void theExclusionSelectorIsTheOneUnderTest() {
        TopicRouteData four = route(4, 1);
        Set<String> excluded = new HashSet<String>();
        excluded.add("broker-0");
        MessageQueue picked = instance.selectOneMessageQueue(TOPIC, four, excluded);
        assertNotNull(picked);
        assertTrue(!"broker-0".equals(picked.getBrokerName()),
                "带排除的入口必须存在且真的排除: " + picked);
        assertEquals(0, instance.routeRefreshes, "直接选队列不该去动路由");
    }
}
