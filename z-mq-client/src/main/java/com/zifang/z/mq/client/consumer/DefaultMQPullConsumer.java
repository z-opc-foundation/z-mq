package com.zifang.z.mq.client.consumer;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 拉模式消费者（对标 RocketMQ DefaultMQPullConsumer）.
 * <p>
 * 用户自行调用 {@link #pull(MessageQueue, long, int)} 拉取消息，自行管理 offset。
 * 适合"应用驱动"场景 (例如按需拉取、批处理、流控)。
 */
public class DefaultMQPullConsumer {

    private static final Logger log = LogManager.getLogger(DefaultMQPullConsumer.class);

    /**
     * 挂起预算在 pull 请求里的字段名 —— 与 Broker 侧
     * {@code PullMessageProcessor.EXT_SUSPEND_TIMEOUT_MILLIS} 是同一个线上契约
     * （client 不依赖 broker, 所以这里只能是字面量; 两侧由 LongPollingWiringGuardTest 钉住同一个值）。
     */
    public static final String SUSPEND_TIMEOUT_FIELD = "suspendTimeoutMillis";

    /** 不带挂起预算时的 pull RPC 超时（接线前写死的 3000ms，逐字保留）。 */
    public static final long PULL_RPC_TIMEOUT_MILLIS = 3000L;

    /**
     * 响应里"这次 pull 因何而醒"的字段名 —— 与 Broker 侧
     * {@code PullMessageProcessor.EXT_SUSPEND_WAKEUP} 同一个线上契约。
     */
    public static final String SUSPEND_WAKEUP_FIELD = "suspendWakeup";

    /**
     * 带挂起预算时，RPC 超时在预算之上再留的余量。
     * <p>
     * 为什么必须有这块余量：Broker 那边挂起到期由 PullRequestHoldService 的
     * 扫描线程兑现（扫描周期 1s），真实释放时刻最晚是"预算 + 一个扫描周期"，再算上响应写回与
     * 网络回程。客户端若只比预算多等一点点，就会出现"Broker 正要返回、客户端已经超时"，
     * 而 pull 超时在客户端被读成 CONNECTION_LOST/异常 ⇒ 接了长轮询反而比不接更坏。
     */
    public static final long SUSPEND_RPC_MARGIN_MILLIS = 3000L;

    /**
     * pull 这一次 RPC 允许的超时：结构上保证"客户端一定比 Broker 醒得晚"。
     * <p>
     * 不变式（被机检钉住，不留在注释里）: 对任意预算 b ≥ 0 都有
     * {@code pullRpcTimeoutMillis(b) > b}，且 Broker 侧实际挂起时长 {@code <= b}。
     *
     * @param suspendBudgetMillis 本次 pull 给 Broker 的挂起预算, {@code <= 0} 表示不挂起
     * @return RPC 超时（毫秒）
     */
    public static long pullRpcTimeoutMillis(long suspendBudgetMillis) {
        if (suspendBudgetMillis <= 0) {
            return PULL_RPC_TIMEOUT_MILLIS;
        }
        return suspendBudgetMillis + SUSPEND_RPC_MARGIN_MILLIS;
    }

    private String consumerGroup;
    private String namesrvAddr;
    private NettyClientConfig nettyClientConfig;
    private String clientId;

    private MQClientInstance mqClientInstance;
    private final ConcurrentHashMap<String, MQClientInstance> instanceTable = new ConcurrentHashMap<>();
    /** 队列 offset 表 (Memory-local). */
    private final Map<MessageQueue, Long> offsetTable = new ConcurrentHashMap<>();

    public DefaultMQPullConsumer(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void start() throws Exception {
        if (this.clientId == null) {
            this.clientId = "PULL_" + consumerGroup + "_" + System.currentTimeMillis();
        }
        if (this.nettyClientConfig == null) {
            this.nettyClientConfig = new NettyClientConfig();
        }
        this.mqClientInstance = getOrCreateInstance();
        log.info("DefaultMQPullConsumer started: group={} namesrv={}", consumerGroup, namesrvAddr);
    }

    public void shutdown() {
        for (MQClientInstance ins : instanceTable.values()) {
            ins.shutdown();
        }
        instanceTable.clear();
        offsetTable.clear();
        log.info("DefaultMQPullConsumer shutdown: group={}", consumerGroup);
    }

    /**
     * 同步拉取（短轮询：读不到就立刻返回）。
     *
     * @param mq         队列
     * @param offset     起始 offset
     * @param maxNums    最多拉多少条
     * @return 拉取结果（包含 nextOffset 与消息列表）
     */
    public PullResult pull(MessageQueue mq, long offset, int maxNums) throws Exception {
        return pull(mq, offset, maxNums, 0L);
    }

    /**
     * 同步拉取，允许 Broker 端长轮询。
     *
     * @param mq                  队列
     * @param offset              起始 offset
     * @param maxNums             最多拉多少条
     * @param suspendBudgetMillis 允许 Broker 在"这个 offset 读不到消息"时挂起多久（毫秒）;
     *                            {@code <= 0} 即短轮询, 请求与响应形状与三参版本逐字相同
     * @return 拉取结果（包含 nextOffset 与消息列表）
     */
    public PullResult pull(MessageQueue mq, long offset, int maxNums, long suspendBudgetMillis) throws Exception {
        if (mq == null) {
            throw new IllegalArgumentException("MessageQueue is null");
        }
        if (mqClientInstance == null) {
            throw new IllegalStateException("consumer not started");
        }

        TopicRouteData routeData = mqClientInstance.getTopicRouteData(mq.getTopic());
        if (routeData == null) {
            throw new RemotingSendRequestException("No route for topic: " + mq.getTopic());
        }
        String brokerAddr = lookupBrokerAddr(routeData, mq.getBrokerName());
        if (brokerAddr == null) {
            throw new RemotingSendRequestException("No broker addr for: " + mq.getBrokerName());
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        request.addExtField("topic", mq.getTopic());
        request.addExtField("queueId", String.valueOf(mq.getQueueId()));
        request.addExtField("offset", String.valueOf(offset));
        request.addExtField("maxNum", String.valueOf(maxNums));
        if (suspendBudgetMillis > 0) {
            // 只带预算的请求才会挂起
            request.addExtField(SUSPEND_TIMEOUT_FIELD, String.valueOf(suspendBudgetMillis));
        }

        RemotingCommand response = mqClientInstance.invokeSync(brokerAddr, request,
                pullRpcTimeoutMillis(suspendBudgetMillis));
        if (response == null) {
            return new PullResult(PullStatus.CONNECTION_LOST, mq, offset, java.util.Collections.emptyList());
        }
        if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
            return new PullResult(PullStatus.SYSTEM_ERROR, mq, offset, java.util.Collections.emptyList());
        }
        PullResultPayload payload = JsonCodec.decode(response.getBody(), PullResultPayload.class);
        if (payload == null) {
            return new PullResult(PullStatus.NO_MATCHED_MSG, mq, offset, java.util.Collections.emptyList());
        }
        long nextOffset = payload.getNextOffset();
        List<MessageExt> msgs = payload.getMessages();
        PullStatus status = msgs == null || msgs.isEmpty()
                ? PullStatus.NO_NEW_MSG
                : PullStatus.FOUND;
        return new PullResult(status, mq, nextOffset, msgs == null ? java.util.Collections.emptyList() : msgs,
                response.getExtField(SUSPEND_WAKEUP_FIELD));
    }

    /**
     * 获取某队列当前 offset（本地缓存）。
     */
    public long getOffset(MessageQueue mq) {
        Long off = offsetTable.get(mq);
        return off == null ? 0L : off;
    }

    /**
     * 保存 offset（用户主动 ack）。
     */
    public void updateOffset(MessageQueue mq, long offset) {
        offsetTable.put(mq, offset);
    }

    private String lookupBrokerAddr(TopicRouteData routeData, String brokerName) {
        if (routeData == null || routeData.getBrokerDatas() == null) {
            return null;
        }

        for (BrokerData bd : routeData.getBrokerDatas()) {
            if (bd.getBrokerName().equals(brokerName)) {
                return bd.selectBrokerAddr();
            }
        }
        return null;
    }

    private MQClientInstance getOrCreateInstance() throws InterruptedException {
        if (mqClientInstance != null && mqClientInstance.getRemotingClient() != null) {
            return mqClientInstance;
        }
        String key = "PULL@" + consumerGroup;
        MQClientInstance inst = instanceTable.get(key);
        if (inst == null) {
            inst = new MQClientInstance(clientId, namesrvAddr, nettyClientConfig);
            inst.start();
            instanceTable.put(key, inst);
        }
        return inst;
    }

    // ===== Getters / Setters =====

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public void setNettyClientConfig(NettyClientConfig nettyClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * 拉取状态（对标 RocketMQ PullStatus）.
     */
    public enum PullStatus {
        /** 找到消息 */
        FOUND,
        /** 没有新消息 */
        NO_NEW_MSG,
        /** 没有匹配的消息 (offset 已越界) */
        NO_MATCHED_MSG,
        /** 系统错误 */
        SYSTEM_ERROR,
        /** 连接丢失 */
        CONNECTION_LOST
    }

    /**
     * 拉取结果封装。
     */
    public static class PullResult {
        private final PullStatus status;
        private final MessageQueue messageQueue;
        private final long nextOffset;
        private final List<MessageExt> msgFoundList;
        /**
         * Broker 这次为什么放开挂起的 pull（长轮询专用）。
         * <p>
         * null = 这次请求没挂起（不带预算的正常形状）；"message" 才代表"消息到达把这条 pull 叫醒了"，
         * 取值见 {@code PullMessageProcessor.EXT_SUSPEND_WAKEUP} 的说明。
         */
        private final String suspendWakeup;

        public PullResult(PullStatus status, MessageQueue mq, long nextOffset, List<MessageExt> msgs) {
            this(status, mq, nextOffset, msgs, null);
        }

        public PullResult(PullStatus status, MessageQueue mq, long nextOffset, List<MessageExt> msgs,
                          String suspendWakeup) {
            this.status = status;
            this.messageQueue = mq;
            this.nextOffset = nextOffset;
            this.msgFoundList = msgs;
            this.suspendWakeup = suspendWakeup;
        }

        public PullStatus getStatus() {
            return status;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public List<MessageExt> getMsgFoundList() {
            return msgFoundList;
        }

        /**
         * @return Broker 回写的"因何而醒"标记, null 表示本次没有挂起
         */
        public String getSuspendWakeup() {
            return suspendWakeup;
        }
    }
}