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
     * 同步拉取。
     *
     * @param mq         队列
     * @param offset     起始 offset
     * @param maxNums    最多拉多少条
     * @return 拉取结果（包含 nextOffset 与消息列表）
     */
    public PullResult pull(MessageQueue mq, long offset, int maxNums) throws Exception {
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

        RemotingCommand response = mqClientInstance.invokeSync(brokerAddr, request, 3000);
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
        return new PullResult(status, mq, nextOffset, msgs == null ? java.util.Collections.emptyList() : msgs);
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
        if (routeData == null || routeData.getBrokerDatas() == null) return null;
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

        public PullResult(PullStatus status, MessageQueue mq, long nextOffset, List<MessageExt> msgs) {
            this.status = status;
            this.messageQueue = mq;
            this.nextOffset = nextOffset;
            this.msgFoundList = msgs;
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
    }
}