package com.zifang.z.mq.client.consumer;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 推模式消费者（对标 RocketMQ DefaultMQPushConsumer）.
 * <p>
 * 内部启动定时拉取线程, 拉到的消息回调 {@link MessageListener}。
 * MVP 实现：单线程轮询所有订阅的队列, 不分队列并发，不支持 rebalance。
 */
public class DefaultMQPushConsumer {

    private static final Logger log = LogManager.getLogger(DefaultMQPushConsumer.class);

    private String consumerGroup;
    private String namesrvAddr;
    private NettyClientConfig nettyClientConfig;
    private String clientId;
    private long pullIntervalMillis = 1000;
    private int pullBatchSize = 32;

    private MQClientInstance mqClientInstance;
    private final ConcurrentHashMap<String, MQClientInstance> instanceTable = new ConcurrentHashMap<>();
    /** 订阅表 (topic -> 订阅信息). */
    private final ConcurrentHashMap<String, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();
    /** 队列 offset 表. */
    private final Map<MessageQueue, Long> offsetTable = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService pullExecutor;
    private MessageListener listener;

    public DefaultMQPushConsumer(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void start() throws Exception {
        if (this.clientId == null) {
            this.clientId = "PUSH_" + consumerGroup + "_" + System.currentTimeMillis();
        }
        if (this.nettyClientConfig == null) {
            this.nettyClientConfig = new NettyClientConfig();
        }
        if (this.listener == null) {
            throw new IllegalStateException("MessageListener not set");
        }
        if (subscriptionTable.isEmpty()) {
            throw new IllegalStateException("No subscription registered. Call subscribe(topic, listener) first.");
        }
        this.mqClientInstance = getOrCreateInstance();
        this.pullExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PushConsumerPullThread");
            t.setDaemon(true);
            return t;
        });
        this.running.set(true);
        this.pullExecutor.scheduleWithFixedDelay(this::doPull, 500, pullIntervalMillis, TimeUnit.MILLISECONDS);
        log.info("DefaultMQPushConsumer started: group={} topics={}", consumerGroup, subscriptionTable.keySet());
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            if (pullExecutor != null) {
                pullExecutor.shutdownNow();
            }
            for (MQClientInstance ins : instanceTable.values()) {
                ins.shutdown();
            }
            instanceTable.clear();
            offsetTable.clear();
            log.info("DefaultMQPushConsumer shutdown: group={}", consumerGroup);
        }
    }

    /**
     * 订阅 Topic（仅支持 tag 为空 = 全订阅）。
     */
    public void subscribe(String topic, MessageListener listener) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is null or empty");
        }
        SubscriptionData data = new SubscriptionData(topic, "");
        subscriptionTable.put(topic, data);
        // 用户多次调用 subscribe 时, 最后一个 listener 覆盖之前的
        this.listener = listener;
    }

    /**
     * 单轮 pull 循环: 拉每个订阅 topic 的所有 queue, 把消息回调给 listener。
     */
    private void doPull() {
        try {
            for (Map.Entry<String, SubscriptionData> entry : subscriptionTable.entrySet()) {
                String topic = entry.getKey();
                TopicRouteData routeData = mqClientInstance.getTopicRouteData(topic);
                if (routeData == null || routeData.getQueueDatas() == null) continue;

                // 收集所有 (broker, queueId) 对
                Set<String> seenBrokers = new HashSet<>();
                for (BrokerData bd : routeData.getBrokerDatas()) {
                    if (!seenBrokers.add(bd.getBrokerName())) continue;
                    String brokerAddr = bd.selectBrokerAddr();
                    if (brokerAddr == null) continue;
                    int qNums = findWriteQueueNums(routeData, bd.getBrokerName());
                    for (int q = 0; q < qNums; q++) {
                        MessageQueue mq = new MessageQueue(topic, bd.getBrokerName(), q);
                        long offset = offsetTable.computeIfAbsent(mq, k -> 0L);
                        try {
                            pullAndDispatch(brokerAddr, mq, offset);
                        } catch (Exception ex) {
                            log.warn("pull mq={} failed: {}", mq, ex.getMessage());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("doPull loop failed", e);
        }
    }

    private int findWriteQueueNums(TopicRouteData routeData, String brokerName) {
        for (com.zifang.z.mq.common.QueueData qd : routeData.getQueueDatas()) {
            if (brokerName.equals(qd.getBrokerName())) {
                return qd.getWriteQueueNums();
            }
        }
        return 1;
    }

    private void pullAndDispatch(String brokerAddr, MessageQueue mq, long offset) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        request.addExtField("topic", mq.getTopic());
        request.addExtField("queueId", String.valueOf(mq.getQueueId()));
        request.addExtField("offset", String.valueOf(offset));
        request.addExtField("maxNum", String.valueOf(pullBatchSize));

        RemotingCommand response;
        try {
            response = mqClientInstance.invokeSync(brokerAddr, request, 3000);
        } catch (RemotingTimeoutException | RemotingSendRequestException ex) {
            log.warn("pull invoke failed: {}", ex.getMessage());
            return;
        }
        if (response == null || response.getCode() != RemotingSysResponseCode.SUCCESS) {
            return;
        }
        PullResultPayload payload = JsonCodec.decode(response.getBody(), PullResultPayload.class);
        if (payload == null || payload.getMessages() == null || payload.getMessages().isEmpty()) {
            return;
        }
        // 回调
        MessageQueueContext ctx = new MessageQueueContext(mq);
        if (listener instanceof MessageListener.Concurrently) {
            ConsumeConcurrentlyStatus status = ((MessageListener.Concurrently) listener).consumeMessage(
                    payload.getMessages().toArray(new MessageExt[0]), ctx);
            if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                offsetTable.put(mq, payload.getNextOffset());
            }
        } else if (listener instanceof MessageListener.Orderly) {
            ConsumeOrderlyStatus status = ((MessageListener.Orderly) listener).consumeMessage(
                    payload.getMessages().toArray(new MessageExt[0]), ctx);
            if (status == ConsumeOrderlyStatus.SUCCESS) {
                offsetTable.put(mq, payload.getNextOffset());
            }
        }
    }

    public long getOffset(MessageQueue mq) {
        Long v = offsetTable.get(mq);
        return v == null ? 0L : v;
    }

    private MQClientInstance getOrCreateInstance() throws InterruptedException {
        if (mqClientInstance != null && mqClientInstance.getRemotingClient() != null) {
            return mqClientInstance;
        }
        String key = "PUSH@" + consumerGroup;
        MQClientInstance inst = instanceTable.get(key);
        if (inst == null) {
            inst = new MQClientInstance(clientId, namesrvAddr, nettyClientConfig);
            inst.start();
            instanceTable.put(key, inst);
        }
        return inst;
    }

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

    public long getPullIntervalMillis() {
        return pullIntervalMillis;
    }

    public void setPullIntervalMillis(long pullIntervalMillis) {
        this.pullIntervalMillis = pullIntervalMillis;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    /**
     * 订阅信息（占位 — 当前实现只关心 topic/tag）.
     */
    public static class SubscriptionData implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private final String subExpression;

        public SubscriptionData(String topic, String subExpression) {
            this.topic = topic;
            this.subExpression = subExpression;
        }

        public String getTopic() {
            return topic;
        }

        public String getSubExpression() {
            return subExpression;
        }
    }

    static {
        // 占位：保证 import 使用
        Collections.emptyList();
    }
}