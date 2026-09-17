package com.zifang.z.mq.client.consumer;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.filter.FilterType;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.message.MessageModel;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.client.consumer.retry.ConsumeRetryService;
import com.zifang.z.mq.client.consumer.retry.RetryPolicy;
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
 * 支持 Broker 端消息过滤（Tag 标签过滤 / SQL92 属性过滤）。
 * 支持消费重试和死信队列。
 */
public class DefaultMQPushConsumer {

    private static final Logger log = LogManager.getLogger(DefaultMQPushConsumer.class);

    private String consumerGroup;
    private String namesrvAddr;
    private NettyClientConfig nettyClientConfig;
    private String clientId;
    private long pullIntervalMillis = 1000;
    private int pullBatchSize = 32;
    private int maxReconsumeTimes = ConsumeRetryService.DEFAULT_MAX_RECONSUME_TIMES;
    private RetryPolicy retryPolicy = RetryPolicy.STEPPED;
    private long fixedRetryIntervalMillis = ConsumeRetryService.DEFAULT_ORDERLY_RETRY_INTERVAL_MILLIS;
    private MessageModel messageModel = MessageModel.CLUSTERING;

    private MQClientInstance mqClientInstance;
    private final ConcurrentHashMap<String, MQClientInstance> instanceTable = new ConcurrentHashMap<>();
    /** 订阅表 (topic -> 订阅信息). */
    private final ConcurrentHashMap<String, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();
    /** 队列 offset 表. */
    private final Map<MessageQueue, Long> offsetTable = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService pullExecutor;
    private MessageListener listener;
    private ConsumeRetryService consumeRetryService;

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

        // 初始化消费重试服务
        this.consumeRetryService = new ConsumeRetryService(consumerGroup, maxReconsumeTimes,
                retryPolicy, fixedRetryIntervalMillis);
        this.consumeRetryService.setRetryCallback(this::doRetryConsume);
        this.consumeRetryService.start();

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
            if (consumeRetryService != null) {
                consumeRetryService.shutdown();
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
     * 订阅 Topic（全订阅，不过滤）。
     *
     * @param topic    目标 Topic
     * @param listener 消息监听器
     */
    public void subscribe(String topic, MessageListener listener) {
        subscribe(topic, "*", FilterType.TAG, listener);
    }

    /**
     * 订阅 Topic（带 Tag 过滤）。
     *
     * @param topic    目标 Topic
     * @param tag      Tag 过滤表达式（如 "TagA"、"TagA||TagB"、"*"）
     * @param listener 消息监听器
     */
    public void subscribe(String topic, String tag, MessageListener listener) {
        subscribe(topic, tag, FilterType.TAG, listener);
    }

    /**
     * 订阅 Topic（带过滤类型和表达式）。
     *
     * @param topic           目标 Topic
     * @param filterExpression 过滤表达式
     * @param filterType      过滤类型（TAG / SQL92）
     * @param listener        消息监听器
     */
    public void subscribe(String topic, String filterExpression, FilterType filterType, MessageListener listener) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is null or empty");
        }
        SubscriptionData data = new SubscriptionData(topic, filterExpression, filterType);
        subscriptionTable.put(topic, data);
        // 用户多次调用 subscribe 时, 最后一个 listener 覆盖之前的
        this.listener = listener;
        log.info("Subscription registered: topic={} filterType={} expression={}",
                topic, filterType, filterExpression);
    }

    /**
     * 单轮 pull 循环: 拉每个订阅 topic 的所有 queue, 把消息回调给 listener。
     * <p>
     * 广播模式下，每个实例独立拉取所有队列；集群模式下，通过 rebalance 分配队列。
     */
    private void doPull() {
        try {
            for (Map.Entry<String, SubscriptionData> entry : subscriptionTable.entrySet()) {
                String topic = entry.getKey();
                SubscriptionData subData = entry.getValue();
                TopicRouteData routeData = mqClientInstance.getTopicRouteData(topic);
                if (routeData == null || routeData.getQueueDatas() == null) {
                    continue;
                }

                // 广播模式：每个实例拉取所有队列
                // 集群模式：当前简化实现也拉取所有队列（完整实现需要 rebalance）
                Set<String> seenBrokers = new HashSet<>();
                for (BrokerData bd : routeData.getBrokerDatas()) {
                    if (!seenBrokers.add(bd.getBrokerName())) {
                        continue;
                    }
                    String brokerAddr = bd.selectBrokerAddr();
                    if (brokerAddr == null) {
                        continue;
                    }
                    int qNums = findWriteQueueNums(routeData, bd.getBrokerName());
                    for (int q = 0; q < qNums; q++) {
                        MessageQueue mq = new MessageQueue(topic, bd.getBrokerName(), q);
                        long offset = offsetTable.computeIfAbsent(mq, k -> 0L);
                        try {
                            pullAndDispatch(brokerAddr, mq, offset, subData);
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

    private void pullAndDispatch(String brokerAddr, MessageQueue mq, long offset, SubscriptionData subData) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        request.addExtField("topic", mq.getTopic());
        request.addExtField("queueId", String.valueOf(mq.getQueueId()));
        request.addExtField("offset", String.valueOf(offset));
        request.addExtField("maxNum", String.valueOf(pullBatchSize));

        // 发送过滤参数到 Broker 端
        if (subData != null && subData.getFilterExpression() != null
                && !subData.getFilterExpression().isEmpty()
                && !"*".equals(subData.getFilterExpression())) {
            request.addExtField("filterType", subData.getFilterType().name());
            request.addExtField("filterExpression", subData.getFilterExpression());
        }

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

        // 客户端二次过滤（Tag 精确匹配）
        List<MessageExt> messages = payload.getMessages();
        if (subData != null && subData.getFilterType() == FilterType.TAG
                && subData.getFilterExpression() != null
                && !"*".equals(subData.getFilterExpression())) {
            messages = clientSideTagFilter(messages, subData.getFilterExpression());
        }

        if (messages.isEmpty()) {
            return;
        }

        // 回调
        MessageQueueContext ctx = new MessageQueueContext(mq);
        if (listener instanceof MessageListener.Concurrently) {
            ConsumeConcurrentlyStatus status = ((MessageListener.Concurrently) listener).consumeMessage(
                    messages.toArray(new MessageExt[0]), ctx);
            if (status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS) {
                offsetTable.put(mq, payload.getNextOffset());
            } else if (status == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
                // 消费失败，将消息加入重试队列
                handleConsumeFailure(messages);
            }
        } else if (listener instanceof MessageListener.Orderly) {
            ConsumeOrderlyStatus status = ((MessageListener.Orderly) listener).consumeMessage(
                    messages.toArray(new MessageExt[0]), ctx);
            if (status == ConsumeOrderlyStatus.SUCCESS) {
                offsetTable.put(mq, payload.getNextOffset());
            } else if (status == ConsumeOrderlyStatus.RECONSUME_LATER
                    || status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT) {
                // 消费失败，将消息加入重试队列
                handleConsumeFailure(messages);
            }
        }
    }

    /**
     * 处理消费失败：将消息加入重试队列。
     */
    private void handleConsumeFailure(List<MessageExt> messages) {
        if (consumeRetryService == null) {
            return;
        }
        for (MessageExt msg : messages) {
            int reconsumeTimes = msg.getReconsumeTimes();
            consumeRetryService.addRetryMessage(msg, reconsumeTimes);
        }
    }

    /**
     * 客户端 Tag 二次过滤（精确匹配）。
     * <p>
     * Broker 端可能基于 hashCode 预过滤，客户端需要再精确匹配一次。
     */
    private List<MessageExt> clientSideTagFilter(List<MessageExt> messages, String tagExpression) {
        if (tagExpression == null || tagExpression.isEmpty() || "*".equals(tagExpression)) {
            return messages;
        }
        String[] tags = tagExpression.split("\\|\\|");
        Set<String> tagSet = new HashSet<>();
        for (String tag : tags) {
            String trimmed = tag.trim();
            if (!trimmed.isEmpty()) {
                tagSet.add(trimmed);
            }
        }
        if (tagSet.isEmpty()) {
            return messages;
        }

        java.util.List<MessageExt> filtered = new java.util.ArrayList<>();
        for (MessageExt msg : messages) {
            String msgTag = msg.getTags();
            if (msgTag != null && tagSet.contains(msgTag)) {
                filtered.add(msg);
            }
        }
        return filtered;
    }

    public long getOffset(MessageQueue mq) {
        Long v = offsetTable.get(mq);
        return v == null ? 0L : v;
    }

    /**
     * 重试消费回调（由 ConsumeRetryService 调用）。
     */
    private boolean doRetryConsume(MessageExt message) {
        if (listener == null) {
            return false;
        }
        try {
            if (listener instanceof MessageListener.Concurrently) {
                ConsumeConcurrentlyStatus status = ((MessageListener.Concurrently) listener).consumeMessage(
                        new MessageExt[]{message}, new MessageQueueContext(new MessageQueue(
                                message.getTopic(), "", message.getQueueId())));
                return status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else if (listener instanceof MessageListener.Orderly) {
                ConsumeOrderlyStatus status = ((MessageListener.Orderly) listener).consumeMessage(
                        new MessageExt[]{message}, new MessageQueueContext(new MessageQueue(
                                message.getTopic(), "", message.getQueueId())));
                return status == ConsumeOrderlyStatus.SUCCESS;
            }
        } catch (Exception e) {
            log.error("Retry consume failed: topic={}, msgId={}", message.getTopic(), message.getMsgId(), e);
        }
        return false;
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

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public long getFixedRetryIntervalMillis() {
        return fixedRetryIntervalMillis;
    }

    public void setFixedRetryIntervalMillis(long fixedRetryIntervalMillis) {
        this.fixedRetryIntervalMillis = fixedRetryIntervalMillis;
    }

    public ConsumeRetryService getConsumeRetryService() {
        return consumeRetryService;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    /**
     * 订阅信息.
     * <p>
     * 包含 Topic、过滤类型和过滤表达式。
     */
    public static class SubscriptionData implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private final String subExpression;
        private final FilterType filterType;

        public SubscriptionData(String topic, String subExpression) {
            this(topic, subExpression, FilterType.TAG);
        }

        public SubscriptionData(String topic, String subExpression, FilterType filterType) {
            this.topic = topic;
            this.subExpression = subExpression;
            this.filterType = filterType != null ? filterType : FilterType.TAG;
        }

        public String getTopic() {
            return topic;
        }

        public String getSubExpression() {
            return subExpression;
        }

        public FilterType getFilterType() {
            return filterType;
        }

        public String getFilterExpression() {
            return subExpression;
        }
    }

    static {
        // 占位：保证 import 使用
        Collections.emptyList();
    }
}