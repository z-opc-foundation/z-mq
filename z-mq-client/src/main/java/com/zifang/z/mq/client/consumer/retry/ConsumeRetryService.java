package com.zifang.z.mq.client.consumer.retry;

import com.zifang.z.mq.common.message.MessageExt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 消费重试服务（对标 RocketMQ ConsumeRetryService）.
 * <p>
 * 管理消费失败消息的重试逻辑：
 * <ul>
 *   <li>消费失败时将消息加入重试队列</li>
 *   <li>根据重试策略（阶梯间隔/固定间隔）定时重新投递</li>
 *   <li>超过最大重试次数后转入死信队列</li>
 * </ul>
 * <p>
 * 重试间隔与 RocketMQ 保持一致：
 * 无序消息阶梯间隔：10s → 30s → 1min → 2min → ... → 2h
 * 顺序消息固定间隔：可配置（默认 30s）
 */
public class ConsumeRetryService {

    private static final Logger log = LogManager.getLogger(ConsumeRetryService.class);

    /** 默认最大重试次数 */
    public static final int DEFAULT_MAX_RECONSUME_TIMES = 16;

    /** 默认顺序消息固定重试间隔（30秒） */
    public static final long DEFAULT_ORDERLY_RETRY_INTERVAL_MILLIS = 30_000L;

    /** 重试消息属性键：重试次数 */
    public static final String PROPERTY_RECONSUME_TIMES = "RECONSUME_TIMES";

    /** 重试消息属性键：原始 Topic */
    public static final String PROPERTY_ORIGINAL_TOPIC = "RETRY_TOPIC";

    private final String consumerGroup;
    private final int maxReconsumeTimes;
    private final RetryPolicy retryPolicy;
    private final long fixedRetryIntervalMillis;
    private final DeadLetterQueue deadLetterQueue;

    /** 重试消息存储：topic -> (msgId -> 待重试消息) */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, RetryMessage>> retryMessageStore = new ConcurrentHashMap<>();

    /** 重试调度器 */
    private ScheduledExecutorService retryExecutor;

    /** 重试回调接口 */
    private RetryCallback retryCallback;

    /**
     * 重试消息包装类。
     */
    static class RetryMessage {
        final MessageExt message;
        final int reconsumeTimes;
        final long nextRetryTimestamp;
        final long originalBornTimestamp;

        RetryMessage(MessageExt message, int reconsumeTimes, long nextRetryTimestamp) {
            this.message = message;
            this.reconsumeTimes = reconsumeTimes;
            this.nextRetryTimestamp = nextRetryTimestamp;
            this.originalBornTimestamp = message.getBornTimestamp();
        }

        boolean isReady() {
            return System.currentTimeMillis() >= nextRetryTimestamp;
        }
    }

    /**
     * 重试回调接口。
     */
    public interface RetryCallback {
        /**
         * 重新投递消息进行消费。
         *
         * @param message 待重试的消息
         * @return 消费是否成功
         */
        boolean retryConsume(MessageExt message);
    }

    public ConsumeRetryService(String consumerGroup) {
        this(consumerGroup, DEFAULT_MAX_RECONSUME_TIMES, RetryPolicy.STEPPED, 0);
    }

    public ConsumeRetryService(String consumerGroup, int maxReconsumeTimes) {
        this(consumerGroup, maxReconsumeTimes, RetryPolicy.STEPPED, 0);
    }

    public ConsumeRetryService(String consumerGroup, int maxReconsumeTimes, RetryPolicy retryPolicy,
                               long fixedRetryIntervalMillis) {
        this.consumerGroup = consumerGroup;
        this.maxReconsumeTimes = maxReconsumeTimes;
        this.retryPolicy = retryPolicy;
        this.fixedRetryIntervalMillis = fixedRetryIntervalMillis;
        this.deadLetterQueue = new DeadLetterQueue(consumerGroup);
    }

    /**
     * 启动重试服务。
     */
    public void start() {
        this.retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ConsumeRetryThread_" + consumerGroup);
            t.setDaemon(true);
            return t;
        });
        // 每秒检查一次重试队列
        this.retryExecutor.scheduleWithFixedDelay(this::processRetryQueue, 1, 1, TimeUnit.SECONDS);
        log.info("ConsumeRetryService started: group={}, maxRetries={}, policy={}",
                consumerGroup, maxReconsumeTimes, retryPolicy);
    }

    /**
     * 停止重试服务。
     */
    public void shutdown() {
        if (retryExecutor != null) {
            retryExecutor.shutdownNow();
        }
        log.info("ConsumeRetryService shutdown: group={}", consumerGroup);
    }

    /**
     * 设置重试回调。
     */
    public void setRetryCallback(RetryCallback retryCallback) {
        this.retryCallback = retryCallback;
    }

    /**
     * 消费失败时调用此方法，将消息加入重试队列。
     *
     * @param message        消费失败的消息
     * @param reconsumeTimes 当前已重试次数
     * @return true 表示已加入重试队列，false 表示已超过最大重试次数（转入死信队列）
     */
    public boolean addRetryMessage(MessageExt message, int reconsumeTimes) {
        if (reconsumeTimes >= maxReconsumeTimes) {
            // 超过最大重试次数，转入死信队列
            deadLetterQueue.putMessage(message, reconsumeTimes,
                    "Exceeded max reconsume times: " + maxReconsumeTimes);
            return false;
        }

        // 计算下次重试时间
        long nextRetryDelay;
        if (retryPolicy == RetryPolicy.FIXED) {
            nextRetryDelay = RetryPolicy.getFixedInterval(fixedRetryIntervalMillis);
        } else {
            nextRetryDelay = RetryPolicy.getSteppedInterval(reconsumeTimes + 1);
        }
        long nextRetryTimestamp = System.currentTimeMillis() + nextRetryDelay;

        // 设置重试次数属性
        message.putProperty(PROPERTY_RECONSUME_TIMES, String.valueOf(reconsumeTimes + 1));
        message.putProperty(PROPERTY_ORIGINAL_TOPIC, message.getTopic());

        RetryMessage retryMsg = new RetryMessage(message, reconsumeTimes + 1, nextRetryTimestamp);

        // 按 Topic 分组存储
        retryMessageStore
                .computeIfAbsent(message.getTopic(), k -> new ConcurrentHashMap<>())
                .put(message.getMsgId(), retryMsg);

        log.debug("Message added to retry queue: topic={}, msgId={}, retryCount={}, nextRetryIn={}ms",
                message.getTopic(), message.getMsgId(), retryMsg.reconsumeTimes, nextRetryDelay);
        return true;
    }

    /**
     * 处理重试队列：检查到期消息并重新投递。
     */
    private void processRetryQueue() {
        if (retryCallback == null) {
            return;
        }

        long now = System.currentTimeMillis();
        for (Map.Entry<String, ConcurrentHashMap<String, RetryMessage>> topicEntry : retryMessageStore.entrySet()) {
            ConcurrentHashMap<String, RetryMessage> msgMap = topicEntry.getValue();
            Iterator<Map.Entry<String, RetryMessage>> iterator = msgMap.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, RetryMessage> entry = iterator.next();
                RetryMessage retryMsg = entry.getValue();

                if (retryMsg.isReady()) {
                    iterator.remove(); // 先移除，避免重复重试
                    try {
                        boolean success = retryCallback.retryConsume(retryMsg.message);
                        if (!success) {
                            // 重试消费失败，再次加入重试队列
                            addRetryMessage(retryMsg.message, retryMsg.reconsumeTimes);
                        }
                    } catch (Exception e) {
                        log.error("Retry consume failed: topic={}, msgId={}",
                                retryMsg.message.getTopic(), retryMsg.message.getMsgId(), e);
                        // 异常也加入重试队列
                        addRetryMessage(retryMsg.message, retryMsg.reconsumeTimes);
                    }
                }
            }
        }
    }

    /**
     * 获取死信队列。
     */
    public DeadLetterQueue getDeadLetterQueue() {
        return deadLetterQueue;
    }

    /**
     * 获取指定 Topic 的待重试消息数量。
     */
    public int getRetryMessageCount(String topic) {
        ConcurrentHashMap<String, RetryMessage> msgMap = retryMessageStore.get(topic);
        return msgMap != null ? msgMap.size() : 0;
    }

    /**
     * 获取所有待重试消息总数。
     */
    public int getTotalRetryMessageCount() {
        int total = 0;
        for (ConcurrentHashMap<String, RetryMessage> msgMap : retryMessageStore.values()) {
            total += msgMap.size();
        }
        return total;
    }

    /**
     * 清空指定 Topic 的重试消息。
     */
    public void clearRetryMessages(String topic) {
        retryMessageStore.remove(topic);
        log.info("Retry messages cleared: group={}, topic={}", consumerGroup, topic);
    }

    @Override
    public String toString() {
        return "ConsumeRetryService{group='" + consumerGroup + "', maxRetries=" + maxReconsumeTimes
                + ", policy=" + retryPolicy + ", totalRetrying=" + getTotalRetryMessageCount()
                + ", dlq=" + deadLetterQueue + "}";
    }
}
