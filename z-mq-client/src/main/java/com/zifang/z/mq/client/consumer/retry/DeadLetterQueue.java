package com.zifang.z.mq.client.consumer.retry;

import com.zifang.z.mq.common.message.MessageExt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 死信队列管理器（对标 RocketMQ DeadLetterQueue）.
 * <p>
 * 当消息消费重试次数超过最大限制时，自动转入死信队列。
 * 死信队列 Topic 命名规则：{@code %DLQ%{consumerGroup}}
 * <p>
 * 消费者可以订阅死信 Topic 进行人工处理或告警。
 */
public class DeadLetterQueue {

    private static final Logger log = LogManager.getLogger(DeadLetterQueue.class);

    /** 死信队列 Topic 前缀 */
    public static final String DLQ_TOPIC_PREFIX = "%DLQ%";

    /** 默认死信消息保留时间（3天） */
    public static final long DEFAULT_RETENTION_MILLIS = 3 * 24 * 60 * 60 * 1000L;

    /** 死信队列存储 */
    private final ConcurrentLinkedQueue<DeadLetterMessage> deadLetterQueue = new ConcurrentLinkedQueue<>();

    /** 统计计数器 */
    private final AtomicLong totalDeadLetters = new AtomicLong(0);
    private final AtomicLong totalExpired = new AtomicLong(0);

    private final String consumerGroup;
    private final long retentionMillis;

    /**
     * 死信消息包装类。
     */
    public static class DeadLetterMessage {
        private final MessageExt message;
        private final int reconsumeTimes;
        private final long deadLetterTimestamp;
        private final String reason;

        public DeadLetterMessage(MessageExt message, int reconsumeTimes, String reason) {
            this.message = message;
            this.reconsumeTimes = reconsumeTimes;
            this.deadLetterTimestamp = System.currentTimeMillis();
            this.reason = reason;
        }

        public MessageExt getMessage() {
            return message;
        }

        public int getReconsumeTimes() {
            return reconsumeTimes;
        }

        public long getDeadLetterTimestamp() {
            return deadLetterTimestamp;
        }

        public String getReason() {
            return reason;
        }
    }

    public DeadLetterQueue(String consumerGroup) {
        this(consumerGroup, DEFAULT_RETENTION_MILLIS);
    }

    public DeadLetterQueue(String consumerGroup, long retentionMillis) {
        this.consumerGroup = consumerGroup;
        this.retentionMillis = retentionMillis;
    }

    /**
     * 将消息转入死信队列。
     *
     * @param message        消息
     * @param reconsumeTimes 已重试次数
     * @param reason         转入死信的原因
     */
    public void putMessage(MessageExt message, int reconsumeTimes, String reason) {
        DeadLetterMessage dlqMessage = new DeadLetterMessage(message, reconsumeTimes, reason);
        deadLetterQueue.offer(dlqMessage);
        totalDeadLetters.incrementAndGet();
        log.warn("Message moved to DLQ: topic={}, msgId={}, reconsumeTimes={}, reason={}, group={}",
                message.getTopic(), message.getMsgId(), reconsumeTimes, reason, consumerGroup);
    }

    /**
     * 从死信队列取出所有消息（用于消费者订阅处理）。
     *
     * @return 死信消息列表
     */
    public List<DeadLetterMessage> pollAll() {
        List<DeadLetterMessage> messages = new ArrayList<>();
        DeadLetterMessage msg;
        while ((msg = deadLetterQueue.poll()) != null) {
            // 检查是否过期
            if (System.currentTimeMillis() - msg.getDeadLetterTimestamp() <= retentionMillis) {
                messages.add(msg);
            } else {
                totalExpired.incrementAndGet();
            }
        }
        return messages;
    }

    /**
     * 获取死信队列大小。
     */
    public int size() {
        return deadLetterQueue.size();
    }

    /**
     * 获取死信队列 Topic 名称。
     */
    public String getDlqTopic() {
        return DLQ_TOPIC_PREFIX + consumerGroup;
    }

    /**
     * 获取总死信消息数。
     */
    public long getTotalDeadLetters() {
        return totalDeadLetters.get();
    }

    /**
     * 获取过期清理的消息数。
     */
    public long getTotalExpired() {
        return totalExpired.get();
    }

    /**
     * 清空死信队列。
     */
    public void clear() {
        deadLetterQueue.clear();
        log.info("DLQ cleared: group={}", consumerGroup);
    }

    @Override
    public String toString() {
        return "DeadLetterQueue{group='" + consumerGroup + "', size=" + deadLetterQueue.size()
                + ", totalDeadLetters=" + totalDeadLetters.get()
                + ", totalExpired=" + totalExpired.get() + "}";
    }
}
