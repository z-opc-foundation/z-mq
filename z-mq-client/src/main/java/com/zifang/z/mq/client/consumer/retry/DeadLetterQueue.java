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

    /** 进程内缓存最多留多少条：真相在死信 Topic 里，这里只兜"发不出去"的那部分 */
    public static final int MAX_CACHED_DEAD_LETTERS = 1024;

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
        private boolean published;

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

        /** 这一条是否已经真发到死信 Topic 上（false = 只活在进程内这一份缓存里）. */
        public boolean isPublished() {
            return published;
        }

        public void setPublished(boolean published) {
            this.published = published;
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
     * 死信的发布通路：把这条消息真发到 {@link #getDlqTopic()} 那个 Topic 上。
     * <p>
     * 只有进程内那份队列的消息是掉电就没的；订阅"死信 Topic 做人工处理或告警"这句话
     * 要靠这条通路把消息送进存储才成立。
     */
    public interface DlqPublisher {
        /**
         * @param message        要转入死信的消息
         * @param reconsumeTimes 已经重投过的次数
         * @param dlqTopic       目标死信 Topic（由 {@link #getDlqTopic()} 算出）
         * @param reason         转入死信的原因
         * @return 是否已经被 broker 收下
         */
        boolean publish(MessageExt message, int reconsumeTimes, String dlqTopic, String reason);
    }

    /** 死信发布通路；没设置时只记进程内那一份 */
    private volatile DlqPublisher publisher;

    /**
     * 将消息转入死信队列。
     * <p>
     * 先往死信 Topic 发（那条才是掉电不丢的真相），再把这一条记进进程内队列当缓存与统计；
     * 发不出去时进程内这一份仍然留着，至少不丢读数。
     *
     * @param message        消息
     * @param reconsumeTimes 已重试次数
     * @param reason         转入死信的原因
     * @return 这条死信是否真的被交到了死信 Topic 上；false 表示只有进程内这一份缓存。
     *         调用方要靠这个读数决定"原来那条还要不要再兜着"。
     */
    public boolean putMessage(MessageExt message, int reconsumeTimes, String reason) {
        String dlqTopic = getDlqTopic();
        DlqPublisher current = this.publisher;
        boolean published = false;
        if (current != null) {
            try {
                published = current.publish(message, reconsumeTimes, dlqTopic, reason);
            } catch (Exception e) {
                log.warn("Dead letter publish failed: group={} topic={} err={}",
                        consumerGroup, dlqTopic, e.getMessage());
            }
        }
        DeadLetterMessage dlqMessage = new DeadLetterMessage(message, reconsumeTimes, reason);
        dlqMessage.setPublished(published);
        totalDeadLetters.incrementAndGet();
        if (!published) {
            // 发不出去的那些才需要进程内这一份兜着；发出去的真相已经在死信 Topic 里，
            // 再留一份就成了"看起来有两份真相"的第二持有者。
            deadLetterQueue.offer(dlqMessage);
            while (deadLetterQueue.size() > MAX_CACHED_DEAD_LETTERS && deadLetterQueue.poll() != null) {
                // 只裁缓存，计数器不动（统计口径是"进过死信多少条"，不是"缓存里还剩多少条"）
                totalExpired.incrementAndGet();
            }
        }
        log.warn("Message moved to DLQ: topic={}, msgId={}, reconsumeTimes={}, reason={}, group={}"
                        + " dlqTopic={} published={}",
                message.getTopic(), message.getMsgId(), reconsumeTimes, reason, consumerGroup,
                dlqTopic, Boolean.valueOf(published));
        return published;
    }

    /**
     * 设置死信发布通路（把死信真发到 {@link #getDlqTopic()}）.
     */
    public void setPublisher(DlqPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * 本消费组的死信 Topic 名（命名规则见类注释）。
     */
    public String getDlqTopic() {
        return DLQ_TOPIC_PREFIX + consumerGroup;
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
