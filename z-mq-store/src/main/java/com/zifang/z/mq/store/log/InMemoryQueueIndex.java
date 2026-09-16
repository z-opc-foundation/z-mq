package com.zifang.z.mq.store.log;

import com.zifang.z.mq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内 Queue 索引 — 给 MVP CommitLog 提供轻量级的 (topic, queueId) -> 消息列表 索引。
 * <p>
 * 真实的 RocketMQ 用 ConsumeQueue + IndexFile 双层索引, 在 CommitLog 写入时通过 ReputMessageService
 * 异步构建。这里直接同步建索引, 用 ConcurrentHashMap + AtomicLong 保证并发安全。
 * <p>
 * 内存占用: 每条消息约 200B (元信息), 100万条消息约 200MB。可配置上限。
 *
 * <p><b>线程安全:</b> 所有公开方法均线程安全, 内部状态用 ConcurrentHashMap + AtomicLong。
 */
public class InMemoryQueueIndex {

    /** 单个 (topic, queueId) 索引条目, 持有按 offset 顺序排列的消息列表与下一个 offset 计数器。 */
    public static final class QueueEntry {
        private final String topic;
        private final int queueId;
        private final ConcurrentMap<Long, MessageExt> byOffset = new ConcurrentHashMap<>();
        private final AtomicLong nextOffset = new AtomicLong(0L);

        QueueEntry(String topic, int queueId) {
            this.topic = topic;
            this.queueId = queueId;
        }

        public String getTopic() { return topic; }
        public int getQueueId() { return queueId; }
        public long getNextOffset() { return nextOffset.get(); }
        public int getSize() { return byOffset.size(); }
    }

    private final ConcurrentMap<String, QueueEntry> entries = new ConcurrentHashMap<>();
    /** 每个 (topic, queueId) 最多保留多少条消息, 0 表示不限制。 */
    private final int maxPerQueue;

    public InMemoryQueueIndex() {
        this(0);
    }

    public InMemoryQueueIndex(int maxPerQueue) {
        this.maxPerQueue = maxPerQueue;
    }

    private static String key(String topic, int queueId) {
        return topic + "@" + queueId;
    }

    public QueueEntry getOrCreateEntry(String topic, int queueId) {
        return entries.computeIfAbsent(key(topic, queueId), k -> new QueueEntry(topic, queueId));
    }

    /**
     * 追加一条消息, 分配单调递增的 queueOffset, 返回分配结果。
     */
    public MessageExt append(String topic, int queueId, MessageExt msg) {
        QueueEntry entry = getOrCreateEntry(topic, queueId);
        long offset = entry.nextOffset.getAndIncrement();
        msg.setQueueOffset(offset);
        msg.setQueueId(queueId);
        entry.byOffset.put(offset, msg);
        // 简单 LRU: 超限时移除最早的条目
        if (maxPerQueue > 0 && entry.byOffset.size() > maxPerQueue) {
            entry.byOffset.entrySet().removeIf(e -> e.getKey() < entry.nextOffset.get() - maxPerQueue);
        }
        return msg;
    }

    /**
     * 查询从指定 offset 开始的最多 maxNum 条消息。
     *
     * @return 列表按 offset 升序排列; 若 offset 超出范围返回空列表
     */
    public List<MessageExt> query(String topic, int queueId, long offset, int maxNum) {
        QueueEntry entry = entries.get(key(topic, queueId));
        if (entry == null || offset >= entry.nextOffset.get()) {
            return new ArrayList<>();
        }
        List<MessageExt> result = new ArrayList<>();
        long end = Math.min(offset + maxNum, entry.nextOffset.get());
        for (long off = offset; off < end; off++) {
            MessageExt m = entry.byOffset.get(off);
            if (m != null) {
                result.add(m);
            }
        }
        return result;
    }

    /** 获取队列当前最大 offset (新消息会分配这个值)。 */
    public long getMaxOffset(String topic, int queueId) {
        QueueEntry entry = entries.get(key(topic, queueId));
        return entry == null ? 0L : entry.nextOffset.get();
    }

    /** 队列中已索引的消息数量。 */
    public int getSize(String topic, int queueId) {
        QueueEntry entry = entries.get(key(topic, queueId));
        return entry == null ? 0 : entry.byOffset.size();
    }

    /** 清空所有索引 (用于测试隔离)。 */
    public void clear() {
        entries.clear();
    }

    /** 总队列数 (topic × queueId)。 */
    public int getQueueCount() {
        return entries.size();
    }
}
