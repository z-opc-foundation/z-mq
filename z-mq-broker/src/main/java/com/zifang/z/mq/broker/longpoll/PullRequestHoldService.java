package com.zifang.z.mq.broker.longpoll;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker 端"挂起 Pull 请求"服务（对标 RocketMQ PullRequestHoldService）.
 * <p>
 * 工作原理:
 * <ol>
 *   <li>Consumer 发 Pull 请求时, 若该 (topic, queueId) 在 offset 处暂无新消息, 则挂起请求</li>
 *   <li>Broker 收到新消息后, notify message arrive → 唤醒所有等待该 (topic, queueId) 的请求</li>
 *   <li>默认挂起 15s 后, 即使无新消息也强制返回 (避免长期挂死)</li>
 *   <li>Consumer 拿到响应后再发起新一轮 Pull</li>
 * </ol>
 *
 * <p>核心收益:
 * <ul>
 *   <li>Consumer 侧零轮询 (节省 CPU + 网络)</li>
 *   <li>消息到达毫秒级响应 (vs 短轮询 1s+ 延迟)</li>
 *   <li>Broker 侧背压保护 — 挂起数量可监控, 超限则拒绝新挂起</li>
 * </ul>
 *
 * <p><b>线程安全:</b> 所有公开方法线程安全, 内部用 ConcurrentMap + AQS-based 同步器.
 *
 * @see <a href="https://github.com/apache/rocketmq/blob/develop/broker/src/main/java/org/apache/rocketmq/broker/processor/PullRequestHoldService.java">RocketMQ PullRequestHoldService</a>
 */
public class PullRequestHoldService {

    private static final Logger log = LogManager.getLogger(PullRequestHoldService.class);

    /** 默认挂起时间 (RocketMQ 也是 15s, 即 brokerConfig.longPollingTimeout). */
    public static final long DEFAULT_HOLD_TIMEOUT_MS = 15_000L;

    /** 挂起请求数量上限 (避免 OOM). */
    public static final int DEFAULT_MAX_HOLD_COUNT = 10_000;

    /** Hold 任务扫描周期 (RocketMQ 5s, 这里 1s 提高响应). */
    private static final long SCAN_INTERVAL_MS = 1_000L;

    private final long holdTimeoutMillis;
    private final int maxHoldCount;

    /** key = topic + "@" + queueId -> 该队列上挂起的请求列表. */
    private final ConcurrentMap<String, ConcurrentMap<SuspendedPull, Boolean>> holdTable = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public PullRequestHoldService() {
        this(DEFAULT_HOLD_TIMEOUT_MS, DEFAULT_MAX_HOLD_COUNT);
    }

    public PullRequestHoldService(long holdTimeoutMillis, int maxHoldCount) {
        this.holdTimeoutMillis = holdTimeoutMillis;
        this.maxHoldCount = maxHoldCount;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "PullRequestHoldService");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * 启动 hold 服务 (后台超时扫描线程).
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            scheduler.scheduleAtFixedRate(this::scanTimeoutHolds, SCAN_INTERVAL_MS, SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS);
            log.info("PullRequestHoldService started, holdTimeout={}ms, maxHold={}", holdTimeoutMillis, maxHoldCount);
        }
    }

    /**
     * 关闭 hold 服务, 唤醒所有挂起请求 (让 Consumer 自己处理空结果).
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            scheduler.shutdownNow();
            // 唤醒所有挂起的请求
            for (ConcurrentMap<SuspendedPull, Boolean> queue : holdTable.values()) {
                for (SuspendedPull req : queue.keySet()) {
                    req.wakeupByTimeout();
                }
                queue.clear();
            }
            log.info("PullRequestHoldService shutdown, totalHoldKeys={}", holdTable.size());
        }
    }

    /**
     * 挂起一个 Pull 请求直到: (a) 该 (topic, queueId) 有新消息到达, (b) 超时, 或 (c) 被取消.
     * <p>
     * 注意: 调用者必须先检查该 (topic, queueId) 在当前 offset 下确实没有新消息, 否则直接返回 null 不挂起.
     *
     * @param topic    Topic 名称
     * @param queueId  队列 ID
     * @param offset   Consumer 期望拉取的 offset
     * @return SuspendedPull 句柄 (可用于 await), null=挂起失败 (超过 maxHoldCount 上限)
     */
    public SuspendedPull suspendPull(String topic, int queueId, long offset) {
        if (totalHoldCount() >= maxHoldCount) {
            log.warn("suspendPull rejected: holdTable full ({} >= {})", totalHoldCount(), maxHoldCount);
            return null;
        }
        SuspendedPull req = new SuspendedPull(topic, queueId, offset, holdTimeoutMillis);
        String key = key(topic, queueId);
        ConcurrentMap<SuspendedPull, Boolean> queue = holdTable.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        queue.put(req, Boolean.TRUE);
        return req;
    }

    /**
     * 新消息到达通知 — 唤醒该 (topic, queueId) 上所有挂起的请求.
     *
     * @param topic   Topic 名称
     * @param queueId 队列 ID
     */
    public void notifyMessageArrived(String topic, int queueId) {
        String key = key(topic, queueId);
        ConcurrentMap<SuspendedPull, Boolean> queue = holdTable.remove(key);
        if (queue != null) {
            int woken = 0;
            for (SuspendedPull req : queue.keySet()) {
                if (req.wakeupByMessage()) {
                    woken++;
                }
            }
            if (woken > 0) {
                log.debug("notifyMessageArrived: topic={} queueId={} woken={}", topic, queueId, woken);
            }
        }
    }

    /**
     * 新消息到达通知 (指定 queueId 列表版本) — 一次唤醒多个 queue 上的挂起请求.
     */
    public void notifyMessageArrived(String topic, java.util.Collection<Integer> queueIds) {
        if (queueIds == null) return;
        for (Integer qid : queueIds) {
            notifyMessageArrived(topic, qid);
        }
    }

    /**
     * 主动取消挂起 (Consumer 主动断开连接时调用).
     */
    public void cancelSuspend(String topic, int queueId) {
        String key = key(topic, queueId);
        ConcurrentMap<SuspendedPull, Boolean> queue = holdTable.get(key);
        if (queue != null) {
            queue.clear();
        }
    }

    /**
     * 当前挂起请求总数 (跨所有 topic+queueId).
     */
    public int totalHoldCount() {
        int total = 0;
        for (ConcurrentMap<SuspendedPull, Boolean> q : holdTable.values()) {
            total += q.size();
        }
        return total;
    }

    /**
     * 当前挂起的 (topic, queueId) 数量.
     */
    public int queueKeyCount() {
        return holdTable.size();
    }

    /**
     * 测试/监控用: 是否存在某 (topic, queueId) 上的挂起请求.
     */
    public boolean hasSuspended(String topic, int queueId) {
        ConcurrentMap<SuspendedPull, Boolean> queue = holdTable.get(key(topic, queueId));
        return queue != null && !queue.isEmpty();
    }

    private void scanTimeoutHolds() {
        long now = System.currentTimeMillis();
        try {
            Iterator<Map.Entry<String, ConcurrentMap<SuspendedPull, Boolean>>> it = holdTable.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, ConcurrentMap<SuspendedPull, Boolean>> entry = it.next();
                ConcurrentMap<SuspendedPull, Boolean> queue = entry.getValue();
                for (SuspendedPull req : queue.keySet()) {
                    if (req.isTimedOut(now)) {
                        if (req.wakeupByTimeout()) {
                            queue.remove(req);
                        }
                    }
                }
                if (queue.isEmpty()) {
                    it.remove();
                }
            }
        } catch (Exception e) {
            log.warn("scanTimeoutHolds error", e);
        }
    }

    private static String key(String topic, int queueId) {
        return topic + "@" + queueId;
    }

    /**
     * 单个挂起的 Pull 请求, 用 AQS 实现线程间唤醒机制.
     */
    public static final class SuspendedPull {

        private final String topic;
        private final int queueId;
        private final long offset;
        private final long createTimeMillis;
        private final long timeoutMillis;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicBoolean released = new java.util.concurrent.atomic.AtomicBoolean(false);
        private final java.util.concurrent.atomic.AtomicBoolean messageWakeup = new java.util.concurrent.atomic.AtomicBoolean(false);

        public SuspendedPull(String topic, int queueId, long offset, long timeoutMillis) {
            this.topic = topic;
            this.queueId = queueId;
            this.offset = offset;
            this.createTimeMillis = System.currentTimeMillis();
            this.timeoutMillis = timeoutMillis;
        }

        public String getTopic() { return topic; }
        public int getQueueId() { return queueId; }
        public long getOffset() { return offset; }
        public long getCreateTimeMillis() { return createTimeMillis; }

        public boolean isTimedOut(long now) {
            return now - createTimeMillis >= timeoutMillis;
        }

        /**
         * 阻塞等待唤醒 (新消息到达 / 超时 / 取消).
         *
         * @return true=被新消息唤醒; false=超时或被中断
         */
        public boolean awaitWakeup() throws InterruptedException {
            latch.await();
            return messageWakeup.get();
        }

        /**
         * 新消息到达时唤醒.
         */
        public boolean wakeupByMessage() {
            if (released.get()) {
                return false;
            }
            messageWakeup.set(true);
            latch.countDown();
            return released.compareAndSet(false, true);
        }

        /**
         * 超时唤醒 (返回 false 让 Consumer 走空结果路径).
         */
        public boolean wakeupByTimeout() {
            if (released.get()) {
                return false;
            }
            messageWakeup.set(false);
            latch.countDown();
            return released.compareAndSet(false, true);
        }
    }
}