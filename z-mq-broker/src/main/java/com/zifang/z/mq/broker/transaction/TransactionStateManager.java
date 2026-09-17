package com.zifang.z.mq.broker.transaction;

import com.zifang.z.mq.common.message.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务状态管理器（对标 RocketMQ TransactionStateManager）.
 * <p>
 * 管理 Half 消息的事务状态：
 * <ul>
 *   <li>存储 Half 消息和事务状态</li>
 *   <li>支持 COMMIT（提交）和 ROLLBACK（回滚）操作</li>
 *   <li>提供待回查消息查询能力</li>
 * </ul>
 * <p>
 * 事务消息流程：
 * <ol>
 *   <li>Producer 发送 Half 消息 → Broker 存储到此管理器</li>
 *   <li>Producer 发送 COMMIT/ROLLBACK → Broker 更新状态</li>
 *   <li>超时未确认 → Broker 发起回查</li>
 * </ol>
 */
public class TransactionStateManager {

    private static final Logger log = LogManager.getLogger(TransactionStateManager.class);

    /** 事务状态枚举 */
    public enum TxState {
        /** Half 消息已存储，等待二次确认 */
        HALF,
        /** 已提交，消息对消费者可见 */
        COMMITTED,
        /** 已回滚，消息丢弃 */
        ROLLBACKED,
        /** 等待回查 */
        CHECKING
    }

    /**
     * 事务消息记录。
     */
    public static class TransactionRecord {
        private final String transactionId;
        private final String originalTopic;
        private final Message halfMessage;
        private TxState state;
        private int checkTimes;
        private long lastCheckTimestamp;
        private long createTimestamp;

        public TransactionRecord(String transactionId, String originalTopic, Message halfMessage) {
            this.transactionId = transactionId;
            this.originalTopic = originalTopic;
            this.halfMessage = halfMessage;
            this.state = TxState.HALF;
            this.checkTimes = 0;
            this.lastCheckTimestamp = 0;
            this.createTimestamp = System.currentTimeMillis();
        }

        // Getters and Setters
        public String getTransactionId() { return transactionId; }
        public String getOriginalTopic() { return originalTopic; }
        public Message getHalfMessage() { return halfMessage; }
        public TxState getState() { return state; }
        public void setState(TxState state) { this.state = state; }
        public int getCheckTimes() { return checkTimes; }
        public void setCheckTimes(int checkTimes) { this.checkTimes = checkTimes; }
        public long getLastCheckTimestamp() { return lastCheckTimestamp; }
        public void setLastCheckTimestamp(long lastCheckTimestamp) { this.lastCheckTimestamp = lastCheckTimestamp; }
        public long getCreateTimestamp() { return createTimestamp; }

        public boolean isPending() {
            return state == TxState.HALF || state == TxState.CHECKING;
        }

        public boolean isExpired(long maxAgeMillis) {
            return System.currentTimeMillis() - createTimestamp > maxAgeMillis;
        }
    }

    /** 事务记录存储: transactionId -> TransactionRecord */
    private final ConcurrentHashMap<String, TransactionRecord> transactionStore = new ConcurrentHashMap<>();

    /** 默认事务超时时间（15分钟） */
    private static final long DEFAULT_TX_TIMEOUT_MILLIS = 15 * 60 * 1000L;

    /** 默认最大回查次数 */
    private static final int DEFAULT_MAX_CHECK_TIMES = 15;

    /** 默认回查间隔（60秒） */
    private static final long DEFAULT_CHECK_INTERVAL_MILLIS = 60_000L;

    private long txTimeoutMillis = DEFAULT_TX_TIMEOUT_MILLIS;
    private int maxCheckTimes = DEFAULT_MAX_CHECK_TIMES;
    private long checkIntervalMillis = DEFAULT_CHECK_INTERVAL_MILLIS;

    /**
     * 存储 Half 消息。
     *
     * @param transactionId 事务 ID
     * @param originalTopic 原始 Topic
     * @param halfMessage   Half 消息
     */
    public void putHalfMessage(String transactionId, String originalTopic, Message halfMessage) {
        TransactionRecord record = new TransactionRecord(transactionId, originalTopic, halfMessage);
        transactionStore.put(transactionId, record);
        log.info("Half message stored: transactionId={} topic={}", transactionId, originalTopic);
    }

    /**
     * 提交事务（COMMIT）。
     *
     * @param transactionId 事务 ID
     * @return 原始 Topic，用于将消息投递到原始 Topic；null 表示未找到
     */
    public String commitTransaction(String transactionId) {
        TransactionRecord record = transactionStore.get(transactionId);
        if (record == null) {
            log.warn("Transaction not found for commit: transactionId={}", transactionId);
            return null;
        }
        record.setState(TxState.COMMITTED);
        log.info("Transaction committed: transactionId={} topic={}", transactionId, record.getOriginalTopic());
        return record.getOriginalTopic();
    }

    /**
     * 回滚事务（ROLLBACK）。
     *
     * @param transactionId 事务 ID
     * @return true 表示回滚成功
     */
    public boolean rollbackTransaction(String transactionId) {
        TransactionRecord record = transactionStore.get(transactionId);
        if (record == null) {
            log.warn("Transaction not found for rollback: transactionId={}", transactionId);
            return false;
        }
        record.setState(TxState.ROLLBACKED);
        log.info("Transaction rolled back: transactionId={}", transactionId);
        return true;
    }

    /**
     * 获取所有待回查的事务记录。
     *
     * @return 待回查的事务记录列表
     */
    public List<TransactionRecord> getPendingTransactions() {
        List<TransactionRecord> pending = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (TransactionRecord record : transactionStore.values()) {
            if (record.isPending() && !record.isExpired(txTimeoutMillis)) {
                // 检查是否到了回查时间
                if (now - record.getLastCheckTimestamp() >= checkIntervalMillis) {
                    pending.add(record);
                }
            }
        }
        return pending;
    }

    /**
     * 更新回查次数和时间戳。
     *
     * @param transactionId 事务 ID
     */
    public void markChecked(String transactionId) {
        TransactionRecord record = transactionStore.get(transactionId);
        if (record != null) {
            record.setCheckTimes(record.getCheckTimes() + 1);
            record.setLastCheckTimestamp(System.currentTimeMillis());
            record.setState(TxState.CHECKING);
        }
    }

    /**
     * 获取事务记录。
     */
    public TransactionRecord getTransaction(String transactionId) {
        return transactionStore.get(transactionId);
    }

    /**
     * 清理已过期的事务记录。
     */
    public int cleanExpiredTransactions() {
        int cleaned = 0;
        for (Map.Entry<String, TransactionRecord> entry : transactionStore.entrySet()) {
            TransactionRecord record = entry.getValue();
            if (record.isExpired(txTimeoutMillis) && record.isPending()) {
                // 超时未确认，标记为回滚
                record.setState(TxState.ROLLBACKED);
                log.warn("Transaction expired and rolled back: transactionId={}", entry.getKey());
                cleaned++;
            }
        }
        return cleaned;
    }

    /**
     * 获取事务存储大小。
     */
    public int size() {
        return transactionStore.size();
    }

    // ==================== Getters and Setters ====================

    public long getTxTimeoutMillis() {
        return txTimeoutMillis;
    }

    public void setTxTimeoutMillis(long txTimeoutMillis) {
        this.txTimeoutMillis = txTimeoutMillis;
    }

    public int getMaxCheckTimes() {
        return maxCheckTimes;
    }

    public void setMaxCheckTimes(int maxCheckTimes) {
        this.maxCheckTimes = maxCheckTimes;
    }

    public long getCheckIntervalMillis() {
        return checkIntervalMillis;
    }

    public void setCheckIntervalMillis(long checkIntervalMillis) {
        this.checkIntervalMillis = checkIntervalMillis;
    }
}
