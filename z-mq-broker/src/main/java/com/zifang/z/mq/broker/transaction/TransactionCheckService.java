package com.zifang.z.mq.broker.transaction;

import com.zifang.z.mq.common.message.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 事务回查调度服务（对标 RocketMQ TransactionCheckService）.
 * <p>
 * 定期扫描待回查的事务消息，向 Producer 发起回查请求：
 * <ul>
 *   <li>扫描 HALF 状态的事务记录</li>
 *   <li>超过回查间隔的记录触发回查</li>
 *   <li>超过最大回查次数的记录自动回滚</li>
 * </ul>
 */
public class TransactionCheckService {

    private static final Logger log = LogManager.getLogger(TransactionCheckService.class);

    private final TransactionStateManager transactionStateManager;
    private final TransactionCheckCallback checkCallback;
    private ScheduledExecutorService checkExecutor;
    private volatile boolean running = false;

    /**
     * 回查回调接口。
     */
    public interface TransactionCheckCallback {
        /**
         * 向 Producer 发起事务回查。
         *
         * @param transactionId 事务 ID
         * @param halfMessage   Half 消息
         * @return Producer 返回的事务状态
         */
        com.zifang.z.mq.client.producer.TransactionState checkTransaction(String transactionId, Message halfMessage);
    }

    public TransactionCheckService(TransactionStateManager transactionStateManager,
                                   TransactionCheckCallback checkCallback) {
        this.transactionStateManager = transactionStateManager;
        this.checkCallback = checkCallback;
    }

    /**
     * 启动回查服务。
     *
     * @param intervalMillis 回查扫描间隔（毫秒）
     */
    public void start(long intervalMillis) {
        if (running) {
            return;
        }
        this.checkExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "TransactionCheckThread");
            t.setDaemon(true);
            return t;
        });
        this.running = true;
        this.checkExecutor.scheduleWithFixedRate(this::doCheck, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
        log.info("TransactionCheckService started: interval={}ms", intervalMillis);
    }

    /**
     * 停止回查服务。
     */
    public void shutdown() {
        this.running = false;
        if (checkExecutor != null) {
            checkExecutor.shutdownNow();
        }
        log.info("TransactionCheckService shutdown");
    }

    /**
     * 执行回查扫描。
     */
    private void doCheck() {
        if (!running || checkCallback == null) {
            return;
        }

        try {
            List<TransactionStateManager.TransactionRecord> pendingList =
                    transactionStateManager.getPendingTransactions();

            for (TransactionStateManager.TransactionRecord record : pendingList) {
                if (record.getCheckTimes() >= transactionStateManager.getMaxCheckTimes()) {
                    // 超过最大回查次数，自动回滚
                    transactionStateManager.rollbackTransaction(record.getTransactionId());
                    log.warn("Transaction max check times reached, auto rolled back: transactionId={}",
                            record.getTransactionId());
                    continue;
                }

                try {
                    // 调用回调进行回查
                    com.zifang.z.mq.client.producer.TransactionState state =
                            checkCallback.checkTransaction(record.getTransactionId(), record.getHalfMessage());

                    switch (state) {
                        case COMMIT:
                            transactionStateManager.commitTransaction(record.getTransactionId());
                            log.info("Transaction check result: COMMIT, transactionId={}", record.getTransactionId());
                            break;
                        case ROLLBACK:
                            transactionStateManager.rollbackTransaction(record.getTransactionId());
                            log.info("Transaction check result: ROLLBACK, transactionId={}", record.getTransactionId());
                            break;
                        case UNKNOWN:
                            // 继续等待下次回查
                            transactionStateManager.markChecked(record.getTransactionId());
                            log.debug("Transaction check result: UNKNOWN, transactionId={} checkTimes={}",
                                    record.getTransactionId(), record.getCheckTimes() + 1);
                            break;
                    }
                } catch (Exception e) {
                    log.error("Transaction check failed: transactionId={}", record.getTransactionId(), e);
                    transactionStateManager.markChecked(record.getTransactionId());
                }
            }

            // 清理过期事务
            int cleaned = transactionStateManager.cleanExpiredTransactions();
            if (cleaned > 0) {
                log.info("Cleaned {} expired transactions", cleaned);
            }
        } catch (Exception e) {
            log.error("Transaction check scan failed", e);
        }
    }

    /**
     * 是否正在运行。
     */
    public boolean isRunning() {
        return running;
    }
}
