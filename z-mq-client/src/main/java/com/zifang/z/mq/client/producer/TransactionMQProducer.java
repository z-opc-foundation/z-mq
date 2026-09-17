package com.zifang.z.mq.client.producer;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.common.message.Message;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务消息生产者（对标 RocketMQ TransactionMQProducer）.
 * <p>
 * 支持两阶段事务消息：
 * <ol>
 *   <li>发送 Half 消息到 Broker（消息暂不可见）</li>
 *   <li>执行本地事务</li>
 *   <li>根据本地事务结果发送 Commit 或 Rollback</li>
 * </ol>
 * <p>
 * 使用示例：
 * <pre>
 * TransactionMQProducer producer = new TransactionMQProducer("tx-group");
 * producer.setNamesrvAddr("localhost:9876");
 * producer.setTransactionListener(new MyTransactionListener());
 * producer.start();
 *
 * Message msg = new Message("topic", "tag", "key", body);
 * producer.sendMessageInTransaction(msg, null);
 * </pre>
 */
public class TransactionMQProducer extends DefaultMQProducer {

    private static final Logger log = LogManager.getLogger(TransactionMQProducer.class);

    private TransactionListener transactionListener;
    private long checkMaxTimes = 15;
    private long checkIntervalMillis = 60_000L;

    /** 本地事务执行状态缓存: transactionId -> TransactionState */
    private final ConcurrentHashMap<String, TransactionState> localTransactionStateTable = new ConcurrentHashMap<>();

    /** Half 消息缓存: transactionId -> Message */
    private final ConcurrentHashMap<String, Message> halfMessageTable = new ConcurrentHashMap<>();

    public TransactionMQProducer(String producerGroup) {
        super(producerGroup);
    }

    public TransactionMQProducer(String producerGroup, NettyClientConfig nettyClientConfig) {
        super(producerGroup, nettyClientConfig);
    }

    /**
     * 发送事务消息（两阶段）。
     *
     * @param message 待发送的消息
     * @param arg     传递给 TransactionListener.executeLocalTransaction 的参数
     * @return 发送结果
     * @throws Exception 发送异常
     */
    public SendResult sendMessageInTransaction(Message message, Object arg) throws Exception {
        if (transactionListener == null) {
            throw new IllegalStateException("TransactionListener not set");
        }

        // 生成事务 ID
        String transactionId = UUID.randomUUID().toString();
        message.putProperty("TRANSACTION_ID", transactionId);
        message.setTransactionId(transactionId);

        // 1. 发送 Half 消息到 Broker
        // 将消息 Topic 临时改为 TRANS_HALF_TOPIC
        String originalTopic = message.getTopic();
        message.setTopic("TRANS_HALF_TOPIC");

        try {
            SendResult sendResult = super.send(message);
            if (sendResult == null || sendResult.getSendStatus() != com.zifang.z.mq.common.protocol.SendStatus.SEND_OK) {
                log.error("Send half message failed: transactionId={}", transactionId);
                return sendResult;
            }

            // 2. 缓存 Half 消息
            halfMessageTable.put(transactionId, message);

            // 3. 执行本地事务
            TransactionState localState;
            try {
                localState = transactionListener.executeLocalTransaction(message, arg);
            } catch (Exception e) {
                log.error("Execute local transaction failed: transactionId={}", transactionId, e);
                localState = TransactionState.UNKNOWN;
            }

            // 4. 缓存本地事务状态
            localTransactionStateTable.put(transactionId, localState);

            // 5. 发送二次确认到 Broker
            sendTransactionCommit(transactionId, originalTopic, localState);

            return sendResult;
        } catch (Exception e) {
            // 发送 Half 消息失败，回滚
            message.setTopic(originalTopic);
            throw e;
        }
    }

    /**
     * 发送事务二次确认（Commit/Rollback）到 Broker。
     */
    private void sendTransactionCommit(String transactionId, String originalTopic, TransactionState state)
            throws Exception {
        if (mqClientInstance == null) {
            log.warn("MQClientInstance not initialized, skip transaction commit: transactionId={}", transactionId);
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION);
        request.addExtField("transactionId", transactionId);
        request.addExtField("originalTopic", originalTopic);
        request.addExtField("transactionState", state.name());

        // 选择 Broker 地址发送
        String brokerAddr = selectBrokerAddr();
        if (brokerAddr == null) {
            log.warn("No broker available for transaction commit: transactionId={}", transactionId);
            return;
        }

        try {
            RemotingCommand response = mqClientInstance.invokeSync(brokerAddr, request, 3000);
            if (response != null) {
                log.info("Transaction commit sent: transactionId={} state={}", transactionId, state);
            }
        } catch (RemotingTimeoutException | RemotingSendRequestException e) {
            log.error("Send transaction commit failed: transactionId={}", transactionId, e);
        }
    }

    /**
     * 处理 Broker 回查（由 MQClientInstance 调用）。
     *
     * @param transactionId 事务 ID
     * @return 事务状态
     */
    public TransactionState checkTransaction(String transactionId) {
        // 先检查本地缓存
        TransactionState cachedState = localTransactionStateTable.get(transactionId);
        if (cachedState != null && cachedState != TransactionState.UNKNOWN) {
            return cachedState;
        }

        // 调用 listener 回查
        Message halfMessage = halfMessageTable.get(transactionId);
        if (halfMessage == null) {
            log.warn("Half message not found for check: transactionId={}", transactionId);
            return TransactionState.ROLLBACK;
        }

        try {
            TransactionState state = transactionListener.checkLocalTransaction(halfMessage);
            if (state != TransactionState.UNKNOWN) {
                localTransactionStateTable.put(transactionId, state);
            }
            return state;
        } catch (Exception e) {
            log.error("Check local transaction failed: transactionId={}", transactionId, e);
            return TransactionState.UNKNOWN;
        }
    }

    /**
     * 清理已完成的事务缓存。
     */
    public void clearTransactionCache(String transactionId) {
        localTransactionStateTable.remove(transactionId);
        halfMessageTable.remove(transactionId);
    }

    private String selectBrokerAddr() {
        // 简单实现：从 NameServer 获取 Broker 地址
        if (mqClientInstance != null) {
            try {
                com.zifang.z.mq.common.TopicRouteData routeData = mqClientInstance.getTopicRouteData("TRANS_HALF_TOPIC");
                if (routeData != null && routeData.getBrokerDatas() != null && !routeData.getBrokerDatas().isEmpty()) {
                    return routeData.getBrokerDatas().get(0).selectBrokerAddr();
                }
            } catch (Exception e) {
                log.debug("Get broker addr failed: {}", e.getMessage());
            }
        }
        return null;
    }

    // ==================== Getters and Setters ====================

    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;
    }

    public long getCheckMaxTimes() {
        return checkMaxTimes;
    }

    public void setCheckMaxTimes(long checkMaxTimes) {
        this.checkMaxTimes = checkMaxTimes;
    }

    public long getCheckIntervalMillis() {
        return checkIntervalMillis;
    }

    public void setCheckIntervalMillis(long checkIntervalMillis) {
        this.checkIntervalMillis = checkIntervalMillis;
    }
}
