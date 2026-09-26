package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.broker.transaction.TransactionStateManager;
import com.zifang.z.mq.common.message.Message;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyRemotingAbstract;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.AppendMessageResult;
import com.zifang.z.mq.store.MessageExtBrokerInner;
import com.zifang.z.mq.store.PutMessageResult;
import com.zifang.z.mq.store.log.CommitLog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 事务消息处理器（对标 RocketMQ TransactionMessageProcessor）.
 * <p>
 * 处理以下请求：
 * <ul>
 *   <li>{@link RequestCode#SEND_MESSAGE_V2} — 发送 Half 消息</li>
 *   <li>{@link RequestCode#END_TRANSACTION} — 事务二次确认（Commit/Rollback）</li>
 *   <li>{@link RequestCode#CHECK_TRANSACTION_STATE} — 事务回查</li>
 * </ul>
 */
public class TransactionMessageProcessor implements NettyRemotingAbstract.NettyRequestProcessor {

    private static final Logger log = LogManager.getLogger(TransactionMessageProcessor.class);

    private final BrokerController brokerController;
    private final TransactionStateManager transactionStateManager;

    public TransactionMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.transactionStateManager = new TransactionStateManager();
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        int code = request.getCode();
        switch (code) {
            case RequestCode.SEND_MESSAGE_V2:
                return processHalfMessage(ctx, request);
            case RequestCode.END_TRANSACTION:
                return processEndTransaction(ctx, request);
            case RequestCode.CHECK_TRANSACTION_STATE:
                return processCheckTransaction(ctx, request);
            default:
                RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR);
                response.setRemark("Unsupported request code: " + code);
                return response;
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 处理 Half 消息发送。
     */
    private RemotingCommand processHalfMessage(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        String topic = request.getExtField("topic");
        String transactionId = request.getExtField("transactionId");
        String queueIdStr = request.getExtField("queueId");

        if (topic == null || transactionId == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("topic or transactionId missing");
            return response;
        }

        int queueId = queueIdStr != null ? Integer.parseInt(queueIdStr) : 0;

        // 解码消息
        MessageExt inbound = decodeMessage(request);
        if (inbound == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("decode body failed");
            return response;
        }

        // 存储 Half 消息
        String originalTopic = inbound.getTopic();
        transactionStateManager.putHalfMessage(transactionId, originalTopic, inbound);

        // 将 Half 消息写入 TRANS_HALF_TOPIC
        CommitLog commitLog = brokerController.getCommitLog();
        if (commitLog == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("commitLog not initialized");
            return response;
        }

        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        copyMessageExtFields(inbound, inner);
        inner.setTopic("TRANS_HALF_TOPIC");
        inner.setQueueId(queueId);
        inner.putProperty("TRANSACTION_ID", transactionId);
        inner.putProperty("ORIGINAL_TOPIC", originalTopic);

        PutMessageResult result = commitLog.putMessage(inner);

        SendResult sendResult = new SendResult();
        if (result.isOk()) {
            sendResult.setSendStatus(SendStatus.SEND_OK);
            sendResult.setMsgId(inner.getMsgId());
            sendResult.setTopic(topic);
            sendResult.setQueueId(queueId);
            AppendMessageResult amr = result.getAppendMessageResult();
            if (amr != null) {
                sendResult.setQueueOffset(inner.getQueueOffset());
            }
        } else {
            sendResult.setSendStatus(SendStatus.SEND_FAILED);
            sendResult.setErrorMsg(result.getPutMessageStatus().name());
        }

        response.setBody(JsonCodec.encode(sendResult));
        return response;
    }

    /**
     * 处理事务二次确认（Commit/Rollback）。
     */
    private RemotingCommand processEndTransaction(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        String transactionId = request.getExtField("transactionId");
        String stateStr = request.getExtField("transactionState");
        String originalTopic = request.getExtField("originalTopic");

        if (transactionId == null || stateStr == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("transactionId or state missing");
            return response;
        }

        com.zifang.z.mq.client.producer.TransactionState state =
                com.zifang.z.mq.client.producer.TransactionState.valueOf(stateStr);

        switch (state) {
            case COMMIT:
                String topic = transactionStateManager.commitTransaction(transactionId);
                if (topic != null) {
                    log.info("Transaction committed: transactionId={} topic={}", transactionId, topic);
                    // 通知 Broker 重新投递消息到原始 Topic
                    // 实际实现中需要从 TRANS_HALF_TOPIC 读取消息并投递到原始 Topic
                }
                break;
            case ROLLBACK:
                transactionStateManager.rollbackTransaction(transactionId);
                log.info("Transaction rolled back: transactionId={}", transactionId);
                break;
            case UNKNOWN:
                log.info("Transaction state unknown: transactionId={}", transactionId);
                break;
        }

        return response;
    }

    /**
     * 处理事务回查。
     */
    private RemotingCommand processCheckTransaction(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        String transactionId = request.getExtField("transactionId");
        if (transactionId == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("transactionId missing");
            return response;
        }

        TransactionStateManager.TransactionRecord record = transactionStateManager.getTransaction(transactionId);
        if (record == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("Transaction not found: " + transactionId);
            return response;
        }

        // 检查回查次数
        if (record.getCheckTimes() >= transactionStateManager.getMaxCheckTimes()) {
            // 超过最大回查次数，自动回滚
            transactionStateManager.rollbackTransaction(transactionId);
            log.warn("Transaction max check times reached, rolled back: transactionId={}", transactionId);
            response.setBody(JsonCodec.encode("ROLLBACK"));
            return response;
        }

        // 更新回查状态
        transactionStateManager.markChecked(transactionId);

        // 返回 Half 消息给客户端进行回查
        response.setBody(JsonCodec.encode(record.getHalfMessage()));
        return response;
    }

    /**
     * 获取事务状态管理器。
     */
    public TransactionStateManager getTransactionStateManager() {
        return transactionStateManager;
    }

    private MessageExt decodeMessage(RemotingCommand request) {
        if (request.getBody() == null || request.getBody().length == 0) {
            return null;
        }
        try {
            return JsonCodec.decode(request.getBody(), MessageExt.class);
        } catch (Exception e) {
            log.error("decode message body failed", e);
            return null;
        }
    }

    private void copyMessageExtFields(MessageExt src, MessageExtBrokerInner dst) {
        dst.setTopic(src.getTopic());
        dst.setTags(src.getTags());
        dst.setKeys(src.getKeys());
        dst.setBody(src.getBody());
        dst.setFlag(src.getFlag());
        dst.setProperties(src.getProperties());
        dst.setBornTimestamp(src.getBornTimestamp());
        dst.setQueueId(src.getQueueId());
        dst.setQueueOffset(src.getQueueOffset());
        dst.setMsgId(src.getMsgId());
        StringBuilder props = new StringBuilder();
        if (src.getProperties() != null) {
            for (java.util.Map.Entry<String, String> e : src.getProperties().entrySet()) {
                props.append(e.getKey()).append("=").append(e.getValue()).append("\n");
            }
        }
        dst.setPropertiesString(props.toString());
    }
}
