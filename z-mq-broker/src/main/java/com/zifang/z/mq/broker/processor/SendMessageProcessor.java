package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerController;
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
import com.zifang.z.mq.store.PutMessageStatus;
import com.zifang.z.mq.store.log.CommitLog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

/**
 * Broker 端"发送消息"处理器（对标 RocketMQ SendMessageProcessor）.
 * <p>
 * 接收 SEND_MESSAGE 请求 → 解码 MessageExt → 写入 CommitLog → 返回 SendResult。
 */
public class SendMessageProcessor implements NettyRemotingAbstract.NettyRequestProcessor {

    private static final Logger log = LogManager.getLogger(SendMessageProcessor.class);

    private final BrokerController brokerController;

    public SendMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        TopicLogHelper.incSendCount();
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        // 处理 oneway
        if (request.isOnewayRPC()) {
            log.debug("sendMessage oneway received, ignore response");
            return null;
        }

        String topic = request.getExtField("topic");
        String queueIdStr = request.getExtField("queueId");
        String brokerName = request.getExtField("brokerName");
        if (topic == null || queueIdStr == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("topic or queueId missing");
            return response;
        }

        int queueId = Integer.parseInt(queueIdStr);
        MessageExt inbound = decodeMessage(request);
        if (inbound == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("decode body failed");
            return response;
        }

        CommitLog commitLog = brokerController.getCommitLog();
        if (commitLog == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("commitLog not initialized");
            return response;
        }

        // 构造 MessageExtBrokerInner
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        copyMessageExtFields(inbound, inner);
        inner.setQueueId(queueId);

        PutMessageResult result = commitLog.putMessage(inner);
        // 构造 SendResult
        SendResult sendResult = new SendResult();
        if (result.isOk()) {
            sendResult.setSendStatus(SendStatus.SEND_OK);
            AppendMessageResult amr = result.getAppendMessageResult();
            if (amr != null) {
                sendResult.setMsgId(inner.getMsgId());
                sendResult.setTopic(topic);
                sendResult.setQueueId(queueId);
                sendResult.setQueueOffset(amr.getWroteOffset());
            }
        } else {
            sendResult.setSendStatus(SendStatus.SEND_FAILED);
            sendResult.setErrorMsg(result.getPutMessageStatus().name());
        }
        // 响应码
        response.setCode(RemotingSysResponseCode.SUCCESS);
        response.setBody(JsonCodec.encode(sendResult));
        TopicLogHelper.incSendSuccess(result.isOk());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
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
        // 序列化属性为字符串，便于写入 CommitLog
        StringBuilder props = new StringBuilder();
        if (src.getProperties() != null) {
            for (java.util.Map.Entry<String, String> e : src.getProperties().entrySet()) {
                props.append(e.getKey()).append("=").append(e.getValue()).append("\n");
            }
        }
        dst.setPropertiesString(props.toString());
    }

    /** 简易统计（避免日志噪音） */
    static class TopicLogHelper {
        static final java.util.concurrent.atomic.AtomicLong SEND_COUNT = new java.util.concurrent.atomic.AtomicLong();
        static final java.util.concurrent.atomic.AtomicLong SEND_SUCCESS = new java.util.concurrent.atomic.AtomicLong();

        static void incSendCount() {
            SEND_COUNT.incrementAndGet();
        }

        static void incSendSuccess(boolean ok) {
            if (ok) SEND_SUCCESS.incrementAndGet();
        }

        public static long getSendCount() { return SEND_COUNT.get(); }
        public static long getSendSuccess() { return SEND_SUCCESS.get(); }
    }
}
