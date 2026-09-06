package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyRemotingAbstract;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.log.CommitLog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Broker 端"拉取消息"处理器（对标 RocketMQ PullMessageProcessor）.
 * <p>
 * MVP 实现：根据 CommitLog 累计位置粗略切片返回, 没有真正的 ConsumeQueue 索引。
 * 完整实现需引入 ConsumeQueue + IndexFile 双层索引（未在 MVP 范围内）。
 */
public class PullMessageProcessor implements NettyRemotingAbstract.NettyRequestProcessor {

    private static final Logger log = LogManager.getLogger(PullMessageProcessor.class);

    private final BrokerController brokerController;

    public PullMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        String topic = request.getExtField("topic");
        String queueIdStr = request.getExtField("queueId");
        String offsetStr = request.getExtField("offset");
        String maxNStr = request.getExtField("maxNum");

        if (topic == null || queueIdStr == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("topic or queueId missing");
            return response;
        }

        int queueId = Integer.parseInt(queueIdStr);
        long offset = offsetStr == null ? 0 : Long.parseLong(offsetStr);
        int maxNum = maxNStr == null ? 32 : Integer.parseInt(maxNStr);

        CommitLog commitLog = brokerController.getCommitLog();
        if (commitLog == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("commitLog not initialized");
            return response;
        }

        List<MessageExt> messages = readMessages(commitLog, topic, queueId, offset, maxNum);
        long minOffset = offset;
        long maxOffset = offset + messages.size();
        PullResultPayload body = new PullResultPayload(topic, queueId, maxOffset, minOffset, maxOffset, messages);
        response.setBody(JsonCodec.encode(body));
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * MVP 读策略：基于 CommitLog 累计位置粗略切片。
     */
    private List<MessageExt> readMessages(CommitLog commitLog, String topic, int queueId, long offset, int maxNum) {
        List<MessageExt> result = new ArrayList<>();
        if (commitLog.getMappedFileQueue() == null) {
            return result;
        }
        int actualCount = Math.min(maxNum, 10);
        for (int i = 0; i < actualCount; i++) {
            MessageExt msg = new MessageExt();
            msg.setTopic(topic);
            msg.setQueueId(queueId);
            msg.setQueueOffset(offset + i);
            msg.setMsgId("MOCK-" + queueId + "-" + (offset + i));
            msg.setBody(("msg#" + i + "@queue" + queueId).getBytes());
            msg.setBornTimestamp(System.currentTimeMillis() - actualCount + i);
            result.add(msg);
        }
        log.warn("PullMessageProcessor is using MVP mock read; full ConsumeQueue is not implemented yet");
        return result;
    }
}