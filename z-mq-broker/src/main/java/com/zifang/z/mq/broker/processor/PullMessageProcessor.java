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

import java.util.List;

/**
 * Broker 端"拉取消息"处理器（对标 RocketMQ PullMessageProcessor）.
 * <p>
 * 当前实现通过 CommitLog.getQueueIndex() 的进程内 InMemoryQueueIndex 查询真实写入的消息,
 * 保证 nextOffset 单调递增和消息内容一致性。
 * <p>
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
        long maxOffset = commitLog.getQueueIndex().getMaxOffset(topic, queueId);
        long minOffset = messages.isEmpty() ? maxOffset : messages.get(0).getQueueOffset();
        // nextOffset 应为最后一条消息的 offset +1, 而非 maxOffset
        long nextOffset = messages.isEmpty() ? offset : messages.get(messages.size() - 1).getQueueOffset() + 1;
        PullResultPayload body = new PullResultPayload(topic, queueId, nextOffset, minOffset, maxOffset, messages);
        response.setBody(JsonCodec.encode(body));
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 从进程内 InMemoryQueueIndex 查询真实消息。
     * <p>
     * nextOffset 由 InMemoryQueueIndex 保证单调递增, 内容来自 CommitLog.putMessage 实际写入的消息。
     */
    private List<MessageExt> readMessages(CommitLog commitLog, String topic, int queueId, long offset, int maxNum) {
        if (commitLog.getQueueIndex() == null) {
            return java.util.Collections.emptyList();
        }
        return commitLog.getQueueIndex().query(topic, queueId, offset, maxNum);
    }
}