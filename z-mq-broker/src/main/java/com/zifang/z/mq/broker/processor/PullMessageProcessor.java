package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.common.filter.FilterType;
import com.zifang.z.mq.common.filter.MessageFilter;
import com.zifang.z.mq.common.filter.Sql92Filter;
import com.zifang.z.mq.common.filter.TagFilter;
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
 * 当前实现通过 CommitLog.getQueueIndex() 的进程内 InMemoryQueueIndex 查询真实写入的消息,
 * 保证 nextOffset 单调递增和消息内容一致性。
 * <p>
 * 支持 Broker 端消息过滤：
 * <ul>
 *   <li>Tag 标签过滤 — 基于消息 Tag 精确匹配</li>
 *   <li>SQL92 属性过滤 — 基于消息属性的 SQL92 表达式求值</li>
 * </ul>
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

        // 读取过滤参数（客户端可选发送）
        String filterTypeStr = request.getExtField("filterType");
        String filterExpression = request.getExtField("filterExpression");

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

        // 读取消息（比请求数量多读一些，用于过滤后仍有足够消息）
        int fetchNum = filterExpression != null && !filterExpression.isEmpty() ? maxNum * 3 : maxNum;
        List<MessageExt> messages = readMessages(commitLog, topic, queueId, offset, fetchNum);

        // Broker 端过滤
        MessageFilter filter = createFilter(filterTypeStr, filterExpression);
        if (filter != null) {
            messages = applyFilter(messages, filter, maxNum);
        } else {
            // 无过滤器时截取到请求数量
            if (messages.size() > maxNum) {
                messages = messages.subList(0, maxNum);
            }
        }

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
     * 根据过滤类型和表达式创建消息过滤器。
     *
     * @param filterTypeStr   过滤类型字符串（TAG / SQL92）
     * @param filterExpression 过滤表达式
     * @return 消息过滤器，null 表示不过滤
     */
    private MessageFilter createFilter(String filterTypeStr, String filterExpression) {
        if (filterExpression == null || filterExpression.isEmpty()) {
            return null;
        }

        FilterType filterType = FilterType.fromString(filterTypeStr);

        switch (filterType) {
            case TAG:
                return new TagFilter(filterExpression);
            case SQL92:
                return new Sql92Filter(filterExpression);
            default:
                return null;
        }
    }

    /**
     * 对消息列表应用过滤器，返回匹配的消息。
     *
     * @param messages 原始消息列表
     * @param filter   过滤器
     * @param maxNum   最大返回数量
     * @return 过滤后的消息列表
     */
    private List<MessageExt> applyFilter(List<MessageExt> messages, MessageFilter filter, int maxNum) {
        List<MessageExt> filtered = new ArrayList<>();
        for (MessageExt msg : messages) {
            if (filtered.size() >= maxNum) {
                break;
            }
            try {
                if (filter.match(msg)) {
                    filtered.add(msg);
                }
            } catch (Exception e) {
                // 过滤异常时跳过该消息（与 RocketMQ 行为一致）
                log.debug("Filter match failed for msg {}: {}", msg.getMsgId(), e.getMessage());
            }
        }
        return filtered;
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