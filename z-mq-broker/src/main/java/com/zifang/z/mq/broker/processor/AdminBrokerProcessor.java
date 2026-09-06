package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.common.QueueData;
import com.zifang.z.mq.common.TopicConfig;
import com.zifang.z.mq.remoting.netty.NettyRemotingAbstract;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker 端"管理"处理器（对标 RocketMQ AdminBrokerProcessor）.
 * <p>
 * 支持：
 * <ul>
 *   <li>CREATE_TOPIC — 创建 Topic（默认 4 读 4 写队列）</li>
 *   <li>VIEW_TOPIC — 查看 Topic 配置</li>
 * </ul>
 */
public class AdminBrokerProcessor implements NettyRemotingAbstract.NettyRequestProcessor {

    private static final Logger log = LogManager.getLogger(AdminBrokerProcessor.class);

    /** 本地 Topic 配置缓存 (subscriber reuse). */
    private final ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

    private final BrokerController brokerController;

    public AdminBrokerProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        int code = request.getCode();
        switch (code) {
            case RequestCode.CREATE_TOPIC:
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return handleCreateTopic(request);
            case RequestCode.GET_ALL_TOPIC_LIST:
                return handleGetAllTopics(request);
            default:
                RemotingCommand response = RemotingCommand.createResponseCommand(
                        RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED);
                response.setOpaque(request.getOpaque());
                response.setRemark("admin code " + code + " not supported");
                return response;
        }
    }

    private RemotingCommand handleCreateTopic(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        String topic = request.getExtField("topic");
        String readNStr = request.getExtField("readQueueNums");
        String writeNStr = request.getExtField("writeQueueNums");
        if (topic == null || topic.isEmpty()) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("topic is empty");
            return response;
        }

        int readN = readNStr == null ? TopicConfig.DEFAULT_READ_QUEUE_NUMS : Integer.parseInt(readNStr);
        int writeN = writeNStr == null ? TopicConfig.DEFAULT_WRITE_QUEUE_NUMS : Integer.parseInt(writeNStr);
        TopicConfig config = new TopicConfig(topic, readN, writeN, TopicConfig.PERM_READ_WRITE);
        topicConfigTable.put(topic, config);
        log.info("Topic created: {} read={} write={}", topic, readN, writeN);

        CreateTopicResult body = new CreateTopicResult();
        body.setTopic(topic);
        body.setQueueDataList(toQueueData(config));
        response.setBody(com.zifang.z.mq.common.util.JsonCodec.encode(body));
        return response;
    }

    private RemotingCommand handleGetAllTopics(RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        TopicListResult body = new TopicListResult();
        body.setTopics(new ArrayList<>(topicConfigTable.keySet()));
        response.setBody(com.zifang.z.mq.common.util.JsonCodec.encode(body));
        return response;
    }

    private List<QueueData> toQueueData(TopicConfig config) {
        List<QueueData> list = new ArrayList<>();
        QueueData qd = new QueueData();
        qd.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
        qd.setReadQueueNums(config.getReadQueueNums());
        qd.setWriteQueueNums(config.getWriteQueueNums());
        qd.setPerm(config.getPerm());
        list.add(qd);
        return list;
    }

    public TopicConfig getTopicConfig(String topic) {
        return topicConfigTable.get(topic);
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public static class CreateTopicResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private String topic;
        private List<QueueData> queueDataList;

        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public List<QueueData> getQueueDataList() { return queueDataList; }
        public void setQueueDataList(List<QueueData> queueDataList) { this.queueDataList = queueDataList; }
    }

    public static class TopicListResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private List<String> topics = new ArrayList<>();

        public List<String> getTopics() { return topics; }
        public void setTopics(List<String> topics) { this.topics = topics; }
    }
}
