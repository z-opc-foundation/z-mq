package com.zifang.z.mq.client.producer;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.message.Message;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.exception.RemotingConnectException;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 默认 MQ 生产者（对标 RocketMQ DefaultMQProducer）.
 * <p>
 * 一个 producerGroup 共享一个 DefaultMQProducer 实例。
 * 内部委托给 MQClientInstance 完成与 NameServer / Broker 的通信。
 */
public class DefaultMQProducer {

    private static final Logger log = LogManager.getLogger(DefaultMQProducer.class);

    private String producerGroup;
    private String namesrvAddr;
    private NettyClientConfig nettyClientConfig;
    private String clientId;
    private long sendMsgTimeoutMillis = 3000;
    private int defaultTopicQueueNums = 4;

    private MQClientInstance mqClientInstance;
    private final ConcurrentHashMap<String, MQClientInstance> instanceTable = new ConcurrentHashMap<>();
    private final AtomicLong msgIdGenerator = new AtomicLong(0);

    public DefaultMQProducer(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public void start() throws Exception {
        if (this.clientId == null) {
            this.clientId = "PRODUCER_" + producerGroup + "_" + System.currentTimeMillis();
        }
        if (this.nettyClientConfig == null) {
            this.nettyClientConfig = new NettyClientConfig();
        }
        MQClientInstance instance = getOrCreateInstance();
        // 通过 MQClientInstance 引用保持
        this.mqClientInstance = instance;
        log.info("DefaultMQProducer started: group={}, namesrv={}", producerGroup, namesrvAddr);
    }

    public void shutdown() {
        // 简化：每个 Producer 共享一份 instance，但生产端通常应保留
        for (MQClientInstance ins : instanceTable.values()) {
            ins.shutdown();
        }
        instanceTable.clear();
        log.info("DefaultMQProducer shutdown: group={}", producerGroup);
    }

    /**
     * 同步发送：阻塞等待结果，失败抛出异常.
     */
    public SendResult send(Message message) throws Exception {
        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }
        if (mqClientInstance == null) {
            throw new IllegalStateException("producer not started");
        }
        validateMessage(message);
        MQClientInstance instance = getOrCreateInstance();

        TopicRouteData routeData = instance.getTopicRouteData(message.getTopic());
        if (routeData == null || routeData.getQueueDatas() == null || routeData.getQueueDatas().isEmpty()) {
            throw new RemotingSendRequestException("No route for topic: " + message.getTopic());
        }

        // 选一个队列
        MessageQueue mq = instance.selectOneMessageQueue(message.getTopic(), routeData);
        if (mq == null) {
            throw new RemotingSendRequestException("No writable queue for topic: " + message.getTopic());
        }

        // 找 master broker 地址
        String brokerAddr = lookupBrokerMasterAddr(routeData, mq.getBrokerName());
        if (brokerAddr == null) {
            throw new RemotingSendRequestException("No master addr for broker: " + mq.getBrokerName());
        }

        // 构造内部消息
        MessageExt inner = buildMessageExt(message, mq);

        // 构造 RemotingCommand
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        request.addExtField("topic", message.getTopic());
        request.addExtField("queueId", String.valueOf(mq.getQueueId()));
        request.addExtField("brokerName", mq.getBrokerName());
        request.setBody(JsonCodec.encode(inner));

        // 同步 RPC
        RemotingCommand response = instance.invokeSync(brokerAddr, request, sendMsgTimeoutMillis);

        SendResult result = new SendResult();
        result.setSendStatus(SendStatus.SEND_OK);
        if (response == null) {
            result.setSendStatus(SendStatus.FLUSH_DISK_TIMEOUT);
            return result;
        }
        if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
            result.setSendStatus(SendStatus.SEND_FAILED);
            result.setErrorMsg(response.getRemark());
            return result;
        }
        if (response.getBody() != null && response.getBody().length > 0) {
            try {
                SendResult remote = JsonCodec.decode(response.getBody(), SendResult.class);
                if (remote != null) {
                    return remote;
                }
            } catch (Exception e) {
                log.warn("decode send result failed, use local", e);
            }
        }
        return result;
    }

    /**
     * 单向发送：不关心结果.
     */
    public void sendOneway(Message message) throws Exception {
        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }
        if (mqClientInstance == null) {
            throw new IllegalStateException("producer not started");
        }
        MQClientInstance instance = getOrCreateInstance();
        TopicRouteData routeData = instance.getTopicRouteData(message.getTopic());
        if (routeData == null || routeData.getQueueDatas() == null || routeData.getQueueDatas().isEmpty()) {
            throw new RemotingSendRequestException("No route for topic: " + message.getTopic());
        }
        MessageQueue mq = new MessageQueue(message.getTopic(),
                routeData.getQueueDatas().get(0).getBrokerName(), 0);
        String brokerAddr = lookupBrokerMasterAddr(routeData, mq.getBrokerName());
        if (brokerAddr == null) {
            throw new RemotingSendRequestException("No master addr for broker: " + mq.getBrokerName());
        }
        MessageExt inner = buildMessageExt(message, mq);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        request.markOnewayRPC();
        request.addExtField("topic", message.getTopic());
        request.addExtField("queueId", String.valueOf(mq.getQueueId()));
        request.addExtField("brokerName", mq.getBrokerName());
        request.setBody(JsonCodec.encode(inner));
        RemotingCommand response = instance.invokeSync(brokerAddr, request, sendMsgTimeoutMillis);
        // Oneway 也不应等响应，但我们这里依旧走 invokeSync，broker 端会按 oneway 处理
    }

    /**
     * 异步发送：SendCallback 回调结果.
     */
    public void send(Message message, SendCallback sendCallback) throws Exception {
        MQClientInstance instance = getOrCreateInstance();
        try {
            SendResult result = send(message);
            if (sendCallback != null) {
                sendCallback.onSuccess(result);
            }
        } catch (Exception e) {
            if (sendCallback != null) {
                sendCallback.onException(e);
            } else {
                throw e;
            }
        }
    }

    /**
     * 按指定队列发送（保证顺序）.
     */
    public SendResult send(Message message, MessageQueue mq) throws Exception {
        if (mq == null) {
            return send(message);
        }
        if (mqClientInstance == null) {
            throw new IllegalStateException("producer not started");
        }
        MQClientInstance instance = getOrCreateInstance();
        TopicRouteData routeData = instance.getTopicRouteData(message.getTopic());
        if (routeData == null) {
            throw new RemotingSendRequestException("No route for topic: " + message.getTopic());
        }
        String brokerAddr = lookupBrokerMasterAddr(routeData, mq.getBrokerName());
        if (brokerAddr == null) {
            throw new RemotingSendRequestException("No master addr for broker: " + mq.getBrokerName());
        }

        MessageExt inner = buildMessageExt(message, mq);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        request.addExtField("topic", message.getTopic());
        request.addExtField("queueId", String.valueOf(mq.getQueueId()));
        request.addExtField("brokerName", mq.getBrokerName());
        request.setBody(JsonCodec.encode(inner));

        RemotingCommand response = instance.invokeSync(brokerAddr, request, sendMsgTimeoutMillis);
        SendResult result = new SendResult();
        if (response == null) {
            result.setSendStatus(SendStatus.FLUSH_DISK_TIMEOUT);
            return result;
        }
        if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
            result.setSendStatus(SendStatus.SEND_FAILED);
            result.setErrorMsg(response.getRemark());
            return result;
        }
        result.setSendStatus(SendStatus.SEND_OK);
        return result;
    }

    private MQClientInstance getOrCreateInstance() throws InterruptedException {
        if (mqClientInstance != null && mqClientInstance.getRemotingClient() != null) {
            return mqClientInstance;
        }
        String key = "PRODUCER@" + producerGroup;
        MQClientInstance inst = instanceTable.get(key);
        if (inst == null) {
            inst = new MQClientInstance(clientId, namesrvAddr, nettyClientConfig);
            inst.start();
            instanceTable.put(key, inst);
        }
        return inst;
    }

    private MessageExt buildMessageExt(Message message, MessageQueue mq) {
        MessageExt ext = new MessageExt();
        ext.setTopic(message.getTopic());
        ext.setTags(message.getTags());
        ext.setKeys(message.getKeys());
        ext.setBody(message.getBody());
        ext.setFlag(message.getFlag());
        ext.setProperties(message.getProperties());
        ext.setBornTimestamp(System.currentTimeMillis());
        ext.setQueueId(mq.getQueueId());
        ext.setQueueOffset(0L);
        ext.setMsgId(generateMsgId(message.getTopic()));
        return ext;
    }

    private String generateMsgId(String topic) {
        return clientId + "-" + topic + "-" + msgIdGenerator.incrementAndGet();
    }

    private String lookupBrokerMasterAddr(TopicRouteData routeData, String brokerName) {
        if (routeData == null || routeData.getBrokerDatas() == null) {
            return null;
        }

        for (BrokerData bd : routeData.getBrokerDatas()) {
            if (bd.getBrokerName().equals(brokerName)) {
                return bd.selectBrokerAddr();
            }
        }
        return null;
    }

    private void validateMessage(Message message) {
        if (message.getTopic() == null || message.getTopic().isEmpty()) {
            throw new IllegalArgumentException("topic is null or empty");
        }
        if (message.getBody() == null) {
            throw new IllegalArgumentException("body is null");
        }
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public long getSendMsgTimeoutMillis() {
        return sendMsgTimeoutMillis;
    }

    public void setSendMsgTimeoutMillis(long sendMsgTimeoutMillis) {
        this.sendMsgTimeoutMillis = sendMsgTimeoutMillis;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public void setNettyClientConfig(NettyClientConfig nettyClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /** P 占位.
     */
    static class P implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
