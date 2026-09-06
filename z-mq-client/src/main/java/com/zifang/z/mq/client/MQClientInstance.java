package com.zifang.z.mq.client;

import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.remoting.exception.RemotingConnectException;
import com.zifang.z.mq.remoting.exception.RemotingException;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.NettyRemotingClient;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import io.netty.channel.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 客户端实例（对标 RocketMQ MQClientInstance）.
 * <p>
 * 一个 ClientId 共享一个实例，负责：
 * <ul>
 *   <li>连接 NameServer 并缓存 Topic 路由</li>
 *   <li>维护到 Broker 的 Netty 长连接池</li>
 *   <li>对外暴露 sendMessage / invokeSync 等 RPC</li>
 * </ul>
 */
public class MQClientInstance {

    private static final Logger log = LogManager.getLogger(MQClientInstance.class);

    private final String clientId;
    private final String namesrvAddr;
    private final NettyClientConfig nettyClientConfig;
    private NettyRemotingClient remotingClient;

    /** 路由表: topic -> TopicRouteData. */
    private final ConcurrentHashMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

    /** Broker 连接缓存: brokerAddr -> Channel. */
    private final ConcurrentHashMap<String, Channel> brokerChannelTable = new ConcurrentHashMap<>();

    /** 选择器: topic -> 自增索引（轮询）. */
    private final ConcurrentHashMap<String, AtomicInteger> queueIndexTable = new ConcurrentHashMap<>();

    public MQClientInstance(String clientId, String namesrvAddr, NettyClientConfig nettyClientConfig) {
        this.clientId = clientId;
        this.namesrvAddr = namesrvAddr;
        this.nettyClientConfig = nettyClientConfig != null ? nettyClientConfig : new NettyClientConfig();
    }

    public void start() {
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
        this.remotingClient.start();
    }

    public void shutdown() {
        if (remotingClient != null) {
            remotingClient.shutdown();
        }
        for (Channel ch : brokerChannelTable.values()) {
            try {
                ch.close();
            } catch (Exception ignore) {
            }
        }
        brokerChannelTable.clear();
    }

    public String getClientId() {
        return clientId;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    /**
     * 向 NameServer 拉取 Topic 路由信息（带本地缓存）.
     */
    public TopicRouteData getTopicRouteData(String topic) {
        TopicRouteData cached = topicRouteTable.get(topic);
        if (cached != null) {
            return cached;
        }
        return fetchTopicRouteDataFromNameServer(topic);
    }

    /**
     * 强制刷新路由缓存.
     */
    public void updateTopicRouteData(String topic, TopicRouteData data) {
        if (data == null) {
            topicRouteTable.remove(topic);
        } else {
            topicRouteTable.put(topic, data);
        }
    }

    /**
     * 真实从 NameServer 拉取 Topic 路由.
     */
    private TopicRouteData fetchTopicRouteDataFromNameServer(String topic) {
        if (namesrvAddr == null || namesrvAddr.isEmpty()) {
            log.warn("No name server address configured");
            return null;
        }
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTE_BY_TOPIC);
        request.addExtField("topic", topic);

        try {
            Channel channel = getOrCreateNameServerChannel();
            if (channel == null) {
                return null;
            }
            RemotingCommand response = invokeAndTranslateException(channel, request, 3000);
            if (response == null) {
                return null;
            }
            if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
                log.warn("GetRouteFromNameServer failed: code={}, remark={}", response.getCode(), response.getRemark());
                return null;
            }
            byte[] body = response.getBody();
            if (body == null || body.length == 0) {
                return null;
            }
            try {
                TopicRouteData data = com.zifang.z.mq.common.util.JsonCodec.decode(body, TopicRouteData.class);
                topicRouteTable.put(topic, data);
                return data;
            } catch (Exception e) {
                log.error("Decode TopicRouteData failed", e);
                return null;
            }
        } catch (RemotingTimeoutException | RemotingSendRequestException | InterruptedException e) {
            log.error("Invoke name server failed for topic {}", topic, e);
            return null;
        }
    }

    /**
     * 获取或创建到 NameServer 的连接（按 "ip:port" 解析, 多个 addr 用 ";" 分隔）.
     */
    private Channel getOrCreateNameServerChannel() {
        String first = namesrvAddr.split(";")[0].trim();
        return getOrCreateBrokerChannel("ns@" + first, first);
    }

    /**
     * 获取或创建到 Broker 的连接.
     */
    public Channel getOrCreateBrokerChannel(String brokerAddr) {
        return getOrCreateBrokerChannel("broker@" + brokerAddr, brokerAddr);
    }

    private Channel getOrCreateBrokerChannel(String key, String addr) {
        Channel ch = brokerChannelTable.get(key);
        if (ch != null && ch.isActive()) {
            return ch;
        }
        try {
            ch = remotingClient.getOrCreateChannel(addr);
            brokerChannelTable.put(key, ch);
            return ch;
        } catch (Exception e) {
            log.error("Connect to {} failed", addr, e);
            return null;
        }
    }

    /**
     * 轮询选择一个写队列.
     */
    public MessageQueue selectOneMessageQueue(String topic, TopicRouteData routeData) {
        if (routeData == null || routeData.getQueueDatas() == null || routeData.getQueueDatas().isEmpty()) {
            return null;
        }
        // 找 master broker 的写队列
        List<MessageQueue> writableQueues = new ArrayList<>();
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String masterAddr = brokerData.selectBrokerAddr();
            if (masterAddr == null) continue;
            for (com.zifang.z.mq.common.QueueData qd : routeData.getQueueDatas()) {
                if (qd.getBrokerName().equals(brokerData.getBrokerName())) {
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        writableQueues.add(new MessageQueue(topic, brokerData.getBrokerName(), i));
                    }
                }
            }
        }
        if (writableQueues.isEmpty()) {
            return null;
        }
        Collections.shuffle(writableQueues); // 打散避免热点
        AtomicInteger idx = queueIndexTable.computeIfAbsent(topic, k -> new AtomicInteger(0));
        int i = Math.abs(idx.getAndIncrement() % writableQueues.size());
        return writableQueues.get(i);
    }

    public List<MessageQueue> getAllWritableQueues(String topic, TopicRouteData routeData) {
        if (routeData == null || routeData.getQueueDatas() == null || routeData.getQueueDatas().isEmpty()) {
            return Collections.emptyList();
        }
        List<MessageQueue> queues = new ArrayList<>();
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String masterAddr = brokerData.selectBrokerAddr();
            if (masterAddr == null) continue;
            for (com.zifang.z.mq.common.QueueData qd : routeData.getQueueDatas()) {
                if (qd.getBrokerName().equals(brokerData.getBrokerName())) {
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        queues.add(new MessageQueue(topic, brokerData.getBrokerName(), i));
                    }
                }
            }
        }
        return queues;
    }

    /**
     * 执行 Blot RPC（对 NameServer 或 Broker 的同步调用）.
     */
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        Channel channel = getOrCreateBrokerChannel(addr);
        if (channel == null) {
            throw new RemotingConnectException(addr);
        }
        return invokeAndTranslateException(channel, request, timeoutMillis);
    }

    private RemotingCommand invokeAndTranslateException(Channel channel, RemotingCommand request, long timeoutMillis)
            throws RemotingTimeoutException, RemotingSendRequestException, InterruptedException {
        try {
            return remotingClient.invokeSync(channel, request, timeoutMillis);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
            }
            if (e instanceof RemotingTimeoutException) {
                throw (RemotingTimeoutException) e;
            }
            if (e instanceof RemotingSendRequestException) {
                throw (RemotingSendRequestException) e;
            }
            throw new RemotingSendRequestException(
                    RemotingSendRequestException.newSendRequestException(channel.remoteAddress().toString(), e));
        }
    }

    /**
     * 获取到指定 Broker 的 Channel.
     */
    public Channel getBrokerChannel(String brokerAddr) {
        return brokerChannelTable.get("broker@" + brokerAddr);
    }

    public void closeBrokerChannel(String brokerAddr) {
        Channel ch = brokerChannelTable.remove("broker@" + brokerAddr);
        if (ch != null) {
            try {
                ch.close();
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * 清空所有路由缓存（如 Broker 全部下线时使用）.
     */
    public void clearRouteCache() {
        topicRouteTable.clear();
    }

    public NettyRemotingClient getRemotingClient() {
        return remotingClient;
    }

    public ConcurrentHashMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }
}
