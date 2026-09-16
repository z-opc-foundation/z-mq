package com.zifang.z.mq.tools.admin;

import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.TopicConfig;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.util.JsonCodec;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群管理 RPC 客户端（对标 mqadmin ClusterAdmin）.
 * <p>
 * 通过 Netty RPC 与 NameServer / Broker 通信，执行运维操作:
 * <ul>
 *   <li>listTopics — 查询所有 Topic</li>
 *   <li>topicRoute — 查询 Topic 路由</li>
 *   <li>listBrokers — 查询所有 Broker</li>
 *   <li>topicConfig — 查询 Broker 上的 Topic 配置</li>
 *   <li>createTopic — 创建 Topic</li>
 * </ul>
 */
public class ClusterAdmin {

    private static final Logger log = LogManager.getLogger(ClusterAdmin.class);

    private final String namesrvAddr;
    private NettyRemotingClient namesrvClient;

    public ClusterAdmin(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void start() {
        this.namesrvClient = new NettyRemotingClient(new NettyClientConfig());
        this.namesrvClient.start();
    }

    public void shutdown() {
        if (namesrvClient != null) {
            namesrvClient.shutdown();
        }
    }

    // ===== NameServer 操作 =====

    /**
     * 查询所有 Topic 列表.
     */
    public List<String> listTopics() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST);
        RemotingCommand response = invokeNamesrv(request);
        if (response == null || response.getCode() != RemotingSysResponseCode.SUCCESS) {
            return Collections.emptyList();
        }
        if (response.getBody() == null || response.getBody().length == 0) {
            return Collections.emptyList();
        }
        try {
            TopicListResult result = JsonCodec.decode(response.getBody(), TopicListResult.class);
            return result != null && result.getTopics() != null ? result.getTopics() : Collections.emptyList();
        } catch (Exception e) {
            log.warn("listTopics decode failed", e);
            return Collections.emptyList();
        }
    }

    /**
     * 查询 Topic 路由.
     */
    public TopicRouteData topicRoute(String topic) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
        request.addExtField("topic", topic);
        RemotingCommand response = invokeNamesrv(request);
        if (response == null || response.getCode() != RemotingSysResponseCode.SUCCESS) {
            return null;
        }
        if (response.getBody() == null || response.getBody().length == 0) {
            return null;
        }
        try {
            return JsonCodec.decode(response.getBody(), TopicRouteData.class);
        } catch (Exception e) {
            log.warn("topicRoute decode failed: {}", topic, e);
            return null;
        }
    }

    /**
     * 查询所有 Broker 集群信息.
     */
    public Map<String, List<BrokerData>> listBrokers() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO);
        RemotingCommand response = invokeNamesrv(request);
        if (response == null || response.getCode() != RemotingSysResponseCode.SUCCESS) {
            return Collections.emptyMap();
        }
        if (response.getBody() == null || response.getBody().length == 0) {
            return Collections.emptyMap();
        }
        try {
            // NameServer 返回的 clusterInfo 为 Map<clusterName, List<BrokerData>>
            // fastjson2 反序列化时 BrokerData 已经是正确的类型
            @SuppressWarnings("unchecked")
            Map<String, List<BrokerData>> result =
                    JsonCodec.decode(response.getBody(), Map.class);
            return result != null ? result : Collections.emptyMap();
        } catch (Exception e) {
            log.warn("listBrokers decode failed", e);
            return Collections.emptyMap();
        }
    }

    /**
     * 创建 Topic (通过 Broker 端口).
     */
    public boolean createTopic(String brokerAddr, String topic, int readQueueNums, int writeQueueNums) {
        NettyRemotingClient brokerClient = new NettyRemotingClient(new NettyClientConfig());
        brokerClient.start();
        try {
            Channel ch = brokerClient.getOrCreateChannel(brokerAddr);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC);
            request.addExtField("topic", topic);
            request.addExtField("readQueueNums", String.valueOf(readQueueNums));
            request.addExtField("writeQueueNums", String.valueOf(writeQueueNums));
            RemotingCommand response = brokerClient.invokeSync(ch, request, 5000);
            return response != null && response.getCode() == RemotingSysResponseCode.SUCCESS;
        } catch (Exception e) {
            log.error("createTopic failed: topic={} broker={}", topic, brokerAddr, e);
            return false;
        } finally {
            brokerClient.shutdown();
        }
    }

    // ===== 内部方法 =====

    private RemotingCommand invokeNamesrv(RemotingCommand request) {
        if (namesrvClient == null) {
            log.warn("namesrvClient not started");
            return null;
        }
        try {
            String first = namesrvAddr.split(";")[0].trim();
            Channel ch = namesrvClient.getOrCreateChannel(first);
            if (ch == null) {
                log.warn("cannot connect to namesrv: {}", first);
                return null;
            }
            return namesrvClient.invokeSync(ch, request, 5000);
        } catch (Exception e) {
            log.error("invokeNamesrv failed: code={}", request.getCode(), e);
            return null;
        }
    }

    /** Topic 列表结果 (用于 JSON 反序列化). */
    public static class TopicListResult {
        private List<String> topics;

        public List<String> getTopics() {
            return topics;
        }

        public void setTopics(List<String> topics) {
            this.topics = topics;
        }
    }
}
