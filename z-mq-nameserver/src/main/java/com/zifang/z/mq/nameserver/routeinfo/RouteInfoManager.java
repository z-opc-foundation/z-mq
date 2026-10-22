package com.zifang.z.mq.nameserver.routeinfo;

import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.QueueData;
import com.zifang.z.mq.common.RegisterBrokerResult;
import com.zifang.z.mq.common.TopicConfig;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.remoting.common.RemotingHelper;
import io.netty.channel.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 路由信息管理器
 * 负责管理Broker路由信息、Topic路由数据、Broker存活状态等
 * <p>
 * 核心数据结构：
 * 1. topicQueueTable - Topic队列信息（Topic -> List<QueueData>）
 * 2. brokerAddrTable - Broker地址信息（BrokerName -> BrokerData）
 * 3. clusterAddrTable - 集群信息（ClusterName -> Set<BrokerName>）
 * 4. brokerLiveTable - Broker存活信息（BrokerAddr -> BrokerLiveInfo）
 */
public class RouteInfoManager {

    private static final Logger log = LogManager.getLogger(RouteInfoManager.class);

    // Broker超时时间（毫秒）- 2分钟
    private static final long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    // 读写锁，用于并发控制
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Topic队列信息：Topic -> List<QueueData>
    private final HashMap<String, List<QueueData>> topicQueueTable = new HashMap<>();

    // Broker地址信息：BrokerName -> BrokerData
    private final HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();

    // 集群信息：ClusterName -> Set<BrokerName>
    private final HashMap<String, Set<String>> clusterAddrTable = new HashMap<>();

    // Broker存活信息：BrokerAddr -> BrokerLiveInfo
    private final ConcurrentHashMap<String, BrokerLiveInfo> brokerLiveTable = new ConcurrentHashMap<>();

    /**
     * 每个 broker <b>自己上报过</b>的 topic 名集合（BrokerName -> Set<Topic>）。
     * <p>
     * 用来区分两种"topic 不在 broker 的上报快照里"：
     * <ul>
     *   <li><b>这个 broker 上次上报过、这次不报了</b> ⇒ broker 上把 topic 删了 ⇒ 路由表里那条要删
     *       （不然控制台列出来的是一条 0 队列的幽灵 topic）；</li>
     *   <li><b>这个 broker 从来没上报过</b> ⇒ topic 是直接发给 NameServer 建的（本仓建 topic 的
     *       正式通路，见 README「快速开始 3」：`ZmqAdmin createTopic` 发 5 号命令字给 NameServer），
     *       broker 本来就不知道它 ⇒ <b>不能</b>因为"这次快照里没有"就把它删掉。</li>
     * </ul>
     * 第二个分支是本轮修的那处缺陷：broker 重启后 NameServer 把这类 topic 的路由删干净，
     * 客户端一律报 "No route for topic"（ZmqE2ETest#testBrokerRestartAndRecover 等 6 支 E2E 红）。
     */
    private final HashMap<String, Set<String>> brokerReportedTopics = new HashMap<>();

    /**
     * 扫描并移除不活跃的Broker
     */
    public void scanNotActiveBroker() {
        Iterator<Map.Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BrokerLiveInfo> entry = it.next();
            long lastUpdateTimestamp = entry.getValue().getLastUpdateTimestamp();
            long channelExpiredTime = entry.getValue().getChannelExpiredTime();

            if ((lastUpdateTimestamp + channelExpiredTime) < System.currentTimeMillis()) {
                // 关闭通道
                RemotingHelper.closeChannel(entry.getValue().getChannel());
                it.remove();
                log.warn("The broker channel expired, {} ,{}ms", entry.getKey(), channelExpiredTime);
                // 从路由表中删除Broker
                this.onChannelDestroy(entry.getKey(), entry.getValue().getChannel());
            }
        }
    }

    /**
     * Broker下线处理
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerNameFound = null;
        String brokerAddrFound = null;

        this.lock.writeLock().lock();
        try {
            // 查找对应的Broker
            Iterator<Map.Entry<String, BrokerLiveInfo>> brokerLiveTableIt = this.brokerLiveTable.entrySet().iterator();
            while (brokerLiveTableIt.hasNext()) {
                Map.Entry<String, BrokerLiveInfo> entry = brokerLiveTableIt.next();
                if (entry.getValue().getChannel() == channel) {
                    brokerAddrFound = entry.getKey();
                    break;
                }
            }

            if (brokerAddrFound != null) {
                // 从BrokerData中移除
                Iterator<Map.Entry<String, BrokerData>> brokerAddrTableIt = this.brokerAddrTable.entrySet().iterator();
                while (brokerAddrTableIt.hasNext()) {
                    BrokerData brokerData = brokerAddrTableIt.next().getValue();
                    brokerNameFound = brokerData.getBrokerName();
                    brokerData.getBrokerAddrs().values().remove(brokerAddrFound);

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        brokerAddrTableIt.remove();
                        // 从clusterAddrTable中移除
                        // 使用迭代器避免lambda中的非final变量问题
                        Iterator<Map.Entry<String, Set<String>>> clusterIt = this.clusterAddrTable.entrySet().iterator();
                        while (clusterIt.hasNext()) {
                            Set<String> brokerNames = clusterIt.next().getValue();
                            brokerNames.remove(brokerNameFound);
                            if (brokerNames.isEmpty()) {
                                clusterIt.remove();
                            }
                        }
                    }
                    break;
                }

                // 从brokerLiveTable中移除
                this.brokerLiveTable.remove(brokerAddrFound);

                // 【本轮改动】这里<b>不</b>再删 topicQueueTable 里属于这个 broker 的队列项。
                //
                // 为什么：topic 是【集群级事实】——本仓建 topic 的正式通路是给 NameServer 发 5 号命令字
                // （README「快速开始 3」的 `ZmqAdmin createTopic`），broker 从头到尾不知道这个 topic。
                // broker 掉线只意味着它的【地址】暂时没有，不意味着 topic 消失了。
                //
                // 删掉的实测后果（本轮修的就是它）：broker 一重启，NameServer 就把 topic 的队列项删干净，
                // 客户端重新查路由得到 "topic not exist" ⇒ 报 "No route for topic"，
                // 6 支 E2E 全红（ZmqE2ETest / FunctionalEdgeTest / StabilityTest / ChaosTest /
                // PushOffsetCommitE2ETest / RetryDeadLetterE2ETest）。
                // broker 重新注册后 brokerAddrTable 拿回地址，路由自动恢复。
                //
                // 代价（如实说明）：broker 永久下线时，它的 topic 仍留在路由表里，
                // 但那条队列项解析不到任何 broker 地址 ⇒ 客户端拿到的是"有队列、没有 broker"，
                // 报的仍是 "No route for topic"，不会误投到别处。
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        if (brokerNameFound != null) {
            log.info("onChannelDestroy: remove broker[{}][{}] from NameServer", brokerNameFound, brokerAddrFound);
        }
    }

    /**
     * 注册Broker
     */
    public RegisterBrokerResult registerBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId,
            final String haServerAddr,
            final Channel channel) {

        RegisterBrokerResult result = new RegisterBrokerResult();

        this.lock.writeLock().lock();
        try {
            // 更新集群信息
            Set<String> brokerNames = this.clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>());
            brokerNames.add(brokerName);

            // 更新Broker地址信息
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (brokerData == null) {
                brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                this.brokerAddrTable.put(brokerName, brokerData);
            }

            // 如果是主Broker，记录HA地址
            if (brokerId == 0) {
                result.setHaServerAddr(haServerAddr);
            }

            // 更新Broker地址
            String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
            result.setMasterAddr(oldAddr);

            // 更新Broker存活信息
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr,
                    new BrokerLiveInfo(System.currentTimeMillis(), BROKER_CHANNEL_EXPIRED_TIME, channel));

            if (prevBrokerLiveInfo != null) {
                log.warn("Broker[{}] Channel has been changed! OldChannel:{}, NewChannel:{}",
                        brokerAddr, prevBrokerLiveInfo.getChannel(), channel);
            }

        } finally {
            this.lock.writeLock().unlock();
        }

        log.info("registerBroker: clusterName={}, brokerAddr={}, brokerName={}, brokerId={}",
                clusterName, brokerAddr, brokerName, brokerId);

        return result;
    }

    /**
     * 注销Broker
     */
    public void unregisterBroker(
            final String clusterName,
            final String brokerAddr,
            final String brokerName,
            final long brokerId) {

        this.lock.writeLock().lock();
        try {
            // 从Broker存活表中移除
            BrokerLiveInfo removed = this.brokerLiveTable.remove(brokerAddr);
            if (removed == null) {
                log.warn("unregisterBroker: brokerAddr[{}] not found in brokerLiveTable", brokerAddr);
                return;
            }

            // 从Broker地址表中移除
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (brokerData != null) {
                String removedAddr = brokerData.getBrokerAddrs().remove(brokerId);
                if (brokerData.getBrokerAddrs().isEmpty()) {
                    this.brokerAddrTable.remove(brokerName);
                    // 从集群表中移除
                    Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                    if (brokerNames != null) {
                        brokerNames.remove(brokerName);
                        if (brokerNames.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                        }
                    }
                }
                log.info("unregisterBroker: remove broker[{}][{}] from brokerAddrTable", brokerName, removedAddr);
            }

        } finally {
            this.lock.writeLock().unlock();
        }

        log.info("unregisterBroker: clusterName={}, brokerAddr={}, brokerName={}, brokerId={}",
                clusterName, brokerAddr, brokerName, brokerId);
    }

    /**
     * 注册 / 覆盖某 Broker 的 Topic 配置。
     * <p>
     * 把 topic + readQueueNums / writeQueueNums / perm 注册到 topicQueueTable，
     * 之后 {@link #pickupTopicRouteData(String)} 就能看到。
     */
    public void registerTopic(final String brokerName, final TopicConfig topicConfig) {
        if (brokerName == null || topicConfig == null || topicConfig.getTopicName() == null) {
            return;
        }
        this.lock.writeLock().lock();
        try {
            QueueData qd = new QueueData();
            qd.setBrokerName(brokerName);
            qd.setReadQueueNums(topicConfig.getReadQueueNums());
            qd.setWriteQueueNums(topicConfig.getWriteQueueNums());
            qd.setPerm(topicConfig.getPerm());
            qd.setTopicSysFlag(0);
            List<QueueData> list = this.topicQueueTable.computeIfAbsent(topicConfig.getTopicName(),
                    k -> new ArrayList<>());
            // 替换同 broker 的旧 entry，避免重复
            list.removeIf(q -> brokerName.equals(q.getBrokerName()));
            list.add(qd);
            log.info("registerTopic: broker={} topic={} readQ={} writeQ={}",
                    brokerName, topicConfig.getTopicName(), qd.getReadQueueNums(), qd.getWriteQueueNums());
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 按 broker 上报的<b>全量快照</b>同步它名下的 Topic 队列项，注册时调用。
     * <p>
     * 快照是全量的，所以"这个 broker 以前上报过、这次不报了"的 topic 要从路由表里删掉：
     * 不删的话，broker 上把 topic 删了、路由表里还在，控制台列出来的是一条没有队列数的
     * 幽灵 topic，客户端也会一直往一个已经不存在的 topic 拉消息。
     * <p>
     * 但"这个 broker <b>从来没上报过</b>"的 topic <b>不许</b>删（{@link #brokerReportedTopics} 记的就是这个差别）：
     * 本仓建 topic 的正式通路是给 NameServer 发 5 号命令字（README「快速开始 3」），
     * broker 本来就不知道这类 topic；照旧"先整体清掉再重填"的话，broker 一重启，
     * 这类 topic 的路由就被清干净，客户端一律报 "No route for topic"。
     * <p>
     * 只动属于该 broker 的 QueueData，别家 broker 服务同一个 topic 时不受影响。
     *
     * @param brokerName     broker 名
     * @param topicConfigList 本次上报的 topic 配置全量快照（null / 空 = 这个 broker 这次没报任何 topic）
     */
    public void syncTopicsOfBroker(final String brokerName, final List<TopicConfig> topicConfigList) {
        if (brokerName == null) {
            return;
        }
        this.lock.writeLock().lock();
        try {
            Set<String> snapshot = new HashSet<>();
            if (topicConfigList != null) {
                for (TopicConfig tc : topicConfigList) {
                    if (tc != null && tc.getTopicName() != null) {
                        snapshot.add(tc.getTopicName());
                    }
                }
            }

            // 只删"上次上报过、这次不报了"的 topic
            Set<String> previous = this.brokerReportedTopics.get(brokerName);
            if (previous != null) {
                for (String removed : previous) {
                    if (!snapshot.contains(removed)) {
                        removeTopicOfBroker(removed, brokerName);
                    }
                }
            }
            this.brokerReportedTopics.put(brokerName, snapshot);

            if (topicConfigList != null) {
                for (TopicConfig tc : topicConfigList) {
                    registerTopic(brokerName, tc);
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 删掉某个 topic 属于某个 broker 的那一条队列项；该 topic 没有别的 broker 了就整个删掉。
     */
    private void removeTopicOfBroker(final String topic, final String brokerName) {
        List<QueueData> queues = this.topicQueueTable.get(topic);
        if (queues == null) {
            return;
        }
        queues.removeIf(q -> brokerName.equals(q.getBrokerName()));
        if (queues.isEmpty()) {
            this.topicQueueTable.remove(topic);
        }
    }

    /**
     * 获取 Topic 队列表 (Topic -> List&lt;QueueData&gt;) 的快照。
     */
    public Map<String, List<QueueData>> getTopicQueueTable() {
        this.lock.readLock().lock();
        try {
            return new HashMap<>(this.topicQueueTable);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * 获取 Broker 地址表 (BrokerName -> BrokerData) 的快照。
     */
    public Map<String, BrokerData> getBrokerAddrTable() {
        this.lock.readLock().lock();
        try {
            return new HashMap<>(this.brokerAddrTable);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * 获取 Cluster 表 (ClusterName -> Set&lt;BrokerName&gt;) 的快照。
     */
    public Map<String, Set<String>> getClusterAddrTable() {
        this.lock.readLock().lock();
        try {
            return new HashMap<>(this.clusterAddrTable);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * 获取Topic路由信息
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();

        this.lock.readLock().lock();
        try {
            // 获取队列信息
            List<QueueData> queueDataList = this.topicQueueTable.get(topic);
            if (queueDataList == null || queueDataList.isEmpty()) {
                return null;
            }
            topicRouteData.setQueueDatas(queueDataList);

            // 获取Broker信息
            Set<String> brokerNameSet = new HashSet<>();
            for (QueueData queueData : queueDataList) {
                brokerNameSet.add(queueData.getBrokerName());
            }

            List<BrokerData> brokerDataList = new ArrayList<>();
            for (String brokerName : brokerNameSet) {
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    brokerDataList.add(brokerData);
                }
            }
            topicRouteData.setBrokerDatas(brokerDataList);

        } finally {
            this.lock.readLock().unlock();
        }

        return topicRouteData;
    }

    /**
     * 获取所有集群信息
     */
    public Map<String, Set<String>> getClusterInfo() {
        this.lock.readLock().lock();
        try {
            return new HashMap<>(this.clusterAddrTable);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * 根据集群名称获取Broker名称列表
     */
    public Set<String> getBrokerNamesByCluster(String clusterName) {
        this.lock.readLock().lock();
        try {
            Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
            if (brokerNames != null) {
                return new HashSet<>(brokerNames);
            }
            return null;
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * 获取系统Topic列表
     */
    public Set<String> getSystemTopicList() {
        this.lock.readLock().lock();
        try {
            Set<String> topicList = new HashSet<>();
            // 添加所有Topic
            topicList.addAll(this.topicQueueTable.keySet());
            // 添加集群名称
            topicList.addAll(this.clusterAddrTable.keySet());
            return topicList;
        } finally {
            this.lock.readLock().unlock();
        }
    }
}

/**
 * Broker存活信息
 */
class BrokerLiveInfo {
    private long lastUpdateTimestamp;
    private long channelExpiredTime;
    private Channel channel;

    public BrokerLiveInfo(long lastUpdateTimestamp, long channelExpiredTime, Channel channel) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.channelExpiredTime = channelExpiredTime;
        this.channel = channel;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public long getChannelExpiredTime() {
        return channelExpiredTime;
    }

    public void setChannelExpiredTime(long channelExpiredTime) {
        this.channelExpiredTime = channelExpiredTime;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}
