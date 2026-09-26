package com.zifang.z.mq.integration;

import com.zifang.z.mq.broker.BrokerConfig;
import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.client.producer.DefaultMQProducer;
import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.QueueData;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.common.message.Message;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.nameserver.NameServerController;
import com.zifang.z.mq.nameserver.NamesrvConfig;
import com.zifang.z.mq.remoting.exception.RemotingConnectException;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.store.MessageStoreConfig;
import com.zifang.z.mq.store.log.FlushDiskType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 「换机器重试」这条边的真链路兑现：一台真 broker + 一份被掺进两个死地址的路由。
 * <p>
 * 单位是<b>存储侧的读数</b>：{@code broker.getCommitLog().getQueueIndex()} 的
 * {@code getMaxOffset(topic, queueId)}（下一位）与 {@code getSize(topic, queueId)}
 * （已索引条数）。重试这件事的代价是"同一条消息在存储里出现第二遍"，而第二遍只会出现在
 * 存储里 —— 客户端的 SendResult 说什么都没用，它一次成功就是一份成功。所以每一处断言写的
 * 都是这两个读数，两边一起断（位点涨了几格 &amp; 索引里有几条）。
 * <p>
 * 为什么要自己掺路由：本支要的是"第一趟必然撞上死地址"。真路由里只有一台 broker 时，
 * 换机器换不出第二台，那条边就只能靠"排除表用完了"那一支兑；掺两个死地址进去才能把
 * "失败 ⇒ 换一台 ⇒ 只写一份"这条链整个跑通，并且<b>是确定性的</b>（候选队列的次序就是
 * {@link TopicRouteData} 里的次序，轮询位从 0 起）。
 * <p>
 * <b>口径</b>：
 * <ul>
 *   <li>没有 sleep 阈值当判据。等异步一律等在被断言的那个读数本身上（存储侧位点），
 *       超时只给"这事根本没发生"一个可读的出路；</li>
 *   <li>端口一律现问；死地址用"问过之后立刻关掉"的空端口，connect 必拒；</li>
 *   <li>{@code storePathRootDir} 与 {@code storePathCommitLog} <b>都</b>显式设进同一个
 *       {@code @TempDir}（只设一个会让 commitlog 逃逸到 {@code ~/store}）。</li>
 * </ul>
 */
public class ProducerRetrySingleStoreE2ETest {

    /** 只用于给"这件事根本没发生"一个可读的出路，不承载任何数值判定. */
    private static final long BAIL_OUT_MILLIS = 60_000L;

    private static final String TOPIC = "ProducerRetryFailoverTopic";
    private static final String PRODUCER_GROUP = "w2g-retry-producer-group";
    private static final String LIVE_BROKER = "LiveBroker";
    private static final String DEAD_BROKER_A = "DeadBrokerA";
    private static final String DEAD_BROKER_B = "DeadBrokerB";
    private static final int QUEUE_ID = 0;

    @TempDir
    Path tempDir;

    private NameServerController nameServer;
    private String namesrvAddr;
    private BrokerController broker;
    private int brokerPort;
    private MQClientInstance clientInstance;
    private TestingProducer producer;
    private final List<Integer> deadPorts = new ArrayList<Integer>();

    @AfterEach
    public void tearDown() {
        if (producer != null) {
            try {
                producer.shutdown();
            } catch (Exception ignore) {
            }
        }
        if (clientInstance != null) {
            try {
                clientInstance.shutdown();
            } catch (Exception ignore) {
            }
        }
        if (broker != null) {
            try {
                broker.shutdown();
            } catch (Exception ignore) {
            }
        }
        if (nameServer != null) {
            try {
                nameServer.shutdown();
            } catch (Exception ignore) {
            }
        }
    }

    // ==================== 1. 死地址 ⇒ 换一台 ⇒ 只写一份 ====================

    @Test
    @DisplayName("真链路: 第一趟撞上注入的死地址, 换到活的那台之后存储里恰好一份, 且位点/索引两边都对得上")
    public void failOverFromADeadAddressWritesExactlyOneCopyIntoTheStore() throws Exception {
        startCluster();
        installDoctoredProducer(routeWithDeadHopsFirst(LIVE_BROKER));

        assertEquals(0L, storedMaxOffset(), "开局存储里必须是空的");
        assertEquals(0, storedSize());

        SendResult result = producer.send(message("w2g-failover-1"));

        assertSame(SendStatus.SEND_OK, result.getSendStatus(),
                "换到活的那台之后这一趟就该成功: " + result);
        assertEquals(0L, result.getQueueOffset(), "活的那台上这也是第一条: " + result);

        awaitStoredCount("第一条消息进存储", 1L);
        // ★ 判据：存储侧两份读数必须都恰好是 1 —— 重试把同一条写两遍就在这里露出来
        assertEquals(1L, storedMaxOffset(),
                "★ 换机重试之后存储里必须恰好一份，位点涨过 1 格就是写了第二遍");
        assertEquals(1, storedSize(), "★ 索引里的条数也必须是一份");
        assertEquals(1L, producer.getSendRetryCount(),
                "这一条链路上恰好换了一台（第一趟死在 DeadBrokerA）");
        assertNotNull(result.getMsgId(), "msgId 必须由存储侧给出，不能是 client 造的");
    }

    @Test
    @DisplayName("真链路: 连发三条, 每一条都只让存储的位点涨一格")
    public void threeSendsOverTheSameDeadFirstHopLeaveExactlyThreeCopies() throws Exception {
        startCluster();
        installDoctoredProducer(routeWithDeadHopsFirst(LIVE_BROKER));

        long[] offsets = new long[3];
        for (int i = 0; i < 3; i++) {
            SendResult result = producer.send(message("w2g-failover-" + i));
            assertSame(SendStatus.SEND_OK, result.getSendStatus(), "第 " + i + " 条: " + result);
            offsets[i] = result.getQueueOffset();
            awaitStoredCount("第 " + (i + 1) + " 条消息进存储", i + 1L);
            assertEquals(i + 1L, storedMaxOffset(),
                    "★ 每条只许让位点涨一格，涨两格就是这条被写了第二遍: 第 " + i + " 条");
            assertEquals(i + 1, storedSize(), "第 " + i + " 条之后索引里的条数");
        }
        assertEquals(0L, offsets[0]);
        assertEquals(1L, offsets[1]);
        assertEquals(2L, offsets[2]);
        // 轮询位在三个实例级队列里继续走，所以三条加起来换了几台是确定的（见首条那一路的推导）
        assertEquals(3L, producer.getSendRetryCount(),
                "三条的重试总账必须与「每一条只涨一格」对得上：计数不是口号");
    }

    // ==================== 2. 反手一把：整份路由都是死的 ====================

    @Test
    @DisplayName("真链路: 路由里全是死地址时, 试满预算把异常交回调用方, 且存储里一份都没有")
    public void anAllDeadRouteExhaustsTheBudgetWithoutWritingAnything() throws Exception {
        startCluster();
        installDoctoredProducer(routeWithDeadHopsFirst(null));

        assertThrows(RemotingConnectException.class, () -> producer.send(message("w2g-all-dead")),
                "换无可换时要把真实的失败交回去，不许就地造一个 SEND_OK");

        assertEquals(3L, producer.getSendRetryCount(),
                "默认 3 次重试的口径：首发 + 3 趟，全都死在连不上");
        // ★ 反手一把的另外半边：一条都没写进去
        assertEquals(0L, storedMaxOffset(), "全是死地址 ⇒ 存储里不该出现任何一份");
        assertEquals(0, storedSize());
        assertEquals(0, broker.getCommitLog().getQueueIndex().getQueueCount(),
                "★ 活的那台连一个队列索引都没被建起来 ⇒ 请求根本没到过它手上");
    }

    // ==================== 集群与路由 ====================

    private void startCluster() throws Exception {
        int nsPort = freePort();
        NamesrvConfig nsCfg = new NamesrvConfig();
        nsCfg.setKvConfigPath(tempDir.resolve("kvConfig.json").toString());
        NettyServerConfig nsNetty = new NettyServerConfig();
        nsNetty.setListenPort(nsPort);
        nameServer = new NameServerController(nsCfg, nsNetty);
        assertTrue(nameServer.initialize(), "NameServer.initialize() 必须成功");
        nameServer.start();
        namesrvAddr = "127.0.0.1:" + nsPort;

        String storeRoot = tempDir.resolve("broker").toString();
        MessageStoreConfig msc = new MessageStoreConfig();
        msc.setStorePathRootDir(storeRoot);
        msc.setStorePathCommitLog(storeRoot + java.io.File.separator + "commitlog");
        msc.setMappedFileSizeCommitLog(1024 * 1024);
        msc.setFlushDiskType(FlushDiskType.SYNC_FLUSH);

        BrokerConfig bc = new BrokerConfig();
        bc.setBrokerName(LIVE_BROKER);
        bc.setBrokerClusterName("W2gRetryCluster");
        bc.setNamesrvAddr(namesrvAddr);

        brokerPort = freePort();
        NettyServerConfig nsc = new NettyServerConfig();
        nsc.setListenPort(brokerPort);
        broker = new BrokerController(bc, msc, nsc);
        assertTrue(broker.initialize(), "BrokerController.initialize() 必须成功");
        broker.start();
    }

    /**
     * 掺好的路由：{@code DeadBrokerA} 排第一（第一趟必然死在它上面），{@code DeadBrokerB} 第二，
     * 活的那台排第三。{@code liveLast} 传 null 时整份路由全是死地址。
     */
    private TopicRouteData routeWithDeadHopsFirst(String liveBrokerName) throws Exception {
        TopicRouteData route = new TopicRouteData();
        List<QueueData> queueDatas = new ArrayList<QueueData>();
        List<BrokerData> brokerDatas = new ArrayList<BrokerData>();

        queueDatas.add(queueData(DEAD_BROKER_A));
        brokerDatas.add(deadBrokerData(DEAD_BROKER_A));
        queueDatas.add(queueData(DEAD_BROKER_B));
        brokerDatas.add(deadBrokerData(DEAD_BROKER_B));
        if (liveBrokerName != null) {
            queueDatas.add(queueData(liveBrokerName));
            java.util.HashMap<Long, String> addrs = new java.util.HashMap<Long, String>();
            addrs.put(BrokerData.MASTER_ID, "127.0.0.1:" + brokerPort);
            brokerDatas.add(new BrokerData("W2gRetryCluster", liveBrokerName, addrs));
        }
        route.setQueueDatas(queueDatas);
        route.setBrokerDatas(brokerDatas);
        return route;
    }

    private static QueueData queueData(String brokerName) {
        QueueData qd = new QueueData();
        qd.setBrokerName(brokerName);
        qd.setReadQueueNums(1);
        qd.setWriteQueueNums(1);
        qd.setPerm(6);
        return qd;
    }

    /** 一个"问过就关掉"的空端口：connect 一定被拒，不必等超时. */
    private BrokerData deadBrokerData(String brokerName) throws Exception {
        int port = freePort();
        deadPorts.add(Integer.valueOf(port));
        java.util.HashMap<Long, String> addrs = new java.util.HashMap<Long, String>();
        addrs.put(BrokerData.MASTER_ID, "127.0.0.1:" + port);
        return new BrokerData("W2gRetryCluster", brokerName, addrs);
    }

    private void installDoctoredProducer(final TopicRouteData doctored) throws Exception {
        clientInstance = new MQClientInstance("w2g-retry-e2e", namesrvAddr, new NettyClientConfig()) {
            @Override
            public TopicRouteData getTopicRouteData(String topic) {
                return TOPIC.equals(topic) ? doctored : super.getTopicRouteData(topic);
            }

            @Override
            public TopicRouteData refreshTopicRouteData(String topic) {
                return TOPIC.equals(topic) ? doctored : super.refreshTopicRouteData(topic);
            }
        };
        clientInstance.start();

        producer = new TestingProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setClientId("w2g-retry-e2e");
        producer.start();
        producer.install(clientInstance);
    }

    // ==================== 读数 ====================

    private long storedMaxOffset() {
        return broker.getCommitLog().getQueueIndex().getMaxOffset(TOPIC, QUEUE_ID);
    }

    private int storedSize() {
        return broker.getCommitLog().getQueueIndex().getSize(TOPIC, QUEUE_ID);
    }

    /** 等在被断言的那个读数本身上：位点涨到 wanted 为止，涨不到就 fail 出可读的出路. */
    private void awaitStoredCount(String what, final long wanted) throws Exception {
        long deadline = System.currentTimeMillis() + BAIL_OUT_MILLIS;
        while (storedMaxOffset() < wanted) {
            if (System.currentTimeMillis() >= deadline) {
                fail("等不到 " + what + "：期望位点至少 " + wanted + "，实际 maxOffset="
                        + storedMaxOffset() + " size=" + storedSize());
            }
            Thread.sleep(20L);
        }
    }

    private static Message message(String key) {
        Message message = new Message();
        message.setTopic(TOPIC);
        message.setKeys(key);
        message.setBody(key.getBytes(StandardCharsets.UTF_8));
        return message;
    }

    private static int freePort() throws Exception {
        ServerSocket probe = new ServerSocket(0);
        try {
            return probe.getLocalPort();
        } finally {
            probe.close();
        }
    }

    /** 只有一个用途：把 {@code mqClientInstance} 换成掺过路由的那一个（该字段是 protected）. */
    private static final class TestingProducer extends DefaultMQProducer {

        TestingProducer(String group) {
            super(group);
        }

        void install(MQClientInstance instance) {
            this.mqClientInstance = instance;
        }
    }
}
