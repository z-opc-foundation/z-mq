package com.zifang.z.mq.spring.host.support;

import com.zifang.z.mq.broker.BrokerConfig;
import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.nameserver.NameServerController;
import com.zifang.z.mq.nameserver.NamesrvConfig;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.MessageStoreConfig;

import java.io.File;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;

/**
 * 测试夹具：在**本 JVM 内**起一套 z-mq 集群（NameServer + Broker），端口随机挑空闲端口。
 *
 * <p><b>为什么不复用共享 broker</b>（TASK-20260925-024 的硬约束）：
 * 共享 broker 上有别的会话在跑，写操作（建 topic、挂消费者组）会污染它们；
 * 所以这套夹具自带 namesrv + broker，端口都是临时挑的空闲端口，测完即销毁。
 *
 * <p><b>取数走真实私有二进制协议</b>（不是直接读对象）：{@link #topicListJson()} 等方法
 * 发的是 nameserver / broker 的只读请求码，拿到的是上游原样回传的 JSON body ——
 * 与 {@code MqProxyController} 的 {@code /api/mq/**} 只读端点读的是同一批请求码，
 * 所以这里量到的就是「broker 侧 topicList / 消费者组」的真实读数。
 */
public class EmbeddedZmqCluster implements AutoCloseable {

    private final int namesrvPort;
    private final int brokerPort;
    private final String storeRoot;

    private NameServerController nameServer;
    private BrokerController broker;

    private EmbeddedZmqCluster(int namesrvPort, int brokerPort, String storeRoot) {
        this.namesrvPort = namesrvPort;
        this.brokerPort = brokerPort;
        this.storeRoot = storeRoot;
    }

    /** 起一套本 JVM 的 namesrv + broker，等 broker 在 namesrv 上注册完成。 */
    public static EmbeddedZmqCluster start() throws Exception {
        int namesrvPort = freePort();
        int brokerPort = freePort();
        String storeRoot = System.getProperty("java.io.tmpdir") + File.separator
                + "zmq-starter-test-" + System.nanoTime();
        EmbeddedZmqCluster cluster = new EmbeddedZmqCluster(namesrvPort, brokerPort, storeRoot);
        cluster.boot();
        return cluster;
    }

    private void boot() throws Exception {
        new File(storeRoot).mkdirs();

        // 1) NameServer
        NamesrvConfig nsConfig = new NamesrvConfig();
        nsConfig.setKvConfigPath(storeRoot + File.separator + "kvConfig.json");
        NettyServerConfig nsNetty = new NettyServerConfig();
        nsNetty.setListenPort(namesrvPort);
        nameServer = new NameServerController(nsConfig, nsNetty);
        nameServer.initialize();
        nameServer.start();

        // 2) Broker
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr("127.0.0.1:" + namesrvPort);
        brokerConfig.setBrokerName("StarterTestBroker");
        brokerConfig.setBrokerClusterName("StarterTestCluster");
        brokerConfig.setBrokerIp1("127.0.0.1");
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(storeRoot + File.separator + "broker");
        storeConfig.setStorePathCommitLog(storeRoot + File.separator + "broker" + File.separator + "commitlog");
        storeConfig.setMappedFileSizeCommitLog(1024 * 1024);
        NettyServerConfig brokerNetty = new NettyServerConfig();
        brokerNetty.setListenPort(brokerPort);
        broker = new BrokerController(brokerConfig, storeConfig, brokerNetty);
        broker.initialize();
        broker.start();

        waitForBrokerRegistration(10000);
    }

    private void waitForBrokerRegistration(long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (nameServer.getRouteInfoManager().getBrokerAddrTable().containsKey("StarterTestBroker")) {
                    return;
                }
            } catch (Exception ignore) {
                // 路由表还没建好，继续等
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("broker 没能在 " + timeoutMillis + "ms 内注册到 namesrv");
    }

    public String namesrvAddr() {
        return "127.0.0.1:" + namesrvPort;
    }

    public String brokerAddr() {
        return "127.0.0.1:" + brokerPort;
    }

    public int namesrvPort() {
        return namesrvPort;
    }

    public int brokerPort() {
        return brokerPort;
    }

    /** nameserver 的 GET_ALL_TOPIC_LIST(8) 原始 body（topic 名清单）。 */
    public String topicListJson() throws Exception {
        return namesrvRaw(RequestCode.GET_ALL_TOPIC_LIST);
    }

    /** nameserver 的 GET_ROUTEINFO_BY_TOPIC(3) 原始 body。 */
    public String routeJson(String topic) throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
        req.addExtField("topic", topic);
        return invoke(namesrvAddr(), req);
    }

    /** broker 的 GET_ALL_TOPIC_CONFIG(320) 原始 body（broker 自己知道的 topic 表）。 */
    public String brokerTopicConfigJson() throws Exception {
        return brokerRaw(RequestCode.GET_ALL_TOPIC_CONFIG);
    }

    /** broker 的 GET_ALL_CONSUMER_OFFSET(321) 原始 body（消费者组位点表）。 */
    public String brokerConsumerOffsetJson() throws Exception {
        return brokerRaw(RequestCode.GET_ALL_CONSUMER_OFFSET);
    }

    private String namesrvRaw(int code) throws Exception {
        return invoke(namesrvAddr(), RemotingCommand.createRequestCommand(code));
    }

    private String brokerRaw(int code) throws Exception {
        return invoke(brokerAddr(), RemotingCommand.createRequestCommand(code));
    }

    private String invoke(String addr, RemotingCommand request) throws Exception {
        MQClientInstance client = new MQClientInstance(
                "z-mq-starter-test-admin-" + System.nanoTime(), namesrvAddr(), new NettyClientConfig());
        try {
            client.start();
            RemotingCommand response = client.invokeSync(addr, request, 5000);
            byte[] body = response.getBody();
            return "responseCode=" + response.getCode()
                    + " remark=" + response.getRemark()
                    + " body=" + (body == null ? "<null>" : new String(body, StandardCharsets.UTF_8));
        } finally {
            try {
                client.shutdown();
            } catch (Exception ignore) {
                // 短命探针客户端，关不掉也不该盖掉真正的读数
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @Override
    public void close() {
        if (broker != null) {
            try {
                broker.shutdown();
            } catch (Exception ignore) {
                // 清理失败不影响断言结果
            }
        }
        if (nameServer != null) {
            try {
                nameServer.shutdown();
            } catch (Exception ignore) {
                // 同上
            }
        }
        deleteDir(new File(storeRoot));
    }

    private void deleteDir(File dir) {
        if (dir == null || !dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                deleteDir(f);
            }
        }
        dir.delete();
    }
}
