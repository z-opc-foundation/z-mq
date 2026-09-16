package com.zifang.z.mq.broker;

import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.store.MessageStoreConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Broker 进程入口（对标 RocketMQ BrokerStartup）.
 * <p>
 * 启动方式：
 * <pre>
 *   java -cp z-mq-broker-*.jar com.zifang.z.mq.broker.BrokerStartup [brokerPort] [namesrvAddr] [storePath]
 * </pre>
 */
public class BrokerStartup {

    private static final Logger log = LogManager.getLogger(BrokerStartup.class);

    public static void main(String[] args) throws Exception {
        BrokerConfig brokerConfig = new BrokerConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        if (args.length >= 1) {
            nettyServerConfig.setListenPort(Integer.parseInt(args[0]));
        } else {
            nettyServerConfig.setListenPort(10911);
        }
        if (args.length >= 2) {
            brokerConfig.setNamesrvAddr(args[1]);
        }
        if (args.length >= 3) {
            messageStoreConfig.setStorePathRootDir(args[2]);
        }

        log.info("Starting Broker, brokerPort={}, namesrvAddr={}, storePath={}",
                nettyServerConfig.getListenPort(),
                brokerConfig.getNamesrvAddr(),
                messageStoreConfig.getStorePathRootDir());

        BrokerController controller = new BrokerController(brokerConfig, messageStoreConfig, nettyServerConfig);
        boolean ok = controller.initialize();
        if (!ok) {
            log.error("Broker initialize failed, exit");
            System.exit(1);
        }
        controller.start();

        Runtime.getRuntime().addShutdownHook(new Thread(controller::shutdown, "BrokerShutdownHook"));

        log.info("Broker started.");
        Thread.currentThread().join();
    }
}