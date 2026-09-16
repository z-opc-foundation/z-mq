package com.zifang.z.mq.broker;

import com.zifang.z.mq.broker.delay.ScheduleMessageService;
import com.zifang.z.mq.broker.longpoll.PullRequestHoldService;
import com.zifang.z.mq.broker.processor.AdminBrokerProcessor;
import com.zifang.z.mq.broker.processor.PullMessageProcessor;
import com.zifang.z.mq.broker.processor.SendMessageProcessor;
import com.zifang.z.mq.common.RegisterBrokerResult;
import com.zifang.z.mq.common.TopicConfig;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyRemotingClient;
import com.zifang.z.mq.remoting.netty.NettyRemotingServer;
import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.MessageStoreConfig;
import com.zifang.z.mq.store.config.ConsumerOffsetManager;
import com.zifang.z.mq.store.log.CommitLog;
import io.netty.channel.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Broker 控制器.
 * <p>
 * API 基础路径: 无 (非 REST, 由 Netty 远程服务端承载客户端请求)
 * 所属模块: z-mq-broker
 * 鉴权: 由 z-mq 集群内部协议保障
 *
 * <p>Broker 是消息存储和转发的核心节点, 负责:
 * <ol>
 *   <li>消息存储 — 将消息持久化到 CommitLog</li>
 *   <li>消息转发 — 向 Consumer 推送消息</li>
 *   <li>消息查询 — 根据条件查询消息</li>
 *   <li>心跳维护 — 向 NameServer 注册并维持心跳</li>
 * </ol>
 */
public class BrokerController {

    private static final Logger log = LogManager.getLogger(BrokerController.class);

    // Broker配置
    private final BrokerConfig brokerConfig;

    // 消息存储配置
    private final MessageStoreConfig messageStoreConfig;

    // Netty服务端配置
    private final NettyServerConfig nettyServerConfig;

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Netty服务端（处理客户端请求）
    private NettyRemotingServer remotingServer;

    // 定时任务调度器
    private ScheduledExecutorService scheduledExecutorService;

    // 业务线程池
    private ExecutorService sendMessageExecutor;
    private ExecutorService pullMessageExecutor;
    private ExecutorService adminBrokerExecutor;

    // 注册到 NameServer 的客户端 (独立 NettyRemotingClient)
    private NettyRemotingClient namesrvRemotingClient;
    private long brokerHeartbeatIntervalMillis = 5000;

    // CommitLog
    private CommitLog commitLog;

    /** 管理命令处理器（CASE_ADMIN_TOPIC 缓存与创建）. */
    private AdminBrokerProcessor adminBrokerProcessor;

    private SendMessageProcessor sendMessageProcessor;
    private PullMessageProcessor pullMessageProcessor;

    /**
     * v2 增强组件:
     * <ul>
     *   <li>consumerOffsetManager: 消费位点持久化, 跨重启恢复</li>
     *   <li>pullRequestHoldService: Pull 长轮询, 新消息毫秒级响应</li>
     *   <li>scheduleMessageService: 18 级内置延迟消息</li>
     * </ul>
     */
    private ConsumerOffsetManager consumerOffsetManager;
    private PullRequestHoldService pullRequestHoldService;
    private ScheduleMessageService scheduleMessageService;

    /** v3 分布式组件: Broker OutAPI (Broker 互查接口). */
    private com.zifang.z.mq.broker.outapi.BrokerOutAPI brokerOutAPI;

    /** v3 分布式组件: SlaveSynchronize (仅 Slave 启动). */
    private com.zifang.z.mq.broker.slave.SlaveSynchronize slaveSynchronize;

    /** v3 HA: 主从同步服务 (HAService 接口). */
    private com.zifang.z.mq.store.ha.HAService haService;

    /**
     * ConsumerOffset / DelayOffset 全局版本号 (每次 commit 自增).
     * 由 BrokerController 持有以便 BrokerOutAPI 暴露给 Slave.
     */
    private final com.zifang.z.mq.common.ha.DataVersion consumerOffsetDataVersion = new com.zifang.z.mq.common.ha.DataVersion();
    private final com.zifang.z.mq.common.ha.DataVersion delayOffsetDataVersion = new com.zifang.z.mq.common.ha.DataVersion();

    /**
     * 构造一个 Broker 控制器, 仅注入配置 (不会立即初始化/启动).
     *
     * @param brokerConfig       Broker 自身配置
     * @param messageStoreConfig 消息存储配置
     * @param nettyServerConfig  Netty 服务端配置
     */
    public BrokerController(
            BrokerConfig brokerConfig,
            MessageStoreConfig messageStoreConfig,
            NettyServerConfig nettyServerConfig) {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.nettyServerConfig = nettyServerConfig;
    }

    /**
     * 初始化 Broker: 加载 CommitLog, 创建 Netty 服务端, 注册处理器, 初始化定时任务与业务线程池.
     *
     * @return 初始化成功返回 true; 加载 CommitLog 失败或抛异常时返回 false
     */
    public boolean initialize() {
        try {
            // 加载CommitLog
            this.commitLog = new CommitLog(this.messageStoreConfig);
            if (!this.commitLog.load()) {
                log.error("load commitlog failed");
                return false;
            }

            // 初始化 v2 增强组件
            this.consumerOffsetManager = new ConsumerOffsetManager(this.messageStoreConfig.getStorePathRootDir());
            this.consumerOffsetManager.start();

            this.pullRequestHoldService = new PullRequestHoldService();
            // pullRequestHoldService 在 start() 时才启动 (后续)

            this.scheduleMessageService = new ScheduleMessageService();
            // scheduleMessageService 在 start() 时才启动 (后续)

            // v3 HA: 创建 HAService (Master / Slave 通用, 内部根据 brokerConfig.isMaster() 切换)
            this.haService = new com.zifang.z.mq.broker.ha.DefaultHAService(this);
            // 把 HA 回调注入 CommitLog, putMessage 成功后通知 HA 服务
            this.commitLog.setHaCallback((offset, body) -> {
                if (this.haService != null) {
                    this.haService.notifyMessageArrived(offset, body);
                }
            });

            // 初始化Netty服务端
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);

            // 初始化定时任务 (先于 processor, 用于心跳等场景)
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "BrokerScheduledThread");
                t.setDaemon(true);
                return t;
            });

            // 初始化业务线程池 (必须先于 registerProcessor, 否则处理器绑定到 null 线程池)
            this.sendMessageExecutor = Executors.newFixedThreadPool(
                    this.brokerConfig.getSendMessageThreadPoolNums(),
                    r -> new Thread(r, "SendMessageThread_"));

            this.pullMessageExecutor = Executors.newFixedThreadPool(
                    this.brokerConfig.getPullMessageThreadPoolNums(),
                    r -> new Thread(r, "PullMessageThread_"));

            this.adminBrokerExecutor = Executors.newFixedThreadPool(
                    this.brokerConfig.getAdminBrokerThreadPoolNums(),
                    r -> new Thread(r, "AdminBrokerThread_"));

            // 注册处理器 (在线程池就绪后)
            this.registerProcessor();

            log.info("Broker initialize successfully");
            return true;
        } catch (Exception e) {
            log.error("Broker initialize failed", e);
            return false;
        }
    }

    /**
     * 向 Netty 远程服务端注册业务处理器 (发送消息 / 拉取消息 / 管理命令 / BrokerOutAPI).
     * <p>
     * 把四个处理器绑定到相应的 NettyRequestCode 上, 并各自使用独立的业务线程池
     * 隔离 IO 与业务执行, 避免慢调用拖垮 IO 线程.
     */
    private void registerProcessor() {
        // 发送消息处理器
        this.sendMessageProcessor = new SendMessageProcessor(this);
        this.remotingServer.registerProcessor(
                RequestCode.SEND_MESSAGE, this.sendMessageProcessor, this.sendMessageExecutor);

        // 拉取消息处理器
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.remotingServer.registerProcessor(
                RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);

        // 管理命令处理器（创建/查询 Topic）
        this.adminBrokerProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerProcessor(
                RequestCode.UPDATE_AND_CREATE_TOPIC, this.adminBrokerProcessor, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(
                RequestCode.GET_ALL_TOPIC_LIST, this.adminBrokerProcessor, this.adminBrokerExecutor);

        // Broker OutAPI (Broker 互查接口, SlaveSynchronize 通过它拉取元数据)
        this.brokerOutAPI = new com.zifang.z.mq.broker.outapi.BrokerOutAPI(this);
        this.remotingServer.registerProcessor(
                RequestCode.GET_ALL_TOPIC_CONFIG, this.brokerOutAPI, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(
                RequestCode.GET_ALL_CONSUMER_OFFSET, this.brokerOutAPI, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(
                RequestCode.GET_ALL_DELAY_OFFSET, this.brokerOutAPI, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(
                RequestCode.GET_ALL_SUBSCRIPTION_GROUP, this.brokerOutAPI, this.adminBrokerExecutor);
        this.remotingServer.registerProcessor(
                RequestCode.QUERY_DATA_VERSION, this.brokerOutAPI, this.adminBrokerExecutor);

        // v3 HA: HA Processor (处理 Slave 上报 offset 等)
        com.zifang.z.mq.broker.ha.HAProcessor haProcessor = new com.zifang.z.mq.broker.ha.HAProcessor(this);
        this.remotingServer.registerProcessor(
                RequestCode.HA_REPORT_OFFSET, haProcessor, this.adminBrokerExecutor);
    }

    /**
     * 启动 Broker: 幂等启动, 实际启动动作仅在首次调用时执行.
     * <p>
     * 流程: 启动 CommitLog → 启动 Netty → 若配置了 NameServer 地址则启动注册客户端并注册.
     *
     * @throws Exception 启动过程中发生的异常
     */
    public void start() throws Exception {
        if (this.running.compareAndSet(false, true)) {
            // 启动CommitLog
            this.commitLog.start();

            // 启动 v2 增强组件
            this.pullRequestHoldService.start();
            this.scheduleMessageService.start();

            // v3 HA: 启动 HAService (Master 模式启动 push 线程, Slave 模式启动 sync 线程)
            this.haService.start();

            // 启动Netty服务端
            this.remotingServer.start();

            // 注册Broker到NameServer（如果配置了NameServer地址）
            if (this.brokerConfig.getNamesrvAddr() != null && !this.brokerConfig.getNamesrvAddr().isEmpty()) {
                this.startRegisterToNameServer();
            }

            // 启动周期 flush 任务 (5s 一次持久化 consumer offset)
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    this.consumerOffsetManager.flushIfNecessary();
                } catch (Exception e) {
                    log.warn("flush consumer offsets failed", e);
                }
            }, 5_000L, 5_000L, TimeUnit.MILLISECONDS);

            // v3 分布式: Slave 启动周期性同步任务 (默认 30s 一次)
            if (!this.brokerConfig.isMaster() && this.brokerConfig.getBrokerMasterAddr() != null) {
                this.slaveSynchronize = new com.zifang.z.mq.broker.slave.SlaveSynchronize(this);
                this.slaveSynchronize.setMasterAddr(this.brokerConfig.getBrokerMasterAddr());
                this.slaveSynchronize.start();
                // 启动时立即同步一次, 然后周期同步
                this.slaveSynchronize.syncAll();
                final long interval = this.brokerConfig.getSyncAllIntervalMillis();
                this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                    try {
                        this.slaveSynchronize.syncAll();
                    } catch (Exception e) {
                        log.warn("slave syncAll failed", e);
                    }
                }, interval, interval, TimeUnit.MILLISECONDS);
                log.info("SlaveSynchronize started, master={} interval={}ms",
                        this.brokerConfig.getBrokerMasterAddr(), interval);
            }

            log.info("Broker started successfully");
        }
    }

    /**
     * 启动一个独立的 Netty 客户端并向 NameServer 发起首次注册, 同时开启心跳周期.
     */
    private void startRegisterToNameServer() throws Exception {
        this.namesrvRemotingClient = new NettyRemotingClient(new com.zifang.z.mq.remoting.netty.NettyClientConfig());
        this.namesrvRemotingClient.start();

        // 立即注册一次
        registerBrokerAll(false, false);

        // 心跳周期
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                registerBrokerAll(false, false);
            } catch (Exception e) {
                log.warn("Broker heartbeat failed", e);
            }
        }, brokerHeartbeatIntervalMillis, brokerHeartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 立即重新注册到 NameServer (Topic 创建后调用, 使新 Topic 尽快可见).
     */
    public void reRegisterToNameServer() {
        try {
            registerBrokerAll(false, false);
        } catch (Exception e) {
            log.warn("reRegisterToNameServer failed", e);
        }
    }

    /**
     * 向所有配置的 NameServer 注册本 Broker, 维持心跳.
     *
     * @param checkOrderConfig 是否需要核对顺序消息配置 (MVP 忽略)
     * @param oneway           是否为 oneway 请求 (不等待响应, MVP 不支持)
     */
    private void registerBrokerAll(boolean checkOrderConfig, boolean oneway) {
        if (namesrvRemotingClient == null) {
            log.warn("namesrvRemotingClient not initialized, skip register");
            return;
        }
        String namesrvAddr = this.brokerConfig.getNamesrvAddr();
        String[] addrs = namesrvAddr.split(";");
        for (String addr : addrs) {
            String trimmed = addr.trim();
            if (trimmed.isEmpty()) { continue; }

            try {
                Channel channel = namesrvRemotingClient.getOrCreateChannel(trimmed);
                RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER);
                request.addExtField("clusterName", this.brokerConfig.getBrokerClusterName());
                request.addExtField("brokerName", this.brokerConfig.getBrokerName());
                request.addExtField("brokerAddr", "localhost:" + this.nettyServerConfig.getListenPort());
                request.addExtField("brokerId", String.valueOf(com.zifang.z.mq.common.BrokerData.MASTER_ID));
                request.addExtField("haServerAddr", "");

                // 上报本地 TopicConfig
                List<TopicConfig> topicConfigs = new java.util.ArrayList<>(
                        this.adminBrokerProcessor.getAllTopicConfigs().values());
                request.setBody(JsonCodec.encode(topicConfigs));

                RemotingCommand response = namesrvRemotingClient.invokeSync(channel, request, 3000);
                if (response != null && response.getCode() == com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SUCCESS
                        && response.getBody() != null && response.getBody().length > 0) {
                    try {
                        RegisterBrokerResult result = JsonCodec.decode(response.getBody(), RegisterBrokerResult.class);
                        log.info("Registered to namesrv={} masterAddr={} haAddr={}",
                                trimmed, result == null ? "?" : result.getMasterAddr(),
                                result == null ? "?" : result.getHaServerAddr());
                    } catch (Exception ignore) {
                        log.info("Registered to namesrv={} (decode body failed)", trimmed);
                    }
                } else {
                    log.warn("Register to namesrv={} returned code={}", trimmed,
                            response == null ? "null" : response.getCode());
                }
            } catch (Exception e) {
                log.warn("Register to namesrv={} failed: {}", trimmed, e.getMessage());
            }
        }
    }

    /**
     * 停止 Broker: 幂等停止, 停止 Netty, 关闭 CommitLog 与所有业务线程池.
     */
    public void shutdown() {
        if (this.running.compareAndSet(true, false)) {
            // 关闭 namesrv 客户端
            if (this.namesrvRemotingClient != null) {
                this.namesrvRemotingClient.shutdown();
            }

            // 停止Netty服务端
            this.remotingServer.shutdown();

            // 停止CommitLog
            this.commitLog.shutdown();

            // 停止 v2 增强组件
            if (this.pullRequestHoldService != null) {
                this.pullRequestHoldService.shutdown();
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }

            if (this.consumerOffsetManager != null) {
                this.consumerOffsetManager.shutdown();
            }

            // v3 分布式: 关闭 SlaveSynchronize
            if (this.slaveSynchronize != null) {
                this.slaveSynchronize.shutdown();
            }

            // v3 HA: 关闭 HAService
            if (this.haService != null) {
                this.haService.shutdown();
            }

            // 关闭线程池
            this.scheduledExecutorService.shutdown();
            this.sendMessageExecutor.shutdown();
            this.pullMessageExecutor.shutdown();
            this.adminBrokerExecutor.shutdown();

            log.info("Broker shutdown successfully");
        }
    }

    // Getters

    /**
     * @return Broker 自身配置
     */
    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    /**
     * @return 消息存储配置
     */
    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    /**
     * @return CommitLog 实例
     */
    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * @return Consumer 位点管理器
     */
    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    /**
     * @return Pull 长轮询服务
     */
    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    /**
     * @return 延迟消息服务
     */
    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    /**
     * @return Broker OutAPI (Broker 互查接口处理器)
     */
    public com.zifang.z.mq.broker.outapi.BrokerOutAPI getBrokerOutAPI() {
        return brokerOutAPI;
    }

    /**
     * @return AdminBrokerProcessor
     */
    public com.zifang.z.mq.broker.processor.AdminBrokerProcessor getAdminBrokerProcessor() {
        return adminBrokerProcessor;
    }

    /**
     * @return ConsumerOffset 版本号 (暴露给 BrokerOutAPI)
     */
    public com.zifang.z.mq.common.ha.DataVersion getConsumerOffsetDataVersion() {
        return consumerOffsetDataVersion;
    }

    /**
     * @return DelayOffset 版本号
     */
    public com.zifang.z.mq.common.ha.DataVersion getDelayOffsetDataVersion() {
        return delayOffsetDataVersion;
    }

    /**
     * @return SlaveSynchronize (Slave 模式下非 null).
     */
    public com.zifang.z.mq.broker.slave.SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    /**
     * @return HAService (主从同步服务).
     */
    public com.zifang.z.mq.store.ha.HAService getHaService() {
        return haService;
    }
}
