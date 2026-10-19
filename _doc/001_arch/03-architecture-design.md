# Z-MQ 架构设计文档

## 1. 项目概述

### 1.1 项目定位

Z-MQ 是一个对标 RocketMQ 的分布式消息中间件，具备高性能、高可靠、高实时性的消息传输能力。

### 1.2 核心特性

- **高性能**：单机写入性能达到十万级 TPS
- **高可靠**：消息持久化，支持同步/异步刷盘
- **高实时**：消息推送毫秒级延迟
- **高扩展**：支持水平扩展，动态扩容
- **多协议**：支持多种消息协议（JMS、MQTT等）

## 2. 整体架构

### 2.1 架构分层

```
┌─────────────────────────────────────────────────────────────────┐
│                        应用层 (Application)                       │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────────────┐ │
│  │ Producer│  │ Consumer│  │ Admin   │  │ 监控告警              │ │
│  └────┬────┘  └────┬────┘  └────┬────┘  └─────────────────────┘ │
│       │            │            │                               │
├───────┼────────────┼────────────┼───────────────────────────────┤
│       │      协议层 (Protocol)   │                               │
│  ┌────┴────────────┴────────────┴────┐                        │
│  │    Remoting 通信层 (Netty)          │                        │
│  └────────────────────────────────────┘                        │
│       │                                                         │
├───────┼─────────────────────────────────────────────────────────┤
│       │              核心层 (Core)                               │
│  ┌────┴────┐  ┌──────────┐  ┌──────────┐  ┌─────────────────┐   │
│  │ Store   │  │ Message  │  │ Rebalance│  │ Delay/Schedule  │   │
│  │ 存储    │  │ 消息处理  │  │ 负载均衡  │  │ 延迟/定时消息    │   │
│  └────┬────┘  └──────────┘  └──────────┘  └─────────────────┘   │
│       │                                                          │
├───────┼──────────────────────────────────────────────────────────┤
│       │              运行层 (Runtime)                           │
│  ┌────┴────┐  ┌──────────┐  ┌──────────┐  ┌─────────────────┐   │
│  │ NameServer│  │ Broker   │  │ Controller│  │ DLedger(可选)  │   │
│  │ 命名服务  │  │ 消息节点  │  │ 控制器    │  │ Raft集群      │   │
│  └─────────┘  └──────────┘  └──────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件说明

#### 2.2.1 NameServer（命名服务）

- **职责**：服务注册与发现、路由管理
- **特点**：无状态、可水平扩展、轻量级
- **核心功能**：
    - Broker 注册与心跳检测
    - 路由信息缓存与分发
    - Topic 路由查找

#### 2.2.2 Broker（消息节点）

- **职责**：消息存储、转发、查询
- **特点**：有状态、主从架构、可水平扩展
- **核心模块**：
    - **Remoting Server**：网络通信层
    - **Store 存储层**：CommitLog、ConsumeQueue、IndexFile
    - **Processor 处理层**：请求处理器
    - **Rebalance 均衡层**：队列负载均衡

#### 2.2.3 Controller（控制器）

- **职责**：Broker 主从切换、集群管理
- **特点**：高可用、强一致性
- **核心功能**：
    - Broker 主从选举
    - 故障自动切换
    - 元数据管理

#### 2.2.4 DLedger（可选）

- **职责**：基于 Raft 的分布式日志存储
- **特点**：强一致性、自动选主
- **应用场景**：金融级可靠性要求

### 2.3 部署架构

#### 2.3.1 最小部署

```
┌─────────────────┐
│   NameServer    │
│   (单节点)       │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
┌───┴───┐ ┌───┴───┐
│Broker │ │Broker │
│Master │ │Master │
└───────┘ └───────┘
```

#### 2.3.2 生产部署

```
┌──────────────────────────────────────────────────────────┐
│                   NameServer 集群 (2节点)                   │
│            ┌──────────────┐      ┌──────────────┐        │
│            │  NameServer  │      │  NameServer  │        │
│            └──────────────┘      └──────────────┘        │
└──────────────────────────────────────────────────────────┘
                            │
┌───────────────────────────┼──────────────────────────────┐
│                      Broker 集群                          │
│  ┌─────────────────┐      │      ┌─────────────────┐     │
│  │   Broker-A      │◄─────┴─────►│   Broker-B      │     │
│  │  ┌──────┐┌────┐ │             │  ┌──────┐┌────┐ │     │
│  │  │Master││Slave││             │  │Master││Slave││     │
│  │  └──┬───┘└──┬─┘ │             │  └──┬───┘└──┬─┘ │     │
│  │     │       │   │             │     │       │   │     │
│  │  ┌──┴───────┴─┐ │             │  ┌──┴───────┴─┐ │     │
│  │  │ Sync Master│ │             │  │ Sync Master│ │     │
│  │  └────────────┘ │             │  └────────────┘ │     │
│  └─────────────────┘             └─────────────────┘     │
└──────────────────────────────────────────────────────────┘
```

## 3. 模块设计

### 3.1 模块划分

```
z-mq/                              # 父项目
├── z-mq-common/                   # 公共模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/common/
│   │       ├── constant/          # 常量定义
│   │       ├── message/           # 消息定义
│   │       ├── protocol/          # 通信协议
│   │       ├── util/              # 工具类
│   │       └── exception/         # 异常定义
│   └── pom.xml
│
├── z-mq-remoting/                 # 网络通信模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/remoting/
│   │       ├── netty/             # Netty实现
│   │       ├── protocol/          # 协议编解码
│   │       ├── common/            # 公共组件
│   │       └── exception/         # 通信异常
│   └── pom.xml
│
├── z-mq-store/                    # 存储模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/store/
│   │       ├── log/               # CommitLog管理
│   │       ├── queue/             # ConsumeQueue管理
│   │       ├── index/             # 索引文件管理
│   │       ├── ha/                # 主从同步(HA)
│   │       ├── dledger/           # DLedger集成
│   │       └── config/            # 存储配置
│   └── pom.xml
│
├── z-mq-broker/                   # Broker模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/broker/
│   │       ├── broker/            # Broker核心
│   │       ├── processor/         # 请求处理器
│   │       ├── longpolling/       # 长轮询实现
│   │       ├── offset/            # 消费进度管理
│   │       ├── subscription/      # 订阅管理
│   │       ├── out/               # 对外服务
│   │       └── config/            # Broker配置
│   └── pom.xml
│
├── z-mq-nameserver/               # NameServer模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/nameserver/
│   │       ├── namesrv/           # NameServer核心
│   │       ├── processor/         # 请求处理器
│   │       ├── routeinfo/         # 路由信息管理
│   │       ├── kvconfig/          # KV配置管理
│   │       └── config/            # NameServer配置
│   └── pom.xml
│
├── z-mq-controller/               # Controller模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/controller/
│   │       ├── controller/        # Controller核心
│   │       ├── elect/             # 选举算法
│   │       ├── metadata/          # 元数据管理
│   │       ├── broker/            # Broker管理
│   │       └── config/            # Controller配置
│   └── pom.xml
│
├── z-mq-client/                   # 客户端模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/client/
│   │       ├── producer/          # 生产者实现
│   │       ├── consumer/          # 消费者实现
│   │       ├── admin/             # 管理客户端
│   │       ├── impl/              # 客户端实现
│   │       └── common/            # 客户端公共类
│   └── pom.xml
│
├── z-mq-tools/                    # 工具模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/tools/
│   │       ├── monitor/           # 监控工具
│   │       ├── export/            # 数据导出
│   │       ├── import/            # 数据导入
│   │       └── command/           # 命令行工具
│   └── pom.xml
│
├── z-mq-example/                  # 示例模块
│   ├── src/main/java/
│   │   └── com/zifang/z/mq/example/
│   │       ├── quickstart/        # 快速开始示例
│   │       ├── order/             # 顺序消息示例
│   │       ├── transaction/       # 事务消息示例
│   │       ├── delay/             # 延迟消息示例
│   │       └── broadcast/         # 广播消费示例
│   └── pom.xml
│
└── pom.xml                        # 父POM
```

### 3.2 模块依赖关系

```
┌─────────────────────────────────────────────────────────────────┐
│                         依赖关系图                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐                                               │
│  │ z-mq-common  │◄──────────────┬──────────────────┐            │
│  │   (公共模块)  │               │                  │            │
│  └──────────────┘               │                  │            │
│         ▲                       │                  │            │
│         │              ┌──────┴──────┐    ┌──────┴──────┐    │
│         │              │ z-mq-store  │    │z-mq-remoting │    │
│  ┌──────┴──────┐       │   (存储模块)  │    │  (通信模块)  │    │
│  │ z-mq-client │       └──────┬──────┘    └──────┬──────┘    │
│  │  (客户端)    │              │                  │            │
│  └─────────────┘              └────────┬─────────┘            │
│                                        │                       │
│                              ┌─────────┴─────────┐              │
│                              │   z-mq-broker    │              │
│                              │   (Broker模块)   │              │
│                              └────────┬─────────┘              │
│                                     │                          │
│  ┌──────────────┐  ┌──────────────┐ │  ┌──────────────┐        │
│  │z-mq-namesrv  │  │z-mq-controller│◄┘  │  z-mq-tools  │        │
│  │(NameServer)  │  │  (控制器)      │    │   (工具)      │        │
│  └──────────────┘  └──────────────┘    └──────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 4. 核心组件详细设计

### 4.1 NameServer 设计

#### 4.1.1 职责

- 服务注册与发现
- 路由信息管理
- 心跳检测
- 集群配置管理

#### 4.1.2 核心数据结构

```java
// Broker 路由信息
public class BrokerData {
    private String brokerName;              // Broker名称
    private HashMap<Long/*brokerId*/, String/*brokerAddr*/> brokerAddrs;
}

// Topic 路由信息
public class TopicRouteData {
    private List<QueueData> queueDatas;       // 队列信息
    private List<BrokerData> brokerDatas;     // Broker信息
}

// 队列数据
public class QueueData {
    private String brokerName;                // Broker名称
    private int readQueueNums;                // 读队列数
    private int writeQueueNums;               // 写队列数
    private int perm;                         // 权限
}
```

#### 4.1.3 启动流程

```
1. 加载配置文件
2. 初始化 Netty 服务器
3. 启动定时任务（扫描不活跃Broker）
4. 注册 JVM 钩子（优雅关闭）
```

### 4.2 Broker 设计

#### 4.2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         Broker 架构                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    BrokerController                       │ │
│  │                   (Broker控制器)                          │ │
│  └──────────────────────┬────────────────────────────────────┘ │
│                         │                                       │
│  ┌──────────────────────┼────────────────────────────────────┐ │
│  │                      ▼            Broker 内部模块           │ │
│  │  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐   │ │
│  │  │ RemotingServer│ │  MessageStore │ │   HaService   │   │ │
│  │  │   网络层      │ │    存储层      │ │   主从同步    │   │ │
│  │  └───────┬───────┘ └───────┬───────┘ └───────┬───────┘   │ │
│  │          │                 │                   │           │ │
│  │  ┌───────┴───────┐ ┌───────┴───────┐ ┌───────┴───────┐   │ │
│  │  │SendProcessor│ │ CommitLog     │ │  HAService    │   │ │
│  │  │PullProcessor│ │ ConsumeQueue  │ │  HAConnection │   │ │
│  │  │...          │ │ IndexFile     │ │  WaitNotify   │   │ │
│  │  └─────────────┘ └───────────────┘ └───────────────┘   │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.2.2 存储设计

##### CommitLog（消息存储文件）

```java
public class CommitLog {
    // 文件大小（默认1GB）
    public static final int MESSAGE_MAGIC_CODE = 0xAABBCCDD;

    // 消息存储结构
    // ┌────────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
    // │ 总消息长度  │ 魔数    │ CRC32   │ 消息体  │ 消息属性│ 消息key │ 消息tag │
    // │ (4字节)   │(4字节) │(4字节) │         │         │         │         │
    // └────────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
}
```

##### ConsumeQueue（消费队列）

```java
public class ConsumeQueue {
    // 每个条目大小（20字节）
    public static final int CQ_STORE_UNIT_SIZE = 20;

    // 条目结构
    // ┌─────────────┬─────────────┬─────────────┐
    // │ CommitLog   │ 消息大小     │ Tag HashCode│
    // │ 物理偏移量   │             │             │
    // │ (8字节)    │ (4字节)     │ (8字节)     │
    // └─────────────┴─────────────┴─────────────┘
}
```

##### IndexFile（索引文件）

```java
public class IndexFile {
    // 文件头大小
    public static final int INDEX_HEADER_SIZE = 40;
    // 每个索引单元大小
    public static final int INDEX_SLOT_SIZE = 4;
    // 每个索引项大小
    public static final int INDEX_UNIT_SIZE = 20;

    // 文件结构
    // ┌──────────┬──────────┬──────────┬──────────┬──────────┐
    // │ Header   │ SlotTable│ IndexList│ Free Space│ End     │
    // │ (40字节) │ (500KB)  │ (可变)   │          │ (4字节) │
    // └──────────┴──────────┴──────────┴──────────┴──────────┘
}
```

#### 2.2.3 刷盘机制

```
┌─────────────────────────────────────────────────────────────────┐
│                      刷盘策略                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐      ┌──────────────────┐                │
│  │  同步刷盘         │      │  异步刷盘         │                │
│  │  SYNC_FLUSH      │      │  ASYNC_FLUSH     │                │
│  └────────┬─────────┘      └────────┬─────────┘                │
│           │                        │                            │
│           ▼                        ▼                            │
│  ┌──────────────────┐      ┌──────────────────┐                │
│  │ 1. 消息写入      │      │ 1. 消息写入      │                │
│  │    MappedFile    │      │    MappedFile    │                │
│  │                  │      │                  │                │
│  │ 2. 刷盘线程      │      │ 2. 提交刷盘请求  │                │
│  │    立即刷盘      │      │    FlushQueue    │                │
│  │                  │      │                  │                │
│  │ 3. 返回成功      │      │ 3. 刷盘线程      │                │
│  │    (等待刷盘)    │      │    批量刷盘      │                │
│  │                  │      │                  │                │
│  │                  │      │ 4. 返回成功      │                │
│  │                  │      │    (立即返回)    │                │
│  └──────────────────┘      └──────────────────┘                │
│                                                                 │
│  适用场景:                     适用场景:                         │
│  - 金融交易                     - 普通消息                       │
│  - 订单处理                     - 日志收集                       │
│  - 强一致性要求                 - 高吞吐优先                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 2.2.4 主从复制

```
┌─────────────────────────────────────────────────────────────────┐
│                      主从复制机制                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐      ┌──────────────────┐                │
│  │   同步复制        │      │   异步复制        │                │
│  │  SYNC_MASTER     │      │  ASYNC_MASTER    │                │
│  └────────┬─────────┘      └────────┬─────────┘                │
│           │                        │                            │
│           ▼                        ▼                            │
│  ┌──────────────────┐      ┌──────────────────┐                │
│  │    Master        │      │    Master        │                │
│  │   (主节点)        │      │   (主节点)        │                │
│  └────────┬─────────┘      └────────┬─────────┘                │
│           │                        │                            │
│           │  写入后同步              │  写入后异步                 │
│           │  等待Slave确认           │  立即返回                   │
│           ▼                        ▼                            │
│  ┌──────────────────┐      ┌──────────────────┐                │
│  │    Slave         │      │    Slave         │                │
│  │   (从节点)        │      │   (从节点)        │                │
│  └──────────────────┘      └──────────────────┘                │
│                                                                 │
│  特点:                       特点:                             │
│  - 数据强一致性               - 高可用优先                       │
│  - 写入延迟增加               - 可能有短暂不一致                  │
│  - Master故障可无缝切换       - 故障时可能丢失少量消息             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 客户端设计

#### 4.3.1 Producer 设计

```java
// 生产者核心类
public class DefaultMQProducer {
    // 生产者组名
    private String producerGroup;
    // 默认Topic队列数
    private volatile int defaultTopicQueueNums = 4;
    // 发送超时时间
    private int sendMsgTimeout = 3000;
    // 消息体大小限制
    private int maxMessageSize = 1024 * 1024 * 4;

    // 发送消息（同步）
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    // 发送消息（异步）
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException;

    // 发送消息（单向）
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException;

    // 发送顺序消息
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException;

    // 发送事务消息
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, final Object arg);
}
```

#### 4.3.2 Consumer 设计

```java
// 推模式消费者
public class DefaultMQPushConsumer {
    // 消费者组
    private String consumerGroup;
    // 消费模式（集群/广播）
    private MessageModel messageModel = MessageModel.CLUSTERING;
    // 消费线程数
    private int consumeThreadMin = 20;
    private int consumeThreadMax = 64;
    // 单队列并行消费数
    private int consumeConcurrentlyMaxSpan = 2000;
    // 最大消费重试次数
    private int maxReconsumeTimes = -1;
    // 消费超时时间
    private long consumeTimeout = 15;
    // 消费顺序性
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    // 注册消息监听器（并发消费）
    public void registerMessageListener(final MessageListenerConcurrently messageListener);

    // 注册消息监听器（顺序消费）
    public void registerMessageListener(final MessageListenerOrderly messageListener);
}

// 拉模式消费者
public class DefaultMQPullConsumer {
    // 拉取消息
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums);

    // 更新消费进度
    public void updateConsumeOffset(MessageQueue mq, long offset);

    // 获取消费进度
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore);
}
```

### 4.4 消息存储设计

#### 4.4.1 存储架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     消息存储架构                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                     消息写入流程                           │ │
│  │                                                           │ │
│  │    Producer ──► CommitLog ──► MappedFile ──► Disk        │ │
│  │                    │                                      │ │
│  │                    ▼                                      │ │
│  │            ┌───────────────┐                              │ │
│  │            │ ReputService  │                              │ │
│  │            │ (异步分发)     │                              │ │
│  │            └───────┬───────┘                              │ │
│  │                    │                                      │ │
│  │                    ▼                                      │ │
│  │        ┌───────────┼───────────┐                          │ │
│  │        ▼           ▼           ▼                          │ │
│  │   ConsumeQueue   IndexFile    Filter                      │ │
│  │   (消费队列)      (索引)       (过滤)                       │ │
│  │                                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                     文件结构                               │ │
│  │                                                           │ │
│  │  ~/store/                                                 │ │
│  │  ├── commitlog/                    # 消息存储目录           │ │
│  │  │   ├── 00000000000000000000     # 1GB文件               │ │
│  │  │   ├── 00000000001073741824     # 1GB文件               │ │
│  │  │   └── ...                                               │ │
│  │  ├── consumequeue/                 # 消费队列目录           │ │
│  │  │   ├── TopicA/                                         │ │
│  │  │   │   ├── 0/                  # 队列0                   │ │
│  │  │   │   │   ├── 00000000000000000000                     │ │
│  │  │   │   │   └── ...                                       │ │
│  │  │   │   └── 1/                  # 队列1                   │ │
│  │  │   └── TopicB/                                          │ │
│  │  ├── index/                       # 索引目录                │ │
│  │  │   ├── 20250322000000000      # 索引文件                │ │
│  │  │   └── ...                                               │ │
│  │  ├── checkpoint                   # 检查点文件            │ │
│  │  ├── abort                        # 异常退出标记             │ │
│  │  └── config/                      # 配置文件              │ │
│  │      ├── consumerFilter.json                              │ │
│  │      ├── consumerOffset.json                              │ │
│  │      └── delayOffset.json                                 │ │
│  │                                                            │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.4.2 存储核心类

```java
// CommitLog 核心类
public class CommitLog {
    // MappedFileQueue 管理多个 MappedFile
    private final MappedFileQueue mappedFileQueue;
    // 消息写入缓冲区
    private final ByteBuffer msgStoreItemMemory;
    // 刷盘服务
    private final FlushCommitLogService flushCommitLogService;
    // 主从同步服务
    private final HAService haService;

    // 消息追加入口
    public PutMessageResult putMessage(final MessageExtBrokerInner msg);

    // 根据偏移量获取消息
    public SelectMappedBufferResult getMessage(final long offset, final int size);
}

// ConsumeQueue 核心类
public class ConsumeQueue {
    // 每个条目大小
    public static final int CQ_STORE_UNIT_SIZE = 20;

    // 加载消费队列
    public boolean load();

    // 根据逻辑偏移量获取索引缓冲区
    public SelectMappedBufferResult getIndexBuffer(final long startIndex);

    // 追加消息索引
    public void putMessagePositionInfoWrapper(final long offset, final int size,
                                                  final long tagsCode, final long cqOffset);
}

// IndexFile 核心类
public class IndexFile {
    // 哈希槽数量
    private static final int hashSlotNum = 5000000;
    // 最大索引数量
    private static final int indexNum = 5000000 * 4;

    // 构建索引键
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp);

    // 根据 key 查询
    public void selectPhyOffset(final List<Long> phyOffsets, final String key,
                                  final int maxNum, final long begin, final long end);
}
```

### 4.5 消息类型设计

#### 4.5.1 普通消息

```java
public class Message implements Serializable {
    private String topic;                    // 消息主题
    private int flag;                        // 消息标志
    private Map<String, String> properties;  // 消息属性
    private byte[] body;                     // 消息体
    private String transactionId;            // 事务ID
}

public class MessageExt extends Message {
    private int queueId;                     // 队列ID
    private int storeSize;                   // 存储大小
    private long queueOffset;                // 队列偏移量
    private int sysFlag;                     // 系统标志
    private long bornTimestamp;              // 消息生成时间
    private SocketAddress bornHost;          // 生成主机
    private long storeTimestamp;             // 存储时间
    private SocketAddress storeHost;         // 存储主机
    private String msgId;                    // 消息ID
    private long commitLogOffset;            // CommitLog偏移量
    private int bodyCRC;                     // 消息体CRC
    private int reconsumeTimes;              // 重试次数
}
```

#### 4.5.2 顺序消息

```java
// 顺序消息生产者
public class OrderMessageProducer {
    /**
     * 发送顺序消息
     * @param msg 消息
     * @param selector 队列选择器
     * @param arg 选择参数（如订单ID）
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg);
}

// 队列选择器
public interface MessageQueueSelector {
    /**
     * 选择队列
     * @param mqs 可用队列列表
     * @param msg 消息
     * @param arg 参数
     */
    MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg);
}

// 示例：按订单ID选择队列
public class OrderIdQueueSelector implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        Long orderId = (Long) arg;
        int index = (int) (orderId % mqs.size());
        return mqs.get(index);
    }
}
```

#### 4.5.3 事务消息

```java
// 事务消息生产者
public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;
    private ExecutorService executorService;

    /**
     * 发送事务消息
     * @param msg 消息
     * @param tranExecuter 本地事务执行器
     * @param arg 参数
     */
    public TransactionSendResult sendMessageInTransaction(
        final Message msg,
        final LocalTransactionExecuter tranExecuter,
        final Object arg
    );
}

// 本地事务执行器
public interface LocalTransactionExecuter {
    /**
     * 执行本地事务
     * @param msg 消息
     * @param arg 参数
     * @return 本地事务状态
     */
    LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg);
}

// 事务状态检查监听器
public interface TransactionCheckListener {
    /**
     * 回查本地事务状态
     * @param msg 消息
     * @return 本地事务状态
     */
    LocalTransactionState checkLocalTransactionState(final MessageExt msg);
}

// 事务状态枚举
public enum LocalTransactionState {
    COMMIT_MESSAGE,    // 提交消息
    ROLLBACK_MESSAGE,  // 回滚消息
    UNKNOW             // 未知状态（等待回查）
}

// 事务消息执行流程
//
// ┌──────────┐      1.发送Half消息      ┌──────────┐
// │ Producer │ ───────────────────────► │  Broker  │
// └────┬─────┘                         └────┬─────┘
//      │                                  │
//      │ 2.执行本地事务                    │
//      ▼                                  │
// ┌──────────┐                           │
// │ 本地事务  │                           │
// └────┬─────┘                           │
//      │                                │
//      │ 3.提交Commit/Rollback          │
//      └───────────────────────────────► │
//                                        │
//      ┌─────────────────────────────────┘
//      │ 4.超时回查
//      ▼
// ┌──────────┐
// │ 回查本地事务│
// └──────────┘
```

#### 4.5.4 延迟消息

```java
// 延迟级别枚举
public enum DelayLevel {
    DELAY_1S(1, "1s"),
    DELAY_5S(2, "5s"),
    DELAY_10S(3, "10s"),
    DELAY_30S(4, "30s"),
    DELAY_1M(5, "1m"),
    DELAY_2M(6, "2m"),
    DELAY_3M(7, "3m"),
    DELAY_4M(8, "4m"),
    DELAY_5M(9, "5m"),
    DELAY_6M(10, "6m"),
    DELAY_7M(11, "7m"),
    DELAY_8M(12, "8m"),
    DELAY_9M(13, "9m"),
    DELAY_10M(14, "10m"),
    DELAY_20M(15, "20m"),
    DELAY_30M(16, "30m"),
    DELAY_1H(17, "1h"),
    DELAY_2H(18, "2h");

    private int level;
    private String desc;
}

// 延迟消息处理流程
//
// ┌───────────────────────────────────────────────────────────┐
// │                     延迟消息处理流程                         │
// ├───────────────────────────────────────────────────────────┤
// │                                                           │
// │  1. 生产者发送延迟消息                                     │
// │     ┌──────────────────────────────────────┐             │
// │     │ msg.setDelayTimeLevel(3); // 延迟10秒 │             │
// │     │ producer.send(msg);                    │             │
// │     └──────────────────────────────────────┘             │
// │                          │                                │
// │                          ▼                                │
// │  2. Broker 接收消息并判断为延迟消息                         │
// │     ┌──────────────────────────────────────┐             │
// │     │ 将消息主题替换为 SCHEDULE_TOPIC    │             │
// │     │ 将队列ID设置为延迟级别             │             │
// │     │ 存储到 CommitLog                   │             │
// │     └──────────────────────────────────────┘             │
// │                          │                                │
// │                          ▼                                │
// │  3. ScheduleMessageService 定时扫描                         │
// │     ┌──────────────────────────────────────┐             │
// │     │ 为每个延迟级别创建定时任务         │             │
// │     │ 扫描对应的 ConsumeQueue            │             │
// │     │ 判断消息是否到达投递时间           │             │
// │     └──────────────────────────────────────┘             │
// │                          │                                │
// │                          ▼                                │
// │  4. 到达时间后进行投递                                     │
// │     ┌──────────────────────────────────────┐             │
// │     │ 恢复原消息主题和队列               │             │
// │     │ 重新存储到 CommitLog               │             │
// │     │ 消费者即可正常消费                 │             │
// │     └──────────────────────────────────────┘             │
// │                                                           │
// └───────────────────────────────────────────────────────────┘
```

## 5. 通信协议设计

### 5.1 协议格式

```
┌─────────────────────────────────────────────────────────────────┐
│                     通信协议格式                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                     协议头 (固定长度)                       │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │ 字段名         │ 长度      │ 说明                        │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │ length         │ 4 bytes  │ 总长度（头+体）              │ │
│  │ serialization  │ 1 byte   │ 序列化方式                   │ │
│  │ headerLength   │ 2 bytes  │ 扩展头长度                   │ │
│  │ protocolType   │ 1 byte   │ 协议类型（Request/Response）│ │
│  │ requestCode    │ 2 bytes  │ 请求码                       │ │
│  │ responseCode   │ 2 bytes  │ 响应码                       │ │
│  │ opaque         │ 4 bytes  │ 请求唯一标识                 │ │
│  │ flag           │ 4 bytes  │ 标志位                       │ │
│  │ remark         │ variable │ 备注（长度+内容）            │ │
│  │ extFields      │ variable │ 扩展字段（Map结构）          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                     协议体（可变长度）                      │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │ 字段名         │ 长度      │ 说明                        │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │ bodyLength     │ 4 bytes  │ 消息体长度                   │ │
│  │ body           │ variable │ 序列化后的消息体             │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 5.2 请求码定义

```java
public class RequestCode {
    // NameServer 相关
    public static final int REGISTER_BROKER = 100;
    public static final int UNREGISTER_BROKER = 101;
    public static final int GET_ROUTEINTO_BY_TOPIC = 102;
    public static final int GET_BROKER_CLUSTER_INFO = 103;
    public static final int WIPE_WRITE_PERM_OF_BROKER = 104;
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 105;
    public static final int DELETE_TOPIC_IN_NAMESRV = 106;
    public static final int GET_KV_CONFIG = 107;
    public static final int PUT_KV_CONFIG = 108;
    public static final int GET_KV_CONFIG_BY_VALUE = 109;
    public static final int DELETE_KV_CONFIG = 110;
    public static final int REGISTER_TOPIC_IN_NAMESRV = 111;
    public static final int GET_TOPICS_BY_CLUSTER = 112;
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 113;

    // Broker 相关
    public static final int SEND_MESSAGE = 200;
    public static final int SEND_MESSAGE_V2 = 201;
    public static final int PULL_MESSAGE = 202;
    public static final int QUERY_MESSAGE = 203;
    public static final int QUERY_BROKER_OFFSET = 204;
    public static final int QUERY_CONSUMER_OFFSET = 205;
    public static final int UPDATE_CONSUMER_OFFSET = 206;
    public static final int UPDATE_AND_CREATE_TOPIC = 207;
    public static final int GET_ALL_TOPIC_CONFIG = 208;
    public static final int GET_TOPIC_CONFIG_LIST = 209;
    public static final int GET_TOPIC_NAME_LIST = 210;
    public static final int UPDATE_BROKER_CONFIG = 211;
    public static final int GET_BROKER_CONFIG = 212;
    public static final int TRIGGER_DELETE_FILES = 213;
    public static final int GET_BROKER_RUNTIME_INFO = 214;
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 215;
    public static final int GET_MAX_OFFSET = 216;
    public static final int GET_MIN_OFFSET = 217;
    public static final int GET_EARLIEST_MSG_STORETIME = 218;
    public static final int VIEW_MESSAGE_BY_ID = 219;
    public static final int HEART_BEAT = 220;
    public static final int UNREGISTER_CLIENT = 221;
    public static final int CONSUME_MESSAGE_DIRECTLY = 222;
    public static final int SEND_REPLY_MESSAGE = 223;
    public static final int GET_BROKER_CLUSTER_ACL_CONFIG = 224;
}
```

## 6. 关键流程设计

### 6.1 消息发送流程

```
┌─────────────────────────────────────────────────────────────────┐
│                     消息发送流程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────┐                                                  │
│  │ Producer  │                                                  │
│  └─────┬─────┘                                                  │
│        │ 1. 发送消息 send(msg)                                    │
│        ▼                                                        │
│  ┌─────────────────────────────────────┐                        │
│  │ 2. 消息校验                          │                        │
│  │    - Topic合法性检查                 │                        │
│  │    - 消息体大小检查                  │                        │
│  │    - 消息属性检查                    │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 3. 获取Topic路由信息                  │                        │
│  │    - 从本地缓存获取                   │                        │
│  │    - 缓存不存在则从NameServer获取      │                        │
│  │    - 更新本地路由缓存                 │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 4. 选择MessageQueue                  │                        │
│  │    - 轮询选择（默认）                  │                        │
│  │    - 根据Hash选择（顺序消息）           │                        │
│  │    - 故障延迟策略（失败时避让）          │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 5. 选择Broker地址                    │                        │
│  │    - 优先选择Master                  │                        │
│  │    - Master不可用时选Slave             │                        │
│  │    - 根据发送策略选择                 │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 6. 发送消息到Broker                   │                        │
│  │    - 构建RemotingCommand             │                        │
│  │    - 调用Netty发送                   │                        │
│  │    - 设置超时时间                    │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 7. 处理发送结果                       │                        │
│  │    - 成功: 返回SendResult             │                        │
│  │    - 失败: 判断是否需要重试            │                        │
│  │    - 超过重试次数: 抛出异常           │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌───────────┐                                                  │
│  │ 返回结果   │                                                  │
│  └───────────┘                                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 消息消费流程

```
┌─────────────────────────────────────────────────────────────────┐
│                     消息消费流程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │                      Push 模式（推荐）                     │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌───────────┐                                                  │
│  │ Consumer  │                                                  │
│  └─────┬─────┘                                                  │
│        │ 1. 启动消费者                                            │
│        ▼                                                        │
│  ┌─────────────────────────────────────┐                        │
│  │ 2. 初始化 RebalanceImpl              │                        │
│  │    - 加载消费进度                     │                        │
│  │    - 计算分配的队列                   │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 3. 向Broker注册消费者                 │                        │
│  │    - 发送心跳包                       │                        │
│  │    - 注册过滤信息                     │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 4. 启动PullMessageService            │                        │
│  │    - 循环拉取消息                     │                        │
│  │    - 维护拉取间隔                     │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 5. 发送Pull请求到Broker               │                        │
│  │    - 构建PullMessageRequestHeader   │                        │
│  │    - 设置消费组和队列信息             │                        │
│  │    - 设置消费进度和拉取数量           │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 6. Broker处理Pull请求                │                        │
│  │    - 获取ConsumerQueue               │                        │
│  │    - 读取索引获取CommitLog偏移量      │                        │
│  │    - 从CommitLog读取消息             │                        │
│  │    - 使用Filter进行过滤              │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 7. 返回拉取结果                       │                        │
│  │    - 消息内容                        │                        │
│  │    - 下次建议拉取偏移量               │                        │
│  │    - 挂起超时时间（无消息时）          │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 8. 消费者处理消息                     │                        │
│  │    - 调用MessageListener            │                        │
│  │    - 执行业务逻辑                    │                        │
│  │    - 返回消费状态                    │                        │
│  │      - CONSUME_SUCCESS              │                        │
│  │      - RECONSUME_LATER              │                        │
│  └─────────────┬───────────────────────┘                        │
│                │                                                │
│                ▼                                                │
│  ┌─────────────────────────────────────┐                        │
│  │ 9. 更新消费进度                       │                        │
│  │    - 消费成功：更新消费进度            │                        │
│  │    - 消费失败：发送重试消息            │                        │
│  └─────────────────────────────────────┘                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 7. 高可用设计

### 7.1 主从架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     主从复制架构                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    同步复制模式                            │ │
│  │              (SYNC_MASTER / SYNC_FLUSH)                    │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────┐                    ┌──────────────┐          │
│  │    Master    │◄───── 同步复制 ────►│    Slave     │          │
│  │   (主节点)    │    (数据+刷盘确认)  │   (从节点)    │          │
│  └──────┬───────┘                    └──────────────┘          │
│         │                                                       │
│         │ 写入确认                                               │
│         ▼                                                       │
│  ┌──────────────┐                                                │
│  │   Producer   │                                                │
│  │   (生产者)   │                                                │
│  └──────────────┘                                                │
│                                                                 │
│  特点：                                                          │
│  - Master和Slave数据强一致                                        │
│  - 写入性能相对较低（需等待Slave确认）                             │
│  - Master故障时Slave可无缝切换                                     │
│  - 适用于金融交易等高可靠场景                                       │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    异步复制模式                            │ │
│  │              (ASYNC_MASTER / ASYNC_FLUSH)                │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────┐                    ┌──────────────┐          │
│  │    Master    │─────► 异步复制 ────►│    Slave     │          │
│  │   (主节点)    │    (不等待确认)     │   (从节点)    │          │
│  └──────┬───────┘                    └──────────────┘          │
│         │                                                       │
│         │ 立即返回                                               │
│         ▼                                                       │
│  ┌──────────────┐                                                │
│  │   Producer   │                                                │
│  │   (生产者)   │                                                │
│  └──────────────┘                                                │
│                                                                 │
│  特点：                                                          │
│  - Master写入后立即返回，性能高                                  │
│  - Slave数据可能略有延迟                                          │
│  - Master故障时可能丢失少量消息                                   │
│  - 适用于日志收集、普通消息等场景                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 故障转移

```
┌─────────────────────────────────────────────────────────────────┐
│                     故障转移流程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                   NameServer 故障                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  场景: NameServer 单节点故障                                      │
│  ┌──────────────┐                                               │
│  │ NameServer-1 │◄──── 故障                                     │
│  └──────────────┘                                               │
│                                                                 │
│  影响:                                                          │
│  - 新Broker无法注册                                               │
│  - 新Consumer无法获取路由                                          │
│  - 存量Consumer/Producer可正常工作（使用本地缓存路由）               │
│                                                                 │
│  恢复:                                                          │
│  1. 启动备用 NameServer-2                                         │
│  2. 配置 Broker/Client 连接多个 NameServer                         │
│  3. 服务自动恢复                                                   │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    Broker 故障                            │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  场景1: Master Broker 故障                                       │
│  ┌──────────────┐                                               │
│  │    Master    │◄──── 故障                                     │
│  └──────┬───────┘                                               │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────┐                                               │
│  │    Slave     │                                               │
│  └──────────────┘                                               │
│                                                                 │
│  自动切换流程（Controller模式）:                                    │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ 1. Controller检测到Master心跳超时                        │    │
│  │ 2. 确认Master确实不可用（多次心跳检查）                    │    │
│  │ 3. 选举Slave为新的Master                                 │    │
│  │ 4. 通知所有Broker更新角色信息                              │    │
│  │ 5. 通知NameServer更新路由信息                              │    │
│  │ 6. 客户端刷新路由，切换到新Master                         │    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                 │
│  场景2: Slave Broker 故障                                      │
│  - 不影响消息写入（Master正常工作）                               │
│  - 高可用降级（无备份）                                           │
│  - 可启动新Slave进行数据同步                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 8. 性能优化

### 8.1 零拷贝技术

```
┌─────────────────────────────────────────────────────────────────┐
│                     零拷贝技术                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    传统拷贝方式                            │ │
│  │                 (4次拷贝，4次切换)                          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  磁盘 ──► 内核缓冲区 ──► 用户缓冲区 ──► Socket缓冲区 ──► 网卡     │
│    │         │            │             │          │            │
│    │         │            │             │          │            │
│   拷贝1     拷贝2         拷贝3          拷贝4                   │
│            上下文切换  上下文切换  上下文切换  上下文切换          │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              mmap + sendfile 零拷贝                        │ │
│  │                 (2次拷贝，2次切换)                          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  磁盘 ◄───► 内核缓冲区 ──────────────────► Socket缓冲区 ──► 网卡 │
│    │    mmap共享                         sendfile               │
│    │                                                            │
│   DMA拷贝                                                      │
│              CPU不参与拷贝                                       │
│                                                                 │
│  Java实现:                                                      │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ // 1. 使用 MappedByteBuffer 进行内存映射               │    │
│  │ MappedByteBuffer mappedBuffer =                        │    │
│  │     fileChannel.map(MapMode.READ_ONLY, 0, fileSize);   │    │
│  │                                                        │    │
│  │ // 2. 使用 FileChannel.transferTo 进行零拷贝发送       │    │
│  │ fileChannel.transferTo(position, count, socketChannel);│    │
│  └────────────────────────────────────────────────────────┘    │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │              sendfile 零拷贝（直接）                       │ │
│  │                 (1次DMA拷贝)                               │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  磁盘 ──────────────────────────────────────► 网卡                │
│              网卡直接从磁盘读取（DMA）                             │
│                                                                 │
│  要求: Linux 2.4+, 网卡支持 DMA gather                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 8.2 内存映射

```java
// CommitLog 中的内存映射实现
public class MappedFile extends ReferenceResource {
    // 文件大小（默认1GB）
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final int TOTAL_MAPPED_VIRTUAL_MEMORY = 0;
    protected static final int TOTAL_MAPPED_FILES = 0;

    // 文件通道
    private FileChannel fileChannel;
    // 内存映射缓冲区
    private MappedByteBuffer mappedByteBuffer;
    // 写位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 提交位置
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    // 刷盘位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 初始化 MappedFile
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);

        // 确保目录存在
        this.file.getParentFile().mkdirs();

        // 创建RandomAccessFile
        this.randomAccessFile = new RandomAccessFile(this.file, "rw");
        this.fileChannel = this.randomAccessFile.getChannel();

        // 创建内存映射
        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize);
        TOTAL_MAPPED_FILES.incrementAndGet();
    }

    /**
     * 追加消息
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.mappedByteBuffer.put(data, 0, data.length);
                this.wrotePosition.addAndGet(data.length);
                return true;
            } catch (Exception e) {
                log.error("append message error", e);
            }
        }
        return false;
    }

    /**
     * 刷盘
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();
                try {
                    // 强制刷盘
                    this.mappedByteBuffer.force();
                } catch (Exception e) {
                    log.error("force flush error", e);
                }
                this.flushedPosition.set(value);
                this.release();
            } else {
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 预热文件（将文件内容加载到页缓存）
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += CommitLog.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // 模拟缺页中断，强制加载到内存
            if (j % pages == 0) {
                if (type == FlushDiskType.SYNC_FLUSH) {
                    byteBuffer.force();
                }
                flush++;
            }
        }
        // 最终刷盘
        if (type == FlushDiskType.SYNC_FLUSH) {
            byteBuffer.force();
        }
        this.flushedPosition.set(this.fileSize);
    }
}
```

## 9. 监控与运维

### 9.1 监控指标

```
┌─────────────────────────────────────────────────────────────────┐
│                     监控指标体系                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    Broker 指标                            │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │                                                           │ │
│  │  存储相关                                                  │ │
│  │  - CommitLog 磁盘使用率                                    │ │
│  │  - ConsumeQueue 磁盘使用率                                 │ │
│  │  - IndexFile 磁盘使用率                                    │ │
│  │  - 剩余磁盘空间                                             │ │
│  │                                                           │ │
│  │  消息相关                                                  │ │
│  │  - 消息写入速率 (TPS)                                       │ │
│  │  - 消息读取速率 (TPS)                                       │ │
│  │  - 消息堆积数量                                             │ │
│  │  - 消息延迟时间                                             │ │
│  │  - 消息大小分布                                             │ │
│  │                                                           │ │
│  │  连接相关                                                  │ │
│  │  - 当前Producer连接数                                       │ │
│  │  - 当前Consumer连接数                                       │ │
│  │  - 历史最大连接数                                            │ │
│  │                                                           │ │
│  │  系统相关                                                  │ │
│  │  - CPU使用率                                               │ │
│  │  - 内存使用率                                               │ │
│  │  - 网络IO                                                   │ │
│  │  - 磁盘IO                                                   │ │
│  │                                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                  Consumer 指标                              │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │                                                           │ │
│  │  - 消费速率 (TPS)                                           │ │
│  │  - 消费延迟（消费位点与最新位点差）                          │ │
│  │  - 消费失败重试次数                                         │ │
│  │  - 消费超时次数                                             │ │
│  │  - 消费成功率                                               │ │
│  │                                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                  Producer 指标                            │ │
│  ├───────────────────────────────────────────────────────────┤ │
│  │                                                           │ │
│  │  - 发送速率 (TPS)                                           │ │
│  │  - 发送成功率                                               │ │
│  │  - 发送平均耗时                                             │ │
│  │  - 发送失败重试次数                                         │ │
│  │  - 消息体平均大小                                           │ │
│  │                                                           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 10. Roadmap 规划

### 10.1 版本规划

```
┌─────────────────────────────────────────────────────────────────┐
│                     版本发布计划                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  v0.1.0 (MVP版本) - 核心功能                                    │
│  ─────────────────────────────                                  │
│  [x] 基础项目结构搭建                                            │
│  [ ] NameServer 基础功能                                         │
│  [ ] Broker 基础存储功能                                         │
│  [ ] Producer 基础发送功能                                         │
│  [ ] Consumer 基础消费功能                                         │
│  [ ] 基于文件的 CommitLog 存储                                    │
│  [ ] 基于文件的 ConsumeQueue 存储                                 │
│                                                                 │
│  v0.2.0 (稳定版本) - 高可用                                       │
│  ────────────────────────────────────────────────────────────   │
│  [ ] Broker 主从架构                                              │
│  [ ] 同步/异步复制                                                │
│  [ ] 同步/异步刷盘                                                │
│  [ ] Master 故障自动切换                                          │
│  [ ] 消费进度持久化                                               │
│  [ ] 消息重试机制                                                 │
│                                                                 │
│  v0.3.0 (高级特性) - 消息类型                                     │
│  ────────────────────────────────────────────────────────────   │
│  [ ] 顺序消息                                                     │
│  [ ] 延迟消息                                                     │
│  [ ] 事务消息                                                     │
│  [ ] 批量消息                                                     │
│  [ ] 消息过滤（Tag/SQL92）                                        │
│  [ ] 消息轨迹                                                     │
│                                                                 │
│  v0.4.0 (性能优化) - 极致性能                                     │
│  ────────────────────────────────────────────────────────────   │
│  [ ] 内存映射优化                                                 │
│  [ ] 文件预热                                                     │
│  [ ] 堆外内存池                                                   │
│  [ ] 批量压缩                                                     │
│  [ ] 传输压缩                                                     │
│  [ ] 自适应限流                                                   │
│                                                                 │
│  v0.5.0 (企业特性) - 生产就绪                                     │
│  ────────────────────────────────────────────────────────────   │
│  [ ] ACL 访问控制                                                 │
│  [ ] SSL/TLS 加密传输                                             │
│  [ ] 消息加密存储                                                 │
│  [ ] 多租户支持                                                   │
│  [ ] 完善的监控指标                                               │
│  [ ] 管理控制台                                                   │
│  [ ] 命令行工具                                                   │
│                                                                 │
│  v1.0.0 (正式版本) - GA                                          │
│  ────────────────────────────────────────────────────────────   │
│  [ ] 全功能稳定                                                   │
│  [ ] 完整测试覆盖                                                 │
│  [ ] 生产验证                                                     │
│  [ ] 完整文档                                                     │
│  [ ] 社区支持                                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 11. 开发规范

### 11.1 代码规范

```
1. 命名规范
   - 类名: 大驼峰 (MessageStore, CommitLog)
   - 方法名: 小驼峰 (putMessage, getMessage)
   - 常量: 全大写下划线 (MESSAGE_MAGIC_CODE)
   - 包名: 全小写 (com.zifang.z.mq.store)

2. 代码格式
   - 缩进: 4个空格
   - 行宽: 120字符
   - 花括号: K&R风格
   - 空行: 方法间1空行，类成员间1空行

3. 注释规范
   - 类注释: 描述类的职责
   - 方法注释: 描述功能、参数、返回值、异常
   - 关键逻辑: 行内注释说明原因而非描述
   - 复杂算法: 提供伪代码或流程说明

4. 异常处理
   - 不忽略异常，必须处理或抛出
   - 使用自定义异常类
   - 异常信息包含上下文
   - 记录异常日志

5. 日志规范
   - 使用SLF4J接口
   - 日志级别: DEBUG < INFO < WARN < ERROR
   - 不记录敏感信息
   - 关键路径记录时间戳
```

### 11.2 测试规范

```
1. 单元测试
   - 测试覆盖率目标: 核心代码 > 80%
   - 使用 JUnit 5 + Mockito
   - 每个方法至少一个正向测试
   - 边界条件测试
   - 异常路径测试

2. 集成测试
   - 组件间交互测试
   - 数据库/缓存集成测试
   - 使用 TestContainers 进行中间件测试

3. 性能测试
   - 基准测试：单线程性能
   - 压力测试：最大吞吐量
   - 稳定性测试：长时间运行
   - 基准对比：与 RocketMQ 对比

4. E2E 测试
   - 完整业务流程测试
   - 故障注入测试
   - 混沌测试
```

### 11.3 文档规范

```
1. README 规范
   - 项目简介
   - 功能特性
   - 快速开始
   - 架构说明
   - 构建部署
   - 贡献指南
   - 开源协议

2. API 文档
   - 接口描述
   - 请求参数（类型、必填、说明、示例）
   - 响应结果（状态码、结构、示例）
   - 错误码说明

3. 架构文档
   - 系统架构图
   - 数据流图
   - 部署架构图
   - 核心流程图

4. 运维文档
   - 部署手册
   - 配置说明
   - 监控指标
   - 故障处理
   - 备份恢复
```

## 12. 附录

### 12.1 术语表

| 术语           | 英文                       | 说明        |
|--------------|--------------------------|-----------|
| 消息           | Message                  | 数据传输的基本单元 |
| 主题           | Topic                    | 消息的分类标识   |
| 队列           | Queue                    | 消息的有序集合   |
| 生产者          | Producer                 | 消息发送者     |
| 消费者          | Consumer                 | 消息接收者     |
| 消费组          | Consumer Group           | 一组消费者     |
| Broker       | Broker                   | 消息存储转发节点  |
| NameServer   | Name Server              | 服务注册与发现   |
| CommitLog    | Commit Log               | 消息存储文件    |
| ConsumeQueue | Consume Queue            | 消费索引文件    |
| 零拷贝          | Zero Copy                | 减少数据拷贝的技术 |
| 主从复制         | Master-Slave Replication | 数据备份机制    |
| 刷盘           | Flush                    | 将内存数据写入磁盘 |

### 12.2 参考资料

1. RocketMQ 官方文档
2. Kafka 设计与实现
3. 《RocketMQ技术内幕》
4. 《消息中间件Apache Kafka》
5. Linux Zero Copy 技术文档
6. JVM 内存模型规范

---

**文档版本**: 1.0
**最后更新**: 2026-03-22
**作者**: Z-MQ 架构组