# z-mq

> 自研分布式消息队列 — NameServer + Broker + Producer + Consumer 完整链路, 类 Apache RocketMQ 架构

---

## 📋 基本信息

| 字段 | 值 |
|------|-----|
| **项目** | z-mq |
| **分类** | 基础设施 · 消息中间件 |
| **父项目** | z-opc (`com.zifang:z-opc:1.0.0-SNAPSHOT`) |
| **默认端口** | `9876 (nameserver) / 10911 (broker) / 8081 (console)` |
| **技术栈** | Java 8 + Netty 4 + Maven 3 + z-util + Vue 3 |
| **文档维护** | z-opc-foundation |
| **最近更新** | 2026-09-08 |

---

## 🎯 一句话定位

> **Z-MQ = NameServer 无状态路由 + Broker CommitLog 顺序写 + Producer 故障重试 + Consumer 长轮询 Pull + 18 级内置延迟消息**
>
> 删掉 NameServer：Producer/Consumer 无法发现 Broker 地址, 集群不可用;
> 删掉 CommitLog 顺序写：消息存储随机 IO, 吞吐量从 10 万 TPS 降到千级;
> 删掉长轮询：Consumer 需要轮询, 消费延迟从毫秒级升到秒级;
> 删掉延迟消息：秒杀订单超时取消、定时任务等业务场景需要自建调度系统.

融合多个开源项目精华:
- **RocketMQ** (Apache) — 整体架构 + CommitLog + ConsumeQueue + 事务消息 + 延迟消息
- **Kafka** (Apache) — Netty 异步通信 + 大规模分区思想
- **NATS** — 简洁的协议设计
- **Pulsar** (Apache) — 长轮询 / BookKeeper 存算分离思路
- **z-vector** (自研) — Page + WAL + Bloom + BufferPool + 复刻融合设计模式

---

## 🏗️ 项目结构

```
z-mq/                                                            ← parent (pom)
├── z-mq-common              公共数据模型 + 协议 + 工具
│   ├── message/             Message / MessageExt
│   ├── protocol/            SendResult / PullResult / SendStatus
│   └── util/                JsonCodec
├── z-mq-remoting            Netty 通信层 (Server + Client + 编解码)
│   ├── netty/               NettyRemotingServer/Client/Handler/Encoder/Decoder
│   ├── protocol/            RemotingCommand / RequestCode / ResponseCode
│   └── common/              Pair / RemotingHelper
├── z-mq-store               存储引擎
│   ├── log/                 CommitLog + MappedFile + MappedFileQueue
│   │                        + InMemoryQueueIndex (轻量级 ConsumeQueue 替代)
│   ├── config/              ConsumerOffsetManager (消费位点持久化)
│   ├── MessageStoreConfig / PutMessageResult / AppendMessageResult
│   └── MessageExtBrokerInner
├── z-mq-nameserver          NameServer 路由服务
│   ├── routeinfo/           RouteInfoManager (ConcurrentHashMap × 5)
│   ├── processor/           DefaultRequestProcessor
│   ├── kvconfig/            KVConfigManager
│   └── NameServerController / NameServerStartup
├── z-mq-broker              Broker 服务端
│   ├── processor/           SendMessageProcessor / PullMessageProcessor / AdminBrokerProcessor
│   ├── longpoll/            PullRequestHoldService (长轮询)
│   ├── delay/               ScheduleMessageService (18 级延迟消息)
│   └── BrokerController / BrokerConfig / BrokerStartup
├── z-mq-client              Producer / Consumer 客户端
│   ├── producer/            DefaultMQProducer / SendCallback
│   └── consumer/            DefaultMQPushConsumer / DefaultMQPullConsumer
│                            + MessageListener + ConsumeConcurrentlyStatus / ConsumeOrderlyStatus
└── z-mq-console/            ⭐ 前端可视化控制台 (Vue 3 + Vite + Element Plus)
    ├── src/
    │   ├── views/           Dashboard / Cluster / Topics / Messages / Consumers / Delay / Settings
    │   ├── api/             后端 API 客户端 + Mock 数据
    │   ├── stores/          Pinia 全局状态
    │   ├── router/          Vue Router 配置
    │   └── App.vue / main.ts
    └── package.json + vite.config.ts
```

---

## 🚀 快速开始

### Maven 构建后端

```bash
# 编译全部模块
mvn clean install -DskipTests

# 编译 + 运行所有单元/集成测试 (66+ 个)
mvn test

# 启动 NameServer
java -cp z-mq-nameserver/target/classes com.zifang.z.mq.nameserver.NameServerStartup

# 启动 Broker
java -cp z-mq-broker/target/classes com.zifang.z.mq.broker.BrokerStartup
```

### 启动前端

```bash
cd z-mq-console
npm install
npm run dev   # http://localhost:8081
```

### 一行 Java 代码发消息

```java
DefaultMQProducer producer = new DefaultMQProducer("my_group");
producer.setNamesrvAddr("localhost:9876");
producer.start();

Message msg = new Message("OrderTopic", "Hello Z-MQ".getBytes(StandardCharsets.UTF_8));
SendResult result = producer.send(msg);
System.out.println("sent: msgId=" + result.getMsgId() + " status=" + result.getSendStatus());

producer.shutdown();
```

### 消费消息

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my_consumer_group");
consumer.setNamesrvAddr("localhost:9876");
consumer.subscribe("OrderTopic", (MessageListener.Concurrently) (msgs, ctx) -> {
    for (MessageExt m : msgs) {
        System.out.println("received: " + new String(m.getBody()));
    }
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
});
consumer.start();
```

---

## ✨ 核心能力 (8 项)

| # | 能力 | 实现状态 | 实现细节 | 参考来源 |
|---|---|---|---|---|
| 1 | **CommitLog 顺序写** | ✅ | MappedFile + ThreadLocal ByteBuffer + 1GB 文件滚动 | RocketMQ |
| 2 | **NameServer 无状态路由** | ✅ | 5 个 ConcurrentHashMap + ReentrantReadWriteLock | RocketMQ |
| 3 | **Netty 异步通信** | ✅ | Netty 4 + RemotingCommand 二进制协议 + 心跳 + 关闭异常 | RocketMQ / Kafka |
| 4 | **Producer 路由重试** | ✅ | 自动选择 Queue + 失败重试 + 故障延迟 | RocketMQ |
| 5 | **Push/Pull Consumer** | ✅ | Push = Pull + 长轮询模拟, 客户端 Rebalance | RocketMQ |
| 6 | **长轮询 Pull** | ✅ | PullRequestHoldService + SuspendedPull + CountDownLatch | RocketMQ |
| 7 | **消费位点持久化** | ✅ | ConsumerOffsetManager + JSON 5s 周期 flush + 原子写 | RocketMQ |
| 8 | **18 级延迟消息** | ✅ | ScheduleMessageService + JDK DelayQueue | RocketMQ |

---

## 📦 各模块详解

### z-mq-common (13 文件)

公共数据模型与协议定义, 被所有其他模块依赖:

| 类 | 说明 |
|----|------|
| `Message` / `MessageExt` | 消息体 + 扩展属性 (topic/queueId/offset/tags/keys/body) |
| `MessageQueue` | (topic, brokerName, queueId) 三元组 |
| `BrokerData` / `QueueData` / `TopicRouteData` | 路由元数据 |
| `TopicConfig` | Topic 配置 (read/write 队列数 + perm 权限位) |
| `SendResult` / `SendStatus` / `PullResultPayload` | 业务协议返回 |
| `RegisterBrokerResult` | Broker 注册响应 (主从地址) |
| `JsonCodec` | JSON 编解码工具 (替代 fastjson, 自研精简版) |

### z-mq-remoting (23 文件)

Netty 通信层, 承担所有 Server/Client 间的请求/响应:

| 类 | 说明 |
|----|------|
| `RemotingCommand` | 协议帧: code + language + version + opaque + flag + body + extFields |
| `RequestCode` | 业务请求码 (SEND_MESSAGE=10 / PULL_MESSAGE=11 / REGISTER_BROKER / ...) |
| `RemotingSysResponseCode` | 系统响应码 (SUCCESS=0 / SYSTEM_ERROR / TIMEOUT) |
| `NettyRemotingServer` / `NettyRemotingClient` | Netty 服务端/客户端封装 |
| `NettyServerHandler` / `NettyServerConfig` / `NettyClientConfig` | 处理与配置 |
| `NettyEncoder` / `NettyDecoder` | 二进制编解码 (4B 长度 + JSON 体) |
| `ResponseFuture` | 同步请求-响应 future 映射 (opaque -> Promise) |
| `NettyRemotingAbstract` | 抽象请求分发逻辑 |
| `NettyConnectManageHandler` | 连接管理 + 心跳 |
| `Pair<T1, T2>` | 通用二元组 |

### z-mq-store (11 文件 + ConsumerOffsetManager)

存储引擎:

| 类 | 说明 |
|----|------|
| `MessageStoreConfig` | 存储配置 (路径 / 文件大小 / 刷盘策略) |
| `CommitLog` | 统一顺序写日志 (RocketMQ 核心设计) |
| `MappedFile` | 单个文件 1GB 滚动 (mmap 待启用) |
| `MappedFileQueue` | 文件队列, 按 offset 寻址 |
| `FlushDiskType` | SYNC_FLUSH / ASYNC_FLUSH |
| `InMemoryQueueIndex` | 进程内 (topic, queueId) -> MessageExt 索引 (轻量级 ConsumeQueue 替代) |
| `MessageExtBrokerInner` | Broker 内部消息 (含 CommitLogOffset 等) |
| **`ConsumerOffsetManager`** | ⭐ v2 新增: 消费位点持久化 (5s 周期 flush + JSON 存储 + 重启加载) |

### z-mq-nameserver (6 文件)

无状态路由服务, Producer/Consumer 查询 Topic 路由:

| 类 | 说明 |
|----|------|
| `RouteInfoManager` | 5 个 ConcurrentHashMap + 读多写少读写锁 |
| `DefaultRequestProcessor` | Netty 业务请求处理 |
| `KVConfigManager` | KV 配置持久化 (kvConfig.json) |
| `NameServerController` / `NamesrvConfig` | 启动与配置 |
| `NameServerStartup` | main 入口 |

### z-mq-broker (10+ 文件)

Broker 服务端:

| 类 | 说明 |
|----|------|
| `BrokerController` | ⭐ v2 增强: 整合 ConsumerOffsetManager / PullRequestHoldService / ScheduleMessageService |
| `BrokerConfig` / `BrokerStartup` | 配置与启动 |
| `SendMessageProcessor` | 处理 SEND_MESSAGE 请求, 写入 CommitLog |
| `PullMessageProcessor` | 处理 PULL_MESSAGE 请求, 从 InMemoryQueueIndex 查询 |
| `AdminBrokerProcessor` | 处理 CREATE_TOPIC / GET_ALL_TOPIC_LIST |
| **`PullRequestHoldService`** | ⭐ v2 新增: 长轮询 (新消息毫秒级响应) |
| **`ScheduleMessageService`** | ⭐ v2 新增: 18 级延迟消息 (1s/5s/10s/.../2h) |

### z-mq-client (9 文件)

Producer / Consumer 客户端:

| 类 | 说明 |
|----|------|
| `DefaultMQProducer` | 同步发送 (含自动重试 + 路由查询) |
| `SendCallback` | 异步发送回调 |
| `DefaultMQPushConsumer` | Push 模式 (内部 = Pull + 长轮询模拟) |
| `DefaultMQPullConsumer` | 显式 Pull |
| `MessageListener.Concurrently` / `.Orderly` | 消费回调接口 |
| `MQClientInstance` | ⭐ v2 增强: NPE 防御 + channel 缓存管理 |
| `ConsumeConcurrentlyStatus` / `ConsumeOrderlyStatus` | 消费结果状态 |

### z-mq-console (前端)

Vue 3 控制台, 见 [z-mq-console/README.md](z-mq-console/README.md).

---

## 🔧 v2 增强 (本版本新增)

### 1. ConsumerOffsetManager (消费位点持久化)

```
位置: z-mq-store/.../config/ConsumerOffsetManager.java
```

**核心特性:**
- 按 `(topic, queueId, group)` 三元组存储消费位点
- 内存读写 O(1) ConcurrentHashMap
- 5s 周期 flush + 关闭强制 flush
- 原子写 (`.tmp` → `rename`) 避免崩溃半截文件
- 启动加载 (从 `consumer_offset.json` 重建)

**API:**
```java
ConsumerOffsetManager mgr = new ConsumerOffsetManager(storePath);
mgr.start();
mgr.commitOffset("OrderTopic", 0, "consumer_group_1", 12345L);
long offset = mgr.queryOffset("OrderTopic", 0, "consumer_group_1");
mgr.flushIfNecessary();  // Broker 周期调用
mgr.shutdown();          // 关闭时强制 flush
```

**测试:** 9 个单元测试 (commit/query/persist/reload/null-safety/flush)

### 2. PullRequestHoldService (长轮询)

```
位置: z-mq-broker/.../longpoll/PullRequestHoldService.java
```

**核心特性:**
- suspendPull 句柄可 await, awaitWakeup 阻塞至新消息/超时
- notifyMessageArrived 消息到达毫秒级唤醒 (替代 1s 短轮询)
- 15s 默认 hold timeout (可配)
- 守护线程 1s 周期扫描超时挂起
- maxHoldCount 限制 (默认 10000) 避免 OOM
- shutdown 唤醒所有挂起请求

**API:**
```java
PullRequestHoldService svc = new PullRequestHoldService(15_000L, 10_000);
svc.start();
SuspendedPull req = svc.suspendPull("OrderTopic", 0, 100);  // 返回句柄
boolean wokenByMsg = req.awaitWakeup();  // 阻塞至被唤醒
svc.notifyMessageArrived("OrderTopic", 0);  // 新消息到达时唤醒
svc.shutdown();
```

**测试:** 9 个单元测试

### 3. ScheduleMessageService (18 级延迟消息)

```
位置: z-mq-broker/.../delay/ScheduleMessageService.java
```

**核心特性:**
- RocketMQ 18 级内置延迟 (1s/5s/10s/30s/1m/.../2h)
- 自定义延迟配置: `'100ms 200ms 500ms 1s'` 等
- JDK `DelayQueue` 实现, 每个 level 一个守护线程
- `schedule(level, key, payload)` + `setListener(onExpired)`
- `cancel(level, key)` 在到期前可取消

**API:**
```java
ScheduleMessageService svc = new ScheduleMessageService();  // 18 levels
svc.setListener((key, payload) -> System.out.println("expired: " + key));
svc.schedule(1, "order_123", order);  // 1 秒后触发
svc.shutdown();
```

**测试:** 11 个单元测试

---

## 🔬 测试矩阵 (66+ 测试)

| 模块 | 测试类 | 测试数 | 覆盖范围 |
|---|---|---|---|
| common | MessageTest / MessageExtTest / SendResultTest / SendStatusTest | 10 | 基础数据结构 |
| common | BrokerDataTest / QueueDataTest / TopicConfigTest | 9 | 路由数据 |
| common | TopicRouteDataTest / RegisterBrokerResultTest / JsonCodecTest | 11 | 协议序列化 |
| common | MessageQueueTest / PullResultPayloadTest | 8 | 队列与拉取 |
| remoting | PairTest / RemotingHelperTest / RemotingSerializableTest | 8 | 公共类 |
| remoting | RequestCodeTest / RemotingSysResponseCodeTest | 4 | 协议码 |
| remoting | NettyServerConfigTest / NettyClientConfigTest | 4 | 配置 |
| remoting | NettyEncoderTest / NettyDecoderTest | 4 | 编解码 |
| remoting | ResponseFutureTest | 3 | Future |
| remoting | RemotingCommandBenchmarkTest | 2 | 性能 |
| store | MessageExtBrokerInnerTest / MessageStoreConfigTest / PutMessageResultTest | 5 | 数据类 |
| store | CommitLogTest / FlushDiskTypeTest / ReferenceResourceTest | 7 | 存储基础 |
| store | **ConsumerOffsetManagerTest** ⭐ | 9 | 位点持久化 |
| store | InMemoryQueueIndexTest | 6 | 进程内索引 |
| store | CommitLogThroughputBenchmarkTest | 1 | 吞吐基准 |
| nameserver | RouteInfoManagerTest | 4 | 路由表 |
| nameserver | NamesrvConfigTest / NameServerControllerTest | 3 | 配置 + 控制器 |
| client | ConsumerStatusTest / MessageListenerTest / MessageQueueContextTest | 6 | Consumer 接口 |
| client | SendCallbackTest / DefaultMQPullConsumerTest | 4 | Producer/Pull |
| client | MQClientInstanceTest | 5 | 客户端实例 |
| broker | BrokerConfigTest / BrokerControllerTest | 5 | Broker 基础 |
| broker | SendMessageProcessorTest / PullMessageProcessorTest / AdminBrokerProcessorTest | 9 | 处理器 |
| broker | **PullRequestHoldServiceTest** ⭐ | 9 | 长轮询 |
| broker | **ScheduleMessageServiceTest** ⭐ | 11 | 延迟消息 |
| broker | ProducerInstanceAccessTest | 2 | 反射工具 |
| broker | ClusterTestHelperTest | 1 | 集群测试工具 |
| broker | ZmqE2ETest | 5 | E2E |
| broker | FunctionalEdgeTest / ChaosTest / StabilityTest | 12+ 鲁棒性 |
| broker | MessageSizeLimitTest / ChannelTableStressTest | 6+ 边界 |
| broker | PerformanceBenchmarkTest | 2 | 性能基准 |
| broker | HalfOpenConnectionChaTest / DuplicateAndIdempotencyTest | 4+ 异常场景 |
| broker | ConcurrentChannelCacheBenchmarkTest | 1+ 并发压测 |
| **合计** | | **66+** | |

> 集成测试 (Performance/Chaos/Stability 等) 在 Maven 默认 `mvn test` 时被排除, 可单独运行.

---

## 📐 设计原则

1. **Producer-Consumer 解耦**: Producer/Consumer 通过 NameServer 发现 Broker, 不直接通信
2. **顺序写优先**: 所有 Topic 共享 CommitLog, 消除随机 IO (RocketMQ 经典设计)
3. **Netty 异步 + 长轮询**: 消息到 Consumer 端毫秒级响应
4. **CommitLog + ConsumeQueue 双层**: 写入只触 CommitLog, 查询走 ConsumeQueue
5. **18 级延迟**: 业务场景开箱即用, 无需外部调度系统
6. **心跳 + 心跳表**: Broker 主动注册, NameServer 30s 检测离线
7. **失败重试**: Producer 自动重试 + 路由刷新
8. **轻量级前端**: Vue 3 + Element Plus + Vite proxy, 部署简单

---

## 🚧 已知限制 (与 RocketMQ 对比)

| 维度 | Z-MQ | RocketMQ |
|------|------|----------|
| 持久化 | MappedByteBuffer 写入 + 周期 flush (部分实现) | 完整 WAL + Sync/Async 双刷 |
| 主从同步 | 未实现 | HAService (DLedger Raft) |
| 事务消息 | 未实现 | Half Message + 本地事务 + 回查 |
| 消息过滤 | 未实现 (Tag) | Tag + SQL92 |
| 索引文件 | InMemoryQueueIndex (重启清空) | 磁盘 ConsumeQueue + IndexFile |
| 大消息 (≥ 1MB) | 支持 | 支持 |
| 多语言客户端 | Java | Java / C++ / Go / Python |

未来 Roadmap 见 [z-mq/_doc/roadmap.md](_doc/roadmap.md).

---

## 🛠 复用 z-util

z-mq 依赖 z-util 公共工具库:

```xml
<dependency>
    <groupId>io.github.yuku123</groupId>
    <artifactId>z-util-core</artifactId>
    <version>1.0.9</version>
</dependency>
```

具体使用:
- `com.zifang.util.core.lang.RandomUtil` (UUID / 短 ID 生成) — 测试 / 启动脚本
- 其他工具类按需引入, 不引入不必要依赖

## 📚 参考资料

- **RocketMQ 源码**: `/Volumes/personal_folder/学习/source-from-github/490_rocketmq/`
- **RocketMQ 源码分析**: `z-biz-creator/z-biz-learning-yuque-loc/yuque/开源研究/002_源码分析/400_rocketmq/`
- **Kafka 源码分析**: `z-biz-creator/z-biz-learning-yuque-loc/yuque/开源研究/002_源码分析/116_kafka/`
- **NATS 源码分析**: `z-biz-creator/z-biz-learning-yuque-loc/yuque/开源研究/002_源码分析/1314_nats.c/`
- **Pulsar 源码分析**: `z-biz-creator/z-biz-learning-yuque-loc/yuque/开源研究/002_源码分析/134_pulsar/`

## 📜 版本历史

### v1.0.0-SNAPSHOT (2026-09-08)

**v2 增强 - ConsumerOffset / LongPolling / DelayMessage:**
- ConsumerOffsetManager: 消费位点持久化 + 重启恢复
- PullRequestHoldService: 长轮询 + SuspendedPull 句柄
- ScheduleMessageService: 18 级延迟消息 + 自定义配置
- BrokerController: 集成 3 个新组件 + 5s 周期 flush

**v1 完善 - 健壮性 + 测试覆盖:**
- NPE 防御 (channel/remoteAddress null 处理)
- 大量单元/集成测试 (66+ 全部通过)
- 测试基础设施 (logback + log4j-slf4j-impl)
- 新增 BenchResult / ProducerInstanceAccess / ClusterTestHelper

**v0 - 基础架构:**
- NameServer + Broker + Client + Store + Common + Remoting 6 模块
- CommitLog + MappedFile + MappedFileQueue
- Netty Remoting Server/Client + RemotingCommand 协议
- RouteInfoManager + ConcurrentHashMap 路由表
- SendMessageProcessor / PullMessageProcessor / AdminBrokerProcessor
- DefaultMQProducer / DefaultMQPushConsumer / DefaultMQPullConsumer

## 🤝 贡献指南

1. Fork 仓库, 创建特性分支
2. 编写代码 + 测试 (新功能必须有单元测试覆盖)
3. 遵循现有代码风格 (Java + SLF4J/Log4j2 + Lombok 选型)
4. 提交前运行 `mvn test` 确保所有测试通过
5. 提交信息格式: `type(scope): subject` (feat/fix/docs/test/refactor)

## 📞 维护

- 模块维护人: z-opc-foundation
- 反馈渠道: GitLab Issues
- 文档: 本 README + 各子模块 README + 代码内 JavaDoc / TypeDoc
- 前端文档: [z-mq-console/README.md](z-mq-console/README.md)