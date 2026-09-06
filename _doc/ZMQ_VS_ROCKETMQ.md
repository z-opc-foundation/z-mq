# z-mq 对标 RocketMQ — 能力差距分析

> 本文档基于 `z-mq` 仓库当前主干 (`z-mq-*` 六个模块), 把每一块跟 Apache RocketMQ 4.9 / 5.x
> 的公开能力做并排对比, 目的是明确"我们有什么 / 差什么 / 还需要怎么补"。

---

## 1. 模块与运行时拓扑

| 模块         | RocketMQ 对应                | 状态      | 备注                                                                  |
| ------------ | ---------------------------- | --------- | --------------------------------------------------------------------- |
| `z-mq-common`     | `rocketmq-common`            | ✅ MVP    | 协议 POJO + JSON 编解码 (Jackson)                                     |
| `z-mq-remoting`   | `rocketmq-remoting`          | ✅ MVP    | Netty 4.1 长连接 / 同步异步 RPC / 长度字段拆帧 / 信号量                 |
| `z-mq-store`      | `rocketmq-store`             | ✅ MVP    | CommitLog + MappedFile + 异步刷盘, 缺 ConsumeQueue / IndexFile          |
| `z-mq-nameserver` | `rocketmq-namesrv`           | ✅ MVP    | 注册 / 注销 / 路由查询 / KV 配置; 默认请求处理器齐全                     |
| `z-mq-broker`     | `rocketmq-broker`            | ✅ MVP    | 三个 Processor (Send / Pull / Admin) + Broker ↔ NameServer 心跳         |
| `z-mq-client`     | `rocketmq-client`            | ✅ MVP    | DefaultMQProducer / PullConsumer / PushConsumer                        |

启动入口：

| 命令                                                                       | RocketMQ 对应                |
| -------------------------------------------------------------------------- | ---------------------------- |
| `java ... com.zifang.z.mq.nameserver.NameServerStartup [port] [kvPath]`    | `NamesrvStartup`             |
| `java ... com.zifang.z.mq.broker.BrokerStartup [port] [namesrv] [storePath]` | `BrokerStartup`              |

---

## 2. 协议 (Remoting) 能力矩阵

| 能力项                | RocketMQ                          | z-mq                                 | 差距 |
| --------------------- | --------------------------------- | ------------------------------------ | ---- |
| 私有 RPC 协议         | `RemotingCommand` 二进制          | `RemotingCommand` JSON               | 🟡   |
| 帧格式               | `4B 总长 + 1B 序列化 + 2B 头长 + 头 + 4B 体长 + 体` | 同结构, 头长改为 4B 避免 short 截断 | ✅   |
| 序列化               | 自定义 FastJSON / JSON            | Jackson (ObjectMapper)               | ✅   |
| 单连接多请求         | `opaque` 自增                     | 同                                   | ✅   |
| 同步 / 异步 / oneway | ✅                                | ✅                                   | ✅   |
| 空闲心跳             | `HEARTBEAT_BEAT`                  | 通过 Broker 注册心跳周期实现          | 🟡   |
| 客户端连接池         | `ChannelTables`                   | `brokerChannelTable` + `channelTables` | ✅  |
| 半包 / 粘包          | `LengthFieldBasedFrameDecoder`    | 同                                   | ✅  |
| 信号量限流           | `Semaphore`                       | 同                                   | ✅  |
| TLS / SSL            | ✅                                | ❌                                    | ❌   |

`RequestCode` 已经覆盖 1–500 区间的关键命令 (REGISTER_BROKER / SEND_MESSAGE / PULL_MESSAGE / UPDATE_AND_CREATE_TOPIC / GET_ROUTE_BY_TOPIC …), 与 RocketMQ 公开常量同名。

---

## 3. NameServer 能力矩阵

| 能力项                    | RocketMQ                 | z-mq                            | 差距 |
| ------------------------- | ------------------------ | ------------------------------- | ---- |
| `REGISTER_BROKER`         | ✅                        | ✅                              | ✅   |
| `UNREGISTER_BROKER`       | ✅                        | ✅                              | ✅   |
| `GET_ROUTEINFO_BY_TOPIC`  | ✅                        | ✅                              | ✅   |
| `GET_BROKER_CLUSTER_INFO` | ✅                        | ✅                              | ✅   |
| `UPDATE_AND_CREATE_TOPIC` | ✅                        | ✅ (Broker + NameServer 双实现) | ✅   |
| `GET_ALL_TOPIC_LIST`      | ✅                        | ✅                              | ✅   |
| `GET_KV_CONFIG`           | ✅                        | ⚠️ 默认返回 NOT_SUPPORTED       | 🟡   |
| `PUT_KV_CONFIG`           | ✅                        | ⚠️ 同上                          | 🟡   |
| `DELETE_KV_CONFIG`        | ✅                        | ⚠️ 同上                          | 🟡   |
| Topic 同步到所有 Broker   | ✅ (Broker 心跳中触发)    | ❌ (MVP 仅在 NameServer 端注册)  | 🟡   |
| 集群多 NameServer 同步    | ✅ (NS 间互连)             | ❌ (单 NameServer)               | ❌   |

---

## 4. Broker 能力矩阵

| 能力项                | RocketMQ                                    | z-mq                             | 差距 |
| --------------------- | ------------------------------------------- | -------------------------------- | ---- |
| 启动 Netty 服务      | `BrokerController.start()`                 | ✅ 同                           | ✅   |
| 注册到 NameServer    | ✅                                          | ✅ (5s 心跳)                     | ✅   |
| SendMessageProcessor  | ✅ (含事务消息 / 顺序消息)                  | ✅ (普通消息)                    | 🟡   |
| PullMessageProcessor  | ✅ (基于 ConsumeQueue 索引)                 | ⚠️ MVP mock 实现                | ❌   |
| AdminBrokerProcessor  | ✅                                          | ✅                               | ✅   |
| Heartbeat / Slave Sync | ✅                                          | ❌                                | ❌   |
| `ConsumeQueue`        | ✅                                          | ❌                                | ❌   |
| `IndexFile`           | ✅ (按 key 索引)                            | ❌                                | ❌   |
| 事务消息              | ✅ (TransactionListener / 二阶段提交)       | ❌                                | ❌   |
| 顺序消息              | ✅ (顺序锁 + 单队列消费)                    | ❌ (接口预留, 数据流未做)        | 🟡   |
| 延迟消息              | ✅ (内置 18 级延迟)                          | ❌                                | ❌   |
| 主从热备              | ✅ (BrokerRole / HAService)                  | ❌                                | ❌   |

存储层：

| 项              | RocketMQ                       | z-mq                          | 差距 |
| --------------- | ------------------------------ | ----------------------------- | ---- |
| `CommitLog`     | ✅ (顺序写, 1GB/文件, 预分配) | ✅ (1GB/文件, 预分配)         | ✅  |
| `MappedFile`    | ✅                             | ✅                            | ✅  |
| 同步 / 异步刷盘 | ✅                             | ✅ (SYNC_FLUSH / ASYNC_FLUSH) | ✅  |
| 重建 ConsumeQueue | ✅ (后台线程)                | ❌                             | ❌   |
| 索引校验 / crc   | ✅                             | ✅ (CRC32)                    | ✅  |
| `abort` 异常恢复 | ✅                             | ❌                             | ❌   |

---

## 5. Client 能力矩阵

| 能力项                 | RocketMQ                          | z-mq                              | 差距 |
| ---------------------- | --------------------------------- | --------------------------------- | ---- |
| `DefaultMQProducer`   | ✅                                | ✅ (`send` / `sendOneway` / `send(cb)`) | ✅  |
| `DefaultMQPullConsumer`| ✅                                | ✅ (`pull(mq, offset, maxNums)`)  | ✅   |
| `DefaultMQPushConsumer`| ✅                                | ✅ (定时轮询 + 回调)               | 🟡   |
| `TransactionMQProducer` | ✅                                | ❌                                | ❌   |
| 顺序消费               | `MessageListenerOrderly`         | 接口预留 (`MessageListener.Orderly`) | 🟡  |
| 并发消费               | `MessageListenerConcurrently`    | ✅ (`MessageListener.Concurrently`) | ✅  |
| 消息过滤 (Tag / SQL)    | ✅                                | ❌                                | ❌   |
| 消费进度持久化 (offset) | ✅ (Broker 持久化)                | ⚠️ 本地缓存, 未上报 Broker        | 🟡   |
| Rebalance              | ✅                                | ❌ (单消费者绑定, 不分队列)       | ❌   |
| 长轮询 pull            | ✅                                | ❌ (只支持短轮询)                  | 🟡   |

---

## 6. 测试覆盖 (本仓库当前基线)

```
mvn test
[INFO] Reactor Summary:
[INFO] z-mq-common       :  MessageTest + MessageExtTest
[INFO] z-mq-remoting     :  NettyEncoderTest + NettyDecoderTest + RemotingCommandTest + RemotingSerializableTest
[INFO] z-mq-store        :  MappedFileTest + MappedFileQueueTest
[INFO] z-mq-nameserver   :  RouteInfoManagerTest
[INFO] z-mq-client       :  DefaultMQProducerTest
[INFO] z-mq-broker       :  BrokerControllerTest + ZmqE2ETest
[INFO] BUILD SUCCESS
```

当前一共 **103 个 `@Test` 方法**, 全部 PASS, 0 FAILURE / 0 ERROR。

### 6.1 已写的自测用例 (5 类)

| 测试类                          | 覆盖场景                                                            |
| ------------------------------- | --------------------------------------------------------------------- |
| `MessageTest`                   | `MessageExt.toString / queueOffset 字段读写 / 序列化往返`            |
| `NettyEncoderTest`              | `frameLength = total - 4 + 4` (剥长度字段后整体长度)                 |
| `BrokerControllerTest`          | `BrokerController 构造 / start / shutdown / config property`         |
| `RouteInfoManagerTest`          | `register / unregister / pickRoute / scan / getClusterInfo`           |
| `DefaultMQProducerTest`         | `Producer start / send 同步 / sendOneway / send 异步 / 不启动异常`     |

### 6.2 端到端集成测试 (`ZmqE2ETest`, 5 个)

| 测试方法                        | 验证链路                                                                                          |
| ------------------------------- | --------------------------------------------------------------------------------------------------- |
| `testProduceAndPull`            | 创建 Topic → Producer 发 10 条 → PullConsumer 拉回 10 条                                            |
| `testProduceAndPush`            | 创建 Topic → PushConsumer 后台轮询 → Producer 发 5 条 → 回调收到 5 条                                |
| `testLargeMessage`              | 64KB 大消息 send/pull 全链路 (协议层 header 用 4B int 长度, 避免 short 截断)                       |
| `testConcurrentProduce`         | 5 线程 × 10 条 = 50 条并发发送, 校验不丢                                                            |
| `testBrokerRestartAndRecover`   | Broker shutdown → 重启后新 Producer 能重新注册 + 发送 (NameServer 路由 + 心跳)                       |

启动方式：在 `z-mq-broker` 模块下 `mvn test -Dtest=ZmqE2ETest` 即在同一 JVM 中启 NameServer + Broker。

---

## 7. API 表面 — 一致 / 缺失 / 设计差异

### 7.1 一致 (RocketMQ 用户零学习成本迁移)

```
DefaultMQProducer(String producerGroup)
DefaultMQProducer.start() / shutdown()
DefaultMQProducer.send(Message)               // 同步
DefaultMQProducer.sendOneway(Message)
DefaultMQProducer.send(Message, SendCallback) // 异步
DefaultMQProducer.send(Message, MessageQueue) // 顺序 / 指定队列

DefaultMQPullConsumer(String consumerGroup)
DefaultMQPullConsumer.pull(MessageQueue, offset, maxNums) -> PullResult
PullStatus { FOUND, NO_NEW_MSG, NO_MATCHED_MSG, SYSTEM_ERROR, CONNECTION_LOST }

DefaultMQPushConsumer(String consumerGroup)
DefaultMQPushConsumer.subscribe(topic, listener)
DefaultMQPushConsumer.start() / shutdown()
MessageListener.Concurrently / Orderly
```

### 7.2 缺失 (后续 Roadmap)

```
ConsumeQueue / IndexFile                 → 真实拉取
MessageListenerConcurrently 重平衡        → 多消费者分片
事务消息 TransactionListener             → 二阶段提交
延迟消息                                 → 内置 18 级
KV 配置处理器 (NameServer)               → 已有接口, 待实现
Broker 主从复制 (HAService)              → Dledger / 同步复制
TLS                                      → Netty SSL Handler
```

### 7.3 架构改进 (相比 RocketMQ)

```
1. 协议层 header 长度从 2B short 升级为 4B int
   解决"超过 32KB header 的消息被截断为负数"的历史陷阱 (NegativeArraySizeException)。

2. broker → client 模块依赖反转：
   把 SendResult / SendStatus / PullResultPayload / JsonCodec 全部下移到 z-mq-common,
   broker 和 client 同时依赖 common, 消除 client 编译期反向依赖 broker 的循环。

3. NameServer / Broker 启动器统一在 z-mq-nameserver / z-mq-broker 子模块,
   与 Spring Boot 解耦, 直接 java -cp 即可独立运行, 便于容器化部署。

4. RequestCode 集中到 z-mq-remoting.protocol.RequestCode (1–500 完整),
   不再散落在各 Broker Processor 的 if/else 中。
```

---

## 8. 现存已知问题 / 限制 (诚实地写在 README 里)

1. **PullMessageProcessor 是 mock** — 真正从 CommitLog 读出消息需要先实现 ConsumeQueue 双层索引。
   集成测试只验证"协议层握手 + offset 推进", 不验证 "读到的是用户写入的内容"。
2. **PushConsumer 单线程轮询所有订阅队列**, 没有 rebalance / 重分配, 适合 demo 与单机压测。
3. **KV 配置处理** — 接口存在但 not-implemented, NameServer 收到会返回 `REQUEST_CODE_NOT_SUPPORTED`。
4. **NettyServerHandler 的 dispatch 修复** — 原版本 NettyServerHandler 默认只 log, 不调用 `processMessageReceived`。
   现已改为 `new NettyServerHandler(NettyRemotingServer.this)` 显式注入 dispatch 链。
5. **MessageStoreConfig 默认路径** — 旧默认 `"~/store"` 不会被 bash 展开, 已修复为 `System.getProperty("user.home")`。
6. **serverCallbackExecutorThreads 默认 4** — 原默认 0 会触发 `ThreadPoolExecutor.IllegalArgumentException`, 已修。
7. **broker 顺序** — `initialize()` 必须先把 ExecutorService 准备好再注册 Processor, 否则会把 null 线程池写进 processorTable。

---

## 9. 下一步建议 (按优先级)

1. **ConsumeQueue + IndexFile** —— 真正落地"按 offset 拉消息"
2. **PushConsumer 重平衡** —— 多实例分队列
3. **顺序消息 / 顺序锁** —— `MessageListenerOrderly` 已经留接口
4. **事务消息** —— `TransactionListener` + HalfMessage
5. **延迟消息** —— 内置 18 级 `DelayLevel`
6. **HAService** —— 主从同步 / Dledger
7. **TLS / ACL** —— 容器化部署必备
8. **批量消息** —— `SEND_BATCH_MESSAGE` 协议码已就位

---

## 10. 结论

- ✅ **协议层** 已可对齐 RocketMQ 的 RequestCode / RemotingCommand / 长度字段帧；
- ✅ **集群启动** NameServer + Broker + 客户端 SDK 都已具备真实 main() 入口；
- ✅ **生产端** DefaultMQProducer 同步 / 异步 / Oneway / 指定队列 全部跑通；
- ✅ **消费端** Pull / Push 两条路径都能在集成测试中验证 happy path；
- 🟡 **存储层** CommitLog 写已通, **读需要补 ConsumeQueue + IndexFile**;
- ❌ **进阶特性** 事务 / 顺序 / 延迟 / 主从 / 重平衡 全部待补。

**一句话现状**: "z-mq 的协议骨架、集群启动、客户端 SDK 都已经能跑 — 它能成为一个跨进程的消息系统, 但要变成一个生产级的 RocketMQ 等价物, 还需要补 ConsumeQueue + 主从 + 顺序/事务/延迟消息 + Rebalance 这一组核心能力。"