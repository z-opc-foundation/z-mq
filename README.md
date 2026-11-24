# z-mq

> 自研分布式消息队列 —— NameServer + Broker + Client(Producer/Consumer) + Store + Remoting，
> **形状**对标 Apache RocketMQ（类名、命令字、Topic/Queue/Offset 语义），但**自有一套 Netty 私有协议**，
> 与 RocketMQ 的报文格式/NameServer 协议**不互通**。

## 它解决什么问题

z-opc 基座需要一条不依赖外部 Kafka/RocketMQ 集群的异步消息通路：进程间事件通知、任务解耦、
延迟投递与死信兜底。z-mq 把 NameServer（路由注册中心）、Broker（存储 + 投递）、Client（生产/消费）、
Store（CommitLog 顺序写 + 内存队列索引）、Remoting（Netty 长连接 RPC）五层全部自养在一个仓里，
对外发布 9 个 jar + 1 个聚合 POM 到 Maven Central，另带一个 Vue 3 控制台工程。

⚠️ 读这份 README 的正确姿势：**下面「已接线」那一栏才是你现在能用的能力**。
这个仓里有一批"类已经写好、协议码已经定义、但没有任何调用方"的形状（延迟消息、广播消费、
批量发送、消息轨迹、ACL、Prometheus 指标、rebalance）。它们在旧 README 里被写成 ✅，
本版本按 `src/main` 实测逐条改正。判据都是同一句：`grep -rn '<类名/方法名>' z-mq-*/src/main/java`
除自身外是否命中。

---

## 📋 基本信息

| 字段 | 值（全部实测，非记忆） |
|------|-----|
| **仓库** | `z-mq`（remote `git@github.com:z-opc-foundation/z-mq.git`，分支 `main`） |
| **Maven 坐标** | `io.github.yuku123:z-mq:${revision}`，`packaging=pom` |
| **当前版本** | `<revision>` = **`1.3.3`**（CI-friendly versions + 常开 flatten-maven-plugin 1.5.0） |
| **父项目** | `io.github.yuku123:z-boot-parent:1.2.4`（`<relativePath/>` 留空，parent 在 repo1 不在磁盘；repo1 实测 200） |
| **版本权威链** | `z-boot-parent` → 第三方地板 + 兄弟仓权威表（两段内容均已并进 parent 自身，`z-boot-dependencies` / `z-boot-fleet` 两个文件夹随 2026-10-08 火车制拓扑退役）；`z-boot-parent` 的 `<z-mq.version>` 现读 **`1.3.3`**（`z-boot/z-boot-parent/pom.xml` 一处）⇒ 本仓 revision 与 parent 槽位同格 |
| **Maven Central** | **已发布**：`z-mq` / `z-mq-common` / `z-mq-remoting` / `z-mq-store` / `z-mq-nameserver` / `z-mq-broker` / `z-mq-client` / `z-mq-spring-boot-starter` / `z-mq-tools` 九个坐标的 `1.3.1` 逐个 ranged GET 实测 **200**（`1.3.0` 同样 200）；组路径 `io/github/yuku123`。2026-10-20 新增的 `z-mq-broker-admin` 不在本实测列表内 —— 它是否已上架只认 repo1 回读（本地 `~/.m2` 里它只有本地安装记录、没有 `central=` 条目） |
| **默认端口** | NameServer **9876**、Broker **10911**（写死在两个 `Startup` 的 `args.length == 0` 分支）；`NettyServerConfig.listenPort` 字段默认 **8888**，但两个 `Startup` 都会覆盖它 ⇒ 8888 不是任何进程的实测端口；控制台 Vite dev **8081** |
| **运行口径** | Java 17（根 `<java.version>8</java.version>`）· Spring Boot **仅** starter 模块用 2.7.18 · 日志 Log4j2 · 序列化 Jackson JSON |
| **模块数** | Maven reactor **9 个**；另有 `z-mq-admin`（npm/Vue 工程，**不在 reactor、不在任何 pom 的 `<modules>` 里**） |
| **最近更新** | 2026-09-30 |

---

## ✅ 能力清单（已接线，`src/main` 里找得到调用方）

| 能力 | 实现位置 | 实测口径 |
|------|----------|----------|
| Broker 注册/注销 + 路由查询 | `nameserver/RouteInfoManager`、`DefaultRequestProcessor`（case 1/2/3/311/4/8/5）、10s 扫描不活跃 broker | NameServer 侧只有这 7 个 case，见下方命令字表；**topic 路由随 broker 重启存活**（broker 掉线只摘它的地址，不删 topic 的队列项；broker 重新注册后路由自动恢复，见 `RouteInfoManager#syncTopicsOfBroker`） |
| 同步 / 异步 / oneway 发送 | `client/producer/DefaultMQProducer#send`、`#send(msg, SendCallback)`、`#sendOneway` | oneway 真走 `invokeOneway`；异步回调固定在 remoting `callbackExecutor` 线程 |
| 指定队列发送（顺序的生产侧入口） | `DefaultMQProducer#send(Message, MessageQueue)` | 没有 `MessageQueueSelector` 接口，只有"你自己把 `MessageQueue` 传进来" |
| 拉取消费 + 长轮询 | `broker/processor/PullMessageProcessor`、`longpoll/PullRequestHoldService` | 空拉按预算挂起，`MAX_SUSPEND_BUDGET_MILLIS = 30_000`；新消息到达即唤醒并回写 `suspendWakeup` |
| 推送消费（伪 push） | `client/consumer/DefaultMQPushConsumer` | **单条** `PushConsumerPullThread` 定时 `doPull` → 串行回调 listener；不是 broker 推 |
| Tag 过滤 + SQL92 过滤 | `common/filter/TagFilter`、`Sql92Filter`（`SqlParser`/`SqlContext`），broker 侧在 `PullMessageProcessor` 构造 | SQL92 支持 `IS [NOT] NULL`/`= <>`/`> < >= <=`/`BETWEEN`/`IN`/`AND OR NOT`/括号；表达式异常 ⇒ 该消息被过滤掉 |
| 消费位点提交与跨重启恢复 | `broker/processor/ConsumerOffsetProcessor`（码 220）+ `store/config/ConsumerOffsetManager`；读取边 `PullMessageProcessor#resolveStartOffset` | 请求不带 offset 时 broker 用已提交位点起读；5s 周期 flush；验收 `ConsumerOffsetRestartE2ETest` |
| Topic 配置持久化 | `store/config/TopicConfigManager`（写边 `AdminBrokerProcessor`，`initialize()` 里先加载） | 重启后不重发 CREATE 也能读到原队列数（`TopicConfigRestartE2ETest`） |
| 建 Topic 的运维通路 | `z-mq-tools`：`ZmqAdmin` + `ClusterAdmin#createTopic` → 码 5 | `ZmqAdmin` 注册的命令只有 4 条：`topicList`/`topicRoute`/`brokerList`/`createTopic` |
| 事务消息（半消息 / 二次确认 / 回查 / 重启恢复） | `broker/processor/TransactionMessageProcessor`（201/251/250）、`transaction/TransactionStateManager`、`TransactionStateRecovery`、`TransactionCheckService`；客户端 `TransactionMQProducer` | 半消息走独立码 201（不与 200 同码分诊）；`recover()` 从 CommitLog 重建 pending 集合；broker 侧回查拿不到本地事务结论时恒返回 `UNKNOWN`，定论必须由 producer 用 251 送进来 |
| 消费失败重投 + 死信 | `client/consumer/retry/ConsumeRetryService`（`DEFAULT_MAX_RECONSUME_TIMES = 16`）、`RetryPolicy.STEPPED/FIXED`、`DeadLetterQueue`（Topic 名 `%DLQ%{group}`）、`BrokerBackedRetryTransport` | 重投副本与死信都经真 `SEND_MESSAGE` 外投；⚠️ 死信 Topic **必须先注册**（本仓没有 autocreate 通路），见 `_doc/007_backlog/feature006_dlq_topic_registration/` |
| 顺序存储 + 崩溃恢复 | `store/log/CommitLog`、`MappedFile`、`MappedFileQueue`、`MessageCodec`（逐条 CRC） | `CommitLog.load()` 无条件全量扫描重建内存索引；`MAX_MESSAGE_SIZE = 4 MiB`，`mappedFileSizeCommitLog = 1 GiB`；记录的 `bodyCRC` 逐条真算真验，**协议层 JSON 往返也不丢**（`MessageExtBrokerInner` 不再重复声明父类同名字段，守卫 `MessageExtBrokerInnerJsonTest`） |
| 同步刷盘 / 异步刷盘 | `store/log/FlushDiskType`（默认 `ASYNC_FLUSH`）、`MessageStoreConfig.syncFlushTimeout = 5000` | 同步刷盘超时 ⇒ broker 明确回 `SendStatus.FLUSH_DISK_TIMEOUT` |
| Slave 元数据同步 | `broker/slave/SlaveSynchronize` + `outapi/BrokerOutAPI`（320/321/322/323/324），`brokerId != 0` 时启动，默认 30s 一轮 | 同步的是 TopicConfig / ConsumerOffset / DelayOffset / 版本号；订阅组那份是空 wrapper（源码自陈 MVP 未实现） |
| Spring Boot 接入 | `z-mq-spring-boot-starter`：`ZmqAutoConfiguration` + `ZmqProperties` + `ZmqTemplate` + `@ZmqListener` + `@EnableZmq` + `META-INF/spring.factories` | 配置前缀是 **`zmq`**（不是 `z.mq`） |
| NameServer KV 配置 | `nameserver/kvconfig/KVConfigManager`（落 `${user.home}/zmq/namesrv/kvConfig.json`） | 类与持久化真实存在，但 **39 个命令字里 `PUT/GET/DELETE_KV_CONFIG` 三个零引用** ⇒ 外部只能靠进程内调用，走不了网络 |

### ⚠️ 只有类型、没有通路（旧 README 写成 ✅ 的那一批）

| 旧 README 的说法 | `src/main` 实测 |
|---|---|
| "✅ 延迟消息（18 个固定延迟级别）" | `ScheduleMessageService` 18 级 DelayQueue + 单测齐备，但 `schedule(...)` 在 `src/main` **零调用方**；`Message#setDelayTimeLevel/getDelayTimeLevel`（property key `DELAY`）除自身与 `MessageTest` 外无人读写 ⇒ **设了级别也不会延迟**，消息立刻可见。排产归属见 `_doc/007_backlog/feature014_delay_message_wiring/` |
| "✅ 广播消费（`MessageModel.BROADCASTING`）" | `DefaultMQPushConsumer` 存了 `messageModel` 字段、只有 getter/setter；`doPull()` 注释自陈"集群模式：当前简化实现也拉取所有队列" ⇒ 同组多实例各自消费全量，**效果既不是集群也不是广播** |
| "✅ 顺序消息（全局/局部顺序，通过 `MessageQueueSelector`）" | 没有 `MessageQueueSelector` 这个类型；rebalance 策略类（`AllocateMessageQueueAveragely`/`ConsistentHash`）只有单测读者、没接进消费者；`MessageListener.Orderly` 有分发分支，但推送消费是单线程串行，既没有队列锁也没有 per-queue 线程 |
| "✅ 批量消息（单批 ≤ 4MiB / ≤ 1024 条）" | `common/message/BatchMessage` 在 `src/main`/`src/test` 均无读者，命令字 `SEND_BATCH_MESSAGE(202)` 零引用 ⇒ 无批量通路 |
| "✅ Producer 自动重试（默认 3 次，可配）" | producer 侧没有任何重试次数/退避字段，`SendResult` 一次定论；`latency/LatencyFaultTolerance` 两类的读者只有测试。未覆盖面已登记在 `_doc/007_backlog/feature012_producer_retry_uncovered_shapes/`（该条登记的代码当时在 wip 工作树，`main` 上现测仍是 0 命中） |
| "✅ 消息轨迹（Trace 全链路）" | `common/trace/TraceService`/`TraceBean`/`TraceType` 的引用只在 `z-mq-common` 自己包内，producer/broker/consumer 一条都不上报 |
| "✅ ACL 访问控制（`aclEnable=true`）" | `remoting/acl/AccessValidator` + `broker/acl/PlainTextAccessValidator` 存在，但没有任何处理器调用它，也没有 `aclEnable` 这个开关（全仓 0 命中）；等拍板见 `_doc/007_backlog/feature001_acl/` |
| "✅ Master-Slave 同步双写（`SYNC_MASTER`）/ 自动故障切换" | 全仓**没有** `BrokerRole` 类型（只有 `brokerId == 0` 判 master）；`HAService#waitForSlaveAck` 的读者只有 `HAServiceTest`；`DefaultHAService#pushToSlave` 是只打 debug 日志的桩（源码注释"MVP 简化: 不通过 Netty 推送"），`HA_PUSH_COMMITLOG(350)` 零引用；`SendStatus.FLUSH_SLAVE_TIMEOUT`/`SLAVE_NOT_AVAILABLE` 两个枚举值无人产出 ⇒ **主从只同步元数据，消息字节不复制，也没有同步双写语义**；等拍板见 `_doc/007_backlog/feature002_sync_master/` |
| "✅ Prometheus 指标 + Grafana Dashboard 模板" | `broker/metrics/BrokerMetrics`（含 `toPrometheusFormat()`）**零调用方**；仓里没有 Prometheus 采集端点、没有 dashboard 文件 —— HTTP 面只有 `z-mq-broker-admin`（`:9090`，带 actuator）与 starter 的运维代理，两者都不吐 Prometheus 指标。排产归属见 `_doc/007_backlog/feature015_broker_metrics_export/` |
| "✅ 可视化控制台（React + Vite + AntD，端口 8080）" | 实际是 **Vue 3 + Element Plus + Pinia + ECharts**，Vite dev 端口 **8081**；代理目标 `z-mq-broker-admin:9090` **如今在本仓存在**（2026-10-20 新增的 `z-mq-broker-admin` 模块，`web/ZMqAdminController` 提供 REST），`src/api/index.ts` 也已从"mock 数据"改成真调用 |
| "✅ Topic 自动创建 / 队列数动态调整" | 全仓 0 处 `autoCreate`；`SendMessageProcessor` 根本不查 `TopicConfig`（写了不报错），但路由只认已在 NameServer 注册过的 topic ⇒ 不给 5 号命令字建 topic，客户端就报 `No route for topic` |
| "存储层 CommitLog + ConsumeQueue + Index" | **没有 `ConsumeQueue` 类，也没有 `IndexFile`**；队列位点是 `store/log/InMemoryQueueIndex`（进程内 `topic@queueId → offset` 表，重启靠 `CommitLog.load()` 全量扫 + CRC 重建） |
| 性能基准表（18,000 / 65,000 QPS、P99 5ms 等） | 仓里没有任何一份能对得上这些读数的基准产物；只有 `PerformanceBenchmarkTest`/`ConcurrentChannelCacheBenchmarkTest` 两支挂钟阈值用例，且被 surefire **默认排除**（见「测试」） ⇒ 本 README 不再保留该表 |

---

## 🏗️ 项目结构

```
z-mq/
├── pom.xml                     # 聚合 POM：parent=z-boot-parent:1.2.4，<revision>=1.3.1，9 个 module
├── README.md                   # 本文件
├── LICENSE                     # MIT
├── _doc/006_release/sonatype-limit-request-email.txt   # 一封纯文本邮件草稿：向 Sonatype 申请提高发布配额（不是配置，也不是代码）
├── z-mq-common/                # 协议 POJO 与共享类型（28 个 main 类）
│   └── com/zifang/z/mq/common/ #   TopicConfig/QueueData/BrokerData/TopicRouteData/MessageQueue、
│                               #   message/{Message,MessageExt,MessageModel,BatchMessage}、
│                               #   protocol/{SendResult,SendStatus,PullResultPayload}、filter/{Tag,Sql92}、
│                               #   ha/{DataVersion,*SerializeWrapper}、trace/*、util/JsonCodec
├── z-mq-remoting/              # Netty 通信层（24）：RemotingCommand 编解码、Netty{Server,Client}Handler、
│                               #   ServiceThread、ResponseFuture、异常族、acl/AccessValidator
├── z-mq-store/                 # 存储层（16）：CommitLog、MappedFile(+Queue)、MessageCodec(CRC)、
│                               #   InMemoryQueueIndex（代替 ConsumeQueue/IndexFile）、config/{Topic,ConsumerOffset}Manager、
│                               #   ha/HAService 接口、FlushDiskType
├── z-mq-nameserver/            # NameServer（6）：NameServerStartup(main)、NameServerController、
│                               #   RouteInfoManager、KVConfigManager、DefaultRequestProcessor
├── z-mq-broker/                # Broker（21）：BrokerStartup(main)、BrokerController、
│                               #   processor/{Send,Pull,ConsumerOffset,AdminBroker,TransactionMessage}、
│                               #   transaction/{State,Check,Recovery}、delay/ScheduleMessageService、
│                               #   longpoll/PullRequestHoldService、ha/DefaultHAService、slave/SlaveSynchronize、
│                               #   outapi/BrokerOutAPI、acl/PlainTextAccessValidator、metrics/BrokerMetrics
├── z-mq-client/                # 客户端（24）：MQClientInstance（路由缓存 + selectOneMessageQueue + RPC 门面）、
│                               #   producer/{DefaultMQProducer,TransactionMQProducer,SendCallback,TransactionListener}、
│                               #   consumer/{DefaultMQPushConsumer,DefaultMQPullConsumer,MessageListener,MessageQueueContext}、
│                               #   consumer/retry/*、consumer/rebalance/*、producer/latency/*
├── z-mq-spring-boot-starter/   # Spring Boot 自动装配（8）：ZmqAutoConfiguration、ZmqProperties(prefix=zmq)、
│                               #   ZmqTemplate、@ZmqListener、@EnableZmq、META-INF/spring.factories、
│                               #   host/{MqHostAutoConfiguration,MqProxyController,ZMqEmbeddedServerConfig}
├── z-mq-tools/                 # CLI（7）：ZmqAdmin + admin/ClusterAdmin + command/{TopicList,TopicRoute,BrokerList,CreateTopic}
├── z-mq-broker-admin/          # 控制台后端（5）：ZMqAdminApplication、web/ZMqAdminController（REST，:9090）、
│                               #   web/dto/{Dtos,MessageQueryRequest}、service/AdminService
├── z-mq-admin/                 # ⚠ npm 工程（Vue 3 + Vite + TS），不在 Maven reactor，package.json 里 private=true
│   ├── package.json            #   name=z-mq-admin, version=1.0.0, scripts: dev/build/preview/lint/type-check
│   └── src/{api,router,stores,views}/  # 7 个页面（Dashboard/Cluster/Topics/Messages/Consumers/Delay/Settings）
└── _doc/                       # 文档，见文末「文档目录」
```

模块依赖方向（各模块 pom 现读）：
`common ← remoting ← {store, client, nameserver, tools}`，`broker ← {store, remoting, nameserver, client}`，
`broker-admin ← {tools, common}`，
`spring-boot-starter ← client`。**`z-mq-broker` 依赖 `z-mq-client`**（端到端用例住在 broker 模块里，
所以 E2E 测试放 `z-mq-broker/src/test`）。**`z-mq-spring-boot-starter` 的运行期依赖仍然只有 client**
（2026-10-22 起它的 `pom.xml` 多了两条 **test scope** 的 `z-mq-broker` / `z-mq-nameserver` ——
starter 自己的测试夹具要起一套本 JVM 的 namesrv + broker 做端到端取证；test scope 不进任何消费者的 classpath）。
9 个模块 POM 均无 `maven.deploy.skip` ⇒ 十个坐标全都会发布。

---

## 🔧 技术栈

| 层级 | 技术（版本按 pom 现读） |
|------|------|
| 语言/字节码 | Java 8（`<java.version>8</java.version>`，compiler 插件 3.12.1，`-parameters`） |
| RPC / 网络 | Netty `netty-all`：**`z-mq-remoting/pom.xml` 直依赖写字面 `4.1.108.Final`**，而根 POM `dependencyManagement` 走地板 `${netty.version}` = `4.1.138.Final` ⇒ 两处不一致，本模块实际编译用的是前者（根 pom 注释"netty 由 parent 供给"对这个模块不成立） |
| 序列化 | Jackson `jackson-databind`（版本由地板供给 2.18.6）；协议头/体一律 JSON，无 protobuf、无 RocketMQ 二进制兼容 |
| 其它库 | `z-mq-common` 直依赖 guava（字面 `33.6.0-jre`，高于地板 32.0.0-jre）、commons-lang3 3.18.0；`z-mq-nameserver` 用 `z-util-core`、`z-mq-remoting` 用 `z-util-parser-json`（**版本不写在模块里，由 `z-boot-parent` 的兄弟仓权威表下发**） |
| 日志 | Log4j2（`log4j-api` + `log4j-slf4j2-impl`），slf4j 属性 `2.0.12`、logback 属性 `1.5.34` 是父链不供给的登记位；地板把 `spring-boot-starter-logging` 的子件全排空 |
| Spring | 只有 `z-mq-spring-boot-starter`：`spring-boot-starter` / `autoconfigure` / `configuration-processor` 三件写面 `2.7.18`（与地板同值） |
| 测试 | JUnit 5.10.2（junit-bom）、Mockito 4.11.0、AssertJ 3.27.7；surefire/failsafe 3.2.5；JaCoCo 0.8.11 |
| 构建 | Maven 多模块 + CI-friendly `${revision}` + flatten-maven-plugin 1.5.0（常开，入库/发布件落字面量）；`central` profile 负责 sources/javadoc/GPG/deploy 到 `https://central.sonatype.com` |
| 控制台 | Vue 3.4 + Vite 5 + TypeScript 5.4 + Element Plus 2.7 + Pinia 2 + ECharts 5（`z-mq-admin/package.json`） |

**部署资产在 `deploy/`**（2026-10-20 起不再"什么都没有"）：`Dockerfile.nameserver` / `Dockerfile.broker` /
`Dockerfile.admin` 三个镜像定义，`docker-compose.yml` 起 **2 个**进程（`z-mq-nameserver` + `z-mq-broker`，
容器内用服务名互访，镜像标签是本地构建的 `z-mq-*:local`、不引用远端镜像），
`Makefile` 提供 `build` / `admin-dist` / `images` / `compose-up` / `compose-down` /
`k8s-apply` / `k8s-status` / `k8s-clean`，`k8s/` 下是 namespace + nameserver/broker StatefulSet +
Service + admin Deployment 五份清单，`env/.env.example` **只放占位符**（端口与路径，无凭据），
`bin/` 两个脚本。仓里仍然没有 `.github/workflows/`。
`_doc/003_script/build.sh` 是一份通用 Spring Boot Docker 模板
（它 `docker build -f ./Dockerfile` 而**仓根**那个文件不存在 —— 镜像文件在 `deploy/` 下、必须带 `-f`），
**照它跑必失败**；本仓真实可用的脚本只有 `package.sh`（`git pull` + `mvn install -DskipTests`）与
`deploy_maven_center.sh`。旧 README 的 compose 片段与镜像名 `ghcr.io/z-opc-foundation/z-mq-*` 全部作废。

---

## 🚀 快速开始

### 1. 编译

```bash
mvn clean install -DskipTests
```

第三方与兄弟仓版本全部由 `z-boot-parent:1.2.4` 下发（第三方地板与兄弟仓权威表均已并入 parent 自身），
构建前确认能解析到 `io.github.yuku123:z-boot-parent:1.2.4`（repo1 实测 200，`<relativePath/>` 留空 ⇒ 不在磁盘上找）。

⚠️ 九个 POM 都**没有** shade/assembly/`spring-boot-maven-plugin`，产物是普通瘦 jar，
**没有可执行 jar**；下面的 `java -cp` 需要自己把依赖拼出来（例如 `mvn dependency:build-classpath`）。

### 2. 起 NameServer，再起 Broker（两个独立进程）

```bash
# NameServer：监听 9876，KV 落 ${user.home}/zmq/namesrv/kvConfig.json
java -cp "$CP" com.zifang.z.mq.nameserver.NameServerStartup 9876

# Broker：监听 10911；第二个参数是 NameServer 地址，第三个是存储根目录
java -cp "$CP" com.zifang.z.mq.broker.BrokerStartup 10911 127.0.0.1:9876 /tmp/zmq-store
```

`BrokerStartup` 的参数位次是 `[brokerPort] [namesrvAddr] [storePath]`（位置参数，**不是 properties 文件**）。
不给 `namesrvAddr` 时 `BrokerConfig` 会退到环境变量 `NAMESRV_ADDR`；`brokerName` / `brokerClusterName`
读同名 JVM system property（默认 `DEFAULT_BROKER` / `DEFAULT_CLUSTER`）。存储根目录默认
`${user.home}/store`。旧 README 那段 `broker-a.properties`（`brokerClusterName=` / `brokerRole=` /
`flushDiskType=` / `listenPort=`）**在本仓没有任何解析者**，照抄不会生效。

### 3. 建 Topic（必须显式建，没有 autocreate）

```bash
java -cp "$CP" com.zifang.z.mq.tools.ZmqAdmin --namesrv 127.0.0.1:9876 \
     createTopic 127.0.0.1:10911 order-events 4 4
java -cp "$CP" com.zifang.z.mq.tools.ZmqAdmin --namesrv 127.0.0.1:9876 topicList
java -cp "$CP" com.zifang.z.mq.tools.ZmqAdmin --namesrv 127.0.0.1:9876 topicRoute order-events
java -cp "$CP" com.zifang.z.mq.tools.ZmqAdmin --namesrv 127.0.0.1:9876 brokerList
```

### 4. Java 客户端（同步发送 + 推送消费）

```java
DefaultMQProducer producer = new DefaultMQProducer("order_group");
producer.setNamesrvAddr("127.0.0.1:9876");
producer.start();                                   // 抛 Exception，必须处理

Message msg = new Message("order-events", "created", "ORDER_001",
        "{\"orderId\":1001}".getBytes(StandardCharsets.UTF_8));   // 构造器签名 (topic, tags, keys, byte[] body)
SendResult result = producer.send(msg);             // 路由里没有该 topic ⇒ 抛 RemotingSendRequestException("No route for topic ...")
```

坐标与版本（`1.3.1` 已实测在 repo1）：

```xml
<dependency>
    <groupId>io.github.yuku123</groupId>
    <artifactId>z-mq-client</artifactId>
    <version>1.3.1</version>
</dependency>
```

### 5. Spring Boot 应用（只接客户端，不嵌 broker）

```xml
<dependency>
    <groupId>io.github.yuku123</groupId>
    <artifactId>z-mq-spring-boot-starter</artifactId>
    <version>1.3.1</version>
</dependency>
```

配置前缀是 **`zmq`**（旧 README 写的 `z.mq.*` 不存在）。全部可写键就这些（`ZmqProperties` 现读，含默认值）：

```yaml
zmq:
  enabled: true                       # 关它 ⇒ ZmqAutoConfiguration 整块不装配
  namesrv-addr: localhost:9876        # 分号分隔多地址
  producer:
    group: zmq-default-producer-group
    send-timeout-millis: 3000
    default-topic-queue-nums: 4
  consumer:
    group: zmq-default-consumer-group
    pull-interval-millis: 1000
    pull-batch-size: 32
```

```java
@SpringBootApplication
@EnableZmq                            // @Import(ZmqAutoConfiguration.class)；spring.factories 也装了它，加不加都装配
public class OrderApp {}

@Component
class OrderEventHandler {
    @Resource private ZmqTemplate zmq;

    void onCreated(Order o) {
        // syncSend 的 body 参数是 String（内部 getBytes(UTF_8)），不是 Object：
        zmq.syncSend("order-events", "created", "ORDER_" + o.getId(), JsonUtils.toJson(o));
    }

    @ZmqListener(topic = "order-events", consumerGroup = "order-cg")
    public void onOrderCreated(String body) { /* ... */ }
}
```

`@ZmqListener` 只有 `topic` / `tag` / `consumerGroup` 三个属性，**没有 `consumeMode`**；且装配代码只读
`topic` 与 `consumerGroup` —— **`tag` 目前不参与订阅**（`ZmqAutoConfiguration#registerListenerBean` 不取
`listener.tag()`，落到 `subscribe(topic, listener)` 即 `*`），要按 tag 过滤请直接给
`DefaultMQPushConsumer#subscribe(topic, tag, listener)`。同 group 的多个 `@ZmqListener` 共用一个
consumer 实例，方法签名支持 `String` / `byte[]` / `MessageExt` 三种入参，方法抛异常只记日志、
**照旧返回 `CONSUME_SUCCESS`**（不会触发重投）。

---

## ⚠️ 进程边界：`*Startup` 会永久挂住调用线程（想内嵌的人必读）

```java
// NameServerStartup.main(...)  /  BrokerStartup.main(...) 的最后两行，逐字：
log.info("NameServer started. Press any key to stop.");
Thread.currentThread().join();      // BrokerStartup 同形：Thread.currentThread().join();
```

`Thread.currentThread().join()` 是"当前线程 join 自己"，永不返回。后果要分两种情况说清：

- **误用**：Spring Boot 应用里用 `CommandLineRunner` / `ApplicationRunner` / `@PostConstruct` 调
  `BrokerStartup.main(...)` 或 `NameServerStartup.main(...)`，容器刷新线程就永久停在那一行 —— 端口已经
  bind、日志已经打印，但 `SpringApplication.run()` 再也不返回，`ApplicationReadyEvent` 不发、
  Web 容器不进服务、 readiness/健康检查永远不过、`@PreDestroy` 与 graceful shutdown 全不生效。
  两个 Startup 都在同一棵依赖树里（`z-mq-broker` 还依赖 `z-mq-nameserver`），所以"在一个进程里
  顺手把 nameserver + broker 都带起来"这条路是**堵死的**，z-mq 必须是独立进程。
- **正确内嵌法**：绕开 `main`，直接用控制器（测试就是这么跑的）：

```java
NettyServerConfig ns = new NettyServerConfig();
ns.setListenPort(9876);
NameServerController c = new NameServerController(new NamesrvConfig(), ns);
if (c.initialize()) { c.start(); }      // initialize()/start() 都不阻塞；shutdown() 自己调
```

`BrokerController` 同形（`initialize()` 返回 `boolean`，失败请自己退出；`start()`/`shutdown()` 幂等）。
`z-mq-spring-boot-starter` 只依赖 `z-mq-client`，**不会**把 broker/nameserver 带进你的 classpath，
所以按第 5 节接 starter 的普通应用不受这个问题影响 —— 只有你想"在自己的进程里同时跑 broker"才会撞上。

---

## 🔌 协议命令字（主链路不是 REST 服务；HTTP 面只有控制台后端与运维代理）

`src/main` 里 `@RequestMapping` **1 命中**（`z-mq-spring-boot-starter` 的 `host/MqProxyController`）：
z-mq 主链路仍只有 Netty 私有帧协议
（`4B 帧长 | 1B 序列化类型(JSON) | 4B 头长 | JSON 头 | 4B 体长 | 体`，`NettyDecoder` 帧上限 16 MiB）；
HTTP 面只有两处 —— 控制台后端 `z-mq-broker-admin`（`server.port=9090`）与上面那个运维代理，
Topic/队列/位点主链路没有 HTTP 路径。
下表是 `RequestCode` 的 39 个常量里**真被接线的那 22 个**（`src/main` 现测）；
其余 **17 个零引用**：`UPDATE_AND_CREATE_TOPIC_LIST`、`GET_KV_CONFIG`、`PUT_KV_CONFIG`、
`DELETE_KV_CONFIG`、`GET_CLUSTER_INFO`、`BROKER_HEARTBEAT`、`SEND_BATCH_MESSAGE`、`PULL_MESSAGE_V2`、
`CONSUMER_HEARTBEAT`、`CONSUMER_HEARTBEAT_V2`、`QUERY_MESSAGE`、`SEND_TRANSFER_MSG`、
`HA_PUSH_COMMITLOG`、`CHECK_ROUTE_EXIST`、`GET_BROKER_CONFIG`、`RESET_CONSUMER_OFFSET`、`TERMINATE_CONSUMER`。

| 码 | 名称 | 处理器 | 进程 |
|---|---|---|---|
| 1 / 2 | `REGISTER_BROKER` / `UNREGISTER_BROKER` | `nameserver.DefaultRequestProcessor`（默认处理器，覆盖全部未注册码） | NameServer :9876 |
| 3 / 311 | `GET_ROUTEINFO_BY_TOPIC` / `GET_ROUTE_BY_TOPIC` | 同上 | NameServer |
| 4 | `GET_BROKER_CLUSTER_INFO` | 同上 | NameServer |
| 5 | `UPDATE_AND_CREATE_TOPIC` | 同上（NameServer 侧登记路由；未收到 broker 注册时"只记 topic 不绑 broker"） | NameServer |
| 7 | `DELETE_TOPIC` | `AdminBrokerProcessor`（`BrokerController` 已把它绑进 remoting 路由表，与 220 同走 `adminBrokerExecutor`） | Broker |
| 8 | `GET_ALL_TOPIC_LIST` | 同上 | NameServer |
| 100 | `BROKER_HEARTBEAT` | —— 零引用；broker 每 5s 重发的是 `REGISTER_BROKER` | — |
| 200 | `SEND_MESSAGE` | `SendMessageProcessor` → `CommitLog.putMessage`（同步刷盘超时可回 `FLUSH_DISK_TIMEOUT`） | Broker :10911 |
| 201 / 251 / 250 | `SEND_MESSAGE_V2`(半消息) / `END_TRANSACTION` / `CHECK_TRANSACTION_STATE` | `TransactionMessageProcessor` | Broker |
| 210 | `PULL_MESSAGE` | `PullMessageProcessor`（长轮询挂起 + `Tag`/`SQL92` 过滤） | Broker |
| 220 | `UPDATE_CONSUMER_OFFSET` | `ConsumerOffsetProcessor`（绑 `adminBrokerExecutor`，不占 pull 池） | Broker |
| 231 | `VIEW_MESSAGE_BY_KEY` | `AdminBrokerProcessor`（同上，`BrokerController` 已绑进 remoting 路由表） | Broker |
| 400 | `CREATE_TOPIC` | `AdminBrokerProcessor` 的 switch 里与 5 号同分支，**但没绑到 remoting 路由表** ⇒ 发 400 得不到处理，CLI 实际发 5 | Broker |
| 320 / 321 / 322 / 323 / 324 | `GET_ALL_TOPIC_CONFIG` / `GET_ALL_CONSUMER_OFFSET` / `GET_ALL_DELAY_OFFSET` / `GET_ALL_SUBSCRIPTION_GROUP` / `QUERY_DATA_VERSION` | `BrokerOutAPI`（Slave 拉元数据；订阅组返回空 wrapper） | Broker |
| 351 | `HA_REPORT_OFFSET` | `HAProcessor`（Slave 上报本地最大 offset） | Broker |

---

## 🖥️ 控制台 `z-mq-admin`（Vue 3 工程，非 Maven 模块）

```bash
cd z-mq-admin
npm install
npm run dev          # Vite，端口 8081，host 0.0.0.0
npm run build        # vue-tsc -b && vite build → dist/
```

环境变量（**只写名字**）：`VITE_API_BASE_URL`（默认 `/api`，见 `src/api/index.ts`）、
`VITE_ZMQ_ADMIN_URL`（Vite 代理目标，默认 `http://localhost:9090`，见 `vite.config.ts`）。

如实说明：它是 7 个页面的 MVP；代理指向的 `z-mq-broker-admin` REST 服务**如今就在本仓**
（`z-mq-broker-admin` 模块，`web/ZMqAdminController`，`server.port=9090`），前端经 Vite 代理或
打进 jar 后的同源 `static/` 访问它。`src/api/index.ts` 早年那句"当前 MVP 提供 mock 数据"已经改成真调用，
只剩 `views/DelayView.vue` 里一处增量统计的 mock 注释。
详细设计见 [`_doc/001_arch/06-admin.md`](_doc/001_arch/06-admin.md)。

---

## 🧪 测试

```bash
mvn test                 # 单元 + 仓内 E2E（真起进程内 NameServer/Broker 控制器并 bind 端口）
mvn test -Pperf          # 把被默认排除的挂钟基准也跑上
mvn verify               # surefire + failsafe + jacoco report
```

实测规模（**2026-10-22 快照**，不是抄文档）：测试类 = `find <模块>/src/test -name '*Test*.java'` 的条数；
`@Test` 方法 = 同一目录树里 `grep -c "@Test"` 的合计。**含本仓守卫 `ReadmeClaimsTest`**（落在 z-mq-common 那行）。
这是带日期的快照、**不是实时读数** —— 以后增删测试类不会回写这张表；要新读数请按上面两条命令重数并改日期。
（2026-10-22 TASK-20260925-024 又在本仓加了测试文件：`z-mq-spring-boot-starter` 1 → **6** 个测试类 / 16 → **24** 个 `@Test`；
按上面那句口径**本表不回写**，下次统一重数。）

| 模块 | 测试类 | `@Test` 方法 |
|---|---|---|
| z-mq-broker | 33 | 202 |
| z-mq-client | 17 | 126 |
| z-mq-store | 16 | 89 |
| z-mq-remoting | 14 | 85 |
| z-mq-common | 15 | 104 |
| z-mq-nameserver | 6 | 63 |
| z-mq-spring-boot-starter | 1 | 16 |
| z-mq-tools | 1 | 15 |
| z-mq-broker-admin | 0 | 0 |
| **合计** | **103** | **700** |

**真正跑了多少**（2026-10-22 本机 `mvn -o -B test` 的 surefire 汇总行，逐模块相加）：
**675 tests / 0 failures / 0 errors / 0 skipped**，分模块 `104 / 82 / 86 / 59 / 126 / 187 / 16 / 15`，
`BUILD SUCCESS`、10 个 reactor 项目全 SUCCESS（`z-mq-broker-admin` 无测试类，不参与计数）。
注意这张表数的是 `@Test` **标记**（含 `@TestInstance` 之类会被一起数进去），与 surefire 实跑的 675 不是同一个口径；
判断"测试绿不绿"只认 surefire 的 `Tests run:` 行，且**`Tests run: 0` 不算绿**（那是测试根本没跑起来）。

如实说明这几条：

- **不需要外部集群/中间件**：`integration` 包下的 `ZmqE2ETest`、`TransactionMessageE2ETest`、
  `PushOffsetCommitE2ETest`、`RestartDurabilityTest` 等在测试 JVM 内直接 new 控制器并真 bind 端口。
- **默认门禁排除基准**：surefire 配了 `<exclude>**/*BenchmarkTest.java</exclude>`
  （`PerformanceBenchmarkTest`、`ConcurrentChannelCacheBenchmarkTest`），因为挂钟阈值断言
  "同代码两次跑一次红一次绿"，一红就让后面 6 个模块不执行。`-Pperf` 才跑 ⇒ 别把默认绿当性能证明。
- **同机并跑会撞端口**：至少 5 个真起监听的测试类把端口写死（登记于
  [`_doc/007_backlog/feature009_test_port_collision/001_待排产归属.md`](_doc/007_backlog/feature009_test_port_collision/001_待排产归属.md)），
  实测曾整类吞掉 `HAServiceTest`；换 `freePort()` 也只是降概率（TOCTOU）。
- **已知的真实红**：位点提交被静默吞（`feature010`）、偶发"通道已建好但一次请求 10s 无响应"（`feature011`，
  怀疑 JVM 停顿）—— 都还没排产修，跑全量遇到时**别归因到自己这次的改动**。
- **跑不过 ≠ 代码坏**：本仓测试从不依赖 MySQL/Redis/Docker，唯一外部条件是 `~/.m2` 能解析
  `z-boot-parent:1.2.4` 及其父链。

---

## 📤 发布与配额（只写实测，不写凭据）

- 发布走根 POM 的 `central` profile（sources + javadoc + GPG 签名 → `https://central.sonatype.com`），
  脚本 [`_doc/003_script/deploy_maven_center.sh`](_doc/003_script/deploy_maven_center.sh)
  子命令 `publish` / `verify` / `gpg-init` / `readme`；**所有凭据从仓根 `.env` 读，`.env` 与 `.gnupg/` 都已被
  `.gitignore` 排除**，README 不复述其值。判"是否对外可见"只认 repo1 回读（且要用 ranged GET），
  `BUILD SUCCESS` 不等于已上线。
- flatten 常开（1.5.0）：入库/发布件里 `${revision}` 落成字面量，消费方拿不到悬空 parent。
- 根目录 [`_doc/006_release/sonatype-limit-request-email.txt`](_doc/006_release/sonatype-limit-request-email.txt) 是一封**邮件草稿纯文本**
  （向 Sonatype 申请提高发布配额/豁免复核），不是配置、不是构建输入，请勿改名接入构建。
  同题材的可执行版本在 `_doc/003_script/send_email_to_sonatype.py`（该脚本把 SMTP 账号口令硬编码在源码里，
  属于待整改项：**不要把它的任何值抄进任何文档或 README**）。
- ⚠️ 一处版本口径待纠：`_doc/007_backlog/feature003_release_1_3_0/` 成文时 repo1 上 `1.3.0` 还是 404；
  本次实测 `1.3.0` 与 `1.3.1` **九个坐标全为 200**，`z-boot-fleet` 也已把 `<z-mq.version>` 抬到 `1.3.1`
  —— 那份待办文档与根 pom 注释里"fleet 钉 1.3.0"的说法都已经过期，以本节读数为准。

---

## 📄 License

[MIT](LICENSE)（根 POM `<licenses>` 同样声明 MIT License）。

_Maintained by the z-opc-foundation organization._

---

## 文档目录

- [`_doc/006_release/`](_doc/006_release/) — 发布与中央仓文档（Sonatype 限额申请）
本项目文档统一收口在 `_doc/` 下。⚠️ `001_arch/` 里有**两代并存**的文档，两代冲突时**以代码为准**，
下表已按现读代码标注哪一份更接近今天：

- [`_doc/001_arch/`](_doc/001_arch/) — 架构与历史文档：
  - [`00-overview.md`](_doc/001_arch/00-overview.md) — 项目定位与上下游关系（最粗略的一页）
  - [`01-module-structure.md`](_doc/001_arch/01-module-structure.md) — 模块结构（7 模块时代，模块名还写作 `z-mq-namesrv`）
  - [`03-architecture-design.md`](_doc/001_arch/03-architecture-design.md) — **另一代**的长版架构文档（1600+ 行，
    还列了根本不存在的 `z-mq-controller` / `z-mq-example`，并宣称支持 JMS/MQTT）。
    **两份都是 2026-09-16 历史塌缩提交里的规划稿，都不是现状**；现在唯一接近真相的结构说明是本 README 的
    「项目结构」+ 上面的能力表
  - [`02-feature.md`](_doc/001_arch/02-feature.md) — P0 功能完成状态清单
  - [`05-p0-completion-report.md`](_doc/001_arch/05-p0-completion-report.md) — P0 结项报告（"完成状态 100%"是当时的口径，
    与本文「只有类型、没有通路」那一栏的实测结论冲突时以实测为准）
  - [`07-roadmap.md`](_doc/001_arch/07-roadmap.md) / [`04-roadmap-iteration.md`](_doc/001_arch/04-roadmap-iteration.md) —
    同一份迭代排期的两个世代，差异只有 `-2` 多了一列空的"负责人"；两者状态格都还停在 ⬜，**不反映已完成事实**
  - [`08-design.md`](_doc/001_arch/08-design.md) — 规划期技术方案（按 9 模块设想写，含未落地部分）
  - [`09-selection.md`](_doc/001_arch/09-selection.md) — **对 RocketMQ 的能力差距分析，与代码最一致的一份**
    （它已点明"缺 ConsumeQueue / IndexFile"）；只看 `001_arch` 就看这份
  - [`06-admin.md`](_doc/001_arch/06-admin.md) — 控制台设计稿（Vue 3 + Element Plus，8081 → 9090 代理拓扑）
  - 空目录如实记录：`_doc/002_deploy/` 与
    `_doc/004_skill/` **目前都是空目录**（没有部署文档、没有 SQL、没有 skill）

- [`_doc/003_script/`](_doc/003_script/) — 脚本：
  - [`deploy_maven_center.sh`](_doc/003_script/deploy_maven_center.sh) — Maven Central 发布入口（publish/verify/gpg-init）
  - [`package.sh`](_doc/003_script/package.sh) — `git pull` + `mvn clean install -DskipTests`
  - [`build.sh`](_doc/003_script/build.sh) — 通用 Spring Boot + Docker 部署模板；**依赖仓里不存在的 `Dockerfile`，
    本仓直接跑不通**，留作历史/待改造
  - [`push.sh`](_doc/003_script/push.sh) — `git add . && git commit -m add && git push`（三行偷懒脚本，会全量 add，慎用）
  - [`send_email_to_sonatype.py`](_doc/003_script/send_email_to_sonatype.py) — 配额申请邮件的发送脚本（内含硬编码 SMTP 凭证，值不入文档）

- [`_doc/007_backlog/`](_doc/007_backlog/) — 非标准命名的**待拍板/待排产登记册**（不在 `001-004` 四类里，
  如实描述：每格一个 `featureNNN_*/001_*.md`，写"广告原文 vs 实测兑现、等谁拍什么"）。
  索引先读 [`README.md`](_doc/007_backlog/README.md)：
  - [`feature001_acl/001_待裁定.md`](_doc/007_backlog/feature001_acl/001_待裁定.md) — ACL 是否要做
  - [`feature002_sync_master/001_待裁定.md`](_doc/007_backlog/feature002_sync_master/001_待裁定.md) — 同步双写口径
  - [`feature003_release_1_3_0/001_待点头.md`](_doc/007_backlog/feature003_release_1_3_0/001_待点头.md) — 发布树选择（现状见上文「发布与配额」）
  - [`feature004_transaction_check/001_等证据后裁定.md`](_doc/007_backlog/feature004_transaction_check/001_等证据后裁定.md) — 事务回查语义
  - [`feature005_metadata_lifecycle/001_待排产.md`](_doc/007_backlog/feature005_metadata_lifecycle/001_待排产.md) — `DataVersion` 恒 0、`DELETE_TOPIC` 空号、零引用命令字
  - [`feature006_dlq_topic_registration/001_待裁定.md`](_doc/007_backlog/feature006_dlq_topic_registration/001_待裁定.md) — 死信 Topic 谁来注册
  - [`feature007_dlq_read_side/001_待排产归属.md`](_doc/007_backlog/feature007_dlq_read_side/001_待排产归属.md) — 死信读侧
  - [`feature008_batch_commit_coupling/001_待裁定.md`](_doc/007_backlog/feature008_batch_commit_coupling/001_待裁定.md) — 失败批次位点整批判定
  - [`feature009_test_port_collision/001_待排产归属.md`](_doc/007_backlog/feature009_test_port_collision/001_待排产归属.md) — 测试端口写死
  - [`feature010_push_offset_silent_commit_failure/001_待排产归属.md`](_doc/007_backlog/feature010_push_offset_silent_commit_failure/001_待排产归属.md) — 位点提交静默失败
  - [`feature011_jvm_pause_drops_broker_response/001_待排产归属.md`](_doc/007_backlog/feature011_jvm_pause_drops_broker_response/001_待排产归属.md) — 偶发 10s 无响应
  - [`feature012_producer_retry_uncovered_shapes/001_待排产归属.md`](_doc/007_backlog/feature012_producer_retry_uncovered_shapes/001_待排产归属.md) — producer 重试未覆盖面
  - [`feature013_console_mock_swallows_every_failure/001_等拍板.md`](_doc/007_backlog/feature013_console_mock_swallows_every_failure/001_等拍板.md) — 控制台 7 个接口的 mock 兜底吞掉全部后端失败
  - [`feature014_delay_message_wiring/001_待排产.md`](_doc/007_backlog/feature014_delay_message_wiring/001_待排产.md) — 延迟消息整条未接线（18 级 DelayQueue 零调用方）
  - [`feature015_broker_metrics_export/001_待排产.md`](_doc/007_backlog/feature015_broker_metrics_export/001_待排产.md) — `BrokerMetrics` 零 feeder、无只读导出端点

各文档详细说明见各子目录。

---

<!-- z-opc-foundation-sibling-registry:v1 -->
## 🔗 兄弟仓全景（z-opc-foundation 组织）

本仓隶属于 [z-opc-foundation](https://github.com/orgs/z-opc-foundation/repositories) 组织——一人公司基座的全部自研件（现存 61 仓，含私有仓；口径 2026-11-24 GitHub org API 实读）。

| 仓 | 一句话说明 |
|---|---|
| [z-agent](https://github.com/z-opc-foundation/z-agent) | 云端 Agent 平台的库与 starter——ReAct/Plan 实现 + 注册中心 + REST 网关 + 只读控制面 |
| [z-agent-kernel](https://github.com/z-opc-foundation/z-agent-kernel) | Agent 框架基座——消息/工具/记忆/provider/会话 SPI + 轻量默认实现（纯 POJO 无 Spring） |
| [z-asset](https://github.com/z-opc-foundation/z-asset) | 企业资产台账——实物设备全生命周期 + 域名/证书/许可到期提醒 |
| [z-audit](https://github.com/z-opc-foundation/z-audit) | 审计与合规留痕——事件模型 + 哈希链 + 事件出口 SPI + 留存期策略 |
| [z-biz](https://github.com/z-opc-foundation/z-biz) | 商务外壳域——客户/合同/票据/对账四聚合 |
| [z-biz-erp](https://github.com/z-opc-foundation/z-biz-erp) | 进销存 ERP——商品/客商/采购/销售/库存流水/应收应付核销（z-biz 业务线） |
| [z-biz-security](https://github.com/z-opc-foundation/z-biz-security) | 业务数据安全防护入口——场景化策略注册 + 安全线四段流水线对外编排 |
| [z-boot](https://github.com/z-opc-foundation/z-boot) | Spring Boot Starter 聚合仓 + 版本权威（火车制）——唯一消费入口 z-boot-parent |
| [z-bot](https://github.com/z-opc-foundation/z-bot) | 本地 agent 应用——终端 CLI + HTTP/SSE web 控制台 + 多通道 gateway（飞书/钉钉/webhook） |
| [z-cache](https://github.com/z-opc-foundation/z-cache) | RESP2 协议兼容的分布式内存缓存——Java 8 + Netty，Redis 客户端零修改直连 |
| [z-camuda](https://github.com/z-opc-foundation/z-camuda) | 【已退役·归档】基于 Camunda 的工作流封装层（原 z-wf 改名，与现行 z-wf 无共同历史） |
| [z-cognition](https://github.com/z-opc-foundation/z-cognition) | 业务认知域/认知舱——术语/概念/证据链 + memory/kb/ontology 渐进式模块 |
| [z-config](https://github.com/z-opc-foundation/z-config) | 分布式配置中心 + 服务注册中心（类 Nacos）——Netty 长连接推送 + React 控制台 |
| [z-ctc](https://github.com/z-opc-foundation/z-ctc) | 4A 中心——认证/授权/账户/审计，多租户 JWT + SSO 跨子域 |
| [z-dms](https://github.com/z-opc-foundation/z-dms) | 数据源管控底座——统一登记、统一建池、统一探活、变更统一推送 |
| [z-env](https://github.com/z-opc-foundation/z-env) | 本地中间件与基础设施环境——31 个 docker-compose 场景 + make 一键运维 |
| [z-etl](https://github.com/z-opc-foundation/z-etl) | ETL 引擎——DataX 式线性作业 + Flink 式 DAG 流管道（键控状态/水位/窗口） |
| [z-ext](https://github.com/z-opc-foundation/z-ext) | Java 扩展点框架——@ExtPoint/@ExtImpl 运行期选实现，五档侵入方式 |
| [z-frontend-common](https://github.com/z-opc-foundation/z-frontend-common) | 全仓共享前端基座——AppLayout/LoginPage 等前端规范的唯一承载 |
| [z-frontend-markdown](https://github.com/z-opc-foundation/z-frontend-markdown) | Markdown 通用共享组件包（npm）——MarkdownLiveEditor/NoteMarkdown/MermaidBlock 单一真源 |
| [z-frontend-render](https://github.com/z-opc-foundation/z-frontend-render) | 平台共享前端渲染引擎——后端 schema 是唯一事实源，报表与低代码共用 |
| [z-graph](https://github.com/z-opc-foundation/z-graph) | 独立图数据库——MVCC 版本链存储 + OpenCypher 子集 + Bolt 风格协议 + React 控制台 |
| [z-gw](https://github.com/z-opc-foundation/z-gw) | 自研高性能 API 网关——Netty 手写运行时，Route/Predicate/Filter 模型可嵌 Spring Boot |
| [z-indexer](https://github.com/z-opc-foundation/z-indexer) | 代码索引服务——仓库注册表 + 定时重建索引 + 符号/调用链查询 |
| [z-introduce](https://github.com/z-opc-foundation/z-introduce) | 组织仓库展示站——全仓实测元数据汇总成可搜索静态站点 |
| [z-lc](https://github.com/z-opc-foundation/z-lc) | 低代码平台——DB 驱动通用 CRUD 引擎 + 报表/模型设计框架 |
| [z-llm-agent](https://github.com/z-opc-foundation/z-llm-agent) | CLI Agent 服务化——把本机 CLI agent（CC/Hermes/Qoder 等）暴露成 OpenAI 兼容对话 API |
| [z-llm-gateway](https://github.com/z-opc-foundation/z-llm-gateway) | LLM 网关——OpenAI/Anthropic 双协议面 + 多 vendor 凭据池、限流与 token 记账 |
| [z-llm-msghub](https://github.com/z-opc-foundation/z-llm-msghub) | msgHub 中心消息路由——WebSocket 中心枢纽 + Agent 侧客户端 SDK |
| [z-mask](https://github.com/z-opc-foundation/z-mask) | 脱敏与遮蔽引擎——按敏感命中位置遮蔽，策略即数据 |
| [z-mcp](https://github.com/z-opc-foundation/z-mcp) | MCP 服务端 + 上游聚合网关——Java 8 手写协议层，Spring Boot 自动装配 |
| [z-meta](https://github.com/z-opc-foundation/z-meta) | 元数据管理平台——租户/应用/接口清单登记 + @BizModel 业务模型采集 |
| [z-mill](https://github.com/z-opc-foundation/z-mill) | 资料磨坊——全格式文档转 Markdown/JSON/纯文本/HTML（对标 MinerU） |
| [z-mist](https://github.com/z-opc-foundation/z-mist) | KMS 分布式密钥/秘密管理平台——集中加密存储 + Netty 协议 + starter 嵌入 |
| [z-mq](https://github.com/z-opc-foundation/z-mq) | 自研分布式消息队列——NameServer+Broker+Store，形状对标 RocketMQ、自有 Netty 协议 **（本仓）** |
| [z-msg](https://github.com/z-opc-foundation/z-msg) | 多通道消息 + WebSocket 实时下发基础设施——短信/邮件/站内信/Webhook/IM 平台 |
| [z-observer](https://github.com/z-opc-foundation/z-observer) | 可观测性域——指标/日志/链路 + 告警引擎 |
| [z-opc](https://github.com/z-opc-foundation/z-opc) | 一人公司统一后端+统一前端——单 JVM all-in-one 承载协作/Agent/RPA/交易/资产等业务域与自研中间件 |
| [z-opc-foundation-lead](https://github.com/z-opc-foundation/z-opc-foundation-lead) | 组织治理大脑——事实表、强制规范、跨仓工具集、新机器恢复钥匙（本清单权威持有处） |
| [z-opcs](https://github.com/z-opc-foundation/z-opcs) | 任务撮合/服务市场——用户付费发布诉求，持有者接单完成，金额自动瓜分 |
| [z-ops](https://github.com/z-opc-foundation/z-ops) | 个人公司运维平台——部署单元/镜像构建/WebShell/K8s 巡检 + Ops Agent 执行器 |
| [z-oss](https://github.com/z-opc-foundation/z-oss) | 对象存储服务——自持 local 后端 + aliyun/s3 对接层，starter 注入 OssTemplate |
| [z-product](https://github.com/z-opc-foundation/z-product) | 产品设计信息汇总仓（开发域 PM 工件） |
| [z-qa](https://github.com/z-opc-foundation/z-qa) | 步骤式 API 链路测试平台——用例/计划/执行/报告/质量看板（对标 TestRail·MeterSphere） |
| [z-rpa](https://github.com/z-opc-foundation/z-rpa) | RPA 产品皮——应用市场/需求中心（执行归 z-script，触发归 z-schedule） |
| [z-rpc](https://github.com/z-opc-foundation/z-rpc) | Java 8 + Netty RPC 框架——微内核 + SPI 插件化，形状参照 Dubbo/gRPC |
| [z-schedule](https://github.com/z-opc-foundation/z-schedule) | 分布式任务调度中枢（对标 XXL-Job）——选主 + 时间环排期，starter 嵌入宿主 |
| [z-script](https://github.com/z-opc-foundation/z-script) | 多 DSL 脚本平台 + Mock 平台——沙箱执行/版本灰度/可注册 MCP Tool，starter 内嵌 |
| [z-security](https://github.com/z-opc-foundation/z-security) | 数据安全域编排层——场景→策略→识别→脱敏→水印→审计 四段流水线编排 |
| [z-sense](https://github.com/z-opc-foundation/z-sense) | 敏感识别与分类分级引擎——数据安全线底座（规则目录 + 可插拔识别器 + 分类分级） |
| [z-skill](https://github.com/z-opc-foundation/z-skill) | Skill 平台聚合器——归一化各家 Skill 形状，再按各家 API 形状发回 |
| [z-surl](https://github.com/z-opc-foundation/z-surl) | 短网址服务——短码生成/解析/命中统计，starter 一行集成 |
| [z-task](https://github.com/z-opc-foundation/z-task) | 任务协作平台——项目空间/看板/任务卡片（看板范式，对标 Trello·Teambition） |
| [z-team](https://github.com/z-opc-foundation/z-team) | 数字员工协同域——全栈独立仓，自 z-opc 毕业 |
| [z-trade](https://github.com/z-opc-foundation/z-trade) | 交易域独立仓——自 z-opc reactor 迁出 |
| [z-util](https://github.com/z-opc-foundation/z-util) | 底层 Java 工具库——89 个模块 POM，Java 8/17 口径，按需引用按需升级 |
| [z-vector](https://github.com/z-opc-foundation/z-vector) | 自研 Java 向量数据库——嵌入式+可独立部署，ANN+Payload 过滤，兼容 Qdrant REST |
| [z-voice-demo](https://github.com/z-opc-foundation/z-voice-demo) | 语音链路 demo——流式识别→kernel LLM→流式 TTS→可打断 的契约验证实现 |
| [z-watermark](https://github.com/z-opc-foundation/z-watermark) | 水印引擎——显式文字/图片隐写/盲水印，三种技术成对「打+验」 |
| [z-webide](https://github.com/z-opc-foundation/z-webide) | 企业级 WebIDE 后端——Git 仓/共享目录秒变 code-server 实例，全生命周期管理 |
| [z-wf](https://github.com/z-opc-foundation/z-wf) | 自研流程引擎——不依赖 Camunda/Flowable，定义层与运行时全自研 |

**已退役仓**（迁至 [z-trash](https://github.com/z-trash) 组织，archived 只读）：

| 仓 | 说明 |
|---|---|
| [z-agent-proxy](https://github.com/z-trash/z-agent-proxy) | 三方 coding agent 适配代理（Aider/Cursor → kernel Agent SPI），2026-10-26 退役 |
| [z-kb](https://github.com/z-trash/z-kb) | 自研知识库系统（对标 LightRAG/RAGFlow），2026-11 并入 z-cognition 认知舱后退役 |
| [z-ontology](https://github.com/z-trash/z-ontology) | 本体层（TBox/ABox/RDFS 推理），并入 z-cognition 后退役 |

另有 [z-report](https://github.com/z-opc-foundation/z-report)（报表域，2026-10-11 撤销并入 z-lc（远端已删除））。

总清单权威持有处在 [z-opc-foundation-lead](https://github.com/z-opc-foundation/z-opc-foundation-lead) `008_组织规范/005_兄弟仓清单.md`。
