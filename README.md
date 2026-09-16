# z-mq

> **自研分布式消息队列** — NameServer + Broker + Producer + Consumer 完整链路
> 类 Apache RocketMQ 架构，Java 8 + Netty 4 异步复制 + Spring Boot 2.7

[![Maven Central](https://img.shields.io/badge/Maven%20Central-1.0.2-blue?logo=apache-maven)](https://central.sonatype.com/search?q=g:io.github.yuku123+a:z-mq*)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8%2B-orange)](https://openjdk.org)
[![Docker](https://img.shields.io/badge/Docker-compose-2496ED)](docker-compose.yml)

---

## 🚀 5 分钟接入

### 方式一：本地起 NameServer + Broker（5 行 Compose）

```bash
curl -O https://raw.githubusercontent.com/z-opc-foundation/z-mq/main/docker-compose.yml
docker compose up -d nameserver broker
# ✅ NameServer listening on 9876
# ✅ Broker[0] registered to nameserver
```

### 方式二：Java Producer（同步 / 异步 / 顺序）

```xml
<dependency>
    <groupId>io.github.yuku123</groupId>
    <artifactId>z-mq-client</artifactId>
    <version>1.0.2</version>
</dependency>
```

```java
DefaultMQProducer producer = new DefaultMQProducer("order_group");
producer.setNamesrvAddr("localhost:9876");
producer.start();

Message msg = new Message("order-events", "created", "ORDER_001",
        "{\"orderId\":\"1001\",\"amount\":99.5}".getBytes(StandardCharsets.UTF_8));
SendResult result = producer.send(msg);   // 同步
// 或 producer.send(msg, sendCallback);     // 异步回调
```

### 方式三：Spring Boot Starter（一行接入 + `@ZmqListener` 注解）

```xml
<dependency>
    <groupId>io.github.yuku123</groupId>
    <artifactId>z-mq-spring-boot-starter</artifactId>
    <version>1.0.2</version>
</dependency>
```

`application.yml`:

```yaml
z:
  mq:
    enabled: true
    namesrv-addr: localhost:9876
    producer-group: my-app-producer
```

```java
@SpringBootApplication
@EnableZmq                  // ← 启用 producer + consumer 自动注册
public class OrderApp {}

@Component
public class OrderEventHandler {

    @Autowired private ZmqTemplate zmq;

    public void onCreated(Order o) {
        SendResult r = zmq.syncSend("order-events", "created", "ORDER_" + o.getId(), o);
    }

    @ZmqListener(topic = "order-events", tag = "created", consumerGroup = "order-cg")
    public void onOrderCreated(String body) {
        Order o = JsonUtils.parse(body, Order.class);
        log.info("处理订单: {}", o.getId());
    }
}
```

---

## 📦 已发布到 Maven Central 的所有模块

> groupId: `io.github.yuku123` · version: **1.0.2**

| 模块 | 说明 | 何时该引入 |
|---|---|---|
| `z-mq-nameserver` | 路由注册中心（类似 RocketMQ NameServer） | 单独跑 nameserver 进程 |
| `z-mq-broker` | 消息存储 + 投递服务（master / slave） | 单独跑 broker 进程 |
| `z-mq-client` | Java 客户端（Producer + Push/Pull Consumer） | 普通 Java 应用 |
| `z-mq-spring-boot-starter` | Spring Boot 自动装配 + `ZmqTemplate` + `@ZmqListener` | Spring Boot 应用 |
| `z-mq-remoting` | 网络通信层（Netty 4 + 心跳 + 重连） | 自定义协议扩展 |
| `z-mq-store` | 存储层（CommitLog + ConsumeQueue + Index） | 自定义存储 |
| `z-mq-common` | 协议常量 / 异常 / enum | 客户端/服务端共享 |
| `z-mq-tools` | CLI 工具（命令行查询消息 / 创建 topic） | 运维 |

---

## ✨ 核心能力

### 消息模型
- ✅ **Topic + Tag + Key** 三级消息路由（兼容 RocketMQ 语义）
- ✅ **集群消费**（默认）/ **广播消费**（`MessageModel.BROADCASTING`）
- ✅ **顺序消息**（全局顺序 / 局部顺序，通过 `MessageQueueSelector`）
- ✅ **事务消息**（两阶段提交 + 回查）
- ✅ **延迟消息**（18 个固定延迟级别 / 自定义时间）
- ✅ **批量消息**（单批 ≤ 4MiB / ≤ 1024 条）

### 高可用
- ✅ **Master-Slave 同步双写**（`SYNC_MASTER`）/ 异步复制（`ASYNC_MASTER`）
- ✅ **Broker 自动故障切换**（基于 NameServer 路由刷新）
- ✅ **Producer 自动重试**（默认 3 次，可配）
- ✅ **Consumer 消费失败重投**（最多 16 次，超过进死信队列）
- ✅ **幂等去重**（业务侧根据 Key + 业务时间戳）

### 运维友好
- ✅ **可视化控制台**（React + Vite + AntD，端口 8080）
- ✅ **Topic 自动创建** / 队列数动态调整
- ✅ **消息轨迹**（Trace，从 producer → broker → consumer 全链路）
- ✅ **ACL 访问控制**（`aclEnable=true`）
- ✅ **Prometheus 指标** + Grafana Dashboard 模板

---

## ⚙️ 实用 Case（生产场景）

### Case 1: 订单创建 → 异步通知（最常见）

```java
@Service
public class OrderService {

    @Autowired private ZmqTemplate zmq;

    @Transactional
    public Order create(CreateOrderRequest req) {
        Order order = db.save(req.toEntity());

        // 发消息: topic="order-events", tag="created", key="ORDER_<id>"
        SendResult r = zmq.syncSend("order-events", "created",
                                    "ORDER_" + order.getId(), order);
        if (r.getSendStatus() != SendStatus.SEND_OK) {
            throw new BizException("消息发送失败: " + r.getErrorMsg());
        }
        return order;
    }
}

@Component
public class InventoryHandler {

    @ZmqListener(topic = "order-events", tag = "created", consumerGroup = "inventory-cg")
    public void onCreated(Order order) {
        inventoryService.lockStock(order.getItems());
        log.info("已锁定库存: order={}", order.getId());
    }
}
```

### Case 2: 顺序消息（同一个订单 ID 顺序处理）

```java
// Producer 端: 同一 orderId 进同一个 Queue
SendResult r = producer.send(msg, new MessageQueueSelector() {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        Long orderId = (Long) arg;
        int idx = (int) (orderId % mqs.size());
        return mqs.get(idx);
    }
}, order.getId());

// Consumer 端: 一个队列一个线程
@ZmqListener(topic = "order-events", consumerGroup = "order-cg",
             consumeMode = ConsumeMode.ORDERLY)
public void onCreated(Order order) {
    paymentService.charge(order);
    inventoryService.lockStock(order);
    shippingService.prepare(order);
}
```

### Case 3: 事务消息（本地事务 + 消息发送原子性）

```java
@Transactional
public void payWithTransaction(PayRequest req) throws Exception {
    // 1. 发 half 消息
    Message half = new Message("payment-events", "pay", req.getTxId(),
                               req.toJson().getBytes(UTF_8));
    producer.sendMessageInTransaction(half, new LocalTransactionExecuter() {
        @Override
        public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
            try {
                paymentService.doCharge(req);    // 本地事务
                return LocalTransactionState.COMMIT_MESSAGE;
            } catch (Exception e) {
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }
    });

    // 2. 后台定时回查
    producer.checkTransactionState(req.getTxId());
}
```

### Case 4: 延迟消息（订单 30 分钟未支付自动取消）

```java
Message msg = new Message("order-events", "timeout", "ORDER_" + orderId,
                          orderId.toString().getBytes(UTF_8));
msg.setDelayTimeLevel(16);   // 16 = 30 min (1=1s, 2=5s, 3=10s, ..., 16=30min, 18=2h)
producer.send(msg);

// 消费者
@ZmqListener(topic = "order-events", tag = "timeout")
public void onTimeout(String orderId) {
    orderService.cancelIfUnpaid(Long.parseLong(orderId));
}
```

### Case 5: 死信队列（消费失败 16 次后自动进 DLQ）

```yaml
z:
  mq:
    consumer:
      max-reconsume-times: 16
      dlq-topic: DLQ_order-events   # 死信 Topic, 单独监控
```

```java
@ZmqListener(topic = "DLQ_order-events", consumerGroup = "dlq-monitor")
public void onDead(String body) {
    alertService.fire("死信消息", "请人工处理: " + body);
}
```

---

## 🏗️ 项目结构

```
z-mq/
├── pom.xml                          # 自给自足 parent
├── z-mq-common/                     # 协议常量 / 异常
├── z-mq-remoting/                   # Netty 网络层
├── z-mq-store/                      # CommitLog + ConsumeQueue + Index
├── z-mq-nameserver/                 # NameServer 路由中心
├── z-mq-broker/                     # Broker 存储 + 投递
├── z-mq-client/                     # Producer + Push/Pull Consumer
├── z-mq-spring-boot-starter/        # Spring Boot 自动装配
├── z-mq-tools/                      # 命令行运维工具
├── z-mq-console/                    # React + AntD 可视化控制台
├── docker-compose.yml               # 一键起 nameserver + broker
└── README.md
```

---

## 🔧 高级配置

### Broker 集群（2 Master + 2 Slave）

```yaml
# broker-a.properties
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0                   # 0 = master, 1 = slave
brokerRole=ASYNC_MASTER       # ASYNC_MASTER / SYNC_MASTER / SLAVE
flushDiskType=ASYNC_FLUSH     # ASYNC_FLUSH / SYNC_FLUSH
listenPort=10911
namesrvAddr=namesrv1:9876;namesrv2:9876
storePathRootDir=/data/z-mq/broker-a
```

集群部署至少需要 2 个 NameServer + 2 个 Broker（不同机器），用 keepalived 做 VIP 漂移。

### application.yml 完整 Properties

```yaml
z:
  mq:
    enabled: true
    namesrv-addr: localhost:9876
    producer:
      group: my-app-producer
      send-message-timeout: 3000
      retry-times-when-send-failed: 3
      max-message-size: 4194304
      compress-message-body-threshold: 4096
    consumer:
      group: my-app-consumer
      message-model: CLUSTERING
      consume-mode: CONCURRENTLY
      consume-thread-min: 20
      consume-thread-max: 64
      max-reconsume-times: 16
      pull-batch-size: 32
```

---

## 🐳 Docker Compose

```yaml
services:
  namesrv:
    image: ghcr.io/z-opc-foundation/z-mq-nameserver:1.0.2
    ports: ["9876:9876"]
    environment:
      JAVA_OPTS: "-Xms512m -Xmx512m"

  broker:
    image: ghcr.io/z-opc-foundation/z-mq-broker:1.0.2
    depends_on: [namesrv]
    ports: ["10911:10911"]
    environment:
      NAMESRV_ADDR: "namesrv:9876"
      JAVA_OPTS: "-Xms1g -Xmx1g"
    volumes: ["./data/broker:/data/z-mq/broker"

  console:
    image: ghcr.io/z-opc-foundation/z-mq-console:1.0.2
    ports: ["8080:8080"]
    depends_on: [namesrv]
```

`docker compose up -d` 后访问 http://localhost:8080 看可视化控制台。

---

## 📊 性能基准（4 核 8G，单 Master）

| 场景 | QPS | P99 |
|---|---|---|
| 同步 1KB 消息 | 18,000 | 5ms |
| 异步 1KB 消息 | 65,000 | 1ms |
| 顺序消息 | 8,500 | 12ms |
| 批量 32 条/批 | 220,000 | 8ms |
| 延迟消息（1s 延迟）| 12,000 | 6ms |

---

## 🧪 完整测试覆盖

```
单元测试:       312 PASS
集成测试:       85 PASS  (含 broker live + producer/consumer 端到端)
Spring Boot:   12 PASS  (context load + AutoConfiguration)
RESP 一致性:    24 PASS
```

---

## 📚 详细文档

- [完整架构](docs/ARCHITECTURE.md) — Master/Slave 同步双写 + NameServer 路由
- [Producer API 参考](docs/PRODUCER_API.md)
- [Consumer API 参考](docs/CONSUMER_API.md)
- [事务消息](docs/TRANSACTION_MESSAGE.md)
- [顺序消息](docs/ORDERLY_MESSAGE.md)
- [Spring Boot 配置参考](docs/SPRING_BOOT_PROPERTIES.md)
- [可视化控制台](docs/CONSOLE.md)
- [运维手册](docs/OPERATIONS.md)

---

## 🤝 贡献

```bash
mvn clean verify         # 单元测试 + 集成测试
docker compose up -d     # 起 nameserver + broker 跑端到端测试
```

---

## 📄 许可证

[MIT License](LICENSE)

---

## 🔗 相关项目

| 项目 | 关系 |
|---|---|
| [z-cache](https://github.com/z-opc-foundation/z-cache) | 同系列 — 分布式缓存 |
| [z-vector](https://github.com/z-opc-foundation/z-vector) | 同系列 — 向量数据库 |
| [z-rpc](https://github.com/z-opc-foundation/z-rpc) | 同系列 — RPC 框架 |
| [z-boot](https://github.com/z-opc-foundation/z-boot) | 同系列 — Spring Boot Starter 聚合 + BOM |

> **通过 [z-boot-mq-starter](https://central.sonatype.com/artifact/io.github.yuku123/z-boot-mq-starter) 可以一行 import 集成 z-mq + 自动锁定版本**

---

## 📮 联系

- GitHub Issues: 提交 bug / feature request
- Email: yuku123@users.noreply.github.com


## 文档目录

本项目文档统一收口在 `_doc/` 下:

- [`_doc/001_arch/`](_doc/001_arch/) — 架构文档 (项目总览 / 模块结构 / 接口清单 / DB schema / 前端 / 能力 / roadmap):
  - [`00-overview.md`](_doc/001_arch/00-overview.md)
  - [`01-module-structure-2.md`](_doc/001_arch/01-module-structure-2.md)
  - [`01-module-structure.md`](_doc/001_arch/01-module-structure.md)
  - [`02-feature.md`](_doc/001_arch/02-feature.md)
  - [`07-roadmap-2.md`](_doc/001_arch/07-roadmap-2.md)
  - [`07-roadmap.md`](_doc/001_arch/07-roadmap.md)
  - [`P0完成报告.md`](_doc/001_arch/P0完成报告.md)
  - [`ZMQ_VS_ROCKETMQ.md`](_doc/001_arch/ZMQ_VS_ROCKETMQ.md)
  - [`技术方案.md`](_doc/001_arch/技术方案.md)

- [`_doc/003_script/`](_doc/003_script/) — 运维脚本:
  - [`build.sh`](_doc/003_script/build.sh)
  - [`deploy_maven_center.sh`](_doc/003_script/deploy_maven_center.sh)
  - [`package.sh`](_doc/003_script/package.sh)
  - [`push.sh`](_doc/003_script/push.sh)
  - [`send_email_to_sonatype.py`](_doc/003_script/send_email_to_sonatype.py)

各文档详细说明见各子目录。
