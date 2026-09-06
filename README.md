# z-mq

> 分布式消息队列

自研分布式消息队列, 含 NameServer + Broker + Producer + Consumer 完整链路, 类 RocketMQ 架构

---

## 📋 基本信息

| 字段 | 值 |
|------|-----|
| **项目** | z-mq |
| **分类** | 基础设施 · 消息队列 |
| **父项目** | z-opc (com.zifang:z-opc:1.0.0-SNAPSHOT) |
| **默认端口** | `9876 (nameserver) / 10911 (broker)` |
| **文档维护** | z-opc-foundation |
| **最近更新** | 2026-09-06 |

---

## 🎯 核心功能

自研分布式消息队列, 含 NameServer + Broker + Producer + Consumer 完整链路, 类 RocketMQ 架构

详细功能特性详见各子模块 README 或源码注释。

---

## 🏗️ 项目结构

```
z-mq/
├── pom.xml                      # 根 POM (引用 z-opc 父项目)
├── README.md                    # 本文档
├── MODULE_NOTE.md               # 来源说明 (从 z-opc 拆分)
```

子模块列表:

| 模块 | 职责 |
|------|------|
| `z-mq-common/` | 公共协议 + Remoting |
| `z-mq-remoting/` | Netty 远程通信 |
| `z-mq-store/` | 消息存储 (CommitLog) |
| `z-mq-nameserver/` | NameServer 路由 |
| `z-mq-broker/` | Broker 服务器 |

---

## 🔧 技术栈

- Java 8+
- Netty Remoting
- CommitLog 存储

---

## 🚀 快速开始

### 前置条件

- JDK 8+ (推荐 JDK 17)
- Maven 3.6+
- 端口 `9876 (nameserver) / 10911 (broker)` 未被占用

### 编译

```bash
# 在 z-opc 父项目下编译 (推荐)
cd /Users/zifang/workplace/idea_workplace/z-opc
mvn clean install -pl :z-opc -am -DskipTests

# 单独编译本模块 (需 ../pom.xml 父项目可用)
cd /Users/zifang/workplace/ceo_workplace/z-opc-foundation/z-mq
mvn clean compile
```

### 运行

```bash
# 启动主服务 (根据项目类型选择)
mvn -pl <启动模块> spring-boot:run
# 或
java -jar <启动模块>/target/*.jar
```

---

## 📦 模块说明

z-mq 由以下子模块组成:

| `z-mq-common/` | 公共协议 + Remoting |
| `z-mq-remoting/` | Netty 远程通信 |
| `z-mq-store/` | 消息存储 (CommitLog) |
| `z-mq-nameserver/` | NameServer 路由 |
| `z-mq-broker/` | Broker 服务器 |

各模块职责详见各子目录下的 `pom.xml` 和源码。

---

## 🧪 测试

```bash
mvn test
```

测试覆盖:
- 单元测试: 各核心服务类
- 集成测试: 端到端调用链路
- 性能测试: 详见 `/src/test` 下的 `*PerformanceTest.java`

---

## 🔌 API 接口

API 接口定义在各子模块的 `controller` 包下。

启动后访问 `http://localhost:9876 (nameserver) / 10911 (broker)/swagger-ui.html` 或 `/doc.html` (knife4j) 查看完整 API 文档。

---

## 🐳 部署

### Docker

```bash
# 构建镜像
docker build -t z-mq:latest .

# 运行容器
docker run -d -p 9876 (nameserver) / 10911 (broker):9876 (nameserver) / 10911 (broker) --name z-mq z-mq:latest
```

### 配置

主要配置文件:
- `application.yml` - Spring Boot 配置
- `logback.xml` - 日志配置
- 环境变量: `JAVA_OPTS`, `SPRING_PROFILES_ACTIVE`

---

## 📚 相关文档

- [MODULE_NOTE.md](./MODULE_NOTE.md) - 从 z-opc 拆分说明
- [z-opc 父项目](https://github.com/yuku123/z-opc) - 完整源码

---

## 📝 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 1.0.0 | 2026-09-06 | 从 z-opc monorepo 拆分独立仓, 文档补齐 |

---

## 📄 License

Internal use only. 版权属于 z-biz。

_Maintained by z-opc-foundation organization._
