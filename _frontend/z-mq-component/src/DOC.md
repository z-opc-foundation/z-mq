# z-mq-component

z-mq（自研 RocketMQ 风格消息中间件）跨仓唯一前端面：把「实例与端口 / 集群与 Broker / Topic 列表 /
Broker 元数据」这一整套消息中间件管理页面收敛成一个可复用的 React 组件包。

- **npm 包名**：`@yuku123/z-mq-component`
- **宿主接入**：`@yuku123/z-mq-component/pages`（manifest + 页面 + API），主壳可只取 `ComponentDocPage`
- **后端**：z-opc 侧 `MqProxyController` 的 `/api/mq/**`（**协议适配器**，不是字节透传代理）
- **技术栈**：React 19 + antd 6 + react-router-dom 7 + vite（library mode，双入口 `.` / `./pages`）

---

## 一、能力概览

路由前缀 `/z-mq`，菜单与路由在 `src/pages-manifest.jsx` 里声明。

| 页面 | 路由（相对 `/z-mq/*`） | 源文件 | 能力 |
|------|--------------------------|--------|------|
| 登录 | `login` | `pages/LoginPage.jsx` | 登录页（suit 侧单独挂这条路由） |
| 首页 | `home` | `pages/HomePage.jsx` | 入口卡片页 |
| 实例与端口 | `instance` | `pages/Instance.jsx` | nameserver / broker 存活、bind 状态等（简版） |
| Topic 清单 | `topics` | `pages/Topics.jsx` | Topic 列表（简版） |
| 实例与端口（详版） | `*/instance` | `pages/InstanceStatus.jsx` | 自省接口 + 只读请求码实发探针 + 归属判定 |
| 集群与 Broker | `*/cluster` | `pages/ClusterPage.jsx` | NameServer 码 4 的 brokerAddrTable / clusterAddrTable |
| Topic 列表（详版） | `*/topics` | `pages/TopicListPage.jsx` | NameServer 码 8 与 Broker 码 8 的**差集** + 按需码 3 路由详情 |
| Broker 元数据 | `*/broker` | `pages/BrokerMetadataPage.jsx` | BrokerOutAPI 码 320/321/322/323/324 五档 |
| 组件文档 | `docs` | `pages/ComponentDoc.jsx` | 本文件渲染页（左侧 240px TOC + 右侧正文） |

`src/pages/MqApp.jsx` 是子路由挂载点（`instance` / `cluster` / `topics` / `broker`），
被 `routes` 里的 `/z-mq/:rest*` 承接，同时从包根具名导出（OpsWorkbench 以 `/mq/*` 挂它）。

`src/pages/AttributionBanner.jsx` 是四张详版页共用的**归属提示条**：z-mq 是子进程型内嵌，
页面必须先证明「应答的端口上是不是我自己 spawn 的那个进程」，再谈数据。

## 二、导出面

```js
// 主入口（. 桶）
import {mqApi, mqErrorText, isMqSuccess, ZMQ_CODE, MqApp, ComponentDocPage} from '@yuku123/z-mq-component'

// 宿主组装面（./pages 桶）
import {menuItems, routes, HomePage, LoginPage, Instance, Topics} from '@yuku123/z-mq-component/pages'
```

`./pages` 桶是**显式具名转发**（`export {menuItems, routes, Instance, Topics, HomePage, LoginPage} from './pages-manifest.jsx'`）。
新增页面时必须回来补一行，否则消费方构建会报 `"Xxx" is not exported by dist/pages.js`。

## 三、API 端点

请求实例用基座默认桶里的 axios（`baseURL='/api'`），路径自带 `/api/mq` 前缀。
下表是 `mqApi` 里**实际调用**的端点 —— `/api/mq/**` 是**穷举**的，
没列出的路径一律 404 并回显可用清单。

| 方法 | 路径 | 上游请求码 | 说明 |
|---|---|---|---|
| GET | `/api/mq/__instance` | —（代理自己应答） | 自省：不碰 NameServer 也不碰 Broker |
| GET | `/api/mq/cluster` | 4 `GET_BROKER_CLUSTER_INFO` | 集群与 broker 地址表 |
| GET | `/api/mq/topics` | 8 `GET_ALL_TOPIC_LIST` | NameServer 侧 topic 名清单（**只有名字**） |
| GET | `/api/mq/route?topic=` | 3 `QUERY_TOPIC_ROUTE_INFO` | 单个 topic 的路由 / 队列详情 |
| GET | `/api/mq/broker/topics?brokerName=` | 8 `GET_ALL_TOPIC_LIST` | Broker 侧 topicConfigTable 的 key 集 |
| GET | `/api/mq/broker/topic-config?brokerName=` | 320 | topic 配置表 + dataVersion |
| GET | `/api/mq/broker/consumer-offset?brokerName=` | 321 | 消费位点表 + dataVersion |
| GET | `/api/mq/broker/delay-offset?brokerName=` | 322 | 延迟消息位点 + dataVersion |
| GET | `/api/mq/broker/subscription?brokerName=` | 323 | 订阅信息（上游恒返回空 wrapper） |
| GET | `/api/mq/broker/data-version?brokerName=` | 324 | topicConfig / consumerOffset / delayOffset 三份 DataVersion 的 map |

### 3.1 响应形状

成功：`{tier, target, requestCode, requestName, elapsedMs, responseCode, remark, data, attributionVerified}`。
失败：HTTP 状态码与 `httpStatus` 同值（502 上游调不通 / 503 端口没人 / 403 拒绝非环回目标 /
400 参数非法 / 404 端点不存在），body 为 `{error, hint, ...}`。

⚠ 代理层把业务码字段叫 **`responseCode`** 而不是 `code` —— 因为共享的 `defaultUnwrap` 见到
「同时有 code 和 data 两个键」会按 z-opc 的 `Result` 信封解包，而 z-mq 成功回的是 `code=0`，
于是每个正常响应都会被 reject 成「请求失败」。

`responseCode` 是 z-mq 的**业务码**不是 HTTP 码：`0=SUCCESS`、`1=SYSTEM_ERROR`、
`2=SYSTEM_BUSY`、`3=REQUEST_CODE_NOT_SUPPORTED`、`4=TRANSACTION_FAILED`。
HTTP 是 200 而 `responseCode=3` 是常见情况 —— 只看 HTTP 状态会把「这个码上游没实现」
当成「集群是空的」。

## 四、页面说明

### 4.1 实例与端口（`InstanceStatus.jsx`）

顶部用 `PageHeader` 标注「自省接口 `GET /api/mq/__instance`（由 MqProxyController 自己应答，
不经过 z-mq）+ 只读请求码实发探针」。自省不通分三种成因分别提示：
302 = 没带会话 Cookie；404 = `MainWebConfig.spaPaths` 或 `/api/mq` 映射没接上。

### 4.2 集群与 Broker（`ClusterPage.jsx`）

NameServer 码 4 的 `brokerAddrTable` / `clusterAddrTable`。这一档**不需要 broker 注册**就能应答；
不通说明 `:namesrvPort` 上根本没有能答话的进程，或答话的那个跟本 JVM 不是一家的（见顶部红条）。

### 4.3 Topic 列表（`TopicListPage.jsx`）

**NameServer 码 8 与 Broker 码 8 的差集**（namesrv 取 `topicQueueTable.keySet()`，
broker 取 `topicConfigTable.keySet()`，两者天然可能不一致），再按需打码 3 拿路由详情。
差值显式列出 —— 不许只画其中一边就宣称「这是全量 topic」。

### 4.4 Broker 元数据（`BrokerMetadataPage.jsx`）

五档（320 topic 配置 / 321 消费位点 / 322 延迟位点 / 323 订阅 / 324 dataVersion）。
地址全靠码 4 发现且**只允许环回**；发现不到就是整页 503 —— 那是「集群里没有注册过的 broker」，
不是这五档坏了。

### 4.5 组件文档

本文件。内容以 `?raw` 在编译期内联，零额外请求；左侧 240px TOC 随滚动联动。

## 五、配置

| 配置项 | 默认值 | 说明 |
|---|---|---|
| 请求 baseURL | `'/api'`（基座默认桶） | 路径自带 `/api/mq` 前缀 |
| `ZMQ_CODE` | 0–4 的码表 | `SUCCESS` / `SYSTEM_ERROR` / `SYSTEM_BUSY` / `REQUEST_CODE_NOT_SUPPORTED` / `TRANSACTION_FAILED` |
| `isMqSuccess(res)` | `res.responseCode === 0` | 判断业务成功（不是 HTTP 成功） |
| `mqErrorText(e)` | — | 把三层错误来源都透出可读文案 |
| `mqUpstreamErrorText(res)` | — | 成功 HTTP 但业务码非 0 时的文案（**不能当空表渲染**） |
| `useInstance()` | — | 四张详版页共用的自省读面（`useCallback` 依赖取空，避免 effect 无限重跑） |

## 六、已知限制

- **只读面**：建删 topic（码 5/7/400）、KV 写（10/11）、重置位点（410）、终止消费者（411）、
  发消息（200/201/202）**全部不可达** —— 代理层没有能传「码」的入口，页面上也没有任何
  清空 / 删除 / 重启按钮。
- **三格故意没做**（1.2.0 里没注册 processor，页面宁可不给这一格也不放假数据）：
  消费者在线状态（`CONSUMER_HEARTBEAT` 221/222）、消息查询（230/231）、Broker 运行配置（402）。
  `PULL_MESSAGE`(210) 虽注册，但会被长轮询挂住连接，读面也不碰。
- **码 323（subscription）恒空**：上游直接返回了一个空的 `ConsumerOffsetSerializeWrapper`
  （body 里根本没有 `subscription` 字样），结构也对不上 —— **绝对不许渲染成「集群里没有订阅组」**。
- **码 322（delay-offset）与码 321 共用同一个 wrapper 类**，差别只有 dataVersion；
  源码注释说「延迟消息 offset 与 Topic 自身 offset 共享」，所以这一格显示的不是独立的延迟队列统计。
- **码 1 既表示「系统错误」也表示「topic 不存在」**（查不存在的 topic 时上游回
  `responseCode=1` + remark `topic X not exist`），只能靠 remark 文本分开。
- **三个常量别按 RocketMQ 的直觉解释**：
  `perm` 是 `READ=1 / WRITE=2 / READ_WRITE=3 / INHERIT=0`（**不是** RocketMQ 的 4/2/6，
  新建 topic 实测就是 3）；`queueDatas[].topicSysFlag` 恒 0（上游压根没 set，是「上游没填」）；
  `brokerDatas[].cluster` 可能是 `null`（`BrokerData.cluster` 没被填），要显示成「上游没填」。
- **地址只允许环回**：代理层按环回白名单拒外部地址 —— 这是正确行为，不是故障。
- **页面空白有两种完全不同的成因**：内嵌子进程没 bind 上，或者 bind 上了但集群真的没 topic。
  四张详版页顶部都会拉一次 `GET /api/mq/__instance` 把结论摊在数据上方。
- z-mq 的内嵌方式是**子进程**（z-mq 的 Main 会 join 卡死 Tomcat），不能用「属主 == 8888 那个 pid」
  当归属证明，归属判定口径见 `services/api.js` 的 `isUnattributed()`。
- `Instance.jsx` / `Topics.jsx` 是简版页，`InstanceStatus.jsx` / `TopicListPage.jsx` 是详版页，
  两套并存、路由不同（前者 `/z-mq/instance`、`/z-mq/topics`，后者 `/z-mq/*/instance`、`/z-mq/*/topics`）。
- 组件包必须 external 基座；`vite.config.js` 的 `external` 是精确字符串匹配，
  `@yuku123/z-frontend-common/ui` 与 `/ui/echarts` 两个子路径必须逐个列出，漏了基座会被复制进 dist。

## 七、变更记录

| 版本 | 变更 |
|---|---|
| 0.1.1 | 新增组件文档页 `ComponentDocPage`（lead 008 §7）：`src/DOC.md` + 基座 `ComponentDoc` 薄包装 |
| 0.1.0 | 首版：实例 / 集群 / Topic / Broker 元数据四张详版页 + 归属提示条 + `mqApi` |
