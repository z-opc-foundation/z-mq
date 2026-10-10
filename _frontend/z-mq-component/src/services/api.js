import request from '@/common'

/**
 * z-mq（自研 RocketMQ 风格消息中间件）管理面数据源。
 *
 * ⚠ 这一族接口和 /api/vector、/api/graph **不是同一种东西**。
 * z-vector 有 QdrantRestServer(HTTP)、z-graph 有 GraphControlServer(JDK HttpServer)，
 * 所以那两个代理是"逐字节透传"。z-mq **一个 HTTP 面都没有**（对 z-mq 全仓 Java 源码扫
 * `HttpServer|RestController|@GetMapping` 命中 0 条）：:9876 与 :10911 都是
 * `NettyRemotingServer` + `RequestCode` 命令字的**私有二进制协议**。
 * 所以 `/api/mq/**` 是 z-opc 侧 `MqProxyController` 用 `MQClientInstance.invokeSync`
 * 发**固定只读请求码**再转 JSON 的**协议适配器**，不是通配转发器 ——
 * 端点列表因此是穷举的，`/api/mq/<任何没列出的路径>` 一律 404 并回显可用清单。
 *
 * 为什么必须有这一层（和另外两张卡同一条理由）：前端只有一个 baseURL 为 /api 的 axios，
 * 而裸 9876/10911 不在 sso.intercept-paths 的两条 pattern 覆盖范围内（那两条是"任意前缀下的
 * api 子树"与"agent 子树"两条 Ant 通配）= 不设防，
 * 且它俩说的是二进制协议，浏览器根本没法直接讲。
 * ⚠ 这两条 pattern 不要把字面量抄进本注释: 形如 星号星号斜杠 的写法会提前闭合块注释
 *   （实测把整段文档变成代码, 页面只剩 "api is not defined" 一句话）。
 *
 * ── 响应形状（两种，页面都要认）────────────────────────────────
 * 成功：`{tier, target, requestCode, requestName, elapsedMs, responseCode, remark, data}`
 *   `data` 就是上游各 handler 自己 `JsonCodec.encode(...)` 的那份 JSON，本层不重命名、不裁剪。
 *   ⚠ 这一格在代理层刻意叫 `responseCode` 而不是 `code`：共享的 `common/utils/request.ts` 里
 *   `defaultUnwrap` 见到"同时有 code 和 data 两个键"就按 z-opc 的 `Result` 信封解包，且要求
 *   `code` 落在 2xx —— 而 z-mq 成功回的是 `code=0`，于是**每一个正常的只读码响应都会被解包成
 *   `Promise.reject(new Error('请求失败'))`**：HTTP 实测 200、curl 拿到完整 JSON，页面却全红
 *   （cluster / topics / broker / 实发探针四档一起坏）。改字段名比给中间件开解包特例便宜，
 *   而且和已有的 `requestCode` 成对，读的人不必猜是哪个码。
 *   注意 **`responseCode` 是 z-mq 的业务码不是 HTTP 码**：`0=SUCCESS`、`1=SYSTEM_ERROR`、
 *   `2=SYSTEM_BUSY`、`3=REQUEST_CODE_NOT_SUPPORTED`、`4=TRANSACTION_FAILED`。
 *   HTTP 会是 200 而 `responseCode=3` —— 只看 HTTP 状态就把"这个码上游没实现"当成"集群是空的"。
 * 失败：`{... , httpStatus, error, hint}` 且 HTTP 状态码同 httpStatus（502 上游调不通 /
 *   503 端口没人 / 403 拒绝非环回目标 / 400 参数非法 / 404 端点不存在）。
 * 每条成功响应还带 `attributionVerified`；为 false 时附 `attributionUnverified` +
 *   `attributionNote`，含义见 InstanceStatus 顶部那块红条。
 *
 * ── 上游各端点的 data 形状（逐个对着 z-mq 1.2.0 的 processor 源码核过，并且用
 *    JsonCodec 实跑过一遍打印出真实 JSON 字节，见交接文件 E9；下面的键名是实跑输出不是猜的）──
 *   GET /api/mq/cluster                码 4   → {brokerAddrTable:{name:{cluster,brokerName,brokerAddrs:{"0":"127.0.0.1:10911"}}}, clusterAddrTable:{cluster:[brokerName]}}
 *   GET /api/mq/topics                 码 8   → {topics:["name", …]}   ← 只有名字，队列数要另打 route
 *   GET /api/mq/route?topic=X          码 3   → {orderTopicConf:0,order:false,queueDatas:[{brokerName,readQueueNums,writeQueueNums,perm,topicSysFlag}],brokerDatas:[{cluster,brokerName,brokerAddrs:{"0":addr}}],filterServerOuterMap:{}}
 *   GET /api/mq/broker/topics          码 8   → {topics:[…]}           ← 本 broker 的 topicConfigTable，和 namesrv 那份可能不一致
 *   GET /api/mq/broker/topic-config    码 320 → {topicConfigTable:{topic:{topicName,readQueueNums,writeQueueNums,perm,order,topicSysFlag,unit}},dataVersion:{timestamp,counter,counterValue}}
 *   GET /api/mq/broker/consumer-offset 码 321 → {offsetTable:{topic:{"<queueId>":maxOffset}},dataVersion:{…}}
 *   GET /api/mq/broker/delay-offset    码 322 → **与 321 同一个 wrapper 类**：{offsetTable:…,dataVersion:…}，
 *                                            差别只有 dataVersion。源码注释「延迟消息 offset 与 Topic 自身 offset 共享」，
 *                                            所以这一格显示的不是独立的延迟队列统计。
 *   GET /api/mq/broker/subscription    码 323 → {offsetTable:{},dataVersion:{timestamp,counter:0,counterValue:0}}
 *                                            ← 注意 body 里根本没有 subscription 字样：MVP 直接返回了一个
 *                                              **空的 ConsumerOffsetSerializeWrapper**（源码如此）。
 *                                              所以这里恒空、而且结构都对不上，绝对不许渲染成"集群里没有订阅组"。
 *   GET /api/mq/broker/data-version    码 324 → {topicConfig:{timestamp,counter,counterValue},consumerOffset:{…},delayOffset:{…}}
 *                                            ← 是「三份 DataVersion 的 map」，不是单个版本号。
 *
 *   两条最容易踩的语义：
 *   1) 码 3 查不存在的 topic 时，上游回的是 `responseCode=1 SYSTEM_ERROR` + remark `"topic X not exist"` ——
 *      **同一个码 1 既表示"系统错误"也表示"topic 不存在"**，只能靠 remark 文本分开，见 routeNotExistRemark()。
 *   2) namesrv 的码 8 取的是 `topicQueueTable.keySet()`，broker 的码 8 取的是 `topicConfigTable.keySet()`，
 *      两者**天然可以不一致**（broker 侧删了/没同步，或 namesrv 上还留着已消失 broker 的项）。
 *      Topic 页把这份差值显式列出来，不许只画其中一边就宣称"这是全量 topic"。
 *
 *   三个常量/字面量的坑（都是 E9 实跑出来的，别按 RocketMQ 的直觉解释）：
 *   · perm：z-mq 自己是 PERM_READ=1 / PERM_WRITE=2 / PERM_READ_WRITE=3 / PERM_INHERIT=0，
 *     **不是 RocketMQ 的 4/2/6**；新建 topic 实测就是 3。看到 perm=6 才该怀疑数据。
 *   · queueDatas[].topicSysFlag：上游 `AdminBrokerProcessor.toQueueData()` 压根没 set 它 ⇒ 恒 0，
 *     不是"这个 topic 没有系统标记"的信息，是"上游没填"。
 *   · brokerDatas[].cluster 会是 `null`（`BrokerData.cluster` 没被填），页面要显示成"上游没填"而不是空白。
 *
 * ── 三格故意没做（不是偷懒，是 1.2.0 没码可发）─────────────────
 *   消费者在线状态：`CONSUMER_HEARTBEAT(221/222)` 在 BrokerController.registerProcessor 里**没注册**。
 *   消息查询：`QUERY_MESSAGE(230)` / `VIEW_MESSAGE_BY_KEY(231)` 同样没注册。
 *   Broker 运行配置：`GET_BROKER_CONFIG(402)` 没注册。
 *   ⇒ 页面上不出现这三类表格，也不放假数据；要做得先在 z-mq 侧补 processor（另立卡）。
 *   `PULL_MESSAGE(210)` 虽注册但会被 `PullRequestHoldService` 长轮询挂住连接，读面也不碰。
 *
 * ── 写面整卡缺席 ────────────────────────────────────────────
 * 建删 topic（5/7/400）、KV 写（10/11）、重置位点（410）、终止消费者（411）、发消息（200/201/202）
 * 全部不可达：代理层没有能传"码"的入口，页面上也没有任何清空/删除/重启按钮。本卡只做读面。
 */
export const mqApi = {
    /** 自省：由 MqProxyController 自己应答，不经过 z-mq，所以内嵌全挂时它也必须能通 */
    instance: () => request.get('/mq/__instance'),

    cluster: () => request.get('/mq/cluster'),
    topics: () => request.get('/mq/topics'),
    route: (topic) => request.get('/mq/route', {params: {topic}}),

    brokerTopics: (brokerName) => request.get('/mq/broker/topics', {params: brokerName ? {brokerName} : {}}),
    brokerTopicConfig: (brokerName) => request.get('/mq/broker/topic-config', {params: brokerName ? {brokerName} : {}}),
    brokerConsumerOffset: (brokerName) => request.get('/mq/broker/consumer-offset', {params: brokerName ? {brokerName} : {}}),
    brokerDelayOffset: (brokerName) => request.get('/mq/broker/delay-offset', {params: brokerName ? {brokerName} : {}}),
    brokerSubscription: (brokerName) => request.get('/mq/broker/subscription', {params: brokerName ? {brokerName} : {}}),
    brokerDataVersion: (brokerName) => request.get('/mq/broker/data-version', {params: brokerName ? {brokerName} : {}}),
}

/** z-mq 的业务码（不是 HTTP 码）；只有 0 是成功 */
export const ZMQ_CODE = {
    0: 'SUCCESS',
    1: 'SYSTEM_ERROR',
    2: 'SYSTEM_BUSY',
    3: 'REQUEST_CODE_NOT_SUPPORTED',
    4: 'TRANSACTION_FAILED',
}

export function isMqSuccess(res) {
    return !!res && res.responseCode === 0
}

/**
 * 失败有三层来源，只认第一层会把另外两层渲染成"没有数据"：
 *   1. 代理层自身的 err()：{status:"error", source:"z-opc-mq-proxy", message, path, shape}
 *   2. remoting 调用失败：{error, hint, target, requestCode}（502/503/403）
 *   3. 上游业务码非 0 但 HTTP 200：{responseCode, remark} —— 这层最容易漏，
 *      特别是 responseCode=3「该请求码上游没实现」，它长得特别像"查到了，但是空的"。
 */
export function mqErrorText(e) {
    const data = e?.response?.data
    if (data && typeof data === 'object') {
        const message = data.message || data.error
        if (message) return `${message}${data.hint ? ` — ${data.hint}` : ''}`
        if (data.remark) return `z-mq 返回 responseCode=${data.responseCode} (${ZMQ_CODE[data.responseCode] || '?'}) ${data.remark}`
    }
    if (typeof data === 'string' && data.trim()) return data.slice(0, 300)
    return e?.message || String(e)
}

/** 成功响应但业务码非 0 —— 页面要单独挂一条，不能当空表渲染 */
export function mqUpstreamErrorText(res) {
    if (!res || res.responseCode === 0) return null
    return `上游返回 responseCode=${res.responseCode}（${ZMQ_CODE[res.responseCode] || '未知码'}）`
        + `${res.remark ? `：${res.remark}` : ''}`
        + (res.responseCode === 3 ? ' —— 这个请求码在 z-mq 当前版本没注册，是"上游没实现"，不是"集群里没有"' : '')
}

/**
 * "数据不属于本次构建"的判据。代理层把 ownerPid 与"本 JVM spawn 的子进程 pid"比过了，
 * 不等就置 attributionUnverified —— z-mq 的内嵌形态是**子进程**，所以不能用
 * "属主 == 8888 那个 pid"来判，代理层已经按子进程判过了，页面只消费结论。
 */
export function isUnattributed(res) {
    return !!(res && (res.attributionUnverified || res.attributionVerified === false))
}

/** 503/连不上 与"上游确实返回了空集合"是两件事，页面必须分开显示 */
export function isNotReachableError(e) {
    const s = e?.response?.status
    if (s === 503 || s === 502) return true
    return /没有任何监听者|不是本 JVM|Address already in use|not bound|Connect/.test(String(mqErrorText(e)))
}

/** broker 元数据里"上游 MVP 恒空/恒简化"的那几格，页面要挂原文而不是留白 */
export const KNOWN_EMPTY_IN_120 = {
    'broker/subscription': 'BrokerOutAPI.handleGetAllSubscriptionGroup 的源码注释写着「MVP: 订阅组配置暂未单独存储, 返回空 wrapper + 默认版本号」，'
        + '而且它实际 new 出来的是一个空的 ConsumerOffsetSerializeWrapper（body 里连 subscription 字段都没有）'
        + '⇒ 这张表恒空，不代表集群里没有订阅组',
    'broker/delay-offset': '源码注释「MVP: 延迟消息 offset 与 Topic 自身 offset 共享 (无 SCHEDULE_TOPIC_XXXX 隔离队列)。'
        + '返回与 GET_ALL_CONSUMER_OFFSET 一致即可」⇒ 这一格的 offsetTable 和 321 那份是同一份数据，只有 dataVersion 不同',
    'broker/consumer-offset': 'collectOffsetTable 的注释「简化: 遍历可能的 queueId (MVP 用 0-15)」且取的是各 group 的 max '
        + '⇒ 显示的是"该 topic 前 16 个队列里出现过的最大 offset"，不是某个消费组的真实位点',
}

/**
 * 码 3 的 responseCode=1 有两种含义，只有 remark 能分开：
 * DefaultRequestProcessor.handleGetRouteInfoByTopic 在 `pickupTopicRouteData(topic) == null` 时
 * 回的是 `SYSTEM_ERROR(1)` + remark `"topic X not exist"`，而 topic 缺失/真正的编码失败也是 1。
 * ⇒ 不许把所有 responseCode=1 都说成"topic 不存在"，也不许把它说成"上游坏了"；这里只判"remark 明确说了 not exist"。
 */
export function routeNotExistRemark(res) {
    if (!res || res.responseCode === 0) return null
    const remark = String(res.remark || '')
    return /not exist/i.test(remark) ? remark : null
}

/** DataVersion 实跑出来是 {timestamp, counter, counterValue}；counter 是 AtomicLong 序列化的数字 */
export function dataVersionText(dv) {
    if (!dv || typeof dv !== 'object') return '—'
    const c = dv.counter ?? dv.counterValue
    const t = dv.timestamp ? new Date(dv.timestamp).toLocaleString('zh-CN', {hour12: false}) : '-'
    return `counter=${c ?? '?'} · timestamp=${t}`
}

/** topic 名要和代理层的 SAFE_TOPIC 对齐，否则 400 会被读成"topic 不存在" */
export const TOPIC_PATTERN = /^[A-Za-z0-9_\-.:;%#]{1,120}$/
