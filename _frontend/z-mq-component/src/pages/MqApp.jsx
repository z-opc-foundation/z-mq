import {Navigate, Route, Routes} from 'react-router-dom'
import InstanceStatus from './InstanceStatus'
import ClusterPage from './ClusterPage'
import TopicListPage from './TopicListPage'
import BrokerMetadataPage from './BrokerMetadataPage'

/**
 * z-mq（自研 RocketMQ 风格消息中间件）管理面 — OpsWorkbench 以 /mq/* 通配挂进来。
 *
 * 与 z-vector / z-graph 那两页的**根本差别**：z-mq 没有 HTTP 管理面。
 * :9876(NameServer) 与 :10911(Broker) 都是 z-mq-remoting 的 Netty 私有二进制协议，
 * 所以 `/api/mq/**` 不是字节透传代理，而是 `MqProxyController` 用 `MQClientInstance.invokeSync`
 * 发**固定只读请求码**再转 JSON 的协议适配器。后果是这四个页面的端点集合是**穷举**的：
 * 上游没注册的码（消费者在线状态 221/222、消息查询 230/231、broker 配置 402）在这里就是没有，
 * 页面宁可不给这一格，也不拿假数据填。
 *
 * 第二个差别：z-mq 的内嵌方式是**子进程**（z-mq 的 Main 会 join 卡死 Tomcat），
 * 所以"页面空白"有两种完全不同的成因 —— 内嵌子进程没 bind 上，或者 bind 上了但集群真的没 topic。
 * 这四页每页顶部都会拉一次 `GET /api/mq/__instance` 并把结论摊在数据上方，
 * 见 ./AttributionBanner.jsx。
 */
export default function MqApp() {
    return (
        <Routes>
            <Route index element={<Navigate to="instance" replace/>}/>
            <Route path="instance" element={<InstanceStatus/>}/>
            <Route path="cluster" element={<ClusterPage/>}/>
            <Route path="topics" element={<TopicListPage/>}/>
            <Route path="broker" element={<BrokerMetadataPage/>}/>
        </Routes>
    )
}
