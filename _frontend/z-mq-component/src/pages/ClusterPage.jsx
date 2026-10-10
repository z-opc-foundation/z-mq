import {useCallback, useEffect, useState} from 'react'
import {Alert, Button, Card, Col, Descriptions, Row, Space, Statistic, Table, Tag} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {mqApi, mqErrorText, mqUpstreamErrorText} from '../services/api'
import {useInstance} from '../services/useInstance'
import {ForeignListenerBanner, UnattributedDataBanner, ZombieChildBanner} from './AttributionBanner'
import {EmptyState} from '@/common/components/ui'
import {PageHeader} from '@/common/components/ui'

/**
 * 集群与 Broker — 码 4 `GET_BROKER_CLUSTER_INFO`。
 *
 * 上游 handler 是 nameserver 的 `handleGetBrokerClusterInfo`，它只是把
 * `RouteInfoManager.brokerAddrTable` / `clusterAddrTable` 各 `new HashMap<>(…)` 一份再
 * `JsonCodec.encode`。所以这张表的语义很硬：<b>"谁向 NameServer 发过 REGISTER_BROKER 心跳"</b>，
 * 不是"谁在跑"。两者能差出一个 broker：进程活着但心跳线程没起来 ⇒ 这里就是空的。
 *
 * 为什么这一页不能省：`MqProxyController` 的 broker 侧端点（/mq/broker/*）的地址**全靠这里发现**，
 * 而且只允许拨环回。这张表空 ⇒ broker 页必然全 503，所以 broker 页报错时先回来看这页。
 */
const BROKER_COLUMNS = [
    {
        title: 'Broker 名', dataIndex: 'brokerName', key: 'brokerName', width: 180,
        render: (v) => <code>{v}</code>
    },
    {title: 'Cluster', dataIndex: 'cluster', key: 'cluster', width: 160, render: (v) => v || <Tag>未上报</Tag>},
    {
        title: 'Master 地址 (brokerId=0)', dataIndex: 'masterAddr', key: 'masterAddr', width: 220,
        render: (v) => <code>{v || '-'}</code>
    },
    {
        title: '全部副本 (id → addr)', dataIndex: 'replicas', key: 'replicas',
        render: (list) => list.length
            ? <Space direction="vertical" size={0}>{list.map((r) => <span key={r.id}><Tag color={r.id === '0' ? 'blue' : 'default'}>id={r.id}</Tag><code>{r.addr}</code></span>)}</Space>
            : <span style={{color: '#94a3b8'}}>—</span>
    },
    {
        title: '端口是否等于本 JVM 配置的 broker 端口', key: 'portMatch', width: 240,
        render: (_, row) => {
            if (!row.port) return <Tag>无地址</Tag>
            if (row.portMatched === null) return <Tag color="warning">配置端口未知</Tag>
            return row.portMatched
                ? <Tag color="success">一致（{row.port}）</Tag>
                : <Tag color="error">不一致：注册的是 {row.port}，本 JVM 配的是 {row.expected}</Tag>
        }
    },
]

export default function ClusterPage() {
    const {instance, poll} = useInstance()
    const [res, setRes] = useState(null)
    const [error, setError] = useState(null)
    const [loading, setLoading] = useState(false)

    const load = useCallback(async () => {
        setLoading(true)
        try {
            const r = await mqApi.cluster()
            setRes(r)
            setError(null)
        } catch (e) {
            setRes(null)
            setError(e)
        } finally {
            setLoading(false)
        }
    }, [])

    useEffect(() => {
        load()
        poll()
    }, [load, poll])

    const data = res?.data || {}
    const addrTable = data.brokerAddrTable && typeof data.brokerAddrTable === 'object' ? data.brokerAddrTable : {}
    const clusterTable = data.clusterAddrTable && typeof data.clusterAddrTable === 'object' ? data.clusterAddrTable : {}
    const expected = instance?.brokerPort ?? null

    const rows = Object.keys(addrTable).map((name) => {
        const bd = addrTable[name] || {}
        const addrs = bd.brokerAddrs && typeof bd.brokerAddrs === 'object' ? bd.brokerAddrs : {}
        const replicas = Object.keys(addrs).map((id) => ({id, addr: String(addrs[id])}))
        const master = addrs['0'] ? String(addrs['0']) : (replicas[0]?.addr || null)
        const port = master ? Number(master.split(':').pop()) : null
        return {
            key: name,
            brokerName: bd.brokerName || name,
            cluster: bd.cluster,
            masterAddr: master,
            replicas,
            port,
            expected,
            portMatched: expected == null ? null : (port === expected),
        }
    })

    const clusterRows = Object.keys(clusterTable).map((c) => ({
        key: c, cluster: c, members: (Array.isArray(clusterTable[c]) ? clusterTable[c] : []).map(String),
    }))

    const upstreamErr = res && !error ? mqUpstreamErrorText(res) : null

    return (
        <div>
            <PageHeader title="集群与 Broker"
                        subtitle="NameServer 码 4 GET_BROKER_CLUSTER_INFO → brokerAddrTable / clusterAddrTable"/>
            <Card extra={<Space>
                <Button icon={<ReloadOutlined/>} loading={loading} onClick={() => { load(); poll() }}>刷新</Button>
            </Space>}>
                <ForeignListenerBanner data={instance}/>
                <ZombieChildBanner data={instance}/>
                {error && (
                    <Alert type="error" showIcon
                           message={`读 NameServer 失败：${mqErrorText(error)}`}
                           description={`这一档是 nameserver 的只读码，不需要 broker 注册就能应答；它不通说明 :${instance?.namesrvPort ?? '?'} 上根本没有能答话的进程（或者答话的那个跟本 JVM 不是一家的，见顶部红条）。` +
                               "端口取自 /api/mq/__instance，写死端口会在换端口后把人指到别的进程上。"}/>
                )}
                {upstreamErr && <Alert type="warning" showIcon style={{marginBottom: 12}} message={upstreamErr}/>}
                {res && !error && (
                    <>
                        <UnattributedDataBanner res={res}/>
                        <Row gutter={16} style={{marginBottom: 12}}>
                            <Col span={6}><Statistic title="已注册 Broker 数" value={rows.length}/></Col>
                            <Col span={6}><Statistic title="Cluster 数" value={clusterRows.length}/></Col>
                            <Col span={6}><Statistic title="请求码" value={`${res.requestCode || '-'} ${res.requestName || ''}`}/></Col>
                            <Col span={6}><Statistic title="往返耗时" value={res.elapsedMs ?? '-'} suffix="ms"/></Col>
                        </Row>
                        {rows.length === 0 ? (
                            <EmptyState title="brokerAddrTable 是空的"
                                        description="没有任何 broker 向这台 NameServer 发过 REGISTER_BROKER（码 1）。两种成因要分开：① broker 子进程没起 / 没 bind 上（看顶部红条与「实例与端口」页的 bind 原文）；② broker 起了但配的 zmq.namesrv-addr 指向的是另一台 NameServer。本页不给「注册 broker」按钮 —— 那是写面，另立卡。"/>
                        ) : (
                            <Table size="small" rowKey="key" loading={loading}
                                   dataSource={rows} columns={BROKER_COLUMNS} pagination={false}/>
                        )}
                    </>
                )}
                {!res && !error && <EmptyState title="还没读到数据" description="刷新以发送一次只读请求码 4。"/>}
            </Card>

            {clusterRows.length > 0 && (
                <Card title="Cluster → Broker 归属" style={{marginTop: 16}}>
                    <Descriptions bordered size="small" column={1}
                                  items={clusterRows.map((c) => ({
                                      key: c.key, label: c.cluster,
                                      children: <Space wrap>{c.members.map((m) => <Tag key={m}>{m}</Tag>)}</Space>
                                  }))}/>
                </Card>
            )}

            <Card title="上游语义提示" style={{marginTop: 16}}>
                <div style={{color: '#64748b', fontSize: 12, lineHeight: 1.9}}>
                    <div>· <code>brokerAddrTable</code> 记的是「谁发过 REGISTER_BROKER 心跳」，不是「谁在跑」。进程活着但心跳线程没起 ⇒ 这里为空。</div>
                    <div>· 上表最后一列是本页自带的第二重归属校验：注册的 broker 端口应当等于
                        <code>zmq.broker-port</code>。不等就说明<b>这台 NameServer 记的是另一套集群</b>，
                        后面 broker 页拿到的地址会被代理层的环回白名单挡掉（403），那是正确行为不是 bug。</div>
                    <div>· 代理层只会拨环回地址（<code>127.0.0.1</code> / <code>localhost</code> / <code>::1</code>），
                        非环回的注册地址一律拒绝，这既是安全边界也是防误连别机的护栏。</div>
                </div>
            </Card>
        </div>
    )
}
