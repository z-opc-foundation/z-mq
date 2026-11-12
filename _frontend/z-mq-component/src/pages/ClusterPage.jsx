import {useCallback, useEffect, useState} from 'react'
import {Alert, Button, Card, Col, Descriptions, Input, Row, Space, Statistic, Table, Tag} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {mqApi, mqErrorText, mqUpstreamErrorText} from '../services/api'
import {useInstance} from '../services/useInstance'
import {ForeignListenerBanner, UnattributedDataBanner, ZombieChildBanner} from './AttributionBanner'
import {EmptyState} from '@yuku123/z-frontend-common/ui'
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
        title: '端口核对', key: 'portMatch', width: 240,
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

    const [keyword, setKeyword] = useState('')
    const filteredRows = keyword.trim()
        ? rows.filter((r) => `${r.brokerName || ''} ${r.cluster || ''} ${r.masterAddr || ''}`.toLowerCase().includes(keyword.trim().toLowerCase()))
        : rows

    const upstreamErr = res && !error ? mqUpstreamErrorText(res) : null

    return (
        <div>
            <Card extra={<Space>
                <Input.Search allowClear placeholder="按 Broker 名 / Cluster / Master 地址过滤" style={{width: 240}}
                              onChange={(e) => setKeyword(e.target.value)}/>
                <Button icon={<ReloadOutlined/>} loading={loading} onClick={() => { load(); poll() }}>刷新</Button>
            </Space>}>
                <ForeignListenerBanner data={instance}/>
                <ZombieChildBanner data={instance}/>
                {error && (
                    <Alert type="error" showIcon
                           message={`读 NameServer 失败：${mqErrorText(error)}`}/>
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
                            <EmptyState title="还没有 broker 注册"
                                        description="没有任何 broker 向这台 NameServer 发过注册心跳，或 broker 子进程未启动。"/>
                        ) : (
                            <Table size="small" rowKey="key" loading={loading}
                                   dataSource={filteredRows} columns={BROKER_COLUMNS} pagination={false /* §2 规则 2 豁免：brokerAddrTable 是 broker 向本 NameServer 的注册记录，一个集群内 broker 数量有界，不随业务增长 */}/>
                        )}
                    </>
                )}
                {!res && !error && <EmptyState title="还没读到数据" description="点击刷新发送一次只读查询。"/>}
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
        </div>
    )
}
