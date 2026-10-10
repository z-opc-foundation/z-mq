import {useCallback, useEffect, useMemo, useState} from 'react'
import {Alert, Button, Card, Col, Descriptions, Row, Space, Statistic, Table, Tag} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {mqApi, mqErrorText, mqUpstreamErrorText, routeNotExistRemark} from '../services/api'
import {useInstance} from '../services/useInstance'
import {AttributionUnavailableBanner, ForeignListenerBanner, UnattributedDataBanner, ZombieChildBanner} from './AttributionBanner'
import {EmptyState, PageHeader} from '@/common/components/ui'

/**
 * Topic 列表 — NameServer 码 8 与 Broker 码 8 **并排**，外加按需下的码 3 路由详情。
 *
 * 为什么一定要两份并列而不是"取一份全量"：两侧取的是不同的表 ——
 * NameServer 的 `handleGetAllTopicList` 回的是 `RouteInfoManager.topicQueueTable.keySet()`，
 * Broker 的回的是 `topicConfigTable.keySet()`。它们天然可以不一致（broker 侧删了没同步、
 * 或 NameServer 上还留着已消失 broker 的注册项）。只画其中一边就宣称"这是全量 topic"，
 * 是本卡最容易伪造出"看起来对"的一格，所以差集显式列出来。
 *
 * 队列数/权限**不在**这两份列表里（码 8 只有名字），必须对单个 topic 下码 3，
 * 所以路由详情是展开时才发的懒请求 —— 打开页面不会打出一串请求。
 */
const TOPIC_COLUMNS = [
    {
        title: 'Topic', dataIndex: 'topic', key: 'topic',
        render: (v) => <code>{v}</code>
    },
    {
        title: 'NameServer (码 8)', dataIndex: 'onNamesrv', key: 'onNamesrv', width: 160,
        render: (v) => v ? <Tag color="success">已注册</Tag> : <Tag color="error">没有</Tag>
    },
    {
        title: 'Broker (码 8)', dataIndex: 'onBroker', key: 'onBroker', width: 140,
        render: (v, row) => v
            ? <Tag color="success">有配置</Tag>
            : (row.brokerUnavailable ? <Tag>未取到</Tag> : <Tag color="error">没有</Tag>)
    },
    {
        title: '一致性', key: 'consistency', width: 200,
        render: (_, row) => row.onNamesrv && row.onBroker
            ? <Tag color="success">两侧一致</Tag>
            : <Tag color="warning">{row.onNamesrv ? '只在 NameServer（疑似残留注册）' : '只在 Broker（未向 NameServer 注册）'}</Tag>
    },
]

export default function TopicListPage() {
    const {instance, poll} = useInstance()
    const [nsRes, setNsRes] = useState(null)
    const [brRes, setBrRes] = useState(null)
    const [error, setError] = useState(null)
    const [brokerError, setBrokerError] = useState(null)
    const [loading, setLoading] = useState(false)
    const [routes, setRoutes] = useState({})

    const load = useCallback(async () => {
        setLoading(true)
        setError(null)
        setBrokerError(null)
        // broker 侧不指定 brokerName：代理层会从 NameServer 的码 4 自己发现地址，并且只允许环回。
        // 发现到多个 broker 时它会取第一个并在响应里回显 multipleBrokersDiscovered，
        // 要逐个看请去「Broker 元数据」页，那里有 broker 选择器。
        await Promise.all([
            mqApi.topics().then(setNsRes).catch((e) => {
                setNsRes(null)
                setError(e)
            }),
            mqApi.brokerTopics().then(setBrRes).catch((e) => {
                setBrRes(null)
                setBrokerError(e)
            }),
        ])
        setLoading(false)
        poll()
    }, [poll])

    useEffect(() => {
        load()
    }, [load])

    const loadRoute = useCallback(async (topic) => {
        if (routes[topic]) return
        setRoutes((prev) => ({...prev, [topic]: {loading: true}}))
        try {
            const r = await mqApi.route(topic)
            setRoutes((prev) => ({...prev, [topic]: r}))
        } catch (e) {
            setRoutes((prev) => ({...prev, [topic]: {loadError: mqErrorText(e)}}))
        }
    }, [routes])

    const nsTopics = nsRes && nsRes.responseCode === 0 && Array.isArray(nsRes.data?.topics) ? nsRes.data.topics.map(String) : null
    const brTopics = brRes && brRes.responseCode === 0 && Array.isArray(brRes.data?.topics) ? brRes.data.topics.map(String) : null

    const rows = useMemo(() => {
        const all = new Set([...(nsTopics || []), ...(brTopics || [])])
        return Array.from(all).sort().map((t) => ({
            key: t,
            topic: t,
            onNamesrv: nsTopics ? nsTopics.includes(t) : false,
            onBroker: brTopics ? brTopics.includes(t) : false,
            brokerUnavailable: !brTopics,
        }))
    }, [nsTopics, brTopics])

    const onlyNamesrv = rows.filter((r) => r.onNamesrv && !r.onBroker).length
    const onlyBroker = rows.filter((r) => !r.onNamesrv && r.onBroker).length
    const nsUpstreamErr = nsRes && !error ? mqUpstreamErrorText(nsRes) : null
    const brUpstreamErr = brRes && !brokerError ? mqUpstreamErrorText(brRes) : null
    const nothingLoaded = !nsTopics && !brTopics

    return (
        <div>
            <PageHeader title="Topic 列表"
                        subtitle="NameServer 码 8 GET_ALL_TOPIC_LIST ∥ Broker 码 8 GET_ALL_TOPIC_LIST → 差集 + 按需码 3 路由详情"/>
            <Card extra={<Space>
                <Button icon={<ReloadOutlined/>} loading={loading} onClick={load}>刷新</Button>
            </Space>}>
                <ForeignListenerBanner data={instance}/>
                <ZombieChildBanner data={instance}/>
                <AttributionUnavailableBanner data={instance}/>
                {error && (
                    <Alert type="error" showIcon style={{marginBottom: 12}}
                           message={`读 NameServer 的 topic 列表失败：${mqErrorText(error)}`}/>
                )}
                {brokerError && (
                    <Alert type="error" showIcon style={{marginBottom: 12}}
                           message={`读 Broker 的 topicConfigTable 失败：${mqErrorText(brokerError)}`}
                           description="Broker 侧的地址全靠 NameServer 的码 4 发现，且只允许环回。这一档失败通常说明集群里根本没有注册过 broker（见「集群与 Broker」页），或者代理层按环回白名单拒了外部地址 —— 后者是正确行为。"/>
                )}
                {nsUpstreamErr && <Alert type="warning" showIcon style={{marginBottom: 12}} message={`NameServer：${nsUpstreamErr}`}/>}
                {brUpstreamErr && <Alert type="warning" showIcon style={{marginBottom: 12}} message={`Broker：${brUpstreamErr}`}/>}
                {nsRes && !error && <UnattributedDataBanner res={nsRes}/>}
                {brRes && !brokerError && <UnattributedDataBanner res={brRes}/>}

                <Row gutter={16} style={{marginBottom: 12}}>
                    <Col span={6}><Statistic title="NameServer 侧 topic 数" value={nsTopics ? nsTopics.length : '—'}/></Col>
                    <Col span={6}><Statistic title="Broker 侧 topic 数" value={brTopics ? brTopics.length : '—'}/></Col>
                    <Col span={6}><Statistic title="只在 NameServer" value={brTopics ? onlyNamesrv : '—'}
                                             valueStyle={onlyNamesrv && brTopics ? {color: '#b45309'} : undefined}/></Col>
                    <Col span={6}><Statistic title="只在 Broker" value={brTopics ? onlyBroker : '—'}
                                             valueStyle={onlyBroker && brTopics ? {color: '#b45309'} : undefined}/></Col>
                </Row>

                {nothingLoaded ? (
                    <EmptyState title="两侧都没取到 topic 列表"
                                description="这不是「集群里没有 topic」的证据：上面任一档报错、或上游回了非 0 业务码（HTTP 仍是 200），都到这里显示为取不到。先读上面的红/黄条定性，再决定要不要怀疑数据。"/>
                ) : (
                    <Table size="small" rowKey="key" loading={loading} dataSource={rows}
                           columns={TOPIC_COLUMNS} pagination={{pageSize: 20, hideOnSinglePage: true}}
                           expandable={{
                               onExpand: (expanded, row) => { if (expanded) loadRoute(row.topic) },
                               expandedRowRender: (row) => <RouteDetail state={routes[row.topic]}/>,
                           }}/>
                )}
            </Card>

            <Card title="这一页的两个语义坑" style={{marginTop: 16}}>
                <div style={{color: '#64748b', fontSize: 12, lineHeight: 1.9}}>
                    <div>· 码 8 只回<b>名字</b>，队列数 / perm / 集群归属要展开走码 3；展开是懒加载，所以打开本页不会打出 N 个请求。</div>
                    <div>· 码 3 查不存在的 topic 时上游回的是 <code>responseCode=1 SYSTEM_ERROR</code> + remark
                        「topic X not exist」—— <b>同一个码 1 既表示系统错误也表示 topic 不存在</b>，
                        只有 remark 能分开，所以下面把 remark 原文贴出来而不是翻译成"出错了"。</div>
                </div>
            </Card>
        </div>
    )
}

function RouteDetail({state}) {
    if (!state) return <span style={{color: '#94a3b8', fontSize: 12}}>展开时向 NameServer 发送码 3 GET_ROUTEINFO_BY_TOPIC。</span>
    if (state.loading) return <span style={{color: '#94a3b8', fontSize: 12}}>查询中…</span>
    if (state.loadError) return <Alert type="error" showIcon message={`路由查询失败：${state.loadError}`}/>
    const notExist = routeNotExistRemark(state)
    const upstreamErr = mqUpstreamErrorText(state)
    const queues = Array.isArray(state.data?.queueDatas) ? state.data.queueDatas : []
    const brokerDatas = Array.isArray(state.data?.brokerDatas) ? state.data.brokerDatas : []
    return (
        <div>
            {notExist && <Alert type="warning" showIcon style={{marginBottom: 8}}
                                message={`NameServer 报这个 topic 没有路由（原文：${notExist}）`}
                                description="上游用的是 SYSTEM_ERROR(1) + remark，不是「查到但是空」，所以这一格不能渲染成空表。"/>}
            {!notExist && upstreamErr && <Alert type="error" showIcon style={{marginBottom: 8}} message={upstreamErr}/>}
            {state.data && <UnattributedDataBanner res={state}/>}
            {state.data && (
                <>
                    <Descriptions bordered size="small" column={3} style={{marginBottom: 8}}
                                  items={[
                                      {key: 'target', label: '目标', children: <code>{state.target || '-'}</code>},
                                      {key: 'order', label: 'order', children: String(state.data.order ?? '-')},
                                      {key: 'elapsed', label: '往返', children: `${state.elapsedMs ?? '-'} ms`},
                                  ]}/>
                    <Table size="small" rowKey={(r) => `${r.brokerName}-${r.queueId ?? ''}`} dataSource={queues}
                           pagination={false}
                           columns={[
                               {title: 'Broker', dataIndex: 'brokerName', key: 'brokerName', render: (v) => <code>{v}</code>},
                               {title: 'readQueueNums', dataIndex: 'readQueueNums', key: 'readQueueNums'},
                               {title: 'writeQueueNums', dataIndex: 'writeQueueNums', key: 'writeQueueNums'},
                               {title: 'perm', dataIndex: 'perm', key: 'perm'},
                               {title: 'topicSysFlag', dataIndex: 'topicSysFlag', key: 'topicSysFlag'},
                           ]}/>
                    {brokerDatas.length > 0 && (
                        <div style={{marginTop: 8, color: '#64748b', fontSize: 12}}>
                            brokerDatas：{brokerDatas.map((b) => `${b.brokerName}→${JSON.stringify(b.brokerAddrs || {})}`).join('，')}
                        </div>
                    )}
                </>
            )}
        </div>
    )
}
