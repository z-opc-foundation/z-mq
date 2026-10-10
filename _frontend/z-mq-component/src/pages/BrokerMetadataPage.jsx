import {useCallback, useEffect, useMemo, useState} from 'react'
import {Alert, Button, Card, Col, Descriptions, Row, Select, Space, Statistic, Table, Tag} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {
    KNOWN_EMPTY_IN_120, dataVersionText, mqApi, mqErrorText, mqUpstreamErrorText,
} from '../services/api'
import {useInstance} from '../services/useInstance'
import {AttributionUnavailableBanner, ForeignListenerBanner, UnattributedDataBanner, ZombieChildBanner} from './AttributionBanner'
import {EmptyState, PageHeader} from '@/common/components/ui'

/**
 * Broker 元数据 — 码 320/321/322/323/324 五档一次拉齐。
 *
 * 这五个码全部走 `BrokerOutAPI`，地址由 NameServer 的码 4 发现，且代理层只允许拨环回。
 * 页面上最容易被"做干净"反而出错的是两点，都在这里显式付了费：
 *   1. **码 323 恒空**。`handleGetAllSubscriptionGroup` 的 MVP 实现直接 new 了一个
 *      空的 `ConsumerOffsetSerializeWrapper` 返回（body 里连 subscription 字样都没有），
 *      所以这张表为空<b>不代表</b>集群里没有订阅组 —— 空表旁边必须挂原文，否则就是拿
 *      "上游没实现"冒充"没有数据"。
 *   2. **码 322 与码 321 是同一份数据**（源码注释：延迟消息 offset 与 topic 自身 offset 共享，
 *      无 SCHEDULE_TOPIC_XXXX 隔离队列），两者只有 dataVersion 不同。并排显示时要把这句话写出来，
 *      不然看起来很像是两套独立的位点统计。
 *
 * 三格故意没有（1.2.0 的 BrokerController.registerProcessor 里没注册）：消费者在线状态
 * (221/222)、消息查询 (230/231)、broker 运行配置 (402)。要做得先在 z-mq 侧补 processor。
 */
const SECTIONS = [
    {key: 'topicConfig', path: 'broker/topic-config', label: 'Topic 配置（码 320）', api: 'brokerTopicConfig'},
    {key: 'consumerOffset', path: 'broker/consumer-offset', label: '消费位点（码 321）', api: 'brokerConsumerOffset'},
    {key: 'delayOffset', path: 'broker/delay-offset', label: '延迟位点（码 322）', api: 'brokerDelayOffset'},
    {key: 'subscription', path: 'broker/subscription', label: '订阅组（码 323）', api: 'brokerSubscription'},
    {key: 'dataVersion', path: 'broker/data-version', label: 'DataVersion（码 324）', api: 'brokerDataVersion'},
]

const TOPIC_CONFIG_COLUMNS = [
    {title: 'Topic', dataIndex: 'topic', key: 'topic', render: (v) => <code>{v}</code>},
    {title: 'readQueueNums', dataIndex: 'readQueueNums', key: 'readQueueNums', width: 130},
    {title: 'writeQueueNums', dataIndex: 'writeQueueNums', key: 'writeQueueNums', width: 140},
    {title: 'perm', dataIndex: 'perm', key: 'perm', width: 90},
    {
        title: 'order', dataIndex: 'order', key: 'order', width: 90,
        render: (v) => v ? <Tag color="blue">true</Tag> : <Tag>false</Tag>
    },
    {title: 'unit', dataIndex: 'unit', key: 'unit', width: 110, render: (v) => v ?? '—'},
]

export default function BrokerMetadataPage() {
    const {instance, poll} = useInstance()
    const [brokers, setBrokers] = useState([])
    const [brokerName, setBrokerName] = useState(null)
    const [clusterError, setClusterError] = useState(null)
    const [states, setStates] = useState({})
    const [loading, setLoading] = useState(false)

    /** broker 候选只从码 4 来 —— 不在这里猜名字，也不接受手填地址（代理层的环回白名单会拒非环回） */
    const loadBrokers = useCallback(async () => {
        try {
            const r = await mqApi.cluster()
            const table = r?.data?.brokerAddrTable || {}
            const names = Object.keys(table)
            setBrokers(names)
            setBrokerName((prev) => (prev && names.includes(prev) ? prev : (names[0] || null)))
            setClusterError(r && r.responseCode !== 0 ? new Error(mqUpstreamErrorText(r) || '码 4 返回非 0') : null)
            return names
        } catch (e) {
            setBrokers([])
            setBrokerName(null)
            setClusterError(e)
            return []
        }
    }, [])

    const loadAll = useCallback(async (name) => {
        setLoading(true)
        await Promise.all(SECTIONS.map(async (s) => {
            setStates((prev) => ({...prev, [s.key]: {loading: true}}))
            try {
                const r = await mqApi[s.api](name || undefined)
                setStates((prev) => ({...prev, [s.key]: r}))
            } catch (e) {
                setStates((prev) => ({...prev, [s.key]: {loadError: mqErrorText(e), raw: e}}))
            }
        }))
        setLoading(false)
        poll()
    }, [poll])

    useEffect(() => {
        loadBrokers()
    }, [loadBrokers])

    /** broker 一变就重取五档；loadAll 只依赖 poll，所以这里不会因为 setStates 而自激 */
    useEffect(() => {
        loadAll(brokerName || undefined)
    }, [brokerName, loadAll])

    const offsetRows = (state) => {
        const table = state?.data?.offsetTable
        if (!table || typeof table !== 'object') return null
        return Object.keys(table).flatMap((topic) => {
            const inner = table[topic]
            if (!inner || typeof inner !== 'object') return []
            return Object.keys(inner).map((q) => ({
                key: `${topic}-${q}`, topic, queueId: q, maxOffset: inner[q],
            }))
        })
    }

    const topicConfigRows = useMemo(() => {
        const table = states.topicConfig?.data?.topicConfigTable
        if (!table || typeof table !== 'object') return null
        return Object.keys(table).map((t) => ({key: t, topic: t, ...(table[t] || {})}))
    }, [states.topicConfig])

    return (
        <div>
            <PageHeader title="Broker 元数据"
                        subtitle="BrokerOutAPI 码 320/321/322/323/324 · 地址由 NameServer 码 4 发现且只允许环回"/>
            <Card extra={<Space>
                <span style={{color: '#64748b', fontSize: 12}}>Broker</span>
                <Select size="small" style={{width: 240}} value={brokerName} onChange={setBrokerName}
                        placeholder={brokers.length ? undefined : '码 4 没有发现任何 broker'}
                        options={brokers.map((n) => ({value: n, label: n}))}/>
                <Button icon={<ReloadOutlined/>} loading={loading} onClick={() => loadAll(brokerName || undefined)}>刷新五档</Button>
            </Space>}>
                <ForeignListenerBanner data={instance}/>
                <ZombieChildBanner data={instance}/>
                <AttributionUnavailableBanner data={instance}/>
                {clusterError && (
                    <Alert type="error" showIcon style={{marginBottom: 12}}
                           message={`无法从 NameServer 发现 broker：${mqErrorText(clusterError)}`}
                           description="下面五档的地址全靠码 4，发现不到就只能整页 503 —— 那是「集群里没有注册过的 broker」，不是这五档本身坏了。"/>
                )}
                <Row gutter={16}>
                    <Col span={8}><Statistic title="码 4 发现的 broker 数" value={brokers.length}/></Col>
                    <Col span={8}><Statistic title="当前取数对象" value={brokerName || '—'}/></Col>
                    <Col span={8}><Statistic title="store 路径存在"
                                             value={instance?.storePathExists == null ? '未知' : (instance.storePathExists ? '是' : '否')}/></Col>
                </Row>
            </Card>

            {SECTIONS.map((s) => (
                <SectionCard key={s.key} section={s} state={states[s.key]}
                             rows={s.key === 'topicConfig' ? topicConfigRows
                                 : (s.key === 'consumerOffset' || s.key === 'delayOffset') ? offsetRows(states[s.key]) : null}/>
            ))}
        </div>
    )
}

function SectionCard({section, state, rows}) {
    const loadError = state?.loadError
    const upstreamErr = state && !loadError && state.responseCode !== undefined ? mqUpstreamErrorText(state) : null
    const note = KNOWN_EMPTY_IN_120[section.path]
    const data = state?.data

    return (
        <Card title={section.label} style={{marginTop: 16}} size="small"
              extra={state?.target ? <Tag>{String(state.target)}</Tag> : null}>
            {!state || state.loading ? <span style={{color: '#94a3b8', fontSize: 12}}>取数中…</span> : null}
            {loadError && <Alert type="error" showIcon message={`调用失败：${loadError}`}/>}
            {upstreamErr && <Alert type="warning" showIcon style={{marginBottom: 12}} message={upstreamErr}/>}
            {state && !loadError && <UnattributedDataBanner res={state}/>}
            {note && (
                <Alert type="info" showIcon style={{marginBottom: 12}}
                       message="上游语义（不是本页的推断）" description={note}/>
            )}
            {state && !loadError && data && section.key === 'topicConfig' && (
                rows && rows.length
                    ? <Table size="small" rowKey="key" dataSource={rows} columns={TOPIC_CONFIG_COLUMNS}
                             pagination={{pageSize: 10, hideOnSinglePage: true}}/>
                    : <EmptyState title="topicConfigTable 是空的" description="本 broker 没有注册任何 topic 配置。"/>
            )}
            {state && !loadError && data && (section.key === 'consumerOffset' || section.key === 'delayOffset') && (
                rows && rows.length
                    ? <Table size="small" rowKey="key" dataSource={rows} pagination={{pageSize: 10, hideOnSinglePage: true}}
                             columns={[
                                 {title: 'Topic', dataIndex: 'topic', key: 'topic', render: (v) => <code>{v}</code>},
                                 {title: 'queueId', dataIndex: 'queueId', key: 'queueId', width: 110},
                                 {title: 'maxOffset', dataIndex: 'maxOffset', key: 'maxOffset', width: 140},
                             ]}/>
                    : <EmptyState title="offsetTable 是空的"
                                  description="注意上面那条「上游语义」：位点表为空可能是真没有消息，也可能只是这条链路上还没产生过写入。"/>
            )}
            {state && !loadError && data && section.key === 'subscription' && (
                <div style={{color: '#64748b', fontSize: 12}}>
                    body 原文（注意它连 <code>subscription</code> 字段都没有，是一个空的
                    <code>ConsumerOffsetSerializeWrapper</code>）：
                    <pre style={{whiteSpace: 'pre-wrap', marginTop: 8, fontSize: 12}}>{JSON.stringify(data, null, 2)}</pre>
                </div>
            )}
            {state && !loadError && data && section.key === 'dataVersion' && (
                <Descriptions bordered size="small" column={1}
                              items={Object.keys(data).map((k) => ({
                                  key: k, label: k, children: dataVersionText(data[k]),
                              }))}/>
            )}
            {state && !loadError && data?.dataVersion && section.key !== 'dataVersion' && (
                <div style={{marginTop: 10, color: '#64748b', fontSize: 12}}>
                    dataVersion：{dataVersionText(data.dataVersion)}
                </div>
            )}
        </Card>
    )
}
