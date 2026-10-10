import {useCallback, useEffect, useState} from 'react'
import {Alert, Button, Card, Descriptions, Space, Table, Tag} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {mqApi, mqErrorText, ZMQ_CODE} from '../services/api'
import {useInstance} from '../services/useInstance'
import {
    AttributionUnavailableBanner,
    ForeignListenerBanner,
    UnattributedDataBanner,
    ZombieChildBanner,
} from './AttributionBanner'
import {PageHeader} from '@/common/components/ui'

/** 上游真实响应：随便挑一条只读码打一发，用来证明"代理层不只是在报自省，它真能发出去" */
function fmtTs(ms) {
    if (!ms) return '-'
    try {
        return new Date(ms).toLocaleString('zh-CN', {hour12: false})
    } catch {
        return String(ms)
    }
}

function fmtAge(ms) {
    if (ms == null) return '-'
    const s = Math.max(0, Math.floor((Date.now() - ms) / 1000))
    if (s < 60) return `${s} 秒前`
    if (s < 3600) return `${Math.floor(s / 60)} 分前`
    if (s < 86400) return `${Math.floor(s / 3600)} 时前`
    return `${Math.floor(s / 86400)} 天前`
}

function tri(v, ok = '是', bad = '否', badColor = 'error') {
    if (v == null) return <Tag>未知</Tag>
    return v ? <Tag color="success">{ok}</Tag> : <Tag color={badColor}>{bad}</Tag>
}

const CODE_COLUMNS = [
    {title: '逻辑端点', dataIndex: 'ep', key: 'ep', width: 240, render: (v) => <code>{v}</code>},
    {title: '请求码', dataIndex: 'code', key: 'code', width: 100, render: (v) => <Tag>{v}</Tag>},
    {title: '常量名', dataIndex: 'name', key: 'name', render: (v) => <code>{v}</code>},
]

/**
 * 实例与端口 — `GET /api/mq/__instance` 的可视化。
 *
 * 这一页是本卡的全部风险集中处，所以它排在第一屏（MqApp 的 index 也重定向到这里）。
 * 需要同时回答四个问题，任何一个单独为真都不构成孵化成功：
 *   1. **自省接口通不通**（它由 MqProxyController 自己应答、完全不碰 z-mq，
 *      所以它不通就是 z-opc 侧的路由/接线问题，跟中间件无关）；
 *   2. **本 JVM 的内嵌实例 bind 上了没有** —— 注意不是"子进程活着没有"：
 *      实测本次构建的两个子进程都 alive、但 `lsof -p <pid> -a -iTCP -sTCP:LISTEN` 是**空的**，
 *      因为 main 线程已因 `BindException: Address already in use` 退出，
 *      而 Netty 的 boss/worker 是非守护线程。这就是 `ZombieChildBanner` 存在的全部理由；
 *   3. **端口上应答的是不是我生的那个进程** —— z-mq 是子进程型内嵌，
 *      所以"属主 == 8888 那个 pid"是错的判据，正确判据是"属主 == 我 spawn 的子 pid"；
 *   4. **bind 失败的原文** —— spawn 器把子进程 stdout/stderr 追加到
 *      `${java.io.tmpdir}/z-mq-{nameserver,broker}.log`，页面直接把它捞出来，
 *      免得排障时还要人去 `tail`。
 *
 * 下面那两块"实打实发一发只读码"是故意留的：自省只能证明"我知道自己没起"，
 * 得有一条真的请求码走通，才能证明协议适配器这条形状本身是成立的。
 */
export default function InstanceStatus() {
    const {instance, instanceError, instanceErrorText, poll} = useInstance()
    const [probe, setProbe] = useState(null)
    const [probeError, setProbeError] = useState(null)
    const [probing, setProbing] = useState(false)

    const runProbe = useCallback(async (which) => {
        setProbing(true)
        try {
            const r = await mqApi[which]()
            setProbe({which, res: r})
            setProbeError(null)
        } catch (e) {
            setProbe(null)
            setProbeError(e)
        } finally {
            setProbing(false)
            poll()
        }
    }, [poll])

    const codeRows = instance
        ? Object.entries(instance.readOnlyRequestCodes || {}).map(([ep, code]) => ({
            ep, code, name: (instance.requestCodeNames || {})[String(code)] || '-',
            key: ep,
        }))
        : []

    return (
        <div>
            <PageHeader title="实例与端口"
                        subtitle="自省接口 GET /api/mq/__instance（由 MqProxyController 自己应答，不经过 z-mq）+ 只读请求码实发探针"/>
            <Card extra={<Space>
                <Button icon={<ReloadOutlined/>} loading={probing} onClick={() => runProbe('cluster')}>实发一发 码4 GET_BROKER_CLUSTER_INFO</Button>
                <Button icon={<ReloadOutlined/>} onClick={poll}>刷新自省</Button>
            </Space>}>
                {instanceError ? (
                    <Alert type="error" showIcon message={`自省接口调用失败：${instanceErrorText}`}
                           description="/api/mq/__instance 不碰 NameServer 也不碰 Broker，由 z-opc 自己的 controller 直接应答；它都不通说明是 z-opc 侧的路由/认证问题（302 到登录页 = 没带会话 Cookie；404 = MainWebConfig.spaPaths 或 /api/mq 映射没接上）。"/>
                ) : (
                    <>
                        <AttributionUnavailableBanner data={instance}/>
                        <ForeignListenerBanner data={instance}/>
                        <ZombieChildBanner data={instance}/>
                        {instance && !instance.embeddedConfigLoaded && (
                            <Alert type="warning" showIcon style={{marginBottom: 16}}
                                   message="ZMqEmbeddedServerConfig 未装载"
                                   description="zmq.enabled 不为 true 时这个 @Configuration 整个不生效 ⇒ 不会有任何子进程被 spawn，页面所有数据必然拿不到。"/>
                        )}
                        <Descriptions bordered size="small" column={2}
                                      items={[
                                          {
                                              key: 'jvm', label: '本 JVM (pid@host)',
                                              children: <code>{instance?.jvm ?? '-'}</code>
                                          },
                                          {
                                              key: 'surface', label: 'z-mq 的管理面形状',
                                              children: <Tag color="volcano">{instance?.surface ?? '-'}</Tag>
                                          },
                                          {
                                              key: 'loaded', label: '内嵌配置已装载',
                                              children: instance ? (instance.embeddedConfigLoaded
                                                  ? <Tag color="success">是</Tag>
                                                  : <Tag color="error">否（zmq.enabled≠true）</Tag>) : '-'
                                          },
                                          {
                                              key: 'addr', label: 'NameServer 地址 / Broker 端口',
                                              children: <code>{instance ? `${instance.namesrvAddr} / ${instance.brokerPort}` : '-'}</code>
                                          },
                                          {
                                              key: 'nsChild', label: `NameServer 子进程已 spawn`,
                                              children: tri(instance?.namesrvChildSpawned, '是', '否（未 spawn 或 boot jar 未找到）')
                                          },
                                          {
                                              key: 'nsPid', label: 'NameServer 子进程 pid / 存活',
                                              children: instance ? <span>
                                                  <code>{instance.namesrvChildPid ?? 'n/a'}</code>{' '}
                                                  {tri(instance.namesrvChildAlive, 'alive', 'dead')}
                                              </span> : '-'
                                          },
                                          {
                                              key: 'nsOwner', label: 'NameServer 端口属主 pid（lsof）',
                                              children: <code>{instance?.namesrvPortOwnerPid ?? '取不到'}</code>
                                          },
                                          {
                                              key: 'nsOwned', label: 'NameServer 端口归我的子进程',
                                              children: tri(instance?.namesrvPortOwnedByMyChild, '是（这才是孵化成功）', '否')
                                          },
                                          {
                                              key: 'brChild', label: 'Broker 子进程已 spawn',
                                              children: tri(instance?.brokerChildSpawned, '是', '否')
                                          },
                                          {
                                              key: 'brPid', label: 'Broker 子进程 pid / 存活',
                                              children: instance ? <span>
                                                  <code>{instance.brokerChildPid ?? 'n/a'}</code>{' '}
                                                  {tri(instance.brokerChildAlive, 'alive', 'dead')}
                                              </span> : '-'
                                          },
                                          {
                                              key: 'brOwner', label: 'Broker 端口属主 pid（lsof）',
                                              children: <code>{instance?.brokerPortOwnerPid ?? '取不到'}</code>
                                          },
                                          {
                                              key: 'brOwned', label: 'Broker 端口归我的子进程',
                                              children: tri(instance?.brokerPortOwnedByMyChild, '是', '否')
                                          },
                                          {
                                              key: 'reach', label: '两个端口此刻 TCP 可连',
                                              children: instance ? (
                                                  <Space>
                                                      {tri(instance.namesrvPortReachable, `:${instance.namesrvPort} 通`, `:${instance.namesrvPort} 不通`, 'warning')}
                                                      {tri(instance.brokerPortReachable, `:${instance.brokerPort} 通`, `:${instance.brokerPort} 不通`, 'warning')}
                                                  </Space>
                                          ) : '-',
                                          },
                                          {
                                              key: 'store', label: 'store 目录（存在 / 是否为空）',
                                              children: instance ? (
                                                  <span><code>{instance.storePath}</code>{' '}
                                                      {tri(instance.storePathExists, '目录在', '目录不存在', 'warning')}{' '}
                                                      {instance.storePathExists && tri(!instance.storePathEmpty, '有内容', '空 = 这个实例没落过任何消息', 'warning')}
                                                  </span>
                                          ) : '-',
                                          },
                                      ]}/>

                        {instance?.surfaceNote && (
                            <div style={{marginTop: 12, color: '#64748b', fontSize: 12}}>{instance.surfaceNote}</div>
                        )}
                    </>
                )}
            </Card>

            <Card title="子进程 bind 失败原文（spawn 器落的日志）" style={{marginTop: 16}}
                  extra={<Tag color="blue">ZMqEmbeddedServerConfig 把子进程 stdout/stderr 追加到这里</Tag>}>
                {instance ? (
                    <Descriptions bordered size="small" column={1}
                                  items={['namesrvSpawnLog', 'brokerSpawnLog'].map((k) => {
                                      const l = instance[k] || {}
                                      return {
                                          key: k,
                                          label: k === 'namesrvSpawnLog' ? 'NameServer' : 'Broker',
                                          children: <div style={{fontSize: 12}}>
                                              <div><code>{l.path}</code>{' '}
                                                  {tri(l.exists, '存在', '不存在', 'warning')}{' '}
                                                  {l.exists ? <span style={{color: '#94a3b8'}}>（{fmtAge(l.lastModified)}写入）</span> : null}
                                              </div>
                                              {l.bindErrorLines
                                                  ? <div style={{color: '#dc2626'}}>
                                                      命中 {l.bindErrorLines} 行 bind 失败，最后一条：<code>{l.lastBindError}</code>
                                                  </div>
                                                  : <div style={{color: '#94a3b8'}}>日志里没有 bind 失败记录</div>}
                                          </div>,
                                      }
                                  })}/>
                ) : <Alert type="info" showIcon message="自省接口没通，读不到日志字段"/>}
            </Card>

            <Card title="实发一发只读请求码（证明协议适配器本身成立，而不只是在报自省）" style={{marginTop: 16}}>
                <Space direction="vertical" style={{width: '100%'}}>
                    <Space wrap>
                        <Button size="small" loading={probing} onClick={() => runProbe('cluster')}>码 4 · 集群注册表</Button>
                        <Button size="small" loading={probing} onClick={() => runProbe('topics')}>码 8 · NameServer topic 列表</Button>
                        <Button size="small" loading={probing} onClick={() => runProbe('brokerTopics')}>Broker 码 8 · 本 broker topic</Button>
                        <Button size="small" loading={probing} onClick={() => runProbe('brokerDataVersion')}>Broker 码 324 · DataVersion</Button>
                    </Space>
                    {probeError && (
                        <Alert type="error" showIcon
                               message={`探针失败：${mqErrorText(probeError)}`}/>
                    )}
                    {probe && (
                        <>
                            <UnattributedDataBanner res={probe.res}/>
                            <Descriptions bordered size="small" column={3}
                                          items={[
                                              {key: 't', label: '打的哪一档', children: <Tag>{probe.res.tier}</Tag>},
                                              {key: 'target', label: '目标地址', children: <code>{probe.res.target}</code>},
                                              {key: 'req', label: '请求码', children: <code>{probe.res.requestCode} {probe.res.requestName}</code>},
                                              {
                                                  key: 'responseCode', label: 'z-mq 业务码',
                                                  children: <Tag color={probe.res.responseCode === 0 ? 'success' : 'error'}>
                                                      {probe.res.responseCode} · {ZMQ_CODE[probe.res.responseCode] || '未知码'}
                                                  </Tag>
                                              },
                                              {key: 'remark', label: 'remark', children: probe.res.remark || <span style={{color: '#94a3b8'}}>—</span>},
                                              {key: 'elapsed', label: '耗时', children: `${probe.res.elapsedMs ?? '-'} ms`},
                                              {key: 'attr', label: '归属已验证', children: tri(probe.res.attributionVerified)},
                                              {key: 'size', label: '响应体字节数', children: JSON.stringify(probe.res.data ?? null).length},
                                              {
                                                  key: 'ts', label: '本页数据读取时刻',
                                                  children: fmtTs(Date.now())
                                              },
                                          ]}/>
                            <pre style={{
                                background: '#0f172a', color: '#e2e8f0', padding: 12, borderRadius: 6,
                                maxHeight: 260, overflow: 'auto', fontSize: 12, margin: 0
                            }}>{JSON.stringify(probe.res.data ?? null, null, 2)}</pre>
                        </>
                    )}
                    {!probe && !probeError && (
                        <Alert type="info" showIcon
                               message="还没发探针"
                               description="自省接口证明的是「z-opc 这一层活着、且知道自己没起」；发一发只读码证明的是「协议适配这条形状走得通」。两个都要。"/>
                    )}
                </Space>
            </Card>

            <Card title="本代理可达的只读请求码（穷举，无通用转发口）" style={{marginTop: 16}}>
                <Table size="small" rowKey="ep" dataSource={codeRows} columns={CODE_COLUMNS}
                       pagination={false}/>
                {instance?.unavailableOnBroker120?.length > 0 && (
                    <Alert type="warning" showIcon style={{marginTop: 12}}
                           message="以下能力在 z-mq 1.2.0 上根本没有请求码，页面不做也不假做"
                           description={
                               <div>
                                   <div style={{fontWeight: 600, marginTop: 4}}>Broker 侧未注册：</div>
                                   <ul style={{margin: '4px 0 0 18px'}}>{instance.unavailableOnBroker120.map((s) => <li key={s}>{s}</li>)}</ul>
                                   <div style={{fontWeight: 600, marginTop: 4}}>NameServer 侧未实现：</div>
                                   <ul style={{margin: '4px 0 0 18px'}}>{(instance.unavailableOnNamesrv120 || []).map((s) => <li key={s}>{s}</li>)}</ul>
                               </div>
                           }/>
                )}
            </Card>
        </div>
    )
}
