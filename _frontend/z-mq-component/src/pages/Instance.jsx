/** 实例与端口：/mq/__instance 自省（内嵌 broker bind/属主/日志三拆开）。 */
import {useEffect, useState} from 'react'
import {Alert, Button, Card, Col, Descriptions, Row, Space, Spin, Statistic, Tag, Typography} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {mqApi} from '../services/api'

const {Title, Paragraph} = Typography

function fmtTs(ms) {
    if (!ms) return '-'
    try { return new Date(ms).toLocaleString('zh-CN', {hour12: false}) } catch { return String(ms) }
}
function fmtAge(ms) {
    if (ms == null) return '-'
    const s = Math.max(0, Math.floor((Date.now() - ms) / 1000))
    if (s < 60) return `${s} 秒前`
    if (s < 3600) return `${Math.floor(s / 60)} 分前`
    if (s < 86400) return `${Math.floor(s / 3600)} 时前`
    return `${Math.floor(s / 86400)} 天前`
}
function tri(v, ok = '是', bad = '否') {
    if (v == null) return <Tag>未知</Tag>
    return v ? <Tag color="success">{ok}</Tag> : <Tag color="error">{bad}</Tag>
}

export default function Instance() {
    const [data, setData] = useState(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState(null)

    const fetch = async () => {
        setLoading(true)
        try {
            setData(await mqApi.instance())
            setError(null)
        } catch (e) {
            setError(e?.message || String(e))
            setData(null)
        } finally { setLoading(false) }
    }

    useEffect(() => {
        fetch()
        const t = setInterval(fetch, 15000)
        return () => clearInterval(t)
    }, [])

    return (
        <div>
            <Space style={{marginBottom: 16}}>
                <Title level={4} style={{margin: 0}}>实例与端口</Title>
                <Button icon={<ReloadOutlined/>} onClick={fetch} loading={loading}>刷新</Button>
                <Typography.Text type="secondary">15s 自动刷新</Typography.Text>
            </Space>
            <Paragraph type="secondary">/mq/__instance 自省：自省接口通不通 / 内嵌 broker bind 没上 / 端口属主是谁 / bind 失败原文，四件事拆开答。</Paragraph>

            {error && <Alert type="error" showIcon style={{marginBottom: 16}} message="后端未连接" description={error}/>}
            {loading && !data && <Spin/>}

            {data && (
                <>
                    <Row gutter={16} style={{marginBottom: 16}}>
                        <Col span={8}><Card><Statistic title="nameserver 存活" value={data.nameserverAlive ? '是' : '否'}/></Card></Col>
                        <Col span={8}><Card><Statistic title="broker 存活" value={data.brokerAlive ? '是' : '否'}/></Card></Col>
                        <Col span={8}><Card><Statistic title="bind 状态" value={data.bound ? '已 bind' : '未 bind'}
                                                       valueStyle={{color: data.bound ? '#3f8600' : '#cf1322'}}/></Card></Col>
                    </Row>
                    <Card title="实例详情">
                        <Descriptions column={2} bordered size="small">
                            {Object.entries(data).filter(([k]) => !['bindError'].includes(k)).map(([k, v]) => (
                                <Descriptions.Item key={k} label={k}>
                                    {typeof v === 'object' ? JSON.stringify(v) : String(v)}
                                </Descriptions.Item>
                            ))}
                        </Descriptions>
                    </Card>
                </>
            )}
        </div>
    )
}
