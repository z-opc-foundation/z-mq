/** Topic 清单：/mq/topics 全量。 */
import {useEffect, useState} from 'react'
import {Alert, Button, Space, Spin, Table, Typography} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {mqApi} from '../services/api'

const {Title, Paragraph} = Typography

export default function Topics() {
    const [topics, setTopics] = useState([])
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState(null)

    const fetch = async () => {
        setLoading(true)
        try {
            const r = await mqApi.topics()
            const list = Array.isArray(r) ? r : r?.topics || []
            setTopics(list.map((t, i) => typeof t === 'string' ? {key: i, name: t} : {key: i, ...t}))
            setError(null)
        } catch (e) {
            setError(e?.message || String(e))
        } finally { setLoading(false) }
    }

    useEffect(() => { fetch() }, [])

    const columns = Object.keys(topics[0] || {name: ''}).map(k => ({
        title: k, dataIndex: k, key: k, ellipsis: true,
        render: (v) => typeof v === 'object' ? JSON.stringify(v) : String(v ?? '—'),
    }))

    return (
        <div>
            <Space style={{marginBottom: 16}}>
                <Title level={4} style={{margin: 0}}>Topic 清单</Title>
                <Button icon={<ReloadOutlined/>} onClick={fetch} loading={loading}>刷新</Button>
            </Space>
            <Paragraph type="secondary">集群当前所有 topic（/mq/topics，从 broker 侧拉取）。</Paragraph>

            {error && <Alert type="error" showIcon style={{marginBottom: 16}} message="后端未连接" description={error}/>}

            <Table rowKey="key" dataSource={topics} columns={columns.length ? columns : [{title: 'name', dataIndex: 'name'}]}
                   loading={loading && !topics.length} size="small" pagination={{pageSize: 20}}/>
        </div>
    )
}
