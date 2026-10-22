/** Topic 清单：/mq/topics 全量。 */
import {useEffect, useState} from 'react'
import {Alert, Button, Space, Typography} from 'antd'
import {ReloadOutlined} from '@ant-design/icons'
import {ListTable} from '@yuku123/z-frontend-common/ui'
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
            </Space>
            <Paragraph type="secondary">集群当前所有 topic（/mq/topics，从 broker 侧拉取）。</Paragraph>

            {error && <Alert type="error" showIcon style={{marginBottom: 16}} message="后端未连接" description={error}/>}

            {/* 列表页走基座 ListTable（lead 008 §2 规则 2/3/4）：分页 / 过滤 / EmptyState 三件套。
                动作按钮（刷新）按 §2 放 cardExtra；
                分页用基座默认的 DEFAULT_PAGINATION（原来手写的 {pageSize:20} 缺
                showSizeChanger / pageSizeOptions / showTotal）。
                topic 列表是全量拉回来的（/mq/topics），所以过滤走端侧：列是按
                topic 对象的键动态生成的，name 是其中稳定存在的一列。 */}
            <ListTable
                rows={topics}
                columns={columns.length ? columns : [{title: 'name', dataIndex: 'name'}]}
                rowKey="key"
                loading={loading && !topics.length}
                searchKeys="name"
                searchPlaceholder="按 topic 名过滤"
                cardExtra={
                    <Button icon={<ReloadOutlined/>} onClick={fetch} loading={loading}>刷新</Button>
                }
            />
        </div>
    )
}
