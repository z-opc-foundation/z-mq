import {useCallback, useEffect, useRef, useState} from 'react'
import {mqApi, mqErrorText} from './api'

/**
 * `GET /api/mq/__instance` 的共享读面。
 *
 * 四张页面 (InstanceStatus / ClusterPage / TopicListPage / BrokerMetadataPage)
 * 都要拿同一份自省数据，且都要在数据回来后按 `instance.namesrvPort` /
 * `instance.brokerPort` 去渲染"这个端口上应答的是不是我自己 spawn 的那个子进程"。
 * 判据口径见 services/api.js 的 isUnattributed() 注释 —— z-mq 是子进程型内嵌，
 * 不能拿"属主 == 8888 那个 pid"当归属证明。
 *
 * poll 必须引用稳定: 四张页面都把它放进 useEffect 依赖数组，
 * 每次渲染换一个函数会让 effect 无限重跑。这里用 ref 兜住请求句柄，
 * useCallback 依赖取空。
 */
export function useInstance() {
    const [instance, setInstance] = useState(null)
    const [instanceError, setInstanceError] = useState(false)
    const [instanceErrorText, setInstanceErrorText] = useState('')
    const alive = useRef(true)

    useEffect(() => () => {
        alive.current = false
    }, [])

    const poll = useCallback(async () => {
        try {
            const res = await mqApi.instance()
            if (!alive.current) return
            setInstance(res)
            setInstanceError(false)
            setInstanceErrorText('')
        } catch (e) {
            if (!alive.current) return
            // 自省不通 ≠ 没有实例：端口/路由问题与"数据为空"必须分开显示，
            // 否则页面会把 z-opc 侧的接线故障画成"这个中间件没起来"。
            setInstance(null)
            setInstanceError(true)
            setInstanceErrorText(mqErrorText(e))
        }
    }, [])

    return {instance, instanceError, instanceErrorText, poll}
}
