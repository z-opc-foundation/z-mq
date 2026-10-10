import {Alert} from 'antd'

/**
 * "这一屏数据不属于本次构建"的红条。
 *
 * z-mq 的坑比 z-vector/z-graph 深一层：那两个的内嵌实例起在**本 JVM 里**，所以
 * "端口属主 != 本 JVM pid" 就是外来；z-mq 在 z-opc 里是 `ZMqEmbeddedServerConfig`
 * 用 ProcessBuilder spawn 的**两个子进程**，属主 pid 天然 != 8888 那个 pid。
 * 真正的判据是 `lsof 拿到的属主 pid == 本 JVM 子进程 pid`，代理层已经算过了
 * （`__instance` 里的 *PortOwnedByMyChild / attributionVerified），这里只负责把它摆在数据上方，
 * 让人不可能"看见一张有数的表就当孵化成功"。
 */
export function ForeignListenerBanner({data}) {
    if (!data) return null
    const {namesrvForeignListenerSuspected, brokerForeignListenerSuspected, namesrvPort, brokerPort} = data
    if (!namesrvForeignListenerSuspected && !brokerForeignListenerSuspected) return null
    const who = [
        namesrvForeignListenerSuspected ? `NameServer :${namesrvPort}（属主 PID ${data.namesrvPortOwnerPid}，我的子进程 PID ${data.namesrvChildPid}）` : null,
        brokerForeignListenerSuspected ? `Broker :${brokerPort}（属主 PID ${data.brokerPortOwnerPid}，我的子进程 PID ${data.brokerChildPid}）` : null,
    ].filter(Boolean).join('；')
    return (
        <Alert type="error" showIcon style={{marginBottom: 16}}
               message="有端口在应答，但应答者不是本次构建 spawn 的子进程 —— 下面所有表格都不算孵化结果"
               description={
                   `外来嫌疑：${who}。` +
                   (data.bindError
                       ? ` 本 JVM 的子进程 bind 失败原文：${data.bindError}`
                       : ' 本 JVM 的子进程没有 bind 成功的记录。') +
                   // 端口取实例自省回来的实际值：写死 9876/10911 会在换端口后把人指到别的进程上
                   ` 处理方式只有两种：让出 ${namesrvPort}/${brokerPort}，或给 zmq.namesrv-port / zmq.broker-port 换端口。` +
                   ' 代理层不会替你去占端口，也不会去 kill 别人的进程。'
               }/>
    )
}

/**
 * 单条数据响应自带的 attributionUnverified —— 比 __instance 的判定更贴近这一屏：
 * 自省接口是"刚才某一瞬间"的快照，而数据请求是另一瞬间，两次都各判一次才安全。
 */
export function UnattributedDataBanner({res}) {
    if (!res || !(res.attributionUnverified || res.attributionVerified === false)) return null
    return (
        <Alert type="warning" showIcon style={{marginBottom: 12}}
               message={`本响应来自 ${res.target || '?'}，但归属未通过（应答者不是本 JVM 的子进程）`}
               description={res.attributionNote}/>
    )
}

/**
 * 子进程活着但一个端口都没 bind 上 —— 这是 z-mq 特有、也是本卡最难自查的一种形态。
 * 原因：main 线程已经因 BindException 退出，Netty 的 boss/worker 是非守护线程，JVM 不退。
 * ⇒ 任何拿 `Process.isAlive()` 当健康探针的自证都会在这里说谎。
 */
export function ZombieChildBanner({data}) {
    if (!data) return null
    const zombies = [
        data.namesrvChildAlive && !data.namesrvPortOwnedByMyChild ? `NameServer pid=${data.namesrvChildPid}` : null,
        data.brokerChildAlive && !data.brokerPortOwnedByMyChild ? `Broker pid=${data.brokerChildPid}` : null,
    ].filter(Boolean)
    if (!zombies.length) return null
    return (
        <Alert type="error" showIcon style={{marginBottom: 16}}
               message={`子进程 isAlive()=true 但端口不归它：${zombies.join('、')} —— 「进程在」不等于「服务在」`}
               description={
                   'z-mq 的 Main 会 Thread.currentThread().join()，所以 ZMqEmbeddedServerConfig 是 spawn 子进程而不是线程内嵌。' +
                   'bind 失败时 main 线程已死、Netty 非守护线程吊着 JVM 不退，于是 isAlive() 一路报"好"。' +
                   '这就是 __instance 把 *ChildAlive 和 *PortOwnedByMyChild 拆成两格的原因。'
               }/>
    )
}

/** 归属探针本身不可用时不许默认"是自己的" */
export function AttributionUnavailableBanner({data}) {
    if (!data || data.attributionAvailable !== false) return null
    return (
        <Alert type="warning" showIcon style={{marginBottom: 16}}
               message="归属无法自证（lsof 探针不可用或已关闭）"
               description={data.attributionMethod + '。此时页面不猜：请人工执行 lsof -nP -iTCP:'
                   + data.namesrvPort + ' -sTCP:LISTEN 与本页 jvm/子进程 pid 对照。'}/>
    )
}
