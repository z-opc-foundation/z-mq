package com.zifang.z.mq.spring.host;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * 宿主侧胶水装配 (2026-10-03 自 z-opc main-starter 平移):
 * MqProxyController (/api/mq/** 自省面) + ZMqEmbeddedServerConfig (内嵌 namesrv/broker spawn).
 *
 * <p>跟随 z.mq.host.enabled 开关 (默认关): 寄生 all-in-one 模式由宿主打开,
 * standalone 分布式模式 (z-mq 独立容器) 不开. ZmqAutoConfiguration (client) 不受影响.
 *
 * <p><b>这里不再有任何「启动即写 namesrv」的装配</b> (2026-10-22, TASK-20260925-024):
 * 原先同包里的 {@code ZMqTopicBootstrap} 是端到端测试产物 —— 它硬编码一个测试 topic 名,
 * 每次启动都替生产集群建/绑那个 topic, 而全仓没有任何真实生产者/消费者用它.
 * 它已被移出生产 jar, 搬进本模块的 <b>src/test</b> 测试夹具
 * ({@code com.zifang.z.mq.spring.host.ZmqTestTopicBootstrap}), 需要时由测试显式调用.
 * ⇒ 生产 starter 的默认形态: 启动不建任何 topic、不挂任何消费者组.
 */
@Configuration
@ConditionalOnProperty(prefix = "z.mq.host", name = "enabled", havingValue = "true", matchIfMissing = false)
@ComponentScan(basePackages = "com.zifang.z.mq.spring.host")
public class MqHostAutoConfiguration {
}
