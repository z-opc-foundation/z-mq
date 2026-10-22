package com.zifang.z.mq.spring.hosttest;

import com.zifang.z.mq.spring.host.MqHostAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * 「宿主胶水装配」的最小宿主应用（**测试夹具**，TASK-20260925-024）。
 *
 * <p>它只做一件事：把 {@link MqHostAutoConfiguration}（也就是生产 starter 里那套
 * {@code /api/mq/**} 自省面 + 内嵌 namesrv/broker spawn）装起来，
 * 让测试能真的**启动一次宿主形态**，而不是只在静态扫描上断言。
 *
 * <p>刻意放在 {@code com.zifang.z.mq.spring.hosttest} 这个包里：
 * {@code MqHostAutoConfiguration} 的 {@code @ComponentScan} 扫的是
 * {@code com.zifang.z.mq.spring.host}，本类不在那个包里 ⇒ 不会被自己扫到、不会自我注册两遍。
 */
@Configuration
@EnableAutoConfiguration
@Import(MqHostAutoConfiguration.class)
public class HostAssemblyApp {
}
