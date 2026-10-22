package com.zifang.z.mq.spring.host;

import com.zifang.z.mq.spring.host.support.EmbeddedZmqCluster;
import com.zifang.z.mq.spring.hosttest.HostAssemblyApp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收：<b>默认形态启动后，broker 侧没有测试 topic、也没有测试消费者组</b>（TASK-20260925-024）。
 *
 * <p><b>这张卡要治的病</b>：端到端测试产物（{@code ZMqTestController} + 它配套的 topic 预创建器）
 * 长在生产 starter 里，且没有 profile 守卫 ⇒ 宿主每次启动都会替生产集群建/绑一个测试 topic
 * （当时硬编码 {@code z-opc-test}），并挂一个测试消费者组（{@code z-opc-test-group}）。
 *
 * <p><b>这张用例怎么量</b>（不是静态扫描，是真的起一次宿主形态）：
 * <ol>
 *   <li>在本 JVM 内起一套 namesrv + broker（临时端口，不碰共享 broker）；</li>
 *   <li>把生产 starter 的宿主胶水装配 {@code MqHostAutoConfiguration} 装起来
 *       （{@code z.mq.host.enabled=true}，与生产同形态）—— <b>ApplicationStartedEvent 会在这一步发出</b>，
 *       旧的 {@code ZMqTopicBootstrap} 正是挂在这个事件上建 topic 的；</li>
 *   <li>等一会儿（给旧实现的重试留出时间），然后用**真实私有二进制协议**读
 *       nameserver 的 topic 清单、broker 的 topic 配置表、broker 的消费者组位点表。</li>
 * </ol>
 *
 * <p><b>牙口</b>：把旧的 {@code ZMqTopicBootstrap} 放回生产 starter，这张用例必须变红
 * （它会在 {@code ApplicationStartedEvent} 之后立刻把测试 topic 建出来）——
 * 否则「没建」只是因为尺子没量到东西。
 */
public class ZmqHostAssemblyNoTestTopicTest {

    /** 曾经由生产 starter 自动创建的那个测试 topic。 */
    private static final String LEGACY_TEST_TOPIC = "z-opc-test";
    /** 曾经由生产 starter 自动挂上的那个测试消费者组。 */
    private static final String LEGACY_TEST_GROUP = "z-opc-test-group";

    /** 给旧实现的「重试建 topic」留出的观察窗（毫秒）。 */
    private static final long OBSERVE_MS = 5000L;

    @Test
    @DisplayName("默认形态启动宿主胶水装配后: broker 侧没有测试 topic、没有测试消费者组")
    public void defaultHostAssemblyCreatesNoTestTopicNorTestConsumerGroup() throws Exception {
        EmbeddedZmqCluster cluster = EmbeddedZmqCluster.start();
        ConfigurableApplicationContext ctx = null;
        try {
            ctx = new SpringApplicationBuilder(HostAssemblyApp.class)
                    .web(WebApplicationType.NONE)
                    .properties(
                            // 宿主胶水装配：与生产同形态（z-opc 侧开的是 z.mq.host.enabled）
                            "z.mq.host.enabled=true",
                            // 旧实现挂在 z.mq.enabled 上 ⇒ 这里必须打开，否则这张用例量不到它
                            "z.mq.enabled=true",
                            "z.mq.namesrv-addr=" + cluster.namesrvAddr(),
                            // 客户端自动装配（ZmqAutoConfiguration）本卡不量，关掉免得它去连别的 namesrv
                            "zmq.enabled=false",
                            "spring.main.banner-mode=off",
                            "logging.level.root=WARN")
                    .run();

            Thread.sleep(OBSERVE_MS);

            // —— 读数（原始 body，来自 nameserver / broker 的只读请求码）
            String topicList = cluster.topicListJson();
            String brokerTopicConfig = cluster.brokerTopicConfigJson();
            String brokerConsumerOffset = cluster.brokerConsumerOffsetJson();
            System.out.println("[zmq024] namesrv topicList        = " + topicList);
            System.out.println("[zmq024] broker topicConfig       = " + brokerTopicConfig);
            System.out.println("[zmq024] broker consumerOffset    = " + brokerConsumerOffset);

            // ① nameserver 的 topic 清单里没有那个测试 topic
            assertFalse(topicList.contains(LEGACY_TEST_TOPIC),
                    "默认形态启动后 nameserver 的 topic 清单里出现了 " + LEGACY_TEST_TOPIC
                            + " ⇒ 生产 starter 仍在启动时替集群建测试 topic。原始读数=" + topicList);

            // ② broker 自己知道的 topic 表里也没有
            assertFalse(brokerTopicConfig.contains(LEGACY_TEST_TOPIC),
                    "默认形态启动后 broker 的 topic 配置表里出现了 " + LEGACY_TEST_TOPIC
                            + " ⇒ 生产 starter 仍在启动时替集群建/绑测试 topic。原始读数=" + brokerTopicConfig);

            // ③ 消费者组位点表里没有那个测试消费者组
            assertFalse(brokerConsumerOffset.contains(LEGACY_TEST_GROUP),
                    "默认形态启动后 broker 的消费者组位点表里出现了 " + LEGACY_TEST_GROUP
                            + " ⇒ 生产 starter 仍在启动时挂测试消费者组。原始读数=" + brokerConsumerOffset);

            // ④ 装配里不许再有「启动即建 topic」的 bean（类名层面的兜底）
            for (String beanName : ctx.getBeanDefinitionNames()) {
                String className = ctx.getBean(beanName).getClass().getName();
                assertFalse(className.contains("TopicBootstrap"),
                        "宿主装配里还有启动即建 topic 的 bean：" + beanName + " / " + className);
            }

            // ⑤ 尺子自检：这张用例必须真的读到了东西（不是空跑）
            assertNotNull(topicList);
            assertTrue(topicList.contains("responseCode="),
                    "没读到 nameserver 的原始响应 ⇒ 这张用例是空跑。原始读数=" + topicList);
            assertTrue(brokerConsumerOffset.contains("responseCode="),
                    "没读到 broker 的原始响应 ⇒ 这张用例是空跑。原始读数=" + brokerConsumerOffset);
            System.out.println("[zmq024] 装配里的 bean = " + Arrays.toString(ctx.getBeanDefinitionNames()));
        } finally {
            if (ctx != null) {
                ctx.close();
            }
            cluster.close();
        }
    }
}
