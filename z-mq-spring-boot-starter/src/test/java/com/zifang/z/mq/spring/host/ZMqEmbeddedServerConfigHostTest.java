package com.zifang.z.mq.spring.host;

import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 守卫：内嵌 namesrv / broker 子进程的 bind host 透传。
 * <p>
 * 那张卡（TASK-20261022-003）说 z-opc 侧改 property 无效，根因在 L3 ——
 * {@code bind(int)} 恒为通配。所以光有 {@code z.mq.server.host} 这个配置项没用，
 * 它必须真的落到子进程的 JVM 上、并且子进程真的按它 bind。
 * <p>
 * 这里断言两件事：
 * <ol>
 *   <li>配置项默认值是 <b>回环</b>（不是 0.0.0.0）；</li>
 *   <li>子进程 JVM 参数里确实带了 {@code -Dz.mq.server.host=}，值就是配置项。</li>
 * </ol>
 */
@DisplayName("ZMqEmbeddedServerConfig bind host 透传")
public class ZMqEmbeddedServerConfigHostTest {

    private ZMqEmbeddedServerConfig newConfig(String host) {
        ZMqEmbeddedServerConfig cfg = new ZMqEmbeddedServerConfig();
        ReflectionTestUtils.setField(cfg, "serverHost", host);
        return cfg;
    }

    @Test
    @DisplayName("z.mq.server.host 默认值是回环 127.0.0.1")
    public void defaultServerHostIsLoopback() {
        ZMqEmbeddedServerConfig cfg = new ZMqEmbeddedServerConfig();
        String host = (String) ReflectionTestUtils.getField(cfg, "serverHost");
        // @Value("${z.mq.server.host:127.0.0.1}") 在纯 POJO 场景下不注入 ⇒ 字段为 null，
        // 真实生效值由 resolveServerHost() 兜底 ⇒ 断言兜底结果是回环。
        assertEquals(NettyServerConfig.DEFAULT_HOST,
                cfg.resolveServerHostForTest(),
                "生效的 bind host 必须是回环，不能是通配地址");
        assertEquals("127.0.0.1", NettyServerConfig.DEFAULT_HOST);
        assertFalse("0.0.0.0".equals(host) || "0.0.0.0".equals(cfg.resolveServerHostForTest()),
                "bind host 绝不能默认成 0.0.0.0");
    }

    @Test
    @DisplayName("空白 / null host 一律退回回环")
    public void blankHostFallsBackToLoopback() {
        assertEquals("127.0.0.1", newConfig("   ").resolveServerHostForTest());
        assertEquals("127.0.0.1", newConfig(null).resolveServerHostForTest());
    }

    @Test
    @DisplayName("子进程 JVM 参数带上 -Dz.mq.server.host")
    public void childJvmArgsCarryHost() {
        List<String> args = newConfig("127.0.0.1").childJvmArgsForTest();
        assertTrue(args.contains("-Dz.mq.server.host=127.0.0.1"),
                "子进程 JVM 参数必须带 -Dz.mq.server.host，实际: " + args);
    }

    @Test
    @DisplayName("显式声明 0.0.0.0 时子进程按通配 bind（容器化部署形态）")
    public void childJvmArgsCarryWildcardWhenExplicitlyAsked() {
        List<String> args = newConfig("0.0.0.0").childJvmArgsForTest();
        assertTrue(args.contains("-Dz.mq.server.host=0.0.0.0"),
                "显式声明 0.0.0.0 必须透传给子进程，实际: " + args);
        // 通配时不能同时把公布地址改成 0.0.0.0 —— 客户端按路由表去连 0.0.0.0 是连不通的
        assertFalse(args.contains("-DbrokerIp1=0.0.0.0"),
                "通配 bind 时公布地址不能是 0.0.0.0");
    }

    @Test
    @DisplayName("回环 bind 时公布地址与 bind 地址一致")
    public void brokerAdvertisedAddressMatchesBindHost() {
        List<String> args = newConfig("127.0.0.1").childJvmArgsForTest();
        assertTrue(args.contains("-DbrokerIp1=127.0.0.1"),
                "bind 回环时公布地址应同为回环，否则客户端按路由表连到别处，实际: " + args);
    }
}
