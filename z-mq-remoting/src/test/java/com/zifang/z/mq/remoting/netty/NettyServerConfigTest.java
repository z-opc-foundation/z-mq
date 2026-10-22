package com.zifang.z.mq.remoting.netty;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * NettyServerConfig 单元测试
 */
public class NettyServerConfigTest {

    @Test
    public void testDefaults() {
        NettyServerConfig c = new NettyServerConfig();
        assertEquals(8888, c.getListenPort());
        assertEquals("127.0.0.1", c.getHost());
        assertEquals(1, c.getServerBossThreads());
        assertEquals(8, c.getServerWorkerThreads());
        assertEquals(3, c.getServerSelectorThreads());
        assertEquals(4, c.getServerCallbackExecutorThreads());
        assertEquals(64, c.getServerAsyncSemaphoreValue());
        assertEquals(256, c.getServerOnewaySemaphoreValue());
        assertEquals(65535, c.getServerSocketSndBufSize());
        assertEquals(65535, c.getServerSocketRcvBufSize());
        assertEquals(1024, c.getServerSocketBacklog());
        assertEquals(120, c.getServerChannelMaxIdleTimeSeconds());
        assertEquals(false, c.isUseEpollNativeSelector());
    }

    @Test
    public void testSetters() {
        NettyServerConfig c = new NettyServerConfig();
        c.setListenPort(12345);
        c.setHost("10.0.0.5");
        c.setServerBossThreads(2);
        c.setServerWorkerThreads(16);
        c.setServerSelectorThreads(4);
        c.setServerCallbackExecutorThreads(8);
        c.setServerAsyncSemaphoreValue(128);
        c.setServerOnewaySemaphoreValue(512);
        c.setServerSocketSndBufSize(32768);
        c.setServerSocketRcvBufSize(32768);
        c.setServerSocketBacklog(2048);
        c.setServerChannelMaxIdleTimeSeconds(60);
        c.setUseEpollNativeSelector(true);
        assertEquals(12345, c.getListenPort());
        assertEquals("10.0.0.5", c.getHost());
        assertEquals(2, c.getServerBossThreads());
        assertEquals(16, c.getServerWorkerThreads());
        assertEquals(4, c.getServerSelectorThreads());
        assertEquals(8, c.getServerCallbackExecutorThreads());
        assertEquals(128, c.getServerAsyncSemaphoreValue());
        assertEquals(512, c.getServerOnewaySemaphoreValue());
        assertEquals(32768, c.getServerSocketSndBufSize());
        assertEquals(32768, c.getServerSocketRcvBufSize());
        assertEquals(2048, c.getServerSocketBacklog());
        assertEquals(60, c.getServerChannelMaxIdleTimeSeconds());
        assertEquals(true, c.isUseEpollNativeSelector());
    }

    @Test
    public void testToStringContainsKeyFields() {
        NettyServerConfig c = new NettyServerConfig();
        c.setListenPort(19999);
        String s = c.toString();
        assertEquals(true, s.contains("19999"));
        assertEquals(true, s.contains("listenPort"));
        assertEquals(true, s.contains("host"));
    }

    /**
     * 默认 bind host 必须是回环，<b>不能是通配地址</b>。
     * <p>
     * z-mq 的 Netty 面走私有二进制协议且协议层没有认证 ⇒ 绑 0.0.0.0 / :: 等于把
     * "可读可写"的能力开放给整个局域网。这条断言就是那张卡（L3 bind 写死通配）的守卫。
     */
    @Test
    public void testDefaultHostIsLoopbackNotWildcard() {
        NettyServerConfig c = new NettyServerConfig();
        assertEquals("127.0.0.1", c.getHost());
        assertEquals(false, c.isWildcardHost());
    }

    /** 空白 host 一律退回回环，绝不因为配错就变成通配暴露。 */
    @Test
    public void testBlankHostFallsBackToLoopback() {
        NettyServerConfig c = new NettyServerConfig();
        c.setHost("   ");
        assertEquals("127.0.0.1", c.getHost());
        c.setHost(null);
        assertEquals("127.0.0.1", c.getHost());
    }

    /** 显式声明通配地址是允许的（容器化部署形态要靠 Service/NodePort 被访问），但必须能被识别出来。 */
    @Test
    public void testWildcardHostIsDetectable() {
        NettyServerConfig c = new NettyServerConfig();
        c.setHost("0.0.0.0");
        assertEquals("0.0.0.0", c.getHost());
        assertEquals(true, c.isWildcardHost());
        c.setHost("::");
        assertEquals(true, c.isWildcardHost());
        c.setHost("127.0.0.1");
        assertEquals(false, c.isWildcardHost());
    }

    /**
     * 系统属性可覆盖 bind host —— 这是子进程拿 host 的通道
     * （{@code ZMqEmbeddedServerConfig} 用 {@code -Dz.mq.server.host=} 透传）。
     */
    @Test
    public void testSystemPropertyOverridesHost() {
        String previous = System.getProperty(NettyServerConfig.HOST_SYSTEM_PROPERTY);
        try {
            System.setProperty(NettyServerConfig.HOST_SYSTEM_PROPERTY, "127.0.0.2");
            NettyServerConfig c = new NettyServerConfig();
            assertEquals("127.0.0.2", c.getHost());
        } finally {
            if (previous == null) {
                System.clearProperty(NettyServerConfig.HOST_SYSTEM_PROPERTY);
            } else {
                System.setProperty(NettyServerConfig.HOST_SYSTEM_PROPERTY, previous);
            }
        }
    }
}
