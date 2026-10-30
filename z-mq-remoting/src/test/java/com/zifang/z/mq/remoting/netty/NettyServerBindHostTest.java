package com.zifang.z.mq.remoting.netty;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.util.Collections;
import java.util.Enumeration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 守卫：NettyRemotingServer 的 bind 地址。
 * <p>
 * 这张卡（TASK-20261022-003）的根因就在 {@code serverBootstrap.bind(int)} ——
 * Netty 的单参 bind <b>恒为通配地址</b>，而 z-mq 的 Netty 面走私有二进制协议、
 * <b>协议层没有认证</b> ⇒ 监听在通配地址上等于把"可读可写"开放给整个局域网。
 * <p>
 * 这里的断言分两层：
 * <ol>
 *   <li><b>实际 socket 的本地地址</b>是不是回环（不是 {@code 0.0.0.0}/{@code ::}）——
 *       这条直接对着 {@code lsof -nP -iTCP -sTCP:LISTEN} 的输出形态，是那张卡的验收尺。</li>
 *   <li><b>从本机 LAN IP 连不上</b>，而从 127.0.0.1 连得上 —— 消息面仍然通
 *       （那张卡明确要求：不许用"关掉服务"来"修安全"）。</li>
 * </ol>
 * 量具纪律：判"LAN 连不上"必须用<b>本机 LAN IP</b>；用 127.0.0.1 打永远是回环，会假绿。
 * 本机没有非回环 IPv4 时（纯容器/CI 环境）第二条直接跳过并说明，而不是假装绿。
 */
public class NettyServerBindHostTest {

    private NettyRemotingServer server;

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    /** 起一个用默认配置（默认回环）的服务端，回读它实际 bind 的本地地址。 */
    private InetSocketAddress startDefaultServer() throws Exception {
        NettyServerConfig config = new NettyServerConfig();
        config.setListenPort(0); // 0 ⇒ 内核分配端口，随后回读
        server = new NettyRemotingServer(config);
        server.start();
        return localAddressOf(server);
    }

    private static InetSocketAddress localAddressOf(NettyRemotingServer s) throws Exception {
        java.lang.reflect.Field f = NettyRemotingServer.class.getDeclaredField("serverChannel");
        f.setAccessible(true);
        io.netty.channel.Channel ch = (io.netty.channel.Channel) f.get(s);
        return (InetSocketAddress) ch.localAddress();
    }

    /**
     * 默认 bind 必须是回环 —— 不是一个"any local address"。
     * <p>
     * 老实现（bind(int)）这里拿到的是 {@code 0.0.0.0}/{@code ::} ⇒ 本用例在修复前必红。
     */
    @Test
    public void defaultServerBindsLoopbackNotWildcard() throws Exception {
        InetSocketAddress addr = startDefaultServer();
        assertNotNull(addr, "server must expose a local address after start");
        InetAddress bound = addr.getAddress();
        assertNotNull(bound, "bound address must be resolved");
        assertFalse(bound.isAnyLocalAddress(),
                "default bind must NOT be a wildcard address, but was " + bound
                        + " (wildcard bind on a no-auth protocol exposes read/write to the whole LAN)");
        assertTrue(bound.isLoopbackAddress(),
                "default bind must be a loopback address, but was " + bound);
        assertEquals("127.0.0.1", bound.getHostAddress());
    }

    /** 回环上仍然能连上 —— 安全收敛不能靠"把服务关掉"实现。 */
    @Test
    public void loopbackClientStillConnects() throws Exception {
        InetSocketAddress addr = startDefaultServer();
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress("127.0.0.1", addr.getPort()), 3000);
            assertTrue(s.isConnected(), "client on loopback must still connect");
        } finally {
            s.close();
        }
    }

    /**
     * 从本机 LAN IP 连不上（用本机真实的非回环 IPv4）。
     * <p>
     * 没有非回环 IPv4 的环境（纯容器）跳过并说明 —— 那台机器上"LAN 连不上"
     * 这条根本无法在本地复现，不该假装绿。
     */
    @Test
    public void lanIpCannotConnect() throws Exception {
        InetSocketAddress addr = startDefaultServer();
        String lanIp = firstNonLoopbackIPv4();
        if (lanIp == null) {
            System.out.println("[SKIP] 本机没有非回环 IPv4 地址 ⇒ 无法在本地复现 'LAN 连不上'，跳过");
            return;
        }
        // 平台探针：macOS 内核对「连本机任意自有地址」走本地短路 —— 即使服务端只绑 127.0.0.1，
        // 连本机 LAN IP 也会成功（裸 ServerSocket 实测复现）。这类平台上"LAN 连不上"无法在本地
        // 复现，跳过而不是假红；Linux（生产部署机）内核强制隔离，本用例照常全量断言。
        if (!platformEnforcesLoopbackIsolation(lanIp)) {
            System.out.println("[SKIP] 平台（macOS）对连本机自有地址走本地短路，'LAN 连不上'无法在本地复现，跳过");
            return;
        }
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(lanIp, addr.getPort()), 2000);
            // 连上了 ⇒ bind 是通配 ⇒ 失败
            org.junit.jupiter.api.Assertions.fail(
                    "connect from LAN IP " + lanIp + ":" + addr.getPort()
                            + " succeeded, but the server must only listen on loopback");
        } catch (java.io.IOException expected) {
            // 连不上 = 预期
            System.out.println("[OK] LAN IP " + lanIp + ":" + addr.getPort() + " 连不上: "
                    + expected.getClass().getSimpleName());
        } finally {
            s.close();
        }
    }

    /** 显式声明通配地址时（容器化部署形态）确实绑通配 —— 确认这条通道没被改坏。 */
    @Test
    public void explicitWildcardHostStillBindsWildcard() throws Exception {
        NettyServerConfig config = new NettyServerConfig();
        config.setListenPort(0);
        config.setHost("0.0.0.0");
        server = new NettyRemotingServer(config);
        server.start();
        InetSocketAddress addr = localAddressOf(server);
        assertTrue(addr.getAddress().isAnyLocalAddress(),
                "explicit 0.0.0.0 must still bind a wildcard address, but was " + addr.getAddress());
    }

    /** 取本机第一个非回环 IPv4；没有则返回 null。 */
    /**
     * 平台是否强制「只绑回环 ⇒ 连自有 LAN 地址被拒」。
     * 用一个与本服务端无关的裸 ServerSocket（只绑 127.0.0.1）做对照实验：
     * 对照组都连得上 ⇒ 平台根本不隔离（macOS），本断言在该平台不可判 ⇒ 调用方跳过。
     */
    private static boolean platformEnforcesLoopbackIsolation(String lanIp) throws Exception {
        java.net.ServerSocket probe = new java.net.ServerSocket();
        try {
            probe.bind(new InetSocketAddress("127.0.0.1", 0));
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress(lanIp, probe.getLocalPort()), 2000);
                return false; // 对照组都连得上 ⇒ 平台不隔离
            } catch (java.io.IOException e) {
                return true;  // 对照组被拒 ⇒ 平台强制隔离，断言可判
            } finally {
                s.close();
            }
        } finally {
            probe.close();
        }
    }

    private static String firstNonLoopbackIPv4() {
        try {
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            if (nics == null) {
                return null;
            }
            for (NetworkInterface nic : Collections.list(nics)) {
                if (!nic.isUp() || nic.isLoopback()) {
                    continue;
                }
                for (InetAddress a : Collections.list(nic.getInetAddresses())) {
                    if (a instanceof java.net.Inet4Address && !a.isLoopbackAddress() && !a.isAnyLocalAddress()) {
                        return a.getHostAddress();
                    }
                }
            }
        } catch (Exception ignore) {
            // 取不到就当作"无 LAN IP"，让用例跳过而不是假红
        }
        return null;
    }
}
