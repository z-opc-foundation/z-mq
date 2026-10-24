package com.zifang.z.mq.spring.host;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 守卫：<b>拨号闸门</b>（TASK-20261023-081 · 腿①「改指集群内独立 z-mq」的前置件）。
 *
 * <p>背景：{@code MqProxyController#invoke()} 校验的是**目标地址**，原来的硬约束是
 * 「只允许环回」（{@code LOOPBACK_ADDR}）⇒ 把 {@code z.mq.namesrv-addr} 指到集群内的
 * z-mq Service 会得 <b>403 refused non-loopback target</b>，不是 200。
 *
 * <p>那道闸门有正当理由（z-mq remoting 层无默认鉴权、9876/10911 通配 bind ⇒
 * 「前端只走 8888」是实际安全边界）⇒ 不能裸放成「允许任意地址」。
 * 新的等效管控是：<b>环回 ∪ 运维在 {@code z.mq.proxy.dial-allowlist} 里声明的固定清单</b>。
 *
 * <p>这里钉住四件事：
 * <ol>
 *   <li>清单为空 ⇒ 只允许环回（<b>与改造前逐字同行为</b>，现有部署零回归）；</li>
 *   <li>清单里的 {@code host:port} 只放行**那一个**端口（不多放行）；</li>
 *   <li>清单里的裸 {@code host} 放行该 host 的任意端口；</li>
 *   <li>白名单之外的一切（含「像白名单的」相似串、别的端口、别的 host）一律拒绝。</li>
 * </ol>
 */
@DisplayName("MqProxyController 拨号闸门（环回 ∪ 显式白名单）")
public class MqProxyControllerDialAllowlistTest {

    @Test
    @DisplayName("默认（清单为空）：环回允许，非环回一律拒绝")
    public void emptyAllowlistKeepsLoopbackOnly() {
        assertTrue(MqProxyController.dialAllowed("127.0.0.1:9877", ""));
        assertTrue(MqProxyController.dialAllowed("127.0.0.1:9877", null));
        assertTrue(MqProxyController.dialAllowed("localhost:9877", "   "));
        assertTrue(MqProxyController.dialAllowed("[::1]:9877", ""));
        assertFalse(MqProxyController.dialAllowed("z-mq-nameserver.z-mq.svc.cluster.local:9876", ""));
        assertFalse(MqProxyController.dialAllowed("10.42.2.93:9876", ""));
        assertFalse(MqProxyController.dialAllowed("example.com:80", ""));
    }

    @Test
    @DisplayName("清单写了 host:port ⇒ 只放行那一个端口")
    public void hostPortEntryAllowsOnlyThatPort() {
        String allow = "z-mq-nameserver.z-mq.svc.cluster.local:9876";
        assertTrue(MqProxyController.dialAllowed("z-mq-nameserver.z-mq.svc.cluster.local:9876", allow));
        assertFalse(MqProxyController.dialAllowed("z-mq-nameserver.z-mq.svc.cluster.local:9877", allow));
        assertFalse(MqProxyController.dialAllowed("z-mq-nameserver.z-mq.svc.cluster.local", allow));
    }

    @Test
    @DisplayName("清单写了裸 host ⇒ 该 host 的任意端口都放行")
    public void bareHostEntryAllowsAnyPort() {
        String allow = "z-mq-broker.z-mq.svc.cluster.local";
        assertTrue(MqProxyController.dialAllowed("z-mq-broker.z-mq.svc.cluster.local:10911", allow));
        assertTrue(MqProxyController.dialAllowed("z-mq-broker.z-mq.svc.cluster.local:10912", allow));
        assertFalse(MqProxyController.dialAllowed("z-mq-other.z-mq.svc.cluster.local:10911", allow));
    }

    @Test
    @DisplayName("多项目录：逗号分隔、忽略空格、大小写不敏感")
    public void multiEntryAllowlist() {
        String allow = " z-mq-nameserver.z-mq.svc.cluster.local:9876 , "
                + "z-mq-broker.z-mq.svc.cluster.local:10911 ";
        assertTrue(MqProxyController.dialAllowed("z-mq-nameserver.z-mq.svc.cluster.local:9876", allow));
        assertTrue(MqProxyController.dialAllowed("z-mq-broker.z-mq.svc.cluster.local:10911", allow));
        assertTrue(MqProxyController.dialAllowed("Z-MQ-BROKER.Z-MQ.SVC.CLUSTER.LOCAL:10911", allow));
        assertFalse(MqProxyController.dialAllowed("z-mq-broker.z-mq.svc.cluster.local:10912", allow));
        assertFalse(MqProxyController.dialAllowed("evil.example.com:10911", allow));
    }

    @Test
    @DisplayName("相似串不是白名单项（不许前缀/后缀匹配蒙混过关）")
    public void similarButDifferentHostIsRefused() {
        String allow = "z-mq-broker.z-mq.svc.cluster.local:10911";
        assertFalse(MqProxyController.dialAllowed("evil-z-mq-broker.z-mq.svc.cluster.local:10911", allow));
        assertFalse(MqProxyController.dialAllowed("z-mq-broker.z-mq.svc.cluster.local.evil.com:10911", allow));
        assertFalse(MqProxyController.dialAllowed("z-mq-broker:10911", allow));
    }

    @Test
    @DisplayName("空地址 / null 一律拒绝")
    public void nullOrEmptyAddrRefused() {
        String allow = "z-mq-nameserver.z-mq.svc.cluster.local:9876";
        assertFalse(MqProxyController.dialAllowed(null, allow));
        assertFalse(MqProxyController.dialAllowed("", allow));
        assertFalse(MqProxyController.dialAllowed("   ", allow));
    }

    @Test
    @DisplayName("清单里写坏的项不影响其它项（fail-closed：坏项当不匹配）")
    public void malformedEntryDoesNotOpenGate() {
        String allow = "not-a-port:abc, z-mq-nameserver.z-mq.svc.cluster.local:9876";
        assertTrue(MqProxyController.dialAllowed("z-mq-nameserver.z-mq.svc.cluster.local:9876", allow));
        assertFalse(MqProxyController.dialAllowed("not-a-port:abc", allow));
        assertFalse(MqProxyController.dialAllowed("not-a-port:9876", allow));
    }

    @Test
    @DisplayName("hostOf 取 host 部分（IPv6 带方括号也对）")
    public void hostOfExtractsHost() {
        assertTrue("z-mq-broker.z-mq.svc.cluster.local"
                .equals(MqProxyController.hostOf("z-mq-broker.z-mq.svc.cluster.local:10911")));
        assertTrue("::1".equals(MqProxyController.hostOf("[::1]:9877")));
    }
}
