package com.zifang.z.mq.integration.support;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 全 JVM 共享的空闲端口分配器。
 *
 * <p>⚠ 为什么必须共享（2026-10-30 实测修掉的真回归）：
 * 端口探测是典型的 TOCTOU —— {@code new ServerSocket(0)} 拿到端口后**立刻 close**，
 * 再回头去 bind。探测与 bind 之间存在窗口期。
 *
 * <p>原先 {@link ClusterTestHelper} 与 {@code RestartDurabilityTest} **各有一份**
 * {@code CLAIMED_PORTS} 静态认领表。两份表互相看不见对方认领过的端口，
 * 于是同一个 JVM 里两个测试类可以各自探到**同一个端口**并先后去 bind
 * ⇒ 后 bind 的那个直接 {@code java.net.BindException: Address already in use}。
 *
 * <p>实测症状：全量跑 {@code mvn test} 时 {@code RestartDurabilityTest} 偶发红，
 * 单独跑同一个测试类**必绿**（因为撞车的另一个测试类没同时在跑）。
 * 这正是端口竞态的典型指纹 —— 单独跑绿、全量跑红。
 *
 * <p>本类把认领表提为**唯一一份** JVM 级共享状态：
 * <ul>
 *   <li>所有测试类共用一张表 ⇒ 探到的端口全局唯一，不会互相撞</li>
 *   <li>仍然保留「探完即放 + 认领去重 + 最多重试 50 次」的原有语义</li>
 * </ul>
 *
 * <p>另外，本类在拿到端口后**不主动持有 socket**（保持与原实现一致的释放时机），
 * 因此仍存在极窄的窗口期；认领表把绝大多数撞车消掉，剩余窗口由重试兜底。
 */
public final class PortAllocator {

    /** 全 JVM 唯一的端口认领表 —— 所有测试类共用这一份。 */
    private static final Set<Integer> CLAIMED_PORTS = ConcurrentHashMap.newKeySet();

    /** 最多探测次数：撞号时换一个新端口重探。 */
    private static final int MAX_ATTEMPTS = 50;

    private PortAllocator() {
    }

    /**
     * 探一个当前空闲且**未被本 JVM 任何测试类认领过**的 TCP 端口。
     *
     * @return 端口号
     * @throws IllegalStateException 连探 50 次都没拿到可用端口
     */
    public static int freePort() {
        for (int i = 0; i < MAX_ATTEMPTS; i++) {
            int p;
            try (ServerSocket ss = new ServerSocket(0)) {
                p = ss.getLocalPort();
            } catch (IOException e) {
                throw new IllegalStateException("找不到空闲端口", e);
            }
            if (CLAIMED_PORTS.add(p)) {
                return p;
            }
        }
        throw new IllegalStateException(MAX_ATTEMPTS + " 次都没探到未被本 JVM 认领的端口");
    }
}
