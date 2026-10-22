package com.zifang.z.mq.spring.host;

import com.zifang.z.mq.client.MQClientInstance;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;

import java.nio.charset.StandardCharsets;

/**
 * z-mq 测试 topic 预创建器 —— <b>测试夹具</b>（2026-10-22 从生产 starter 搬到这里，TASK-20260925-024）。
 *
 * <p><b>为什么它曾经在生产 starter 里、现在不在了</b>：
 * 原类是 {@code com.zifang.z.mq.spring.host.ZMqTopicBootstrap}，一个 {@code @Configuration}，
 * 挂在 {@code MqHostAutoConfiguration} 的 {@code @ComponentScan} 下 ⇒ <b>宿主一启动它就跟着启动</b>。
 * 它硬编码一个测试 topic 名（当时是 {@code z-opc-test}），每次启动都替 namesrv 建/绑那个 topic；
 * 而全仓（任何仓）没有任何真实生产者/消费者用那个 topic ——
 * 真正用它的是那张卡的 {@code ZMqTestController}（端到端测试 Controller），而那个 Controller
 * 早在 2026-10-03 就随 {@code z-opc-main-web} 模块被删了。
 * ⇒ 剩下的只有一个「每次启动都往生产集群写一次」的副作用，而它的服务对象已经不存在。
 *
 * <p><b>它现在住在 src/test</b> ⇒ 永不进生产 jar。端到端测试要建 topic 时，由测试显式调用：
 * <pre>
 *   ZmqTestTopicBootstrap bootstrap = new ZmqTestTopicBootstrap(namesrvAddr, "z-opc-test");
 *   assertTrue(bootstrap.bootstrap());
 * </pre>
 * 这里是全仓唯一的「建测试 topic」入口，topic 名由调用方给 ⇒ 不再有硬编码的生产副作用。
 *
 * <p><b>时序关键（与原类一致，踩过的坑别再踩）</b>：
 * <ol>
 *   <li>z-mq 1.0.2 的 broker 不会在收到 SendMessage 时自动向 nameserver 注册 topic，
 *       第一次 producer 发消息时 nameserver 返回 "topic not exist"，producer 报 "No route"；</li>
 *   <li>nameserver 的 UPDATE_AND_CREATE_TOPIC 是 idempotent 的，但**只在 broker 已注册**时
 *       才真的把 topic 绑到 broker —— 在注册之前调会拿到 code=0 却仍然 "No route"；</li>
 *   <li>所以必须等 broker 注册完成后再调，并且<b>重试到 nameserver 真的把 topic 绑上 broker</b>
 *       （用 GET_ROUTEINFO_BY_TOPIC 看 queueDatas 非空）才算成功。</li>
 * </ol>
 */
public class ZmqTestTopicBootstrap {

    /** 默认读写队列数（与原生产类一致）。 */
    public static final int DEFAULT_QUEUE_NUMS = 4;

    private static final long RETRY_INTERVAL_MS = 1000L;
    private static final long DEFAULT_TIMEOUT_MS = 60000L;

    private final String namesrvAddr;
    private final String topic;

    private int attempts;
    private String lastReason = "(never tried)";

    public ZmqTestTopicBootstrap(String namesrvAddr, String topic) {
        if (namesrvAddr == null || namesrvAddr.trim().isEmpty()) {
            throw new IllegalArgumentException("namesrvAddr 不能为空");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        this.namesrvAddr = namesrvAddr.trim();
        this.topic = topic.trim();
    }

    /** 用默认超时（60s）建 topic 并等到它真的绑上 broker。 */
    public boolean bootstrap() throws InterruptedException {
        return bootstrap(DEFAULT_TIMEOUT_MS);
    }

    /**
     * 建 topic 并等到它真的绑上 broker；超时返回 false。
     *
     * @param timeoutMs 总超时（毫秒）
     * @return true = topic 已建且已绑到至少一个 broker
     */
    public boolean bootstrap(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            attempts++;
            try {
                if (doCreateTopic()) {
                    lastReason = "topic bound to broker";
                    return true;
                }
            } catch (Exception e) {
                lastReason = e.getClass().getSimpleName() + ": " + e.getMessage();
            }
            Thread.sleep(RETRY_INTERVAL_MS);
        }
        return false;
    }

    /** 已经试了多少次（给测试输出看，别只报成败）。 */
    public int getAttempts() {
        return attempts;
    }

    /** 最后一次失败的原因（成功时是 "topic bound to broker"）。 */
    public String getLastReason() {
        return lastReason;
    }

    public String getTopic() {
        return topic;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    private boolean doCreateTopic() throws Exception {
        MQClientInstance client = new MQClientInstance(
                "z-mq-test-topic-bootstrap", namesrvAddr, new NettyClientConfig());
        try {
            client.start();

            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC);
            request.addExtField("topic", topic);
            request.addExtField("readQueueNums", String.valueOf(DEFAULT_QUEUE_NUMS));
            request.addExtField("writeQueueNums", String.valueOf(DEFAULT_QUEUE_NUMS));

            RemotingCommand response = client.invokeSync(namesrvAddr, request, 5000);
            if (response.getCode() != 0) {
                lastReason = "code=" + response.getCode() + " remark=" + response.getRemark();
                return false;
            }
            if (verifyTopicBound(client)) {
                return true;
            }
            lastReason = "topic registered but not bound to any broker yet";
            return false;
        } finally {
            client.shutdown();
        }
    }

    /**
     * 调 GET_ROUTEINFO_BY_TOPIC 验证 topic 是否真正绑了 broker（queueDatas 非空）。
     * <p>
     * TopicRouteData 的 JSON 形状：
     * {"orderTopicConf":0,"order":false,"queueDatas":[{...brokerName...}],"brokerDatas":[{...}]}
     * <p>
     * 关键：TopicRouteData 字段里**没有 topic 名字段**（topic 在 request extField 里），
     * 所以 body 里只看 "queueDatas" / "brokerDatas" / "brokerName"。
     */
    private boolean verifyTopicBound(MQClientInstance client) throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC);
        req.addExtField("topic", topic);
        RemotingCommand resp = client.invokeSync(namesrvAddr, req, 3000);
        if (resp.getCode() != 0) {
            lastReason = "GET_ROUTEINFO_BY_TOPIC code=" + resp.getCode() + " remark=" + resp.getRemark();
            return false;
        }
        byte[] body = resp.getBody();
        if (body == null || body.length < 30) {
            lastReason = "route body 为空或过短";
            return false;
        }
        String bodyStr = new String(body, StandardCharsets.UTF_8);
        int idx = bodyStr.indexOf("\"queueDatas\":");
        if (idx < 0) {
            lastReason = "route body 里没有 queueDatas 字段";
            return false;
        }
        int bracketIdx = bodyStr.indexOf('[', idx);
        if (bracketIdx < 0) {
            lastReason = "route body 里 queueDatas 没有数组";
            return false;
        }
        int closeBracketIdx = bodyStr.indexOf(']', bracketIdx);
        // [] 是空, [{...}] 是非空
        return closeBracketIdx > bracketIdx + 1;
    }
}
