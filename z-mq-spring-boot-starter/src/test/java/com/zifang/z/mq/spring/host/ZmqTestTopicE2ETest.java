package com.zifang.z.mq.spring.host;

import com.zifang.z.mq.client.consumer.ConsumeConcurrentlyStatus;
import com.zifang.z.mq.client.consumer.DefaultMQPushConsumer;
import com.zifang.z.mq.client.consumer.MessageListener;
import com.zifang.z.mq.client.consumer.MessageQueueContext;
import com.zifang.z.mq.client.producer.DefaultMQProducer;
import com.zifang.z.mq.common.message.Message;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.spring.host.support.EmbeddedZmqCluster;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验收：<b>反向对照 —— 显式打开该能力时，端到端仍能跑通一次</b>（TASK-20260925-024）。
 *
 * <p>本卡选的方案是「把端到端测试能力整体搬进 z-mq 仓自己的测试夹具」，
 * 所以「显式打开」= <b>测试里显式调用夹具 {@link ZmqTestTopicBootstrap}</b>：
 * <pre>
 *   new ZmqTestTopicBootstrap(namesrvAddr, "z-opc-test").bootstrap()
 * </pre>
 * 生产 starter 默认形态下这条能力<b>不装配</b>（见 {@link ZmqHostAssemblyNoTestTopicTest}），
 * 但能力本身没被删掉 —— 这里证明它还能用：producer → broker → consumer 跑通一次，
 * 且 sent / received 计数非零。
 *
 * <p>跑在**本 JVM 自己起的** namesrv + broker 上（临时端口），不碰共享 broker。
 */
public class ZmqTestTopicE2ETest {

    /** 曾经由生产 starter 自动创建的那个测试 topic —— 这里由测试显式创建。 */
    private static final String TEST_TOPIC = "z-opc-test";
    /** 曾经由生产 starter 自动挂上的那个测试消费者组 —— 这里由测试显式挂。 */
    private static final String TEST_GROUP = "z-opc-test-group";
    private static final int MESSAGE_COUNT = 3;

    @Test
    @DisplayName("反向对照: 显式建 topic 后 producer → broker → consumer 跑通, sent/received 非零")
    public void explicitTopicBootstrapStillWorksEndToEnd() throws Exception {
        EmbeddedZmqCluster cluster = EmbeddedZmqCluster.start();
        DefaultMQProducer producer = null;
        DefaultMQPushConsumer consumer = null;
        try {
            // ① 显式打开该能力：建 topic 并等到它真的绑到 broker
            ZmqTestTopicBootstrap bootstrap = new ZmqTestTopicBootstrap(cluster.namesrvAddr(), TEST_TOPIC);
            boolean ready = bootstrap.bootstrap(30000L);
            System.out.println("[zmq024] bootstrap topic=" + TEST_TOPIC
                    + " ready=" + ready + " attempts=" + bootstrap.getAttempts()
                    + " lastReason=" + bootstrap.getLastReason());
            assertTrue(ready, "显式建 topic 失败（attempts=" + bootstrap.getAttempts()
                    + ", lastReason=" + bootstrap.getLastReason() + "）⇒ 端到端能力被搬坏了");

            // ② producer → broker
            producer = new DefaultMQProducer("ZMQ_STARTER_TEST_PRODUCER");
            producer.setNamesrvAddr(cluster.namesrvAddr());
            producer.start();
            int sent = 0;
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                Message message = new Message(TEST_TOPIC, ("zmq024-msg-" + i).getBytes(StandardCharsets.UTF_8));
                SendResult sendResult = producer.send(message);
                assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus(),
                        "send status 应为 SEND_OK，实际 " + sendResult.getSendStatus());
                sent++;
            }

            // ③ broker → consumer
            CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
            CopyOnWriteArrayList<MessageExt> received = new CopyOnWriteArrayList<>();
            consumer = new DefaultMQPushConsumer(TEST_GROUP);
            consumer.setNamesrvAddr(cluster.namesrvAddr());
            consumer.setPullIntervalMillis(300);
            consumer.subscribe(TEST_TOPIC, new MessageListener.Concurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(MessageExt[] msgs, MessageQueueContext ctx) {
                    for (MessageExt m : msgs) {
                        received.add(m);
                        latch.countDown();
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();

            assertTrue(latch.await(20, TimeUnit.SECONDS),
                    "20s 内没收到 " + MESSAGE_COUNT + " 条消息 ⇒ 端到端链路不通");

            int receivedCount = received.size();
            // 形状照旧那张卡的 /api/test/zmq/stats（sent / received / queue_size）留个可读的读数
            System.out.println("[zmq024] stats = {\"sent\":" + sent
                    + ",\"received\":" + receivedCount
                    + ",\"queue_size\":" + receivedCount + "}");
            System.out.println("[zmq024] namesrv topicList = " + cluster.topicListJson());
            System.out.println("[zmq024] broker consumerOffset = " + cluster.brokerConsumerOffsetJson());

            assertTrue(sent > 0, "sent 必须是非零");
            assertTrue(receivedCount > 0, "received 必须是非零");
            assertEquals(sent, receivedCount, "producer 发出的条数与 consumer 收到的条数应当一致");
        } finally {
            if (consumer != null) {
                consumer.shutdown();
            }
            if (producer != null) {
                producer.shutdown();
            }
            cluster.close();
        }
    }
}
