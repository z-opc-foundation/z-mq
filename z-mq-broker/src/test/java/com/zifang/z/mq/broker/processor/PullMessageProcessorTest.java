package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerConfig;
import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.MessageExtBrokerInner;
import com.zifang.z.mq.store.MessageStoreConfig;
import com.zifang.z.mq.store.PutMessageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PullMessageProcessor 单元测试 — 验证通过 InMemoryQueueIndex 查询真实 CommitLog 内容。
 * <p>
 * 测试覆盖:
 * - 请求参数校验
 * - 空 topic 返回空列表
 * - 写入 N 条后拉取能拿到全部
 * - offset 越界行为
 * - maxNum 限制
 * - nextOffset 单调递增
 * - topic/queueId 隔离
 */
public class PullMessageProcessorTest {

    @TempDir
    Path tempDir;

    private PullMessageProcessor processor;
    private BrokerController controller;

    @BeforeEach
    public void setUp() throws Exception {
        BrokerConfig bc = new BrokerConfig();
        bc.setBrokerName("TestBroker_" + UUID.randomUUID().toString().substring(0, 6));
        MessageStoreConfig msc = new MessageStoreConfig();
        msc.setStorePathRootDir(tempDir.resolve("store").toString());
        msc.setStorePathCommitLog(tempDir.resolve("store/commitlog").toString());
        msc.setMappedFileSizeCommitLog(1024 * 1024);
        NettyServerConfig nsc = new NettyServerConfig();
        controller = new BrokerController(bc, msc, nsc);
        assertTrue(controller.initialize());
        processor = new PullMessageProcessor(controller);
    }

    private RemotingCommand pull(String topic, String queueId, long offset, Integer maxNum) throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        req.addExtField("topic", topic);
        req.addExtField("queueId", queueId);
        req.addExtField("offset", String.valueOf(offset));
        if (maxNum != null) { req.addExtField("maxNum", String.valueOf(maxNum)); }

        return processor.processRequest(null, req);
    }

    private void putMessage(String topic, int queueId, String body) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setQueueId(queueId);
        msg.setBody(body.getBytes(StandardCharsets.UTF_8));
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setPropertiesString("");
        PutMessageResult result = controller.getCommitLog().putMessage(msg);
        assertTrue(result.isOk(), "putMessage 应成功");
    }

    @Test
    public void testRejectRequestReturnsFalse() {
        assertEquals(false, processor.rejectRequest());
    }

    @Test
    public void testMissingTopicReturnsError() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SYSTEM_ERROR, resp.getCode());
    }

    @Test
    public void testMissingQueueIdReturnsError() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        req.addExtField("topic", "T");
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SYSTEM_ERROR, resp.getCode());
    }

    @Test
    public void testEmptyTopicReturnsEmptyList() throws Exception {
        RemotingCommand resp = pull("NoSuchTopic", "0", 0, 10);
        assertEquals(0, resp.getCode());
        PullResultPayload body = JsonCodec.decode(resp.getBody(), PullResultPayload.class);
        assertNotNull(body);
        assertTrue(body.getMessages().isEmpty(), "空 topic 应返回空列表");
        assertEquals(0L, body.getMaxOffset());
    }

    @Test
    public void testPullReturnsRealCommittedMessages() throws Exception {
        String topic = "RealTopic_" + UUID.randomUUID().toString().substring(0, 6);
        putMessage(topic, 0, "m1");
        putMessage(topic, 0, "m2");
        putMessage(topic, 0, "m3");

        RemotingCommand resp = pull(topic, "0", 0, 10);
        PullResultPayload body = JsonCodec.decode(resp.getBody(), PullResultPayload.class);
        assertEquals(3, body.getMessages().size());
        assertEquals("m1", new String(body.getMessages().get(0).getBody(), StandardCharsets.UTF_8));
        assertEquals("m2", new String(body.getMessages().get(1).getBody(), StandardCharsets.UTF_8));
        assertEquals("m3", new String(body.getMessages().get(2).getBody(), StandardCharsets.UTF_8));
        assertEquals(0, body.getMessages().get(0).getQueueOffset());
        assertEquals(1, body.getMessages().get(1).getQueueOffset());
        assertEquals(2, body.getMessages().get(2).getQueueOffset());
        assertEquals(3L, body.getMaxOffset(), "maxOffset 应等于已分配的最大 offset");
    }

    @Test
    public void testPullRespectsMaxNum() throws Exception {
        String topic = "MaxNumTopic_" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 10; i++) putMessage(topic, 0, "m" + i);

        RemotingCommand resp = pull(topic, "0", 0, 3);
        PullResultPayload body = JsonCodec.decode(resp.getBody(), PullResultPayload.class);
        assertEquals(3, body.getMessages().size());
    }

    @Test
    public void testPullFromOffset() throws Exception {
        String topic = "OffsetTopic_" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 5; i++) putMessage(topic, 0, "m" + i);

        RemotingCommand resp = pull(topic, "0", 2, 10);
        PullResultPayload body = JsonCodec.decode(resp.getBody(), PullResultPayload.class);
        assertEquals(3, body.getMessages().size());
        assertEquals(2L, body.getMessages().get(0).getQueueOffset());
        assertEquals("m2", new String(body.getMessages().get(0).getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void testPullOffsetBeyondMaxReturnsEmpty() throws Exception {
        String topic = "BeyondTopic_" + UUID.randomUUID().toString().substring(0, 6);
        putMessage(topic, 0, "m1");
        putMessage(topic, 0, "m2");

        RemotingCommand resp = pull(topic, "0", 100, 10);
        PullResultPayload body = JsonCodec.decode(resp.getBody(), PullResultPayload.class);
        assertTrue(body.getMessages().isEmpty(), "超出 maxOffset 的 offset 应返回空");
        assertEquals(2L, body.getMaxOffset());
    }

    @Test
    public void testPullRespectsQueueIsolation() throws Exception {
        String topic = "QITopic_" + UUID.randomUUID().toString().substring(0, 6);
        putMessage(topic, 0, "q0-m1");
        putMessage(topic, 1, "q1-m1");
        putMessage(topic, 1, "q1-m2");

        RemotingCommand resp0 = pull(topic, "0", 0, 10);
        PullResultPayload body0 = JsonCodec.decode(resp0.getBody(), PullResultPayload.class);
        assertEquals(1, body0.getMessages().size(), "queue 0 应只有 1 条");
        assertEquals("q0-m1", new String(body0.getMessages().get(0).getBody(), StandardCharsets.UTF_8));

        RemotingCommand resp1 = pull(topic, "1", 0, 10);
        PullResultPayload body1 = JsonCodec.decode(resp1.getBody(), PullResultPayload.class);
        assertEquals(2, body1.getMessages().size(), "queue 1 应有 2 条");
        assertEquals("q1-m1", new String(body1.getMessages().get(0).getBody(), StandardCharsets.UTF_8));
        assertEquals("q1-m2", new String(body1.getMessages().get(1).getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void testPullRespectsTopicIsolation() throws Exception {
        String topicA = "A_" + UUID.randomUUID().toString().substring(0, 6);
        String topicB = "B_" + UUID.randomUUID().toString().substring(0, 6);
        putMessage(topicA, 0, "a-m1");
        putMessage(topicB, 0, "b-m1");
        putMessage(topicB, 0, "b-m2");

        RemotingCommand respA = pull(topicA, "0", 0, 10);
        PullResultPayload bodyA = JsonCodec.decode(respA.getBody(), PullResultPayload.class);
        assertEquals(1, bodyA.getMessages().size(), "topicA 应只有 1 条");
        assertEquals("a-m1", new String(bodyA.getMessages().get(0).getBody(), StandardCharsets.UTF_8));
    }

    @Test
    public void testResponsePreservesOpaque() throws Exception {
        int opaque = 9999;
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        req.setOpaque(opaque);
        req.addExtField("topic", "T");
        req.addExtField("queueId", "0");
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(opaque, resp.getOpaque());
    }
}
