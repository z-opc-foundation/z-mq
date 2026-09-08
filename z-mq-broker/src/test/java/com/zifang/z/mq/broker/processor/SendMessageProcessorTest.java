package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerConfig;
import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.MessageStoreConfig;
import com.zifang.z.mq.store.log.CommitLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SendMessageProcessor 单元测试 — 驱动 SendMessageProcessor.processRequest 各分支。
 */
public class SendMessageProcessorTest {

    @TempDir
    Path tempDir;

    private BrokerController controller;
    private SendMessageProcessor processor;

    @BeforeEach
    public void setUp() throws Exception {
        BrokerConfig bc = new BrokerConfig();
        bc.setBrokerName("TestBroker");
        bc.setBrokerClusterName("TestCluster");
        MessageStoreConfig msc = new MessageStoreConfig();
        msc.setStorePathRootDir(tempDir.resolve("store").toString());
        msc.setStorePathCommitLog(tempDir.resolve("store/commitlog").toString());
        msc.setMappedFileSizeCommitLog(1024 * 1024);
        NettyServerConfig nsc = new NettyServerConfig();
        controller = new BrokerController(bc, msc, nsc);
        assertTrue(controller.initialize());
        // 不调用 start(), 只用 initialize 后的 commitLog
        processor = new SendMessageProcessor(controller);
    }

    @Test
    public void testRejectRequestReturnsFalse() {
        assertEquals(false, processor.rejectRequest());
    }

    @Test
    public void testMissingTopicReturnsError() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("queueId", "0");
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SYSTEM_ERROR, resp.getCode());
        assertTrue(resp.getRemark().contains("topic or queueId"));
    }

    @Test
    public void testMissingQueueIdReturnsError() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("topic", "T");
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SYSTEM_ERROR, resp.getCode());
    }

    @Test
    public void testMissingBodyReturnsError() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("topic", "T");
        req.addExtField("queueId", "0");
        req.addExtField("brokerName", "TestBroker");
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SYSTEM_ERROR, resp.getCode());
        assertTrue(resp.getRemark().contains("decode body failed"));
    }

    @Test
    public void testInvalidJsonBodyReturnsError() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("topic", "T");
        req.addExtField("queueId", "0");
        req.addExtField("brokerName", "TestBroker");
        req.setBody("not-a-json".getBytes());
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(com.zifang.z.mq.remoting.netty.RemotingSysResponseCode.SYSTEM_ERROR, resp.getCode());
    }

    @Test
    public void testSendSuccess() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("topic", "TestTopic");
        req.addExtField("queueId", "0");
        req.addExtField("brokerName", "TestBroker");
        MessageExt ext = new MessageExt();
        ext.setTopic("TestTopic");
        ext.setBody("hello".getBytes());
        ext.setTags("tagA");
        ext.setKeys("k1");
        req.setBody(JsonCodec.encode(ext));

        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(0, resp.getCode());
        SendResult sr = JsonCodec.decode(resp.getBody(), SendResult.class);
        assertNotNull(sr);
        assertEquals(SendStatus.SEND_OK, sr.getSendStatus());
    }

    @Test
    public void testOnewayReturnsNull() throws Exception {
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.markOnewayRPC();
        req.addExtField("topic", "TestTopic");
        req.addExtField("queueId", "0");
        req.addExtField("brokerName", "TestBroker");
        MessageExt ext = new MessageExt();
        ext.setTopic("TestTopic");
        ext.setBody("hello".getBytes());
        req.setBody(JsonCodec.encode(ext));

        RemotingCommand resp = processor.processRequest(null, req);
        assertNull(resp);
    }

    @Test
    public void testResponsePreservesOpaque() throws Exception {
        int opaque = 123456;
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.setOpaque(opaque);
        req.addExtField("topic", "T");
        req.addExtField("queueId", "0");
        req.addExtField("brokerName", "TestBroker");
        MessageExt ext = new MessageExt();
        ext.setTopic("T");
        ext.setBody("hello".getBytes());
        req.setBody(JsonCodec.encode(ext));
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(opaque, resp.getOpaque());
    }

    @Test
    public void testCountersIncrementedOnSuccess() throws Exception {
        long before = SendMessageProcessor.TopicLogHelper.getSendCount();
        long beforeOk = SendMessageProcessor.TopicLogHelper.getSendSuccess();
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("topic", "CT");
        req.addExtField("queueId", "0");
        req.addExtField("brokerName", "TestBroker");
        MessageExt ext = new MessageExt();
        ext.setTopic("CT");
        ext.setBody("hi".getBytes());
        req.setBody(JsonCodec.encode(ext));
        processor.processRequest(null, req);
        assertTrue(SendMessageProcessor.TopicLogHelper.getSendCount() > before);
        assertTrue(SendMessageProcessor.TopicLogHelper.getSendSuccess() > beforeOk);
    }

    @Test
    public void testCommitLogAvailable() {
        // 验证 setUp 中 controller.initialize 后 commitLog 已存在
        assertNotNull(controller.getCommitLog());
    }
}
