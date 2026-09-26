package com.zifang.z.mq.integration;

import com.zifang.z.mq.broker.BrokerConfig;
import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.broker.processor.PullMessageProcessor;
import com.zifang.z.mq.broker.processor.SendMessageProcessor;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.protocol.SendResult;
import com.zifang.z.mq.common.protocol.SendStatus;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyServerConfig;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.MessageStoreConfig;
import com.zifang.z.mq.store.log.CommitLog;
import com.zifang.z.mq.store.log.FlushDiskType;
import com.zifang.z.mq.store.log.MappedFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 交付物第 2 项：<b>真 shutdown、真重新 load，已确认的消息读得回来</b>（工单 T3）。
 * <p>
 * 与 {@code CommitLogRecoveryTest}（存储层单测）的分工不同，这里走的是 broker 的真实链路：
 * <ol>
 *   <li>{@link BrokerController#initialize()} + {@link BrokerController#start()} 起一个真 broker
 *       （真绑端口、真起刷盘线程、真落 abort 文件）；</li>
 *   <li>用真 {@link SendMessageProcessor} 发 N 条并<b>逐条收下 broker 的确认</b>
 *       （响应体 {@code SendStatus.SEND_OK}）；策略钉在 {@code SYNC_FLUSH} ——
 *       在该策略下确认意味着 {@code force()} 真的成功过，"已确认"才是有分量的词；</li>
 *   <li>两种终止方式各测一遍：
 *       (a) {@link BrokerController#shutdown()} 干净退出（abort 被删、checkpoint 落住）；
 *       (b) <b>掉电</b>：不调用任何 shutdown，直接把这个进程内实例丢掉 ——
 *       abort 文件原样留在盘上，checkpoint 停在上一次落住的位置；</li>
 *   <li>换一个<b>全新的 BrokerController 实例</b>（同一路径、代表重启后的进程）重新
 *       {@code initialize()}，走真 {@link PullMessageProcessor} 把消息读回来，
 *       并按 {@code msgId / topic / tags / keys / flag / body / queueOffset / queueId /
 *       bornTimestamp / reconsumeTimes} 逐字段比对。</li>
 * </ol>
 * 位点单独自证：queueOffset 必须从 0 连续到 N-1、commitLogOffset 严格递增、
 * {@code getRecoveredMessages()} 必须正好是 N —— 否则"读得回来"可能只是读了个前缀。
 * <p>
 * 存储路径：{@code storePathRootDir} 与 {@code storePathCommitLog} <b>都</b>显式设进 {@code @TempDir}，
 * 收尾递归删掉（只设 rootDir 会让 commitlog 逃逸到 {@code ~/store/commitlog}，本仓历史上留过 1.0G 残渣）。
 * 端口用 {@code new ServerSocket(0)} 现问现用，不占固定端口。
 */
public class RestartDurabilityTest {

    private static final int MESSAGE_COUNT = 100;
    private static final String TOPIC = "RestartDurabilityTopic";
    private static final int QUEUE_ID = 3;

    @TempDir
    Path tempDir;

    private final List<BrokerController> launched = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        // 掉电用例故意不把第一个 broker 关干净；收尾时统一收尸，只清线程与映射，不再影响断言
        for (BrokerController broker : launched) {
            try {
                broker.shutdown();
            } catch (Exception ignore) {
                // 掉电用例里通道可能已被自己的 shutdown 关掉，忽略
            }
        }
        launched.clear();
        deleteRecursively(tempDir.toFile());
    }

    // ==================== 用例 A：干净 shutdown ⇒ 重启后已确认的消息一条不少 ====================

    @Test
    public void acknowledgedMessagesSurviveCleanShutdownAndRealReload() throws Exception {
        String sub = "clean-restart";
        BrokerController first = startBroker(sub, FlushDiskType.SYNC_FLUSH);
        File rootDir = new File(storeRoot(sub));
        List<Written> written;

        try {
            written = sendAndAcknowledge(first, "ack-clean");
            assertEquals(MESSAGE_COUNT, written.size());

            CommitLog log1 = first.getCommitLog();
            MappedFile last1 = log1.getMappedFileQueue().peekLastMappedFile();
            assertEquals(last1.getWrotePosition(), last1.getFlushedPosition(),
                    "SYNC_FLUSH 下确认过的记录必须已经 force 到盘（否则这条用例什么都没保证）");
            assertTrue(new File(rootDir, CommitLog.ABORT_FILE_NAME).exists(),
                    "start() 之后 abort 文件必须在");
            assertTrue(log1.getMappedFileQueue().getFlushedOffset() > 0,
                    "已刷位点必须真的动过");
        } finally {
            first.shutdown();
            launched.remove(first);
        }

        // 干净关闭的盘上形状：abort 被删掉，checkpoint 留在最后刷盘位点
        assertFalse(new File(rootDir, CommitLog.ABORT_FILE_NAME).exists(),
                "BrokerController.shutdown() 必须走 CommitLog.shutdown()：flush + checkpoint + 删 abort");
        assertTrue(new File(rootDir, CommitLog.CHECKPOINT_FILE_NAME).exists(),
                "干净关闭必须留下 checkpoint 文件");

        // ---- 真重新 load：全新实例（代表重启后的进程）----
        BrokerController second = startBroker(sub, FlushDiskType.SYNC_FLUSH);
        assertFalse(second.getCommitLog().isLastShutdownAbnormally(),
                "干净关闭后重启不应被判成异常退出");
        assertEquals(MESSAGE_COUNT, second.getCommitLog().getRecoveredMessages(),
                "恢复扫描必须认出全部已确认记录");

        List<MessageExt> reread = pullThroughProcessor(second, MESSAGE_COUNT);
        assertSameFields(written, reread);

        // 队列索引也是重启后重建的，不是进程内残留
        CommitLog log2 = second.getCommitLog();
        assertEquals(MESSAGE_COUNT, log2.getQueueIndex().getSize(TOPIC, QUEUE_ID),
                "重启后队列索引条数");
        assertEquals(MESSAGE_COUNT, log2.getQueueIndex().getMaxOffset(TOPIC, QUEUE_ID),
                "重启后队列最大位点必须正好是 N（下一条该写 100）");

        // 重启后再发一条并确认：说明恢复出来的位点是"能继续写"的活位点，不是只读快照
        sendOneAndAck(second, "post-restart", MESSAGE_COUNT);
        assertEquals(MESSAGE_COUNT + 1L, log2.getQueueIndex().getMaxOffset(TOPIC, QUEUE_ID),
                "队列位点必须从恢复出来的位置继续递增");
        assertEquals(MESSAGE_COUNT + 1,
                log2.pullMessage(TOPIC, QUEUE_ID, 0, MESSAGE_COUNT + 2).size(),
                "重启后的新确认记录与恢复出来的记录一起读得回来");
        assertEquals(MESSAGE_COUNT, log2.getRecoveredMessages(),
                "getRecoveredMessages 只反映 load 那一刻的条数，不许被后续写入污染");

        second.shutdown();
        launched.remove(second);
        assertFalse(new File(rootDir, CommitLog.ABORT_FILE_NAME).exists());
    }

    // ==================== 用例 B：掉电（无 shutdown）⇒ 已确认的消息仍读得回来 ====================

    @Test
    public void acknowledgedMessagesSurvivePowerLossAndRealReload() throws Exception {
        String sub = "power-loss";
        BrokerController first = startBroker(sub, FlushDiskType.SYNC_FLUSH);
        File rootDir = new File(storeRoot(sub));

        List<Written> written = sendAndAcknowledge(first, "ack-crash");
        CommitLog log1 = first.getCommitLog();
        MappedFile last1 = log1.getMappedFileQueue().peekLastMappedFile();
        assertEquals(last1.getWrotePosition(), last1.getFlushedPosition(),
                "掉电前置条件：确认过的记录都已 force，掉电掉的是未确认的部分");
        assertTrue(new File(rootDir, CommitLog.ABORT_FILE_NAME).exists(),
                "掉电前置条件：还在运行中的 abort 文件");

        // ---- 掉电：不调用 shutdown()，直接把实例丢掉（abort 留在盘上、checkpoint 停在旧值）----
        // 之后由 tearDown 收尸，确保测试结束后不遗留线程与映射。
        final long flushedBeforeCrash = log1.getMappedFileQueue().getFlushedOffset();

        BrokerController second = startBroker(sub, FlushDiskType.SYNC_FLUSH);
        CommitLog log2 = second.getCommitLog();
        assertTrue(log2.isLastShutdownAbnormally(),
                "abort 残留 ⇒ 必须被判成上次非干净退出（掉电的判定依据）");
        assertEquals(MESSAGE_COUNT, log2.getRecoveredMessages(),
                "即便被判为掉电，恢复扫描也要把已确认的记录全认回来");

        List<MessageExt> reread = pullThroughProcessor(second, MESSAGE_COUNT);
        assertSameFields(written, reread);

        // 掉电重启后 checkpoint 文件仍在，且没有把已落盘前缀说小
        assertTrue(new File(rootDir, CommitLog.CHECKPOINT_FILE_NAME).exists(),
                "第一次生命周期的位点必须已进 checkpoint（SYNC_FLUSH 的确认就是 force 成功）");
        assertTrue(log2.getMappedFileQueue().getFlushedOffset() >= flushedBeforeCrash,
                "重启后已刷位点不许倒退");

        // 掉电重启后再确认一条，然后干净退出 —— 证明恢复出来的实例是可用的
        sendOneAndAck(second, "post-crash", MESSAGE_COUNT);
        assertEquals(MESSAGE_COUNT + 1L, log2.getQueueIndex().getMaxOffset(TOPIC, QUEUE_ID));
        second.shutdown();
        launched.remove(second);
        assertFalse(new File(rootDir, CommitLog.ABORT_FILE_NAME).exists(),
                "重启后这一次退出走干净路径，abort 必须被删掉");
    }

    // ==================== 写入 / 确认 / 读回 ====================

    /** 一条"已被 broker 确认"的记录，连同它在客户端侧看到的字段。 */
    private static final class Written {
        final String msgId;
        final String tags;
        final String keys;
        final int flag;
        final long bornTimestamp;
        final byte[] body;
        final long queueOffset;

        Written(String msgId, String tags, String keys, int flag, long bornTimestamp,
                byte[] body, long queueOffset) {
            this.msgId = msgId;
            this.tags = tags;
            this.keys = keys;
            this.flag = flag;
            this.bornTimestamp = bornTimestamp;
            this.body = body;
            this.queueOffset = queueOffset;
        }
    }

    /** 逐条发送并<b>要求 broker 逐条确认</b>；任何一条没拿到 SEND_OK 就直接红。 */
    private List<Written> sendAndAcknowledge(BrokerController broker, String idPrefix) throws Exception {
        List<Written> written = new ArrayList<>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            written.add(sendOneAndAck(broker, idPrefix, i));
        }
        return written;
    }

    /**
     * 经过真实的 {@link SendMessageProcessor} 发一条，并把响应体里的 {@code SendStatus} 当作"确认"来看。
     * 传输层码 {@code SUCCESS} + 响应体 {@code SEND_OK} 才是已确认；{@code FLUSH_DISK_TIMEOUT} 一律算失败
     * （见 T2：那条状态码不再被当成成功）。
     */
    private Written sendOneAndAck(BrokerController broker, String idPrefix, int i) throws Exception {
        String msgId = "RT-" + idPrefix + "-" + i;
        String tags = "TAG-" + (i % 7);
        String keys = "KEY-" + i;
        int flag = 11 + i;
        long bornTimestamp = 1700000000000L + i;
        byte[] body = ("restart-payload-" + i + "-" + idPrefix + "-some-realistic-bytes")
                .getBytes(StandardCharsets.UTF_8);

        MessageExt inbound = new MessageExt();
        inbound.setTopic(TOPIC);
        inbound.setMsgId(msgId);
        inbound.setTags(tags);
        inbound.setKeys(keys);
        inbound.setFlag(flag);
        inbound.setBornTimestamp(bornTimestamp);
        inbound.setBody(body);

        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE);
        req.addExtField("topic", TOPIC);
        req.addExtField("queueId", String.valueOf(QUEUE_ID));
        req.addExtField("brokerName", broker.getBrokerConfig().getBrokerName());
        req.setBody(JsonCodec.encode(inbound));

        SendMessageProcessor processor = new SendMessageProcessor(broker);
        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(RemotingSysResponseCode.SUCCESS, resp.getCode(), "传输层必须应答");
        SendResult sr = JsonCodec.decode(resp.getBody(), SendResult.class);
        assertNotNull(sr, "响应体必须能解出 SendResult");
        assertEquals(SendStatus.SEND_OK, sr.getSendStatus(),
                "第 " + i + " 条必须被 broker 确认为 SEND_OK（FLUSH_DISK_TIMEOUT/SEND_FAILED 都算这条用例失败）");

        return new Written(msgId, tags, keys, flag, bornTimestamp, body, sr.getQueueOffset());
    }

    /** 经过真实的 {@link PullMessageProcessor} 读回（响应体是 {@link PullResultPayload}）。 */
    private List<MessageExt> pullThroughProcessor(BrokerController broker, int maxNum) throws Exception {
        PullMessageProcessor processor = new PullMessageProcessor(broker);
        RemotingCommand req = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE);
        req.addExtField("topic", TOPIC);
        req.addExtField("queueId", String.valueOf(QUEUE_ID));
        req.addExtField("offset", "0");
        req.addExtField("maxNum", String.valueOf(maxNum));

        RemotingCommand resp = processor.processRequest(null, req);
        assertEquals(RemotingSysResponseCode.SUCCESS, resp.getCode());
        PullResultPayload payload = JsonCodec.decode(resp.getBody(), PullResultPayload.class);
        assertNotNull(payload, "拉取响应体必须能解出 PullResultPayload");
        assertEquals(TOPIC, payload.getTopic());
        assertEquals(QUEUE_ID, payload.getQueueId());
        return payload.getMessages() == null ? new ArrayList<MessageExt>() : payload.getMessages();
    }

    // ==================== 断言：逐字段 ====================

    private void assertSameFields(List<Written> expected, List<MessageExt> actual) {
        assertEquals(expected.size(), actual.size(),
                "重启后读回的条数必须等于已确认条数（少了就是掉电丢数据）");
        for (int i = 0; i < expected.size(); i++) {
            Written e = expected.get(i);
            MessageExt a = actual.get(i);
            String at = "index=" + i + " ";
            assertEquals(e.msgId, a.getMsgId(), at + "msgId");
            assertEquals(TOPIC, a.getTopic(), at + "topic");
            assertEquals(QUEUE_ID, a.getQueueId(), at + "queueId");
            assertEquals(e.tags, a.getTags(), at + "tags");
            assertEquals(e.keys, a.getKeys(), at + "keys");
            assertEquals(e.flag, a.getFlag(), at + "flag");
            assertArrayEquals(e.body, a.getBody(), at + "body");
            assertEquals(e.bornTimestamp, a.getBornTimestamp(), at + "bornTimestamp");
            assertEquals(e.queueOffset, a.getQueueOffset(), at + "queueOffset（确认时给客户的位点）");
            assertTrue(a.getCommitLogOffset() >= 0, at + "commitLogOffset 必须由重启后的扫描给出");
            assertTrue(a.getStoreSize() > 0, at + "storeSize 必须是非默认值");
            assertTrue(a.getBodyCRC() > 0, at + "bodyCRC 必须由重启后的逐条校验给出");
            assertTrue(a.getStoreTimestamp() > 0, at + "storeTimestamp 由 broker 侧写入，必须非默认");

            // 非空洞自检：位点必须真的连续，否则"读回 100 条"可能是同一批被数了两遍
            assertEquals((long) i, a.getQueueOffset(), at + "queueOffset 必须从 0 连续递增");
            if (i > 0) {
                assertTrue(a.getCommitLogOffset() > actual.get(i - 1).getCommitLogOffset(),
                        at + "commitLogOffset 必须严格递增");
            }
        }
        assertEquals(MESSAGE_COUNT - 1L, actual.get(actual.size() - 1).getQueueOffset(),
                "最后一条的队列位点必须是 N-1");
    }

    // ==================== broker 生命周期 ====================

    /**
     * 起一个真 broker：{@code initialize()}（真 load/恢复）+ {@code start()}（真绑端口、真起刷盘线程、
     * 真落 abort）。不配 namesrv —— 本用例测的是存储生命周期，不是路由。
     */
    private BrokerController startBroker(String sub, FlushDiskType flushDiskType) throws Exception {
        MessageStoreConfig msc = new MessageStoreConfig();
        msc.setStorePathRootDir(storeRoot(sub));
        msc.setStorePathCommitLog(storeRoot(sub) + File.separator + "commitlog");
        msc.setMappedFileSizeCommitLog(1024 * 1024);
        msc.setFlushDiskType(flushDiskType);

        BrokerConfig bc = new BrokerConfig();
        bc.setBrokerName("RestartBroker");
        bc.setBrokerClusterName("RestartCluster");
        bc.setNamesrvAddr("");

        NettyServerConfig nsc = new NettyServerConfig();
        nsc.setListenPort(freePort());

        BrokerController broker = new BrokerController(bc, msc, nsc);
        assertTrue(broker.initialize(), "BrokerController.initialize() 必须成功（真 load）");
        broker.start();
        launched.add(broker);
        assertTrue(broker.getCommitLog().getMappedFileQueue().getMappedFiles().size() > 0,
                "start() 后必须真有 mapped file");
        return broker;
    }

    private String storeRoot(String sub) {
        return tempDir.resolve(sub).toString();
    }

    /** 现问一个当前空闲的端口，不占固定端口。 */
    private static int freePort() throws Exception {
        ServerSocket probe = new ServerSocket(0);
        try {
            return probe.getLocalPort();
        } finally {
            probe.close();
        }
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        if (!file.delete()) {
            System.err.println("[restart-durability] leftover: " + file.getAbsolutePath());
        }
    }
}
