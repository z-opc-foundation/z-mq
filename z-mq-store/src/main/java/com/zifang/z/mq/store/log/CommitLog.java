package com.zifang.z.mq.store.log;

import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.store.AppendMessageResult;
import com.zifang.z.mq.store.MessageExtBrokerInner;
import com.zifang.z.mq.store.MessageStoreConfig;
import com.zifang.z.mq.store.PutMessageResult;
import com.zifang.z.mq.store.PutMessageStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;

/**
 * CommitLog 实现
 * 消息存储主文件，所有消息顺序写入CommitLog
 * <p>
 * 存储结构：
 * ┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐
 * │总大小  │魔数    │CRC32   │队列ID  │标志    │ Born时间│ Born   │Store   │
 * │(4B)    │(4B)    │(4B)    │(4B)    │(4B)    │(8B)    │Host   │时间    │
 * ├────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┤
 * │Store   │消息ID  │Commit  │消息体  │主题    │属性    │...    │        │
 * │Host    │(16B)   │Log偏移 │大小(4B)│(可变)  │(可变)  │       │        │
 * │(8B)    │        │(8B)    │        │        │        │       │        │
 * └────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘
 */
public class CommitLog {

    // 魔数
    public static final int MESSAGE_MAGIC_CODE = 0xAABBCCDD;
    // 消息魔数（用于判断消息合法性）
    public static final int MESSAGE_MAGIC_CODE_V2 = 0xCDDDABBA;
    // 空白魔数（用于填充）
    public static final int BLANK_MAGIC_CODE = 0xBBCCDDEE;
    private static final Logger log = LogManager.getLogger(CommitLog.class);
    // 默认消息体大小（用于分配缓冲区）
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024 * 4; // 4MB
    // 消息存储配置
    private final MessageStoreConfig messageStoreConfig;
    // MappedFile队列
    private final MappedFileQueue mappedFileQueue;
    // 消息编码缓冲区（线程本地）
    private final ThreadLocal<ByteBuffer> msgStoreItemMemory;
    // 刷盘服务
    private final FlushCommitLogService flushCommitLogService;

    public CommitLog(final MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        this.mappedFileQueue = new MappedFileQueue(
                messageStoreConfig.getStorePathCommitLog(),
                messageStoreConfig.getMappedFileSizeCommitLog());
        this.msgStoreItemMemory = ThreadLocal.withInitial(() -> ByteBuffer.allocate(MAX_MESSAGE_SIZE));

        // 根据刷盘策略选择服务
        if (FlushDiskType.SYNC_FLUSH == messageStoreConfig.getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }
    }

    /**
     * 启动CommitLog
     */
    public void start() {
        this.flushCommitLogService.start();
        log.info("CommitLog started");
    }

    /**
     * 关闭CommitLog
     */
    public void shutdown() {
        this.flushCommitLogService.shutdown();
        log.info("CommitLog shutdown");
    }

    /**
     * 加载CommitLog
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log {}", result ? "OK" : "Failed");
        return result;
    }

    /**
     * 存储消息
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // 设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());

        // 设置消息体CRC
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));

        // 获取当前写入的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        if (mappedFile == null) {
            log.error("create mapped file error");
            return new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
        }

        // 序列化消息
        byte[] encoded = encode(msg);

        // 检查消息大小
        if (encoded.length > MAX_MESSAGE_SIZE) {
            log.warn("message size exceeded, size: {}", encoded.length);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // 追加到MappedFile
        AppendMessageResult result = mappedFile.appendMessagesInner(encoded, msg);

        switch (result.getStatus()) {
            case PUT_OK:
                break;
            case END_OF_FILE:
                // 文件已满，创建新文件并重试
                mappedFile = this.mappedFileQueue.getLastMappedFile();
                if (mappedFile == null) {
                    log.error("create mapped file error");
                    return new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null);
                }
                result = mappedFile.appendMessagesInner(encoded, msg);
                break;
            default:
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }

        // 根据刷盘策略处理
        handleFlushAndHA(result);

        return new PutMessageResult(PutMessageStatus.PUT_OK, result);
    }

    /**
     * 编码消息
     */
    private byte[] encode(MessageExtBrokerInner msg) {
        ByteBuffer buffer = msgStoreItemMemory.get();
        buffer.clear();

        // 预分配容量
        int bodyLength = msg.getBody() != null ? msg.getBody().length : 0;
        int topicLength = msg.getTopic() != null ? msg.getTopic().getBytes().length : 0;
        int propertiesLength = msg.getPropertiesString() != null ? msg.getPropertiesString().getBytes().length : 0;

        int totalSize = 4 // 总大小
                + 4 // 魔数
                + 4 // CRC32
                + 4 // 队列ID
                + 4 // 标志
                + 8 // Born时间戳
                + 8 // Born Host
                + 8 // 存储时间戳
                + 8 // Store Host
                + 16 // 消息ID
                + 8 // CommitLog偏移量
                + 4 // 消息体大小
                + 2 // 主题长度
                + topicLength
                + 2 // 属性长度
                + propertiesLength
                + bodyLength;

        if (buffer.capacity() < totalSize) {
            // 重新分配更大的缓冲区
            buffer = ByteBuffer.allocate(totalSize);
            msgStoreItemMemory.set(buffer);
        }

        // 写入消息数据
        int storeSize = writeMessageData(buffer, msg, totalSize);

        buffer.flip();
        byte[] result = new byte[storeSize];
        buffer.get(result);
        return result;
    }

    /**
     * 写入消息数据到缓冲区
     */
    private int writeMessageData(ByteBuffer buffer, MessageExtBrokerInner msg, int totalSize) {
        int bodyLength = msg.getBody() != null ? msg.getBody().length : 0;
        byte[] topicData = msg.getTopic() != null ? msg.getTopic().getBytes() : new byte[0];
        byte[] propertiesData = msg.getPropertiesString() != null ? msg.getPropertiesString().getBytes() : new byte[0];

        // 计算各字段位置
        int pos = 0;

        // 总大小（稍后回填）
        int totalSizePos = pos;
        buffer.putInt(0); // 占位
        pos += 4;

        // 魔数
        buffer.putInt(MESSAGE_MAGIC_CODE);
        pos += 4;

        // CRC32（稍后计算）
        int crcPos = pos;
        buffer.putInt(0); // 占位
        pos += 4;

        // 队列ID
        buffer.putInt(msg.getQueueId());
        pos += 4;

        // 标志
        buffer.putInt(msg.getFlag());
        pos += 4;

        // Born时间戳
        buffer.putLong(msg.getBornTimestamp());
        pos += 8;

        // Born Host (简化处理，只存IP)
        buffer.putLong(0);
        pos += 8;

        // 存储时间戳
        buffer.putLong(msg.getStoreTimestamp());
        pos += 8;

        // Store Host (简化处理)
        buffer.putLong(0);
        pos += 8;

        // 消息ID (简化处理)
        byte[] msgIdData = new byte[16];
        buffer.put(msgIdData);
        pos += 16;

        // CommitLog偏移量
        buffer.putLong(msg.getCommitLogOffset());
        pos += 8;

        // 消息体大小
        buffer.putInt(bodyLength);
        pos += 4;

        // 主题长度
        buffer.putShort((short) topicData.length);
        pos += 2;

        // 主题内容
        buffer.put(topicData);
        pos += topicData.length;

        // 属性长度
        buffer.putShort((short) propertiesData.length);
        pos += 2;

        // 属性内容
        buffer.put(propertiesData);
        pos += propertiesData.length;

        // 消息体
        if (bodyLength > 0) {
            buffer.put(msg.getBody());
            pos += bodyLength;
        }

        // 回填总大小
        buffer.putInt(totalSizePos, pos);

        // 计算CRC32 (简化处理，使用固定值)
        int crc32 = 0;
        buffer.putInt(crcPos, crc32);

        return pos;
    }

    /**
     * 处理刷盘和高可用
     */
    private void handleFlushAndHA(AppendMessageResult result) {
        // 同步刷盘：等待刷盘完成
        if (FlushDiskType.SYNC_FLUSH == this.messageStoreConfig.getFlushDiskType()) {
            // 提交刷盘请求
            GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
            ((GroupCommitService) this.flushCommitLogService).putRequest(request);
            // 等待刷盘完成
            boolean flushOK = request.waitForFlush(this.messageStoreConfig.getSyncFlushTimeout());
            if (!flushOK) {
                log.error("sync flush message timeout");
            }
        }
        // 异步刷盘由FlushRealTimeService定时处理
    }

    /**
     * 获取MappedFile队列
     */
    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    /**
     * 追加消息内部方法
     */
    private AppendMessageResult appendMessagesInner(MappedFile mappedFile, byte[] data, MessageExtBrokerInner msg) {
        // 写入数据
        long wroteOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();

        // 设置CommitLog偏移量
        msg.setCommitLogOffset(wroteOffset);

        // 追加数据
        boolean result = mappedFile.appendMessage(data);

        if (!result) {
            return new AppendMessageResult(AppendMessageResult.AppendMessageStatus.END_OF_FILE, wroteOffset, 0, System.currentTimeMillis());
        }

        return new AppendMessageResult(AppendMessageResult.AppendMessageStatus.PUT_OK, wroteOffset, data.length, System.currentTimeMillis());
    }
}

// AppendMessageResult, PutMessageResult, PutMessageStatus, MessageExtBrokerInner
// moved to com.zifang.z.mq.store.*

// MessageStoreConfig moved to its own file: com.zifang.z.mq.store.MessageStoreConfig
// FlushDiskType moved to its own file: com.zifang.z.mq.store.log.FlushDiskType

// MessageExtBrokerInner moved to com.zifang.z.mq.store.MessageExtBrokerInner (public class).

/**
 * 刷盘服务基类
 */
abstract class FlushCommitLogService extends ServiceThread {
    protected static final int RETRY_TIMES_OVER = 3;
}

/**
 * 同步刷盘服务 - GroupCommit
 */
class GroupCommitService extends FlushCommitLogService {
    // 待提交请求队列
    private final java.util.concurrent.LinkedBlockingQueue<GroupCommitRequest> requestQueue =
            new java.util.concurrent.LinkedBlockingQueue<>();

    public void putRequest(GroupCommitRequest request) {
        requestQueue.offer(request);
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                GroupCommitRequest request = requestQueue.take();
                // 执行刷盘
                doCommit(request);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void doCommit(GroupCommitRequest request) {
        // 模拟刷盘完成
        request.wakeupCustomer();
    }

    @Override
    public String getServiceName() {
        return "GroupCommitService";
    }
}

/**
 * 异步刷盘服务
 */
class FlushRealTimeService extends FlushCommitLogService {
    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                Thread.sleep(1000); // 1秒刷盘一次
                // 执行刷盘
                doFlush();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void doFlush() {
        // 异步刷盘实现
        // logger not accessible here - moved logging to commit level
    }

    @Override
    public String getServiceName() {
        return "FlushRealTimeService";
    }
}

/**
 * GroupCommit请求
 */
class GroupCommitRequest {
    private final long nextOffset;
    private final java.util.concurrent.CountDownLatch countDownLatch = new java.util.concurrent.CountDownLatch(1);
    private volatile boolean flushOK = false;

    public GroupCommitRequest(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void wakeupCustomer() {
        this.flushOK = true;
        this.countDownLatch.countDown();
    }

    public boolean waitForFlush(long timeoutMillis) {
        try {
            this.countDownLatch.await(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            return this.flushOK;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}

// PutMessageResult moved to com.zifang.z.mq.store.PutMessageResult

/**
 * 工具类
 */
class UtilAll {
    public static int crc32(byte[] data) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}

/**
 * 服务线程基类
 */
abstract class ServiceThread implements Runnable {
    protected volatile boolean stopped = false;
    protected Thread thread;

    public void start() {
        this.thread = new Thread(this, getServiceName());
        this.thread.start();
    }

    public void shutdown() {
        this.stopped = true;
        if (this.thread != null) {
            this.thread.interrupt();
        }
    }

    public boolean isStopped() {
        return stopped;
    }

    public abstract String getServiceName();
}
