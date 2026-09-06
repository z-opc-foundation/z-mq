package com.zifang.z.mq.store.log;

import com.zifang.z.mq.store.AppendMessageResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 内存映射文件
 * 提供文件的内存映射访问，支持顺序读写
 */
public class MappedFile extends ReferenceResource {

    // OS页大小 4KB
    public static final int OS_PAGE_SIZE = 1024 * 4;
    private static final Logger log = LogManager.getLogger(MappedFile.class);
    // 总映射虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 总映射文件数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    // 当前写入位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    // 提交位置（用于异步刷盘）
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    // 刷盘位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    // 文件大小
    protected int fileSize;
    // 文件起始偏移量
    protected long fileFromOffset = 0;
    // 文件通道
    private FileChannel fileChannel;
    // 内存映射缓冲区
    private MappedByteBuffer mappedByteBuffer;
    // 文件名称
    private String fileName;
    // 文件对象
    private File file;
    // 是否是队列中第一个创建的文件
    private boolean firstCreateInQueue = false;

    // 最后修改时间戳
    private long lastModifiedTimestamp = System.currentTimeMillis();

    // 存储时间戳
    private long storeTimestamp = System.currentTimeMillis();

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    /**
     * 清理MappedByteBuffer
     */
    public static void cleanMappedByteBuffer(final MappedByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }
        try {
            java.lang.reflect.Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null) {
                java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            log.warn("clean MappedByteBuffer failed", e);
        }
    }

    /**
     * 获取总映射虚拟内存
     */
    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 获取总映射文件数
     */
    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    /**
     * 初始化文件
     */
    public void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);

        // 确保目录存在
        this.file.getParentFile().mkdirs();

        // 创建RandomAccessFile
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(this.file, "rw");
            randomAccessFile.setLength(fileSize);
        } finally {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        }

        // 创建FileChannel
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

        // 创建内存映射
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

        // 更新统计信息
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
        TOTAL_MAPPED_FILES.incrementAndGet();

        log.info("MappedFile initialized: {}, size: {}", fileName, fileSize);
    }

    /**
     * 追加消息
     */
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    /**
     * 追加消息（指定偏移和长度）
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.mappedByteBuffer.put(data, offset, length);
                this.wrotePosition.addAndGet(length);
                return true;
            } catch (Exception e) {
                log.error("append message error", e);
            }
        } else {
            log.warn("append message failed, fileSize: {}, currentPos: {}, length: {}",
                    this.fileSize, currentPos, length);
        }
        return false;
    }

    /**
     * 追加消息（内部方法，供CommitLog使用）
     */
    public AppendMessageResult appendMessagesInner(final byte[] data, Object msg) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.mappedByteBuffer.put(data, 0, data.length);
                this.wrotePosition.addAndGet(data.length);
                return new AppendMessageResult(AppendMessageResult.AppendMessageStatus.PUT_OK,
                        this.fileFromOffset + currentPos, data.length, System.currentTimeMillis());
            } catch (Exception e) {
                log.error("append message error", e);
                return new AppendMessageResult(AppendMessageResult.AppendMessageStatus.UNKNOWN_ERROR,
                        this.fileFromOffset + currentPos, 0, System.currentTimeMillis());
            }
        } else {
            return new AppendMessageResult(AppendMessageResult.AppendMessageStatus.END_OF_FILE,
                    this.fileFromOffset + currentPos, 0, System.currentTimeMillis());
        }
    }

    /**
     * 获取MappedByteBuffer
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    /**
     * 获取文件大小
     */
    public int getFileSize() {
        return fileSize;
    }

    /**
     * 获取文件通道
     */
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 获取写入位置
     */
    public int getWrotePosition() {
        return wrotePosition.get();
    }

    /**
     * 设置写入位置
     */
    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 获取刷盘位置
     */
    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    /**
     * 设置刷盘位置
     */
    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    /**
     * 获取提交位置
     */
    public int getCommittedPosition() {
        return this.committedPosition.get();
    }

    /**
     * 设置提交位置
     */
    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 获取文件名称
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * 获取MappedByteBuffer
     */
    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    /**
     * 获取当前可读位置
     */
    public int getReadPosition() {
        return this.wrotePosition.get();
    }

    /**
     * 刷盘
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();
                try {
                    // 强制刷盘
                    this.mappedByteBuffer.force();
                } catch (Exception e) {
                    log.error("force flush error", e);
                }
                this.flushedPosition.set(value);
                this.release();
            } else {
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 是否可以刷盘
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 是否写满
     */
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 选择映射缓冲区
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if (pos + size <= readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }
        return null;
    }

    /**
     * 选择映射缓冲区（从指定位置到末尾）
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }
        return null;
    }

    /**
     * 获取文件起始偏移量
     */
    public long getFileFromOffset() {
        return fileFromOffset;
    }

    /**
     * 设置文件起始偏移量
     */
    public void setFileFromOffset(long fileFromOffset) {
        this.fileFromOffset = fileFromOffset;
    }

    /**
     * 销毁文件
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[{}] " + (result ? "OK, " : "Failed, ") + "cost={}ms",
                        this.fileName, System.currentTimeMillis() - beginTime);
            } catch (Exception e) {
                log.warn("destroy file " + this.fileName + " failed. ", e);
            }
            return true;
        } else {
            log.warn("destroy mapped file[{}]failed, cleanupOver:{}.",
                    fileName, isCleanupOver());
            return false;
        }
    }

    @Override
    public boolean cleanup(long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[{}] have not shutdown, ignore cleanup.", this.fileName);
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[{}] have cleanup, do not do it again.", this.fileName);
            return true;
        }

        cleanMappedByteBuffer(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("cleanup file[{}]  OK, cost: {} ms", this.fileName, 0);
        return true;
    }

    /**
     * 获取引用计数
     */
    public long getRefCount() {
        return this.refCount.get();
    }

    /**
     * 是否是队列中第一个创建的文件
     */
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    /**
     * 设置是否是队列中第一个创建的文件
     */
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 获取最后修改时间戳
     */
    public long getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    /**
     * 获取存储时间戳
     */
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    /**
     * 设置存储时间戳
     */
    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    /**
     * 预热文件
     */
    public void warmUp() {
        // 简单实现：顺序写入每个页的第一个字节
        int pageSize = OS_PAGE_SIZE;
        for (int i = 0; i < this.fileSize; i += pageSize) {
            this.mappedByteBuffer.put(i, (byte) 0);
        }
        this.wrotePosition.set(0);
    }
}

/**
 * 选择映射缓冲区结果
 */
class SelectMappedBufferResult {
    private final long startOffset;
    private final ByteBuffer byteBuffer;
    private final int size;
    private final MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getSize() {
        return size;
    }

    public MappedFile getMappedFile() {
        return mappedFile;
    }

    /**
     * 释放资源
     */
    public void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
        }
    }
}
