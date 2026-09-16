package com.zifang.z.mq.store.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MappedFile队列管理
 * 管理多个MappedFile，提供顺序访问能力
 */
public class MappedFileQueue {

    private static final Logger log = LogManager.getLogger(MappedFileQueue.class);

    // 存储目录
    private final String storePath;

    // 单个文件大小
    private final int mappedFileSize;

    // MappedFile列表
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    // 已刷盘位置
    private volatile long flushedWhere = 0;

    // 已提交位置（异步刷盘时使用）
    private volatile long committedWhere = 0;

    public MappedFileQueue(final String storePath, final int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        load();
    }

    /**
     * 加载已有的MappedFile
     */
    public boolean load() {
        File dir = new File(this.storePath);
        if (!dir.exists()) {
            dir.mkdirs();
            return true;
        }

        if (!dir.isDirectory()) {
            log.error("{} is not a directory", this.storePath);
            return false;
        }

        // 获取所有文件并按名称排序
        File[] files = dir.listFiles();
        if (files != null && files.length > 0) {
            Arrays.sort(files, Comparator.comparing(File::getName));

            for (File file : files) {
                if (file.isFile()) {
                    try {
                        MappedFile mappedFile = new MappedFile(file.getAbsolutePath(), this.mappedFileSize);
                        mappedFile.setWrotePosition(this.mappedFileSize);
                        mappedFile.setFlushedPosition(this.mappedFileSize);
                        mappedFile.setCommittedPosition(this.mappedFileSize);
                        this.mappedFiles.add(mappedFile);
                        log.info("load mapped file {} success", file.getAbsolutePath());
                    } catch (IOException e) {
                        log.error("load mapped file {} failed", file.getAbsolutePath(), e);
                    }
                }
            }
        }

        return true;
    }

    /**
     * 获取最后一个MappedFile，如果不存在或已满则创建新的
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean createIfNotExists) {
        MappedFile mappedFileLast = null;

        // 获取最后一个文件
        if (!this.mappedFiles.isEmpty()) {
            mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }

        // 如果最后一个文件已满或不存在，创建新文件
        if (mappedFileLast == null || mappedFileLast.isFull()) {
            if (createIfNotExists) {
                long fileOffset;
                if (mappedFileLast == null) {
                    fileOffset = startOffset - (startOffset % this.mappedFileSize);
                } else {
                    fileOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
                }

                String fileName = this.storePath + File.separator
                        + String.format("%020d", fileOffset);

                try {
                    MappedFile mappedFile = new MappedFile(fileName, this.mappedFileSize);
                    mappedFile.setFileFromOffset(fileOffset);
                    this.mappedFiles.add(mappedFile);
                    return mappedFile;
                } catch (IOException e) {
                    log.error("create mapped file {} failed", fileName, e);
                }
            }
        }

        return mappedFileLast;
    }

    /**
     * 获取最后一个MappedFile
     */
    public MappedFile getLastMappedFile() {
        return getLastMappedFile(0, true);
    }

    /**
     * 根据偏移量查找MappedFile
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile mappedFileFirst = this.getFirstMappedFile();
            if (mappedFileFirst == null) {
                return null;
            }

            int index = (int) ((offset / this.mappedFileSize) - (mappedFileFirst.getFileFromOffset() / this.mappedFileSize));
            if (index < 0 || index >= this.mappedFiles.size()) {
                if (returnFirstOnNotFound) {
                    return mappedFileFirst;
                }
                return null;
            }

            return this.mappedFiles.get(index);
        } catch (Exception e) {
            log.error("findMappedFileByOffset exception", e);
        }
        return null;
    }

    /**
     * 获取第一个MappedFile
     */
    public MappedFile getFirstMappedFile() {
        if (this.mappedFiles.isEmpty()) {
            return null;
        }
        return this.mappedFiles.get(0);
    }

    /**
     * 获取所有MappedFile
     */
    public CopyOnWriteArrayList<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    /**
     * 删除最后一个MappedFile
     */
    public void deleteLastMappedFile() {
        if (!this.mappedFiles.isEmpty()) {
            MappedFile lastMappedFile = this.mappedFiles.remove(this.mappedFiles.size() - 1);
            lastMappedFile.destroy(1000);
            log.info("delete last mapped file {} success", lastMappedFile.getFileName());
        }
    }

    /**
     * 删除过期文件
     */
    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval,
                                       final long intervalForcibly, final boolean cleanImmediately) {
        Object[] mfs = this.mappedFiles.toArray();

        if (mfs == null || mfs.length == 0) {
            return 0;
        }

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<>();

        for (int i = 0; i < mfsLength; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
            if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                if (mappedFile.destroy(intervalForcibly)) {
                    files.add(mappedFile);
                    deleteCount++;

                    if (files.size() >= deleteFilesInterval) {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        this.mappedFiles.removeAll(files);

        return deleteCount;
    }

    /**
     * 计算数据偏移量
     */
    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty()) {
            return 0;
        }

        long flushed = this.flushedWhere;
        if (flushed == 0) {
            flushed = getMaxOffset();
        }

        return getMaxOffset() - flushed;
    }

    /**
     * 获取最大偏移量
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取最小偏移量
     */
    public long getMinOffset() {
        if (!this.mappedFiles.isEmpty()) {
            return this.mappedFiles.get(0).getFileFromOffset();
        }
        return -1;
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }

    public String getStorePath() {
        return storePath;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    /**
     * 获取MappedFile列表数量
     */
    public int getMappedFileCount() {
        return this.mappedFiles.size();
    }
}
