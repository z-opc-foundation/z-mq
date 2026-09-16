package com.zifang.z.mq.store.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MappedFile 单元测试
 */
public class MappedFileTest {

    @TempDir
    Path tempDir;

    private File testFile;
    private MappedFile mappedFile;

    @BeforeEach
    public void setUp() throws Exception {
        testFile = tempDir.resolve("test_mapped_file.dat").toFile();
    }

    @AfterEach
    public void tearDown() {
        if (mappedFile != null) {
            mappedFile.destroy(1000);
        }
    }

    @Test
    public void testConstructorWithFile() throws Exception {
        int fileSize = 1024 * 1024; // 1MB

        mappedFile = new MappedFile(testFile.getAbsolutePath(), fileSize);

        assertNotNull(mappedFile);
        assertEquals(fileSize, mappedFile.getFileSize());
        assertTrue(testFile.exists());
    }

    @Test
    public void testGetMappedByteBuffer() throws Exception {
        int fileSize = 1024 * 1024;
        mappedFile = new MappedFile(testFile.getAbsolutePath(), fileSize);

        MappedByteBuffer buffer = mappedFile.getMappedByteBuffer();

        assertNotNull(buffer);
    }

    @Test
    public void testFlush() throws Exception {
        int fileSize = 1024 * 1024;
        mappedFile = new MappedFile(testFile.getAbsolutePath(), fileSize);

        // Write some data
        MappedByteBuffer buffer = mappedFile.getMappedByteBuffer();
        byte[] data = "Test data for flush".getBytes(StandardCharsets.UTF_8);
        buffer.put(data);

        // Flush the data
        int flushed = mappedFile.flush(0);

        // Flush returns flushed position
        assertTrue(flushed >= 0);
    }

    @Test
    public void testIsAvailable() throws Exception {
        int fileSize = 1024 * 1024;
        mappedFile = new MappedFile(testFile.getAbsolutePath(), fileSize);

        assertTrue(mappedFile.isAvailable());
    }

    @Test
    public void testGetFileName() throws Exception {
        int fileSize = 1024 * 1024;
        mappedFile = new MappedFile(testFile.getAbsolutePath(), fileSize);

        assertNotNull(mappedFile.getFileName());
        assertTrue(mappedFile.getFileName().contains("test_mapped_file"));
    }
}
