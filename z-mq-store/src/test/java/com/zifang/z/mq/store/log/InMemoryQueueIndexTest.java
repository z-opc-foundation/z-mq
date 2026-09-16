package com.zifang.z.mq.store.log;

import com.zifang.z.mq.common.message.MessageExt;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * InMemoryQueueIndex 单元测试 — 验证 (topic, queueId) 索引的单调递增和并发安全。
 */
public class InMemoryQueueIndexTest {

    private InMemoryQueueIndex index;

    @BeforeEach
    public void setUp() {
        index = new InMemoryQueueIndex();
    }

    @Test
    public void testAppendAssignsMonotonicOffsets() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 100; i++) {
            MessageExt msg = new MessageExt();
            index.append(topic, 0, msg);
            assertEquals(i, msg.getQueueOffset(),
                    "第 " + i + " 条消息 offset 应等于 " + i);
        }
        assertEquals(100, index.getSize(topic, 0));
        assertEquals(100L, index.getMaxOffset(topic, 0));
    }

    @Test
    public void testQueryReturnsMessagesInOrder() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 10; i++) {
            MessageExt msg = new MessageExt();
            msg.setBody(("msg-" + i).getBytes());
            index.append(topic, 0, msg);
        }

        List<MessageExt> all = index.query(topic, 0, 0, 100);
        assertEquals(10, all.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, all.get(i).getQueueOffset());
            assertEquals("msg-" + i, new String(all.get(i).getBody()));
        }
    }

    @Test
    public void testQueryFromOffset() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 10; i++) {
            MessageExt msg = new MessageExt();
            index.append(topic, 0, msg);
        }
        List<MessageExt> partial = index.query(topic, 0, 5, 100);
        assertEquals(5, partial.size());
        assertEquals(5L, partial.get(0).getQueueOffset());
        assertEquals(9L, partial.get(4).getQueueOffset());
    }

    @Test
    public void testQueryRespectsMaxNum() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 20; i++) {
            MessageExt msg = new MessageExt();
            index.append(topic, 0, msg);
        }
        List<MessageExt> three = index.query(topic, 0, 0, 3);
        assertEquals(3, three.size());
    }

    @Test
    public void testQueryBeyondMaxReturnsEmpty() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 5; i++) {
            MessageExt msg = new MessageExt();
            index.append(topic, 0, msg);
        }
        List<MessageExt> beyond = index.query(topic, 0, 100, 10);
        assertTrue(beyond.isEmpty());
    }

    @Test
    public void testDifferentQueuesHaveIndependentOffsets() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 5; i++) {
            index.append(topic, 0, new MessageExt());
        }
        for (int i = 0; i < 5; i++) {
            index.append(topic, 1, new MessageExt());
        }
        assertEquals(5, index.getSize(topic, 0));
        assertEquals(5, index.getSize(topic, 1));
        assertEquals(5L, index.getMaxOffset(topic, 0));
        assertEquals(5L, index.getMaxOffset(topic, 1));

        List<MessageExt> q0 = index.query(topic, 0, 0, 10);
        List<MessageExt> q1 = index.query(topic, 1, 0, 10);
        assertEquals(5, q0.size());
        assertEquals(5, q1.size());
        assertEquals(0L, q0.get(0).getQueueOffset());
        assertEquals(0L, q1.get(0).getQueueOffset());
    }

    @Test
    public void testDifferentTopicsHaveIndependentOffsets() {
        String topicA = "A" + UUID.randomUUID().toString().substring(0, 6);
        String topicB = "B" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 3; i++) {
            index.append(topicA, 0, new MessageExt());
            index.append(topicB, 0, new MessageExt());
        }
        assertEquals(3, index.getSize(topicA, 0));
        assertEquals(3, index.getSize(topicB, 0));
    }

    @Test
    public void testEmptyTopicQueryReturnsEmpty() {
        List<MessageExt> empty = index.query("nonexistent", 0, 0, 10);
        assertTrue(empty.isEmpty());
        assertEquals(0L, index.getMaxOffset("nonexistent", 0));
    }

    @Test
    public void testConcurrentAppendMonotonic() throws Exception {
        String topic = "CC" + UUID.randomUUID().toString().substring(0, 6);
        int threads = 10;
        int perThread = 100;
        int total = threads * perThread;
        CountDownLatch done = new CountDownLatch(total);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    index.append(topic, 0, new MessageExt());
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        // 所有 offset 应当从 0 到 total-1 都出现, 无重复
        assertEquals(total, index.getMaxOffset(topic, 0));
        List<MessageExt> all = index.query(topic, 0, 0, total * 2);
        assertEquals(total, all.size(), "并发写入后应能拉到全部 " + total + " 条");
        long prevOffset = -1;
        for (MessageExt m : all) {
            assertTrue(m.getQueueOffset() > prevOffset,
                    "offset 应单调递增: " + prevOffset + " -> " + m.getQueueOffset());
            prevOffset = m.getQueueOffset();
        }
    }

    @Test
    public void testClear() {
        String topic = "T";
        index.append(topic, 0, new MessageExt());
        index.append(topic, 0, new MessageExt());
        assertEquals(2, index.getSize(topic, 0));
        index.clear();
        assertEquals(0, index.getSize(topic, 0));
        assertTrue(index.query(topic, 0, 0, 10).isEmpty());
    }

    @Test
    public void testQueueCount() {
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        index.append(topic, 0, new MessageExt());
        index.append(topic, 1, new MessageExt());
        index.append(topic, 2, new MessageExt());
        assertEquals(3, index.getQueueCount());
    }

    @Test
    public void testMaxPerQueueEviction() {
        InMemoryQueueIndex bounded = new InMemoryQueueIndex(3);
        String topic = "T" + UUID.randomUUID().toString().substring(0, 6);
        for (int i = 0; i < 10; i++) {
            bounded.append(topic, 0, new MessageExt());
        }
        // maxPerQueue=3 时只保留最后 3 条 (offset 7, 8, 9)
        assertEquals(10L, bounded.getMaxOffset(topic, 0));
        // size 受 maxPerQueue 限制
        assertTrue(bounded.getSize(topic, 0) <= 3);
    }
}
