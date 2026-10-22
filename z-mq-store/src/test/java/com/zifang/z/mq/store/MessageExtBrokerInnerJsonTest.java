package com.zifang.z.mq.store;

import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.util.JsonCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 协议层守卫：{@link MessageExtBrokerInner} 走 JSON（协议 Body 的编解码入口是
 * {@link JsonCodec}）往返之后，<b>bodyCRC / storeTimestamp / commitLogOffset 三个字段不许丢</b>。
 * <p>
 * 这条守卫钉的是一处真实缺陷：{@code MessageExtBrokerInner} 曾经<b>重复声明</b>了
 * {@code MessageExt} 里已经有的三个同名字段（bodyCRC / storeTimestamp / commitLogOffset），
 * 于是同一个对象上存在两份同名字段：子类那份被 {@code CommitLog} / {@code MessageCodec} 写过值，
 * 父类那份始终是默认值 0。协议用的 JSON 序列化器（z-util 的 {@code JsonUtil}）按
 * “类 + 父类”逐层收字段，同名键会输出两遍，解析端取到的是<b>父类那份的 0</b>
 * ⇒ 客户端拿到的 bodyCRC 恒为 0，重启后逐条校验 bodyCRC 的用例直接红。
 * <p>
 * 反证：把 {@code MessageExtBrokerInner} 里这三个字段删掉（改为继承父类那一份），
 * 本用例转绿；把它们放回去（并只写子类那一份），本用例转红。
 */
public class MessageExtBrokerInnerJsonTest {

    @Test
    public void jsonRoundTripKeepsBodyCrcStoreTimestampAndCommitLogOffset() {
        byte[] body = "hello-zmq".getBytes(StandardCharsets.UTF_8);
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setMsgId("MSG_ID_1");
        inner.setTopic("JsonGuardTopic");
        inner.setQueueId(2);
        inner.setBody(body);
        inner.setPropertiesString("K=V\n");

        final int expectedCrc = crc32(body);
        inner.setBodyCRC(expectedCrc);
        inner.setStoreTimestamp(1700000000123L);
        inner.setCommitLogOffset(4096L);

        String json = JsonCodec.toJson(inner);
        // 同名字段如果还在，JSON 里就会出现两遍同名的键 —— 这本身就是缺陷的可读指纹
        assertFalse(json.contains("\"bodyCRC\":0,"),
                "JSON 里不该出现父类那份没算过的 bodyCRC=0，实际=" + json);
        assertEquals(1, countOccurrences(json, "\"bodyCRC\""),
                "bodyCRC 在 JSON 里必须只出现一次，实际=" + json);
        assertEquals(1, countOccurrences(json, "\"storeTimestamp\""),
                "storeTimestamp 在 JSON 里必须只出现一次，实际=" + json);
        assertEquals(1, countOccurrences(json, "\"commitLogOffset\""),
                "commitLogOffset 在 JSON 里必须只出现一次，实际=" + json);

        MessageExt back = JsonCodec.fromJson(json, MessageExt.class);
        assertEquals(expectedCrc, back.getBodyCRC(), "bodyCRC 往返后必须保持");
        assertTrue(back.getBodyCRC() != 0, "bodyCRC 不许是默认值 0");
        assertEquals(1700000000123L, back.getStoreTimestamp(), "storeTimestamp 往返后必须保持");
        assertEquals(4096L, back.getCommitLogOffset(), "commitLogOffset 往返后必须保持");
        assertEquals("JsonGuardTopic", back.getTopic());
    }

    private static int crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data, 0, data.length);
        return (int) crc.getValue();
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        int idx = text.indexOf(token);
        while (idx >= 0) {
            count++;
            idx = text.indexOf(token, idx + token.length());
        }
        return count;
    }
}
