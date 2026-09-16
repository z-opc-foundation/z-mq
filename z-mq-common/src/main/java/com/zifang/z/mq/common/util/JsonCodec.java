package com.zifang.z.mq.common.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.List;

/**
 * 简单的 JSON 编解码工具（对标 RocketMQ JsonUtil）.
 * <p>
 * 协议层 Body 使用，简化 JSON 序列化。
 * <p>
 * 设计原则：放在 z-mq-common，使 broker / client / nameserver 都能引用，避免
 * 协议层 POJO 反向依赖 client 模块导致的循环依赖。
 */
public final class JsonCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonCodec() {
    }

    public static byte[] encode(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (IOException e) {
            throw new RuntimeException("JSON encode failed", e);
        }
    }

    public static <T> T decode(byte[] bytes, Class<T> clazz) {
        try {
            return MAPPER.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new RuntimeException("JSON decode failed", e);
        }
    }

    /**
     * 解码为 List&lt;T&gt;，避免调用方手工构造 TypeReference.
     */
    public static <T> List<T> decodeList(byte[] bytes, Class<T> elementClass) {
        try {
            return MAPPER.readValue(bytes,
                    MAPPER.getTypeFactory().constructCollectionType(List.class, elementClass));
        } catch (IOException e) {
            throw new RuntimeException("JSON decode list failed", e);
        }
    }

    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException("JSON encode failed", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException("JSON decode failed", e);
        }
    }
}