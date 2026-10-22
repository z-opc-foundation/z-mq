package com.zifang.z.mq.common.util;

import com.zifang.util.json.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 简单的 JSON 编解码工具（对标 RocketMQ JsonUtil）.
 * <p>
 * 协议层 Body 使用，JSON 序列化。
 * <p>
 * 设计原则：放在 z-mq-common，使 broker / client / nameserver 都能引用，避免
 * 协议层 POJO 反向依赖 client 模块导致的循环依赖。
 */
public final class JsonCodec {

    private JsonCodec() {
    }

    /** 顶层数组 → List&lt;Object&gt;（元素为 LinkedHashMap / 标量）。非数组时抛 IllegalArgumentException。 */
    @SuppressWarnings("unchecked")
    private static List<Object> readArray(String json) {
        Object plain = JsonUtil.toPlainValue(JsonUtil.parse(json));
        if (!(plain instanceof List)) {
            throw new IllegalArgumentException("expected a JSON array");
        }
        return (List<Object>) plain;
    }

    public static byte[] encode(Object obj) {
        try {
            return JsonUtil.toJson(obj).getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            throw new RuntimeException("JSON encode failed", e);
        }
    }

    public static <T> T decode(byte[] bytes, Class<T> clazz) {
        return fromJson(new String(bytes, StandardCharsets.UTF_8), clazz);
    }

    /**
     * 解码为 List&lt;T&gt;，避免调用方手工构造 TypeReference.
     */
    public static <T> List<T> decodeList(byte[] bytes, Class<T> elementClass) {
        try {
            List<T> result = new ArrayList<>();
            for (Object item : JsonCodec.readArray(new String(bytes, StandardCharsets.UTF_8))) {
                if (item == null) {
                    result.add(null);
                } else if (item instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> itemMap = (Map<String, Object>) item;
                    result.add(JsonUtil.fromMap(itemMap, elementClass));
                } else {
                    result.add(elementClass.cast(item));
                }
            }
            return result;
        } catch (RuntimeException e) {
            throw new RuntimeException("JSON decode list failed", e);
        }
    }

    public static String toJson(Object obj) {
        try {
            return JsonUtil.toJson(obj);
        } catch (RuntimeException e) {
            throw new RuntimeException("JSON encode failed", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null) {
            return null;
        }
        try {
            T value = JsonUtil.fromJson(json, clazz);
            // z-util 的解析器对「控制字符 / 纯垃圾字节」这类输入是宽松的：直接给 null。
            // 本工具对外的契约是「解不出来就抛」，所以这里把那种宽松收敛掉，
            // 免得协议层拿到一个静默的 null 当成正常结果。
            if (value == null && !"null".equals(json.trim())) {
                throw new IllegalArgumentException("not valid JSON: " + json);
            }
            return value;
        } catch (RuntimeException e) {
            throw new RuntimeException("JSON decode failed", e);
        }
    }
}
