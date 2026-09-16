package com.zifang.z.mq.remoting.protocol;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * 远程通信序列化接口
 * 提供对象与字节数组/JSON字符串之间的转换
 */
public abstract class RemotingSerializable {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 从字节数组解码对象
     */
    public static <T> T decode(byte[] data, Class<T> clazz) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return objectMapper.readValue(data, clazz);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 从字节数组解码对象（带泛型）
     */
    public static <T> T decode(byte[] data, TypeReference<T> typeReference) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            String json = new String(data, StandardCharsets.UTF_8);
            return objectMapper.readValue(json, typeReference);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 从字符串解码对象
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 从字符串解码对象（带泛型）
     */
    public static <T> T fromJson(String json, TypeReference<T> typeReference) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将对象转换为字节数组（静态方法）
     */
    public static byte[] encode(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将对象转换为JSON字符串（静态方法）
     */
    public static String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将对象编码为字节数组（JSON格式）
     */
    public byte[] encode() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将对象编码为JSON字符串
     */
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            return null;
        }
    }
}
