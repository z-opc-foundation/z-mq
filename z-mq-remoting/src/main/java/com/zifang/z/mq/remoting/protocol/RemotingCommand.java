package com.zifang.z.mq.remoting.protocol;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 远程通信命令
 * 协议格式：
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                      协议头 (固定长度)                          │
 * ├────────────┬────────────┬────────────┬────────────┬────────────┤
 * │ 总长度     │ 序列化类型 │ 头部长度   │ 协议类型   │ 请求码     │
 * │ (4 bytes) │ (1 byte)   │ (2 bytes)  │ (1 byte)   │ (2 bytes)  │
 * ├────────────┼────────────┼────────────┼────────────┼────────────┤
 * │ 响应码     │ 请求标识   │ 标志位     │ 备注长度   │ 备注内容   │
 * │ (2 bytes) │ (4 bytes) │ (4 bytes)  │ (2 bytes)  │ (可变)     │
 * ├────────────┼────────────┼────────────┴────────────┴────────────┤
 * │ 扩展字段长度 │ 扩展字段 (Map<String, String>)                  │
 * │ (2 bytes)   │ (可变)                                          │
 * └─────────────┴────────────────────────────────────────────────┘
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                      协议体 (可变长度)                          │
 * ├─────────────────────────────────────────────────────────────────┤
 * │ 消息体长度 (4 bytes) │ 序列化后的消息体 (可变)                  │
 * └─────────────────────────────────────────────────────────────────┘
 */
public class RemotingCommand {

    // 协议常量
    public static final int RPC_TYPE = 0; // Request
    public static final int RPC_ONEWAY = 1; // Oneway
    public static final int RPC_RESPONSE = 2; // Response

    // 序列化类型
    public static final int SERIALIZE_TYPE_JSON = 0;

    // 消息ID生成器
    private static final AtomicInteger REQUEST_ID = new AtomicInteger(0);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // 协议头字段
    private int code;                    // 请求码或响应码
    private int version = 0;            // 协议版本
    private int opaque = REQUEST_ID.getAndIncrement(); // 请求唯一标识
    private int flag = 0;               // 标志位（RPC类型等）
    private String remark;              // 备注信息
    private Map<String, String> extFields; // 扩展字段

    // 协议体
    private transient byte[] body;    // 消息体

    public RemotingCommand() {
        this.extFields = new HashMap<>();
    }

    public RemotingCommand(int code) {
        this();
        this.code = code;
    }

    /**
     * 创建请求命令
     */
    public static RemotingCommand createRequestCommand(int code) {
        return new RemotingCommand(code);
    }

    /**
     * 创建响应命令
     */
    public static RemotingCommand createResponseCommand(int code) {
        RemotingCommand cmd = new RemotingCommand(code);
        cmd.markResponseType();
        return cmd;
    }

    /**
     * 创建响应命令（带备注）
     */
    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand(code);
        cmd.markResponseType();
        cmd.setRemark(remark);
        return cmd;
    }

    /**
     * 从ByteBuffer解码：格式与 NettyDecoder 逐字节一致（见类注释）。
     * <p>
     * 读法：[int 帧长][byte 序列化类型][int 头部长度][头部][int 消息体长度][消息体]。
     * 任何字段越界都判非法并返回 null，不再拿半截 buffer 猜。
     */
    public static RemotingCommand decode(ByteBuffer byteBuffer) {
        if (byteBuffer == null || byteBuffer.remaining() < 13) {
            // 4(帧长) + 1(类型) + 4(头长) + 4(体长) 是最小合法帧
            return null;
        }

        // 读取帧长：只统计本 4 字节之后的字节数（与 LengthFieldBasedFrameDecoder 同语义）
        int frameLength = byteBuffer.getInt();
        if (frameLength < 9 || frameLength > byteBuffer.remaining()) {
            return null;
        }

        // 读取序列化类型
        byte serializeType = byteBuffer.get();

        // 读取头部长度（int，与 NettyEncoder.writeInt 对齐；老的 getShort 是第二种格式的根源）
        int headerLength = byteBuffer.getInt();
        if (headerLength < 0 || headerLength > byteBuffer.remaining()) {
            return null;
        }

        // 读取头部数据
        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        // 解析头部：decodeHeader 保证非 null（解析失败退化成空命令，字段由下面的断言用例兜底）
        RemotingCommand cmd = decodeHeader(headerData);
        cmd.setSerializeTypeCurrentRPC(serializeType);

        if (byteBuffer.remaining() < 4) {
            return null;
        }
        // 读取消息体长度
        int bodyLength = byteBuffer.getInt();
        if (bodyLength < 0 || bodyLength > byteBuffer.remaining()) {
            return null;
        }

        // 读取消息体
        if (bodyLength > 0) {
            byte[] bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
            cmd.setBody(bodyData);
        }

        return cmd;
    }

    /**
     * 解码头部
     *
     * @param headerData 头部字节数组
     * @return RemotingCommand 实例
     */
    public static RemotingCommand decodeHeader(byte[] headerData) {
        try {
            RemotingCommand cmd = objectMapper.readValue(
                    new String(headerData, StandardCharsets.UTF_8), RemotingCommand.class);
            if (cmd == null) {
                cmd = new RemotingCommand();
            }
            if (cmd.extFields == null) {
                cmd.extFields = new HashMap<>();
            }
            return cmd;
        } catch (Exception e) {
            // 解码失败时返回空对象，避免上层 NPE — 业务侧能看到 code=0/remark 缺失
            return new RemotingCommand();
        }
    }

    /**
     * 编码为ByteBuffer：产出的帧与 {@code NettyEncoder} 写出的完全一致，
     * 因此既能被 {@code NettyDecoder} 解开，也能被 {@link #decode(ByteBuffer)} 解开。
     * <p>
     * 帧布局：[int 帧长][byte 序列化类型][int 头部长度][头部][int 消息体长度][消息体]，
     * 其中帧长不含自己那 4 字节。
     */
    public ByteBuffer encode() {
        // 序列化头部
        byte[] headerData = encodeHeader();
        if (headerData == null) {
            return null;
        }
        int bodyLength = body != null ? body.length : 0;

        // 帧长 = 类型(1) + 头长字段(4) + 头(N) + 体长字段(4) + 体(M)，不含帧长字段自己
        long frameLength = 1L + 4L + headerData.length + 4L + bodyLength;
        if (frameLength > Integer.MAX_VALUE) {
            // 超出协议可表达范围（NettyDecoder 的 FRAME_MAX_LENGTH 更早就会拒）
            return null;
        }

        // 创建ByteBuffer：4 字节帧长 + 帧体
        ByteBuffer buffer = ByteBuffer.allocate(4 + (int) frameLength);

        // 写入帧长（不含本字段）
        buffer.putInt((int) frameLength);

        // 写入序列化类型
        buffer.put((byte) SERIALIZE_TYPE_JSON);

        // 写入头部长度（int，与 NettyEncoder 一致）
        buffer.putInt(headerData.length);

        // 写入头部数据
        buffer.put(headerData);

        // 写入消息体长度
        buffer.putInt(bodyLength);

        // 写入消息体
        if (bodyLength > 0) {
            buffer.put(body);
        }

        buffer.flip();
        return buffer;
    }

    /**
     * 标记为响应类型
     */
    public void markResponseType() {
        this.flag |= (1 << 1);
    }

    /**
     * 标记为单向调用
     */
    public void markOnewayRPC() {
        this.flag |= (1 << 2);
    }

    /**
     * 判断是否为响应
     */
    public boolean isResponseType() {
        return (this.flag & (1 << 1)) != 0;
    }

    /**
     * 判断是否为单向调用
     */
    public boolean isOnewayRPC() {
        return (this.flag & (1 << 2)) != 0;
    }

    /**
     * 获取协议类型（请求/响应）
     *
     * @return RemotingCommandType 类型枚举
     */
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }
        return RemotingCommandType.REQUEST_COMMAND;
    }

    /**
     * 获取序列化类型
     *
     * @return 序列化类型代码
     */
    public int getSerializeTypeCurrentRPC() {
        return SERIALIZE_TYPE_JSON;
    }

    /**
     * 设置序列化类型（占位方法）
     *
     * @param serializeTypeCurrentRPC 序列化类型
     */
    public void setSerializeTypeCurrentRPC(int serializeTypeCurrentRPC) {
        // 当前仅支持JSON序列化
    }

    /**
     * 编码头部
     *
     * @return 头部字节数组
     */
    public byte[] encodeHeader() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (Exception e) {
            return null;
        }
    }

    // ==================== Getters and Setters ====================

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public Map<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(Map<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (this.extFields == null) {
            this.extFields = new HashMap<>();
        }
        this.extFields.put(key, value);
    }

    public String getExtField(String key) {
        return this.extFields != null ? this.extFields.get(key) : null;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "code=" + code +
                ", version=" + version +
                ", opaque=" + opaque +
                ", flag=" + flag +
                ", remark='" + remark + '\'' +
                ", extFields=" + extFields +
                ", body=" + (body != null ? body.length + " bytes" : "null") +
                '}';
    }
}
