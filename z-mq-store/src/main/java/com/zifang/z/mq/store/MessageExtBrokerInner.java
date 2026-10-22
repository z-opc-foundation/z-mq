package com.zifang.z.mq.store;

import com.zifang.z.mq.common.message.MessageExt;

/**
 * Broker 内部消息（对标 RocketMQ MessageExtBrokerInner）.
 * <p>
 * 在 CommitLog 写入时由 Broker 构造，包含存储需要的元信息（storeTimestamp, commitLogOffset, bodyCRC）。
 * <p>
 * <b>这三个元信息字段本类不再自己声明</b>，一律继承 {@link MessageExt} 的同名同类型字段。
 * 原因（协议层实测缺陷，别改回去）：协议 Body 的 JSON 序列化（{@code JsonUtil}）按
 * “本类 → 父类”逐层收字段，同名字段会在 JSON 里输出<b>两遍</b>，
 * 解析端取的是<b>后一遍</b>（父类那份）。本类自己声明时，被 {@code CommitLog} /
 * {@code MessageCodec} 写过值的是本类那份，父类那份恒为默认值 0
 * ⇒ 客户端拿到的 bodyCRC / storeTimestamp / commitLogOffset 全是 0
 * （实测见 {@code MessageExtBrokerInnerJsonTest} 与
 * {@code z-mq-broker/src/test/.../RestartDurabilityTest} 的 bodyCRC 断言）。
 * <p>
 * 本类只保留父类没有的 {@code propertiesString}（Broker 侧 "k=v\n" 渲染后的原始串）。
 */
public class MessageExtBrokerInner extends MessageExt {

    private static final long serialVersionUID = 1L;

    private String propertiesString;

    public MessageExtBrokerInner() {
        super();
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }
}
