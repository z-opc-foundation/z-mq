package com.zifang.z.mq.common.ha;

import com.zifang.z.mq.common.TopicConfig;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TopicConfig 同步包装类（对标 RocketMQ TopicConfigSerializeWrapper）.
 * <p>
 * Master → Slave 全量同步 TopicConfig 时使用, 携带版本号.
 */
public class TopicConfigSerializeWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
    private DataVersion dataVersion = new DataVersion();

    public TopicConfigSerializeWrapper() {
    }

    public TopicConfigSerializeWrapper(Map<String, TopicConfig> table) {
        if (table != null) {
            this.topicConfigTable.putAll(table);
        }
    }

    public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}