package com.zifang.z.mq.common;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Broker 注册结果（对标 RocketMQ RegisterBrokerResult）.
 * <p>
 * Broker 向 NameServer 注册后，NameServer 把这个对象放在响应 Body 里返回。
 */
public class RegisterBrokerResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String haServerAddr;
    private String masterAddr;
    private Set<String> changedTopicSet = new HashSet<>();

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    public Set<String> getChangedTopicSet() {
        return changedTopicSet;
    }

    public void setChangedTopicSet(Set<String> changedTopicSet) {
        this.changedTopicSet = changedTopicSet;
    }

    @Override
    public String toString() {
        return "RegisterBrokerResult{" +
                "haServerAddr='" + haServerAddr + '\'' +
                ", masterAddr='" + masterAddr + '\'' +
                ", changedTopicSet=" + changedTopicSet +
                '}';
    }
}
