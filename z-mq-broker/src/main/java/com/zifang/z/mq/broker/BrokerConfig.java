package com.zifang.z.mq.broker;

/**
 * Broker 配置（对标 RocketMQ BrokerConfig）.
 * <p>
 * 字段命名与 RocketMQ 保持一致, 缺省值由系统环境变量 / 属性注入.
 * <p>
 * 该类被同包和子包 (例如 processor 包) 共享, 故设为 public.
 */
public class BrokerConfig {

    private String namesrvAddr = System.getenv("NAMESRV_ADDR");
    private String brokerName = System.getProperty("brokerName", "DEFAULT_BROKER");
    private String brokerClusterName = System.getProperty("brokerClusterName", "DEFAULT_CLUSTER");
    private int sendMessageThreadPoolNums = 16;
    private int pullMessageThreadPoolNums = 16;
    private int adminBrokerThreadPoolNums = 4;

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }
}