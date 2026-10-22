package com.zifang.z.mq.remoting.netty;

/**
 * Netty 服务端配置
 */
public class NettyServerConfig {

    /**
     * 监听地址的默认值：<b>回环</b>。
     * <p>
     * z-mq 的 Netty 面走私有二进制协议，<b>协议层没有认证</b> ⇒ 监听在通配地址
     * （0.0.0.0 / ::）等于把"可读写"的能力开放给整个局域网。所以默认必须是回环，
     * 需要对外暴露的部署形态（容器化、要被 Service/NodePort 访问的）必须
     * <b>显式声明</b> bind host，不能靠默认值。
     */
    public static final String DEFAULT_HOST = "127.0.0.1";

    /** 显式声明 bind host 的系统属性名（与 Spring 侧 {@code z.mq.server.host} 同名）。 */
    public static final String HOST_SYSTEM_PROPERTY = "z.mq.server.host";

    /** 显式声明 bind host 的环境变量名（容器化部署用；Dockerfile / k8s env 注入）。 */
    public static final String HOST_ENV = "ZMQ_SERVER_HOST";

    // 监听端口
    private int listenPort = 8888;

    /**
     * 监听地址（bind host）。默认 {@link #DEFAULT_HOST}（回环）。
     * <p>
     * 解析顺序：系统属性 {@value #HOST_SYSTEM_PROPERTY} → 环境变量 {@value #HOST_ENV}
     * → 默认回环。取不到就是回环，<b>绝不退化成通配地址</b>。
     */
    private String host = resolveHost();

    // Boss线程数（通常1个）
    private int serverBossThreads = 1;

    // Worker线程数（默认CPU核数 * 2）
    private int serverWorkerThreads = 8;

    // Selector线程数
    private int serverSelectorThreads = 3;

    // 回调执行线程数 (默认 4，避免 0 触发 ThreadPoolExecutor IllegalArgumentException)
    private int serverCallbackExecutorThreads = 4;

    // 异步信号量值
    private int serverAsyncSemaphoreValue = 64;

    // 单向发送信号量值
    private int serverOnewaySemaphoreValue = 256;

    // Socket发送缓冲区大小
    private int serverSocketSndBufSize = 65535;

    // Socket接收缓冲区大小
    private int serverSocketRcvBufSize = 65535;

    // Socket backlog
    private int serverSocketBacklog = 1024;

    // 通道最大空闲时间（秒）
    private int serverChannelMaxIdleTimeSeconds = 120;

    // 是否使用Epoll（Linux）
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    /**
     * 监听地址（bind host）。默认回环 {@value #DEFAULT_HOST}。
     */
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = normalize(host);
    }

    /**
     * 解析生效的 bind host：系统属性 → 环境变量 → 默认回环。
     * <p>
     * 取不到就是回环，<b>绝不退化成通配地址</b>：协议层没有认证，通配 bind
     * 等于把读写能力开放给整个局域网。
     */
    private static String resolveHost() {
        String v = System.getProperty(HOST_SYSTEM_PROPERTY);
        if (isBlank(v)) {
            v = System.getenv(HOST_ENV);
        }
        return normalize(v);
    }

    /** 归一化：空白一律退回默认回环；其余去空白。 */
    private static String normalize(String v) {
        if (isBlank(v)) {
            return DEFAULT_HOST;
        }
        return v.trim();
    }

    private static boolean isBlank(String v) {
        return v == null || v.trim().isEmpty();
    }

    /** 是否为通配地址（0.0.0.0 / ::）。 */
    public boolean isWildcardHost() {
        return "0.0.0.0".equals(host) || "::".equals(host) || "::0".equals(host);
    }

    public int getServerBossThreads() {
        return serverBossThreads;
    }

    public void setServerBossThreads(int serverBossThreads) {
        this.serverBossThreads = serverBossThreads;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public int getServerSocketBacklog() {
        return serverSocketBacklog;
    }

    public void setServerSocketBacklog(int serverSocketBacklog) {
        this.serverSocketBacklog = serverSocketBacklog;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    @Override
    public String toString() {
        return "NettyServerConfig{" +
                "listenPort=" + listenPort +
                ", host=" + host +
                ", serverBossThreads=" + serverBossThreads +
                ", serverWorkerThreads=" + serverWorkerThreads +
                ", serverSelectorThreads=" + serverSelectorThreads +
                ", serverCallbackExecutorThreads=" + serverCallbackExecutorThreads +
                ", serverAsyncSemaphoreValue=" + serverAsyncSemaphoreValue +
                ", serverOnewaySemaphoreValue=" + serverOnewaySemaphoreValue +
                ", serverSocketSndBufSize=" + serverSocketSndBufSize +
                ", serverSocketRcvBufSize=" + serverSocketRcvBufSize +
                ", serverSocketBacklog=" + serverSocketBacklog +
                ", serverChannelMaxIdleTimeSeconds=" + serverChannelMaxIdleTimeSeconds +
                ", useEpollNativeSelector=" + useEpollNativeSelector +
                '}';
    }
}
