# Z-MQ 技术实现方案

## 1. 项目工程方案

### 1.1 Maven多模块结构

```
z-mq/
├── pom.xml                          # 父POM - 统一管理依赖版本
├── z-mq-common/                     # 公共模块
│   ├── pom.xml
│   └── src/
├── z-mq-remoting/                   # 网络通信模块
│   ├── pom.xml
│   └── src/
├── z-mq-store/                      # 存储模块
│   ├── pom.xml
│   └── src/
├── z-mq-broker/                     # Broker模块
│   ├── pom.xml
│   └── src/
├── z-mq-nameserver/                 # NameServer模块
│   ├── pom.xml
│   └── src/
├── z-mq-client/                     # 客户端模块
│   ├── pom.xml
│   └── src/
├── z-mq-tools/                      # 工具模块
│   ├── pom.xml
│   └── src/
└── z-mq-example/                    # 示例模块
    ├── pom.xml
    └── src/
```

### 1.2 依赖管理方案

```xml
<!-- 父POM依赖管理 -->
<properties>
    <!-- 编译版本 -->
    <java.version>17</java.version>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>

    <!-- 依赖版本 -->
    <netty.version>4.1.108.Final</netty.version>
    <slf4j.version>2.0.12</slf4j.version>
    <logback.version>1.5.3</logback.version>
    <fastjson2.version>2.0.47</fastjson2.version>
    <junit.version>5.10.2</junit.version>
    <mockito.version>5.11.0</mockito.version>
</properties>

<dependencyManagement>
    <dependencies>
        <!-- 内部模块 -->
        <dependency>
            <groupId>com.zifang</groupId>
            <artifactId>z-mq-common</artifactId>
            <version>${project.version}</version>
        </dependency>
        <!-- ... 其他模块 -->

        <!-- 外部依赖 -->
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-all</artifactId>
            <version>${netty.version}</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>${slf4j.version}</version>
        </dependency>
        <dependency>
            <groupId>com.alibaba.fastjson2</groupId>
            <artifactId>fastjson2</artifactId>
            <version>${fastjson2.version}</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

## 2. 存储实现方案

### 2.1 CommitLog 存储方案

```java
/**
 * CommitLog 核心设计
 *
 * 设计要点：
 * 1. 顺序写入 - 磁盘顺序IO性能最优
 * 2. 内存映射 - 使用MappedByteBuffer减少用户态/内核态切换
 * 3. 文件预分配 - 提前创建文件并预热，减少运行时开销
 * 4. 刷盘策略 - 支持同步/异步刷盘，平衡可靠性和性能
 */
public class CommitLog {

    // 单个文件大小 1GB
    public static final int mappedFileSize = 1024 * 1024 * 1024;

    // 消息魔数 - 用于校验消息合法性
    public static final int MESSAGE_MAGIC_CODE = 0xAABBCCDD;

    // MappedFile队列 - 管理多个顺序文件
    private final MappedFileQueue mappedFileQueue;

    // 刷盘服务
    private final FlushCommitLogService flushCommitLogService;

    // 主从同步服务
    private final HAService haService;

    /**
     * 消息存储结构（CommitLog）
     *
     * ┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐
     * │总大小  │魔数    │CRC32   │队列ID  │标志    │ Born时间│ Born   │Store   │
     * │(4B)    │(4B)    │(4B)    │(4B)    │(4B)    │(8B)    │Host   │时间    │
     * ├────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┤
     * │Store   │消息ID  │Commit  │消息体  │主题    │属性    │...    │        │
     * │Host    │(16B)   │Log偏移 │大小(4B)│(可变)  │(可变)  │       │        │
     * │(8B)    │        │(8B)    │        │        │        │       │        │
     * └────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘
     */

    /**
     * 异步存储消息（主入口）
     *
     * 流程：
     * 1. 设置消息存储时间
     * 2. 计算消息体CRC32校验
     * 3. 获取当前MappedFile
     * 4. 追加消息到缓冲区
     * 5. 提交刷盘请求（异步）
     * 6. 提交主从同步请求（如开启）
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {

        // 设置存储时间戳
        msg.setStoreTimestamp(System.currentTimeMillis());

        // 生成消息ID
        msg.setMsgId(MessageDecoder.createMessageId(
            this.msgIdMemory,
            msg.getStoreHost(),
            msg.getStoreTimestamp()
        ));

        // 获取当前可写入的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 序列化消息
        byte[] encoded = MessageDecoder.encode(msg);

        // 追加到MappedFile
        AppendMessageResult result = mappedFile.appendMessagesInner(msg, callback);

        // 提交刷盘请求
        CompletableFuture<PutMessageStatus> flushFuture = submitFlushRequest(result, msg);

        // 提交HA请求
        CompletableFuture<PutMessageStatus> haFuture = submitReplicaRequest(result, msg);

        // 组合结果
        return CompletableFuture.allOf(flushFuture, haFuture)
            .thenApply(v -> new PutMessageResult(PutMessageStatus.PUT_OK, result));
    }
}
```

### 2.2 ConsumeQueue 存储方案

```java
/**
 * ConsumeQueue 设计
 *
 * 设计要点：
 * 1. 定长条目 - 每个条目固定20字节，支持快速随机访问
 * 2. 顺序索引 - 记录消息在CommitLog的位置和大小
 * 3. 内存映射 - 同样使用MappedFile管理
 * 4. 异步构建 - 由ReputService异步从CommitLog构建
 *
 * 文件路径: ${storePath}/consumequeue/{topic}/{queueId}/{fileName}
 */
public class ConsumeQueue {

    // 单个条目大小 20 bytes
    public static final int CQ_STORE_UNIT_SIZE = 20;

    // 每个文件的条目数 (默认 30万条，约6MB)
    public static final int mappedFileSize = 300000 * CQ_STORE_UNIT_SIZE;

    // ConsumeQueue条目结构
    // ┌────────────────┬────────────────┬────────────────┐
    // │ CommitLog偏移量 │   消息大小      │   Tag HashCode │
    // │   (8 bytes)    │   (4 bytes)    │   (8 bytes)    │
    // └────────────────┴────────────────┴────────────────┘

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final MessageStore messageStore;

    /**
     * 根据逻辑偏移量获取索引缓冲区
     *
     * @param startIndex 起始索引（条目序号）
     * @return 索引缓冲区
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        // 计算在MappedFile中的物理偏移量
        int mappedFileSize = this.mappedFileQueue.getMappedFileSize();
        long offset = startIndex * CQ_STORE_UNIT_SIZE;

        // 找到对应的MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            // 计算在MappedFile内的偏移量
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    /**
     * 追加消息索引
     *
     * @param offset CommitLog中的物理偏移量
     * @param size 消息大小
     * @param tagsCode Tag的HashCode（用于过滤）
     * @param cqOffset ConsumeQueue中的逻辑偏移量
     * @return 是否成功
     */
    public boolean putMessagePositionInfo(final long offset, final int size,
                                          final long tagsCode, final long cqOffset) {

        // 构造20字节的索引条目
        byte[] buffer = new byte[CQ_STORE_UNIT_SIZE];

        // CommitLog偏移量 (8 bytes)
        buffer[0] = (byte) (offset & 0xFF);
        buffer[1] = (byte) ((offset >> 8) & 0xFF);
        // ... 继续填充8字节

        // 消息大小 (4 bytes)
        buffer[8] = (byte) (size & 0xFF);
        // ... 继续填充4字节

        // Tag HashCode (8 bytes)
        buffer[12] = (byte) (tagsCode & 0xFF);
        // ... 继续填充8字节

        // 追加到MappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        return mappedFile.appendMessage(buffer);
    }
}
```

### 2.3 IndexFile 索引方案

```java
/**
 * IndexFile 设计
 *
 * 用途：根据Key或时间范围快速查询消息
 *
 * 文件结构：
 * ┌─────────────┬─────────────┬─────────────┬─────────────┬─────────────┐
 * │   Header    │  Hash Slot  │  Index Item │    ...      │  End Sign   │
 * │  (40 bytes) │  (500W * 4B)│  (20 bytes) │             │  (4 bytes)  │
 * └─────────────┴─────────────┴─────────────┴─────────────┴─────────────┘
 *
 * Header结构：
 * - beginTimestamp (8B) 最小消息存储时间
 * - endTimestamp (8B) 最大消息存储时间
 * - beginPhyOffset (8B) 最小物理偏移量
 * - endPhyOffset (8B) 最大物理偏移量
 * - hashSlotCount (4B) 使用的HashSlot数量
 * - indexCount (4B) 已写入的索引条目数量
 */
public class IndexFile {

    // 哈希槽数量 500万
    private static final int hashSlotNum = 5000000;

    // 最大索引条目数
    private static final int indexNum = 5000000 * 4;

    // 文件头大小 40 bytes
    private static final int INDEX_HEADER_SIZE = 40;

    // 哈希槽大小 4 bytes
    private static final int hashSlotSize = 4;

    // 索引条目大小 20 bytes
    private static final int indexSize = 20;

    // 文件总大小
    public static final int INDEX_FILE_SIZE =
        INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);

    private final MappedFile mappedFile;
    private final ByteBuffer mappedByteBuffer;

    /**
     * 构建索引
     *
     * @param key 索引Key（如订单号）
     * @param phyOffset CommitLog物理偏移量
     * @param storeTimestamp 消息存储时间戳
     * @return 是否成功
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 计算Key的HashCode
        int keyHash = indexKeyHashMethod(key);
        // 映射到HashSlot位置
        int slotPos = keyHash % hashSlotNum;
        int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;

        // 读取当前slot值（即上次写入的index位置）
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

        // 获取当前时间戳（相对时间，单位秒）
        int timeDiff = (int) ((storeTimestamp - getBeginTimestamp()) / 1000);

        // 获取当前可写入的index位置
        int absIndexPos = INDEX_HEADER_SIZE + hashSlotNum * hashSlotSize + this.indexCount * indexSize;

        // 写入Index条目 (20 bytes)
        // 1. Key HashCode (4 bytes)
        this.mappedByteBuffer.putInt(absIndexPos, keyHash);
        // 2. CommitLog物理偏移量 (8 bytes)
        this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
        // 3. 时间差 (4 bytes)
        this.mappedByteBuffer.putInt(absIndexPos + 12, timeDiff);
        // 4. 上一个索引位置（形成链表，用于处理Hash冲突）
        this.mappedByteBuffer.putInt(absIndexPos + 16, slotValue);

        // 更新HashSlot指向最新的index位置
        this.mappedByteBuffer.putInt(absSlotPos, this.indexCount);

        // 更新Header中的index计数
        this.mappedByteBuffer.putInt(INDEX_HEADER_SIZE - 4, ++this.indexCount);

        // 更新物理偏移量范围
        if (this.indexCount == 1) {
            this.mappedByteBuffer.putLong(INDEX_HEADER_SIZE - 40, phyOffset);
            this.mappedByteBuffer.putLong(INDEX_HEADER_SIZE - 24, phyOffset);
        } else {
            this.mappedByteBuffer.putLong(INDEX_HEADER_SIZE - 24, phyOffset);
        }

        return true;
    }

    /**
     * 根据Key查询消息物理偏移量
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key,
                                final int maxNum, final long begin, final long end) {
        int keyHash = indexKeyHashMethod(key);
        int slotPos = keyHash % hashSlotNum;
        int absSlotPos = INDEX_HEADER_SIZE + slotPos * hashSlotSize;

        // 读取当前slot存储的index位置
        int slotValue = this.mappedByteBuffer.getInt(absSlotPos);

        // 遍历链表查找匹配的key
        while (slotValue != 0 && phyOffsets.size() < maxNum) {
            int absIndexPos = INDEX_HEADER_SIZE + hashSlotNum * hashSlotSize + slotValue * indexSize;

            int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
            long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
            int timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 12);
            int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 16);

            // 校验时间范围
            long storeTime = this.getBeginTimestamp() + timeDiff * 1000;
            if (storeTime >= begin && storeTime <= end) {
                // Hash匹配则加入结果
                if (keyHash == keyHashRead) {
                    phyOffsets.add(phyOffsetRead);
                }
            }

            // 继续遍历链表
            slotValue = prevIndexRead;
        }
    }
}
```

## 3. 网络通信方案

### 3.1 Netty服务端实现

```java
/**
 * Netty Remoting Server 实现
 */
public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {

    // 服务器配置
    private final NettyServerConfig nettyServerConfig;

    // Boss线程组 - 处理连接事件
    private EventLoopGroup eventLoopGroupBoss;

    // Worker线程组 - 处理IO事件
    private EventLoopGroup eventLoopGroupSelector;

    // Netty Channel
    private ServerBootstrap serverBootstrap;

    // 业务线程池 - 处理业务逻辑
    private ExecutorService publicExecutor;

    // 端口监听
    private int port = 0;

    @Override
    public void start() {
        // 创建Boss线程组（通常1个线程）
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyBoss_%d", threadIndex.incrementAndGet()));
            }
        });

        // 创建Worker线程组（默认CPU核数*2）
        this.eventLoopGroupSelector = new NioEventLoopGroup(
            this.nettyServerConfig.getServerSelectorThreads(),
            new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int threadTotal = nettyServerConfig.getServerSelectorThreads();
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerSelector_%d_%d",
                        threadTotal, threadIndex.incrementAndGet()));
                }
            }
        );

        // 创建业务线程池
        this.publicExecutor = Executors.newFixedThreadPool(
            this.nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerWorker_" + threadIndex.incrementAndGet());
                }
            }
        );

        // 配置ServerBootstrap
        this.serverBootstrap = new ServerBootstrap()
            .group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, this.nettyServerConfig.getServerSocketBacklog())
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_SNDBUF, this.nettyServerConfig.getServerSocketSndBufSize())
            .childOption(ChannelOption.SO_RCVBUF, this.nettyServerConfig.getServerSocketRcvBufSize())
            .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                        // 空闲检测
                        .addLast(new IdleStateHandler(0, 0,
                            nettyServerConfig.getServerChannelMaxIdleTimeSeconds()))
                        // 解码器
                        .addLast(new NettyDecoder())
                        // 编码器
                        .addLast(new NettyEncoder())
                        // 连接管理
                        .addLast(new NettyConnectManageHandler())
                        // 业务处理器
                        .addLast(new NettyServerHandler());
                }
            });

        try {
            // 绑定端口并启动
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
            log.info("NettyRemotingServer started on port {}", this.port);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to start NettyRemotingServer", e);
        }
    }
}
```

### 3.2 协议编解码方案

```java
/**
 * 自定义协议编解码
 *
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
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand cmd, ByteBuf out)
            throws Exception {

        // 记录起始位置用于回填总长度
        int beginIndex = out.writerIndex();

        // 1. 总长度占位（最后回填）
        out.writeBytes(LENGTH_PLACEHOLDER);

        // 2. 序列化类型
        out.writeByte(cmd.getSerializeTypeCurrentRPC());

        // 3. 扩展头长度
        byte[] headerData = cmd.encodeHeader();
        out.writeShort((short) headerData.length);

        // 4. 扩展头数据
        out.writeBytes(headerData);

        // 5. 消息体
        byte[] body = cmd.getBody();
        if (body != null) {
            out.writeInt(body.length);
            out.writeBytes(body);
        } else {
            out.writeInt(0);
        }

        // 回填总长度
        int endIndex = out.writerIndex();
        out.setInt(beginIndex, endIndex - beginIndex);
    }
}

/**
 * 解码器
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    private static final int FRAME_MAX_LENGTH = 16777216; // 16MB

    public NettyDecoder() {
        // 参数：最大帧长度、长度字段偏移、长度字段大小、长度调整、跳过字节数
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            // 解析协议
            // 1. 序列化类型
            byte serializeType = frame.readByte();

            // 2. 头部长度
            short headerLength = frame.readShort();

            // 3. 头部数据
            byte[] headerData = new byte[headerLength];
            frame.readBytes(headerData);

            // 4. 解析头部
            RemotingCommand cmd = RemotingCommand.decode(headerData);
            cmd.setSerializeTypeCurrentRPC(serializeType);

            // 5. 消息体
            int bodyLength = frame.readInt();
            if (bodyLength > 0) {
                byte[] bodyData = new byte[bodyLength];
                frame.readBytes(bodyData);
                cmd.setBody(bodyData);
            }

            return cmd;

        } catch (Exception e) {
            log.error("Decode error", e);
            throw e;
        } finally {
            if (frame != null) {
                frame.release();
            }
        }
    }
}
```

## 4. 迭代一实施方案：基础框架搭建

### 4.1 模块创建顺序

```
迭代一任务分解：

Week 1 - 第1-2天：
□ 创建父POM和基础目录结构
□ 创建 z-mq-common 模块
  - 定义Message、MessageExt基础类
  - 定义通信协议结构
  - 定义系统常量

Week 1 - 第3-4天：
□ 创建 z-mq-remoting 模块
  - Netty服务端/客户端基础实现
  - 协议编解码器
  - 心跳机制

Week 1 - 第5天：
□ 编写单元测试
□ 验证基础通信功能

Week 2 - 第1-2天：
□ 完善基础工具类
□ 添加异常处理
□ 完善日志记录

Week 2 - 第3-4天：
□ 代码审查和优化
□ 编写迭代一文档

Week 2 - 第5天：
□ 迭代一里程碑检查
□ 准备迭代二
```

### 4.2 关键类清单（迭代一）

```
z-mq-common 模块：
├── com.zifang.z.mq.common.message
│   ├── Message.java              # 消息基础类
│   ├── MessageExt.java            # 扩展消息类
│   └── MessageDecoder.java        # 消息编解码
├── com.zifang.z.mq.common.protocol
│   ├── RemotingCommand.java       # 通信协议命令
│   ├── RemotingSerializable.java  # 序列化接口
│   └── RequestCode.java           # 请求码定义
├── com.zifang.z.mq.common.constant
│   ├── MQConstants.java           # 系统常量
│   └── FlushDiskType.java         # 刷盘类型
└── com.zifang.z.mq.common.util
    ├── NetworkUtil.java             # 网络工具
    └── TimeUtils.java               # 时间工具

z-mq-remoting 模块：
├── com.zifang.z.mq.remoting.netty
│   ├── NettyRemotingServer.java   # Netty服务端
│   ├── NettyRemotingClient.java   # Netty客户端
│   ├── NettyEncoder.java          # 编码器
│   ├── NettyDecoder.java          # 解码器
│   └── NettyEvent.java            # Netty事件
├── com.zifang.z.mq.remoting.common
│   ├── RemotingHelper.java        # 通信工具
│   ├── ServiceThread.java         # 服务线程基类
│   └── Pair.java                  # 键值对
└── com.zifang.z.mq.remoting.exception
    ├── RemotingException.java     # 通信异常
    └── RemotingTimeoutException.java # 超时异常
```

## 5. 测试策略

### 5.1 单元测试规范

```java
/**
 * CommitLog单元测试示例
 */
@DisplayName("CommitLog 存储测试")
public class CommitLogTest {

    private CommitLog commitLog;
    private MessageStoreConfig config;
    private String storePath;

    @BeforeEach
    void setUp() throws IOException {
        // 创建临时目录
        storePath = Files.createTempDirectory("commitlog_test_").toString();

        config = new MessageStoreConfig();
        config.setStorePathCommitLog(storePath + "/commitlog");

        commitLog = new CommitLog(new DefaultMessageStore(config));
    }

    @AfterEach
    void tearDown() {
        commitLog.shutdown();
        // 清理临时目录
        FileUtils.deleteDirectory(new File(storePath));
    }

    @Test
    @DisplayName("正常消息存储测试")
    void testPutMessage() {
        // Given
        MessageExtBrokerInner msg = createTestMessage("TestTopic", "TestBody");

        // When
        PutMessageResult result = commitLog.putMessage(msg);

        // Then
        assertNotNull(result);
        assertEquals(PutMessageStatus.PUT_OK, result.getPutMessageStatus());
        assertTrue(result.getAppendMessageResult().getWroteBytes() > 0);
    }

    @Test
    @DisplayName("批量消息存储测试")
    void testPutMessages() {
        // Given
        int count = 100;
        List<MessageExtBrokerInner> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(createTestMessage("TestTopic", "Message" + i));
        }

        // When
        long start = System.currentTimeMillis();
        for (MessageExtBrokerInner msg : messages) {
            commitLog.putMessage(msg);
        }
        long elapsed = System.currentTimeMillis() - start;

        // Then
        double tps = (count * 1000.0) / elapsed;
        System.out.println("Batch write TPS: " + tps);
        assertTrue(tps > 1000, "TPS should be greater than 1000");
    }

    @Test
    @DisplayName("消息读取测试")
    void testGetMessage() {
        // Given - 先存储消息
        MessageExtBrokerInner msg = createTestMessage("TestTopic", "TestBody");
        PutMessageResult putResult = commitLog.putMessage(msg);
        long offset = putResult.getAppendMessageResult().getWroteOffset();
        int size = putResult.getAppendMessageResult().getWroteBytes();

        // When
        SelectMappedBufferResult result = commitLog.getMessage(offset, size);

        // Then
        assertNotNull(result);
        assertNotNull(result.getByteBuffer());

        // 解码验证
        MessageExt readMsg = MessageDecoder.decode(result.getByteBuffer());
        assertEquals("TestTopic", readMsg.getTopic());
        assertEquals("TestBody", new String(readMsg.getBody()));

        result.release();
    }

    private MessageExtBrokerInner createTestMessage(String topic, String body) {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setBody(body.getBytes(StandardCharsets.UTF_8));
        msg.setTags("TestTag");
        msg.setKeys("TestKey");
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(new InetSocketAddress("127.0.0.1", 0));
        msg.setStoreHost(new InetSocketAddress("127.0.0.1", 10911));
        return msg;
    }
}
```

## 6. 风险与应对

| 风险      | 影响 | 可能性 | 应对措施                   |
|---------|----|-----|------------------------|
| 存储性能不达标 | 高  | 中   | 提前做技术预研，使用内存映射、顺序写入等优化 |
| 网络通信稳定性 | 高  | 中   | 充分测试边界场景，完善重试、心跳机制     |
| 开发进度延期  | 中  | 高   | 设置检查点，及时调整优先级，分阶段交付    |
| 人员变动    | 中  | 低   | 文档完善，知识共享，代码审查         |
| 测试覆盖不足  | 中  | 中   | 引入自动化测试，持续集成，设定覆盖率门槛   |

---

**文档版本**: 1.0
**最后更新**: 2026-03-30
**关联文档**: [技术架构.md](./技术架构.md), [排期计划.md](./排期计划.md)
