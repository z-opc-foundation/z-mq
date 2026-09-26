package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.broker.BrokerController;
import com.zifang.z.mq.broker.longpoll.PullRequestHoldService;
import com.zifang.z.mq.common.filter.FilterType;
import com.zifang.z.mq.common.filter.MessageFilter;
import com.zifang.z.mq.common.filter.Sql92Filter;
import com.zifang.z.mq.common.filter.TagFilter;
import com.zifang.z.mq.common.message.MessageExt;
import com.zifang.z.mq.common.protocol.PullResultPayload;
import com.zifang.z.mq.common.util.JsonCodec;
import com.zifang.z.mq.remoting.netty.NettyRemotingAbstract;
import com.zifang.z.mq.remoting.netty.RemotingSysResponseCode;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import com.zifang.z.mq.remoting.protocol.RequestCode;
import com.zifang.z.mq.store.config.ConsumerOffsetManager;
import com.zifang.z.mq.store.log.CommitLog;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Broker 端"拉取消息"处理器（对标 RocketMQ PullMessageProcessor）.
 * <p>
 * 当前实现通过 CommitLog.getQueueIndex() 的进程内 InMemoryQueueIndex 查询真实写入的消息,
 * 保证 nextOffset 单调递增和消息内容一致性。
 * <p>
 * 支持 Broker 端消息过滤：
 * <ul>
 *   <li>Tag 标签过滤 — 基于消息 Tag 精确匹配</li>
 *   <li>SQL92 属性过滤 — 基于消息属性的 SQL92 表达式求值</li>
 * </ul>
 */
public class PullMessageProcessor implements NettyRemotingAbstract.NettyRequestProcessor {

    private static final Logger log = LogManager.getLogger(PullMessageProcessor.class);

    /**
     * Pull 请求里的"挂起预算"字段名 (毫秒).
     * <p>
     * 缺省或 <=0 表示"这次请求不做长轮询" —— 处理器按接线前的路径逐字返回。
     * 之所以要请求自带这个数, 而不是用服务级的 15s 默认值: 见本类
     * {@link #suspendAndReread} 说明（挂起占的是 pull 线程, 而客户端 RPC 超时是另一个数）。
     */
    public static final String EXT_SUSPEND_TIMEOUT_MILLIS = "suspendTimeoutMillis";

    /**
     * 单次挂起时长的硬上界 (毫秒): 挂起会占住 pull 业务线程, 不能让一个恶意/写错的请求
     * 把 16 个线程永久占光。客户端算 RPC 超时时用的是自己给的预算 (不减这个上界),
     * 所以"被上界截断"只会让 broker 醒得更早, 不会让客户端等不到响应。
     */
    public static final long MAX_SUSPEND_BUDGET_MILLIS = 30_000L;

    /** 挂起表满 (suspendPull 返回 null) 时写进响应 remark 的话术 —— 必须是显式响应, 不许 NPE. */
    static final String REMARK_SUSPEND_TABLE_FULL = "suspend table full, returned current offset";

    /**
     * 请求里"消费组"的字段名 —— 线上契约，client 侧的同名字面量在
     * {@code ConsumerOffsetRequests.EXT_CONSUMER_GROUP}（两侧由 ConsumerOffsetWiringGuardTest 钉住同一个值）。
     * <p>
     * 为什么 pull 请求必须带它：位点是按 {@code (topic, queueId, group)} 三元组存的
     * （{@code ConsumerOffsetManager#makeKey}），请求里没有 group 就没有 key ⇒
     * "没带 offset 就按已提交位点起读"这件事结构上做不到，只能退回 0。
     * 提交侧（{@link ConsumerOffsetProcessor}）用的是同一个字段名。
     */
    public static final String EXT_CONSUMER_GROUP = "consumerGroup";

    /**
     * 响应里回写"这次 pull 是被什么放开的"的字段名。
     * <p>
     * 为什么必须有这个字段: 挂起到期后也要重读一次再返回,
     * 于是"消息到了被叫醒"和"挂到超时才醒、顺便读到那条消息"在<b>消息内容上无法区分</b> ——
     * 只断言"pull 拿到了那条消息"的用例, 把到达侧唤醒整个摘掉也照样绿, 量不到唤醒路径。
     * 这个字段把"因何而醒"变成可断言的事实, 取值来自 {@code SuspendedPull#awaitWakeup()} 的返回值
     * (true 只可能由 {@code notifyMessageArrived → wakeupByMessage} 造成)。
     */
    public static final String EXT_SUSPEND_WAKEUP = "suspendWakeup";

    /** 真被"消息到达"叫醒 (到达侧唤醒兑现). */
    public static final String WAKEUP_BY_MESSAGE = "message";
    /** 登记挂起之后、进入等待之前就已经读到消息 (丢唤醒窗口的自愈路径, 自己补发了一次 notify). */
    public static final String WAKEUP_ARRIVED_WHILE_SUSPENDING = "arrived-while-suspending";
    /** 挂起到点由超时扫描放开, 没有任何到达通知. */
    public static final String WAKEUP_BY_TIMEOUT = "timeout";
    /** pull 线程等待期间被中断. */
    public static final String WAKEUP_BY_INTERRUPT = "interrupted";
    /** 挂起表满, 直接按当前位点返回. */
    public static final String WAKEUP_TABLE_FULL = "table-full";
    /** hold 服务没在跑 (没 start / 已 shutdown), 不允许挂起. */
    public static final String WAKEUP_HOLD_NOT_STARTED = "hold-service-not-started";

    private final BrokerController brokerController;

    public PullMessageProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setOpaque(request.getOpaque());

        String topic = request.getExtField("topic");
        String queueIdStr = request.getExtField("queueId");
        String offsetStr = request.getExtField("offset");
        String maxNStr = request.getExtField("maxNum");

        // 读取过滤参数（客户端可选发送）
        String filterTypeStr = request.getExtField("filterType");
        String filterExpression = request.getExtField("filterExpression");

        if (topic == null || queueIdStr == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("topic or queueId missing");
            return response;
        }

        int queueId = Integer.parseInt(queueIdStr);
        long offset = resolveStartOffset(request, topic, queueId, offsetStr);
        int maxNum = maxNStr == null ? 32 : Integer.parseInt(maxNStr);
        // 挂起预算: 请求没带就是短轮询, 后面的分支一律不进入
        long suspendBudgetMillis = parseSuspendBudgetMillis(request.getExtField(EXT_SUSPEND_TIMEOUT_MILLIS));

        CommitLog commitLog = brokerController.getCommitLog();
        if (commitLog == null) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark("commitLog not initialized");
            return response;
        }

        // 读取消息（比请求数量多读一些，用于过滤后仍有足够消息）
        MessageFilter filter = createFilter(filterTypeStr, filterExpression);
        List<MessageExt> messages = readAndFilter(commitLog, topic, queueId, offset, maxNum, filter);

        // 长轮询: 只有"这个 offset 确实读不到消息"且"请求带了挂起预算"时才挂起
        // (PullRequestHoldService#suspendPull 的 javadoc 要求调用者先确认这一点)
        if (messages.isEmpty() && suspendBudgetMillis > 0) {
            messages = suspendAndReread(commitLog, topic, queueId, offset, maxNum, filter,
                    suspendBudgetMillis, response);
        }

        return fillPullResponse(response, commitLog, topic, queueId, offset, messages);
    }

    /**
     * 挂起当前 pull 线程等"该 (topic, queueId) 有新消息", 醒来后重读一次。
     * <p>
     * <b>为什么是"占着线程等"而不是"先返回 null、消息到了再往 ctx 写"</b>:
     * {@code NettyRemotingAbstract#processRequestCommand} 在处理器返回 null 时会立刻代写一个
     * SUCCESS 空响应，同一 opaque 的第二个响应进来时 responseTable 里的
     * 条目早被第一个摘掉 ⇒ 客户端永远看不见。改那个契约属于 W3, 本支不动。
     * <p>
     * <b>代价与上界</b>: 本方法跑在 {@code pullMessageExecutor} 上, 池子大小
     * {@code BrokerConfig.pullMessageThreadPoolNums}=16 ⇒ 同时挂起的 pull 上限就是 16,
     * 第 17 个在池子里排队。这是可接受的第一步 (本仓不要求实现 Netty 全异步写回)。
     * 每次挂起的时长来自请求预算 (不是 15s 默认值), 再被 {@link #MAX_SUSPEND_BUDGET_MILLIS} 截一刀。
     *
     * @return 挂起/超时并重读之后的消息列表 (可能仍为空)
     */
    private List<MessageExt> suspendAndReread(CommitLog commitLog, String topic, int queueId, long offset,
                                              int maxNum, MessageFilter filter, long suspendBudgetMillis,
                                              RemotingCommand response) {
        PullRequestHoldService holdService = brokerController.getPullRequestHoldService();
        if (holdService == null || !holdService.isStarted()) {
            // 挂起的"到点必醒"由 hold 服务的扫描线程兑现; 它没在跑就不能挂, 否则这条 pull 线程
            // 再也没有回来的一天。退化为按当前 offset 直接返回 (与不带预算时同一形状)。
            response.addExtField(EXT_SUSPEND_WAKEUP, WAKEUP_HOLD_NOT_STARTED);
            log.warn("pull long-polling requested (budget={}ms) but PullRequestHoldService is not started, "
                    + "respond immediately: topic={} queueId={} offset={}", suspendBudgetMillis, topic, queueId, offset);
            return java.util.Collections.emptyList();
        }

        long holdMillis = Math.min(suspendBudgetMillis, MAX_SUSPEND_BUDGET_MILLIS);
        PullRequestHoldService.SuspendedPull suspended = holdService.suspendPull(topic, queueId, offset, holdMillis);
        if (suspended == null) {
            // maxHoldCount 满 → 显式响应 (成功码 + 当前位点的空结果 + remark), 不许 NPE
            response.setRemark(REMARK_SUSPEND_TABLE_FULL);
            response.addExtField(EXT_SUSPEND_WAKEUP, WAKEUP_TABLE_FULL);
            log.warn("suspend rejected (hold table full), respond immediately: topic={} queueId={} offset={}",
                    topic, queueId, offset);
            return java.util.Collections.emptyList();
        }

        // 补一次读: 关掉"我第一次读 → 登记挂起"这段窗口里消息已经写进来的丢唤醒可能。
        // 看到了消息就自己 notifyMessageArrived —— 那既是"消息确实到了"的事实, 也顺手把自己和别人
        // 在该队列上的登记摘掉 (notify 是唯一能把 holdTable 条目整个清掉的外部入口;
        // 直接 wakeupByTimeout 会留下 released=true 的僵尸条目, 扫描线程再也摘不掉它)。
        List<MessageExt> reread = readAndFilter(commitLog, topic, queueId, offset, maxNum, filter);
        if (!reread.isEmpty()) {
            holdService.notifyMessageArrived(topic, queueId);
            response.addExtField(EXT_SUSPEND_WAKEUP, WAKEUP_ARRIVED_WHILE_SUSPENDING);
            return reread;
        }

        boolean wokenByMessage;
        try {
            wokenByMessage = suspended.awaitWakeup();
        } catch (InterruptedException e) {
            // 线程被中断 (broker 关闭 / 池子 shutdownNow): 立刻按空结果返回。
            // 登记的条目留给扫描线程按到期时间摘, 不额外唤醒别人。
            Thread.currentThread().interrupt();
            response.addExtField(EXT_SUSPEND_WAKEUP, WAKEUP_BY_INTERRUPT);
            log.warn("await wakeup interrupted, respond with current offset: topic={} queueId={} offset={}",
                    topic, queueId, offset);
            return reread;
        }

        response.addExtField(EXT_SUSPEND_WAKEUP, wokenByMessage ? WAKEUP_BY_MESSAGE : WAKEUP_BY_TIMEOUT);
        // 无论被消息叫醒还是到点自己醒, 都只重读一次再返回
        List<MessageExt> afterWakeup = readAndFilter(commitLog, topic, queueId, offset, maxNum, filter);
        if (log.isDebugEnabled()) {
            log.debug("long-polling pull woke up: topic={} queueId={} offset={} byMessage={} got={} holdMillis={}",
                    topic, queueId, offset, wokenByMessage, afterWakeup.size(), holdMillis);
        }
        return afterWakeup;
    }

    /**
     * 按 offset 读消息并按需过滤/截断 (与接线前同一套算法, 只是被抽出来供"重读"复用).
     */
    private List<MessageExt> readAndFilter(CommitLog commitLog, String topic, int queueId, long offset,
                                           int maxNum, MessageFilter filter) {
        int fetchNum = filter != null ? maxNum * 3 : maxNum;
        List<MessageExt> messages = readMessages(commitLog, topic, queueId, offset, fetchNum);
        if (filter != null) {
            return applyFilter(messages, filter, maxNum);
        }
        // 无过滤器时截取到请求数量
        if (messages.size() > maxNum) {
            messages = messages.subList(0, maxNum);
        }
        return messages;
    }

    /**
     * 把一次 pull 的读数装进响应 (nextOffset/minOffset/maxOffset 的算法与接线前逐字一致).
     */
    private RemotingCommand fillPullResponse(RemotingCommand response, CommitLog commitLog, String topic,
                                             int queueId, long offset, List<MessageExt> messages) {
        long maxOffset = commitLog.getQueueIndex().getMaxOffset(topic, queueId);
        long minOffset = messages.isEmpty() ? maxOffset : messages.get(0).getQueueOffset();
        // nextOffset 应为最后一条消息的 offset +1, 而非 maxOffset
        long nextOffset = messages.isEmpty() ? offset : messages.get(messages.size() - 1).getQueueOffset() + 1;
        PullResultPayload body = new PullResultPayload(topic, queueId, nextOffset, minOffset, maxOffset, messages);
        response.setBody(JsonCodec.encode(body));
        return response;
    }

    /**
     * 解析这次 pull 的起始位点。
     * <ul>
     *   <li><b>请求带了 offset</b> ⇒ 以请求为准（显式优先，与接线前逐字一致）。</li>
     *   <li><b>请求没带 offset</b> ⇒ 问 {@code ConsumerOffsetManager.queryOffset(topic, queueId, group)}，
     *       也就是"跨重启恢复"这条广告真正兑现的那一步。</li>
     * </ul>
     * 回落到 0 只剩两种情形，两种都是<b>结构上不可能按组恢复</b>而不是偷懒：
     * ①请求没带消费组（三元组 key 缺一半）；②该组从没提交过位点（{@code queryOffset} 约定返回 -1）。
     * 都不许抛，否则老的、不带 group 的请求会从今天起全部失败。
     */
    private long resolveStartOffset(RemotingCommand request, String topic, int queueId, String offsetStr) {
        if (offsetStr != null && !offsetStr.trim().isEmpty()) {
            return Long.parseLong(offsetStr.trim());
        }
        String group = request.getExtField(EXT_CONSUMER_GROUP);
        if (group == null || group.trim().isEmpty()) {
            return 0L;
        }
        ConsumerOffsetManager offsetManager = brokerController.getConsumerOffsetManager();
        if (offsetManager == null) {
            return 0L;
        }
        long committed = offsetManager.queryOffset(topic, queueId, group.trim());
        if (committed < 0) {
            // 该组在这个队列上没有提交记录: 与接线前同一形状 (从 0 开始)
            return 0L;
        }
        if (log.isDebugEnabled()) {
            log.debug("pull offset resolved from committed position: topic={} queueId={} group={} offset={}",
                    topic, queueId, group, committed);
        }
        return committed;
    }

    /**
     * 解析挂起预算: null/空 → 0 (不挂起); 其余按毫秒解析, 非法值与 maxNum 等字段一样直接抛出。
     */
    private static long parseSuspendBudgetMillis(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return 0L;
        }
        return Long.parseLong(raw.trim());
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 根据过滤类型和表达式创建消息过滤器。
     *
     * @param filterTypeStr   过滤类型字符串（TAG / SQL92）
     * @param filterExpression 过滤表达式
     * @return 消息过滤器，null 表示不过滤
     */
    private MessageFilter createFilter(String filterTypeStr, String filterExpression) {
        if (filterExpression == null || filterExpression.isEmpty()) {
            return null;
        }

        FilterType filterType = FilterType.fromString(filterTypeStr);

        switch (filterType) {
            case TAG:
                return new TagFilter(filterExpression);
            case SQL92:
                return new Sql92Filter(filterExpression);
            default:
                return null;
        }
    }

    /**
     * 对消息列表应用过滤器，返回匹配的消息。
     *
     * @param messages 原始消息列表
     * @param filter   过滤器
     * @param maxNum   最大返回数量
     * @return 过滤后的消息列表
     */
    private List<MessageExt> applyFilter(List<MessageExt> messages, MessageFilter filter, int maxNum) {
        List<MessageExt> filtered = new ArrayList<>();
        for (MessageExt msg : messages) {
            if (filtered.size() >= maxNum) {
                break;
            }
            try {
                if (filter.match(msg)) {
                    filtered.add(msg);
                }
            } catch (Exception e) {
                // 过滤异常时跳过该消息（与 RocketMQ 行为一致）
                log.debug("Filter match failed for msg {}: {}", msg.getMsgId(), e.getMessage());
            }
        }
        return filtered;
    }

    /**
     * 读取消息：索引只给位点，消息内容由 CommitLog 从 CommitLog 盘上读回来。
     * <p>
     * nextOffset 由 InMemoryQueueIndex 保证单调递增, 内容来自 CommitLog 记录的字节（必经盘）。
     */
    private List<MessageExt> readMessages(CommitLog commitLog, String topic, int queueId, long offset, int maxNum) {
        if (commitLog.getQueueIndex() == null) {
            return java.util.Collections.emptyList();
        }
        return commitLog.pullMessage(topic, queueId, offset, maxNum);
    }
}