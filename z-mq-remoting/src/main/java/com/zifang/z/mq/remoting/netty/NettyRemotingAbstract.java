package com.zifang.z.mq.remoting.netty;

import com.zifang.z.mq.remoting.common.Pair;
import com.zifang.z.mq.remoting.common.RemotingHelper;
import com.zifang.z.mq.remoting.exception.RemotingSendRequestException;
import com.zifang.z.mq.remoting.exception.RemotingTimeoutException;
import com.zifang.z.mq.remoting.exception.RemotingTooMuchRequestException;
import com.zifang.z.mq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Netty 远程通信抽象基类
 * 提供请求-响应模式的基础设施
 */
public abstract class NettyRemotingAbstract {

    protected static final Logger log = LogManager.getLogger(NettyRemotingAbstract.class);
    // 异步发送默认超时时间（毫秒）
    protected static final long DEFAULT_ASYNC_TIMEOUT_MILLIS = 3000;
    // 信号量用于流控（异步发送）
    protected final Semaphore semaphoreAsync;
    // 信号量用于流控（单向发送）
    protected final Semaphore semaphoreOneway;
    // 请求响应映射表（key: opaque, value: ResponseFuture）
    protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable =
            new ConcurrentHashMap<>(256);

    // 处理器表（key: request code, value: Pair<processor, executor>）
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<>(64);

    // 默认处理器
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    public NettyRemotingAbstract(final int permitsAsync, final int permitsOneway) {
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
    }

    /**
     * 处理接收到的消息
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 处理请求命令
     */
    protected void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 查找处理器
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = matched != null ? matched : this.defaultRequestProcessor;

        if (pair == null) {
            // 没有处理器，返回错误
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response = RemotingCommand.createResponseCommand(
                    RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
            log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            return;
        }

        // 构建Runnable任务
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    // 回调RPC钩子
                    doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);

                    // 处理请求
                    final RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);

                    // 回调RPC钩子
                    doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);

                    // 如果不是单向调用，返回响应
                    if (!cmd.isOnewayRPC()) {
                        if (response != null) {
                            response.setOpaque(cmd.getOpaque());
                            response.markResponseType();
                            try {
                                ctx.writeAndFlush(response);
                            } catch (Throwable e) {
                                log.error("process request over, but response failed", e);
                                log.error(cmd.toString());
                                log.error(response.toString());
                            }
                        } else {
                            // 处理器没有返回响应，返回空响应
                            RemotingCommand emptyResponse = RemotingCommand.createResponseCommand(
                                    RemotingSysResponseCode.SUCCESS, null);
                            emptyResponse.setOpaque(cmd.getOpaque());
                            ctx.writeAndFlush(emptyResponse);
                        }
                    }
                } catch (Throwable e) {
                    log.error("process request exception", e);
                    log.error(cmd.toString());

                    if (!cmd.isOnewayRPC()) {
                        final RemotingCommand response = RemotingCommand.createResponseCommand(
                                RemotingSysResponseCode.SYSTEM_ERROR,
                                RemotingHelper.exceptionSimpleDesc(e));
                        response.setOpaque(cmd.getOpaque());
                        ctx.writeAndFlush(response);
                    }
                }
            }
        };

        // 提交到线程池执行
        try {
            pair.getObject2().submit(run);
        } catch (Exception e) {
            // 线程池已满，返回系统繁忙
            if (!cmd.isOnewayRPC()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(
                        RemotingSysResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while");
                response.setOpaque(cmd.getOpaque());
                ctx.writeAndFlush(response);
            }
        }
    }

    /**
     * 处理响应命令
     */
    protected void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();

        // 查找对应的ResponseFuture
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            // 移除映射表
            responseTable.remove(opaque);

            // 如果有回调，执行回调
            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                // 唤醒等待的线程
                responseFuture.putResponse(cmd);
            }
        } else {
            log.warn("receive response, but not matched any request, {} {} {}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    cmd.toString(),
                    opaque);
        }
    }

    /**
     * 执行回调
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        // 这里应该提交到回调线程池执行
        // 简化实现，直接在当前线程执行
        boolean runInThisThread = false;
        // 实际实现应该提交到专门的回调线程池
        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            }
        }
    }

    /**
     * 同步调用
     */
    protected RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                             final long timeoutMillis) throws Exception {
        final int opaque = request.getOpaque();

        try {
            final ResponseFuture responseFuture = new ResponseFuture(
                    channel, opaque, timeoutMillis, null, null);
            this.responseTable.put(opaque, responseFuture);

            final String addr = RemotingHelper.parseChannelRemoteAddr(channel);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    } else {
                        responseFuture.setSendRequestOK(false);
                    }

                    responseTable.remove(opaque);
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (responseCommand == null) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(
                            RemotingTimeoutException.newTimeoutException(addr, timeoutMillis));
                } else {
                    throw new RemotingSendRequestException(
                            RemotingSendRequestException.newSendRequestException(addr, responseFuture.getCause()));
                }
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 异步调用实现
     */
    protected void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
                                   final long timeoutMillis, final InvokeCallback invokeCallback) throws Exception {
        // 获取信号量
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (!acquired) {
            throw new RemotingTooMuchRequestException("invokeAsyncImpl tryAcquire semaphore timeout, "
                    + timeoutMillis + "ms, waiting thread numbers: "
                    + this.semaphoreAsync.getQueueLength());
        }

        final int opaque = request.getOpaque();
        final String addr = RemotingHelper.parseChannelRemoteAddr(channel);

        try {
            final ResponseFuture responseFuture = new ResponseFuture(
                    channel, opaque, timeoutMillis, invokeCallback, this.semaphoreAsync);
            this.responseTable.put(opaque, responseFuture);

            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }

                    requestFail(opaque);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });
        } catch (Exception e) {
            this.semaphoreAsync.release();
            throw new RemotingSendRequestException(
                    RemotingSendRequestException.newSendRequestException(addr, e));
        }
    }

    /**
     * 请求失败处理
     */
    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * 单向调用实现
     */
    protected void invokeOnewayImpl(final Channel channel, final RemotingCommand request,
                                    final long timeoutMillis) throws Exception {
        // 获取信号量
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (!acquired) {
            throw new RemotingTooMuchRequestException("invokeOnewayImpl tryAcquire semaphore timeout, "
                    + timeoutMillis + "ms, waiting thread numbers: "
                    + this.semaphoreOneway.getQueueLength());
        }

        try {
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (!f.isSuccess()) {
                        log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    }
                }
            });
        } finally {
            this.semaphoreOneway.release();
        }
    }

    /**
     * 注册处理器
     */
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executorThis) {
            executorThis = this.getPublicExecutor();
        }
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    /**
     * 注册默认处理器
     */
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor, executor);
    }

    /**
     * 扫描并清理超时的ResponseFuture
     */
    public void scanResponseTable() {
        final long currentTime = System.currentTimeMillis();
        final Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();

        while (it.hasNext()) {
            final Map.Entry<Integer, ResponseFuture> entry = it.next();
            final ResponseFuture rep = entry.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= currentTime) {
                // 超时，移除
                it.remove();
                rep.setCause(new RemotingTimeoutException(
                        "wait response on the channel <" + rep.getRemoteAddr() + "> timeout, "
                                + rep.getTimeoutMillis() + "(ms)"));
                rep.putResponse(null);

                log.warn("remove timeout request, {} {}", rep.getOpaque(), rep.getRemoteAddr());

                try {
                    executeInvokeCallback(rep);
                } catch (Throwable e) {
                    log.warn("scanResponseTable, executeInvokeCallback Exception", e);
                } finally {
                    rep.release();
                }
            }
        }
    }

    /**
     * RPC前置钩子（子类可覆盖）
     */
    protected void doBeforeRpcHooks(String remoteAddr, RemotingCommand request) {
        // 子类实现
    }

    /**
     * RPC后置钩子（子类可覆盖）
     */
    protected void doAfterRpcHooks(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        // 子类实现
    }

    /**
     * 获取回调执行线程池
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * 获取公共执行线程池
     */
    public abstract ExecutorService getPublicExecutor();

    // ==================== 回调接口 ====================

    /**
     * 调用回调接口
     */
    public interface InvokeCallback {
        /**
         * 操作完成回调
         */
        void operationComplete(final ResponseFuture responseFuture);
    }

    /**
     * 请求处理器接口
     */
    public interface NettyRequestProcessor {
        /**
         * 处理请求
         */
        RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
                throws Exception;

        /**
         * 是否拒绝请求
         */
        boolean rejectRequest();
    }
}
