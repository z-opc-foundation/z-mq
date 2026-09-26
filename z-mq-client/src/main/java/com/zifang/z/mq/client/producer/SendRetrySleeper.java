package com.zifang.z.mq.client.producer;

/**
 * 重试之前"等一会儿"这个动作的出口。
 * <p>
 * 单列成一个接口只为一个理由：等待是可注入的，测试就能把"退避跑了几次、每次拿到多少毫秒"
 * 当成读数来断言，而不是拿挂钟差值当判据。默认实现 {@link #THREAD} 用 {@code Thread.sleep}，
 * 0 毫秒直接返回。
 */
public interface SendRetrySleeper {

    /**
     * @param delayMillis {@link SendRetryBackoff} 算出来的等待时长；0 表示不必等
     */
    void await(long delayMillis) throws InterruptedException;

    /** 生产用的实现：真的睡一会儿. */
    SendRetrySleeper THREAD = new SendRetrySleeper() {
        @Override
        public void await(long delayMillis) throws InterruptedException {
            if (delayMillis > 0L) {
                Thread.sleep(delayMillis);
            }
        }
    };

    /** 什么都不等的实现：调用方要"立刻换下一台"时用. */
    SendRetrySleeper NO_WAIT = new SendRetrySleeper() {
        @Override
        public void await(long delayMillis) {
            // 故意不等待：退避时长仍然是被测的那个数
        }
    };
}
