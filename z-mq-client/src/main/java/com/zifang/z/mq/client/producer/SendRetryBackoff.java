package com.zifang.z.mq.client.producer;

/**
 * 重试之前的退避时长怎么算 —— 可注入，因此"退避了多久"是一个能被数出来的量，而不是挂钟读数。
 * <p>
 * 刻意只回答"该等多久"，不回答"等不等"：等待动作在 {@link SendRetrySleeper} 那一侧，
 * 这样测试可以把两者都换成记录器，断言"退避被调用了几次、每次参数是什么"，
 * 而不需要拿真实时间当判据。
 * <p>
 * 默认实现 {@link FlowControlAware} 只对{@link SendRetryPolicy.Tier#FLOW_CONTROL 流控}那一档退避：
 * 流控的语义是"稍后再来"，而"连不上那台机器"的下一台就在路由里，多等一毫秒只是把这次发送拖慢。
 */
public interface SendRetryBackoff {

    /**
     * @param retryIndex 第几次重试（从 1 起，不含首次那次尝试）
     * @param tier       上一条失败落到的那一档
     * @return 这一次重试之前该等多久（毫秒）；0 表示不必等
     */
    long delayMillisFor(int retryIndex, SendRetryPolicy.Tier tier);

    /** 默认档：非流控不退避，流控按 20ms 起步、每次翻倍、封顶 200ms. */
    SendRetryBackoff DEFAULT = new FlowControlAware(20L, 2, 200L);

    /** 指数退避实现；参数全部由构造方给，便于测试拿一组好数的值. */
    class FlowControlAware implements SendRetryBackoff {

        private final long baseMillis;
        private final int factor;
        private final long capMillis;

        public FlowControlAware(long baseMillis, int factor, long capMillis) {
            this.baseMillis = baseMillis;
            this.factor = factor;
            this.capMillis = capMillis;
        }

        @Override
        public long delayMillisFor(int retryIndex, SendRetryPolicy.Tier tier) {
            if (tier != SendRetryPolicy.Tier.FLOW_CONTROL || retryIndex < 1 || baseMillis <= 0L) {
                return 0L;
            }
            long delay = baseMillis;
            for (int i = 1; i < retryIndex; i++) {
                delay = delay * factor;
                if (delay >= capMillis) {
                    return capMillis;
                }
            }
            return Math.min(delay, capMillis);
        }
    }
}
