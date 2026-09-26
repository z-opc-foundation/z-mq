package com.zifang.z.mq.client;

import com.zifang.z.mq.common.BrokerData;
import com.zifang.z.mq.common.MessageQueue;
import com.zifang.z.mq.common.QueueData;
import com.zifang.z.mq.common.TopicRouteData;
import com.zifang.z.mq.remoting.netty.NettyClientConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 「换一台没试过的机器」这把尺本身的刻度：带排除的选队列入口必须是<b>确定性轮询 + 精确排除</b>，
 * 而不是"打散之后也许换一台"。
 * <p>
 * 这里量的三件事都是重试循环赖以成立的前提，却又都不在循环里：
 * <ol>
 *   <li>被排除那台在一次都不会被选中，其余各台<b>均匀到格</b>（200 轮 ⇒ 100/100，不是"大致"）；</li>
 *   <li>排除表用完了要能给出 null —— "换无可换"必须是可判读的一件事，而不是偷偷回到某一台；</li>
 *   <li>旧的那条入口（不带排除、按打散选）行为不变：三轮之内能覆盖到全部三台。
 *       本支只加新入口，不改旧语义。</li>
 * </ol>
 * 轮询位是实例级的（{@code topic} 一个格子），所以每个用例都用新建的实例，起点为 0。
 */
public class MQClientInstanceExcludeBrokerTest {

    private static final String TOPIC = "ExcludeBrokerTopic";
    private static final int ROUNDS = 200;

    private MQClientInstance instance;

    @BeforeEach
    public void freshInstanceWithZeroIndex() {
        instance = new MQClientInstance("exclude-broker-tester", "127.0.0.1:9876", new NettyClientConfig());
    }

    @Test
    @DisplayName("排除掉的那台 200 轮一次都不许被选中, 其余两台精确到 100/100")
    public void excludedBrokerIsNeverPickedAgainAndTheRestSplitExactly() {
        TopicRouteData three = route(3, 1);
        Set<String> excluded = new HashSet<String>(Collections.singletonList("broker-1"));

        Set<String> picked = new HashSet<String>();
        int[] counts = new int[3];
        for (int i = 0; i < ROUNDS; i++) {
            MessageQueue mq = instance.selectOneMessageQueue(TOPIC, three, excluded);
            assertNotNull(mq, "第 " + i + " 轮就不该选不出队列: 还有两台可写");
            counts[brokerIndexOf(mq.getBrokerName())]++;
            picked.add(mq.getBrokerName());
        }
        assertEquals(0, counts[1], "被排除的那台一次都没被跳过才对: " + Arrays.toString(counts));
        assertEquals(100, counts[0], "剩下两台必须精确平分（轮询不是抽签）: " + Arrays.toString(counts));
        assertEquals(100, counts[2], Arrays.toString(counts));
        assertEquals(new HashSet<String>(Arrays.asList("broker-0", "broker-2")), picked);
    }

    @Test
    @DisplayName("排除按 brokerName 整台生效: 同一台的三个写队列都被跳过, 另两台的队列照常出现")
    public void exclusionIsPerBrokerNotPerQueue() {
        TopicRouteData three = route(3, 3);
        Set<String> excluded = new HashSet<String>(Collections.singletonList("broker-1"));
        Set<Integer> queuesOfSurvivors = new HashSet<Integer>();
        for (int i = 0; i < ROUNDS; i++) {
            MessageQueue mq = instance.selectOneMessageQueue(TOPIC, three, excluded);
            assertNotNull(mq);
            assertTrue(!"broker-1".equals(mq.getBrokerName()),
                    "排除整台，不许从它身上抽出任何一个队列: " + mq);
            queuesOfSurvivors.add(Integer.valueOf(mq.getQueueId()));
        }
        assertEquals(3, queuesOfSurvivors.size(),
                "另外三台各三个写队列里, 三个 queueId 都该被轮到: " + queuesOfSurvivors);
    }

    @Test
    @DisplayName("全部排除 ⇒ 返回 null（换无可换要能判读, 不许悄悄回到某一台）")
    public void everythingExcludedYieldsNull() {
        TopicRouteData three = route(3, 2);
        Set<String> excluded = new HashSet<String>(Arrays.asList("broker-0", "broker-1", "broker-2"));
        assertNull(instance.selectOneMessageQueue(TOPIC, three, excluded));
        // 没有写队列的那一份路由也一样给 null（这一档不是"排除完了"，是"根本没得写"）
        TopicRouteData noWritable = route(2, 1);
        for (QueueData qd : noWritable.getQueueDatas()) {
            qd.setWriteQueueNums(0);
        }
        assertNull(instance.selectOneMessageQueue(TOPIC, noWritable, Collections.<String>emptySet()));
    }

    @Test
    @DisplayName("null 与空排除表都等于「不排除」: 三台都要能被选中")
    public void noExclusionStillReachesEveryBroker() {
        TopicRouteData three = route(3, 1);
        Set<String> seenNullArg = new HashSet<String>();
        for (int i = 0; i < ROUNDS; i++) {
            seenNullArg.add(instance.selectOneMessageQueue(TOPIC, three, null).getBrokerName());
        }
        assertEquals(3, seenNullArg.size(), "排除表为 null 时必须覆盖三台: " + seenNullArg);

        Set<String> seenEmpty = new HashSet<String>();
        for (int i = 0; i < ROUNDS; i++) {
            seenEmpty.add(instance.selectOneMessageQueue(TOPIC, three,
                    new HashSet<String>()).getBrokerName());
        }
        assertEquals(3, seenEmpty.size(), "空排除表同样不排除任何东西: " + seenEmpty);
    }

    @Test
    @DisplayName("确定性不是「永远同一台」: 连续六轮走的是轮询位, 一圈之后必须原样重复")
    public void theDeterministicPickRepeatsForTheSameIndexSlot() {
        TopicRouteData three = route(3, 1);
        List<String> sequence = new ArrayList<String>();
        for (int i = 0; i < 6; i++) {
            sequence.add(instance.selectOneMessageQueue(TOPIC, three, null).getBrokerName());
        }
        assertEquals(sequence.subList(0, 3), sequence.subList(3, 6),
                "轮询位每三台绕一圈, 第二段必须与第一段一模一样: " + sequence);
        assertEquals(3, new HashSet<String>(sequence).size(), "一圈里三台各被选中一次: " + sequence);
    }

    @Test
    @DisplayName("旧那条不带排除的入口语义没被改动: 打散的那把照样覆盖全部三台")
    public void legacySelectorStillReachesAllBrokers() {
        TopicRouteData three = route(3, 1);
        Set<String> seen = new HashSet<String>();
        for (int i = 0; i < ROUNDS; i++) {
            seen.add(instance.selectOneMessageQueue(TOPIC, three).getBrokerName());
        }
        assertEquals(3, seen.size(),
                "旧入口还是那条打散的路, 本支没把它改成确定性: " + seen);
    }

    // ==================== 造路由 ====================

    private static int brokerIndexOf(String brokerName) {
        return Integer.parseInt(brokerName.substring("broker-".length()));
    }

    /** N 台 broker、每台 M 个写队列；地址齐备（选队列要看得到 master 地址）. */
    private static TopicRouteData route(int brokerCount, int writeQueueNums) {
        TopicRouteData route = new TopicRouteData();
        List<QueueData> queueDatas = new ArrayList<QueueData>();
        List<BrokerData> brokerDatas = new ArrayList<BrokerData>();
        for (int i = 0; i < brokerCount; i++) {
            String name = "broker-" + i;
            QueueData qd = new QueueData();
            qd.setBrokerName(name);
            qd.setReadQueueNums(writeQueueNums);
            qd.setWriteQueueNums(writeQueueNums);
            qd.setPerm(6);
            queueDatas.add(qd);

            HashMap<Long, String> addrs = new HashMap<Long, String>();
            addrs.put(BrokerData.MASTER_ID, "127.0.0." + (i + 1) + ":" + (20000 + i));
            brokerDatas.add(new BrokerData("exclude-cluster", name, addrs));
        }
        route.setQueueDatas(queueDatas);
        route.setBrokerDatas(brokerDatas);
        return route;
    }
}
