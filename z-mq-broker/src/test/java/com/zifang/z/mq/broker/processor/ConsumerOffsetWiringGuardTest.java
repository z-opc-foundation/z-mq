package com.zifang.z.mq.broker.processor;

import com.zifang.z.mq.client.consumer.ConsumerOffsetRequests;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * W2b 机检守卫（工单 §2.1 / §2.2 / §2.3 + §4 CP-C）。
 * <p>
 * 全部是"扫真实 src/main 源文件 + 读常量"的结构性检查，不起进程 —— 它管的是<b>接线</b>，
 * 端到端语义由 {@code ConsumerOffsetRestartE2ETest} 管。分工的理由：本仓出现过
 * "能力写进 javadoc、src/main 里零实现"，那种形状用行为测试测不到（根本没东西可测），
 * 只能用"这个名字在 src/main 里除了声明之外有没有读者"来量。
 * <ol>
 *   <li><b>{@code UPDATE_CONSUMER_OFFSET}(220) 必须有 registerProcessor 侧的读者</b>。
 *       工单 09-26 在 {@code 7948185} 上的读数是"除 {@code RequestCode.java:84} 那行声明外零引用"
 *       ⇒ 这条守卫在接上之前<b>应该是红的</b>。</li>
 *   <li><b>{@code queryOffset} 必须有 pull 路径上的读者</b>：声明在
 *       {@code ConsumerOffsetManager.java:107}，工单读数同样是零读者（pull 写死从 0 开始）。</li>
 *   <li><b>client 侧必须真的提交位点</b>：{@code commitOffset(} 在 z-mq-client 的 src/main 里
 *       除声明外必须有读者（两个消费者各自的 offsetTable 不再是唯一真相）。</li>
 *   <li><b>线上契约同名</b>：pull 请求里"消费组"这个字段名在 client 与 broker 两侧必须是同一个字符串，
 *       且 client 真的往 pull 请求上写它 —— 没有 group 就没有"按组恢复"的 key，
 *       第 2 条的 queryOffset 只能永远返回 -1。</li>
 *   <li><b>尺自检</b>：同一次运行里必须既量出非 0（两个已知存在项）也量出 0（一个不存在项）。</li>
 * </ol>
 */
public class ConsumerOffsetWiringGuardTest {

    /** 协议常量的声明文件自身（声明行与它的 javadoc 不算读者）. */
    private static final String REQUEST_CODE_FILE = "RequestCode.java";
    /** 位点后端的声明文件自身. */
    private static final String OFFSET_MANAGER_FILE = "ConsumerOffsetManager.java";

    /**
     * 线上契约里"消费组"的字段名（client 与 broker 两侧各有一处字面量声明）。
     * 这里刻意用字面量而不是某一侧的常量：CP-C 的红要量的是<b>接线</b>，
     * 在两侧常量还不存在时也要能编译，否则"先跑红"这一步做不出来。
     */
    private static final String CONSUMER_GROUP_FIELD = "consumerGroup";

    // ==================== 1. CP-C: 提交侧接线 ====================

    @Test
    @DisplayName("CP-C: UPDATE_CONSUMER_OFFSET 在 src/main 里除声明外必须有 registerProcessor 侧的读者（工单读数=零引用, 接上之前该红）")
    public void updateConsumerOffsetCodeHasAProcessorSideReader() throws IOException {
        Scan all = scanMain("RequestCode.UPDATE_CONSUMER_OFFSET");
        report("RequestCode.UPDATE_CONSUMER_OFFSET", all);
        Scan readers = all.excludingFiles(REQUEST_CODE_FILE);
        assertTrue(readers.hitLines >= 1,
                "UPDATE_CONSUMER_OFFSET(220) 在 src/main 里除声明行外零引用 ⇒ 没有任何处理器绑它, "
                        + "这个请求码发过来就是 REQUEST_CODE_NOT_SUPPORTED ⇒ \"消费位点持久化\" 的提交侧根本没接线。"
                        + " 全部命中=" + all.describeHits());
        Scan registering = readers.thatRegisterProcessors();
        assertTrue(registering.hitLines >= 1,
                "UPDATE_CONSUMER_OFFSET 的读者必须出现在某个调用 registerProcessor 的文件里"
                        + "（只被 javadoc/注释提一句不算接线）, 实际命中=" + readers.describeHits());
        assertTrue(registering.touchesFile("BrokerController.java"),
                "接线点应当在 BrokerController.registerProcessor()（本仓其余处理器都注册在那儿）, 实际="
                        + registering.describeHits());
    }

    // ==================== 2. CP-C: 读取侧接线 ====================

    @Test
    @DisplayName("CP-C: queryOffset 在 src/main 里除自身声明外必须有读者, 且读者在 pull 路径上")
    public void queryOffsetHasAReaderOnThePullPath() throws IOException {
        Scan all = scanMain("queryOffset(");
        report("queryOffset(", all);
        Scan readers = all.excludingFiles(OFFSET_MANAGER_FILE);
        assertTrue(readers.hitLines >= 1,
                "ConsumerOffsetManager.queryOffset 零读者 ⇒ broker 存了位点却从不按组回读, "
                        + "pull 只能从 0 开始（工单 §1 的读数）。全部命中=" + all.describeHits());
        assertTrue(readers.touchesFile("PullMessageProcessor.java"),
                "按组恢复的读取点必须在 pull 请求路径上（PullMessageProcessor）, 实际="
                        + readers.describeHits());
    }

    // ==================== 3. client 侧提交 ====================

    @Test
    @DisplayName("§2.3: z-mq-client 的 src/main 里必须真的发提交请求 —— 进程内 offsetTable 不许是唯一真相")
    public void clientSideActuallyCommitsOffsetsToBroker() throws IOException {
        // 提交这件事必须被两个消费者各自走到（pull = 显式入口, push = listener 成功后写穿）
        Scan commit = scanModules(false, "ConsumerOffsetRequests.sendOffsetCommit(");
        report("ConsumerOffsetRequests.sendOffsetCommit(", commit);
        assertTrue(commit.hitLines >= 2,
                "两个消费者都必须真的把位点提交给 broker, 量到 " + commit.hitLines + " 处 ⇒ 位点仍然只留在进程里: "
                        + commit.describeHits());
        assertTrue(commit.touchesFile("DefaultMQPullConsumer.java"),
                "pull 消费者要有能落出去的提交入口（§2.3 只要求显式入口）: " + commit.describeHits());
        assertTrue(commit.touchesFile("DefaultMQPushConsumer.java"),
                "push 消费者要在 listener 返回成功处写穿提交: " + commit.describeHits());

        // 请求构造只该有一个口; 两个消费者各写一遍字段名, 就是"字段名漂了一侧静默失效"的入口
        Scan built = scanModules(false, "buildUpdateConsumerOffsetRequest(");
        report("buildUpdateConsumerOffsetRequest(", built);
        assertTrue(built.touchesFile("ConsumerOffsetRequests.java"),
                "提交请求的构造必须收在共用口里: " + built.describeHits());
        assertFalse(built.touchesFile("DefaultMQPullConsumer.java")
                        && built.touchesFile("DefaultMQPushConsumer.java"),
                "两个消费者各自手搓 UPDATE_CONSUMER_OFFSET 请求 ⇒ 字段名会各漂各的: " + built.describeHits());

        // pull 消费者那个"显式提交入口"得真是 public 的, 否则 §2.3 那一半只是内部自娱
        Scan api = scanModules(false, "public long commitOffset(");
        report("public long commitOffset(", api);
        assertTrue(api.touchesFile("DefaultMQPullConsumer.java"),
                "§2.3: pull 消费者必须提供显式的位点提交入口: " + api.describeHits());

        // 读侧的对应物: 不带 offset 的拉取入口
        Scan pullNoOffset = scanModules(false, "pullFromCommittedOffset(");
        report("pullFromCommittedOffset(", pullNoOffset);
        assertTrue(pullNoOffset.touchesFile("DefaultMQPullConsumer.java"),
                "client 侧要能走到\"请求不带 offset ⇒ broker 按已提交位点起读\"那条路: "
                        + pullNoOffset.describeHits());
    }

    // ==================== 4. 线上契约: pull 请求里得有 group ====================

    @Test
    @DisplayName("§2.2 结论: pull 请求必须自带消费组字段（两侧字段名同名, 且真的读/写）")
    public void pullRequestCarriesConsumerGroupOnBothSides() throws IOException {
        // 字段名的真相只该有一处字面量; 两侧不同名 = 按组恢复静默失效
        assertEquals(CONSUMER_GROUP_FIELD, ConsumerOffsetRequests.EXT_CONSUMER_GROUP,
                "client 侧写进请求的字段名变了 ⇒ broker 读不到, 而且是静默的");
        assertEquals(PullMessageProcessor.EXT_CONSUMER_GROUP, ConsumerOffsetRequests.EXT_CONSUMER_GROUP,
                "broker 读的消费组字段与 client 写的不同名 ⇒ 按组恢复永远拿不到 key");

        Scan brokerReads = scanModules(true, "getExtField(EXT_CONSUMER_GROUP)");
        report("getExtField(EXT_CONSUMER_GROUP)", brokerReads);
        assertTrue(brokerReads.touchesFile("PullMessageProcessor.java"),
                "broker 侧没有从 pull 请求里读消费组 ⇒ \"按组恢复\"没有 key, queryOffset 只能永远返回 -1。"
                        + "工单 §2.2 要证伪的前提就是这个, 实测今天的命中=" + brokerReads.describeHits());

        // client 两个消费者各自往 pull 请求上写 group
        Scan clientWrites = scanModules(false, "ConsumerOffsetRequests.EXT_CONSUMER_GROUP");
        report("client 写消费组字段", clientWrites);
        assertTrue(clientWrites.touchesFile("DefaultMQPullConsumer.java")
                        && clientWrites.touchesFile("DefaultMQPushConsumer.java"),
                "两个消费者的 pull 请求都得带上消费组, 否则 §2.2 那条真缺陷原样留着, 实际="
                        + clientWrites.describeHits());
    }

    // ==================== 5. 尺自检: 阳性对照必须非 0, 阴性对照必须 0 ====================

    @Test
    @DisplayName("尺自检: 同一个扫描器必须既量得出非 0（三个已知存在项）也量得出 0（一个不存在项）")
    public void theScannerSeesBothZeroAndNonZero() throws IOException {
        Scan positive = scanMain("RequestCode.GET_ALL_CONSUMER_OFFSET");
        report("RequestCode.GET_ALL_CONSUMER_OFFSET", positive);
        Scan positiveReaders = positive.excludingFiles(REQUEST_CODE_FILE);
        assertTrue(positiveReaders.hitLines >= 1,
                "阳性对照失败: GET_ALL_CONSUMER_OFFSET 本该在 src/main 里有读者"
                        + "（BrokerController.registerProcessor / BrokerOutAPI / SlaveSynchronize）, 却量到 0"
                        + " ⇒ 扫描器没在扫真的东西, 上面所有 0 命中都是空跑");
        assertTrue(positiveReaders.touchesFile("BrokerController.java"),
                "阳性对照要看得见 BrokerController 上那条 registerProcessor: "
                        + positiveReaders.describeHits());
        assertTrue(positiveReaders.thatRegisterProcessors().hitLines >= 1,
                "阳性对照走的是与第 1 条守卫<b>同一个</b> registerProcessor 判据 —— 它必须非 0,"
                        + " 否则那条断言的 0 命中没有意义: " + positiveReaders.describeHits());

        Scan commitReader = scanMain(".commitOffset(");
        report(".commitOffset(", commitReader);
        assertTrue(commitReader.excludingFiles(OFFSET_MANAGER_FILE).hitLines >= 1
                        && commitReader.touchesFile("SlaveSynchronize.java"),
                "阳性对照: 提交位点的调用点本该存在（本仓 SlaveSynchronize 早就在调它）, 量不到说明扫描范围错了: "
                        + commitReader.describeHits());

        Scan absent = scanMain("w2bNeedleThatCannotExistInThisRepo_7c4e2b(");
        report("不存在的针", absent);
        assertTrue(absent.javaFiles >= 100,
                "扫描器只读了 " + absent.javaFiles + " 个 src/main 文件, 分母小到不可信（工单口径: 全仓 211 个 .java）");
        assertEquals(0, absent.hitLines,
                "不存在的针必须 0 命中, 否则命中数没有意义: " + absent.describeHits());
    }

    // ==================== 扫描工具 ====================

    private static void report(String needle, Scan scan) {
        System.out.println("[w2b-guard] needle=" + needle + " javaFilesScanned=" + scan.javaFiles
                + " hitLines=" + scan.hitLines + " hits=" + scan.describeHits()
                + " | user.dir=" + System.getProperty("user.dir"));
    }

    /** 扫仓里每个 z-mq-* 模块的 src/main（不含 target）. */
    private static Scan scanMain(String needle) throws IOException {
        return scanModules(true, needle);
    }

    /**
     * @param allModules false 时只扫 z-mq-client（用来量"这件事是不是 client 侧做的"）
     */
    private static Scan scanModules(boolean allModules, String needle) throws IOException {
        File repoRoot = resolveRepoRoot();
        Scan scan = new Scan(needle);
        File[] children = repoRoot.listFiles();
        if (children == null) {
            throw new IOException("cannot list repo root: " + repoRoot.getAbsolutePath());
        }
        Arrays.sort(children);
        for (File module : children) {
            if (!module.isDirectory() || !module.getName().startsWith("z-mq-")) {
                continue;
            }
            if (!allModules && !"z-mq-client".equals(module.getName())) {
                continue;
            }
            File srcMain = new File(module, "src/main");
            if (srcMain.isDirectory()) {
                walk(srcMain, scan);
            }
        }
        return scan;
    }

    /**
     * surefire 的工作目录是模块目录, 往上找到同时含 z-mq-broker / z-mq-client / z-mq-store 的那一层。
     * 找不到就 fail —— 让"扫错地方"表现为红, 而不是表现为一个漂亮的 0 命中。
     */
    private static File resolveRepoRoot() {
        File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
        List<String> tried = new ArrayList<>();
        for (int up = 0; up < 6 && dir != null; up++) {
            tried.add(dir.getAbsolutePath());
            if (new File(dir, "z-mq-broker").isDirectory() && new File(dir, "z-mq-client").isDirectory()
                    && new File(dir, "z-mq-store").isDirectory()) {
                return dir;
            }
            dir = dir.getParentFile();
        }
        fail("cannot resolve repo root from user.dir; tried " + tried);
        return null;
    }

    private static void walk(File file, Scan scan) throws IOException {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children == null) {
                throw new IOException("cannot list " + file.getAbsolutePath());
            }
            Arrays.sort(children);
            for (File child : children) {
                if (child.isDirectory() && "target".equals(child.getName())) {
                    continue; // 生成物不算源码
                }
                walk(child, scan);
            }
            return;
        }
        if (!file.getName().endsWith(".java")) {
            return;
        }
        scan.javaFiles++;
        String text = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        boolean registers = text.contains("registerProcessor(");
        int hitLines = 0;
        String[] lines = text.split("\r?\n", -1);
        for (int i = 0; i < lines.length; i++) {
            if (!lines[i].contains(scan.needle)) {
                continue;
            }
            if (isProse(lines[i])) {
                // 注释/javadoc 里提一句不算读者 —— CP-B 实测过: 把真的调用摘掉、只留 javadoc 里那句
                // "问 queryOffset(topic, queueId, group)" 时, 不过滤注释的守卫照样绿, 那是把尺本身修漏了。
                continue;
            }
            hitLines++;
            scan.hits.add(file.getName() + ":" + (i + 1));
        }
        if (hitLines > 0) {
            scan.hitLines += hitLines;
            scan.files.add(new HitFile(file, hitLines, registers));
        }
    }

    /** 这一行是散文（javadoc / 行注释 / 块注释续行）而不是代码? */
    private static boolean isProse(String line) {
        String t = line.trim();
        return t.startsWith("*") || t.startsWith("/*") || t.startsWith("//");
    }

    /** 一个 src/main 文件对某根针的命中读数. */
    private static final class HitFile {
        private final File file;
        private final int hitLines;
        /** 这个文件里有没有 registerProcessor( 调用 —— 用来把"注释提一句"和"真接线"分开. */
        private final boolean registersProcessors;

        HitFile(File file, int hitLines, boolean registersProcessors) {
            this.file = file;
            this.hitLines = hitLines;
            this.registersProcessors = registersProcessors;
        }
    }

    /** 一趟扫描的读数：读了多少文件、命中多少行、命中在哪. */
    private static final class Scan {
        private final String needle;
        private int javaFiles;
        private int hitLines;
        private final List<HitFile> files = new ArrayList<>();
        private final List<String> hits = new ArrayList<>();

        Scan(String needle) {
            this.needle = needle;
        }

        Scan excludingFiles(String... fileNameSuffixes) {
            Scan out = new Scan(needle);
            out.javaFiles = javaFiles;
            for (HitFile hf : files) {
                boolean drop = false;
                for (String suffix : fileNameSuffixes) {
                    if (hf.file.getName().endsWith(suffix)) {
                        drop = true;
                        break;
                    }
                }
                if (!drop) {
                    out.files.add(hf);
                    out.hitLines += hf.hitLines;
                }
            }
            for (HitFile hf : out.files) {
                for (String hit : hits) {
                    if (hit.startsWith(hf.file.getName() + ":")) {
                        out.hits.add(hit);
                    }
                }
            }
            return out;
        }

        /** 只留"这个文件里有 registerProcessor( 调用"的命中（区分接线与注释）. */
        Scan thatRegisterProcessors() {
            Scan out = new Scan(needle);
            out.javaFiles = javaFiles;
            for (HitFile hf : files) {
                if (hf.registersProcessors) {
                    out.files.add(hf);
                    out.hitLines += hf.hitLines;
                    for (String hit : hits) {
                        if (hit.startsWith(hf.file.getName() + ":")) {
                            out.hits.add(hit);
                        }
                    }
                }
            }
            return out;
        }

        boolean touchesFile(String fileNameSuffix) {
            for (HitFile hf : files) {
                if (hf.file.getName().endsWith(fileNameSuffix)) {
                    return true;
                }
            }
            return false;
        }

        String describeHits() {
            return hits.isEmpty() ? "<无命中>" : hits.toString();
        }
    }
}
