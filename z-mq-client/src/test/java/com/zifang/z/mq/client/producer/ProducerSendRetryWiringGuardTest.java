package com.zifang.z.mq.client.producer;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 「Producer 自动重试」这条对外承诺的接线守卫（机检，不起进程）。
 * <p>
 * README 那一行的承诺是「默认 3 次、可配」。一句承诺被写坏的方式通常不是行为测试没过，
 * 而是<b>接线被拆掉</b>：循环还在但同步发送不再经过它、白名单从表里搬回了 {@code if} 链、
 * 计数器只剩 getter 没有读者、退避被换成挂钟。这些都能让行为测试依旧绿，
 * 所以这里量的是盘面形状：
 * <ol>
 *   <li>同步发送这条路必须真的走进带白名单判决的循环，循环体里三件义务（问表、
 *       换机器/重取路由、记退避与计数）都得在；</li>
 *   <li>「哪种异常可以重试」只能写在表里：发送循环里一根 {@code instanceof Remoting*} 都不许有，
 *       加档位的 {@code new Rule(} 只许出现在表那个文件里；</li>
 *   <li>重试计数必须有读者：{@code src/main} 业务路径 0 个（只剩声明），
 *       测试树里必须 &ge; 1 个（否则这个数就是口号）；「可配」那个旋钮两侧同理；</li>
 *   <li>换机器是构造保证的：带排除的那条选择入口里不许有 {@code Collections.shuffle}，
 *       而旧的那条入口保持原样（它还是打散的那把，不能被顺手改成确定性）；</li>
 *   <li>等待必须由可注入的 sleeper 承担，发送循环自己不许睡挂钟；
 *       不在本支承诺范围内的路（半消息、oneway、异步、指定队列）必须没被接进这条循环。</li>
 * </ol>
 * 针一律是字符串字面量而不是类型引用：这样「实现还不存在/被摘掉」时这个文件照样编译得出来，
 * 才量得出先红后绿。每根针都配一把阳性对照 —— 只报 0 命中而不证明这把尺看得见非 0，等于没跑。
 */
public class ProducerSendRetryWiringGuardTest {

    private static final String PRODUCER = "DefaultMQProducer.java";
    private static final String POLICY = "SendRetryPolicy.java";
    private static final String SLEEPER = "SendRetrySleeper.java";
    private static final String INSTANCE = "MQClientInstance.java";
    private static final String TRANSACTION = "TransactionMQProducer.java";

    // ==================== 1. 同步发送真的走进循环 ====================

    @Test
    @DisplayName("机检: 同步发送这条路必须经过带白名单判决的循环，且循环体里三件义务都在")
    public void theSendPathActuallyRunsTheWhitelistLoop() throws IOException {
        String entry = bodyOf(PRODUCER, "protected SendResult sendWithRequestCode(");
        assertTrue(entry.contains("sendWithRetryLoop("),
                "README 承诺的是同步发送会自动重试；sendWithRequestCode 不进循环，这句话就没有承载它的代码。"
                        + "实际方法体=" + oneLine(entry));

        String loop = bodyOf(PRODUCER, "private SendResult sendWithRetryLoop(");
        // 义务一：每一趟的失败都由表下判决（组包、发送、响应三条入口都要问）
        assertTrue(loop.contains("explain("), "循环体里没有问表的 explain(: " + oneLine(loop));
        assertTrue(loop.contains("willRetry("), "循环体里没有把判决落成次数的 willRetry(: " + oneLine(loop));
        assertTrue(loop.contains("decideForBodyStatus("),
                "响应体里的存储结论没走表，这一档随时可能被人按状态值放开: " + oneLine(loop));
        assertTrue(loop.contains("decideForResponseCode("), "响应码没走表，同上: " + oneLine(loop));
        // 义务二：换机器与重取路由是两件不同的事，都得在
        assertTrue(loop.contains("refreshTopicRouteData("),
                "「先重取路由」那一档没有承载代码: " + oneLine(loop));
        assertTrue(loop.contains("triedBrokerNames.add("),
                "失败之后没把这一台记进排除表，则下一趟很可能又是同一台: " + oneLine(loop));
        assertTrue(loop.contains("prepareSend(instance, message, requestCode, route, triedBrokerNames"),
                "重试那一趟没把排除表交给组包，则「换机器」不是构造保证的: " + oneLine(loop));
        String prepares = bodyOf(PRODUCER,
                "private PreparedSend prepareSend(MQClientInstance instance, Message message, int requestCode,");
        assertTrue(prepares.contains("selectOneMessageQueue(topic, routeData, excludedBrokerNames"),
                "组包这条路没走带排除的选队列入口: " + oneLine(prepares));
        // 义务三：退避与计数
        assertTrue(loop.contains("delayMillisFor("), "没有问退避: " + oneLine(loop));
        assertTrue(loop.contains(".await("), "算了退避却没等: " + oneLine(loop));
        assertTrue(loop.contains("sendRetryCount.incrementAndGet()"),
                "重试计数没在重试那一趟上涨: " + oneLine(loop));
        assertTrue(loop.contains("for ("), "重试不是一趟一趟走出来的: " + oneLine(loop));
    }

    @Test
    @DisplayName("机检: 「哪种异常可重试」只能写在表里 —— 循环里一根 instanceof 都不许有")
    public void theWhitelistLivesOnlyInTheTable() throws IOException {
        Scan inLoop = scanFile(PRODUCER, "instanceof Remoting");
        report("DefaultMQProducer 里的 instanceof Remoting*", inLoop);
        assertEquals(0, inLoop.codeHitLines(),
                "发送循环自己认异常类型，则表与代码是两处真相、改表不生效: " + inLoop.describe());

        Scan tableRefs = scanFile(PRODUCER, "SendRetryPolicy");
        report("DefaultMQProducer 里对表的引用", tableRefs);
        assertTrue(tableRefs.codeHitLines() >= 5,
                "循环几乎不引用表，说明判决不是从表里读出来的: " + tableRefs.describe());

        // 阳性对照：同一把 instanceof 尺在表那个文件里必须量出非 0
        Scan inTable = scanFile(POLICY, "instanceof Remoting");
        report("SendRetryPolicy 里的 instanceof Remoting*（阳性对照）", inTable);
        assertTrue(inTable.codeHitLines() >= 5,
                "表里都没有按类型分类的代码，则这把尺是坏的（或表被搬走了）: " + inTable.describe());
    }

    @Test
    @DisplayName("机检: 只有表那个文件能加行；行数与档位数量同阶")
    public void theTableIsTheOnlyPlaceThatAddsRows() throws IOException {
        Scan rows = scanMain("new Rule(");
        report("src/main 里所有 new Rule(", rows);
        Scan outsideTable = rows.excludingFile(POLICY);
        assertEquals(0, outsideTable.codeHitLines(),
                "表外还有人在加档位，则顺序与兜底方向都不再由那张表说了算: " + outsideTable.describe());
        Scan insideTable = rows.onlyFile(POLICY);
        assertTrue(insideTable.codeHitLines() >= 9,
                "点名的档位少于 9 行，README 那条承诺的白名单就没钉全: " + insideTable.describe());
    }

    // ==================== 2. 计数与旋钮的读者 ====================

    @Test
    @DisplayName("机检: 重试计数在 src/main 里零读者、在测试树里有读者（两处都得量）")
    public void theRetryCounterHasReadersWhereTheyBelong() throws IOException {
        Scan mainSide = scanMain("getSendRetryCount(").excludingFile(PRODUCER);
        report("src/main 里 getSendRetryCount( 的读者（声明文件之外）", mainSide);
        assertEquals(0, mainSide.codeHitLines(),
                "src/main 的业务路径自己读这个数，则它就不是对外出口而是内部记账；"
                        + "这个数要有测试读者、业务侧零读者才对: " + mainSide.describe());

        Scan testSide = scanTestTree("getSendRetryCount(")
                .excludingFile("ProducerSendRetryWiringGuardTest.java");
        report("src/test 里 getSendRetryCount( 的读者（本守卫之外）", testSide);
        assertTrue(testSide.codeHitLines() >= 1,
                "一个没有读者的计数器就是口号：它涨没涨没人量得到: " + testSide.describe());

        // 阳性对照：同一把尺量「可配」那个旋钮，两侧都必须非 0
        Scan knob = scanMain("RetryTimesWhenSendFailed");
        report("src/main 里的 RetryTimesWhenSendFailed（getter/setter 那两侧）", knob);
        assertTrue(knob.codeHitLines() >= 2,
                "README 那句「默认 3 次，可配」的落点就是这一对读写口，少于 2 处说明它还没被接通: "
                        + knob.describe());
        assertTrue(knob.touchesFile(PRODUCER), "旋钮必须长在 producer 上: " + knob.describe());
        Scan knobTest = scanTestTree("setRetryTimesWhenSendFailed(");
        report("src/test 里 setRetryTimesWhenSendFailed(", knobTest);
        assertTrue(knobTest.codeHitLines() >= 1,
                "「可配」这件事得有读者真去配一次: " + knobTest.describe());
    }

    // ==================== 3. 换机器是构造保证的 ====================

    @Test
    @DisplayName("机检: 带排除的选队列入口不许打散；旧入口保持它原来的形状")
    public void failOverIsConstructedNotShuffled() throws IOException {
        String excluding = bodyOf(INSTANCE,
                "public MessageQueue selectOneMessageQueue(String topic, TopicRouteData routeData,");
        assertTrue(!excluding.contains("Collections.shuffle("),
                "重试要的是换一台没试过的机器，打散只保证「随机里也许换一台」: " + oneLine(excluding));
        assertTrue(excluding.contains("excludedBrokerNames"),
                "带排除的入口没真的在用排除表: " + oneLine(excluding));
        assertTrue(excluding.contains("& Integer.MAX_VALUE"),
                "轮询位自增绕回负数会算出负下标，取模前必须抹符号位: " + oneLine(excluding));

        // 阳性对照 + 边界：旧那条入口还是打散的那把，本支不许顺手改它的语义
        String legacy = bodyOf(INSTANCE,
                "public MessageQueue selectOneMessageQueue(String topic, TopicRouteData routeData)");
        assertTrue(legacy.contains("Collections.shuffle("),
                "旧的选队列入口被改成确定性，则现有那些依赖打散的行为测试就不是在测它了: " + oneLine(legacy));
    }

    @Test
    @DisplayName("机检: 发送循环自己不许睡挂钟；等待必须由可注入的 sleeper 承担")
    public void waitingGoesThroughTheInjectableSleeper() throws IOException {
        Scan loopSleeps = scanFile(PRODUCER, "Thread.sleep(");
        report("DefaultMQProducer 里的 Thread.sleep(", loopSleeps);
        assertEquals(0, loopSleeps.codeHitLines(),
                "发送路径上睡挂钟，则退避跑了几次、参数是多少都量不出来: " + loopSleeps.describe());

        // 阳性对照：唯一那处真的睡在 sleeper 的默认实现里，且它在接口后面
        Scan realSleep = scanFile(SLEEPER, "Thread.sleep(");
        report("SendRetrySleeper 默认实现里的 Thread.sleep(", realSleep);
        assertTrue(realSleep.codeHitLines() >= 1,
                "等待这件事没有一处真实现，那注入点就是空的: " + realSleep.describe());
        Scan await = scanFile(SLEEPER, "await(");
        assertTrue(await.codeHitLines() >= 1,
                "接口上那一个 await( 声明不见了: " + await.describe());
    }

    // ==================== 4. 不在本支承诺范围内的路 ====================

    @Test
    @DisplayName("机检: 半消息 / oneway / 异步 / 指定队列这四条路没被接进循环")
    public void theOtherSendPathsAreNotWiredIntoTheLoop() throws IOException {
        Scan half = scanFile(TRANSACTION, "sendWithRetryLoop(");
        report("TransactionMQProducer 里的 sendWithRetryLoop(", half);
        assertEquals(0, half.codeHitLines(),
                "半消息的二次确认必须发回写它的那一台，换机器就是把结论发丢了，所以它不进这条循环: "
                        + half.describe());
        Scan halfPrepares = scanFile(TRANSACTION, "prepareMessageSend(");
        assertTrue(halfPrepares.codeHitLines() >= 1,
                "半消息不再走它自己那两个出口了？本支没打算动它: " + halfPrepares.describe());

        String oneway = bodyOf(PRODUCER, "public void sendOneway(");
        assertTrue(!oneway.contains("sendWithRetryLoop("),
                "oneway 没有结论可言，接进重试循环等于把同一条消息写两遍: " + oneLine(oneway));
        String async = bodyOf(PRODUCER, "public void send(Message message, SendCallback");
        assertTrue(!async.contains("sendWithRetryLoop("),
                "异步那条路本支不动: " + oneLine(async));
        String toQueue = bodyOf(PRODUCER, "public SendResult send(Message message, MessageQueue mq)");
        assertTrue(!toQueue.contains("sendWithRetryLoop("),
                "调用方指定了队列，换机器就是违背调用方的意思: " + oneLine(toQueue));
    }

    // ==================== 5. 尺自检 ====================

    @Test
    @DisplayName("尺自检: 文件数下限、注释不算代码、不存在的针必须量出 0")
    public void theRulerIsCalibrated() throws IOException {
        File root = resolveRepoRoot();
        File producerDir = new File(root, "z-mq-client/src/main/java/com/zifang/z/mq/client/producer");
        List<String> javaFiles = new ArrayList<String>();
        collectJava(producerDir, javaFiles);
        assertTrue(javaFiles.size() >= 8,
                "producer 包的 .java 文件数低于下限，说明扫错了目录: " + javaFiles.size()
                        + " @ " + producerDir.getAbsolutePath());

        Scan absent = scanMain("w2g-prose-only-needle-nobody-writes-this");
        report("一根哪里都不存在的针（阴性对照）", absent);
        assertEquals(0, absent.rawHitLines(), "不存在的针必须 0 命中: " + absent.describe());

        // 注释与代码分得开：retryTimesWhenSendFailed 同时出现在 javadoc 与代码行上
        Scan knob = scanMain("retryTimesWhenSendFailed");
        report("retryTimesWhenSendFailed（含 javadoc 那一侧）", knob);
        assertTrue(knob.rawHitLines() > knob.codeHitLines(),
                "这根针在 javadoc 里也出现过，raw 必须大于 code，否则这把尺分不清注释与代码: "
                        + knob.describe());
        assertTrue(knob.codeHitLines() >= 9,
                "代码行上的读数少于下限，则注释过滤把尺子压扁了: " + knob.describe());
    }

    // ==================== 扫描实现 ====================

    /** 一次扫描的读数：raw = 文本里出现（含注释），code = 出现在非注释代码行. */
    private static final class Scan {
        final String needle;
        final List<Hit> hits = new ArrayList<Hit>();

        Scan(String needle) {
            this.needle = needle;
        }

        Scan(String needle, List<Hit> hits) {
            this.needle = needle;
            this.hits.addAll(hits);
        }

        int rawHitLines() {
            return hits.size();
        }

        int codeHitLines() {
            int n = 0;
            for (Hit h : hits) {
                if (h.isCode) {
                    n++;
                }
            }
            return n;
        }

        Scan excludingFile(String fileName) {
            List<Hit> kept = new ArrayList<Hit>();
            for (Hit h : hits) {
                if (!h.file.endsWith("/" + fileName)) {
                    kept.add(h);
                }
            }
            return new Scan(needle + " (除 " + fileName + ")", kept);
        }

        Scan onlyFile(String fileName) {
            List<Hit> kept = new ArrayList<Hit>();
            for (Hit h : hits) {
                if (h.file.endsWith("/" + fileName)) {
                    kept.add(h);
                }
            }
            return new Scan(needle + " (仅 " + fileName + ")", kept);
        }

        boolean touchesFile(String fileName) {
            for (Hit h : hits) {
                if (h.file.endsWith("/" + fileName)) {
                    return true;
                }
            }
            return false;
        }

        String describe() {
            if (hits.isEmpty()) {
                return "0 命中";
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hits.size(); i++) {
                sb.append(hits.get(i));
                if (i < hits.size() - 1) {
                    sb.append(" | ");
                }
            }
            return sb.toString();
        }
    }

    /** 一根针的一次命中：哪个文件、哪一行、算不算代码行. */
    private static final class Hit {
        final String file;
        final int line;
        final boolean isCode;

        Hit(String file, int line, boolean isCode) {
            this.file = file;
            this.line = line;
            this.isCode = isCode;
        }

        @Override
        public String toString() {
            return file + ":" + line + (isCode ? "" : "(注释)");
        }
    }

    private static Scan scanMain(String needle) throws IOException {
        return scan(needle, true);
    }

    private static Scan scanTestTree(String needle) throws IOException {
        return scan(needle, false);
    }

    private static Scan scan(String needle, boolean mainSide) throws IOException {
        File root = resolveRepoRoot();
        Scan scan = new Scan(needle);
        File[] children = root.listFiles();
        assertTrue(children != null && children.length > 0, "列不出仓库根: " + root.getAbsolutePath());
        Arrays.sort(children);
        for (File module : children) {
            if (!module.isDirectory() || !module.getName().startsWith("z-mq-")) {
                continue;
            }
            File src = new File(module, mainSide ? "src/main" : "src/test");
            if (src.isDirectory()) {
                walk(src, needle, scan);
            }
        }
        return scan;
    }

    private static Scan scanFile(String fileName, String needle) throws IOException {
        File file = findSource(fileName);
        Scan scan = new Scan(needle);
        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        boolean insideBlock = false;
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.contains(needle)) {
                String stripped = stripComment(line, insideBlock);
                scan.hits.add(new Hit(file.getPath(), i + 1,
                        stripped != null && !stripped.trim().isEmpty()));
            }
            insideBlock = trackBlockComment(line, insideBlock);
        }
        return scan;
    }

    /** 取某个方法（以给定签名行开头）到配对右花括号为止的方法体. */
    private static String bodyOf(String fileName, String signatureNeedle) throws IOException {
        File file = findSource(fileName);
        List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
        int start = -1;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains(signatureNeedle)) {
                start = i;
                break;
            }
        }
        assertTrue(start >= 0, fileName + " 里找不到签名行「" + signatureNeedle
                + "」，方法被改名或删掉了，本支承诺的接线就不成立了");
        int depth = 0;
        boolean opened = false;
        StringBuilder body = new StringBuilder();
        for (int i = start; i < lines.size(); i++) {
            String line = lines.get(i);
            body.append(line).append('\n');
            for (int c = 0; c < line.length(); c++) {
                char ch = line.charAt(c);
                if (ch == '{') {
                    depth++;
                    opened = true;
                } else if (ch == '}') {
                    depth--;
                }
            }
            if (opened && depth <= 0) {
                return body.toString();
            }
        }
        throw new IllegalStateException(fileName + " 的方法体花括号没配对（从第 " + (start + 1) + " 行起）");
    }

    private static File findSource(String fileName) {
        File root = resolveRepoRoot();
        List<File> found = new ArrayList<File>();
        File[] children = root.listFiles();
        if (children != null) {
            Arrays.sort(children);
            for (File module : children) {
                if (!module.isDirectory() || !module.getName().startsWith("z-mq-")) {
                    continue;
                }
                collectNamed(new File(module, "src"), fileName, found);
            }
        }
        assertEquals(1, found.size(),
                "文件「" + fileName + "」在 z-mq-* 各模块的 src 下应当唯一，实际找到 " + found.size()
                        + " 个: " + found);
        return found.get(0);
    }

    private static void collectNamed(File dir, String fileName, List<File> out) {
        if (dir == null || !dir.isDirectory()) {
            return;
        }
        File[] children = dir.listFiles();
        if (children == null) {
            return;
        }
        Arrays.sort(children);
        for (File child : children) {
            if (child.isDirectory()) {
                collectNamed(child, fileName, out);
            } else if (child.getName().equals(fileName)) {
                out.add(child);
            }
        }
    }

    private static void collectJava(File dir, List<String> out) {
        if (dir == null || !dir.isDirectory()) {
            return;
        }
        File[] children = dir.listFiles();
        if (children == null) {
            return;
        }
        Arrays.sort(children);
        for (File child : children) {
            if (child.isDirectory()) {
                collectJava(child, out);
            } else if (child.getName().endsWith(".java")) {
                out.add(child.getPath());
            }
        }
    }

    private static void walk(File dir, String needle, Scan scan) throws IOException {
        File[] children = dir.listFiles();
        if (children == null) {
            return;
        }
        Arrays.sort(children);
        for (File child : children) {
            if (child.isDirectory()) {
                walk(child, needle, scan);
                continue;
            }
            if (!child.getName().endsWith(".java")) {
                continue;
            }
            List<String> lines = Files.readAllLines(child.toPath(), StandardCharsets.UTF_8);
            boolean insideBlock = false;
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line.contains(needle)) {
                    String stripped = stripComment(line, insideBlock);
                    scan.hits.add(new Hit(child.getPath(), i + 1,
                            stripped != null && !stripped.trim().isEmpty()));
                }
                insideBlock = trackBlockComment(line, insideBlock);
            }
        }
    }

    /** 整行是注释（或在块注释里）时返回 null. */
    private static String stripComment(String line, boolean insideBlockComment) {
        if (insideBlockComment) {
            return null;
        }
        String trimmed = line.trim();
        if (trimmed.startsWith("//") || trimmed.startsWith("*") || trimmed.startsWith("/*")) {
            return null;
        }
        int slash = line.indexOf("//");
        if (slash >= 0) {
            return line.substring(0, slash);
        }
        return line;
    }

    private static boolean trackBlockComment(String line, boolean inside) {
        String trimmed = line.trim();
        if (inside) {
            return !trimmed.contains("*/");
        }
        int open = trimmed.indexOf("/*");
        if (open < 0) {
            return false;
        }
        return trimmed.indexOf("*/", open + 2) < 0;
    }

    private static String oneLine(String body) {
        return body.replace('\n', ' ').replace('\r', ' ');
    }

    private static void report(String what, Scan scan) {
        System.out.println("[w2g-wiring] " + what + ": raw=" + scan.rawHitLines()
                + " code=" + scan.codeHitLines() + " hits=" + scan.describe());
    }

    private static File resolveRepoRoot() {
        File dir = new File("").getAbsoluteFile();
        for (int i = 0; i < 6 && dir != null; i++) {
            if (new File(dir, "z-mq-client").isDirectory() && new File(dir, "z-mq-broker").isDirectory()) {
                return dir;
            }
            dir = dir.getParentFile();
        }
        throw new IllegalStateException("找不到仓库根（从 " + new File("").getAbsoluteFile()
                + " 往上六层里没有同时含 z-mq-client 与 z-mq-broker 的目录）");
    }
}
