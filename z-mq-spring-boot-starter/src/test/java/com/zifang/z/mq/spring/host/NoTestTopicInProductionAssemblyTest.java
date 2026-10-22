package com.zifang.z.mq.spring.host;

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
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 静态守卫：<b>生产源码里不许再出现「测试 topic」与「启动即建 topic」的装配</b>（TASK-20260925-024）。
 *
 * <p>为什么行为用例之外还要一把静态尺：行为用例（{@link ZmqHostAssemblyNoTestTopicTest}）
 * 证明的是「这一套宿主形态启动后没建 topic」；但如果哪天有人把那段代码换个形状搬回来
 * （比如不再叫 {@code ZMqTopicBootstrap}、或者换个 topic 名），行为用例可能仍然绿 ——
 * 因为它只量「那个名字的 topic」。这把尺量的是<b>形状</b>：
 * <ol>
 *   <li>全仓 {@code src/main} 里不许再出现测试 topic 名 {@code z-opc-test}（含消费者组名）；</li>
 *   <li>生产 starter 的 {@code src/main} 里不许再有任何「往 namesrv 建 topic」的写码调用
 *       （{@code RequestCode.UPDATE_AND_CREATE_TOPIC}）—— 写码白名单那道闸（只读码）也管着它；</li>
 *   <li>生产 starter 的 {@code src/main} 里不许再有 topic 预创建器这类类（文件名/类名层面）。</li>
 * </ol>
 *
 * <p><b>尺自检</b>（否则「0 命中」可能只是没扫到东西）：
 * <ul>
 *   <li>分母：扫到的 java 文件数必须够大（否则是空跑）；</li>
 *   <li>阳性对照：同一把尺必须能在 <b>src/test</b> 里量到那两行（测试夹具确实用了写码）——
 *       量不到就说明这把尺根本看不见写码。</li>
 * </ul>
 *
 * <p>注释必须剥掉再数：{@code MqHostAutoConfiguration} / {@code MqProxyController} 的 javadoc 里
 * 会提到这段历史（那是**说明**，不是**装配**）—— 不剥注释的话守卫会把自己人当违规。
 */
public class NoTestTopicInProductionAssemblyTest {

    /** 曾经被生产 starter 自动创建的测试 topic。 */
    private static final String LEGACY_TEST_TOPIC = "z-opc-test";
    /** 曾经被生产 starter 自动挂上的测试消费者组。 */
    private static final String LEGACY_TEST_GROUP = "z-opc-test-group";
    /** 生产 starter 里那个建 topic 的写码（写码白名单之外的东西）。 */
    private static final String CREATE_TOPIC_CODE = "RequestCode.UPDATE_AND_CREATE_TOPIC";
    /** 旧预创建器的类名。 */
    private static final String LEGACY_BOOTSTRAP_CLASS = "ZMqTopicBootstrap";

    @Test
    @DisplayName("生产源码里没有测试 topic 名、没有启动即建 topic 的写码")
    public void productionSourcesCarryNoTestTopicNorTopicCreation() throws IOException {
        // ① 全仓 src/main：测试 topic 名 / 消费者组名一律不许出现
        Scan topics = scanSrcMain(LEGACY_TEST_TOPIC);
        System.out.println("[zmq024-guard] 全仓 src/main 扫了 " + topics.javaFiles
                + " 个 java 文件，命中 \"" + LEGACY_TEST_TOPIC + "\" " + topics.hitLines + " 行 " + topics.describeHits());
        assertTrue(topics.javaFiles >= 50,
                "只扫到 " + topics.javaFiles + " 个 src/main 文件 ⇒ 分母太小，这把尺是空跑");
        assertEquals(0, topics.hitLines,
                "生产源码里又出现了测试 topic 名 \"" + LEGACY_TEST_TOPIC + "\"（" + topics.describeHits()
                        + "）⇒ 测试产物重新长回了生产装配");

        Scan groups = scanSrcMain(LEGACY_TEST_GROUP);
        assertEquals(0, groups.hitLines,
                "生产源码里又出现了测试消费者组名 \"" + LEGACY_TEST_GROUP + "\"（" + groups.describeHits() + "）");

        // ② 生产 starter 的 src/main：写码（建 topic）不许有
        Scan writes = scanModuleSrcMain("z-mq-spring-boot-starter", CREATE_TOPIC_CODE);
        System.out.println("[zmq024-guard] starter src/main 写码命中 " + writes.hitLines + " 行 " + writes.describeHits());
        assertEquals(0, writes.hitLines,
                "生产 starter 里又有往 namesrv 建 topic 的写码 " + CREATE_TOPIC_CODE + "（" + writes.describeHits()
                        + "）⇒ 启动即写生产集群的那条边又回来了");

        // ③ 生产 starter 的 src/main：旧预创建器不许复活（文件层面）
        File legacy = new File(moduleDir("z-mq-spring-boot-starter"),
                "src/main/java/com/zifang/z/mq/spring/host/" + LEGACY_BOOTSTRAP_CLASS + ".java");
        assertTrue(!legacy.exists(),
                "生产 starter 里又出现了 " + legacy.getName() + " ⇒ 端到端测试产物又住回生产 jar 了");

        // ④ 尺自检（阳性对照）：同一把尺必须能在 src/test 里量到那两行
        Scan testScope = scanModuleSrcTest("z-mq-spring-boot-starter", CREATE_TOPIC_CODE);
        System.out.println("[zmq024-guard] starter src/test 写码命中 " + testScope.hitLines + " 行 " + testScope.describeHits());
        assertTrue(testScope.javaFiles >= 1, "src/test 一行都没扫到 ⇒ 这把尺的阳性对照没法成立");
        assertTrue(testScope.hitLines >= 1,
                "尺的阳性对照失败：测试夹具里应当有建 topic 的写码 " + CREATE_TOPIC_CODE
                        + "，却量到 0 ⇒ 这把尺看不见写码，上面那些 0 命中都是空跑");

        // ⑤ 尺自检（阴性对照）：不存在的针必须 0 命中
        Scan absent = scanSrcMain("w4dNeedleThatCannotExistInThisRepo_9f31ab(");
        assertEquals(0, absent.hitLines, "不存在的针必须 0 命中: " + absent.describeHits());
    }

    // ==================== 扫描工具 ====================

    private static File repoRoot() {
        File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
        List<String> tried = new ArrayList<>();
        for (int up = 0; up < 6 && dir != null; up++) {
            tried.add(dir.getAbsolutePath());
            if (new File(dir, "z-mq-spring-boot-starter").isDirectory()
                    && new File(dir, "z-mq-broker").isDirectory()
                    && new File(dir, "README.md").isFile()) {
                return dir;
            }
            dir = dir.getParentFile();
        }
        fail("定位仓根失败（user.dir=" + System.getProperty("user.dir") + "），试过 " + tried);
        return null;
    }

    private static File moduleDir(String module) {
        File dir = new File(repoRoot(), module);
        if (!dir.isDirectory()) {
            fail("找不到模块目录：" + dir);
        }
        return dir;
    }

    private static Scan scanSrcMain(String needle) throws IOException {
        Scan scan = new Scan(needle);
        File root = repoRoot();
        File[] children = root.listFiles();
        assertTrue(children != null, "列仓根失败：" + root);
        Arrays.sort(children);
        for (File module : children) {
            if (!module.isDirectory() || !module.getName().startsWith("z-mq-")) {
                continue;
            }
            File srcMain = new File(module, "src/main");
            if (srcMain.isDirectory()) {
                walk(srcMain, scan);
            }
        }
        return scan;
    }

    private static Scan scanModuleSrcMain(String module, String needle) throws IOException {
        Scan scan = new Scan(needle);
        walk(new File(moduleDir(module), "src/main"), scan);
        return scan;
    }

    private static Scan scanModuleSrcTest(String module, String needle) throws IOException {
        Scan scan = new Scan(needle);
        walk(new File(moduleDir(module), "src/test"), scan);
        return scan;
    }

    private static void walk(File file, Scan scan) throws IOException {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children == null) {
                throw new IOException("列目录失败：" + file.getAbsolutePath());
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
        String text = stripComments(new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
        String[] lines = text.split("\r?\n", -1);
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].contains(scan.needle)) {
                scan.hitLines++;
                scan.hits.add(file.getName() + ":" + (i + 1));
            }
        }
    }

    /** 剥掉块注释与行注释（字符串字面量保留）。 */
    private static String stripComments(String src) {
        StringBuilder out = new StringBuilder(src.length());
        boolean inBlock = false;
        boolean inLine = false;
        boolean inString = false;
        boolean inChar = false;
        for (int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            char next = i + 1 < src.length() ? src.charAt(i + 1) : '\0';
            if (inBlock) {
                if (c == '*' && next == '/') {
                    inBlock = false;
                    i++;
                }
                continue;
            }
            if (inLine) {
                if (c == '\n') {
                    inLine = false;
                    out.append(c);
                }
                continue;
            }
            if (inString) {
                if (c == '\\') {
                    out.append(c);
                    if (next != '\0') {
                        out.append(next);
                        i++;
                    }
                    continue;
                }
                if (c == '"') {
                    inString = false;
                }
                out.append(c);
                continue;
            }
            if (inChar) {
                if (c == '\\') {
                    out.append(c);
                    if (next != '\0') {
                        out.append(next);
                        i++;
                    }
                    continue;
                }
                if (c == '\'') {
                    inChar = false;
                }
                out.append(c);
                continue;
            }
            if (c == '/' && next == '/') {
                inLine = true;
                i++;
                continue;
            }
            if (c == '/' && next == '*') {
                inBlock = true;
                i++;
                continue;
            }
            if (c == '"') {
                inString = true;
                out.append(c);
                continue;
            }
            if (c == '\'') {
                inChar = true;
                out.append(c);
                continue;
            }
            out.append(c);
        }
        return out.toString();
    }

    /** 一趟扫描的读数。 */
    private static final class Scan {
        private final String needle;
        private int javaFiles;
        private int hitLines;
        private final List<String> hits = new ArrayList<>();

        Scan(String needle) {
            this.needle = needle;
        }

        String describeHits() {
            return hits.isEmpty() ? "<无命中>" : hits.toString();
        }
    }
}
