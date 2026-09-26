# 待办事项索引

这里放**等主编/用户拍板才能开工**的事项。每条一个 `featureNNN_<名字>/` 目录，目录里 `001_*.md` 写清：
广告原文、**实测**兑现现状（带命令与读数）、等谁拍什么、拍完之后做什么。

| 目录 | 等什么 | 卡住的范围 | 状态 |
|---|---|---|---|
| `feature001_acl/` | 用户拍 3 问 | README:127「ACL 访问控制（`aclEnable=true`）」 | 待裁定 |
| `feature002_sync_master/` | 用户拍 2 问 | README:117/278「Master-Slave 同步双写（`SYNC_MASTER`）」 | 待裁定 |
| `feature003_release_1_3_0/` | 用户点头 2 件 | 发 Central `1.3.0`、抬 z-boot 的 `z-mq.version` pin | 待点头 |
| `feature004_transaction_check/` | W2f 证据先回来 | README:112「事务消息（两阶段提交 + 回查）」⇒ 回查需二次裁定 | 等证据 |
| `feature005_metadata_lifecycle/` | 不等裁定，等排产归属 | `DataVersion` 两条恒 0 且不落盘、`DELETE_TOPIC` 有码无处理器、17 个零引用协议码 | 待排产 |
| `feature006_dlq_topic_registration/` | 用户拍板（范围扩张） | 死信 Topic `%DLQ%{group}` 要显式注册才投得出去：client 零建 topic 口（0 命中 vs 5 文件阳性对照）、broker 无 autocreate ⇒ 三选一（broker 特例自动登记 / client 启动登记 / 诚实化改文档） | 待裁定 |
| `feature007_dlq_read_side/` | 不等裁定，等 W4 归属 | `pollAll()` 在 src/main 仍 0 读者（守卫被 `getDlqTopic` 满足 ⇒ 别把守卫绿当闭环完）；进程内死信缓存要不要一个批量取走口 | 待排产 |
| `feature008_batch_commit_coupling/` | 用户拍 2 问（提交点粒度、要不要写进文档） | 失败批次的位点提交是**整批**判的（`DefaultMQPushConsumer` 的 `handleConsumeFailure`，`main = 903f8a2` 上实测 `:366-384`；判据 `grep -n 'handleConsumeFailure\|allPersisted'` 现读，别抄行号）⇒ 一条没后继拖着全批重拉、已落盘的那几条各再多一份副本；这个形状零用例覆盖 | 待裁定 |
| `feature009_test_port_collision/` | 不等裁定，等 W4 归属 | 5 个起真监听的测试类把端口写死（10+1 个五位字面量）⇒ 同机并跑必 bind 失败，实测吞掉 `HAServiceTest` 整类 7 格只留 1 条错误读数；**另实测一次 `freePort()` 自己也会红**（探到就 close ⇒ TOCTOU，本机 load 28 那轮 `TopicConfigRestartE2ETest` 中招）⇒ 换写法只是降概率 | 待排产 |
| `feature010_push_offset_silent_commit_failure/` | 不等拍板，等排产归属（W2i 或 W4） | **未改动的 `d703508` 上跑全套，每轮都有 1–2 条真实的位点提交失败被吞**（7 遍本机 clean 实测：`IT_PUSH_CONSUMER` 7/7、`tx-message-group` 6/7，全是 `err=null` ⇒ 异常类型丢了）；W2h 装了出口（`getOffsetCommitFailures()` + 结构守卫）但**这 2 条落在哪一路、为什么失败没人断**——计数器只在各自消费者实例上，跨测试类不汇总 | 待排产 |
| `feature011_jvm_pause_drops_broker_response/` | 不等拍板，等排产归属 | 本机 12 轮 clean 里 **1 轮 1 条**具名红：通道已建好（server 绑定成功 + `channelActive`）而一条请求 10s 无响应 ⇒ `RemotingTimeoutException`。**不是 feature009 的撞号**：超时点前整个 JVM **7.715s 零日志输出**（05:35:13.082 → 05:35:20.797），像停顿不像丢包；同形状那条出自 W2e-R 的树（387 份日志里共 2 份）⇒ 与 W2h 无关，已三条尺分开。证"停顿"要开 fork 的 GC/安全点日志（**动 `<argLine>` ⇒ 排产决定**） | 待排产 |
| `feature012_producer_retry_uncovered_shapes/` | 不等拍板，等排产归属（W2i 或 W4） | Producer 重试那一波（`wip/w2g-0927 = b0f06e1`，本机两轮 clean 710/0）**交付了但留了五项没测的形状**：退避封顶/`base<=0` 两支 0 用例（test 侧 `FlowControlAware` 0 命中 vs main 侧 4）、第 5/6 档（写失败/超时）无真 remoting E2E（那支 E2E 只有 3 支且全走 `RemotingConnectException`）、异步/oneway/指定队列/半消息"不进循环"只有机检守卫无行为测、`sendRetryCount` 并发精确性 0 构造、两机对账未证（250 腿阻塞）。⚠ 被登记的代码**还不在 `main`**，命令里的 `SRC` 是那棵工作树 | 待排产 |

**登记口径**（2026-09-26 23:06–23:12 实测后更正）：先前这里写过"另有一批登记未修的缺陷不在这里，逐条在战役卡 §6"——
那句话把两类东西混在了一起。实际是：**需要拍板的**在上面的表里；**不需要拍板、只需要排产归属的**从 feature005 起也进本册。
顺带修掉当时那句说过头的表述："~~`DataVersion` 从不落盘~~" ⇒ 实测准确的是
**版本号会被 JSON 编码进 `GET_ALL_*` 响应载荷（`BrokerOutAPI:87/97/111`），但没有一处写进磁盘文件，且三条版本里两条的 counter 永不自增**，
证据与复跑命令在 `feature005_metadata_lifecycle/001_待排产.md`。

约定：
- 数字**一律现场测**，不抄本文档里的旧数；每份文档都写了取数命令。
- 拍完 ≠ 做完：拍完之后按文档里"票在哪"那节开工，收口要我自己本机 clean + 250 各复跑一遍。
- 这里只是"等拍板"的登记面。战役工单在 `z-opc-foundation-lead/002_项目需求/002_任务队列/pending/TASK-20260925-038.md`。
