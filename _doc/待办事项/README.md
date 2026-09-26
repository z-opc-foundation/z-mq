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

**登记口径**（2026-09-26 23:06–23:12 实测后更正）：先前这里写过"另有一批登记未修的缺陷不在这里，逐条在战役卡 §6"——
那句话把两类东西混在了一起。实际是：**需要拍板的**在上面的表里；**不需要拍板、只需要排产归属的**从 feature005 起也进本册。
顺带修掉当时那句说过头的表述："~~`DataVersion` 从不落盘~~" ⇒ 实测准确的是
**版本号会被 JSON 编码进 `GET_ALL_*` 响应载荷（`BrokerOutAPI:87/97/111`），但没有一处写进磁盘文件，且三条版本里两条的 counter 永不自增**，
证据与复跑命令在 `feature005_metadata_lifecycle/001_待排产.md`。

约定：
- 数字**一律现场测**，不抄本文档里的旧数；每份文档都写了取数命令。
- 拍完 ≠ 做完：拍完之后按文档里"票在哪"那节开工，收口要我自己本机 clean + 250 各复跑一遍。
- 这里只是"等拍板"的登记面。战役工单在 `z-opc-foundation-lead/002_项目需求/002_任务队列/pending/TASK-20260925-038.md`。
