# 待办事项索引

这里放**等主编/用户拍板才能开工**的事项。每条一个 `featureNNN_<名字>/` 目录，目录里 `001_*.md` 写清：
广告原文、**实测**兑现现状（带命令与读数）、等谁拍什么、拍完之后做什么。

| 目录 | 等什么 | 卡住的范围 | 状态 |
|---|---|---|---|
| `feature001_acl/` | 用户拍 3 问 | README:127「ACL 访问控制（`aclEnable=true`）」 | 待裁定 |
| `feature002_sync_master/` | 用户拍 2 问 | README:117/278「Master-Slave 同步双写（`SYNC_MASTER`）」 | 待裁定 |
| `feature003_release_1_3_0/` | 用户点头 2 件 | 发 Central `1.3.0`、抬 z-boot 的 `z-mq.version` pin | 待点头 |
| `feature004_transaction_check/` | W2f 证据先回来 | README:112「事务消息（两阶段提交 + 回查）」⇒ 回查需二次裁定 | 等证据 |

**另有一批"登记未修"的缺陷不在这里**（不需要拍板，只是排队）：`DELETE_TOPIC`/`UPDATE_AND_CREATE_TOPIC_LIST` 有码无处理器、
`DataVersion` 从不落盘、`TopicConfigManager.persist()` 咽 IOException 等，逐条在战役卡 `TASK-20260925-038.md` 的 W2d 收口节 §6。

约定：
- 数字**一律现场测**，不抄本文档里的旧数；每份文档都写了取数命令。
- 拍完 ≠ 做完：拍完之后按文档里"票在哪"那节开工，收口要我自己本机 clean + 250 各复跑一遍。
- 这里只是"等拍板"的登记面。战役工单在 `z-opc-foundation-lead/002_项目需求/002_任务队列/pending/TASK-20260925-038.md`。
