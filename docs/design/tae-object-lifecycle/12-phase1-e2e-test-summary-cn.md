# Lifecycle Phase 1 E2E 测试结果总结

> 更新：2026-08-11
>
> 对应实现：PR [#26655](https://github.com/matrixorigin/matrixone/pull/26655)、Issue [#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 实现方案定稿：[11-domain-model-and-data-flow-cn.md](11-domain-model-and-data-flow-cn.md)
>
> 使用与领导演示：[13-leader-demo-and-usage-guide-cn.md](13-leader-demo-and-usage-guide-cn.md)
>
> 结论：**Phase 1 的核心功能正确性已获得多层 E2E 证据，Commercial GA 仍为 Conditional Go。**

本文只汇总已经实际执行并保留证据的测试，不把设计目标、单元测试推断或后续计划写成
已完成认证。它是 10 号验证基线的阶段性证据快照，不替代其 Gate I、Stop-Ship 和问题
分类规则。

## 1. 一句话结论

在正确启动的 `1 Log + 1 TN + 2 CN + 1 Proxy + MinIO` Kind 集群中，当前 Phase 1 已经
实际跑通 Whole Archive、Mixed Rewrite、Dataset Restore、按时间范围 Restore、`SHOW
LIFECYCLE RESTORES`、已提交 DML 后的归档/恢复，以及若干关键故障和 CN 状态点重启。

所有已完成样本均满足以下核心不变量：

1. Archive 未经完整 Provider readback 和普通 final transaction 确认时，源数据不会退休；
2. 成功发布后，active 数据和 Restore 数据的行、ID、reference ledger 或逐列内容守恒；
3. `COMMIT_UNKNOWN` 保持 fail-closed，Payload 不被 Cleanup 删除；
4. 明确失败的外部副作用会由 Cleanup Root 在延迟与 quiescence 后收敛，而不会误标
   `CLEANED`；
5. Restore 失败/重试不会重复导入 Chunk 或留下隐藏 staging 表。

这不是 1/10 TiB、500--1000 Binding、真实云 Provider 或长期 soak 的完成声明。未完成项
列在第 8 节。

## 2. 版本、环境与证据口径

测试证据跨越实现迭代，不能抹去版本差异。结论按下表使用：

| 证据组 | 代码 SHA | 拓扑 | 可用于证明 | 不应外推为 |
| --- | --- | --- | --- | --- |
| 当前正确性与故障复验 | `07e649b31976d8c6e27eb3d3ee42137a7f06a257` | 1 Log、1 TN、2 CN、1 Proxy、MinIO | 主链、小中数据正确性、真实 Stage Provider 故障、CN 状态点、Restore 重试、类型矩阵 | TN 状态点重启、真实 commit response lost、TB/soak |
| Range Restore / SHOW 复验 | `f81c54092cf1d51149c981aba6807da52c767173` | 同类 2 CN 正确拓扑 | 范围选择、冻结 Attempt、范围恢复响应丢失和 SHOW 可见性 | 当前 Head 上的全部稳定性认证 |
| 规模与 TN 重启复验 | `6ef133fb4b33da4f0f33709beb5f46ed89084de5` | 1 Log、1 TN、2 CN、1 Proxy、MinIO，独立 hostPath 数据根 | 3300 万行流、已提交 DML、正确拓扑下的 TN 重启 | 当前 Head 的逐 SHA 回归、1/10 TiB 或长期稳定性 |
| 亿级规模参考 | `6ef133fb4b33da4f0f33709beb5f46ed89084de5` | 单 all-in-one MO + MinIO，MO 本地数据面 ephemeral | 时间有序流的 Archive/Restore 吞吐和 Parquet 修复后无旧 panic | 持久化重启、2 CN 高可用或 Commercial GA |

所有多节点样本均显式使用：

```yaml
command: ["/mo-service"]
args: ["-cfg", "/etc/mo-config/<component>.toml"]
```

并核对 `/proc/1/cmdline` 不含镜像默认的 `-launch`。这一步很重要：早期把 `-cfg` 仅追加到
镜像 Entrypoint 后，每个 Pod 都错误启动一套 QuickStart，多个 TN 写入同一 checkpoint
namespace；该环境产生的 LSN 逆序 panic 已判为**无效测试环境**，不用于本功能结论。

所有 Archive 样本使用隔离 tenant、database、Stage 和 MinIO prefix；小数据和流式样本使用
`mo_ctl('dn','flush', ...)` 让数据及时形成可扫描 Object。flush 仅是测试准备，不代表生产
环境的异步 Object 边界或归档时延承诺。

原始 SQL、集群日志、MinIO LIST 和汇总 TSV 存在测试工作区的
`test/lifecycle-demo/artifacts/` 与 `test/lifecycle-soak/artifacts/`；这些运行 artifact 不随
本设计分支提交。每次复验必须生成新的隔离 scope 和 artifact，不能以旧 Root 或旧 Dataset
替代新样本。

## 3. 正常 Archive / Restore 主链

| 场景 | 实测结果 | 结论 |
| --- | --- | --- |
| Whole Archive / Restore | 源表由 8 行变为 0 行；一个 `PUBLISHED` Dataset；恢复表 8 行，ID 和 36 | 通过 |
| Mixed Rewrite / Restore | 原表 16 行中，8 条过期行归档并恢复，8 条未来行保留 active；active ID 和 16036，Restore ID 和 8036 | 通过 |
| 小流式时间序列 | 初始 100,000 行、归档期间再写 20,000 行；active=50,000、Restore=70,000、10 个 `PUBLISHED` Dataset；合计 ID 和 7,200,060,000 | 通过 |
| 33,000,000 行时间序列 | 21,000,000 行 Archive、12,000,000 行 active、42 个 `PUBLISHED` Dataset；active 与 Restore ID 和合计 544,500,016,500,000 | 通过（历史规模证据） |
| 110,000,000 行时间有序流 | 70,000,000 行 Archive、40,000,000 行 active、5 个 Dataset 和 5 个 `DONE` Restore；ID 和合计 6,050,000,055,000,000 | 通过（单 Pod 历史规模证据） |

33M 和 110M 样本模拟的是“历史段持续退休、未来在线数据持续到达”的时间有序业务流，不是
只有两个固定日期的静态样本。110M 的第一个源 TAE Object 约 2,211 万行；Lifecycle 会把
源端大 Object 切分为多个 Parquet Payload 与 Manifest，不会把全部数据写成单一超大对象。

高混合比例样本还验证了保护性拒绝：在 110M 行、70% 历史 / 30% 未来的交错布局中，已有
69,973,882 行成功发布；剩余 26,118 行因
`MIXED_LAYOUT_BLOCKED: rewrite amplification 27.60 exceeds 20.00` 被延后，源行仍可见且
未产生错误 Dataset。这是 Mixed Rewrite 写放大保护的 fail-closed 行为，不是数据丢失或
Parquet panic。它也说明“每一条过期行立即归档”不是 Phase 1 的无条件承诺。

## 4. 数据正确性：DML、Ledger 与类型

### 4.1 已提交 UPDATE / DELETE 后归档

3,000,000 行混合日期样本在绑定前完成过期/活跃行 UPDATE、两类 DELETE 与 Lifecycle 列
前后迁移。最终：

| 检查项 | 实测值 |
| --- | ---: |
| Archive / Restore 行 | 2,030,000 |
| active 行 | 870,000 |
| active + Restore ID 和 | 4,485,001,450,000 |
| `updated-old` | 仅在 Restore 中且保持更新值 |
| `updated-live` | 仅在 active 表中且保持更新值 |
| 两段删除 ID | active 与 Restore 均不存在 |

另一个 12,000 行独立 reference ledger 样本的预期为 8,500 Archive、2,800 active、700
DELETE。最终 `active_mismatch`、`archive_mismatch`、`missing_rows`、`unexpected_rows`、
`duplicate_ids` 和 `deleted_leaks` 均为 0。该 oracle 通过双向差集检查完整行内容，证明力
强于只比较 count 或 ID sum。

### 4.2 post-S 并发 DML

在 Root 已处于 `FINALIZING`、但 final transaction 尚未开始时暂停，再执行 DML 并释放：

| 场景 | 结果 |
| --- | --- |
| Whole Archive + DELETE source 行 | 发生写写冲突，Root 为 `DELETE_PENDING`，Dataset=0，源表保留 DELETE 后的 3 行 |
| Mixed Archive + DELETE L | 成功发布 4 行 Dataset，live DELETE 经现有 TransferTable 转移；active=3、Restore=4，ledger 无重复或泄漏 |
| Mixed Archive + DELETE E | 保守 abort，Dataset=0，源表正确保留 7 行，未发布包含已删除 E 的 Archive |
| Mixed Archive + UPDATE L | 仅保留更新后 live 值；Archive 仅含 E，联合无重复 ID |

这组测试使用了一个默认关闭、仅供 E2E 的本地 `before-final-commit-barrier`。它使等待释放后
继续同一成功 finalization 路径；现有普通 `WAIT` fault 释放后会直接返回注入错误，无法证明
该语义。此 barrier 当时尚未随 PR 提交，因此该组结果是重要功能证据，但不能伪装成“裸
PR Head 已不经测试辅助覆盖”的证据。

### 4.3 类型与 AUTO_INCREMENT

宽表 Archive / Restore 覆盖 signed/unsigned BIGINT、DECIMAL(20,4)、DATE、DATETIME、
TIMESTAMP、CHAR/VARCHAR/TEXT、BLOB、JSON、BOOL、DOUBLE 的 `-0` 和全 NULL 行。独立
expected ledger 对每列做 NULL-safe 双向比较，差集均为 0；恢复后 AUTO_INCREMENT 下一值为
3，大于归档最大 id=2。

## 5. Range Restore 与可观测性

| 场景 | 实测结果 | 结论 |
| --- | --- | --- |
| 单 Dataset 范围 | `[2020-02-01, 2020-03-01)` 恢复 ID 2、3、4，共 3 行，ID 和 9；左端包含、右端排除 | 通过 |
| 跨 Dataset 范围 | 从 10 个 Dataset 选择并恢复 17,493 行，ID 和 874,502,559；与逐 Dataset 独立基线相同 | 通过 |
| Attempt 冻结和重试 | 同一 RANGE 重试不新增 Attempt、不重复导入；后续 Dataset 不会混入已冻结 Attempt | 通过 |
| 副作用前拒绝 | 空区间、无交集、既有目标表与非法 SHOW 分页均在创建 Attempt、lease、隐藏表或 Provider GET 前失败 | 通过 |
| Restore 发布响应丢失 | `after-restore-publish` 首次 SQL 返回错误，但目标表和唯一 `RANGE/DONE` Attempt 已提交；重试仍为 3 行、Attempt 数为 1 | 通过 |
| `SHOW LIFECYCLE RESTORES` | 显示 `scope`、Dataset/Chunk 数、`lifecycle_column_type=DATETIME`、范围端点、deadline、state、恢复行数；LIMIT/OFFSET 和租户隔离有效 | 通过 |

范围语义固定为 `[from,to)`。范围 Restore 发布独立新表，不覆盖当前源表。当前 SHOW 的范围端点
使用内部 canonical bigint 展示，逻辑与筛选正确；这属于可观测性显示改进项，不是数据正确性
缺陷。

## 6. 故障、Cleanup 与重启

### 6.1 真实 Stage Provider 与 Cleanup 闭环

故障代理位于 Lifecycle Archive Stage endpoint 前，并以代理访问日志确认请求实际经过代理，
不是对 CN 的 SHARED FileService 做无效代理。

| 故障 | 必须满足的合同 | 实测结果 |
| --- | --- | --- |
| Payload PUT = 503 | 源可见、Dataset=0、外部对象最终清理 | 通过；Root `CLEANED`，精确 MinIO prefix 为空 |
| full-readback Payload GET = 503 | 已写 Payload/Manifest 也不得发布 | 通过；源 4 行、Dataset=0，Root `CLEANED`、prefix 空 |
| Manifest PUT = 429 | 不退休源、不发布半 Dataset | 通过；SDK 重试后源 4 行、Dataset=0，Root `CLEANED`、prefix 空 |
| Cleanup LIST 失败 | 不得错误标为 `CLEANED` | 通过；先保持 `DELETING`，移除故障并经过 10 分钟 quiescence 后 `CLEANED`，prefix 空 |
| before-full-readback | 未完成 readback 不得退休源 | 通过；源 4 行、Dataset=0、Root `DELETE_PENDING`；另一个独立样本已自然收敛为 `CLEANED` 且 prefix 空 |

已观察到的完整失败闭环是：失败时源数据保留 → Root 延迟重试 → Provider 恢复 → LIST/Delete →
quiescence → `CLEANED`。没有通过手工删除 Catalog 行或对象伪造收敛。

### 6.2 CN 状态点与 Unknown

- CN 在 `after-payload-put` 暂停并严格 `scale 0 → 确认旧 Pod 退出 → scale 1`：旧 Root 在
  lease deadline 后进入 `DELETE_PENDING`，随后 `CLEANED`，prefix 为空；fresh attempt 只发布
  一个 4 行 Dataset，Restore 逐行正确。
- CN 在 final transaction 前崩溃：Root 到 deadline 后进入 `COMMIT_UNKNOWN`，Dataset=0、源
  表仍为 3 行；Stage prefix 中 2 个 Manifest 和 1 个 Payload 被保留，后续同 source 不允许
  新 retirement。这符合 unknown 不猜测、不误删的合同。

上面的 Unknown 是 FINALIZING 崩溃样本，不等价于“TN 已提交而 CN 丢失 commit response”。
真实 commit-response-lost 仍未覆盖。

### 6.3 Restore 重试

| 场景 | 实测结果 |
| --- | --- |
| before-restore-publish | 首次失败后正式目标表不可见；唯一 Attempt 保持 `IMPORTING`；解除故障重试使用同一 restore_id 并 `DONE`，目标 4 行、hidden staging=0 |
| after-restore-chunk，多 Chunk Dataset | 510,362 行、8 Chunk；首次 ordinal 0 已提交，重试复用同一 Attempt；最终 rows/distinct IDs=510,362/510,362，Receipt ordinal 连续 0--7，hidden staging=0 |

### 6.4 TN 重启

在正确的独立 hostPath 数据根与单一 HAKeeper/TN 拓扑中，TN 使用 `scale 0 → 等待完全退出 →
scale 1` 重启，TAE checkpoint 加载和 WAL replay 正常完成；没有 `failed to get tn shard ID`、
LSN 单调性异常或 panic。重启后，33M 流 active 表仍为 12,000,000 行，DML 源表仍为 870,000
行，系统表为 65 个 `PUBLISHED` Root 和 1 个故障样本 Root。

这证明该版本和拓扑下的普通 TN 持久恢复主链可用；它不替代 SyncProtection 已注册、final
prepare/commit、Restore chunk 等精确 TN 状态点重启测试。

## 7. 本阶段问题分类

| 项目 | 结论 |
| --- | --- |
| Parquet `GenericWriter[any]` 大 Mixed panic | 已修复后通过大规模时间有序样本；旧 `[3520:3480]` slice-bounds panic 不再出现 |
| 旧 2CN/TN checkpoint LSN 逆序 panic | 无效 Kubernetes 启动方式造成多个 QuickStart/TN 共用 checkpoint namespace，不归因 Lifecycle PR；已通过显式 `command/args` 和独立数据根纠正 |
| 高混合布局 `MIXED_LAYOUT_BLOCKED` | 保护性 fail-closed 拒绝，未丢数据；属于产品限制/布局与阈值认证问题，不是成功归档的错误声明 |
| 早期 Python S3 代理写出 0 字节 Manifest | 代理未完整转发 chunked PUT 的测试工具问题；这些样本无效，已改用完整 Nginx 反代重测 |
| `await-cleanup` 仅等待 15 分钟 | 测试 harness 等待小于默认 `cleanup_after + quiescence`；已以更长实际时间窗口验证独立 Root 的最终 `CLEANED`，脚本超时本身不是产品泄漏 |

本次证据未发现“未确认 Archive 即退休 source”、“PUBLISHED Dataset 丢行/重复恢复”或
“Cleanup 删除 `COMMIT_UNKNOWN` Payload”的 Lifecycle 产品缺陷。

## 8. 未完成认证与下一步

下列项目没有被本报告标为通过，仍按
[10-validation-debug-baseline-cn.md](10-validation-debug-baseline-cn.md) 的 Gate I 执行：

1. Provider timeout、PUT 已成功但响应丢失、迟到 PUT、Cleanup DELETE response-lost、内容截断/
   篡改、持续 429/503 与 Retry-After；
2. 普通 Merge 抢先退休同 source、Lifecycle 先提交后普通 Merge，以及真实 exact source CAS
   压力；
3. TN 在 SyncProtection、final prepare/commit 附近重启；Restore chunk 中断、Purge/lease
   竞争和真实 commit-response-lost；
4. 时间范围恰好命中 4096 / 4097 Dataset 的副作用前边界；
5. 1 TiB 常见规模、10 TiB 认证规模、50→200→500→1000 Binding active coexistence；
6. 24 小时、7 天和 30 天 soak，以及普通 DML/Merge/checkpoint/GC/logtail 的长期 P99、
   backlog、heap 和对象数量门禁；
7. 当前 PR Head 改动后，对历史 33M/110M 规模样本的逐 SHA 重跑。

因此发布口径应为：**Phase 1 具备进入稳定性、容量与放量认证的功能正确性基础；尚不具备
Commercial GA 已认证完成的声明条件。**

## 9. 复验要求

每次后续修复或 Head 变更后，至少重新执行：

1. fresh bootstrap、Whole、Mixed、readback fault 与最终 Cleanup；
2. reference ledger、Range Restore、`SHOW LIFECYCLE RESTORES`；
3. 本次改动涉及的故障/重启边界；
4. 受影响规模阶梯，而不是仅复用旧 artifact；
5. `Dataset`、`Restore Attempt`、SYS `Cleanup Root`、精确 MinIO prefix、Pod 重启和日志的
   联合取证。

任何新的 panic、源数据在无 `PUBLISHED` Dataset 时消失、active/Restore ledger 差集非零、
`COMMIT_UNKNOWN` Payload 被删除，或终态 Root/隐藏表/lease 持续增长，均为 Stop-Ship。
