# TAE Object Lifecycle Commercial GA 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal：** 在不改变普通查询、普通 Merge 和现有 TAE GC 策略的前提下，交付可面向商业客户的表级 TTL/Archive：Whole Object 快速退休、小比例 Mixed Row DELETE、direct-readable Parquet/ZSTD、Restore 新表，以及失败后的可靠对账和清理。

**Architecture：** Lifecycle 是独立后台控制面。候选发现复用 `CollectObjectList` 和 Object Footer；数据导出使用固定 SI Snapshot 的高层 Reader；Whole Object 通过 tagged `StrictObjectRetire` 事务协议退休；Mixed 小尾部通过现有 `Relation.Delete` 退休；活动对象最终仍由现有 TAE GC 回收。第一次外部 PUT 前注册 system-owned Root，任何未知事务结果都由 Reconciler 查询事务状态并在一致性事务中读取 Receipt 后决策。

**Tech Stack：** Go、MatrixOne CN/TN 事务与 TAE、TaskService、MO Catalog cluster/system tables、ObjectStorage、Parquet/ZSTD、protobuf、SQL BVT、Go unit/integration/chaos tests。

---

## 1. 执行规则

### 1.1 开发循环

每个 Gate 都是一个可独立评审、可关闭开关、可回滚的新能力集合。只有当前 Gate 的测试全部通过，才允许进入下一个 Gate。

每个 Task 遵守同一循环：

1. 先增加失败测试，证明当前代码缺少该能力；
2. 只实现使当前测试通过的最小代码；
3. 运行包级、相关 CGo 和故障注入测试；
4. 按资源 Owner、失败路径、等待终止条件和重启恢复自审；
5. 形成一个语义完整的小提交；
6. 通过 Gate Review 后再进入下一 Gate。

禁止在同一个 PR 中顺带实施：

- 普通 Merge 策略优化；
- 普通 Reader 性能重构；
- TAE GC 算法重构；
- 通用事务 wire envelope 重构；
- ObjectStorage 全局 versioning 抽象；
- archive-aware Backup/DR；
- Deep Archive、Legal Hold、WORM。

### 1.2 推荐包边界

```text
pkg/lifecycle/
├── catalog/        # Catalog DAO、CAS、状态机，不执行对象 I/O
├── coordinator/    # 扫描、配额、公平调度、Job/Attempt 编排
├── objectindex/    # 仅绑定表的派生 Object Index
├── reader/         # 固定 Snapshot 精确对象读取
├── archive/        # Parquet、Manifest、校验、ArchiveStore
├── retire/         # Whole/Mixed 提交客户端与 Receipt 对账
├── cleanup/        # Root、Sweeper、Reconciler、Purge
├── restore/        # Restore Attempt、Chunk、发布
├── admission/      # Feature Guard 与支持矩阵
├── observability/  # 指标、状态摘要、诊断信息
└── testutil/       # fake provider、故障注入和状态机断言
```

依赖方向：

```text
coordinator
  -> catalog/objectindex/reader/archive/retire/cleanup/restore

archive/cleanup/restore
  -> ArchiveStore

retire
  -> engine.Relation / txn operator

TAE rpc
  -> 只认识 StrictObjectRetire wire，不依赖 pkg/lifecycle
```

`pkg/lifecycle` 可以调用稳定的 engine/fileservice/txn 接口；TAE 内核不能反向 import `pkg/lifecycle`。

### 1.3 Catalog 版本

本文按当前树中的 `pkg/bootstrap/versions/v4_0_5` 写实施路径。开发开始前由 Release Owner 确认：

- 该版本尚未冻结：直接增加升级项；
- 该版本已经冻结：先建立下一个版本目录，再机械替换本文的 `v4_0_5`；
- 禁止把新表放入已经发布且不会再次执行的旧升级步骤。

---

## 2. Gate A：产品契约、Catalog 与 Feature Guard

### Task A1：冻结 SQL AST 和错误码

**Files：**

- Modify: `pkg/sql/parsers/dialect/mysql/mysql_sql.y`
- Add: `pkg/sql/parsers/tree/lifecycle.go`
- Modify: `proto/plan.proto`
- Modify: `pkg/sql/plan/build_ddl.go`
- Modify: `pkg/frontend/stmt_kind.go`
- Modify: `pkg/common/moerr/error.go`
- Test: `pkg/sql/parsers/dialect/mysql/mysql_sql_test.go`
- Test: `pkg/sql/plan/build_ddl_test.go`

**实现：**

- [ ] 按 `01-product-sql-contract-cn.md` 增加 Profile、Binding、Pause/Resume、Dry-run、Restore、Purge 和 Show AST。
- [ ] Duration 同时保存原始 SQL 和规范化微秒；禁止用字符串比较周期。
- [ ] 校验 `expire_after/archive_after/late_arrival_grace/purge_after` 的顺序和互斥关系。
- [ ] 稳定区分“不支持、Guard 冲突、状态冲突、预算阻断、Mixed 布局阻断、结果未知”。
- [ ] 覆盖引用标识符、UTC 时间列、负周期、相同阈值、未知选项的 parser round-trip。

**验证：**

```bash
make generate
go test ./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan
```

**提交：** `feat(lifecycle): add lifecycle SQL syntax and stable errors`

### Task A2：创建 Catalog 表和升级测试

**Files：**

- Add: `pkg/bootstrap/versions/v4_0_5/lifecycle_catalog.go`
- Modify: `pkg/bootstrap/versions/v4_0_5/cluster_upgrade_list.go`
- Modify: `pkg/bootstrap/versions/v4_0_5/tenant_upgrade_list.go`
- Modify: `pkg/bootstrap/versions/v4_0_5/upgrade_test.go`
- Modify: `pkg/catalog/types.go`
- Add: `pkg/lifecycle/catalog/schema.go`
- Add: `pkg/lifecycle/catalog/schema_test.go`

**实现：**

- [ ] 按 `02-catalog-state-machine-cn.md` 创建 tenant cluster tables 和 system-account retained tables。
- [ ] 需要 CAS/索引的状态、version、epoch、identity 必须是独立列，不以 JSON 代替。
- [ ] 为调度查询建立有界索引：`binding.next_scan_at`、`job.state/lease_deadline`、`root.state/next_retry_at`。
- [ ] Object Index 主键为 `(account_incarnation, table_id, index_generation, object_id)`，不能使用全局自增 ID。
- [ ] 所有 tenant Lifecycle 表把 `account_incarnation` 放入主键/查询条件，防止 account ID 复用或迟到旧事务跨租户污染。
- [ ] Account identity/current 冻结权威 `mo_account` RowID；找不到或不匹配时禁止 Profile/Binding/Root 懒创建。
- [ ] Root Object 使用 attempt-scoped 主键，确保每个外部 key 只有一个清理 Owner。
- [ ] 测试空集群、已有租户、重复升级、中断后重试、多个 CN 同时观察版本。
- [ ] 降级前必须先关闭 Feature Guard 和后台任务；旧节点不得删除新 Catalog。

**验证：**

```bash
go test ./pkg/bootstrap/versions/v4_0_5 ./pkg/lifecycle/catalog
```

**提交：** `feat(lifecycle): bootstrap lifecycle catalog`

### Task A3：实现 Catalog DAO 和状态 CAS

**Files：**

- Add: `pkg/lifecycle/catalog/model.go`
- Add: `pkg/lifecycle/catalog/store.go`
- Add: `pkg/lifecycle/catalog/cas.go`
- Add: `pkg/lifecycle/catalog/state_machine_test.go`

**实现：**

- [ ] 每次转换都带旧 `state/version/epoch` 条件。
- [ ] DAO 返回 `Applied/NotApplied/Unknown`，不能把零 affected rows 一律解释成幂等成功。
- [ ] Job、Attempt、Root、Dataset、Restore Attempt 使用独立表驱动状态测试。
- [ ] 测试 stale executor、重复请求、反向转换、并发 lease 和版本溢出。
- [ ] 并发测试覆盖 Profile rotate/Root register/DROP Account、Root/Purge/Restore lease 的固定 system lock order；禁止持锁跨 provider/tenant RPC。
- [ ] 正确性不依赖进程内 mutex；mutex 只能减少重复工作。
- [ ] CAS 失败返回当前权威状态，供调用者选择退出、对账或重试。

**验证：**

```bash
go test -race ./pkg/lifecycle/catalog
```

**提交：** `feat(lifecycle): add durable lifecycle state machines`

### Task A4：实现 Profile 和 Table Feature Guard

**Files：**

- Add: `pkg/lifecycle/catalog/profile.go`
- Add: `pkg/lifecycle/admission/guard.go`
- Add: `pkg/lifecycle/admission/guard_test.go`
- Modify: CDC、Publication、FK、index/plugin 的 create/drop 入口
- Modify: `pkg/sql/compile/ddl.go`

**实现：**

- [ ] Profile 冻结 `(profile_id, profile_version, storage_namespace_id, endpoint, bucket, prefix)`。
- [ ] 实现 `ADD VERSION` 和指定 version 的 credential rotation；Binding 只冻结创建时最新 ACTIVE version，后台永不按名称漂移到新版本。
- [ ] Credential rotation 只产生新 credential generation，不能改变 namespace。
- [ ] 强制 authenticated TLS 和 provider server-side encryption；Profile/Dataset/Root/Manifest 冻结 encryption/KMS identity digest。
- [ ] 更换 KMS key identity 创建新 Profile version；不能把历史 Dataset 静默指向新 key。
- [ ] 被 Dataset/Root/Restore 引用的 Profile 只能停用，不能删除或重指向。
- [ ] Lifecycle、CDC、Publication、FK、索引/插件都 CAS 同一 `(account_incarnation, table_id)` Guard 行。
- [ ] 首次使用任一特性都懒创建 Guard；唯一键冲突关闭“双方同时看见不存在”的竞态。
- [ ] Guard 保存 table generation、feature bitset、dependency epoch 和 version。
- [ ] DROP/TRUNCATE/ALTER COPY 更新 generation；最终退休事务 CAS captured generation/epoch/version。
- [ ] 首个 GA 拒绝隐藏二级/唯一索引表，不实现索引联动删除。
- [ ] Snapshot/PITR/Clone/Branch/Backup 与 Lifecycle Binding 双向互斥。

**验证：**

```bash
go test -race ./pkg/lifecycle/catalog ./pkg/lifecycle/admission ./pkg/frontend ./pkg/sql/compile
```

**提交：** `feat(lifecycle): add profile identity and serialize feature admission`

### Gate A 门禁

- [ ] SQL 契约、Catalog DDL 与设计逐字段核对；
- [ ] `Bind || Create CDC/Index` 首次竞态测试通过；
- [ ] 升级中断和重复升级测试通过；
- [ ] 总开关关闭时新 DDL fail-closed，后台不运行。

---

## 3. Gate B：Object Index、Planner、Reader 与 Archive

### Task B1：实现仅绑定表的派生 Object Index

**Files：**

- Add: `pkg/lifecycle/objectindex/index.go`
- Add: `pkg/lifecycle/objectindex/backfill.go`
- Add: `pkg/lifecycle/objectindex/reconcile.go`
- Add: `pkg/lifecycle/objectindex/index_test.go`
- Reference: `pkg/frontend/object_list.go`
- Reference: `pkg/vm/engine/test/object_list_test.go`

**实现：**

- [ ] 使用 `engine.Relation.CollectObjectList(from,to,...)` 分页采集，不订阅内部 Logtail 回调。
- [ ] 初次绑定：记录 W0、全量分页、补扫 `(W0,W1]`、CAS READY。
- [ ] Index 只保存 Planner 摘要和 exact identity；Footer 才是退休前权威。
- [ ] `last_seen_ts/delete_ts/index_version` 使重复扫描幂等。
- [ ] 每表 object 数、backfill bytes、分页 rows 和单轮 wall time 都有硬上限。
- [ ] 崩溃后从 Catalog watermark 继续，不依赖内存 replay。
- [ ] 抽样与 `CollectObjectList`/Footer 对账；漂移时进入 `INDEX_REBUILDING`，禁止退休。
- [ ] 测试 Merge 在 backfill 中替换对象、重复页、CN 重启和旧 checkpoint。

**验证：**

```bash
go test -race ./pkg/lifecycle/objectindex ./pkg/vm/engine/test
```

**提交：** `feat(lifecycle): add rebuildable bound-table object index`

### Task B2：实现候选 Planner 和 Dry-run

**Files：**

- Add: `pkg/lifecycle/coordinator/planner.go`
- Add: `pkg/lifecycle/coordinator/candidate.go`
- Add: `pkg/lifecycle/coordinator/dryrun.go`
- Add: `pkg/lifecycle/coordinator/planner_test.go`

**实现：**

- [ ] 只查询到期 Binding，不扫描 `mo_tables`。
- [ ] ZoneMap 仅把对象分为 `WHOLE_CANDIDATE/MIXED_CANDIDATE/NOT_ELIGIBLE/NEED_FOOTER`。
- [ ] Whole 最终判定必须由 exact Footer 和固定 Snapshot Reader 确认。
- [ ] 按天形成有界 child Job；一个 child Job 只对应一个 table generation 和一个 Dataset。
- [ ] 估算 source bytes、expired/live rows、tombstone bytes、affected blocks 和 retained bytes。
- [ ] 超限返回可解释阻断，不创建无限任务。
- [ ] Dry-run 不创建 Root、不读取 payload、不执行写事务。

**验证：** `go test ./pkg/lifecycle/coordinator`

**提交：** `feat(lifecycle): plan bounded lifecycle candidates`

### Task B3：实现固定 Snapshot Exact Reader

**Files：**

- Add: `pkg/lifecycle/reader/spec.go`
- Add: `pkg/lifecycle/reader/exact_reader.go`
- Add: `pkg/lifecycle/reader/report.go`
- Add: `pkg/lifecycle/reader/exact_reader_test.go`
- Modify/Add: `pkg/vm/engine/disttae/` 中必要的最小 exact RelData helper
- Reference: `pkg/vm/engine/readutil/reader.go`

**实现：**

- [ ] 输入为持久化 RelData/Object identity、固定 SI Snapshot、投影和谓词，禁止传入内存 rows。
- [ ] 首版 callback 串行，Borrowed Batch exactly-once release。
- [ ] 复用 `readutil.NewReader`、MVCC、Tombstone 和 transfer，不绕过可见性。
- [ ] 报告实际 Object/Block/Row、visible/expired/live、物理 bytes 和逻辑 digest。
- [ ] 使用 `DeleteKeyProjection` 描述单 PK、复合 PK 编码或无 PK fake key。
- [ ] 达到 rows/bytes/time/cancel 时释放 Reader、Batch、mpool 和 spill。
- [ ] 支持单 Object 2,097,152 rows、3 GiB、最大 varlen block 的 streaming。
- [ ] 测试 tombstone、并发 Merge、RowID transfer、callback error/panic 和 cancel。

**验证：**

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/lifecycle/reader ./pkg/vm/engine/disttae
```

**提交：** `feat(lifecycle): add fixed-snapshot exact object reader`

### Task B4：实现 Root-before-PUT 和 ArchiveStore

**Files：**

- Add: `pkg/lifecycle/archive/store.go`
- Add: `pkg/lifecycle/archive/object_key.go`
- Add: `pkg/lifecycle/cleanup/root_writer.go`
- Add: `pkg/lifecycle/archive/fake_store_test.go`
- Add: `pkg/lifecycle/cleanup/root_writer_test.go`

**实现：**

- [ ] 每个 Put/MultipartCreate/Copy 前先提交 Root 和预期 deterministic key。
- [ ] key 包含 account incarnation、table generation、job、attempt、ordinal 和角色，不含可变表名。
- [ ] adapter 提供 `PutImmutable/Open/Head/DeleteExact/AbortMultipart`。
- [ ] 已有 key 只有 size+SHA 一致才算幂等成功。
- [ ] provider 无 version ID 时依赖不可覆盖 key 和 Head checksum；有 version ID 时冻结具体 version。
- [ ] multipart ID 在创建成功后立即登记；响应丢失时由 deterministic key、provider lifecycle rule 和孤儿审计兜底。
- [ ] provider 无 conditional-create PUT 时，REGISTERED/UPLOADING 丢失 writer epoch 必须废弃旧 attempt/root 并用新 prefix 重做；禁止同 key 原地接管上传。
- [ ] 只有完整 VERIFIED 且无未收敛 I/O 的 Root 可由新 epoch 接手 finalize；FINALIZING 只对账原 txn。
- [ ] 测试 Root 失败不 PUT、PUT 成功 Root 更新失败、response lost、stale runner、同 key 不同内容。

**验证：** `go test -race ./pkg/lifecycle/archive ./pkg/lifecycle/cleanup`

**提交：** `feat(lifecycle): register cleanup ownership before archive writes`

### Task B5：实现 Parquet/ZSTD、Manifest 和全量校验

**Files：**

- Add: `pkg/lifecycle/archive/parquet_writer.go`
- Add: `pkg/lifecycle/archive/type_mapping.go`
- Add: `pkg/lifecycle/archive/manifest.go`
- Add: `pkg/lifecycle/archive/verify.go`
- Add: corresponding tests

**实现：**

- [ ] 实现 MO 到 Parquet 类型映射；默认 ZSTD。
- [ ] 目标文件 128–512 MiB、Row Group 64–128 MiB；文件数和单文件均有硬上限。
- [ ] canonical row encoding 规范化 NULL、NaN、时区、Decimal scale、JSON/Binary。
- [ ] Manifest 保存 schema version、source snapshot/table/schema/objects、files/root/profile identity。
- [ ] PUT 后必须从 provider 重新 Open 并全量验证 checksum、schema、row count 和 logical Merkle root。
- [ ] Verify 失败 Root 可清理，但不能发布或退休源数据。
- [ ] 测试所有支持类型、最大 varlen、损坏、截断、错误 metadata 和并发 writer。

**验证：** `go test -race ./pkg/lifecycle/archive`

**提交：** `feat(lifecycle): write and verify canonical parquet archives`

### Gate B 门禁

- [ ] 非绑定表和 Dry-run 无扫描/PUT；
- [ ] Reader Batch 所有权和取消路径通过 race/leak test；
- [ ] Root-before-PUT 故障注入通过；
- [ ] 最大单 Object streaming 通过；
- [ ] Manifest 包含 Restore 所有必需字段。

---

## 4. Gate C：Strict Whole Object Retire P0

### Task C1：定义 protobuf 和版本能力

**Files：**

- Modify: `proto/api.proto`
- Regenerate: protobuf generated files
- Add: `pkg/vm/engine/cmd_util/lifecycle.go`
- Modify: `pkg/vm/engine/cmd_util/type.go`
- Add: `pkg/vm/engine/cmd_util/lifecycle_test.go`

**实现：**

- [ ] 在 `api.Entry.EntryType` 追加 `StrictObjectRetire`，不复用 `file_name`。
- [ ] 在 `api.Entry` 追加 typed payload，保持 protobuf 编号向后兼容。
- [ ] Payload 保存 table/schema generation、Guard/Binding version、attempt/epoch、snapshot/cutoff、exact objects、footer/read root。
- [ ] `ParseEntryList` 返回 typed request，普通 Entry 行为不变。
- [ ] CN 在集群协议版本不足时 fail-closed，不把新 Entry 发给旧 TN。
- [ ] 测试 unknown enum/field、缺字段、超大 payload、重复 Object。

**验证：**

```bash
make generate
go test ./pkg/vm/engine/cmd_util
```

**提交：** `feat(tae): define strict object retire wire entry`

### Task C2：CN workspace 发送 typed Entry

**Files：**

- Modify: `pkg/vm/engine/disttae/txn_table.go`
- Modify: `pkg/vm/engine/disttae/txn_table_delegate.go`
- Add: `pkg/vm/engine/disttae/strict_object_retire_test.go`

**实现：**

- [ ] 增加仅 Lifecycle 使用的内部 `StrictRetireObjects` 接口。
- [ ] Payload 进入原始 txn writes 和 `PrecommitWriteCmd.EntryList`。
- [ ] Entry 与 Dataset/Receipt cluster-table writes 使用同一 txn operator。
- [ ] 不调用现有 SoftDeleteObject filename magic。
- [ ] CN 限制 Object 数和 payload bytes；TN 仍独立验证。
- [ ] 1PC、2PC、workspace dump/retry 测试 payload 不丢失。

**验证：**

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/vm/engine/disttae
```

**提交：** `feat(disttae): emit strict object retire entries`

### Task C3：TN fail-closed 验证

**Files：**

- Modify: `pkg/vm/engine/tae/rpc/handle.go`
- Add: `pkg/vm/engine/tae/rpc/strict_object_retire.go`
- Add: `pkg/vm/engine/tae/rpc/strict_object_retire_test.go`
- Modify/Add: `pkg/vm/engine/tae/catalog/` validation helpers

**实现：**

- [ ] iterator/handler 明确认识 typed request。
- [ ] 验证 account/database/table、table generation、schema digest、Guard/Binding version、attempt/epoch。
- [ ] Object 在事务验证 Snapshot 上必须仍是 live exact object，footer/row count/ZoneMap 与 payload 一致。
- [ ] TN 重新验证所有可见行满足 cutoff，不能信任 Planner。
- [ ] Object-not-found 不算成功；只有同 attempt 已确定提交才幂等，被 Merge 替换必须冲突。
- [ ] 任一条件失败整事务 abort，不允许只写 Dataset。
- [ ] malformed/unknown version 返回稳定错误，不 panic。

**验证：**

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/vm/engine/tae/rpc
```

**提交：** `feat(tae): validate strict object retire requests`

### Task C4：有界 Tombstone 探测和 Strict DropIntent

**Files：**

- Modify/Add: `pkg/vm/engine/tae/txn/txnimpl/`
- Modify/Add: `pkg/vm/engine/tae/tables/`
- Modify: `pkg/vm/engine/tae/catalog/object.go`
- Modify: `pkg/vm/engine/tae/catalog/object_list.go`
- Modify: corresponding command/replay files
- Add: package-local tests

**实现：**

- [ ] `HasCommittedObjectTombstoneInRange` 是 early-exit boolean/error，不返回完整集合。
- [ ] 搜索 committed object、in-memory tombstone 和事务可见范围，按 object/block/RowID ZoneMap 剪枝。
- [ ] 同时验证 apply high watermark 覆盖 final TS、history/GC low watermark 未越过 source Snapshot；历史不完整必须冲突重导出。
- [ ] 设置对象数、bytes、wall time 上限；上限返回 retryable conflict，不能返回 false。
- [ ] 不分配现有 1 GiB delete buffer，不持锁等待 I/O。
- [ ] Strict retire 为 exact Object 注册 DropIntent；同 attempt 重复幂等，不同 attempt/普通 Merge 互斥。
- [ ] Prepare 再验证 Object/generation/tombstone；Commit 复用现有 Object delete MVCC node。
- [ ] WAL/replay 带齐 identity；Abort 释放 intent。
- [ ] 测试 `Lifecycle || Merge/DELETE/Lifecycle`、Prepare twice、commit/abort replay。

**验证：**

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/vm/engine/tae/catalog ./pkg/vm/engine/tae/txn/... ./pkg/vm/engine/tae/tables
```

**提交：** `feat(tae): commit strict object retirement with replay`

### Task C5：协议故障矩阵

**Files：**

- Add: `pkg/lifecycle/retire/strict_client.go`
- Add: `pkg/lifecycle/retire/strict_client_test.go`
- Add: existing distributed transaction suite integration tests

**实现：**

- [ ] Dataset、Receipt、Strict Entry 在同一事务。
- [ ] 覆盖 1PC、2PC、重复 Prepare、CN/TN kill、ErrTAENeedRetry、response lost、replay。
- [ ] 断言 Dataset 与 Object 退休同存同无，未 Verify 永不退休，Merge 抢先则 abort，unknown 保留 Root。
- [ ] 旧 CN/TN 混部时 capability guard 阻止发送。

**提交：** `test(lifecycle): prove strict retire transaction semantics`

### Gate C 门禁

- [ ] TAE/Transaction Maintainer 完成 wire/replay 专审；
- [ ] 普通 Merge/GC 路径无策略变化；
- [ ] 1PC/2PC/replay/混部矩阵自动化；
- [ ] 任一 exact 条件失败均整事务 abort。

---

## 5. Gate D：Whole Job、对账与清理

### Task D1：Whole Archive Job

**Files：**

- Add: `pkg/lifecycle/coordinator/whole_job.go`
- Add: `pkg/lifecycle/coordinator/whole_job_test.go`
- Add: `pkg/lifecycle/retire/receipt.go`

**实现：**

- [ ] 固定顺序：Candidate → Root → Read → Put → Verify → Finalizing → Commit → Reconcile。
- [ ] 每阶段检查 job/attempt/executor epoch 和 lease。
- [ ] Snapshot 只覆盖有界事务窗口，不创建长生命周期 GC pin。
- [ ] Reader report、Manifest、Strict payload 的 Object/root/rows 完全一致。
- [ ] Whole 在 Reader EOF、Payload PUT complete、source root 冻结后立即关闭只读 txn，再做 Provider readback/Manifest；Mixed 仍保留同一 SI txn 到 DELETE commit。
- [ ] Final txn 原子写 Dataset、Receipt、watermark 并发送 Strict Entry。
- [ ] 明确 abort 时 Root 转 DELETE_PENDING；unknown 保持 FINALIZING。
- [ ] 测试每两个状态之间 kill、重试、stale runner 和 pause。

**提交：** `feat(lifecycle): execute verified whole-object archive jobs`

### Task D2：事务结果 Reconciler

**Files：**

- Add: `pkg/lifecycle/cleanup/reconciler.go`
- Add: `pkg/lifecycle/cleanup/reconciler_test.go`
- Add: `pkg/lifecycle/catalog/receipt.go`

**实现：**

- [ ] 事务服务返回 committed/aborted/unknown 三态。
- [ ] committed 后在正常一致性事务中读取 Receipt/Dataset/root；暂不可见只 WAIT/RETRY。
- [ ] aborted 后确认 Receipt 不存在再清 staging。
- [ ] unknown 保留 Root 和对象，deadline 只告警，不自动释放。
- [ ] txn status 接近保留上限时升级人工处置，不能猜 abort。
- [ ] root mismatch 只有在明确 committed 且一致性读后才判 corruption。

**提交：** `feat(lifecycle): reconcile in-doubt archive commits`

### Task D3：不可逆 Sweeper

**Files：**

- Add: `pkg/lifecycle/cleanup/sweeper.go`
- Add: `pkg/lifecycle/cleanup/delete_protocol.go`
- Add: `pkg/lifecycle/cleanup/sweeper_test.go`

**实现：**

- [ ] 删除前要求 purge eligible/owner dropped、无 lease、grace 到期，并 CAS DELETE_PENDING。
- [ ] `DELETE_PENDING -> DELETING` 后禁止新增 lease/reference 和取消。
- [ ] 按 Root Object 精确 key/version 删除，不靠 prefix LIST 决定所有权。
- [ ] 重复 Delete 幂等；每个对象 HEAD 不存在后才 CLEANED。
- [ ] CLEANED tombstone 至少保留 30 天，且不得短于最大 I/O deadline + multipart convergence + quiescence window，防止迟到 PUT。
- [ ] provider 错误进入有界退避和 DELETE_FAILED，不忙循环。
- [ ] 测试 response lost、eventual LIST、HEAD stale、旧 runner 迟到、部分删除。

**提交：** `feat(lifecycle): sweep archive objects with irreversible ownership`

### Gate D 门禁

- [ ] Root/Attempt/Job/Dataset 每种状态都有唯一 Owner；
- [ ] COMMIT_UNKNOWN 永不自动清理；
- [ ] DELETE_PENDING 与 Restore lease CAS 同一 access generation；
- [ ] Coordinator 关闭后 Reconciler/Sweeper 仍可收敛。

---

## 6. Gate E：小比例 Mixed SI DELETE

### Task E1：固定 SI、Delete key 和并发证明

**Files：**

- Add: `pkg/lifecycle/retire/mixed_txn.go`
- Add: `pkg/lifecycle/retire/delete_key.go`
- Add: `pkg/lifecycle/retire/mixed_txn_test.go`
- Modify only if required: normal DELETE key preprocessing helper

**实现：**

- [ ] 一个普通 writable SI transaction 覆盖 Reader、provider readback、`Relation.Delete`、Dataset/Receipt。
- [ ] 禁止用悲观 RC 的两个独立 SQL 拼接 SELECT/DELETE。
- [ ] 复用正常 DELETE 的实际 key：单 PK、复合 PK encoded key、无 PK fake key。
- [ ] 证明 user DELETE/UPDATE/transfer 产生冲突；若存在静默双提交，停止 Gate 并补 TN 协议。
- [ ] 同一 txn 的 Root 在外部写前已登记；commit unknown 进入 Gate D 对账。

**验证：**

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/lifecycle/retire ./pkg/sql/compile
```

**提交：** `feat(lifecycle): prepare mixed deletes in one SI transaction`

### Task E2：Spill 和预算

**Files：**

- Add: `pkg/lifecycle/retire/delete_spill.go`
- Add: `pkg/lifecycle/retire/budget.go`
- Add: corresponding tests

**实现：**

- [ ] RowID/Delete key 按批 spill；内存只保留当前窗口。
- [ ] spill 路径 attempt-scoped，成功/abort/cancel/restart 都有唯一清理 Owner。
- [ ] rows、raw key bytes、affected blocks、source/txn/spill bytes、wall time 双阶段门禁。
- [ ] 预计超限在 Delete 前进入 `MIXED_LAYOUT_BLOCKED`；执行越界则 abort。
- [ ] rolling cluster budget 纳入当前 Tombstone/Merge backlog。
- [ ] blocked 不无限自动重试，只在 layout/policy/version 变化或 RECHECK 后重试。
- [ ] 测试磁盘满、spill 损坏、cancel、重启和估算偏低。

**提交：** `feat(lifecycle): bound mixed delete resources`

### Task E3：Mixed 最终事务

**Files：**

- Add: `pkg/lifecycle/coordinator/mixed_job.go`
- Add: `pkg/lifecycle/coordinator/mixed_job_test.go`

**实现：**

- [ ] 同一 SI txn 内读取、归档全量重读校验、Delete、Dataset/Receipt。
- [ ] Root FINALIZING 后才 commit；unknown 进入 Reconciler。
- [ ] commit 后普通 SELECT 仅靠现有 MVCC/Tombstone 不可见，无 TTL filter。
- [ ] Lifecycle 不要求 Merge 立即回收 Tombstone 或物理空间。
- [ ] 测试边界预算、user DML、Merge、commit unknown。

**提交：** `feat(lifecycle): archive bounded mixed rows with normal deletes`

### Gate E 门禁

- [ ] Mixed 只复用普通 DELETE；
- [ ] 并发 DML 不丢行、不重复归档；
- [ ] Tombstone 最坏成本有集群硬上限；
- [ ] 大 Mixed 明确阻断，不降级为不受控 DELETE。

---

## 7. Gate F：DROP、Purge 与 Restore

### Task F1：轻量 Owner Tombstone

**Files：**

- Modify: `pkg/sql/compile/ddl.go`
- Modify: `pkg/frontend/authenticate.go`
- Add: `pkg/lifecycle/catalog/owner.go`
- Add: `pkg/lifecycle/catalog/owner_test.go`
- Extend: existing DROP tests

**实现：**

- [ ] DROP TABLE/DATABASE 在原 Catalog 事务中写 owner tombstone，不等待 provider。
- [ ] DROP ACCOUNT 在同一个 system-account DROP 事务中写 account identity/tombstone、删除匹配 incarnation/version 的 current row，再清 tenant cluster rows。
- [ ] DROP rollback 时 tombstone 不可见。
- [ ] DROP 覆盖正常 purge 时间，但等待已有 read/restore lease 收敛。
- [ ] in-flight Job 看到 tombstone 后 fence/abort 或对账。
- [ ] 测试 DROP 与 Root、PUT、Final Commit、Restore、Purge 的所有边界竞态。

**验证：**

```bash
go test -race ./pkg/lifecycle/catalog ./pkg/sql/compile ./pkg/frontend
```

**提交：** `feat(lifecycle): record lightweight archive owner drops`

### Task F2：异步 Purge

**Files：**

- Add: `pkg/lifecycle/cleanup/purge.go`
- Add: `pkg/lifecycle/cleanup/purge_test.go`
- Modify: Frontend dispatch

**实现：**

- [ ] Purge 先 CAS Dataset PURGE_PENDING/access generation，不直接删 provider。
- [ ] 新 Restore lease 与 PURGE_PENDING/DELETING 互斥。
- [ ] 已有 lease 有 deadline，超时 fence/告警，不能永久阻塞。
- [ ] Sweeper 删除完成后再将 Manifest/Dataset 标 PURGED。
- [ ] 保留最小 Dataset 审计 tombstone，ID 不复用。
- [ ] Dry-run 返回 objects/bytes/lease/阻断原因。

**提交：** `feat(lifecycle): purge datasets asynchronously`

### Task F3：Restore 分块导入

**Files：**

- Add: `pkg/lifecycle/restore/attempt.go`
- Add: `pkg/lifecycle/restore/chunk.go`
- Add: `pkg/lifecycle/restore/import.go`
- Add: corresponding tests

**实现：**

- [ ] 解析 Dataset/Profile/Manifest 后获取 access-generation lease。
- [ ] staging 表位于受保护隐藏 namespace，名字包含 attempt ID。
- [ ] Manifest 验证后按 Parquet Row Group 生成 deterministic chunk。
- [ ] 每 chunk 独立普通事务插入 staging，并在同事务写 chunk receipt。
- [ ] response lost 读取 chunk receipt 对账，禁止重复插入。
- [ ] 复用正常 INSERT 预处理，覆盖 composite/fake PK、Decimal、时区、AUTO_INCREMENT 元数据。
- [ ] AUTO_INCREMENT 原值恢复后将 sequence 水位推进到 `max+1` 并测试后续 INSERT；若现有路径无法保证则准入拒绝。
- [ ] 每 chunk 有 rows/bytes/time/memory/spill 上限。
- [ ] 测试 CN kill、重复 chunk、坏文件、provider 暂失、配额不足。

**提交：** `feat(lifecycle): restore archive chunks into hidden staging`

### Task F4：Restore 全量验证和原子发布

**Files：**

- Add: `pkg/lifecycle/restore/verify.go`
- Add: `pkg/lifecycle/restore/publish.go`
- Add: `pkg/lifecycle/restore/publish_test.go`
- Add: lifecycle SQL BVT under `test/distributed/cases/`

**实现：**

- [ ] 按 Manifest 顺序再次全量重读所有 Payload，校验 checksum、schema、row count 和每个 Dataset logical root。
- [ ] 校验所有 chunk Receipt、staging schema 和 `SELECT COUNT(*)`；不按 staging 物理行序重算有序 Dataset root。
- [ ] Payload root、Receipt rows 和 staging count 全部匹配才进入 PUBLISHING；正常 INSERT/事务路径负责已写 Batch 的原子持久化。
- [ ] 发布事务 CAS Dataset/access generation、attempt、目标 database generation 和目标名不存在。
- [ ] 原子发布为独立新表，不覆盖源表。
- [ ] publish response lost 通过目标 table identity + restore receipt 对账。
- [ ] 失败 staging 由 attempt owner 清理，不误删已发布表。
- [ ] BVT 覆盖完整查询、schema、重复 Restore、同名冲突、权限。

**提交：** `feat(lifecycle): verify and atomically publish restored tables`

### Task F5：fail-closed 支持矩阵

**Files：**

- Add: `pkg/lifecycle/admission/support_matrix.go`
- Modify: Snapshot/PITR/Clone/Branch/Backup/DR 入口
- Add: corresponding tests

**实现：**

- [ ] Lifecycle-bound 表的普通 Snapshot/PITR Restore、Clone、Branch、Backup 明确拒绝。
- [ ] Bind 前检查已有对象/任务，反向也拒绝。
- [ ] DR/failover 无 archive Catalog/payload 时返回 `ARCHIVE_NOT_AVAILABLE_IN_DR`，不能返回空 Dataset。
- [ ] 错误明确“不支持”，不能显示“恢复成功但历史行缺失”。
- [ ] 新增相关能力默认 fail-closed。

**提交：** `feat(lifecycle): fail closed for non archive-aware recovery`

### Gate F 门禁

- [ ] DROP 主路径无 provider I/O，rollback 不误删；
- [ ] Restore chunk exactly-once、发布 response-lost 通过；
- [ ] Purge/Restore CAS 竞态通过；
- [ ] Backup/DR 不会静默产生不完整数据。

---

## 8. Gate G：调度、容量、观测与运维

### Task G1：TaskService Runner

**Files：**

- Add: `pkg/lifecycle/coordinator/task.go`
- Add: `pkg/lifecycle/coordinator/runner.go`
- Add: `pkg/lifecycle/coordinator/runner_test.go`
- Modify: CN task executor registration path

**实现：**

- [ ] TaskService 仅负责投递/lease，Catalog 才是业务状态权威。
- [ ] runner 每步读取 job/attempt epoch；外部 I/O 前后都检查 stale。
- [ ] 初始并发：单表 1、数据库 2、账户 4、集群 8。
- [ ] account/database/table 分层公平，单一大表不能占满集群。
- [ ] pause/kill switch 停止新任务；FINALIZING 交 Reconciler。
- [ ] TaskService 记录丢失后可从 Catalog 重建。
- [ ] 测试 duplicate delivery、lease steal、runner crash、task cleanup/rebuild。

**提交：** `feat(lifecycle): schedule durable bounded lifecycle jobs`

### Task G2：容量控制

**Files：**

- Add: `pkg/lifecycle/coordinator/quota.go`
- Add: `pkg/lifecycle/coordinator/quota_test.go`
- Add: `pkg/lifecycle/observability/capacity.go`

**实现：**

- [ ] 对 active jobs、read/write bytes、Root/Object、Index rows、Tombstone、spill、cleanup backlog、retained bytes、restore staging 分层限额。
- [ ] 资源预约使用 CAS；success/abort/unknown 分别释放，unknown 不释放可能仍在用的资源。
- [ ] 等待有 deadline 和 `CAPACITY_BLOCKED`，不无限占 worker。
- [ ] 越界 fence 当前读写阶段；FINALIZING 只对账。
- [ ] 测试计数漂移重算、double release、runner kill、集群重启。

**提交：** `feat(lifecycle): enforce hierarchical lifecycle budgets`

### Task G3：SHOW、指标和管理动作

**Files：**

- Add: `pkg/lifecycle/observability/metrics.go`
- Add: `pkg/lifecycle/observability/status.go`
- Add: `pkg/lifecycle/coordinator/admin.go`
- Add: corresponding tests
- Modify: Frontend SHOW/admin dispatch

**实现：**

- [ ] SHOW 输出 Binding、Candidate、Job、Attempt、Dataset、Restore、Root/Purge 和最后错误。
- [ ] Prometheus 只用低基数 label；ID 放结构化日志/trace。
- [ ] 告警覆盖 COMMIT_UNKNOWN、FINALIZING timeout、DELETE_FAILED、Index 漂移、orphan、Mixed blocked、quota saturation。
- [ ] 日志带 account incarnation/table generation/job/attempt/root/txn，禁止输出 credential。
- [ ] PAUSE/RESUME/RECHECK/CANCEL 使用状态 CAS；FINALIZING 不能强制当 abort。
- [ ] 不提供跳过 checksum、强制删除有 lease Dataset 等破坏不变量的命令。
- [ ] 管理动作写审计日志和操作人。

**提交：** `feat(lifecycle): expose status and safe administration`

### Gate G 门禁

- [ ] 所有队列、表、缓存、重试有硬上限；
- [ ] 所有等待有 deadline、cancel 和退出 Owner；
- [ ] kill switch 不破坏 unknown 对账；
- [ ] SRE 能区分配置、资源、冲突和系统故障。

---

## 9. Gate H：P0 证明、规模认证和 GA

### Task H1：P0 自动化

- [ ] 将 `10-p0-test-ga-acceptance-cn.md` 每个 case ID 映射到测试名和 CI job。
- [ ] Reader、Strict、Mixed、Root、Reconcile、Cleanup、Restore 分包运行。
- [ ] 1PC/2PC/replay/kill 使用确定性故障注入，不依赖人工看日志。
- [ ] 每次测试自动检查 active rows、Dataset/Receipt、payload、Root/lease、staging、goroutine、memory、spill 和 orphan。
- [ ] 无法自动断言的状态先补诊断，不能以“进程未报错”代替数据不变量。

**提交：** `test(lifecycle): automate commercial ga p0 matrix`

### Task H2：1 TiB 常见规模认证

- [ ] 时间有序、5% late arrival、1% Mixed 的 1 TiB 表。
- [ ] 同时运行 INSERT/UPDATE/DELETE、普通 Merge、Archive、Restore 和 GC。
- [ ] 连续 72 小时，覆盖 CN/TN 重启、TaskService 接管和 provider 限流。
- [ ] 记录吞吐、查询 P50/P99、Merge backlog、Tombstone、retained bytes、对象数、Restore RTO 和 provider 成本。
- [ ] 与关闭 Lifecycle 的基线对比普通查询/Merge 回归。
- [ ] 验证 `active ∪ archive = 基准 Snapshot` 且 `active ∩ archive = ∅`。

### Task H3：10 TiB 单表认证

- [ ] 使用真实 10 TiB 逻辑表和最大 Object/varlen 边界，不做等比缩小模拟。
- [ ] 覆盖时间有序、高度乱序；乱序表应稳定阻断，不拖垮集群。
- [ ] 连续 7 天，覆盖每日 Archive、持续 Merge、滚动重启、provider 慢/错和 Restore 抽样。
- [ ] 证明 Index 无单行热点、不扫描非绑定表、Catalog/Task queue 有界。
- [ ] 证明 retained bytes、spill、memory 和 cleanup backlog 有硬上限。

### Task H4：滚动升级与回滚

- [ ] 新旧 CN/TN 混部时 Feature Guard 关闭。
- [ ] 所有 TN 能力就绪后才允许 Bind/Strict retire。
- [ ] 关闭功能时 FINALIZING 仍由兼容 Reconciler 处理。
- [ ] 回滚前停止新 Job、收敛 unknown、清 staging、保留 Dataset/Root Catalog。
- [ ] 旧节点不误解析 tagged Entry、不删除 Lifecycle Catalog。

### Task H5：分阶段放量

```text
Stage 0  内部 synthetic
Stage 1  最多 50 张表
Stage 2  最多 200 张表
Stage 3  最多 500 张表
Stage 4  最多 1000 张显式绑定表
```

每阶段必须：

- [ ] 完成规定观察周期；
- [ ] 无数据不变量失败、无法解释的 Root/orphan；
- [ ] 无普通查询/Merge/GC 严重回归；
- [ ] unknown、cleanup、Mixed blocked、Restore failure 可运维；
- [ ] Lifecycle、TAE、Transaction、Frontend、SRE、QA Owner 联合签字。

任一数据不变量失败立即关闭新 Bind/Job、保留 unknown 供对账、停止放量，完成 RCA 和重新认证后再恢复。

### Task H6：Commercial GA 签字

- [ ] Gate A–G 全部通过；
- [ ] P0 文档无豁免项；
- [ ] 1/10 TiB 和 Stage 4 认证通过；
- [ ] 文档明确不支持 Legal Hold/WORM、Deep Archive、archive-aware Backup/DR；
- [ ] Restore/Purge 使用真实 provider 演练；
- [ ] 数据丢失、重复归档、误清理、静默不完整恢复均为零；
- [ ] SRE Runbook、告警、容量仪表盘和 kill switch 可用。

---

## 10. PR 拆分与核心改动预算

推荐 PR：

1. SQL AST + Catalog migration；
2. Catalog DAO + Feature Guard；
3. Object Index + Planner；
4. Exact Reader；
5. ArchiveStore + Root；
6. Parquet/Manifest/Verify；
7. Strict wire；
8. TN validation + replay；
9. Whole Job + Reconciler；
10. Sweeper/Purge；
11. Mixed SI DELETE；
12. Restore chunk + publish；
13. DROP/support matrix；
14. Scheduler/quota/observability；
15. P0 chaos/scale certification。

每个 PR 必须写明：对应设计章节/P0 case、新不变量、失败 Owner、是否改变普通 DML/查询/Merge/GC、测试结果、Feature flag/回滚方式、尚未实现能力。

| 既有核心路径 | 允许改动 | 禁止顺带改动 |
|---|---|---|
| `proto/api.proto` | 新 tagged Entry/payload | 通用事务 envelope |
| DistTAE workspace | Strict entry；复用正常 Delete | 普通 DML 编码重写 |
| TAE RPC | 解析/验证 Strict | 改普通 Entry 语义 |
| TAE Catalog/Txn | strict intent、exact CAS、replay | 改普通 Merge 策略 |
| DDL | Guard、Owner tombstone | provider 同步清理 |
| Snapshot/Backup 入口 | fail-closed | archive-aware 恢复 |
| Reader | 独立 exact helper | 普通 SELECT TTL filter |
| GC | 零策略改动 | Lifecycle 专属回收策略 |

任何 PR 超出该表，必须先修改 ADR 并重新评审，不能以“顺便优化”为理由合入。

---

## 11. Definition of Done

- 功能：Whole Archive、受限 Mixed Archive、TTL、Restore、Purge、DROP cleanup、Dry-run 可用；
- 安全：未 Verify 不删源、unknown 不清理、发布与退休原子、Restore 全量校验；
- 并发：Merge/DML/DDL/双 Job/双 Sweeper/双 Restore 竞态有测试；
- 重启：CN/TN/TaskService/provider response lost 后仅靠 Catalog 可对账；
- 资源：内存、spill、任务、Index、Root、对象、Tombstone、retained bytes 有上限；
- 运维：状态、告警、kill switch、recheck、cleanup Runbook 齐全；
- 兼容：滚动升级 fail-closed，旧节点不接收新 wire；
- 性能：1/10 TiB 和 1000 绑定表认证通过；
- 产品：支持矩阵和非目标无歧义，不宣传为七年不可删的合规归档。

只有以上全部满足，才可以把 Issue #24552/#24853 对应的首个 Commercial GA 标记为完成。
