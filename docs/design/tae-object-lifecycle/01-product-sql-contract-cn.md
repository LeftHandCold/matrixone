# 产品与 SQL 合同

> 本文唯一负责 Lifecycle 的用户可见 SQL、权限、时间语义、支持矩阵和错误分类。

## 1. 产品对象

首个 Commercial GA 对外暴露四种对象：

1. **Archive Profile**：外部对象存储的不可变版本化身份。
2. **Lifecycle Binding**：一张普通表的一条 TTL 或 Archive 策略。
3. **Archive Dataset**：一次成功 Archive child 发布的不可变数据集。
4. **Lifecycle Job**：系统执行记录，可查询、暂停、重试或重新评估。

不对外暴露 Discovery scan state/Candidate、Attempt Control、Cleanup Root 和 Receipt
的写接口。它们只通过 `SHOW`/系统视图提供诊断字段。

## 2. SQL 语法

### 2.1 Archive Profile

```sql
CREATE ARCHIVE PROFILE profile_name
FROM STAGE stage_name;

ALTER ARCHIVE PROFILE profile_name
ADD VERSION FROM STAGE stage_name;

ALTER ARCHIVE PROFILE profile_name
VERSION profile_version
ROTATE CREDENTIALS FROM STAGE stage_name;

DROP ARCHIVE PROFILE profile_name;

SHOW ARCHIVE PROFILES;
SHOW ARCHIVE PROFILE VERSIONS profile_name;
SHOW CREATE ARCHIVE PROFILE profile_name;
```

语义：

- `CREATE ... FROM STAGE` 读取 Stage 当前 URL/credential，规范化 provider、endpoint、bucket/container 和 prefix；
- 创建 `profile_version = 1` 和新的 `storage_namespace_id`；
- `ADD VERSION` 在同一 `profile_id/profile_name` 下创建 `version + 1`；允许新的 namespace/KMS identity，只影响未来 Binding；
- Binding 解析 Profile 名时冻结当时最新 ACTIVE version，之后不自动迁移；
- Stage 后续变更不影响已创建 Profile；
- `ROTATE CREDENTIALS` 必须指定仍存在的 version，只创建新 credential generation，不修改该 version 的 namespace identity；
- 新 Stage 必须解析到完全相同的 provider、canonical endpoint、bucket/container 和 immutable prefix；
- credential rotation 的 namespace/KMS identity 不同返回 `LIFECYCLE_PROFILE_NAMESPACE_CHANGED`，用户必须 `ADD VERSION`；
- Profile 被 Binding、Dataset、Root 或未完成清理引用时禁止 `DROP`；
- `DROP ARCHIVE PROFILE` 只在所有 version 均无引用时删除；首个 GA 不提供强制删除单个被引用 version；
- Profile 名可以重建，但 `profile_id` 不复用。

首个 GA 支持的 Profile capability：

```text
PUT stream
GET exact key
HEAD/Stat
LIST deterministic prefix
multipart create/list/abort
idempotent DELETE by key
```

现有 `fileservice.ObjectStorage` 没有统一 version ID/CAS Delete，因此首个 GA 把 key 本身作为 immutable object identity。Manifest 中 `provider_version` 可为空，但 `key + size + sha256` 不可为空。

### 2.2 表级 TTL

推荐规范语法：

```sql
ALTER TABLE db.table_name
SET LIFECYCLE TTL
ON event_time
EXPIRE AFTER INTERVAL 30 DAY;

ALTER TABLE db.table_name
UNSET LIFECYCLE;
```

为 Issue #24552 的建表语法提供等价语法糖：

```sql
CREATE TABLE logs (
    id BIGINT PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    payload JSON
) TTL event_time + INTERVAL 30 DAY;
```

Parser/Plan 必须把两种语法规范化成同一种 Binding：

```text
mode = TTL
lifecycle_column_id = event_time column ID
duration_micros = 30 days
```

### 2.3 表级 Archive

```sql
ALTER TABLE db.table_name
SET LIFECYCLE ARCHIVE
ON event_time
ARCHIVE AFTER INTERVAL 90 DAY
RETAIN FOR INTERVAL 730 DAY
USING PROFILE profile_name [VERSION profile_version];
```

规范化 Binding：

```text
mode = ARCHIVE
lifecycle_column_id
archive_after_micros
archive_retention_micros
profile_id
profile_version
storage_namespace_id
```

未指定 `VERSION` 时，Binding DDL 在同一 system transaction 选择当时最高 ACTIVE
version；指定时必须存在且 ACTIVE。两种方式都会把 ID/version/namespace 冻结进
Binding，后续 `ADD VERSION` 不自动迁移已有 Binding。

同一表同时只能有一条 ACTIVE Binding。TTL 与 Archive 不能串成多级策略。

### 2.4 Binding 管理

```sql
ALTER TABLE db.table_name PAUSE LIFECYCLE;
ALTER TABLE db.table_name RESUME LIFECYCLE;

SHOW LIFECYCLE FOR TABLE db.table_name;
SHOW LIFECYCLE JOBS FOR TABLE db.table_name [LIMIT n];
SHOW LIFECYCLE DATASETS FOR TABLE db.table_name [LIMIT n];

ALTER TABLE db.table_name
RECHECK LIFECYCLE [DRY RUN];
```

语义：

- `PAUSE` 禁止创建新 child，不取消已经进入 final transaction 的 child；
- `PAUSE` 会请求正在 Reader/Uploading 的 child 安全取消；
- 取消的 Archive staging 由 Root/Sweeper 清理；
- `RESUME` 递增 Binding generation 并创建新的调度 epoch；
- `RECHECK` 为 blocked 对象创建新 child generation，不复活旧 attempt；
- `DRY RUN` 只刷新候选和成本，不退休数据。

### 2.5 Restore

```sql
RESTORE ARCHIVE
FOR TABLE db.source_table
FROM '2025-01-01 00:00:00'
TO   '2025-02-01 00:00:00'
TO TABLE db.restored_table;
```

也支持精确 Dataset：

```sql
RESTORE ARCHIVE
DATASET ('dataset-id-1', 'dataset-id-2')
TO TABLE db.restored_table;
```

约束：

- 时间区间按 Dataset 的 `min_lifecycle_value/max_lifecycle_value` 选择相交集合；
- 恢复完整 Dataset，不对 Parquet 做行级时间裁剪；
- Dataset 集合必须属于同一 logical source table 和兼容 schema lineage；
- 目标表必须不存在；
- Restore 创建独立普通表；
- 不把数据写回源表；
- 不自动创建源表当前索引、FK、CDC、Publication 或插件；
- 成功前目标表名不可见。

### 2.6 Purge

```sql
ALTER TABLE db.source_table
PURGE ARCHIVE BEFORE '2025-01-01 00:00:00';
```

该语句只把满足资格的 Dataset 推进到异步 `DELETE_PENDING`，不等待 Provider Delete。

资格：

```text
dataset.max_lifecycle_value < requested_before
AND now >= dataset.purge_eligible_at
AND matching Root.observed_commit_ts is known
AND now >= Root.observed_commit_ts + 24 hours
AND owner still exists
```

首个 GA 不提供 `FORCE PURGE` 绕过 `purge_eligible_at`。DROP owner 是独立产品契约，可放弃 Restore 并异步清理，不等价于用户 Purge。

## 3. 时间语义

每个 child 创建时冻结：

```text
evaluation_time_utc
cutoff = evaluation_time_utc - duration
expired = lifecycle_value < cutoff
```

规则：

- 等于 cutoff 不到期；
- retry 不改变 `evaluation_time_utc`；
- source set 变化必须创建新 child generation；
- 所有计算使用 UTC；
- `DATE` 按 UTC 零点转换；
- `DATETIME` 没有 session timezone，按存储语义比较；
- `TIMESTAMP` 先按内部 UTC 值比较；
- 不支持 calendar month/year，因为月长和时区边界会使固定 cutoff 不唯一；
- 生命周期列必须 `NOT NULL`；
- 更新生命周期列允许，但会使对象趋向 Mixed，且由普通 UPDATE 冲突语义处理。
- `evaluation_time_utc` 来自 MO 事务/时间服务的统一时间，不直接使用 worker 本地 wall clock；
- duration/cutoff/purge 计算全部使用 checked arithmetic，发生 underflow/overflow 时 Job fail closed；
- `action_after` 和 `archive_retention` 单项首个 GA hard max 为 100 年，DDL 时拒绝更大值。

Archive 生命周期保留截止时间：

```text
purge_eligible_at =
  max_lifecycle_value + archive_after + archive_retention

Archive payload deletable =
  Dataset.purge_eligible_at reached
  AND Root.observed_commit_ts is known
  AND Root.observed_commit_ts + 24 hours reached
  AND ordinary reference/lease/owner predicates pass
```

`observed_commit_ts`不是final tenant transaction中的插入字段。它由Reconciler从权威
Txn GetStatus取得，并在事务状态仍可查询时持久化到system-owned Root。该值未知时
fail closed，任何Purge都不得开始。

`RETAIN FOR` 是 minimum retention。它不承诺到期立即删除，也不提供 maximum retention、Legal Hold 或 WORM。

## 4. DDL 准入

### 4.1 表要求

Binding 只允许：

- 普通持久化用户表；
- 单一物理主表；
- 生命周期列类型受支持且 `NOT NULL`；
- 首个 GA 表内没有 generated column；
- AUTO_INCREMENT 显式值写入和 sequence 水位恢复能力已通过集群 capability/P0；
- TableDef 能解析实际 delete key；
- 没有不支持依赖；
- 当前没有用户 Snapshot/PITR/Backup/Clone/Branch 操作；
- 集群 capability 满足至少 Discovery/Dry-run；真正退休还需
  tagged Lifecycle commit/reservation/protection capability。

以下表拒绝：

- 临时表、外表、View、系统表、cluster table；
- Partition Service 的隐藏子表；
- 含隐藏二级索引/唯一索引物理表；
- FK parent 或 child；
- CDC source；
- Publication table；
- Fulltext/Vector/plugin-managed table；
- Lifecycle-aware Backup/DR 未实现时被其引用的表；
- 任意 generated column；
- lifecycle column 是 hidden/nullable/unsupported type；
- AUTO_INCREMENT Restore sequence capability 未就绪的表。

### 4.2 依赖竞态

Lifecycle bind 和所有不支持依赖的创建/删除操作必须取得同一个`mo_tables`逻辑行
排他锁。Guard只属于已绑定或仍在收敛的Lifecycle表，不用于给所有普通表建立一份
影子Catalog。

禁止以下实现：

```text
Lifecycle: 查到 no CDC
CDC:       查到 no Binding
双方不写同一行
双方提交
```

正确流程：

```text
Lifecycle bind:
  acquire existing mo_tables row DDL lock
    -> read authoritative dependency state
    -> reject if unsupported dependency exists
    -> under account-scoped bind capacity lock, count current account incarnation Guards
    -> reject if count reaches max-bound-tables-per-account
    -> insert Binding + Feature Guard
    -> commit

unsupported dependency/DDL:
  acquire the same mo_tables row DDL lock
    -> read Binding
    -> no Binding: execute normal path; do not create Guard
    -> active/reconciling Binding: read/CAS Guard and reject or fence
    -> commit
```

`mo_tables`行锁必须持有到事务终态。DDL先提交时，后续Bind从真实Catalog看到依赖；
Bind先提交时，后续DDL看到Binding/Guard。涉及多张表的FK等操作按稳定logical table
identity排序加锁。Unbind只有在active child和unknown final transaction收敛后才能删除
Guard。未绑定普通表不写Guard、不更新Guard，也不进入Lifecycle状态机。

`max-bound-tables-per-account`是首个 GA 的唯一强制 Binding 数量不变量，默认 1000。
它只统计当前 account incarnation 的 authoritative Guard；不在 tenant DDL 事务里尝试
读取或维护跨账户全局计数。全集群同时运行的 Discovery/child/rewrite/finalization 由
Scheduler 和 TN admission 独立硬限流；“约 1000 张全集群绑定表”仅是发布认证容量，
不是可被多个账户绕过的 Catalog 配额承诺。

### 4.3 DDL 交互

| 操作 | ACTIVE Binding 时的语义 |
|---|---|
| RENAME TABLE | 允许；logical/physical ID 不变，更新显示名称和 Guard version |
| ADD/DROP/RENAME/MODIFY COLUMN | 拒绝；必须先 `UNSET LIFECYCLE` 并等待 child 收敛 |
| ALTER TABLE COPY | 拒绝 |
| TRUNCATE TABLE | 允许；fence child，递增 Guard generation，普通 TRUNCATE 正常执行 |
| DROP TABLE | 允许；放弃 Restore，写 owner tombstone，异步清理 |
| DROP DATABASE | 允许；写一条数据库级 owner tombstone，异步清理 |
| DROP ACCOUNT | 允许；写 system account owner tombstone，异步清理 |
| CREATE/DROP INDEX | 拒绝；首个 GA 的绑定表不支持隐藏索引 |
| CREATE/DROP CDC/FK/Publication/插件依赖 | 拒绝并 CAS Guard |

`UNSET LIFECYCLE`：

1. DDL transaction 把 Guard/Binding 原子推进到 `DISABLING` 并提交；
2. SQL 返回 accepted/status ID，不同步等待外部 I/O 或 unknown transaction；
3. 后台停止新 child，等待 `FINALIZING/FINAL_RETRYABLE/COMMIT_UNKNOWN` 收敛；
4. 请求 Reader/Uploading child cancel；
5. 清理未发布 staging；
6. Binding 进入 `DISABLED`；
7. 只有 `DISABLED` 后才允许创建原先冲突的依赖；
8. 已发布 Dataset 仍从属于源表，由独立 Dataset Purge Scanner 按 retention/owner drop 清理，不依赖 Binding 仍 ACTIVE。

## 5. 用户可见状态

Binding：

```text
ENABLING
ACTIVE
PAUSED
DISABLING
DISABLED
ERROR
```

Job：

```text
PLANNED
REGISTERED
READING
UPLOADING
VERIFIED
REWRITING
FINALIZING
FINAL_RETRYABLE
COMMIT_UNKNOWN
COMMITTED
RETRYABLE
ABORTED
MIXED_LAYOUT_BLOCKED
RESOURCE_BLOCKED
CONFLICT_BLOCKED
MANUAL_RECONCILE_REQUIRED
```

Dataset：

```text
PUBLISHED
DELETE_PENDING
DELETING
PURGED
DELETE_FAILED
```

用户不能手工把 `COMMIT_UNKNOWN` 改成 committed/aborted，也不能把 `DELETING` 改回 `PUBLISHED`。

## 6. 权限

| 操作 | 权限 |
|---|---|
| CREATE/ALTER/DROP ARCHIVE PROFILE | account admin；credential 只交给密钥系统 |
| SET/UNSET/PAUSE/RESUME LIFECYCLE | 表 `ALTER` + account lifecycle privilege |
| SHOW LIFECYCLE/JOBS/DATASETS | 表 `SELECT` 或 account lifecycle monitor |
| RESTORE ARCHIVE | 源表 `SELECT` + 目标库 `CREATE TABLE` + lifecycle restore |
| PURGE ARCHIVE | 表 `ALTER` + lifecycle purge |
| 运维 reconcile/kill switch | system admin |

Manifest、Root 和系统表不得向普通用户显示 credential、签名 URL 或原始 secret。

## 7. 错误合同

### 7.1 可重试

| Symbolic code | 语义 |
|---|---|
| `LIFECYCLE_OBJECT_CHANGED` | Merge/DDL 已替换 exact Object，需要 replan |
| `LIFECYCLE_GUARD_CHANGED` | Binding/schema/dependency generation 变化 |
| `LIFECYCLE_TOMBSTONE_CHANGED` | Whole Archive 导出后出现新 DELETE/UPDATE |
| `LIFECYCLE_RESERVATION_CONFLICT` | source Object 已被普通 Merge 或另一 attempt 占用 |
| `LIFECYCLE_PROTECTION_EXPIRED` | GC protection 丢失或过期，必须 abort/replan |
| `LIFECYCLE_PROVIDER_RETRYABLE` | Provider 可重试错误 |
| `LIFECYCLE_TXN_RETRYABLE` | 正常事务冲突或 `ErrTAENeedRetry` |

可重试错误仍受 attempt count、conflict age 和 bytes 上限。耗尽后进入明确 blocked/failed 状态。

### 7.2 阻断但不丢数据

| Symbolic code | 结果 |
|---|---|
| `LIFECYCLE_SMALL_MIXED_BUDGET_EXCEEDED` | 不删除，改由 Lifecycle Rewrite 重新规划 |
| `LIFECYCLE_REWRITE_RESOURCE_EXCEEDED` | `RESOURCE_BLOCKED` |
| `LIFECYCLE_OVERSIZE_UNSUPPORTED` | Block读取峰值无法可靠估算或超过认证上限；`RESOURCE_BLOCKED` |
| `LIFECYCLE_REWRITE_AMPLIFICATION_EXCEEDED` | `MIXED_LAYOUT_BLOCKED` |
| `LIFECYCLE_CONFLICT_AGE_EXCEEDED` | `CONFLICT_BLOCKED` |
| `LIFECYCLE_CAPABILITY_NOT_READY` | 只允许 Dry-run/Export-only |
| `LIFECYCLE_UNSUPPORTED_DEPENDENCY` | Binding/依赖创建失败 |
| `LIFECYCLE_DISCOVERY_NOT_READY` | 暂停调度 |
| `LIFECYCLE_QUOTA_EXCEEDED` | 延迟执行，不退休源数据 |

### 7.3 数据或外部依赖错误

| Symbolic code | 结果 |
|---|---|
| `LIFECYCLE_ARCHIVE_ROOT_MISMATCH` | 不发布、不退休，保留证据并告警 |
| `LIFECYCLE_MANIFEST_INVALID` | Dataset 不可 Restore |
| `LIFECYCLE_ARCHIVE_UNAVAILABLE` | Profile namespace/credential/provider 不可用 |
| `LIFECYCLE_DELETE_FAILED` | Root 保留，后台重试/人工修复 |
| `LIFECYCLE_RESTORE_VERIFY_FAILED` | 目标表不发布，清理 staging |

### 7.4 结果未知

`LIFECYCLE_COMMIT_UNKNOWN` 不是失败码。前端返回：

```text
Lifecycle final transaction result is unknown.
attempt_id=<id>; reconciliation continues; source/archive ownership is retained.
```

后台进入 `COMMIT_UNKNOWN`，不能立即重跑退休动作。

## 8. 对外 SLO

首个 GA 承诺：

- 成功 final commit 后，新事务不再看到已退休活动行；
- Archive Dataset 能按 Manifest 完整 Restore 到新表；
- Job 失败时源数据不因该 Job 被删除；
- DROP 不等待 Provider；
- 所有失败和积压可查询、可告警、可停止扩大。

首个 GA不承诺：

- 到 cutoff 瞬间完成；
- 高频 UPDATE/DELETE 表的 Archive lag；
- 高度乱序 Mixed 表最终一定自动退休；
- Provider 一定降低 raw object bytes 或总账单；
- DROP 后 Archive 可恢复；
- Backup/DR/failover 包含 Archive；
- Snowflake 完整 Storage Lifecycle Policy 等价能力。

## 9. 示例

客户的事件表每天约 1 TiB，`event_time` 基本随 INSERT 增长：

```sql
CREATE STAGE archive_stage
URL = 's3://archive-bucket/mo-history/customer-a/'
CREDENTIALS = {...};

CREATE ARCHIVE PROFILE history_profile
FROM STAGE archive_stage;

ALTER TABLE prod.events
SET LIFECYCLE ARCHIVE
ON event_time
ARCHIVE AFTER INTERVAL 90 DAY
RETAIN FOR INTERVAL 730 DAY
USING PROFILE history_profile;

ALTER TABLE prod.events RECHECK LIFECYCLE DRY RUN;
SHOW LIFECYCLE FOR TABLE prod.events;
```

预期效果：

- 90 天前的 Whole Object 逐批归档并从活动表退休；
- cutoff 边界的少量 Mixed 行在预算内走普通 DELETE；
- 中/大 Mixed 由独立 Lifecycle Rewrite 一次归档过期行、重写存活行并退休旧
  Object；
- 只有 Rewrite 放大或资源超过 release profile 时才显示
  `MIXED_LAYOUT_BLOCKED`/`RESOURCE_BLOCKED`；
- 调查历史时：

```sql
RESTORE ARCHIVE
FOR TABLE prod.events
FROM '2025-01-01'
TO   '2025-02-01'
TO TABLE investigation.events_202501;
```

恢复表是独立普通表；原表仍只包含活动窗口。
