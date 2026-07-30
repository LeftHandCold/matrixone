# Catalog、身份与状态机详细设计

> 本文唯一负责 Lifecycle Catalog 表、身份、版本、状态转换、事务边界和资源 Owner。
>
> 文中的 DDL 是规范逻辑 schema。实现必须加入当前发布版本的 bootstrap/upgrade，
> 并按 MO 系统表约定补充 table ID、comment 和权限；不得减少唯一键、版本列或状态条件。

## 1. Catalog 分平面

Lifecycle 有两个 Catalog 平面：

### 1.1 Tenant transaction plane

这些表必须能和源表 DELETE/Object retire 在同一普通 tenant 事务中提交：

- Binding；
- Feature Guard；
- Discovery Scan State、有界 Candidate和固定大小Rewrite runtime stats；
- Dataset；
- Receipt；
- Restore chunk Receipt；
- table/database owner tombstone。

它们使用 `mo_catalog` cluster table，随正常 tenant Catalog 可见性和 DROP ACCOUNT 清理。

### 1.2 System retained control plane

这些表必须在 tenant 删除后继续负责外部副作用和结果对账：

- account incarnation；
- Archive Profile/version；
- Job/Attempt Control；
- Cleanup Root 和 Root Object；
- account owner tombstone；
- Restore Attempt；
- Restore lease。

它们是 system-account-owned 的普通系统表，不是 tenant cluster table。所有行显式包含
`account_id + account_incarnation`，并只通过受控内部接口访问。

两个平面不做跨 account 2PC。协议使用：

```text
system control write-ahead ownership
  -> tenant atomic retirement transaction
  -> system reconciliation
```

安全性由 write-ahead Root/Attempt、tenant Receipt 和 transaction identity 闭环。

## 2. 身份和编码

### 2.1 ID 类型

| 字段 | 编码 |
|---|---|
| account_id/database_id/table_id | `BIGINT UNSIGNED` |
| account_incarnation | UUID 的 16-byte canonical binary |
| binding_id/job_id/attempt_id/dataset_id/root_id/restore_id | UUID 16-byte binary |
| object_id | `objectio.ObjectNameShort` 的固定 binary 编码 |
| transaction_id | 原始 txn ID binary，不转成日志字符串后再解析 |
| digest/root/checksum | SHA-256 32 bytes |
| TS | MO `types.TS` 的固定 binary，另存可查询 UTC timestamp 时明确标注 |
| state/version/epoch | `TINYINT/SMALLINT/BIGINT UNSIGNED` |

所有 digest 在进入 SQL 之前按 binary canonical encoding 计算。显示层可转 hex/UUID。

### 2.2 Account incarnation

当前 `account_id` 可能在删除后被复用，不能单独作为长期 Archive owner。新增：

```sql
CREATE TABLE mo_catalog.mo_lifecycle_account_identities (
    account_id            BIGINT UNSIGNED NOT NULL,
    account_incarnation   BINARY(16) NOT NULL,
    account_catalog_rowid VARBINARY(32) NOT NULL,
    state                 TINYINT UNSIGNED NOT NULL,
    create_at             TIMESTAMP(6) NOT NULL,
    drop_at               TIMESTAMP(6) NULL,
    version               BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (account_id, account_incarnation),
    KEY lifecycle_account_state (account_id, state)
);

CREATE TABLE mo_catalog.mo_lifecycle_account_current (
    account_id            BIGINT UNSIGNED NOT NULL,
    account_incarnation   BINARY(16) NOT NULL,
    account_catalog_rowid VARBINARY(32) NOT NULL,
    version               BIGINT UNSIGNED NOT NULL,
    updated_at            TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_id)
);
```

状态：

```text
ACTIVE -> DROPPED
```

不能用 `(account_id, state)` 唯一键模拟 partial unique index：同一个 `account_id`
被多次复用和删除后会存在多条 `DROPPED` 历史行。`account_current` 才是唯一当前身份，
`account_identities` 保存历史。

首次创建 Profile/Binding 时：

1. system transaction 按主键锁定权威 `mo_account` 行并读取其持久 RowID；
2. 按主键锁定 `account_current(account_id)`；
3. current 存在时校验 catalog RowID 相同且对应 identity 为 `ACTIVE`；
4. current 不存在时插入随机 incarnation、catalog RowID 的 identity 和 current；
5. tenant Binding/Profile 冻结该 incarnation。

`account_catalog_rowid` 使用 MO RowID 的固定 binary，不使用 account name、created_time
或可变 status/version。找不到权威 `mo_account` RowID，或 RowID 与 current 不同，
旧调用必须失败，不能为已经删除的账户重新创建 incarnation。

DROP ACCOUNT 当前由 system account 的大事务清理目标 tenant cluster rows。Lifecycle
必须在**同一个现有 DROP ACCOUNT 事务**中：

1. 按 account ID/当前 catalog RowID CAS identity `ACTIVE -> DROPPED`；
2. 插入 account owner tombstone；
3. 按 `(account_id, incarnation, account_catalog_rowid, version)` 删除 `account_current`；
4. 清理目标 tenant cluster rows；
5. 提交。

这样 DROP 事务失败时不会留下假的 owner-dropped 事实；也不引入跨事务提交窗口。
后续复用同一个 `account_id` 会创建新的 incarnation，历史 Root 永远不会被新租户接管。

## 3. Tenant transaction plane

### 3.1 Feature Guard

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_feature_guard (
    account_incarnation     BINARY(16) NOT NULL,
    logical_database_id     BIGINT UNSIGNED NOT NULL,
    logical_table_id        BIGINT UNSIGNED NOT NULL,
    physical_database_id    BIGINT UNSIGNED NOT NULL,
    physical_table_id       BIGINT UNSIGNED NOT NULL,
    schema_generation       BIGINT UNSIGNED NOT NULL,
    binding_generation      BIGINT UNSIGNED NOT NULL,
    dependency_bits         BIGINT UNSIGNED NOT NULL,
    owner_state             TINYINT UNSIGNED NOT NULL,
    guard_digest            BINARY(32) NOT NULL,
    version                 BIGINT UNSIGNED NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_incarnation, logical_table_id)
);
```

`dependency_bits` 固定位：

| Bit | 含义 |
|---:|---|
| 0 | CDC |
| 1 | FK parent/child |
| 2 | Publication |
| 3 | materialized secondary/unique index |
| 4 | Fulltext |
| 5 | Vector |
| 6 | plugin-managed dependency |
| 7 | Snapshot/PITR |
| 8 | Backup/Clone/Branch |
| 9 | DR/failover relationship |

`owner_state`：

```text
ACTIVE
TRUNCATING
DROPPING
DROPPED
```

首次 CAS：

```sql
INSERT ... version = 1
```

唯一键冲突后重读，不使用 `INSERT IGNORE`。每次受控操作执行：

```sql
UPDATE ... SET ..., version = version + 1
WHERE account_incarnation = ?
  AND logical_table_id = ?
  AND version = ?
  AND guard_digest = ?;
```

affected rows 必须为 1；否则事务 abort 并重做准入检查。

Guard不是全表dependency registry。只有Lifecycle Binding仍占有功能，或Unbind后仍有
active child、`FINALIZING/COMMIT_UNKNOWN`需要收敛时才允许存在；ERROR/PAUSED状态仍属于
未解除的Binding，也继续占有Guard。DROP TABLE完成owner tombstone/fence后由Root接管
Cleanup，不需要保留tenant Guard。

Lifecycle Bind和所有不兼容DDL/依赖创建先获取相同的`mo_tables`逻辑行排他锁并持有到
事务终态。Bind在锁内检查真实依赖后原子插入Binding+Guard；普通DDL在锁内先读Binding，
无Binding时不创建、不读取、不CAS Guard，按现有路径继续。Bind/DDL首次竞态由现有
table DDL lock关闭，不依赖给未绑定表预建Guard。多表操作按logical table identity稳定
排序加锁；Guard只有在Binding和全部未决child收敛后才能删除。

`max-bound-tables`的准入计数就是权威Guard行数，而不是只数`ACTIVE Binding`：

```text
COUNT(authoritative Feature Guard rows)
<= max-bound-tables
```

因此Unbind卡在unknown child时仍占用一个名额，新Bind在同一权威计数达到上限时返回
`LIFECYCLE_BINDING_CAPACITY`，不能通过“先Unbind一批旧表、再Bind新表”制造无界Guard。
Bind先取得仅用于Lifecycle Bind的cluster-scoped capacity lock，再取得table DDL lock，
在capacity lock内分页计算最多1000行的Guard权威计数并持锁到Bind事务终态；锁顺序固定为
`capacity -> logical table identity`。该锁不进入普通DDL、DML、查询或Merge路径，获取
失败、超时或计数不确定都拒绝新Bind。
同一logical table在旧Guard收敛删除前禁止重新Bind。DROP TABLE在同一DROP事务写入
system-owned owner tombstone并fence旧attempt后，可以随tenant Catalog级联删除Guard；
后续外部对象清理由Root接管，不再依赖tenant Guard。

`schema_generation` 在以下动作递增：

- TRUNCATE；
- ALTER COPY/会替换 physical table 的 DDL；
- Lifecycle 不支持但解除 Binding 后执行的 schema change；
- physical table ID 变化；
- 恢复/Clone 产生新的物理表。

Rename 只更新显示名，不改变 table ID，但递增 Guard `version`，使正在 finalizing 的 child 重读 owner。

### 3.2 Binding

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_bindings (
    binding_id              BINARY(16) NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    logical_database_id     BIGINT UNSIGNED NOT NULL,
    logical_table_id        BIGINT UNSIGNED NOT NULL,
    physical_database_id    BIGINT UNSIGNED NOT NULL,
    physical_table_id       BIGINT UNSIGNED NOT NULL,
    schema_generation       BIGINT UNSIGNED NOT NULL,
    schema_version          BIGINT UNSIGNED NOT NULL,
    schema_digest           BINARY(32) NOT NULL,
    lifecycle_column_id     BIGINT UNSIGNED NOT NULL,
    lifecycle_column_type   SMALLINT UNSIGNED NOT NULL,
    mode                    TINYINT UNSIGNED NOT NULL,
    action_after_micros     BIGINT UNSIGNED NOT NULL,
    archive_retention_us    BIGINT UNSIGNED NOT NULL,
    profile_id              BINARY(16) NULL,
    profile_version         BIGINT UNSIGNED NULL,
    storage_namespace_id    BINARY(16) NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    binding_generation      BIGINT UNSIGNED NOT NULL,
    active_child_generation BIGINT UNSIGNED NOT NULL,
    active_attempt_id       BINARY(16) NULL,
    active_executor_epoch   BIGINT UNSIGNED NOT NULL,
    next_scan_at            TIMESTAMP(6) NOT NULL,
    last_error_code         VARCHAR(64) NULL,
    last_error_message      VARCHAR(1024) NULL,
    version                 BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_incarnation, logical_table_id),
    UNIQUE KEY lifecycle_binding_id (account_incarnation, binding_id)
);
```

`mode`：

```text
TTL = 1
ARCHIVE = 2
```

Binding 状态机：

```text
ENABLING
  -> ACTIVE              Guard准入完成且Discovery scan state已创建
  -> ERROR               准入/Discovery terminal error

ACTIVE
  -> PAUSED              用户/配额/kill switch
  -> DISABLING           UNSET
  -> ERROR               Catalog 不变量损坏

PAUSED
  -> ACTIVE              RESUME，binding generation + 1
  -> DISABLING

DISABLING
  -> DISABLED            无 active/in-doubt child
  -> ERROR
```

禁止 `ERROR -> ACTIVE` 直接更新。修复原因后执行显式 `RECHECK/RESUME`，创建新 generation。

`active_attempt_id/epoch` 是 tenant final transaction 的 stale executor fence：

```sql
UPDATE binding
SET ...
WHERE account_incarnation = ?
  AND binding_id = ?
  AND binding_generation = ?
  AND active_child_generation = ?
  AND active_attempt_id = ?
  AND active_executor_epoch = ?
  AND version = ?;
```

final transaction affected rows 不是 1 时必须整体 abort。

### 3.3 Discovery Scan State 和 Candidate

当前 Relation Metadata/PartitionState 是当前有效 Object 的唯一权威来源。Lifecycle
不把全部 Object 复制到 Catalog，也不要求 Logtail/Merge 双写一个新索引。

每个 Binding 只有一行分页扫描状态：

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_scan_state (
    account_incarnation    BINARY(16) NOT NULL,
    binding_id             BINARY(16) NOT NULL,
    binding_generation     BIGINT UNSIGNED NOT NULL,
    physical_table_id      BIGINT UNSIGNED NOT NULL,
    schema_generation      BIGINT UNSIGNED NOT NULL,
    cycle_id               BIGINT UNSIGNED NOT NULL,
    cycle_snapshot_ts      VARBINARY(16) NOT NULL,
    after_object_id        VARBINARY(60) NULL,
    collect_watermark_ts   VARBINARY(16) NOT NULL,
    state                  TINYINT UNSIGNED NOT NULL,
    next_scan_at           TIMESTAMP(6) NOT NULL,
    cycle_started_at       TIMESTAMP(6) NOT NULL,
    cycle_completed_at     TIMESTAMP(6) NULL,
    last_error_code        VARCHAR(64) NULL,
    last_error_message     VARCHAR(1024) NULL,
    version                BIGINT UNSIGNED NOT NULL,
    updated_at             TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_incarnation, binding_id)
);
```

`after_object_id` 只在固定 `cycle_snapshot_ts` 内解释。Binding/schema/physical table
generation 变化时旧 cursor 作废，新 cycle 从空 cursor 开始。

只有进入近期执行窗口的 Object 才形成有界 Candidate：

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_candidates (
    account_incarnation    BINARY(16) NOT NULL,
    candidate_id           BINARY(32) NOT NULL,
    binding_id             BINARY(16) NOT NULL,
    binding_generation     BIGINT UNSIGNED NOT NULL,
    discovery_cycle_id     BIGINT UNSIGNED NOT NULL,
    child_generation       BIGINT UNSIGNED NOT NULL,
    physical_table_id      BIGINT UNSIGNED NOT NULL,
    schema_generation      BIGINT UNSIGNED NOT NULL,
    source_snapshot_ts     VARBINARY(16) NOT NULL,
    source_object_id       VARBINARY(60) NOT NULL,
    source_object_digest   BINARY(32) NOT NULL,
    lifecycle_zm_digest    BINARY(32) NOT NULL,
    classification        TINYINT UNSIGNED NOT NULL,
    estimated_source_bytes BIGINT UNSIGNED NOT NULL,
    estimated_expired_rows BIGINT UNSIGNED NOT NULL,
    estimated_live_rows    BIGINT UNSIGNED NOT NULL,
    cutoff_utc             TIMESTAMP(6) NOT NULL,
    next_action_at         TIMESTAMP(6) NOT NULL,
    state                  TINYINT UNSIGNED NOT NULL,
    version                BIGINT UNSIGNED NOT NULL,
    created_at             TIMESTAMP(6) NOT NULL,
    updated_at             TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_incarnation, candidate_id),
    KEY lifecycle_candidate_due (
        account_incarnation,
        binding_id,
        state,
        next_action_at
    )
);
```

`candidate_id` 是
`H(binding_id, binding_generation, cycle_id, object_id, cutoff)`，分页事务重试时幂等。
每表、每账户和集群 Candidate 数都有硬上限；完成、stale 或 replan 后只保留短审计
窗口。

Candidate 不是 retirement authority。Executor 必须重新读取当前 Relation Metadata，
获取 source reservation/GC protection，并在 TN Prepare 再校验 exact Object。可选
packed Discovery Summary 只能是可丢弃优化，Catalog 仅保存 root/version/watermark。

### 3.3.1 Binding Rewrite Runtime Stats

为限制高度乱序表反复整Object Rewrite，只增加每Binding固定大小的rolling统计，
不建立逐Object Index，也不把高频计数更新写入Binding权威CAS行：

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_binding_runtime_stats (
    account_incarnation       BINARY(16) NOT NULL,
    binding_id                BINARY(16) NOT NULL,
    window_kind               TINYINT UNSIGNED NOT NULL,
    bucket_ordinal            TINYINT UNSIGNED NOT NULL,
    bucket_start              TIMESTAMP(6) NOT NULL,
    attempted_source_bytes    BIGINT UNSIGNED NOT NULL,
    committed_retired_expired_bytes BIGINT UNSIGNED NOT NULL,
    aborted_read_bytes        BIGINT UNSIGNED NOT NULL,
    aborted_write_bytes       BIGINT UNSIGNED NOT NULL,
    consecutive_blocked_count INT UNSIGNED NOT NULL,
    version                   BIGINT UNSIGNED NOT NULL,
    updated_at                TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (
        account_incarnation,
        binding_id,
        window_kind,
        bucket_ordinal
    )
);
```

`window_kind`固定为24小时的hour bucket ring和7天的day bucket ring；bucket数量是
release-profile常量，禁止按Job/Object无限追加。更新只CAS当前bucket，不修改Binding
generation、active attempt或Feature Guard，因此不会把Scanner/finalizer集中到同一
热点行。bucket rollover先按`bucket_start + version` CAS清零后复用ordinal。
`consecutive_blocked_count`不跨bucket求和；rollover时从最近有效bucket复制，后续
成功commit清零、blocked加一，因此重启和跨整点都不会绕过阻断。

权威计算：

```text
rewrite_amplification =
  sum(attempted_source_bytes, window)
  / max(sum(committed_retired_expired_bytes, window), 1)
```

`attempted_source_bytes`在Rewrite开始读取前记账，失败/abort也保留；只有final
transaction committed后才增加`committed_retired_expired_bytes`。读写一部分后失败分别进入
`aborted_read_bytes/aborted_write_bytes`。阈值由认证结果冻结，超限只生成
`MIXED_LAYOUT_BLOCKED`，不能修改数据或通过重启清空历史。

### 3.4 Dataset

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_datasets (
    dataset_id              BINARY(16) NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    binding_id              BINARY(16) NOT NULL,
    logical_database_id     BIGINT UNSIGNED NOT NULL,
    logical_table_id        BIGINT UNSIGNED NOT NULL,
    source_physical_table_id BIGINT UNSIGNED NOT NULL,
    source_schema_generation BIGINT UNSIGNED NOT NULL,
    source_schema_version   BIGINT UNSIGNED NOT NULL,
    source_schema_digest    BINARY(32) NOT NULL,
    source_snapshot_ts      VARBINARY(16) NOT NULL,
    evaluation_time_utc     TIMESTAMP(6) NOT NULL,
    cutoff_utc              TIMESTAMP(6) NOT NULL,
    source_object_digest    BINARY(32) NOT NULL,
    source_visible_rows     BIGINT UNSIGNED NOT NULL,
    source_content_root     BINARY(32) NOT NULL,
    manifest_key            VARCHAR(2048) NOT NULL,
    manifest_size           BIGINT UNSIGNED NOT NULL,
    manifest_sha256         BINARY(32) NOT NULL,
    manifest_root           BINARY(32) NOT NULL,
    payload_count           INT UNSIGNED NOT NULL,
    payload_bytes           BIGINT UNSIGNED NOT NULL,
    lifecycle_min           VARBINARY(64) NOT NULL,
    lifecycle_max           VARBINARY(64) NOT NULL,
    profile_id              BINARY(16) NOT NULL,
    profile_version         BIGINT UNSIGNED NOT NULL,
    storage_namespace_id    BINARY(16) NOT NULL,
    encryption_digest       BINARY(32) NOT NULL,
    purge_eligible_at       TIMESTAMP(6) NOT NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    access_generation       BIGINT UNSIGNED NOT NULL,
    publish_txn_id          VARBINARY(128) NOT NULL,
    last_restore_id         BINARY(16) NULL,
    last_restore_at         TIMESTAMP(6) NULL,
    version                 BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_incarnation, dataset_id),
    KEY lifecycle_dataset_time (
        account_incarnation,
        logical_table_id,
        lifecycle_min,
        lifecycle_max
    ),
    KEY lifecycle_dataset_purge_due (
        account_incarnation,
        state,
        purge_eligible_at,
        dataset_id
    )
);
```

Dataset 只在 final transaction 中从“不存在”插入为 `PUBLISHED`。`VERIFIED_NOT_PUBLISHED`
属于 system Root/Manifest 状态，不在 tenant 可见 Dataset 表制造预发布行。
`publish_txn_id`是final transaction内可写入的权威identity；真正CommitTS在提交前尚不
存在，不进入本行。`purge_eligible_at`只表示数据生命周期保留截止时间。Purge还必须
等待matching Root的`observed_commit_ts`非NULL并超过minimum publish grace。

状态机：

```text
PUBLISHED
  -> DELETE_PENDING         retention/PURGE/owner cleanup

DELETE_PENDING
  -> DELETING

DELETING
  -> PURGED
  -> DELETE_FAILED

DELETE_FAILED
  -> DELETING               新 Sweeper generation 接管重试
```

`DELETING` 不可回到 `PUBLISHED`。如果需要保留，必须在进入删除前取消 Purge；进入删除后只能复制到新 immutable Dataset。

Provider、credential 或 DR 环境暂时不可访问不是 Dataset 持久状态。Dataset 保持
`PUBLISHED`，Restore 返回 `LIFECYCLE_ARCHIVE_UNAVAILABLE` 并记录独立健康指标；
恢复可访问性后无需改 Dataset 状态。内容校验失败是安全告警，同样不能通过一次
状态更新绕过 checksum 或自动进入 Purge。

### 3.5 Receipt

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_receipts (
    attempt_id              BINARY(16) NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    entry_digest            BINARY(32) NOT NULL,
    transaction_id          VARBINARY(128) NOT NULL,
    binding_id              BINARY(16) NOT NULL,
    child_generation        BIGINT UNSIGNED NOT NULL,
    executor_epoch          BIGINT UNSIGNED NOT NULL,
    retirement_mode         TINYINT UNSIGNED NOT NULL,
    source_object_digest    BINARY(32) NOT NULL,
    source_visible_rows     BIGINT UNSIGNED NOT NULL,
    source_content_root     BINARY(32) NULL,
    dataset_id              BINARY(16) NULL,
    manifest_root           BINARY(32) NULL,
    protocol_version        INT UNSIGNED NOT NULL,
    capability_version      INT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (account_incarnation, attempt_id),
    UNIQUE KEY lifecycle_txn_receipt (
        account_incarnation,
        transaction_id
    ),
    KEY lifecycle_receipt_gc (
        account_incarnation,
        retirement_mode,
        created_at,
        attempt_id
    )
);
```

Receipt：

- 只插入，不更新；
- 与退休在同一 tenant transaction；
- 重复 attempt ID 但 digest 不同是 corruption，不能覆盖；
- 是 committed 的不可变业务证据；
- 对账读取必须在正常一致性事务中进行。

Receipt不保存提交前无法获得的CommitTS。Reconciler用`transaction_id`查询权威事务终态，
并把已提交事务的真实CommitTS持久化到system-owned Root；不能提交后回写不可变Receipt。

### 3.6 Restore chunk Receipt

Restore 可以分多个有界事务写隐藏 staging table。每个已提交 chunk 必须和 staging
写入在同一 tenant 事务中记录：

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_restore_chunks (
    restore_id              BINARY(16) NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    dataset_id              BINARY(16) NOT NULL,
    payload_ordinal         INT UNSIGNED NOT NULL,
    row_group_ordinal       INT UNSIGNED NOT NULL,
    chunk_digest            BINARY(32) NOT NULL,
    transaction_id          VARBINARY(128) NOT NULL,
    row_count               BIGINT UNSIGNED NOT NULL,
    content_root            BINARY(32) NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (
        account_incarnation,
        restore_id,
        dataset_id,
        payload_ordinal,
        row_group_ordinal
    ),
    UNIQUE KEY lifecycle_restore_chunk_txn (
        account_incarnation,
        transaction_id
    ),
    KEY lifecycle_restore_chunk_gc (
        account_incarnation,
        created_at,
        restore_id
    )
);
```

Restore chunk同样不在原事务中保存CommitTS。`transaction_id + chunk_digest`负责幂等与
unknown对账；需要长期观测值时写Restore Attempt的system-owned可变状态，不更新chunk
Receipt。

该 Receipt 只插入、不更新。相同 chunk key 但 digest 不同是 corruption。它解决
Restore chunk commit response lost 后的重复插入问题。

### 3.7 Table/database owner tombstone

```sql
CREATE CLUSTER TABLE mo_catalog.mo_lifecycle_owner_tombstones (
    owner_kind             TINYINT UNSIGNED NOT NULL,
    logical_database_id    BIGINT UNSIGNED NOT NULL,
    logical_table_id       BIGINT UNSIGNED NOT NULL,
    account_incarnation    BINARY(16) NOT NULL,
    owner_generation       BIGINT UNSIGNED NOT NULL,
    drop_txn_id            VARBINARY(128) NOT NULL,
    drop_at                TIMESTAMP(6) NOT NULL,
    version                BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (
        account_incarnation,
        owner_kind,
        logical_database_id,
        logical_table_id,
        owner_generation
    )
);
```

`owner_kind`：

```text
TABLE = 1
DATABASE = 2
```

`TABLE` 必须保存真实 logical table ID；`DATABASE` 固定
`logical_table_id = 0`，而正常可分配 table ID 不允许为 0。查询必须先匹配
`owner_kind`，不能把 0 当通配符。

DROP 主事务只插入一行，不枚举 Dataset/Payload。DROP ACCOUNT 后这些 tenant rows 会消失，因此账户删除使用 system retained tombstone。

## 4. System retained control plane

所有 system retained 表：

- comment 包含 `mo_no_del_hint`；
- 由 system account 创建；
- 普通 tenant 无直接 DML 权限；
- 查询必须带 account incarnation 和分页 key；
- 不允许无条件扫描全表。

### 4.1 Archive Profile version

```sql
CREATE TABLE mo_catalog.mo_lifecycle_archive_profiles (
    account_id              BIGINT UNSIGNED NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    profile_id              BINARY(16) NOT NULL,
    profile_name            VARCHAR(256) NOT NULL,
    profile_version         BIGINT UNSIGNED NOT NULL,
    storage_namespace_id    BINARY(16) NOT NULL,
    provider_type           VARCHAR(32) NOT NULL,
    canonical_endpoint      VARCHAR(1024) NOT NULL,
    bucket_container        VARCHAR(512) NOT NULL,
    immutable_prefix        VARCHAR(1024) NOT NULL,
    capability_bits         BIGINT UNSIGNED NOT NULL,
    encryption_mode         TINYINT UNSIGNED NOT NULL,
    kms_key_ref             VARCHAR(1024) NOT NULL,
    encryption_digest       BINARY(32) NOT NULL,
    credential_generation   BIGINT UNSIGNED NOT NULL,
    credential_ref          VARCHAR(1024) NOT NULL,
    cleanup_credential_ref  VARCHAR(1024) NOT NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    namespace_digest        BINARY(32) NOT NULL,
    version                 BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (profile_id, profile_version),
    UNIQUE KEY lifecycle_profile_name (
        account_incarnation,
        profile_name,
        profile_version
    )
);
```

namespace 字段对已发布 version 不可更新。credential rotation 只更新：

```text
credential_generation + 1
credential_ref
cleanup_credential_ref
version + 1
```

并要求新 credential 通过 namespace digest 和 capability probe。

`encryption_mode/kms_key_ref` 是 Profile version 的存储身份组成部分。`ADD VERSION`
创建新 version 并允许更换 KMS key identity；旧 version 不能原地修改。provider 在同一 key identity
内部轮换 key material 时，不改变历史 Dataset 的解密路由。`encryption_digest`
覆盖 mode、canonical key identity 和 provider encryption capability，不包含密钥材料。
provider-managed SSE 使用规范常量作为 `kms_key_ref`，不以空字符串表达不同语义。

Profile 名解析只用于 Binding DDL：在同一 system transaction 中选择最高 ACTIVE
version 并冻结 `(profile_id, profile_version, storage_namespace_id)`。Job/Restore/Purge
以后只按冻结 ID/version 查找，不能再次按名称解析“最新版本”。

### 4.2 Job/Attempt Control

```sql
CREATE TABLE mo_catalog.mo_lifecycle_job_control (
    account_id              BIGINT UNSIGNED NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    logical_database_id     BIGINT UNSIGNED NOT NULL,
    logical_table_id        BIGINT UNSIGNED NOT NULL,
    binding_id              BINARY(16) NOT NULL,
    job_id                  BINARY(16) NOT NULL,
    parent_job_id           BINARY(16) NULL,
    child_generation        BIGINT UNSIGNED NOT NULL,
    attempt_id              BINARY(16) NOT NULL,
    executor_epoch          BIGINT UNSIGNED NOT NULL,
    task_kind               TINYINT UNSIGNED NOT NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    evaluation_time_utc     TIMESTAMP(6) NOT NULL,
    cutoff_utc              TIMESTAMP(6) NOT NULL,
    source_snapshot_ts      VARBINARY(16) NULL,
    source_object_digest    BINARY(32) NULL,
    source_object_count     INT UNSIGNED NOT NULL,
    source_bytes            BIGINT UNSIGNED NOT NULL,
    entry_digest            BINARY(32) NULL,
    final_txn_id            VARBINARY(128) NULL,
    final_txn_status        TINYINT UNSIGNED NOT NULL,
    lease_owner_cn          VARCHAR(64) NULL,
    lease_expire_at         TIMESTAMP(6) NULL,
    heartbeat_at            TIMESTAMP(6) NULL,
    attempt_count           INT UNSIGNED NOT NULL,
    conflict_started_at     TIMESTAMP(6) NULL,
    next_action_at          TIMESTAMP(6) NOT NULL,
    deadline_at             TIMESTAMP(6) NOT NULL,
    last_error_code         VARCHAR(64) NULL,
    last_error_message      VARCHAR(1024) NULL,
    state_version           BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (attempt_id),
    UNIQUE KEY lifecycle_child_generation (
        job_id,
        child_generation
    ),
    KEY lifecycle_job_due (
        state,
        next_action_at,
        account_incarnation,
        logical_table_id
    )
);
```

`task_kind`：

```text
POLICY_SCAN
DISCOVERY_SCAN
WHOLE_TTL
WHOLE_ARCHIVE
SMALL_MIXED_TTL
SMALL_MIXED_ARCHIVE
MIXED_REWRITE_TTL
MIXED_REWRITE_ARCHIVE
RESTORE
PURGE
CLEANUP
RECONCILE
```

退休 child 状态机：

```text
PLANNED
  -> REGISTERED
  -> READING | REWRITING

REGISTERED | READING | REWRITING
  -> UPLOADING             # Archive output finalization
  -> VERIFIED              # TTL or completed Archive verification
  -> RETRYABLE | ABORTED
  -> RESOURCE_BLOCKED | MIXED_LAYOUT_BLOCKED | CONFLICT_BLOCKED

UPLOADING
  -> VERIFIED
  -> RETRYABLE | ABORTED

VERIFIED
  -> FINALIZING

FINALIZING
  -> COMMITTED | ABORTED | COMMIT_UNKNOWN

COMMIT_UNKNOWN
  -> COMMITTED | ABORTED | MANUAL_RECONCILE_REQUIRED
```

blocked 是当前 child generation 的终态；重新执行必须由 Planner 创建新 generation。
`MANUAL_RECONCILE_REQUIRED`仍保留原 transaction identity 和所有权，不代表 aborted。

Job Control 是调度和对账权威；TaskService task 只是唤醒/投递。

状态转换必须使用：

```sql
UPDATE ...
SET state = ?, state_version = state_version + 1, ...
WHERE attempt_id = ?
  AND state = ?
  AND executor_epoch = ?
  AND state_version = ?;
```

affected rows 不是 1 表示 executor 已被 fence。

### 4.3 Cleanup Root

```sql
CREATE TABLE mo_catalog.mo_lifecycle_cleanup_roots (
    root_id                 BINARY(16) NOT NULL,
    account_id              BIGINT UNSIGNED NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    logical_database_id     BIGINT UNSIGNED NOT NULL,
    logical_table_id        BIGINT UNSIGNED NOT NULL,
    source_physical_table_id BIGINT UNSIGNED NOT NULL,
    source_schema_generation BIGINT UNSIGNED NOT NULL,
    job_id                  BINARY(16) NOT NULL,
    attempt_id              BINARY(16) NOT NULL,
    executor_epoch          BIGINT UNSIGNED NOT NULL,
    root_kind               TINYINT UNSIGNED NOT NULL,
    dataset_id              BINARY(16) NULL,
    profile_id              BINARY(16) NULL,
    profile_version         BIGINT UNSIGNED NULL,
    archive_storage_namespace_id BINARY(16) NULL,
    archive_encryption_digest BINARY(32) NULL,
    tae_storage_namespace_id BINARY(16) NULL,
    tae_encryption_digest   BINARY(32) NULL,
    deterministic_prefix    VARCHAR(2048) NOT NULL,
    manifest_key            VARCHAR(2048) NULL,
    manifest_size           BIGINT UNSIGNED NOT NULL,
    manifest_sha256         BINARY(32) NULL,
    manifest_root           BINARY(32) NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    access_generation       BIGINT UNSIGNED NOT NULL,
    final_txn_id            VARBINARY(128) NULL,
    final_entry_digest      BINARY(32) NULL,
    final_txn_status        TINYINT UNSIGNED NOT NULL,
    observed_commit_ts      VARBINARY(16) NULL,
    lease_owner_cn          VARCHAR(64) NULL,
    lease_expire_at         TIMESTAMP(6) NULL,
    first_io_at             TIMESTAMP(6) NULL,
    last_io_at              TIMESTAMP(6) NULL,
    cleanup_after           TIMESTAMP(6) NULL,
    quiescence_until        TIMESTAMP(6) NULL,
    state_version           BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (root_id),
    UNIQUE KEY lifecycle_attempt_root (attempt_id),
    UNIQUE KEY lifecycle_dataset_root (dataset_id),
    KEY lifecycle_cleanup_due (state, cleanup_after)
);
```

`root_kind` 区分 `ARCHIVE`、`ARCHIVE_REWRITE` 和 `TTL_REWRITE`。字段组合：

| root_kind | Archive namespace/encryption | TAE namespace/encryption |
|---|---|---|
| `ARCHIVE` | NOT NULL | NULL |
| `ARCHIVE_REWRITE` | NOT NULL | NOT NULL |
| `TTL_REWRITE` | NULL，Profile/Dataset为空 | NOT NULL |

Archive Rewrite同时写Archive Provider和当前TAE shared FileService，两者通常不是同一
namespace，不能用一个父级`storage_namespace_id`表示。两个identity都在Root创建时
冻结；credential rotation不能改变对应namespace。Root Object再按kind选择其中一个。
未启用额外encryption时仍使用canonical `NO_ENCRYPTION`配置digest，不以NULL绕过
Object级identity校验；父字段NULL只表示该Root不使用这一类namespace。

`root_kind`表示attempt可能产生的外部namespace集合，不等同于最终
`LifecycleCommitEntry.mode`。计划为Archive Rewrite的attempt可以在首次E行前创建
`ARCHIVE_REWRITE`父Root并冻结双namespace，随后因`live == 0`退化为
`WHOLE_ARCHIVE`；此时允许只有Archive child，禁止TAE range/live/booking child。
计划为Rewrite不意味着必须预创建child：

| 实际分类结果 | Root | TAE range/live child | Booking child |
|---|---|---|---|
| `visible == 0` | 无 | 无 | 无 |
| `expired == 0, live > 0` | 仅在已开始live写入时存在，随后清理 | 可存在但不得`TAE_OWNED` | 无 |
| `expired > 0, live == 0` Archive | `ARCHIVE_REWRITE`可退化Whole | 无 | 无 |
| `expired > 0, live == 0` TTL | 无 | 无 | 无 |
| `expired > 0, live > 0` | 按计划创建 | 必须 | 必须 |

Root ID、segment/range/booking名称可以在读前预分配并加入SyncProtection，但Catalog
Root/child只在对应第一次外部副作用前持久化。

Root 状态机：

```text
REGISTERED
  -> UPLOADING
  -> DELETE_PENDING        明确 abort/cancel before publish

UPLOADING
  -> VERIFIED
  -> DELETE_PENDING

VERIFIED
  -> FINALIZING
  -> DELETE_PENDING

FINALIZING
  -> PUBLISHED             Archive committed + matching Receipt/Dataset
  -> POST_COMMIT_CLEANUP   TTL Rewrite committed + matching Receipt
  -> DELETE_PENDING        explicitly aborted
  -> FINALIZING            in-doubt

PUBLISHED
  -> DELETE_PENDING        retention/PURGE/owner drop

POST_COMMIT_CLEANUP
  -> TRANSFERRED           temporary booking全部确认删除

TRANSFERRED
  -> TRANSFERRED           TAE已经接管live且booking已删；短审计期后Catalog GC

DELETE_PENDING
  -> DELETING

DELETING
  -> CLEANED
  -> DELETE_FAILED

DELETE_FAILED
  -> DELETING

CLEANED
  -> CLEANED               保留 tombstone 直到 quiescence
```

不可转换：

- `FINALIZING -> DELETE_PENDING` 仅凭 timeout；
- `DELETING -> PUBLISHED`；
- `CLEANED -> UPLOADING`；
- `TRANSFERRED -> DELETE_PENDING`；
- `POST_COMMIT_CLEANUP -> DELETE_PENDING`；
- 新 executor 复用旧 deterministic prefix。

### 4.4 Root Object

```sql
CREATE TABLE mo_catalog.mo_lifecycle_cleanup_objects (
    root_id                 BINARY(16) NOT NULL,
    object_kind             TINYINT UNSIGNED NOT NULL,
    ordinal                 INT UNSIGNED NOT NULL,
    storage_namespace_id    BINARY(16) NOT NULL,
    encryption_digest       BINARY(32) NOT NULL,
    immutable_key           VARCHAR(2048) NOT NULL,
    immutable_key_digest    BINARY(32) NOT NULL,
    provider_version        VARCHAR(512) NULL,
    upload_id               VARCHAR(1024) NULL,
    tae_segment_id          BINARY(16) NULL,
    tae_ordinal_limit       SMALLINT UNSIGNED NULL,
    size_bytes              BIGINT UNSIGNED NOT NULL,
    sha256                  BINARY(32) NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    delete_attempts         INT UNSIGNED NOT NULL,
    last_error_code         VARCHAR(64) NULL,
    state_version           BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (root_id, object_kind, ordinal),
    UNIQUE KEY lifecycle_immutable_key (
        storage_namespace_id,
        immutable_key_digest
    )
);
```

Provider key 只在 namespace 内唯一，不能对 `immutable_key` 单列建立全局唯一约束。
也不能把 2048-byte key 直接做宽唯一索引。`immutable_key_digest` 是 canonical UTF-8
key bytes 的 SHA-256；唯一键冲突后必须读取原 key 做完整比较，digest 相同但 key
不同按 corruption 处理。Root Object按`object_kind`校验父Root：

```text
ARCHIVE_PAYLOAD/MANIFEST/SIDECAR
  -> storage_namespace_id == root.archive_storage_namespace_id
  -> encryption_digest == root.archive_encryption_digest

TAE_LIVE_SEGMENT_RANGE/LIVE_STAGING_OBJECT/TRANSFER_BOOKING
  -> storage_namespace_id == root.tae_storage_namespace_id
  -> encryption_digest == root.tae_encryption_digest
```

任何kind对应的父identity为空、namespace不匹配或跨namespace key复用都按corruption
处理。Catalog不变量检查器持续抽样校验。

`object_kind`：

```text
ARCHIVE_PAYLOAD
ARCHIVE_MANIFEST
ARCHIVE_SIDECAR
TAE_LIVE_SEGMENT_RANGE
TAE_LIVE_STAGING_OBJECT
TAE_TRANSFER_BOOKING
```

Root Object 状态除上传态外增加 `TAE_OWNED`。只有一致性读取确认 final transaction
committed 后，live/range child 才能从 `VERIFIED` 转为`TAE_OWNED`；Sweeper永远
不能删除`TAE_OWNED`。这里的`TAE_OWNED`只适用于live/range child；temporary
booking在committed后走`DELETE_PENDING -> DELETING -> DELETED`。
`TAE_LIVE_SEGMENT_RANGE`冻结attempt专属segmentID和ordinal hard limit，负责枚举
尚未来得及写exact child的FileService对象。

状态：

```text
ALLOCATED
MULTIPART_CREATED
PUTTING
PUT_COMPLETE
VERIFIED
TAE_OWNED
DELETE_PENDING
DELETING
DELETED
DELETE_FAILED
ABORTED_MULTIPART
```

`ordinal`是`object_kind`内的确定性局部序号，不是Root内全局分配器。Archive
Payload、live Object和booking可各自从0开始；它们不会因kind不同发生主键冲突。
Root不保存无限数组。每个对象一行，按
`(root_id, object_kind, ordinal)`分页。

Lifecycle Rewrite创建的live/range/booking在final结果确定前，其物理Owner始终是
Cleanup Root。复用的TAE Merge txn entry只借用这些文件，不能在
`PrepareRollback`、`ErrTAENeedRetry`或普通error defer中删除。只有：

```text
committed + matching Receipt -> live/range VERIFIED -> TAE_OWNED
explicitly aborted           -> live/range/booking -> DELETE_PENDING
unknown                      -> 保持 VERIFIED
```

普通Merge不创建这些Root Object，仍由普通Merge entry清理自己的created files。
Lifecycle的Owner严格分层：

| 资源 | Owner |
|---|---|
| Root live/booking物理文件 | final结果明确前始终为Cleanup Root |
| SoftDelete/Create成功产生的Catalog node | API成功后立即为整个internal TAE txn |
| slab/page/TransferDels/decoder buffer | LogTxnEntry前为唯一builder，成功后为txn entry |

在任何SoftDelete/Create前，internal txn的ephemeral TxnMemo必须claim
`(attempt_id, entry_digest)` generation slot。只有`BUILDING` owner可以mutation；
失败使slot进入`FAILED`并rollback整个generation，禁止builder手动撤销Catalog node或
在同一generation重建。普通Merge的注册前runtime残留由
[#26445](https://github.com/matrixorigin/matrixone/issues/26445)独立跟踪；Lifecycle
优先实现local builder/slot，不以普通Merge修复作为前置。

### 4.5 Account owner tombstone

```sql
CREATE TABLE mo_catalog.mo_lifecycle_account_tombstones (
    account_id              BIGINT UNSIGNED NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    drop_txn_identity       VARBINARY(128) NOT NULL,
    drop_at                 TIMESTAMP(6) NOT NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    next_action_at          TIMESTAMP(6) NOT NULL,
    state_version           BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (account_id, account_incarnation),
    KEY lifecycle_account_cleanup_due (
        state,
        next_action_at,
        account_id,
        account_incarnation
    )
);
```

该行只表示 owner 已放弃所有 Archive Restore。它不表示 Provider 对象已经删除。

### 4.6 Restore Attempt

```sql
CREATE TABLE mo_catalog.mo_lifecycle_restore_attempts (
    restore_id              BINARY(16) NOT NULL,
    account_id              BIGINT UNSIGNED NOT NULL,
    account_incarnation     BINARY(16) NOT NULL,
    logical_table_id        BIGINT UNSIGNED NOT NULL,
    dataset_set_digest      BINARY(32) NOT NULL,
    executor_epoch          BIGINT UNSIGNED NOT NULL,
    target_database_id      BIGINT UNSIGNED NOT NULL,
    target_table_name       VARCHAR(512) NOT NULL,
    target_table_name_normalized VARCHAR(512) NOT NULL,
    staging_database_id     BIGINT UNSIGNED NULL,
    staging_table_id        BIGINT UNSIGNED NULL,
    staging_table_name      VARCHAR(512) NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    expected_rows           BIGINT UNSIGNED NOT NULL,
    restored_rows           BIGINT UNSIGNED NOT NULL,
    expected_root           BINARY(32) NOT NULL,
    restored_root           BINARY(32) NULL,
    current_dataset_id      BINARY(16) NULL,
    current_payload_ordinal INT UNSIGNED NULL,
    current_row_group       INT UNSIGNED NULL,
    current_chunk_digest    BINARY(32) NULL,
    current_txn_id          VARBINARY(128) NULL,
    current_txn_status      TINYINT UNSIGNED NOT NULL,
    lease_expire_at         TIMESTAMP(6) NULL,
    next_action_at          TIMESTAMP(6) NOT NULL,
    state_version           BIGINT UNSIGNED NOT NULL,
    last_error_code         VARCHAR(64) NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (restore_id),
    KEY lifecycle_restore_target_lookup (
        account_incarnation,
        target_database_id,
        target_table_name_normalized,
        state
    ),
    KEY lifecycle_restore_due (
        state,
        next_action_at,
        restore_id
    )
);
```

Restore 创建时可以用普通读检查并减少重复工作，但该索引不是正确性锁。两个 Restore
并发选择同一目标名时，最终发布事务必须取得正常 database/table-name DDL 锁，并以
目标表不存在的 Catalog 条件决胜；失败者清理自己的 staging。这样不依赖 MO 当前
不具备的 partial unique index，也不会因历史 `ABORTED/COMMITTED` 行阻止后续操作。

状态：

```text
PLANNED -> LEASED -> STAGING_CREATED -> WRITING -> VERIFYING
  -> PUBLISHING -> COMMITTED

任意 pre-commit 状态
  -> ABORTING -> ABORTED
  -> CLEANUP_FAILED
```

### 4.7 Access lease

```sql
CREATE TABLE mo_catalog.mo_lifecycle_access_leases (
    dataset_id              BINARY(16) NOT NULL,
    access_generation       BIGINT UNSIGNED NOT NULL,
    restore_id              BINARY(16) NOT NULL,
    executor_epoch          BIGINT UNSIGNED NOT NULL,
    lease_expire_at         TIMESTAMP(6) NOT NULL,
    state                   TINYINT UNSIGNED NOT NULL,
    state_version           BIGINT UNSIGNED NOT NULL,
    created_at              TIMESTAMP(6) NOT NULL,
    updated_at              TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (dataset_id, restore_id),
    KEY lifecycle_access_lease_due (
        state,
        lease_expire_at,
        dataset_id
    )
);
```

lease 状态：

```text
ACTIVE -> RELEASED
ACTIVE -> EXPIRED
ACTIVE -> FENCED
```

Lease row 不是永久 reference。Sweeper 只等待同一 `access_generation` 且未过期的 ACTIVE lease。

## 5. 事务边界

System control-plane 事务的全局锁序：

```text
account current/identity
  -> Profile version
  -> Job/Attempt Control（按attempt ID）
  -> Cleanup Root（按root ID）
  -> Root Object（按root ID, object kind, ordinal）
  -> Access Lease（按dataset ID, restore ID）
```

能拆成两个已提交事务的步骤不跨 RPC 持锁。Restore Attempt、Account Tombstone 等
单行控制状态也使用 ID 升序，一次事务最多处理一个有界 page。Sweeper 读取
owner tombstone 后再开独立 Root CAS，不能反向持 Root 锁等待 account identity。
任何实现需要反向顺序时必须拆事务和使用 version CAS，不允许新增锁序例外。

### 5.1 Binding DDL transaction

同一 tenant transaction：

```text
table DDL lock
  -> inspect authoritative table dependencies
  -> reject when unsupported dependency exists
  -> insert/update Binding
  -> create/CAS Feature Guard
  -> commit
```

Profile 版本在 system registry 已经 committed，Binding 只冻结 identity，不和 Profile 创建组成 2PC。
不兼容DDL/依赖创建获取同一锁后先读Binding；无Binding时不创建Guard。Unbind仅在
active/unknown child全部收敛后删除Guard。

### 5.2 Child claim

1. system transaction CAS Job Control lease/epoch；
2. tenant transaction CAS Binding active attempt/epoch；
3. system transaction确认 claim；
4. 任一步失败均释放或过期 lease，不开始 external PUT。

这个握手不负责数据原子性，只负责 stale executor fencing。

### 5.3 Final tenant transaction

Whole Archive：

```text
Root VERIFIED -> FINALIZING committed in system transaction
  -> begin tenant pessimistic transaction
  -> CAS Guard/Binding/active attempt
  -> insert Dataset
  -> insert Receipt
  -> internal adapter returns txn-bound LifecycleCatalogPairToken
  -> SealLifecycleCommit consumes token and atomically enters SEALED
  -> commit payload contains Catalog writes + Lifecycle tag
  -> commit
```

Mixed Archive：

```text
begin tenant writable SI transaction
  -> Reader/PUT/readback
  -> Root VERIFIED -> FINALIZING committed in system transaction
  -> resume same tenant SI transaction
  -> Relation.Delete
  -> CAS Guard/Binding/active attempt
  -> insert Dataset/Receipt
  -> commit
```

注意：system Root transaction 与 tenant SI transaction 不共享锁。Root CAS 完成后才向 tenant txn 写 DELETE/Catalog；Root 失败则 tenant txn rollback。

Mixed Rewrite：

```text
Root VERIFIED -> FINALIZING committed in system transaction
  -> begin short tenant transaction
  -> CAS Guard/Binding/active attempt
  -> insert Dataset/Receipt               # Archive
     or insert Receipt only               # TTL
  -> internal adapter returns txn-bound token then SealLifecycleCommit(
       exact source Objects + staged live Objects + transfer booking)
  -> commit payload contains Catalog writes + Lifecycle tag
  -> commit
```

这里wire为兼容Whole仍使用`source Objects`集合；`MIXED_REWRITE_*`模式的协议基数
必须等于1，且只允许immutable external booking。

TTL Whole/Rewrite不写Dataset，但同样在一个事务中写普通Receipt Catalog DML，并通过
独立CN commit-control把tag追加到同一个可重放commit payload。control-only不是合法生产
状态：token缺失或失效时finalizer拒绝提交。Seal后transaction立即Commit，不再进入通用SQL
statement流程。TTL Rewrite committed
后把live/range child标为`TAE_OWNED`，
Root进入`POST_COMMIT_CLEANUP`；temporary booking全部删除后才标`TRANSFERRED`。
所有模式都不依赖长时间 SQL 表锁。

TTL Whole：

```text
Attempt Control -> FINALIZING committed in system transaction
  -> tenant final transaction
  -> Guard/Binding + retire/delete + Receipt
```

### 5.4 Reconciliation transaction

Reconciler 不修改 tenant retirement transaction。它只：

1. 查询原 `final_txn_id`；
2. 在新的正常一致性 tenant read transaction 读取 Receipt/Dataset；
3. 在 system transaction CAS Root/Attempt 状态。

不允许“对账时补插 Receipt”或“看到 Object 已消失就补建 Dataset”。

## 6. 状态机通用规则

每张可变 control 表必须有：

```text
state
state_version
executor_epoch（执行相关行）
updated_at
next_action_at/deadline_at（等待相关行）
last_error
```

所有更新使用前置状态和 version CAS。禁止：

- 无条件 `UPDATE ... WHERE id = ?`；
- `INSERT ... ON DUPLICATE KEY UPDATE` 覆盖不同 digest；
- executor 只凭 TaskService epoch 更新；
- timeout 自动推断 final transaction aborted；
- `DELETE` control row 作为状态转换。

终态行的物理清理使用独立 retention：

| 行 | 最短保留 |
|---|---:|
| TTL Receipt | committed、无 in-doubt 引用后 90 天 |
| Archive Receipt | Dataset 为 PUBLISHED/删除中时始终保留；Dataset PURGED 后 30 天 |
| Restore chunk Receipt | Restore 终态、无 unknown chunk、staging 已发布/清理后 30 天 |
| COMMITTED/ABORTED Job Control | 30 天 |
| Blocked/Manual reconcile | 原因解除或人工审计后 30 天 |
| CLEANED Root tombstone | `max(30 天, max_io_deadline + multipart convergence + 24h quiescence)` |
| PURGED Dataset stub | 30 天 |
| Table/database owner tombstone | matching Dataset PURGED、Root CLEANED/TRANSFERRED、无 in-doubt 后 30 天 |
| Account tombstone | 全部 Root CLEANED/TRANSFERRED 后 30 天 |
| DROPPED account identity | matching Account tombstone 和全部 Root 清理后 30 天 |

保留期到达也必须由有界 GC Job 分页删除，不能在业务事务中批量清空。
`FINALIZING/COMMIT_UNKNOWN/MANUAL_RECONCILE_REQUIRED` 引用的 Receipt、chunk 或 identity
无论年龄多大都不能由 retention GC 删除。

## 7. 资源 Owner 表

| 资源 | 创建 Owner | Owner 转移 | 最终清理 Owner |
|---|---|---|---|
| Binding generation | DDL txn | Scheduler CAS | Binding GC |
| Discovery scan state/Candidate | Scanner epoch | Child claim CAS | Planner/Reconciler |
| Reader Batch | Reader | 不转移 | Reader |
| writable SI txn | Mixed Executor | commit unknown 时 txn client resolver | txn client |
| multipart | Root Object row | Writer lease -> Sweeper generation | Sweeper |
| staging Payload | Cleanup Root | Root PUBLISHED 后仍由 Root/Dataset共同引用 | Purge/owner-drop Sweeper |
| published Dataset | tenant Dataset | DELETE_PENDING CAS | Sweeper |
| source Object reservation | Lifecycle attempt | 不转移；TTL/epoch fence | TN reservation manager |
| source GC protection | Lifecycle attempt | 不转移；TTL/epoch fence | TN SyncProtection manager |
| Rewrite dense-memory admission/slab | Rewrite Executor/mergesort task | final entry可短暂借用copy；不持久转移 | task release/txn rollback or commit |
| live TAE staging Object | Cleanup Root | final commit 后转给 TAE Catalog | Sweeper/TAE GC |
| Lifecycle external transfer booking | Cleanup Root | 不转移；txn entry只读借用 | committed/aborted明确后Root Sweeper |
| TAE in-memory transfer page | final transaction | external booking在同一final txn的Prepare/retry中重建；commit后不转移 | TAE txn entry/transfer table TTL；TN restart后旧事务缺页必须冲突 |
| Restore staging table | Restore Attempt | executor epoch CAS | Restore Sweeper |
| Restore lease | Restore Attempt | 不转移；过期/fence | Lease Reconciler |

Executor 的 `defer` 只能清理仍由该 executor 独占、且未持久转移 Owner 的资源。

## 8. Catalog 访问包

新增独立包，禁止在 Executor 中散落 SQL 字符串：

```text
pkg/lifecycle/catalog/
  binding.go
  guard.go
  discovery_state.go
  runtime_stats.go
  dataset.go
  receipt.go
  profile.go
  job_control.go
  cleanup_root.go
  restore.go
  owner_tombstone.go
  transitions.go
```

每个 repository 方法返回：

```go
type CASResult struct {
    Matched bool
    Version uint64
}
```

方法命名体现事务要求：

```go
BindInTenantTxn(...)
PublishDatasetInTenantTxn(...)
InsertReceiptInTenantTxn(...)
MarkRootFinalizingInSystemTxn(...)
ClaimAttemptInSystemTxn(...)
```

禁止一个方法内部静默新开事务后又返回给调用方，造成调用方误以为仍在同一原子边界。

## 9. Schema 升级和降级

升级分两个生产版本：

1. Safety Release：
   - 创建system/tenant Catalog表和只读视图；
   - 所有TN在Batch解析前对unknown EntryType fail closed；
   - TN heartbeat经HAKeeper/ClusterDetails发布Lifecycle protocol capability；
   - CN支持authoritative refresh，但retirement永久关闭；
   - 只开放Discovery/Dry-run/Export-only。
2. Retirement Release：
   - 只有全部CN/TN的exact ServiceID/ShardID/ReplicaID capability ready后才能启用；
   - Whole、小Mixed、Rewrite分别受独立stage gate控制。

降级规则：

- 存在 ACTIVE Binding 或未 PURGED Dataset 时，不允许降到不认识 Lifecycle
  Catalog/tagged commit entry的版本；
- emergency downgrade 必须先 cluster kill switch、等待 finalizing/unknown 收敛、暂停 Binding；
- 旧版本不得把未知系统表当普通 tenant 表清理；
- 进入支持Lifecycle tag的协议代后，unknown version/非法tag必须在Batch解析前返回不支持，
  不能跳过后继续commit；
- 不能降到缺少Safety Release安全parser的版本。若产品不提供这一前置版本，则启用
  retirement后的滚动降级不在支持范围，不能用capability fence替代该限制。

## 10. Catalog 不变量检查器

后台每 10 分钟分页检查：

```text
Dataset PUBLISHED -> exactly one matching Root and Receipt
Root PUBLISHED -> matching Dataset/Receipt or owner already dropped; observed_commit_ts non-null
Root FINALIZING -> final_txn_id and entry_digest non-empty
Root DELETING/CLEANED -> no new ACTIVE lease at same/higher access generation
Root POST_COMMIT_CLEANUP -> matching Receipt; live/range TAE_OWNED
Root TRANSFERRED -> matching Receipt; live/range TAE_OWNED; booking DELETED
Receipt -> immutable digest and unique txn ID
Binding ACTIVE -> Guard dependency_bits == 0 and generation matches
Discovery scan state -> Binding/physical/schema generation matches
Job active -> Binding active attempt/epoch matches
Account current -> exactly one matching ACTIVE identity
Account tombstone -> no matching current row for the same incarnation
Root Object -> object_kind matches parent Archive/TAE namespace and encryption identity
```

任何安全不变量失败：

1. 集群 retirement kill switch；
2. 相关 scope 进入 `MANUAL_RECONCILE_REQUIRED`；
3. 不自动删除 Payload；
4. 保留 Catalog 和 Provider 证据；
5. 触发 P0 级告警。
