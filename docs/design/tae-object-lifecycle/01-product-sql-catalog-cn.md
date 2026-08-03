# 01 产品、SQL与最小Catalog详细设计

## 1. Phase 1产品范围

Phase 1是Issue #24552和#24853的首个Commercial GA子集：

- 表级TTL；
- 表级Archive；
- direct-readable Parquet/ZSTD；
- Restore到独立新表；
- Purge；
- 500～1000张显式绑定表和1/10 TiB认证。

不包含ONLINE_COLD、Deep Archive、account/database继承、Time Travel/Fail-safe替代品。
因此Phase 1完成不能直接关闭Issue #24853的全部产品愿景。

## 2. SQL合同

Phase 1只交付建表后的`ALTER TABLE`管理语法，不承诺`CREATE TABLE ... LIFECYCLE`：

```sql
ALTER TABLE db.t SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL '90' DAY,
  ACTION ARCHIVE,
  STAGE archive_stage,
  PURGE ELIGIBLE AFTER INTERVAL '730' DAY
);

ALTER TABLE db.t SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL '7' DAY,
  ACTION DELETE
);

ALTER TABLE db.t PAUSE LIFECYCLE;
ALTER TABLE db.t RESUME LIFECYCLE;
ALTER TABLE db.t UNSET LIFECYCLE;

SHOW LIFECYCLE FOR TABLE db.t;
SHOW LIFECYCLE JOBS;
SHOW LIFECYCLE JOBS LIMIT 1000 OFFSET 1000;
SHOW LIFECYCLE DATASETS FOR TABLE db.t;
SHOW LIFECYCLE DATASETS FOR TABLE db.t LIMIT 1000 OFFSET 1000;

RESTORE ARCHIVE DATASET '<dataset-id>' TO TABLE db.restored_t;
PURGE ARCHIVE DATASET '<dataset-id>';
```

`JOBS`和`DATASETS`默认、最大每页均为1000行，使用稳定的时间+ID排序；
`OFFSET + LIMIT`最大为1,000,000，禁止通过移除LIMIT改成无界查询。该接口读取live
Catalog，属于诊断性best-effort翻页：Catalog静止时可枚举窗口内全部Dataset ID，并发新增、
状态变化或终态回收期间跨页可能重复或漏项。`FOR TABLE`的Binding结果仍是单行。

时间语义：

- Phase 1只接受正数`INTERVAL ... DAY`，不接受MONTH/YEAR等变长日历单位；
- `expire_interval + late_arrival_grace`和`purge_interval`都必须能由worker使用的
  `time.Duration`精确表示；当前最大值为106751天，SET时越界直接拒绝，禁止运行时溢出成
  更早的cutoff或Purge时间；
- 每个child冻结`evaluation_time`，同一attempt重试不能重新取“现在”；
- `effective_cutoff = evaluation_time - expire_interval - late_arrival_grace`；
- TIMESTAMP按UTC比较；DATE/DATETIME使用Binding冻结的`evaluation_timezone`；
- `expire_at`表示开始具备处理资格，不承诺到点瞬间不可见；
- Archive完成前源数据继续可见；
- Archive成功后普通SELECT不再看到已退休行；
- `PURGE ELIGIBLE AFTER`按Lifecycle列的行龄计算，必须大于`EXPIRE AFTER`；
- 首个GA不为计算精确最早时间增加逐行min/max协议；使用保守边界
  `purge_eligible_at = effective_cutoff + purge_interval`。因为Dataset内每个归档行的
  Lifecycle值都不晚于cutoff，这保证最年轻归档行也达到保留期限，只可能比
  `max(lifecycle_value) + purge_interval`更晚Purge，绝不会提前删除；
- `purge_eligible_at`表示最早允许Purge，不表示到点同步删除，显式PURGE也不能提前绕过；
- policy变化不追溯修改已发布Dataset。

## 3. 准入

Lifecycle列必须：

- `NOT NULL DATE/DATETIME/TIMESTAMP`；
- 是稳定Column ID，不依赖列名；
- 不允许表达式、虚拟列或不可确定函数；
- 当前schema可由Archive canonical encoder支持。

Phase 1拒绝：

- 逻辑分区表和物理Partition child；
- FK；
- Fulltext、Vector、插件、隐藏索引表，以及已经存在二级/唯一索引的源基表；
- inline-only Stage secret；
- 未经部署认证的Archive Stage，以及启用对象Versioning的Bucket/Container；
- `ENUM`、`SET`和typed ARRAY等仅靠OID不能无损重建的编码SQL类型；
- append-only语义无法保证的外部表。

这些检查只发生在Binding DDL和相关表级DDL，不进入普通DML。Snapshot/PITR创建、Clone/
Data Branch和普通同集群Publication/Subscription允许与Lifecycle共存，不属于Binding准入
依赖。CDC/CCPR不属于Phase 1准入依赖：Lifecycle既不查询其Catalog，也不修改其接口；
Object退休不会产生逐行DELETE，因此其下游完整性不属于Lifecycle GA保证。

Phase 1权限合同按当前实现冻结为管理员控制面，不在稳定性清理阶段放宽普通用户入口：

- SET/UNSET/PAUSE/RESUME/SHOW/RESTORE/PURGE由account admin执行；
- Archive Binding还必须通过Stage引用、部署认证和credential handle校验；
- RESTORE还需要目标database可创建新表；
- system-owned Root不允许tenant SQL直接查询或修改，只通过受审计的SHOW视图暴露摘要。

## 4. 新增Catalog原则

通过正常bootstrap/upgrade新增Lifecycle表，不修改现有`mo_tables`、`mo_columns`、
`mo_stages`列结构。没有逐Object Catalog行。

Tenant表由普通事务读写，但只允许Lifecycle内部adapter写，tenant用户不能直接DML；
Cleanup Root必须由system account持有，使账户删除后仍可清理。
Cleanup Root物理上复用MO已有Cluster Table类型并保持自动`account_id=0`，仅用于滚动升级时
让不知道新表名的旧CN沿既有租户过滤安全返回空集；它不是按`owner_account_id`分片或级联
删除的tenant数据表。`owner_account_id`仍是唯一业务Owner，`DROP ACCOUNT`后Root继续由
system Reconciler读取和清理。

部署必须同时覆盖两条建表路径：已存在tenant由版本upgrade创建五张tenant表；当前版本
新建tenant由标准`createSqls`创建相同五张表。五张tenant表登记为普通predefined tenant
Catalog表，Cleanup Root单独登记为system-account table。Restore Attempt/Chunk中的
`account_id=0`只是在旧CN按未知Cluster Table执行`DROP ACCOUNT ... WHERE account_id=?`
时使用的兼容哨兵，不参与Owner、查询或索引语义。

## 5. Binding

逻辑表名：`mo_catalog.mo_lifecycle_bindings`。

```text
binding_id                 UUID/BINARY(16) PK
account_id                 UINT32
database_id                UINT64
logical_table_id           UINT64
physical_table_id          UINT64
binding_generation         UINT64
schema_digest              BINARY(32)
lifecycle_column_id        UINT64
action                     ENUM(DELETE, ARCHIVE)
expire_after_days          UINT32
late_arrival_grace_days    UINT32
evaluation_timezone        VARCHAR
stage_id                   UINT64 NULL
stage_identity_digest      BINARY(32) NULL
purge_after_days           UINT32 NULL
scan_snapshot_ts           BINARY
scan_last_object_name      BINARY
scan_wrapped               BOOL
last_full_scan_at          TIMESTAMP
state                      ENUM(ACTIVE, PAUSED, BLOCKED, DISABLING)
version                    UINT64
created_at/updated_at       TIMESTAMP
```

唯一键：`(account_id, physical_table_id)`。

以下变化递增`binding_generation`：

- Policy、Lifecycle列、Stage identity；
- physical table替换；
- 影响逻辑归档值的schema变化。

Binding不保存active attempt、Object Index、Candidate或高频运行统计。

`Binding.schema_digest`是源表final fence，覆盖源Column ID和所有影响读取/分类语义的schema
字段；它不用于要求Restore新表复用源Column ID。

Binding控制面转换：

```text
SET/CREATE -> ACTIVE
ACTIVE <-> PAUSED
ACTIVE/PAUSED -> BLOCKED（需显式修复后RESUME）
ACTIVE/PAUSED/BLOCKED -> DISABLING -> row deleted
```

PAUSE/UNSET立即阻止新child。UNSET在DDL Gate下递增generation或删除Binding，使旧finalizer
失败；已经PUBLISHED的Dataset不随UNSET改变，仍可Restore/Purge。普通worker lease只负责
收敛当前child，不写active-attempt字段。

## 6. Dataset

逻辑表名：`mo_catalog.mo_lifecycle_datasets`。

```text
dataset_id                 BINARY(16) PK
account_id                 UINT32
binding_id/generation      BINARY(16)/UINT64
logical_table_id           UINT64
source_physical_table_id   UINT64
source_snapshot_ts         BINARY
evaluation_time/cutoff     TIMESTAMP/TIMESTAMP
source_set_digest          BINARY(32)
schema_descriptor_digest   BINARY(32)
lifecycle_min/max          BINARY/BINARY
root_id/attempt_id         BINARY(16)
manifest_key               TEXT
manifest_sha256            BINARY(32)
content_hash               BINARY(32)
row_count/logical_bytes    UINT64
stage_id                   UINT64
stage_identity_blob        BLOB
purge_eligible_at          TIMESTAMP
state                      ENUM(PUBLISHED, DELETE_PENDING, DELETING, PURGED)
version                    UINT64
access_generation          UINT64
restore_lease_id           BINARY(16) NULL
restore_deadline           TIMESTAMP NULL
publish_txn_id             BINARY
created_at/updated_at       TIMESTAMP
```

唯一键：`(root_id, attempt_id)`。Dataset本身是Archive发布权威，不增加Archive Receipt。
`Dataset.schema_descriptor_digest`只验证Manifest中历史逻辑descriptor的canonical bytes；
Restore按descriptor的结构投影创建新表，不新增第二个持久restore schema摘要。
`Dataset.content_hash`等于Manifest中的`dataset_content_hash`。
`stage_id`用于Stage DROP/ALTER的索引化引用检查；`stage_identity_blob`继续冻结实际Provider
位置和加密身份，不能用解析blob的全表扫描代替索引。Dataset不保留`ERROR`状态：Archive
验证失败时Dataset尚未发布，Restore错误写Restore Attempt；已经PUBLISHED的Dataset保持
可Restore或Purge。

后台终态回收使用`(state, updated_at, dataset_id)`索引；Purge资格、按表查询和Stage引用
继续使用各自索引，禁止用定时全表扫描代替。

`version`从1开始，Dataset状态、Restore lease和Purge的每次条件更新都必须同时比较旧值并
递增；`access_generation`只表达Payload访问代际，不能代替通用行版本。

## 7. TTL Receipt

逻辑表名：`mo_catalog.mo_lifecycle_ttl_receipts`。

```text
receipt_id                 BINARY(16) PK
account_id/binding_id      UINT32/BINARY(16)
binding_generation         UINT64
physical_table_id          UINT64
source_snapshot_ts         BINARY
evaluation_time/cutoff     TIMESTAMP/TIMESTAMP
source_set_digest          BINARY(32)
expired_rows/retired_bytes UINT64
root_id/attempt_id         BINARY(16) NULL
publish_txn_id             BINARY
created_at                 TIMESTAMP
```

它与TTL退休同事务写入，用于SHOW、审计和commit-unknown只读对账。

## 8. Cleanup Root

逻辑表名：system account的`mo_catalog.mo_lifecycle_cleanup_roots`。物理Cluster Table类型
只复用旧CN已有的租户过滤；新CN仍把它登记为system-account管理表。产品保证是tenant
无法观察或修改Root行，不依赖“表名一定无法解析”这一更强假设。

```text
account_id                UINT32 = 0  # 物理兼容列，不是业务Owner
root_id                   BINARY(16) PK
attempt_id                BINARY(16)
mode                       ENUM(ARCHIVE_WHOLE, ARCHIVE_REWRITE, TTL_REWRITE)
owner_account_id           UINT32
logical/physical_table_id  UINT64
executor_epoch             UINT64
worker_lease_deadline      TIMESTAMP
archive_namespace_blob     BLOB NULL
credential_handle          TEXT NULL
archive_prefix             TEXT NULL
manifest_key/digest        TEXT/BINARY(32) NULL
tae_namespace_blob         BLOB NULL
segment_id                 BINARY NULL
booking_prefix             TEXT NULL
ordinal_upper_bound        UINT32 NULL
reserved_cleanup_bytes     UINT64
source_set_digest          BINARY(32)
final_txn_id               BINARY NULL
state                      ENUM(REGISTERED, UPLOADING, VERIFIED, FINALIZING,
                                PUBLISHED, COMMIT_UNKNOWN, DELETE_PENDING,
                                DELETING, CLEANED)
state_version              UINT64
cleanup_after              TIMESTAMP
temporary_cleanup_done     BOOL
quiescence_since           TIMESTAMP NULL
last_list_at/last_error     TIMESTAMP/TEXT
created_at/updated_at       TIMESTAMP
```

一次attempt一行，不建立逐文件明细。

只有会产生Provider Payload、TAE live staging或external booking的attempt创建Root。
Whole TTL和TTL小Mixed不为事务终态额外创建Root，避免把Root变成Terminal Journal。

唯一键：`(root_id)`；另建`UNIQUE(attempt_id)`。所有CAS包含`root_id + attempt_id +
state + state_version`，防止把其他attempt推进。

Root只增加与实际后台查询一致的轻量索引：
`(state, cleanup_after, root_id)`用于待清理工作，
`(state, temporary_cleanup_done, updated_at, root_id)`用于发布后临时文件清理，
`(state, updated_at, root_id)`用于终态元数据回收。它们只作用于新增Lifecycle表，不进入
普通表DML、查询或Merge路径。

## 9. Restore元数据

逻辑表名：

```text
mo_catalog.mo_lifecycle_restore_attempts
mo_catalog.mo_lifecycle_restore_chunks
```

两者属于发起Restore的tenant；源owner已DROP时不保证继续Restore。Restore Attempt：

```text
restore_id                 BINARY(16) PK
account_id                 UINT32 = 0  # 旧CN滚动升级兼容，不参与业务语义
dataset_id                 BINARY(16)
lease_id/deadline          BINARY(16)/TIMESTAMP
staging_database_id        UINT64
staging_table_id           UINT64
hidden_name                TEXT
target_database_id         UINT64
target_name                TEXT
state                      ENUM(IMPORTING, PUBLISHING, DONE, FAILED)
next_chunk_ordinal         UINT64
restored_rows              UINT64
verified_content_hash      BINARY(32) NULL
last_error/updated_at       TEXT/TIMESTAMP
```

`staging_database_id/staging_table_id`是隐藏表的精确身份；隐藏名由
`__mo_lifecycle_restore_<restore-id>`确定。`staging_table_id`只能参与Rename/DROP前的
身份校验，禁止作为无名称校验的DROP目标。

只有该前缀加32位十六进制restore ID的精确形状才是大小写不敏感的frontend保留命名空间：
用户不能创建、改名或访问其中的表，同前缀但非该形状的历史用户表不受影响。
Lifecycle内部SQLExecutor仍按普通表处理。隔离只依赖O(32)名称判断，不增加Catalog列、权限
状态或特殊relkind。Manifest中的逻辑列不包含目标表由普通DDL创建的
`__mo_fake_pk_col`；它由Chunk普通事务复用现有incrservice生成。

Dataset lease CAS、隐藏表CREATE和本Attempt INSERT必须在同一个普通MO事务中提交。隐藏表
不能先提交后再补Attempt；任一失败整体回滚，响应未知时通过Dataset lease、Attempt和精确
隐藏身份对账。

Attempt只使用四个状态：

```text
IMPORTING -> DONE
IMPORTING -> FAILED
```

`PUBLISHING`不是一个单独提交、可长期停留的业务阶段。它只允许在最终普通发布事务内部作为
CAS中间值出现；同一事务继续完成`SetOffset`、Rename、`DONE`和Dataset lease释放。事务
回滚后外部仍看到`IMPORTING`，事务提交后外部看到`DONE`。`DONE/FAILED`是终态。发布和
清理都必须条件更新同一Attempt行，不能只读取状态后执行DDL。

Chunk Receipt：

```text
account_id                 UINT32 = 0  # 旧CN滚动升级兼容，不参与业务语义
restore_id                 BINARY(16)
chunk_ordinal              UINT64
file_ordinal/row_group_ordinal UINT32/UINT32
chunk_digest               BINARY(32)
row_count                  UINT64
logical_bytes              UINT64
canonical_content_hash     BINARY(32)
created_at                 TIMESTAMP
PRIMARY KEY (restore_id, chunk_ordinal)
```

单个Restore首版串行推进chunk。对应数据INSERT、Chunk Receipt、`next_chunk_ordinal`和
`restored_rows`在同一普通事务提交；`chunk_digest`不是主键的一部分。相同ordinal的不同
digest是corruption，相同digest按Receipt幂等成功。`verified_content_hash`不在Chunk事务中
更新，只在全部Receipt通过最终校验并发布新表的普通事务中一次性写入。
AUTO_INCREMENT最大正值保存在Manifest，并由Archive full readback从最终MO逻辑值复核；
Restore每个Chunk又在最终MO vectors上验证Manifest冻结的内容Hash，因此最终发布直接使用
Manifest中的全局最大值，不在Receipt中再维护第二份逐Chunk最大值。

## 10. Catalog索引与访问路径

这些是控制面索引，不是逐Object Lifecycle Index：

| 表 | 必需索引 | 用途 |
|---|---|---|
| Binding | `(account_id, physical_table_id)` unique；`(state, binding_id)` | DDL准入和Scheduler游标分页 |
| Dataset | `(root_id, attempt_id)` unique；`(account_id, logical_table_id, state)`；`(state, purge_eligible_at)`；`(state, updated_at, dataset_id)`；`(stage_id, state)` | SHOW、对账、Stage引用、Purge和终态回收 |
| TTL Receipt | `(binding_id, source_set_digest)`；`(root_id, attempt_id)`；`(created_at)` | unknown/Root对账和审计GC |
| Root | `(state, cleanup_after, root_id)`；`(state, temporary_cleanup_done, updated_at, root_id)`；`(state, updated_at, root_id)`；`(owner_account_id, logical_table_id)` | system Reconcile/Sweeper和终态回收 |
| Restore Attempt | `(dataset_id, state)`；`(state, deadline)`；`(state, updated_at, restore_id)` | lease恢复、超时清理和终态回收 |

所有后台扫描都使用索引游标和rows/bytes page cap，不允许周期性无条件全表物化。

## 11. 终态元数据回收

所有增长必须有终点，默认值由发布配置冻结：

| 记录 | 可删除条件 | 默认审计窗口 |
|---|---|---|
| TTL Receipt | Binding不存在或已超过审计窗口，且没有`root_id + attempt_id`精确匹配的unknown Root引用 | 30天 |
| PURGED Dataset | Root已CLEANED且无Restore Attempt | 90天 |
| CLEANED Root | quiescence结束、无Dataset/Restore引用 | 30天 |
| DONE/FAILED Restore Attempt | 无lease、隐藏表已处理 | 30天 |
| Restore Chunk Receipt | Restore终态且审计窗口结束 | 30天 |

保留窗口可配置但有全局rows/bytes hard cap。到达cap时暂停Lifecycle，不影响普通MO。
回收按5分钟一个有界account page持续推进，而不是每天只回收一页；单个unknown Root只
保留自己的Receipt，不能阻塞同账户其他Binding的终态回收。

## 12. 表级DDL fence与周边功能边界

Lifecycle只保留自身需要的薄管理路径fence，不建设Feature Guard表：

- 表DDL和`SET LIFECYCLE`复用现有`mo_tables`逻辑行锁；绑定表的identity/schema/不支持
  表内依赖变更由该锁、Binding generation和Finalizer exact检查共同关闭；
- `SET LIFECYCLE`在持有表锁后访问system account中已存在的
  `mo_feature_registry(feature_code='LIFECYCLE')`行，只用于Lifecycle自己的release、配置
  和容量控制顺序。该行不是跨产品功能的数据正确性barrier；
- Snapshot/PITR创建和保留直接复用现有Object MVCC与GC保护，不读取Binding，也不访问
  Lifecycle feature row。Snapshot/PITR Database/Table Restore若源或目标scope包含
  `ARCHIVE` Binding、非`PURGED` Dataset或非`CLEANED`的`ARCHIVE_*` Root，必须在任何
  破坏性Restore事务提交前fail closed；直接Account Restore存在任意Lifecycle Binding时
  拒绝，Cluster逻辑Restore不扩展TTL兼容；Restore与`SET LIFECYCLE`/Archive finalizer并发
  不在Phase 1认证范围，执行前必须关闭并drain Lifecycle数据任务；
- Clone/Data Branch只复制目标时间点的活动数据，目标表使用新身份且不继承Binding、
  Dataset或Archive Payload；普通同集群Publication/Subscription直接读取发布者活动表。
  这些创建和运行路径不访问Lifecycle feature row；
- Lifecycle不接入CDC/CCPR控制面；SET不查询其Task/Watermark，其创建和运行路径也不增加
  Lifecycle hook。Lifecycle不保证下游收到Object退休对应的逐行DELETE；
- Lifecycle不修改普通物理Backup的准入和创建路径。物理Backup可能包含Binding、Dataset、
  Root、Stage和release gate等Catalog状态，但不会复制Stage中的外部Archive Payload。
  含这些状态的物理Backup Restore在Phase 1 unsupported：恢复环境必须在任何Lifecycle
  Coordinator/Sweeper tick前隔离Lifecycle任务和原Archive namespace删除凭据；仅设置
  `enabled=false`不足，因为历史Root cleanup在正常集群中不受release gate阻断；
- Finalizer仍使用Binding generation/physical table/schema与exact source Object检查决定
  是否退休，不把feature row当作数据正确性Owner。

`SET LIFECYCLE`固定采用`mo_tables -> Lifecycle feature row`锁顺序，只串行Lifecycle自身
控制面。普通表DDL只取得已有`mo_tables`锁并执行索引化Binding lookup；Snapshot/PITR/
Clone/Branch/Publication、普通查询、DML和Merge均不访问feature row，未绑定表不创建任何
Lifecycle Guard元数据。

Phase 1产品行为采用fail-closed：

- DROP TABLE/DATABASE/ACCOUNT允许，按05放弃Restore并异步清理；
- SET LIFECYCLE拒绝逻辑分区表和物理Partition child；已绑定表拒绝转换为分区表；
- UNSET/PAUSE/RESUME和Policy更新允许，但必须推进Binding generation；
- 绑定期间TRUNCATE、ALTER COPY、Lifecycle列变更、其他schema变更和新增不支持表内依赖
  拒绝；
- 用户需要这些DDL时先UNSET，待在途Root收敛后执行，再按新physical/schema重新SET；
- 未绑定表的不兼容DDL只复用现有`mo_tables`锁并执行按`(account_id,
  physical_table_id)`的Binding存在性查询，不跨feature-row barrier，也不创建Guard或其他
  Catalog行。

## 13. 用户可见错误

错误按“可重试、阻断、数据/外部错误”分类，Scheduler不能对所有错误无限重试：

| 类别 | 示例 | 行为 |
|---|---|---|
| Retryable | source被Merge替换、Provider 429、Scheduler/CN资源上限 | 保留源数据，退避后重新Discovery |
| Blocked | mixed超认证上限、schema/type不支持、依赖能力冲突 | Binding进入BLOCKED，等待用户/配置处理 |
| Attempt failed | readback/hash失败、SyncProtection丢失 | Root异步清理，fresh attempt |
| Unknown | final commit结果未知 | Root COMMIT_UNKNOWN，暂停相同source |
| Terminal data error | Manifest/schema/file hash不一致 | 禁止Restore/Purge误判，告警人工处理 |

错误必须带`binding_id/root_id/attempt_id`供诊断，但Metrics label不得使用这些高基数字段。
