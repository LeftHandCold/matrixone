# DROP、Purge 与 Restore 详细设计

> 本文唯一负责 owner DROP、Archive Purge、Restore lease、隐藏 staging table、
> chunk幂等、原子发布和 Snapshot/Backup/DR限制。

## 1. 产品契约

首个 GA 的 Archive 从属于源表和账户：

- 源表存在且Dataset未Purge时允许Restore；
- DROP TABLE/DATABASE/ACCOUNT 表示放弃对应Archive的Restore能力；
- DROP不等待Provider；
- Drop后Root/Sweeper异步Delete；
- 不提供UNDROP Archive；
- 不提供Legal Hold/WORM；
- 不提供跨账户Transfer；
- 不保证Archive随Backup/DR复制。

这是有意比Snowflake更简单的语义，产品文案必须明确。

## 2. DROP TABLE

现有 `pkg/sql/compile/ddl.go` 的DROP TABLE主事务已经有系统表钩子。新增一个轻量hook：

```text
acquire normal DDL/table lock
  -> CAS Feature Guard owner_state ACTIVE -> DROPPING
  -> fence Binding/active child generation
  -> insert tenant table owner tombstone
  -> existing dbSource.Delete
  -> commit normal DROP transaction
```

owner tombstone字段：

```text
kind=TABLE
logical db/table ID
account incarnation
owner/schema generation
drop txn ID/time
```

DROP路径不做：

- 查询Dataset列表；
- 查询Root；
- Provider LIST/DELETE；
- 等待Restore lease；
- 等待Lifecycle Job停止。

DROP提交后：

- 新Lifecycle claim因Guard/Binding/Relation不存在而失败；
- Reader/Uploading worker在下一epoch检查停止；
- FINALIZING/COMMIT_UNKNOWN先按原txn查询Terminal Journal；matching aborted且无
  Receipt/Dataset后由Sweeper异步清理；
- Restore final publish因owner CAS失败；
- Sweeper按Root异步清理。

DROP rollback时owner tombstone和Guard变化一起rollback，Archive仍可Restore。

## 3. DROP DATABASE

不在DROP数据库时为每张表逐个枚举外部对象。主事务：

```text
insert tenant owner tombstone(kind=DATABASE, logical_database_id)
  -> existing database Catalog drop
  -> commit
```

如果当前实现会逐表调用DROP hook，可以保留表tombstone，但数据库tombstone是有界兜底。

Sweeper查询Root时：

```text
matching table tombstone
OR matching database tombstone
  -> owner dropped
```

## 4. DROP ACCOUNT

`pkg/frontend/authenticate.go` 的DROP ACCOUNT已经在system account事务中清理目标tenant的cluster-table数据。新增写必须在同一事务：

```text
lock current mo_account RowID
  -> CAS matching lifecycle account identity ACTIVE -> DROPPED
  -> insert system retained account owner tombstone
  -> delete matching incarnation/catalog-RowID lifecycle account current row
  -> existing delete all target tenant cluster rows
  -> commit
```

顺序要求：

- retained tombstone在同一txn内先写；
- current row只按匹配的incarnation/version删除，不能删除新建租户身份；
- tenant Dataset/Binding/owner tombstone随后可以被现有逻辑删除；
- 事务失败时两者都不提交；
- 不另开“先写tombstone再DROP”的事务，避免DROP失败却误判owner已删除。

DROP ACCOUNT完成后，Cleanup Root/Profile cleanup credential仍在system retained registry。

Account Cleanup Reconciler 还必须按 tombstone 的旧 `account_incarnation` 分页删除所有
Lifecycle tenant cluster rows，并持续到最大 tenant transaction deadline +
quiescence window 内没有新行。所有 Lifecycle cluster table 都显式保存并过滤
`account_incarnation`；因此极端迟到的旧 tenant transaction 即使在 DROP 清理后提交，
也不会被复用同一 `account_id` 的新租户看见。发现迟到行时重启 quiescence timer。

## 5. Owner dropped与final transaction竞态

### 5.1 DROP先提交

- Binding/Guard/Dataset行不存在或owner state dropped；
- Lifecycle final CAS affected rows = 0；
- tenant final txn abort；
- Root只有在matching ABORTED Journal且无Receipt/Dataset后才DELETE_PENDING。

### 5.2 Lifecycle先提交

- Dataset/Receipt/retirement原子成功；
- 随后的DROP删除tenant可见Dataset；
- account/table owner tombstone触发Root cleanup；
- 不再承诺Restore。

### 5.3 Lifecycle commit unknown

即使owner随后DROP：

- 仍按原txn ID查询Lifecycle Terminal Journal；
- committed：不需要恢复Dataset用户可见性，Root直接按owner dropped进入DELETE_PENDING；
- matching aborted且无Receipt/Dataset：清理staging；
- unknown：Root保持FINALIZING，不因为DROP猜测结果。

这样不会删除一个仍可能是commit participant正在引用的Payload。

## 6. Purge

### 6.1 正常retention Purge

Purge Scanner直接分页扫描tenant Dataset，不依赖Binding仍然ACTIVE：

```text
state=PUBLISHED
AND purge_eligible_at <= now
ORDER BY purge_eligible_at, dataset_id
LIMIT 1000
```

Dataset页只生成候选；进入`DELETE_PENDING`前还必须按dataset ID读取matching system Root：

```text
Root.state = PUBLISHED
AND Root.observed_commit_ts IS NOT NULL
AND Root.observed_commit_ts + minimum_publish_grace <= now
```

任一条件不满足都跳过并告警，不能因为Dataset索引命中就开始Purge。

原因是 `UNSET LIFECYCLE` 只停止新归档，已经发布的 Dataset 仍要在 retention 到期后
正常 Purge。以 Binding 为入口会让这些 Dataset 永久泄漏。

Coordinator 从 system account identity/Profile/Root registry 分页取得仍有 Lifecycle
资源的 account incarnation，再切换到对应 tenant context 使用
`(account_incarnation,state,purge_eligible_at,dataset_id)` 索引查询。它不对集群所有
普通账户或 `mo_tables` 做全量扫描；owner 已 DROP 的账户直接走 system Root cleanup。

tenant transaction：

```text
CAS Dataset PUBLISHED -> DELETE_PENDING
AND access_generation/version expected
```

然后system Purge Job按dataset ID找到Root，进入Root删除协议。

### 6.2 用户 `PURGE ARCHIVE BEFORE`

只选择：

```text
max_lifecycle_value < requested_before
AND purge_eligible_at <= now
AND state=PUBLISHED
AND matching Root observed_commit_ts + minimum_publish_grace <= now
```

语句返回：

```text
accepted dataset count/bytes
purge job IDs
```

不等待Provider。没有`FORCE`绕过minimum retention。

### 6.3 Dataset/Root两平面收敛

可能窗口：

```text
tenant Dataset DELETE_PENDING committed
  -> crash before Root update
```

周期性Purge Reconciler按tenant Dataset state发现并推进Root。

反向：

```text
Root CLEANED
  -> crash before tenant Dataset PURGED
```

Reconciler确认Root CLEANED后，tenant txn CAS Dataset `DELETING/DELETE_PENDING -> PURGED`。

Dataset `PURGED`是Catalog状态，不替代Provider不存在确认。

### 6.4 Owner drop覆盖retention

owner dropped：

```text
ignores purge_eligible_at
but still requires:
  final txn not in-doubt
  Restore leases fenced/expired
  Root irreversible delete CAS
```

这是“放弃Restore”契约，不是maximum retention。

## 7. Restore 总体流程

```text
resolve Dataset set
  -> acquire all Dataset/Root leases
  -> full Manifest/Payload verification
  -> create protected staging table
  -> load bounded Parquet row-group chunks
  -> chunk Receipt + staging rows atomic per txn
  -> verify all chunks/rows/root
  -> renew leases
  -> final tenant txn CAS owner/Dataset versions
  -> atomic rename staging -> requested target
  -> release leases
  -> cleanup Restore control/chunk Receipts
```

Restore允许TB级，不用一个超长事务装下整个Dataset。

## 8. Dataset解析

### 8.1 时间范围

按logical source table和schema generation查询：

```text
Dataset state=PUBLISHED
AND dataset.lifecycle range intersects requested range
```

时间范围只选择Dataset，不过滤Dataset内行。CLI/SHOW必须预览：

- Dataset count；
- 实际覆盖min/max；
- rows/bytes；
- schema generations；
- estimated GET/restore bytes。

### 8.2 精确Dataset

用户给出的ID：

- 必须全部属于当前account incarnation；
- logical source table一致；
- state=PUBLISHED；
- schema digest一致；
- 不能重复；
- 按dataset ID排序形成set digest。

### 8.3 Schema

首个GA只允许同一schema digest的Dataset合并到一个目标表。跨digest要求分别Restore。

## 9. Lease获取

多个Dataset按dataset ID升序，避免两个Restore反向拿lease。

每个Dataset：

1. tenant consistent read Dataset `PUBLISHED/version/access_generation`；
2. system txn读取Root `PUBLISHED/access_generation`；
3. CAS/insert Access Lease：

```text
dataset_id
same access_generation
restore_id/executor_epoch
expires_at = now + 5 minutes
```

4. 全部lease取得后，tenant再读一次全部Dataset状态/version；
5. 任一变化，释放已取得lease并失败。

Lease每60秒续租，expiry 5分钟。每个Payload/row-group读取前后检查。

Purge进入DELETE_PENDING需要提高同一Root `access_generation`，因此与新lease CAS互斥。

## 10. Restore Attempt

用户语句先创建system Restore Attempt：

```text
restore_id
dataset set digest
target db/name
expected rows/root
state=PLANNED
executor epoch
```

`target db/name` 在进入 Attempt 前必须经过与普通 `CREATE TABLE/RENAME TABLE` 完全
相同的大小写、引用标识符和 Catalog normalization；Attempt 同时保留用户显示名和
规范名。最终 DDL 锁、目标不存在检查和 response-lost 对账都使用规范名，不能由
Lifecycle 自己实现第二套名称比较。

TaskService投递generic Restore task。恢复可异步；用户可：

```sql
SHOW RESTORE ARCHIVE JOB <restore_id>;
```

取消：

- `PLANNED/LEASED/WRITING`可请求cancel；
- `PUBLISHING`后只等待最终txn结果；
- cancel不删除Dataset；
- staging由Restore Attempt清理。

## 11. 隐藏 staging table

### 11.1 名称和访问

新增常量：

```text
catalog.LifecycleRestoreTablePrefix =
  "__mo_lifecycle_restore_"
```

名称：

```text
__mo_lifecycle_restore_<restore-id-hex>
```

Staging位于目标database，是普通TAE表，schema来自Manifest。它不使用index hidden table prefix，不触发index plugin逻辑。

Frontend/Planner必须：

- SHOW TABLES/information_schema过滤该prefix；
- 普通用户显式SELECT/DDL该prefix返回access denied；
- 只有带internal Restore capability context的代码可访问；
- target用户名称禁止使用该reserved prefix。

表保持普通relkind，避免新增TAE table format。原子rename后新名称不再受prefix保护。

### 11.2 创建幂等

Restore Attempt先持久化deterministic staging name，再执行CREATE。

crash窗口：

```text
CREATE committed
  -> system Attempt尚未记录table ID
```

Reconciler按deterministic name查表：

- schema digest/restore ID ownership匹配：adopt并记录ID；
- 名称存在但不匹配：terminal conflict，不覆盖；
- 不存在：重试CREATE。

Restore ID ownership不写用户列；保存在Restore Attempt和受保护的Catalog table comment/property。最终publish前移除内部comment。

## 12. 写入实现

### 12.1 不使用前端全内存Parquet export

读取复用Archive verification的Parquet decoder和canonical type mapping。

新增：

```text
pkg/lifecycle/restore/
  resolver.go
  leases.go
  staging.go
  decoder.go
  loader.go
  chunk.go
  publish.go
  cleanup.go
```

### 12.2 复用正常INSERT预处理

Archive Payload只有业务列。Staging table可能需要：

- fake PK；
- composite PK hidden encoding；
- default/internal columns；
- auto increment定义（值已在业务列时不得重新分配）。

Restore不能直接裸调用`Relation.Write`跳过这些规则。抽取/复用正常INSERT pipeline的batch入口：

```go
type InternalBatchInserter interface {
    Prepare(
        ctx context.Context,
        tableDef *plan.TableDef,
        inputBusinessColumns []uint64,
    ) error

    InsertBatch(
        ctx context.Context,
        txn client.TxnOperator,
        rel engine.Relation,
        business *batch.Batch,
    ) (rows uint64, err error)
}
```

它复用：

- preinsert列映射；
- fake/composite key生成；
- NULL/type/width检查；
- PK duplicate检查。

`AUTO_INCREMENT` 业务列写入 Manifest/Payload 中的原值，Restore 不重新分配；但正常
INSERT/auto-increment service 必须把目标表 sequence 水位推进到至少
`max(restored explicit value) + 1`，并在发布前验证。若当前显式值写入路径不能提供
该原子、可恢复语义，Binding 准入必须拒绝含 AUTO_INCREMENT 的表，不能先发布再让
后续 INSERT 发生重复值。

首个 GA 准入拒绝任何 generated column。否则 Restore 必须决定“写归档值”还是
“按新引擎重新计算”，还要处理表达式版本差异；该语义不允许在首版隐式选择。

由于Binding拒绝隐藏二级/唯一索引、FK和插件，Restore不需要维护相关隐藏表。

### 12.3 Chunk

一个chunk是一个Parquet row group，且：

```text
rows             <= 1,000,000
decoded bytes    <= 256 MiB
txn wall         <= 5 minutes
workspace        <= 512 MiB
```

超限row group在Archive Writer阶段就不应产生；旧不兼容Manifest拒绝Restore。

## 13. Chunk幂等和commit unknown

每个chunk key：

```text
(restore_id, dataset_id, payload_ordinal, row_group_ordinal)
```

处理：

1. 查询tenant `mo_lifecycle_restore_chunks`；
2. 已有相同digest Receipt：不再插入，仍可重读用于overall root；
3. 没有Receipt：
   - 读取/校验chunk；
   - 分配tenant txn ID；
   - system Restore Attempt写前记录current chunk/txn/digest；
   - tenant txn写staging rows；
   - 同txn插Restore chunk Receipt；
   - Restore adapter签发opaque token并追加matching
     `LifecycleCommitEntry(RESTORE_CHUNK)`；
   - TxnCommitRequest field 5携带同一TerminalIdentity；
   - commit。

commit unknown：

- 不重插；
- system Attempt保持current txn；
- 使用Lifecycle Terminal Journal + consistent chunk Receipt对账；
- committed后advance；
- matching aborted后ACK旧终态，再用新txn重试同一chunk；
- unknown保持fail closed。

Restore chunk/publish transaction和retirement final一样，在`TxnCommitRequest`携带optional
`LifecycleTerminalIdentity`，并在ordinary writes后追加matching tagged Lifecycle control。
Journal查询必须早于`maybeAddTxn`和staging写入；同一txn ID重复请求直接返回原终态，
identity mismatch直接协议错误。header/tag任一缺失都整体abort，避免旧TN忽略unknown
proto field后静默提交Restore写。普通Restore读取和普通用户事务不访问Journal。

相同chunk key已有不同digest：

- cluster Restore kill switch；
- staging不发布；
- 保留证据。

## 14. Restore内容验证

Restore按Manifest顺序读取所有Dataset/Payload/row group。即使chunk已由旧worker提交，也重新OpenExact并验证，以得到一次完整端到端证明。

必须满足：

```text
all Manifest/Payload SHA valid
decoded total rows == sum Dataset rows
decoded overall content root == expected combined root
all chunk Receipts present with matching digest/rows
sum chunk receipt rows == staging expected rows
SELECT COUNT(*) from protected staging == expected rows
no active/unknown chunk txn
```

多个Dataset的combined root：

```text
SHA256(
  "MO-LIFECYCLE-RESTORE-SET-V1"
  || ordered(dataset_id, manifest_root, dataset_row_count)
)
```

每个Dataset自身root必须独立匹配。不能只用combined catalog值跳过Payload验证。

Staging不按物理row order重算Dataset root；root在exact Parquet decode时计算，普通MO写入正确性由正常INSERT txn/row count/PK检查保证。

## 15. 原子发布

发布前：

1. 所有lease续到至少`now + 5min`；
2. Restore Attempt `VERIFYING -> PUBLISHING` CAS；
3. 创建tenant final DDL txn；
4. acquire source owner Guard/table lock和target database/name DDL lock，按ID升序；
5. CAS source Guard/Binding仍ACTIVE且owner未DROP；
6. 对每个Dataset执行version CAS并记录`last_restore_at/version+1`，与Purge形成write conflict；
7. recheck staging table ID/schema/row count；
8. target name仍不存在；
9. 使用现有RenameTable Alter把deterministic staging name改为target name；
10. 清除internal restore comment/property；
11. Restore adapter签发opaque publish token，追加
    `LifecycleCommitEntry(RESTORE_PUBLISH)`和matching TerminalIdentity；
12. commit。

结果：

- commit前用户看不到staging；
- commit后完整目标表一次可见；
- target是普通非Lifecycle绑定表；
- Archive Dataset不改变。

response lost：

- Restore Attempt保持PUBLISHING + final txn ID；
- Lifecycle Terminal Journal对账；
- committed后按target table ID/name确认；
- matching aborted后ACK旧终态，staging仍在，可用新txn重试publish；
- unknown不DROP staging。

## 16. Owner/Purge与Restore竞态

### 16.1 Restore先拿lease

- Purge不能把Root推进DELETING；
- tenant Dataset可尝试DELETE_PENDING，但final publish的Dataset version CAS会冲突；
- 如果Purge先更新tenant Dataset，Restore final abort；
- 如果Restore final先commit，目标表已独立，随后Purge可删除Archive。

### 16.2 Purge先DELETE_PENDING

- access generation变化；
- 新lease拒绝；
- Restore失败，不创建/发布目标。

### 16.3 DROP owner

- owner tombstone/Guard使final publish CAS失败；
- 已加载staging由Restore cleanup删除；
- lease释放/fence后Root清理；
- DROP后不允许通过已持有旧lease发布新表。

## 17. Restore失败清理

状态：

```text
failure/cancel
  -> ABORTING
  -> release/fence leases
  -> wait chunk/final txn not unknown
  -> normal DROP staging table
  -> delete tenant Restore chunk Receipts in pages
  -> ABORTED
```

staging DROP失败：

```text
CLEANUP_FAILED
  -> retain table ID/name
  -> bounded retry
  -> alert
```

不能把受保护staging暴露给用户让其自行处理。

## 18. Snapshot/PITR/Backup/Clone/Branch/DR矩阵

首个GA不实现Archive-aware restore，因此双向fail closed：

| 操作 | Lifecycle-bound source |
|---|---|
| CREATE user Snapshot | 拒绝 |
| CREATE PITR policy覆盖该表/db/account | 拒绝 |
| Backup包含该表 | 拒绝 |
| Clone/Branch该表/db | 拒绝 |
| Restore普通Snapshot/PITR到该表 | 拒绝 |
| DR/failover复制Archive | 不支持 |
| DR目标执行RESTORE ARCHIVE | `ARCHIVE_UNAVAILABLE/UNSUPPORTED` |
| Lifecycle bind时已有上述reference | 拒绝bind |

原因：

```text
普通Snapshot/PITR只知道活动TAE数据
Lifecycle已经把历史行退休到Archive
若继续普通restore，会返回“成功但历史缺失”的表
```

不支持必须在操作开始前报错，不能返回空Dataset或不完整表。

Restore产生的新普通表默认没有Lifecycle Binding，因此可正常进入后续Backup/Snapshot流程。

## 19. Deep Archive

首个GA Profile必须支持直接GET。Provider需要异步thaw/restore job的Deep Archive不支持：

- 不创建provider restore request；
- 不维护days-long thaw state；
- 不给用户假装“正在Restore”；
- DDL创建Profile时capability probe拒绝。

未来Deep Archive是新的Profile capability和Restore状态机，不改变当前Dataset格式，但需独立设计。

## 20. 限额

| 项 | 初始hard limit |
|---|---:|
| concurrent Restore cluster/account | 2 / 1 |
| Dataset per Restore | 10,000 |
| Payload per Restore | 100,000 |
| total Restore bytes | 10 TiB |
| staging tables per account | 4 |
| chunk rows | 1,000,000 |
| chunk decoded bytes | 256 MiB |
| chunk txn | 5 min |
| lease expiry/renew | 5 min / 60s |
| final publish txn | 60s |
| staging max age pre-alert | 24h |

超过不部分发布；用户拆分时间范围。

## 21. 测试要求

DROP：

- table/database/account commit/rollback；
- account tombstone与tenant rows同txn；
- no Provider call onDROP latency path；
- DROP与Reader/PUT/final/unknown/Restore各状态竞态；
- tenant删除后Root/Profile仍可清理。

Purge：

- minimum retention；
- owner drop override；
- Dataset/Root两平面crash窗口；
- lease CAS；
- Delete failure不标PURGED。

Restore：

- hidden prefix不可见/不可访问；
- create staging response lost/adopt；
- single/composite/fake PK；
- NULL/Decimal/timezone/JSON/BLOB；
- chunk commit response lost不重复；
- crash/restart每个row group；
- 1TiB/10TiB多chunk；
- fullroot/count mismatch；
- final rename atomic；
- target name冲突；
- owner drop/Purge race；
- cancel/cleanup failed；
- source/target不同db；
- restored table不自动绑定Lifecycle。

Support matrix：

- 每个Snapshot/PITR/Backup/Clone/Branch入口正反向拒绝；
- DR返回明确unsupported；
- 未绑定普通表功能不受影响。
