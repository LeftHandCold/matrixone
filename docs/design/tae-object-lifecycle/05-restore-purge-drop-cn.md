# 05 Restore、Purge与DROP详细设计

## 1. Restore产品合同

Restore始终创建独立新表：

```sql
RESTORE ARCHIVE DATASET '<dataset-id>' TO TABLE db.restored_t;
```

一个历史Dataset可以多次恢复为不同新表。Restore不写回源表，不依赖源表当前schema。

Phase 1恢复：

- Manifest schema descriptor定义的列结构和数据；
- AUTO_INCREMENT只恢复列属性和安全起始值，不复用历史运行状态。

不自动恢复：

- PK、UNIQUE/CHECK/FK、二级索引、CDC、Publication；
- Fulltext、Vector、插件；
- 用户/角色授权、masking/row policy；
- 源表当前默认值和当前schema变更。

## 2. Manifest准入

开始Restore前验证：

- Dataset为PUBLISHED；
- Manifest key/digest匹配；
- `manifest_format_version`、schema format、hash formula和canonical encoder版本受支持；
- Stage identity和credential handle可解析；
- 所有文件key位于Dataset冻结namespace；
- 文件数/bytes在Restore预算内。

未知类型或版本fail closed，不能按Parquet推测MO类型。

## 3. 初始化事务与Lease

Dataset一次最多一个Restore lease。第一个持久副作用必须由一个普通MO事务原子完成：

```text
CAS Dataset:
  state == PUBLISHED
  no unexpired restore lease
  version/access_generation unchanged
-> set restore_lease_id, restore_deadline
-> version = version + 1
CREATE hidden staging table from Manifest descriptor
INSERT Restore Attempt(
  state = IMPORTING,
  exact staging_database_id/hidden_name/staging_table_id,
  target identity, lease/deadline)
-> commit
```

目标名已存在、Dataset CAS、CREATE或Attempt INSERT任一失败，整个事务回滚，不能留下无
Attempt Owner的隐藏表。提交响应未知时按Dataset lease、Attempt和精确隐藏表身份做普通
Catalog对账；禁止重新创建第二张隐藏表。

Attempt固定deadline在Restore入口转换成同一个`context deadline`，覆盖Manifest GET、每个
Payload GET、Chunk INSERT和最终Publish；每个Chunk普通事务还必须CAS同一`IMPORTING`
Attempt的lease ID、deadline和`next_chunk_ordinal`。最终发布事务再CAS Dataset仍为
`PUBLISHED`且lease匹配。首版不续租，不在每次Provider GET前增加一次Dataset Catalog查询；
deadline到期后当前I/O/事务应由现有context取消并停止新GET/INSERT。

Restore lease获取与Purge CAS同一条Dataset行；两者只能一个成功。Purge不会先改变state或
`access_generation`再允许旧Restore继续读取。

每个CN在读取Dataset/Manifest、创建Archive FileService、获取lease或创建隐藏表之前，先
通过一个进程内fail-fast semaphore；首发容量为1。占满时只返回`RESOURCE_BUSY`且不产生
任何Restore副作用，并暴露active Restore gauge。这个限制只隔离Restore解码、canonical
cells和MO Batch的CN heap峰值，不建设TaskService Slot、TN permit或持久状态机。

读取Manifest后、初始化隐藏表前必须校验Dataset冻结的`root_id/attempt_id`、
`manifest_sha256`、`schema_descriptor_digest`、`FULL_READBACK_VERIFIED`以及Root-scoped
Manifest/Payload namespace完全一致。Stage/FileService仍由Dataset冻结的Stage identity
构造；不增加第二套Archive Profile。

## 4. Hidden staging table

名称：

```text
__mo_lifecycle_restore_<restore-id>
```

创建在目标database，普通用户不可直接访问。使用Manifest schema descriptor创建；必须与
Dataset lease和Restore Attempt INSERT处于第3节同一事务，不能先CREATE后补Attempt。

只有精确匹配`__mo_lifecycle_restore_<32位十六进制restore-id>`的名称才属于大小写不敏感
的保留staging命名空间。首个GA只增加一个O(32)名称检查：

- frontend SQL不能CREATE/RENAME到该精确形状，也不能SELECT、INSERT、UPDATE、DELETE、
  TRUNCATE、ALTER、DROP、DUMP TABLE或LOAD TABLE该形状的表；
- SHOW和`information_schema.TABLES`不展示该精确形状；
- Lifecycle内部SQLExecutor保持`IsFrontend=false`，继续使用普通CREATE/INSERT/RENAME/DROP；
- 检查不读取Lifecycle Catalog，不增加权限表、relkind、Guard或新的状态机。

普通用户历史上使用同一文字前缀但不符合上述精确形状的表不受影响。这项隔离只保护Restore
自己新增的staging副作用；不改变普通用户表的事务、DDL或存储实现。

descriptor中的`source_column_id`只用于lineage；普通DDL为目标列分配新Column ID。
Restore校验ordinal/name/type/nullability/AUTO_INCREMENT等Phase 1结构，
不要求目标Column ID等于源Column ID。

## 5. 分块写入

```text
for each manifest file ordinal
  -> read and verify SHA-256
  -> decode row groups
  -> validate row_count/logical_bytes against certified Restore caps
  -> convert to MO vectors
  -> 对最终MO vectors使用canonical encoder
  -> verify row_count/logical_bytes/canonical chunk hash/digest
  -> one normal transaction:
       INSERT data into staging
       INSERT chunk Receipt
       CAS next_chunk_ordinal
       UPDATE restored_rows
```

Manifest只描述历史逻辑列，Phase 1也不恢复源PK/索引；普通`CREATE TABLE`因此为staging表
创建MO现有的隐藏`__mo_fake_pk_col`。Chunk在canonical hash校验完成后，必须按目标
`TableDef`顺序组装最终写Batch，并在同一个普通`TxnOperator`中调用现有
`incrservice.InsertValues`生成该fake PK，再调用现有`Relation.Write`。归档中的用户列值
（包括显式AUTO_INCREMENT值）保持不变；fake PK不进入Archive hash、Manifest或Receipt。
`GetTableDef`附加的`row_id`只是读取伪列，必须从最终写Batch排除。
目标表缺少预期fake PK、出现其他隐藏列或逻辑列布局漂移时fail closed。这里不复制
PreInsert、不建立第二套序列服务，也不改变普通INSERT路径。

首版单个Restore串行处理chunk。一个Chunk严格等于Manifest中的一个Parquet Row Group；
按`(file_ordinal, row_group_ordinal)`升序展平后从0开始连续生成全局
`chunk_ordinal`。Restore直接消费Manifest中的边界，不得按`chunk-bytes`、INSERT Batch
大小或worker版本重新切分、合并。Receipt主键是`(restore_id, chunk_ordinal)`；
`chunk_digest`、`row_count`、`logical_bytes`和`canonical_content_hash`是普通列。

Manifest中每个Chunk必须满足`max-restore-chunk-rows`和
`max-restore-chunk-logical-bytes`。Restore在GET/解码前先验证声明值，在解码后重新计算
实际值；缺失、0值、超限或不匹配均fail closed。压缩文件size不能替代逻辑字节上限。
重算必须发生在类型转换、CHAR/DECIMAL/TIMESTAMP/JSON等MO语义正规化完成后的最终
MO vectors上，不能对Parquet decoder的中间物理表示计算后直接INSERT。

重试前查询Receipt只是优化，真正并发边界由主键和普通事务保证：

- 同ordinal且digest一致：事务冲突或响应丢失后重读Receipt，按幂等成功处理；
- 同ordinal但digest不同：标记corruption，整个Restore fail closed；
- 数据INSERT、Receipt、`next_chunk_ordinal`和`restored_rows`必须一起提交或一起回滚；
- CN在提交成功后、更新内存进度前crash，新worker以Receipt和Attempt行为准。

Restore不使用tagged entry，完全复用普通INSERT事务。

## 6. 内容验证和发布

全部chunk完成后：

- Manifest descriptor digest完整，目标表结构投影与descriptor一致；
- restored row count等于Manifest；
- 按`chunk_ordinal`读取Receipt，并使用02冻结的有序聚合公式重建
  `Manifest.dataset_content_hash`；
- Receipt数量等于`Manifest.total_chunk_count`，ordinal严格连续覆盖
  `0..total_chunk_count-1`，且无缺失或重复；
- 每个Receipt的row count、logical bytes、chunk digest和canonical content hash与Manifest
  对应Row Group完全一致；
- 重建Hash等于Manifest中的`dataset_content_hash`，`hash_formula_version`受支持；
- lease仍有效。

不持久化SHA-256内部状态，不重新扫描隐藏表，也不为最终Hash重新读取全部Payload。
Receipt按主键分页顺序流式聚合，不一次性物化全部记录；Manifest和Receipt chunk数都必须
小于等于`max_chunks_per_dataset`。AUTO_INCREMENT最大正值来自已经过Archive full
readback验证的Manifest；所有Restore Chunk又逐个验证最终MO vectors的canonical hash，
不在Receipt中维护第二套max聚合协议。

最后使用一个普通事务：

```text
最终发布事务：
CAS Dataset:
  state == PUBLISHED
  restore_lease_id == this lease
  restore_deadline > transaction time
  access_generation/version unchanged
CAS Restore Attempt:
  state == IMPORTING
  lease_id/deadline仍匹配
  next_chunk_ordinal == Manifest.total_chunk_count
  restored_rows == Manifest.row_count
  verified_content_hash IS NULL
-> 同一事务内临时置为PUBLISHING
确认Catalog中的(staging_database_id, hidden_name, staging_table_id)完全匹配
-> 按hidden_name原子改名/发布，table_id保持不变
-> 对每个有值的AUTO_INCREMENT列调用现有
   incrservice.ValidateAutoColumnOffset(target_type, archived_max_positive_value)
   incrservice.SetOffset(staging_table_id, target_column_name,
                         archived_max_positive_value_uint64, this txn)
-> Restore Attempt.verified_content_hash = recomputed dataset_content_hash
-> Restore Attempt = DONE
-> clear Dataset restore lease
-> Dataset.version = version + 1
-> commit
```

`PUBLISHING`不允许在一个独立前置事务中提交。它只是上述最终事务内部的CAS中间值：
rollback后外部仍为`IMPORTING`，commit后外部直接看到`DONE`。因此不存在
“PUBLISHING已经提交但final transaction尚未创建”的额外恢复状态。

`verified_content_hash`此前必须为NULL，只在该最终普通事务中一次性写入；Chunk事务不得把
普通SHA-256 digest当作可续算内部状态增量更新。

`SetOffset`与Rename/DONE使用同一个`TxnOperator`。参数是已恢复最大正值本身，不是
`max+1`；现有allocator从该offset之后分配。若最大正值已经达到目标整数类型上限，发布仍可
成功，但后续省略AUTO_INCREMENT值的INSERT必须按现有MO语义返回out-of-range，不能回绕。
没有归档正值（仅NULL、0或负值）的AUTO_INCREMENT列不调用`SetOffset`，沿用新表初始
offset。

Purge更新同一Dataset行，因此并发时只能一方成功：Purge先成功则Restore发布事务整体回滚；
Restore先成功则新表发布并释放lease，Purge随后可以继续。响应未知沿用普通DDL和目标table
identity对账，不建设Lifecycle终态协议。

## 7. Restore失败

- 读取/校验失败：Attempt进入有条件清理；
- lease deadline：停止新GET/INSERT，进入有条件清理；
- CN crash：新worker按Receipt继续；
- credential失败：保持Attempt并告警，deadline后abort；
- 发布失败且目标名不存在：可在deadline内重试；
- 发布结果未知：禁止Purge，先按普通Catalog检查目标table identity。
- 明确abort且仍持有自己的lease：必须在同一普通事务中按精确身份DROP隐藏表、把Attempt置为
  FAILED并CAS清除lease；只要lease仍非NULL，Purge就不能推进。CAS失败说明发布或其他终态已
  推进，只能重新按本节身份规则对账，不能覆盖新状态。

SQL级重试在分配新`restore_id`前同时查找两种精确身份：

- 有效`IMPORTING` Attempt且hidden名称仍映射`staging_table_id`：继续原Attempt；
- `DONE` Attempt且target名称仍映射同一`staging_table_id`：把前一次发布视为幂等成功。

若用户已经DROP该恢复目标，旧`DONE`记录不再匹配物理身份，允许以后以同名发起新的Restore。
因此发布响应丢失不会重复导入，也不会让历史DONE记录永久占住表名。

owner丢失或发布响应未知后，在一个普通一致性事务中按Catalog身份收敛：

```text
(target_database_id, target_name) -> staging_table_id
  -> 禁止清理，按发布成功方向对账；
     若Attempt不是DONE或Dataset lease未清除，与同事务发布合同矛盾，fail closed并告警

(staging_database_id, hidden_name) -> staging_table_id
AND target_name不映射到该table_id
  -> 允许执行下述Attempt CAS + identity DROP事务；
     若迟到的发布事务也存在，由普通MO对同一Attempt/Catalog table entry的WW conflict决胜

两个身份都不匹配或同时形成矛盾映射
  -> fail closed并告警，不DROP
```

隐藏表仍存在不证明final transaction“从未创建”，只证明清理事务可以安全参与普通事务
竞争。`staging_table_id`只用于身份核对，禁止仅凭table ID执行DROP。隐藏表清理由一个短
普通事务完成：

```text
CAS Restore Attempt:
  restore_id/lease_id匹配
  state IN (IMPORTING, PUBLISHING)
  state != DONE
-> state = FAILED

同一事务的一致性Catalog读取：
  (target_database_id, target_name)不得映射到staging_table_id
  (staging_database_id, hidden_name, staging_table_id)必须完全匹配

-> 只允许按hidden_name执行普通DROP
-> commit
```

`hidden_name`固定为`__mo_lifecycle_restore_<restore-id>`，由Attempt的`restore_id`确定。若
Attempt CAS失败，清理者必须重新读取：看到`DONE`或目标名称已映射到同一
`staging_table_id`立即停止；隐藏名称、database ID或table ID任一不匹配时fail closed并
告警，不得删除任何表。DROP失败则整个普通事务回滚，Attempt不能单独变成`FAILED`。
清理事务自身返回`ErrTxnUnknown`时保持Attempt，不得再次盲目DROP；接管者重新执行本节
一致性身份对账：

- `Attempt=FAILED`、target不映射该ID且hidden已不存在：清理事务已经可见，收敛成功；
- Attempt仍非终态且hidden精确存在：可以重新发起完整CAS+identity DROP事务；
- `Attempt=DONE`或target映射该ID：发布成功方向，禁止清理；
- 其余组合：fail closed并告警。

worker不能在unknown后单独把Attempt改成FAILED。CAS、身份读取和DROP必须在同一事务，禁止
使用事务外预读结果执行删除。

发布事务与清理事务都写同一Attempt行，并对同一Catalog table entry执行Rename或DROP；
因此旧worker、deadline cleanup和commit response lost的并发最终只能一方提交，不需要新的
claim、Journal或专用事务协议。

## 8. Purge

用户Purge或后台到达`purge_eligible_at`触发。源owner仍存在时，显式PURGE早于
`purge_eligible_at`必须拒绝；DROP owner表示产品契约已放弃Restore，可覆盖该时间：

```text
Dataset PUBLISHED AND restore_lease_id IS NULL
-> CAS DELETE_PENDING and increment access_generation
-> reject new Restore lease
-> Root PUBLISHED -> DELETE_PENDING
-> Sweeper deletes
-> Dataset DELETING -> PURGED
```

对应Catalog CAS必须带Dataset version，并等价于：

```sql
UPDATE mo_lifecycle_datasets
SET state = 'DELETE_PENDING',
    access_generation = access_generation + 1,
    version = version + 1
WHERE dataset_id = ?
  AND state = 'PUBLISHED'
  AND version = ?
  AND restore_lease_id IS NULL;
```

显式Purge遇到任何非空lease（包括已经到期但尚未清理的lease）都返回
`RESTORE_IN_PROGRESS`；后台Purge不等待lease或Provider，只延迟重试。lease deadline只使
Attempt具备cleanup资格，不直接使Purge具备准入资格。过期Restore必须先由`CleanupHidden`
在同一普通事务中按精确身份DROP隐藏表、把Attempt置为FAILED并释放Dataset lease；只有随后
重试且成功取得上述`restore_lease_id IS NULL` CAS的Purge才能推进Root。

## 9. DROP

DROP TABLE/DATABASE/ACCOUNT沿用普通MO业务语义：

- 不等待Provider；
- DROP后不保证Archive Restore；
- 不建设owner tombstone；
- 不保留Legal Hold/WORM；
- Cleanup Reconciler通过Binding/Dataset/Root owner查询发现孤儿。
- DROP TABLE在既有表DDL事务中删除对应Binding；DROP DATABASE在既有数据库DDL事务中按
  `(account_id, database_id)`补删Binding，覆盖已缺失Relation留下的孤儿元数据。两条路径
  都不访问Provider；Dataset/Payload仍由system Root异步收敛。

竞态：

- Lifecycle先提交：Dataset短暂PUBLISHED，owner扫描驱动Purge；
- DROP先提交：final exact table/DDL Gate失败，Root清理；
- commit unknown：Root保持COMMIT_UNKNOWN，不因DROP猜测；
- Restore进行中遇DROP owner：Phase 1立即停止新GET/chunk、abort Attempt并按本节身份事务
  清理隐藏表。

## 10. Snapshot/PITR、Clone/Publication与Backup/DR

Snapshot/PITR创建和保留允许与Lifecycle共存。Lifecycle退休沿用普通Object create/drop TS，
退休前的Snapshot或覆盖退休时间的PITR由现有MVCC/GC保护旧Object；引用到期后仍由普通GC
回收。Lifecycle不增加Snapshot/PITR专用引用、WAL或Replay协议。

Phase 1不支持Snapshot/PITR Restore Lifecycle Archive scope。Database/Table源或目标scope
只要存在`ARCHIVE` Binding、非`PURGED` Dataset或非`CLEANED`的`ARCHIVE_*` Root，Restore
必须在任何破坏性Restore事务提交前fail closed。直接Account Restore会恢复tenant Lifecycle
Catalog，因此存在任意TTL/Archive Binding时也拒绝，避免旧physical table identity的Binding
被重新激活。Cluster逻辑Restore兼容性不在Phase 1范围，保留已有Archive安全检查而不扩展
TTL修复。上述检查不跨完整Restore持有Lifecycle锁，也不恢复外部Payload；运维必须先关闭并
drain Lifecycle数据任务。

Clone/Data Branch只复制目标时间点的活动数据。目标表使用普通新表身份，不继承Binding、
Dataset或Archive Payload；用户需要时对新表重新执行`SET LIFECYCLE`。普通同集群
Publication/Subscription直接读取发布者物理表的活动视图，可以与Lifecycle共存；旧查询
继续服从其普通MVCC snapshot，新查询看到退休后的活动状态。CDC/CCPR属于另一条复制链路，
Phase 1不接入、不修改，也不承诺其下游收到Object退休对应的逐行DELETE。

Lifecycle不修改、扫描或阻断普通物理Backup创建。物理Backup可能包含Binding、Dataset、
Cleanup Root、Stage和release gate等Catalog状态，但不会复制Stage中的外部Archive Payload。
因此含Lifecycle状态的物理Backup Restore在Phase 1 unsupported：恢复环境不得自动启动
Lifecycle Coordinator/Cleanup Sweeper，也不得继续持有原Archive namespace的删除权限；
这两项隔离必须在任何Lifecycle tick前完成。仅设置`enabled=false`不足，因为正常集群在gate
关闭后仍会收敛历史Root。完整Backup/DR兼容由后续独立设计实现，不能把这种活动数据恢复
宣传为完整历史恢复。

## 11. Purge与Root一致性

Dataset控制逻辑可见性，Root控制物理删除：

- Dataset DELETE_PENDING但Root仍PUBLISHED：Reconciler补触发；
- Root DELETE_PENDING但Dataset仍PUBLISHED：Sweeper禁止删除并告警；
- Root CLEANED后Dataset才进入PURGED；
- PURGED Dataset按审计窗口GC。

## 12. 测试

- 多次Restore同一Dataset到不同表；
- schema含DECIMAL/TIMESTAMP/JSON/NULL/CHAR/AUTO_INCREMENT/PK；
- 初始化事务在Dataset lease、CREATE hidden和Attempt INSERT前后逐点crash/response lost；
- chunk同ordinal相同/不同digest并发、commit response lost和CN crash恢复；
- Receipt有序聚合Hash与Manifest一致，缺失/重复ordinal必须失败；
- frontend用户对staging的读写、DDL、DUMP/LOAD、按ID解析和保留名称CREATE/RENAME全部
  被拒绝；内部
  Restore初始化、Chunk导入、发布和清理不受影响；
- 无源PK的Dataset恢复时，每个Chunk通过现有incrservice生成非NULL且唯一的目标fake PK；
  Chunk事务rollback/response lost不得留下已提交数据却没有Receipt的分裂结果；
- Restore在最终MO vectors上重算Hash；AUTO_INCREMENT最大正值readback一致，发布后下一值、
  各整数类型上限和overflow行为正确；
- 最终发布事务在Attempt CAS、SetOffset、Rename、DONE和lease释放各点crash/response lost；
- target映射到staging table ID、两个身份均不匹配和cleanup `ErrTxnUnknown`均fail closed；
- Purge与GET/chunk/publish逐点竞态；
- 显式Purge遇任意非空lease（含已过期但尚未完成隐藏表清理）返回
  `RESTORE_IN_PROGRESS`，后台Purge不等待并延迟重试；
- lease到期只触发`CleanupHidden`资格；隐藏表、Attempt和lease在同一事务收敛完成后，Purge
  才能进入`DELETE_PENDING`；
- lease deadline、CN crash、Stage credential轮换；
- DROP table/database/account；
- Manifest版本不支持、文件篡改和目标表名冲突。
