# 07 P0证明与Commercial GA测试矩阵

## 1. 决策

Object算法可以进入原型。第2～5节是已保留的基础安全门禁；第6节汇总协议冻结前所有
Lifecycle自身P0。全部通过前不能宣布协议完成或Commercial GA。

## 2. P0-1 Cleanup Root闭环

证明：

- 所有pre-final失败进入DELETE_PENDING；
- worker crash/lease loss有唯一处理者；
- COMMIT_UNKNOWN合法收敛；
- PUBLISHED由Purge/owner loss触发清理；
- Dataset只控制逻辑，Root始终控制Payload物理删除；
- live Object commit后只归TAE。
- Archive Rewrite提交后只清booking、保留Payload；
- TTL Rewrite提交后清booking但不删除TAE已接管live Object。
- Archive/TTL Rewrite退化为Whole retire时，commit entry不携带created Object/booking，
  但Root mode仍为`ARCHIVE_REWRITE`/`TTL_REWRITE`并清理本attempt预留过的TAE namespace；
- Archive和TTL都覆盖protection后、source read前、final commit前和final commit后的确定性
  故障点；pre-commit故障不能退休source，post-commit故障不能把已发布Owner转入清理。

故障点覆盖每个Root状态进入前后。

finalizer原子性还必须覆盖：删除/损坏thin entry时Dataset/TTL Receipt不能单独提交；thin
entry失败时普通Catalog写必须随整个事务rollback。

## 3. P0-2 Immutable key和multipart

测试旧worker慢PUT、新worker新attempt、迟到完成：

- key不能相同；
- Dataset只引用验证过的write-id；
- 旧Root清理不碰新prefix；
- Manifest digest key不可覆盖；
- provider incomplete multipart规则已认证；
- CN crash后未完成multipart最终被Provider回收。

## 4. P0-3 Schema Descriptor

Golden类型矩阵：

```text
integer/unsigned/float/decimal
date/datetime/timestamp/time
char/varchar/text/binary
json/uuid/enum/blob
NULL/non-NULL
AUTO_INCREMENT
```

验证历史Dataset在源表DROP/schema变化后仍能创建新表；AUTO_INCREMENT计数器安全推进；
PK、UNIQUE/CHECK/FK、二级索引、CDC等不应自动出现。

## 5. P0-4 Rolling upgrade fail-closed

- 新CN→具备安全解析但不支持V1的TN：Batch解析前unsupported，无panic/mutation；
- 未部署安全解析补丁的更老TN：发布控制面禁止开启retirement或路由V1；
- 旧CN→新TN：普通请求正常；
- 旧CN与新Catalog混部时，`DROP ACCOUNT`不会因Lifecycle新表缺少`account_id`失败，且tenant
  读取system Cleanup Root只能得到空集；Root的`account_id=0`不得替代`owner_account_id`；
- tenant异步upgrade尚未创建Lifecycle表时，普通ALTER/INDEX/TRUNCATE/DROP等管理DDL只把
  精确`ErrNoSuchTable`视为“无Binding”，其他错误继续fail closed；SET/SHOW/RESTORE等
  Lifecycle命令仍明确失败且不产生副作用；
- unknown protocol version：fail closed；
- workspace普通DML发生dump/compact/sort后，thin entry字节仍只追加一次；
- 普通Catalog写缺失的control-only生产事务必须在CN拒绝；
- ordinary transaction可选指针恒nil，不分配Lifecycle对象；
- mixed version集群retirement开关不可开启；
- 测试/认证Export-only可显式运行；生产release关闭时不创建新Root或Provider PUT；
- 升级全员ready后开启；
- 降级前等待FINALIZING/COMMIT_UNKNOWN。

## 6. Lifecycle协议P0

### 6.1 Restore Chunk唯一性与可重启Hash

- Catalog主键必须是`(restore_id, chunk_ordinal)`，`chunk_digest`是普通列；
- 同ordinal、相同digest并发：最多一个事务写入数据和Receipt，另一方重读后幂等成功；
- 同ordinal、不同digest并发：冲突方检测corruption，Restore fail closed；
- 数据INSERT、Receipt、`next_chunk_ordinal`和`restored_rows`同事务提交/回滚；
- chunk commit成功但response lost，以及成功后更新内存进度前CN crash；
- 一个Manifest Row Group恰好对应一个Chunk，按file/row-group ordinal展平后全局编号；
- 不同Restore worker、`chunk-bytes`和INSERT Batch配置得到相同Chunk边界与ordinal；
- Manifest保存且校验`total_chunk_count`、`dataset_content_hash`和
  `hash_formula_version`；
- 按ordinal读取Receipt可重建Manifest的Dataset聚合Hash；
- Receipt必须严格覆盖`0..total_chunk_count-1`；缺失、重复、ordinal不连续、row count
  或chunk hash不一致均禁止发布；
- Chunk事务不得更新增量SHA digest；最终发布事务一次性写
  `Attempt.verified_content_hash`；
- Row Group行数和canonical logical bytes分别覆盖cap-1、cap、cap+1；
- 极高压缩率、大BLOB/VARBINARY和单行超logical bytes上限；
- Writer必须在越界前flush；单行超限返回`RESOURCE_BLOCKED`且源数据仍可见；
- Manifest Row Group `logical_bytes`缺失、为0、超限或与readback/Restore实际值不一致；
- Restore必须在GET/解码前拒绝声明超限，不能用压缩size替代；
- Archive后调低运行时阈值返回`RESOURCE_BLOCKED`，恢复到release hard cap内可继续；
- 同一Manifest reader/version跨升级保持认证Restore hard cap，非法降级fail closed；
- Manifest/Receipt达到`max_chunks_per_dataset`边界，聚合使用有界分页内存；
- 不持久化SHA内部状态，不依赖重新扫描隐藏表或全部Payload；
- Parquet Row Group转换为最终MO vectors后重算canonical hash，再进入INSERT。

### 6.2 Restore/Purge Lease与隐藏表Owner

- Dataset lease、CREATE hidden和INSERT Attempt必须在同一普通事务；
- 初始化事务在CREATE/Attempt INSERT/commit response lost各故障点不留下无Owner隐藏表；
- Restore acquire与Purge竞争同一Dataset CAS；
- 有任意非空lease时（包括已过期但尚未清理）显式Purge返回
  `RESTORE_IN_PROGRESS`；
- 有任意非空lease时后台Purge不等待，延迟重试；
- lease deadline只使Attempt具备cleanup资格；`CleanupHidden`必须在同一普通事务中精确DROP
  隐藏表、置Attempt FAILED并释放lease，随后Purge才能进入`DELETE_PENDING`；
- 过期lease下并发`CleanupHidden`与Purge，验证Purge不会抢先改变Dataset state并导致隐藏表
  清理事务回滚或留下永久孤儿；
- 一旦进入`DELETE_PENDING`，旧worker的GET/chunk必须失败，且不存在“旧lease继续读”；
- Restore最终DDL事务和Purge同时到达，验证双方CAS同一Dataset行且只能一方提交；
- Restore发布事务必须校验Attempt IMPORTING、lease、Chunk/row进度、
  `verified_content_hash IS NULL`和隐藏表精确身份；
- 最终发布事务在IMPORTING CAS、SetOffset、Rename、DONE和lease释放前后逐点crash；
- Restore发布成功必须同时清除lease、写`verified_content_hash`并把Attempt置为DONE；
- 发布事务与deadline cleanup、旧worker cleanup逐点竞态，最终只能一方提交；
- Rename后table ID保持不变，旧worker不得按ID删除正式目标表；
- cleanup只能CAS非DONE Attempt，并校验隐藏名、database ID和table ID后按名称DROP；
- cleanup CAS失败、目标名映射同一table ID、hidden identity不匹配和commit response lost；
- `PUBLISHING`不得由独立事务提交；rollback后外部仍为IMPORTING，commit后直接为DONE；
- 发布响应未知时target映射同一table ID停止清理，hidden精确映射时与迟到发布普通WW竞争；
- SQL重试发现`DONE + target名称/物理table ID`精确匹配时幂等成功，不创建第二个Attempt；
  目标表DROP后旧DONE不再匹配，允许以后重新Restore；
- in-doubt时target/hidden均不匹配，或cleanup `ErrTxnUnknown`结果尚不可判定时fail closed，
  不盲目DROP；
- cleanup unknown后分别覆盖`FAILED+hidden absent`、`nonterminal+hidden exact`、
  `DONE/target published`和矛盾组合；
- DROP不等待lease或Provider，后台按相同CAS收敛。
- frontend用户对精确canonical staging执行SELECT/INSERT/UPDATE/DELETE/TRUNCATE/ALTER/
  DROP/RENAME、DUMP TABLE/LOAD TABLE和按ID解析均被拒绝，CREATE/RENAME到其大小写变体
  也被拒绝；同前缀但非32位十六进制ID形状的升级前用户表继续可用；
- 内部SQLExecutor仍能完成staging CREATE、Chunk写入、Rename发布和失败DROP，普通表解析
  不访问Lifecycle Catalog；
- 无源PK表Restore的每个Chunk都由现有incrservice生成非NULL、唯一fake PK；Chunk事务
  rollback/response lost时数据、Receipt和进度仍一起提交或回滚。

### 6.3 Tombstone unknown fail-closed

- Snapshot Reader和SyncProtection消费同一次Tombstone选择的exact identities；
- 有效ZoneMap明确不相交时才允许排除；
- ZoneMap缺失、未初始化、legacy、截断、解码失败和异常RowID范围均保守纳入；
- unknown路径不得调用`RowidPrefixEq`决定排除；
- 完整metadata无法解析则attempt失败；
- 保守集合超过files/bytes上限返回`RESOURCE_BLOCKED`，不能继续退休源Object。

### 6.4 分区表准入

- SET LIFECYCLE拒绝逻辑分区表；
- SET LIFECYCLE拒绝物理Partition child；
- 已绑定表转换为分区表必须在DDL Gate fail closed；
- 拒绝发生在任何Candidate、Root或Provider副作用之前；
- 非分区普通表路径不增加Partition metadata访问。

### 6.5 非Versioned Archive Stage

- 无部署认证/allowlist记录的Stage拒绝Archive Binding；
- 认证目标必须是专用、Versioning关闭的Bucket/Container；
- Versioned Bucket不能被错误宣称支持精确物理Purge；
- incomplete multipart回收规则仍是独立准入条件；
- 撤销认证后暂停新Archive，相关cleanup不误标`CLEANED`；
- Provider运维清理历史版本并重新认证后才能收敛异常Root；
- 通用FileService不新增version ID API。

### 6.6 Data source与Tombstone protection wire

- retire entry的`data_sources[]`只含Data Object，不含Tombstone Object；
- `source_set_digest`只覆盖physical table和按Object ID排序的Data ObjectStats；
- Reader输入与SyncProtection protection set来自同一次Tombstone选择；
- Tombstone Object只被保护和读取，TN永远不对其调用`SoftDeleteObject`；
- CN/TN source digest golden bytes完全一致，旧`is_tombstone`字段号必须`reserved`且V1
  编码器不能产生；
- 构造包含共享Tombstone Object的source/protection集合，证明退休一个Data Object不会Drop
  protection-only Tombstone。

### 6.7 Restore初始化Owner

- Dataset lease、hidden table CREATE和Restore Attempt INSERT同事务提交/回滚；
- CREATE成功后Attempt INSERT失败、commit前crash和commit response lost；
- 对账必须通过Dataset lease、Attempt和精确hidden identity找到唯一Owner；
- 不允许“先CREATE、后补Attempt”的实现或第二张隐藏表。

### 6.8 AUTO_INCREMENT恢复

- Manifest按AUTO_INCREMENT列保存`has_positive_value/max_positive_value`，full
  readback从最终MO逻辑值复核；
- Manifest保存全局maxima并由Archive full readback从最终MO逻辑值验证；
- Chunk提交后、最终发布前crash不能依赖进程内max，也不能重扫TB级隐藏表/Payload；
- Restore Chunk在最终MO vectors上复核Hash和max后INSERT；
- Rename/DONE事务先验证目标类型上限，再使用同一`TxnOperator`调用现有`SetOffset(max)`；
- 发布后下一次自动分配严格大于已恢复最大值；
- int/uint各宽度的负值、0、普通最大正值、类型上限和out-of-range；
- 类型上限不执行`max+1`，不能回绕；
- 仅NULL、0或负值时不调用SetOffset，沿用新表初始offset；
- SetOffset失败使Rename、Attempt DONE和Dataset lease clear整体回滚。

### 6.9 持久格式版本

- Dataset初始`version=1`，lease/state/Purge CAS全部比较并递增；
- 并发Restore/Purge使用旧version只能一方成功；
- Manifest顶层`manifest_format_version=1`；
- 缺失、0和未知Manifest版本在解析可变长度字段、GET Payload或CREATE表前fail closed；
- schema/hash/encoder子版本不能替代Manifest parser版本。

### 6.10 TB级资源保险丝

- `rewrite_amplification`低于、等于、高于release profile边界；
- 1% expired的最大Object连续多轮不会无限重写live data；
- account/cluster固定窗口source bytes按开始读取的attempt计费，失败重试不返还；
- Coordinator切换在当前窗口保守停止Rewrite，不能重置预算；
- Restore按Dataset logical bytes执行account/cluster active staging准入；
- 两个CN并发Restore初始化必须由现有feature row短锁串行；前一事务提交后，后一事务在同一
  普通事务中看到新Attempt并重新计算容量，不能共同越过account/cluster hard cap；
- 前一初始化事务abort时不留下reservation，response lost/late commit由普通事务结果和
  Attempt身份对账；账户枚举、lock wait或30秒deadline超限必须在Dataset lease、隐藏表和
  Attempt首次副作用前fail-closed；
- cleanup backlog cap暂停Archive Whole/Rewrite和TTL Rewrite等所有Root creator；
- `max-bound-tables`只作为认证集群上限，不生成分布式Slot。

## 7. Object与Reader

- Whole单/多源，任一source冲突全abort；
- Whole在64 source/4 GiB初始Release Profile上按任一先到切分；一个批次只生成一个
  Dataset/Root，不能回退为一Object一Dataset；
- Mixed完整物理Block；
- D/E/L随机交错属性测试；
- all D、all E、all L；
- DoMergeAndWrite mapping round-trip；
- source/transfer digest跨CN/TN golden bytes一致，domain/version变化必须拒绝；
- Data source digest不包含Tombstone，protection-only Tombstone不进入Drop集合；
- CreatedObjs顺序不可改变；
- created Object Level沿用普通Merge晋级，不能把高Level source降级；
- max Object；非默认BlockMaxRows/ObjectMaxBlocks必须在读Block前`RESOURCE_BLOCKED`；
- created Object/booking实际ordinal超过Root write-ahead upper bound时fail closed；
- oversize Block读取前拒绝；
- Batch release exactly once；
- ArchiveWriter callback不持有borrowed vector，Close/错误/取消下buffer exactly-once释放。
- Manifest保存`source_column_id`；Restore目标列使用新ID，结构校验忽略源ID。

## 8. 并发Tombstone

- S前DELETE不进Archive/live；
- S后L DELETE正确transfer；
- Archive E/NoTransfer DELETE abort；
- TTL E DELETE保守abort路径；
- Whole Archive post-S DELETE abort；
- phase 1/2错误不吞；
- delta rows/bytes/blocks/deadline；
- Prepare后旧RowID DELETE冲突；
- BigDelete guard与多个小事务累计路径。

## 9. Merge、WAL、Replay、GC

- 普通Merge抢先，Lifecycle CAS失败；
- Lifecycle先提交，普通Merge安全失败；
- TN restart；
- NeedRetry；
- 1PC/2PC；
- response lost；
- NeedRetry后的内部TAE txn从entry/booking重建，不复用上一代runtime指针；
- LogTxnEntry前后逐点失败，runtime exactly-once释放且Root文件不被txn rollback删除；
- WAL replay后source/new Object可见性；
- GC只删DropIntent源文件；
- SyncProtection注册后重Stat、attempt绝对deadline到期、TN restart。
- Data source set与Tombstone set职责分离，但Reader输入和Protection identities必须来自同一
  Tombstone选择结果；注册后重新Stat证明两类exact文件都被保护；

## 10. Discovery

- 一页0/1/max Object；
- maxMetaBytes先命中；
- Lifecycle列为sort key时不额外读metadata；
- 非sort key只加载目标seqnum ZoneMap并受requests/bytes限制；
- ZoneMap缺失/截断/seqnum变化进入Reader classify；
- evaluation_time重试不漂移，UTC/DATE/DATETIME时区和DST golden test；
- cursor stale重置；
- merge新增Object排在cursor前；
- end wrap；
-强制full_scan_interval；
- 百万Object不构造全表slice；
- 持续Merge下没有永久饥饿；
- 大量无Binding tenant时，一轮Coordinator最多消费配置的Binding分页数，cursor在下轮继续，
  不能为了凑满`max-bindings-per-run`遍历全部account；
- `expire + late arrival`和`purge`超过106751天时SET必须拒绝，边界值不能在worker的
  `time.Duration`计算中溢出；
- 1000 Binding公平性。

## 11. Cleanup

- Root-before每种副作用；
- PUT成功登记前crash；
- Manifest前/后crash；
- booking写成功返回前crash；
- delayed PUT和LIST一致性；
- Dataset Purge与Root触发丢消息；
- credential轮换；
- Stage认证撤销/Versioning合同被破坏时不误报物理清理完成；
- cleanup backlog cap；
- cleanup cap同时暂停TTL Rewrite等所有Root creator；
- CLEANED审计GC。

## 12. Restore/Purge/DROP

- Restore chunk相同/不同digest、response lost和duplicate；
- Receipt有序聚合hash、schema/row count；
- lease与Purge逐点竞态；
- active lease下Purge fail-fast/后台延迟；
- deadline abort；
- hidden table publish response lost；
- multiple Restore；
- DROP table/database/account；
- Stage unavailable；
- unsupported schema version。

## 13. 管理路径依赖与DDL Gate

先测普通MO基线，再测Lifecycle：

- DROP/TRUNCATE/ALTER COPY；
- ADD/DROP/RENAME Lifecycle列；
- SET/UNSET Lifecycle；
- schema digest变化；
- finalizer与DDL同时到达。
- 首次SET与Snapshot/PITR/Publication/Clone/Branch创建同时到达；
- pessimistic/optimistic事务下`SET LIFECYCLE`的`mo_tables -> feature row`锁顺序，以及
  普通表DDL不取得feature row；
- feature关闭时只允许历史Cleanup Root的有界reconcile/sweep、过期Restore隐藏表清理和
  终态元数据压缩，不启动Binding调度、新Restore或数据路径；未绑定表的普通查询/DML/Merge
  不访问Lifecycle元数据，可能冲突的管理DDL仍只走一次有界控制面检查；
- 重复Coordinator tick不排队；Root放弃、Rollback、Close和临时清理在父context取消后仍
  使用独立固定deadline终止；
- 新CN→旧TN、旧CN→新TN、滚动升级、关闭retirement后降级。

验收必须证明真实锁或WW conflict，不能以“最后读取值正确”代替互斥。CDC使用PITR
依赖门禁。物理Backup在retirement gate开启、任一账户仍有Binding/非`PURGED` Dataset，
或system account仍有非`CLEANED` Root时必须拒绝；只有gate关闭且三类状态均清空/收敛后
才允许。DR不能静默恢复不完整历史。

## 14. 规模

1 TiB常见场景：

- 时间有序/乱序；
- 持续INSERT/UPDATE/DELETE/Merge；
- Archive、TTL、Restore并发；
- Provider限流。

10 TiB认证：

- 最大Object；
- retained source bytes；
- staging和cleanup backlog；
- 失败重跑写放大；
- full scan周期；
- 普通MO active coexistence。

## 15. Soak与放量

- 30天soak；
- 50→200→500→1000表；
- 按认证Release Profile施加最大Root/Dataset/Receipt产生率，证明Cleanup和终态Compactor
  消费率不低于产生率且backlog无持续正斜率；否则Stop Ship、提高有界批次或降低生产上限；
- feature off/无Binding对照；
- 所有P0证据包含commit SHA、配置、数据生成器、故障点和原始指标。

## 16. Stop Ship

任一情况阻止GA：

- 未验证Archive已退休源数据；
- published Payload可被覆盖/误删；
- source和new Object双不可见；
- protection-only Tombstone Object被错误退休；
- Restore静默缺行/错类型；
- Restore隐藏表在Attempt创建前成为无Owner副作用；
- Restore AUTO_INCREMENT下一值重复或回绕；
- Restore同ordinal不同digest被重复导入；
- 已发布Dataset包含超认证上限且无法恢复的单Chunk；
- 旧Restore清理者删除Rename后的正式新表；
- Tombstone metadata unknown被当作不相交而跳过；
- 逻辑分区表被部分处理；
- Versioned Bucket被宣称完成物理Purge但历史版本仍计费；
- unknown被当abort清理；
- 普通MO明显回归；
- 无界Root/Receipt/Restore记录；
- 老TN解析新entry panic或发生mutation。
- Mixed Rewrite或Restore staging突破release profile并持续挤压普通MO。
