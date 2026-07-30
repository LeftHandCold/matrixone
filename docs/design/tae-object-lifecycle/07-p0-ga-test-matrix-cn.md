# 07 P0证明与Commercial GA测试矩阵

## 1. 决策

Object算法可以进入原型。第2～5节是已保留的基础安全门禁；第6节是本轮协议冻结前新增的
五项Lifecycle P0。全部通过前不能宣布协议完成或Commercial GA。

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
- unknown protocol version：fail closed；
- workspace普通DML发生dump/compact/sort后，thin entry字节仍只追加一次；
- 普通Catalog写缺失的control-only生产事务必须在CN拒绝；
- ordinary transaction可选指针恒nil，不分配Lifecycle对象；
- mixed version集群retirement开关不可开启；
- Export-only可运行；
- 升级全员ready后开启；
- 降级前等待FINALIZING/COMMIT_UNKNOWN。

## 6. 本轮五项Lifecycle P0

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
- Manifest/Receipt达到`max_chunks_per_dataset`边界，聚合使用有界分页内存；
- 不持久化SHA内部状态，不依赖重新扫描隐藏表或全部Payload。

### 6.2 Restore/Purge Lease

- Restore acquire与Purge竞争同一Dataset CAS；
- 有有效lease时显式Purge返回`RESTORE_IN_PROGRESS`；
- 有有效lease时后台Purge不等待，延迟重试；
- lease终止或deadline后，Purge才能进入`DELETE_PENDING`；
- 一旦进入`DELETE_PENDING`，旧worker的GET/chunk必须失败，且不存在“旧lease继续读”；
- Restore最终DDL事务和Purge同时到达，验证双方CAS同一Dataset行且只能一方提交；
- Restore发布成功必须同时清除lease并把Attempt置为DONE；
- DROP不等待lease或Provider，后台按相同CAS收敛。

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

## 7. Object与Reader

- Whole单/多源，任一source冲突全abort；
- Mixed完整物理Block；
- D/E/L随机交错属性测试；
- all D、all E、all L；
- DoMergeAndWrite mapping round-trip；
- source/transfer digest跨CN/TN golden bytes一致，domain/version变化必须拒绝；
- CreatedObjs顺序不可改变；
- created Object Level沿用普通Merge晋级，不能把高Level source降级；
- max Object和非默认BlockMaxRows/ObjectMaxBlocks；
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
- SyncProtection注册后重Stat、续租失败、TN restart。
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

## 13. DDL最后Gate

先测普通MO基线，再测Lifecycle：

- DROP/TRUNCATE/ALTER COPY；
- ADD/DROP/RENAME Lifecycle列；
- SET/UNSET Lifecycle；
- schema digest变化；
- finalizer与DDL同时到达。

验收必须证明真实锁或WW conflict，不能以“最后读取值正确”代替互斥。

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
- feature off/无Binding对照；
- 所有P0证据包含commit SHA、配置、数据生成器、故障点和原始指标。

## 16. Stop Ship

任一情况阻止GA：

- 未验证Archive已退休源数据；
- published Payload可被覆盖/误删；
- source和new Object双不可见；
- Restore静默缺行/错类型；
- Restore同ordinal不同digest被重复导入；
- Tombstone metadata unknown被当作不相交而跳过；
- 逻辑分区表被部分处理；
- Versioned Bucket被宣称完成物理Purge但历史版本仍计费；
- unknown被当abort清理；
- 普通MO明显回归；
- 无界Root/Receipt/Restore记录；
- 老TN解析新entry panic或发生mutation。
