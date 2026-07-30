# 07 P0证明与Commercial GA测试矩阵

## 1. 决策

Object算法可以进入原型。以下P0全部通过前不能冻结协议或宣布GA。

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

## 6. Object与Reader

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

## 7. 并发Tombstone

- S前DELETE不进Archive/live；
- S后L DELETE正确transfer；
- Archive E/NoTransfer DELETE abort；
- TTL E DELETE保守abort路径；
- Whole Archive post-S DELETE abort；
- phase 1/2错误不吞；
- delta rows/bytes/blocks/deadline；
- Prepare后旧RowID DELETE冲突；
- BigDelete guard与多个小事务累计路径。

## 8. Merge、WAL、Replay、GC

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
- Data source set与保护用Tombstone set分离；注册后重新Stat证明两类exact文件都被保护；

## 9. Discovery

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

## 10. Cleanup

- Root-before每种副作用；
- PUT成功登记前crash；
- Manifest前/后crash；
- booking写成功返回前crash；
- delayed PUT和LIST一致性；
- Dataset Purge与Root触发丢消息；
- credential轮换；
- cleanup backlog cap；
- CLEANED审计GC。

## 11. Restore/Purge/DROP

- Restore chunk response lost/duplicate；
- hash/schema/row count；
- lease与Purge逐点竞态；
- deadline abort；
- hidden table publish response lost；
- multiple Restore；
- DROP table/database/account；
- Stage unavailable；
- unsupported schema version。

## 12. DDL最后Gate

先测普通MO基线，再测Lifecycle：

- DROP/TRUNCATE/ALTER COPY；
- ADD/DROP/RENAME Lifecycle列；
- SET/UNSET Lifecycle；
- schema digest变化；
- finalizer与DDL同时到达。

验收必须证明真实锁或WW conflict，不能以“最后读取值正确”代替互斥。

## 13. 规模

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

## 14. Soak与放量

- 30天soak；
- 50→200→500→1000表；
- feature off/无Binding对照；
- 所有P0证据包含commit SHA、配置、数据生成器、故障点和原始指标。

## 15. Stop Ship

任一情况阻止GA：

- 未验证Archive已退休源数据；
- published Payload可被覆盖/误删；
- source和new Object双不可见；
- Restore静默缺行/错类型；
- unknown被当abort清理；
- 普通MO明显回归；
- 无界Root/Receipt/Restore记录；
- 老TN解析新entry panic或发生mutation。
