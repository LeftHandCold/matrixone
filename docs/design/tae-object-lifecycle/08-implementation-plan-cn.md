# 08 实施计划与代码边界

## 1. Gate A：Catalog与Discovery

交付：

- Lifecycle Catalog upgrade；
- table-level Binding SQL；
- logical partition table/physical child fail-closed admission；
- Active Binding registry；
- bounded `ScanLifecycleObjects`；
- cursor wrap/full scan；
- Dry-run和资源限额。

代码区域：

```text
pkg/frontend
pkg/sql/parsers / pkg/sql/plan / pkg/sql/compile
pkg/catalog
pkg/vm/engine/disttae/logtailreplay
pkg/vm/engine/disttae/lifecycle
pkg/taskservice
```

DoD：百万Object分页不构造全表slice；普通查询/DML/Merge在未绑定路径无Lifecycle Catalog
读，相关DDL至多一次索引化Binding lookup；分区表在任何副作用前被拒绝。

## 2. Gate B：Reader与Archive格式原型

交付：

- Whole/Mixed exact Reader；
- SyncProtection适配；
- canonical encoder；
- Parquet/ZSTD writer；
- 带`source_column_id`语义的schema descriptor/Manifest；
- Row Group chunk hash和Dataset有序聚合公式；
- 内存或测试临时FileService中的round-trip。

此Gate不退休源数据，也不开放生产Provider PUT。真实外部副作用必须等待Gate C的Root。

## 3. Gate C：Cleanup Root

交付：

- system Root table和CAS API；
- Root-before-side-effect；
- deterministic prefix/write-id；
- Stage冻结target adapter、引用DDL和credential handle验证；
- 部署Stage认证/allowlist与非Versioned专用Bucket准入；
- immutable key、Provider full readback和生产Export-only；
- Provider multipart规则认证；
- Sweeper、quiescence和metadata GC；
- commit unknown只读对账；
- hard caps/metrics。

P0-1/P0-2通过后才能冻结。

## 4. Gate D：Whole thin entry

交付：

- unknown Entry在Batch前fail closed；
- `api.Entry.EntryType=7`、payload field=11的`LifecycleRetireEntry` protobuf和dispatch；
- disttae workspace的nil-by-default `lifecycleRetireEntry`和`genWriteReqs`追加点；
- 复用`PrecommitWriteCmd.SyncProtectionJobId`，不复制第二个protection字段；
- Finalizer普通事务；
- Whole exact source CAS；
- Dataset/TTL Receipt同事务；
- rolling upgrade开关；
- WAL/Replay/GC测试。

不建设HAKeeper capability。先独立发布unknown Entry安全解析，再发布生成V1 entry的CN；
集群升级控制面只判断全部TN达到这个兼容下限。

## 5. Gate E：Mixed Rewrite

交付：

- `LifecycleRewriteHost`；
- full physical Block + D/E/L；
-复用`DoMergeAndWrite`；
- Lifecycle-only external booking path allocator/force external；
- Root-owned staging rollback边界；
- bounded post-S Tombstone visitor；
- single-source admission。

普通Merge调用接口不变，新增options默认nil。

## 6. Gate F：TTL small Mixed

交付：

- fixed SI transaction；
- RowID + real/fake PK；
- bounded `Relation.Delete`；
- TTL Receipt；
- Tombstone/backlog/transaction limits；
- 超限Rewrite/Blocked。

## 7. Gate G：Restore/Purge

交付：

- Dataset lease；
- Manifest schema建隐藏表；
- 串行chunk普通INSERT + Receipt + Attempt进度同事务；
- Receipt主键`(restore_id, chunk_ordinal)`及不同digest fail-closed；
- Receipt有序聚合hash/row验证；
- DDL发布、Attempt DONE和Dataset lease清除同一普通事务；
- active lease下Purge返回/延迟，lease结束后触发Root；
- DROP异步清理。

Restore不增加tagged entry。

## 8. Gate H：DDL fence（最后）

顺序：

1. 普通Merge/DDL基线测试；
2. Lifecycle finalizer/DDL测试；
3. 通用Bug则提Issue并修公共路径；
4. Lifecycle独有缺口选择`mo_tables`锁或Binding write CAS；
5. 绑定表按01的Phase 1矩阵fail closed，未绑定表不创建额外元数据；
6. 所有竞态测试通过。

Gate H前只允许受控无不兼容DDL的退休验证。

## 9. Gate I：认证与发布

- P0矩阵；
- provider认证；
- 1/10 TiB；
- 30天soak；
- active coexistence；
- Lifecycle不增加TN专用permit；认证若触发普通Merge公共资源缺陷，关联公共Issue或降低
  认证上限后重新测试；
- 50→1000放量；
- Runbook和kill switch；
- Phase 1产品文档明确未覆盖Issue #24853剩余能力。

## 10. 包边界

建议新增：

```text
pkg/vm/engine/disttae/lifecycle/
  discovery.go
  reader.go
  rewrite_host.go
  archive_writer.go
  finalizer.go
  limits.go

pkg/vm/engine/tae/rpc/handle_lifecycle.go
pkg/vm/engine/tae/tables/txnentries/lifecycle_objects.go
pkg/frontend/lifecycle_*.go
pkg/catalog/tuplesParse.go                 unknown/tagged Entry pre-dispatch
pkg/vm/engine/disttae/tools.go             thin entry append before payload encode
pkg/vm/engine/disttae/types.go             nil-by-default control pointer
```

允许对现有Merge抽取/参数化的窄修改：

- external booking path allocator和Owner；
- Merge txn entry的`CreatedObjectOwnedByLifecycleRoot`可选模式，普通默认不变；
- Tombstone visitor可配置起点/预算；
- thin entry dispatch；
- unknown Entry安全解析。

禁止：

- 修改普通Merge选择/排序；
- 普通DML查询Lifecycle；
- Provider I/O进入TN Prepare；
- 第二套Object writer/transfer/WAL/GC；
- 为Lifecycle修通用Txn终态问题。

## 11. Review边界

每个PR必须回答：

1. 是否触达未绑定普通路径；
2. 资源Q1唯一Owner；
3. Q2等待终止；
4. Q3增长上限；
5. crash/restart代际；
6. feature off兼容；
7. 是否恢复了已删除的重型协议。

## 12. 公共MO前置与非前置

Lifecycle不私下复制公共修复：

- [#26376](https://github.com/matrixorigin/matrixone/issues/26376) 的Merge slab OOM panic必须在
  Commercial GA基线中由普通Merge公共实现修复；Lifecycle不增加私有recover吞掉panic；
- [#26377](https://github.com/matrixorigin/matrixone/issues/26377) 的普通Merge Prepare预算不作为
  Lifecycle阻塞项，因为Lifecycle使用自己的有界delta visitor；
- [#26445](https://github.com/matrixorigin/matrixone/issues/26445) 的普通Merge注册前清理可独立
  修复；Lifecycle builder按03的Owner合同自行关闭本路径，不改变普通Merge默认。

Gate测试若发现新的普通MO通用Bug，单独提Issue/公共修复；设计文档只记录依赖，不增加
Lifecycle补偿状态机。

## 13. Definition of Done

每个Gate：

- 设计接口与代码一致；
- 单元/并发/故障测试；
- deadline、retry和hard cap；
- 普通Merge公共问题只记录依赖/公共修复，不增加Lifecycle私有资源或事务状态机；
- 指标/错误/Runbook；
- Markdown和链接检查；
- 普通MO回归；
- commit SHA和证据归档。
