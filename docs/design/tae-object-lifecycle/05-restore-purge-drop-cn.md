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
- schema format和canonical encoder版本受支持；
- Stage identity和credential handle可解析；
- 所有文件key位于Dataset冻结namespace；
- 文件数/bytes在Restore预算内。

未知类型或版本fail closed，不能按Parquet推测MO类型。

## 3. Lease

Dataset一次最多一个Restore lease：

```text
PUBLISHED + no active lease
-> CAS access_generation/version
-> set restore_lease_id, restore_deadline
```

每次GET和每个chunk前验证：

- Dataset仍PUBLISHED；
- lease ID匹配；
- 当前时间小于固定restore_deadline；
- access_generation未变化。

worker可续租心跳，但不能超过创建时冻结的absolute deadline。

## 4. Hidden staging table

名称：

```text
__mo_lifecycle_restore_<restore-id>
```

创建在目标database，普通用户不可直接访问。使用Manifest schema descriptor创建；目标名
已存在时在开始前失败。

## 5. 分块写入

```text
for each manifest file ordinal
  -> read and verify SHA-256
  -> decode row groups
  -> canonical hash
  -> convert to MO vectors
  -> normal INSERT into staging
  -> insert chunk Receipt in same transaction
```

chunk key是`(restore_id, file_ordinal, row_group_ordinal, chunk_digest)`的稳定映射。
重试前查询Receipt；存在且digest一致跳过，不一致报corruption。

Restore不使用tagged entry，完全复用普通INSERT事务。

## 6. 内容验证和发布

全部chunk完成后：

- schema digest相等；
- restored row count等于Manifest；
- restored canonical content hash等于Manifest；
- 无缺失/重复chunk；
- lease仍有效。

最后用普通DDL事务把隐藏表原子改名/发布为目标新表。响应未知沿用普通DDL对账，不建设
Lifecycle终态协议。

## 7. Restore失败

- 读取/校验失败：Attempt ABORTED，普通DROP隐藏表；
- lease deadline：停止新GET/INSERT，DROP隐藏表；
- CN crash：新worker按Receipt继续；
- credential失败：保持Attempt并告警，deadline后abort；
- 发布失败且目标名不存在：可在deadline内重试；
- 发布结果未知：禁止Purge，先按普通Catalog检查目标table identity。

## 8. Purge

用户Purge或后台到达`purge_eligible_at`触发。源owner仍存在时，显式PURGE早于
`purge_eligible_at`必须拒绝；DROP owner表示产品契约已放弃Restore，可覆盖该时间：

```text
Dataset PUBLISHED
-> CAS DELETE_PENDING and increment access_generation
-> reject new Restore lease
-> existing lease may finish only before frozen deadline
-> Root PUBLISHED -> DELETE_PENDING
-> Sweeper deletes
-> Dataset DELETING -> PURGED
```

Purge SQL事务不等待Provider。若Restore超时，先abort Restore并清理隐藏表。

## 9. DROP

DROP TABLE/DATABASE/ACCOUNT沿用普通MO业务语义：

- 不等待Provider；
- DROP后不保证Archive Restore；
- 不建设owner tombstone；
- 不保留Legal Hold/WORM；
- Cleanup Reconciler通过Binding/Dataset/Root owner查询发现孤儿。

竞态：

- Lifecycle先提交：Dataset短暂PUBLISHED，owner扫描驱动Purge；
- DROP先提交：final exact table/DDL Gate失败，Root清理；
- commit unknown：Root保持COMMIT_UNKNOWN，不因DROP猜测；
- Restore进行中遇DROP owner：Phase 1立即停止新GET/chunk、abort Attempt并清理隐藏表。

## 10. Backup/PITR/DR

Lifecycle绑定表的普通Snapshot/PITR/Backup/Clone/Branch在Phase 1准入时拒绝。DR目标没有
Archive Catalog/Stage时，`RESTORE ARCHIVE`明确返回unsupported，不能返回空数据或声称
恢复完整。

## 11. Purge与Root一致性

Dataset控制逻辑可见性，Root控制物理删除：

- Dataset DELETE_PENDING但Root仍PUBLISHED：Reconciler补触发；
- Root DELETE_PENDING但Dataset仍PUBLISHED：Sweeper禁止删除并告警；
- Root CLEANED后Dataset才进入PURGED；
- PURGED Dataset按审计窗口GC。

## 12. 测试

- 多次Restore同一Dataset到不同表；
- schema含DECIMAL/TIMESTAMP/JSON/NULL/CHAR/AUTO_INCREMENT/PK；
- chunk commit response lost和重复；
- Purge与GET/chunk/publish逐点竞态；
- lease deadline、CN crash、Stage credential轮换；
- DROP table/database/account；
- Manifest版本不支持、文件篡改和目标表名冲突。
