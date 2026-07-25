# P0 协议证明与 Commercial GA 验收

> 本文唯一负责 P0 case、故障/并发/升级/规模测试、证据产物和GA决策门禁。
>
> 任何“代码已完成”“测试大体通过”都不能替代本文件的明确exit criteria。

## 1. 决策规则

### 1.1 允许进入开发

```text
详细设计通过
  -> P0协议原型开发 Go
```

### 1.2 允许Preview退休

```text
全部P0 case通过
AND Catalog/Feature Guard/Profile/capability闭环
AND no open data invariant bug
  -> limited Preview Conditional Go
```

### 1.3 允许Commercial GA

```text
全部P0/P1 GA case通过
AND 1/10 TiB认证
AND provider certification
AND rolling upgrade/downgrade fence
AND Stage 0..4放量完成
AND 30天无数据不变量失败
  -> Commercial GA Go
```

任一“Archive已退休但无法确定Restore”的路径为No-Go。

## 2. 测试层级

| 层 | 目的 |
|---|---|
| Unit | canonical encoding、CAS、状态机、budget边界 |
| Package integration | disttae Reader/Delete、TAE Strict/replay、Catalog txn |
| Distributed integration | multi-CN/TN、1PC/2PC、LockService、TaskService |
| Chaos | crash、response lost、网络分区、Provider迟到副作用 |
| Scale/soak | 1/10 TiB、1000 Binding、backlog/retained bytes |
| Provider certification | 每种支持Profile的PUT/GET/LIST/multipart/Delete一致性 |
| Upgrade | old/new CN/TN、protocol gate、rollback fence |

所有测试必须能在CI小规模执行等价模型；TiB/provider/soak由nightly/release环境执行。

## 3. P0-1 Exact Reader与Batch Owner

### 3.1 要证明

- exact persisted Object/Block全集且只读该集合；
- 固定Snapshot和Snapshot Tombstone；
- 不混入in-memory rows；
- callback借用边界；
- success/error/cancel/panic exactly-once cleanup；
- complete empty与short read可区分；
- canonical root稳定；
- streaming内存有界。

### 3.2 Case

| ID | 场景 | 通过条件 |
|---|---|---|
| RDR-001 | 3 Object × 多Block顺序 | Object/Block/row offset canonical |
| RDR-002 | 同表有额外Object | 输出不包含额外Object |
| RDR-003 | workspace/in-memory insert | 输出不包含非source持久化row |
| RDR-004 | Snapshot前Tombstone | 删除行不输出，coverage完整 |
| RDR-005 | Snapshot后Tombstone | 旧Snapshot输出；final协议另行拦截 |
| RDR-006 | 0 visible完整Object | report complete且0合法 |
| RDR-007 | Object not found | error，complete=false |
| RDR-008 | block短读/checksum错 | error，不发布 |
| RDR-009 | callback同步copy | Batch复用后copy仍正确 |
| RDR-010 | callback错误 | 不再回调，Reader/Batch关闭一次 |
| RDR-011 | callback panic | recover、cleanup、Job失败 |
| RDR-012 | context cancel每个read点 | 无goroutine/Batch/mpool leak |
| RDR-013 | Batch rows/bytes边界 | 不超过hard，oversize单行受控 |
| RDR-014 | Merge+GC并发读 | 读成功完整或明确失败；失败不退休 |
| RDR-015 | Batch/file切分变化 | row count/root一致 |
| RDR-016 | NaN/-0/Decimal/timezone/NULL | canonical/readback一致 |
| RDR-017 | race detector 1000次 | 无data race/use-after-free |

### 3.3 Exit

- `go test -race`通过；
- fault injection所有path mpool/goroutine/file handle回到baseline；
- 3 GiB单Object streaming峰值内存不超过profile；
- 无完整性依赖sample/ETag。

## 4. P0-2 Mixed可写SI事务

### 4.1 要证明

```text
Reader Snapshot == Archive Snapshot == DELETE Snapshot
```

并证明正常DML并发不会把用户已删除/更新行发布到Archive后成功commit。

### 4.2 Case

| ID | 场景 | 通过条件 |
|---|---|---|
| MIX-001 | Txn显式SI | Snapshot从begin到commit不变 |
| MIX-002 | RC对照 | 测试能复现Snapshot变化并被实现拒绝 |
| MIX-003 | Snapshot Operator | Delete明确拒绝 |
| MIX-004 | single PK | RowID/key与普通DELETE一致 |
| MIX-005 | composite PK | 共享encoder，一致 |
| MIX-006 | fake PK | 可删除且不进Payload |
| MIX-007 | varlen key | actual bytes计入budget |
| MIX-008 | user DELETE after read | Lifecycle abort，不发布Dataset |
| MIX-009 | user UPDATE after read | Lifecycle abort/retry |
| MIX-010 | concurrent Merge transfer | success正确删除或明确conflict |
| MIX-011 | transfer page missing | fail closed |
| MIX-012 | Guard/schema/TRUNCATE | 整txn abort |
| MIX-013 | Provider verified后Delete失败 | Dataset不发布，Root cleanup |
| MIX-014 | commit response lost | 不重复Delete/insertDataset |
| MIX-015 | TTL no-op | 无Receipt/无DELETE |
| MIX-016 | Archive no selected rows | 无空Dataset/Root |

### 4.3 关键失败判定

若MIX-008/009显示现有`Relation.Delete`会对Snapshot后同RowID删除静默成功：

- P0失败；
- 必须增加普通row lock或commit validation；
- 不能靠“重复删除结果一样”接受，因为Archive会复活用户已删行。

## 5. P0-3 Strict Object Retire

### 5.1 Wire

| ID | 场景 | 通过条件 |
|---|---|---|
| STR-001 | V1 deterministic marshal | digest稳定 |
| STR-002 | old TN/unknown enum | 整txn unsupported abort |
| STR-003 | unknown protocol | abort |
| STR-004 | duplicate/reordered object | reject |
| STR-005 | 64 Object/512KiB边界 | limit严格 |
| STR-006 | `ErrTAENeedRetry` | 原payload/digest重放 |

### 5.2 Condition

| ID | 场景 | 通过条件 |
|---|---|---|
| STR-010 | exact object current | DropIntent提交 |
| STR-011 | Object missing | conflict，不吞ExpectedEOB |
| STR-012 | Merge替换 | stats/object conflict |
| STR-013 | prior DropIntent other txn | conflict |
| STR-014 | same txn same digest duplicate | no-op |
| STR-015 | same txn different digest | reject |
| STR-016 | schema version/digest变化 | abort |
| STR-017 | lifecycle max不再过期 | abort |
| STR-018 | null/unsupported ZoneMap | abort |
| STR-019 | Nth Object失败 | 前N-1全回滚 |

### 5.3 WAL/replay

| ID | 场景 | 通过条件 |
|---|---|---|
| STR-020 | 1PC | Receipt/DropAt原子 |
| STR-021 | 2PC participant失败 | 全abort |
| STR-022 | prepare后TN crash | replay无partial retire |
| STR-023 | WAL append前/后crash | existing Object Drop replay正确 |
| STR-024 | commit后response lost | status+Receipt收敛 |
| STR-025 | checkpoint/restart | Object不可见且GC正常 |
| STR-026 | ordinary Merge SoftDelete | ExpectedEOB宽松语义不变 |

## 6. P0-4 Whole Archive Tombstone与表锁

### 6.1 时序义务

必须证明：

```text
DML started before table lock
  -> finalizer waits
  -> DML committed Tombstone <= final_validation_ts
  -> TN delta finds it

DML started after table lock
  -> waits until final txn end
```

### 6.2 Tombstone case

| ID | 场景 | 通过条件 |
|---|---|---|
| TMB-001 | lower bound exact source TS | source TS Tombstone不算new |
| TMB-002 | source TS.Next | 找到 |
| TMB-003 | upper final TS | 找到 |
| TMB-004 | memory Tombstone | 找到 |
| TMB-005 | persisted Tombstone Object | 找到 |
| TMB-006 | unrelated Object Tombstone | 不误报 |
| TMB-007 | first match early exit | 不物化全Batch |
| TMB-008 | watermark未到 | NeedRetry，不阻塞 |
| TMB-009 | history low watermark越过source Snapshot | conflict/re-export，不提交 |
| TMB-009 | rows/bytes overflow | fail closed |
| TMB-010 | context deadline | 30s内退出 |

### 6.3 Lock conflict matrix

| 并发路径 | 必须验证 |
|---|---|
| INSERT | lock等待/最终新行不受当前source影响 |
| DELETE local/distributed | 锁或delta覆盖 |
| UPDATE | 锁或delta覆盖 |
| merge-delete | 锁或delta覆盖 |
| TRUNCATE/DROP/ALTER | DDL lock/Guard冲突 |
| background Merge | exact Object Drop conflict |
| internal mutation | 分类到lock/delta/conflict/准入拒绝 |

case IDs `LCK-001..LCK-020`按每条路径的before-lock/in-flight/after-lock三点生成。

### 6.4 Deadlock

用真实LockService：

- table -> Guard -> Binding -> Dataset -> TN顺序；
- DROP/DDL/DML反向依赖；
- remote lock owner CN crash；
- 30s timeout；
- unknown commit lock resolver。

运行wait-for graph断言无环；超时后无遗留table lock。

## 7. P0-5 Root-before-PUT与commit unknown

### 7.1 PUT crash matrix

| ID | crash点 | 通过条件 |
|---|---|---|
| ROOT-001 | Root前 | Provider 0副作用 |
| ROOT-002 | Root commit后 | Sweeper可清Root |
| ROOT-003 | Object row后 | 可清 |
| ROOT-004 | multipart create返回前 | prefix LIST收敛 |
| ROOT-005 | create返回/observer前 | LIST upload并abort |
| ROOT-006 | part中 | abort/delete |
| ROOT-007 | complete response lost | HEAD/readback判定 |
| ROOT-008 | Object row PUT_COMPLETE前后 | 不重复覆盖 |
| ROOT-009 | Manifest PUT/readback | 不发布 |
| ROOT-010 | stale writer迟到PUT | quiescence发现并删 |
| ROOT-011 | UPLOADING期间epoch接管 | new attempt/new prefix；旧PUT不能覆盖新Dataset |
| ROOT-012 | VERIFIED后finalizer接管 | 不再PUT，只允许finalize/原txn对账 |

### 7.2 final txn

| ID | 结果 | 通过条件 |
|---|---|---|
| REC-001 | committed + Receipt/Dataset | Root PUBLISHED |
| REC-002 | aborted + no Receipt | DELETE_PENDING |
| REC-003 | response lost/ACTIVE | FINALIZING |
| REC-004 | committed但apply水位未到 | WAIT，不报corrupt |
| REC-005 | committed水位到但Receipt缺 | kill switch/manual |
| REC-006 | aborted但Receipt有 | kill switch/manual |
| REC-007 | 24h unknown | manual且不清理 |
| REC-008 | owner DROP duringunknown | 仍对账原txn |
| REC-009 | duplicate Reconciler | CAS单一收敛 |
| REC-010 | restart | txn ID/digest完整恢复 |

### 7.3 Delete安全

| ID | 场景 | 通过条件 |
|---|---|---|
| CLN-001 | active Restore lease | 不能DELETING |
| CLN-002 | new lease race | access generation只胜一方 |
| CLN-003 | stale sweeper | 重复key Delete安全 |
| CLN-004 | Delete success但HEAD有 | 不CLEANED |
| CLN-005 | LIST lag | 两轮确认 |
| CLN-006 | credential revoke | DELETE_FAILED保留证据 |
| CLN-007 | manifest last | 诊断可用 |
| CLN-008 | late PUT after first clean | 重删并重启quiescence |

## 8. P0-6 Mixed预算

对每一hard budget自动生成：

```text
limit - 1
limit
limit + 1
```

case前缀 `BGT-*`：

- source bytes/rows；
- expired rows/ratio；
- affected blocks；
- actual delete key bytes；
- workspace/WAL estimate；
- Archive bytes；
- txn/provider duration；
- rolling table Tombstone；
- account/cluster backlog；
- snapshot age/retained bytes。

通过条件：

- `+1`在第一次Delete前blocked；
- source数据可见；
- staging有Root并可清；
- 不通过拆分无限小Job绕过rolling limit；
- Cleanup/Reconcile资源不被饿死。

## 9. P0-7 Restore

### 9.1 基本

| ID | 场景 | 通过条件 |
|---|---|---|
| RST-001 | 单Dataset | schema/rows/root一致 |
| RST-002 | 多Dataset | 有序set root |
| RST-003 | single/composite/fake PK | normal insert semantics |
| RST-004 | all supported types | round-trip |
| RST-004A | AUTO_INCREMENT explicit values | sequence水位>=max+1；后续INSERT不冲突 |
| RST-005 | target exists | 无覆盖 |
| RST-006 | hidden staging | 用户不可见/不可访问 |
| RST-007 | final rename | 原子可见 |

### 9.2 Chunk/restart

| ID | 场景 | 通过条件 |
|---|---|---|
| RST-010 | create table response lost | adopt exact staging |
| RST-011 | each row-group crash point | receipt驱动resume |
| RST-012 | chunk commit response lost | 不重复插 |
| RST-013 | receipt digest mismatch | 不发布 |
| RST-014 | worker epoch接管 | stale不能续租/写 |
| RST-015 | 1TiB restore | bounded txn/memory |

### 9.3 Purge/DROP

| ID | 场景 | 通过条件 |
|---|---|---|
| RST-020 | Purge vs lease | CAS只胜一方 |
| RST-021 | owner DROP loading | staging清理，不发布 |
| RST-022 | owner DROP publishing | owner CAS |
| RST-023 | lease expire | 停读/不发布 |
| RST-024 | final commit unknown | staging不误删 |

## 10. Feature Guard/Profile/Catalog Gate

虽不在七项编号内，以下仍是P0：

### 10.1 Guard首次创建竞态

两两并发：

```text
Lifecycle bind
CDC create
FK create
Publication create
Index/plugin create
Snapshot/Backup/Clone create
TRUNCATE/ALTER/DROP
```

每组循环10,000次，只允许一方提交或双方在兼容状态提交；不允许unsupported dependency + ACTIVE Binding。

### 10.2 Profile

- Stage后续修改不影响Profile；
- ADD VERSION只影响未来Binding，历史Dataset仍解析冻结version；
- namespace变化rotation拒绝；
- credential rotation same namespace；
- TLS/SSE/KMS identity与Manifest/Root/Dataset digest一致；
- 更换KMS identity必须新Profile version，禁止静默降级/改key；
- Profile被Root/Dataset引用不能drop；
- owner DROP后cleanup credential可删对象；
- secret不进Manifest/log。

### 10.3 Catalog

- 每个state transition合法/非法；
- CAS version/epoch；
- bootstrap/upgrade/restart；
- account incarnation复用；
- `mo_account` RowID变化产生新incarnation，旧进程不能懒创建新current；
- DROP后迟到旧tenant row按旧incarnation隔离并由quiescence cleanup；
- DROP ACCOUNT tombstone同txn；
- UNSET后无ACTIVE Binding的Dataset仍按purge_eligible_at清理；
- invariant checker故障自动kill switch。

## 11. Object Index/Scheduler Gate

case：

- W0 backfill + concurrentdelta；
- inclusive range边界；
- stale Checkpoint rebuild；
- crash cursor resume；
- old epoch不能READY；
- 1000 Binding不访问未绑定表；
- 100 million Index row容量模型；
- no global cursor/row hotspot；
- fair share；
- cleanup minimum workers；
- quota high/low watermark；
- Merge高频替换最终收敛。

Index全删重建不能影响活动表或导致错误retire。

## 12. Snapshot/Backup/DR fail-closed Gate

每个入口：

- bind时已有reference；
- Binding active后create；
- ordinary restore到Lifecycle source；
- DR target Restore Archive；
- owner DROP。

必须在操作开始前明确错误。任何“成功但历史行缺失”使GA No-Go。

## 13. Upgrade Gate

矩阵：

| CN | TN | cluster protocol | 预期 |
|---|---|---|---|
| old | old | old | 无Lifecycle retire |
| new | old | old | Index/Dry-run only |
| old+new | new | old | no Strict entry |
| new | new | old | no Strict until protocol advance |
| new | new | new | retire |
| old TN receives new entry | mixed fault | any | explicit abort |

流程：

1. Catalog upgrade；
2. new CN rollout；
3. new TN rollout；
4. query all protocol versions；
5. advance final protocol；
6. retirement enable；
7. rolling restart；
8. kill switch；
9. downgrade到支持Catalog但不retire版本；
10. 不允许降到破坏Catalog/replay的版本。

## 14. Provider Certification

每个宣称支持的 provider/profile独立运行：

- single/multipart PUT；
- CreateMultipart observer窗口；
- exact GET/full read；
- HEAD size；
- prefix LIST；
- multipart LIST/abort；
- Delete + HEAD/LIST收敛；
- credential rotation；
- endpoint/bucket/prefix canonicalization；
- transient 429/5xx/timeouts；
- response lost；
- minimum storage/early delete计费信息能被cost model录入。

仅通过MinIO模拟不能自动认证OSS/COS/其他S3-compatible provider。

## 15. 1 TiB / 10 TiB认证

### 15.1 数据

至少三种layout：

1. 时间严格有序，Whole占比>99%；
2. 1%～5%边界Mixed；
3. 高度乱序，大Mixed应blocked。

包含：

- fixed/varlen/Decimal/time/JSON/BLOB；
- single/composite/fake PK；
-持续INSERT；
-持续普通Merge；
-受控UPDATE/DELETE。

### 15.2 1 TiB

每种layout：

- actual active TAE data >= 1 TiB；
- Index backfill/catchup；
- TTL和Archive；
- full readback；
- full Restore；
- Purge；
- CN/TN restart和Provider fault。

### 15.3 10 TiB

单表actual active data >= 10 TiB：

- 全表Object Index backfill；
- 7天持续Merge/insert；
- 每天至少1 TiB due batch；
- 累计Archive/retire覆盖整表10 TiB旧窗口；
- 至少一次1 TiB连续Restore；
- 通过Manifest/Dataset分段验证10 TiB全部Published数据；
- GC/retained bytes、Root/Object count、Catalog rows、memory稳定；
- 不扫描集群未绑定表。

“10 TiB ObjectStats模拟”只能用于Planner基准，不能替代该认证。

## 16. Soak

Stage 4前：

```text
1000 Binding
30 days
mixed/whole/provider故障注入
weekly rolling restart
weekly Restore/Purge
continuous invariant checker
```

通过：

- P0 invariant failure = 0；
- unknown最终权威收敛率满足目标；
- Cleanup backlog无单调增长；
- Index obsolete/backlog有界；
- Tombstone/Merge/GC恢复到low watermark；
- goroutine/mpool/temp file无趋势泄漏；
- p99普通查询/Merge回归在批准阈值内。

## 17. 性能回归

未绑定表：

- SELECT/INSERT/UPDATE/DELETE；
- Merge throughput；
- Logtail apply；
- checkpoint/GC；
- CN memory。

要求Lifecycle feature关闭或无Binding时：

```text
普通热路径无Lifecycle Catalog lookup
无Object Index写
无新增Provider请求
性能差异在噪声/批准阈值内
```

绑定表正常DML允许Guard只进入DDL/依赖控制面，不进入每行DML。

## 18. 证据产物

每个release candidate保存：

```text
commit SHA/config/protocol version
Catalog schema version
test case manifest/result
chaos seed/timeline
provider/version/region
1/10 TiB data generator/schema
metrics/dashboard export
invariant checker report
cost report
Restore row/root report
Purge LIST/HEAD proof
known limitations
```

证据可重跑，不只保留截图。

## 19. GA Stop Ship

任一项立即停止：

- Archive-before-retire违反；
- Dataset/Receipt/retirement非原子；
- committed/aborted结果无法权威判定且资源被清；
- Restore重复/缺行/root mismatch；
- DROP丢失Root owner；
- stale runner覆盖/删除仍引用对象；
- old TN跳过unknown entry；
- 大Mixed绕过budget造成无界backlog；
- 未绑定表普通Merge/GC出现Lifecycle回归；
- support matrix静默返回不完整数据。

## 20. 最终签署

需要以下Owner签署：

- SQL/Frontend；
- DistTAE Reader/Txn；
- TAE Catalog/MVCC/GC；
- Txn/LockService；
- TaskService；
- FileService/provider；
- Backup/PITR/DR；
- QA/Chaos；
- SRE/Cloud；
- Product。

签署针对本文件的case和证据，不是笼统“方案可行”。
