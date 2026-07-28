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
全部P0和GA case通过
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
| Package integration | disttae Reader/Delete/Rewrite、TAE Lifecycle commit/replay、Catalog txn |
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

## 5. P0-3 `OpCommitLifecycle`、WAL 与 Replay

### 5.1 Wire

| ID | 场景 | 通过条件 |
|---|---|---|
| WIR-001 | V1 deterministic marshal | digest稳定 |
| WIR-002 | old TN/unknown opcode | 整txn unsupported abort |
| WIR-003 | unknown version/mode/field | abort |
| WIR-004 | duplicate/reordered source Object | reject |
| WIR-005 | entry Object/bytes边界 | limit严格 |
| WIR-006 | `ErrTAENeedRetry` | 原payload/digest重放 |
| WIR-007 | old TN看到new protobuf字段 | 不可能降级为普通Merge提交 |
| WIR-008 | Dataset/Receipt缺少entry | finalizer API拒绝commit |

### 5.2 Condition

| ID | 场景 | 通过条件 |
|---|---|---|
| CMT-010 | Whole exact Object current | DropIntent提交 |
| CMT-011 | Object missing | conflict，不吞ExpectedEOB |
| CMT-012 | Merge替换 | stats/object conflict |
| CMT-013 | prior DropIntent other txn | conflict |
| CMT-014 | same txn same digest duplicate | no-op |
| CMT-015 | same txn different digest | reject |
| CMT-016 | schema/Guard/Binding变化 | abort |
| CMT-017 | Whole lifecycle max不再过期 | abort |
| CMT-018 | Rewrite created Object/digest不符 | abort |
| CMT-019 | Nth source/created Object失败 | 全部回滚 |

### 5.3 WAL/replay

| ID | 场景 | 通过条件 |
|---|---|---|
| WAL-020 | 1PC | Receipt/source/live Object原子 |
| WAL-021 | 2PC participant失败 | 全abort |
| WAL-022 | prepare后TN crash | replay无partial retire/publish |
| WAL-023 | WAL append前/后crash | source Drop/live create/transfer一致 |
| WAL-024 | commit后response lost | status+Receipt收敛 |
| WAL-025 | checkpoint/restart | Object可见性和GC正常 |
| WAL-026 | ordinary Merge SoftDelete | 原 opcode/语义不变 |

## 6. P0-4 Source Reservation、GC Protection 与并发 Tombstone

### 6.1 Reservation 线性化

| ID | 场景 | 通过条件 |
|---|---|---|
| RSV-001 | Lifecycle先reserve，scheduler Merge | scheduler跳过 |
| RSV-002 | Lifecycle先reserve，CN/用户强制Merge | final admission拒绝 |
| RSV-003 | Merge先进入final admission | Lifecycle claim失败并replan |
| RSV-004 | 两个Lifecycle attempt | 只有一个token有效 |
| RSV-005 | lease renew/expire | stale token不能Prepare |
| RSV-006 | TN restart丢失reservation | final fail closed，不猜测恢复 |
| RSV-007 | source已被Merge替换 | exact CAS conflict |
| RSV-008 | unrelated Object Merge | 不互相阻塞 |
| RSV-009 | Merge check通过、DropIntent未安装 | merge admission ticket阻止Lifecycle Acquire |
| RSV-010 | Merge安装DropIntent后ticket释放 | Lifecycle由Object MVCC拒绝 |
| RSV-011 | Merge handler error/panic | ticket exactly-once释放 |
| RSV-012 | 多表/多Object压力 | 无单全局mutex热点 |

Scheduler skip 只是性能优化；`OpCommitMerge`和`OpCommitLifecycle`的TN最终准入才是
安全边界。

### 6.2 GC SyncProtection

| ID | 场景 | 通过条件 |
|---|---|---|
| GCP-001 | exact data+tombstone filenames注册 | GC不删除 |
| GCP-002 | GC cycle运行中注册 | 返回retry，Job不读source |
| GCP-003 | renew成功 | 长build继续 |
| GCP-004 | renew失败/过期 | final Prepare失败，staging清理 |
| GCP-005 | TN restart丢保护 | final abort/replan |
| GCP-006 | source Merge后DropIntent | protection期间物理文件仍可读 |
| GCP-007 | final committed后release | existing GC最终回收旧source |
| GCP-008 | commit unknown | protection/Root不因Job deadline误清 |
| GCP-009 | live Object写出但未final | orphan GC不能删除 |
| GCP-010 | external booking未final | duplicate Prepare前GC不能删除 |
| GCP-011 | writer越过冻结range | 下一物理写前失败 |

### 6.3 Tombstone 与 transfer

| ID | 场景 | 通过条件 |
|---|---|---|
| TMB-001 | source Snapshot前Tombstone | 归入snapshot-deleted/NoTransfer |
| TMB-002 | build中删除survivor | phase-1/2 transfer到new RowID |
| TMB-003 | build中删除expired row | NoTransfer产生正常冲突，Lifecycle abort |
| TMB-004 | memory Tombstone | 被增量检查覆盖 |
| TMB-005 | persisted Tombstone Object | 被增量检查覆盖 |
| TMB-006 | unrelated Object Tombstone | 不误报 |
| TMB-007 | delta watermark未到 | NeedRetry，不阻塞goroutine |
| TMB-008 | transfer page过期/缺失 | fail closed |
| TMB-009 | delta rows/bytes超限 | abort/replan，不退休 |
| TMB-010 | context deadline | 有界退出并保留正确Owner |

## 7. P0-5 Root-before-side-effect 与 commit unknown

### 7.1 Archive/TAE staging crash matrix

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
| ROOT-013 | live TAE staging write前/后crash | Root可枚举并删除未发布Object |
| ROOT-014 | transfer booking write前/后crash | Root持有，unknown不删 |
| ROOT-015 | TTL Rewrite committed | 先`POST_COMMIT_CLEANUP`，booking删除后才`TRANSFERRED` |
| ROOT-016 | Archive Rewrite committed | live child标`TAE_OWNED`，payload仍由Root/Dataset拥有 |
| ROOT-017 | segment range预注册 | range Root child committed后才出现FileService write |
| ROOT-018 | Sync后、exact child前crash | Sweeper枚举有限range并找到对象 |
| ROOT-019 | 实际writer触达硬上限 | 下一Object写前停止，无unowned file |
| ROOT-020 | object name不确定 | P0失败，必须实现before-create hook |
| ROOT-021 | duplicate Prepare/NeedRetry | immutable booking可重复读取，Prepare不删除 |
| ROOT-022 | Archive Rewrite committed | 只删booking child，不改变PUBLISHED或删除Payload |

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

## 8. P0-6 Mixed Rewrite 行守恒、Transfer 与清理

### 8.1 单次 split

| ID | 场景 | 通过条件 |
|---|---|---|
| RWT-001 | 0 expired | no-op，不产生空live替换 |
| RWT-002 | 0 live | 自动退化为Whole退休 |
| RWT-003 | 1 live row | 新Object/transfer正确 |
| RWT-004 | max legal Object | streaming有界 |
| RWT-005 | 多Object reshape | 新Object target和ordinal合法 |
| RWT-006 | Archive sink失败 | live staging/booking清理，source不退 |
| RWT-007 | live writer失败 | Archive staging清理，source不退 |
| RWT-008 | callback/release失败 | 无double release/leak |
| RWT-009 | 多source callback交错 | Archive substream/root仍按source/block/row canonical |
| RWT-009A | Batch/file切分变化 | row count/root不变 |
| RWT-009B | spill中CN crash | 新boot janitor清旧目录，active boot不被删 |

必须逐行证明：

```text
source_physical = snapshot_deleted + expired_visible + live_visible
source_visible  = expired_visible + live_visible
archive_rows    = expired_visible        # Archive
new_live_rows   = live_visible
```

### 8.2 Transfer 和原子发布

| ID | 场景 | 通过条件 |
|---|---|---|
| RWT-010 | every survivor | 恰好一个有效destination |
| RWT-011 | snapshot-deleted | `api.NoTransfer` |
| RWT-012 | expired | `api.NoTransfer` |
| RWT-013 | output Object ordinal 0/254 | 合法 |
| RWT-014 | output Object ordinal 255 | admission拒绝/重plan |
| RWT-015 | create live Nth失败 | source Drop全部回滚 |
| RWT-016 | source Drop Nth失败 | live create全部回滚 |
| RWT-017 | transfer phase-2失败 | final transaction abort |
| RWT-018 | response lost/replay | 不重复create/drop/transfer |

### 8.3 普通 Merge 无回归

- 未绑定表完全不进入 Lifecycle discovery/reservation；
- 无 reservation 时普通 Merge 的选择、write、transfer、WAL 和 GC 字节级/语义级
  回归；
- Lifecycle 只复用 Merge 原语，不调用普通 Merge scheduler；
- 普通 Merge 不执行 Archive Provider I/O；
- feature关闭时不创建 Lifecycle Root/booking。

## 9. P0-7 小 Mixed 与 Rewrite 资源预算

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

- 小 Mixed `+1` 在第一次 Delete 前回滚并切换 Rewrite；
- Rewrite `+1` 在 final transaction 前进入 `RESOURCE_BLOCKED`；
- source数据可见；
- staging有Root并可清；
- 不通过拆分无限小Job绕过rolling limit；
- Cleanup/Reconcile资源不被饿死。

## 10. P0-8 Restore

### 10.1 基本

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

### 10.2 Chunk/restart

| ID | 场景 | 通过条件 |
|---|---|---|
| RST-010 | create table response lost | adopt exact staging |
| RST-011 | each row-group crash point | receipt驱动resume |
| RST-012 | chunk commit response lost | 不重复插 |
| RST-013 | receipt digest mismatch | 不发布 |
| RST-014 | worker epoch接管 | stale不能续租/写 |
| RST-015 | 1TiB restore | bounded txn/memory |

### 10.3 Purge/DROP

| ID | 场景 | 通过条件 |
|---|---|---|
| RST-020 | Purge vs lease | CAS只胜一方 |
| RST-021 | owner DROP loading | staging清理，不发布 |
| RST-022 | owner DROP publishing | owner CAS |
| RST-023 | lease expire | 停读/不发布 |
| RST-024 | final commit unknown | staging不误删 |

## 11. Feature Guard/Profile/Catalog Gate

虽不在八项编号内，以下基础能力仍是P0：

### 11.1 Guard首次创建竞态

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

### 11.2 Profile

- Stage后续修改不影响Profile；
- ADD VERSION只影响未来Binding，历史Dataset仍解析冻结version；
- namespace变化rotation拒绝；
- credential rotation same namespace；
- TLS/SSE/KMS identity与Manifest/Root/Dataset digest一致；
- 更换KMS identity必须新Profile version，禁止静默降级/改key；
- Profile被Root/Dataset引用不能drop；
- owner DROP后cleanup credential可删对象；
- secret不进Manifest/log。

### 11.3 Catalog

- 每个state transition合法/非法；
- CAS version/epoch；
- bootstrap/upgrade/restart；
- account incarnation复用；
- `mo_account` RowID变化产生新incarnation，旧进程不能懒创建新current；
- DROP后迟到旧tenant row按旧incarnation隔离并由quiescence cleanup；
- DROP ACCOUNT tombstone同txn；
- UNSET后无ACTIVE Binding的Dataset仍按purge_eligible_at清理；
- invariant checker故障自动kill switch。

## 12. Discovery/Scheduler Gate

case：

- 固定Snapshot分页和Object ID inclusive/exclusive边界；
- page commit前/后crash cursor resume；
- Merge在cursor前生成new Object，下一full cycle最终发现；
- stale Candidate revalidation失败但不误退休；
- `CollectObjectList` watermark断档后回退full cycle；
- 1000 Binding不访问未绑定表；
- 百万Object不生成百万Catalog row；
- 每Binding O(1) scan state和有界Candidate；
- no global cursor/row hotspot，单大表不能垄断Scanner；
- fair share；
- cleanup minimum workers；
- quota high/low watermark；
- Merge高频替换最终收敛。

删除全部 Candidate/可选 Summary 后重建不能影响活动表或导致错误 retire。

## 13. Snapshot/Backup/DR fail-closed Gate

每个入口：

- bind时已有reference；
- Binding active后create；
- ordinary restore到Lifecycle source；
- DR target Restore Archive；
- owner DROP。

必须在操作开始前明确错误。任何“成功但历史行缺失”使GA No-Go。

## 14. Upgrade Gate

矩阵：

| CN | TN | cluster protocol | 预期 |
|---|---|---|---|
| old | old | old | 无Lifecycle retire |
| new | old | old | Discovery/Dry-run only |
| old+new | new | old | no `OpCommitLifecycle` |
| new | new | old | no retire until protocol advance |
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

## 15. Provider Certification

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

## 16. 1 TiB / 10 TiB认证

### 16.1 数据

至少三种layout：

1. 时间严格有序，Whole占比>99%；
2. 1%～5%边界Mixed；
3. 高度乱序，大 Mixed 必须走 Rewrite；超过已认证放大预算才 blocked。

包含：

- fixed/varlen/Decimal/time/JSON/BLOB；
- single/composite/fake PK；
-持续INSERT；
-持续普通Merge；
-受控UPDATE/DELETE。

### 16.2 1 TiB

每种layout：

- actual active TAE data >= 1 TiB；
- Discovery full cycle/增量加速；
- TTL和Archive；
- full readback；
- full Restore；
- Purge；
- CN/TN restart和Provider fault。

### 16.3 10 TiB

单表actual active data >= 10 TiB：

- 百万当前 Object 的分页 Discovery full cycle；
- 7天持续Merge/insert；
- 每天至少1 TiB due batch；
- 累计Archive/retire覆盖整表10 TiB旧窗口；
- 至少一次1 TiB连续Restore；
- 通过Manifest/Dataset分段验证10 TiB全部Published数据；
- GC/retained bytes、Root/Object count、Catalog rows、memory稳定；
- 不扫描集群未绑定表。

“10 TiB ObjectStats模拟”只能用于Planner基准，不能替代该认证。

## 17. Soak

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
- Candidate/Summary/backlog有界；
- Tombstone/Merge/GC恢复到low watermark；
- goroutine/mpool/temp file无趋势泄漏；
- p99普通查询/Merge回归在批准阈值内。

## 18. 性能回归

未绑定表：

- SELECT/INSERT/UPDATE/DELETE；
- Merge throughput；
- Logtail apply；
- checkpoint/GC；
- CN memory。

要求Lifecycle feature关闭或无Binding时：

```text
普通热路径无Lifecycle Catalog lookup
无逐Object Lifecycle Catalog写
无新增Provider请求
性能差异在噪声/批准阈值内
```

绑定表正常DML允许Guard只进入DDL/依赖控制面，不进入每行DML。

## 19. 证据产物

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

## 20. GA Stop Ship

任一项立即停止：

- Archive-before-retire违反；
- Dataset/Receipt/retirement非原子；
- committed/aborted结果无法权威判定且资源被清；
- Restore重复/缺行/root mismatch；
- DROP丢失Root owner；
- stale runner覆盖/删除仍引用对象；
- old TN跳过unknown entry；
- Mixed Rewrite绕过resource/amplification budget造成无界backlog；
- Rewrite row-conservation或transfer invariant失败；
- protection失效后仍退休source Object；
- 未绑定表普通Merge/GC出现Lifecycle回归；
- support matrix静默返回不完整数据。

## 21. 最终签署

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
