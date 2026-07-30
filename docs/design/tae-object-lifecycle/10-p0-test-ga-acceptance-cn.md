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
| RDR-013 | split-capable Batch与block-based Reader | 前者不超过hard；后者按读取前认证门禁 |
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

## 5. P0-3 Tagged Lifecycle Entry、WAL 与 Replay

### 5.1 Wire

| ID | 场景 | 通过条件 |
|---|---|---|
| WIR-001 | V1 deterministic marshal | digest稳定 |
| WIR-002 | old TN/unsupported tag capability | CN不发送退休；整txn unsupported abort |
| WIR-003 | unknown version/mode/field | abort |
| WIR-004 | duplicate/reordered source Object | reject |
| WIR-005 | entry Object/bytes边界 | limit严格 |
| WIR-006 | `ErrTAENeedRetry` G1 -> G2 | 原payload/digest重放；G2重建私有entry |
| WIR-007 | old TN或route identity变化 | capability与ServiceID/ShardID/ReplicaID fence在发送前禁止retire |
| WIR-008 | Dataset/Receipt缺少entry | finalizer API拒绝commit |
| WIR-009 | Rewrite source基数0/1/2 | 只有1通过 |
| WIR-010 | Rewrite inline transfer非空 | reject |
| WIR-011 | external booking identity/layout | exact key/version/SHA/digest全部匹配 |
| WIR-012 | 路由到非source table TN shard | 发送前拒绝/重新解析路由 |
| WIR-013 | protection job有效但set digest错误 | Prepare拒绝 |
| WIR-014 | Mixed entry的expired/live任一为0 | TN拒绝；CN必须先no-op或切Whole |
| WIR-015 | Whole N个source、N个同序layout proof | 1～64全部逐项验证 |
| WIR-016 | proof缺失/多余/重排/Object ID或Stats错绑 | Prepare拒绝 |
| WIR-017 | Rewrite layout proof基数0/2 | side effect前拒绝 |
| WIR-018 | Booking V1 unknown version/flag/trailing bytes | Prepare拒绝 |
| WIR-019 | Booking actual rows/全D-E block/尾部slot错误 | Prepare拒绝 |
| WIR-020 | Booking Root/kind ordinal/TAE namespace错绑 | Prepare拒绝 |
| WIR-021 | canonical payload digest或完整文件SHA错误 | Prepare拒绝 |
| WIR-022 | Rewrite token退化同类Whole/Empty | 允许且仍满足基数/child矩阵 |
| WIR-023 | Whole token升级Rewrite或TTL/Archive互换 | Prepare拒绝 |
| WIR-024 | G2复用G1 entry/Catalog node/TransferTable指针 | 测试必须检测并失败 |
| WIR-025 | retry/restart | 绝对deadline、generation和累计预算不续期 |
| WIR-026 | concurrent duplicate decode | mutation前仅一个BUILDING owner；follower不执行I/O/mutation |
| WIR-027 | Dataset/Receipt与Lifecycle tag任一缺失 | 整个事务拒绝提交 |
| WIR-028 | phase1/page install/LogTxnEntry逐点失败 | 无slab/page/TransferDels残留 |
| WIR-029 | 跨booking page重复destination/缺口 | 全局bitmap拒绝 |
| WIR-030 | digest golden vector | CN/TN/离线codec四种digest逐字节一致 |
| WIR-031 | slab 4/16 MiB边界与非默认schema Extra | 按allocator容量计费或读取前拒绝 |
| WIR-032 | Root deadline D与commit request不一致/触发fallback | 发送前拒绝，不获得新deadline |
| WIR-033 | `bat=nil` tag混入普通txn.writes | 测试先证明会被过滤；正式API禁止该路径 |
| WIR-034 | control穿过dump/compact/merge/sort | payload bytes/digest逐字节不变 |
| WIR-035a | 底层append helper：ordinary Entry为空、control非空 | 仍能编码tag，证明`bat=nil`不被静默过滤 |
| WIR-035b | production finalizer：pair缺失、空Entry或只有无关ordinary write | `ErrLifecycleCatalogPairMissing`，不得发送请求 |
| WIR-036 | control same/different digest重复设置 | same幂等；different发送前拒绝 |
| WIR-037 | ServiceID/ShardID/ReplicaID/capability变化、多候选shard或refresh失败 | 发送前拒绝/replan |
| WIR-038 | G1/G2 replay budget | 同一HandleCommit Owner累计，不按generation重置 |
| WIR-039 | generation owner在SoftDelete/Create前竞争 | 恰好一个BUILDING owner |
| WIR-040 | builder部分mutation失败 | slot FAILED且整代rollback；同代禁止重建 |
| WIR-041 | overlapping duplicate Commit | TxnService串行；恰好一个storage HandleCommit执行 |
| WIR-042 | terminal后迟到duplicate | deadline拒绝，或source preflight后进入RECONCILE_REQUIRED；不读Booking |
| WIR-043 | SEALED事务外部mutation | POISONED；只能full rollback，Commit不得发送请求 |
| WIR-044 | COMMITTING内部workspace路径 | 专用internal helper可完成merge/dump/transfer，不接受外部mutation |
| WIR-045 | Lifecycle commit admission饱和/超时/cancel/G1->G2 retry | 一次HandleCommit恰好一次获取/释放；hard cap不被retry绕过，无Booking I/O泄漏或永久等待 |
| WIR-046 | COMMITTING中route refresh/编码失败或response lost | 前者POISONED+full rollback；后者unknown finalize且Root资产不删 |

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
| CMT-020 | 任一`SourceLayoutProof`数量/顺序/内容变化 | abort，不二次业务值扫描 |
| CMT-021 | output level错误/降级 | abort |

### 5.3 WAL/replay

| ID | 场景 | 通过条件 |
|---|---|---|
| WAL-020 | 1PC | Receipt/source/live Object原子 |
| WAL-021 | 2PC participant失败 | 全abort |
| WAL-022 | prepare后TN crash | replay无partial retire/publish |
| WAL-023 | WAL append前/后crash | source Drop/live create/Catalog状态一致；不声称Replay运行时transfer page |
| WAL-024 | commit后response lost | status+Receipt收敛 |
| WAL-025 | checkpoint/restart | Object可见性和GC正常 |
| WAL-026 | ordinary Merge SoftDelete | 原 opcode/语义不变 |
| WAL-027 | 大Archive Payload | Parquet/Manifest bytes不进入TAE WAL |
| WAL-028 | commit前/后crash | 前者source保留+Root清理，后者Replay恢复Drop/live |
| WAL-029 | TN restart丢失已提交运行时transfer page | 旧RowID事务RW/WW conflict，不静默成功 |
| WAL-030 | duplicate Prepare/NeedRetry | 由immutable Booking V1重建当前final txn page |
| WAL-031 | final commit明确失败 | source始终可见，Dataset和new live Object都不可发布 |
| WAL-032 | final commit成功的一致性读 | source与new live Object不允许双重可见或同时不可见 |

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

Scheduler skip只是性能优化；普通`OpCommitMerge`和tagged Lifecycle entry的TN最终准入才是
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
| GCP-012 | 传入另一个有效job ID | expected filename-set digest不匹配，Prepare拒绝 |

### 6.3 Tombstone 与 transfer

| ID | 场景 | 通过条件 |
|---|---|---|
| TMB-001 | source Snapshot前Tombstone | 归入snapshot-deleted/NoTransfer |
| TMB-002 | build中删除survivor | phase-1/2 transfer到new RowID |
| TMB-002A | DELETE提交于`(S, finalTxn.StartTS]` | 必须从S收集并transfer；不得因final txn较晚创建而漏掉 |
| TMB-003A | build中删除Archive expired row | NoTransfer使Lifecycle abort/re-export |
| TMB-003B | build中删除TTL expired row | NoTransfer使Lifecycle abort/rebuild |
| TMB-003C | post-S删除命中snapshot-deleted/nil Block map | final txn abort，不按普通Merge规则skip |
| TMB-004 | memory Tombstone | 被增量检查覆盖 |
| TMB-005 | persisted Tombstone Object | 被增量检查覆盖 |
| TMB-006 | unrelated Object Tombstone | 不误报 |
| TMB-007 | delta watermark未到 | deadline内NeedRetry/等待，不永久阻塞goroutine |
| TMB-008 | transfer page过期/缺失 | fail closed |
| TMB-009 | delta rows/bytes/blocks limit±1 | 超限abort/blocked，不退休 |
| TMB-010 | phase-1/phase-2 context deadline | 有界退出并保留正确Owner |
| TMB-011 | phase-2 scan返回error | error不被吞，final txn abort |
| TMB-012 | Whole Archive/TTL post-S delete | 独立validator发现并abort/rebuild |
| TMB-013 | Whole Archive/TTL delta超限 | fail closed，不发布Dataset/不退休source |
| TMB-014 | 多Whole source分别未超、合计超限 | 按final transaction聚合后abort |
| TMB-015 | 用户先读旧RowID，Whole Prepare/commit后DELETE | commit冲突，不能静默成功 |
| TMB-016 | TMB-015中Lifecycle commit后TN restart | 仍冲突，不依赖WAL恢复page |
| TMB-017 | 用户DELETE先Prepare，Whole Archive后Prepare | validator发现并abort/re-export |
| TMB-018 | duplicate Write/Prepare | 复用原entry的absolute deadline和delta预算，不能重新获得60s/全额预算 |
| TMB-019 | client在Prepare后断连 | 不任意取消已Prepare事务，但TN内部deadline保证有界终止 |

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
| ROOT-023 | NeedRetry触发PrepareRollback | live staging物理文件仍存在 |
| ROOT-024 | retry最终commit | Catalog指向的每个created Object可全读 |
| ROOT-025 | final明确aborted | 只有Root Sweeper删除live/booking |
| ROOT-026 | final unknown | Merge entry与Root均不删除物理staging |
| ROOT-027 | Archive Rewrite双namespace | Payload走archive identity，live/booking走TAE identity |
| ROOT-028 | Root Object kind/namespace错配 | side effect前拒绝并触发invariant告警 |
| ROOT-029 | visible=0 | 不创建Root/child，不发布空Dataset |
| ROOT-030 | Rewrite首次E行、尚无L行时crash | 仅Archive child可枚举清理，无TAE range/booking |
| ROOT-031 | 首次L行前后crash | FileService write前range child已commit |
| ROOT-032 | live=0退化Whole | 无TAE range/live/booking，Whole正常提交 |
| ROOT-033 | expired=0且已写live staging | no-op；Root清理，不转TAE_OWNED |
| ROOT-034 | Root Object不同kind都用ordinal 0 | composite主键无冲突、分页稳定 |

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

本节的“Review P0-1～P0-6”是 Object Rewrite评审发现的六个子证明义务，不重新编号
本文件顶层P0 Gate；六项必须全部关闭。

### 8.1 单次 split

| ID | 场景 | 通过条件 |
|---|---|---|
| RWT-001 | 0 expired、live>0 | no-op，不产生空live替换，已写staging由Root清理 |
| RWT-002 | 0 live、expired>0 | final mode切为Whole；Archive/TTL执行独立post-S delete validator |
| RWT-002A | source visible为0 | EMPTY_ARCHIVE/Whole退休空Object，不创建Dataset |
| RWT-003 | 1 live row | 新Object/transfer正确 |
| RWT-004 | max legal Object | streaming有界 |
| RWT-005 | 单source reshape成多输出Object | target和ordinal合法 |
| RWT-006 | Archive sink失败 | live staging/booking清理，source不退 |
| RWT-007 | live writer失败 | Archive staging清理，source不退 |
| RWT-008 | callback/release失败 | 无double release/leak |
| RWT-009 | 输入第二个source Object | 首次Root/FileService/Provider副作用前拒绝 |
| RWT-009A | Batch/file切分变化 | row count/root不变 |
| RWT-009B | host尝试乱序block | 显式block ordinal驱动保持canonical，不创建排序spill |

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
| RWT-018 | response lost/replay | 不重复create/drop；当前final txn可从booking重建page |
| RWT-019 | table comment关闭普通transfer | Lifecycle仍为live行生成destination |
| RWT-020 | Archive expired post-S delete | abort且不修改已写Archive |
| RWT-021 | TTL expired post-S delete | 与Archive统一abort/rebuild |
| RWT-022 | post-S delete命中nil map/NoTransfer/越界 | typed error使整个final transaction abort |
| RWT-023 | CreatedObjs被重排 | `created_layout_digest`不匹配，Prepare拒绝 |
| RWT-024 | producer后注入mapping修改/重排 | `transfer_mapping_digest`不匹配，Prepare拒绝 |
| RWT-025 | CN booking success/error/cancel | producer slab都exactly-once Release |
| RWT-026 | duplicate Prepare并发解码 | 仅一个注册；失败者私有TransferTable释放，无共享修改或double Release |

### 8.3 Review P0-1：Root-owned staging

- 普通Merge使用`CreatedObjectOwnedByMergeEntry`，rollback仍删除created files；
- Lifecycle固定`CreatedObjectOwnedByCleanupRoot`；
- 对Write/Prepare/NeedRetry/rollback/commit response每个crash点验证物理文件Owner；
- committed后live child才转`TAE_OWNED`；
- aborted后才`DELETE_PENDING`；unknown保持`VERIFIED`。

### 8.4 Review P0-2：有界 Tombstone delta

- phase 1/2分别注入rows、bytes、blocks和deadline的limit-1/limit/limit+1；
- `WaitTombstoneObjectCommitted`可取消；
- scan/visitor/transfer page任一错误向上返回；
- 序列化总command始终小于70 MiB WAL上限并保留认证安全余量；
- 超限进入`CONFLICT_BLOCKED/RESOURCE_BLOCKED`，不无限retry。

### 8.5 Review P0-3：survivor-only transfer

- `DoTransfer()`固定true，不读普通Merge comment；
- D/E为NoTransfer，L恰好一个destination；
- Archive/TTL E及D槽位的并发DELETE都导致abort/rebuild；
- L的并发DELETE/UPDATE转到new RowID；
- Whole Archive/TTL独立post-S delete validator。

### 8.6 Review P0-4：external booking only

- Lifecycle inline字段非空时TN拒绝；
- entry只有按ordinal排列的exact booking key/version/size/SHA/layout digest；
- Booking V1只覆盖actual physical rows，不覆盖BlockMaxRows尾部；
- 零mapping Block仍有record，未出现的source slot统一为`NoTransfer`；
- mapping按source offset严格升序，`created_layout_digest`冻结producer顺序和布局；
- destination使用`uint8 ObjIdx + uint16 BlkIdx + uint32 RowIdx`且无padding；最大
  2,097,152-row全live Object连同envelope必须低于32 MiB认证上限；
- magic/version/flags/endian/字段顺序/length/digest范围按05固定；
- Root ID、kind-local ordinal和TAE namespace digest必须与Root child一致；
- duplicate Prepare和NeedRetry可重复读取同一immutable booking；
- booking被篡改、短读、错version、缺Block或
  `created_layout_digest/transfer_mapping_digest`错误均fail closed；
- producer TransferTable编码再解码后每个mapping和`CreatedObjs`顺序完全一致；
- committed/aborted前booking不会被任何handler删除。

### 8.7 Review P0-5：单源与资源峰值

- `MIXED_REWRITE_* source_objects != 1`均拒绝；
- admission按physical block slots，不按live rows；
- 当前最大Object且几乎全部live时测CN slab、TN copy、detached page和serialization峰值；
- Rewrite cluster并发1/2/4认证，默认1；
- mpool分配失败返回error，不触发进程panic，Root/source正确收敛。

### 8.8 Review P0-6：TN信任边界

- TN按source Object同序重算exact ObjectStats和全部`SourceLayoutProof`；
- TN从Object metadata验证物理行数，校验CN计数等式、created rows、mapping
  bounds/count/digest和`CreatedObjs`顺序；
- 整个entry共享的全局destination bitmap必须拒绝跨page重复destination和created row
  缺口，但不声称重新证明
  source row的TTL分类或destination业务语义；
- TN不读取TTL业务值或Provider Parquet；
- CN classifier做属性测试、mutation test和1/10 TiB源/Archive/live对账；
- `DoMergeAndWrite` producer属性测试覆盖source mapping唯一性和writer输出顺序；
- Booking编解码round-trip必须保持TransferTable逐项相同；
- 伪造count、source offset、destination、created order、layout/digest的每一种组合
  均被TN拒绝。

### 8.9 Block读取和统一内存令牌

- Object metadata column extent估算做checked uint64运算；
- 估算未知、溢出及`max_certified_block_read_bytes`的limit-1/limit/limit+1；
- task parent token在`DoMergeAndWrite`和dense slab分配前取得；
- Block source/decode token在`BlockDataReadNoCopy`前取得并随borrowed Batch释放；
- success/error/cancel/panic下父/子token均exactly once释放；
- 并发分配者消耗mpool余量时token原子reserve只允许预算内任务进入，不做
  `Available()` check/use；
- token不足时没有Provider/FileService payload read、Root外物理副作用或mpool panic；
- 3 GiB Object由多个认证Block组成时streaming通过；
- 未认证oversize Block/varlen row进入
  `LIFECYCLE_OVERSIZE_UNSUPPORTED/RESOURCE_BLOCKED`，
  source保持可见。

### 8.10 Review P0-7：可重放 tagged entry

- Dataset/Receipt普通Catalog写和Lifecycle control来自同一个finalizer/commit payload；
- G1 NeedRetry后G2完整重放tag，不能只提交Catalog DML；
- 每代从immutable Booking重建私有entry/Catalog node/TransferTable/runtime page；
- HandleCommit-local replay context不保存或返回旧generation可变指针；
- `TxnCommitRequest.DeadlineUnixNano`、最大generation和累计I/O/CPU/delta预算不续期；
- same external txn/different digest、tag和Catalog写任一缺失均整体abort。

### 8.11 Review P0-8：注册前 runtime Owner

- builder -> txn entry只在`txn.LogTxnEntry`成功后转移Owner；
- 故障注入冻结`LogTxnEntry error=append前、nil=append后`，不增加receipt/CAS；
- phase1、page install、LogTxnEntry前/中/后逐点注入失败；
- 注册前释放slab/page/TransferDels/decoder buffer，注册后由entry exactly-once释放；
- SoftDelete/Create成功后的Catalog node只由整个txn rollback，builder不局部清理；
- 所有错误、rollback和NeedRetry都不能删除Root-owned live/booking文件；
- Lifecycle local builder独立通过矩阵；普通Merge #26445单独修复，不作为Gate C前置。

### 8.12 Review P0-9：CN commit-control

- `Transaction.lifecycleCommit`是单值、深拷贝、immutable；
- 不进入普通workspace size、statement offset、dump/compact/sort、PK dedup、Batch GC；
- `genWriteReqs`在普通Entry之后直接追加，不调用`toPBEntry`；
- 空ordinary Entry只允许私有编码helper单测；Archive必须有Dataset+Receipt，TTL必须有
  Receipt，production缺pair统一`ErrLifecycleCatalogPairMissing`并禁止发请求；
- same digest/bytes幂等，different digest/第二条control拒绝；
- route只冻结ServiceID/ShardID/ReplicaID/protocol version；Address发送前权威重解析，route
  refresh不持有txn.Lock，多候选shard fail closed；
- `OPEN -> SEALED -> COMMITTING -> TERMINAL`，外部mutation转`POISONED`并只能full
  rollback；内部commit helper不走外部mutation检查；
- rollback/finalize只释放CN bytes，不删除Root/provider对象。

### 8.13 Review P0-10：generation-local slot

- slot位于internal txn ephemeral TxnMemo，不进入WAL/Memo序列化；
- wire无副作用校验后、Booking decode和SoftDelete/Create前claim；
- BUILDING follower有界等待/返回，REGISTERED/FAILED返回同一逻辑结果；
- owner在每个Catalog mutation和runtime安装点失败都使整代rollback；
- FAILED不能回到NEW；G2使用新TxnMemo、新slot和新runtime对象；
- 只有slot owner消费HandleCommit replay budget并执行I/O。

### 8.14 普通 Merge 无回归

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
- Rewrite physical slots/dense transfer reservation；
- external booking bytes/pages；
- Tombstone delta rows/bytes/blocks；
- txn/provider duration；
- rolling table Tombstone；
- account/cluster backlog；
- snapshot age/retained bytes；
- max certified Block estimate/task peak memory token；
- 24h/7d attempted source/committed retired expired/aborted Rewrite bytes和
  amplification。

通过条件：

- 小 Mixed `+1` 在第一次 Delete 前回滚并切换 Rewrite；
- Build预算`+1`在final transaction前进入`RESOURCE_BLOCKED`；
- final Tombstone delta `+1`使transaction abort并进入有界blocked状态；
- source数据可见；
- staging有Root并可清；
- 不通过拆分无限小Job绕过rolling limit；
- Cleanup/Reconcile资源不被饿死。

Rewrite amplification额外验证：

- failed/aborted attempt计入attempted source bytes；
- 只有committed final txn增加retired bytes；
- hour/day固定bucket rollover和CAS并发；
- CN restart/TN restart后统计不清零；
- 达阈值进入`MIXED_LAYOUT_BLOCKED`且不继续占Rewrite worker；
- runtime stats更新不修改Binding/Guard权威CAS行，不形成单表外全局热点。

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
- Root kind的Archive/TAE namespace字段NULL/NOT NULL组合；
- Root Object kind只能引用对应Archive或TAE namespace/encryption identity；
- Root Object主键为`(root_id, object_kind, ordinal)`，ordinal按kind局部递增；
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
| old+new | new | old | no Lifecycle retirement tag |
| new | new | old | no retire until protocol advance |
| new | new | new | retire |
| router将向old TN发送new entry | mixed fault | any | 发送前fence；该txn不发起retire |

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
