# Attempt、Cleanup Root 与 Reconciliation 详细设计

> 本文唯一负责 system-owned Attempt/Root、Provider/TAE staging write-ahead
> ownership、immutable key、multipart、commit unknown、迟到写、Sweeper 和删除
> 收敛。

## 1. 两种 Control

### 1.1 Attempt Control

所有 child 在执行前创建 system-owned Job/Attempt Control：

- TTL：负责 final transaction identity 和结果对账；
- Archive 和 Mixed Rewrite：除上述职责外，关联 Cleanup Root；
- Discovery/Restore/Purge：负责 lease、epoch和终态。

### 1.2 Cleanup Root

Archive 或 Mixed Rewrite 在第一次外部副作用前创建 Cleanup Root。外部副作用包括
Archive Provider PUT，也包括写入 TAE shared FileService 的 live staging Object 和
transfer booking。Root 是：

- 所有 staging/published external objects 的唯一清理 Owner；
- tenant DROP 后仍可枚举的 retained metadata；
- commit unknown 期间禁止误删的 fence；
- Archive Provider与TAE FileService namespace/encryption/key identity的冻结点。

Dataset Catalog 是用户可见性权威；Root 是外部资源所有权权威。两者职责不能互换。

## 2. Attempt 创建

Scheduler持久化 child后，worker CAS claim：

```text
Job Control state runnable
AND lease expired/unowned
AND binding child generation current
  -> attempt_id
  -> executor_epoch + 1
  -> lease owner/deadline
```

Attempt冻结：

```text
account identity
logical/physical table identity
Binding/Guard/schema generation
evaluation_time/cutoff
source Object set/digest
task mode
profile identity（Archive）
dataset ID（Archive，预分配）
deadline
```

attempt ID一旦关联 source digest，不得原地换 source set。

## 3. Root-before-first-side-effect

attempt开始时只预分配确定性Root ID、segment/range/booking名称和SyncProtection
集合；这不是外部副作用，也不创建Root行。首次真正准备写Archive payload、live TAE
Object或transfer booking时：

1. system transaction重新锁定/校验account current的incarnation与`mo_account` RowID；
2. 按root kind冻结Archive和/或TAE两个namespace/encryption identity，并用
   attempt/dataset/profile构造deterministic immutable prefix；
3. 插入 Root `REGISTERED`；
4. 等待 commit；
5. 为第一个外部Object按`(root_id, object_kind, kind-local ordinal)`插Root Object
   `ALLOCATED`；
6. 等待 commit；
7. 才调用Archive Provider或TAE FileService执行对应multipart/PUT/write。

正确时序：

```text
Attempt REGISTERED
  -> Root REGISTERED committed
  -> Root Object ALLOCATED committed
  -> Provider multipart/PUT or TAE FileService write
  -> exact identity persisted
```

禁止：

```text
PUT/multipart create
  -> best-effort insert Root
```

### 3.1 Multipart create窗口

Provider可能在 `CreateMultipart` 返回后、`OnMultipartCreated`持久化前crash。Root已冻结prefix，因此Sweeper可以：

```text
ListAttemptUploads(prefix)
  -> 找到Catalog未记录upload
  -> abort
```

Profile必须支持按prefix列举multipart；不支持则不能作为GA Archive Profile。

## 4. Immutable key

每个 attempt prefix包含随机不可复用 UUID。规则：

- key只分配一次；
- 不允许 overwrite；
- provider没有version ID时，key就是exact identity；
- response lost后先HEAD/LIST/全读验证，不能直接重PUT覆盖；
- key已存在但size/SHA不同是corruption；
- 同一executor epoch内的response-lost retry只恢复同一Root/Object状态；
- re-export创建新attempt/new prefix。

不可变key解决：

- 同一epoch的重复上传和response lost对账；
- 不同attempt之间的覆盖隔离。

不可变key本身**不能**阻止已经提交给provider的stale PUT覆盖同一个key，也不自动
解决Delete安全。Delete必须先进入不可逆状态并fence所有新reference；writer接管
必须遵守下一节的new-attempt规则。

## 5. Writer lease与Root状态

首个 GA provider 接口没有统一的 conditional-create/`If-None-Match` PUT，因此：

```text
Root REGISTERED/UPLOADING 丢失 writer executor epoch
  -> fence old attempt
  -> old Root 请求 DELETE_PENDING，并等待最大I/O/迟到PUT窗口
  -> scheduler创建 new attempt ID + new Root + new prefix
  -> 从Reader/Archive重新执行
```

禁止新worker在旧 `REGISTERED/UPLOADING` Root上提高epoch后继续上传。否则旧worker
已发出的迟到PUT可能在新workerreadback后覆盖同一key。

只有：

- 同一executor epoch内的I/O retry；或
- Root已完整 `VERIFIED`，所有Root Object均VERIFIED、没有未收敛multipart/PUT，
  新worker只接手final transaction；

才允许继续使用同一Root。`FINALIZING` 只能由Reconciler按原txn对账，不能接管上传。

每次Provider操作前：

```text
Root state in REGISTERED/UPLOADING
AND root.executor_epoch == worker epoch
AND lease not expired
AND owner not dropped or cleanup not requested
```

操作后再次检查：

- epoch丢失：不开始下一个I/O；
- 当前I/O可能已完成：不由stale worker清理，交给Root Reconciler；
- Worker不能把Root从新executor状态改回旧状态。
- 如果epoch已丢失，旧worker即使收到PUT成功也只能停止；Root/Sweeper负责发现和清理，
  不能写入新attempt或把旧Root推进VERIFIED。

Root version CAS包含：

```text
root_id + state + state_version + executor_epoch
```

## 6. PUT 状态

Root Object：

```text
ALLOCATED
  -> MULTIPART_CREATED
  -> PUTTING
  -> PUT_COMPLETE
  -> VERIFIED
       |-> TAE_OWNED                          # committed live/range，Root不可再删
       `-> DELETE_PENDING -> DELETING -> DELETED
                                               # aborted staging或temporary booking
```

单PUT可以跳过 `MULTIPART_CREATED`，但仍必须从 `ALLOCATED -> PUTTING` 持久化后执行。

每一行还冻结 `object_kind`：

```text
ARCHIVE_PAYLOAD
ARCHIVE_MANIFEST
ARCHIVE_SIDECAR
TAE_LIVE_SEGMENT_RANGE
TAE_LIVE_STAGING_OBJECT
TAE_TRANSFER_BOOKING
```

前 3 种冻结 namespace/key/provider version/size/SHA；后 3 种冻结 FileService
namespace以及segment range或exact object name/size/checksum。
Root Object主键是`(root_id, object_kind, ordinal)`，其中ordinal只在kind内确定性
递增；不同kind可以各自从0开始。唯一key约束仍阻止同一物理key被重指向另一Root。
同一attempt/kind/ordinal不得重指向另一物理文件。

`TAE_LIVE_SEGMENT_RANGE`只在分类首次发现L行后、把该Batch返回给mergesort/writer前
冻结attempt专属segmentID和ordinal hard limit；它为“FileService write成功、exact
child尚未提交”的窗口提供可枚举Owner。若没有L行则不创建range；range已创建但实际
未写出Object时走删除终态，不能转`TAE_OWNED`。

Archive Rewrite父Root同时冻结`archive_storage_namespace_id`和
`tae_storage_namespace_id`。前3种object kind只能使用前者，后3种只能使用后者；
不能因为它们属于同一个attempt就假设两个FileService namespace相同。

Range child在writer关闭、actual ordinal count和全部exact child冻结后从
`ALLOCATED -> VERIFIED`；其`VERIFIED`只证明命名范围闭合，不表示range内每个ordinal
都有文件。Sweeper仍需对整个有界range执行Stat来覆盖write/exact-child之间的crash
窗口。

`PUT_COMPLETE` 条件：

- provider调用返回；
- key/provider version/size/SHA已得到；
- exact identity写入Root Object committed。

`VERIFIED` 条件：

- OpenExact到EOF；
- size/SHA/Parquet/content root一致；
- 状态CAS。

Root `VERIFIED` 条件：

```text
all required Root Objects VERIFIED
AND Archive mode has VERIFIED Manifest Root Object
AND payload count/bytes bounded
AND Archive mode freezes manifest identity/root
AND Rewrite mode freezes live-object and transfer digests
```

其中“required”由实际分类结果决定，不由Planner初始mode猜测：

```text
visible == 0:
  no Root

expired > 0, live == 0:
  Archive可有Payload/Manifest；不得有TAE range/live/booking
  TTL无外部副作用，不创建Root

expired == 0, live > 0:
  no final retirement；若已创建range/live，Root只允许cleanup，不允许TAE_OWNED

expired > 0, live > 0:
  Archive/TTL按mode要求range/live/booking，Archive再要求Payload/Manifest
```

### 6.1 TAE entry只是物理文件借用者

Lifecycle Rewrite必须给复用的Merge txn entry传入
`CreatedObjectOwnedByCleanupRoot`。从第一个FileService write到权威结果确定：

- live staging、segment range和external booking的物理Owner都是Root；
- txn entry拥有临时Catalog node、内存transfer table/page和rollback动作；
- txn entry的`PrepareRollback`不能物理删除Root child；
- `ErrTAENeedRetry`、duplicate Prepare和response lost不能转移Owner；
- committed + matching Receipt后live/range转`TAE_OWNED`；
- explicitly aborted后Root才把staging/booking转`DELETE_PENDING`；
- unknown时任何一方都不Delete。

普通Merge继续使用`CreatedObjectOwnedByMergeEntry`并保持原rollback删除行为。两种模式
必须由构造期enum区分，不能靠运行时查询Root是否存在。

## 7. Final transaction write-ahead

Archive：

```text
allocate tenant final txn
  -> Root CAS VERIFIED -> FINALIZING
       final_txn_id
       final_entry_digest
       executor_epoch
  -> commit system txn
  -> tenant final txn may retire
```

TTL Whole：

```text
allocate tenant final txn
  -> Attempt Control CAS ... -> FINALIZING
       final_txn_id
       final_entry_digest
       executor_epoch
  -> commit system txn
  -> tenant final txn may retire
```

TTL Rewrite 与 Archive 一样先把 Cleanup Root 置为 `FINALIZING`，因为它已经拥有 live
staging Object/transfer booking；不能只依赖可随 tenant 删除的 Attempt row。

系统写前记录不和tenant txn持锁交叉：

- tenant txn可先分配ID，但尚未获取table/row/Catalog写锁；
- system CAS完成并释放锁；
- 再进入tenant final transaction写阶段。

如果system CAS失败：

- 明确rollback尚未写入的tenant txn；
- 不退休；
- Root保持原Owner。

## 8. 提交结果分类

### 8.1 明确 committed

需要全部证据：

```text
Txn GetStatus == COMMITTED(commit_ts)
AND a normal consistent tenant read at snapshot >= commit_ts
    sees Receipt(attempt_id, txn_id, entry_digest)
AND non-empty Archive sees Dataset(dataset_id, manifest_root)
```

然后：

```text
Archive Whole/Rewrite:
  Root FINALIZING -> PUBLISHED
  仅当live > 0时：Rewrite live/range child -> TAE_OWNED
  仅当booking存在时：Rewrite booking child -> DELETE_PENDING

TTL Rewrite:
  仅当live > 0时：live/range Root Object -> TAE_OWNED
  仅当booking存在时：booking Root Object -> DELETE_PENDING
  Root FINALIZING -> POST_COMMIT_CLEANUP
  all booking deleted -> TRANSFERRED

TTL Whole/EMPTY_ARCHIVE:
  Attempt FINALIZING -> COMMITTED
```

`TAE_OWNED/POST_COMMIT_CLEANUP/TRANSFERRED` 只在一致性读取确认 Receipt 且 txn
service确认committed之后写入。`TAE_OWNED`表示normal TAE Catalog/WAL/GC已接管
live文件，Sweeper禁止删除。`POST_COMMIT_CLEANUP`期间只允许删除temporary
booking，不能删除live Object。

### 8.2 明确 aborted

```text
Txn GetStatus == ABORTED
AND consistent tenant read sees no matching Receipt/Dataset
```

然后：

```text
Archive Root -> DELETE_PENDING
TTL Attempt -> ABORTED
TTL Rewrite Root -> DELETE_PENDING
```

### 8.3 In-doubt

包括：

- Commit RPC timeout/connection lost；
- Txn status ACTIVE/unknown；
- TN unavailable；
- commit status committed但一致性read尚未到commit_ts；
- status service短暂错误。

动作：

```text
Job -> COMMIT_UNKNOWN
Root/Attempt remains FINALIZING
keep txn identity/digest
release worker execution slot
schedule RECONCILE
pause same table retirement
```

不清理Payload，不新开final txn。

## 9. 一致性 Receipt 读取

`Txn GetStatus=COMMITTED`后Catalog read可能暂时落后。Reconciler：

1. 用 txn client timestamp waiter等待可服务timestamp `>= commit_ts`，deadline 10秒；
2. 创建正常tenant RC/SI read transaction，Snapshot `>= commit_ts`；
3. 精确按attempt/txn ID读Receipt和Dataset；
4. 未达到水位：`WAIT_APPLY`，不是corruption；
5. 达到水位仍缺Receipt：进入P0 invariant error，不自动清理。

不自建Lifecycle participant apply watermark。

`ABORTED`但读到匹配Receipt，或`COMMITTED`但Receipt digest不同：

- cluster retirement kill switch；
- Root保持FINALIZING；
- `MANUAL_RECONCILE_REQUIRED`；
- 保存证据。

## 10. Txn status保留

GA要求Txn GetStatus权威记录/可解析窗口至少覆盖：

```text
max automatic reconcile age = 24 hours
operator investigation window = 7 days
```

如果现有Txn Service在该窗口前丢失状态，P0必须增加可查询的normal transaction result receipt或延长保留。不能用source Object missing推断成功。

超过24小时仍in-doubt：

```text
MANUAL_RECONCILE_REQUIRED
  -> Root/Attempt仍FINALIZING
  -> same table retirement paused
  -> worker slot released
  -> hourly bounded status probe
```

运维不能直接点“按失败清理”。人工处置必须取得权威txn/Receipt证据并留下审计。

## 11. Reconciler

新增：

```text
pkg/lifecycle/cleanup/
  reconciler.go
  receipt_reconcile.go
  uploads.go
  roots.go
  attempts.go
```

每分钟由TaskService唤醒一个逻辑Reconciler。每轮：

```text
query state in (FINALIZING, COMMIT_UNKNOWN, MANUAL_RECONCILE_REQUIRED)
AND next_action_at <= now
ORDER BY next_action_at, attempt_id
LIMIT 1000
```

每行：

- 独立context/deadline；
- 独立system transaction；
- 不持锁跨RPC；
- exponential backoff with jitter；
- failure不阻塞其他行；
- process crash靠state/version重新接管。

## 12. Cleanup触发

Root进入`DELETE_PENDING`的合法原因：

1. Archive/Rewrite attempt明确aborted/cancelled且未发布；
2. final transaction明确aborted；
3. Dataset达到purge_eligible且tenant Dataset已CAS（Archive payload）；
4. table/database/account owner dropped；
5. Restore/verify前的staging失效；
6. immutable identity已被新Dataset显式复制并旧Dataset正常Purge。

非法原因：

- executor lease过期；
- response lost；
- final txn超时；
- 暂时读不到Dataset；
- TaskService把task分配给新worker；
- Profile credential暂时失败。

### 12.1 committed Rewrite 的 temporary booking

`TAE_TRANSFER_BOOKING`不属于Archive Dataset，也不需要等待Purge。只有满足：

```text
final txn authoritatively committed
AND matching Receipt visible
AND child kind == TAE_TRANSFER_BOOKING
AND child state == VERIFIED/DELETE_FAILED
```

才允许child级CAS：

```text
VERIFIED -> DELETE_PENDING -> DELETING -> DELETED
```

这个路径：

- 不改变`PUBLISHED` Archive Root；
- 不读取或删除Archive Payload/Manifest；
- 不删除`TAE_OWNED` live/range child；
- TTL Rewrite全部booking为`DELETED`后才把Root
  `POST_COMMIT_CLEANUP -> TRANSFERRED`；
- Delete失败只把该child置为`DELETE_FAILED`并重试，不回滚已提交数据。

## 13. Delete协议

### 13.1 Fence新访问

进入删除：

```text
CAS Root PUBLISHED/VERIFIED/DELETE_FAILED
  -> DELETE_PENDING
  -> access_generation + 1
```

之后：

- 禁止新Restore lease；
- 旧generation lease不再续租；
- 等待同一旧generation有效lease释放/过期/fence；
- 不允许取消后重新引用同一key。

### 13.2 不可逆 `DELETING`

条件：

```text
Root state == DELETE_PENDING
AND no active lease at previous access generation
AND final txn not in-doubt
AND cleanup credential available
```

CAS：

```text
DELETE_PENDING -> DELETING
state_version + 1
new sweeper executor_epoch
```

进入后不可回到PUBLISHED。

### 13.3 删除顺序

```text
1. Archive namespace存在时 ListAttemptUploads(prefix)
2. abort all Archive multipart
3. list Root Object rows page
4. delete unpublished transfer booking
5. delete unpublished TAE live staging Object
6. delete Archive Payload/SIDECAR
7. delete Manifest last
8. 分别在Archive namespace LIST prefix，并在TAE namespace枚举segment range/Stat
9. 按各自namespace reconcile Catalog未记录的orphan/late objects
10. HEAD/Stat exact keys确认不存在
11. repeat empty LIST/HEAD confirmation
12. Root -> CLEANED
```

已经由 committed final transaction 发布到 TAE Catalog 的 live Object 和 transfer
信息不再由 Root 删除，而由正常 TAE Merge/GC/WAL replay 所有；temporary booking
仍由Root按child状态单独删除。Manifest 最后删，
便于中途故障诊断；进入DELETING后不再允许Restore，因此这不是可见性要求。

每批：

```text
delete keys <= 1000
delete request bytes <= 4 MiB
deadline <= 2 minutes
```

### 13.4 确认不存在

Profile capability probe确定read-after-delete/list一致性参数。默认要求：

```text
two consecutive:
  LIST prefix empty
  all known keys HEAD/Stat not found
separated by 5 minutes
```

任一对象仍存在：

- Root保持DELETING；
- 重新排队；
- 不标CLEANED。

provider Delete返回success但对象仍可读不算删除完成。

## 14. Stale runner Delete安全

旧/新Sweeper重复Delete安全的前提：

```text
Root already irreversible DELETING
AND no new reference can be created
AND immutable key never reused
```

不是因为Delete API本身有CAS。现有ObjectStorage只有key Delete，因此：

- Delete只针对Root exact immutable key；
- 不删除宽prefix；
- 不删除可复用业务路径；
- provider支持version ID时ArchiveStore适配器额外按version删除；
- stale writer看到DELETING必须停止PUT。

## 15. 迟到 PUT

场景：

```text
writer发出PUT
  -> lease丢失/cleanup第一次完成
  -> provider迟到完成PUT
```

控制：

- Provider单I/O hard deadline 2分钟；
- executor每次I/O前后检查epoch；
- `CLEANED` Root tombstone保留；
- deterministic prefix永不复用；
- Sweeper在quiescence window对Archive namespace重复LIST，并对TAE deterministic
  range重复Stat；两类identity分别验证。

默认：

```text
multipart convergence allowance = 1 hour
quiescence window               = 24 hours
CLEANED metadata retention      = 30 days
```

在quiescence内发现新对象：

```text
Root remains logically CLEANED but cleanup_generation + 1
  -> DELETING_LATE_OBJECTS
  -> delete
  -> restart quiescence timer
```

不能把Root恢复成PUBLISHED。

## 16. DELETE_FAILED

触发：

- credential revoked；
- bucket/namespace不存在；
- provider持续5xx；
- Delete/HEAD/LIST超出8次自动attempt；
- immutable identity不一致；
- multipart无法枚举/abort。

状态：

```text
Root DELETE_FAILED
objects exact list retained
cleanup credential ref retained
next_action_at bounded
last error visible
```

自动retry：

```text
1m, 5m, 30m, 2h, 6h, 24h, daily
```

达到8次后每天轻量probe，不占worker；P1告警。credential恢复后新Sweeper epoch CAS接管。

`DELETE_FAILED`不能：

- 删除Catalog证据；
- 标PURGED/CLEANED；
- 释放Profile version；
- 静默忽略。

## 17. Root与Dataset Owner转移

Root始终保留外部清理Owner。`PUBLISHED`表示：

- tenant Dataset允许Restore；
- Root禁止清理 Archive payload；已提交 live/range 子对象标为 `TAE_OWNED`，
  temporary booking按child状态清理；
- Root等待Dataset Purge/owner drop。

不是把所有权完全转给可随tenant DROP消失的Dataset。这样DROP ACCOUNT后system Root仍能清理。

`TRANSFERRED` 只用于没有 Archive Dataset 的 TTL Rewrite：Root 已把全部
live/range子对象转给TAE，并已删除temporary booking，不再拥有可删除对象。它
保留短审计期后可分页GC，不能走Provider Purge。

## 18. Attempt终态清理

TTL Whole Attempt：

- COMMITTED/ABORTED后保留30天；
- COMMIT_UNKNOWN/Manual不删除；
- 无外部对象。

Archive/Rewrite Attempt：

- Root CLEANED后Job Control可按30天清理；
- Root POST_COMMIT_CLEANUP必须继续调度temporary booking清理；
- Root TRANSFERRED后Job Control和Root可按30天清理；
- Root PUBLISHED时Attempt可终态，但Root保留到Purge；
- blocked/cancelled必须确认Root DELETE_PENDING/CLEANED。

控制表GC每批1000行，按终态时间分页。

## 19. Q1-Q3

| 等待/资源 | Q1 谁收尾 | Q2 谁通知 | Q3 何时结束 |
|---|---|---|---|
| Provider PUT | ArchiveStore；失联后Root Sweeper | ctx/observer/Root epoch | PUT complete、error或2m deadline |
| multipart | Root Object/Sweeper | upload observer + LIST | abort/complete且provider确认 |
| TAE live staging write | Cleanup Root；txn entry只借用 | ctx/Root epoch/FileService Stat/txn status | committed后TAE_OWNED，或aborted后exact staging已删除；unknown保持 |
| external transfer booking | Cleanup Root | txn status/Receipt + child CAS | committed/aborted明确后DELETED；unknown保持 |
| source reservation/protection | Lifecycle attempt | renew/expiry/TN Prepare | final result确定或 attempt abort |
| Rewrite dense slab/memory token | Rewrite Executor/mergesort task | ctx、allocator error、txn completion | host close或entry commit/rollback；exactly-once release |
| Tombstone delta scan/watermark wait | final TAE txn entry | caller ctx/deadline/limit/error | EOF、limit、cancel或60s final deadline |
| final txn | txn client resolver + Reconciler | GetStatus/Receipt | committed或aborted；unknown转manual不猜测 |
| worker lease | Job Control | heartbeat/expiry | terminal或new epoch接管 |
| Restore lease | Restore Attempt/Lease Reconciler | renew/release/expiry | released、expired、fenced |
| Payload Delete | Sweeper | Delete+HEAD/LIST | 两次确认不存在 |
| late PUT window | Root Sweeper | periodic LIST | quiescence完整无新对象 |

Control-plane长等待（I/O重试、commit unknown、lease、quiescence）必须释放worker
槽，不通过永久阻塞goroutine保活。TN final Prepare内的delta scan是同步短临界路径，
可以占用当前执行槽，但受60秒deadline和rows/bytes/blocks hard limit约束，退出时不
遗留goroutine或内存Owner。

## 20. 故障测试

Root-before-side-effect：

- Root insert前禁止 Provider/FileService 写；
- Root commit后、multipart前crash；
- multipart create后observer前crash；
- first part/last part/complete response各点crash。

Commit：

- Root FINALIZING前/后；
- tenant prepare/commit/response各点；
- status committed但logtail未apply；
- status aborted；
- status长期unknown；
- committed但Receipt digest错触发kill switch。

Cleanup：

- delete每个object kind+ordinal前后crash；
- manifest最后删；
- Delete success但HEAD仍存在；
- LIST lag；
- credential revoke/rotate；
- stale two sweepers；
- Restore lease与DELETE_PENDING竞态；
- late PUT在首次CLEANED后出现。

Scale：

- 1 million Root Objects（含 archive/live staging/booking）分页；
- 1000 active roots + 1 million terminal rows；
- cleanup minimum worker share；
- orphan multipart上限；
- system tables无global cursor热点。
