# 小 Mixed Object 普通 DELETE 详细设计

> 本文唯一负责小 Mixed 的可写 SI 事务、delete key、Relation.Delete、Archive/TTL
> 原子性、预算、并发冲突和 blocked 语义。

## 1. 结论

首个 GA 不重写 Mixed Object。只有一个 Mixed Object 的少量到期行满足全部硬预算时：

```text
one normal writable SI transaction
  -> exact Reader at txn Snapshot
  -> filter expired visible rows
  -> optional Archive PUT + full readback
  -> Root FINALIZING
  -> existing Relation.Delete(RowID + actual delete key)
  -> Guard/Binding + optional Dataset + Receipt
  -> normal commit
```

提交后：

- 普通 SELECT 通过现有 MVCC/Tombstone 不再看到到期行；
- 普通 Merge/Vacuum/GC按现有逻辑处理 Tombstone；
- Lifecycle 不写隐藏查询 filter；
- Lifecycle 不生成新 live Object/transfer map；
- 超预算进入 `MIXED_LAYOUT_BLOCKED`，源行继续可见。

## 2. 为什么必须是 SI

MO 默认悲观事务常用 RC。RC 会在语句边界推进 Snapshot：

```text
SELECT expired rows at S1
Provider PUT/readback
DELETE at S2
```

如果 S2 不同于 S1，Archive 行集和 DELETE 行集可能不一致。

Mixed 使用显式：

```go
client.WithTxnMode(txn.TxnMode_Pessimistic)
client.WithTxnIsolation(txn.TxnIsolation_SI)
```

事务从创建到 commit 的 `SnapshotTS` 不变。不能：

- 使用 `CloneSnapshotOp`，因为 `Relation.Delete` 明确拒绝 Snapshot Operator；
- 使用两个事务分别 SELECT 和 DELETE；
- 使用 RC statement snapshot；
- 导出后重读当前表再删除“看起来相同”的行。

## 3. 代码组件

新增：

```text
pkg/lifecycle/retire/
  txn.go
  delete_key.go
  delete_batch.go
  budget.go
  conflict.go

pkg/lifecycle/coordinator/
  mixed_job.go
```

核心接口：

```go
type TxnFactory interface {
    NewWritableSI(
        ctx context.Context,
        account AccountIdentity,
        maxDuration time.Duration,
    ) (*LifecycleTxn, error)
}

type LifecycleTxn struct {
    Op      client.TxnOperator
    Engine engine.Engine
    Proc   *process.Process
}

func (t *LifecycleTxn) Relation(
    ctx context.Context,
    databaseID uint64,
    tableID uint64,
) (engine.Relation, error)

func (t *LifecycleTxn) Commit(ctx context.Context) error
func (t *LifecycleTxn) Rollback(ctx context.Context) error
```

`NewWritableSI`：

1. 从 service runtime取得 txn client/engine；
2. 使用最新可服务 timestamp创建 normal internal txn；
3. 显式 SI + pessimistic；
4. `Engine.New(ctx, op)`；
5. 创建独立 `Process` 和 mpool account；
6. 设置 account/user/role context为受控 lifecycle service identity；
7. 注册 max workspace/duration admission；
8. 不借用用户 Session 的 txn handler。

`LifecycleTxn` 是唯一 txn Owner。commit返回 unknown后 Owner转移到现有 txn unknown resolver + Attempt/Root Reconciler。

## 4. 固定 Snapshot

child开始：

```text
txn = NewWritableSI
source_snapshot_ts = txn.Op.SnapshotTS()
```

随后所有 source操作使用同一个：

- `engine.Database(..., txn.Op)`；
- `Relation`；
- exact Reader；
- `Relation.Delete`；
- tenant Catalog repository；
- final commit。

每个步骤前断言：

```text
txn isolation == SI
txn status == ACTIVE
txn SnapshotTS == source_snapshot_ts
txn is not Snapshot Operator
```

任何 snapshot变化视为实现错误，rollback。

## 5. source Object 权威确认

在 SI txn Snapshot：

1. 打开 physical table；
2. 当前 Relation Metadata中查 exact Object；
3. 校验 Binding/Guard physical/schema generation；
4. 校验 ObjectStats digest；
5. Object不存在或变化：rollback/replan；
6. exact Reader只读该一个 Object。

Index 的 Mixed分类不是最终条件。

## 6. 实际 delete key

`Relation.Delete` 的普通 RowID路径使用两列 Batch：

```text
attrs = [catalog.Row_ID, "pk"]
vec0  = RowID
vec1  = actual encoded delete key
```

不能简单取用户声明的第一列。新增：

```go
type DeleteKeyProjection struct {
    ColumnName string
    Seqnums    []uint16
    Type       types.Type
    Encoder    DeleteKeyEncoder
    Kind       DeleteKeyKind
}

func ResolveDeleteKeyProjection(
    tableDef *plan.TableDef,
) (DeleteKeyProjection, error)
```

解析规则必须与普通 SQL DELETE 的 `DeleteCtx.PrimaryKeyIdx` 一致：

| 表 | delete key |
|---|---|
| 单列显式 PK | PK列 |
| 复合 PK | MO持久化的 composite primary key编码列/同一编码函数 |
| 无显式 PK | MO持久化 fake PK |

Binding 准入时必须证明：

- key是持久化且 exact Reader可投影；
- encoder与 SQL DELETE相同；
- Restore业务投影不会额外暴露fake/composite hidden key。

建议把普通 DELETE 中的 key解析/Batch构造抽成共享 helper，而不是 Lifecycle复制编码：

```text
pkg/sql/colexec/deletion/delete_key.go
```

共享接口：

```go
func ResolvePhysicalDeleteKey(*plan.TableDef) (DeleteKeyProjection, error)

func AppendDeleteRow(
    mp *mpool.MPool,
    dst *batch.Batch,
    rowID types.Rowid,
    key vectorValue,
) error
```

普通 DELETE 的行为必须有回归测试。

## 7. Reader projection

Mixed TTL：

```text
business columns: lifecycle column only if needed for filter
control:
  __mo_rowid
  actual delete key input/encoded vector
```

Mixed Archive：

```text
business:
  all user-visible source schema columns
control:
  __mo_rowid
  actual delete key
```

Reader callback对每个到期行：

1. Archive模式同步写业务列到 Parquet；
2. 复制 RowID和encoded delete key到 bounded delete buffer/spill；
3. 更新实际 rows/bytes/blocks budget；
4. 任一 hard limit到达立即停止；
5. 此时尚未调用 `Relation.Delete`，所以可以安全 rollback并清理staging。

## 8. Delete buffer

到期 RowID/delete key不能无限留内存。

采用 bounded encrypted local spill：

```text
memory delete buffer <= 64 MiB
spill file           <= 256 MiB
spill files          <= 8
```

spill：

- 位于 CN task-specific temp目录；
- 文件名包含 attempt ID；
- 进程内唯一 Owner是Mixed Executor；
- 内容为 length-delimited RowID + encoded key；
- 可选本地临时加密key只在attempt内存；
- 不作为 durable recovery source；
- crash后由 CN temp scavenger按age清理；
- final txn commit unknown期间不需要spill恢复，因为DELETE已进入txn workspace。

Reader结束后才开始按批调用 Delete：

```text
delete batch rows  <= 8,192
delete batch bytes <= 16 MiB
```

每个 Batch attrs/vec类型与普通 DELETE完全一致。

## 9. Archive Mixed 顺序

固定：

```text
begin writable SI txn
  -> exact Reader
  -> lazy create Root before first PUT
  -> stream Parquet
  -> close all Payload
  -> full readback
  -> Manifest VERIFIED
  -> all hard budgets still satisfied
  -> system txn Root VERIFIED -> FINALIZING(final txn ID = SI txn ID)
  -> start txn workspace statement
  -> Relation.Delete batches
  -> tenant CAS Guard/Binding/active attempt
  -> insert Dataset
  -> insert Receipt
  -> end statement
  -> commit same SI txn
```

在第一次 `Relation.Delete` 后：

- 不再执行 Provider PUT/GET/readback；
- 不再改变 Manifest；
- 只允许本地 delete/Catalog/commit；
- 失败必须 rollback/unknown reconcile。

Root进入 FINALIZING 之前不能开始 Delete。

## 10. TTL Mixed 顺序

```text
begin writable SI txn
  -> exact Reader/filter/copy delete buffer
  -> all budgets satisfied
  -> Attempt Control FINALIZING(final txn ID)
  -> Relation.Delete batches
  -> CAS Guard/Binding/active attempt
  -> insert Receipt
  -> commit
```

无到期行：

- rollback read-only SI txn；
- Job标记 `NOOP_COMMITTED`；
- 不写Receipt，因为没有退休副作用；
- Index row更新 `next_action_at`。

## 11. Catalog 写如何保持同一事务

不能使用会自行 `BEGIN/COMMIT` 的普通 `BackgroundExec`。

新增 tenant txn-bound repository adapter：

```go
type TenantTxnCatalog interface {
    CASBindingAndGuard(ctx context.Context, txn *LifecycleTxn, ...) error
    InsertDataset(ctx context.Context, txn *LifecycleTxn, ...) error
    InsertReceipt(ctx context.Context, txn *LifecycleTxn, ...) error
}
```

实现选项只能二选一，并在 P0固定：

1. 直接用同一 Engine/Relation写 system cluster table；
2. 使用能够显式绑定已有 `TxnOperator` 的 internal SQL executor。

禁止：

- 新开第二 tenant txn；
- system `BackgroundExec`隐式autocommit；
- Provider成功后用另一个事务插Dataset。

P0优先采用已有 Engine Relation写入，因为原子边界更直接；SQL executor只用于不参与退休原子性的system Root事务。

## 12. 硬预算

一个 Mixed child只有一个 source Object。必须同时满足：

| 预算 | Soft | Hard |
|---|---:|---:|
| source compressed bytes | 1 GiB | 当前认证最大 Object，首个 profile 3 GiB |
| source physical rows | 1,000,000 | 2,097,152 |
| expired visible rows | 250,000 | 1,000,000 |
| expired ratio | 1% | 5% |
| affected blocks | 64 | 256 |
| RowID + delete key raw bytes | 32 MiB | 64 MiB |
| estimated txn workspace | 128 MiB | 256 MiB |
| estimated WAL/Logtail | 64 MiB | 128 MiB |
| Archive selected business bytes | 256 MiB | 512 MiB |
| transaction wall time | 15 min | 30 min |
| Provider single I/O | 1 min | 2 min |

Hard limit任一超过：

```text
before Delete:
  rollback txn
  Root DELETE_PENDING if present
  MIXED_LAYOUT_BLOCKED

after Delete:
  no new work should make budget grow beyond precomputed hard bound
  unexpected overflow -> rollback/commit-unknown protocol + P0 alert
```

### 12.1 Tombstone估算

最低原始成本：

```text
expired_rows * (types.RowidSize + encoded_delete_key_bytes_per_row)
```

Admission还乘以 release safety factor 2.0覆盖：

- Batch/vector overhead；
- txn workspace；
- RPC/protobuf；
- Tombstone Object；
- WAL/Logtail；
- Merge/Vacuum放大。

使用实际encoded key bytes，不用固定“PK 8B”估计。

### 12.2 Rolling backlog

即使单Job小，也不能无限连续制造Tombstone。

表级24小时rolling limits：

```text
Mixed deleted rows      <= 5 million
Mixed raw tombstone     <= 512 MiB
affected blocks         <= 4,096
```

集群同时检查：

- Tombstone Object backlog；
- Merge backlog；
- GC lag；
- WAL/Logtail pressure；
- workspace admission。

达到上限暂停Mixed，不影响Whole。

## 13. 并发语义

### 13.1 普通 Merge

```text
SI Reader snapshot references Object A
Merge replaces A -> B
Lifecycle Delete carries old RowID
```

复用现有 RowID transfer：

- transfer成功：DELETE作用到新位置；
- transfer page不存在/过期：事务冲突/abort；
- 不允许静默丢弃未解析RowID；
- Lifecycle不生成transfer map。

P0必须覆盖 Merge在 Reader前、中、Delete后、Prepare前提交。

### 13.2 用户 DELETE

如果用户在 SI Snapshot后删除同一选中行，Lifecycle commit必须：

- 因普通 w-w/delete conflict abort；
- 或由共享DELETE语义证明重复删除不会发布包含该行的Archive。

首个GA安全要求选择前者。若当前 `Relation.Delete` 对该并发场景会静默成功，P0不通过，必须增加选中RowID的普通锁/commit validation；不能仅靠文档假设。

### 13.3 用户 UPDATE

UPDATE通常表现为旧RowID Tombstone + 新行：

- 旧行与Lifecycle DELETE冲突；
- 新行不在固定Snapshot；
- Lifecycle事务abort；
- 下一child重新判断新生命周期值。

不能让旧Archive版本和新活动行同时被当作一次成功退休。

### 13.4 INSERT

新INSERT不在source Object/Snapshot：

- 不影响当前child；
- 即使生命周期值已经过期，也由后续Object/child处理；
- 不承诺cutoff瞬间捕获迟到数据。

### 13.5 DDL/DROP

Guard/Binding CAS：

- TRUNCATE/schema change/drop使Mixed txn abort；
- owner dropped后不得插Dataset；
- staging由Root清理；
- COMMIT_UNKNOWN仍先对账。

## 14. 锁

Mixed不拿Whole Archive的exclusive table lock。它只使用：

- 普通 SI pessimistic transaction；
- `Relation.Delete`现有Row/PK锁；
- Catalog row/unique key锁；
- normal txn lockservice/unknown resolver。

固定顺序：

```text
source exact read（无写锁）
  -> Provider I/O（无table/row写锁）
  -> Root FINALIZING system txn completes
  -> Relation.Delete row locks
  -> Guard/Binding
  -> Dataset/Receipt
  -> commit
```

不能在Provider I/O期间持有Row锁。

## 15. 失败和Owner

| 失败 | 动作 |
|---|---|
| exact Object变化 | rollback/replan |
| Reader/预算失败 | rollback；无Delete |
| Archive PUT/readback失败 | rollback；Root cleanup |
| Root FINALIZING失败 | rollback；不Delete |
| Relation.Delete失败 | rollback；Root等待明确abort后cleanup |
| Catalog CAS失败 | rollback |
| commit明确aborted | Root DELETE_PENDING/Attempt ABORTED |
| commit unknown | Root/Attempt保持FINALIZING；spill/local Reader资源可释放 |
| worker crash before Delete | txn timeout/rollback；Root收敛 |
| worker crash after Delete before commit response | existing txn resolver + Receipt对账 |

## 16. 为什么不拆成无限小 DELETE

把一个大Mixed Object每次删除1万行并不能自动变安全：

- 每次都会制造Tombstone/WAL/Logtail；
- Object在数百个txn中持续Mixed；
- Merge/transfer冲突概率升高；
- backlog增长可能快于Vacuum；
- 最终仍可达到TB级写放大。

因此：

- expired ratio/rolling backlog是硬门槛；
- 大Mixed进入blocked；
- 不用“多跑几千个小Job”绕过。

## 17. 测试要求

Txn/Snapshot：

- 显式SI且Snapshot不变；
- Snapshot Operator Delete被拒绝；
- RC对照测试证明为何不安全；
- Reader/Archive/Delete同一TxnOperator；
- Catalog insert与Delete原子。

Delete key：

- 单PK；
- 复合PK；
- varlen PK；
- fake PK；
- NULL不允许的key路径；
- 与普通SQL DELETE Batch byte-for-byte一致。

Budget：

- 每个hard边界-1/等于/+1；
- 2,097,152 rows单Object；
- 3 GiB source streaming；
- 256 MiB单varlen限制；
- rolling backlog；
- spill crash cleanup。

并发：

- user DELETE/UPDATE before/after Reader；
- Merge transfer success/conflict/missing page；
- TRUNCATE/DDL/DROP；
- two Lifecycle workers被Binding active attempt fence；
- Provider慢导致txn deadline。

Commit：

- 1PC/2PC；
- response lost；
- Root FINALIZING后rollback；
- Dataset/Receipt有且DELETE无的状态不可达；
- DELETE提交但Dataset无的状态不可达；
- no-op不产生空Dataset。

回归：

- 普通 Relation.Delete行为不变；
- Merge/Tombstone/Vacuum未增加Lifecycle分支；
- 未绑定表无新事务或锁。
