# TPCC 多 CN duplicate 根因复盘（stale subscribe / `FOR UPDATE` 收敛版）

> **2026-04-07 更正**：当前这轮**稳定复现**并已着手修复的主链路，不是旧版本文档重点描述的 TN `merge/flush transfer` 交叠。那条链仍可能解释另一类更难复现的 TN commit-time false duplicate，但**不是**这轮多 CN TPCC 持续命中的 CN statement-time business-key duplicate 主因。

## 1. 当前一句话结论

当前最强结论是：

**某个 CN 在事务已经拿到 `snapshotTS` 之后，首次按需 subscribe `bmsql_district` 这类热点表时，旧代码只保证“订阅成功”，没有保证这张表的 latest `PartitionState` 已经 apply 到当前事务可见边界 `snapshotTS.Prev()`。随后 `SELECT ... FOR UPDATE` 可能在这张新订阅表上读到旧的 `d_next_o_id`，后续 `INSERT bmsql_oorder` 继续使用旧 `o_id`，最终在 CN statement 期 dedup 报业务键 duplicate。**

这次修复也围绕这个点展开：

- 在 latest `PartitionState` 上记录**真实已经 apply 完成**的 table-logtail 上界 `appliedLogtailTS`
- 只在**fresh subscribe** 场景下，等这张表的 latest state 追到当前事务可见边界 `snapshotTS.Prev()` 再让读路径信任它
- 之前已经提交的 `PKPersistedBetween()` transient-object hardening **保留**，因为它修的是一个真实的 persisted-path 漏洞，只是不是这轮稳定复现的主因

## 2. 直接证据

### 2.1 最新 `(7,5,6444)` duplicate 的现场链路

- `logs/cn2.log:74852`
  - txn `f5088a308f0e45d418a3f8b6589f999a`
  - `RC-FOR-UPDATE-PK-CHECK`
  - `path:"not-flushed"`
  - `changed:false`
- `logs/cn2.log:75119-75122`
  - 同一 txn 最后在 CN `dedupjoin/join.go:485` / frontend trace 上报
  - `Duplicate entry '(7,5,6444)' for key '(o_w_id,o_d_id,o_id)'`
  - 失败 SQL 是 `INSERT INTO bmsql_oorder ... ; 6444 ; 5 ; 7 ...`

这说明最新稳定复现的失败面已经不是最初那类 TN commit-time `__mo_cpkey_col` duplicate，而是：

> **CN 先读错了 district 的 order id，随后在 statement 期直接把旧业务键插重。**

### 2.2 手动 checkpoint / flush 只是放大器，不是独立根因

- 同窗 `logs/tn.log:22474-22475` 出现 `send logtail channel blocked`
- 同窗还有大量 `wait too long` / `failed to valid txn`

这些现象更像是：

> **fresh subscribe + 热点表 logtail apply 延迟** 被放大出来了

而不是“checkpoint 本身直接制造 duplicate”。

## 3. 这次真正修的代码点

### 3.1 问题点：fresh subscribe 之后，latest table state 没有对事务 snapshot 做 freshness 校验

关键链路：

- `pkg/vm/engine/disttae/logtail_consumer.go:updatePartitionOfPush`
  - 订阅/更新表 logtail 时会更新 latest `PartitionState`
- `pkg/vm/engine/disttae/txn_table.go:getPartitionState`
  - 读路径按需 `toSubscribeTable()`
  - 旧代码在 fresh subscribe 后直接信任返回的 latest `PartitionState`

旧代码缺的不是“有没有 subscribe 成功”，而是：

> **这张表本身的 latest state 是否已经 apply 到当前事务可见边界 `snapshotTS.Prev()`**

### 3.2 修复方法

本次修复做了两件事：

1. `PartitionState` 新增 `appliedLogtailTS`
   - 由 `updatePartitionOfPush()` 在每次成功 apply table logtail 后推进
   - 它表示：**这张表在当前 CN 的 latest state 至少已经 apply 到哪一个 table-logtail 上界**
2. `txnTable.getPartitionState()` 在 **fresh subscribe** 场景下：
   - 不再只看“是否 subscribe 成功”
   - 而是等待 latest part 的 `appliedLogtailTS >= txn.snapshotTS.Prev()`
   - 满足后才让读路径继续使用这张表的 latest state

这避免了“事务已经有 `snapshotTS`，但这张刚订阅表的本地 state 还没追平实际可见边界 `snapshotTS.Prev()`”的 stale read。

## 4. 为什么之前那版 fix 没删

之前提交的 persisted-path fix：

- `pkg/vm/engine/disttae/logtailreplay/blocks_iter.go`
- `pkg/vm/engine/disttae/txn_table.go:PKPersistedBetween()`

修的是另一个真实问题：

> flush / merge / checkpoint 把对象在 `[from,to]` 内 create+delete 掉时，旧代码会把 transient object 漏掉，导致 RC retry 的 persisted-path 误判成 unchanged

所以那版 fix **不是错的**，只是：

- **它修的是 secondary persisted-path hole**
- **不是这轮稳定复现 duplicate 的主分支**

因此它保留，不回退。

## 5. 下面旧 TN 分析怎么理解

下面剩余章节保留，作为：

- 原始 `__mo_cpkey_col` / `__mo_%1_delete_rowid` 路线的历史分析
- 另一类更偏 TN commit-time false duplicate 的背景材料

但阅读时要注意：

> **它们不再代表这轮多 CN TPCC 稳定复现 duplicate 的主根因。**

## 2. 先把误区纠正掉

前一轮分析里，有一个结论需要收回到更准确的表述：

- `pkg/vm/engine/disttae/txn.go` 里 `legacyPKIndex()` 和 `resolvePKCheckPosForWrite()` 的确明确跳过了 `catalog.CPrimaryKeyColName`，也就是 hidden composite PK `__mo_cpkey_col`
- 所以 CN workspace 对 composite PK **确实没有**普通单列主键那种早期 `checkDup()`

但是这件事只能说明：

> **为什么问题没有在 CN 更早暴露**

它解释不了：

> **为什么 TN `Phase_Freeze` 最后真的看到了两份相同 `__mo_cpkey_col` 的可见版本**

如果 TN 侧 transfer / tombstone masking / dedup 没问题，那么即使 CN 不做早期检查，commit 阶段也不应该“凭空”看到 duplicate。

因此，之前把根因压到 CN `persisted-delete` / `object-rewrite` 上，说重了。现在看，CN 那边最多只是放大条件，不是决定性触发器。

## 3. 原始故障到底发生在什么地方

真正抛错的位置在 TN，而不是 CN：

- `pkg/vm/engine/tae/txn/txnimpl/txndb.go:464-483`
  - `Freeze()` 里先执行 `table.PrePreareTransfer(...)`
  - 再执行 `table.PrePrepareDedup(...)`
- `pkg/vm/engine/tae/txn/txnimpl/table.go:1205-1238`
  - `DoPrecommitDedupByPK()` 先 `incrementalGetRowsByPK(...)`
  - 再 `findDeletes(...)`
  - 如果仍然还有 rowid 没被删掉，就直接在 `txnimpl/table.go:1228` 打 `Duplicate`

也就是说，TN 的逻辑是：

1. 先把 transfer 相关状态准备一遍
2. 再按主键做 dedup
3. dedup 时会尝试用 tombstone 信息把“应该被认为已删除的旧版本”屏蔽掉
4. 如果有一份旧版本没有被正确屏蔽，就会和当前新版本一起命中同一个 PK，于是 commit 报 duplicate

## 4. TN 侧最关键的代码链路

### 4.1 Freeze 的顺序

`pkg/vm/engine/tae/txn/txnimpl/txndb.go:464-483`

```go
nowTS := db.store.rt.Now()
for _, table := range db.tables {
    if err = table.PrePreareTransfer(ctx, txnif.FreezePhase, nowTS); err != nil {
        return
    }
}

for _, table := range db.tables {
    if err = table.PrePrepareDedup(ctx, false, txnif.FreezePhase, nowTS); err != nil {
        return
    }
    ...
}
```

这里说明 `Freeze()` 不是“纯 dedup”，而是先碰 transfer，再碰 dedup。

### 4.2 dedup 是怎么决定“旧版本是否已经被删掉”的

`pkg/vm/engine/tae/txn/txnimpl/table.go:1205-1238`

- `incrementalGetRowsByPK(...)` 先按 PK 找候选 rowid
- `findDeletes(...)` 再试图把已删除 rowid 置空
- 只要有 rowid 没被置空，就直接报 duplicate

`pkg/vm/engine/tae/txn/txnimpl/table.go:1122-1190,1645-1733`

- `findDeletes(...)` 先调用 `contains(...)`
- `contains(...)` 会用 workspace tombstone rowid，以及已存在 tombstone object 中的 rowid，去 mask 掉候选 rowid
- `findDeletes(...)` 之后还会 `WaitTombstoneObjectCommitted(to)`，再遍历 tombstone object

因此，**dedup 的正确性依赖于 transfer / tombstone 状态在这个窗口里是稳定且一致的**。

### 4.3 merge 和 flushTableTail 都会做 transfer

`pkg/vm/engine/tae/tables/txnentries/mergeobjects.go:97-106`

```go
entry.collectTs = rt.Now()
entry.transCntBeforeCommit, _, err = entry.collectDelsAndTransfer(ctx, entry.txn.GetStartTS(), entry.collectTs)
```

`pkg/vm/engine/tae/tables/txnentries/flushTableTail.go:113-123`

```go
entry.collectTs = rt.Now()
entry.transCntBeforeCommit, err = entry.collectDelsAndTransfer(ctx, entry.txn.GetStartTS(), entry.collectTs)
```

也就是说，后台 merge 和 flushTableTail 都会在同一张表上重新整理 delete / transfer 关系；这正好和用户事务在 `Freeze()` 里依赖这些信息做 dedup 的路径发生交集。

## 5. 原始 TN 日志为什么强烈指向这个根因

最关键的是 `Explore-logs-dn.txt:534-564` 这一段，按时间线看：

| 时间 | 事件 | 含义 |
| --- | --- | --- |
| `18:06:56.582` | `[MERGE-END]` on `bmsql_stock` | 一个大 merge 刚结束 |
| `18:06:56.776` | `[FLUSH-START]` on `bmsql_stock` | 同表 flushTableTail 开始 |
| `18:06:56.804` | `[FLUSH-STEP] sort-key="__mo_cpkey_col"` | flush 正在按 hidden composite PK 处理 |
| `18:06:57.089` | `[MERGE-PREPARE-COMMIT] total-transfer=47671` | merge 正在提交 transfer 结果 |
| `18:06:57.185` | `txnimpl/table.go:1228 Duplicate` | 用户事务在 `Phase_Freeze` 的 dedup 处报错 |
| `18:06:57.327` | `[FLUSH-END]` | flush 此时才真正结束 |
| `18:06:57.329` | `[FLUSH-PREPARE-COMMIT] transfer-rows=149` | flush 随后提交 transfer 结果 |

这个时间窗说明的不是“CN workspace 自己搞错了”。

它说明的是：

> **在用户事务做 Freeze dedup 的那一刻，`bmsql_stock` 同时正处在 merge transfer 已准备提交、flushTableTail 仍在处理中、并且马上也要提交 transfer 的状态。**

这和 TN 代码里的 `PrePreareTransfer -> PrePrepareDedup -> findDeletes/contains` 路径完全对上。

## 6. 根因的具体机制

把代码和日志拼起来，当前可以收敛出下面这个机制：

1. TPCC `NewOrder` 会反复打到 `bmsql_stock(s_w_id, s_i_id)` 这种 composite PK 表
2. 在高并发多 CN 下，`bmsql_stock` 后台持续有 merge / flushTableTail
3. 某个用户事务在 commit 时进入 TN `Freeze()`
4. `Freeze()` 先做 `PrePreareTransfer`
5. 随后 `PrePrepareDedup()` 通过 `incrementalGetRowsByPK()` 找到了同一个 hidden composite PK 的候选 rowid
6. 这时本来应该由 `findDeletes()` / `contains()` 用 tombstone / transfer 信息把旧版本 rowid mask 掉
7. 但在这次故障窗口里，后台 merge / flushTableTail 正在同表上做 `collectDelsAndTransfer()` 并提交 transfer 结果
8. 结果就是：**有一份旧版本在 dedup 那一刻没有被正确识别为“已删除/已转移”**
9. dedup 于是同时看到了旧版本和新版本
10. 最终对 `__mo_cpkey_col` 抛出 duplicate

这就是为什么 duplicate 是：

- **在 TN 抛的**
- **在 commit 才抛的**
- **表名是 `bmsql_stock`**
- **错误键是 hidden composite PK `__mo_cpkey_col`**

## 7. 为什么 TPCC 不是根因

TPCC 的 `NewOrder` 业务允许热点更新，也允许一个订单里出现重复 item，从而让同一个 `bmsql_stock` 逻辑行在一个事务或短时间窗口里被多次更新。

这类负载会把问题窗口放大，但它本身不是 bug。原因很简单：

- 对数据库引擎来说，“同一主键行被多次更新”是合法场景
- 只要版本链、删除链和 dedup 正确，最终都不应该报主键 duplicate

所以 TPCC 的角色是：

- **触发器**
- **放大器**

不是：

- **根因本体**

## 8. 为什么现在不再把焦点放在 CN 上

之前把重点放在 CN，有两个事实支撑：

1. `disttae` 对 hidden composite PK 没有早期 `checkDup()`
2. RC statement boundary 的确会 `merge -> dump -> transfer`

这两个事实仍然成立，但它们不再足以支撑“根因在 CN”这个结论。原因是：

### 8.1 远端 live repro 长时间打不出来

已经做过这些尝试：

- 单机 MatrixOne：没复现
- 多 CN 远端 `10.222.1.50:6001`：没复现
- 手写 SQL 热点更新：没复现
- 强制重复 item 的 TPCC-like NewOrder 压测：没复现
- 在 disttae 上加 `MO_CPKEY_REPRO_RANDOM_SLEEP_*`：你跑了很久也没复现
- 再加 `MO_CPKEY_REPRO_FORCE_FLUSH`：仍然没复现

如果根因真主要在 CN statement boundary / persisted-delete，这一轮人为放大窗口应该已经明显提高命中率。但现实并没有。

### 8.2 原始故障日志更像 TN 后台任务交叠

真正的故障窗口恰好卡在：

- merge transfer 即将提交
- flushTableTail 仍未结束
- 用户事务 Freeze dedup 正在执行

这个组合，比单纯的 CN flush / persisted-delete，更能解释“为什么 duplicate 只在 commit 的 TN 阶段出现”。

## 9. 已确认但不是主因的事实

下面这些仍然是对的，只是定位上要降级：

### 9.1 CN hidden composite PK 没有早期 dup check

`pkg/vm/engine/disttae/txn.go:394-418`

- `legacyPKIndex()` 明确跳过 `catalog.CPrimaryKeyColName`

`pkg/vm/engine/disttae/txn.go:1071-1075`

- `resolvePKCheckPosForWrite()` 对 `catalog.CPrimaryKeyColName` 返回 `(-1, true, nil)`

这意味着 composite PK 的 hidden key 不会像普通单列主键那样在 CN workspace 被提前 dedup。

**意义：**它解释了“为什么没更早失败”，但不解释“为什么 TN 最后真的看到 duplicate”。

### 9.2 TPCC 现场里 `statement_info` 看到的 `FOR UPDATE` 不是根表

CN 侧 `statement_info` 里最显眼的是：

```sql
SELECT d_tax, d_next_o_id
FROM bmsql_district
WHERE d_w_id = ? AND d_id = ?
FOR UPDATE
```

但 TN 真正报 duplicate 的表是 `bmsql_stock`，这一点以 TN 日志为准。

### 9.3 TPCC 跑完后 check 数据没问题，说明了什么

这条观察很重要，而且它**不支持“TN 没问题”这个结论**，更像是在帮我们把问题类型进一步缩小：

- 如果跑完 TPCC 后做数据校验没有发现错误，说明这次故障**更像是 commit 阶段的假阳性 duplicate / false conflict**
- 也就是说，TN 在某个窗口里**错误地把一个合法事务判成了 duplicate，于是事务失败回滚**
- 这和“已经提交了坏数据”不是一回事

因此，这条现象更合理的解释是：

> **TN 的问题更偏向“可见性/transfer/dedup 交叠下的误判”，而不是“静默写坏数据”。**

这反而和当前 TN 根因判断是一致的：

- 如果 dedup 在 `Phase_Freeze` 里把旧版本误当成仍然可见，它会直接拒绝提交
- 事务被拒绝以后，最终落盘数据仍然可能保持正确

所以“check 数据没问题”说明这个 bug **目前更像保守性误杀**，而不是数据损坏；但这并不说明 TN 可以排除，相反，它更像是在说明 **TN 的 commit dedup / transfer 协调有误判**。

## 10. 现在最合理的修复方向

真正的修复方向不应再是“只在 CN 上加更多随机 sleep”。更合理的是：

### 10.1 主修复方向

让 TN `Freeze()` 做 dedup 时，看到一个**稳定的一致视图**，不要和后台 merge / flushTableTail 的 transfer 提交窗口交叉污染。

可行思路包括：

1. 在 `PrePrepareDedup()` 前后，对同表后台 transfer 结果的可见性做更严格的序列化
2. 让 `findDeletes()` / `contains()` 能覆盖 transfer 改写中的过渡状态，避免旧 rowid 漏 mask
3. 检查 `PrePreareTransfer()` 与后台 merge / flush transfer 共存时，是否存在对象 / rowid / tombstone 可见性不一致

### 10.2 防御性补强

即使主因在 TN，CN 侧也仍然建议补一层：

- 对 hidden composite PK 增加更早的 workspace `checkDup()` 覆盖

这不能替代主修复，但它能：

- 更早发现问题
- 缩小错误传播窗口
- 让线上更快暴露异常点

## 11. 为什么当前还没有稳定复现

当前没能稳定复现，不是因为问题不存在，而是因为命中的条件比之前想的更苛刻：

- 不是只要“同事务多次更新同一 composite PK”就会出
- 也不是只要“CN statement boundary flush”就会出
- **更可能是必须同时命中 TN 上同表后台 merge / flush transfer 的特定交叠窗口**

这也解释了为什么：

- 现场 TPCC 能偶发打出
- 人工脚本和简单并发压测却很难稳定复现

### 11.1 最新实测：只开 3 个 TN `sleep` hook，没复现，但性能从 `25000` 掉到 `700`

这次结果本身是合理的，而且信息量很大。

先说性能下降为什么**正常**：

- `fault` 里的 `sleep` 动作单位是**秒**，不是毫秒
- 代码里 `SLEEP` 的实现就是：

```go
time.Sleep(time.Duration(e.iarg) * time.Second)
```

- 所以 `add_fault_point(..., 'sleep', 1, ...)` 的含义是：**每次命中就睡 1 秒**

而你开的 3 个 hook：

- `fj/tn/merge/post_collect_transfer`
- `fj/tn/flushtail/post_collect_transfer`
- `fj/txn/freeze_after_transfer_before_dedup`

都在 `bmsql_stock` 这种 TPCC 最热的 composite PK 表路径上。结果就是：

- 前台提交在 TN `Freeze()` 里频繁被卡 1 秒
- 后台 merge / flushTableTail 也频繁被卡 1 秒
- 同一张热点表的 transfer / dedup / 后台维护一起堆积

所以从 `25000` 掉到 `700` 这种级别的吞吐暴跌，**是预期内的，不是异常现象**。

但更重要的是：**性能暴跌却仍然没复现，这反而进一步收窄了问题。**

这说明问题很可能不是：

> “只要把 transfer 和 dedup 的大窗口粗暴拉长，就能打出来”

更像是：

> **必须命中一个更细、更具体的状态切换点。**

换句话说，`always-on` 的 1 秒 sleep 有两个副作用：

1. 它确实把系统拖慢了
2. 但它也可能把原本短促、尖锐的竞态，变成了**更串行、更保守**的执行序列

因此，粗粒度 sleep 既可能“放大窗口”，也可能“抹平那个真正危险的微小时序”。

### 11.2 这次结果对根因判断意味着什么

这次结果没有推翻 TN 方向，反而让 TN 方向更具体了：

- **TN 根因方向仍然成立**
- 但“merge/flush transfer 与 Freeze dedup 有交叠”这个表述还不够细
- 决定性窗口更可能靠近：
  - `MERGE-PREPARE-COMMIT`
  - `FLUSH-PREPARE-COMMIT`
  - `findDeletes()/contains()` 之后到 duplicate 判定之前
  - 或 `WaitTombstoneObjectCommitted(to)` 相关的可见性边界

也就是说，问题更像是：

> **某个旧 rowid 在一个极窄的 commit/visibility 边界上漏掉了 tombstone mask**

而不是：

> **只要 transfer 和 dedup 同时活跃就一定会出**

### 11.3 最新实测：只开 `fj/txn/dedup_after_find_deletes`，16:25 打出了 TPCC 业务键 duplicate，但这次仍不能算原始 `__mo_cpkey_col` 复现

这次结果很重要，因为它说明：

> **只在 `findDeletes()` 之后插一个 hook，就足以把 TN 提交路径拖到非常危险的状态。**

但同样重要的是：

> **16:25 这次 client 看到的 duplicate，仍然不能直接等价成“原始 `__mo_cpkey_col` 根因已经复现”。**

先看 client 侧：

- `nohup.out:107839-108003`
  - `16:24:57` 到 `16:25:24` 连续出现大量 `Communications link failure`
  - 同时终端大量重连
- `nohup.out:108434-108439`
  - `16:26:21` 才出现一条真正的业务键 duplicate：
  - `Duplicate entry '(7,1,6385)' for key '(o_w_id,o_d_id,o_id)'`

再看 CN：

- `logs/cn1.log:114918-114965`
- `logs/cn2.log:134951-134991`

同一时间段里可以看到：

- `wait too long` 持续达到 **1-4 分钟**
- `failed to valid txn`
- `initRemote 2`
- `broken pipe`
- `SELECT ... FOR UPDATE`
- `UPDATE bmsql_district ...`

也就是说，这一波首先表现为：

> **TN 提交路径严重堆积 -> CN 锁等待和会话写回失败 -> client 超时断连重连 -> 最后才出现一条业务唯一键 duplicate。**

再看 TN：

- `logs/tn.log:34641-34878`
  - 大量 `SLOW-LOG-PrePrepareDedup`
  - 单次 `PrePrepareDedup` 经常是 **2-5 秒**
  - `commit txn too slow` 经常达到 **4-5 分钟**
  - 期间反复出现 `TransferPage load`
  - 同窗还出现了：
    - `bmsql_new_order` 的 `FLUSH-START/FLUSH-END`  
      `logs/tn.log:34814-34822`
    - `bmsql_district` 的 `FLUSH-START/FLUSH-END`  
      `logs/tn.log:34824-34828`
    - `bmsql_district` tombstone 的 `MERGE-START/MERGE-END`  
      `logs/tn.log:34847-34851`
    - `bmsql_stock` 的 merge gather  
      `logs/tn.log:34829-34831`, `34860-34862`

但是关键点在于：

> **在 `16:24-16:26` 这个窗口里，没有看到 TN 内部的 `Duplicate { ... }`，也没有看到 `__mo_cpkey_col` 或 `__mo_%1_delete_rowid` 的 duplicate。**

同窗能看到的 TN 内部失败是：

- `logs/tn.log:34933-34936`
  - `16:26:36`
  - `Phase_PrePrepare`
  - `r-w conflict`
  - 对象 id 落在 `282836`，也就是 `bmsql_stock`

所以这次更准确的归类应该是：

1. **单个 `fj/txn/dedup_after_find_deletes` hook 已经足够证明 `findDeletes() -> duplicate 判定` 这段窗口极其敏感**
2. 它确实会把 TN 提交路径拖到：
   - `PrePrepareDedup` 2-5 秒
   - commit 堆积到 4-5 分钟
   - 锁等待、broken pipe、client 重连
3. 但这次 `16:26:21` 的 `(o_w_id,o_d_id,o_id)` duplicate，更像是：
   - 连接超时 / 结果不确定
   - client 重连继续执行 / 重试
   - 最后落到业务键重复
4. **由于同窗没有 TN 内部 `__mo_cpkey_col` / `__mo_%1_delete_rowid` duplicate，所以这次不能当成原始根因已被严格复现**

这次结果对根因判断的价值在于：

- 它**支持**“TN dedup 边界是主战场”
- 它**支持**“`after_find_deletes` 已经非常接近决定性窗口”
- 但它**还不支持**“原始 `__mo_cpkey_col` 那条旧 rowid 漏 mask 机制已经被直接打中”

因此，下一步更应该做的是：

> **在 `after_find_deletes` 命中的事务上，直接把 PK、候选 rowid、被 mask 的 rowid、最终提交结果一起打出来。**

### 11.4 最新实测：同时开 `fj/txn/dedup_after_get_rows_by_pk` 和 `fj/txn/dedup_after_find_deletes`，更容易打出 `order_line` 业务键 duplicate，但这条路径仍不等价于原始 `__mo_cpkey_col`

这轮 `nohup.out` 里一共出现了 3 条 `1062`：

- `10:59:46`：`Duplicate entry '(6,7,6580,1)' for key '(ol_w_id,ol_d_id,ol_o_id,ol_number)'`
- `11:00:16`：`Duplicate entry '(1,10,6640,1)' for key '(ol_w_id,ol_d_id,ol_o_id,ol_number)'`
- `11:00:17`：`Duplicate entry '(1,2,6515,1)' for key '(ol_w_id,ol_d_id,ol_o_id,ol_number)'`

这次和前几轮最大的区别是：

> **这 3 条 duplicate 不只是 client 侧看到，CN 自己也明确把它记成了 `INSERT INTO bmsql_order_line ...` 的 statement 执行期 duplicate。**

直接证据在：

- `logs/cn1.log:4530-4532`
- `logs/cn1.log:4889-4891`
- `logs/cn2.log:4917-4919`

这些日志都清楚指向：

- `dedupjoin/join.go:485`
- 失败 statement 是 `INSERT INTO bmsql_order_line ...`
- 紧接着事务直接 `Rollback`

这说明这轮 `1062` 的失败面已经不是“client 超时后自己猜测失败”，而是：

> **CN 在 SQL 执行期，就把这个 `order_line` insert 判成了 duplicate。**

从代码上看，这条路径和原始 `__mo_cpkey_col` 的报错面并不一样：

- `pkg/sql/plan/bind_insert.go:517-524`
  - INSERT 会构造一个 `Node_DEDUP`
  - children 是 `{scanNodeID, lastNodeID}`
- `pkg/sql/compile/compile.go:2401-2402`
  - 编译时先走 probe side，再把另一侧作为 build side 送进 join map
- `pkg/sql/colexec/dedupjoin/join.go:453-485`
  - 命中后直接按 `DedupColName` 返回 duplicate

所以这轮 `1062` 更准确的含义是：

> **`INSERT INTO bmsql_order_line` 在 CN 的 DEDUP JOIN 阶段，已经看到了同一业务复合键对应的可见行。**

这里的“可见行”目前有两种可能：

1. 另一个事务已经把同一 `(ol_w_id,ol_d_id,ol_o_id,ol_number)` 提交出来了
2. 同一个超长事务里，这条 `order_line` 已经更早写入过一次，后来又被重复执行了一次

现有日志还不能 100% 区分这两种情况，但它已经足以排除一件事：

> **这次不是原始那种“TN commit 时才在 hidden `__mo_cpkey_col` 上打 duplicate”的路径。**

因为同窗的 TN 只看到了：

- `logs/tn.log:3450-3524`
- 大量 `SLOW-LOG-PrePrepareDedup`
- 大量 `commit txn too slow`，时长达到 `7-8m`
- 一条 `w-w conflict`

但没有看到：

- `txnimpl/table.go:1228/1234 Duplicate`
- `__mo_cpkey_col`
- `__mo_%1_delete_rowid`
- `TN-COMPOSITE-PK-FINDDELETES-UNMASKED`
- `TN-DEDUP-DUPLICATE-DETAIL`

所以这轮更准确的归类应该是：

1. `after_get_rows_by_pk` + `after_find_deletes` 这组 hook 的确把系统推到了更敏感的区域
2. 但它当前更容易打出来的是 **CN 语句期的业务复合键 duplicate**
3. 业务复合键和 hidden `__mo_cpkey_col` 在“逻辑 composite PK”层面确实是一回事
4. 但**它们不是同一个报错面、也不是同一个判重阶段**
5. 如果把这轮误记成“原始 `__mo_cpkey_col` 已复现”，会把两条问题链混在一起

这次结果真正的价值是：

- 它证明双 hook 会把事务拖成超长事务
- 它证明 logical composite-key duplicate 已经能在 SQL 层面被看见
- 但如果目标仍然是原始 `__mo_cpkey_col` 根因，下一轮必须继续以 TN 内部 tag 和 TN duplicate 为准，而不能只看 client/CN 的 `1062`

### 11.5 最新实测：手动 `force checkpoint` + `RC-FOR-UPDATE` trace，已经把这轮 `bmsql_oorder` duplicate 明确收敛到“陈旧 `SELECT ... FOR UPDATE` 结果”

这轮和前几轮最大的不同，不是又多了一条 `1062`，而是：

> **这次同一个 txn 内，后续 district 语句已经看到了 `d_next_o_id=6566`，但后面的 `oorder` insert 仍然在用 `6564`。**

这条证据链比之前任何一轮都更直接。

先看 client：

- `nohup.out:3122`
  - `11:55:29`
  - `Duplicate entry '(10,7,6564)' for key '(o_w_id,o_d_id,o_id)'`

再看 CN 直接报错面：

- `logs/cn2.log:11148-11150`
  - duplicate 出自 `dedupjoin/join.go:485`
  - 失败语句是：

```sql
INSERT INTO bmsql_oorder (...) VALUES (6564, 7, 10, ...)
```

所以这次用户侧看到的业务键 duplicate，本体就是：

> **这笔事务真的试图往 `bmsql_oorder(10,7,6564)` 再插一行。**

但真正关键的，是同一个 txn 的 district trace：

- 同一连接 / 同一 txn：
  - `logs/cn2.log:10815`
  - `logs/cn2.log:11077`
  - `logs/cn2.log:11149-11150`
  - 都对应：
    - `connectionId 196718`
    - `txn_id = f5088a308f0e45d418a3f672bf8b2acf`

- 在 duplicate 前几毫秒，`logs/cn2.log:10910`
  - 同一个 txn 在 `bmsql_district(10,7)` 上的 lock 输入 batch 是：
  - `d_next_o_id = 6566`

- 紧接着 `logs/cn2.log:10953`
  - 同一个 txn 的这次 lock 结果是：
  - `refresh-ts = 0-0`
  - 也就是这条语句本身没有走“需要 retry 才能看新值”的路径

把这两组事实放在一起，含义非常明确：

1. **同一个 txn 后续在 `bmsql_district(10,7)` 上已经看到了 `d_next_o_id=6566`**
2. **但同一个 txn 后面的 `INSERT bmsql_oorder` 仍然在用 `o_id=6564`**

这说明：

> **`6564` 不是这条事务在 duplicate 发生当下从 district 当前行里刚读出来的值，而是一个更早拿到、但已经过期的 order id。**

而在 TPCC `NewOrder` 路径里，`o_id` 正是从前面的 district 读取链路带下来的。  
因此，这轮最合理、也最贴合日志的归因已经不是“client 自己乱了”或“纯粹 statement replay”，而是：

> **前面的 `SELECT ... FROM bmsql_district ... FOR UPDATE` 给了这笔事务一个已经过期的 `d_next_o_id`。**

换句话说，这次 run 对之前两个竞争假设的影响是：

- **显著加强**：RC / logtail / lock 边界上的 **stale `FOR UPDATE` result**
- **显著削弱**：单纯“后面 insert 自己重放 / 自己重复执行”就是全部原因

因为如果只是“后面 insert 自己重放”，很难解释：

> **为什么同一个 txn 随后在同一行 district 上已经看到了 `6566`，而业务链里仍然在沿用更老的 `6564`。**

### 11.6 手动 `checkpoint` 和这次 duplicate 的关系：高度相关，但更像放大器/探针，不像唯一根因

这轮你手动触发的 `checkpoint` 不应当被忽略，而且它和 duplicate 的时间关系非常紧：

- `logs/tn.log:3168-3175`
  - `11:55:29.393`
  - `flusher.force`
  - 明确覆盖了：
    - `bmsql_district`
    - `bmsql_oorder`
    - `bmsql_new_order`
    - `bmsql_order_line`
    - `bmsql_stock`

- `logs/tn.log:3192-3193`
  - `11:55:29.885`
  - `send logtail channel blocked`

- `logs/tn.log:3195`
  - 一个超长事务刚在
  - `1775534129887453597-0`
  - 提交完成

- 紧接着 CN 就在 `logs/cn2.log:10896-10899`
  - 收到了 `bmsql_district` 的
    - `DataObject`
    - `TombstoneObject`
    - `Delete`
    - `Insert`
  - 也就是说，district 的对象化 / flush 后 logtail apply 就发生在 duplicate 前几毫秒

所以，这次手动 `checkpoint` 和 duplicate 的关系，当前最合理的表述应该是：

1. **它高度相关**
2. **它很可能显著放大了 district / oorder 这条链路上的可见性切换窗口**
3. **它让 latent 的 stale-`FOR UPDATE` 问题更容易在“前一条语句拿旧值、后一条语句已能看到新值”的形式下暴露出来**

但它更像是：

> **把问题打亮的探针 / 放大器**

而不是：

> **脱离原有 bug 机制、单独凭空制造 duplicate 的唯一根因**

原因很简单：

- 即使手动 `checkpoint` 让 flush / logtail 切换更频繁
- **前一条 `SELECT ... FOR UPDATE` 也仍然不应该把已经过期的 `d_next_o_id` 交给业务继续使用**

所以这轮 run 反而让这条结论更硬了：

> **checkpoint 解释了“为什么这次更容易观察到问题”，但真正不该发生的，是 stale district order-id 被 `FOR UPDATE` 成功返回。**

## 12. 新加的 TN hook（为了复现）

为了把复现重点从 CN 挪到 TN，这一轮已经在 TN 相关路径上补了新的 fault hook，而且只在 **hidden composite PK 表** 上触发：

| hook | 位置 | 用途 |
| --- | --- | --- |
| `fj/tn/merge/post_collect_transfer` | `txnentries/mergeobjects.go` | 卡住 merge 在 `collectDelsAndTransfer()` 之后、prepare transfer page 之前 |
| `fj/tn/flushtail/post_collect_transfer` | `txnentries/flushTableTail.go` | 卡住 flushTableTail 在 `collectDelsAndTransfer()` 之后、add transfer pages 之前 |
| `fj/txn/freeze_after_transfer_before_dedup` | `txnimpl/txndb.go` | 卡住用户事务 `Freeze()` 在 `PrePreareTransfer` 之后、`PrePrepareDedup` 之前 |
| `fj/txn/dedup_after_get_rows_by_pk` | `txnimpl/table.go` | 卡住 dedup 在 `incrementalGetRowsByPK()` 之后 |
| `fj/txn/dedup_after_find_deletes` | `txnimpl/table.go` | 卡住 dedup 在 `findDeletes()` 之后 |

这些 hook 可以有两种用法：

1. **本地 manual test**
   - 配成 `wait`，再用 `getwaiters/notify/notifyall` 精准编排时序
2. **dev-up / 集群现场**
   - 直接配成 `sleep`
   - 用 SQL fault injection 把窗口拉长，不需要再改容器里的环境变量

对应的 manual repro 也补了一条：

- `TestManualCompositePKDuplicateAfterTransferBeforeDedup`

它和之前的 `TestManualCompositePKDuplicateAfterMerge` 相比，更贴近现在这个“**TN 已经做完 transfer，但 dedup 看到的状态被后台任务改动**”的根因判断。

## 13. 接下来怎么办

### 13.1 第一优先级：把观测点加到 TN，而不是继续盲目放大 CN sleep

建议优先在下面几个点加定向日志 / hook，只针对 `bmsql_stock` 或目标 table id：

1. `pkg/vm/engine/tae/tables/txnentries/mergeobjects.go`
   - `collectDelsAndTransfer()` 前后
   - 记录 transfer row 数、对象 id、collectTs、commitTs
2. `pkg/vm/engine/tae/tables/txnentries/flushTableTail.go`
   - 同样记录 transfer 相关信息
3. `pkg/vm/engine/tae/txn/txnimpl/txndb.go`
   - `Freeze()` 里 `PrePreareTransfer` 与 `PrePrepareDedup` 前后打点
4. `pkg/vm/engine/tae/txn/txnimpl/table.go`
   - 在 duplicate 前，打印该 PK 命中的 rowid 列表、哪些 rowid 被 `findDeletes()` mask 掉、哪些没被 mask

核心目标不是加更多随机延迟，而是**直接证明：duplicate 那次到底是哪一个旧 rowid 没被 tombstone / transfer 正确屏蔽。**

### 13.2 第二优先级：先用新 hook 直接拉长 TN 窗口

这一步已经试过第一轮：**3 个 coarse hook 全开 + `sleep=1s`，性能大幅下降但没复现。**

所以这一步现在要调整成更细的策略，而不是继续把 3 个 hook 常驻全开：

1. **不要再默认 3 个 coarse hook 一直开**
2. 优先只开一个 hook，或者只让它按频率稀疏触发
3. 如果继续现场压测，优先把 hook 往 dedup 内部收紧，而不是继续扩大粗窗口

更合适的顺序是：

- `fj/tn/merge/post_collect_transfer`
- `fj/tn/flushtail/post_collect_transfer`
- `fj/txn/freeze_after_transfer_before_dedup`

先单独试，不要三者一直全开。

如果还要继续配 `sleep`，更建议：

- 只开一个
- 配频率，不要每次都打

例如思路上可以改成“每 100 次或 1000 次命中一次”，而不是每次命中都睡 1 秒。

如果这样还不够，再叠加：

- `fj/txn/dedup_after_get_rows_by_pk`
- `fj/txn/dedup_after_find_deletes`

尤其是 `fj/txn/dedup_after_find_deletes`，现在比前面 3 个 coarse hook 更值得优先试，因为它更靠近“旧 rowid 是否真的漏 mask”这个决定性点。

但从最新双 hook 实测看，`after_get_rows_by_pk + after_find_deletes` **不适合默认一直双开常驻**。它虽然更容易打出 duplicate，但当前更容易先把系统推到 **CN `dedupjoin` 语句期 business-key duplicate**，而不是原始 TN commit-time `__mo_cpkey_col` duplicate。

也就是说，下一轮不应该再追求“把 TN 大窗口整体拖慢”，而应该追求：

> **在更靠近 duplicate 判定的位置，做更窄、更可控的放大。**

### 13.3 第三优先级：做一个 TN 定向控制窗口的本地回归

比起继续跑远端黑盒压测，更应该做：

- 一个本地多 CN / 单 TN 的 deterministic 回归
- 用 fault point 或专门 hook，把 `mergeobjects` / `flushTableTail` 卡在 `collectDelsAndTransfer()` 到 prepare-commit 之间
- 同时让用户事务进入 `Freeze()` 的 `PrePrepareDedup()`

这样才能稳定打中真正的故障窗口。

### 13.4 第四优先级：修复后再补 CN 防线

修完 TN 主因后，再决定是否补：

- hidden composite PK 的 CN workspace 早期 `checkDup()`

这一步是加固，不是主修。

## 14. 当前结论的最终表述

我现在建议把结论固定成下面这句话：

> **这次多 CN TPCC 的 `__mo_cpkey_col` duplicate，根因是 TN/TAE 在 `bmsql_stock` 上，后台 merge/flush transfer 与用户事务 `Phase_Freeze` dedup 的交叠导致旧版本未被正确 tombstone-mask，最终在 commit 时把旧版本和新版本同时当成可见记录；CN 对 hidden composite PK 不做早期 dup check 只是让问题没有更早暴露。**

这比“根因在 TPCC”或者“根因主要在 CN workspace persisted-delete”都更符合目前的代码和日志证据。
