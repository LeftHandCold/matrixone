# Native FTS 当前问题分析（更新至 2026-04-21）

基于 `fulltext_single_node_report_cn.md`、代码实现和最近的远端验证结果。

---

## 1. 当前已经站住的部分

### 1.1 默认配置与主链路功能

当前远端环境下，以下能力已经稳定可用：

1. **默认配置可用**
   - 不需要额外 `set global`；
   - 省略 `WITH PARSER` 的 FULLTEXT DDL、建索引和查询都可以直接跑通。
2. **查询主链路可用**
   - `1M` 单机脚本三阶段通过；
   - 自然语言、boolean、小 probe、mixed-coverage 都继续正确。
3. **大 DML correctness 主链路已站住**
   - `100k` tail update/delete 已恢复正确；
   - `3M mixed update/delete` 在 checkpoint 前后都继续正确。
4. **多 CN / proxy correctness 本轮已补验证**
   - `10.222.1.50` 上的 `2CN + 1 proxy` 形态已经补跑；
   - `proxy/CN1/CN2` 三入口在建索引、插入、update、delete、checkpoint 前后查询结果都保持一致。

### 1.2 当前代码状态

到这一轮为止，代码主线状态可以概括为：

1. flush / merge / compaction 会生成 native sidecar；
2. query-time tail segment 与 tombstone 过滤已接入；
3. incomplete coverage 时仍保留 v1 fallback，当前仍是 **native 优先 + v1 兜底** 的混合架构；
4. v4 sidecar、lazy decode、partial read、batched exact-term read、empty-directory fix、empty-sidecar guard 都已落地；
5. runtime sidecar registry、checkpoint sidecar row、replay registry rebuild、GC registry-first delete closure、backup/restore aux-file closure 代码路径都已补齐。

---

## 2. 已修复的关键问题

以下问题当前都不再是“正在阻塞使用”的 active blocker：

| 问题 | 当前状态 |
|------|----------|
| Boolean mode 大表 `service not found` | 已修复 |
| 省略 `WITH PARSER` 报 `invalid parser` | 已修复 |
| fileservice `SHARED` / default fallback | 已修复 |
| `flushTableTail` 最终可见 object 缺 sidecar | 已修复 |
| 100k tail update/delete correctness | 已修复 |
| data-first 索引后写入导致 `MATCH` 归零 | 已修复 |
| 3M mixed residual delete / `rows_total=2982480` | 已修复 |
| append-only persisted `MATCH=0` 延迟塌陷 | 本轮未复现 |

这意味着：**当前最主要的风险已经不再是“明显的 correctness P0 还没查清”，而是治理闭环和性能收口。**

---

## 3. 当前真正还没完成的问题

### 3.1 P1：native sidecar 覆盖还没做满

这是当前**最关键**、也最贴近“为什么性能还没有完全收回来”的问题。

`inspect / repair / reconcile` 第一版已经落地，但在 `2026-04-21` 的单机 `2M` 大回归里，直接暴露出：

1. `fts_native_case.docs`
   - `visible_objects=3`
   - `registry_objects=2`
   - `locator_objects=2`
   - 还有 **1 个 visible object 缺 registry + locator**
2. `fts_mixed_case.docs`
   - `visible_objects=2`
   - `registry_objects=0`
   - `locator_objects=0`
   - **2 个 visible object 都缺 registry + locator**
3. 对这两张表执行 `fts reconcile` 都是 **0 修复**
   - 说明这不是简单的 metadata 漂移；
   - 而是这些 object **源头上就没有可回填的 native sidecar / locator artifacts**。

这件事的含义非常直接：

> **当前正确性已经可以靠 fallback 站住，但 native 覆盖并没有做满，因此性能收益还不能在所有 persisted 场景里稳定兑现。**

### 3.2 P1：inspect / repair / reconcile 还需要第二阶段

第一版在线运维能力已经有了：

- `inspect fts show`
- `inspect fts repair`
- `inspect fts reconcile`

它已经能回答：

1. 哪些 object 缺 registry
2. 哪些 object 缺 locator
3. 哪些 locator 损坏/不一致
4. 哪些对象可安全做 `registry <- locator` 或 `locator <- registry`

但还没有完成的，是第二阶段能力：

1. orphan sidecar 清理
2. sidecar 文件重建
3. 更强的历史 metadata regeneration

所以这条线不再是“完全没有”，而是**第一版已完成，第二版治理能力还没做完**。

### 3.3 P1：backup / restore 仍缺远端实机演练

backup/restore 对齐的代码路径已经补上：

- backup 会带上 sidecar / locator flat files；
- checkpoint rewrite 会保 sidecar rows；
- GC metadata copy 会补 locator 扩展的 auxiliary closure。

但仍然**缺少远端 Linux 测试机上的真实 backup/restore drill**。  
也就是说，代码链路已经补齐，但“恢复后 sidecar/locator 文件闭包 + 查询结果”还没有在实机上再走一遍。

### 3.4 P1：高命中 persisted query 性能还没收官

当前性能结论已经进一步收敛：

1. `2M` 单机脚本里：
   - native tail query `nativeprobe=200000`：`339ms`
   - checkpoint 后 persisted query `nativeprobe=200000`：`405ms`
   - mixed `legacytoken=1M`：`918ms`
   - mixed `newtoken=1M`：`745ms`
2. 这说明当前单机版已经**不是不能用**；
3. 但也说明：
   - 高命中 persisted / mixed query 还没有到“明显更强、更稳定”的理想状态；
   - 尤其在 native 覆盖不满的情况下，fallback 会继续吞掉一部分性能收益。

也就是说，当前性能问题的本质已经不是“读错了”，而是：

> **native 主路径已经更稳，但 coverage 不满 + 高命中 persisted query 仍让端到端延迟没有完全收回来。**

### 3.5 P1：更长时间 soak / restart / recovery 验证还不够

这轮已经补过：

- 单机默认配置
- `1M`
- `2M`
- `3M mixed update/delete`
- `2CN + 1 proxy`

但如果目标是更强的生产信心，还需要：

1. 更长时间 soak；
2. 节点重启 / replay / recovery 后的持续验证；
3. backup/restore 后的验证；
4. 更大规模 workload 的持续回归。

---

## 4. 最新远端结论

### 4.1 单机 / 默认配置

- `1M` 单机脚本三阶段通过；
- `1M` boolean probe 通过；
- `3M mixed update/delete` checkpoint 前后继续正确；
- `2M` 单机脚本三阶段也继续通过；
- 当前没有新的单机默认配置 correctness P0。

但单机 `2M` 回归同时新增了一个**更有价值的发现**：

1. 大表结果仍然正确；
2. `inspect` 却显示部分 visible object 根本没有 native sidecar / locator；
3. `reconcile` 无法修复，说明缺口在写路径/产物源头，不只是 metadata 漂移。

因此当前单机版的更准确状态是：

> **正确性已经站住，但 native coverage 还没有做满。**

### 4.2 2CN + 1 proxy

在 `10.222.1.50` 上：

- proxy：`6001`
- CN1：`16001`
- CN2：`16002`

本轮补做的结论是：

1. 省略 `WITH PARSER` 与显式 `WITH PARSER ngram` 在三入口都成功；
2. `proxy` 上的三阶段 FULLTEXT 回归继续通过；
3. 跨 CN 交叉流程（`CN1` 建表/更新、`CN2` 建索引、`proxy` 插入/删除）在三入口上 checkpoint 前后都一致；
4. 更大的 `1M insert + 100k update + 50k delete` 多 CN case 也继续正确。

所以到这一轮为止，可以明确补充一句：

> **当前没有新的“只在多 CN / proxy 形态下才暴露”的 FULLTEXT correctness bug。**

### 4.3 proxy 的新增观察

这轮顺手观察到一个现象：

1. **串行短连接** 采样时，proxy 会出现明显的单边粘滞；
2. **并发连接** 采样时，两台 CN 会正常分流。

这更像 **proxy 路由策略 / 连接复用特征**，当前**不是 FULLTEXT correctness bug**，但会影响后续 proxy 压测口径。

---

## 5. 当前商用判断

- 如果“商用”指的是 **灰度、试商用、POC、受控生产验证**，结合当前已经验证过的默认配置、`1M`/`2M` 单机、`3M mixed update/delete` 和 **`2CN + 1 proxy` 远端验证** 来看，**可以**。
- 如果“商用”指的是 **全场景成熟 GA**，当前还**不建议直接下这个结论**。

差距现在主要不在 correctness P0，而在：

1. native sidecar 覆盖还没做满；
2. `inspect / repair / reconcile` 第二阶段还没完成；
3. backup/restore 还缺远端实机 drill；
4. 高命中 persisted query 的性能优化还没收官；
5. soak / recovery 验证还不够长。

更准确的表述应该是：

> **当前已经进入“核心链路可灰度 / 可试商用”的阶段，但还不是“所有场景都可以直接宣称全面成熟商用 GA”的阶段。**

---

## 6. 当前优先级总结

| 优先级 | 事项 | 影响 | 当前状态 |
|--------|------|------|----------|
| P1 | native sidecar 覆盖补齐 / registry-first coverage | 性能 / 架构闭环 | 进行中 |
| P1 | inspect / repair / reconcile 第二阶段（orphan/rebuild） | 稳定性 / 运维 | 进行中 |
| P1 | backup / restore 远端实机演练 | 恢复 / 运维闭环 | 未完成 |
| P1 | postings 压缩 / object-block pruning / early top-k | 查询性能 | 下一阶段主线 |
| P1 | 单机 / 多 CN / soak / recovery 验证 | 生产信心 | 部分完成，仍需继续 |
| P2 | BM25 / 统计与 tombstone 的精细化治理 | 打分精度 | 可继续优化 |
| ✅ | Boolean mode 大表报错 | 功能 | 已修复 |
| ✅ | data-first 索引后写入导致 `MATCH` 归零 | 正确性 | 已修复 |
| ✅ | 100k tail update/delete correctness | 正确性 | 已修复 |
| ✅ | 3M mixed residual delete / `rows_total=2982480` | 正确性 | 已修复 |
| ✅ | append-only persisted `MATCH=0` 延迟塌陷 | 正确性 | 本轮未复现 |
| ✅ | native sidecar flush / merge / tail query 主路径 | 功能 | 已落地 |
| ✅ | batched sidecar read + empty-sidecar guard | 读路径稳定性 | 远端验证已通过 |
| ✅ | inspect / repair / reconcile 第一版 | 运维基础能力 | 已落地 |
| ✅ | 2CN + 1 proxy 远端 correctness 验证 | 分布式信心 | 本轮已补通过 |
