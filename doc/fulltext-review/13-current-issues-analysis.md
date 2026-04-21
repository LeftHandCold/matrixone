# Native FTS 当前问题分析（更新至 2026-04-20）

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

### 3.1 P1：inspect / repair / reconcile 仍未完成

这是当前最值得继续推进的缺口。

虽然系统现在已经有：

- runtime sidecar registry
- checkpoint sidecar metadata
- replay registry rebuild
- GC registry-first delete closure
- backup/restore auxiliary file closure

但还缺官方运维能力去回答和修复：

1. 哪些 object **应该**有 sidecar；
2. 哪些 locator 损坏；
3. 哪些 registry entry 缺失；
4. 哪些 sidecar 是 orphan；
5. 如何对历史对象做 metadata backfill / reconcile。

这块不补完，系统就还缺“可排障、可巡检、可修复”的治理闭环。

### 3.2 P1：backup / restore 仍缺远端实机演练

backup/restore 对齐这条线的代码已经补上：

- backup 会带上 sidecar / locator flat files；
- checkpoint rewrite 会保 sidecar rows；
- GC metadata copy 也会补 locator 扩展的 auxiliary closure。

但这轮仍然**缺少远端 Linux 测试机上的真实 backup/restore drill**。  
也就是说，代码链路已经补齐，但还没有把“恢复后 sidecar/locator 文件闭包 + 查询结果”在实机上再走一遍。

### 3.3 P1：高命中 persisted query 性能还没收官

当前性能结论已经比较清楚：

1. **第一阶段（lazy term decode）收益最明确**
   - `1M` 单机脚本里，native-ready persisted query `nativeprobe` 曾从 `306ms` 降到 `169ms`，约 **45%** 改善。
2. **第二阶段（partial read）correctness 没问题，但收益不稳定**
   - 瓶颈已从“整文件读”转成了“太多小 range read 的 round-trip 开销”。
3. **第三阶段（batched exact-term read + guard）把 read path 做得更稳**
   - correctness 已站住；
   - 但高命中 query 在 `3M` 规模下仍大致是：
     - `persistnew=350ms`
     - `stablegamma=1221ms`
     - `zzzyyyxxx=2340ms`
     - `+stablegamma -persistnew=1547ms`

所以当前性能问题的本质不是“读错了”，而是：

> **读路径已经更稳定，但“高命中 persisted query 的端到端延迟”还没有被拉到理想水平。**

### 3.4 P1：更长时间 distributed / soak / recovery 验证还不够

这轮已经补上了 `2CN + 1 proxy` 远端验证，且没有打到新的 FULLTEXT correctness bug。  
但如果目标是“更强生产信心”，还需要：

1. 更长时间 soak；
2. 节点重启 / replay / recovery 后的持续验证；
3. backup/restore 后的验证；
4. 更大规模 distributed workload 的持续回归。

---

## 4. 最新远端结论

### 4.1 单机 / 默认配置

- `1M` 单机脚本三阶段通过；
- `1M` boolean probe 通过；
- `3M mixed update/delete` checkpoint 前后继续正确；
- 当前没有新的单机默认配置 correctness P0。

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

- 如果“商用”指的是 **灰度、试商用、POC、受控生产验证**，结合当前已经验证过的默认配置、`1M` 单机、`3M mixed update/delete` 和 **`2CN + 1 proxy` 远端验证** 来看，**可以**。
- 如果“商用”指的是 **全场景成熟 GA**，当前还**不建议直接下这个结论**。

差距现在主要不在 correctness P0，而在：

1. `inspect / repair / reconcile` 还没完成；
2. backup/restore 还缺远端实机 drill；
3. 高命中 persisted query 的性能优化还没收官；
4. distributed / soak / recovery 验证还不够长。

更准确的表述应该是：

> **当前已经进入“核心链路可灰度 / 可试商用”的阶段，但还不是“所有场景都可以直接宣称全面成熟商用 GA”的阶段。**

---

## 6. 当前优先级总结

| 优先级 | 事项 | 影响 | 当前状态 |
|--------|------|------|----------|
| P1 | inspect / repair / reconcile | 稳定性 / 运维 | 进行中 |
| P1 | backup / restore 远端实机演练 | 恢复 / 运维闭环 | 未完成 |
| P1 | postings 压缩 / object-block pruning / early top-k | 查询性能 | 下一阶段主线 |
| P1 | 多 CN / 分布式 / soak / recovery 验证 | 生产信心 | 部分完成，仍需继续 |
| P2 | BM25 / 统计与 tombstone 的精细化治理 | 打分精度 | 可继续优化 |
| ✅ | Boolean mode 大表报错 | 功能 | 已修复 |
| ✅ | data-first 索引后写入导致 `MATCH` 归零 | 正确性 | 已修复 |
| ✅ | 100k tail update/delete correctness | 正确性 | 已修复 |
| ✅ | 3M mixed residual delete / `rows_total=2982480` | 正确性 | 已修复 |
| ✅ | append-only persisted `MATCH=0` 延迟塌陷 | 正确性 | 本轮未复现 |
| ✅ | native sidecar flush / merge / tail query 主路径 | 功能 | 已落地 |
| ✅ | batched sidecar read + empty-sidecar guard | 读路径稳定性 | 远端验证已通过 |
| ✅ | 2CN + 1 proxy 远端 correctness 验证 | 分布式信心 | 本轮已补通过 |
