# ADR：以 TAE Object 而不是 SQL Partition 作为生命周期执行边界

> 状态：Accepted
>
> 日期：2026-07-24
>
> 适用范围：MatrixOne Issue #24552 / #24853
>
> 规范设计：[TAE 对象级数据生命周期概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)

## 背景

MatrixOne 需要为普通表提供原生 TTL 和可恢复归档。早期方案要求用户先按生命周期列建立 SQL Range Partition，再以完整分区作为封存、归档和退休单元。

当前 MO Partition Service 使用隐藏物理子表承担路由；`Redefine` 仍包含临时表、`INSERT SELECT`、Drop 和 Rename 的整表迁移路径。大量数据库、表和按天分区会把对象生命周期问题放大为 Catalog 表数量、路由和 DDL 问题。非分区表、已有其他分区键的表及迟到数据也无法自然适配。

TAE 数据已经以不可变 Object 组织，Object 具有行数、大小、ZoneMap 和稳定身份；现有 `mergesort.DoMergeAndWrite` 提供流式读写、排序和 transfer-map 原语。

## 决策

1. Lifecycle Scanner、Job、source ref 和最终事务以**有界的 exact TAE Object set**为执行边界。
2. SQL Partition 不是功能依赖。已有分区只能作为候选发现和物理对齐优化。
3. 不新增第二套 Merge Engine，也不修改普通 Merge 的候选、Level、Overlap、Small、目标大小或调度策略。
4. Mixed Object 使用独立的 **Lifecycle Rewrite Executor**：
   - 过期行进入 Discard/Archive Sink；
   - 存活行使用稳定的 Merge sort/write 原语写回 ObjectIO；
   - 最终通过独立、可重放的 Lifecycle Commit 协议替换源对象。
5. Whole Object、Mixed Object、TTL 和 Archive 使用同一 Catalog、source-ref、commit receipt、依赖准入和资源预算模型。
6. correctness 由持久化 source ref、dependency fingerprint、事务 CAS 和 durable receipt 保证；TN scope reservation 只减少与普通 Merge 的冲突，是可后置优化。

## 为什么不选择其他方案

### 强制 SQL Range Partition

- 不能覆盖普通非分区表；
- 对海量表产生隐藏子表和 DDL 放大；
- 生命周期列未必等于业务分区键；
- 迟到数据仍需额外协议；
- 当前 Partition 模块不是对象级 GC、事务和归档所有权系统。

### 在普通 Merge 中直接执行 Lifecycle

- 会让未启用 Feature 的表进入 Policy 和 Archive 分支；
- 外部上传、重试和 Manifest 状态会污染 TN 后台 Merge；
- 无法保持普通 Merge 的故障域和行为稳定。

### 复制一套 Merge Engine

- 重复排序、writer、transfer、schema 演进和 tombstone 逻辑；
- 修复需要维护两套实现；
- 长期数据格式和正确性会漂移。

### 先用大批量 SQL DELETE 作为正式实现

- 会产生大量 tombstone 和后续 Merge 放大；
- 行级事务、WAL/RPC 和索引更新难以支撑 TB 级；
- 可以用于小规模实验或对照测试，不能作为 Commercial GA 主路径。

## 实施顺序

```text
Read-only Planner/Object Index/Dry-run
  -> Export-only Parquet + verification
  -> replayable commit/source-ref/GC/admission/resource protocol
  -> Whole Object TTL/Archive
  -> Mixed Object Lifecycle Rewrite Executor
  -> direct-readable and restore-required Archive/Restore/Purge
  -> reservation、聚簇建议等优化
```

前两步可以在不退休活动数据的情况下先验证功能和成本。只要开始从活动表移除数据，commit、GC、dependency admission 和资源上限就属于正确性前提，不得以“后续优化”为由跳过。

## 影响

正面影响：

- 非分区表和 TB 级表使用统一机制；
- 普通 Merge 策略不受 Lifecycle Policy 侵入；
- Job、事务和失败范围有明确上限；
- 可以根据 whole/mixed 比例解释实际重写成本。

代价：

- Mixed Object 必须读取并重写存活行；
- 需要新的 Lifecycle Commit、exact source ref 和 dependency handler；
- Object Index 必须处理 generation、断档恢复和 obsolete GC；
- 生命周期列不聚簇时重写放大会较高。

## 重新评估条件

只有同时满足以下条件才重新评估“SQL Partition 作为可选 fast path”，且不会改变本 ADR 的默认对象级边界：

- Partition 成为可证明独立的物理对象集合而不是隐藏子表放大；
- partition retirement、index、CDC、GC 和 DDL 已有完整原子协议；
- 大量表/分区 Catalog 与调度基准证明优于对象级路径；
- 非分区表仍由对象级路径完整支持。
