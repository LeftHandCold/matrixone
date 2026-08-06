# Lifecycle Phase 1 领域模型与数据流

## 1. 目的与范围

本文从使用者和实现者共同需要的角度，定义 Lifecycle Phase 1 的核心对象、责任边界和端到端数据流。

Phase 1 支持：

- 对显式绑定表执行 TTL 删除或 Archive；
- Archive 将已过期数据写入外部 Stage，并在验证成功后退休源 TAE Object；
- 将**单个 Dataset**恢复到一个独立新表；
- 在失败、进程重启和对象存储迟到写入后安全清理未发布副作用。

Phase 1 不把 Archive 变成独立 Backup/DR：不支持恢复回原表、范围 Restore、多 Dataset 异步父任务、ONLINE_COLD、Deep Archive、CDC/CCPR 或 Lifecycle-aware Backup/DR。

## 2. 核心对象总览

```text
Binding（表策略）
  │ 有界扫描 TAE Object
  ▼
Archive Attempt + Cleanup Root（一次执行与外部副作用所有权）
  │ 写 Payload / Manifest，full readback 验证
  ▼
Dataset（一次成功发布的归档数据集）
  │ 用户选择一个 Dataset
  ▼
Restore Attempt + Chunk Receipt（恢复到独立新表）
```

| 对象 | 粒度 | 主要责任 | 终态/保留 |
|---|---|---|---|
| Binding | 一张启用表 | 策略、schema/table fence、扫描游标 | UNSET 删除；不删除历史 Dataset |
| Archive Attempt | 一次 Object 处理 | 本次 worker 的执行身份 | 成功发布或失败清理后结束 |
| Cleanup Root | 一次 Attempt 的副作用根 | Payload、booking、TAE staging 的清理 Owner | 物理清理完成后 CLEANED |
| Payload | 外部对象文件 | 实际归档行数据 | 由 Root 异步物理删除 |
| Manifest | 一个 Dataset | Payload 目录、Schema、Hash、固定 chunk 边界 | 随 Dataset 保留/清理 |
| Dataset | 一次成功 Archive 发布 | 用户可见、Restore/Purge 的归档单元 | PUBLISHED → DELETE_PENDING → PURGED |
| Restore Attempt | 一次恢复动作 | 隐藏表、lease、进度、发布/清理 Owner | DONE 或清理终结 |
| Chunk Receipt | 一个固定 Restore chunk | 防重复导入及可恢复进度 | 随 Restore Attempt 回收 |

## 3. Binding：表级策略合同

Binding 由 `ALTER TABLE ... SET LIFECYCLE` 创建。它不保存归档数据，也不是一次后台任务；它定义的是某张表后续如何被 Lifecycle 处理。

Binding 至少冻结：

- 逻辑表和当前物理表身份；
- Lifecycle 时间列；
- `EXPIRE AFTER`、TTL/Archive action、Stage 和 `PURGE ELIGIBLE AFTER`；
- 绑定 generation、schema digest/physical table fence；
- 有界扫描 cursor、最近扫描/成功/延迟信息。

Lifecycle Scheduler 只枚举 Binding。未绑定表不会进入 Lifecycle 扫描，普通查询、DML、普通 Merge、WAL、Replay、GC 均不访问 Lifecycle Catalog。

一张表可产生多个 Dataset。例如每天归档一次，半年可能有约 180 个 Dataset；Binding 是表级长期策略，Dataset 是一次成功归档的发布单元。

## 4. Archive Attempt 与 Cleanup Root：一次执行和副作用所有权

Archive Attempt 是一次 Whole Object 或 Mixed Object 处理的执行身份。其关键身份为：

```text
root_id + attempt_id
```

每个 Attempt 使用新的、不可复用的 namespace：

```text
archive/<root_id>/<attempt_id>/payload-<ordinal>-<write-id>.parquet
archive/<root_id>/<attempt_id>/manifest-<digest>.json
tae-staging/<root_id>/<attempt_id>/...
booking/<root_id>/<attempt_id>/...
```

这避免旧 worker 的迟到 PUT 覆盖新 worker 已 readback 验证的数据。

Cleanup Root 是外部副作用的唯一 Owner。在第一次 Provider PUT、multipart、TAE live staging 或 external booking 前，必须先持久化 Root。Root 保存：

- `root_id`、`attempt_id`、owner account/table；
- 冻结的 Archive Stage identity 与 credential handle；
- Archive、booking、TAE staging namespace；
- manifest 身份、资源预留和 cleanup deadline；
- 状态、版本和 executor identity。

典型状态流转：

```text
REGISTERED → UPLOADING → VERIFIED → FINALIZING → PUBLISHED
                 │             │          │
                 └─────────────┴──────────┴→ DELETE_PENDING → DELETING → CLEANED

FINALIZING → COMMIT_UNKNOWN
COMMIT_UNKNOWN → PUBLISHED（发现 matching Dataset/Receipt）
COMMIT_UNKNOWN → DELETE_PENDING（确认未发布且可安全清理）
```

职责边界：

- Dataset 发布前，Root 拥有全部 Archive Payload、booking 和未发布 staging 的清理责任；
- Dataset 发布后，Dataset 决定是否可 Restore、保留期和 Purge；Root 继续执行 Provider 物理删除；
- 已成功提交的 live TAE Object 交回现有 WAL/GC，不建立 Lifecycle 私有 GC。

## 5. Payload 与 Manifest：归档内容和不可变目录

### 5.1 Payload

Payload 是外部 Stage 中真正的归档数据文件，格式为 Parquet + ZSTD。选择它是为了保留列式类型、NULL 和批量读取能力，并固定 Row Group，以支持稳定的可重试 Restore chunk；CSV 会引入 NULL、转义、Decimal、Timestamp、JSON 和二进制类型的恢复歧义。

每个 Payload 文件至少有稳定 ordinal、对象 key、size、SHA-256、行数和 Row Group 信息。Archive 写入完成后必须 full readback，重新验证 schema、内容 hash、文件 hash 和行数。

### 5.2 Manifest

Manifest 是一个小的、不可变的 JSON 目录，不承载业务行数据。其版本化内容包括：

- `manifest_format_version`、canonical encoder/hash formula version；
- Root/Attempt/Dataset 与冻结 Stage identity；
- 完整逻辑 Schema Descriptor；
- 文件及 Row Group 的固定顺序、key、size、SHA-256、行数、logical bytes；
- 总行数、逻辑字节、内容 hash；
- AUTO_INCREMENT 列最大值等恢复所需元数据。

Schema digest 只用于验证一致性，不能单独用于创建恢复表；因此 Manifest 同时保存可读的、版本化逻辑 Schema Descriptor。

## 6. Dataset：用户可操作的发布单元

Dataset 只在 Archive 成功后出现。它表示：

> 一批历史数据已写入并 full-readback 验证，且 Dataset/Receipt 发布与源 Object 退休在同一个普通 MO 事务内完成。

最终事务原子完成：

```text
写 Dataset/TTL Receipt
+ thin Lifecycle retire entry
+ exact source Object CAS / SoftDelete
= 普通 MO 事务提交
```

因此禁止以下状态：

- Dataset 已发布但源 Object 未退休；
- 源 Object 已退休但 Payload/readback 未完成；
- source CAS 失败但 Dataset 仍发布。

Dataset 保存 `dataset_id`、源表/Binding generation、归档时间范围、Root/Attempt、Stage/Manifest identity、schema/content hash、row count、logical bytes、Purge 时间和状态。

Dataset 生命周期：

```text
PUBLISHED → DELETE_PENDING → PURGED
```

`PURGE ELIGIBLE AFTER` 表示最早可物理删除时间，不是精确删除 deadline。Provider 故障、lease 或 cleanup retry 可使物理删除延后。

## 7. Restore Attempt 与 Chunk Receipt：恢复到独立新表

Phase 1 的 Restore 是单 Dataset Restore：

```sql
RESTORE ARCHIVE DATASET '<dataset_id>'
INTO history.events_2025_01;
```

它绝不覆盖原表。流程如下：

```text
读取 Dataset + Manifest
→ 获取 Dataset Restore lease
→ 同一普通事务创建隐藏 staging table + Restore Attempt
→ 按固定 Row Group / Chunk 导入
→ 每个 chunk 同事务写数据、Receipt 和进度
→ 验证全部 Receipt 与聚合内容 hash
→ Rename 隐藏表到目标新表，并将 Attempt 标为 DONE
```

Restore Attempt 记录 `restore_id`、Dataset、lease/deadline、隐藏表精确身份、目标新表、下一个 chunk、已恢复行数、状态、错误和最终 hash。

Chunk 的定义固定为一个 Parquet Row Group：

```text
chunk_ordinal = file ordinal 与 row-group ordinal 展平后的全局连续序号
```

每个 chunk 的普通事务为：

```text
INSERT 数据
+ INSERT Chunk Receipt(restore_id, chunk_ordinal, chunk_digest, row_count, canonical_content_hash)
+ CAS next_chunk_ordinal
+ UPDATE restored_rows
+ COMMIT
```

Chunk Receipt 的唯一键是 `(restore_id, chunk_ordinal)`：相同 digest 的重试幂等；不同 digest 代表损坏并 fail-closed。最终只读取小规模 Receipt，以稳定序号重建聚合 hash，不扫描 TB 级 staging 表或重新读取全部 Payload。

发布与清理都 CAS 同一个 Restore Attempt，并验证 staging database ID、table ID、hidden name。旧 worker 不能仅凭一个不变的 table ID 删除已经 rename 发布的新表。

## 8. TTL Receipt 与 Archive Dataset 的区别

TTL-only 不产生外部 Payload，使用 TTL Receipt 记录已安全退休的数据以支持幂等、对账和观测。

Archive 必须产生 Dataset，因为用户需要查询归档、恢复、保留和 Purge：

```text
TTL:     源 Object 退休 → TTL Receipt
Archive: Payload + Manifest full readback → 源 Object 退休 → Dataset 发布 → Restore/Purge
```

## 9. Whole 与 Mixed Object 的数据流

### Whole Object

完全过期 Object 可有界成批处理：

```text
exact Reader → Parquet/ZSTD → full readback
→ Dataset/Receipt + thin retire entry + exact Object CAS
→ 原子提交
```

### Mixed Object

Mixed 严格一次只处理一个源 Object：

```text
读取完整物理 Block
→ D：Snapshot 时已删除
→ E：已过期，写 Archive
→ L：仍存活
→ D∪E 形成 delete bitmap
→ DoMergeAndWrite 仅输出 L
→ 复用现有 TransferTable、booking、Create/Drop/Transfer
→ 最终事务原子发布 Dataset、新 L Object 并退休源 Object
```

Lifecycle 不自行生成或修补 row mapping；`DoMergeAndWrite` 是唯一 TransferTable producer。普通 Merge 的选择、排序、WAL、Replay、GC 均保持公共实现。

## 10. 用户需要理解的 Phase 1 语义

- `EXPIRE AFTER` 是最短年龄阈值，不是精确删除时间；实际延迟受扫描周期、Object 布局、系统负载和 Provider 状态影响。
- Archive 最适合 append-mostly、时间列与 Object 布局相关的时间序列表；随机时间分布可能产生 Mixed Rewrite 或 `MIXED_LAYOUT_BLOCKED`。
- `PAUSE` 停止新的退休；已发布 Dataset 仍遵守原 Purge 时间。
- `UNSET` 删除 Binding，但保留已发布 Dataset 到其 Purge 时间；再次 SET 只影响未来 Dataset。
- Restore 结果是历史数据恢复表，不是源表的完整结构克隆；Phase 1 不自动恢复 PK、Unique/Secondary Index、FK、Check、CDC/Publication 等依赖。
- `DROP TABLE/DATABASE/ACCOUNT` 明确放弃 Archive Restore 承诺，后台清理对应 Payload；Archive 不是独立 Backup。

## 11. 后续产品化方向（不属于本期）

用户最终需要按表和时间范围恢复，例如“恢复半年前的三个月”。这需要选择多个 Dataset、精确处理边界 Dataset、按 schema generation 分组并采用异步父 Restore Attempt。该能力应作为独立后续 PR，不能扩大 Phase 1 单 Dataset Restore 的事务和状态机。
