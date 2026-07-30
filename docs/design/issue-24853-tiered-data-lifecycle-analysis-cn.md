# MatrixOne Issue #24853：分层数据生命周期行业调研与历史结论

> **非规范文档。** 实现只引用[概要设计](issue-24552-24853-tae-object-lifecycle-overview-design-cn.md)、
> [ADR](issue-24552-24853-object-lifecycle-boundary-adr-cn.md)和
> [Commercial GA实现设计](tae-object-lifecycle/commercial-ga-implementation-design-cn.md)。
>
> 本文保留行业概念、成本原理和方案演进，不再保存已经废弃的实现协议。

## 1. 问题本质

数据生命周期包含三个独立问题：

| 问题 | 目的 | 典型能力 |
|---|---|---|
| 历史版本保留 | 恢复误操作或时间点状态 | Snapshot、PITR、Fail-safe |
| 业务数据生命周期 | 某行何时删除或归档 | TTL、Archive、Purge |
| 物理存储放置 | 活动数据放在哪种介质 | cache、object storage class |

Issue #24853需要的是第二类。不能用PITR代替业务Archive，也不能让Bucket Lifecycle直接
迁移TAE仍在使用的Object。

## 2. 对象存储已经很便宜，Archive还省什么

HOT和Archive都可能位于S3/OSS/COS兼容存储。收益来自：

1. 退休在线TAE Object，缩小查询、Merge、checkpoint、GC和Metadata工作集；
2. Parquet/ZSTD只保留恢复所需的数据和schema，不保留在线MVCC/索引/多版本负担；
3. Provider支持时，Archive Stage可选择更便宜的storage class；
4. 归档不进入CN cache和普通查询路径。

相同逻辑数据使用相同压缩算法，不会因为叫“冷数据”就自动获得更高压缩率。成本模型必须
分别展示活动引擎成本、Payload字节、请求/取回费和Restore计算成本。

## 3. COOL、COLD和ARCHIVE

这些词不是统一行业标准：

- Online cool/cold通常仍可直接GET，只是单价、取回费或延迟不同；
- Deep archive可能需要异步thaw数小时；
- 不同云厂商同名层级的最低存储时间和API语义不同。

首个MO GA不提供ONLINE_COLD和Deep Archive，只提供：

```text
ACTIVE TAE data
  -> direct-readable Parquet/ZSTD Archive
  -> Restore to a new table when needed
```

## 4. Snowflake的实际产品边界

Snowflake Storage Lifecycle Policies的价值不是每天扫描所有行并重新压缩：

- 引擎维护自己的微分区metadata；
-策略根据metadata选择可能满足条件的微分区；
- 全部满足时可按完整物理单元处理；
- 混合单元需要重写或由后台维护重新组织；
- archived data离开普通在线查询路径，访问前需要Restore；
- 不同层的核心差异是可访问性、恢复流程和计费，而不是SQL压缩算法突然改变。

MO可借鉴“metadata筛选、copy/verify/retire、显式Restore”，不能照搬Snowflake内部协议。

## 5. Doris、StarRocks等方案

开源分析系统常见方式包括：

- Partition/tablet级TTL或storage cooldown；
- 本地盘到对象存储的tier迁移；
- compaction时淘汰过期行；
- shared-data架构中的cache与对象存储分离。

这些系统通常以Partition/tablet/segment作为天然物理边界。MO当前SQL Partition不是可靠的
TAE文件所有权边界，因此最终选择TAE Object。

## 6. MO方案演进

### 已否决

- 强制客户按时间Range Partition；
- Bucket Lifecycle直接迁移活动Object；
- 修改普通Merge策略承载Archive；
- 大Mixed写海量Tombstone；
- 查询时隐式TTL filter；
- 新建完整第二套Merge/事务/WAL/GC；
- 为尚未复现的通用MO事务问题建设Lifecycle私有Journal。

### 当前路线

```text
只扫描显式Binding
-> TAE Metadata有界分类
-> Whole exact retire
   或单源Mixed Rewrite
-> Parquet/ZSTD + full readback
-> 普通事务原子发布Dataset并退休exact Object
-> Restore到独立新表
-> Cleanup Root异步清理外部副作用
```

TTL小Mixed可在严格预算内复用普通DELETE。Archive Mixed一律Rewrite。

## 7. 为什么Mixed Rewrite可以复用Merge

一个Object包含：

```text
D = snapshot时已删除
E = 已过期
L = 仍存活
```

Lifecycle读取完整原始Block，把`D ∪ E`作为`DoMergeAndWrite`的delete bitmap：

- D不输出；
- E写Archive或丢弃；
- L写新TAE Object；
- 现有Merge producer为L生成RowID transfer。

这样不产生大规模逐行Tombstone，也不修改普通SELECT。Lifecycle不自己计算mapping。

## 8. 外部格式

CSV不适合作为权威Archive格式，因为类型、NULL、时区、DECIMAL和嵌套值容易丢失语义。
Parquet/ZSTD提供列式类型、统计信息、跨系统读取和成熟生态。

不要求每天一个永久小文件，也不按月再做不可控的全量合并。Writer按目标文件大小流式切分，
Manifest记录稳定ordinal；后续是否做Archive compaction是独立优化，不属于首个GA。

## 9. 当前安全边界

- Archive full readback后才能退休源数据；
- Dataset与Object mutation同事务；
- final transaction exact ObjectStats CAS；
- 从source snapshot TS处理post-S DELETE；
- 读取期间使用现有GC SyncProtection；
- 第一次副作用前创建Cleanup Root；
- Stage位置不可变、凭据handle可轮换；
- Restore/Purge使用有期限lease；
- commit unknown不猜测、不清理；
- 所有队列、内存、I/O和staging有硬上限。

## 10. DDL fence状态

设计保留DDL fence，但排在最后实现：

- 当前没有证据证明普通MO Merge/DDL存在通用正确性Bug；
- 先完成Object路径并建立并发测试；
- 通用Bug走公共Issue和公共修复；
- Lifecycle独有缺口才给绑定表增加复用`mo_tables`行锁的薄校验；
- 不建立Feature Guard或active-attempt状态机。

## 11. 客户A示例

支付客户按`created_at`设置90天Archive：

1. 近90天保留在活动表，正常查询和更新；
2. 90天以前的数据由后台按Object处理；
3. 归档成功后原表不再看见这些行；
4. 调查时Restore指定Dataset到新表；
5. 达到最终期限后Purge；
6. DROP源表或账户后不再承诺Restore，后台异步清理。

如果客户要求归档数据继续毫秒级UPDATE、DROP后仍强制保留七年，首个GA不适用。

## 12. 最终结论

MO不需要模拟多层在线存储状态。首个Commercial GA最小且完整的能力是：

> ACTIVE TAE Object → verified Parquet/ZSTD Dataset → exact source retirement →
> Restore new table → asynchronous Purge。

详细协议和GA门禁以权威实现设计为准。
