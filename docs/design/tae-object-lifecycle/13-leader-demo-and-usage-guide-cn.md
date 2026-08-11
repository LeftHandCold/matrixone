# Lifecycle Phase 1 领导演示与使用步骤

> 更新：2026-08-11
>
> 对应实现：PR [#26655](https://github.com/matrixorigin/matrixone/pull/26655)、Issue [#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 实现原理：[11-domain-model-and-data-flow-cn.md](11-domain-model-and-data-flow-cn.md)
>
> 测试证据：[12-phase1-e2e-test-summary-cn.md](12-phase1-e2e-test-summary-cn.md)

本文面向领导演示、产品介绍和测试交接，重点回答四个问题：用户怎么配置、后台做了什么、
数据库状态怎么变化、归档数据怎么恢复。推荐现场逐步执行，每完成一步就停下来观察表和
Lifecycle 状态；正常演示不启用故障注入，不运行长时间 soak。

## 1. 演示要让领导看到什么

一场 10～15 分钟的演示只需要说明下面四个产品效果：

1. 普通业务表可以通过一条 `ALTER TABLE ... SET LIFECYCLE` 绑定归档策略；
2. 到期历史行由后台任务异步归档，未到期在线行继续保留在普通 MatrixOne 表中；
3. 归档成功后形成可观察的 `PUBLISHED` Dataset，数据保存在 Archive Stage；
4. 用户既可以恢复完整 Dataset，也可以按源表和时间范围恢复到独立新表，并通过
   `SHOW LIFECYCLE RESTORES` 查看过程与结果。

推荐使用两个业务场景：

| 场景 | 初始状态 | Lifecycle 后 | 要说明的效果 |
| --- | ---: | ---: | --- |
| Whole | 8 条历史行 | active=0，Archive=8 | 整个旧 Object 可以退休 |
| Mixed | 8 条历史行 + 8 条未来在线行 | active=8，Archive=8 | 只归档过期行，在线行继续服务 |

演示完成后再恢复：Whole Dataset 恢复 8 行，Mixed Dataset 恢复 8 行；对 Mixed 做
`[2020-02-01, 2020-03-01)` 的范围恢复，只得到 3 条 2 月数据。

## 2. 15 分钟演示顺序

| 时间 | 操作 | 数据库里应看到的变化 | 讲解重点 |
| --- | --- | --- | --- |
| 0～1 分钟 | 环境检查 | 2 CN、1 TN、1 Log、1 Proxy、MinIO 正常 | 这是正规多服务拓扑，不是多个 QuickStart |
| 1～2 分钟 | 创建隔离 tenant/database/Stage | 新数据库和归档 Stage 可见 | 每次演示使用独立对象存储前缀 |
| 2～4 分钟 | 写入 Whole/Mixed 数据 | Whole=8，Mixed=16，Binding=0 | 此时就是普通在线业务表 |
| 4～5 分钟 | `SET LIFECYCLE` | 两个 Binding 为 `ACTIVE/ARCHIVE` | SQL 很短，后台归档异步运行 |
| 5～8 分钟 | 等待并观察 Archive | Whole active=0；Mixed active=8；Dataset=`PUBLISHED` | 历史数据退出在线层，在线数据不受影响 |
| 8～10 分钟 | Dataset Restore | 两张独立恢复表各 8 行，Attempt=`DONE` | Restore 不覆盖源表 |
| 10～12 分钟 | Range Restore | 只恢复 2 月的 3 行，Attempt=`RANGE/DONE` | 范围语义为 `[from,to)` |
| 12～15 分钟 | 最终逐行对照 | Whole 0+8=8；Mixed 8+8=16 | 数据没有丢失或重复 |

## 3. 演示前准备

### 3.1 启动当前 Kind 演示集群

当前 50 演示环境使用 Kind `mo-lifecycle-range`。如果节点容器处于停止状态：

```bash
docker start mo-lifecycle-range-control-plane

kubectl --context kind-mo-lifecycle-range \
  wait --for=condition=Ready node --all --timeout=120s

kubectl --context kind-mo-lifecycle-range \
  -n mo-lifecycle-range get pods
```

预期看到 `mo-cn1`、`mo-cn2`、`mo-tn`、`mo-log`、`mo-proxy` 和 `minio` 全部为
`Running/Ready`。

另开一个终端保持 Proxy 端口转发：

```bash
kubectl --context kind-mo-lifecycle-range \
  -n mo-lifecycle-range \
  port-forward svc/mo-proxy 6005:6009
```

### 3.2 设置本次独立演示 ID

在演示操作终端执行：

```bash
cd /home/mo/matrixone

export LIFECYCLE_DEMO_CONFIG=$PWD/test/lifecycle-demo/config.range-e2e.env
export RUN_ID=leader_$(date +%m%d_%H%M)
```

`RUN_ID` 会进入 tenant、database、table、Stage 和 MinIO prefix 名称。每次使用新值，避免
上一次演示的 Dataset、Restore Attempt 或 Root 影响本次画面。

现场不要执行 `normal-all`。它适合彩排；正式演示应逐条执行下面的命令，看到结果并解释后
再继续。

## 4. 第一步：确认环境

执行：

```bash
test/lifecycle-demo/run-demo.sh preflight
```

脚本检查：

- Lifecycle system Cleanup Root 表存在；
- `LIFECYCLE` Feature row 存在；
- 2 CN、1 TN、1 Log、1 Proxy、MinIO 均为 Ready；
- 所有 MO 进程使用 `/mo-service -cfg ...`；
- `/proc/1/cmdline` 中没有 `-launch`；
- MinIO bucket 可访问。

这一阶段不修改业务数据。可以向领导说明：每个 Pod 只启动自己的服务角色，后台归档使用
真实 MinIO Stage，不是本地文件拷贝演示。

## 5. 第二步：创建演示租户、数据库与 Stage

执行：

```bash
test/lifecycle-demo/run-demo.sh setup
```

脚本会输出本次的 tenant、account ID、database、Stage 和 Archive URL。逻辑上对应的用户操作是：

```sql
CREATE DATABASE lifecycle_demo;

CREATE STAGE archive_stage
URL='s3://<bucket>/<isolated-prefix>/archive/'
CREDENTIALS={
  'PROVIDER'='minio',
  'ENDPOINT'='http://<minio-service>:9000',
  'AWS_REGION'='us-east-1'
}
ENABLE=true;
```

生产环境中，Feature release gate 和 Stage certificate 由管理员预先配置。普通用户使用已授权
Stage，不需要了解 Cleanup Root、credential handle 等内部协议。

此时数据库变化是：

```text
tenant / database       已创建
archive Stage           ENABLED
Lifecycle 业务表        尚未创建
Dataset / Restore       0
```

## 6. 第三步：写入普通业务数据，观察归档前状态

执行：

```bash
test/lifecycle-demo/run-demo.sh normal-prepare
```

脚本创建：

```sql
CREATE TABLE <whole_table> (
  id INT PRIMARY KEY,
  created_at DATETIME NOT NULL,
  payload VARCHAR(32)
);

CREATE TABLE <mixed_table> (
  id INT PRIMARY KEY,
  created_at DATETIME NOT NULL,
  payload VARCHAR(32)
);
```

Whole 写入 8 条 2020 年历史数据。Mixed 写入：

- 8 条分布在 2020 年 1～6 月的历史数据；
- 8 条 2030 年未来数据，用来代表仍需在线服务的数据。

脚本输出应为：

```text
whole  row_count=8   id_sum=36
mixed  row_count=16  id_sum=24072

mixed expired  row_count=8  id_sum=8036
mixed live     row_count=8  id_sum=16036

Lifecycle bindings=0
```

这一步是演示的“Before”画面。向领导说明：Lifecycle 尚未配置，两张表就是普通 MatrixOne
表，所有行都在活动数据层可见。

可以在 MySQL 窗口查看：

```sql
SELECT * FROM <whole_table> ORDER BY id;
SELECT * FROM <mixed_table> ORDER BY id;
```

## 7. 第四步：用公开 SQL 启用 Lifecycle

执行：

```bash
test/lifecycle-demo/run-demo.sh normal-bind
```

现场重点展示真正的产品 SQL：

```sql
ALTER TABLE <whole_table> SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL 1 DAY,
  ACTION ARCHIVE,
  STAGE <archive_stage>,
  PURGE ELIGIBLE AFTER INTERVAL 2 DAY
);

ALTER TABLE <mixed_table> SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL 1 DAY,
  ACTION ARCHIVE,
  STAGE <archive_stage>,
  PURGE ELIGIBLE AFTER INTERVAL 2 DAY
);
```

演示使用 1 天 TTL 和 2 天 Purge 窗口只是为了立即命中固定历史数据；生产环境可以配置为
90 天、365 天等业务保留周期。

预期状态：

```text
<whole_table>  ACTIVE  ARCHIVE  expire_after_days=1
<mixed_table>  ACTIVE  ARCHIVE  expire_after_days=1
```

也可以使用公开命令查看策略：

```sql
SHOW LIFECYCLE FOR TABLE <database>.<whole_table>;
SHOW LIFECYCLE FOR TABLE <database>.<mixed_table>;
```

要讲清楚两个时序：`ALTER TABLE` 只负责原子写入 Binding，很快返回；真正的 Object 扫描、
Provider PUT/readback 和 source retirement 由后台 Coordinator 异步完成，业务连接不等待对象
存储操作。

## 8. 第五步：观察后台 Archive 后的状态变化

执行：

```bash
test/lifecycle-demo/run-demo.sh normal-await
```

通常需要等待一个或两个调度周期。脚本会一直等待到两张表的 Dataset 都已发布，再打印：

| 观察项 | 归档前 | 归档后 |
| --- | ---: | ---: |
| Whole active | 8 | 0 |
| Whole Dataset | 0 | `PUBLISHED / 8` |
| Mixed active | 16 | 8 |
| Mixed Dataset | 0 | `PUBLISHED / 8` |
| Cleanup Root | 0 | `PUBLISHED`，`last_error` 为空 |

此时查询 Mixed 表：

```sql
SELECT id, created_at, payload
FROM <mixed_table>
ORDER BY id;
```

只会看到 ID 2001～2008 的 `live-*` 行。ID 1001～1008 的历史行不再占用在线活动表，但已经
由 `PUBLISHED` Dataset 持有，不是被无记录删除。

公开状态查询：

```sql
SHOW LIFECYCLE DATASETS FOR TABLE <database>.<whole_table> LIMIT 100;
SHOW LIFECYCLE DATASETS FOR TABLE <database>.<mixed_table> LIMIT 100;
SHOW LIFECYCLE JOBS LIMIT 100;
```

向领导说明后台数据流：

```text
TAE Object
  → 判断 expired/live
  → expired 写 Parquet/ZSTD 到 Stage
  → 全量回读并校验 hash/schema/row count
  → 普通 MO 事务发布 Dataset、退休 source
  → Mixed 同一事务发布新的 live Object
```

在 Dataset `PUBLISHED` 前，源 Object 不会退休；所以用户看到的状态不会是“在线数据已经消失，
但归档还没成功”。

## 9. 第六步：恢复完整 Dataset

执行：

```bash
test/lifecycle-demo/run-demo.sh normal-restore
```

核心产品 SQL：

```sql
RESTORE ARCHIVE DATASET '<whole-dataset-id>'
TO TABLE <whole_restored>;

RESTORE ARCHIVE DATASET '<mixed-dataset-id>'
TO TABLE <mixed_restored>;
```

预期：

```text
whole_restored  rows=8  id_sum=36
mixed_restored  rows=8  id_sum=8036
```

Restore 始终创建一张独立新表，不覆盖源表，也不会把历史行自动混回当前在线表。这样用户可以
先查询、导出、审计或对比，再自行决定后续业务动作。

查看恢复历史：

```sql
SHOW LIFECYCLE RESTORES;
```

应看到两个记录：

```text
scope=DATASET
dataset_count=1
lifecycle_column_type=DATETIME
state=DONE
restored_rows=8
```

## 10. 第七步：按时间范围恢复

执行：

```bash
test/lifecycle-demo/run-demo.sh normal-range
```

核心 SQL：

```sql
RESTORE ARCHIVE TABLE <database>.<mixed_table>
BETWEEN '2020-02-01 00:00:00' AND '2020-03-01 00:00:00'
TO TABLE <mixed_feb_restored>;
```

Phase 1 的范围语义是半开区间 `[from,to)`：包含 2 月 1 日，不包含 3 月 1 日。该样本应只
恢复：

```text
1002  2020-02-01  expired-feb-left
1003  2020-02-10  expired-feb-mid
1004  2020-02-28  expired-feb-last
```

最终 `rows=3`、`id_sum=3009`。ID 1005 的时间正好是 3 月 1 日，因此不在结果中。

再次执行：

```sql
SHOW LIFECYCLE RESTORES;
```

除前面的 `DATASET/DONE` 外，还应看到：

```text
scope=RANGE
lifecycle_column_type=DATETIME
state=DONE
restored_rows=3
```

范围 Restore 会在开始时冻结命中的 Dataset 集合。恢复过程中即使又发布了时间重叠的新
Dataset，本次重试仍使用原集合，不会把结果悄悄扩大。

## 11. 第八步：展示最终业务视图与数据守恒

执行：

```bash
test/lifecycle-demo/run-demo.sh normal-show
```

脚本逐行打印 active 和 restored 数据。最终核心结果：

| 场景 | Active | 完整 Dataset Restore | Active + Restore |
| --- | ---: | ---: | ---: |
| Whole | 0 | 8 | 8 |
| Mixed | 8 | 8 | 16 |

Mixed 的最终分布是：

- `expired-*` 只在完整恢复表；
- `live-*` 只在 active 表；
- Range Restore 是归档数据的一个 3 行查询副本，不改变 Dataset 和源表；
- 没有同一 ID 同时出现在 active 与完整 Restore，也没有缺失。

结束时的建议讲法：

> 用户只需要创建 Stage 并给表绑定 Lifecycle。后台自动把到期数据从在线 TAE Object 转为
> 可验证的 Archive Dataset，未到期数据继续在线。需要历史数据时，可以恢复一个 Dataset，
> 也可以按原表时间范围恢复到独立新表，所有操作和状态都能通过 SHOW 查询。

## 12. 现场手工连接与观察 SQL

脚本在 `setup` 后打印实际 tenant 和 database。当前演示配置的连接形式为：

```bash
mysql -h127.0.0.1 -P6005 \
  -u "lc_range_${RUN_ID}:admin" -p'<password>' \
  -D "rangee2e_${RUN_ID}"
```

建议准备一个单独的 MySQL 窗口，按步骤执行：

```sql
-- 业务表是否存在。
SHOW TABLES;

-- 归档前后直接观察在线行。
SELECT * FROM ld_<run_id>_whole ORDER BY id;
SELECT * FROM ld_<run_id>_mixed ORDER BY id;

-- 查看策略、Dataset、任务和 Restore。
SHOW LIFECYCLE FOR TABLE rangee2e_<run_id>.ld_<run_id>_mixed;
SHOW LIFECYCLE DATASETS FOR TABLE rangee2e_<run_id>.ld_<run_id>_mixed LIMIT 100;
SHOW LIFECYCLE JOBS LIMIT 100;
SHOW LIFECYCLE RESTORES LIMIT 100;

-- 查看恢复后的逐行结果。
SELECT * FROM ld_<run_id>_mixed_restored ORDER BY id;
SELECT * FROM ld_<run_id>_mixed_feb_restored ORDER BY id;
```

如果需要更底层的测试证据，可以查询 tenant Catalog 的 Dataset/Attempt 和 SYS Catalog 的
Cleanup Root；领导演示主线不需要展示内部表，公开 `SHOW` 命令已经足够解释产品效果。

## 13. 正常运维控制方法

绑定后可以暂停、恢复或解除策略：

```sql
ALTER TABLE <table> PAUSE LIFECYCLE;
ALTER TABLE <table> RESUME LIFECYCLE;
ALTER TABLE <table> UNSET LIFECYCLE;
```

- `PAUSE`：停止该 Binding 启动新的退休工作，已经发布的 Dataset 保留；
- `RESUME`：恢复后台扫描；
- `UNSET`：移除 Binding，表回到未绑定状态；已有 Dataset 不会因为 UNSET 被同步删除。

`EXPIRE AFTER` 表示数据达到最短年龄，不承诺阈值到达的瞬间立刻完成归档。实际时延取决于
调度周期、Object 布局、系统资源和 Provider 状态。

## 14. 给领导常见问题的回答

### 14.1 Active 数据在哪里

未到期数据仍在普通 MatrixOne TAE Object 中，普通查询、INSERT、UPDATE、DELETE、Merge、
checkpoint、WAL、Replay 和 GC 使用现有路径。未绑定表不进入 Lifecycle 扫描。

### 14.2 Archive 数据在哪里

实际行数据以 Parquet/ZSTD Payload 保存在用户配置的 Archive Stage，Manifest 保存 schema、
文件、Row Group、hash、行数和时间范围。tenant Catalog 的 Dataset 保存可见性和 Restore
元数据，不把归档行塞进系统表。

### 14.3 为什么归档不是 SQL 返回时立即完成

`SET LIFECYCLE` 是策略配置。后台需要扫描 Object、写 Provider、完整回读、校验内容，再用
普通 MO 事务原子发布 Dataset 和退休源 Object。异步执行可以避免业务 DDL 长时间等待对象
存储。

### 14.4 Archive 数据能否像普通在线表一样直接查询

Phase 1 不实现 `ONLINE_COLD` 查询层。需要历史数据时，将 Dataset 或时间范围恢复到独立新表
后按普通 SQL 查询。

### 14.5 Restore 会不会覆盖当前源表

不会。Phase 1 Restore 只发布独立新表，不覆盖原表，不自动恢复源表的索引、FK、CDC 或
Publication 依赖。

### 14.6 这次演示是否代表 Commercial GA 完成

不是。演示证明功能怎么使用和小数据结果正确；完整测试证据及未完成的容量、故障、放量和
soak 门禁见 [12-phase1-e2e-test-summary-cn.md](12-phase1-e2e-test-summary-cn.md)。

## 15. 已完成彩排结果

`leader_rehearsal_0810` 已完整跑通正常流程：

| 检查项 | 结果 |
| --- | --- |
| Whole | active=0，Dataset Restore=8 |
| Mixed | active=8，Dataset Restore=8 |
| Dataset | 2 个，均 `PUBLISHED` |
| Dataset Restore | 2 个，均 `DONE` |
| Cleanup Root | 2 个，均 `PUBLISHED`，`last_error` 为空 |
| 数据守恒 | Whole 0+8=8；Mixed 8+8=16 |

Range Restore 的相同产品 SQL 已在独立 E2E 中验证 `[2020-02-01,2020-03-01)` 左闭右开、
3 行结果和 `RANGE/DONE` 可见性。正式演示前建议使用新的 `RUN_ID` 运行一次 `normal-all`
彩排；正式现场仍逐步运行，确保每个状态变化都能停下来解释。
