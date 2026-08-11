# Lifecycle Phase 1 测试过程、使用步骤与结果

> 更新：2026-08-11
>
> 对应实现：PR [#26655](https://github.com/matrixorigin/matrixone/pull/26655)、Issue [#24853](https://github.com/matrixorigin/matrixone/issues/24853)
>
> 实现原理：[11-domain-model-and-data-flow-cn.md](11-domain-model-and-data-flow-cn.md)
>
> 完整测试结论：[12-phase1-e2e-test-summary-cn.md](12-phase1-e2e-test-summary-cn.md)

本文记录本轮 E2E 实际做了什么、SQL 如何执行、数据库状态如何变化以及最终结果。前半部分
按正常产品使用顺序说明 Lifecycle；后半部分记录我们在此基础上追加的数据正确性、故障、
重启和规模测试。文中的 SQL 使用占位符，实际运行时每轮都使用独立 tenant、database、
Stage 和对象存储 prefix。

## 1. 整体测试流程

本轮不是只执行一条 TTL SQL，而是按下面顺序逐层验证：

```text
确认代码 SHA 和集群拓扑
  → fresh bootstrap 与 Feature 默认状态
  → 创建隔离 tenant / database / Archive Stage
  → 创建表并写入历史、在线和混合日期数据
  → flush/checkpoint，使测试数据形成可扫描 TAE Object
  → ALTER TABLE ... SET LIFECYCLE
  → 等待后台发现 Object、写 Archive、full readback、发布 Dataset
  → 检查 active 表、Dataset、Cleanup Root 和 MinIO 对象
  → RESTORE 单个 Dataset
  → RESTORE 源表时间范围
  → SHOW LIFECYCLE RESTORES
  → 用 count、ID sum、双向差集和逐列 ledger 校验数据
  → 追加 UPDATE/DELETE、Provider 故障、CN/TN 重启和规模测试
```

每个阶段只有在前一阶段通过后才继续。失败样本保留源表、Dataset、Root、Provider LIST 和
Pod 日志证据，不通过清表或重启环境把失败改写成成功。

## 2. 环境和 fresh bootstrap

有效多节点环境为：

```text
1 LogService + HAKeeper
1 TN
2 CN
1 Proxy
1 MinIO
```

所有 MO Pod 显式运行：

```yaml
command: ["/mo-service"]
args: ["-cfg", "/etc/mo-config/<component>.toml"]
```

测试先检查 `/proc/1/cmdline` 不包含 `-launch`，确认只有 Log Pod 启动 HAKeeper、只有 TN Pod
启动 TAE。这样避免镜像默认 QuickStart 与 `-cfg` 同时存在，导致每个 Pod 各自启动一套完整
集群。

fresh bootstrap 后执行：

```sql
SELECT COUNT(*)
FROM mo_catalog.mo_lifecycle_cleanup_roots;

SELECT feature_code, enabled, scope_spec
FROM mo_catalog.mo_feature_registry
WHERE feature_code = 'LIFECYCLE';
```

预期：Cleanup Root 表存在且为空；`LIFECYCLE` Feature row 存在，默认 `enabled=false`。

## 3. 创建隔离测试范围和 Archive Stage

测试脚本的 `setup` 阶段执行的主要 SQL 如下。

### 3.1 创建 tenant 和 database

SYS 连接：

```sql
CREATE ACCOUNT <tenant>
ADMIN_NAME '<admin>'
IDENTIFIED BY '<password>';
```

tenant 连接：

```sql
CREATE DATABASE <database>;
USE <database>;
```

### 3.2 创建 Stage

```sql
CREATE STAGE <archive_stage>
URL='s3://<bucket>/<run-id>/archive/'
CREDENTIALS={
  'PROVIDER'='minio',
  'ENDPOINT'='http://<minio-service>:9000',
  'AWS_REGION'='us-east-1'
}
ENABLE=true;
```

测试环境随后把该 Stage 的 account ID、stage ID、canonical URL、Provider、endpoint、region、
credential handle、versioning 和 multipart 条件写入 Lifecycle release config：

```sql
SELECT mo_feature_registry_upsert(
  'LIFECYCLE',
  'TAE object lifecycle retirement',
  '{
    "archive_stages": [{
      "account_id": <account-id>,
      "stage_id": <stage-id>,
      "canonical_url": "s3://<bucket>/<run-id>/archive/",
      "provider": "minio",
      "endpoint": "http://<minio-service>:9000",
      "region": "us-east-1",
      "credential_handle": "default",
      "versioning_disabled": true,
      "abort_incomplete_multipart": true
    }]
  }',
  true
);
```

普通使用者只需要使用已经由管理员认证和授权的 Stage；上面的 registry 配置属于 release 和
测试环境准备，不是每张业务表都要重复执行的 SQL。

## 4. 正常使用步骤

### 4.1 创建带时间列的业务表

Lifecycle 列使用 `NOT NULL DATE/DATETIME/TIMESTAMP`。测试使用：

```sql
CREATE TABLE events (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL,
  event_type VARCHAR(32) NOT NULL,
  created_at DATETIME NOT NULL,
  payload VARCHAR(256),
  amount DECIMAL(20,4)
);
```

写入历史和在线数据：

```sql
INSERT INTO events VALUES
  (1, 101, 'purchase', '2020-01-15 10:00:00', 'old-jan', 10.0000),
  (2, 102, 'purchase', '2020-02-01 00:00:00', 'old-feb-left', 20.0000),
  (3, 103, 'click',    '2020-02-10 12:00:00', 'old-feb-mid', 30.0000),
  (4, 104, 'login',    '2020-02-28 23:59:59', 'old-feb-last', 40.0000),
  (5, 105, 'refund',   '2020-03-01 00:00:00', 'old-mar-right', 50.0000),
  (6, 106, 'purchase', '2030-01-01 00:00:00', 'live-1', 60.0000),
  (7, 107, 'click',    '2030-02-01 00:00:00', 'live-2', 70.0000);
```

测试中为了及时形成可扫描 Object，执行：

```sql
SELECT mo_ctl('dn', 'flush', '<database>.events.<account-id>');
```

该 flush 是 E2E 准备手段，不属于用户配置 Lifecycle 的必要步骤，也不表示生产 Object 边界
必须由用户手工控制。

### 4.2 绑定 Lifecycle Archive 策略

```sql
ALTER TABLE events SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL 90 DAY,
  ACTION ARCHIVE,
  STAGE <archive_stage>,
  PURGE ELIGIBLE AFTER INTERVAL 365 DAY
);
```

语义：

- `COLUMN created_at`：使用该列判断数据年龄；
- `EXPIRE AFTER 90 DAY`：至少达到 90 天后才有资格被后台处理；
- `ACTION ARCHIVE`：过期行写入 Archive Stage，而不是只做 TTL 删除；
- `PURGE ELIGIBLE AFTER 365 DAY`：Dataset 到达保留期后才允许进入 Purge。

`ALTER TABLE` 只原子写入 Binding，然后返回。Object 扫描、Provider PUT/readback 和 source
retirement 在后台异步执行。

查看策略：

```sql
SHOW LIFECYCLE FOR TABLE <database>.events;
```

预期 Binding 为 `ACTIVE`，action 为 `ARCHIVE`，并显示 Lifecycle 列、过期周期、Stage 和
Purge 周期。

### 4.3 查看后台归档状态

```sql
SHOW LIFECYCLE DATASETS FOR TABLE <database>.events LIMIT 100;
SHOW LIFECYCLE JOBS LIMIT 100;
```

测试脚本还使用 tenant Catalog 做确定性断言：

```sql
SELECT LOWER(HEX(dataset_id)), state, row_count, logical_bytes
FROM mo_catalog.mo_lifecycle_datasets
WHERE source_physical_table_id = <physical-table-id>
ORDER BY created_at;
```

使用 SYS 连接检查 Cleanup Root：

```sql
SELECT HEX(root_id), physical_table_id, state, cleanup_after, last_error
FROM mo_catalog.mo_lifecycle_cleanup_roots
WHERE physical_table_id = <physical-table-id>
ORDER BY created_at;
```

正常成功路径的表现：

```text
Dataset.state = PUBLISHED
Root.state    = PUBLISHED
last_error    = empty
```

Archive 只有在 Payload、Manifest 和 Provider full readback 全部验证成功后，才用一个普通 MO
事务发布 Dataset 并退休源 Object。Mixed Object 同一事务还会发布只含 live 行的新 Object。

### 4.4 恢复一个 Dataset

先从 `SHOW LIFECYCLE DATASETS` 或 Catalog 查询取得 Dataset ID，然后执行：

```sql
RESTORE ARCHIVE DATASET '<dataset-id>'
TO TABLE events_restored;
```

Restore 创建独立新表，不覆盖当前源表。查看结果：

```sql
SELECT COUNT(*), COUNT(DISTINCT id), COALESCE(SUM(id), 0)
FROM events_restored;

SHOW LIFECYCLE RESTORES LIMIT 100;
```

成功 Attempt 的主要字段：

```text
scope=DATASET
dataset_count=1
state=DONE
restored_rows=<Dataset row_count>
```

### 4.5 按源表和时间范围恢复

```sql
RESTORE ARCHIVE TABLE <database>.events
BETWEEN '2020-02-01 00:00:00' AND '2020-03-01 00:00:00'
TO TABLE events_feb_restored;
```

当前范围语义固定为半开区间 `[from,to)`：包含 2 月 1 日，不包含 3 月 1 日。用上面的样本应
恢复 ID 2、3、4，不包含 ID 5。

```sql
SELECT id, created_at, payload
FROM events_feb_restored
ORDER BY id;

SHOW LIFECYCLE RESTORES LIMIT 100;
```

对应 Attempt 应显示 `scope=RANGE`、`state=DONE`、命中的 Dataset/Chunk 数和最终恢复行数。
初始化成功后，命中的 Dataset 集合会冻结到 Attempt；重试不会混入后续新发布的重叠 Dataset。

### 4.6 暂停、继续和解除策略

```sql
ALTER TABLE events PAUSE LIFECYCLE;
ALTER TABLE events RESUME LIFECYCLE;
ALTER TABLE events UNSET LIFECYCLE;
```

- `PAUSE`：停止该 Binding 启动新的退休工作；已经发布的 Dataset 保留；
- `RESUME`：恢复后台扫描；
- `UNSET`：删除 Binding，业务表回到未绑定状态；已有 Dataset 不会被同步删除。

## 5. 小数据 Whole / Mixed 主链实际 SQL

测试首先使用小数据把状态变化看清楚，再放大数据量。

### 5.1 Whole Archive

```sql
CREATE TABLE lifecycle_whole (
  id INT PRIMARY KEY,
  created_at DATETIME NOT NULL,
  payload VARCHAR(32)
);

INSERT INTO lifecycle_whole VALUES
  (1, '2020-01-01', 'alpha'),
  (2, '2020-01-01', 'bravo'),
  (3, '2020-01-01', 'charlie'),
  (4, '2020-01-01', 'delta'),
  (5, '2020-01-01', 'echo'),
  (6, '2020-01-01', 'foxtrot'),
  (7, '2020-01-01', 'golf'),
  (8, '2020-01-01', 'hotel');

SELECT mo_ctl('dn', 'flush', '<database>.lifecycle_whole.<account-id>');

ALTER TABLE lifecycle_whole SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL 1 DAY,
  ACTION ARCHIVE,
  STAGE <archive_stage>,
  PURGE ELIGIBLE AFTER INTERVAL 2 DAY
);
```

后台发布后执行：

```sql
SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM lifecycle_whole;

SELECT LOWER(HEX(dataset_id)), state, row_count
FROM mo_catalog.mo_lifecycle_datasets
WHERE source_physical_table_id = <whole-physical-table-id>;
```

实际结果：源表 `0/0`；一个 `PUBLISHED` Dataset，`row_count=8`。随后：

```sql
RESTORE ARCHIVE DATASET '<whole-dataset-id>'
TO TABLE lifecycle_whole_restored;

SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM lifecycle_whole_restored;
```

结果为 8 行、ID 和 36。

### 5.2 Mixed Rewrite

```sql
CREATE TABLE lifecycle_mixed (
  id INT PRIMARY KEY,
  created_at DATETIME NOT NULL,
  payload VARCHAR(32)
);

INSERT INTO lifecycle_mixed VALUES
  (1001, '2020-01-01', 'expired-1'),
  (1002, '2020-01-01', 'expired-2'),
  (1003, '2020-01-01', 'expired-3'),
  (1004, '2020-01-01', 'expired-4'),
  (1005, '2020-01-01', 'expired-5'),
  (1006, '2020-01-01', 'expired-6'),
  (1007, '2020-01-01', 'expired-7'),
  (1008, '2020-01-01', 'expired-8'),
  (2001, '2030-01-01', 'live-1'),
  (2002, '2030-01-01', 'live-2'),
  (2003, '2030-01-01', 'live-3'),
  (2004, '2030-01-01', 'live-4'),
  (2005, '2030-01-01', 'live-5'),
  (2006, '2030-01-01', 'live-6'),
  (2007, '2030-01-01', 'live-7'),
  (2008, '2030-01-01', 'live-8');

SELECT mo_ctl('dn', 'flush', '<database>.lifecycle_mixed.<account-id>');

ALTER TABLE lifecycle_mixed SET LIFECYCLE (
  COLUMN created_at,
  EXPIRE AFTER INTERVAL 1 DAY,
  ACTION ARCHIVE,
  STAGE <archive_stage>,
  PURGE ELIGIBLE AFTER INTERVAL 2 DAY
);
```

后台发布后：

```sql
SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM lifecycle_mixed;
```

实际结果为 8 行、ID 和 16036，只剩 ID 2001～2008。恢复 Dataset：

```sql
RESTORE ARCHIVE DATASET '<mixed-dataset-id>'
TO TABLE lifecycle_mixed_restored;

SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM lifecycle_mixed_restored;
```

结果为 8 行、ID 和 8036，只包含 ID 1001～1008。

最终守恒：

```text
active rows + restored rows = 8 + 8 = 16
active ID sum + restored ID sum = 16036 + 8036 = 24072
```

## 6. 流式与规模数据 SQL

小样本通过后，使用 `generate_series` 按批写入多日期数据。历史批次的核心 SQL：

```sql
INSERT INTO chronological_stream
SELECT
  result,
  result % 100000,
  CASE result % 5
    WHEN 0 THEN 'purchase'
    WHEN 1 THEN 'click'
    WHEN 2 THEN 'login'
    WHEN 3 THEN 'refund'
    ELSE 'support'
  END,
  TIMESTAMPADD(day, -(45 + (result % 120)), UTC_TIMESTAMP()),
  CONCAT('historical-', result, '-', REPEAT('x', (result % 128) + 16)),
  result % 100000
FROM generate_series(<batch-start>, <batch-end>);
```

绑定 Lifecycle 后继续写未来在线数据：

```sql
INSERT INTO chronological_stream
SELECT
  result,
  result % 100000,
  CASE result % 5
    WHEN 0 THEN 'purchase'
    WHEN 1 THEN 'click'
    WHEN 2 THEN 'login'
    WHEN 3 THEN 'refund'
    ELSE 'support'
  END,
  TIMESTAMPADD(day, 1 + (result % 90), UTC_TIMESTAMP()),
  CONCAT('live-', result, '-', REPEAT('y', (result % 128) + 16)),
  result % 100000
FROM generate_series(<batch-start>, <batch-end>);
```

每若干批次执行一次 flush，然后通过以下 SQL 检查：

```sql
SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM chronological_stream;

SELECT state, COUNT(*), COALESCE(SUM(row_count), 0)
FROM mo_catalog.mo_lifecycle_datasets
GROUP BY state;

SELECT state, COUNT(*)
FROM mo_catalog.mo_lifecycle_restore_attempts
GROUP BY state;
```

实测规模结果：

| 写入规模 | Archive / Restore | active | Dataset | 数据守恒 |
| ---: | ---: | ---: | ---: | --- |
| 120,000 | 70,000 | 50,000 | 10 | 行数和 ID 和守恒 |
| 33,000,000 | 21,000,000 | 12,000,000 | 42 | ID 和为 544,500,016,500,000 |
| 110,000,000 | 70,000,000 | 40,000,000 | 5 | ID 和为 6,050,000,055,000,000 |

110M 时间有序样本中，5 个 Restore Attempt 全部为 `DONE`，没有再出现已经修复的 Parquet
`[3520:3480]` slice-bounds panic。

## 7. UPDATE / DELETE 测试 SQL

测试不是只归档静态 INSERT 数据。另一个混合样本在绑定 Lifecycle 前执行：

```sql
UPDATE lifecycle_dml_e2e
SET payload = 'updated-old', amount = amount + 100
WHERE id BETWEEN 1 AND 1000;

UPDATE lifecycle_dml_e2e
SET payload = 'updated-live', amount = amount + 200
WHERE id BETWEEN 70001 AND 71000;

DELETE FROM lifecycle_dml_e2e
WHERE id BETWEEN 1001 AND 2000;

DELETE FROM lifecycle_dml_e2e
WHERE id BETWEEN 71001 AND 72000;

UPDATE lifecycle_dml_e2e
SET created_at = '2030-01-01', payload = 'moved-to-live'
WHERE id = 2001;

UPDATE lifecycle_dml_e2e
SET created_at = '2020-01-01', payload = 'moved-to-expired'
WHERE id = 72001;
```

绑定并归档后检查：

```sql
SELECT id, payload
FROM lifecycle_dml_e2e
WHERE id IN (2001, 70001);

SELECT COUNT(*)
FROM lifecycle_dml_e2e
WHERE id IN (1001, 71001);

SELECT COUNT(*)
FROM <restored-table>
WHERE id IN (1001, 71001);
```

3M 放大样本最终：2,030,000 行 Archive/Restore，870,000 行 active；`updated-old` 在 Restore
保持最新值，`updated-live` 在 active 保持最新值，两段 DELETE 在两边都不存在，合计 ID 和
4,485,001,450,000。

12,000 行 reference ledger 进一步执行双向差集：

```sql
-- active 中不允许存在不符合 ledger 的行。
SELECT COUNT(*)
FROM <active-table> a
LEFT JOIN <expected-ledger> e ON e.id = a.id
WHERE e.id IS NULL
   OR e.expected_location <> 'ACTIVE'
   OR NOT (a.created_at <=> e.created_at)
   OR NOT (a.payload <=> e.payload);

-- ledger 期望的 active 行必须全部存在。
SELECT COUNT(*)
FROM <expected-ledger> e
LEFT JOIN <active-table> a ON a.id = e.id
WHERE e.expected_location = 'ACTIVE'
  AND a.id IS NULL;
```

Archive/Restore 方向使用相同的双向比较，并检查 `COUNT(DISTINCT id)` 和 DELETE 泄漏。最终
`active_mismatch`、`archive_mismatch`、`missing_rows`、`unexpected_rows`、
`duplicate_ids`、`deleted_leaks` 全部为 0。

## 8. Range Restore 实际 SQL 与结果

单 Dataset 范围测试：

```sql
RESTORE ARCHIVE TABLE <database>.<source-table>
BETWEEN '2020-02-01 00:00:00' AND '2020-03-01 00:00:00'
TO TABLE range_restored;

SELECT id, created_at
FROM range_restored
ORDER BY id;

SHOW LIFECYCLE RESTORES LIMIT 100;
```

实际只恢复 ID 2、3、4，共 3 行、ID 和 9；左端包含、右端排除。

跨 Dataset 测试从 10 个 `PUBLISHED` Dataset 中选择重叠范围：恢复 17,493 行，ID 和
874,502,559，与逐 Dataset 恢复后独立过滤的基线一致。

还执行了：

- 同一 Range SQL 重试：不新增 Attempt、不重复导入；
- 空区间、无交集、目标表已存在：全部在副作用前失败；
- `after-restore-publish` 响应丢失：首次 SQL 返回错误，但目标表和唯一 `RANGE/DONE` Attempt
  已提交；重试后仍为 3 行、Attempt 数仍为 1；
- 另一个租户执行 `SHOW LIFECYCLE RESTORES`：返回 0 行，证明租户隔离。

## 9. 故障测试中执行的 SQL

内置 readback fault 样本先启用故障注入：

```sql
SELECT enable_fault_injection();

SELECT add_fault_point(
  'tae-object-lifecycle/before-full-readback',
  ':::',
  'echo',
  0,
  'test-readback'
);
```

然后对新表执行与正常路径相同的 INSERT、flush 和 `ALTER TABLE ... SET LIFECYCLE`。等待 Root
出现后查询：

```sql
SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM lifecycle_fault_source;

SELECT COUNT(*)
FROM mo_catalog.mo_lifecycle_datasets
WHERE source_physical_table_id = <fault-table-id>;
```

SYS 查询：

```sql
SELECT HEX(root_id), state, cleanup_after, last_error
FROM mo_catalog.mo_lifecycle_cleanup_roots
WHERE physical_table_id = <fault-table-id>;
```

实际结果：源表仍为 4 行、Dataset=0，Root 先进入 `DELETE_PENDING` 并记录 readback 错误。

清理故障点时向所有服务实例广播 remove，再关闭 FaultInjector：

```sql
SELECT fault_inject(
  'all.',
  'remove_fault_point',
  'tae-object-lifecycle/before-full-readback'
);

SELECT disable_fault_injection();
```

随后等待 `cleanup_after + quiescence` 并重复查询 Root/MinIO prefix。独立样本最终 Root 为
`CLEANED`，精确 prefix 为空，源数据从未退休。

真实 Provider 故障不是用 SQL 模拟，而是把 Stage endpoint 指向可控代理，再执行相同的
Archive SQL。代理分别返回 Payload PUT 503、full-readback GET 503、Manifest PUT 429 和
Cleanup LIST 失败。每轮都使用上面的源表、Dataset 和 Root SQL 判定。结果均满足：

```text
故障期间：source 可见，Dataset=0
恢复之后：失败 Root 最终 CLEANED，精确 MinIO prefix 为空
```

## 10. Restore Chunk 重试 SQL 与结果

对 510,362 行、8 Chunk 的真实 Dataset 执行：

```sql
SELECT enable_fault_injection();

SELECT add_fault_point(
  'tae-object-lifecycle/after-restore-chunk',
  ':::',
  'echo',
  0,
  'test-after-chunk'
);

RESTORE ARCHIVE DATASET '<dataset-id>'
TO TABLE restore_chunk_target;
```

第一次执行在 ordinal 0 已提交后返回错误。此时检查：

```sql
SELECT restore_id, state, next_chunk_ordinal, restored_rows
FROM mo_catalog.mo_lifecycle_restore_attempts
WHERE target_name = 'restore_chunk_target';

SELECT chunk_ordinal, row_count
FROM mo_catalog.mo_lifecycle_restore_chunks
WHERE restore_id = <restore-id>
ORDER BY chunk_ordinal;
```

实际为唯一 Attempt、`IMPORTING`、`next_chunk_ordinal=1`、`restored_rows=65536`，Receipt 只有
ordinal 0。解除故障后重试同一 Restore SQL，复用原 restore ID 并最终 `DONE`：目标表
510,362 行、distinct ID 510,362，Receipt 共 8 个且 ordinal 连续 0～7，隐藏 staging 表为 0。

## 11. CN/TN 重启操作与检查

重启不是 SQL，但重启后使用相同 SQL 验证 Catalog 和业务数据。

TN 采用严格顺序：

```bash
kubectl --context <context> -n <namespace> scale deployment/mo-tn --replicas=0
kubectl --context <context> -n <namespace> wait --for=delete pod -l app=mo-tn --timeout=120s
kubectl --context <context> -n <namespace> scale deployment/mo-tn --replicas=1
kubectl --context <context> -n <namespace> wait --for=condition=Ready pod -l app=mo-tn --timeout=300s
```

重启后执行：

```sql
SELECT COUNT(*), COALESCE(SUM(id), 0)
FROM chronological_stream;

SELECT state, COUNT(*)
FROM mo_catalog.mo_lifecycle_datasets
GROUP BY state;

SHOW LIFECYCLE RESTORES LIMIT 100;
```

正确拓扑下 TN checkpoint 加载和 WAL replay 成功；33M 流 active 仍为 12,000,000 行，DML
active 仍为 870,000 行，没有 LSN 单调性 panic。

CN 在 `after-payload-put` 状态点停止后，旧 Root 到 lease deadline 转 `DELETE_PENDING`，最终
`CLEANED` 且 prefix 为空；fresh attempt 只发布一个 4 行 Dataset，Restore 逐行正确。

CN 在 final transaction 前停止的样本最终进入 `COMMIT_UNKNOWN`：Dataset=0、源表仍为 3 行，
Stage 中 Payload/Manifest 保留，Cleanup 没有误删。这是 fail-closed 崩溃样本，不等同于真实
TN commit response lost。

## 12. 最终测试结果

| 能力 | 实际结果 | 结论 |
| --- | --- | --- |
| fresh bootstrap | Cleanup Root 表存在；Feature row 默认关闭 | 通过 |
| Whole Archive / Restore | active=0，Restore=8，ID 和 36 | 通过 |
| Mixed Rewrite / Restore | active=8，Restore=8，总计 16，无重复 | 通过 |
| 小流式数据 | 70,000 Archive、50,000 active、10 Dataset | 通过 |
| 33M 时间序列 | 21M Archive、12M active、42 Dataset，ID 和守恒 | 通过（历史规模版本） |
| 110M 时间序列 | 70M Archive、40M active、5 Dataset，全部 Restore `DONE` | 通过（单 Pod 历史规模版本） |
| UPDATE / DELETE | 更新值位置正确、删除行两边不存在、ledger 六项差集为 0 | 通过 |
| 单 Dataset Range | 3 行、ID 2/3/4，`[from,to)` 正确 | 通过 |
| 跨 Dataset Range | 17,493 行，与独立基线一致 | 通过 |
| `SHOW LIFECYCLE RESTORES` | DATASET/RANGE、类型、状态、行数、分页、租户隔离正确 | 通过 |
| Provider PUT/GET/Manifest/LIST 故障 | source 保留、Dataset=0、失败 Root 最终清理 | 通过 |
| Restore Chunk 重试 | 8 Chunk 连续、无重复、hidden staging=0 | 通过 |
| CN 状态点 | Payload 清理和 `COMMIT_UNKNOWN` fail-closed 符合合同 | 通过 |
| TN 普通重启 | checkpoint/WAL replay 后数据和 Catalog 保持 | 通过（历史规模版本） |

当前证据没有发现以下 Stop-Ship 问题：

- Archive 未确认却退休源 Object；
- Dataset `PUBLISHED` 但 Restore 丢行或重复；
- active 与完整 Restore 同时缺行或重复行；
- Cleanup 删除 `COMMIT_UNKNOWN` Payload；
- 正确多节点拓扑下出现 Lifecycle 引起的 panic。

## 13. 结果边界

本轮结果证明 Phase 1 核心功能已经具备进入稳定性和容量认证的正确性基础，但以下项目没有被
写成已通过：

- 真实 commit response lost；
- PUT 成功但响应丢失、迟到 PUT、Cleanup DELETE response lost、持续限流；
- 普通 Merge 抢先替换 source 的完整压力矩阵；
- TN 在 SyncProtection/final prepare/commit 精确状态点重启；
- 4096/4097 Dataset 范围边界；
- 1 TiB、10 TiB、50→1000 Binding 和长期 soak；
- 当前最新 Head 上对历史 33M/110M 样本的逐 SHA 重跑。

因此当前发布口径仍为：核心 Archive/Restore/Range Restore 使用链路和关键错误语义已通过
E2E；Commercial GA 仍为 Conditional Go。
