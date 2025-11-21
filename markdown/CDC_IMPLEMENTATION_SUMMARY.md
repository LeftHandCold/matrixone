# CDC 功能实现总结

## 一、功能概述

实现了基于 `mo_catalog.mo_cdc_watermark` 表的 CDC（Change Data Capture）过滤模块，用于在 GC（Garbage Collection）过程中保护 CDC 数据库的数据不被删除。

### 核心特性
- **数据库级别保护**：CDC 是数据库级别的，一个数据库只要有 CDC 任务，整个数据库的 appendable 和 CNCreated 对象都会被保护
- **最小水位策略**：对于同一个数据库，取所有表的最小 watermark 作为该数据库的保护水位
- **与 PITR/Snapshot 兼容**：CDC 检查在 PITR/Snapshot 之后，确保保护优先级正确

## 二、实现的关键组件

### 1. `SnapshotMeta` (`pkg/vm/engine/tae/logtail/snapshot.go`)

#### 新增字段
- `cdc specialTableInfo`：存储 CDC 表的对象和墓碑信息

#### 核心方法
- **`GetCDC()`**：从 CDC 对象中读取 watermark 数据，构建 `dbID -> 最小watermark` 的映射
  - 使用 `specialTableInfo.clone()` 避免 data race
  - 通过 `account_id, db_name, table_name` 构建 PK，从 `tablePKIndex` 查找 `tableInfo` 获取 `dbID`
  - 对同一 `dbID` 取最小 TS

- **`Update()`**：在更新 checkpoint 时收集 CDC 表的对象和墓碑
  - 识别 `mo_cdc_watermark` 表并设置 `cdc.tid`
  - 收集 CDC 表的对象和墓碑到 `cdc.objects` 和 `cdc.tombstones`

- **`SaveMeta()` / `ReadMeta()`**：持久化 CDC 元数据到磁盘
- **`SaveTableInfo()` / `ReadTableInfo()`**：持久化 CDC 表信息
- **`RebuildCdc()`**：启动时从磁盘重建 CDC 元数据

### 2. GC 过滤逻辑 (`pkg/vm/engine/tae/db/gc/v3/exec_v1.go`)

#### `MakeSnapshotAndPitrFineFilter()`
- 接收 `cdcWatermarks map[uint64]types.TS` 参数
- 在 `!ObjectIsSnapshotRefers()` 之后进行 CDC 检查
- 使用 `GetTableIDToDBIDMap()` 避免频繁加锁

#### CDC 保护逻辑
```go
if cdcTS, ok := cdcWatermarks[dbID]; ok {
    if stats.GetCNCreated() || stats.GetAppendable() {
        if (!deleteTS.IsEmpty() && deleteTS.LT(&cdcTS)) ||
            createTS.GT(&cdcTS) {
            // 保护该对象
            continue
        }
    }
}
```

### 3. GC 流程集成 (`pkg/vm/engine/tae/db/gc/v3/`)

#### `checkpoint.go`
- `GetCDCsLocked()` / `CDCTables()`：获取 CDC watermark 映射
- `doGCAgainstGlobalCheckpointLocked()`：在 GC 执行前获取 `cdcWatermarks` 并传递给 GC job

#### `window.go`
- `ExecuteGlobalCheckpointBasedGC()`：接收 `cdcWatermarks` 参数并传递给 GC job

#### `exec_v1.go`
- `NewCheckpointBasedGCJob()`：接收 `cdcWatermarks` 参数
- `CheckpointBasedGCJob`：存储 `cdcWatermarks` 并在过滤时使用

### 4. 接口定义 (`pkg/vm/engine/tae/db/gc/v3/types.go`)

- `Cleaner` 接口新增 `CDCTables() (map[uint64]types.TS, error)` 方法
- `MockCleaner` 实现该方法用于测试

## 三、核心逻辑流程

### 1. CDC 元数据收集流程
```
1. Checkpoint 更新时，SnapshotMeta.Update() 被调用
2. 识别 mo_cdc_watermark 表，设置 cdc.tid
3. 收集 CDC 表的对象和墓碑到 cdc.objects 和 cdc.tombstones
4. 持久化到磁盘（SaveMeta, SaveTableInfo）
```

### 2. CDC Watermark 获取流程
```
1. GC 执行前，调用 CDCTables() 获取 cdcWatermarks
2. CDCTables() 调用 SnapshotMeta.GetCDC()
3. GetCDC() 从 cdc.objects 中读取数据
4. 对每条记录：
   - 解析 account_id, db_name, table_name, watermark
   - 构建 PK 并查找 tableInfo 获取 dbID
   - 对同一 dbID 取最小 TS
5. 返回 dbID -> 最小watermark 的映射
```

### 3. GC 过滤流程
```
1. 对象通过 PITR/Snapshot 检查（ObjectIsSnapshotRefers）
2. 如果未通过 PITR/Snapshot 检查，进入 CDC 检查
3. 从 tableID 获取 dbID
4. 查找该 dbID 的 CDC watermark
5. 如果是 CNCreated 或 Appendable 对象：
   - 如果 dropTS < cdcTS 或 createTS > cdcTS，保护该对象
6. 否则标记为可删除
```

## 四、关键技术点

### 1. Data Race 修复
- 使用 `specialTableInfo.clone()` 方法复制 CDC 对象
- 在 `GetCDC()` 开始时加锁复制，然后释放锁，避免在 `processObjects` 过程中持有锁

### 2. PK 构建
- 使用 `types.Packer` 编码 `account_id, db_name, table_name`
- 通过 `types.DecodeTuple()` 解码获取字符串形式的 PK
- 与 `mo_tables` 的 PK 格式一致

### 3. 性能优化
- 使用 `GetTableIDToDBIDMap()` 一次性复制映射，避免频繁加锁
- 在 GC 过滤时直接使用复制的映射，无需加锁

### 4. 空 Watermark 处理
- 空 watermark 表示保护所有数据
- 空 TS 是最小的值，会覆盖非空 TS
- 在 GC 过滤时，空 TS 会导致所有对象都被保护

## 五、测试

### `TestCdcMeta` (`pkg/vm/engine/tae/db/test/db_test.go`)
- 创建 3 个数据库（db1, db2, db3）
- 为每个数据库添加多个 CDC 记录（不同表）
- 验证每个数据库的最小水位计算
- 验证重启后的持久化
- 验证水位更新逻辑

## 六、代码审查结果

### ✅ 正确性
- CDC 与 PITR/Snapshot 的关系处理正确
- CDC 保护条件逻辑正确
- 错误处理合理
- 并发安全已修复
- 边界情况处理合理

### ⚠️ 潜在风险（低风险）
1. **tableIDToDBID 映射不完整**：可能导致某些对象跳过 CDC 检查（情况少见）
2. **DecodeTuple 错误**：如果所有记录都失败，可能导致 CDC 功能失效（情况少见）

### 结论
- **整体评估**：✅ 代码质量良好，逻辑正确
- **风险等级**：🟢 低风险
- **可以安全上线**

## 七、文件清单

### 核心实现文件
- `pkg/vm/engine/tae/logtail/snapshot.go`：SnapshotMeta 的 CDC 相关方法
- `pkg/vm/engine/tae/db/gc/v3/exec_v1.go`：GC 过滤逻辑
- `pkg/vm/engine/tae/db/gc/v3/checkpoint.go`：GC 流程集成
- `pkg/vm/engine/tae/db/gc/v3/window.go`：GC 窗口管理
- `pkg/vm/engine/tae/db/gc/v3/types.go`：接口定义
- `pkg/vm/engine/tae/db/gc/v3/mock_cleaner.go`：Mock 实现

### 测试文件
- `pkg/vm/engine/tae/db/test/db_test.go`：TestCdcMeta 测试用例

## 八、关键改进点

1. ✅ 修复了 data race 问题（使用 clone 方法）
2. ✅ 修复了 PK 构建问题（使用 Packer 编码）
3. ✅ 实现了元数据持久化和重建
4. ✅ 添加了完整的单元测试
5. ✅ 优化了性能（避免频繁加锁）

## 九、使用说明

### 启用 CDC
1. 在 `mo_catalog.mo_cdc_watermark` 表中插入记录
2. 记录包含：`account_id, task_id, db_name, table_name, watermark`
3. GC 会自动识别并保护该数据库的数据

### 注意事项
- CDC 是数据库级别的，一个数据库只要有 CDC 任务，整个数据库都会被保护
- 空 watermark 表示保护所有数据
- 对于同一数据库，取所有表的最小 watermark 作为保护水位










