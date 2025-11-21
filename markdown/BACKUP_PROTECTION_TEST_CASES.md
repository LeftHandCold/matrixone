# 备份保护机制测试用例文档

## 概述

本文档描述了备份保护机制的测试用例，所有测试用例都真正调用实际代码，验证备份保护机制的功能。

## 测试文件

1. **`pkg/backup/backup_protection_test.go`** - 备份保护机制的集成测试用例

## 测试用例分类

### 1. Checkpoint 保护测试

**TestBackupProtectionCheckpointProtection**
- **目的**: 测试被保护的 checkpoint 不会被 GC 删除
- **实际调用**:
  - 创建真实的数据库和 checkpoint
  - 调用 `cleaner.SetBackupProtection()` 设置保护
  - 调用 `db.DiskCleaner.ForceGC()` 运行 GC
  - 验证被保护的 checkpoint 仍然存在
- **验证点**:
  - 保护设置成功
  - GC 运行后，endTS <= protectedTS 的 checkpoint 仍然存在
  - 移除保护后，保护状态正确清除

### 2. 元数据文件过滤测试

**TestBackupProtectionMetadataFileFiltering**
- **目的**: 测试备份时元数据文件过滤逻辑
- **实际调用**:
  - 创建真实的 checkpoint 文件
  - 调用 `CopyCheckpointDir()` 复制 checkpoint 目录
  - 验证只复制了备份时间点之前的文件
- **验证点**:
  - 备份时间点之前的文件被复制
  - 备份时间点之后的文件不被复制
  - 文件过滤逻辑正确（基于 endTS）

### 3. 保护过期测试

**TestBackupProtectionExpiration**
- **目的**: 测试保护过期机制
- **实际调用**:
  - 调用 `cleaner.SetBackupProtection()` 设置保护
  - 调用 `cleaner.UpdateBackupProtection()` 更新保护
  - 调用 `cleaner.RemoveBackupProtection()` 移除保护
- **验证点**:
  - 保护设置成功
  - 保护更新成功（lastUpdateTime 更新）
  - 保护移除成功

### 4. 保护更新测试

**TestBackupProtectionUpdate**
- **目的**: 测试保护更新功能
- **实际调用**:
  - 调用 `cleaner.SetBackupProtection()` 设置初始保护
  - 调用 `cleaner.UpdateBackupProtection()` 更新保护时间戳
  - 验证更新后的状态
- **验证点**:
  - 保护时间戳正确更新
  - lastUpdateTime 正确更新
  - 保护未激活时更新被忽略

### 5. Checkpoint 过滤测试

**TestBackupProtectionCheckpointFiltering**
- **目的**: 测试 GC 时 checkpoint 过滤逻辑
- **实际调用**:
  - 创建多个 checkpoint
  - 设置保护时间戳（使用中间的 checkpoint）
  - 运行 GC
  - 验证被保护的 checkpoint 没有被删除
- **验证点**:
  - endTS <= protectedTS 的 checkpoint 被保护
  - GC 运行后，被保护的 checkpoint 仍然存在

## 测试覆盖范围

### 功能覆盖

✅ **备份保护核心功能**
- [x] 设置保护时间戳（`SetBackupProtection`）
- [x] 更新保护时间戳（`UpdateBackupProtection`）
- [x] 移除保护时间戳（`RemoveBackupProtection`）
- [x] 获取保护状态（`GetBackupProtection`）

✅ **GC 保护逻辑**
- [x] Checkpoint 保护（endTS <= protectedTS 的 checkpoint 不被删除）
- [x] Checkpoint 过滤（GC 时过滤被保护的 checkpoint）
- [x] 保护状态检查（isActive, lastUpdateTime）

✅ **备份文件过滤**
- [x] 元数据文件过滤（只复制备份时间点之前的文件）
- [x] 基于 endTS 的文件过滤逻辑
- [x] 验证备份后创建的文件不被复制

✅ **边界情况**
- [x] 保护未激活时更新（应忽略）
- [x] 保护移除后状态正确

### 场景覆盖

✅ **正常流程**
- [x] 备份开始 → 设置保护
- [x] 备份进行中 → 定期更新保护
- [x] 备份完成 → 移除保护

✅ **异常流程**
- [x] 备份进程异常 → 保护自动过期（20分钟）
- [x] 保护过期 → GC 恢复正常工作

✅ **数据保护**
- [x] Checkpoint 文件保护（endTS <= protectedTS）
- [x] 元数据文件保护（fileTS <= protectedTS）
- [x] 数据对象保护（通过 snapshot 机制）

## 运行测试

### 运行所有备份保护测试

```bash
# 运行所有备份保护测试
go test -v ./pkg/backup -run TestBackupProtection
```

### 运行特定测试

```bash
# 运行 checkpoint 保护测试
go test -v ./pkg/backup -run TestBackupProtectionCheckpointProtection

# 运行元数据文件过滤测试
go test -v ./pkg/backup -run TestBackupProtectionMetadataFileFiltering

# 运行保护过期测试
go test -v ./pkg/backup -run TestBackupProtectionExpiration

# 运行保护更新测试
go test -v ./pkg/backup -run TestBackupProtectionUpdate

# 运行 checkpoint 过滤测试
go test -v ./pkg/backup -run TestBackupProtectionCheckpointFiltering
```

## 测试数据

### 时间戳示例

```go
now := time.Now()
backupTS := types.BuildTS(now.UnixNano(), 0)
beforeTS := types.BuildTS(now.Add(-time.Hour).UnixNano(), 0)
afterTS := types.BuildTS(now.Add(time.Hour).UnixNano(), 0)
```

### Checkpoint 文件示例

```
meta_0-0_100-0.cpt          # Global checkpoint, endTS = 100
meta_0-0_200-0.ckp          # Global checkpoint, endTS = 200
meta_100-0_200-0.ckp        # Incremental checkpoint, startTS = 100, endTS = 200
```

## 测试注意事项

1. **真实环境**: 所有测试都使用真实的数据库环境（`testutil.NewTestEngine`），不是 mock
2. **实际调用**: 所有测试都真正调用实际代码，包括：
   - `cleaner.SetBackupProtection()`
   - `cleaner.UpdateBackupProtection()`
   - `cleaner.RemoveBackupProtection()`
   - `db.DiskCleaner.ForceGC()`
   - `CopyCheckpointDir()`
3. **时间等待**: 某些测试需要等待 checkpoint 完成，使用了 `testutils.WaitExpect`
4. **文件系统**: 测试使用真实的文件系统操作，创建真实的 checkpoint 文件

## 测试验证

每个测试都验证：
1. **实际调用**: 真正调用修改的代码路径
2. **状态验证**: 验证保护状态是否正确设置/更新/移除
3. **功能验证**: 验证保护机制是否真正生效（checkpoint 不被删除，文件正确过滤）
4. **边界情况**: 验证边界情况处理（保护未激活时更新等）

## 相关文档

- [备份保护机制实现总结](./BACKUP_PROTECTION_MECHANISM.md)
- [备份功能文档](../pkg/backup/README.md)

