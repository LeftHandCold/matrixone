# 备份功能测试覆盖率优化

## 概述

本文档描述了为提高 `pkg/backup` 包的测试覆盖率而添加的测试用例。

## 覆盖率问题分析

根据 `final-result-files/pr_coverage.out` 的覆盖率报告，以下代码路径未被覆盖：

1. **`getParallelCount` 函数的不同 CPU 数量分支** (行 118-123)
   - CPU < 8: 返回 50
   - CPU < 16: 返回 80
   - CPU < 32: 返回 128
   - CPU < 64: 返回 256
   - CPU >= 64: 返回 512

2. **`execBackup` 中的备份保护相关代码** (行 404-412, 727-844)
   - 备份保护设置成功后的 ticker 启动逻辑
   - 备份保护更新逻辑
   - 错误处理分支

3. **`CopyFile` 中的 `dstDir != ""` 分支** (行 727-732)
   - 带目标目录的文件复制
   - 文件重命名逻辑

4. **`copyFileAndGetMetaFiles` 中的过滤逻辑** (行 520-522, 525-527)
   - 基于时间戳的文件过滤
   - 目标文件已存在时的处理

## 新增测试用例

### 1. TestGetParallelCount

**目的**: 测试 `getParallelCount` 函数在不同 CPU 数量下的返回值

**覆盖代码**:
- `getParallelCount` 函数的所有分支
- 不同 CPU 数量下的并行数计算逻辑

**测试场景**:
- 自定义 count 在有效范围内
- CPU < 8
- CPU < 16
- CPU < 32
- CPU < 64
- CPU >= 64

### 2. TestCopyFileWithDstDir

**目的**: 测试 `CopyFile` 函数在指定目标目录时的行为

**覆盖代码**:
- `CopyFile` 函数中的 `dstDir != ""` 分支
- 文件重命名逻辑 (`newNames` 参数)
- 目标目录路径拼接

**测试场景**:
- 带目标目录和文件重名的文件复制
- 带目标目录但不重名的文件复制

### 3. TestCopyFileWithRetry

**目的**: 测试 `CopyFileWithRetry` 函数

**覆盖代码**:
- `CopyFileWithRetry` 函数
- 重试机制封装

### 4. TestCopyFileAndGetMetaFilesWithFiltering

**目的**: 测试 `copyFileAndGetMetaFiles` 函数的时间戳过滤逻辑

**覆盖代码**:
- `copyFileAndGetMetaFiles` 函数中的时间戳过滤
- 基于 `endTS` 的文件过滤逻辑
- 跳过备份时间点之后的文件

**测试场景**:
- 创建两个 checkpoint 文件：一个在备份时间点之前，一个在之后
- 验证只复制备份时间点之前的文件

### 5. TestExecBackupWithProtectionUpdate

**目的**: 测试 `execBackup` 中备份保护相关的代码路径

**覆盖代码**:
- `execBackup` 中 `exec == nil` 的分支（测试环境）
- 备份保护设置的代码路径

**注意**: 由于 `execBackup` 需要完整的 checkpoint 结构，此测试主要覆盖 `exec == nil` 的情况，实际的保护更新逻辑在集成测试中验证。

## 测试文件

**`pkg/backup/coverage_test.go`** - 新增的覆盖率测试文件

## 运行测试

```bash
# 运行所有覆盖率测试
go test -v ./pkg/backup -run TestGetParallelCount
go test -v ./pkg/backup -run TestCopyFile
go test -v ./pkg/backup -run TestCopyFileAndGetMetaFiles
go test -v ./pkg/backup -run TestExecBackupWithProtectionUpdate

# 运行所有测试并生成覆盖率报告
go test -v -coverprofile=coverage.out ./pkg/backup
go tool cover -html=coverage.out -o coverage.html
```

## 预期覆盖率提升

这些测试用例预期能够覆盖以下未覆盖的代码：

1. ✅ `getParallelCount` 的所有分支（100% 覆盖）
2. ✅ `CopyFile` 中的 `dstDir != ""` 分支
3. ✅ `copyFileAndGetMetaFiles` 中的过滤逻辑
4. ✅ `execBackup` 中 `exec == nil` 的分支（测试环境路径）

## 注意事项

1. **Mock 限制**: 由于 `runtime.ServiceRuntime` 的复杂性，某些测试（如 `TestExecBackupWithProtectionUpdate`）主要覆盖测试环境路径（`exec == nil`），实际的保护更新逻辑在集成测试中验证。

2. **测试环境**: 这些测试使用内存文件系统（`fileservice.NewMemoryFS()`），不依赖外部资源，运行速度快。

3. **CPU Mock**: `TestGetParallelCount` 使用 `gostub` 来 mock `runtime.NumCPU`，确保能够测试所有 CPU 数量分支。

## 后续优化建议

1. **集成测试**: 对于备份保护更新逻辑，建议在集成测试中验证完整的流程（包括 ticker 更新）。

2. **BVT 测试**: 可以考虑添加简单的 BVT SQL 测试来覆盖备份功能的端到端流程。

3. **错误场景**: 可以添加更多错误场景的测试，如文件复制失败、checkpoint 解析错误等。

## 相关文档

- [备份保护机制实现总结](./BACKUP_PROTECTION_MECHANISM.md)
- [备份保护机制优化](./BACKUP_PROTECTION_OPTIMIZATION.md)

