# 数据恢复

## 1. 概述

数据恢复是备份的逆过程，将备份数据还原到数据库中。本章介绍恢复的基本原理和流程。

## 2. 恢复流程概述

```
┌─────────────────────────────────────────────────────────────┐
│                    Step 1: 读取元数据                        │
│                    解析 mo_meta 文件                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Step 2: 验证备份                          │
│                    检查版本兼容性                            │
│                    验证文件完整性                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Step 3: 恢复配置                          │
│                    还原配置文件                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Step 4: 恢复 TAE 数据                     │
│                    复制数据文件                              │
│                    恢复 Checkpoint                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Step 5: 恢复 HAKeeper                     │
│                    还原集群状态                              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Step 6: 启动验证                          │
│                    启动数据库                                │
│                    验证数据完整性                            │
└─────────────────────────────────────────────────────────────┘
```

## 3. 元数据解析

### 3.1 mo_meta 文件格式

```csv
version,0823,
buildinfo,GoVersion: go1.21|BranchName: main|CommitID: abc123|...,
launchconfig,cn,cn_config_xxx.toml
launchconfig,dn,dn_config_xxx.toml
```

### 3.2 解析元数据

```go
func fromCsvBytes(data []byte) ([][]string, error) {
    r := csv.NewReader(bytes.NewReader(data))
    return r.ReadAll()
}
```

## 4. 文件完整性验证

### 4.1 校验和验证

每个备份文件都有对应的 `.sha256` 校验和文件：

```go
func readFileAndCheck(
    ctx context.Context, 
    fs fileservice.FileService, 
    path string,
) ([]byte, error) {
    
    // 读取数据
    data, err := readFile(ctx, fs, path)
    
    // 计算校验和
    hash := sha256.New()
    hash.Write(data)
    newChecksum := hexStr(hash.Sum(nil))
    
    // 读取保存的校验和
    savedChecksumData, _ := readFile(ctx, fs, path + ".sha256")
    savedChecksum := hexStr(savedChecksumData)
    
    // 比较
    if newChecksum != savedChecksum {
        return nil, moerr.NewInternalError(ctx, "checksum mismatch")
    }
    
    return data, nil
}
```

## 5. TAE 数据恢复

### 5.1 文件列表

备份时生成的 `tae_list` 文件包含所有 TAE 数据文件：

```csv
path,size,checksum,needCopy,ts
object1.blk,1024,abc123...,true,0-1
object2.blk,2048,def456...,true,0-2
```

### 5.2 Checkpoint 恢复

备份 Checkpoint 的特殊处理：

```go
// 备份 Checkpoint 的 LSN 为 0
func (s *runnerStore) AddBackupCKPEntry(entry *CheckpointEntry) (success bool) {
    entry.entryType = ET_Incremental
    success = s.AddICKPFinishedEntry(entry)
    
    // 重置所有 LSN
    it := s.incrementals.Iter()
    for it.Next() {
        e := it.Item()
        e.ckpLSN = 0
        e.truncateLSN = 0
    }
    return
}
```

### 5.3 Replay 流程

```go
// 在 runner.go 中
if entry.IsBackup() {
    if ok := r.store.AddBackupCKPEntry(entry); !ok {
        logutil.Warn("Replay-Backup-AddFailed", ...)
    }
}
```

## 6. HAKeeper 恢复

### 6.1 读取 HAKeeper 数据

```go
// 从备份读取 HAKeeper 状态
data, err := readFileAndCheck(ctx, fs, "hakeeper/hk_data")
```

### 6.2 恢复集群状态

HAKeeper 数据包含：
- 集群拓扑信息
- 节点状态
- 调度信息

## 7. 增量恢复

### 7.1 恢复顺序

```
1. 恢复全量备份 (Base)
2. 按时间顺序应用增量备份
   - 增量备份 1 (T1)
   - 增量备份 2 (T2)
   - ...
   - 增量备份 N (Tn)
```

### 7.2 Checkpoint 合并

增量恢复时需要合并 Checkpoint：

```go
func MergeCkpMeta(
    ctx context.Context,
    sid string,
    dstFs fileservice.FileService,
    cnLocation, tnLocation objectio.Location,
    start, end types.TS,
) (string, error)
```

## 8. 恢复验证

### 8.1 数据完整性检查

恢复后应进行以下验证：

1. 检查所有表是否可访问
2. 验证行数是否正确
3. 检查索引是否有效
4. 验证约束是否满足

### 8.2 一致性检查

```sql
-- 检查表数量
SELECT COUNT(*) FROM mo_catalog.mo_tables;

-- 检查数据库数量
SELECT COUNT(*) FROM mo_catalog.mo_database;

-- 验证特定表的数据
SELECT COUNT(*) FROM your_table;
```

## 9. 故障处理

### 9.1 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| 校验和不匹配 | 文件损坏 | 重新备份或使用其他备份 |
| 版本不兼容 | 备份版本过旧 | 升级或使用兼容版本恢复 |
| 文件缺失 | 备份不完整 | 检查备份完整性 |
| 空间不足 | 目标空间不够 | 扩展存储空间 |

### 9.2 恢复失败处理

1. 检查错误日志
2. 验证备份文件完整性
3. 确认版本兼容性
4. 检查存储空间
5. 必要时联系支持

## 10. 最佳实践

### 10.1 恢复前准备

1. 确认备份文件完整
2. 检查目标环境配置
3. 预留足够存储空间
4. 准备回滚方案

### 10.2 恢复后操作

1. 验证数据完整性
2. 更新配置（如需要）
3. 重建统计信息
4. 测试应用连接
5. 监控系统状态
