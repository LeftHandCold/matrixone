# 备份保护机制 (Backup Protection Mechanism)

## 概述

备份保护机制用于解决备份过程中因时间过长导致 GC (垃圾回收) 删除备份时间点数据的问题。该机制通过以下方式工作：

1. **保护注册**: 备份开始时，向 GC 系统注册保护，指定需要保护的时间戳
2. **心跳维护**: 备份过程中定期发送心跳，维持保护状态
3. **GC 检查**: GC 执行前检查当前 GC 时间戳是否受保护，如果受保护则跳过 GC
4. **自动清理**: 备份完成后自动移除保护，或在心跳超时后自动过期

## 架构组件

### 1. BackupProtectionManager
- 管理所有活跃的备份保护
- 提供保护的增加、删除、更新和查询功能
- 自动清理过期的保护

### 2. mo_ctl 命令扩展
新增 `BACKUP_PROTECTION` 命令，支持以下操作：
- `add`: 添加备份保护
- `remove`: 移除备份保护  
- `heartbeat`: 更新心跳
- `list`: 列出活跃保护

### 3. GC 保护检查
修改 GC 逻辑，在执行全局检查点 GC 前检查 GC 时间戳是否受备份保护

## 使用方式

### 自动使用（推荐）

备份功能已经集成了保护机制，正常使用备份功能即可：

```sql
-- 执行备份，系统会自动设置保护
BACKUP DATABASE mydb TO S3 OPTION '{"endpoint":"...", "bucket":"mybucket"}';
```

### 手动控制

也可以通过 mo_ctl 手动管理保护：

```sql
-- 添加保护
SELECT mo_ctl('dn','BACKUP_PROTECTION','{"action":"add","backup_id":"backup-123","backup_ts":"2023-12-01 10:00:00.000000","protected_paths":["shared/ckp","shared/gc"]}');

-- 更新心跳
SELECT mo_ctl('dn','BACKUP_PROTECTION','{"action":"heartbeat","backup_id":"backup-123"}');

-- 列出活跃保护
SELECT mo_ctl('dn','BACKUP_PROTECTION','{"action":"list"}');

-- 移除保护
SELECT mo_ctl('dn','BACKUP_PROTECTION','{"action":"remove","backup_id":"backup-123"}');
```

## 配置参数

### 心跳超时时间
默认 10 分钟，可在 BackupProtectionManager 初始化时设置：

```go
mgr := NewBackupProtectionManager()
mgr.HeartbeatTimeout = 15 * time.Minute  // 设置为 15 分钟
```

### 清理检查间隔  
默认 1 分钟，可在 BackupProtectionManager 初始化时设置：

```go
mgr := NewBackupProtectionManager()
mgr.CleanupInterval = 30 * time.Second  // 设置为 30 秒
```

### 备份心跳间隔
默认 2 分钟，在备份代码中设置：

```go
ticker := time.NewTicker(2 * time.Minute) // 每 2 分钟发送一次心跳
```

## 保护规则

### 时间戳保护
- 如果 `文件时间戳 <= 备份时间戳`，文件受保护
- 使用保守策略，GC 时使用 `types.MaxTs()` 进行检查

### 路径保护
- 空路径数组 `[]` 表示保护所有文件
- 指定路径使用前缀匹配，如 `"shared/ckp"` 保护所有以此开头的文件
- 支持通配符 `"*"` 表示保护所有文件

### 保护层级
1. **数据文件**: 通过空路径保护所有数据文件
2. **检查点文件**: `shared/ckp` 路径下的检查点元数据
3. **GC 元数据**: `shared/gc` 路径下的垃圾回收元数据

## 故障处理

### 心跳丢失
- 如果备份进程异常退出，心跳会停止
- 系统会在 `HeartbeatTimeout` 后自动清理过期保护
- 默认超时时间为 10 分钟，足够处理大多数情况

### 保护管理器故障
- 如果 TN 重启，保护管理器会重新初始化
- 内存中的保护会丢失，但这是安全的，因为：
  - 备份进程会检测到连接中断并重试
  - 重新注册保护或终止备份

### GC 保护检查失败
- 如果保护检查出错，GC 会跳过删除，采用保守策略
- 在日志中记录保护检查的详细信息

## 性能考虑

### 内存使用
- 每个保护条目占用约 200-300 字节内存
- 支持同时保护数百个备份任务

### GC 性能影响
- 每次删除前增加保护检查，开销极小（纳秒级）
- 批量删除时只进行一次过滤，不影响整体性能

### 网络开销
- 心跳消息很小（< 1KB），每 2 分钟发送一次
- mo_ctl 命令复用现有 RPC 机制，无额外连接开销

## 日志和监控

### 关键日志事件
```
backup-protection-added     - 保护添加成功
backup-protection-removed   - 保护移除成功  
backup-protection-expired   - 保护超时过期
backup-protection-heartbeat - 心跳更新
GC-Skip-Protected-File      - GC 跳过受保护文件
GC-Protected-Files-Skipped  - GC 跳过保护文件汇总
```

### 监控指标
- 活跃保护数量
- 保护过期频率
- GC 跳过文件数量
- 备份平均耗时

## 故障排查

### 备份失败：文件已被删除
检查：
1. 是否有 `backup-protection-added` 日志
2. 心跳是否正常发送
3. GC 是否有跳过保护文件的日志

### GC 无法删除文件
检查：
1. 是否有活跃的备份保护
2. 使用 `list` 命令查看当前保护状态
3. 检查保护的时间戳和路径是否正确

### 内存泄漏
检查：
1. 过期保护是否被正确清理
2. `CleanupInterval` 是否配置合理
3. 备份是否正确调用 `remove` 清理保护

## 最佳实践

1. **及时清理**: 备份完成后立即移除保护，避免不必要的资源占用
2. **合理超时**: 根据备份规模设置合适的心跳超时时间
3. **路径精确**: 尽量指定具体的保护路径，避免过度保护
4. **监控保护**: 定期检查活跃保护数量，防止异常堆积
5. **日志分析**: 关注 GC 跳过文件的日志，评估保护效果
