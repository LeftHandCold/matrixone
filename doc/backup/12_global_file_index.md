# 全局文件索引

## 1. 背景

### 1.1 问题描述

增量备份在处理 Clone 操作产生的文件时存在问题：

1. Clone 操作会复用源表的对象文件，产生同名文件的多个引用
2. 备份时按文件名去重，只保留一条记录
3. 去重时保留的 TS 不确定，可能导致恢复时找不到文件

### 1.2 问题场景

```
T100: object1 创建 (createTS=T100)
T120: 增量备份 -> object1 复制到 backup_t120/, tae_list 记录 TS=T100
T150: Clone object1 (新记录 createTS=T150)
T170: 增量备份 -> 去重后可能保留 TS=T100, NeedCopy=false
T200: 增量备份 -> 去重后可能保留 TS=T150, NeedCopy=false

恢复 T200 时:
- binarySearch(T150) -> 找到 backup_t170
- 但 backup_t170 中没有 object1（因为 NeedCopy=false）
- 恢复失败！
```

## 2. 解决方案

### 2.1 全局文件索引

在备份根目录维护一个全局文件索引，记录每个文件首次被备份的位置：

```
backup_root/
├── file_index_0-xxx.idx      # 索引文件
├── file_index_0-yyy.idx
├── mo_br.meta
├── full-xxx/
└── incr-yyy/
```

### 2.2 索引文件格式

**文件命名**: `file_index_<backupTS>.idx`

**内容格式** (CSV):
```
filename,backupID,backupTS
object1,full-xxx,0-1704067200000000000
object2,incr-yyy,0-1704153600000000000
```

### 2.3 索引文件管理

- 保留最新 5 个索引文件
- 每次备份成功后更新索引
- 支持增量修复（基于旧索引）

## 3. 工作流程

### 3.1 备份流程

```
1. MO 执行备份，生成 tae_list
2. mo-backup 读取 tae_list
3. mo-backup 加载最新索引文件
4. mo-backup 更新索引（添加本次新备份的文件）
5. mo-backup 保存新索引文件
6. mo-backup 清理旧索引文件
```

### 3.2 恢复流程

```
1. mo-backup 根据目标备份的时间戳加载对应的索引文件
   - 查找 <= 目标备份时间戳的最新索引
   - 例如：恢复 t200 的备份，加载 file_index_t200.idx（而不是最新的 t300）
2. 对于每个需要恢复的文件:
   a. 如果 NeedCopy=true，从当前备份复制
   b. 如果 NeedCopy=false:
      - 策略1: 查询索引获取 backupID
      - 策略2: 二分查找（按 TS）
      - 策略3: 遍历所有备份目录（兜底）
3. 复制文件到恢复目录
```

**重要**：恢复时必须加载与目标备份时间点匹配的索引，而不是最新的索引。因为最新索引可能包含目标备份之后新增的文件信息，这些文件在目标备份时还不存在。

### 3.3 索引修复流程

```
1. 查找目标时间点之前的最新有效索引
2. 以该索引为基础
3. 扫描该索引之后的所有备份集
4. 从每个备份的 tae_list 中提取 NeedCopy=true 的文件
5. 生成新的索引文件
```

## 4. 代码实现

### 4.1 索引数据结构

```go
// mo-backup/pkg/config/file_index.go

type FileIndexEntry struct {
    FileName string // 对象文件名
    BackupID string // 备份ID (如 "full-xxx" 或 "incr-yyy")
    BackupTS string // 备份时间戳
}

type FileIndex struct {
    BackupTS string                     // 索引创建时的备份时间戳
    Entries  map[string]*FileIndexEntry // 文件名 -> 条目
}
```

### 4.2 索引操作

```go
// mo-backup/pkg/backup/file_index.go

// 加载最新索引
func LoadLatestFileIndex(ctx context.Context, metaFs fileservice.FileService) (*config.FileIndex, error)

// 保存索引
func SaveFileIndex(ctx context.Context, metaFs fileservice.FileService, index *config.FileIndex) error

// 备份后更新索引
func UpdateFileIndexAfterBackup(ctx context.Context, backup *Backup) error

// 修复索引
func RepairFileIndex(ctx context.Context, metaFs, globalFs fileservice.FileService, targetTS string) (*config.FileIndex, error)

// 清理旧索引
func CleanupOldFileIndexes(ctx context.Context, metaFs fileservice.FileService, keepCount int) error
```

### 4.3 恢复时使用索引

```go
// mo-backup/pkg/run/restore.go

func restore(...) error {
    // 获取目标备份的时间戳
    targetMeta := mobrMetas[backupPath.BaseID]
    var targetTS types.TS
    if targetMeta != nil {
        targetTS = types.StringToTS(targetMeta.BackupTS)
    }
    
    // 加载 <= 目标时间戳的最新索引
    fileIndex, err := backup.LoadFileIndexByTS(ctx, backupListFS, targetTS)
    // ...
}

// mo-backup/pkg/backup/file_index.go

// LoadFileIndexByTS 加载 <= 目标时间戳的最新索引
func LoadFileIndexByTS(ctx context.Context, metaFs fileservice.FileService, targetTS types.TS) (*config.FileIndex, error) {
    // 如果 targetTS 为空，加载最新索引
    if targetTS.IsEmpty() {
        return LoadLatestFileIndex(ctx, metaFs)
    }
    
    // 查找 <= targetTS 的最新索引
    indexName := config.GetFileIndexNameByTS(indexNames, targetTS)
    return LoadFileIndex(ctx, metaFs, indexName)
}

// mo-backup/pkg/config/file_index.go

// GetFileIndexNameByTS 返回 <= 目标时间戳的最新索引文件名
func GetFileIndexNameByTS(names []string, targetTS types.TS) string {
    infos := ParseFileIndexInfos(names) // 按时间戳排序，最新的在前
    for _, info := range infos {
        if info.BackupTS.LessEq(&targetTS) {
            return info.Name
        }
    }
    return ""
}
```

**copyTae 中的三层查找策略**:

```go
func copyTae(..., fileIndex *config.FileIndex) error {
    for _, file := range taeFiles {
        if !file.NeedCopy() {
            var meta *restoreMeta
            
            // 策略1: 使用索引
            if fileIndex != nil {
                if backupID, _ := backup.GetFileLocationFromIndex(fileIndex, file.Path()); backupID != "" {
                    meta = findRestoreMetaByBackupID(restoreMetas, backupID)
                }
            }
            
            // 策略2: 二分查找
            if meta == nil {
                meta = binarySearch(restoreMetas, file.TS())
            }
            
            // 策略3: 遍历查找
            if meta == nil {
                meta = findFileInRestoreMetas(ctx, restoreMetas, file.Path())
            }
            
            // ...
        }
    }
}
```

### 4.4 MO 备份时读取索引

MO 在执行备份时会读取全局文件索引，用于判断文件是否已经在之前的备份中存在，避免重复复制。

```go
// pkg/backup/file_index.go

// GlobalFileIndex 是 MO 侧的简化索引结构
// 只需要知道文件是否存在，不需要知道具体在哪个备份
type GlobalFileIndex struct {
    files map[string]bool
}

// LoadGlobalFileIndex 从备份根目录加载最新的全局文件索引
func LoadGlobalFileIndex(ctx context.Context, rootFs fileservice.FileService) (*GlobalFileIndex, error)

// Has 检查文件是否在索引中
func (idx *GlobalFileIndex) Has(fileName string) bool
```

**备份流程中的调用链**:

```go
// pkg/backup/backup.go
var backupTae = func(ctx context.Context, sid string, config *Config) error {
    fs := fileservice.SubPath(config.TaeDir, taeDir)
    
    // 1. 从备份根目录加载全局文件索引
    globalIndex, err := LoadGlobalFileIndex(ctx, config.TaeDir)
    if err != nil {
        logutil.Warnf("backup: failed to load global file index: %v", err)
        // 加载失败不影响备份，继续使用原有逻辑
    }
    
    // 2. 将索引传递给 BackupData
    return BackupData(ctx, sid, config.SharedFs, fs, "", config, globalIndex)
}

// pkg/backup/tae.go
func BackupData(..., globalIndex *GlobalFileIndex) error {
    // ...
    return execBackup(..., globalIndex)
}

func execBackup(..., globalIndex *GlobalFileIndex) error {
    // 在去重逻辑中使用索引
    for _, oName := range oNames {
        objName := oName.Location.Name().String()
        
        // 检查文件是否在当前备份目录中
        if dstHave[objName] {
            oName.NeedCopy = false
        }
        
        // 检查文件是否在全局索引中（已在之前的备份中存在）
        if globalIndex != nil && globalIndex.Has(objName) {
            oName.NeedCopy = false
        }
        
        // ...
    }
}
```

## 5. 职责划分

| 操作 | 执行方 | 说明 |
|------|--------|------|
| 读取索引（备份时） | MO | 判断文件是否已备份，设置 NeedCopy |
| 生成/更新索引 | mo-backup | 备份成功后更新 |
| 读取索引（恢复时） | mo-backup | 查找文件所在的备份目录 |
| 修复索引 | mo-backup | 手动触发 |
| merge 更新索引 | mo-backup | merge 后生成新索引 |

## 6. 优点

1. **解决 Clone 问题**: 索引记录文件的实际位置，不依赖 TS
2. **高效查找**: O(1) 查找文件位置
3. **三层兜底**: 索引 → 二分 → 遍历
4. **增量修复**: 基于旧索引快速修复
5. **向后兼容**: 没有索引时仍可使用原有逻辑

## 7. 使用示例

### 7.1 正常备份

```bash
# 全量备份
mo_br backup --host 127.0.0.1 --port 6001 --backup_dir filesystem --path /backup

# 增量备份
mo_br backup --host 127.0.0.1 --port 6001 --backup_dir filesystem --path /backup --backup_type incremental --base_id <full_backup_id>
```

备份成功后会自动更新索引文件。

### 7.2 恢复

```bash
mo_br restore --backup_dir filesystem --path /backup --restore_dir filesystem --restore_path /restore
```

恢复时会自动加载索引文件。

### 7.3 修复索引

```bash
# 修复到最新备份（默认）
mo_br repair_index --backup_dir filesystem --path /backup

# 修复到指定备份 ID
mo_br repair_index --backup_dir filesystem --path /backup --backup_id 019b92ab-1bf8-77d9-a0f5-136a3052e936
```

如果索引文件损坏，可以手动修复。修复时会：
1. 查找目标备份之前的最新有效索引作为基础
2. 扫描基础索引之后到目标备份之间的所有备份
3. 从每个备份的 `tae_list` 中提取 `NeedCopy=true` 的文件
4. 生成新的索引文件

**示例输出**：
```
===== Repair File Index =====
Target backup: incremental (ID: 019b92ab-1bf8-77d9-a0f5-136a3052e936, latest)

----- Repair Plan -----
Base index:    file_index_1767692237161617889-1.idx
Backups to scan: 1 (incremental repair)

===== Repair Summary =====
Index Entries: 12345
Index File:    file_index_1767692246041421816-1.idx
Duration:      2.5s
==========================
```

## 8. 代码文件清单

### 8.1 MO 侧 (pkg/backup/)

| 文件 | 说明 |
|------|------|
| `file_index.go` | 新增。定义 `GlobalFileIndex` 结构和 `LoadGlobalFileIndex` 函数 |
| `backup.go` | 修改。`backupTae` 函数加载索引并传递给 `BackupData` |
| `tae.go` | 修改。`BackupData` 和 `execBackup` 函数增加 `globalIndex` 参数 |

### 8.2 mo-backup 侧

| 文件 | 说明 |
|------|------|
| `pkg/config/file_index.go` | 新增。定义 `FileIndex`、`FileIndexEntry` 数据结构和工具函数 |
| `pkg/backup/file_index.go` | 新增。索引的加载、保存、更新、修复等操作 |
| `pkg/backup/backup.go` | 修改。备份成功后调用 `UpdateFileIndexAfterBackup` |
| `pkg/backup/merge.go` | 修改。merge 后生成新的索引文件 |
| `pkg/run/restore.go` | 修改。恢复时加载索引，实现三层查找策略 |
| `pkg/run/repair_index.go` | 新增。索引修复命令实现 |

## 9. 数据流图

```
┌─────────────────────────────────────────────────────────────────────┐
│                           备份流程                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────┐    ┌──────────────┐    ┌─────────────┐                │
│  │ mo_br   │───>│ MO (backup)  │───>│ tae_list    │                │
│  │ backup  │    │              │    │ (NeedCopy)  │                │
│  └─────────┘    └──────────────┘    └─────────────┘                │
│       │               │                    │                        │
│       │               │ 读取索引           │                        │
│       │               v                    │                        │
│       │         ┌──────────────┐           │                        │
│       │         │ file_index   │           │                        │
│       │         │ _xxx.idx     │           │                        │
│       │         └──────────────┘           │                        │
│       │                                    │                        │
│       │<───────────────────────────────────┘                        │
│       │                                                             │
│       │ 更新索引                                                    │
│       v                                                             │
│  ┌──────────────┐                                                   │
│  │ file_index   │                                                   │
│  │ _yyy.idx     │ (新索引)                                          │
│  └──────────────┘                                                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                           恢复流程                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────┐    ┌──────────────┐    ┌─────────────┐                │
│  │ mo_br   │───>│ 加载索引     │───>│ 查找文件    │                │
│  │ restore │    │              │    │             │                │
│  └─────────┘    └──────────────┘    └─────────────┘                │
│                                            │                        │
│                                            v                        │
│                        ┌───────────────────────────────────┐        │
│                        │ 三层查找策略:                      │        │
│                        │ 1. 索引查找 (O(1))                │        │
│                        │ 2. 二分查找 (O(log n))            │        │
│                        │ 3. 遍历查找 (O(n)) - 兜底         │        │
│                        └───────────────────────────────────┘        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 10. 测试

### 10.1 单元测试

MO 侧的单元测试位于 `pkg/backup/file_index_test.go`：

```bash
# 运行所有 file_index 相关测试
go test -v -run "FileIndex|ParseFileIndex" ./pkg/backup/...
```

| 测试函数 | 说明 |
|---------|------|
| `TestGlobalFileIndex_Basic` | 测试基本的 Add、Has、Size 操作 |
| `TestGlobalFileIndex_NilSafety` | 测试 nil 安全性 |
| `TestIsFileIndexName` | 测试索引文件名识别 |
| `TestParseFileIndexTS` | 测试从文件名解析时间戳 |
| `TestLoadGlobalFileIndex_*` | 测试各种场景下的索引加载 |
| `TestGlobalFileIndex_UsedInBackup` | 测试在备份场景中的使用 |

### 10.2 集成测试

集成测试脚本位于 `test/backup/` 目录：

```bash
# 运行所有回归测试
cd test/backup
./run_all_tests.sh

# 单独运行某个测试
./test_backup_basic.sh      # 基础功能测试
./test_backup_clone.sh      # Clone 场景测试
./test_backup_index.sh      # 索引功能测试

# 运行稳定性测试
./test_backup_stability.sh --duration 3600    # 运行1小时
./test_backup_stability.sh --iterations 100   # 运行100轮
```

| 脚本 | 说明 |
|------|------|
| `test_backup_basic.sh` | 基础功能回归测试（全量备份、增量备份、恢复） |
| `test_backup_clone.sh` | Clone 文件备份恢复测试（核心场景） |
| `test_backup_index.sh` | 全局文件索引功能测试（生成、清理、修复） |
| `test_backup_stability.sh` | 长时间稳定性测试 |

### 10.3 测试场景覆盖

1. **基础功能**
   - 全量备份
   - 增量备份
   - 恢复到最新备份
   - 恢复到指定备份

2. **Clone 场景**（核心）
   - Clone 后备份
   - 多次增量备份后恢复 Clone 文件
   - 恢复到 Clone 之前的时间点
   - 删除源表后恢复 Clone 表

3. **索引功能**
   - 索引文件自动生成
   - 索引文件清理（保留5个）
   - 索引文件修复
   - 恢复时使用正确的索引（按时间戳匹配）
   - sha256 校验文件同步删除

4. **稳定性测试**
   - 随机操作（插入、Clone、删除、备份、恢复）
   - 长时间运行
   - 错误统计
