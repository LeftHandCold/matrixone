# 备份流程详解

## 1. 备份入口

备份从 `Backup` 函数开始：

```go
func Backup(
    ctx context.Context,
    sid string,
    bs *tree.BackupStart,
    cfg *Config,
) error
```

## 2. 完整备份流程

```
┌─────────────────────────────────────────────────────────────┐
│                    Step 1: 参数验证                          │
│                  metasMustBeSet()                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Step 2: 设置文件系统                         │
│         setupFilesystem() / setupS3()                        │
│    - GeneralDir: 通用文件（配置等）                          │
│    - TaeDir: TAE 数据文件                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                Step 3: 备份构建信息                          │
│                  backupBuildInfo()                           │
│    - 版本号                                                  │
│    - 构建时间、分支、CommitID                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Step 4: 备份配置文件                         │
│                   backupConfigs()                            │
│    - CN/DN/Log/Proxy 配置                                    │
│    - Launch 配置                                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Step 5: 备份 TAE 数据                       │
│                     backupTae()                              │
│    - 触发 Checkpoint                                         │
│    - 收集文件列表                                            │
│    - 并行复制数据                                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                Step 6: 备份 HAKeeper                         │
│                  backupHakeeper()                            │
│    - 获取 HAKeeper 状态数据                                  │
│    - 写入备份目录                                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Step 7: 保存元数据                           │
│                    saveMetas()                               │
│    - 生成 mo_meta 文件                                       │
│    - 包含版本、构建信息、配置列表                            │
└─────────────────────────────────────────────────────────────┘
```

## 3. 详细步骤说明

### 3.1 文件系统设置

```go
// 本地文件系统
if !bs.IsS3 {
    cfg.GeneralDir, _, err = setupFilesystem(ctx, bs.Dir, true)
    cfg.TaeDir, _, err = setupFilesystem(ctx, bs.Dir, false)
    cfg.Parallelism = uint16(parallel)
} else {
    // S3 存储
    s3Conf, err = getS3Config(ctx, bs.Option)
    cfg.GeneralDir, _, err = setupS3(ctx, s3Conf, true)
    cfg.TaeDir, _, err = setupS3(ctx, s3Conf, false)
    cfg.Parallelism = s3Conf.parallelism
}
```

### 3.2 构建信息备份

```go
func backupBuildInfo(ctx context.Context, cfg *Config) error {
    cfg.Metas.AppendVersion(Version)
    cfg.Metas.AppendBuildinfo(buildInfo())
    return nil
}

func buildInfo() string {
    infos := []string{
        "GoVersion: " + version.GoVersion,
        "BranchName: " + version.BranchName,
        "CommitID: " + version.CommitID,
        "BuildTime: " + version.BuildTime,
        "Version: " + version.Version,
    }
    return strings.Join(infos, "|")
}
```

### 3.3 配置文件备份

```go
func backupConfigs(ctx context.Context, cfg *Config) error {
    for typ, files := range launchConfigPaths {
        for _, f := range files {
            err = backupConfigFile(ctx, typ, f, cfg)
        }
    }
    return err
}
```

### 3.4 TAE 数据备份

这是备份的核心步骤，详见 [TAE 数据备份](04_tae_backup.md)。

### 3.5 HAKeeper 备份

```go
func backupHakeeper(ctx context.Context, config *Config) error {
    fs := fileservice.SubPath(config.TaeDir, hakeeperDir)
    haData, err := config.HAkeeper.GetBackupData(ctx)
    return writeFile(ctx, fs, HakeeperFile, haData)
}
```

### 3.6 元数据保存

```go
func saveMetas(ctx context.Context, cfg *Config) error {
    lines := cfg.Metas.CsvString()
    metas, err := ToCsvLine2(lines)
    return writeFile(ctx, cfg.GeneralDir, moMeta, []byte(metas))
}
```

## 4. 错误处理

备份过程中的错误处理策略：

1. **配置文件缺失**: 记录日志但不中断备份
2. **文件复制失败**: 支持重试机制
3. **GC 保护失败**: 记录警告但继续备份
4. **存储空间不足**: 立即返回错误

## 5. 备份产物结构

```
backup_dir/
├── mo_meta                 # 备份元数据
├── mo_meta.sha256          # 元数据校验和
├── config/                 # 配置文件
│   ├── cn_xxx.toml
│   ├── dn_xxx.toml
│   └── ...
├── tae/                    # TAE 数据
│   ├── ckp/               # Checkpoint 文件
│   ├── gc/                # GC 元数据
│   ├── tae_list           # 文件列表
│   ├── tae_sum            # 汇总信息
│   └── *.blk              # 数据块文件
└── hakeeper/              # HAKeeper 数据
    └── hk_data
```

## 6. 时间线示例

```
T0: 开始备份
    │
T1: 设置文件系统 (~100ms)
    │
T2: 备份构建信息 (~10ms)
    │
T3: 备份配置文件 (~500ms)
    │
T4: 触发 Checkpoint (~1-5s)
    │
T5: 设置 GC 保护 (~100ms)
    │
T6: 并行复制数据 (取决于数据量)
    │
T7: 复制 Checkpoint/GC 元数据 (~1s)
    │
T8: 备份 HAKeeper (~100ms)
    │
T9: 保存元数据 (~100ms)
    │
T10: 清理 GC 保护 (~100ms)
    │
T11: 备份完成
```
