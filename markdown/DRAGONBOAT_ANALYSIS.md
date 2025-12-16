# Dragonboat NFS 错误分析

## 错误信息
```
panic: operation not supported
at dragonboat/nodehost.go:1684 (startShard)
at dragonboat/nodehost.go:2265 (panicNow)
```

## 源码位置

Dragonboat 源码已下载到：
```
/Users/shenjiangwei/go/pkg/mod/github.com/matrixorigin/dragonboat/v4@v4.0.0-20241019050137-1c6138e9cf8b
```

## 错误调用链

1. **nodehost.go:1657** - `startShard` 方法
   ```go
   if err := nh.env.CreateSnapshotDir(did, shardID, replicaID); err != nil {
       panicNow(err)  // 错误传播到这里
   }
   ```

2. **internal/server/environment.go:184** - `CreateSnapshotDir`
   ```go
   func (env *Env) CreateSnapshotDir(did uint64, shardID uint64, replicaID uint64) error {
       // ...
       if err := fileutil.Mkdir(path, env.fs); err != nil {
           return err  // 错误从这里返回
       }
   }
   ```

3. **internal/fileutil/utils.go:136** - `Mkdir`
   ```go
   func Mkdir(dir string, fs vfs.IFS) error {
       // ...
       if err := fs.MkdirAll(dir, defaultDirFileMode); err != nil {
           return err
       }
       return SyncDir(parent, fs)  // 调用 SyncDir
   }
   ```

4. **internal/fileutil/utils.go:168** - `SyncDir` **← 失败点**
   ```go
   func SyncDir(dir string, fs vfs.IFS) (err error) {
       // ...
       df, err := fs.OpenDir(vfs.Clean(dir))
       if err != nil {
           return err
       }
       defer func() {
           err = firstError(err, ws(df.Close()))
       }()
       return ws(df.Sync())  // ← NFS 不支持目录 fsync，返回 "operation not supported"
   }
   ```

## 根本原因

**NFS 不支持对目录文件描述符执行 `fsync()` 操作**

- `df.Sync()` 在底层调用 `fsync(fd)`，其中 `fd` 是目录的文件描述符
- NFS 协议和实现通常不支持目录级别的 `fsync()`
- 这会导致返回 `ENOTSUP` (Operation not supported) 错误

## 解决方案

1. **使用本地文件系统**（推荐）
   - 将 logservice 的 `DataDir` 配置到本地文件系统（ext4, xfs 等）

2. **使用网络块设备**
   - 使用 iSCSI、FC 等网络块设备，而不是 NFS
   - 这些设备在客户端看来是本地块设备，支持所有本地文件系统操作

3. **提前检测**
   - 代码中已添加 `checkNFSFilesystem` 函数，在启动时检测 NFS
   - 如果检测到 NFS，会在初始化 dragonboat 之前返回明确的错误

## 查看源码的命令

```bash
# 查看 SyncDir 函数
cat /Users/shenjiangwei/go/pkg/mod/github.com/matrixorigin/dragonboat/v4@v4.0.0-20241019050137-1c6138e9cf8b/internal/fileutil/utils.go | sed -n '139,169p'

# 查看 CreateSnapshotDir 函数
cat /Users/shenjiangwei/go/pkg/mod/github.com/matrixorigin/dragonboat/v4@v4.0.0-20241019050137-1c6138e9cf8b/internal/server/environment.go | sed -n '184,210p'

# 查看 startShard 方法
cat /Users/shenjiangwei/go/pkg/mod/github.com/matrixorigin/dragonboat/v4@v4.0.0-20241019050137-1c6138e9cf8b/nodehost.go | sed -n '1631,1720p'
```
