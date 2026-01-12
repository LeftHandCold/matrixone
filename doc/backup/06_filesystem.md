# 文件系统操作

## 1. 概述

Backup 模块通过 FileService 抽象层支持多种存储后端，包括本地文件系统和 S3 对象存储。

## 2. FileService 类型与 Checksum 机制

### 2.1 两种 FileService 类型

备份系统使用两种不同的 FileService 类型，它们在文件格式上有重要区别：

| 类型 | 创建方式 | Checksum | 适用场景 |
|------|----------|----------|----------|
| **LocalFS** | `GetForBackup()` | 带块级 CRC32 checksum | TAE 数据文件和元数据 |
| **LocalETLFS** | `GetForETL()` | 无 checksum | 备份元数据、配置文件 |

### 2.2 LocalFS 的 Checksum 机制

当使用 `GetForBackup()` 创建本地 FileService 时，返回的是 `LocalFS`，它会在文件内容中嵌入 checksum：

```
文件格式: [4字节CRC32][2044字节数据][4字节CRC32][2044字节数据]...
块大小: 2048 字节 (4字节checksum + 2044字节内容)
```

**读取时**：`LocalFS.Read()` 会自动验证每个块的 checksum，如果不匹配会返回 `checksum not match` 错误。

**写入时**：`LocalFS.Write()` 会自动为每个块计算并写入 checksum。

### 2.3 LocalETLFS（无 Checksum）

当使用 `GetForETL()` 创建本地 FileService 时，返回的是 `LocalETLFS`，它直接读写原始数据，不添加任何 checksum。

### 2.4 MO Server 备份的 FileService 使用

MO Server 在执行备份时使用 **LocalFS** (`forETL=false`) 写入所有 TAE 相关文件：
- `tae/` 目录下的所有数据文件（对象文件）
- `tae/tae_list` - TAE 文件列表
- `tae/tae_sum` - TAE 备份摘要
- `tae/ckp/*` - Checkpoint 文件
- `tae/gc/*` - GC 文件

### 2.5 mo-backup 工具的 FileService 使用

mo-backup 工具使用 **LocalETLFS** (`forETL=true`) 写入备份元数据：
- `backup_meta` - 备份元数据
- `mo_meta` - MO 元数据
- `config/*` - 配置文件
- `hakeeper/*` - HAKeeper 文件

### 2.6 文件类型与 FileService 对应关系

| 文件类型 | 示例 | 写入者 | FileService 类型 |
|----------|------|--------|------------------|
| TAE 数据文件 | `tae/*.blk`, 对象文件 | MO Server | LocalFS (`forETL=false`) |
| TAE 元数据 | `tae/tae_list`, `tae/tae_sum` | MO Server | LocalFS (`forETL=false`) |
| Checkpoint | `tae/ckp/*` | MO Server | LocalFS (`forETL=false`) |
| GC 文件 | `tae/gc/*` | MO Server | LocalFS (`forETL=false`) |
| 备份元数据 | `backup_meta`, `mo_meta` | mo-backup | LocalETLFS (`forETL=true`) |
| 配置文件 | `config/*` | mo-backup | LocalETLFS (`forETL=true`) |
| HAKeeper | `hakeeper/*` | mo-backup | LocalETLFS (`forETL=true`) |

### 2.7 常见错误：checksum not match

如果使用错误的 FileService 类型读取文件，会导致 `checksum not match` 错误：

```
错误场景：用 LocalFS 读取没有 checksum 块的文件（如 backup_meta）
错误信息：internal error: checksum not match
```

**解决方案**：确保读取文件时使用与写入时相同类型的 FileService。

### 2.8 Merge 命令的 FileService 使用

在执行 merge 命令时，需要使用正确的 FileService 类型：

```go
// 读取 TAE 文件（MO Server 写入，带 checksum）
targetTaeFs, _ := fs.SetupFilesystem(backupPath, false)  // LocalFS
taeFs := fileservice.SubPath(targetTaeFs, "tae")

// 读取备份元数据（mo-backup 写入，无 checksum）
targetEtlFs, _ := fs.SetupFilesystem(backupPath, true)   // LocalETLFS

// 写入目标目录
dstTaeFs, _ := fs.SetupFilesystem(targetPath, false)     // LocalFS for TAE
dstEtlFs, _ := fs.SetupFilesystem(targetPath, true)      // LocalETLFS for meta
```

## 2. FileService 设置

### 2.1 本地文件系统

```go
func setupFilesystem(
    ctx context.Context, 
    path string, 
    forETL bool,
) (res fileservice.FileService, readPath string, err error) {
    
    return setupFileservice(ctx, &pathConfig{
        isS3:             false,
        forETL:           forETL,
        filesystemConfig: filesystemConfig{path: path},
    })
}
```

### 2.2 S3 对象存储

```go
func setupS3(
    ctx context.Context, 
    s3 *s3Config, 
    forETL bool,
) (res fileservice.FileService, readPath string, err error) {
    
    return setupFileservice(ctx, &pathConfig{
        isS3:     true,
        forETL:   forETL,
        s3Config: *s3,
    })
}
```

### 2.3 统一设置函数

```go
func setupFileservice(
    ctx context.Context, 
    conf *pathConfig,
) (res fileservice.FileService, readPath string, err error) {
    
    if conf.isS3 {
        s3opts, err = makeS3Opts(&conf.s3Config)
        if conf.forETL {
            s3path := fileservice.JoinPath(s3opts, etlFSDir(conf.filepath))
            res, readPath, err = fileservice.GetForETL(ctx, nil, s3path)
        } else {
            s3path := fileservice.JoinPath(s3opts, conf.filepath)
            res, err = fileservice.GetForBackup(ctx, s3path)
        }
        res = fileservice.SubPath(res, conf.filepath)
    } else {
        if conf.forETL {
            res, readPath, err = fileservice.GetForETL(ctx, nil, etlFSDir(conf.path))
        } else {
            res, err = fileservice.GetForBackup(ctx, conf.path)
        }
    }
    
    return res, readPath, err
}
```

## 3. S3 配置解析

```go
func getS3Config(ctx context.Context, option []string) (*s3Config, error) {
    conf := &s3Config{}
    
    for i := 0; i < len(option); i += 2 {
        switch strings.ToLower(option[i]) {
        case "endpoint":
            conf.endpoint = option[i+1]
        case "region":
            conf.region = option[i+1]
        case "access_key_id":
            conf.accessKeyId = option[i+1]
        case "secret_access_key":
            conf.secretAccessKey = option[i+1]
        case "bucket":
            conf.bucket = option[i+1]
        case "filepath":
            conf.filepath = option[i+1]
        case "is_minio":
            conf.isMinio = (option[i+1] == "true")
        case "parallelism":
            parall, _ := strconv.ParseUint(option[i+1], 10, 16)
            conf.parallelism = uint16(parall)
        }
    }
    
    return conf, nil
}
```

### 3.1 S3 选项字符串生成

```go
func makeS3Opts(s3 *s3Config) (string, error) {
    buf := new(strings.Builder)
    w := csv.NewWriter(buf)
    
    opts := []string{
        "s3-opts",
        "endpoint=" + s3.endpoint,
        "region=" + s3.region,
        "key=" + s3.accessKeyId,
        "secret=" + s3.secretAccessKey,
        "bucket=" + s3.bucket,
        "role-arn=" + s3.roleArn,
        "is-minio=" + strconv.FormatBool(s3.isMinio),
    }
    
    w.Write(opts)
    w.Flush()
    return buf.String(), nil
}
```

## 4. 文件写入

### 4.1 带校验和的写入

```go
func writeFile(
    ctx context.Context, 
    fs fileservice.FileService, 
    path string, 
    data []byte,
) error {
    
    // 写入数据文件
    _, err = fileservice.DoWithRetry(
        "BackupWrite",
        func() (int, error) {
            return 0, fs.Write(ctx, fileservice.IOVector{
                FilePath: path,
                Entries: []fileservice.IOEntry{{
                    Offset: 0,
                    Size:   int64(len(data)),
                    Data:   data,
                }},
            })
        },
        64,
        fileservice.IsRetryableError,
    )
    
    // 计算并写入校验和
    checksum := sha256.Sum256(data)
    checksumFile := path + ".sha256"
    
    _, err = fileservice.DoWithRetry(
        "BackupWrite",
        func() (int, error) {
            return 0, fs.Write(ctx, fileservice.IOVector{
                FilePath: checksumFile,
                Entries: []fileservice.IOEntry{{
                    Offset: 0,
                    Size:   int64(len(checksum)),
                    Data:   checksum[:],
                }},
            })
        },
        64,
        fileservice.IsRetryableError,
    )
    
    return err
}
```

## 5. 文件读取

### 5.1 基本读取

```go
func readFile(
    ctx context.Context, 
    fs fileservice.FileService, 
    path string,
) ([]byte, error) {
    
    iov := &fileservice.IOVector{
        FilePath: path,
        Entries: []fileservice.IOEntry{{
            Offset: 0,
            Size:   -1,  // 读取整个文件
        }},
    }
    
    err := fs.Read(ctx, iov)
    return iov.Entries[0].Data, err
}
```

### 5.2 带校验的读取

```go
func readFileAndCheck(
    ctx context.Context, 
    fs fileservice.FileService, 
    path string,
) ([]byte, error) {
    
    // 读取数据
    data, err := readFile(ctx, fs, path)
    
    // 计算新校验和
    hash := sha256.New()
    hash.Write(data)
    newChecksumData := hash.Sum(nil)
    newChecksum := hexStr(newChecksumData)
    
    // 读取保存的校验和
    checksumFile := path + ".sha256"
    savedChecksumData, err := readFile(ctx, fs, checksumFile)
    savedChecksum := hexStr(savedChecksumData)
    
    // 比较校验和
    if strings.Compare(savedChecksum, newChecksum) != 0 {
        return nil, moerr.NewInternalError(ctx, 
            checksumErrorInfo(newChecksum, savedChecksum, path))
    }
    
    return data, err
}
```

## 6. 文件复制

### 6.1 带重试的复制

```go
func CopyFileWithRetry(
    ctx context.Context, 
    srcFs, dstFs fileservice.FileService, 
    name, dstDir string, 
    newName ...string,
) ([]byte, error) {
    
    return fileservice.DoWithRetry(
        "CopyFile",
        func() ([]byte, error) {
            return CopyFile(ctx, srcFs, dstFs, name, dstDir, newName...)
        },
        64,
        fileservice.IsRetryableError,
    )
}
```

### 6.2 实际复制实现

```go
func CopyFile(
    ctx context.Context, 
    srcFs, dstFs fileservice.FileService, 
    name, dstDir string, 
    newNames ...string,
) ([]byte, error) {
    
    // 构建路径
    newName := name
    if dstDir != "" {
        name = path.Join(dstDir, name)
        if len(newNames) > 0 {
            newName = path.Join(dstDir, newNames[0])
        } else {
            newName = name
        }
    }
    
    // 读取源文件
    var reader io.ReadCloser
    ioVec := &fileservice.IOVector{
        FilePath: name,
        Entries: []fileservice.IOEntry{{
            ReadCloserForRead: &reader,
            Offset:            0,
            Size:              -1,
        }},
        Policy: fileservice.SkipAllCache,
    }
    err := srcFs.Read(ctx, ioVec)
    defer reader.Close()
    
    // 边读边计算校验和
    hasher := sha256.New()
    hashingReader := io.TeeReader(reader, hasher)
    
    // 写入目标文件
    dstIoVec := fileservice.IOVector{
        FilePath: newName,
        Entries: []fileservice.IOEntry{{
            ReaderForWrite: hashingReader,
            Offset:         0,
            Size:           -1,
        }},
        Policy: fileservice.SkipAllCache,
    }
    err = dstFs.Write(ctx, dstIoVec)
    
    return hasher.Sum(nil), nil
}
```

## 7. 目录操作

### 7.1 ETL 目录

```go
func etlFSDir(filepath string) string {
    return filepath + "/_"
}
```

### 7.2 子路径

```go
// 创建子路径 FileService
fs := fileservice.SubPath(config.TaeDir, taeDir)
```

## 8. 错误处理

### 8.1 重试机制

所有文件操作都支持重试：
- 最大重试次数: 64
- 使用 `fileservice.IsRetryableError` 判断是否可重试

### 8.2 校验和错误

```go
func checksumErrorInfo(newChecksum, savedChecksum, path string) string {
    return fmt.Sprintf(
        "checksum %s of %s is not equal to %s", 
        newChecksum, path, savedChecksum,
    )
}
```
