# 性能问题

## 1. 并行复制问题

### 1.1 🟠 并行度计算不够智能

**现状**:
```go
func getParallelCount(count int) int {
    if count > 0 && count < 512 {
        return count
    }
    cupNum := runtime2.NumCPU()
    if cupNum < 8 {
        return 50
    } else if cupNum < 16 {
        return 80
    } else if cupNum < 32 {
        return 128
    } else if cupNum < 64 {
        return 256
    }
    return 512
}
```

**问题**:
- 只考虑 CPU 核心数，不考虑网络带宽和 IO 能力
- 对于 S3 备份，网络可能是瓶颈
- 对于本地备份，磁盘 IO 可能是瓶颈

**建议**:
```go
type ParallelConfig struct {
    MaxCPUParallel    int
    MaxNetworkParallel int
    MaxIOParallel     int
}

func getParallelCount(config ParallelConfig, isS3 bool) int {
    cpuLimit := runtime.NumCPU() * 4
    if cpuLimit > config.MaxCPUParallel {
        cpuLimit = config.MaxCPUParallel
    }
    
    if isS3 {
        // S3 受网络限制
        return min(cpuLimit, config.MaxNetworkParallel)
    }
    // 本地受 IO 限制
    return min(cpuLimit, config.MaxIOParallel)
}
```

### 1.2 🟠 Job Scheduler 效率问题

**现状**:
```go
backupJobs := make([]*tasks.Job, len(files))
// 先创建所有 job
for n := range files {
    backupJobs[idx] = getJob(srcFs, dstFs, files[n])
    idx++
}
// 再调度所有 job
for n := range backupJobs {
    err := jobScheduler.Schedule(backupJobs[n])
}
// 最后等待所有 job
for n := range backupJobs {
    ret := backupJobs[n].WaitDone()
}
```

**问题**:
- 预先创建所有 job 占用内存
- 调度和等待分离，无法及时处理错误
- 没有优先级调度

**建议**:
```go
func parallelCopyData(...) ([]*taeFile, error) {
    results := make(chan *taeFile, parallelCount)
    errors := make(chan error, 1)
    
    // 使用 worker pool
    var wg sync.WaitGroup
    for i := 0; i < parallelCount; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for file := range fileChan {
                result, err := copyFile(file)
                if err != nil {
                    select {
                    case errors <- err:
                    default:
                    }
                    return
                }
                results <- result
            }
        }()
    }
    
    // 发送文件
    go func() {
        for _, file := range files {
            fileChan <- file
        }
        close(fileChan)
    }()
    
    // 收集结果
    go func() {
        wg.Wait()
        close(results)
    }()
    
    // ...
}
```

## 2. IO 效率问题

### 2.1 🟠 小文件复制效率低

**现状**:
- 每个文件单独读写
- 对于大量小文件，IO 开销大

**建议**:
```go
// 批量复制小文件
func batchCopySmallFiles(ctx context.Context, srcFs, dstFs fileservice.FileService,
    files []*objectio.BackupObject, threshold int64) error {
    
    var smallFiles, largeFiles []*objectio.BackupObject
    for _, f := range files {
        if f.Size < threshold {
            smallFiles = append(smallFiles, f)
        } else {
            largeFiles = append(largeFiles, f)
        }
    }
    
    // 小文件打包复制
    if len(smallFiles) > 0 {
        if err := copyFilesAsTar(ctx, srcFs, dstFs, smallFiles); err != nil {
            return err
        }
    }
    
    // 大文件并行复制
    return parallelCopyData(srcFs, dstFs, largeFiles, parallelCount, nil)
}
```

### 2.2 🟠 没有使用缓冲 IO

**现状**:
```go
hashingReader := io.TeeReader(reader, hasher)
dstIoVec := fileservice.IOVector{
    Entries: []fileservice.IOEntry{{
        ReaderForWrite: hashingReader,  // 直接使用
    }},
}
```

**建议**:
```go
// 使用缓冲
bufReader := bufio.NewReaderSize(reader, 1024*1024)  // 1MB buffer
hashingReader := io.TeeReader(bufReader, hasher)
```

### 2.3 🟡 Checkpoint 加载串行

**现状**:
```go
for i, name := range names {
    // 串行加载每个 checkpoint
    oneNames, data, err = logtail.LoadCheckpointEntriesFromKey(...)
}
```

**建议**:
```go
// 并行加载 checkpoint
func loadCheckpointsParallel(ctx context.Context, names []string) ([]*objectio.BackupObject, error) {
    var wg sync.WaitGroup
    results := make(chan []*objectio.BackupObject, len(names))
    errors := make(chan error, 1)
    
    for _, name := range names {
        wg.Add(1)
        go func(n string) {
            defer wg.Done()
            objs, _, err := logtail.LoadCheckpointEntriesFromKey(...)
            if err != nil {
                select {
                case errors <- err:
                default:
                }
                return
            }
            results <- objs
        }(name)
    }
    
    // ...
}
```

## 3. 内存使用问题

### 3.1 🟠 文件列表占用大量内存

**现状**:
```go
files := make(map[string]*objectio.BackupObject, 0)
// 所有文件信息都在内存中
```

**问题**:
- 大规模备份时，文件列表可能占用 GB 级内存
- 可能导致 OOM

**建议**:
```go
// 使用迭代器模式
type FileIterator interface {
    Next() (*objectio.BackupObject, bool)
    Close()
}

// 或者分批处理
func processFilesInBatches(files []*objectio.BackupObject, batchSize int,
    processor func([]*objectio.BackupObject) error) error {
    
    for i := 0; i < len(files); i += batchSize {
        end := i + batchSize
        if end > len(files) {
            end = len(files)
        }
        if err := processor(files[i:end]); err != nil {
            return err
        }
    }
    return nil
}
```

### 3.2 🟡 GC Window 内存未及时释放

**位置**: `tae.go:CopyGCDir()`

```go
for _, metaFile := range metaFiles {
    window := gc.NewGCWindow(common.DebugAllocator, srcFs)
    // ...
    defer window.Close()  // 延迟释放
}
```

**问题**:
- 所有 window 在函数返回时才释放
- 循环中内存持续增长

## 4. 网络效率问题

### 4.1 🟠 S3 请求未优化

**现状**:
- 每个文件单独请求
- 没有使用 S3 批量操作

**建议**:
```go
// 使用 S3 批量上传
func batchUploadToS3(ctx context.Context, files []*taeFile) error {
    // 使用 S3 Multipart Upload 或 Batch Operations
}

// 使用 S3 Transfer Acceleration
func setupS3WithAcceleration(config *s3Config) (*s3Config, error) {
    config.endpoint = strings.Replace(config.endpoint, 
        "s3.", "s3-accelerate.", 1)
    return config, nil
}
```

### 4.2 🟡 没有连接池复用

**现状**:
- 每次请求可能创建新连接

**建议**:
- 配置 HTTP 客户端连接池
- 复用 TCP 连接

## 5. 进度报告问题

### 5.1 🟡 进度报告效率低

**现状**:
```go
go func() {
    for {
        // 每 5 秒打印一次
        time.Sleep(time.Second * 5)
        logutil.Info("backup", ...)
    }
}()
```

**问题**:
- 固定间隔，不够灵活
- 只有日志，没有回调

**建议**:
```go
type ProgressReporter interface {
    OnProgress(copied, total int64, filesCopied, totalFiles int)
    OnFileComplete(file string, size int64, duration time.Duration)
    OnError(file string, err error)
}

type LogProgressReporter struct {
    interval time.Duration
    lastReport time.Time
}

func (r *LogProgressReporter) OnProgress(copied, total int64, ...) {
    if time.Since(r.lastReport) < r.interval {
        return
    }
    r.lastReport = time.Now()
    logutil.Info("backup progress", ...)
}
```

## 6. 性能指标缺失

### 6.1 🟠 没有性能基准

**建议添加的指标**:
```go
var (
    // 吞吐量
    backupThroughput = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mo_backup_throughput_bytes_per_second",
        },
        []string{"type"},
    )
    
    // 延迟分布
    fileCopyLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "mo_backup_file_copy_latency_seconds",
            Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
        },
        []string{"size_bucket"},
    )
    
    // 队列深度
    pendingFiles = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "mo_backup_pending_files",
        },
    )
)
```

## 7. 优化建议汇总

| 问题 | 影响 | 优化方案 | 预期收益 |
|-----|------|---------|---------|
| 并行度固定 | 资源利用不充分 | 动态调整 | 20-50% |
| 小文件效率低 | IO 开销大 | 批量处理 | 30-60% |
| 串行加载 Checkpoint | 启动慢 | 并行加载 | 50-80% |
| 内存占用高 | OOM 风险 | 分批处理 | 降低 50% |
| S3 请求未优化 | 网络效率低 | 批量操作 | 20-40% |
