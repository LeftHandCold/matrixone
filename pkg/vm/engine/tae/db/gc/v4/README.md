# GC v4 Architecture Design

## 概述

GC v4 是对 GC v3 的重大架构升级，主要变化是将 GC 元数据存储从文件系统迁移到系统表，提供更好的可查询性、一致性和可维护性。

## 架构对比

### v3 架构 (文件系统存储)
```
gc/
├── meta_1000_2000.ckp      # GC检查点元数据
├── snap_1000_2000.snap     # 快照元数据
├── acct_1000_2000.acct     # 账户表信息
└── ...
```

### v4 架构 (系统表存储)
```sql
-- GC元数据管理表
CREATE TABLE mo_gc_metadata (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_name VARCHAR(255) NOT NULL,
    metadata_type ENUM('checkpoint', 'snapshot', 'account', 'object_list'),
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    account_id INT,
    database_id BIGINT,
    table_id BIGINT,
    content LONGBLOB,
    watermark_type ENUM('scan', 'gc', 'checkpoint_gc'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_task_type (task_name, metadata_type),
    INDEX idx_time_range (start_ts, end_ts),
    INDEX idx_watermark (watermark_type, end_ts)
);

-- 对象列表表
CREATE TABLE mo_gc_objects (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    object_name VARCHAR(512) NOT NULL,
    object_stats LONGBLOB NOT NULL,
    create_ts TIMESTAMP NOT NULL,
    delete_ts TIMESTAMP,
    database_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    account_id INT NOT NULL,
    object_type ENUM('data', 'tombstone') NOT NULL,
    gc_status ENUM('pending', 'processed', 'deleted') DEFAULT 'pending',
    task_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_object_name (object_name),
    INDEX idx_table_time (table_id, create_ts, delete_ts),
    INDEX idx_gc_status (gc_status, created_at),
    INDEX idx_task_name (task_name)
);

-- 快照引用表
CREATE TABLE mo_gc_snapshots (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    snapshot_id VARCHAR(255) NOT NULL,
    snapshot_name VARCHAR(255),
    snapshot_ts TIMESTAMP NOT NULL,
    level ENUM('cluster', 'account', 'database', 'table') NOT NULL,
    account_id INT,
    database_id BIGINT,
    table_id BIGINT,
    account_name VARCHAR(255),
    database_name VARCHAR(255),
    table_name VARCHAR(255),
    object_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_snapshot_ts (snapshot_ts),
    INDEX idx_level_account (level, account_id),
    INDEX idx_table_snapshot (table_id, snapshot_ts)
);

-- PITR信息表
CREATE TABLE mo_gc_pitr (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    pitr_id VARCHAR(255) NOT NULL,
    pitr_name VARCHAR(255),
    create_account VARCHAR(255),
    create_time TIMESTAMP NOT NULL,
    modified_time TIMESTAMP NOT NULL,
    level ENUM('cluster', 'account', 'database', 'table') NOT NULL,
    account_id INT,
    database_id BIGINT,
    table_id BIGINT,
    account_name VARCHAR(255),
    database_name VARCHAR(255),
    table_name VARCHAR(255),
    object_id BIGINT,
    length_value INT NOT NULL,
    time_unit ENUM('y', 'mo', 'd', 'h', 'm') NOT NULL,
    drop_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pitr_level (level, account_id),
    INDEX idx_pitr_time (create_time, modified_time)
);

-- 水位线管理表
CREATE TABLE mo_gc_watermarks (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    watermark_type ENUM('scan', 'gc', 'checkpoint_gc') NOT NULL,
    watermark_ts TIMESTAMP NOT NULL,
    checkpoint_location VARCHAR(1024),
    checkpoint_version INT,
    task_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_watermark_type (watermark_type),
    INDEX idx_watermark_ts (watermark_ts)
);
```

## 核心组件设计

### 1. SystemTableMetadataStore
负责与系统表交互的核心存储层

### 2. GCMetadataManager  
管理GC元数据的CRUD操作

### 3. WatermarkManager
管理各种水位线信息

### 4. SnapshotTableManager
管理快照和PITR信息

### 5. ObjectListManager
管理对象列表和GC状态

## 主要优势

### 1. 可查询性
- 标准SQL查询接口
- 复杂的多维度查询支持
- 实时监控和统计

### 2. 一致性
- 事务保证
- ACID特性
- 并发控制

### 3. 可维护性
- 标准化的元数据格式
- 更好的数据治理
- 简化的备份恢复

### 4. 扩展性
- 支持分片和扩展
- 更好的性能优化
- 灵活的索引策略

## 迁移策略

### 阶段1: 双写模式
- 同时写入文件和系统表
- v3读取文件，v4读取系统表
- 验证数据一致性

### 阶段2: 迁移模式  
- 从v3文件迁移到v4系统表
- 提供迁移工具和验证机制
- 逐步切换读取来源

### 阶段3: 纯v4模式
- 完全使用系统表
- 移除文件系统依赖
- 清理遗留代码

## 性能优化

### 1. 索引策略
- 时间范围索引
- 多维度复合索引
- 分区表支持

### 2. 批量操作
- 批量插入优化
- 批量查询优化
- 分页查询支持

### 3. 缓存策略
- 元数据缓存
- 查询结果缓存
- 智能预取

## 监控和可观测性

### 1. 元数据统计
```sql
-- 查看GC对象统计
SELECT 
    gc_status,
    object_type,
    COUNT(*) as object_count,
    SUM(LENGTH(object_stats)) as total_size
FROM mo_gc_objects 
GROUP BY gc_status, object_type;

-- 查看快照分布
SELECT 
    level,
    DATE(snapshot_ts) as snapshot_date,
    COUNT(*) as snapshot_count
FROM mo_gc_snapshots 
GROUP BY level, DATE(snapshot_ts)
ORDER BY snapshot_date DESC;
```

### 2. 性能监控
- GC执行时间
- 元数据操作延迟
- 系统表大小增长

### 3. 健康检查
- 数据完整性验证
- 水位线连续性检查
- 孤儿数据清理

## API 设计

### 1. MetadataStore接口
```go
type MetadataStore interface {
    // 对象管理
    SaveObjects(ctx context.Context, objects []ObjectInfo) error
    LoadObjectsByTimeRange(ctx context.Context, start, end types.TS) ([]ObjectInfo, error)
    UpdateObjectGCStatus(ctx context.Context, objectNames []string, status GCStatus) error
    
    // 快照管理
    SaveSnapshots(ctx context.Context, snapshots []SnapshotInfo) error
    LoadSnapshotsByLevel(ctx context.Context, level SnapshotLevel, entityID uint64) ([]SnapshotInfo, error)
    
    // 水位线管理
    SaveWatermark(ctx context.Context, watermark WatermarkInfo) error
    LoadWatermark(ctx context.Context, wType WatermarkType) (*WatermarkInfo, error)
    
    // 元数据管理
    SaveMetadata(ctx context.Context, metadata MetadataInfo) error
    LoadMetadata(ctx context.Context, taskName string, mType MetadataType) ([]MetadataInfo, error)
}
```

### 2. 查询接口
```go
type QueryService interface {
    // 统计查询
    GetGCStatistics(ctx context.Context, timeRange TimeRange) (*GCStatistics, error)
    
    // 对象查询  
    QueryObjects(ctx context.Context, filter ObjectFilter) ([]ObjectInfo, error)
    
    // 快照查询
    QuerySnapshots(ctx context.Context, filter SnapshotFilter) ([]SnapshotInfo, error)
    
    // 健康检查
    ValidateDataIntegrity(ctx context.Context) (*IntegrityReport, error)
}
```

## 部署和运维

### 1. 初始化
- 系统表创建脚本
- 默认配置初始化
- 权限和安全设置

### 2. 升级
- v3到v4的迁移脚本
- 数据验证工具
- 回滚机制

### 3. 维护
- 定期清理策略
- 性能优化建议
- 故障排查指南 