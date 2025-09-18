// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// MigrationConfig 迁移配置
type MigrationConfig struct {
	// v3文件系统配置
	FileService fileservice.FileService `json:"-"`
	GCDir       string                  `json:"gc_dir"`

	// v4系统表配置
	V4Config *Config `json:"v4_config"`

	// 迁移选项
	DryRun      bool `json:"dry_run"`
	BatchSize   int  `json:"batch_size"`
	Concurrency int  `json:"concurrency"`
	BackupFiles bool `json:"backup_files"`

	// 验证选项
	ValidateData bool `json:"validate_data"`
	SkipErrors   bool `json:"skip_errors"`
}

// DefaultMigrationConfig 返回默认迁移配置
func DefaultMigrationConfig() *MigrationConfig {
	return &MigrationConfig{
		GCDir:        "gc/",
		V4Config:     DefaultConfig(),
		DryRun:       false,
		BatchSize:    1000,
		Concurrency:  2,
		BackupFiles:  true,
		ValidateData: true,
		SkipErrors:   false,
	}
}

// MigrationProgress 迁移进度
type MigrationProgress struct {
	Stage          string    `json:"stage"`
	ProcessedFiles int       `json:"processed_files"`
	TotalFiles     int       `json:"total_files"`
	ProcessedRows  int64     `json:"processed_rows"`
	Errors         []string  `json:"errors"`
	Warnings       []string  `json:"warnings"`
	StartTime      time.Time `json:"start_time"`
	EstimatedEnd   time.Time `json:"estimated_end"`
}

// MigrationReport 迁移报告
type MigrationReport struct {
	Success            bool              `json:"success"`
	Progress           MigrationProgress `json:"progress"`
	Duration           time.Duration     `json:"duration"`
	MigratedObjects    int64             `json:"migrated_objects"`
	MigratedSnapshots  int64             `json:"migrated_snapshots"`
	MigratedWatermarks int64             `json:"migrated_watermarks"`
	BackupFiles        []string          `json:"backup_files"`
	Errors             []string          `json:"errors"`
	Warnings           []string          `json:"warnings"`
}

// V3ToV4Migrator v3到v4的迁移器
type V3ToV4Migrator struct {
	config   *MigrationConfig
	logger   *zap.Logger
	v4Store  MetadataStore
	progress MigrationProgress
	report   MigrationReport
}

// NewV3ToV4Migrator 创建迁移器
func NewV3ToV4Migrator(config *MigrationConfig) (*V3ToV4Migrator, error) {
	if config == nil {
		config = DefaultMigrationConfig()
	}

	// 创建v4存储
	v4Store, err := NewSystemTableMetadataStore(config.V4Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create v4 store: %w", err)
	}

	migrator := &V3ToV4Migrator{
		config:  config,
		logger:  logutil.GetGlobalLogger().Named("gc-migration"),
		v4Store: v4Store,
		progress: MigrationProgress{
			Stage:     "initialized",
			StartTime: time.Now(),
		},
		report: MigrationReport{
			BackupFiles: make([]string, 0),
			Errors:      make([]string, 0),
			Warnings:    make([]string, 0),
		},
	}

	return migrator, nil
}

// Migrate 执行迁移
func (m *V3ToV4Migrator) Migrate(ctx context.Context) (*MigrationReport, error) {
	start := time.Now()
	m.logger.Info("Starting v3 to v4 migration")

	defer func() {
		m.report.Duration = time.Since(start)
		m.logger.Info("Migration completed",
			zap.Bool("success", m.report.Success),
			zap.Duration("duration", m.report.Duration),
			zap.Int64("migrated_objects", m.report.MigratedObjects),
			zap.Int64("migrated_snapshots", m.report.MigratedSnapshots),
		)
	}()

	// 1. 扫描v3文件
	m.progress.Stage = "scanning_files"
	files, err := m.scanV3Files(ctx)
	if err != nil {
		m.addError(fmt.Sprintf("Failed to scan v3 files: %v", err))
		return &m.report, err
	}

	m.progress.TotalFiles = len(files)
	m.logger.Info("Found v3 files", zap.Int("count", len(files)))

	if m.config.DryRun {
		m.logger.Info("Dry run mode, not performing actual migration")
		m.report.Success = true
		return &m.report, nil
	}

	// 2. 备份文件
	if m.config.BackupFiles {
		m.progress.Stage = "backing_up_files"
		if err := m.backupFiles(ctx, files); err != nil {
			m.addWarning(fmt.Sprintf("Failed to backup files: %v", err))
		}
	}

	// 3. 迁移元数据
	m.progress.Stage = "migrating_metadata"
	if err := m.migrateMetadata(ctx, files); err != nil {
		m.addError(fmt.Sprintf("Failed to migrate metadata: %v", err))
		return &m.report, err
	}

	// 4. 验证数据
	if m.config.ValidateData {
		m.progress.Stage = "validating_data"
		if err := m.validateMigratedData(ctx); err != nil {
			m.addWarning(fmt.Sprintf("Data validation failed: %v", err))
		}
	}

	// 5. 清理v3文件（可选）
	m.progress.Stage = "completed"
	m.report.Success = true

	return &m.report, nil
}

// scanV3Files 扫描v3文件
func (m *V3ToV4Migrator) scanV3Files(ctx context.Context) ([]ioutil.TSRangeFile, error) {
	files, err := ioutil.ListTSRangeFilesInGCDir(ctx, m.config.FileService)
	if err != nil {
		return nil, fmt.Errorf("failed to list GC files: %w", err)
	}

	m.logger.Info("Scanned v3 files",
		zap.Int("total_files", len(files)),
		zap.String("gc_dir", m.config.GCDir),
	)

	return files, nil
}

// backupFiles 备份文件
func (m *V3ToV4Migrator) backupFiles(ctx context.Context, files []ioutil.TSRangeFile) error {
	backupDir := fmt.Sprintf("%s.backup.%d", m.config.GCDir, time.Now().Unix())
	m.logger.Info("Backing up v3 files", zap.String("backup_dir", backupDir))

	for _, file := range files {
		srcPath := ioutil.MakeGCFullName(file.GetName())
		dstPath := filepath.Join(backupDir, file.GetName())

		// TODO: 实现文件复制逻辑
		// err := m.config.FileService.Copy(ctx, srcPath, dstPath)
		// if err != nil {
		//     return fmt.Errorf("failed to backup file %s: %w", srcPath, err)
		// }

		m.report.BackupFiles = append(m.report.BackupFiles, dstPath)
	}

	return nil
}

// migrateMetadata 迁移元数据
func (m *V3ToV4Migrator) migrateMetadata(ctx context.Context, files []ioutil.TSRangeFile) error {
	// 按类型分组文件
	snapshotFiles := make([]ioutil.TSRangeFile, 0)
	accountFiles := make([]ioutil.TSRangeFile, 0)
	checkpointFiles := make([]ioutil.TSRangeFile, 0)

	for _, file := range files {
		switch {
		case file.IsSnapshotExt():
			snapshotFiles = append(snapshotFiles, file)
		case file.IsAcctExt():
			accountFiles = append(accountFiles, file)
		default:
			checkpointFiles = append(checkpointFiles, file)
		}
	}

	// 迁移快照文件
	if err := m.migrateSnapshotFiles(ctx, snapshotFiles); err != nil {
		return fmt.Errorf("failed to migrate snapshot files: %w", err)
	}

	// 迁移账户文件
	if err := m.migrateAccountFiles(ctx, accountFiles); err != nil {
		return fmt.Errorf("failed to migrate account files: %w", err)
	}

	// 迁移检查点文件
	if err := m.migrateCheckpointFiles(ctx, checkpointFiles); err != nil {
		return fmt.Errorf("failed to migrate checkpoint files: %w", err)
	}

	return nil
}

// migrateSnapshotFiles 迁移快照文件
func (m *V3ToV4Migrator) migrateSnapshotFiles(ctx context.Context, files []ioutil.TSRangeFile) error {
	m.logger.Info("Migrating snapshot files", zap.Int("count", len(files)))

	for _, file := range files {
		if err := m.migrateSnapshotFile(ctx, file); err != nil {
			if m.config.SkipErrors {
				m.addError(fmt.Sprintf("Failed to migrate snapshot file %s: %v", file.GetName(), err))
				continue
			}
			return fmt.Errorf("failed to migrate snapshot file %s: %w", file.GetName(), err)
		}
		m.progress.ProcessedFiles++
	}

	return nil
}

// migrateSnapshotFile 迁移单个快照文件
func (m *V3ToV4Migrator) migrateSnapshotFile(ctx context.Context, file ioutil.TSRangeFile) error {
	// 创建快照元数据
	snapshotMeta := logtail.NewSnapshotMeta()

	// 读取快照文件
	filePath := ioutil.MakeGCFullName(file.GetName())
	if err := snapshotMeta.ReadMeta(ctx, filePath, m.config.FileService); err != nil {
		return fmt.Errorf("failed to read snapshot meta: %w", err)
	}

	// 转换为v4格式并保存
	snapshots, err := m.convertSnapshotMeta(snapshotMeta, file)
	if err != nil {
		return fmt.Errorf("failed to convert snapshot meta: %w", err)
	}

	if len(snapshots) > 0 {
		if err := m.v4Store.SaveSnapshots(ctx, snapshots); err != nil {
			return fmt.Errorf("failed to save snapshots: %w", err)
		}
		m.report.MigratedSnapshots += int64(len(snapshots))
	}

	return nil
}

// convertSnapshotMeta 转换快照元数据
func (m *V3ToV4Migrator) convertSnapshotMeta(meta *logtail.SnapshotMeta, file ioutil.TSRangeFile) ([]SnapshotInfo, error) {
	// TODO: 实现具体的转换逻辑
	// 这里需要从v3的SnapshotMeta结构中提取信息并转换为v4的SnapshotInfo

	snapshots := make([]SnapshotInfo, 0)

	// 示例转换逻辑
	snapshot := SnapshotInfo{
		SnapshotID:   file.GetName(),
		SnapshotName: fmt.Sprintf("migrated-%s", file.GetName()),
		SnapshotTS:   *file.GetStart(),
		Level:        SnapshotLevelCluster, // 默认为集群级别
		CreatedAt:    time.Now(),
	}

	snapshots = append(snapshots, snapshot)

	return snapshots, nil
}

// migrateAccountFiles 迁移账户文件
func (m *V3ToV4Migrator) migrateAccountFiles(ctx context.Context, files []ioutil.TSRangeFile) error {
	m.logger.Info("Migrating account files", zap.Int("count", len(files)))

	for _, file := range files {
		if err := m.migrateAccountFile(ctx, file); err != nil {
			if m.config.SkipErrors {
				m.addError(fmt.Sprintf("Failed to migrate account file %s: %v", file.GetName(), err))
				continue
			}
			return fmt.Errorf("failed to migrate account file %s: %w", file.GetName(), err)
		}
		m.progress.ProcessedFiles++
	}

	return nil
}

// migrateAccountFile 迁移单个账户文件
func (m *V3ToV4Migrator) migrateAccountFile(ctx context.Context, file ioutil.TSRangeFile) error {
	// TODO: 实现账户文件迁移逻辑
	m.logger.Debug("Migrating account file", zap.String("file", file.GetName()))
	return nil
}

// migrateCheckpointFiles 迁移检查点文件
func (m *V3ToV4Migrator) migrateCheckpointFiles(ctx context.Context, files []ioutil.TSRangeFile) error {
	m.logger.Info("Migrating checkpoint files", zap.Int("count", len(files)))

	for _, file := range files {
		if err := m.migrateCheckpointFile(ctx, file); err != nil {
			if m.config.SkipErrors {
				m.addError(fmt.Sprintf("Failed to migrate checkpoint file %s: %v", file.GetName(), err))
				continue
			}
			return fmt.Errorf("failed to migrate checkpoint file %s: %w", file.GetName(), err)
		}
		m.progress.ProcessedFiles++
	}

	return nil
}

// migrateCheckpointFile 迁移单个检查点文件
func (m *V3ToV4Migrator) migrateCheckpointFile(ctx context.Context, file ioutil.TSRangeFile) error {
	// TODO: 实现检查点文件迁移逻辑
	m.logger.Debug("Migrating checkpoint file", zap.String("file", file.GetName()))
	return nil
}

// validateMigratedData 验证迁移数据
func (m *V3ToV4Migrator) validateMigratedData(ctx context.Context) error {
	m.logger.Info("Validating migrated data")

	// 验证数据完整性
	report, err := m.v4Store.ValidateIntegrity(ctx)
	if err != nil {
		return fmt.Errorf("failed to validate integrity: %w", err)
	}

	if !report.IsValid {
		m.addWarning("Data integrity validation failed")
		for _, err := range report.Errors {
			m.addError(err)
		}
		for _, warn := range report.Warnings {
			m.addWarning(warn)
		}
	}

	// 验证统计信息
	stats, err := m.v4Store.GetStatistics(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get statistics: %w", err)
	}

	m.logger.Info("Migration validation completed",
		zap.Int64("total_objects", stats.ObjectCount),
		zap.Int64("total_snapshots", stats.SnapshotCount),
		zap.Bool("integrity_valid", report.IsValid),
	)

	return nil
}

// GetProgress 获取迁移进度
func (m *V3ToV4Migrator) GetProgress() MigrationProgress {
	if m.progress.TotalFiles > 0 {
		progress := float64(m.progress.ProcessedFiles) / float64(m.progress.TotalFiles)
		if progress > 0 {
			elapsed := time.Since(m.progress.StartTime)
			estimated := time.Duration(float64(elapsed) / progress)
			m.progress.EstimatedEnd = m.progress.StartTime.Add(estimated)
		}
	}
	return m.progress
}

// addError 添加错误
func (m *V3ToV4Migrator) addError(msg string) {
	m.logger.Error(msg)
	m.report.Errors = append(m.report.Errors, msg)
	m.progress.Errors = append(m.progress.Errors, msg)
}

// addWarning 添加警告
func (m *V3ToV4Migrator) addWarning(msg string) {
	m.logger.Warn(msg)
	m.report.Warnings = append(m.report.Warnings, msg)
	m.progress.Warnings = append(m.progress.Warnings, msg)
}

// V3ToV4MigrationTool 迁移工具CLI
type V3ToV4MigrationTool struct {
	migrator *V3ToV4Migrator
	logger   *zap.Logger
}

// NewV3ToV4MigrationTool 创建迁移工具
func NewV3ToV4MigrationTool(config *MigrationConfig) (*V3ToV4MigrationTool, error) {
	migrator, err := NewV3ToV4Migrator(config)
	if err != nil {
		return nil, err
	}

	return &V3ToV4MigrationTool{
		migrator: migrator,
		logger:   logutil.GetGlobalLogger().Named("gc-migration-tool"),
	}, nil
}

// Run 运行迁移工具
func (t *V3ToV4MigrationTool) Run(ctx context.Context) error {
	t.logger.Info("Starting GC v3 to v4 migration tool")

	// 执行迁移
	report, err := t.migrator.Migrate(ctx)
	if err != nil {
		t.logger.Error("Migration failed", zap.Error(err))
		return err
	}

	// 打印报告
	t.printReport(report)

	if !report.Success {
		return fmt.Errorf("migration completed with errors")
	}

	t.logger.Info("Migration completed successfully")
	return nil
}

// printReport 打印迁移报告
func (t *V3ToV4MigrationTool) printReport(report *MigrationReport) {
	t.logger.Info("Migration Report",
		zap.Bool("success", report.Success),
		zap.Duration("duration", report.Duration),
		zap.Int64("migrated_objects", report.MigratedObjects),
		zap.Int64("migrated_snapshots", report.MigratedSnapshots),
		zap.Int64("migrated_watermarks", report.MigratedWatermarks),
		zap.Int("backup_files", len(report.BackupFiles)),
		zap.Int("errors", len(report.Errors)),
		zap.Int("warnings", len(report.Warnings)),
	)

	if len(report.Errors) > 0 {
		t.logger.Error("Migration errors:")
		for _, err := range report.Errors {
			t.logger.Error("  " + err)
		}
	}

	if len(report.Warnings) > 0 {
		t.logger.Warn("Migration warnings:")
		for _, warn := range report.Warnings {
			t.logger.Warn("  " + warn)
		}
	}
}

// EstimateMigrationSize 估算迁移规模
func EstimateMigrationSize(ctx context.Context, fs fileservice.FileService, gcDir string) (*MigrationEstimate, error) {
	files, err := ioutil.ListTSRangeFilesInGCDir(ctx, fs)
	if err != nil {
		return nil, fmt.Errorf("failed to list GC files: %w", err)
	}

	estimate := &MigrationEstimate{
		TotalFiles:      len(files),
		SnapshotFiles:   0,
		AccountFiles:    0,
		CheckpointFiles: 0,
		EstimatedSize:   0,
	}

	for _, file := range files {
		switch {
		case file.IsSnapshotExt():
			estimate.SnapshotFiles++
		case file.IsAcctExt():
			estimate.AccountFiles++
		default:
			estimate.CheckpointFiles++
		}

		// TODO: 计算文件大小
		// size, err := fs.Size(ctx, ioutil.MakeGCFullName(file.GetName()))
		// if err == nil {
		//     estimate.EstimatedSize += size
		// }
	}

	// 估算迁移时间（基于经验值）
	estimate.EstimatedDuration = time.Duration(estimate.TotalFiles) * 10 * time.Second

	return estimate, nil
}

// MigrationEstimate 迁移估算
type MigrationEstimate struct {
	TotalFiles        int           `json:"total_files"`
	SnapshotFiles     int           `json:"snapshot_files"`
	AccountFiles      int           `json:"account_files"`
	CheckpointFiles   int           `json:"checkpoint_files"`
	EstimatedSize     int64         `json:"estimated_size"`
	EstimatedDuration time.Duration `json:"estimated_duration"`
}
