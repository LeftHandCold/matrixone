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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// GCConfig represents the complete GC configuration
type GCConfig struct {
	// Core GC settings
	Enabled             bool `json:"enabled" yaml:"enabled"`
	CheckEnabled        bool `json:"check_enabled" yaml:"check_enabled"`
	CheckpointGCEnabled bool `json:"checkpoint_gc_enabled" yaml:"checkpoint_gc_enabled"`

	// Performance tuning
	CoarseEstimateRows      int     `json:"coarse_estimate_rows" yaml:"coarse_estimate_rows"`
	CoarseProbility         float64 `json:"coarse_probility" yaml:"coarse_probility"`
	CanGCCacheSize          int     `json:"can_gc_cache_size" yaml:"can_gc_cache_size"`
	MaxMergeCheckpointCount int     `json:"max_merge_checkpoint_count" yaml:"max_merge_checkpoint_count"`

	// Memory management
	InMemoryStagedSize int `json:"in_memory_staged_size" yaml:"in_memory_staged_size"`
	BufferSize         int `json:"buffer_size" yaml:"buffer_size"`

	// Timing settings
	ScanInterval            time.Duration `json:"scan_interval" yaml:"scan_interval"`
	GCInterval              time.Duration `json:"gc_interval" yaml:"gc_interval"`
	CheckpointRetentionTime time.Duration `json:"checkpoint_retention_time" yaml:"checkpoint_retention_time"`

	// Concurrency settings
	MaxConcurrentTasks int `json:"max_concurrent_tasks" yaml:"max_concurrent_tasks"`
	WorkerPoolSize     int `json:"worker_pool_size" yaml:"worker_pool_size"`

	// File management
	MaxFilesPerBatch      int           `json:"max_files_per_batch" yaml:"max_files_per_batch"`
	FileDeleteBatchSize   int           `json:"file_delete_batch_size" yaml:"file_delete_batch_size"`
	TempFileRetentionTime time.Duration `json:"temp_file_retention_time" yaml:"temp_file_retention_time"`

	// Advanced settings
	EnableMetrics          bool    `json:"enable_metrics" yaml:"enable_metrics"`
	EnableDetailedLogging  bool    `json:"enable_detailed_logging" yaml:"enable_detailed_logging"`
	EnableDebugMode        bool    `json:"enable_debug_mode" yaml:"enable_debug_mode"`
	MaxRetryAttempts       int     `json:"max_retry_attempts" yaml:"max_retry_attempts"`
	RetryBackoffMultiplier float64 `json:"retry_backoff_multiplier" yaml:"retry_backoff_multiplier"`

	// Resource limits
	MaxMemoryUsage      int64 `json:"max_memory_usage" yaml:"max_memory_usage"`
	MaxDiskUsage        int64 `json:"max_disk_usage" yaml:"max_disk_usage"`
	MaxNetworkBandwidth int64 `json:"max_network_bandwidth" yaml:"max_network_bandwidth"`
}

// DefaultGCConfig returns a GC configuration with sensible defaults
func DefaultGCConfig() *GCConfig {
	return &GCConfig{
		// Core GC settings
		Enabled:             true,
		CheckEnabled:        true,
		CheckpointGCEnabled: true,

		// Performance tuning
		CoarseEstimateRows:      DefaultCoarseEstimateRows,
		CoarseProbility:         DefaultCoarseProbility,
		CanGCCacheSize:          DefaultCanGCTailSize,
		MaxMergeCheckpointCount: 10,

		// Memory management
		InMemoryStagedSize: DefaultInMemoryStagedSize,
		BufferSize:         DefaultBufferSize,

		// Timing settings
		ScanInterval:            5 * time.Minute,
		GCInterval:              15 * time.Minute,
		CheckpointRetentionTime: 24 * time.Hour,

		// Concurrency settings
		MaxConcurrentTasks: 4,
		WorkerPoolSize:     8,

		// File management
		MaxFilesPerBatch:      1000,
		FileDeleteBatchSize:   100,
		TempFileRetentionTime: 1 * time.Hour,

		// Advanced settings
		EnableMetrics:          true,
		EnableDetailedLogging:  false,
		EnableDebugMode:        false,
		MaxRetryAttempts:       3,
		RetryBackoffMultiplier: 2.0,

		// Resource limits
		MaxMemoryUsage:      2 * mpool.GB,
		MaxDiskUsage:        100 * mpool.GB,
		MaxNetworkBandwidth: 100 * malloc.MB,
	}
}

// Validate validates the GC configuration
func (gc *GCConfig) Validate() error {
	if gc.CoarseEstimateRows <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("coarse_estimate_rows must be positive, got %d", gc.CoarseEstimateRows),
			"coarse_estimate_rows",
		)
	}

	if gc.CoarseProbility <= 0 || gc.CoarseProbility >= 1 {
		return NewConfigValidationError(
			fmt.Errorf("coarse_probility must be between 0 and 1, got %f", gc.CoarseProbility),
			"coarse_probility",
		)
	}

	if gc.CanGCCacheSize <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("can_gc_cache_size must be positive, got %d", gc.CanGCCacheSize),
			"can_gc_cache_size",
		)
	}

	if gc.MaxMergeCheckpointCount <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("max_merge_checkpoint_count must be positive, got %d", gc.MaxMergeCheckpointCount),
			"max_merge_checkpoint_count",
		)
	}

	if gc.InMemoryStagedSize <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("in_memory_staged_size must be positive, got %d", gc.InMemoryStagedSize),
			"in_memory_staged_size",
		)
	}

	if gc.BufferSize <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("buffer_size must be positive, got %d", gc.BufferSize),
			"buffer_size",
		)
	}

	if gc.ScanInterval <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("scan_interval must be positive, got %v", gc.ScanInterval),
			"scan_interval",
		)
	}

	if gc.GCInterval <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("gc_interval must be positive, got %v", gc.GCInterval),
			"gc_interval",
		)
	}

	if gc.MaxConcurrentTasks <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("max_concurrent_tasks must be positive, got %d", gc.MaxConcurrentTasks),
			"max_concurrent_tasks",
		)
	}

	if gc.WorkerPoolSize <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("worker_pool_size must be positive, got %d", gc.WorkerPoolSize),
			"worker_pool_size",
		)
	}

	if gc.MaxFilesPerBatch <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("max_files_per_batch must be positive, got %d", gc.MaxFilesPerBatch),
			"max_files_per_batch",
		)
	}

	if gc.FileDeleteBatchSize <= 0 {
		return NewConfigValidationError(
			fmt.Errorf("file_delete_batch_size must be positive, got %d", gc.FileDeleteBatchSize),
			"file_delete_batch_size",
		)
	}

	if gc.MaxRetryAttempts < 0 {
		return NewConfigValidationError(
			fmt.Errorf("max_retry_attempts must be non-negative, got %d", gc.MaxRetryAttempts),
			"max_retry_attempts",
		)
	}

	if gc.RetryBackoffMultiplier <= 1.0 {
		return NewConfigValidationError(
			fmt.Errorf("retry_backoff_multiplier must be greater than 1.0, got %f", gc.RetryBackoffMultiplier),
			"retry_backoff_multiplier",
		)
	}

	return nil
}

// SetDefaults sets default values for unset fields
func (gc *GCConfig) SetDefaults() {
	defaults := DefaultGCConfig()

	if gc.CoarseEstimateRows <= 0 {
		gc.CoarseEstimateRows = defaults.CoarseEstimateRows
	}
	if gc.CoarseProbility <= 0 {
		gc.CoarseProbility = defaults.CoarseProbility
	}
	if gc.CanGCCacheSize <= 0 {
		gc.CanGCCacheSize = defaults.CanGCCacheSize
	}
	if gc.MaxMergeCheckpointCount <= 0 {
		gc.MaxMergeCheckpointCount = defaults.MaxMergeCheckpointCount
	}
	if gc.InMemoryStagedSize <= 0 {
		gc.InMemoryStagedSize = defaults.InMemoryStagedSize
	}
	if gc.BufferSize <= 0 {
		gc.BufferSize = defaults.BufferSize
	}
	if gc.ScanInterval <= 0 {
		gc.ScanInterval = defaults.ScanInterval
	}
	if gc.GCInterval <= 0 {
		gc.GCInterval = defaults.GCInterval
	}
	if gc.CheckpointRetentionTime <= 0 {
		gc.CheckpointRetentionTime = defaults.CheckpointRetentionTime
	}
	if gc.MaxConcurrentTasks <= 0 {
		gc.MaxConcurrentTasks = defaults.MaxConcurrentTasks
	}
	if gc.WorkerPoolSize <= 0 {
		gc.WorkerPoolSize = defaults.WorkerPoolSize
	}
	if gc.MaxFilesPerBatch <= 0 {
		gc.MaxFilesPerBatch = defaults.MaxFilesPerBatch
	}
	if gc.FileDeleteBatchSize <= 0 {
		gc.FileDeleteBatchSize = defaults.FileDeleteBatchSize
	}
	if gc.TempFileRetentionTime <= 0 {
		gc.TempFileRetentionTime = defaults.TempFileRetentionTime
	}
	if gc.MaxRetryAttempts <= 0 {
		gc.MaxRetryAttempts = defaults.MaxRetryAttempts
	}
	if gc.RetryBackoffMultiplier <= 1.0 {
		gc.RetryBackoffMultiplier = defaults.RetryBackoffMultiplier
	}
	if gc.MaxMemoryUsage <= 0 {
		gc.MaxMemoryUsage = defaults.MaxMemoryUsage
	}
	if gc.MaxDiskUsage <= 0 {
		gc.MaxDiskUsage = defaults.MaxDiskUsage
	}
	if gc.MaxNetworkBandwidth <= 0 {
		gc.MaxNetworkBandwidth = defaults.MaxNetworkBandwidth
	}
}

// Clone creates a deep copy of the configuration
func (gc *GCConfig) Clone() *GCConfig {
	return &GCConfig{
		Enabled:                 gc.Enabled,
		CheckEnabled:            gc.CheckEnabled,
		CheckpointGCEnabled:     gc.CheckpointGCEnabled,
		CoarseEstimateRows:      gc.CoarseEstimateRows,
		CoarseProbility:         gc.CoarseProbility,
		CanGCCacheSize:          gc.CanGCCacheSize,
		MaxMergeCheckpointCount: gc.MaxMergeCheckpointCount,
		InMemoryStagedSize:      gc.InMemoryStagedSize,
		BufferSize:              gc.BufferSize,
		ScanInterval:            gc.ScanInterval,
		GCInterval:              gc.GCInterval,
		CheckpointRetentionTime: gc.CheckpointRetentionTime,
		MaxConcurrentTasks:      gc.MaxConcurrentTasks,
		WorkerPoolSize:          gc.WorkerPoolSize,
		MaxFilesPerBatch:        gc.MaxFilesPerBatch,
		FileDeleteBatchSize:     gc.FileDeleteBatchSize,
		TempFileRetentionTime:   gc.TempFileRetentionTime,
		EnableMetrics:           gc.EnableMetrics,
		EnableDetailedLogging:   gc.EnableDetailedLogging,
		EnableDebugMode:         gc.EnableDebugMode,
		MaxRetryAttempts:        gc.MaxRetryAttempts,
		RetryBackoffMultiplier:  gc.RetryBackoffMultiplier,
		MaxMemoryUsage:          gc.MaxMemoryUsage,
		MaxDiskUsage:            gc.MaxDiskUsage,
		MaxNetworkBandwidth:     gc.MaxNetworkBandwidth,
	}
}

// String returns a string representation of the configuration
func (gc *GCConfig) String() string {
	return fmt.Sprintf("GCConfig{Enabled:%v, CoarseEstimateRows:%d, CoarseProbility:%f, CanGCCacheSize:%d, MaxMergeCheckpointCount:%d}",
		gc.Enabled, gc.CoarseEstimateRows, gc.CoarseProbility, gc.CanGCCacheSize, gc.MaxMergeCheckpointCount)
}

// ConfigManager implementation
type gcConfigManager struct {
	config *GCConfig
}

// NewConfigManager creates a new configuration manager
func NewConfigManager(config *GCConfig) ConfigManager {
	if config == nil {
		config = DefaultGCConfig()
	}
	config.SetDefaults()
	return &gcConfigManager{config: config}
}

func (gcm *gcConfigManager) GetCoarseEstimateRows() int {
	return gcm.config.CoarseEstimateRows
}

func (gcm *gcConfigManager) GetCoarseProbility() float64 {
	return gcm.config.CoarseProbility
}

func (gcm *gcConfigManager) GetCanGCCacheSize() int {
	return gcm.config.CanGCCacheSize
}

func (gcm *gcConfigManager) GetMaxMergeCheckpointCount() int {
	return gcm.config.MaxMergeCheckpointCount
}

func (gcm *gcConfigManager) Validate() error {
	return gcm.config.Validate()
}

func (gcm *gcConfigManager) SetDefaults() {
	gcm.config.SetDefaults()
}

// Additional configuration getters
func (gcm *gcConfigManager) GetConfig() *GCConfig {
	return gcm.config.Clone()
}

func (gcm *gcConfigManager) UpdateConfig(newConfig *GCConfig) error {
	if err := newConfig.Validate(); err != nil {
		return err
	}
	gcm.config = newConfig.Clone()
	return nil
}

func (gcm *gcConfigManager) IsEnabled() bool {
	return gcm.config.Enabled
}

func (gcm *gcConfigManager) IsCheckEnabled() bool {
	return gcm.config.CheckEnabled
}

func (gcm *gcConfigManager) IsCheckpointGCEnabled() bool {
	return gcm.config.CheckpointGCEnabled
}

func (gcm *gcConfigManager) GetScanInterval() time.Duration {
	return gcm.config.ScanInterval
}

func (gcm *gcConfigManager) GetGCInterval() time.Duration {
	return gcm.config.GCInterval
}

func (gcm *gcConfigManager) GetMaxConcurrentTasks() int {
	return gcm.config.MaxConcurrentTasks
}

func (gcm *gcConfigManager) GetWorkerPoolSize() int {
	return gcm.config.WorkerPoolSize
}

func (gcm *gcConfigManager) GetMaxFilesPerBatch() int {
	return gcm.config.MaxFilesPerBatch
}

func (gcm *gcConfigManager) GetFileDeleteBatchSize() int {
	return gcm.config.FileDeleteBatchSize
}

func (gcm *gcConfigManager) IsMetricsEnabled() bool {
	return gcm.config.EnableMetrics
}

func (gcm *gcConfigManager) IsDetailedLoggingEnabled() bool {
	return gcm.config.EnableDetailedLogging
}

func (gcm *gcConfigManager) IsDebugModeEnabled() bool {
	return gcm.config.EnableDebugMode
}

func (gcm *gcConfigManager) GetMaxRetryAttempts() int {
	return gcm.config.MaxRetryAttempts
}

func (gcm *gcConfigManager) GetRetryBackoffMultiplier() float64 {
	return gcm.config.RetryBackoffMultiplier
}

func (gcm *gcConfigManager) GetMaxMemoryUsage() int64 {
	return gcm.config.MaxMemoryUsage
}

func (gcm *gcConfigManager) GetMaxDiskUsage() int64 {
	return gcm.config.MaxDiskUsage
}

func (gcm *gcConfigManager) GetMaxNetworkBandwidth() int64 {
	return gcm.config.MaxNetworkBandwidth
}
