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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultSyncProtectionTTL is the default TTL for sync protection
	// If a protection is not renewed within this duration, it will be force cleaned
	DefaultSyncProtectionTTL = 20 * time.Minute

	// DefaultMaxSyncProtections is the default maximum number of sync protections
	DefaultMaxSyncProtections = 100
)

// SyncProtection represents a single sync protection entry
type SyncProtection struct {
	JobID      string            // Sync job ID
	Objects    map[string]struct{} // Protected object names
	ValidTS    int64             // Valid timestamp (nanoseconds), needs to be renewed
	SoftDelete bool              // Whether soft deleted
	CreateTime time.Time         // Creation time for logging
}

// SyncProtectionManager manages sync protection entries
type SyncProtectionManager struct {
	sync.RWMutex
	protections map[string]*SyncProtection // jobID -> protection
	gcRunning   atomic.Bool                // Whether GC is running
	ttl         time.Duration              // TTL for non-soft-deleted protections
	maxCount    int                        // Maximum number of protections
}

// NewSyncProtectionManager creates a new SyncProtectionManager
func NewSyncProtectionManager() *SyncProtectionManager {
	return &SyncProtectionManager{
		protections: make(map[string]*SyncProtection),
		ttl:         DefaultSyncProtectionTTL,
		maxCount:    DefaultMaxSyncProtections,
	}
}

// SetGCRunning sets the GC running state
func (m *SyncProtectionManager) SetGCRunning(running bool) {
	m.gcRunning.Store(running)
	logutil.Debug(
		"GC-Sync-Protection-GC-State-Changed",
		zap.Bool("running", running),
	)
}

// IsGCRunning returns whether GC is running
func (m *SyncProtectionManager) IsGCRunning() bool {
	return m.gcRunning.Load()
}

// RegisterSyncProtection registers a new sync protection
// Returns error if GC is running or job already exists
func (m *SyncProtectionManager) RegisterSyncProtection(
	jobID string,
	objects []string,
	validTS int64,
) error {
	m.Lock()
	defer m.Unlock()

	// Check if GC is running
	if m.gcRunning.Load() {
		logutil.Info(
			"GC-Sync-Protection-Register-Rejected-GC-Running",
			zap.String("job-id", jobID),
		)
		return moerr.NewInternalErrorNoCtx("GC is running, please retry later")
	}

	// Check if job already exists
	if _, ok := m.protections[jobID]; ok {
		logutil.Warn(
			"GC-Sync-Protection-Register-Already-Exists",
			zap.String("job-id", jobID),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("sync protection already exists: %s", jobID))
	}

	// Check max count
	if len(m.protections) >= m.maxCount {
		logutil.Warn(
			"GC-Sync-Protection-Register-Max-Count-Reached",
			zap.String("job-id", jobID),
			zap.Int("current-count", len(m.protections)),
			zap.Int("max-count", m.maxCount),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("sync protection max count reached: %d", m.maxCount))
	}

	// Build object set
	objectSet := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		objectSet[obj] = struct{}{}
	}

	m.protections[jobID] = &SyncProtection{
		JobID:      jobID,
		Objects:    objectSet,
		ValidTS:    validTS,
		SoftDelete: false,
		CreateTime: time.Now(),
	}

	logutil.Info(
		"GC-Sync-Protection-Registered",
		zap.String("job-id", jobID),
		zap.Int64("valid-ts", validTS),
		zap.Int("object-count", len(objects)),
		zap.Int("total-protections", len(m.protections)),
	)
	return nil
}


// RenewSyncProtection renews the valid timestamp of a sync protection
func (m *SyncProtectionManager) RenewSyncProtection(jobID string, validTS int64) error {
	m.Lock()
	defer m.Unlock()

	p, ok := m.protections[jobID]
	if !ok {
		logutil.Warn(
			"GC-Sync-Protection-Renew-Not-Found",
			zap.String("job-id", jobID),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("sync protection not found: %s", jobID))
	}

	if p.SoftDelete {
		logutil.Warn(
			"GC-Sync-Protection-Renew-Already-Soft-Deleted",
			zap.String("job-id", jobID),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("sync protection is soft deleted: %s", jobID))
	}

	oldValidTS := p.ValidTS
	p.ValidTS = validTS

	logutil.Debug(
		"GC-Sync-Protection-Renewed",
		zap.String("job-id", jobID),
		zap.Int64("old-valid-ts", oldValidTS),
		zap.Int64("new-valid-ts", validTS),
	)
	return nil
}

// UnregisterSyncProtection soft deletes a sync protection
// Returns error if job not found (sync job needs to handle rollback)
func (m *SyncProtectionManager) UnregisterSyncProtection(jobID string) error {
	m.Lock()
	defer m.Unlock()

	p, ok := m.protections[jobID]
	if !ok {
		logutil.Warn(
			"GC-Sync-Protection-Unregister-Not-Found",
			zap.String("job-id", jobID),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("sync protection not found: %s", jobID))
	}

	p.SoftDelete = true

	logutil.Info(
		"GC-Sync-Protection-Soft-Deleted",
		zap.String("job-id", jobID),
		zap.Int64("valid-ts", p.ValidTS),
	)
	return nil
}

// CleanupSoftDeleted cleans up soft-deleted protections when checkpoint watermark > validTS
// This should be called during GC when processing checkpoints
func (m *SyncProtectionManager) CleanupSoftDeleted(checkpointWatermark int64) {
	m.Lock()
	defer m.Unlock()

	for jobID, p := range m.protections {
		// Condition: soft delete state AND checkpoint watermark > validTS
		if p.SoftDelete && checkpointWatermark > p.ValidTS {
			delete(m.protections, jobID)
			logutil.Info(
				"GC-Sync-Protection-Cleaned-Soft-Deleted",
				zap.String("job-id", jobID),
				zap.Int64("valid-ts", p.ValidTS),
				zap.Int64("checkpoint-watermark", checkpointWatermark),
			)
		}
	}
}

// CleanupExpired cleans up expired protections (TTL exceeded and not soft deleted)
// This handles crashed sync jobs that didn't unregister
func (m *SyncProtectionManager) CleanupExpired() {
	m.Lock()
	defer m.Unlock()

	now := time.Now()
	for jobID, p := range m.protections {
		validTime := time.Unix(0, p.ValidTS)

		// Non soft delete state, but TTL exceeded without renewal
		if !p.SoftDelete && now.Sub(validTime) > m.ttl {
			delete(m.protections, jobID)
			logutil.Warn(
				"GC-Sync-Protection-Force-Cleaned-Expired",
				zap.String("job-id", jobID),
				zap.Int64("valid-ts", p.ValidTS),
				zap.Duration("age", now.Sub(validTime)),
				zap.Duration("ttl", m.ttl),
			)
		}
	}
}

// GetProtectionCount returns the number of protections
func (m *SyncProtectionManager) GetProtectionCount() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.protections)
}

// GetProtectionCountByState returns the count of protections by state
func (m *SyncProtectionManager) GetProtectionCountByState() (active, softDeleted int) {
	m.RLock()
	defer m.RUnlock()

	for _, p := range m.protections {
		if p.SoftDelete {
			softDeleted++
		} else {
			active++
		}
	}
	return
}

// HasProtection checks if a job has protection
func (m *SyncProtectionManager) HasProtection(jobID string) bool {
	m.RLock()
	defer m.RUnlock()
	_, ok := m.protections[jobID]
	return ok
}

// IsProtected checks if an object name is protected by any protection
func (m *SyncProtectionManager) IsProtected(objectName string) bool {
	m.RLock()
	defer m.RUnlock()

	for _, p := range m.protections {
		if _, ok := p.Objects[objectName]; ok {
			return true
		}
	}
	return false
}

// FilterProtectedFiles filters out protected files from the list
// Returns files that are NOT protected (can be deleted)
func (m *SyncProtectionManager) FilterProtectedFiles(files []string) []string {
	m.RLock()
	defer m.RUnlock()

	if len(m.protections) == 0 {
		return files
	}

	// Build merged set of all protected objects
	protectedSet := make(map[string]struct{})
	for _, p := range m.protections {
		for obj := range p.Objects {
			protectedSet[obj] = struct{}{}
		}
	}

	result := make([]string, 0, len(files))
	skipped := 0
	for _, f := range files {
		if _, protected := protectedSet[f]; !protected {
			result = append(result, f)
		} else {
			skipped++
		}
	}

	if skipped > 0 {
		logutil.Info(
			"GC-Sync-Protection-Filtered-Files",
			zap.Int("total", len(files)),
			zap.Int("skipped", skipped),
			zap.Int("can-delete", len(result)),
		)
	}
	return result
}

// GetProtectedObjectCount returns the total number of protected objects
func (m *SyncProtectionManager) GetProtectedObjectCount() int {
	m.RLock()
	defer m.RUnlock()

	count := 0
	for _, p := range m.protections {
		count += len(p.Objects)
	}
	return count
}
