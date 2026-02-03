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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
	JobID      string                   // Sync job ID
	BF         *bloomfilter.BloomFilter // BloomFilter for protected objects
	ValidTS    int64                    // Valid timestamp (nanoseconds), needs to be renewed
	SoftDelete bool                     // Whether soft deleted
	CreateTime time.Time                // Creation time for logging
}

// SyncProtectionManager manages sync protection entries
type SyncProtectionManager struct {
	sync.RWMutex
	protections map[string]*SyncProtection // jobID -> protection
	gcRunning   atomic.Bool                // Whether GC is running
	ttl         time.Duration              // TTL for non-soft-deleted protections
	maxCount    int                        // Maximum number of protections
	mp          *mpool.MPool               // Memory pool for vector operations
}

// NewSyncProtectionManager creates a new SyncProtectionManager
func NewSyncProtectionManager() *SyncProtectionManager {
	mp, _ := mpool.NewMPool("sync_protection", 0, mpool.NoFixed)
	return &SyncProtectionManager{
		protections: make(map[string]*SyncProtection),
		ttl:         DefaultSyncProtectionTTL,
		maxCount:    DefaultMaxSyncProtections,
		mp:          mp,
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

// RegisterSyncProtection registers a new sync protection with BloomFilter
// bfData is base64 encoded BloomFilter bytes
// Returns error if GC is running or job already exists
func (m *SyncProtectionManager) RegisterSyncProtection(
	jobID string,
	bfData string,
	validTS int64,
) error {
	m.Lock()
	defer m.Unlock()

	// Debug: print received data info with hash
	bfDataHash := fmt.Sprintf("%x", sha256.Sum256([]byte(bfData)))
	logutil.Info(
		"GC-Sync-Protection-Register-Received",
		zap.String("job-id", jobID),
		zap.Int("bf-data-len", len(bfData)),
		zap.String("bf-data-sha256", bfDataHash),
		zap.String("bf-data-prefix", func() string {
			if len(bfData) > 100 {
				return bfData[:100] + "..."
			}
			return bfData
		}()),
	)

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

	// Decode base64 BloomFilter data
	bfBytes, err := base64.StdEncoding.DecodeString(bfData)
	if err != nil {
		logutil.Error(
			"GC-Sync-Protection-Register-Decode-Error",
			zap.String("job-id", jobID),
			zap.Int("bf-data-len", len(bfData)),
			zap.Error(err),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to decode bloom filter: %v", err))
	}

	// Calculate hash of decoded bytes
	decodedHash := fmt.Sprintf("%x", sha256.Sum256(bfBytes))
	logutil.Info(
		"GC-Sync-Protection-Register-Decoded",
		zap.String("job-id", jobID),
		zap.Int("bf-bytes-len", len(bfBytes)),
		zap.String("bf-bytes-sha256", decodedHash),
		zap.String("bf-bytes-prefix", fmt.Sprintf("%v", bfBytes[:min(64, len(bfBytes))])),
	)

	// Unmarshal BloomFilter
	bf := &bloomfilter.BloomFilter{}
	if err := bf.Unmarshal(bfBytes); err != nil {
		logutil.Error(
			"GC-Sync-Protection-Register-Unmarshal-Error",
			zap.String("job-id", jobID),
			zap.Int("bf-bytes-len", len(bfBytes)),
			zap.Error(err),
		)
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to unmarshal bloom filter: %v", err))
	}

	// Debug: verify BloomFilter is valid and test a sample
	logutil.Info(
		"GC-Sync-Protection-Register-BF-Valid",
		zap.String("job-id", jobID),
		zap.Bool("bf-valid", bf.Valid()),
	)

	// Debug: try to test a simple string to verify BF works
	testVec := vector.NewVec(types.T_varchar.ToType())
	testStr := "test-string-12345"
	if err := vector.AppendBytes(testVec, []byte(testStr), false, m.mp); err == nil {
		// This should return false since we didn't add this string
		result := bf.TestRow(testVec, 0)
		logutil.Info(
			"GC-Sync-Protection-Register-BF-Test-Random",
			zap.String("job-id", jobID),
			zap.String("test-str", testStr),
			zap.Bool("result", result),
		)
	}
	testVec.Free(m.mp)

	// Debug: test with a sample object name format to verify BF works with real data
	sampleObjectName := "019c2296-5e73-79fc-986f-7a3f4584d960_00000"
	testVec2 := vector.NewVec(types.T_varchar.ToType())
	if err := vector.AppendBytes(testVec2, []byte(sampleObjectName), false, m.mp); err == nil {
		result := bf.TestRow(testVec2, 0)
		logutil.Info(
			"GC-Sync-Protection-Register-BF-Test-Sample-Object",
			zap.String("job-id", jobID),
			zap.String("sample-object", sampleObjectName),
			zap.Int("sample-object-len", len(sampleObjectName)),
			zap.String("sample-object-bytes", fmt.Sprintf("%v", []byte(sampleObjectName))),
			zap.Bool("result", result),
		)
	}
	testVec2.Free(m.mp)

	m.protections[jobID] = &SyncProtection{
		JobID:      jobID,
		BF:         bf,
		ValidTS:    validTS,
		SoftDelete: false,
		CreateTime: time.Now(),
	}

	logutil.Info(
		"GC-Sync-Protection-Registered",
		zap.String("job-id", jobID),
		zap.Int64("valid-ts", validTS),
		zap.Int("bf-size", len(bfBytes)),
		zap.String("bf-base64-sha256", bfDataHash),
		zap.String("bf-bytes-sha256", decodedHash),
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
			if p.BF != nil {
				p.BF.Free()
			}
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
			if p.BF != nil {
				p.BF.Free()
			}
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

// IsProtected checks if an object name is protected by any BloomFilter
func (m *SyncProtectionManager) IsProtected(objectName string) bool {
	m.RLock()
	defer m.RUnlock()

	if len(m.protections) == 0 {
		return false
	}

	// Create a vector with single string for testing
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(m.mp)
	if err := vector.AppendBytes(vec, []byte(objectName), false, m.mp); err != nil {
		return false
	}

	for _, p := range m.protections {
		if p.BF == nil || !p.BF.Valid() {
			continue
		}
		// Use TestRow for single element test
		if p.BF.TestRow(vec, 0) {
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

	if len(m.protections) == 0 || len(files) == 0 {
		logutil.Info(
			"GC-Sync-Protection-Filter-Skip",
			zap.Int("protections", len(m.protections)),
			zap.Int("files", len(files)),
		)
		return files
	}

	// Collect all valid BloomFilters
	var bfs []*bloomfilter.BloomFilter
	var jobIDs []string
	for jobID, p := range m.protections {
		if p.BF != nil && p.BF.Valid() {
			bfs = append(bfs, p.BF)
			jobIDs = append(jobIDs, jobID)
			logutil.Info(
				"GC-Sync-Protection-Filter-BF-Found",
				zap.String("job-id", jobID),
				zap.Bool("soft-delete", p.SoftDelete),
			)
		} else {
			logutil.Warn(
				"GC-Sync-Protection-Filter-BF-Invalid",
				zap.String("job-id", jobID),
				zap.Bool("bf-nil", p.BF == nil),
			)
		}
	}

	if len(bfs) == 0 {
		logutil.Warn(
			"GC-Sync-Protection-Filter-No-Valid-BF",
			zap.Int("protections", len(m.protections)),
		)
		return files
	}

	logutil.Info(
		"GC-Sync-Protection-Filter-Start",
		zap.Int("files-to-check", len(files)),
		zap.Int("bf-count", len(bfs)),
		zap.Strings("job-ids", jobIDs),
	)

	// Print sample files to check
	if len(files) > 0 {
		sampleCount := 5
		if len(files) < sampleCount {
			sampleCount = len(files)
		}
		logutil.Info(
			"GC-Sync-Protection-Filter-Sample-Files-To-Delete",
			zap.Strings("sample-files", files[:sampleCount]),
			zap.Int("file-0-len", len(files[0])),
			zap.String("file-0-bytes", fmt.Sprintf("%v", []byte(files[0]))),
		)
	}

	result := make([]string, 0, len(files))
	skipped := 0
	protectedFiles := make([]string, 0)

	// Create a vector for batch testing
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(m.mp)

	for i, f := range files {
		vec.Reset(types.T_varchar.ToType())
		if err := vector.AppendBytes(vec, []byte(f), false, m.mp); err != nil {
			// On error, keep the file (don't delete)
			logutil.Error(
				"GC-Sync-Protection-Filter-Vector-Error",
				zap.String("file", f),
				zap.Error(err),
			)
			skipped++
			continue
		}

		protected := false
		for bfIdx, bf := range bfs {
			if bf.TestRow(vec, 0) {
				protected = true
				if len(protectedFiles) < 10 {
					logutil.Info(
						"GC-Sync-Protection-Filter-File-Protected-By-BF",
						zap.String("file", f),
						zap.String("job-id", jobIDs[bfIdx]),
					)
				}
				break
			}
		}

		if !protected {
			result = append(result, f)
			// Log first few unprotected files for debugging
			if i < 5 {
				logutil.Info(
					"GC-Sync-Protection-Filter-File-NOT-Protected",
					zap.String("file", f),
					zap.Int("file-len", len(f)),
					zap.String("file-bytes", fmt.Sprintf("%v", []byte(f))),
				)
			}
		} else {
			skipped++
			protectedFiles = append(protectedFiles, f)
		}
	}

	// Log protected files summary
	if len(protectedFiles) > 0 {
		sampleProtected := protectedFiles
		if len(sampleProtected) > 10 {
			sampleProtected = sampleProtected[:10]
		}
		logutil.Info(
			"GC-Sync-Protection-Filter-Protected-Files-Summary",
			zap.Int("protected-count", len(protectedFiles)),
			zap.Strings("sample-protected", sampleProtected),
		)
	}

	logutil.Info(
		"GC-Sync-Protection-Filtered-Files-Result",
		zap.Int("total", len(files)),
		zap.Int("skipped", skipped),
		zap.Int("can-delete", len(result)),
		zap.Int("protected", len(protectedFiles)),
	)

	return result
}

// DebugTestFile tests if a single file is protected and logs detailed info
func (m *SyncProtectionManager) DebugTestFile(fileName string) bool {
	m.RLock()
	defer m.RUnlock()

	logutil.Info(
		"GC-Sync-Protection-Debug-Test-File",
		zap.String("file", fileName),
		zap.Int("file-len", len(fileName)),
		zap.Int("protections", len(m.protections)),
	)

	if len(m.protections) == 0 {
		logutil.Info("GC-Sync-Protection-Debug-No-Protections")
		return false
	}

	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(m.mp)

	if err := vector.AppendBytes(vec, []byte(fileName), false, m.mp); err != nil {
		logutil.Error(
			"GC-Sync-Protection-Debug-Vector-Error",
			zap.Error(err),
		)
		return false
	}

	for jobID, p := range m.protections {
		if p.BF == nil {
			logutil.Info(
				"GC-Sync-Protection-Debug-BF-Nil",
				zap.String("job-id", jobID),
			)
			continue
		}
		if !p.BF.Valid() {
			logutil.Info(
				"GC-Sync-Protection-Debug-BF-Invalid",
				zap.String("job-id", jobID),
			)
			continue
		}

		result := p.BF.TestRow(vec, 0)
		logutil.Info(
			"GC-Sync-Protection-Debug-Test-Result",
			zap.String("job-id", jobID),
			zap.String("file", fileName),
			zap.Bool("protected", result),
		)
		if result {
			return true
		}
	}

	return false
}
