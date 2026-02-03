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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncProtectionManager_RegisterAndUnregister(t *testing.T) {
	mgr := NewSyncProtectionManager()

	jobID := "test-job-1"
	objects := []string{"object1", "object2", "object3"}
	validTS := time.Now().UnixNano()

	// Test register
	err := mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.NoError(t, err)
	assert.Equal(t, 1, mgr.GetProtectionCount())
	assert.True(t, mgr.HasProtection(jobID))

	// Test duplicate register
	err = mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test unregister (soft delete)
	err = mgr.UnregisterSyncProtection(jobID)
	require.NoError(t, err)
	assert.Equal(t, 1, mgr.GetProtectionCount()) // Still exists, just soft deleted

	active, softDeleted := mgr.GetProtectionCountByState()
	assert.Equal(t, 0, active)
	assert.Equal(t, 1, softDeleted)

	// Test unregister non-existent job
	err = mgr.UnregisterSyncProtection("non-existent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestSyncProtectionManager_GCRunningBlock(t *testing.T) {
	mgr := NewSyncProtectionManager()

	jobID := "test-job-1"
	objects := []string{"object1"}
	validTS := time.Now().UnixNano()

	// Set GC running
	mgr.SetGCRunning(true)
	assert.True(t, mgr.IsGCRunning())

	// Register should fail when GC is running
	err := mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GC is running")

	// Set GC not running
	mgr.SetGCRunning(false)
	assert.False(t, mgr.IsGCRunning())

	// Register should succeed now
	err = mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.NoError(t, err)
}

func TestSyncProtectionManager_Renew(t *testing.T) {
	mgr := NewSyncProtectionManager()

	jobID := "test-job-1"
	objects := []string{"object1"}
	validTS1 := time.Now().UnixNano()

	// Register
	err := mgr.RegisterSyncProtection(jobID, objects, validTS1)
	require.NoError(t, err)

	// Renew
	validTS2 := time.Now().Add(time.Minute).UnixNano()
	err = mgr.RenewSyncProtection(jobID, validTS2)
	require.NoError(t, err)

	// Renew non-existent job
	err = mgr.RenewSyncProtection("non-existent", validTS2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Soft delete and try to renew
	err = mgr.UnregisterSyncProtection(jobID)
	require.NoError(t, err)

	err = mgr.RenewSyncProtection(jobID, validTS2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "soft deleted")
}

func TestSyncProtectionManager_CleanupSoftDeleted(t *testing.T) {
	mgr := NewSyncProtectionManager()

	// Register multiple jobs
	job1 := "job-1"
	job2 := "job-2"

	validTS1 := int64(1000)
	validTS2 := int64(2000)

	err := mgr.RegisterSyncProtection(job1, []string{"obj1"}, validTS1)
	require.NoError(t, err)
	err = mgr.RegisterSyncProtection(job2, []string{"obj2"}, validTS2)
	require.NoError(t, err)

	// Soft delete both
	err = mgr.UnregisterSyncProtection(job1)
	require.NoError(t, err)
	err = mgr.UnregisterSyncProtection(job2)
	require.NoError(t, err)

	assert.Equal(t, 2, mgr.GetProtectionCount())

	// Cleanup with watermark = 1500 (only job1 should be cleaned)
	mgr.CleanupSoftDeleted(1500)
	assert.Equal(t, 1, mgr.GetProtectionCount())
	assert.False(t, mgr.HasProtection(job1))
	assert.True(t, mgr.HasProtection(job2))

	// Cleanup with watermark = 2500 (job2 should be cleaned)
	mgr.CleanupSoftDeleted(2500)
	assert.Equal(t, 0, mgr.GetProtectionCount())
}

func TestSyncProtectionManager_CleanupExpired(t *testing.T) {
	mgr := NewSyncProtectionManager()
	mgr.ttl = 100 * time.Millisecond // Short TTL for testing

	jobID := "test-job-1"
	objects := []string{"object1"}

	// Register with old validTS
	oldValidTS := time.Now().Add(-200 * time.Millisecond).UnixNano()
	err := mgr.RegisterSyncProtection(jobID, objects, oldValidTS)
	require.NoError(t, err)

	assert.Equal(t, 1, mgr.GetProtectionCount())

	// Cleanup expired
	mgr.CleanupExpired()
	assert.Equal(t, 0, mgr.GetProtectionCount())
}

func TestSyncProtectionManager_CleanupExpired_NotSoftDeleted(t *testing.T) {
	mgr := NewSyncProtectionManager()
	mgr.ttl = 100 * time.Millisecond

	jobID := "test-job-1"

	// Register with old validTS
	oldValidTS := time.Now().Add(-200 * time.Millisecond).UnixNano()
	err := mgr.RegisterSyncProtection(jobID, []string{"object1"}, oldValidTS)
	require.NoError(t, err)

	// Soft delete it
	err = mgr.UnregisterSyncProtection(jobID)
	require.NoError(t, err)

	// CleanupExpired should NOT clean soft-deleted entries
	mgr.CleanupExpired()
	assert.Equal(t, 1, mgr.GetProtectionCount()) // Still exists
}

func TestSyncProtectionManager_IsProtected(t *testing.T) {
	mgr := NewSyncProtectionManager()

	jobID := "job-1"
	objects := []string{"protected-obj"}
	validTS := time.Now().UnixNano()

	err := mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.NoError(t, err)

	assert.True(t, mgr.IsProtected("protected-obj"))
	assert.False(t, mgr.IsProtected("unprotected-obj"))
}

func TestSyncProtectionManager_FilterProtectedFiles(t *testing.T) {
	mgr := NewSyncProtectionManager()

	jobID := "job-1"
	objects := []string{"protected1", "protected2"}
	validTS := time.Now().UnixNano()

	err := mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.NoError(t, err)

	files := []string{"protected1", "protected2", "unprotected1", "unprotected2"}
	canDelete := mgr.FilterProtectedFiles(files)

	assert.Len(t, canDelete, 2)
	assert.Contains(t, canDelete, "unprotected1")
	assert.Contains(t, canDelete, "unprotected2")
	assert.NotContains(t, canDelete, "protected1")
	assert.NotContains(t, canDelete, "protected2")
}

func TestSyncProtectionManager_FilterProtectedFiles_NoProtection(t *testing.T) {
	mgr := NewSyncProtectionManager()

	files := []string{"file1", "file2", "file3"}
	canDelete := mgr.FilterProtectedFiles(files)

	assert.Equal(t, files, canDelete)
}

func TestSyncProtectionManager_MaxCount(t *testing.T) {
	mgr := NewSyncProtectionManager()
	mgr.maxCount = 2 // Set low max for testing

	objects := []string{"obj"}
	validTS := time.Now().UnixNano()

	// Register up to max
	err := mgr.RegisterSyncProtection("job-1", objects, validTS)
	require.NoError(t, err)
	err = mgr.RegisterSyncProtection("job-2", objects, validTS)
	require.NoError(t, err)

	// Should fail when max reached
	err = mgr.RegisterSyncProtection("job-3", objects, validTS)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max count reached")
}

func TestSyncProtectionManager_ConcurrentAccess(t *testing.T) {
	mgr := NewSyncProtectionManager()

	// Concurrent register/unregister
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			jobID := "job-" + string(rune('0'+id))
			objects := []string{"obj"}
			validTS := time.Now().UnixNano()

			_ = mgr.RegisterSyncProtection(jobID, objects, validTS)
			_ = mgr.RenewSyncProtection(jobID, validTS+1000)
			_ = mgr.UnregisterSyncProtection(jobID)
			_ = mgr.IsProtected("obj")
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestSyncProtectionManager_FullWorkflow(t *testing.T) {
	mgr := NewSyncProtectionManager()

	// Simulate sync job workflow
	jobID := "sync-job-123"
	objects := []string{"table1/obj1", "table1/obj2", "table2/obj1"}
	validTS := time.Now().UnixNano()

	// Step 1: Check GC not running, register protection
	assert.False(t, mgr.IsGCRunning())
	err := mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.NoError(t, err)

	// Step 2: Simulate GC starts (should not affect existing protection)
	mgr.SetGCRunning(true)

	// Step 3: GC tries to delete files - protected files should be filtered
	filesToDelete := []string{"table1/obj1", "table1/obj2", "table2/obj1", "table3/obj1"}
	canDelete := mgr.FilterProtectedFiles(filesToDelete)
	assert.Len(t, canDelete, 1)
	assert.Contains(t, canDelete, "table3/obj1")

	// Step 4: GC ends
	mgr.SetGCRunning(false)

	// Step 5: Sync job completes, soft delete protection
	err = mgr.UnregisterSyncProtection(jobID)
	require.NoError(t, err)

	// Step 6: Next GC cleans up soft-deleted protection when watermark > validTS
	checkpointWatermark := validTS + 1000000 // Watermark > validTS
	mgr.CleanupSoftDeleted(checkpointWatermark)
	assert.Equal(t, 0, mgr.GetProtectionCount())
}

func TestSyncProtectionManager_CheckpointWatermarkEdgeCase(t *testing.T) {
	mgr := NewSyncProtectionManager()

	jobID := "job-1"
	objects := []string{"obj"}
	validTS := int64(1000)

	err := mgr.RegisterSyncProtection(jobID, objects, validTS)
	require.NoError(t, err)
	err = mgr.UnregisterSyncProtection(jobID)
	require.NoError(t, err)

	// Watermark == validTS: should NOT be cleaned (need strictly greater)
	mgr.CleanupSoftDeleted(1000)
	assert.Equal(t, 1, mgr.GetProtectionCount())

	// Watermark > validTS: should be cleaned
	mgr.CleanupSoftDeleted(1001)
	assert.Equal(t, 0, mgr.GetProtectionCount())
}

func TestSyncProtectionManager_MultipleJobsProtection(t *testing.T) {
	mgr := NewSyncProtectionManager()

	// Register multiple jobs with overlapping objects
	job1 := "job-1"
	job2 := "job-2"
	validTS := time.Now().UnixNano()

	err := mgr.RegisterSyncProtection(job1, []string{"obj1", "obj2"}, validTS)
	require.NoError(t, err)
	err = mgr.RegisterSyncProtection(job2, []string{"obj2", "obj3"}, validTS)
	require.NoError(t, err)

	// All objects should be protected
	assert.True(t, mgr.IsProtected("obj1"))
	assert.True(t, mgr.IsProtected("obj2"))
	assert.True(t, mgr.IsProtected("obj3"))

	// Soft delete job1
	err = mgr.UnregisterSyncProtection(job1)
	require.NoError(t, err)

	// obj1 and obj2 should still be protected (obj2 by job2, obj1 by soft-deleted job1)
	assert.True(t, mgr.IsProtected("obj1"))
	assert.True(t, mgr.IsProtected("obj2"))
	assert.True(t, mgr.IsProtected("obj3"))

	// Cleanup job1
	mgr.CleanupSoftDeleted(validTS + 1)

	// Now obj1 should not be protected, but obj2 and obj3 still are
	assert.False(t, mgr.IsProtected("obj1"))
	assert.True(t, mgr.IsProtected("obj2"))
	assert.True(t, mgr.IsProtected("obj3"))
}

func TestSyncProtectionManager_GetProtectedObjectCount(t *testing.T) {
	mgr := NewSyncProtectionManager()

	assert.Equal(t, 0, mgr.GetProtectedObjectCount())

	validTS := time.Now().UnixNano()
	err := mgr.RegisterSyncProtection("job-1", []string{"obj1", "obj2"}, validTS)
	require.NoError(t, err)
	assert.Equal(t, 2, mgr.GetProtectedObjectCount())

	err = mgr.RegisterSyncProtection("job-2", []string{"obj3"}, validTS)
	require.NoError(t, err)
	assert.Equal(t, 3, mgr.GetProtectedObjectCount())
}
