// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestBackupProtectionManager(t *testing.T) {
	mgr := NewBackupProtectionManager()
	defer mgr.Stop()

	// Test adding protection
	backupID := "test-backup-123"
	backupTS := types.BuildTS(time.Now().UnixNano(), 0)

	mgr.AddProtection(backupID, backupTS)

	// Test heartbeat update
	assert.True(t, mgr.UpdateHeartbeat(backupID))
	assert.False(t, mgr.UpdateHeartbeat("non-existent"))

	// Test protection check
	testTS := types.BuildTS(time.Now().UnixNano()-1000, 0) // earlier than backup
	assert.True(t, mgr.IsProtected(testTS))

	// Test with timestamp after backup (should not be protected)
	futureTS := types.BuildTS(time.Now().UnixNano()+1000, 0)
	assert.False(t, mgr.IsProtected(futureTS))

	// Test getting active protections
	protections := mgr.GetActiveProtections()
	assert.Len(t, protections, 1)
	assert.Equal(t, backupID, protections[0].ID)

	// Test removing protection
	mgr.RemoveProtection(backupID)
	protections = mgr.GetActiveProtections()
	assert.Len(t, protections, 0)
}

func TestBackupProtectionExpiry(t *testing.T) {
	mgr := NewBackupProtectionManager()
	mgr.HeartbeatTimeout = 100 * time.Millisecond // very short timeout for testing
	mgr.CleanupInterval = 50 * time.Millisecond
	defer mgr.Stop()

	backupID := "test-backup-expiry"
	backupTS := types.BuildTS(time.Now().UnixNano(), 0)

	mgr.AddProtection(backupID, backupTS)

	// Verify protection exists
	assert.Len(t, mgr.GetActiveProtections(), 1)

	// Wait for expiry
	time.Sleep(200 * time.Millisecond)

	// Protection should be expired and removed
	protections := mgr.GetActiveProtections()
	assert.Len(t, protections, 0)
}
