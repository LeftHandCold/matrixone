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

package gc

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestBackupProtectionManager(t *testing.T) {
	manager := GetGlobalBackupProtectionManager()
	assert.NotNil(t, manager)

	// Initially no protection should be active
	_, isProtected := manager.GetProtectedTimestamp()
	assert.False(t, isProtected)

	// Test start protection
	protectedTS := types.BuildTS(time.Now().UnixNano(), 0)
	req := BackupProtectionRequest{
		Action:    "start",
		Timestamp: protectedTS,
	}
	reqData, _ := json.Marshal(req)
	response := manager.HandleBackupProtectionRequest(string(reqData))

	var resp BackupProtectionResponse
	err := json.Unmarshal([]byte(response), &resp)
	assert.NoError(t, err)
	assert.True(t, resp.Success)

	// Check if protection is now active
	currentTS, isProtected := manager.GetProtectedTimestamp()
	assert.True(t, isProtected)
	assert.True(t, currentTS.Equal(&protectedTS))

	// Test heartbeat
	heartbeatReq := BackupProtectionRequest{
		Action:    "heartbeat",
		Timestamp: protectedTS,
	}
	heartbeatData, _ := json.Marshal(heartbeatReq)
	heartbeatResponse := manager.HandleBackupProtectionRequest(string(heartbeatData))

	var heartbeatResp BackupProtectionResponse
	err = json.Unmarshal([]byte(heartbeatResponse), &heartbeatResp)
	assert.NoError(t, err)
	assert.True(t, heartbeatResp.Success)

	// Test stop protection
	stopReq := BackupProtectionRequest{
		Action:    "stop",
		Timestamp: protectedTS,
	}
	stopData, _ := json.Marshal(stopReq)
	stopResponse := manager.HandleBackupProtectionRequest(string(stopData))

	var stopResp BackupProtectionResponse
	err = json.Unmarshal([]byte(stopResponse), &stopResp)
	assert.NoError(t, err)
	assert.True(t, stopResp.Success)

	// Check if protection is now inactive
	_, isProtected = manager.GetProtectedTimestamp()
	assert.False(t, isProtected)
}

func TestBackupProtectionIsProtected(t *testing.T) {
	manager := GetGlobalBackupProtectionManager()

	// Start protection
	protectedTS := types.BuildTS(time.Now().UnixNano(), 0)
	req := BackupProtectionRequest{
		Action:    "start",
		Timestamp: protectedTS,
	}
	reqData, _ := json.Marshal(req)
	manager.HandleBackupProtectionRequest(string(reqData))

	// Test IsProtected method
	// Files created before or at protected timestamp should be protected
	beforeTS := types.BuildTS(protectedTS.Physical()-1000, 0)
	assert.True(t, manager.IsProtected(&beforeTS))
	assert.True(t, manager.IsProtected(&protectedTS))

	// Files created after protected timestamp should not be protected
	afterTS := types.BuildTS(protectedTS.Physical()+1000, 0)
	assert.False(t, manager.IsProtected(&afterTS))

	// Clean up
	stopReq := BackupProtectionRequest{
		Action:    "stop",
		Timestamp: protectedTS,
	}
	stopData, _ := json.Marshal(stopReq)
	manager.HandleBackupProtectionRequest(string(stopData))
}

func TestBackupProtectionTimeout(t *testing.T) {
	manager := GetGlobalBackupProtectionManager()

	// Start protection
	protectedTS := types.BuildTS(time.Now().UnixNano(), 0)
	req := BackupProtectionRequest{
		Action:    "start",
		Timestamp: protectedTS,
	}
	reqData, _ := json.Marshal(req)
	manager.HandleBackupProtectionRequest(string(reqData))

	// Manually set last heartbeat to an old time to simulate timeout
	manager.mu.Lock()
	manager.lastHeartbeat = time.Now().Add(-BackupProtectionTimeout - time.Minute)
	manager.mu.Unlock()

	// Check if protection is considered expired
	_, isProtected := manager.GetProtectedTimestamp()
	assert.False(t, isProtected)

	// Cleanup expired protection should work
	manager.CleanupExpiredProtection()
	_, isProtected = manager.GetProtectedTimestamp()
	assert.False(t, isProtected)
}
