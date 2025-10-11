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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const (
	// Backup protection timeout (20 minutes)
	BackupProtectionTimeout = 20 * time.Minute
)

// BackupProtectionManager manages backup protection timestamps
type BackupProtectionManager struct {
	mu            sync.RWMutex
	protectedTS   types.TS
	lastHeartbeat time.Time
	active        bool
}

// BackupProtectionRequest represents a request to protect/unprotect backup timestamp
type BackupProtectionRequest struct {
	Action    string   `json:"action"`    // "start", "heartbeat", "stop"
	Timestamp types.TS `json:"timestamp"` // protected timestamp
}

// BackupProtectionResponse represents the response to backup protection request
type BackupProtectionResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

var globalBackupProtectionManager = &BackupProtectionManager{}

// GetGlobalBackupProtectionManager returns the global backup protection manager
func GetGlobalBackupProtectionManager() *BackupProtectionManager {
	return globalBackupProtectionManager
}

// HandleBackupProtectionRequest handles backup protection requests
func (m *BackupProtectionManager) HandleBackupProtectionRequest(requestData string) string {
	var req BackupProtectionRequest
	if err := json.Unmarshal([]byte(requestData), &req); err != nil {
		response := BackupProtectionResponse{
			Success: false,
			Message: "invalid request format: " + err.Error(),
		}
		responseBytes, _ := json.Marshal(response)
		return string(responseBytes)
	}

	var response BackupProtectionResponse

	switch req.Action {
	case "start":
		response = m.startProtection(req.Timestamp)
	case "heartbeat":
		response = m.updateHeartbeat(req.Timestamp)
	case "stop":
		response = m.stopProtection(req.Timestamp)
	default:
		response = BackupProtectionResponse{
			Success: false,
			Message: "unknown action: " + req.Action,
		}
	}

	responseBytes, _ := json.Marshal(response)
	return string(responseBytes)
}

// startProtection starts protecting the given timestamp
func (m *BackupProtectionManager) startProtection(protectedTS types.TS) BackupProtectionResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active {
		return BackupProtectionResponse{
			Success: false,
			Message: "backup protection already active for timestamp " + m.protectedTS.ToString(),
		}
	}

	m.protectedTS = protectedTS
	m.lastHeartbeat = time.Now()
	m.active = true

	logutil.Infof("[BackupProtection] Started protection for timestamp %s", protectedTS.ToString())
	return BackupProtectionResponse{
		Success: true,
		Message: "protection started for timestamp " + protectedTS.ToString(),
	}
}

// updateHeartbeat updates the heartbeat for the protected timestamp
func (m *BackupProtectionManager) updateHeartbeat(protectedTS types.TS) BackupProtectionResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active {
		return BackupProtectionResponse{
			Success: false,
			Message: "no active backup protection",
		}
	}

	if !m.protectedTS.Equal(&protectedTS) {
		return BackupProtectionResponse{
			Success: false,
			Message: "timestamp mismatch: expected " + m.protectedTS.ToString() + ", got " + protectedTS.ToString(),
		}
	}

	m.lastHeartbeat = time.Now()
	logutil.Debugf("[BackupProtection] Updated heartbeat for timestamp %s", protectedTS.ToString())
	return BackupProtectionResponse{
		Success: true,
		Message: "heartbeat updated for timestamp " + protectedTS.ToString(),
	}
}

// stopProtection stops the protection
func (m *BackupProtectionManager) stopProtection(protectedTS types.TS) BackupProtectionResponse {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.active {
		return BackupProtectionResponse{
			Success: true,
			Message: "no active backup protection",
		}
	}

	if !m.protectedTS.Equal(&protectedTS) {
		return BackupProtectionResponse{
			Success: false,
			Message: "timestamp mismatch: expected " + m.protectedTS.ToString() + ", got " + protectedTS.ToString(),
		}
	}

	m.active = false
	logutil.Infof("[BackupProtection] Stopped protection for timestamp %s", protectedTS.ToString())
	return BackupProtectionResponse{
		Success: true,
		Message: "protection stopped for timestamp " + protectedTS.ToString(),
	}
}

// IsProtected checks if a timestamp is currently protected from GC
func (m *BackupProtectionManager) IsProtected(ts *types.TS) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.active {
		return false
	}

	// Check if protection has timed out
	if time.Since(m.lastHeartbeat) > BackupProtectionTimeout {
		// Protection has timed out, but we don't modify state in read lock
		// The cleanup will be done by a separate process or next write operation
		logutil.Warnf("[BackupProtection] Protection for timestamp %s has timed out", m.protectedTS.ToString())
		return false
	}

	// A file is protected if its create timestamp is less than or equal to the protected timestamp
	// and its drop timestamp (if not empty) is greater than the protected timestamp
	return ts.LE(&m.protectedTS)
}

// GetProtectedTimestamp returns the currently protected timestamp and whether protection is active
func (m *BackupProtectionManager) GetProtectedTimestamp() (types.TS, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.active {
		return types.TS{}, false
	}

	// Check if protection has timed out
	if time.Since(m.lastHeartbeat) > BackupProtectionTimeout {
		return types.TS{}, false
	}

	return m.protectedTS, true
}

// CleanupExpiredProtection removes expired protection (should be called periodically)
func (m *BackupProtectionManager) CleanupExpiredProtection() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active && time.Since(m.lastHeartbeat) > BackupProtectionTimeout {
		logutil.Warnf("[BackupProtection] Cleaning up expired protection for timestamp %s", m.protectedTS.ToString())
		m.active = false
	}
}
