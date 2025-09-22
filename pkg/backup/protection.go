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
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// BackupProtection represents a backup protection entry
type BackupProtection struct {
	ID         string    // unique identifier for the backup
	BackupTS   types.TS  // backup timestamp to protect
	StartTime  time.Time // when the protection started
	LastUpdate time.Time // last heartbeat update
}

// BackupProtectionManager manages backup protections
type BackupProtectionManager struct {
	mu          sync.RWMutex
	protections map[string]*BackupProtection

	// Configuration
	HeartbeatTimeout time.Duration // how long without heartbeat before protection expires
	CleanupInterval  time.Duration // how often to clean up expired protections

	// Stop channel for cleanup goroutine
	stopCh chan struct{}
	stopWg sync.WaitGroup
}

// NewBackupProtectionManager creates a new backup protection manager
func NewBackupProtectionManager() *BackupProtectionManager {
	mgr := &BackupProtectionManager{
		protections:      make(map[string]*BackupProtection),
		HeartbeatTimeout: 10 * time.Minute, // default 10 minutes timeout
		CleanupInterval:  time.Minute,      // check every minute
		stopCh:           make(chan struct{}),
	}

	// Start cleanup goroutine
	mgr.stopWg.Add(1)
	go mgr.cleanupLoop()

	return mgr
}

// AddProtection adds a new backup protection
func (mgr *BackupProtectionManager) AddProtection(id string, backupTS types.TS) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	protection := &BackupProtection{
		ID:         id,
		BackupTS:   backupTS,
		StartTime:  time.Now(),
		LastUpdate: time.Now(),
	}

	mgr.protections[id] = protection

	logutil.Info("backup-protection-added",
		zap.String("id", id),
		zap.String("backup_ts", backupTS.ToString()),
	)
}

// UpdateHeartbeat updates the heartbeat for a protection
func (mgr *BackupProtectionManager) UpdateHeartbeat(id string) bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	protection, exists := mgr.protections[id]
	if !exists {
		return false
	}

	protection.LastUpdate = time.Now()

	logutil.Debug("backup-protection-heartbeat",
		zap.String("id", id),
		zap.String("last_update", protection.LastUpdate.Format(time.RFC3339)),
	)

	return true
}

// RemoveProtection removes a backup protection
func (mgr *BackupProtectionManager) RemoveProtection(id string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if protection, exists := mgr.protections[id]; exists {
		delete(mgr.protections, id)

		logutil.Info("backup-protection-removed",
			zap.String("id", id),
			zap.String("backup_ts", protection.BackupTS.ToString()),
			zap.Duration("duration", time.Since(protection.StartTime)),
		)
	}
}

// IsProtected checks if a timestamp is protected by any active backup
func (mgr *BackupProtectionManager) IsProtected(ts types.TS) bool {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	for _, protection := range mgr.protections {
		// Check if this backup protects the given timestamp
		if ts.LE(&protection.BackupTS) {
			return true
		}
	}

	return false
}

// GetActiveProtections returns all active protections
func (mgr *BackupProtectionManager) GetActiveProtections() []*BackupProtection {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	protections := make([]*BackupProtection, 0, len(mgr.protections))
	for _, protection := range mgr.protections {
		// Create a copy to avoid race conditions
		protectionCopy := *protection
		protections = append(protections, &protectionCopy)
	}

	return protections
}

// cleanupLoop runs the cleanup process for expired protections
func (mgr *BackupProtectionManager) cleanupLoop() {
	defer mgr.stopWg.Done()

	ticker := time.NewTicker(mgr.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mgr.stopCh:
			return
		case <-ticker.C:
			mgr.cleanupExpiredProtections()
		}
	}
}

// cleanupExpiredProtections removes protections that haven't been updated within the timeout
func (mgr *BackupProtectionManager) cleanupExpiredProtections() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	now := time.Now()
	expiredIDs := make([]string, 0)

	for id, protection := range mgr.protections {
		if now.Sub(protection.LastUpdate) > mgr.HeartbeatTimeout {
			expiredIDs = append(expiredIDs, id)
		}
	}

	for _, id := range expiredIDs {
		protection := mgr.protections[id]
		delete(mgr.protections, id)

		logutil.Warn("backup-protection-expired",
			zap.String("id", id),
			zap.String("backup_ts", protection.BackupTS.ToString()),
			zap.Duration("elapsed", now.Sub(protection.LastUpdate)),
			zap.Duration("timeout", mgr.HeartbeatTimeout),
		)
	}
}

// Stop stops the backup protection manager
func (mgr *BackupProtectionManager) Stop() {
	close(mgr.stopCh)
	mgr.stopWg.Wait()
}

// Global instance (should be initialized in the service startup)
var GlobalBackupProtectionManager *BackupProtectionManager

// InitBackupProtectionManager initializes the global backup protection manager
func InitBackupProtectionManager() {
	if GlobalBackupProtectionManager != nil {
		GlobalBackupProtectionManager.Stop()
	}
	GlobalBackupProtectionManager = NewBackupProtectionManager()
}

// StopBackupProtectionManager stops the global backup protection manager
func StopBackupProtectionManager() {
	if GlobalBackupProtectionManager != nil {
		GlobalBackupProtectionManager.Stop()
		GlobalBackupProtectionManager = nil
	}
}
