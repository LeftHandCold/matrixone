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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	// GC protection heartbeat interval (5 minutes)
	GCProtectionHeartbeatInterval = 5 * time.Minute
	// GC protection timeout (20 minutes)
	GCProtectionTimeout = 20 * time.Minute
)

// BackupGCProtection represents the backup GC protection mechanism
type BackupGCProtection struct {
	mu            sync.RWMutex
	protectedTS   types.TS
	lastHeartbeat time.Time
	active        bool
	stopCh        chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	sid           string
}

// BackupProtectionRequest represents a request to protect/unprotect backup timestamp
type BackupProtectionRequest struct {
	Action    string   `json:"action"`    // "start", "heartbeat", "stop"
	Timestamp types.TS `json:"timestamp"` // protected timestamp
}

// BackupProtectionResponse represents the response from GC node
type BackupProtectionResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// NewBackupGCProtection creates a new backup GC protection instance
func NewBackupGCProtection(ctx context.Context, sid string) *BackupGCProtection {
	ctx, cancel := context.WithCancel(ctx)
	return &BackupGCProtection{
		ctx:    ctx,
		cancel: cancel,
		sid:    sid,
		stopCh: make(chan struct{}),
	}
}

// StartProtection starts protecting the given timestamp from GC
func (p *BackupGCProtection) StartProtection(ctx context.Context, protectedTS types.TS) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active {
		return moerr.NewInternalError(ctx, "backup protection already active")
	}

	// Send start protection request to GC node
	if err := p.sendProtectionRequest(ctx, "start", protectedTS); err != nil {
		return err
	}

	p.protectedTS = protectedTS
	p.lastHeartbeat = time.Now()
	p.active = true

	// Start heartbeat goroutine
	go p.heartbeatLoop()

	logutil.Infof("[BackupGCProtection] Started protection for timestamp %s", protectedTS.ToString())
	return nil
}

// StopProtection stops the GC protection
func (p *BackupGCProtection) StopProtection(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.active {
		return nil // already stopped
	}

	// Send stop protection request to GC node
	if err := p.sendProtectionRequest(ctx, "stop", p.protectedTS); err != nil {
		logutil.Errorf("[BackupGCProtection] Failed to send stop protection request: %v", err)
		// Continue to stop local protection even if remote call fails
	}

	p.active = false
	close(p.stopCh)
	p.cancel()

	logutil.Infof("[BackupGCProtection] Stopped protection for timestamp %s", p.protectedTS.ToString())
	return nil
}

// heartbeatLoop sends periodic heartbeat to maintain protection
func (p *BackupGCProtection) heartbeatLoop() {
	ticker := time.NewTicker(GCProtectionHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.mu.RLock()
			if !p.active {
				p.mu.RUnlock()
				return
			}
			protectedTS := p.protectedTS
			p.mu.RUnlock()

			if err := p.sendProtectionRequest(p.ctx, "heartbeat", protectedTS); err != nil {
				logutil.Errorf("[BackupGCProtection] Failed to send heartbeat: %v", err)
				continue
			}

			p.mu.Lock()
			p.lastHeartbeat = time.Now()
			p.mu.Unlock()

			logutil.Debugf("[BackupGCProtection] Sent heartbeat for timestamp %s", protectedTS.ToString())
		}
	}
}

// sendProtectionRequest sends protection request to GC node via mo_ctl
func (p *BackupGCProtection) sendProtectionRequest(ctx context.Context, action string, protectedTS types.TS) error {
	v, ok := runtime.ServiceRuntime(p.sid).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(ctx, "no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)

	req := BackupProtectionRequest{
		Action:    action,
		Timestamp: protectedTS,
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("select mo_ctl('dn','BackupProtection','%s')", string(reqBytes))
	opts := executor.Options{}
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	defer res.Close()

	var retBytes [][][]byte
	res.ReadRows(func(_ int, cols []*vector.Vector) bool {
		retBytes = append(retBytes, executor.GetBytesRows(cols[0]))
		return true
	})

	if len(retBytes) == 0 || len(retBytes[0]) == 0 {
		return moerr.NewInternalError(ctx, "empty response from GC node")
	}

	var ctlResult ctl.Result
	if err := json.Unmarshal(retBytes[0][0], &ctlResult); err != nil {
		return err
	}

	responseStr, ok := ctlResult.Data.([]interface{})
	if !ok {
		return moerr.NewInternalError(ctx, "invalid response format from GC node")
	}
	for _, rs := range responseStr {
		str, ok := rs.(string)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid ctl string")
		}
		// Check for simple string response format
		if str == "OK" {
			// Success case
			return nil
		} else if strings.HasPrefix(str, "ERROR:") {
			// Error case
			return moerr.NewInternalError(ctx, fmt.Sprintf("GC protection request failed: %s", str))
		} else {
			// Unknown response format
			return moerr.NewInternalError(ctx, fmt.Sprintf("unexpected response from GC node: %s", str))
		}
	}

	return nil
}

// IsActive returns whether protection is currently active
func (p *BackupGCProtection) IsActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.active
}

// GetProtectedTS returns the currently protected timestamp
func (p *BackupGCProtection) GetProtectedTS() types.TS {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.protectedTS
}

// GetLastHeartbeat returns the last heartbeat time
func (p *BackupGCProtection) GetLastHeartbeat() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastHeartbeat
}
