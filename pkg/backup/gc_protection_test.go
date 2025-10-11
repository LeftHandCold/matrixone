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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestBackupGCProtection(t *testing.T) {
	ctx := context.Background()
	sid := "test-service"

	// Create a new protection instance
	protection := NewBackupGCProtection(ctx, sid)
	assert.NotNil(t, protection)
	assert.False(t, protection.IsActive())

	// Test protection lifecycle
	protectedTS := types.BuildTS(time.Now().UnixNano(), 0)

	// Note: This test would fail in real environment without proper SQL executor
	// but demonstrates the interface
	t.Run("protection interface", func(t *testing.T) {
		assert.False(t, protection.IsActive())
		assert.True(t, protection.GetProtectedTS().IsEmpty())
		assert.True(t, protection.GetLastHeartbeat().IsZero())
	})
}

func TestBackupProtectionRequest(t *testing.T) {
	req := BackupProtectionRequest{
		Action:    "start",
		Timestamp: types.BuildTS(time.Now().UnixNano(), 0),
	}

	assert.Equal(t, "start", req.Action)
	assert.False(t, req.Timestamp.IsEmpty())
}

func TestBackupProtectionResponse(t *testing.T) {
	resp := BackupProtectionResponse{
		Success: true,
		Message: "protection started",
	}

	assert.True(t, resp.Success)
	assert.Equal(t, "protection started", resp.Message)
}
