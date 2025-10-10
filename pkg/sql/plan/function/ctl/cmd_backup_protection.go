// Copyright 2023 Matrix Origin
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

package ctl

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// BackupProtectionRequest represents a backup protection request
type BackupProtectionRequest struct {
	Action   string `json:"action"`    // "add", "remove", "heartbeat", "list"
	BackupID string `json:"backup_id"` // unique backup identifier
	BackupTS string `json:"backup_ts"` // backup timestamp
}

// BackupProtectionResponse represents a backup protection response
type BackupProtectionResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	Protections string `json:"protections,omitempty"` // JSON string of protections
}

func handleBackupProtection() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpBackupProtection,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			var req BackupProtectionRequest
			if parameter != "" {
				if err := json.Unmarshal([]byte(parameter), &req); err != nil {
					return nil, err
				}
			}

			payload, err := types.Encode(&cmd_util.BackupProtectionCmd{
				Action:   req.Action,
				BackupID: req.BackupID,
				BackupTS: req.BackupTS,
			})
			return payload, err
		},
		func(data []byte) (any, error) {
			var resp BackupProtectionResponse
			if err := types.Decode(data, &resp); err != nil {
				return nil, err
			}
			return resp, nil
		})
}
