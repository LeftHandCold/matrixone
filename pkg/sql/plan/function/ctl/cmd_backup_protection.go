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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleBackupProtection() handleFunc {
	return GetTNHandlerFunc(
		api.OpCode_OpBackupProtection,
		func(_ string) ([]uint64, error) {
			return nil, nil
		},
		func(dnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			// parameter is the JSON string containing backup protection request
			payload, err := types.Encode(&api.BackupProtectionRequest{
				RequestData: parameter,
			})
			return payload, err
		},
		func(data []byte) (any, error) {
			resp := &api.TNStringResponse{}
			types.Decode(data, resp)
			return resp.ReturnStr, nil
		})
}
