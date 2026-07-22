// Copyright 2026 Matrix Origin
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

package dispatch

import (
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const issue25816DispatchOperatorDiagnosticMarker = "issue25816-dispatch-operator"

var nextIssue25816DispatchOperatorEventSequence atomic.Uint64

func logIssue25816DispatchOperatorDiagnostic(
	event string,
	d *Dispatch,
	proc *process.Process,
	detailFormat string,
	detailArgs ...any,
) {
	if proc == nil && d != nil && d.ctr != nil {
		proc = d.ctr.remoteProc
	}
	service := ""
	stmtID := "00000000-0000-0000-0000-000000000000"
	contextErr := error(nil)
	if proc != nil {
		if proc.Base != nil {
			service = proc.GetService()
			if profile := proc.GetStmtProfile(); profile != nil {
				stmtID = profile.GetStmtId().String()
			}
		}
		if proc.Ctx != nil {
			contextErr = proc.Ctx.Err()
		}
	}

	var ctr *container
	var remoteInfo process.RemotePipelineInformationChannel
	prepared := false
	isRemote := false
	remoteRegCount := 0
	remoteUUIDs := ""
	if d != nil {
		ctr = d.ctr
		remoteRegCount = len(d.RemoteRegs)
		uuids := make([]string, 0, remoteRegCount)
		for i := range d.RemoteRegs {
			uuids = append(uuids, d.RemoteRegs[i].Uuid.String())
		}
		remoteUUIDs = strings.Join(uuids, ",")
		if ctr != nil {
			remoteInfo = ctr.remoteInfo
			prepared = ctr.prepared
			isRemote = ctr.isRemote
		}
	}

	format := issue25816DispatchOperatorDiagnosticMarker +
		" event_seq=%d event=%s service=%s stmt_id=%s dispatch=%p ctr=%p remote_info=%p proc=%p" +
		" remote_reg_count=%d remote_uuids=%s prepared=%t is_remote=%t context_err=%v"
	args := []any{
		nextIssue25816DispatchOperatorEventSequence.Add(1),
		event,
		service,
		stmtID,
		d,
		ctr,
		remoteInfo,
		proc,
		remoteRegCount,
		remoteUUIDs,
		prepared,
		isRemote,
		contextErr,
	}
	if detailFormat != "" {
		format += " " + detailFormat
		args = append(args, detailArgs...)
	}
	logutil.Infof(format, args...)
}
