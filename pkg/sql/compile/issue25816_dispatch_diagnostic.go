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

package compile

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const issue25816RemoteDispatchDiagnosticMarker = "issue25816-remote-dispatch"

var nextIssue25816RemoteDispatchEventSequence atomic.Uint64

type remoteDispatchReceiveTrace struct {
	batchCount int
	execCount  int
}

func (trace remoteDispatchReceiveTrace) terminationEvent(end bool, bat *batch.Batch, err error) string {
	if trace.execCount == 0 {
		switch {
		case err != nil:
			return "receiver-error-before-first-exec"
		case end:
			return "receiver-end-before-first-exec"
		case bat == nil:
			return "receiver-nil-batch-before-first-exec"
		}
	}
	if err != nil {
		return "receiver-error-after-exec"
	}
	if end {
		return "receiver-end-after-exec"
	}
	return "receiver-nil-batch-after-exec"
}

func logIssue25816RemoteDispatchDiagnostic(
	proc *process.Process,
	event string,
	detailFormat string,
	detailArgs ...any,
) {
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

	format := issue25816RemoteDispatchDiagnosticMarker +
		" event_seq=%d event=%s service=%s stmt_id=%s proc=%p context_err=%v"
	args := []any{
		nextIssue25816RemoteDispatchEventSequence.Add(1),
		event,
		service,
		stmtID,
		proc,
		contextErr,
	}
	if detailFormat != "" {
		format += " " + detailFormat
		args = append(args, detailArgs...)
	}
	logutil.Infof(format, args...)
}
