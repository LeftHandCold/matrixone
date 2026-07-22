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
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const issue25816RemoteDispatchDiagnosticMarker = "issue25816-remote-dispatch"

var nextIssue25816RemoteDispatchEventSequence atomic.Uint64

type remoteDispatchReceiveTrace struct {
	batchCount int
	execCount  int
}

type issue25816ScopeDiagnosticSnapshot struct {
	magic               string
	nodeID              string
	nodeAddr            string
	localAddr           string
	mcpu                int
	rootType            string
	preScopeCount       int
	remoteReceiverCount int
}

func newIssue25816ScopeDiagnosticSnapshot(
	s *Scope,
	localAddr string,
) issue25816ScopeDiagnosticSnapshot {
	snapshot := issue25816ScopeDiagnosticSnapshot{localAddr: localAddr, rootType: "<nil>"}
	if s == nil {
		return snapshot
	}
	snapshot.magic = magicShow(s.Magic)
	snapshot.nodeID = s.NodeInfo.Id
	snapshot.nodeAddr = s.NodeInfo.Addr
	snapshot.mcpu = s.NodeInfo.Mcpu
	snapshot.rootType = fmt.Sprintf("%T", s.RootOp)
	snapshot.preScopeCount = len(s.PreScopes)
	snapshot.remoteReceiverCount = len(s.RemoteReceivRegInfos)
	return snapshot
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

func logIssue25816ScopeDiagnostic(
	event string,
	s *Scope,
	localAddr string,
	detailFormat string,
	detailArgs ...any,
) {
	var proc *process.Process
	if s != nil {
		proc = s.Proc
	}
	snapshot := newIssue25816ScopeDiagnosticSnapshot(s, localAddr)
	format := "scope=%p magic=%s node_id=%s node_addr=%s local_addr=%s mcpu=%d" +
		" root_type=%s pre_scope_count=%d remote_receiver_count=%d"
	args := []any{
		s,
		snapshot.magic,
		snapshot.nodeID,
		snapshot.nodeAddr,
		snapshot.localAddr,
		snapshot.mcpu,
		snapshot.rootType,
		snapshot.preScopeCount,
		snapshot.remoteReceiverCount,
	}
	if detailFormat != "" {
		format += " " + detailFormat
		args = append(args, detailArgs...)
	}
	logIssue25816RemoteDispatchDiagnostic(proc, event, format, args...)
}

func logIssue25816ScopeRemoteReceivers(event string, s *Scope, localAddr string) {
	if s == nil {
		return
	}
	for i := range s.RemoteReceivRegInfos {
		info := &s.RemoteReceivRegInfos[i]
		logIssue25816ScopeDiagnostic(
			event,
			s,
			localAddr,
			"remote_receiver_ordinal=%d receiver_idx=%d receiver_uuid=%s receiver_from_addr=%s",
			i,
			info.Idx,
			info.Uuid.String(),
			info.FromAddr,
		)
	}
}

func logIssue25816DispatchRegistrationVisit(
	event string,
	s *Scope,
	localAddr string,
	d *dispatch.Dispatch,
	origin string,
	mode dispatchReceiverRegistrationMode,
) {
	logIssue25816ScopeDiagnostic(
		event,
		s,
		localAddr,
		"registration_origin=%s registration_mode=%d dispatch=%p remote_reg_count=%d",
		origin,
		mode,
		d,
		len(d.RemoteRegs),
	)
	for i := range d.RemoteRegs {
		reg := &d.RemoteRegs[i]
		logIssue25816ScopeDiagnostic(
			"dispatch-registration-remote-target",
			s,
			localAddr,
			"registration_origin=%s registration_mode=%d dispatch=%p remote_reg_ordinal=%d remote_uuid=%s remote_node_addr=%s",
			origin,
			mode,
			d,
			i,
			reg.Uuid.String(),
			reg.NodeAddr,
		)
	}
}
