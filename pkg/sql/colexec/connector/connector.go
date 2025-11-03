// Copyright 2021 Matrix Origin
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

package connector

import (
	"bytes"
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "connector"

func (connector *Connector) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": pipe connector")
}

func (connector *Connector) Prepare(proc *process.Process) error {
	if connector.ctr.sp == nil {
		connector.ctr.sp = pSpool.InitMyPipelineSpool(proc.Mp(), 1)
	}

	if connector.OpAnalyzer == nil {
		connector.OpAnalyzer = process.NewAnalyzer(connector.GetIdx(), connector.IsFirst, connector.IsLast, "connector")
	} else {
		connector.OpAnalyzer.Reset()
	}
	return nil
}

func (connector *Connector) Call(proc *process.Process) (vm.CallResult, error) {
	result, err := vm.ChildrenCall(connector.GetChildren(0), proc, connector.OpAnalyzer)
	if err != nil {
		return result, err
	}

	// [TEST CODE] Simulate CN cancel: randomly return CancelResult to simulate context cancellation
	// This simulates the scenario where a remote CN gets canceled during execution
	// ChildrenCall gets data from child operators (like TableScan, Aggregate, etc.)
	// If we return CancelResult here, it simulates the CN being canceled after processing some data
	// Only simulate for remote CNs (those that have Reg.Ch2 for sending data)
	if connector.Reg != nil && connector.Reg.Ch2 != nil {
		// 30% probability to simulate cancel
		if rand.Float32() < 0.30 && result.Batch != nil && !result.Batch.IsEmpty() {
			logutil.Warnf("[TEST CODE] Simulating CN cancel on remote CN: returning CancelResult (simulating context.Done())")
			// Return CancelResult to simulate context cancellation, stopping data transmission
			return vm.CancelResult, nil
		}
	}

	// pipeline ends normally.
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}
	// batch with no data, no need to send.
	if result.Batch.IsEmpty() {
		result.Batch = batch.EmptyBatch
		return result, nil
	}

	var queryDone bool
	queryDone, err = connector.ctr.sp.SendBatch(proc.Ctx, 0, result.Batch, nil)
	if queryDone || err != nil {
		return result, err
	}

	connector.Reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(connector.ctr.sp, 0)
	return result, nil
}
