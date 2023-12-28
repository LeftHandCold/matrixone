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

package gc2

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type checkpointGCEntry struct {
	checkpoint *checkpoint.CheckpointEntry
}

func NewCheckpointGCEntry(checkpoint *checkpoint.CheckpointEntry) GCEntry {
	return &checkpointGCEntry{checkpoint: checkpoint}
}

func (e *checkpointGCEntry) GetStartTS() types.TS {
	return e.checkpoint.GetStart()
}

func (e *checkpointGCEntry) GetEndTS() types.TS {
	return e.checkpoint.GetEnd()
}

func (e *checkpointGCEntry) String() string {
	return e.String()
}

func (e *checkpointGCEntry) compareGC(entry GCEntry) bool {
	return false
}

type checkpointGCEntryFactory struct {
	ckpClient checkpoint.RunnerReader
}

type checkpointGCTableFactory struct {
}

func (f *checkpointGCTableFactory) NewGCTable() GCTable {
	return NewCheckpointGCTable()
}

func NewCheckpointGCEntryFactory(ckpClient checkpoint.RunnerReader) GCEntryFactory {
	return &checkpointGCEntryFactory{ckpClient: ckpClient}
}

func NewCheckpointGCTable() GCTable {
	table := checkpointGCTable{
		objects: make(map[string]*ObjectEntry),
	}
	return &table
}

func (e *checkpointGCEntry) collectData(
	ctx context.Context,
	fs fileservice.FileService,
) ([]*batch.Batch, error) {
	_, data, err := logtail.LoadCheckpointEntriesFromKey(ctx, fs,
		e.checkpoint.GetLocation(), e.checkpoint.GetVersion(), nil)
	if err != nil {
		return nil, err
	}
	dataBatch := make([]*batch.Batch, 3)
	ins, _, del, delTxn := data.GetBlkBatchs()
	dataBatch[0] = containers.ToCNBatch(ins)
	dataBatch[1] = containers.ToCNBatch(del)
	dataBatch[2] = containers.ToCNBatch(delTxn)
	return dataBatch, nil
	return nil, nil
}

func (e *checkpointGCEntryFactory) GetEntries(entry GCEntry) []GCEntry {
	checkpoints := e.ckpClient.ICKPSeekLT(entry.(*checkpointGCEntry).GetEndTS(), 10)

	if len(checkpoints) == 0 {
		return nil
	}
	candidates := make([]GCEntry, 0)
	for _, ckp := range checkpoints {
		candidates = append(candidates, NewCheckpointGCEntry(ckp))
	}
	return candidates
}

func (e *checkpointGCEntryFactory) GetCompareEntry() (GCEntry, error) {
	maxGlobalCKP := e.ckpClient.MaxGlobalCheckpoint()
	if maxGlobalCKP == nil {
		return nil, nil
	}
	return &checkpointGCEntry{checkpoint: maxGlobalCKP}, nil
}
