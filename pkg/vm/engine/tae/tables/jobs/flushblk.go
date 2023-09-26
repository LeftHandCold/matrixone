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

package jobs

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushBlkTask struct {
	*tasks.BaseTask
	data      *containers.Batch
	delta     *containers.Batch
	meta      *catalog.BlockEntry
	fs        *objectio.ObjectFS
	name      objectio.ObjectName
	blocks    []objectio.BlockObject
	schemaVer uint32
	seqnums   []uint16
}

func NewFlushBlkTask(
	ctx *tasks.Context,
	schemaVer uint32,
	seqnums []uint16,
	fs *objectio.ObjectFS,
	meta *catalog.BlockEntry,
	data *containers.Batch,
	delta *containers.Batch,
) *flushBlkTask {
	task := &flushBlkTask{
		schemaVer: schemaVer,
		seqnums:   seqnums,
		data:      data,
		meta:      meta,
		fs:        fs,
		delta:     delta,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushBlkTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushBlkTask) Execute(ctx context.Context) error {
	seg := task.meta.ID.Segment()
	num, _ := task.meta.ID.Offsets()
	name := objectio.BuildObjectName(seg, num)
	task.name = name
	writer, err := blockio.NewBlockWriterNew(task.fs.Service, name, task.schemaVer, task.seqnums)
	if err != nil {
		return err
	}
	var pkIdx int
	if task.meta.GetSchema().HasPK() {
		pkIdx = task.meta.GetSchema().GetSingleSortKeyIdx()
		writer.SetPrimaryKey(uint16(pkIdx))
	}

	_, err = writer.WriteBatch(containers.ToCNBatch(task.data))
	if err != nil {
		return err
	}
	if task.delta != nil {
		_, err := writer.WriteTombstoneBatch(containers.ToCNBatch(task.delta))
		if err != nil {
			return err
		}
	}
	task.blocks, _, err = writer.Sync(ctx)
	if task.meta.GetSchema().HasPK() {
		metaLoc1 := blockio.EncodeLocation(name, task.blocks[0].GetExtent(), uint32(task.data.Length()), task.blocks[0].GetID())
		var reader *blockio.BlockReader
		reader, err = blockio.NewObjectReader(task.fs.Service, metaLoc1)
		if err != nil {
			return err
		}
		var bf objectio.BloomFilter
		bf, _, err = reader.LoadAllBF(ctx)
		if err != nil {
			return err
		}
		buf := bf.GetBloomFilter(uint32(task.blocks[0].GetID()))
		bfIndex := index.NewEmptyBinaryFuseFilter()
		if err = index.DecodeBloomFilter(bfIndex, buf); err != nil {
			return err
		}
		val := task.data.Vecs[pkIdx]
		for j := 0; j < val.Length(); j++ {
			key := val.Get(j)
			v := types.EncodeValue(key, val.GetType().Oid)
			var exist bool
			exist, err = bfIndex.MayContainsKey(v)
			if err != nil {
				panic(err)
			}
			if !exist {
				logutil.Infof("keyyy %v not exist in bloom filter, table is %v, block is %v, meta is %v, pk is %d", key, task.meta.GetSchema().Name, 0, metaLoc1.String(), pkIdx)
				logutil.Infof("bfIndex is %v, buf is %v", bfIndex.String(), buf)
			}
		}
	}
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.Flush.Add(1)
	})
	return err
}
