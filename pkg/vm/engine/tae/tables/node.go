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

package tables

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"sync/atomic"
)

type appendableNode struct {
	file      file.Block
	block     *dataBlock
	data      *containers.Batch
	rows      uint32
	exception *atomic.Value
}

func newNode(mgr base.INodeManager, block *dataBlock, file file.Block) *appendableNode {
	impl := new(appendableNode)
	impl.exception = new(atomic.Value)
	impl.block = block
	impl.file = file
	impl.rows = file.ReadRows()
	return impl
}

func (node *appendableNode) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if coarse {
		node.block.mvcc.RLock()
		defer node.block.mvcc.RUnlock()
		return node.rows
	}
	// TODO: fine row count
	// 1. Load txn ts zonemap
	// 2. Calculate fine row count
	return 0
}

func (node *appendableNode) CheckUnloadable() bool {
	return !node.block.mvcc.HasActiveAppendNode()
}

func (node *appendableNode) GetDataCopy(maxRow uint32) (columns *containers.Batch, err error) {
	if exception := node.exception.Load(); exception != nil {
		err = exception.(error)
		return
	}
	node.block.RLock()
	columns = node.data.CloneWindow(0, int(maxRow), containers.DefaultAllocator)
	node.block.RUnlock()
	return
}

func (node *appendableNode) GetColumnDataCopy(
	maxRow uint32,
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	if exception := node.exception.Load(); exception != nil {
		err = exception.(error)
		return
	}
	node.block.RLock()
	if node.data == nil {
		node.OnLoad()
	}
	if buffer != nil {
		win := node.data.Vecs[colIdx]
		if maxRow < uint32(node.data.Vecs[colIdx].Length()) {
			win = win.Window(0, int(maxRow))
		}
		vec = containers.CloneWithBuffer(win, buffer, containers.DefaultAllocator)
	} else {
		vec = node.data.Vecs[colIdx].CloneWindow(0, int(maxRow), containers.DefaultAllocator)
	}
	node.block.RUnlock()
	return
}

func (node *appendableNode) OnLoad() {
	if exception := node.exception.Load(); exception != nil {
		logutil.Error("[Exception]", common.ExceptionField(exception))
		return
	}
	var err error
	schema := node.block.meta.GetSchema()
	opts := new(containers.Options)
	opts.Capacity = int(schema.BlockMaxRows)
	opts.Allocator = ImmutMemAllocator
	if node.data, err = node.file.LoadBatch(
		schema.AllTypes(),
		schema.AllNames(),
		schema.AllNullables(),
		opts); err != nil {
		node.exception.Store(err)
	}
	if node.data.Length() != int(node.rows) {
		logutil.Fatalf("Load %d rows but %d expected: %s", node.data.Length(), node.rows, node.block.meta.String())
	}
}

func (node *appendableNode) OnUnload() {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		return
	}
	node.data.Close()
	node.data = nil
}

func (node *appendableNode) Close() (err error) {
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	return
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		err = exception.(error)
		return
	}
	left := node.block.meta.GetSchema().BlockMaxRows - node.rows
	if left == 0 {
		err = data.ErrNotAppendable
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	return
}

func (node *appendableNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, node.block.prefix, startRow, length)
	if err != nil {
		return
	}
	defer col.Close()
	vec := node.data.Vecs[node.block.meta.GetSchema().PhyAddrKey.Idx]
	node.block.Lock()
	vec.Extend(col)
	node.block.Unlock()
	return
}

func (node *appendableNode) ApplyAppend(bat *containers.Batch, txn txnif.AsyncTxn) (from int, err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		err = exception.(error)
		return
	}
	schema := node.block.meta.GetSchema()
	if node.data == nil {
		node.OnLoad()
	}
	from = int(node.rows)
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		if def.IsPhyAddr() {
			continue
		}
		destVec := node.data.Vecs[def.Idx]
		node.block.Lock()
		destVec.Extend(bat.Vecs[srcPos])
		node.block.Unlock()
	}
	if err = node.FillPhyAddrColumn(uint32(from), uint32(bat.Length())); err != nil {
		return
	}
	node.rows += uint32(bat.Length())
	return
}
