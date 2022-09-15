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

package objectio

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/adaptors"
)

type blockFile struct {
	common.RefHelper
	seg     *segmentFile
	rows    uint32
	id      *common.ID
	ts      uint64
	columns []*columnBlock
	writer  *Writer
	reader  *Reader
}

func newBlock(id uint64, seg *segmentFile, colCnt int, indexCnt map[int]int) *blockFile {
	blockID := &common.ID{
		SegmentID: seg.id.SegmentID,
		BlockID:   id,
	}
	bf := &blockFile{
		seg:     seg,
		id:      blockID,
		columns: make([]*columnBlock, colCnt),
	}
	bf.reader = NewReader(seg.fs, bf)
	bf.writer = NewWriter(seg.fs)
	bf.OnZeroCB = bf.close
	for i := range bf.columns {
		cnt := 0
		if indexCnt != nil {
			cnt = indexCnt[i]
		}
		bf.columns[i] = newColumnBlock(bf, cnt, i)
	}
	bf.Ref()
	return bf
}

func (bf *blockFile) Fingerprint() *common.ID {
	return bf.id
}

func (bf *blockFile) close() {
	bf.Close()
	err := bf.Destroy()
	if err != nil {
		panic(any("Destroy error"))
	}
}

func (bf *blockFile) WriteRows(rows uint32) (err error) {
	bf.rows = rows
	return nil
}

func (bf *blockFile) ReadRows() uint32 {
	return bf.rows
}

func (bf *blockFile) WriteTS(ts types.TS) (err error) {
	return
}

func (bf *blockFile) ReadTS() (ts types.TS, err error) {
	return
}

func (bf *blockFile) WriteDeletes(buf []byte) (err error) {
	return bf.writer.WriteDeletes(bf.ts, bf.id, buf)
}

func (bf *blockFile) ReadDeletes(buf []byte) (err error) {
	return
}

func (bf *blockFile) GetDeletesFileStat() (stat common.FileInfo) {
	return nil
}

func (bf *blockFile) WriteIndexMeta(buf []byte) (err error) {
	return bf.writer.WriteIndexMeta(bf.id, buf)
}

func (bf *blockFile) LoadIndexMeta() (any, error) {
	return bf.reader.LoadIndexMeta(bf.id)
}

func (bf *blockFile) OpenColumn(colIdx int) (colBlk file.ColumnBlock, err error) {
	if colIdx >= len(bf.columns) {
		err = file.ErrInvalidParam
		return
	}
	bf.columns[colIdx].Ref()
	colBlk = bf.columns[colIdx]
	return
}

func (bf *blockFile) Close() error {
	return nil
}

func (bf *blockFile) Destroy() error {
	return nil
}

func (bf *blockFile) Sync() error { return nil }

func (bf *blockFile) LoadBatch(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	return bf.reader.LoadABlkColumns(colTypes, colNames, nullables, opts)
}

func (bf *blockFile) WriteColumnVec(_ types.TS, colIdx int, vec containers.Vector) (err error) {
	cb, err := bf.OpenColumn(colIdx)
	if err != nil {
		return err
	}
	defer cb.Close()
	w := adaptors.NewBuffer(nil)
	defer w.Close()
	if _, err = vec.WriteTo(w); err != nil {
		return
	}
	err = cb.WriteData(w.Bytes())
	return
}

func (bf *blockFile) WriteBatch(bat *containers.Batch, ts types.TS) (err error) {
	if err = bf.WriteRows(uint32(bat.Length())); err != nil {
		return
	}
	for colIdx := range bat.Attrs {
		if err = bf.WriteColumnVec(ts, colIdx, bat.Vecs[colIdx]); err != nil {
			return
		}
	}
	return
}

func (bf *blockFile) LoadDeletes() (mask *roaring.Bitmap, err error) {
	return bf.reader.LoadDeletes(bf.id)
}

func (bf *blockFile) LoadUpdates() (masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]any) {
	return bf.reader.LoadUpdates()
}
