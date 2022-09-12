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
	"bytes"
	"encoding/binary"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Block is the organizational structure of a batch in objectio
// Write one batch at a time, and batch and block correspond one-to-one
type Block struct {
	// fd is the handle of the block
	fd     int
	header *BlockHeader
	// columns is the vector in the batch
	columns []ColumnObject
	// data is the batch to be written
	data   *batch.Batch
	object *Object
	extent Extent
}

func NewBlock(batch *batch.Batch, object *Object) BlockObject {
	header := &BlockHeader{
		columnCount: uint16(len(batch.Vecs)),
	}
	block := &Block{
		header:  header,
		data:    batch,
		object:  object,
		columns: make([]ColumnObject, len(batch.Vecs)),
	}
	for i := range block.columns {
		block.columns[i] = NewColumnBlock(uint16(i), block)
	}
	return block
}

func (b *Block) GetExtent() Extent {
	return b.extent
}

func (b *Block) GetColumn(idx uint16) (ColumnObject, error) {
	return b.columns[idx], nil
}

func (b *Block) GetRows() (uint32, error) {
	panic(any("implement me"))
}

func (b *Block) GetMeta() *BlockMeta {
	return &BlockMeta{
		header: b.header,
	}
}

func (b *Block) MarshalMeta() ([]byte, error) {
	var (
		err    error
		buffer bytes.Buffer
	)
	// write header
	if err = binary.Write(&buffer, binary.BigEndian, b.header.tableId); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, b.header.segmentId); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, b.header.blockId); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, b.header.columnCount); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, binary.BigEndian, uint32(0)); err != nil {
		return nil, err
	}
	reserved := make([]byte, 34)
	if err = binary.Write(&buffer, binary.BigEndian, reserved); err != nil {
		return nil, err
	}
	// write columns meta
	for _, column := range b.columns {
		columnMeta, err := column.(*ColumnBlock).MarshalMeta()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(&buffer, binary.BigEndian, columnMeta); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (b *Block) UnMarshalMeta(data []byte) error {
	var err error
	cache := bytes.NewBuffer(data)
	b.header = &BlockHeader{}
	if err = binary.Read(cache, binary.BigEndian, &b.header.tableId); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.segmentId); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.blockId); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.columnCount); err != nil {
		return err
	}
	if err = binary.Read(cache, binary.BigEndian, &b.header.checksum); err != nil {
		return err
	}
	reserved := make([]byte, 34)
	if err = binary.Read(cache, binary.BigEndian, &reserved); err != nil {
		return err
	}
	b.columns = make([]ColumnObject, b.header.columnCount)
	for i, _ := range b.columns {
		b.columns[i] = NewColumnBlock(uint16(i), b)
		err = b.columns[i].(*ColumnBlock).UnMarshalMate(cache)
		if err != nil {
			return err
		}
	}
	return err
}
