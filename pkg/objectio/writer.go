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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/pierrec/lz4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectWriter struct {
	sync.RWMutex
	object *Object
	blocks []BlockObject
	buffer *ObjectBuffer
	name   string
	lastId uint32
}

func NewObjectWriter(name string, fs fileservice.FileService) (Writer, error) {
	object := NewObject(name, fs)
	writer := &ObjectWriter{
		name:   name,
		object: object,
		buffer: NewObjectBuffer(name),
		blocks: make([]BlockObject, 0),
		lastId: 0,
	}
	err := writer.WriteHeader()
	return writer, err
}

func (w *ObjectWriter) WriteHeader() error {
	var (
		err    error
		header bytes.Buffer
	)
	h := Header{magic: Magic, version: Version}
	header.Write(types.EncodeFixed[uint64](h.magic))
	header.Write(types.EncodeFixed[uint16](h.version))
	header.Write(make([]byte, 22))
	_, _, err = w.buffer.Write(header.Bytes())
	return err
}

func (w *ObjectWriter) Write(batch *batch.Batch) (BlockObject, error) {
	block := NewBlock(uint16(len(batch.Vecs)), w.object, w.name)
	w.AddBlock(block.(*Block))
	for i, vec := range batch.Vecs {
		buf, err := vec.MarshalBinary()
		if err != nil {
			return nil, err
		}
		originSize := len(buf)
		// TODO:Now by default, lz4 compression must be used for Write,
		// and parameters need to be passed in later to determine the compression type
		data := make([]byte, lz4.CompressBlockBound(originSize))
		if buf, err = compress.Compress(buf, data, compress.Lz4); err != nil {
			return nil, err
		}
		offset, length, err := w.buffer.Write(buf)
		if err != nil {
			return nil, err
		}
		block.(*Block).columns[i].(*ColumnBlock).meta.location = Extent{
			id:         uint32(block.GetMeta().header.blockId),
			offset:     uint32(offset),
			length:     uint32(length),
			originSize: uint32(originSize),
		}
		block.(*Block).columns[i].(*ColumnBlock).meta.alg = compress.Lz4
		block.(*Block).columns[i].(*ColumnBlock).meta.typ = uint8(vec.GetType().Oid)
	}
	return block, nil
}

func (w *ObjectWriter) WriteIndex(fd BlockObject, index IndexData) error {
	var err error

	block := w.GetBlock(fd.GetID())
	if block == nil || block.columns[index.GetIdx()] == nil {
		return moerr.NewInternalErrorNoCtx("object io: not found")
	}
	err = index.Write(w, block)
	return err
}

func (w *ObjectWriter) WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error) {
	var err error
	w.RLock()
	defer w.RUnlock()
	if len(w.blocks) == 0 {
		logutil.Warn("object io: no block needs to be written")
	}
	var buf bytes.Buffer
	metaLen := 0
	start := 0
	for _, block := range w.blocks {
		meta, err := block.(*Block).MarshalMeta()
		if err != nil {
			return nil, err
		}
		offset, length, err := w.buffer.Write(meta)
		if err != nil {
			return nil, err
		}
		if start == 0 {
			start = offset
		}
		metaLen += length
		buf.Write(types.EncodeFixed[uint32](uint32(offset)))
		buf.Write(types.EncodeFixed[uint32](uint32(length)))
		buf.Write(types.EncodeFixed[uint32](uint32(length)))
	}
	buf.Write(types.EncodeFixed[uint32](uint32(len(w.blocks))))
	buf.Write(types.EncodeFixed[uint64](uint64(Magic)))
	_, _, err = w.buffer.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}
	err = w.Sync(ctx, items...)
	if err != nil {
		return nil, err
	}
	for i := range w.blocks {
		w.blocks[i].(*Block).extent = Extent{
			id:         uint32(i),
			offset:     uint32(start),
			length:     uint32(metaLen),
			originSize: uint32(metaLen),
		}
	}

	// The buffer needs to be released at the end of WriteEnd
	// Because the outside may hold this writer
	// After WriteEnd is called, no more data can be written
	w.buffer = nil
	return w.blocks, err
}

// Sync is for testing
func (w *ObjectWriter) Sync(ctx context.Context, items ...WriteOptions) error {
	w.buffer.SetDataOptions(items...)
	err := w.object.fs.Write(ctx, w.buffer.GetData())
	if err != nil {
		return err
	}
	return err
}

func (w *ObjectWriter) AddBlock(block *Block) {
	w.Lock()
	defer w.Unlock()
	block.id = w.lastId
	w.blocks = append(w.blocks, block)
	//w.blocks[block.id] = block
	w.lastId++
}

func (w *ObjectWriter) GetBlock(id uint32) *Block {
	w.Lock()
	defer w.Unlock()
	return w.blocks[id].(*Block)
}
