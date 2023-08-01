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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const (
	SEQNUM_UPPER    = math.MaxUint16 - 5 // reserved 5 column for special committs、committs etc.
	SEQNUM_ROWID    = math.MaxUint16
	SEQNUM_ABORT    = math.MaxUint16 - 1
	SEQNUM_COMMITTS = math.MaxUint16 - 2
)

type WriteType int8

const (
	WriteTS WriteType = iota
)

const ZoneMapSize = index.ZMSize

type ZoneMap = index.ZM
type StaticFilter = index.StaticFilter

var NewZM = index.NewZM
var BuildZM = index.BuildZM

type ColumnMetaFetcher interface {
	MustGetColumn(seqnum uint16) ColumnMeta
}

type WriteOptions struct {
	Type WriteType
	Val  any
}

type ReadBlockOptions struct {
	Id    uint16
	Idxes map[uint16]bool
}

// Writer is to virtualize batches into multiple blocks
// and write them into filefservice at one time
type Writer interface {
	// Write writes one batch to the Buffer at a time,
	// one batch corresponds to a virtual block,
	// and returns the handle of the block.
	Write(batch *batch.Batch) (BlockObject, error)

	// Write metadata for every column of all blocks
	WriteObjectMeta(ctx context.Context, totalRow uint32, metas []ColumnMeta)

	// WriteEnd is to write multiple batches written to
	// the buffer to the fileservice at one time
	WriteEnd(ctx context.Context, items ...WriteOptions) ([]BlockObject, error)
}

// Reader is to read data from fileservice
type objectReader interface {
	Init(location Location, fs fileservice.FileService)
	Reset()
	// ReadAllMeta is read the meta of all blocks in an object
	ReadAllMeta(ctx context.Context, m *mpool.MPool) (ObjectMeta, error)

	GetObject() *Object
	GetMetaExtent() *Extent
	GetObjectName() *ObjectName
	GetName() string
	CacheMetaExtent(ext *Extent)
	ReadZM(
		ctx context.Context,
		blk uint16,
		seqnums []uint16,
		m *mpool.MPool,
	) (zms []ZoneMap, err error)

	ReadMeta(
		ctx context.Context,
		m *mpool.MPool,
	) (meta ObjectMeta, err error)

	ReadOneBlock(
		ctx context.Context,
		idxs []uint16,
		typs []types.Type,
		blk uint16,
		m *mpool.MPool,
	) (ioVec *fileservice.IOVector, err error)

	ReadSubBlock(
		ctx context.Context,
		idxs []uint16,
		typs []types.Type,
		blk uint16,
		m *mpool.MPool,
	) (ioVecs []*fileservice.IOVector, err error)

	ReadAll(
		ctx context.Context,
		idxs []uint16,
		m *mpool.MPool,
	) (ioVec *fileservice.IOVector, err error)

	ReadOneBF(
		ctx context.Context,
		blk uint16,
	) (bf StaticFilter, size uint32, err error)

	ReadAllBF(
		ctx context.Context,
	) (bfs BloomFilter, size uint32, err error)

	ReadExtent(
		ctx context.Context,
		extent Extent,
	) ([]byte, error)

	ReadMultiBlocks(
		ctx context.Context,
		opts map[uint16]*ReadBlockOptions,
		m *mpool.MPool,
	) (ioVec *fileservice.IOVector, err error)

	ReadMultiSubBlocks(
		ctx context.Context,
		opts map[uint16]*ReadBlockOptions,
		m *mpool.MPool,
	) (ioVec *fileservice.IOVector, err error)
}
