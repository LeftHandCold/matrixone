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

package blockio

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"io"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type Reader struct {
	reader  objectio.Reader
	key     string
	meta    *Meta
	name    string
	locs    []objectio.Extent
	readCxt context.Context
}

type ReadAttr struct {
	colTypes  []types.Type
	colNames  []string
	nullables []bool
	idxs      []uint16
	block     objectio.BlockObject
}

func NewReader(cxt context.Context, fs *objectio.ObjectFS, key string) (*Reader, error) {
	meta, err := DecodeMetaLocToMeta(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(meta.GetKey(), fs.Service)
	if err != nil {
		return nil, err
	}
	return &Reader{
		reader:  reader,
		key:     key,
		meta:    meta,
		readCxt: cxt,
	}, nil
}

func NewBlockReader(cxt context.Context, service fileservice.FileService, key string) (*Reader, error) {
	meta, err := DecodeMetaLocToMeta(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(meta.GetKey(), service)
	if err != nil {
		return nil, err
	}
	return &Reader{
		reader:  reader,
		key:     key,
		meta:    meta,
		readCxt: cxt,
	}, nil
}

func NewCheckpointReader(cxt context.Context, fs fileservice.FileService, key string) (*Reader, error) {
	name, locs, err := DecodeMetaLocToMetas(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, fs)
	if err != nil {
		return nil, err
	}
	return &Reader{
		reader:  reader,
		key:     key,
		name:    name,
		locs:    locs,
		readCxt: cxt,
	}, nil
}

func (r *Reader) BlkColumnByMetaLoadJob(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject,
) *tasks.Job {
	exec := func(_ context.Context) (result *tasks.JobResult) {
		bat, err := r.LoadBlkColumnsByMeta(colTypes, colNames, nullables, block)
		return &tasks.JobResult{
			Err: err,
			Res: bat,
		}
	}
	return tasks.NewJob(uuid.NewString(), r.readCxt, exec)
}

func (r *Reader) BlkColumnsByMetaAndIdxLoadJob(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject,
	idx int,
) *tasks.Job {
	exec := func(_ context.Context) (result *tasks.JobResult) {
		bat, err := r.LoadBlkColumnsByMetaAndIdx(
			colTypes,
			colNames,
			nullables,
			block,
			idx)
		return &tasks.JobResult{
			Err: err,
			Res: bat,
		}
	}
	return tasks.NewJob(uuid.NewString(), r.readCxt, exec)
}

func (r *Reader) LoadBlkColumnsByMeta(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject) (*containers.Batch, error) {
	bat := containers.NewBatch()
	if block.GetExtent().End() == 0 {
		return bat, nil
	}
	idxs := make([]uint16, len(colNames))
	for i := range colNames {
		idxs[i] = uint16(i)
	}
	ioResult, err := r.reader.Read(r.readCxt, block.GetExtent(), idxs, nil)
	if err != nil {
		return nil, err
	}

	for i := range colNames {
		pkgVec := vector.New(colTypes[i])
		data := make([]byte, len(ioResult.Entries[i].Object.([]byte)))
		copy(data, ioResult.Entries[i].Object.([]byte))
		if err = pkgVec.Read(data); err != nil && !errors.Is(err, io.EOF) {
			return bat, err
		}
		var vec containers.Vector
		if pkgVec.Length() == 0 {
			vec = containers.MakeVector(colTypes[i], nullables[i])
		} else {
			vec = containers.NewVectorWithSharedMemory(pkgVec, nullables[i])
		}
		bat.AddVector(colNames[i], vec)
		bat.Vecs[i] = vec

	}
	return bat, nil
}

func (r *Reader) LoadBlkColumnsByMetaAndIdx(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject,
	idx int) (*containers.Batch, error) {
	bat := containers.NewBatch()

	if block.GetExtent().End() == 0 {
		return nil, nil
	}
	col, err := block.GetColumn(uint16(idx))
	if err != nil {
		return bat, err
	}
	data, err := col.GetData(r.readCxt, nil)
	if err != nil {
		return bat, err
	}
	pkgVec := vector.New(colTypes[0])
	v := make([]byte, len(data.Entries[0].Object.([]byte)))
	copy(v, data.Entries[0].Object.([]byte))
	if err = pkgVec.Read(v); err != nil && !errors.Is(err, io.EOF) {
		return bat, err
	}
	var vec containers.Vector
	if pkgVec.Length() == 0 {
		vec = containers.MakeVector(colTypes[0], nullables[0])
	} else {
		vec = containers.NewVectorWithSharedMemory(pkgVec, nullables[0])
	}
	bat.AddVector(colNames[0], vec)
	return bat, nil
}

func (r *Reader) LoadZoneMapAndRowsByMetaLoc(
	ctx context.Context,
	idxs []uint16,
	blockInfo catalog.BlockInfo,
	m *mpool.MPool) ([][2]any, uint32, error) {
	_, extent, rows := DecodeMetaLoc(blockInfo.MetaLoc)
	zonemapList := make([][2]any, len(idxs))

	obs, err := r.reader.ReadMeta(ctx, []objectio.Extent{extent}, m)
	if err != nil {
		return nil, 0, err
	}

	for i, idx := range idxs {
		column, err := obs[0].GetColumn(idx)
		if err != nil {
			return nil, 0, err
		}
		data, err := column.GetIndex(ctx, objectio.ZoneMapType, m)
		if err != nil {
			return nil, 0, err
		}
		bytes := data.(*objectio.ZoneMap).GetData()
		meta := column.GetMeta()
		t := types.Type{
			Oid: types.T(meta.GetType()),
		}
		zm := index.NewZoneMap(t)
		err = zm.Unmarshal(bytes[:])
		if err != nil {
			return nil, rows, err
		}

		min := zm.GetMin()
		max := zm.GetMax()
		if min == nil || max == nil {
			return nil, rows, nil
		}
		zonemapList[i] = [2]any{min, max}
	}

	return zonemapList, rows, nil
}

func (r *Reader) LoadBlkColumns(
	ctx context.Context,
	attr ReadAttr,
	m *mpool.MPool,
) (*batch.Batch, error) {
	if attr.block == nil {
		block, err := r.ReadMeta(m)
		if err != nil {
			return nil, err
		}
		attr.block = block
	}
	bat := batch.NewWithSize(len(attr.idxs))
	iov, err := r.reader.Read(ctx, attr.block.GetExtent(), attr.idxs, m)
	if err != nil {
		return nil, err
	}
	for i, entry := range iov.Entries {
		bat.Vecs[i] = vector.New(catalog.MetaColTypes[attr.idxs[i]])
		if err = bat.Vecs[i].Read(entry.Object.([]byte)); err != nil {
			return nil, err
		}
	}
	return bat, nil
}

func (r *Reader) LoadAllBlkColumns(
	ctx context.Context,
	attr ReadAttr,
	size int64,
	m *mpool.MPool,
) ([]*batch.Batch, error) {
	blocks, err := r.reader.ReadAllMeta(r.readCxt, size, m)
	if err != nil {
		return nil, err
	}
	bats := make([]*batch.Batch, 0)
	for _, block := range blocks {
		attr.block = block
		batch, err := r.LoadBlkColumns(ctx, attr, m)
		if err != nil {
			return nil, err
		}
		bats = append(bats, batch)
	}
	return bats, nil
}

func (r *Reader) ReadMeta(m *mpool.MPool) (objectio.BlockObject, error) {
	extents := make([]objectio.Extent, 1)
	extents[0] = r.meta.GetLoc()
	block, err := r.reader.ReadMeta(r.readCxt, extents, m)
	if err != nil {
		return nil, err
	}
	return block[0], err
}

func (r *Reader) ReadMetas(m *mpool.MPool) ([]objectio.BlockObject, error) {
	block, err := r.reader.ReadMeta(r.readCxt, r.locs, m)
	return block, err
}

func (r *Reader) GetDataObject(idx uint16, m *mpool.MPool) objectio.ColumnObject {
	block, err := r.ReadMeta(m)
	if err != nil {
		panic(any(err))
	}
	if block == nil {
		return nil
	}
	object, err := block.GetColumn(idx)
	if err != nil {
		panic(any(err))
	}
	return object
}
