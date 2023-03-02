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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type BlockReader struct {
	reader objectio.Reader
	key    string
	name   string
	meta   objectio.Extent
}

func NewBlockReader(service fileservice.FileService, key string) (*BlockReader, error) {
	name, _, meta, _, err := DecodeLocation(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		name:   name,
		meta:   meta,
	}, nil
}

func NewFileReader(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		name:   name,
	}, nil
}

func NewCheckPointReader(service fileservice.FileService, key string) (*BlockReader, error) {
	name, locs, err := DecodeMetaLocToMetas(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		key:    key,
		reader: reader,
		name:   name,
		meta:   locs[0],
	}, nil
}

func (r *BlockReader) LoadColumns(ctx context.Context, idxs []uint16,
	ids []uint32, m *mpool.MPool) ([]*batch.Batch, error) {
	bats := make([]*batch.Batch, 0)
	if r.meta.End() == 0 {
		return bats, nil
	}
	ioVectors, err := r.reader.Read(ctx, r.meta, idxs, ids, nil, LoadZoneMapFunc, LoadColumnFunc)
	if err != nil {
		return nil, err
	}
	for y, _ := range ids {
		bat := batch.NewWithSize(len(idxs))
		for i := range idxs {
			bat.Vecs[i] = ioVectors.Entries[y*len(idxs)+i].Object.(*vector.Vector)
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func (r *BlockReader) LoadAllColumns(ctx context.Context, idxs []uint16,
	size int64, m *mpool.MPool) ([]*batch.Batch, error) {
	blocks, err := r.reader.ReadAllMetaWithFunc(ctx, size, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	bats := make([]*batch.Batch, 0)
	ioVectors, err := r.reader.Read(ctx, blocks[0].GetExtent(), idxs, nil, nil, LoadZoneMapFunc, LoadColumnFunc)
	if err != nil {
		return nil, err
	}
	for y, _ := range blocks {
		bat := batch.NewWithSize(len(idxs))
		for i := range idxs {
			bat.Vecs[i] = ioVectors.Entries[y*len(idxs)+i].Object.(*vector.Vector)
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func (r *BlockReader) LoadZoneMaps(ctx context.Context, idxs []uint16,
	ids []uint32, m *mpool.MPool) ([][]*index.ZoneMap, error) {
	blocks, err := r.reader.ReadMeta(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksZoneMap := make([][]*index.ZoneMap, len(ids))
	for i, id := range ids {
		blocksZoneMap[i], err = r.LoadZoneMap(ctx, idxs, blocks[id], m)
		if err != nil {
			return nil, err
		}
	}
	return blocksZoneMap, nil
}

func (r *BlockReader) LoadBlocksMeta(ctx context.Context, m *mpool.MPool) ([]objectio.BlockObject, error) {
	_, locs, err := DecodeMetaLocToMetas(r.key)
	if err != nil {
		return nil, err
	}
	return r.reader.ReadMeta(ctx, locs, m, LoadZoneMapFunc)
}

func (r *BlockReader) LoadZoneMap(
	ctx context.Context,
	idxs []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]*index.ZoneMap, error) {
	zoneMapList := make([]*index.ZoneMap, len(idxs))
	for i, idx := range idxs {
		column, err := block.GetColumn(idx)
		if err != nil {
			return nil, err
		}
		zm, err := column.GetIndex(ctx, objectio.ZoneMapType, m)
		if err != nil {
			return nil, err
		}
		data := zm.(*objectio.ZoneMap).GetData()

		zoneMapList[i] = data.(*index.ZoneMap)
	}

	return zoneMapList, nil
}

func (r *BlockReader) LoadBloomFilter(ctx context.Context, idx uint16,
	ids []uint32, m *mpool.MPool) ([]index.StaticFilter, error) {
	blocks, err := r.reader.ReadMeta(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksBloomFilters := make([]index.StaticFilter, len(ids))
	for i, id := range ids {
		column, err := blocks[id].GetColumn(idx)
		if err != nil {
			return nil, err
		}
		bf, err := column.GetIndexWithFunc(ctx, objectio.BloomFilterType, LoadBloomFilterFunc, m)
		if err != nil {
			return nil, err
		}
		blocksBloomFilters[i] = bf.(*objectio.BloomFilter).GetData().(index.StaticFilter)
	}
	return blocksBloomFilters, nil
}

func (r *BlockReader) LoadColumnsByTS(ctx context.Context, idxs []uint16, info catalog.BlockInfo,
	ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	return bat, nil
}

func LoadZoneMapFunc(buf []byte, typ types.Type) (any, error) {
	zm := index.NewZoneMap(typ)
	err := zm.Unmarshal(buf[:])
	if err != nil {
		return nil, err
	}
	return zm, err
}

func LoadBloomFilterFunc(size int64) objectio.ToObjectFunc {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		bf, err := index.NewBinaryFuseFilterFromSource(decompressed)
		if err != nil {
			return nil, 0, err
		}
		return bf, int64(len(decompressed)), nil
	}
}

func LoadColumnFunc(size int64) objectio.ToObjectFunc {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		vec := vector.New(types.Type{})
		if err = vec.Read(decompressed); err != nil {
			return nil, 0, err
		}
		return vec, int64(len(decompressed)), nil
	}
}
