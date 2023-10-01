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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

func LoadColumnsData(
	ctx context.Context,
	metaType objectio.DataMetaType,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
) (bat *batch.Batch, err error) {
	name := location.Name()
	var meta objectio.ObjectMeta
	var ioVectors *fileservice.IOVector
	if meta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
		return
	}
	dataMeta := meta.MustGetMeta(metaType)
	if ioVectors, err = objectio.ReadOneBlock(ctx, &dataMeta, name.String(), location.ID(), cols, typs, m, fs); err != nil {
		return
	}
	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		obj, err = objectio.Decode(ioVectors.Entries[i].CachedData.Bytes())
		if err != nil {
			return
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}
	//TODO call CachedData.Release
	return
}

func LoadColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
) (bat *batch.Batch, err error) {
	return LoadColumnsData(ctx, objectio.SchemaData, cols, typs, fs, location, m)
}

func LoadTombstoneColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
) (bat *batch.Batch, err error) {
	return LoadColumnsData(ctx, objectio.SchemaTombstone, cols, typs, fs, location, m)
}

func LoadBF(
	ctx context.Context,
	loc objectio.Location,
	cache model.LRUCache,
	fs fileservice.FileService,
	noLoad bool,
) (bf objectio.BloomFilter, err error) {
	v, ok := cache.Get(ctx, *loc.ShortName())
	if ok {
		bf = objectio.BloomFilter(v)
		return
	}
	if noLoad {
		return
	}
	r, _ := NewObjectReader(fs, loc)
	v, _, err = r.LoadAllBF(ctx)
	if err != nil {
		return
	}
	cache.Set(ctx, *loc.ShortName(), v)
	bf = objectio.BloomFilter(v)
	return
}

func GetLocationWithBlockID(
	ctx context.Context,
	fs fileservice.FileService,
	block string,
) (objectio.ObjectMeta, objectio.Location, error) {
	sid, fileName, id, filenum := GetObjectFileName(block)
	reader, err := NewFileReader(fs, fileName)
	if err != nil {
		return nil, nil, err
	}
	meta, err := reader.reader.ReadAllMeta(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	extent := reader.reader.GetMetaExtent()
	name := objectio.BuildObjectName(&sid, filenum)
	location := objectio.BuildLocation(name, *extent, 0, id)
	return meta, location, nil
}

func DebugDataWithBlockID(
	ctx context.Context,
	fs fileservice.FileService,
	block string,
) (*batch.Batch, error) {
	meta, location, err := GetLocationWithBlockID(ctx, fs, block)
	if err != nil {
		return nil, err
	}

	data := meta.MustDataMeta()
	idxes := make([]uint16, data.BlockHeader().ColumnCount())
	for i := range idxes {
		idxes[i] = uint16(i)
	}
	bat, err := objectio.ReadOneBlockWithOutType(ctx, &data, location.Name().String(), uint32(location.ID()), idxes, fileservice.SkipMemory, fs)
	logutil.Infof("DebugDataWithBlockID: %s", bat.String())
	return bat, nil
}
