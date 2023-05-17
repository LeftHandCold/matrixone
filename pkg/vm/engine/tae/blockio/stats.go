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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type ObjectStats struct {
	objectCount       uint32
	metaLength        uint64
	length            uint64
	originLength      uint64
	indexLength       uint64
	indexOriginLength uint64
}

func StatTable(ctx context.Context, table *catalog.TableEntry, fs fileservice.FileService) (*ObjectStats, error) {
	objects, err := GetTableObjects(table)
	if err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		return nil, nil
	}
	var length, originLength, indexLength, indexOriginLength, metaLength uint64
	for _, location := range objects {
		meta, err := objectio.FastLoadObjectMeta(ctx, location, fs)
		if err != nil {
			return nil, err
		}
		metaLength += uint64(len(meta))
		blkLength, blkOrigLength := objectio.GetObjectDataSize(meta)
		length += uint64(blkLength)
		originLength += uint64(blkOrigLength)
		indexLength += uint64(meta.BlockHeader().BFExtent().Length())
		indexOriginLength += uint64(meta.BlockHeader().BFExtent().OriginSize())
	}
	return &ObjectStats{
		objectCount:       uint32(len(objects)),
		metaLength:        metaLength,
		length:            length,
		originLength:      originLength,
		indexLength:       indexLength,
		indexOriginLength: indexOriginLength,
	}, nil
}

func GetTableObjects(table *catalog.TableEntry) (map[objectio.ObjectNameShort]*objectio.Location, error) {
	objects := make(map[objectio.ObjectNameShort]*objectio.Location)
	tableProcessor := new(catalog.LoopProcessor)
	tableProcessor.BlockFn = func(be *catalog.BlockEntry) error {
		if !be.IsActive() {
			return nil
		}
		location := be.Location
		shortName := location.ShortName()
		if objects[*shortName] == nil {
			objects[*shortName] = &location
		}
		return nil
	}
	err := table.RecurLoop(tableProcessor)
	if err != nil {
		return nil, err
	}
	return objects, nil
}
