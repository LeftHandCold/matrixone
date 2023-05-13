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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"path"
	"strconv"
)

func LoadColumns(ctx context.Context,
	cols []uint16,
	typs []types.Type,
	fs fileservice.FileService,
	location objectio.Location,
	m *mpool.MPool,
	accountId uint32) (bat *batch.Batch, err error) {
	name := location.Name()
	fileName := path.Join(strconv.Itoa(int(accountId)), name.String())
	extent := location.Extent()
	var meta objectio.ObjectMeta
	var ioVectors *fileservice.IOVector
	if meta, err = objectio.ReadObjectMeta(ctx, fileName, &extent, false, fs); err != nil {
		return
	}
	if ioVectors, err = objectio.ReadOneBlock(ctx, &meta, fileName, location.ID(), cols, typs, m, fs); err != nil {
		return
	}
	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		obj, err = objectio.Decode(ioVectors.Entries[i].ObjectBytes)
		if err != nil {
			return
		}
		bat.Vecs[i] = obj.(*vector.Vector)
	}
	return
}
