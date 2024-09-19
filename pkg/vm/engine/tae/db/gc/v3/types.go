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

package gc

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	DefaultInMemoryStagedSize = mpool.MB * 32
)

var ObjectTableAttrs []string
var ObjectTableTypes []types.Type

func init() {
	ObjectTableAttrs = []string{
		"stats",
		"created_ts",
		"deleted_ts",
		"db_id",
		"table_id",
	}
	ObjectTableTypes = []types.Type{
		objectio.VarcharType,
		objectio.TSType,
		objectio.TSType,
		objectio.Uint64Type,
		objectio.Uint64Type,
	}
}

func NewObjectTableBatch() *Batch {
	ret := batch.New(false, ObjectTableAttrs)
	ret.SetVector(0, vector.NewVec(ObjectTableTypes[0]))
	ret.SetVector(1, vector.NewVec(ObjectTableTypes[1]))
	ret.SetVector(2, vector.NewVec(ObjectTableTypes[2]))
	ret.SetVector(3, vector.NewVec(ObjectTableTypes[3]))
	ret.SetVector(4, vector.NewVec(ObjectTableTypes[4]))
	return ret
}
