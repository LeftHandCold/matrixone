// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"context"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ Index = (*mutableIndex)(nil)

type mutableIndex struct {
	art     index.SecondaryIndex
	zonemap index.ZM
}

func NewPkMutableIndex(typ types.Type) *mutableIndex {
	return &mutableIndex{
		art:     index.NewSimpleARTMap(),
		zonemap: index.NewZM(typ.Oid, typ.Scale),
	}
}

func (idx *mutableIndex) BatchUpsert(keysCtx *index.KeysCtx,
	offset int) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if err = index.BatchUpdateZM(idx.zonemap, keysCtx.Keys); err != nil {
		return
	}
	// logutil.Infof("Pre: %s", idx.art.String())
	err = idx.art.BatchInsert(keysCtx, uint32(offset))
	// logutil.Infof("Post: %s", idx.art.String())
	return
}

func (idx *mutableIndex) Delete(key any) (err error) {
	defer func() {
		err = TranslateError(err)
	}()
	if _, err = idx.art.Delete(key); err != nil {
		return
	}
	return
}

func (idx *mutableIndex) GetActiveRow(key any) (row []uint32, err error) {
	defer func() {
		err = TranslateError(err)
		// logutil.Infof("[Trace][GetActiveRow] key=%v: err=%v", key, err)
	}()
	exist := idx.zonemap.Contains(key)
	// 1. key is definitely not existed
	if !exist {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	// 2. search art tree for key
	ikey := types.EncodeValue(key, idx.zonemap.GetType())
	row, err = idx.art.Search(ikey)
	err = TranslateError(err)
	return
}

func (idx *mutableIndex) String() string {
	return idx.art.String()
}
func (idx *mutableIndex) Dedup(ctx context.Context, key any, skipfn func(row uint32) (err error)) (err error) {
	exist := idx.zonemap.Contains(key)
	if !exist {
		return
	}
	ikey := types.EncodeValue(key, idx.zonemap.GetType())
	rows, err := idx.art.Search(ikey)
	if err == index.ErrNotFound {
		err = nil
		return
	}
	for _, row := range rows {
		if err = skipfn(row); err != nil {
			return
		}
	}
	return
}

func (idx *mutableIndex) BatchDedup(
	ctx context.Context,
	keys containers.Vector,
	skipfn func(row uint32) (err error),
	zm []byte,
	_ objectio.BloomFilter,
) (keyselects *roaring.Bitmap, err error) {
	inputZM := index.ZM(zm)
	if inputZM.Valid() {
		if exist := idx.zonemap.FastIntersect(inputZM); !exist {
			return
		}
	} else {
		if keys.Length() == 1 {
			err = idx.Dedup(ctx, keys.ShallowGet(0), skipfn)
			return
		}
		// 1. all keys are definitely not existed
		if exist := idx.zonemap.FastContainsAny(keys); !exist {
			return
		}
	}
	op := func(v []byte, _ bool, _ int) error {
		rows, err := idx.art.Search(v)
		if err == index.ErrNotFound {
			return nil
		}
		for _, row := range rows {
			if err = skipfn(row); err != nil {
				return err
			}
		}
		return nil
	}
	if err = containers.ForeachWindowBytes(keys, 0, keys.Length(), op, nil); err != nil {
		if moerr.IsMoErrCode(err, moerr.OkExpectedDup) || moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return
		} else {
			panic(err)
		}
	}
	return
}

func (idx *mutableIndex) Destroy() error {
	return idx.Close()
}

func (idx *mutableIndex) Close() error {
	idx.art = nil
	idx.zonemap = nil
	return nil
}
