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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Reader struct {
	block *blockFile
	fs    *ObjectFS
}

func NewReader(fs *ObjectFS, block *blockFile) *Reader {
	return &Reader{
		fs:    fs,
		block: block,
	}
}

func (r *Reader) LoadDeletes(id *common.ID) (mask *roaring.Bitmap, err error) {
	/*name := EncodeDeleteName(id, r.fs)
	f, err := r.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	size := f.Stat().Size()
	if size == 0 {
		return
	}
	node := common.GPool.Alloc(uint64(size))
	defer common.GPool.Free(node)
	if _, err = f.Read(node.Buf[:size]); err != nil {
		return
	}
	mask = roaring.New()
	err = mask.UnmarshalBinary(node.Buf[:size])*/
	return
}

func (r *Reader) Read(
	version uint64,
	id *common.ID,
	data []byte) (err error) {
	/*name := EncodeColBlkNameWithVersion(id, version, r.fs)
	f, err := r.fs.OpenFile(name, os.O_RDWR)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = f.Read(data)*/
	return
}

func (r *Reader) LoadABlkColumns(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	opts *containers.Options) (bat *containers.Batch, err error) {
	bat = containers.NewBatch()
	var f common.IRWFile
	for i, colBlk := range r.block.columns {
		if f, err = colBlk.OpenDataFile(); err != nil {
			return
		}
		defer f.Unref()
		vec := containers.MakeVector(colTypes[i], nullables[i], opts)
		bat.AddVector(colNames[i], vec)
		size := f.Stat().Size()
		if size == 0 {
			continue
		}
		buf := make([]byte, size)
		if _, err = f.Read(buf); err != nil {
			return
		}
		r := bytes.NewBuffer(buf)
		if _, err = vec.ReadFrom(r); err != nil {
			return
		}
		bat.Vecs[i] = vec
	}
	return
}
