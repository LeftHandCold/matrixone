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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type objectReaderImpV1 struct {
	baseImp
}

func newObjectReaderV1(
	oname *ObjectName,
	metaExt *Extent,
	fs fileservice.FileService,
	opts ...ReaderOptionFunc,
) (readerImp, error) {
	name := oname.String()
	reader := &baseImp{
		Object: Object{
			name: name,
			fs:   fs,
		},
		oname:   oname,
		metaExt: metaExt,
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func (r *objectReaderImpV1) ReadOneBlock(
	ctx context.Context,
	idxs []uint16,
	typs []types.Type,
	blk uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var objMeta ObjectMeta
	if objMeta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta := ObjectDataMeta(objMeta)
	return ReadOneBlockWithMeta(ctx, &meta, r.name, blk, idxs, typs, m, r.fs, constructorFactory)
}
