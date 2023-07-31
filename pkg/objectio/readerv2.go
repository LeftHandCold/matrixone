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

type objectReaderV2 struct {
	baseObjectReader
}

func newObjectReaderWithStrV2(name string, fs fileservice.FileService, opts ...ReaderOptionFunc) (objectReader, error) {
	reader := &objectReaderV2{}
	reader.Object = Object{
		name: name,
		fs:   fs,
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func getReader() objectReader {
	return &objectReaderV2{}
}

func newObjectReaderV2(
	oname *ObjectName,
	metaExt *Extent,
	fs fileservice.FileService,
	opts ...ReaderOptionFunc,
) (objectReader, error) {
	name := oname.String()
	reader := &objectReaderV2{}
	reader.Object = Object{
		name: name,
		fs:   fs,
	}
	reader.oname = oname
	reader.metaExt = metaExt
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func (r *objectReaderV2) getDataMeta(ctx context.Context, m *mpool.MPool) (meta ObjectDataMeta, err error) {
	var objMeta ObjectMeta
	if objMeta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta = objMeta.MustDataMeta()
	return
}

func (r *objectReaderV2) ReadOneBlock(
	ctx context.Context,
	idxs []uint16,
	typs []types.Type,
	blk uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectDataMeta
	if meta, err = r.getDataMeta(ctx, m); err != nil {
		return
	}
	return ReadOneBlockWithMeta(ctx, &meta, r.name, blk, idxs, typs, m, r.fs, constructorFactory)
}

func (r *objectReaderV2) ReadAll(
	ctx context.Context,
	idxs []uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectDataMeta
	if meta, err = r.getDataMeta(ctx, m); err != nil {
		return
	}
	return ReadAllBlocksWithMeta(ctx, &meta, r.name, idxs, r.noLRUCache, m, r.fs, constructorFactory)
}

func (r *objectReaderV2) ReadMultiBlocks(
	ctx context.Context,
	opts map[uint16]*ReadBlockOptions,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectDataMeta
	if meta, err = r.getDataMeta(ctx, m); err != nil {
		return
	}
	return ReadMultiBlocksWithMeta(
		ctx,
		r.name,
		&meta,
		opts,
		false,
		m,
		r.fs,
		constructorFactory)
}
