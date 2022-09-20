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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type zonemapNode struct {
	*buffer.Node
	mgr     base.INodeManager
	file    objectio.ColumnObject
	zonemap *index.ZoneMap
	dataTyp types.Type
}

func newZonemapNode(mgr base.INodeManager, file objectio.ColumnObject, id *common.ID, typ types.Type) *zonemapNode {
	impl := new(zonemapNode)
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(file.GetMeta().GetLocation().Length()))
	impl.LoadFunc = impl.OnLoad
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestroy
	impl.file = file
	impl.mgr = mgr
	impl.dataTyp = typ
	mgr.RegisterNode(impl)
	return impl
}

func (n *zonemapNode) OnLoad() {
	if n.zonemap != nil {
		// no-op
		return
	}
	fsData, err := n.file.GetIndex(objectio.ZoneMapType)
	if err != nil {
		panic(err)
	}
	data := fsData.(*objectio.ZoneMap)
	n.zonemap = index.NewZoneMap(n.dataTyp)
	err = n.zonemap.Unmarshal(data.GetMin(), data.GetMax())
	if err != nil {
		panic(err)
	}
}

func (n *zonemapNode) OnUnload() {
	if n.zonemap == nil {
		// no-op
		return
	}
	n.zonemap = nil
}

func (n *zonemapNode) OnDestroy() {
	//n.file.Unref()
}

func (n *zonemapNode) Close() (err error) {
	if err = n.Node.Close(); err != nil {
		return err
	}
	n.zonemap = nil
	return nil
}

type ZMReader struct {
	node *zonemapNode
}

func NewZMReader(mgr base.INodeManager, file objectio.ColumnObject, id *common.ID, typ types.Type) *ZMReader {
	return &ZMReader{
		node: newZonemapNode(mgr, file, id, typ),
	}
}

func (reader *ZMReader) Destroy() (err error) {
	if err = reader.node.Close(); err != nil {
		return err
	}
	return nil
}

func (reader *ZMReader) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.zonemap.ContainsAny(keys)
}

func (reader *ZMReader) Contains(key any) bool {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.zonemap.Contains(key)
}

type ZMWriter struct {
	cType       CompressType
	writer      objectio.Writer
	block       objectio.BlockObject
	zonemap     *index.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewZMWriter() *ZMWriter {
	return &ZMWriter{}
}

func (writer *ZMWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType CompressType, colIdx uint16, internalIdx uint16) error {
	writer.writer = wr
	writer.block = block
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *ZMWriter) Finalize() (*IndexMeta, error) {
	if writer.zonemap == nil {
		panic("unexpected error")
	}
	appender := writer.writer
	meta := NewEmptyIndexMeta()
	meta.SetIndexType(BlockZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.zonemap.Marshal()
	if err != nil {
		return nil, err
	}
	min := make([]byte, 32)
	copy(min, iBuf[:31])
	max := make([]byte, 32)
	copy(max, iBuf[32:])
	zonemap, err := objectio.NewZoneMap(writer.colIdx, min, max)
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(iBuf))
	compressed := Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	err = appender.WriteIndex(writer.block, zonemap)
	if err != nil {
		return nil, err
	}
	//meta.SetStartOffset(startOffset)
	return meta, nil
}

func (writer *ZMWriter) AddValues(values containers.Vector) (err error) {
	typ := values.GetType()
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = data.ErrWrongType
			return
		}
	}
	ctx := new(index.KeysCtx)
	ctx.Keys = values
	ctx.Count = values.Length()
	err = writer.zonemap.BatchUpdate(ctx)
	return
}

func (writer *ZMWriter) SetMinMax(min, max any, typ types.Type) (err error) {
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = data.ErrWrongType
			return
		}
	}
	writer.zonemap.SetMin(min)
	writer.zonemap.SetMax(max)
	return
}
