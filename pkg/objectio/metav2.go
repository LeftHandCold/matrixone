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

import "github.com/matrixorigin/matrixone/pkg/container/types"

type SchemaType uint16

const (
	SchemaData SchemaType = 0
	SchemaTombstone
	SchemaCkp
)

const (
	schemaTypeOff    = maxSeqOff + maxSeqLen
	schemaTypeLen    = 2
	schemaAreaOff    = schemaTypeOff + schemaTypeLen
	schemaAreaLen    = ExtentSize
	headerDummyOffV2 = schemaAreaOff + schemaAreaLen
	headerDummyLenV2 = 20
	headerLenV2      = headerDummyOffV2 + headerDummyLenV2
)

type objectMetaV2 []byte

func buildObjectMetaV2(count uint16) objectMetaV2 {
	length := headerLenV2 + uint32(count)*colMetaLen
	buf := make([]byte, length)
	return buf[:]
}

func (o objectMetaV2) BlockHeader() BlockHeader {
	return BlockHeader(o[:headerLenV2])
}

func (o objectMetaV2) MustGetColumn(seqnum uint16) ColumnMeta {
	if seqnum > o.BlockHeader().MaxSeqnum() {
		return BuildObjectColumnMeta()
	}
	return GetObjectColumnMeta(seqnum, o[headerLenV2:])
}

func (o objectMetaV2) AddColumnMeta(idx uint16, col ColumnMeta) {
	offset := headerLenV2 + uint32(idx)*colMetaLen
	copy(o[offset:offset+colMetaLen], col)
}

func (o objectMetaV2) Length() uint32 {
	return headerLenV2 + uint32(o.BlockHeader().MetaColumnCount())*colMetaLen
}

func (o objectMetaV2) BlockCount() uint32 {
	return uint32(o.BlockHeader().Sequence())
}

func (o objectMetaV2) BlockIndex() BlockIndex {
	offset := o.Length()
	return BlockIndex(o[offset:])
}

func (o objectMetaV2) GetBlockMeta(id uint32) BlockObject {
	offset, length := o.BlockIndex().BlockMetaPos(id)
	return BlockObject(o[offset : offset+length])
}

func (o objectMetaV2) GetColumnMeta(blk uint32, seqnum uint16) ColumnMeta {
	return o.GetBlockMeta(blk).MustGetColumn(seqnum)
}

func (o objectMetaV2) IsEmpty() bool {
	return len(o) == 0
}

func BuildBlockHeaderV2() BlockHeader {
	var buf [headerLenV2]byte
	return buf[:]
}

func (bh BlockHeader) SchemaType() uint16 {
	return types.DecodeUint16(bh[schemaTypeOff : schemaTypeOff+schemaTypeLen])
}

func (bh BlockHeader) SetSchemaType(schemaType uint16) {
	copy(bh[schemaTypeOff:schemaTypeOff+schemaTypeLen], types.EncodeUint16(&schemaType))
}

func (bh BlockHeader) SchemaAreaExtent() Extent {
	return Extent(bh[schemaAreaOff : schemaAreaOff+schemaAreaLen])
}

func (bh BlockHeader) SetSchemaAreaExtent(location Extent) {
	copy(bh[schemaAreaOff:schemaAreaOff+schemaAreaLen], location)
}
