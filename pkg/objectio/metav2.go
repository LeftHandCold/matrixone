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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type SchemaType uint16

const (
	SchemaData      SchemaType = 0
	SchemaTombstone SchemaType = 1
	SchemaCkp       SchemaType = 2
)

const (
	schemaTypeOff    = maxSeqOff + maxSeqLen
	schemaTypeLen    = 2
	schemaAreaOff    = schemaTypeOff + schemaTypeLen
	schemaAreaLen    = 4
	headerDummyOffV2 = schemaAreaOff + schemaAreaLen
	headerDummyLenV2 = 29
	headerLenV2      = headerDummyOffV2 + headerDummyLenV2
)
const InvalidSchemaType = 0xFF

type BlockHeaderV2 []byte

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

const (
	schemaCountLen   = 2
	schemaType       = 2
	schemaBlockCount = 2
	typePosLen       = schemaType + schemaBlockCount
)

type SchemaTypeIndex []byte

func BuildSchemaTypeIndex(count uint16) SchemaTypeIndex {
	length := schemaCountLen + count*typePosLen
	buf := make([]byte, length)
	return buf[:]
}

func (oh SchemaTypeIndex) SchemaCount() uint16 {
	return types.DecodeUint16(oh[:schemaCountLen])
}

func (oh SchemaTypeIndex) SetSchemaCount(cnt uint16) {
	copy(oh[:schemaCountLen], types.EncodeUint16(&cnt))
}

func (oh SchemaTypeIndex) SchemaMeta(pos uint16) (uint16, uint16) {
	offStart := schemaCountLen + pos*typePosLen
	offEnd := schemaCountLen + pos*typePosLen + schemaType
	return types.DecodeUint16(oh[offStart:offEnd]), types.DecodeUint16(oh[offStart+schemaBlockCount : offEnd+schemaBlockCount])
}

func (oh SchemaTypeIndex) SetSchemaMeta(pos uint16, st uint16, count uint16) {
	offStart := schemaCountLen + pos*typePosLen
	offEnd := schemaCountLen + pos*typePosLen + schemaType
	copy(oh[offStart:offEnd], types.EncodeUint16(&st))
	copy(oh[offStart+schemaBlockCount:offEnd+schemaBlockCount], types.EncodeUint16(&count))
}

func (oh SchemaTypeIndex) Length() uint32 {
	return uint32(oh.SchemaCount()*typePosLen + schemaCountLen)
}
