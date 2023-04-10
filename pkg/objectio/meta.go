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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const ExtentSize = 16

const ObjectColumnMetaSize = 72
const FooterSize = 8 /*Magic*/ + 4 /*metaStart*/ + 4 /*metaLen*/

type ObjectMeta []byte

func BuildObjectMeta(count uint16) ObjectMeta {
	length := headerLen + uint32(count)*objectColumnMetaLen
	buf := make([]byte, length)
	return buf[:]
}

func (o ObjectMeta) BlockHeader() BlockHeader {
	return BlockHeader(o[:headerLen])
}

func (o ObjectMeta) ObjectColumnMeta(idx uint16) ObjectColumnMeta {
	return GetObjectColumnMeta(idx, o)
}

func (o ObjectMeta) Length() uint32 {
	return headerLen + uint32(o.BlockHeader().ColumnCount())*objectColumnMetaLen
}

func (o ObjectMeta) BlockCount() uint32 {
	return uint32(o.BlockHeader().BlockID())
}

func (o ObjectMeta) BlockIndex() BlockIndex {
	end := o.Length() + 4 + uint32(o.BlockHeader().BlockID())*8
	return BlockIndex(o[o.Length():end])
}

func (o ObjectMeta) ObjectNext() []byte {
	return o[o.Length():]
}

func (o ObjectMeta) GetBlockMeta(id uint32) BlockMeta {
	offset, length := o.BlockIndex().BlockMetaPos(id)
	return BlockMeta(o[offset:length])
}

func (o ObjectMeta) GetColumnMeta(idx uint16, id uint32) ColumnMeta {
	return o.GetBlockMeta(id).ColumnMeta(idx)
}

const (
	blockCountLen = 4
	blockOffset   = 4
	blockLen      = 4
	posLen        = blockOffset + blockLen
)

type BlockIndex []byte

func (oh BlockIndex) BlockCount() uint32 {
	return types.DecodeUint32(oh[:blockCountLen])
}

func (oh BlockIndex) BlockMetaPos(BlockID uint32) (uint32, uint32) {
	offStart := blockCountLen + BlockID*posLen
	offEnd := blockCountLen + BlockID*posLen + blockOffset
	return types.DecodeUint32(oh[offStart:offEnd]), types.DecodeUint32(oh[offStart+blockLen : offEnd+blockLen])
}

func (oh BlockIndex) Length() uint32 {
	return blockCountLen * posLen
}

const (
	ndvLen              = 4
	nullCntOff          = ndvLen
	nullCntLen          = 4
	oZoneMapOff         = nullCntOff + nullCntLen
	oZoneMapLen         = 64
	objectColumnMetaLen = oZoneMapOff + oZoneMapLen
)

// ObjectColumnMeta len 4 + 4 + 64 = 72 bytes
type ObjectColumnMeta []byte

func GetObjectColumnMeta(idx uint16, data []byte) ObjectColumnMeta {
	offset := RowsLen + uint32(idx)*objectColumnMetaLen
	return data[offset : offset+objectColumnMetaLen]
}

func BuildObjectColumnMeta() ObjectColumnMeta {
	var buf [objectColumnMetaLen]byte
	return buf[:]
}

func (om ObjectColumnMeta) Ndv() uint32 {
	return types.DecodeUint32(om[:ndvLen])
}

func (om ObjectColumnMeta) SetNdv(cnt uint32) {
	copy(om[:ndvLen], types.EncodeUint32(&cnt))
}

func (om ObjectColumnMeta) NullCnt() uint32 {
	return types.DecodeUint32(om[nullCntOff : nullCntOff+nullCntLen])
}

func (om ObjectColumnMeta) SetNullCnt(cnt uint32) {
	copy(om[nullCntOff:nullCntOff+nullCntLen], types.EncodeUint32(&cnt))
}

func (om ObjectColumnMeta) ZoneMap() ZoneMap {
	return ZoneMap(om[oZoneMapOff : oZoneMapOff+oZoneMapLen])
}

func (om ObjectColumnMeta) SetZoneMap(zm []byte) {
	copy(om[oZoneMapOff:oZoneMapOff+oZoneMapLen], zm)
}

// +---------------------------------------------------------------------------------------------+
// |                                           Header                                            |
// +-------------+---------------+--------------+---------------+---------------+----------------+
// | TableID(8B) | SegmentID(8B) | BlockID(8B)  | ColumnCnt(2B) | Reserved(34B) |  Chksum(4B)    |
// +-------------+---------------+--------------+---------------+---------------+----------------+
// |                                         ColumnMeta                                          |
// +---------------------------------------------------------------------------------------------+
// |                                         ColumnMeta                                          |
// +---------------------------------------------------------------------------------------------+
// |                                         ColumnMeta                                          |
// +---------------------------------------------------------------------------------------------+
// |                                         ..........                                          |
// +---------------------------------------------------------------------------------------------+
// Header Size = 64B
// TableID = Table ID for Block
// SegmentID = Segment ID
// BlockID = Block ID
// ColumnCnt = The number of column in the block
// Chksum = Block metadata checksum
// Reserved = 34 bytes reserved space

const (
	tableIDLen        = 8
	segmentIDOff      = tableIDLen
	segmentIDLen      = 8
	blockIDOff        = segmentIDOff + segmentIDLen
	blockIDLen        = 8
	rowsOff           = blockIDOff + blockIDLen
	rowsLen           = 4
	columnCountOff    = rowsOff + rowsLen
	columnCountLen    = 2
	headerDummyOff    = columnCountOff + columnCountLen
	headerDummyLen    = 34
	headerCheckSumOff = headerDummyOff + headerDummyLen
	headerCheckSumLen = 4
	headerLen         = headerCheckSumOff + headerCheckSumLen
)

type BlockMeta []byte

func BuildBlockMeta(count uint16) BlockMeta {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	return buf[:]
}

func GetBlockMeta(id uint32, data []byte) BlockMeta {
	metaOff := uint32(0)
	idOff := uint32(0)
	columnCount := uint16(0)
	for {
		header := BlockHeader(data[metaOff : metaOff+headerLen])
		columnCount = header.ColumnCount()
		metaLen := headerLen + uint32(columnCount)*colMetaLen
		metaOff += metaLen
		if id == idOff {
			break
		}
		idOff++
	}
	return data[metaOff : metaOff+headerLen+uint32(columnCount)*colMetaLen]
}

func (bm BlockMeta) BlockHeader() BlockHeader {
	return BlockHeader(bm[:headerLen])
}

func (bm BlockMeta) SetBlockMetaHeader(header BlockHeader) {
	copy(bm[:headerLen], header)
}

func (bm BlockMeta) ColumnMeta(idx uint16) ColumnMeta {
	return GetColumnMeta(idx, bm)
}

func (bm BlockMeta) AddColumnMeta(idx uint16, col ColumnMeta) {
	offset := headerLen + idx*colMetaLen
	copy(bm[offset:offset+colMetaLen], col)
}

func (bm BlockMeta) IsEmpty() bool {
	if len(bm) == 0 {
		return true
	}
	return false
}

type BlockHeader []byte

func BuildBlockHeader() BlockHeader {
	var buf [headerLen]byte
	return buf[:]
}

func (bh BlockHeader) TableID() uint64 {
	return types.DecodeUint64(bh[:tableIDLen])
}

func (bh BlockHeader) SetTableID(id uint64) {
	copy(bh[:tableIDLen], types.EncodeUint64(&id))
}

func (bh BlockHeader) SegmentID() uint64 {
	return types.DecodeUint64(bh[segmentIDOff : segmentIDOff+segmentIDLen])
}

func (bh BlockHeader) SetSegmentID(id uint64) {
	copy(bh[segmentIDOff:segmentIDOff+segmentIDLen], types.EncodeUint64(&id))
}

func (bh BlockHeader) BlockID() uint64 {
	return types.DecodeUint64(bh[blockIDOff : blockIDOff+blockIDLen])
}

func (bh BlockHeader) SetBlockID(id uint64) {
	copy(bh[blockIDOff:blockIDOff+blockIDLen], types.EncodeUint64(&id))
}

func (bh BlockHeader) Rows() uint32 {
	return types.DecodeUint32(bh[rowsOff : rowsOff+rowsLen])
}

func (bh BlockHeader) SetRows(rows uint32) {
	copy(bh[rowsOff:rowsOff+rowsLen], types.EncodeUint32(&rows))
}

func (bh BlockHeader) ColumnCount() uint16 {
	return types.DecodeUint16(bh[columnCountOff : columnCountOff+columnCountLen])
}

func (bh BlockHeader) SetColumnCount(count uint16) {
	copy(bh[columnCountOff:columnCountOff+columnCountLen], types.EncodeUint16(&count))
}

func (bh BlockHeader) IsEmpty() bool {
	if len(bh) == 0 {
		return true
	}
	return false
}

// +---------------------------------------------------------------------------------------------------------------+
// |                                                    ColumnMeta                                                 |
// +--------+-------+----------+--------+---------+--------+--------+------------+---------+----------+------------+
// |Type(1B)|Idx(2B)| Algo(1B) |Offset(4B)|Size(4B)|oSize(4B)|Min(32B)|Max(32B)|BFoffset(4b)|BFlen(4b)|BFoSize(4B) |
// +--------+-------+----------+--------+---------+--------+--------+------------+---------+----------+------------+
// |                                        Reserved(32B)                                             | Chksum(4B) |
// +---------------------------------------------------------------------------------------------------------------+
// ColumnMeta Size = 128B
// Type = Metadata type, always 0, representing column meta, used for extension.
// Idx = Column index
// Algo = Type of compression algorithm for column data
// Offset = Offset of column data
// Size = Size of column data
// oSize = Original data size
// Min = Column min value
// Max = Column Max value
// BFoffset = Bloomfilter data offset
// Bflen = Bloomfilter data size
// BFoSize = Bloomfilter original data size
// Chksum = Data checksum
// Reserved = 32 bytes reserved space

const (
	typeLen            = 1
	idxOff             = typeLen
	idxLen             = 2
	algOff             = idxOff + idxLen
	algLen             = 1
	locationOff        = algOff + algLen
	locationLen        = 16
	zoneMapOff         = locationOff + locationLen
	zoneMapLen         = 64
	bloomFilterOff     = zoneMapOff + zoneMapLen
	bloomFilterLen     = 16
	colMetaDummyOff    = bloomFilterOff + bloomFilterLen
	colMetaDummyLen    = 32
	colMetaChecksumOff = colMetaDummyOff + colMetaDummyOff
	colMetaChecksumLen = 4
	colMetaLen         = colMetaChecksumOff + colMetaChecksumLen
)

func GetColumnMeta(idx uint16, data []byte) ColumnMeta {
	offset := headerLen + uint32(idx)*colMetaLen
	return data[offset : offset+colMetaLen]
}

type ColumnMeta []byte

func BuildColumnMeta() ColumnMeta {
	var buf [colMetaLen]byte
	return buf[:]
}

func (cm ColumnMeta) Type() uint8 {
	return types.DecodeUint8(cm[:typeLen])
}

func (cm ColumnMeta) setType(t uint8) {
	copy(cm[:typeLen], types.EncodeUint8(&t))
}

func (cm ColumnMeta) Idx() uint16 {
	return types.DecodeUint16(cm[idxOff : idxOff+idxLen])
}

func (cm ColumnMeta) setIdx(idx uint16) {
	copy(cm[idxOff:idxOff+idxLen], types.EncodeUint16(&idx))
}

func (cm ColumnMeta) Alg() uint8 {
	return types.DecodeUint8(cm[algOff : algOff+algLen])
}

func (cm ColumnMeta) setAlg(alg uint8) {
	copy(cm[algOff:algOff+algLen], types.EncodeUint8(&alg))
}

func (cm ColumnMeta) Location() Extent {
	extent := Extent{}
	extent.Unmarshal(cm[locationOff : locationOff+locationLen])
	return extent
}

func (cm ColumnMeta) setLocation(location Extent) {
	copy(cm[locationOff:locationOff+locationLen], location.Marshal())
}

func (cm ColumnMeta) ZoneMap() ZoneMap {
	return ZoneMap(cm[zoneMapOff : zoneMapOff+zoneMapLen])
}

func (cm ColumnMeta) setZoneMap(zm ZoneMap) {
	copy(cm[zoneMapOff:zoneMapOff+zoneMapLen], zm)
}

func (cm ColumnMeta) BloomFilter() Extent {
	extent := Extent{}
	extent.Unmarshal(cm[bloomFilterOff : bloomFilterOff+bloomFilterLen])
	return extent
}

func (cm ColumnMeta) setBloomFilter(location Extent) {
	copy(cm[bloomFilterOff:bloomFilterOff+bloomFilterLen], location.Marshal())
}

func (cm ColumnMeta) Checksum() uint32 {
	return types.DecodeUint32(cm[colMetaChecksumOff : colMetaChecksumOff+colMetaChecksumLen])
}

func (cm ColumnMeta) IsEmpty() bool {
	if len(cm) == 0 {
		return true
	}
	return false
}

type Header struct {
	magic   uint64
	version uint16
	//dummy   [22]byte
}

type Footer struct {
	magic     uint64
	metaStart uint32
	metaLen   uint32
}

func (f *Footer) Marshal() []byte {
	var buffer bytes.Buffer
	buffer.Write(types.EncodeUint32(&f.metaStart))
	buffer.Write(types.EncodeUint32(&f.metaLen))
	buffer.Write(types.EncodeUint64(&f.magic))
	return buffer.Bytes()
}

func (f *Footer) Unmarshal(data []byte) error {
	f.metaStart = types.DecodeUint32(data)
	data = data[4:]
	f.metaLen = types.DecodeUint32(data)
	data = data[4:]
	f.magic = types.DecodeUint64(data)
	return nil
}
