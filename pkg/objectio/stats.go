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

func GetObjectDataSize(objectMeta ObjectMeta) (uint32, uint32) {
	var length, origLength uint32
	for i := uint32(0); i < objectMeta.BlockCount(); i++ {
		blkLength, blkOrigLength := GetBlockDataSize(objectMeta.GetBlockMeta(i))
		length += blkLength
		origLength += blkOrigLength
	}
	return length, origLength
}

func GetBlockDataSize(block BlockObject) (uint32, uint32) {
	var length, origLength uint32
	header := block.BlockHeader()
	for i := uint16(0); i < header.ColumnCount(); i++ {
		location := block.ColumnMeta(i).Location()
		length += location.Length()
		origLength += location.OriginSize()
	}
	return length, origLength
}
