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
	"fmt"
	"unsafe"
)

const ExtentSize = 13

type Extent struct {
	alg        uint8
	offset     uint32
	length     uint32
	originSize uint32
}

func NewExtent(alg uint8, offset, length, originSize uint32) Extent {
	return Extent{
		alg:        alg,
		offset:     offset,
		length:     length,
		originSize: originSize,
	}
}

func (ex Extent) Alg() uint8 { return ex.alg }

func (ex Extent) End() uint32 { return ex.offset + ex.length }

func (ex Extent) Offset() uint32 { return ex.offset }

func (ex Extent) Length() uint32 { return ex.length }

func (ex Extent) OriginSize() uint32 { return ex.originSize }

func (ex Extent) Marshal() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&ex)), ExtentSize)
}
func (ex *Extent) Unmarshal(data []byte) {
	ex = (*Extent)(unsafe.Pointer(&data[0]))
}

func (ex Extent) String() string {
	return fmt.Sprintf("%d_%d_%d_%d", ex.alg, ex.offset, ex.length, ex.originSize)
}
