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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
)

type ZmReader struct {
	metaKey objectio.Location
	idx     uint16
	reader  dataio.Reader
}

func NewZmReader(fs *objectio.ObjectFS, idx uint16, metaLoc objectio.Location) *ZmReader {
	reader, _ := blockio.NewObjectReaderNew(fs.Service, metaLoc)
	return &ZmReader{
		metaKey: metaLoc,
		idx:     idx,
		reader:  reader,
	}
}

func (r *ZmReader) getZoneMap() (dataio.Index, error) {
	zmList, err := r.reader.LoadZoneMaps(context.Background(), []uint16{r.idx}, []uint32{r.metaKey.ID()}, nil)
	if err != nil {
		// TODOa: Error Handling?
		return nil, err
	}
	return zmList[0][0], err
}

func (r *ZmReader) Contains(key any) bool {
	zm, err := r.getZoneMap()
	if err != nil {
		// TODOa: Error Handling?
		return false
	}
	return zm.Contains(key)
}

func (r *ZmReader) FastContainsAny(keys containers.Vector) (ok bool) {
	zm, err := r.getZoneMap()
	if err != nil {
		// TODOa: Error Handling?
		return false
	}
	return zm.FastContainsAny(keys)
}

func (r *ZmReader) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	zm, err := r.getZoneMap()
	if err != nil {
		// TODOa: Error Handling?
		return
	}
	return zm.ContainsAny(keys)
}

func (r *ZmReader) Destroy() error { return nil }
