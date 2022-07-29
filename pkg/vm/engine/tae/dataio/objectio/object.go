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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"strings"

	"os"
	"path"
	// "strconv"
	// "strings"
)

type ObjectType uint8

const (
	DataType ObjectType = iota
	MetadataSegType
	MetadataBlkType
	NodeType
)

const (
	ObjectSize = 64 * 1024 * 1024
	PageSize   = 4096
	MetaSize   = 512
	HoleSize   = 8 * MetaSize
)

const (
	DATA  = "data"
	META  = "meta"
	SEG   = "seg"
	BLK   = "blk"
	INODE = "inode"
)

type Object struct {
	name      string
	oFile     *os.File
	allocator *ObjectAllocator
	oType     ObjectType
	// mutex     sync.Mutex // unused
}

func OpenObject(name string, oType ObjectType, dir string) (object *Object, err error) {
	object = &Object{
		name:  name,
		oType: oType,
	}
	path := path.Join(dir, encodeName(name, oType))
	if _, err = os.Stat(path); os.IsNotExist(err) {
		object.oFile, err = os.Create(path)
		return
	}

	if object.oFile, err = os.OpenFile(path, os.O_RDWR, os.ModePerm); err != nil {
		return
	}
	return
}

func (o *Object) Mount(capacity uint64, pageSize uint32) {
	o.allocator = NewObjectAllocator(capacity, pageSize)
}

func (o *Object) Append(data []byte, offset int64) (n int, err error) {
	n, err = o.oFile.WriteAt(data, offset)
	if err != nil {
		return
	}
	return
}

func (o *Object) Read(offset int64, data []byte) (length int, err error) {
	length, err = o.oFile.ReadAt(data, offset)
	return
}

func (o *Object) GetSize() uint64 {
	return o.allocator.GetAvailable()
}

func encodeName(name string, oType ObjectType) string {
	if oType == NodeType {
		return fmt.Sprintf("%s.%s", name, INODE)
	} else if oType == MetadataSegType {
		return fmt.Sprintf("%s.%s", name, SEG)
	} else if oType == MetadataBlkType {
		return fmt.Sprintf("%s.%s", name, BLK)
	}
	return fmt.Sprintf("%s.%s", name, DATA)
}

func decodeName(name string) (id string, oType ObjectType, err error) {
	oName := strings.Split(name, ".")
	if len(oName) != 2 {
		err = fmt.Errorf("%w: %s", file.ErrInvalidName, name)
		return
	}
	id = oName[0]
	if oName[1] == DATA {
		oType = DataType
	} else if oName[1] == INODE {
		oType = NodeType
	} else if oName[1] == SEG {
		oType = MetadataSegType
	} else if oName[1] == BLK {
		oType = MetadataBlkType
	}
	return
}
