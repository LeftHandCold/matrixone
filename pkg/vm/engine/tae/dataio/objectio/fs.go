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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"github.com/pierrec/lz4"
	"os"
	gsort "sort"
	"strings"
	"sync"
)

type ObjectFS struct {
	sync.RWMutex
	common.RefHelper
	nodes     map[string]tfs.File
	data      map[string]*Object
	driver    *MetaDriver
	attr      *Attr
	lastId    uint64
	lastInode uint64
	seq       uint64
	uploader  *uploaderQueue
	work      *workHandler
}

type Attr struct {
	algo uint8
	dir  string
}

func NewObjectFS() tfs.FS {
	fs := &ObjectFS{
		attr: &Attr{
			algo: compress.Lz4,
		},
		nodes: make(map[string]tfs.File),
		data:  make(map[string]*Object),
	}
	fs.driver = newMetaDriver(fs)
	fs.lastId = 1
	fs.lastInode = 1
	return fs
}

func (o *ObjectFS) SetDir(dir string) {
	o.attr.dir = dir
}

func (o *ObjectFS) Mount() {
	o.work = NewWorkHandler(64, o)
	o.uploader = NewUploaderQueue(20000, 1000, o.work.OnObjects)
	o.uploader.Start()
	o.RebuildObject()
	o.driver.Replay()
}

func (o *ObjectFS) OpenDir(name string, nodes *map[string]tfs.File) (*map[string]tfs.File, tfs.File, error) {
	dir := (*nodes)[name]
	if dir == nil {
		o.seq++
		dir = openObjectDir(o, name)
		(*nodes)[name] = dir
	}
	return &dir.(*ObjectDir).nodes, dir, nil
}

func (o *ObjectFS) OpenFile(name string, flag int) (file tfs.File, err error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileName := strings.Split(name, "/")
	nodes := &o.nodes
	var dir tfs.File
	paths := len(fileName)
	if strings.Contains(name, ".") {
		paths--
	}
	for i := 0; i < paths; i++ {
		parent := fileName[i]
		if i > 0 {
			parent = fileName[i-1]
		}
		nodes, dir, err = o.OpenDir(fileName[i], nodes)
		dir.(*ObjectDir).inode.parent = parent
		if err != nil {
			return nil, err
		}
	}
	if !strings.Contains(name, ".") {
		return dir, nil
	}
	file = dir.(*ObjectDir).OpenFile(o, fileName[paths])
	return file, nil
}

func (o *ObjectFS) ReadDir(dir string) ([]common.FileInfo, error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	name := strings.Split(dir, "/")
	entry := o.nodes[name[0]]
	if entry == nil {
		return nil, os.ErrNotExist
	}
	if len(name) > 1 {
		entry = entry.(*ObjectDir).nodes[name[1]]
	}
	return entry.(*ObjectDir).LookUp()
}

func (o *ObjectFS) Remove(name string) error {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	fileName := strings.Split(name, "/")
	dir := o.nodes[fileName[0]]
	if dir == nil {
		return os.ErrNotExist
	}
	return dir.(*ObjectDir).Remove(fileName[1])
}

func (o *ObjectFS) RemoveAll(dir string) error {
	return nil
}

func (o *ObjectFS) MountInfo() *tfs.MountInfo {
	return nil
}

func (o *ObjectFS) GetData(size uint64, file *ObjectFile) (object *Object, err error) {
	o.RWMutex.Lock()
	defer o.RWMutex.Unlock()
	objectName := file.parent.inode.name
	object = o.data[objectName]
	if object == nil {
		object, err = OpenObject(objectName, DataType, o.attr.dir)
		object.Mount(ObjectSize, PageSize)
		o.data[objectName] = object
		return
	}
	return
}

func (o *ObjectFS) GetDataWithId(id string) *Object {
	return o.data[id]
}

func (o *ObjectFS) Append(file *ObjectFile, data []byte) (n int, err error) {
	dataObject, err := o.GetData(uint64(len(data)), file)
	if err != nil {
		return
	}
	buf := data
	if o.attr.algo == compress.Lz4 {
		buf = make([]byte, lz4.CompressBlockBound(len(buf)))
		if buf, err = compress.Compress(data, buf, compress.Lz4); err != nil {
			return
		}
	}
	offset, allocated := dataObject.allocator.Allocate(uint64(len(buf)))
	_, err = dataObject.Append(buf, int64(offset))
	if err != nil {
		return int(allocated), err
	}
	file.inode.mutex.Lock()
	file.inode.extents = append(file.inode.extents, Extent{
		typ:    APPEND,
		oid:    dataObject.name,
		offset: uint32(offset),
		length: uint32(allocated),
		data:   entry{offset: 0, length: uint32(len(buf))},
	})
	file.inode.size += uint64(len(buf))
	file.inode.dataSize += uint64(len(data))
	file.inode.objectId = dataObject.name
	file.inode.seq++
	file.inode.mutex.Unlock()
	err = o.driver.Append(file)
	return int(allocated), err
}

func (o *ObjectFS) Read(inode *Inode, data []byte) (n int, err error) {
	bufLen := len(data)
	if bufLen == 0 {
		return 0, nil
	}
	dataObject := o.GetDataWithId(inode.parent)
	return dataObject.oFile.ReadAt(data, int64(inode.extents[0].offset))
}

func (o *ObjectFS) Sync(file *ObjectFile) error {
	data := o.GetDataWithId(file.inode.objectId)
	if data == nil {
		return nil
	}
	return data.oFile.Sync()
}

func (o *ObjectFS) GetLastId() {

}

func (o *ObjectFS) Delete(file tfs.File) error {
	return nil
}

func (o *ObjectFS) RebuildObject() error {
	files, err := os.ReadDir(o.attr.dir)
	if err != nil {
		return err
	}
	for _, info := range files {
		file, err := info.Info()
		if err != nil {
			return err
		}
		id, oType, err := decodeName(file.Name())
		if err != nil {
			return err
		}
		/*if id > o.lastId {
			o.lastId = id
		}*/
		if oType == DataType {
			object, err := OpenObject(id, DataType, o.attr.dir)
			if err != nil {
				return err
			}
			object.Mount(ObjectSize, PageSize)
			object.allocator.available = p2roundup(uint64(file.Size()), PageSize)
			o.data[object.name] = object
		} else if oType == NodeType {
			object, err := OpenObject(id, NodeType, o.attr.dir)
			if err != nil {
				return err
			}
			object.Mount(ObjectSize, MetaSize)
			object.allocator.available = p2roundup(uint64(file.Size()), MetaSize)
			o.driver.inode = append(o.driver.inode, object)
		} else if oType == MetadataBlkType {
			object, err := OpenObject(id, MetadataBlkType, o.attr.dir)
			if err != nil {
				return err
			}
			object.Mount(ObjectSize, MetaSize)
			object.allocator.available = p2roundup(uint64(file.Size()), MetaSize)
			o.driver.blk = append(o.driver.blk, object)
		}
	}
	//Sort(o.data)
	//Sort(o.driver.inode)
	//Sort(o.driver.blk)
	return nil
}

type ObjectList []*Object

func (s ObjectList) Len() int           { return len(s) }
func (s ObjectList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ObjectList) Less(i, j int) bool { return s[i].name < s[j].name }

func Sort(data []*Object) {
	gsort.Sort(ObjectList(data))
}
