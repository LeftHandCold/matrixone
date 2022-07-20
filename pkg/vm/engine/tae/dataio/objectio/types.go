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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/tfs"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Extension int16

const (
	ColumnBlockExt Extension = iota + 1000
	ComposedUpdatesExt
	ColumnUpdatesExt
	DeletesExt
	IndexExt
	MetaIndexExt
)

var ExtName map[Extension]string = map[Extension]string{
	ColumnBlockExt:     "cblk",
	ComposedUpdatesExt: "cus",
	ColumnUpdatesExt:   "cu",
	DeletesExt:         "del",
	IndexExt:           "idx",
	MetaIndexExt:       "midx",
}

func ExtensionName(ext Extension) (name string) {
	name, found := ExtName[ext]
	if !found {
		panic(any(fmt.Sprintf("Unknown ext: %d", ext)))
	}
	return
}

func DecodeName(name string) []string {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "-")
	return info
}

func EncodeDir(id *common.ID) (dir string) {
	segDir := fmt.Sprintf("%d", id.SegmentID)
	blkDir := fmt.Sprintf("%d-%d", id.SegmentID, id.BlockID)
	name := filepath.Join(segDir, blkDir)
	return name
}

func EncodeColBlkNameWithVersion(id *common.ID, version uint64, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d-%d.%s", id.Idx, version, ExtensionName(ColumnBlockExt))
	name = filepath.Join(dir, basename)
	return
}

func DecodeColBlkOrUpdateName(name string) (id *common.ID, version uint64, err error) {
	info := DecodeName(name)
	idx, err := strconv.ParseUint(info[0], 10, 32)
	if err != nil {
		return
	}
	id = &common.ID{Idx: uint16(idx)}
	version, err = strconv.ParseUint(info[1], 10, 64)
	if err != nil {
		return
	}
	return
}

func EncodeIndexName(id *common.ID, idx int, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d-%d.%s", id.Idx, idx, ExtensionName(IndexExt))
	name = filepath.Join(dir, basename)
	return
}

func DecodeIndexName(name string) (id *common.ID, idx uint64, err error) {
	info := DecodeName(name)
	col, err := strconv.ParseUint(info[0], 10, 32)
	if err != nil {
		return
	}
	id = &common.ID{Idx: uint16(col)}
	idx, err = strconv.ParseUint(info[1], 10, 32)
	if err != nil {
		return
	}
	return
}

func EncodeMetaIndexName(id *common.ID, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d.%s", id.BlockID, ExtensionName(MetaIndexExt))
	name = filepath.Join(dir, basename)
	return
}

func DecodeMetaIndexName(name string) (id *common.ID, err error) {
	info := DecodeName(name)
	blockID, err := strconv.ParseUint(info[0], 10, 32)
	if err != nil {
		return
	}
	id = &common.ID{BlockID: blockID}
	return
}

func EncodeDeleteNameWithVersion(id *common.ID, version uint64, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d-%d.%s", id.BlockID, version, ExtensionName(DeletesExt))
	name = filepath.Join(dir, basename)
	return
}

func DecodeDeleteName(name string) (id *common.ID, version uint64, err error) {
	info := DecodeName(name)
	blockID, err := strconv.ParseUint(info[0], 10, 32)
	if err != nil {
		return
	}
	id = &common.ID{BlockID: blockID}
	version, err = strconv.ParseUint(info[1], 10, 32)
	if err != nil {
		return
	}
	return
}

func EncodeUpdateNameWithVersion(id *common.ID, version uint64, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d-%d.%s", id.Idx, version, ExtensionName(ColumnUpdatesExt))
	name = filepath.Join(dir, basename)
	return
}

// func EncodeComposedColumnFileName(id *common.ID, attrs []int, fs file.FS) (name string) {
// 	dir := fs.EncodeDir(id)
// 	name = fs.Join(dir, common.IntsToStr(attrs, "-"))
// 	return
// }
