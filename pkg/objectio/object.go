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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"go.uber.org/zap/buffer"
	"os"
	"path"
)

const Magic = 0xFFFFFFFF
const Version = 1
const FSName = "local"

type Object struct {
	// name is the object file's name
	name string
	// fs is an instance of fileservice
	fs fileservice.FileService
}

func NewObject(name string, fs fileservice.FileService) *Object {
	object := &Object{
		name: name,
		fs:   fs,
	}
	return object
}

func (o *Object) Write(vector fileservice.IOVector) error {
	dir := "./mo-data/s3"
	name := path.Join(dir, vector.FilePath)
	file, err := os.Create(name)
	if err != nil {
		return err
	}
	var buf buffer.Buffer
	for _, entry := range vector.Entries {
		buf.Write(entry.Data)
	}
	_, err = file.WriteAt(buf.Bytes(), 0)
	return err
}

func ObjectRead(vector *fileservice.IOVector) error {
	dir := "./mo-data/s3"
	name := path.Join(dir, vector.FilePath)
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	for i := range vector.Entries {
		vector.Entries[i].Data = make([]byte, vector.Entries[i].Size)
		_, err = file.ReadAt(vector.Entries[i].Data, vector.Entries[i].Offset)
		if err != nil {
			return err
		}
		obj, size, err := vector.Entries[i].ToObject(bytes.NewReader(vector.Entries[i].Data), vector.Entries[i].Data)
		if err != nil {
			return err
		}
		vector.Entries[i].Object = obj
		vector.Entries[i].ObjectSize = size
	}
	return err
}
