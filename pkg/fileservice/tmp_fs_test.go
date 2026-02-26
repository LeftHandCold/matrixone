// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// errorListFS wraps a FileService and makes List always return an error.
type errorListFS struct {
	FileService
	listErr error
}

func (f *errorListFS) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		// yield nil entry with error, same as LocalETLFS does on failure
		yield(nil, f.listErr)
	}
}

func TestTmpFileServiceGCWithListError(t *testing.T) {
	dir := t.TempDir()
	etlfs, err := NewLocalETLFS("test", dir)
	assert.Nil(t, err)

	service := &TmpFileService{
		FileService: &errorListFS{
			FileService: etlfs,
			listErr:     fmt.Errorf("simulated list error"),
		},
		gcInterval: time.Hour,
		apps:       make(map[string]*AppFS),
		appsMu:     sync.RWMutex{},
		wg:         sync.WaitGroup{},
	}

	// Register an app so gc actually iterates
	_, err = service.GetOrCreateApp(&AppConfig{
		Name: "test-app",
		GCFn: func(filePath string, fs FileService) (bool, error) {
			return false, nil
		},
	})
	assert.Nil(t, err)

	// This must not panic
	assert.NotPanics(t, func() {
		service.gc(context.Background())
	})
}

func TestTmpFileServiceGCWithCancelledContext(t *testing.T) {
	dir := t.TempDir()
	etlfs, err := NewLocalETLFS("test", dir)
	assert.Nil(t, err)

	service := &TmpFileService{
		FileService: etlfs,
		gcInterval:  time.Hour,
		apps:        make(map[string]*AppFS),
		appsMu:      sync.RWMutex{},
		wg:          sync.WaitGroup{},
	}

	_, err = service.GetOrCreateApp(&AppConfig{
		Name: "test-app",
		GCFn: func(filePath string, fs FileService) (bool, error) {
			return false, nil
		},
	})
	assert.Nil(t, err)

	// Cancel context before gc runs — LocalETLFS.List will yield (nil, ctx.Err())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NotPanics(t, func() {
		service.gc(ctx)
	})
}
