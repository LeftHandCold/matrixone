// Copyright 2025 Matrix Origin
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

package backup

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestParallelCopyFlatFiles(t *testing.T) {
	defer testutils.AfterTest(t)()

	ctx := context.Background()
	srcFS, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dstFS, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	writeFile := func(path string, data []byte) {
		require.NoError(t, srcFS.Write(ctx, fileservice.IOVector{
			FilePath: path,
			Entries: []fileservice.IOEntry{{
				ReaderForWrite: bytes.NewReader(data),
				Size:           int64(len(data)),
			}},
		}))
	}

	writeFile("obj.fts.__idx_body", []byte("sidecar"))
	writeFile("obj.fts.locator", []byte("locator"))

	files := map[string]*logtail.BackupFlatFile{
		"obj.fts.__idx_body": {
			Path:     "obj.fts.__idx_body",
			CreateTS: types.BuildTS(10, 0),
			NeedCopy: true,
		},
		"obj.fts.locator": {
			Path:     "obj.fts.locator",
			CreateTS: types.BuildTS(10, 0),
			NeedCopy: true,
		},
	}

	taeFiles, err := parallelCopyFlatFiles(srcFS, dstFS, files, 2)
	require.NoError(t, err)
	require.Len(t, taeFiles, 2)

	dstEntries, err := fileservice.SortedList(dstFS.List(ctx, ""))
	require.NoError(t, err)
	require.Len(t, dstEntries, 2)

	got := make(map[string]bool, len(taeFiles))
	for _, file := range taeFiles {
		got[file.path] = file.needCopy
	}
	require.Equal(t, map[string]bool{
		"obj.fts.__idx_body": true,
		"obj.fts.locator":    true,
	}, got)
}

func TestCopyFTSAuxFilesWithLocators(t *testing.T) {
	defer testutils.AfterTest(t)()

	ctx := context.Background()
	srcFS, err := fileservice.NewMemoryFS("src", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	dstFS, err := fileservice.NewMemoryFS("dst", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	writeFile := func(path string, data []byte) {
		require.NoError(t, srcFS.Write(ctx, fileservice.IOVector{
			FilePath: path,
			Entries: []fileservice.IOEntry{{
				ReaderForWrite: bytes.NewReader(data),
				Size:           int64(len(data)),
			}},
		}))
	}

	writeFile("obj", []byte("base"))
	sidecarPath := ftnative.SidecarPath("obj", "__idx_body")
	writeFile(sidecarPath, []byte("sidecar"))
	require.NoError(t, ftnative.WriteSidecarLocator(
		ctx,
		srcFS,
		"obj",
		[]ftnative.SidecarLocatorEntry{{
			IndexTable: "__idx_body",
			FilePath:   sidecarPath,
		}},
	))

	taeFiles, err := copyFTSAuxFilesWithLocators(
		ctx,
		srcFS,
		dstFS,
		"obj",
		types.BuildTS(20, 0),
		make(map[string]struct{}),
	)
	require.NoError(t, err)
	require.Len(t, taeFiles, 2)

	dstEntries, err := fileservice.SortedList(dstFS.List(ctx, ""))
	require.NoError(t, err)
	require.Len(t, dstEntries, 2)
	got := make(map[string]struct{}, len(taeFiles))
	for _, file := range taeFiles {
		got[file.path] = struct{}{}
	}
	require.Contains(t, got, ftnative.SidecarLocatorPath("obj"))
	require.Contains(t, got, sidecarPath)
}
