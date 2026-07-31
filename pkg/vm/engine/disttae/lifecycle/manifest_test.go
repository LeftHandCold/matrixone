// Copyright 2026 Matrix Origin
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

package lifecycle

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArchiveDatasetHashV1ByteContract(t *testing.T) {
	schemaDigest := [sha256.Size]byte{1, 2, 3, 4}
	chunks := []ArchiveChunk{
		{
			ChunkOrdinal:         0,
			RowCount:             17,
			LogicalBytes:         101,
			CanonicalContentHash: [sha256.Size]byte{0xaa},
		},
		{
			ChunkOrdinal:         1,
			RowCount:             23,
			LogicalBytes:         202,
			CanonicalContentHash: [sha256.Size]byte{0xbb},
		},
	}

	sum := sha256.New()
	_, _ = sum.Write([]byte("matrixone/lifecycle/archive-dataset/v1"))
	writeManifestTestUint16(sum, 1)
	_, _ = sum.Write(schemaDigest[:])
	writeManifestTestUint64(sum, 2)
	for _, chunk := range chunks {
		writeManifestTestUint64(sum, chunk.ChunkOrdinal)
		writeManifestTestUint64(sum, chunk.RowCount)
		writeManifestTestUint64(sum, chunk.LogicalBytes)
		_, _ = sum.Write(chunk.CanonicalContentHash[:])
	}
	var expected [sha256.Size]byte
	copy(expected[:], sum.Sum(nil))

	require.Equal(t, expected, computeArchiveDatasetHash(schemaDigest, chunks))
}

func TestArchiveManifestRejectsDatasetChunkCountAboveCertifiedLimit(t *testing.T) {
	files := make([]ArchiveFile, maxArchiveChunksPerDataset+1)
	for index := range files {
		files[index] = ArchiveFile{
			FileOrdinal: uint32(index),
			Chunks: []ArchiveChunk{{
				ChunkOrdinal:    uint64(index),
				FileOrdinal:     uint32(index),
				RowGroupOrdinal: 0,
			}},
		}
	}
	err := validateArchiveManifestShape(&ArchiveManifest{
		ManifestFormatVersion: archiveManifestFormatVersion,
		HashFormulaVersion:    archiveHashFormulaVersion,
		CanonicalEncoder:      canonicalEncoderVersion,
		TotalChunkCount:       uint64(len(files)),
		Files:                 files,
	})
	require.ErrorContains(t, err, "certified chunk limit")
}

type manifestTestWriter interface {
	Write([]byte) (int, error)
}

func writeManifestTestUint16(writer manifestTestWriter, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeManifestTestUint64(writer manifestTestWriter, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}
