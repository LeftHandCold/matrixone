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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
)

const (
	archiveManifestFormatVersion uint16 = 1
	archiveHashFormulaVersion    uint16 = 1
	// Phase 1 writes exactly one Row Group per payload file, so this one
	// certified bound caps payload files, Restore Chunk Receipts, Manifest
	// collection growth, and Restore aggregation memory at the same time.
	maxArchiveChunksPerDataset = 4096
)

var archiveDatasetHashDomain = []byte("matrixone/lifecycle/archive-dataset/v1")

type ArchiveManifest struct {
	ManifestFormatVersion uint16             `json:"manifest_format_version"`
	HashFormulaVersion    uint16             `json:"hash_formula_version"`
	CanonicalEncoder      uint16             `json:"canonical_encoder_version"`
	RootID                string             `json:"root_id"`
	AttemptID             string             `json:"attempt_id"`
	Schema                SchemaDescriptor   `json:"schema"`
	SchemaDigest          [32]byte           `json:"schema_digest"`
	ContentHash           [32]byte           `json:"content_hash"`
	RowCount              uint64             `json:"row_count"`
	LogicalBytes          uint64             `json:"logical_bytes"`
	TotalChunkCount       uint64             `json:"total_chunk_count"`
	Files                 []ArchiveFile      `json:"files"`
	AutoIncrementMaxima   []AutoIncrementMax `json:"auto_increment_maxima,omitempty"`
	VerificationStatus    string             `json:"verification_status"`
}

type ArchiveFile struct {
	FileOrdinal uint32         `json:"file_ordinal"`
	Key         string         `json:"key"`
	Size        uint64         `json:"size"`
	SHA256      [32]byte       `json:"sha256"`
	Chunks      []ArchiveChunk `json:"chunks"`
}

type ArchiveChunk struct {
	ChunkOrdinal         uint64   `json:"chunk_ordinal"`
	FileOrdinal          uint32   `json:"file_ordinal"`
	RowGroupOrdinal      uint32   `json:"row_group_ordinal"`
	RowCount             uint64   `json:"row_count"`
	LogicalBytes         uint64   `json:"logical_bytes"`
	CanonicalContentHash [32]byte `json:"canonical_content_hash"`
}

type AutoIncrementMax struct {
	ColumnOrdinal uint32 `json:"column_ordinal"`
	Value         string `json:"value"`
}

func MarshalArchiveManifest(manifest *ArchiveManifest) ([]byte, [32]byte, error) {
	if err := validateArchiveManifestShape(manifest); err != nil {
		return nil, [32]byte{}, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, [32]byte{}, err
	}
	return encoded, sha256.Sum256(encoded), nil
}

func ParseArchiveManifest(encoded []byte) (*ArchiveManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest ArchiveManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	if err := validateArchiveManifestShape(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func validateArchiveManifestShape(manifest *ArchiveManifest) error {
	if manifest == nil {
		return fmt.Errorf("nil Lifecycle archive manifest")
	}
	if manifest.ManifestFormatVersion != archiveManifestFormatVersion {
		return fmt.Errorf(
			"unsupported Lifecycle manifest version %d",
			manifest.ManifestFormatVersion,
		)
	}
	if manifest.HashFormulaVersion != archiveHashFormulaVersion {
		return fmt.Errorf(
			"unsupported Lifecycle archive hash formula %d",
			manifest.HashFormulaVersion,
		)
	}
	if manifest.CanonicalEncoder != canonicalEncoderVersion {
		return fmt.Errorf(
			"unsupported Lifecycle canonical encoder %d",
			manifest.CanonicalEncoder,
		)
	}
	if len(manifest.Files) > maxArchiveChunksPerDataset {
		return fmt.Errorf(
			"Lifecycle manifest exceeds the certified chunk limit %d",
			maxArchiveChunksPerDataset,
		)
	}
	if manifest.TotalChunkCount != uint64(len(manifest.Files)) {
		return fmt.Errorf(
			"Lifecycle manifest chunk count %d does not match file count %d",
			manifest.TotalChunkCount,
			len(manifest.Files),
		)
	}
	var rows uint64
	var logicalBytes uint64
	for fileIndex, file := range manifest.Files {
		if file.FileOrdinal != uint32(fileIndex) {
			return fmt.Errorf("Lifecycle archive file ordinals are not continuous")
		}
		if len(file.Chunks) != 1 {
			return fmt.Errorf("Lifecycle Phase 1 requires exactly one row group per payload file")
		}
		chunk := file.Chunks[0]
		if chunk.ChunkOrdinal != uint64(fileIndex) ||
			chunk.FileOrdinal != uint32(fileIndex) ||
			chunk.RowGroupOrdinal != 0 {
			return fmt.Errorf("Lifecycle archive chunk ordinals are not canonical")
		}
		rows += chunk.RowCount
		logicalBytes += chunk.LogicalBytes
	}
	if rows != manifest.RowCount || logicalBytes != manifest.LogicalBytes {
		return fmt.Errorf("Lifecycle archive manifest totals do not match chunks")
	}
	var previous uint32
	for index, maximum := range manifest.AutoIncrementMaxima {
		if int(maximum.ColumnOrdinal) >= len(manifest.Schema.Columns) ||
			!manifest.Schema.Columns[maximum.ColumnOrdinal].AutoIncrement {
			return fmt.Errorf("Lifecycle archive auto-increment ordinal is invalid")
		}
		if index > 0 && maximum.ColumnOrdinal <= previous {
			return fmt.Errorf("Lifecycle archive auto-increment ordinals are not canonical")
		}
		if _, ok := new(big.Int).SetString(maximum.Value, 10); !ok {
			return fmt.Errorf("Lifecycle archive auto-increment maximum is invalid")
		}
		previous = maximum.ColumnOrdinal
	}
	return nil
}

func computeArchiveDatasetHash(
	schemaDigest [32]byte,
	chunks []ArchiveChunk,
) [32]byte {
	sum := sha256.New()
	_, _ = sum.Write(archiveDatasetHashDomain)
	var number [8]byte
	binary.BigEndian.PutUint16(number[:2], archiveHashFormulaVersion)
	_, _ = sum.Write(number[:2])
	_, _ = sum.Write(schemaDigest[:])
	binary.BigEndian.PutUint64(number[:], uint64(len(chunks)))
	_, _ = sum.Write(number[:])
	for _, chunk := range chunks {
		binary.BigEndian.PutUint64(number[:], chunk.ChunkOrdinal)
		_, _ = sum.Write(number[:])
		binary.BigEndian.PutUint64(number[:], chunk.RowCount)
		_, _ = sum.Write(number[:])
		binary.BigEndian.PutUint64(number[:], chunk.LogicalBytes)
		_, _ = sum.Write(number[:])
		_, _ = sum.Write(chunk.CanonicalContentHash[:])
	}
	var result [32]byte
	copy(result[:], sum.Sum(nil))
	return result
}

func archiveManifestKey(prefix string, digest [32]byte) string {
	return fmt.Sprintf("%s/manifest-%s.json", prefix, hex.EncodeToString(digest[:]))
}
