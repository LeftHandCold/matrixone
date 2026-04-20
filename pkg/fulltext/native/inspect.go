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

package native

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type SidecarFileInfo struct {
	Path           string
	SegmentVersion uint32
	DocCount       int64
	TokenSum       int64
}

func InspectSidecarFile(
	ctx context.Context,
	fs fileservice.FileService,
	filePath string,
) (SidecarFileInfo, bool, error) {
	info := SidecarFileInfo{Path: filePath}
	prefix, exists, err := readSidecarRange(ctx, fs, filePath, 0, int64(segmentPrefixLen))
	if err != nil || !exists {
		return info, exists, err
	}
	magic, headerLen, err := parseSegmentPrefix(prefix)
	if err != nil {
		return info, true, err
	}

	switch magic {
	case segmentMagicV4:
		headerBytes, exists, err := readSidecarRange(ctx, fs, filePath, int64(segmentPrefixLen), int64(headerLen))
		if err != nil {
			return info, true, err
		}
		if !exists {
			return info, false, moerr.NewFileNotFoundNoCtx(filePath)
		}
		segment := &Segment{}
		header, err := readSegmentHeaderV4(headerBytes, segment)
		if err != nil {
			return info, true, err
		}
		info.SegmentVersion = 4
		info.DocCount = header.DocCount
		info.TokenSum = header.TokenSum
		return info, true, nil
	default:
		vec := &fileservice.IOVector{
			FilePath: filePath,
			Entries: []fileservice.IOEntry{{
				Offset: 0,
				Size:   -1,
			}},
		}
		if err := fs.Read(ctx, vec); err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				return info, false, nil
			}
			return info, true, err
		}
		segment, err := UnmarshalBinary(vec.Entries[0].Data)
		if err != nil {
			return info, true, err
		}
		info.SegmentVersion = segmentVersionFromMagic(magic)
		info.DocCount = segment.DocCount
		info.TokenSum = segment.TokenSum
		return info, true, nil
	}
}

func segmentVersionFromMagic(magic [8]byte) uint32 {
	switch magic {
	case segmentMagicV4:
		return 4
	case segmentMagicV3:
		return 3
	case segmentMagicV2:
		return 2
	case segmentMagicV1:
		return 1
	default:
		return 0
	}
}
