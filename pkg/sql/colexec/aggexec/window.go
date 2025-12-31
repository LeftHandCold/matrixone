// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	io "io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func SingleWindowReturnType(_ []types.Type) types.Type {
	return types.T_int64.ToType()
}

type i64Slice []int64

func (s i64Slice) MarshalBinary() ([]byte, error) {
	return types.EncodeSlice[int64](s), nil
}

// special structure for a single column window function.
type singleWindowExec struct {
	singleAggInfo
	ret aggResultWithFixedType[int64]

	// groups [][]int64
	groups []i64Slice
}

func makeRankDenseRankRowNumber(mp *mpool.MPool, info singleAggInfo) AggFuncExec {
	return &singleWindowExec{
		singleAggInfo: info,
		ret:           initAggResultWithFixedTypeResult[int64](mp, info.retType, info.emptyNull, 0, false),
	}
}

func (exec *singleWindowExec) GroupGrow(more int) error {
	exec.groups = append(exec.groups, make([]i64Slice, more)...)
	return exec.ret.grows(more)
}

func (exec *singleWindowExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *singleWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	value := vector.MustFixedColWithTypeCheck[int64](vectors[0])[row]
	exec.groups[groupIndex] = append(exec.groups[groupIndex], value)
	return nil
}

func (exec *singleWindowExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *singleWindowExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, dist, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}
	if dist != nil {
		return nil, moerr.NewInternalErrorNoCtx("dist should have been nil")
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			encoded.Groups[i] = types.EncodeSlice[int64](exec.groups[i])
		}
	}
	return encoded.Marshal()
}

func (exec *singleWindowExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return marshalRetAndGroupsToBuffer(
		cnt, flags, buf,
		&exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *singleWindowExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	return marshalChunkToBuffer(
		chunk, buf,
		&exec.ret.optSplitResult, exec.groups, nil)
}

func (exec *singleWindowExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	err := unmarshalFromReaderNoGroup(reader, &exec.ret.optSplitResult)
	if err != nil {
		return err
	}
	exec.ret.setupT()

	ngrp, err := types.ReadInt64(reader)
	if err != nil {
		return err
	}
	if ngrp != 0 {
		exec.groups = make([]i64Slice, ngrp)
		for i := range exec.groups {
			_, bs, err := types.ReadSizeBytes(reader)
			if err != nil {
				return err
			}
			exec.groups[i] = types.DecodeSlice[int64](bs)
		}
	}
	return nil
}

func (exec *singleWindowExec) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	if len(exec.groups) > 0 {
		exec.groups = make([]i64Slice, len(groups))
		for i := range exec.groups {
			if len(groups[i]) > 0 {
				exec.groups[i] = types.DecodeSlice[int64](groups[i])
			}
		}
	}
	// group used by above,
	return exec.ret.unmarshalFromBytes(result, empties, nil)
}

func (exec *singleWindowExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *singleWindowExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *singleWindowExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*singleWindowExec)
	exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
	return nil
}

func (exec *singleWindowExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*singleWindowExec)
	for i := range groups {
		if groups[i] != GroupNotMatched {
			groupIdx1 := int(groups[i] - 1)
			groupIdx2 := i + offset

			exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
		}
	}
	return nil
}

func (exec *singleWindowExec) SetExtraInformation(partialResult any, groupIndex int) error {
	panic("window function do not support the extra information")
}

func (exec *singleWindowExec) Flush() ([]*vector.Vector, error) {
	switch exec.singleAggInfo.aggID {
	case WinIdOfRank:
		return exec.flushRank()
	case WinIdOfDenseRank:
		return exec.flushDenseRank()
	case WinIdOfRowNumber:
		return exec.flushRowNumber()
	}
	return nil, moerr.NewInternalErrorNoCtx("invalid window function")
}

func (exec *singleWindowExec) Free() {
	exec.ret.free()
}

func (exec *singleWindowExec) Size() int64 {
	var size int64
	size += exec.ret.Size()
	for _, group := range exec.groups {
		size += int64(cap(group)) * int64(types.T_int64.ToType().TypeSize())
	}
	// 24 is the size of a slice header.
	size += int64(cap(exec.groups)) * 24
	return size
}

func (exec *singleWindowExec) flushRank() ([]*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		sn := int64(1)
		for i := 1; i < len(group); i++ {
			m := int(group[i] - group[i-1])

			for k := idx + m; idx < k; idx++ {
				x, y := exec.ret.updateNextAccessIdx(idx)

				values[x][y] = sn
			}
			sn += int64(m)
		}
	}
	return exec.ret.flushAll(), nil
}

func (exec *singleWindowExec) flushDenseRank() ([]*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		sn := int64(1)
		for i := 1; i < len(group); i++ {
			m := int(group[i] - group[i-1])

			for k := idx + m; idx < k; idx++ {
				x, y := exec.ret.updateNextAccessIdx(idx)

				values[x][y] = sn
			}
			sn++
		}
	}
	return exec.ret.flushAll(), nil
}

func (exec *singleWindowExec) flushRowNumber() ([]*vector.Vector, error) {
	values := exec.ret.values

	idx := 0
	for _, group := range exec.groups {
		if len(group) == 0 {
			continue
		}

		n := group[len(group)-1] - group[0]
		for j := int64(1); j <= n; j++ {
			x, y := exec.ret.updateNextAccessIdx(idx)

			values[x][y] = j
			idx++
		}
	}
	return exec.ret.flushAll(), nil
}

// LagReturnType returns the type of the first argument (the expression to lag)
func LagReturnType(args []types.Type) types.Type {
	if len(args) > 0 {
		return args[0]
	}
	return types.T_any.ToType()
}

// lagWindowExec is the executor for LAG window function
// LAG(expr [, offset [, default]]) OVER (...)
type lagWindowExec struct {
	singleAggInfo
	ret aggResultWithBytesType

	// store the values for each group
	// each group contains the actual values from the input
	groups [][]lagValue
}

type lagValue struct {
	isNull bool
	data   []byte
}

func makeLagExec(mp *mpool.MPool, aggID int64, isDistinct bool, params ...types.Type) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewInternalErrorNoCtx("window function does not support `distinct`")
	}

	retType := types.T_any.ToType()
	if len(params) > 0 {
		retType = params[0]
	}

	info := singleAggInfo{
		aggID:     aggID,
		distinct:  false,
		argType:   retType,
		retType:   retType,
		emptyNull: true,
	}
	return newLagWindowExec(mp, info), nil
}

func newLagWindowExec(mp *mpool.MPool, info singleAggInfo) *lagWindowExec {
	return &lagWindowExec{
		singleAggInfo: info,
		ret:           initAggResultWithBytesTypeResult(mp, info.retType, info.emptyNull, "", false),
	}
}

func (exec *lagWindowExec) AggID() int64 {
	return exec.singleAggInfo.aggID
}

func (exec *lagWindowExec) IsDistinct() bool {
	return exec.singleAggInfo.distinct
}

func (exec *lagWindowExec) TypesInfo() ([]types.Type, types.Type) {
	return []types.Type{exec.singleAggInfo.argType}, exec.singleAggInfo.retType
}

func (exec *lagWindowExec) GroupGrow(more int) error {
	exec.groups = append(exec.groups, make([][]lagValue, more)...)
	return exec.ret.grows(more)
}

func (exec *lagWindowExec) PreAllocateGroups(more int) error {
	return exec.ret.preExtend(more)
}

func (exec *lagWindowExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if len(vectors) == 0 {
		return nil
	}

	vec := vectors[0]
	isNull := vec.IsNull(uint64(row))

	var data []byte
	if !isNull {
		data = vec.GetRawBytesAt(row)
		// Make a copy of the data
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		data = dataCopy
	}

	exec.groups[groupIndex] = append(exec.groups[groupIndex], lagValue{
		isNull: isNull,
		data:   data,
	})
	return nil
}

func (exec *lagWindowExec) GetOptResult() SplitResult {
	return &exec.ret.optSplitResult
}

func (exec *lagWindowExec) marshal() ([]byte, error) {
	d := exec.singleAggInfo.getEncoded()
	r, em, _, err := exec.ret.marshalToBytes()
	if err != nil {
		return nil, err
	}

	encoded := EncodedAgg{
		Info:    d,
		Result:  r,
		Empties: em,
		Groups:  nil,
	}
	if len(exec.groups) > 0 {
		encoded.Groups = make([][]byte, len(exec.groups))
		for i := range encoded.Groups {
			// Encode each group's values
			var buf bytes.Buffer
			groupLen := int32(len(exec.groups[i]))
			buf.Write(types.EncodeInt32(&groupLen))
			for _, v := range exec.groups[i] {
				buf.Write(types.EncodeBool(&v.isNull))
				if !v.isNull {
					dataLen := int32(len(v.data))
					buf.Write(types.EncodeInt32(&dataLen))
					buf.Write(v.data)
				}
			}
			encoded.Groups[i] = buf.Bytes()
		}
	}
	return encoded.Marshal()
}

func (exec *lagWindowExec) SaveIntermediateResult(cnt int64, flags [][]uint8, buf *bytes.Buffer) error {
	return moerr.NewInternalErrorNoCtx("lag window function does not support intermediate result")
}

func (exec *lagWindowExec) SaveIntermediateResultOfChunk(chunk int, buf *bytes.Buffer) error {
	return moerr.NewInternalErrorNoCtx("lag window function does not support intermediate result")
}

func (exec *lagWindowExec) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	return moerr.NewInternalErrorNoCtx("lag window function does not support unmarshal")
}

func (exec *lagWindowExec) unmarshal(mp *mpool.MPool, result, empties, groups [][]byte) error {
	return exec.ret.unmarshalFromBytes(result, empties, nil)
}

func (exec *lagWindowExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *lagWindowExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	panic("implement me")
}

func (exec *lagWindowExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*lagWindowExec)
	exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
	return nil
}

func (exec *lagWindowExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*lagWindowExec)
	for i := range groups {
		if groups[i] != GroupNotMatched {
			groupIdx1 := int(groups[i] - 1)
			groupIdx2 := i + offset
			exec.groups[groupIdx1] = append(exec.groups[groupIdx1], other.groups[groupIdx2]...)
		}
	}
	return nil
}

func (exec *lagWindowExec) SetExtraInformation(partialResult any, groupIndex int) error {
	panic("window function do not support the extra information")
}

func (exec *lagWindowExec) Flush() ([]*vector.Vector, error) {
	return exec.flushLag()
}

func (exec *lagWindowExec) Free() {
	exec.ret.free()
}

func (exec *lagWindowExec) Size() int64 {
	var size int64
	size += exec.ret.Size()
	for _, group := range exec.groups {
		for _, v := range group {
			size += int64(len(v.data)) + 1 // 1 for isNull bool
		}
		size += int64(cap(group)) * 24 // approximate size of lagValue struct
	}
	size += int64(cap(exec.groups)) * 24
	return size
}

func (exec *lagWindowExec) flushLag() ([]*vector.Vector, error) {
	// LAG returns the value from a row that is offset rows before the current row
	// Default offset is 1
	offset := 1

	totalRows := 0
	for _, group := range exec.groups {
		totalRows += len(group)
	}

	resultVec := vector.NewVec(exec.retType)
	mp := exec.ret.mp

	for _, group := range exec.groups {
		for i := range group {
			// LAG looks back 'offset' rows
			lagIdx := i - offset
			if lagIdx < 0 {
				// No previous row, return NULL
				if err := vector.AppendBytes(resultVec, nil, true, mp); err != nil {
					resultVec.Free(mp)
					return nil, err
				}
			} else {
				lagVal := group[lagIdx]
				if lagVal.isNull {
					if err := vector.AppendBytes(resultVec, nil, true, mp); err != nil {
						resultVec.Free(mp)
						return nil, err
					}
				} else {
					if err := vector.AppendBytes(resultVec, lagVal.data, false, mp); err != nil {
						resultVec.Free(mp)
						return nil, err
					}
				}
			}
		}
	}

	return []*vector.Vector{resultVec}, nil
}
