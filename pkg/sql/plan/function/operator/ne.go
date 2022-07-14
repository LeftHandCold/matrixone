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

package operator

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var retType = types.T_bool.ToType()

func allocateBoolVector(length int64, proc *process.Process) (*vector.Vector, error) {
	vec, err := proc.AllocVector(retType, length)
	if err != nil {
		return nil, err
	}
	vec.Col = encoding.DecodeBoolSlice(vec.Data)
	vec.Col = vec.Col.([]bool)[:length]
	return vec, nil
}

func FillNullPos(vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		rows := vec.Nsp.Np.ToArray()
		cols := vec.Col.([]bool)
		for _, row := range rows {
			cols[row] = false
		}
	}
}

func ScalarNeNotScalar[T NormalType](sv, nsv *vector.Vector, col1, col2 []T, proc *process.Process) (*vector.Vector, error) {
	length := int64(vector.Length(nsv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	value := col1[0]
	for i := range vcols {
		vcols[i] = value != col2[i]
	}
	nulls.Or(nsv.Nsp, nil, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func isBytesNe(b1, b2 []byte) bool {
	if len(b1) != len(b2) {
		return true
	}
	return !bytes.Equal(b1, b2)
}

func ScalarStringNeNotScalar(sv, nsv *vector.Vector, str []byte, col *types.Bytes, proc *process.Process) (*vector.Vector, error) {
	var i int64
	length := int64(vector.Length(nsv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i = 0; i < length; i++ {
		vcols[i] = isBytesNe(str, col.Get(i))
	}
	nulls.Or(nsv.Nsp, nil, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func ScalarDecimal64NeNotScalar(sv, nsv *vector.Vector, str types.Decimal64, col []types.Decimal64, proc *process.Process) (*vector.Vector, error) {
	var i int64
	length := int64(vector.Length(nsv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i = 0; i < length; i++ {
		vcols[i] = types.CompareDecimal64Decimal64(str, col[i], sv.Typ.Scale, nsv.Typ.Scale) != 0
	}
	nulls.Or(nsv.Nsp, nil, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func ScalarDecimal128NeNotScalar(sv, nsv *vector.Vector, str types.Decimal128, col []types.Decimal128, proc *process.Process) (*vector.Vector, error) {
	var i int64
	length := int64(vector.Length(nsv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i = 0; i < length; i++ {
		vcols[i] = types.CompareDecimal128Decimal128(str, col[i], sv.Typ.Scale, nsv.Typ.Scale) != 0
	}
	nulls.Or(nsv.Nsp, nil, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func NeGeneral[T NormalType](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustTCols[T](v1), vector.MustTCols[T](v2)
	if v1.IsScalarNull() || v2.IsScalarNull() {
		return HandleWithNullCol(vs, proc)
	}
	c1, c2 := v1.IsScalar(), v2.IsScalar()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = col1[0] != col2[0]
		return vec, nil
	case c1 && !c2:
		return ScalarNeNotScalar(v1, v2, col1, col2, proc)
	case !c1 && c2:
		return ScalarNeNotScalar(v2, v1, col2, col1, proc)
	}
	// case !c1 && !c2
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		vcols[i] = col1[i] != col2[i]
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func NeDecimal64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustTCols[types.Decimal64](v1), vector.MustTCols[types.Decimal64](v2)

	if v1.IsScalarNull() || v2.IsScalarNull() {
		return HandleWithNullCol(vs, proc)
	}

	c1, c2 := v1.IsScalar(), v2.IsScalar()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = types.CompareDecimal64Decimal64(col1[0], col2[0], v1.Typ.Scale, v2.Typ.Scale) != 0
		return vec, nil
	case c1 && !c2:
		return ScalarDecimal64NeNotScalar(v1, v2, col1[0], col2, proc)
	case !c1 && c2:
		return ScalarDecimal64NeNotScalar(v2, v1, col2[0], col1, proc)
	}
	// case !c1 && !c2
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		j := int64(i)
		vcols[i] = types.CompareDecimal64Decimal64(col1[j], col2[j], v1.Typ.Scale, v2.Typ.Scale) != 0
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func NeDecimal128(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustTCols[types.Decimal128](v1), vector.MustTCols[types.Decimal128](v2)

	if v1.IsScalarNull() || v2.IsScalarNull() {
		return HandleWithNullCol(vs, proc)
	}

	c1, c2 := v1.IsScalar(), v2.IsScalar()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = types.CompareDecimal128Decimal128(col1[0], col2[0], v1.Typ.Scale, v2.Typ.Scale) != 0
		return vec, nil
	case c1 && !c2:
		return ScalarDecimal128NeNotScalar(v1, v2, col1[0], col2, proc)
	case !c1 && c2:
		return ScalarDecimal128NeNotScalar(v2, v1, col2[0], col1, proc)
	}
	// case !c1 && !c2
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		j := int64(i)
		vcols[i] = types.CompareDecimal128Decimal128(col1[j], col2[j], v1.Typ.Scale, v2.Typ.Scale) != 0
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func NeString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustBytesCols(v1), vector.MustBytesCols(v2)

	if v1.IsScalarNull() || v2.IsScalarNull() {
		return HandleWithNullCol(vs, proc)
	}

	c1, c2 := v1.IsScalar(), v2.IsScalar()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = isBytesNe(col1.Get(0), col2.Get(0))
		return vec, nil
	case c1 && !c2:
		return ScalarStringNeNotScalar(v1, v2, col1.Get(0), col2, proc)
	case !c1 && c2:
		return ScalarStringNeNotScalar(v2, v1, col2.Get(0), col1, proc)
	}
	// case !c1 && !c2
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		j := int64(i)
		vcols[i] = isBytesNe(col1.Get(j), col2.Get(j))
	}
	nulls.Or(v1.Nsp, v2.Nsp, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}
