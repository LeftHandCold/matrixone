package hash

import (
	"bytes"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vm/process"
)

func NewGroup(sel int64) *Group {
	return &Group{
		Sel: sel,
	}
}

func (g *Group) Free(proc *process.Process) {
	if g.Data != nil {
		proc.Free(g.Data)
		g.Data = nil
	}
}

func (g *Group) Fill(sels, matched []int64, vecs, gvecs []*vector.Vector, diffs []bool, proc *process.Process) ([]int64, error) {
	for i, vec := range vecs {
		switch vec.Typ.Oid {
		case types.T_int8:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int8)
				gv := gvec.Col.([]int8)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int8)
				gv := gvec.Col.([]int8)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int16:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int16)
				gv := gvec.Col.([]int16)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int16)
				gv := gvec.Col.([]int16)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int32:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int32)
				gv := gvec.Col.([]int32)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int32)
				gv := gvec.Col.([]int32)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_int64:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]int64)
				gv := gvec.Col.([]int64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]int64)
				gv := gvec.Col.([]int64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint8:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint8)
				gv := gvec.Col.([]uint8)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint8)
				gv := gvec.Col.([]uint8)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint16:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint16)
				gv := gvec.Col.([]uint16)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint16)
				gv := gvec.Col.([]uint16)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint32:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint32)
				gv := gvec.Col.([]uint32)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint32)
				gv := gvec.Col.([]uint32)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_uint64:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]uint64)
				gv := gvec.Col.([]uint64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]uint64)
				gv := gvec.Col.([]uint64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_float32:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float32)
				gv := gvec.Col.([]float32)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]float32)
				gv := gvec.Col.([]float32)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_float64:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (gv != vs[sel])
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.([]float64)
				gv := gvec.Col.([]float64)[g.Sel]
				for i, sel := range sels {
					diffs[i] = diffs[i] || (gv != vs[sel])
				}
			}
		case types.T_decimal:
		case types.T_date:
		case types.T_datetime:
		case types.T_char:
		case types.T_varchar:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
				}
			}
		case types.T_json:
			gvec := gvecs[i]
			lnull := vec.Nsp.Any()
			rnull := gvec.Nsp.Contains(uint64(g.Sel))
			switch {
			case lnull && rnull:
				for i, sel := range sels {
					if !vec.Nsp.Contains(uint64(sel)) { // only null eq null
						diffs[i] = true
					}
				}
			case lnull && !rnull: // null is not value
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					if vec.Nsp.Contains(uint64(sel)) {
						diffs[i] = true
					} else {
						diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
					}
				}
			case !lnull && rnull: // null is not value
				for i := range sels {
					diffs[i] = true
				}
			default:
				vs := vec.Col.(*types.Bytes)
				gvs := gvec.Col.(*types.Bytes)
				gv := gvs.Get(int(g.Sel))
				for i, sel := range sels {
					diffs[i] = diffs[i] || (bytes.Compare(gv, vs.Get(int(sel))) != 0)
				}
			}
		}
	}
	n := len(sels)
	matched = matched[:0]
	remaining := sels[:0]
	for i := 0; i < n; i++ {
		if diffs[i] {
			remaining = append(remaining, sels[i])
		} else {
			matched = append(matched, sels[i])
		}
	}
	if len(matched) > 0 {
		length := len(g.Sels) + len(matched)
		if cap(g.Sels) < length {
			data, err := proc.Alloc(int64(length) * 8)
			if err != nil {
				return nil, err
			}
			copy(data, g.Data)
			proc.Free(g.Data)
			g.Sels = encoding.DecodeInt64Slice(data)
			g.Data = data[:length]
			g.Sels = g.Sels[:length]
		}
		g.Sels = append(g.Sels, matched...)
	}
	return remaining, nil

}
