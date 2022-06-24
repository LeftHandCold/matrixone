package containers

import (
	"bytes"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type MemAllocator = stl.MemAllocator
type Options = containers.Options
type Bytes = stl.Bytes

var NewBytes = stl.NewBytes

type ItOp = func(v any, row int) error

type VectorView interface {
	IsView() bool
	Nullable() bool
	IsNull(i int) bool
	HasNull() bool
	NullMask() *roaring64.Bitmap

	Data() []byte
	Bytes() *Bytes
	Slice() any
	DataWindow(offset, length int) []byte
	Get(i int) any

	Length() int
	Capacity() int
	Allocated() int
	GetAllocator() stl.MemAllocator
	GetType() types.Type
	String() string

	Foreach(op ItOp, sels *roaring.Bitmap) error
	ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) error

	WriteTo(w io.Writer) (int64, error)
}

type Vector interface {
	Nullable() bool
	IsNull(i int) bool
	HasNull() bool
	NullMask() *roaring64.Bitmap

	ResetWithData(bs *Bytes, nulls *roaring64.Bitmap)

	IsView() bool
	GetView() VectorView
	Data() []byte
	Bytes() *Bytes
	Slice() any
	DataWindow(offset, length int) []byte
	Get(i int) any
	Update(i int, v any)
	Delete(i int)
	Append(v any)
	AppendMany(vs ...any)
	Extend(o Vector)
	ExtendWithOffset(src Vector, srcOff, srcLen int)
	Compact(deletes *roaring.Bitmap)
	CloneWindow(offset, length int) Vector

	Equals(o Vector) bool
	Length() int
	Capacity() int
	Allocated() int
	GetAllocator() stl.MemAllocator
	GetType() types.Type
	String() string
	Window(offset, length int) VectorView

	Foreach(op ItOp, sels *roaring.Bitmap) error
	ForeachWindow(offset, length int, op ItOp, sels *roaring.Bitmap) error

	WriteTo(w io.Writer) (int64, error)
	ReadFrom(r io.Reader) (int64, error)

	ReadFromFile(common.IVFile, *bytes.Buffer) error

	Close()
}

type Batch struct {
	Attrs   []string
	Vecs    []Vector
	Deletes *roaring.Bitmap
	nameidx map[string]int
	// refidx  map[int]int
}
