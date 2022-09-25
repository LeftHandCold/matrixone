// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package containers

import (
	"bytes"
	"testing"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/stretchr/testify/assert"
)

func withAllocator(opts *Options) *Options {
	if opts == nil {
		opts = new(Options)
	}
	allocator := stl.NewSimpleAllocator()
	opts.Allocator = allocator
	return opts
}

func TestVector1(t *testing.T) {
	opts := new(Options)
	opts.Capacity = 1
	opts.Allocator = stl.NewSimpleAllocator()
	vec := NewVector[int64](opts)
	now := time.Now()

	for i := 0; i < 500; i++ {
		vec.Append(int64(i))
	}
	t.Log(time.Since(now))
	t.Log(vec.String())
	allocator := vec.GetAllocator()
	assert.True(t, allocator.Usage() > 0)
	now = time.Now()
	for i := 0; i < 500; i++ {
		v := vec.Get(i)
		assert.Equal(t, int64(i), v)
	}
	t.Log(time.Since(now))

	vec.Update(100, int64(999))
	v := vec.Get(100)
	assert.Equal(t, int64(999), v)

	assert.Equal(t, 500, vec.Length())
	vec.Delete(80)
	assert.Equal(t, 499, vec.Length())

	vec2 := NewVector[int64]()
	for i := 0; i < 100; i++ {
		vec2.Append(int64(i + 1000))
	}
	vec.AppendMany(vec2.Slice()...)
	assert.Equal(t, 100+499, vec.Length())

	vec.Close()
	vec2.Close()
	assert.True(t, allocator.Usage() == 0)
}

func TestVector2(t *testing.T) {
	vec := NewVector[[]byte]()
	defer vec.Close()
	vec.Append([]byte("hello"))
	t.Log(vec.String())
	v := vec.Get(0)
	assert.Equal(t, "hello", string(v))
	vec.Append([]byte("world"))
	assert.Equal(t, 2, vec.Length())
	vec.Delete(0)
	assert.Equal(t, 1, vec.Length())
	v = vec.Get(0)
	assert.Equal(t, "world", string(v))
}

func TestVector3(t *testing.T) {
	opts := new(Options)
	opts.Allocator = stl.NewSimpleAllocator()
	vec := NewVector[[]byte](opts)
	vec.Append([]byte("h1"))
	vec.Append([]byte("h2"))
	vec.Append([]byte("h3"))
	vec.Append([]byte("h4"))
	assert.Equal(t, 4, vec.Length())
	vec.Update(1, []byte("hello"))
	t.Logf("%s", vec.Get(3))
	t.Logf("%s", vec.Get(2))
	t.Logf("%s", vec.Get(1))
	t.Logf("%s", vec.Get(0))
	assert.Equal(t, "h1", string(vec.Get(0)))
	assert.Equal(t, "hello", string(vec.Get(1)))
	assert.Equal(t, "h3", string(vec.Get(2)))
	assert.Equal(t, "h4", string(vec.Get(3)))
	t.Log(vec.String())
	alloc := vec.GetAllocator()
	t.Log(alloc.String())
	vec.Close()
	assert.Equal(t, 0, alloc.Usage())
}

func TestVector4(t *testing.T) {
	opts := &Options{
		Allocator: stl.NewSimpleAllocator(),
	}
	vec := NewVector[[]byte](opts)
	vec.Append([]byte("h1"))
	vec.Append([]byte("h2"))
	vec.Append([]byte("h3"))
	vec.Append([]byte("h4"))
	assert.Equal(t, 4, vec.Length())
	vec.Delete(1)
	assert.Equal(t, 3, vec.Length())
	t.Logf("%s", vec.Get(2))
	t.Logf("%s", vec.Get(1))
	t.Logf("%s", vec.Get(0))
	assert.Equal(t, "h1", string(vec.Get(0)))
	assert.Equal(t, "h3", string(vec.Get(1)))
	assert.Equal(t, "h4", string(vec.Get(2)))
	t.Log(vec.String())
	alloc := vec.GetAllocator()
	t.Log(alloc.String())
	vec.Close()
	assert.Equal(t, 0, alloc.Usage())
}

func TestVector5(t *testing.T) {
	opts := &Options{
		Allocator: stl.NewSimpleAllocator(),
	}
	vec := NewVector[[]byte](opts)
	vec.Append([]byte("h1"))
	vec.Append([]byte("hh2"))
	vec.Append([]byte("hhh3"))
	vec.Append([]byte("hhhh4"))
	assert.Equal(t, 4, vec.Length())
	vec2 := vec.Clone(0, 1)
	assert.Equal(t, 1, vec2.Length())
	assert.Equal(t, "h1", string(vec2.Get(0)))
	vec2.Close()
	vec2 = vec.Clone(0, 2)
	assert.Equal(t, 2, vec2.Length())
	assert.Equal(t, "h1", string(vec2.Get(0)))
	assert.Equal(t, "hh2", string(vec2.Get(1)))
	vec2.Close()
	vec2 = vec.Clone(0, 3)
	assert.Equal(t, 3, vec2.Length())
	assert.Equal(t, "h1", string(vec2.Get(0)))
	assert.Equal(t, "hh2", string(vec2.Get(1)))
	assert.Equal(t, "hhh3", string(vec2.Get(2)))
	vec2.Close()
	vec2 = vec.Clone(0, 4)
	assert.Equal(t, 4, vec2.Length())
	assert.Equal(t, "h1", string(vec2.Get(0)))
	assert.Equal(t, "hh2", string(vec2.Get(1)))
	assert.Equal(t, "hhh3", string(vec2.Get(2)))
	assert.Equal(t, "hhhh4", string(vec2.Get(3)))
	vec2.Close()

	vec2 = vec.Clone(1, 1)
	assert.Equal(t, 1, vec2.Length())
	assert.Equal(t, "hh2", string(vec2.Get(0)))
	vec2.Close()
	vec2 = vec.Clone(1, 2)
	assert.Equal(t, 2, vec2.Length())
	assert.Equal(t, "hh2", string(vec2.Get(0)))
	assert.Equal(t, "hhh3", string(vec2.Get(1)))
	vec2.Close()
	vec2 = vec.Clone(1, 3)
	assert.Equal(t, 3, vec2.Length())
	assert.Equal(t, "hh2", string(vec2.Get(0)))
	assert.Equal(t, "hhh3", string(vec2.Get(1)))
	assert.Equal(t, "hhhh4", string(vec2.Get(2)))
	vec2.Close()
	vec.Close()
	t.Log(opts.Allocator.String())
	assert.Equal(t, 0, opts.Allocator.Usage())
}

func TestVector6(t *testing.T) {
	w := bytes.Buffer{}
	for i := 0; i < 10; i++ {
		v := int64(i)
		vs := unsafe.Slice((*byte)(unsafe.Pointer(&v)), 8)
		w.Write(vs)
	}
	buf := w.Bytes()
	t.Logf("cap:%d,size:%d", cap(buf), len(buf))
	opts := &Options{
		BinaryData: &stl.BinaryData{
			Payload:       buf,
			FixedType:     true,
			FixedTypeSize: stl.Sizeof[int64](),
		},
	}
	vec := NewVector[int64](opts)
	t.Log(vec.String())
	assert.Equal(t, 0, vec.Allocated())
	vec.Update(3, int64(3333))
	t.Log(vec.String())
	slice := unsafe.Slice((*int64)(unsafe.Pointer(&buf[0])), vec.Length())
	assert.Equal(t, int64(3333), slice[3])

	vec.Update(4, int64(444))
	assert.Equal(t, int64(444), slice[4])

	vec.Append(int64(99))
	assert.True(t, vec.Allocated() > 0)
	t.Log(vec.String())

	vec.Update(3, int64(4444))
	assert.Equal(t, int64(3333), slice[3])
}

func TestVector7(t *testing.T) {
	allocator := stl.NewSimpleAllocator()
	opts := new(Options)
	opts.Allocator = allocator
	vec := NewVector[int16](opts)
	vec.Append(int16(1))
	vec.Append(int16(2))
	vec.Append(int16(3))
	d := vec.Data()
	assert.Equal(t, len(d), 3*stl.Sizeof[int16]())
	s := vec.Slice()
	assert.Equal(t, 3, len(s))
	vec.Close()

	vec3 := NewVector[byte](opts)
	vec3.AppendMany([]byte("world")...)
	s2 := vec3.Slice()
	assert.Equal(t, 5, len(s2))
	d = vec3.Data()
	assert.Equal(t, 5, len(d))
	vec3.Close()
	assert.Equal(t, 0, allocator.Usage())

	vec2 := NewVector[[]byte](opts)
	vec2.Append([]byte("h1"))
	vec2.Append([]byte("hh2"))
	vec2.Append([]byte("hhh3"))
	vec2.Append([]byte("hhhh4"))
	bs := vec2.BinaryData()
	t.Log(vec2.String())

	allocated := allocator.Usage()

	opt2 := new(Options)
	opt2.Allocator = allocator
	opt2.BinaryData = bs

	vec4 := NewVector[[]byte](opt2)
	assert.Equal(t, vec2.Length(), vec4.Length())
	assert.Equal(t, vec2.Get(0), vec4.Get(0))
	assert.Equal(t, vec2.Get(1), vec4.Get(1))
	assert.Equal(t, vec2.Get(2), vec4.Get(2))
	assert.Equal(t, vec2.Get(3), vec4.Get(3))
	assert.Equal(t, allocated, allocator.Usage())

	vec2.Close()
}

func TestVector8(t *testing.T) {
	allocator := stl.NewSimpleAllocator()
	opts := new(Options)
	opts.Allocator = allocator
	vec := NewVector[int32](opts)
	vec.AppendMany(int32(1), int32(3), int32(9))
	t.Log(vec.String())
	assert.Equal(t, 3, vec.Length())

	w := new(bytes.Buffer)
	_, err := vec.WriteTo(w)
	assert.NoError(t, err)

	buf := w.Bytes()
	vec2 := NewVector[int32](opts)

	r := bytes.NewBuffer(buf)
	_, err = vec2.ReadFrom(r)
	assert.NoError(t, err)
	t.Log(vec2.String())
	vec.Close()
	vec2.Close()
	t.Log(allocator.String())
	assert.Zero(t, allocator.Usage())
}

func TestVector9(t *testing.T) {
	allocator := stl.NewSimpleAllocator()
	opts := new(Options)
	opts.Allocator = allocator
	vec := NewVector[[]byte](opts)
	vec.AppendMany([]byte("h1"), []byte("hh2"),
		[]byte("hhh3"), []byte("hhhh4"))
	vec.Delete(1)
	assert.Equal(t, 3, vec.Length())
	w := new(bytes.Buffer)
	_, err := vec.WriteTo(w)
	assert.NoError(t, err)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	vec2 := NewVector[[]byte](opts)
	_, err = vec2.ReadFrom(r)
	assert.NoError(t, err)
	t.Log(vec2.String())
	assert.Equal(t, 3, vec2.Length())
	assert.Equal(t, vec.Get(0), vec2.Get(0))
	assert.Equal(t, vec.Get(1), vec2.Get(1))
	assert.Equal(t, vec.Get(2), vec2.Get(2))
	vec.Close()
	vec2.Close()
	assert.Zero(t, allocator.Usage())
}

func TestVector10(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewVector[[]byte](opts)
	h1 := "h1"
	h2 := "hh2"
	h3 := "hhh3"
	h4 := "hhhh4"
	vec.Append([]byte(h1))
	vec.Append([]byte(h2))
	vec.Append([]byte(h3))
	vec.Append([]byte(h4))

	data := vec.BinaryData()

	vec2 := NewVector[[]byte](opts)
	vec2.ReadData(data, true)
	t.Log(vec2.String())
	assert.Equal(t, vec.Length(), vec2.Length())
	assert.Equal(t, vec.Capacity(), vec2.Capacity())
	assert.Equal(t, vec.Get(0), vec2.Get(0))
	assert.Equal(t, vec.Get(1), vec2.Get(1))
	assert.Equal(t, vec.Get(2), vec2.Get(2))
	assert.Equal(t, vec.Get(3), vec2.Get(3))
	assert.Zero(t, vec2.Allocated())

	vec3 := NewVector[[]byte](opts)
	vec3.ReadData(data, false)
	t.Log(vec3.String())
	assert.Equal(t, vec.Allocated(), vec3.Allocated())
	assert.Equal(t, vec.Length(), vec3.Length())
	for i := 0; i < vec.Length(); i++ {
		assert.Equal(t, vec.Get(i), vec3.Get(i))
	}
	vec3.Append([]byte("x1"))
	assert.Equal(t, vec.Length()+1, vec3.Length())

	vec.Close()
	vec2.Close()
	vec3.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func TestVector11(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewVector[[]byte](opts)
	h1 := "h1"
	h2 := "hh2"
	h3 := "hhh3"
	h4 := "hhhh4"
	vec.Append([]byte(h1))
	vec.Append([]byte(h2))
	vec.Append([]byte(h3))
	vec.Append([]byte(h4))
	usage := opts.Allocator.Usage()
	allocted := vec.Allocated()
	assert.Equal(t, usage, allocted)
	assert.Equal(t, 4, vec.Length())
	vec.Reset()
	assert.Zero(t, vec.Length())
	assert.Equal(t, usage, opts.Allocator.Usage())
	assert.Equal(t, usage, vec.Allocated())

	vec.Append([]byte("x1"))
	assert.Equal(t, 1, vec.Length())
	assert.Equal(t, "x1", string(vec.Get(0)))
	t.Log(vec.String())
	vec.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func TestVector12(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewVector[[]byte](opts)
	h1 := "h1"
	h2 := "hh2"
	h3 := "hhh3"
	h4 := "hhhh4"
	vec.Append([]byte(h1))
	vec.Append([]byte(h2))
	vec.Append([]byte(h3))
	vec.Append([]byte(h4))

	w := new(bytes.Buffer)
	_, err := vec.WriteTo(w)
	assert.NoError(t, err)
	buf := w.Bytes()
	vec2 := NewVector[[]byte](opts)
	n, err := vec2.InitFromSharedBuf(buf)
	assert.Equal(t, int(n), len(buf))
	assert.NoError(t, err)
	assert.Zero(t, vec2.Allocated())
	for i := 0; i < vec.Length(); i++ {
		assert.Equal(t, vec.Get(i), vec2.Get(i))
	}

	vec2.Close()
	vec.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func TestStrVector1(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewStrVector2[[]byte](opts)
	h1 := "h1"
	h2 := "hh2"
	h3 := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh3"
	h4 := "hhhh4"
	h5 := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh5"
	h6 := "hhhhhh6"
	vec.Append([]byte(h1))
	vec.Append([]byte(h2))
	vec.Append([]byte(h3))
	vec.Append([]byte(h4))

	t.Log(vec.String())
	assert.Equal(t, vec.area.Length(), len(h3))
	assert.Equal(t, 4, vec.Length())

	min, max := vec.getAreaRange(0, 2)
	assert.Equal(t, 0, min)
	assert.Equal(t, 0, max)

	min, max = vec.getAreaRange(1, 2)
	assert.Equal(t, 0, min)
	assert.Equal(t, len(h3), max)
	assert.Equal(t, []byte(h3), vec.area.Slice()[min:max])

	min, max = vec.getAreaRange(1, 3)
	assert.Equal(t, 0, min)
	assert.Equal(t, len(h3), max)

	vec.Append([]byte(h5))
	vec.Append([]byte(h6))

	assert.Equal(t, 6, vec.Length())
	assert.Equal(t, vec.area.Length(), len(h3)+len(h5))

	min, max = vec.getAreaRange(3, 2)
	assert.Equal(t, len(h3), min)
	assert.Equal(t, len(h3)+len(h5), max)
	assert.Equal(t, []byte(h5), vec.area.Slice()[min:max])
	t.Logf("%s", vec.area.Slice()[min:max])

	w := new(bytes.Buffer)
	_, err := vec.WriteTo(w)
	assert.NoError(t, err)
	buf := w.Bytes()
	vec2 := NewStrVector2[[]byte](opts)
	n, err := vec2.InitFromSharedBuf(buf)
	assert.Equal(t, int(n), len(buf))
	assert.NoError(t, err)
	assert.Zero(t, vec2.Allocated())
	t.Log(vec2.String())
	for i := 0; i < vec.Length(); i++ {
		assert.Equal(t, vec.Get(i), vec2.Get(i))
	}

	vec2.Close()
	vec.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func TestStrVector2(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewStrVector2[[]byte](opts)
	h1 := "h1"
	h2 := "hh2"
	h3 := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh3"
	h4 := "hhhh4"
	vec.Append([]byte(h1))
	vec.Append([]byte(h2))
	vec.Append([]byte(h3))
	vec.Append([]byte(h4))

	data := vec.BinaryData()

	vec3 := NewStrVector2[[]byte](opts)
	vec3.ReadData(data, true)
	assert.Zero(t, vec3.Allocated())
	for i := 0; i < vec.Length(); i++ {
		assert.Equal(t, vec.Get(i), vec3.Get(i))
	}

	vec4 := NewStrVector2[[]byte](opts)
	vec4.ReadData(data, false)
	assert.NotZero(t, vec4.Allocated())
	for i := 0; i < vec.Length(); i++ {
		assert.Equal(t, vec.Get(i), vec4.Get(i))
	}
	t.Log(vec4.String())

	vec4.Close()
	vec3.Close()
	vec.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func TestStrVector3(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewStrVector2[[]byte](opts)
	h1 := "h1"
	h2 := "hh2"
	h3 := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh3"
	h4 := "hhhh4"
	h5 := "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhxy5"
	h6 := "hhhhhh6"
	vec.Append([]byte(h1))
	vec.Append([]byte(h2))
	vec.Append([]byte(h3))
	vec.Append([]byte(h4))
	vec.Append([]byte(h5))
	vec.Append([]byte(h6))

	assert.Equal(t, 6, vec.Length())
	vec.RangeDelete(1, 2)
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, h1, string(vec.Get(0)))
	assert.Equal(t, h4, string(vec.Get(1)))
	assert.Equal(t, h5, string(vec.Get(2)))
	assert.Equal(t, h6, string(vec.Get(3)))

	vec.Update(1, []byte(h3))
	for i := 0; i < vec.Length(); i++ {
		t.Logf("%s", vec.Get(i))
	}

	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, h1, string(vec.Get(0)))
	assert.Equal(t, h3, string(vec.Get(1)))
	assert.Equal(t, h5, string(vec.Get(2)))
	assert.Equal(t, h6, string(vec.Get(3)))

	vec.Update(1, []byte(h2))
	for i := 0; i < vec.Length(); i++ {
		t.Logf("%s", vec.Get(i))
	}
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, h1, string(vec.Get(0)))
	assert.Equal(t, h2, string(vec.Get(1)))
	assert.Equal(t, h5, string(vec.Get(2)))
	assert.Equal(t, h6, string(vec.Get(3)))

	vec.Update(2, []byte(h3))
	for i := 0; i < vec.Length(); i++ {
		t.Logf("%s", vec.Get(i))
	}
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, h1, string(vec.Get(0)))
	assert.Equal(t, h2, string(vec.Get(1)))
	assert.Equal(t, h3, string(vec.Get(2)))
	assert.Equal(t, h6, string(vec.Get(3)))

	vec.Update(2, []byte(h5))
	for i := 0; i < vec.Length(); i++ {
		t.Logf("%s", vec.Get(i))
	}
	assert.Equal(t, 4, vec.Length())
	assert.Equal(t, h1, string(vec.Get(0)))
	assert.Equal(t, h2, string(vec.Get(1)))
	assert.Equal(t, h5, string(vec.Get(2)))
	assert.Equal(t, h6, string(vec.Get(3)))

	vec.Close()
	assert.Zero(t, opts.Allocator.Usage())
}

func getBytes(i int) []byte {
	if i%2 == 0 {
		return []byte("hhhhhhhhhhhhhhhhhxxxxxxxxxe")
	}
	return []byte("yyyk")
}

func TestStrVector4(t *testing.T) {
	opts := withAllocator(nil)
	vec := NewStrVector2[[]byte](opts)
	for i := 0; i < 10000; i++ {
		vec.Append(getBytes(i))
	}
	now := time.Now()

	i := 0
	vec.Update(i, getBytes(1))
	vec.Update(i, getBytes(0))
	vec.Update(i, getBytes(1))
	vec.Update(i, getBytes(0))
	vec.Delete(i)

	t.Log(time.Since(now))
	vec.Close()
	assert.Zero(t, opts.Allocator.Usage())
}
