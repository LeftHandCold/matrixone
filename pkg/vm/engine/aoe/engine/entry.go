package engine

import (
	"bytes"
	"sync"
)

var (
	_bufferPool = sync.Pool{
		New: func() interface{} {
			b := bytes.NewBuffer(make([]byte, 1<<20))
			return b
		},
	}
)

func GetEntry() *bytes.Buffer {
	e := _bufferPool.Get().(*bytes.Buffer)
	return e
}

func Free(bs []*bytes.Buffer)  {
	for _, b := range bs {
		_bufferPool.Put(b)
	}
}