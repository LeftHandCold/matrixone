package codec

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/tfs"
)

type Reader struct {
	fs tfs.FS
}

func NewReader(fs tfs.FS) *Reader {
	return &Reader{
		fs: fs,
	}
}
