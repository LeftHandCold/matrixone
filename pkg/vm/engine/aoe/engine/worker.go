package engine

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func (w *worker) ID() int32 {
	return w.id
}

func (w *worker) Start(refCount []uint64, attrs []string)  {

	if len(w.cds) == 0 {
		w.cds = make([][]*bytes.Buffer, 32)
		w.dds = make([][]*bytes.Buffer, 32)
		for i := 0; i < 32; i++ {
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for a := range attrs {
				cds[a] = bytes.NewBuffer(make([]byte, 1<<10))
				dds[a] = bytes.NewBuffer(make([]byte, 1<<10))
			}
			w.cds[i] = cds
			w.dds[i] = dds
		}
	}
	var j int
	j = 0;
	for i :=0; i < len(w.blocks); i++ {
		if i < len(w.blocks) - 1 {
			w.blocks[i+1].Prefetch(attrs)
		}

		if j == 32 {
			j = 0
		}
		bat, err := w.blocks[i].Read(refCount, attrs, w.cds[j], w.dds[j])
		if err != nil {
			panic("error")
		}
		/*if bat != nil {
			for _, v := range bat.Vecs[0].Col.([]int32) {
				if v == 547330 {
					w.batnum++
				}
			}
		}*/
		n := vector.Length(bat.Vecs[0])
		if n > cap(w.zs) {
			w.zs = make([]int64, n)
		}
		bat.Zs = w.zs[:n]
		for i := 0; i < n; i++ {
			bat.Zs[i] = 1
		}
		w.storeReader.SetBatch(bat)
		j++
	}
	logutil.Infof("reader %p, i is %d", w, w.batnum)
	w.storeReader.RemoveWorker(w.id)
}
