package engine

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"time"
)

func (w *worker) ID() int32 {
	return w.id
}

func (w *worker) Start(refCount []uint64, attrs []string)  {
	num := 64
	tim := time.Now()
	if len(w.cds) == 0 {
		w.cds = make([][]*bytes.Buffer, num)
		w.dds = make([][]*bytes.Buffer, num)
		for i := 0; i < num; i++ {
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			w.cds[i] = cds
			w.dds[i] = dds
		}
	}
	logutil.Infof("make time is %d", time.Since(tim).Milliseconds())
	var j int
	j = 0
	for i :=0; i < len(w.blocks); i++ {
		if j == num {
			j = 0
		}
		for i := range attrs {
			tim := time.Now()
			w.cds[j][i] = GetEntry()
			w.dds[j][i] = GetEntry()
			w.dequeue += time.Since(tim).Milliseconds()
		}

		if i < len(w.blocks) - 1 {
			w.blocks[i+1].Prefetch(attrs)
		}

		bat, err := w.blocks[i].Read(refCount, attrs, w.cds[j], w.dds[j])
		if err != nil {
			panic("error")
		}
		n := vector.Length(bat.Vecs[0])
		if n > cap(w.zs) {
			w.zs = make([]int64, n)
		}
		bat.Zs = w.zs[:n]
		for i := 0; i < n; i++ {
			bat.Zs[i] = 1
		}
		data := &data{
			bat:	bat,
			cds:	w.cds[j],
			dds:	w.dds[j],
		}
		w.storeReader.SetBatch(data)
		j++
	}
	w.storeReader.RemoveWorker(w.id)
	logutil.Infof("id is %d, dequeue is %d", w.id, w.dequeue)
}
