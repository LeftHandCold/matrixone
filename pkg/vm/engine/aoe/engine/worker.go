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

func (w *worker) Alloc(attrs []string) *batData{
	num := 4
	if len(w.batDatas) == 0 {
		tim := time.Now()
		w.batDatas = make([]*batData, num)
		for i := 0; i < num; i++ {
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for a := range attrs {
				cds[a] = bytes.NewBuffer(make([]byte, 1<<20))
				dds[a] = bytes.NewBuffer(make([]byte, 1<<20))
			}
			w.batDatas[i] = &batData{
				bat: nil,
				cds: cds,
				dds: dds,
				use: false,
				workerid: w.id,
			}
		}
		logutil.Infof("workerId: %d, make latency: %d", w.id, time.Since(tim).Milliseconds())
	}
	for {
		t := time.Now()
		for j := range w.batDatas {
			if w.batDatas[j] == nil {
				logutil.Infof("batDatas is nil")
			}
			if !w.batDatas[j].use {
				w.batDatas[j].use = true
				w.allocLatency += time.Since(t).Milliseconds()
				return w.batDatas[j]
			}
		}
		time.Sleep(time.Microsecond * 500)
	}
}

func (w *worker) Start(refCount []uint64, attrs []string)  {
	for  {
		exec := false
		for i := range w.readers{
			if w.readers[i].(*aoeReader).blocks == nil || len(w.readers[i].(*aoeReader).blocks) == 0 {
				if !w.readers[i].(*aoeReader).rclose {
					w.storeReader.SetBatch(nil, w.readers[i].(*aoeReader))
					close(w.readers[i].(*aoeReader).rhs)
					w.readers[i].(*aoeReader).rclose = true
				}
				continue
			}
			exec = true
			j := i+1
			if j == len(w.readers) {
				j = 0
			}
			if len(w.readers[j].(*aoeReader).blocks) > 1 {
				w.readers[j].(*aoeReader).blocks[0].Prefetch(attrs)
			}
			data := w.Alloc(attrs)
			readLate := time.Now()
			bat, err := w.readers[i].(*aoeReader).blocks[0].Read(refCount, attrs, data.cds, data.dds)
			w.readLatency += time.Since(readLate).Milliseconds()
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
			data.bat = bat
			tim := time.Now()
			w.storeReader.SetBatch(data, w.readers[i].(*aoeReader))
			w.readers[i].(*aoeReader).blocks = w.readers[i].(*aoeReader).blocks[1:]
			w.enqueue += time.Since(tim).Milliseconds()
		}
		if !exec {
			break
		}
	}
	logutil.Infof("workerId: %d, alloc latency: %d, enqueue latency: %d, read latency: %d",
		w.id, w.allocLatency, w.enqueue, w.readLatency)
	w.storeReader.RemoveWorker(w.id)
}
