package engine

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
)

func (s *store) SetBlocks(blocks []aoe.Block){
	s.blocks = blocks
}

func (s *store) GetBatch(refCount []uint64, attrs []string, id int) *batch.Batch {
	if !s.start {
		s.mu.Lock()
		if s.start {
			s.mu.Unlock()
			goto GET
		}
		s.start = true
		s.mu.Unlock()
		s.ReadStart(refCount, attrs)
	}

GET:bat, ok := <-s.rhs[id]
	s.batnum++
	if !ok {
		return nil
	}
	return bat
}

func (s *store) SetBatch(bat *batch.Batch, id int){
	s.rhs[id] <- bat
}

func (s *store) ReadStart(refCount []uint64, attrs []string) {
	num := 4
	mod := len(s.blocks) / num
	rmod := len(s.rhs) /num
	logutil.Infof("blocks is %d", len(s.blocks))
	if mod == 0 {
		mod = 1
	}
	workers := make([]worker, 0)
	for i := 0; i < num; i++ {
		if i == num-1 || i == len(s.blocks)-1 {

			wk := worker{
				rhs: s.rhs[i*rmod:],
				blocks: s.blocks[i*mod:],
				id:		int32(i),
				storeReader: s,
			}
			workers = append(workers, wk)
			break
		}
		wk := worker{
			rhs: s.rhs[i*rmod : (i+1)*rmod],
			blocks: s.blocks[i*mod : (i+1)*mod],
			id:		int32(i),
			storeReader: s,
		}
		workers = append(workers, wk)
	}
	s.workers = len(workers)
	for j := 0; j < len(workers); j++{
		go workers[j].Start(refCount, attrs)
	}
}

func (s *store) RemoveWorker(id int32) {
	if s.workers < 1 {
		panic("workers error")
	}
	s.workers--
	/*for i, v := range s.workers{
		if v.ID() == id {
			if i == len(s.workers) {
				s.workers = s.workers[i:]
				break
			}
			s.workers = append(s.workers[:i], s.workers[i+1:]...)
		}
	}*/
	/*if s.workers == 0 {
		logutil.Infof("batnum is %d", s.batnum)
		s.SetBatch(nil)
		close(s.rhs)
	}*/
}