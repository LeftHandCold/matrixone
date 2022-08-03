package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/panjf2000/ants/v2"
	"sync"
)

type workHandler struct {
	pool *ants.Pool
	wg   *sync.WaitGroup
	fs   *ObjectFS
}

func NewWorkHandler(num int, fs *ObjectFS) *workHandler {
	pool, err := ants.NewPool(num)
	if err != nil {
		panic(any(err))
	}
	h := &workHandler{
		pool: pool,
		wg:   &sync.WaitGroup{},
		fs:   fs,
	}
	return h
}

func (w *workHandler) doHandle(object *Object) {
	closure := func(object *Object, wg *sync.WaitGroup) func() {
		return func() {
			if object != nil {
				logutil.Infof("Sync: %s End", object.name)
			} else {
				logutil.Infof("Sync: object Not Exist")
			}
			object.wg.Done()
			wg.Done()
		}
	}
	w.wg.Add(1)
	err := w.pool.Submit(closure(object, w.wg))
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (w *workHandler) OnObjects(objects []*Object) {
	for _, object := range objects {
		w.doHandle(object)
	}
}
