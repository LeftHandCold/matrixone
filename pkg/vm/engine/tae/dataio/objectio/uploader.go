package objectio

import (
	"context"
	"sync"
	"sync/atomic"
)

type OnItemsCB = func(...any)
type uploaderQueue struct {
	queue     chan any
	ctx       context.Context
	wg        sync.WaitGroup
	pending   int64
	batchSize int
	onItemsCB OnItemsCB
}

func NewUploaderQueue(queueSize, batchSize int, onItem OnItemsCB) *uploaderQueue {
	q := &uploaderQueue{
		queue:     make(chan any, queueSize),
		batchSize: batchSize,
		onItemsCB: onItem,
	}
	return q
}

func (q *uploaderQueue) Start() {
	q.wg.Add(1)
	items := make([]any, 0, q.batchSize)
	go func() {
		defer q.wg.Done()
		for {
			select {
			case <-q.ctx.Done():
				return
			case item := <-q.queue:
				items = append(items, item)
			Left:
				for i := 0; i < q.batchSize-1; i++ {
					select {
					case item = <-q.queue:
						items = append(items, item)
					default:
						break Left
					}
				}
				cnt := len(items)
				q.onItemsCB(items...)
				items = items[:0]
				atomic.AddInt64(&q.pending, int64(cnt)*(-1))
			}
		}
	}()
}
