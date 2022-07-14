package taosSql

import (
	"container/list"
	"sync"
	"time"

	"github.com/taosdata/driver-go/v2/wrapper"
	"github.com/taosdata/driver-go/v2/wrapper/handler"
)

type HandlerRecycle struct {
	Handlers             *list.List
	mu                   sync.RWMutex
	handlerPool          *handler.HandlerPool
	elemMaxLifeTime      time.Duration
	recycleCheckInterval time.Duration
}

type recycleElem struct {
	H           *handler.Handler
	PorcessTime time.Time
}

func NewHandlerRecycle(pool *handler.HandlerPool, recycleCheckInterval, elemMaxLifeTime time.Duration) *HandlerRecycle {
	c := &HandlerRecycle{
		Handlers:             list.New(),
		handlerPool:          pool,
		recycleCheckInterval: recycleCheckInterval,
		elemMaxLifeTime:      elemMaxLifeTime,
	}
	c.start()
	return c
}

func (c *HandlerRecycle) Put(h *handler.Handler) {
	if h != nil {
		c.mu.Lock()
		c.Handlers.PushBack(&recycleElem{
			H:           h,
			PorcessTime: time.Now(),
		})
		c.mu.Unlock()
	}
}

func (c *HandlerRecycle) start() {
	tm := time.NewTimer(c.recycleCheckInterval)
	go func() {
		for {
			tm.Reset(c.recycleCheckInterval)
			select {
			case <-tm.C:
				c.doRecycle()
			}
		}
	}()
}

func (c *HandlerRecycle) doRecycle() {
	c.mu.Lock()
	e := c.Handlers.Front()
	for i := 0; e != nil; i++ {
		ev := e.Value
		ne := e.Next()
		if ev != nil {
			cl := ev.(*recycleElem)
			h := cl.H
			var ret *handler.AsyncResult

			select {
			case ret = <-h.Caller.QueryResult:
				if ret != nil && ret.Res != nil {
					locker.Lock()
					wrapper.TaosFreeResult(ret.Res)
					locker.Unlock()
				}
			case ret = <-h.Caller.FetchResult:
			default:
			}

			if ret != nil || time.Now().Sub(cl.PorcessTime) > c.elemMaxLifeTime {
				c.Handlers.Remove(e)
				if ret != nil {
					c.handlerPool.Put(h)
				} else {
					c.handlerPool.Put(handler.NewHandler())
				}
			}
		} else {
			c.Handlers.Remove(e)
		}
		e = ne
		if i&1 != 0 {
			c.mu.Unlock()
			c.mu.Lock()
		}
	}
	c.mu.Unlock()
}
