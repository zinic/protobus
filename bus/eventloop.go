package bus

import (
	"time"
	"sync"
	"runtime"

	"github.com/zinic/gbus/concurrent"
)

func NewEventLoop(callTarget func() (yieldExecution bool)) (el *EventLoop) {
	return &EventLoop {
		waitGroup: &sync.WaitGroup{},
		shutdown: concurrent.NewReferenceLocker(false),
		callTarget: callTarget,
	}
}


type EventLoop struct {
	waitGroup *sync.WaitGroup
	shutdown concurrent.ReferenceLocker
	callTarget func() (yieldExecution bool)
}

func (evloop *EventLoop) Stop() {
	evloop.shutdown.Set(true)
	evloop.waitGroup.Wait()
}

func (evloop *EventLoop) Loop() (err error) {
	defer evloop.waitGroup.Done()
	evloop.waitGroup.Add(1)

	for !evloop.shutdown.Get().(bool) {
		if !evloop.callTarget() {
			time.Sleep(50 * time.Millisecond)
		} else {
			runtime.Gosched()
		}
	}

	return
}