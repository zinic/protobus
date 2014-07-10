package bus

import (
	"time"
	"sync"

	"github.com/zinic/protobus/concurrent"
)

func NewEventLoop(callTarget func()) (el *EventLoop) {
	return &EventLoop {
		waitGroup: &sync.WaitGroup{},
		shutdown: concurrent.NewReferenceLocker(false),
		callTarget: callTarget,
	}
}


type EventLoop struct {
	waitGroup *sync.WaitGroup
	shutdown concurrent.ReferenceLocker
	callTarget func()
}

func (evloop *EventLoop) Stop() {
	evloop.shutdown.Set(true)
	evloop.waitGroup.Wait()
}

func (evloop *EventLoop) Loop() (err error) {
	defer evloop.waitGroup.Done()
	evloop.waitGroup.Add(1)

	for !evloop.shutdown.Get().(bool) {
		evloop.callTarget()
		time.Sleep(1 * time.Millisecond)
	}

	return
}