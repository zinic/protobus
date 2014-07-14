package concurrent

import (
	"sync"

	"github.com/zinic/protobus/context"
)

func NewMutexContext(mutex *sync.Mutex) (mutexContet context.Context) {
	lockContextProvider := func(call context.ContextBound, args... interface{}) (err error) {
		mutex.Lock()
		defer mutex.Unlock()

		return call(args)
	}

	return context.Using(lockContextProvider)
}

func NewLockerContext() (lockerContext context.Context) {
	return NewMutexContext(&sync.Mutex{})
}