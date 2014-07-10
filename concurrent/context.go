package concurrent

import (
	"sync"

	"github.com/zinic/protobus/context"
)

func NewLockerContext() (lockerContext context.Context) {
	mutex := &sync.Mutex{}
	lockContextProvider := func(call context.ContextBound, args... interface{}) (err error) {
		mutex.Lock()
		defer mutex.Unlock()

		return call(args)
	}

	return context.Using(lockContextProvider)
}