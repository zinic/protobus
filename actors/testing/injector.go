package testing

import (
	"time"

	"github.com/zinic/protobus/bus"
)

func Injector(event bus.Event, injectionInterval time.Duration) (source bus.Source) {
	return bus.NewSimpleSource(func(outgoing chan bus.Event, actx bus.ActorContext) (err error) {
		for {
			outgoing <- event
			time.Sleep(injectionInterval)
		}

		return
	})
}