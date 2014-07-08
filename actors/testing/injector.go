package testing

import (
	"time"

	"github.com/zinic/gbus/bus"
)

func Injector(event bus.Event, injectionInterval time.Duration) (source bus.Source) {
	lastInjection := time.Now()

	return bus.SimpleSource(func() (reply bus.Event) {
		now := time.Now()
		difference := now.Sub(lastInjection)

		if difference.Nanoseconds() > injectionInterval.Nanoseconds() {
			lastInjection = now
			reply = event
		}

		return
	})
}