package testing

import (
	"time"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/concurrent"
)

func NewInjector(event bus.Event, interval time.Duration) (injector bus.Source) {
	return &InjectorSource {
		event: event,
		running: concurrent.NewReferenceLocker(false),
		interval: interval,
	}
}

type InjectorSource struct {
	event bus.Event
	running concurrent.ReferenceLocker
	interval time.Duration
}

func (injector *InjectorSource) Start(outgoing chan bus.Event, actx bus.ActorContext) (err error) {
	injector.running.Set(true)

	var elapsedNanos int64 = 0
	for injector.running.Get().(bool) {
		then := time.Now()

		if elapsedNanos >= injector.interval.Nanoseconds() {
			elapsedNanos = 0
			outgoing <- injector.event
		}

		time.Sleep(1 * time.Millisecond)
		elapsedNanos += time.Now().Sub(then).Nanoseconds()
	}

	return
}

func (injector *InjectorSource) Stop() (err error) {
	injector.running.Set(false)
	return
}