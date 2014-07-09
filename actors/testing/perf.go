package testing

import (
	"time"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/log"
)

type Sampler struct {
	event bus.Event
	msgCount int64
	startupTime time.Time
}

func (sampler *Sampler) Init(actx bus.ActorContext) (err error) {
	sampler.msgCount = 0
	sampler.startupTime = time.Now()
	sampler.event = bus.NewEvent("perf-test", "")

	return
}

func (sampler *Sampler) Shutdown() (err error) {
	elapsed := time.Now().Sub(sampler.startupTime)
	processed := float64(sampler.msgCount) / elapsed.Seconds()

	log.Infof("Processed %d messages in %d seconds for %d messages per second.", sampler.msgCount, elapsed.Seconds(), processed)
	return
}

func (sampler *Sampler) Push(event bus.Event) {
	sampler.msgCount++
	return
}

func (sampler *Sampler) Pull() (reply bus.Event) {
	return sampler.event
}