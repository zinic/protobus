package testing

import (
	"time"

	"github.com/zinic/gbus/bus"
	"github.com/zinic/gbus/log"
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
	elapsed := time.Now().Sub(sampler.startupTime) * time.Second
	processed := sampler.msgCount / int64(elapsed)

	log.Infof("Processed %n messages in %n seconds.", processed, elapsed)
	return
}

func (sampler *Sampler) Push(message bus.Message) {
	sampler.msgCount++
	return
}

func (sampler *Sampler) Pull() (reply bus.Event) {
	return sampler.event
}