package main

import (
	"os"
	"syscall"

	"github.com/zinic/gbus/bus"
	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/sources/unix"
	"github.com/zinic/gbus/sources/testing"
)

type SignalSink struct {
	controllerBus bus.Bus
}

func (uss *SignalSink) Init(actx bus.ActorContext) (err error) {
	return
}

func (uss *SignalSink) Shutdown() (err error) {
	return
}

func (uss *SignalSink) Push(message bus.Message) {
	msgPayload := message.Payload()
	if sig, typeOk := msgPayload.(os.Signal); typeOk {
		switch sig {
			case os.Interrupt:
				uss.controllerBus.Shutdown()

			case syscall.SIGTERM:
				uss.controllerBus.Shutdown()
		}
	}
}

func main() {
	log.Info("Starting GBus")

	mainBus := bus.NewGBus("main")

	mainBus.RegisterActor("sampler", &testing.Sampler{})
	mainBus.RegisterActor("unix::signal_source", &unix.SignalSource{})
	mainBus.RegisterActor("main::signal_sink", &SignalSink {
		controllerBus: mainBus,
	})

	// mainBus.Bind("sampler", "sampler")

	if err := mainBus.Bind("unix::signal_source", "main::signal_sink"); err == nil {
		mainBus.Start()
		mainBus.Join()
	} else {
		log.Errorf("Failed to bind signal source to sink. Reason: %v", err)
	}
}