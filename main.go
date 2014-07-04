package main

import (
	"os"
	"syscall"

	"github.com/zinic/gbus/bus"
	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/wiring"
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
	mainBus.RegisterActor("printer", bus.SimpleSink(func (message bus.Message) {
		log.Infof("Got message: %v", message)
	}))

	mainBus.RegisterActor("splitter", wiring.NewPipe(1024))
	mainBus.RegisterActor("unix::signal_source", &unix.SignalSource{})
	mainBus.RegisterActor("main::signal_sink", &SignalSink {
		controllerBus: mainBus,
	})

	if err := mainBus.Bind("unix::signal_source", "splitter"); err != nil {
		log.Errorf("Failed during bind: %v", err)
	}

	if err := mainBus.Bind("splitter", "printer"); err != nil {
		log.Errorf("Failed during bind: %v", err)
	}

	if err := mainBus.Bind("splitter", "main::signal_sink"); err != nil {
		log.Errorf("Failed during bind: %v", err)
	}

	// mainBus.Bind("sampler", "sampler")

	mainBus.Start()
	mainBus.Join()
}