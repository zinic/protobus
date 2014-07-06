package main

import (
	"os"
	"strings"
	"syscall"
	"runtime/pprof"

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

	if err := mainBus.Bind("unix::signal_source", "main::signal_sink"); err != nil {
		log.Errorf("Failed during bind: %v", err)
	}

	if err := mainBus.Bind("sampler", "sampler"); err != nil {
		log.Errorf("Failed during bind: %v", err)
	}

	if strings.ToLower(os.Getenv("PROFILE")) == "true" {
		if profileFile, err := os.Create("./gbus.prof"); err == nil {
			pprof.StartCPUProfile(profileFile)
			defer pprof.StopCPUProfile()
		} else {
			log.Errorf("Failed to create profiler file: %v", err)
		}
	}

	mainBus.Start()
	mainBus.Join()
}