package main

import (
	"os"
	"time"
	"syscall"

	"github.com/zinic/gbus/bus"
	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/actors/unix"
	"github.com/zinic/gbus/actors/zeromq"
	"github.com/zinic/gbus/actors/testing"
)

func main() {
	log.Info("Starting GBus")
	mainBus := bus.NewGBus("main")

	mainSignalSink := bus.SimpleSink(func(event bus.Event) {
		payload := event.Payload()
		if sig, typeOk := payload.(os.Signal); typeOk {
			switch sig {
				case os.Interrupt:
					mainBus.Shutdown()

				case syscall.SIGTERM:
					mainBus.Shutdown()
			}
		}
	})

	mainDebugSink := bus.SimpleSink(func(event bus.Event) {
		log.Infof("Caught event %v", event)
	})

	injectorEvent := bus.NewEvent("testing::injector", &zeromq.Message {Destination: "tcp://127.0.0.1:5555", Contents: "testing"})
	injectorSource := testing.Injector(injectorEvent, 1 * time.Second)

	mainBus.RegisterActor("zeromq::sender", zeromq.DefaultZMQSink())
	mainBus.RegisterActor("zeromq::server", zeromq.DefaultZMQGetSource())
	mainBus.RegisterActor("testing::injector", injectorSource)
	mainBus.RegisterActor("unix::signal_source", &unix.SignalSource{})
	mainBus.RegisterActor("main::signal_sink", mainSignalSink)
	mainBus.RegisterActor("main::debug", mainDebugSink)

	bindings := map[string][]string {
		"unix::signal_source": []string {
			"main::signal_sink",
		},
		"zeromq::server": []string {
			"main::debug",
		},
		"testing::injector": []string {
			"zeromq::sender",
		},
	}

	for source, sinks := range bindings {
		for _, sink := range sinks {
			if err := mainBus.Bind(source, sink); err != nil {
				log.Errorf("Failed during bind of source %s to sink %s: %v", source, sink, err)
			}
		}
	}

	mainBus.Start()
	mainBus.Join()
}