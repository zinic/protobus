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
	log.Info("Starting Example Bus")
	mainBus := bus.NewProtoBus("main")

	// Register our ZeroMQ source and sink. Binding to either allows for remote communication.
	mainBus.RegisterActor("zeromq::sender", zeromq.DefaultZMQSink())
	mainBus.RegisterActor("zeromq::server", zeromq.DefaultZMQGetSource())

	// A testing source that injects events every second. First we create the event we want to inject
	eventPayload := &zeromq.Message {Destination: "tcp://127.0.0.1:5555", Contents: "testing"}
	injectorEvent := bus.NewEvent("testing::injector", eventPayload)

	// Next we create the actual injector source and then register it
	injectorSource := testing.Injector(injectorEvent, 1 * time.Second)
	mainBus.RegisterActor("testing::injector", injectorSource)

	// Register Unix signal handling for catching interrupts and shutting down. Using the
	// SimpleSink function allows us to create a sink without having to have the additional
	// fanfare methods specified such as Init and Shutdown.
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

	mainBus.RegisterActor("unix::signal_source", &unix.SignalSource{})
	mainBus.RegisterActor("main::signal_sink", mainSignalSink)

	// Register a debug sink for printing out messages caught
	mainDebugSink := bus.SimpleSink(func(event bus.Event) {
		log.Infof("Caught event %v", event)
	})
	mainBus.RegisterActor("main::debug", mainDebugSink)

	// Create our bus bindings
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

	// Register the bindings
	for source, sinks := range bindings {
		for _, sink := range sinks {
			if err := mainBus.Bind(source, sink); err != nil {
				log.Errorf("Failed during bind of source %s to sink %s: %v", source, sink, err)
			}
		}
	}

	// Start the bus and wait for it to exit
	mainBus.Start()
	mainBus.Join()
}