package main

import (
	"os"
	"time"
	"syscall"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/actors/unix"
	"github.com/zinic/protobus/actors/zeromq"
	"github.com/zinic/protobus/actors/testing"
)

func main() {
	log.Info("Starting Example Bus")
	mainBus := bus.NewProtoBus("main")

	// Register our ZeroMQ source and sink. Binding to either allows for remote communication.
	mainBus.RegisterSink("zeromq::sender", zeromq.DefaultZMQSink())
	mainBus.RegisterSource("zeromq::server", zeromq.DefaultZMQSource("tcp://127.0.0.1:5555"))

	// A testing source that injects events every second. First we create the event we want to inject
	eventPayload := &bus.Message {Destination: "tcp://127.0.0.1:5555", Contents: "testing"}
	injectorEvent := bus.NewEvent("testing::injector", eventPayload)

	// Next we create the actual injector sources and then register them
	mainBus.RegisterSource("testing::injector_a", testing.NewInjector(injectorEvent, 1 * time.Second))
	mainBus.RegisterSource("testing::injector_b", testing.NewInjector(injectorEvent, 2 * time.Second))
	mainBus.RegisterSource("testing::injector_c", testing.NewInjector(injectorEvent, 100 * time.Millisecond))
	mainBus.RegisterSource("testing::injector_d", testing.NewInjector(injectorEvent, 500 * time.Millisecond))
	mainBus.RegisterSource("testing::injector_e", testing.NewInjector(injectorEvent, 5 * time.Second))
	mainBus.RegisterSource("testing::injector_f", testing.NewInjector(injectorEvent, 15 * time.Second))

	// Register Unix signal handling for catching interrupts and shutting down. Using the
	// SimpleSink function allows us to create a sink without having to have the additional
	// fanfare methods specified such as Init and Shutdown.
	mainSignalSink := bus.NewSimpleSink(func(incoming <-chan bus.Event, ctx bus.ActorContext) (err error) {
		for {
			if event, ok := <-incoming; ok {
				if event.Payload() != nil {
					if sig, typeOk := event.Payload().(os.Signal); typeOk {
						switch sig {
							case os.Interrupt:
								mainBus.Shutdown()

							case syscall.SIGTERM:
								mainBus.Shutdown()
						}
					}
				}
			} else {
				break
			}
		}

		return
	})

	mainBus.RegisterSource("unix::signal_source", &unix.SignalSource{})
	mainBus.RegisterSink("main::signal_sink", mainSignalSink)

	// Register a debug sink for printing out messages caught
	mainDebugSink := bus.NewSimpleSink(func(incoming <-chan bus.Event, ctx bus.ActorContext) (err error) {
		for {
			if event, ok := <-incoming; ok {
				log.Infof("Caught event %v", event)
			} else {
				break
			}
		}
		return
	})
	mainBus.RegisterSink("main::debug", mainDebugSink)

	// Create our bus bindings
	bindings := map[string][]string {
		"unix::signal_source": []string {
			"main::signal_sink",
		},
		"zeromq::server": []string {
			"main::debug",
		},
		"testing::injector_a": []string {
			"zeromq::sender",
		},
		"testing::injector_b": []string {
			"zeromq::sender",
		},
		"testing::injector_c": []string {
			"zeromq::sender",
		},
		"testing::injector_d": []string {
			"zeromq::sender",
		},
		"testing::injector_e": []string {
			"zeromq::sender",
		},
		"testing::injector_f": []string {
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
	mainBus.Wait()
}