package main

import (
	"os"
	"syscall"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/actors/unix"
	"github.com/zinic/protobus/actors/zeromq"
	"github.com/zinic/protobus/actors/router"
)

func main() {
	log.Info("Starting Example Bus")
	mainBus := bus.NewProtoBus("main")

	// Register our ZeroMQ source and sink. Binding to either allows for remote communication.
	mainBus.RegisterSink("zeromq::sender", zeromq.DefaultZMQSink())
	mainBus.RegisterSource("zeromq::server", zeromq.DefaultZMQSource("tcp://127.0.0.1:4545"))

	if _, err := mainBus.RegisterSource("router::server", router.NewRouter("tcp://127.0.0.1:4545", []string{"tcp://127.0.0.1"})); err != nil {
		log.Errorf("Failed: %v\n", err)
	}

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

	// Create our bus bindings
	bindings := map[string][]string {
		"unix::signal_source": []string {
			"main::signal_sink",
		},
		"router::server": []string {
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