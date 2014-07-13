package unix

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/log"
)


type SignalSource struct {
	signalChannel chan os.Signal
}

func (uss *SignalSource) Start(outgoing chan bus.Event, actx bus.ActorContext) (err error) {
	log.Debug("Initializing SignalSource.")

	uss.signalChannel = make(chan os.Signal, 1)
	signal.Notify(uss.signalChannel, os.Interrupt, syscall.SIGTERM)

	for {
		if sig, ok := <- uss.signalChannel; ok {
			outgoing <- bus.NewEvent("unix::signal", sig)
		} else {
			break
		}
	}

	return
}

func (uss *SignalSource) Stop() (err error) {
	signal.Stop(uss.signalChannel)
	close(uss.signalChannel)
	return
}