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

func (uss *SignalSource) Init(actx bus.ActorContext) (err error) {
	log.Debug("Initializing SignalSource.")

	uss.signalChannel = make(chan os.Signal, 1)
	signal.Notify(uss.signalChannel, os.Interrupt, syscall.SIGTERM)
	return
}

func (uss *SignalSource) Shutdown() (err error) {
	close(uss.signalChannel)
	return
}

func (uss *SignalSource) Pull() (reply bus.Event) {
	select {
		case sig := <- uss.signalChannel:
			if sig != nil {
				reply = bus.NewEvent("signal", sig)
			}

		default:
	}
	return
}