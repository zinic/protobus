package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/zinic/gbus/bus"
)

type UnixSignalSource struct {
	signalChannel chan os.Signal
}

func (uss *UnixSignalSource) Init(actx bus.ActorContext) (err error) {
	uss.signalChannel = make(chan os.Signal, 32)
	signal.Notify(uss.signalChannel, os.Interrupt, syscall.SIGTERM)
	return
}

func (uss *UnixSignalSource) Close() (err error) {
	close(uss.signalChannel)
	return
}

func (uss *UnixSignalSource) Pull() (reply bus.Event) {
	select {
		case sig := <- uss.signalChannel:
			reply = bus.NewEvent("signal", sig)
	}
	return
}

type UnixSignalSink struct {
	controllerBus bus.Bus
}

func (uss *UnixSignalSink) Init(actx bus.ActorContext) (err error) {
	return
}

func (uss *UnixSignalSink) Close() (err error) {
	return
}

func (uss *UnixSignalSink) Push(message bus.Message) (reply bus.Event) {
	msgPayload := message.Payload()
	if sig, typeOk := msgPayload.(os.Signal); typeOk {
		switch sig {
			case os.Interrupt:
				uss.controllerBus.Stop()

			case syscall.SIGTERM:
				uss.controllerBus.Stop()
		}
	}

	return
}

func main() {
	mainBus := bus.NewGBus("main")
	mainBus.RegisterSource("unix_signal_source", &UnixSignalSource{})
	mainBus.RegisterSink("main_unix_signal_sink", &UnixSignalSink {
		controllerBus: mainBus,
	})

	mainBus.Start()
	mainBus.Join()
}