package wiring

import (
	"github.com/zinic/gbus/bus"
)

func NewPipe(maxBuffered int) (actor bus.Actor) {
	return &Pipe {
		msgBuffer: make(chan bus.Message, maxBuffered),
	}
}

type Pipe struct {
	msgBuffer chan bus.Message
}

func (pipe *Pipe) Init(actx bus.ActorContext) (err error) {
	pipe.msgBuffer = make(chan bus.Message, 1024)
	return
}

func (pipe *Pipe) Shutdown() (err error) {
	close(pipe.msgBuffer)
	return
}

func (pipe *Pipe) Push(message bus.Message) {
	pipe.msgBuffer <- message
}

func (pipe *Pipe) Pull() (reply bus.Event) {
	select {
		case msg := <- pipe.msgBuffer:
			if msg != nil {
				reply = bus.NewEvent(msg.Action(), msg.Payload())
			}

		default:
	}

	return
}