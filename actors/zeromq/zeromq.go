package zeromq

import (
	"fmt"

	zmq "github.com/pebbe/zmq4"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/context"
	"github.com/zinic/protobus/concurrent"
)

type SocketContext struct {
	socket *zmq.Socket
	active concurrent.ReferenceLocker
	workChannel chan []byte
}

type SocketManager struct {
	sockets map[string]*SocketContext
	socketsContext context.Context
	socketWorkerGroup *concurrent.TaskGroup
}

func (sm *SocketManager) socket(dst string) (ctx *SocketContext, err error) {
	sm.socketsContext(func() {
		ctx = sm.sockets[dst]

		if ctx == nil {
			var socket *zmq.Socket

			if socket, err = zmq.NewSocket(zmq.PUSH); err == nil {
				if err = socket.Connect(dst); err == nil {
					ctx = &SocketContext {
						socket: socket,
						active: concurrent.NewReferenceLocker(false),
						workChannel: make(chan []byte, 1024),
					}

					sm.sockets[dst] = ctx
				}
			}
		}
	})

	return
}

func send(ctx *SocketContext, tg *concurrent.TaskGroup) {
	select {
		case data := <- ctx.workChannel:
			for sent := 0; sent < len(data); {
				if written, err := ctx.socket.SendBytes(data[sent:], 0); err == nil {
					sent += written
				} else {
					log.Errorf("ZMQ send failed: %v", err)
					break
				}
			}

			tg.Schedule(send, ctx, tg)

		default:
			ctx.active.Set(false)
	}

	return
}

func (sm *SocketManager) Send(dst string, data []byte) (err error) {
	var socketCtx *SocketContext

	if socketCtx, err = sm.socket(dst); err == nil {
		socketCtx.workChannel <- data

		if !socketCtx.active.Get().(bool) {
			socketCtx.active.Set(true)

			sm.socketWorkerGroup.Schedule(send, socketCtx, sm.socketWorkerGroup)
		}
	}

	return
}

func DefaultZMQSink() (sink bus.Sink) {
	return NewZMQSink(JSONMessageMarshaller)
}

func NewZMQSink(marshaller bus.MessageMarshaller) (sink bus.Sink) {
	return &ZMQSink {
		SocketManager {
			sockets: make(map[string]*SocketContext),
			socketsContext: concurrent.NewLockerContext(),
			socketWorkerGroup: concurrent.NewTaskGroup(&concurrent.TaskGroupConfig {
				Name: "zmq-workers",
				MaxQueuedTasks: 4096,
				MaxActiveWorkers: 4,
			}),
		},
		marshaller,
	}
}

type ZMQSink struct {
	SocketManager
	marshaller bus.MessageMarshaller
}

func (zmqs *ZMQSink) Init(actx bus.ActorContext) (err error) {
	zmqs.socketWorkerGroup.Start()

	return
}

func (zmqs *ZMQSink) Shutdown() (err error) {
	zmqs.socketWorkerGroup.Stop()
	zmqs.socketWorkerGroup.Join()

	zmqs.socketsContext(func() {
		for _, sockCtx := range zmqs.sockets {
			sockCtx.socket.Close()
		}

		zmqs.sockets = make(map[string]*SocketContext)
	})
	return
}

func (zmqs *ZMQSink) Push(event bus.Event) (err error) {
	payload := event.Payload()

	if payload != nil {
		if message, typeOk := payload.(*bus.Message); typeOk {
			output := struct {
				Action interface{}
				Payload interface{}
			}{
				Action: event.Action(),
				Payload: message,
			}

			var data []byte
			if data, err = zmqs.marshaller(output); err == nil {
				zmqs.Send(message.Destination, data)
			} else {
				err = fmt.Errorf("Failed to encode %v: %v", output, err)
			}
		}
	}

	return
}

func DefaultZMQSource() (source bus.Source) {
	unmarshaller := &JSONMessageUnmarshaller{}
	return NewZMQSource(unmarshaller.Unmarshall)
}

func NewZMQSource(unmarshaller bus.MessageUnmarshaller) (source bus.Source) {
	return &ZMQSource {
		unmarshaller: unmarshaller,
	}
}

type ZMQSource struct {
	received []byte
	socket *zmq.Socket
	unmarshaller bus.MessageUnmarshaller
}

func (zmqs *ZMQSource) Start(outgoing chan bus.Event, actx bus.ActorContext) (err error) {
	var socket *zmq.Socket
	if socket, err = zmq.NewSocket(zmq.PULL); err != nil {
		if err = socket.Bind("tcp://127.0.0.1:5555"); err != nil {
			for {
				if received, err := socket.Recv(0); err != nil {
					log.Errorf("Failed to recieve from ZMQ PULL socket: %v", err)
				} else 	{
					var event bus.Event
					if unmarshalled, err := zmqs.unmarshaller([]byte(received), &event); err != nil {
						log.Debugf("Failed to unmarshal ZMQ message: %v", err)
					} else if unmarshalled {
						outgoing <- bus.NewEvent(event.Action, event.Payload)
					}
				}
			}
		}
	}

	return
}

func (zmqs *ZMQSource) Stop() (err error) {
	return
}