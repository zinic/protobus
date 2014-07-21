package zeromq

import (
	"fmt"
	"time"
	"syscall"

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
		SocketManager: SocketManager {
			sockets: make(map[string]*SocketContext),
			socketsContext: concurrent.NewLockerContext(),
			socketWorkerGroup: concurrent.NewTaskGroup(&concurrent.TaskGroupConfig {
				Name: "zmq-workers",
				MaxQueuedTasks: 4096,
				MaxActiveWorkers: 4,
			}),
		},
		marshaller: marshaller,
	}
}

type ZMQSink struct {
	SocketManager
	marshaller bus.MessageMarshaller
}

func (zmqs *ZMQSink) Start(incoming <-chan bus.Event, actx bus.ActorContext) (err error) {
	zmqs.socketWorkerGroup.Start()

	for {
		if event, ok := <- incoming; ok {
			zmqs.push(event)
		} else {
			break
		}
	}
	return
}

func (zmqs *ZMQSink) Stop() (err error) {
	zmqs.socketWorkerGroup.Stop()
	zmqs.socketWorkerGroup.Join(5 * time.Second)

	zmqs.socketsContext(func() {
		for _, sockCtx := range zmqs.sockets {
			sockCtx.socket.Close()
		}

		zmqs.sockets = make(map[string]*SocketContext)
	})
	return
}

func (zmqs *ZMQSink) push(event bus.Event) (err error) {
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

func DefaultZMQSource(bindAddr string) (source bus.Source) {
	unmarshaller := &JSONMessageUnmarshaller{}
	return NewZMQSource(bindAddr, unmarshaller.Unmarshall)
}

func NewZMQSource(bindAddr string, unmarshaller bus.MessageUnmarshaller) (source bus.Source) {
	return &ZMQSource {
		bindAddr: bindAddr,
		active: concurrent.NewReferenceLocker(false),
		unmarshaller: unmarshaller,
	}
}

type ZMQSource struct {
	bindAddr string
	received []byte
	socket *zmq.Socket
	active concurrent.ReferenceLocker
	unmarshaller bus.MessageUnmarshaller
}

func (zmqs *ZMQSource) loop(outgoing chan<- bus.Event) (err error) {
	var lastSleepDuration time.Duration = 1

	for zmqs.active.Get().(bool) {
		if received, err := zmqs.socket.Recv(zmq.DONTWAIT); err != nil {
			if err == syscall.EAGAIN {
				if lastSleepDuration < 10 {
					lastSleepDuration += 1
				}

				time.Sleep(lastSleepDuration * time.Millisecond)
			} else {
				err = fmt.Errorf("Failed to recieve from ZMQ PULL socket: %v", err)
				break
			}
		} else 	{
			lastSleepDuration  = 1

			var event struct {
				Action interface{}
				Payload bus.Message
			}

			if unmarshalled, err := zmqs.unmarshaller([]byte(received), &event); err != nil {
				log.Errorf("Failed to unmarshal ZMQ message: %v", err)
			} else if unmarshalled {
				outgoing <- bus.NewEvent(event.Action, event.Payload)
			}
		}
	}

	err = zmqs.socket.Close()
	return
}

func (zmqs *ZMQSource) Start(outgoing chan<- bus.Event, actx bus.ActorContext) (err error) {
	zmqs.active.Set(true)

	if zmqs.socket, err = zmq.NewSocket(zmq.PULL); err == nil {
		if err = zmqs.socket.Bind(zmqs.bindAddr); err == nil {
			zmqs.loop(outgoing)
		}
	}

	return
}

func (zmqs *ZMQSource) Stop() (err error) {
	zmqs.active.Set(false)
	zmqs.socket.Close()
	return
}