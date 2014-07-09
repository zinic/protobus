package zeromq

import (
	"fmt"
	"syscall"
	zmq "github.com/pebbe/zmq4"

	"github.com/zinic/gbus/bus"
	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/context"
	"github.com/zinic/gbus/concurrent"
)

type Message struct {
	Contents string
	Source string
	Destination string
}

type MessageMarshaller func(value interface{}) (data []byte, err error)
type MessageUnmarshaller func(data []byte, value interface{}) (unmarshalled bool, err error)

type SocketContext struct {
	socket *zmq.Socket
	workChannel chan []byte
}

type SocketManager struct {
	sockets map[string]*SocketContext
	activeSockets []string
	socketsContext context.Context
	socketWorkerGroup *concurrent.TaskGroup
}

func (sm *SocketManager) SetInactive(addr string) {
	var idx int
	for idx = 0; idx < len(sm.activeSockets); idx++ {
		if sm.activeSockets[idx] == addr {
			sm.activeSockets = append(sm.activeSockets[:idx], sm.activeSockets[idx+1:]...)
			break
		}
	}
}

func (sm *SocketManager) SetActive(addr string) {
	sm.activeSockets = append(sm.activeSockets, addr)
}

func (sm *SocketManager) Active(addr string) (active bool) {
	active = false
	for _, activeAddr := range sm.activeSockets {
		if activeAddr == addr {
			active = true
			break
		}
	}

	return
}

func (sm *SocketManager) send(socket *zmq.Socket, data []byte) (err error) {
	for sent := 0; sent < len(data); {
		if written, err := socket.SendBytes(data[sent:], 0); err == nil {
			sent += written
		} else {
			log.Errorf("ZMQ send failed: %v", err)
			break
		}
	}

	return
}

func (sm *SocketManager) Send(dst string, data []byte) (err error) {
	sm.socketsContext(func() {
		socketCtx := sm.sockets[dst]

		if socketCtx == nil {
			var socket *zmq.Socket

			if socket, err = zmq.NewSocket(zmq.PUSH); err == nil {
				if err = socket.Connect(dst); err == nil {
					socketCtx = &SocketContext {
						socket: socket,
						workChannel: make(chan []byte, 4096),
					}
				}
			}
		}

		if socketCtx != nil {
			socketCtx.workChannel <- data

			if !sm.Active(dst) {
				sm.SetActive(dst)

				var socketSend func() (err error)
				socketSend = func() (err error) {
					select {
						case event := <- socketCtx.workChannel:
							sm.send(socketCtx.socket, event)
							sm.socketWorkerGroup.Schedule(socketSend)

						default:
							sm.SetInactive(dst)
					}

					return
				}

				sm.socketWorkerGroup.Schedule(socketSend)
			}
		} else {	log.Infof("Sending %v %v", dst, data)

			err = fmt.Errorf("No destination %s found.", dst)
		}
	})

	return
}

func DefaultZMQSink() (sink bus.Sink) {
	return NewZMQSink(JSONMessageMarshaller)
}

func NewZMQSink(marshaller MessageMarshaller) (sink bus.Sink) {
	return &ZMQSink {
		SocketManager {
			sockets: make(map[string]*SocketContext),
			socketsContext: context.NewLockerContext(),
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
	marshaller MessageMarshaller
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

func (zmqs *ZMQSink) Push(event bus.Event) {
	payload := event.Payload()

	if payload != nil {
		if message, typeOk := payload.(*Message); typeOk {
			output := struct {
				Action interface{}
				Payload interface{}
			}{
				Action: event.Action(),
				Payload: message,
			}

			if data, err := zmqs.marshaller(output); err == nil {
				zmqs.Send(message.Destination, data)
			} else {
				log.Errorf("Failed to encode %v: %v", output, err)
			}
		}
	}
}

func DefaultZMQGetSource() (source bus.Source) {
	unmarshaller := &JSONMessageUnmarshaller{}
	return NewZMQSource(unmarshaller.Unmarshall)
}

func NewZMQSource(unmarshaller MessageUnmarshaller) (source bus.Source) {
	return &ZMQSource {
		unmarshaller: unmarshaller,
	}
}

type ZMQSource struct {
	socket *zmq.Socket
	unmarshaller MessageUnmarshaller
}

func (zmqs *ZMQSource) Init(actx bus.ActorContext) (err error) {
	if zmqs.socket, err = zmq.NewSocket(zmq.PULL); err == nil {
		err = zmqs.socket.Bind("tcp://127.0.0.1:5555")
	} else {
		log.Errorf("Failed to init ZMQ PULL socket: %v", err)
	}

	return
}

func (zmqs *ZMQSource) Shutdown() (err error) {
	zmqs.socket.Close()
	return
}

func (zmqs *ZMQSource) Pull() (reply bus.Event) {
	if recvStr, err := zmqs.socket.Recv(zmq.DONTWAIT); err != nil {
		if err != syscall.EAGAIN {
			log.Errorf("Failed to recieve from ZMQ PULL socket: %v", err)
		}
	} else {
		var event struct {
			Action interface{}
			Payload *Message
		}

		if unmarshalled, err := zmqs.unmarshaller([]byte(recvStr), &event); err == nil && unmarshalled {
			reply = bus.NewEvent(event.Action, event.Payload)
		} else if err != nil {
			log.Debugf("Failed to unmarshal ZMQ message: %v", err)
		}
	}

	return
}