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

type SocketManager struct {
	sockets map[string]*zmq.Socket
	socketsContext context.Context
	socketWorkerGroup *concurrent.TaskGroup
}

func (sm *SocketManager) pushSocket(addr string) (socket *zmq.Socket, err error) {
	if addr != "" {
		sm.socketsContext(func() {
			if socket = sm.sockets[addr]; socket == nil {
				if socket, err = zmq.NewSocket(zmq.PUSH); err == nil {
					err = socket.Connect(addr)
				}
			}
		})
	} else {
		err = fmt.Errorf("Destination specified for message is empty.")
	}

	return
}

func DefaultZMQSink() (sink bus.Sink) {
	return NewZMQSink(JSONMessageMarshaller)
}

func NewZMQSink(marshaller MessageMarshaller) (sink bus.Sink) {
	return &ZMQSink {
		SocketManager {
			sockets: make(map[string]*zmq.Socket),
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
	return
}

func (zmqs *ZMQSink) Shutdown() (err error) {
	zmqs.socketsContext(func() {
		for _, sock := range zmqs.sockets {
			sock.Close()
		}

		zmqs.sockets = make(map[string]*zmq.Socket)
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
				if socket, err := zmqs.pushSocket(message.Destination); err == nil {
					go func() {
						for sent := 0; sent < len(data); {
							if written, err := socket.SendBytes(data[sent:], 0); err == nil {
								sent += written
							} else {
								log.Errorf("ZMQ send failed: %v", err)
								break
							}
						}
					}()
				} else {
					log.Errorf("Failed to get socket for %s: %v", message.Destination, err)
				}
			} else {
				log.Errorf("Failed to JSON encode %v: %v", output, err)
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