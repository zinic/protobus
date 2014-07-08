package zeromq

import (
	"syscall"
	"encoding/json"
	zmq "github.com/pebbe/zmq4"

	"github.com/zinic/gbus/bus"
	"github.com/zinic/gbus/log"
)

type ZMQSink struct {
	socket *zmq.Socket
}

func (zmqs *ZMQSink) Init(actx bus.ActorContext) (err error) {
	if zmqs.socket, err = zmq.NewSocket(zmq.PUSH); err == nil {
		zmqs.socket.Connect("tcp://127.0.0.1:5555")
	} else {
		log.Errorf("Failed to init ZMQ PUSH socket: %v", err)
	}

	return
}

func (zmqs *ZMQSink) Shutdown() (err error) {
	zmqs.socket.Close()
	return
}

func (zmqs *ZMQSink) Push(message bus.Message) {
	output := struct {
		Action interface{}
		Payload interface{}
	}{
		Action: message.Action(),
		Payload: message.Payload(),
	}

	if json, err := json.Marshal(output); err == nil {
		go func(b []byte, socket *zmq.Socket) {
			for sent := 0; sent < len(b); {
				if written, err := socket.SendBytes(b[sent:], 0); err == nil {
					sent += written
				} else {
					log.Errorf("ZMQ send failed: %v", err)
					break
				}
			}
		}(json, zmqs.socket)
	} else {
		log.Errorf("Failed to JSON encode %v: %v", output, err)
	}
}

func NewZMQSource() (zmqs ZMQSource) {
	return ZMQSource {
		msgBuffer: make([]byte, 0),
	}
}

type ZMQSource struct {
	socket *zmq.Socket
	msgBuffer []byte
	jsonTreeDepth int
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
		data := []byte(recvStr)

		var idx int
		for idx = 0; idx < len(data); idx++ {
			switch data[idx] {
				case '{':
					zmqs.jsonTreeDepth += 1
				case '}':
					zmqs.jsonTreeDepth -= 1
			}

			if zmqs.jsonTreeDepth == 0 {
				idx += 1
				break
			}
		}

		if zmqs.jsonTreeDepth == 0 {
			var input struct {
				Action interface{}
				Payload interface{}
			}

			zmqs.msgBuffer = append(zmqs.msgBuffer, data[:idx]...)

			if err = json.Unmarshal(zmqs.msgBuffer, &input); err == nil {
				reply = bus.NewEvent(input.Action, input.Payload)
			} else {
				log.Errorf("Failed to parse JSON message: %v", err)
			}

			zmqs.msgBuffer = data[idx:]
		}
	}

	return
}