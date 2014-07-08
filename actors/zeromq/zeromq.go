package zeromq

import (
	"fmt"
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
		zmqs.socket = nil
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
			octetCount := fmt.Sprintf("%d", len(b))
			b = append([]byte(octetCount), b...)

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
		zmqs.socket = nil
	}

	return
}

func (zmqs *ZMQSource) Shutdown() (err error) {
	zmqs.socket.Close()
	return
}

func (zmqs *ZMQSource) Pull() (reply bus.Event) {
	if recvStr, err := zmqs.socket.Recv(zmq.DONTWAIT); err != nil {
		log.Errorf("Magic failure: %v", err)
	} else if len(recvStr) > 0 {
		data := []byte(recvStr)

		idx := 0
		for idx < len(data) {
			switch data[idx] {
				case '{':
					zmqs.jsonTreeDepth += 1
				case '}':
					zmqs.jsonTreeDepth -= 1
			}

			if zmqs.jsonTreeDepth == 0 {
				break
			}
		}

		if zmqs.jsonTreeDepth == 0 {
			var input struct {
				Action interface{}
				Payload interface{}
			}

			zmqs.msgBuffer = append(zmqs.msgBuffer, data[:idx]...)

			if err = json.Unmarshal(zmqs.msgBuffer, input); err == nil {
				reply = bus.NewEvent(input.Action, input.Payload)
			}

			zmqs.msgBuffer = data[idx:]
		}
	}

	return
}