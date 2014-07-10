package bus

type Message struct {
	Contents string
	Source string
	Destination string
}

type MessageMarshaller func(value interface{}) (data []byte, err error)
type MessageUnmarshaller func(data []byte, value interface{}) (unmarshalled bool, err error)