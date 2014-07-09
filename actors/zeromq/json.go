package zeromq

import (
	"encoding/json"
)

func JSONMessageMarshaller(value interface{}) (data []byte, err error) {
	return json.Marshal(value)
}

type JSONMessageUnmarshaller struct {
	msgBuffer []byte
	dataTreeDepth int
}

func (jm *JSONMessageUnmarshaller) Unmarshall(data []byte, value interface{}) (unmarshalled bool, err error) {
	unmarshalled = false

	var idx int
	for idx = 0; idx < len(data); idx++ {
		switch data[idx] {
			case '{':
				jm.dataTreeDepth += 1
			case '}':
				jm.dataTreeDepth -= 1
		}

		if jm.dataTreeDepth == 0 {
			idx += 1
			break
		}
	}

	if jm.dataTreeDepth == 0 {
		jm.msgBuffer = append(jm.msgBuffer, data[:idx]...)

		if err = json.Unmarshal(jm.msgBuffer, value); err == nil {
			unmarshalled = true
		}

		jm.msgBuffer = data[idx:]
	}

	return
}
