package bus

// Interfaces
type Event interface {
	Action() (action interface{})
	Payload() (payload interface{})
}

func NewEvent(action interface{}, payload interface{}) (event Event) {
	return &ProtoEvent {
		action: action,
		payload: payload,
	}
}

// Implementation
type ProtoEvent struct {
	action interface{}
	payload interface{}
}

func (pe *ProtoEvent) Action() (action interface{}) {
	return pe.action
}

func (pe *ProtoEvent) Payload() (payload interface{}) {
	return pe.payload
}
