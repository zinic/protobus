package bus

import (
	"github.com/nu7hatch/gouuid"
	"github.com/zinic/gbus/concurrent"
)

type Event interface {
	Action() (action interface{})
	Payload() (payload interface{})
}

type Message interface {
	Event
	Source() (source *uuid.UUID)
}


// ===============
type ActorContext interface {
	Bus() (bus Bus)
}

type Actor interface {
	Init(actx ActorContext) (err error)
	Close() (err error)
}

type Source interface {
	Actor
	Pull() (reply Event)
}

type Sink interface {
	Actor
	Push(message Message) (reply Event)
}

// ===============
type ActorHandle interface {
	Id() (id *uuid.UUID)
	Kill() (err error)

	OnClose(func())
}

type TaskHandle interface {
	ActorHandle
}

type SourceHandle interface {
	ActorHandle
}

type SinkHandle interface {
	ActorHandle
	Listen(action string) (err error)
}


// ===============
type Bus interface {
	Start() (err error)
	Stop() (err error)
	Join() (err error)

	SubmitTask(task concurrent.Task) (th TaskHandle)
	RegisterSink(name string, si Sink) (sh SinkHandle)
	RegisterSource(name string, si Source) (sh SourceHandle)
}