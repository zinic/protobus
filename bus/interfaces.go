package bus

import (
	"github.com/nu7hatch/gouuid"
	"github.com/zinic/gbus/concurrent"
)

// ===============
type ActorId *uuid.UUID

// ===============
type Event interface {
	Action() (action interface{})
	Payload() (payload interface{})
}

type Message interface {
	Event
	Source() (source Source)
}


// ===============
type ActorContext interface {
	ActorId() (id ActorId)
	Bus() (bus Bus)
}

type Actor interface {
	Init(actx ActorContext) (err error)
	Shutdown() (err error)
}

type Source interface {
	Actor
	Pull() (reply Event)
}

type Sink interface {
	Actor
	Push(message Message)
}


// ===============
type Handle interface {
	ActorId() (id ActorId)
}

type ActorHandle interface {
	Handle
	Kill() (err error)
}


// ===============
type Daemon interface {
	Start() (err error)
	Shutdown()
	Join() (err error)
}

type Bus interface {
	Daemon

	Bind(source, sink string) (err error)

	RegisterTask(task concurrent.Task) (handle Handle, err error)
	RegisterActor(name string, actor Actor) (ah ActorHandle, err error)
}