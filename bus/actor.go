package bus

import (
	"github.com/zinic/gbus/log"
)

// Interfaces
type Handle interface {
	Id() (id int64)
}

type ActorHandle interface {
	Handle
	Kill() (err error)
}

type ActorContext interface {
	ActorId() (id int64)
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
	Push(event Event)
}

func SimpleSource(pull func() (event Event)) (source Source) {
	return &SimpleActor {
		pull: pull,
	}
}

func SimpleSink(push func(event Event)) (source Source) {
	return &SimpleActor {
		push: push,
	}
}

// Implementation
type SimpleActor struct {
	pull func() (event Event)
	push func(event Event)
}

func (sa *SimpleActor) Init(actx ActorContext) (err error) {
	return
}

func (sa *SimpleActor) Shutdown() (err error) {
	return
}

func (sa *SimpleActor) Push(event Event) {
	if sa.push != nil {
		sa.push(event)
	} else {
		log.Warning("Push called on a SimpleActor that has no push method set. Check your bindings.")
	}
}

func (sa *SimpleActor) Pull() (reply Event) {
	if sa.pull != nil {
		reply = sa.pull()
	} else {
		log.Warning("Pull called on a SimpleActor that has no pull method set. Check your bindings.")
	}

	return
}


type GBusActorContext struct {
	id int64
	bus Bus
}

func (gbac *GBusActorContext) Bus() (bus Bus) {
	return gbac.bus
}

func (gbac *GBusActorContext) ActorId() (id int64) {
	return gbac.id
}


type GBusActorHandle struct {
	id int64
}

func (gbah *GBusActorHandle) ActorId() (id int64) {
	return gbah.id
}

func (gbah *GBusActorHandle) Kill() (err error) {
	return
}
