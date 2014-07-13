package bus

import (
	"github.com/zinic/protobus/log"
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
	Name() (name string)
}

type Source interface {
	Start(outgoing chan Event, ctx ActorContext) (err error)
	Stop() (err error)
}

type Sink interface {
	Init(ctx ActorContext) (err error)
	Shutdown() (err error)

	Push(event Event) (err error)
}

func NewSimpleSource(start func(outgoing chan Event, actx ActorContext) (err error)) (source Source) {
	return &SimpleSource {
		start: start,
	}
}

func NewSimpleSink(push func(event Event) (err error)) (sink Sink) {
	return &SimpleSink {
		push: push,
	}
}

// Implementation
type SimpleSource struct {
	start func(outgoing chan Event, actx ActorContext) (err error)
}

func (sa *SimpleSource) Start(outgoing chan Event, actx ActorContext) (err error) {
	return sa.start(outgoing, actx)
}

func (sa *SimpleSource) Stop() (err error) {
	return
}


type SimpleSink struct {
	push func(event Event) (err error)
}

func (sa *SimpleSink) Init(actx ActorContext) (err error) {
	return
}

func (sa *SimpleSink) Shutdown() (err error) {
	return
}

func (sa *SimpleSink) Push(event Event) (err error) {
	if sa.push != nil {
		err = sa.push(event)
	} else {
		log.Warning("Push called on a SimpleSink that has no push method set. Check your bindings.")
	}

	return
}

type ProtoBusActorContext struct {
	name string
}

func (gbac *ProtoBusActorContext) Name() (name string) {
	return gbac.name
}


type ProtoBusActorHandle struct {
	id int64
}

func (gbah *ProtoBusActorHandle) ActorId() (id int64) {
	return gbah.id
}

func (gbah *ProtoBusActorHandle) Kill() (err error) {
	return
}
