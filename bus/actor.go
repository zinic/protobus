package bus

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

type Actor interface {
	Stop() (err error)
}

type Source interface {
	Actor
	Start(outgoing chan<- Event, ctx ActorContext) (err error)
}

type Sink interface {
	Actor
	Start(incoming <-chan Event, ctx ActorContext) (err error)
}

func NewSimpleSource(start func(outgoing chan<- Event, actx ActorContext) (err error)) (source Source) {
	return &SimpleSource {
		start: start,
	}
}

func NewSimpleSink(start func(incoming <-chan Event, actx ActorContext) (err error)) (sink Sink) {
	return &SimpleSink {
		start: start,
	}
}

// Implementation
type SimpleSource struct {
	start func(incoming chan<- Event, actx ActorContext) (err error)
}

func (sa *SimpleSource) Start(outgoing chan<- Event, actx ActorContext) (err error) {
	return sa.start(outgoing, actx)
}

func (sa *SimpleSource) Stop() (err error) {
	return
}

type SimpleSink struct {
	start func(incoming <-chan Event, actx ActorContext) (err error)
}

func (sa *SimpleSink) Start(incoming <-chan Event, actx ActorContext) (err error) {
	return sa.start(incoming, actx)
}

func (sa *SimpleSink) Stop() (err error) {
	return
}

// Ctx crap
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
