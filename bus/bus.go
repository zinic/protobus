package bus

import (
	"fmt"

	"github.com/zinic/gbus/log"
	"github.com/nu7hatch/gouuid"
	"github.com/zinic/gbus/concurrent"
)


func NewGBus(busName string) (bus Bus) {
	return &GBus {
		taskGroup: concurrent.NewTaskGroup(fmt.Sprintf("%s-tg", busName)),
	}
}


// ===============
type GBus struct {
	sources map[string]Source
	sinks map[string]Sink

	taskGroup *concurrent.TaskGroup
}

func (gbus *GBus) Start() (err error) {
	log.Debug("Task group started")
	gbus.taskGroup.Start()

	ctx := &GBusActorContext {
		bus: gbus,
	}

	for name, actor := range gbus.sources {
		gbus.initActor(name, actor, ctx)
	}

	for name, actor := range gbus.sinks {
		gbus.initActor(name, actor, ctx)
	}

	gbus.taskGroup.Schedule(func() (err error) {
		// Stopped here
		// I'm looking for stuff to get processing of events working. This
		// means routing probably x.x
	})

	return
}

func (gbus *GBus) initActor(name string, actor Actor, ctx ActorContext) {
	gbus.taskGroup.Schedule(func() (err error) {
		if err := actor.Init(ctx); err != nil {
			log.Errorf("Actor %s failed to initialize: %v.", name, err)
			gbus.Stop()
		}

		return
	})
}

func (gbus *GBus) Stop() (err error) {
	gbus.taskGroup.Stop()
	return
}

func (gbus *GBus) Join() (err error) {
	gbus.taskGroup.Join()
	return
}

func (gbus *GBus) SubmitTask(task concurrent.Task) (th TaskHandle) {
	gbus.taskGroup.Schedule(task)
	return
}

func (gbus *GBus) RegisterSink(name string, sink Sink) (sh SinkHandle) {
	return
}

func (gbus *GBus) RegisterSource(name string, source Source) (sh SourceHandle) {
	return
}


// ===============
func NewEvent(action, payload interface{}) (event Event) {
	return &DefaultEvent {
		action: action,
		payload: payload,
	}
}

type DefaultEvent struct {
	action interface{}
	payload interface{}
}

func (de *DefaultEvent) Action() (action interface{}) {
	return de.action
}

func (de *DefaultEvent) Payload() (payload interface{}) {
	return de.payload
}


// ===============
type DefaultMessage struct {
	DefaultEvent
	source *uuid.UUID
}

func (dm *DefaultMessage) Source() (uuid *uuid.UUID) {
	return dm.source
}

/*
func (dm *DefaultMessage) Action() (action interface{}) {
	return dm.action
}

func (dm *DefaultMessage) Payload() (payload interface{}) {
	return dm.payload
}
*/


// ===============
type GBusActorContext struct {
	bus Bus
}

func (gbac *GBusActorContext) Bus() (bus Bus) {
	return gbac.bus
}


// ===============
type GBusActorHandle struct {
	id *uuid.UUID
}

func (gbah *GBusActorHandle) Id() (id *uuid.UUID) {
	return gbah.id
}

func (gbah *GBusActorHandle) Kill() (err error) {
	return
}

type GBusSinkHandle struct {
	GBusActorHandle
}

func (handle GBusSinkHandle) Listen(action string) (err error) {
	return nil
}
