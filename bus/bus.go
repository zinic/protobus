package bus

import (
	"fmt"
	"time"

	"github.com/nu7hatch/gouuid"

	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/context"
	"github.com/zinic/gbus/concurrent"
)

const (
	SHUTDOWN_POLL_INTERVAL = 50 * time.Millisecond
	DEFAULT_SHUTDOWN_WAIT_DURATION = 5 * time.Second
)

func shutdownActor(name string, actor Actor, shutdownChan chan int) {
	log.Debugf("Shutting down actor: %s.", name)

	if actor.Shutdown() == nil {
		shutdownChan <- 0
	} else {
		shutdownChan <- 1
	}
}

func waitForCompletion(taskCount int, timeRemaining time.Duration, checkInterval time.Duration, completionChan chan int) (timeTaken time.Duration) {
	timeTaken = 0

	for taskCount > 0 {
		then := time.Now()

		select {
		case <- completionChan:
			taskCount -= 1

		default:
			time.Sleep(checkInterval)
			timeTaken += time.Now().Sub(then)

			if timeTaken >= timeRemaining {
				log.Error("Timed out waiting for tasks to finish. Moving on.")
				taskCount = 0
			}
		}
	}

	return
}

func NewGBus(busName string) (bus Bus) {
	gbus := &GBus {
		bindings: make(map[string][]string),
		bindingsContext: context.NewLockerContext(),

		actors: make(map[string]Actor),
		actorsContext: context.NewLockerContext(),
	}

	gbus.taskGroup = concurrent.NewTaskGroup(fmt.Sprintf("%s-tg", busName))
	gbus.eventLoop = NewEventLoop(gbus.scan)

	return gbus
}


// ===============
type GBus struct {
	eventLoop *EventLoop
	taskGroup *concurrent.TaskGroup

	bindings map[string][]string
	bindingsContext context.Context

	actors map[string]Actor
	actorsContext context.Context
}

func (gbus *GBus) Start() (err error) {
	gbus.taskGroup.Schedule(gbus.eventLoop.Loop)
	gbus.taskGroup.Start()
	return
}

func (gbus *GBus) Source(name string) (source Source) {
	gbus.actorsContext(func() {
		if actor, found := gbus.actors[name]; found {
			source = actor.(Source)
		}
	})

	return
}

func (gbus *GBus) Sink(name string) (sink Sink) {
	gbus.actorsContext(func() {
		if actor, found := gbus.actors[name]; found {
			sink = actor.(Sink)
		}
	})

	return
}

func (gbus *GBus) Bindings() (bindingsCopy map[string][]string) {
	bindingsCopy = make(map[string][]string)

	gbus.bindingsContext(func() {
		for k, v := range gbus.bindings {
			bindingsCopy[k] = v
		}
	})

	return
}

func (gbus *GBus) Bind(source, sink string) (err error) {
	gbus.actorsContext(func() {
		if _, found := gbus.actors[source]; !found {
			err = fmt.Errorf("No source %s registered.", source)
		}

		if _, found := gbus.actors[sink]; !found {
			err = fmt.Errorf("No sinke %s registered.", sink)
		}
	})

	if err == nil {
		gbus.bindingsContext(func() {
			sinks := gbus.bindings[source]

			if sinks == nil {
				sinks = make([]string, 0)
			}

			sinks = append(sinks, sink)
			gbus.bindings[source] = sinks
		})
	}

	return
}

func (gbus *GBus) Shutdown() {
	log.Infof("Shutting down GBus %s.", gbus.taskGroup.Id)

	gbus.taskGroup.Schedule(func() (err error) {
		gbus.shutdown(DEFAULT_SHUTDOWN_WAIT_DURATION, SHUTDOWN_POLL_INTERVAL)
		return
	})
}

func (gbus *GBus) shutdown(waitPeriod time.Duration, checkInterval time.Duration) (err error) {
	// Wait for the evloop to exit
	gbus.eventLoop.Stop()

	// ---
	activeTasks := 0
	shutdownChan := make(chan int, len(gbus.actors))

	for source, _ := range gbus.bindings {
		if actor, found := gbus.actors[source]; found {
			activeTasks += 1
			delete(gbus.actors, source)

			gbus.taskGroup.Schedule(func() (err error) {
				shutdownActor(source, actor, shutdownChan)
				return
			})
		}
	}
	waitForCompletion(activeTasks, waitPeriod, checkInterval, shutdownChan)

	activeTasks = 0
	for _, sinks := range gbus.bindings {
		for _, sink := range sinks {
			if actor, found := gbus.actors[sink]; found {
				activeTasks += 1
				delete(gbus.actors, sink)

				gbus.taskGroup.Schedule(func() (err error) {
					shutdownActor(sink, actor, shutdownChan)
					return
				})
			}
		}
	}
	waitForCompletion(activeTasks, waitPeriod, checkInterval, shutdownChan)

	gbus.taskGroup.Stop()
	return
}

func (gbus *GBus) Join() (err error) {
	gbus.taskGroup.Join()
	return
}

func (gbus *GBus) RegisterTask(task concurrent.Task) (handle Handle, err error) {
	gbus.taskGroup.Schedule(task)
	return
}

func (gbus *GBus) RegisterActor(name string, actor Actor) (ah ActorHandle, err error) {
	gbus.actorsContext(func() {
		if _, found := gbus.actors[name]; !found {
			gbus.actors[name] = actor
			gbus.initActor(name, actor)
		} else {
			err = fmt.Errorf("Failed to add actor %s. Reason: actor already registered.", name)
		}
	})

	return
}

func (gbus *GBus) scan() (eventProcessed bool) {
	eventProcessed = false

	for source, sinks := range gbus.Bindings() {
		sourceInst := gbus.Source(source)

		if sourceReply := sourceInst.Pull(); sourceReply != nil {
			eventProcessed = true
			gbus.dispatch(NewMessage(sourceInst, sourceReply), sinks)
		}
	}
	return
}

func (gbus *GBus) dispatch(message Message, sinks []string) () {
	for _, sink := range sinks {
		sinkInst := gbus.Sink(sink)

		// TODO: Handle sink reply
		gbus.taskGroup.Schedule(func() (err error) {
			sinkInst.Push(message)
			return
		})
	}
}

func (gbus *GBus) initActor(name string, actor Actor) {
	ctx := &GBusActorContext {
		bus: gbus,
	}

	gbus.taskGroup.Schedule(func() (err error) {
		if err := actor.Init(ctx); err != nil {
			log.Errorf("Actor %s failed to initialize: %v.", name, err)
			gbus.Shutdown()
		}

		return
	})
}


// ===============
func SimpleSource(pull func() (message Message)) (source Source) {
	return &SimpleActor {
		pull: pull,
	}
}
func SimpleSink(push func(message Message)) (source Source) {
	return &SimpleActor {
		push: push,
	}
}

type SimpleActor struct {
	pull func() (message Message)
	push func(message Message)
}

func (sa *SimpleActor) Init(actx ActorContext) (err error) {
	return
}

func (sa *SimpleActor) Shutdown() (err error) {
	return
}

func (sa *SimpleActor) Push(message Message) {
	sa.push(message)
}

func (sa *SimpleActor) Pull() (reply Event) {
	return sa.pull()
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
	source Source
}

func NewMessage(source Source, event Event) (message Message) {
	return &DefaultMessage {
		DefaultEvent {
			action: event.Action(),
			payload: event.Payload(),
		},

		source,
	}
}

func (dm *DefaultMessage) Source() (source Source) {
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
	id ActorId
	bus Bus
}

func (gbac *GBusActorContext) Bus() (bus Bus) {
	return gbac.bus
}

func (gbac *GBusActorContext) ActorId() (id ActorId) {
	return gbac.id
}


// ===============
type GBusActorHandle struct {
	id *uuid.UUID
}

func (gbah *GBusActorHandle) ActorId() (id ActorId) {
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
