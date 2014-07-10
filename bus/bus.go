package bus

import (
	"fmt"
	"time"
	"runtime"

	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/context"
	"github.com/zinic/protobus/concurrent"
)

const (
	DEFAULT_MAX_TASKS_QUEUED = 32768
	SHUTDOWN_POLL_INTERVAL = 50 * time.Millisecond
	DEFAULT_SHUTDOWN_WAIT_DURATION = 5 * time.Second
)

type Daemon interface {
	Start() (err error)
	Shutdown()
	Join() (err error)
}

type Bus interface {
	Daemon

	Bind(source, sink string) (err error)

	RegisterTask(task interface{}) (handle Handle, err error)
	RegisterActor(name string, actor Actor) (ah ActorHandle, err error)
}

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

func NewProtoBus(name string) (bus Bus) {
	protobus := &ProtoBus {
		bindings: make(map[string][]string),
		bindingsContext: concurrent.NewLockerContext(),

		actors: make(map[string]Actor),
		actorsContext: concurrent.NewLockerContext(),
	}

	tgConfig := &concurrent.TaskGroupConfig {
		Name: fmt.Sprintf("%s-tg", name),
		MaxQueuedTasks: DEFAULT_MAX_TASKS_QUEUED,
		MaxActiveWorkers: runtime.NumCPU(),
	}

	protobus.taskGroup = concurrent.NewTaskGroup(tgConfig)
	protobus.eventLoop = NewEventLoop(protobus.scan)

	return protobus
}


// ===============
type ProtoBus struct {
	eventLoop *EventLoop
	taskGroup *concurrent.TaskGroup

	bindings map[string][]string
	bindingsContext context.Context

	actors map[string]Actor
	actorsContext context.Context
}

func (protobus *ProtoBus) Start() (err error) {
	if _, err = protobus.taskGroup.Schedule(protobus.eventLoop.Loop); err == nil {
		err = protobus.taskGroup.Start()
	}

	return
}

func (protobus *ProtoBus) Source(name string) (source Source) {
	protobus.actorsContext(func() {
		if actor, found := protobus.actors[name]; found {
			source = actor.(Source)
		}
	})

	return
}

func (protobus *ProtoBus) Sink(name string) (sink Sink) {
	protobus.actorsContext(func () {
		if actor, found := protobus.actors[name]; found {
			sink = actor.(Sink)
		}
	})

	return
}

func (protobus *ProtoBus) Bindings() (bindingsCopy map[string][]string) {
	bindingsCopy = make(map[string][]string)

	protobus.bindingsContext(func() {
		for k, v := range protobus.bindings {
			bindingsCopy[k] = v
		}
	})

	return
}

func (protobus *ProtoBus) Bind(source, sink string) (err error) {
	protobus.bindingsContext(func() {
		sinks := protobus.bindings[source]

		if sinks == nil {
			sinks = make([]string, 0)
		}

		sinks = append(sinks, sink)
		protobus.bindings[source] = sinks
	})

	return
}

func (protobus *ProtoBus) Shutdown() {
	log.Infof("Shutting down ProtoBus %s.", protobus.taskGroup.Config.Name)
	protobus.taskGroup.Schedule(protobus.shutdown, DEFAULT_SHUTDOWN_WAIT_DURATION, SHUTDOWN_POLL_INTERVAL)
}

func (protobus *ProtoBus) shutdown(waitPeriod time.Duration, checkInterval time.Duration) (err error) {
	// Wait for the evloop to exit
	protobus.eventLoop.Stop()

	// ---
	activeTasks := 0
	shutdownChan := make(chan int, len(protobus.actors))

	for source, _ := range protobus.bindings {
		if actor, found := protobus.actors[source]; found {
			activeTasks += 1
			delete(protobus.actors, source)

			log.Infof("Scheduling shutdown of: %s", source)
			protobus.taskGroup.Schedule(shutdownActor, source, actor, shutdownChan)
		}
	}
	waitForCompletion(activeTasks, waitPeriod, checkInterval, shutdownChan)

	activeTasks = 0
	for _, sinks := range protobus.bindings {
		for _, sink := range sinks {
			if actor, found := protobus.actors[sink]; found {
				activeTasks += 1
				delete(protobus.actors, sink)

				log.Infof("Scheduling shutdown of: %s", sink)
				protobus.taskGroup.Schedule(shutdownActor, sink, actor, shutdownChan)
			}
		}
	}
	waitForCompletion(activeTasks, waitPeriod, checkInterval, shutdownChan)

	protobus.taskGroup.Stop()
	return
}

func (protobus *ProtoBus) Join() (err error) {
	protobus.taskGroup.Join()
	return
}

func (protobus *ProtoBus) RegisterTask(task interface{}) (handle Handle, err error) {
	protobus.taskGroup.Schedule(task)
	return
}

func initActor(protobus *ProtoBus, actor Actor, ctx ActorContext) {
	if err := actor.Init(ctx); err != nil {
		log.Errorf("Actor %s failed to initialize: %v.", ctx.Name, err)
	}

	protobus.actorsContext(func() {
		protobus.actors[ctx.Name()] = actor
	})
}

func (protobus *ProtoBus) RegisterActor(name string, actor Actor) (ah ActorHandle, err error) {
	var alreadyRegistered bool

	protobus.actorsContext(func() {
		_, alreadyRegistered = protobus.actors[name]
	})

	if alreadyRegistered {
		log.Errorf("Failed to add actor %s. Reason: actor already registered.", name)
	} else {
		var ctx ActorContext = &ProtoBusActorContext {
			name: name,
		}

		protobus.taskGroup.Schedule(initActor, protobus, actor, ctx)
	}

	return
}

func pull(source Source, sinks []string, protobus *ProtoBus) {
	if reply := source.Pull(); reply != nil {
		protobus.dispatch(reply, sinks)
	}
}

func (protobus *ProtoBus) scan() () {
	for source, sinks := range protobus.Bindings() {
		sourceInst := protobus.Source(source)

		if sourceInst == nil {
			continue
		}

		protobus.taskGroup.Schedule(pull, sourceInst, sinks, protobus)
	}

	return
}

func push(sink Sink, event Event) {
	sink.Push(event)
}

func (protobus *ProtoBus) dispatch(event Event, sinks []string) () {
	for _, sink := range sinks {
		sinkInst := protobus.Sink(sink)

		if sinkInst == nil {
			continue
		}

		protobus.taskGroup.Schedule(push, sinkInst, event)
	}
}