package bus

import (
	"fmt"
	"time"
	"runtime"

	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/context"
	"github.com/zinic/gbus/concurrent"
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

	RegisterTask(task concurrent.Task) (handle Handle, err error)
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
	gbus := &ProtoBus {
		bindings: make(map[string][]string),
		bindingsContext: context.NewLockerContext(),

		actors: make(map[string]Actor),
		actorsContext: context.NewLockerContext(),
	}

	tgConfig := &concurrent.TaskGroupConfig {
		Name: fmt.Sprintf("%s-tg", name),
		MaxQueuedTasks: DEFAULT_MAX_TASKS_QUEUED,
		MaxActiveWorkers: runtime.NumCPU(),
	}

	gbus.taskGroup = concurrent.NewTaskGroup(tgConfig)
	gbus.eventLoop = NewEventLoop(gbus.scan)

	return gbus
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

func (gbus *ProtoBus) Start() (err error) {
	if _, err = gbus.taskGroup.Schedule(gbus.eventLoop.Loop); err == nil {
		err = gbus.taskGroup.Start()
	}

	return
}

func (gbus *ProtoBus) Source(name string) (source Source) {
	gbus.actorsContext(func() {
		if actor, found := gbus.actors[name]; found {
			source = actor.(Source)
		}
	})

	return
}

func (gbus *ProtoBus) Sink(name string) (sink Sink) {
	gbus.actorsContext(func () {
		if actor, found := gbus.actors[name]; found {
			sink = actor.(Sink)
		}
	})

	return
}

func (gbus *ProtoBus) Bindings() (bindingsCopy map[string][]string) {
	bindingsCopy = make(map[string][]string)

	gbus.bindingsContext(func() {
		for k, v := range gbus.bindings {
			bindingsCopy[k] = v
		}
	})

	return
}

func (gbus *ProtoBus) Bind(source, sink string) (err error) {
	gbus.bindingsContext(func() {
		sinks := gbus.bindings[source]

		if sinks == nil {
			sinks = make([]string, 0)
		}

		sinks = append(sinks, sink)
		gbus.bindings[source] = sinks
	})

	return
}

func (gbus *ProtoBus) Shutdown() {
	log.Infof("Shutting down ProtoBus %s.", gbus.taskGroup.Config.Name)

	gbus.taskGroup.Schedule(func() (err error) {
		gbus.shutdown(DEFAULT_SHUTDOWN_WAIT_DURATION, SHUTDOWN_POLL_INTERVAL)
		return
	})
}

func (gbus *ProtoBus) shutdown(waitPeriod time.Duration, checkInterval time.Duration) (err error) {
	// Wait for the evloop to exit
	gbus.eventLoop.Stop()

	// ---
	activeTasks := 0
	shutdownChan := make(chan int, len(gbus.actors))

	for source, _ := range gbus.bindings {
		if actor, found := gbus.actors[source]; found {
			activeTasks += 1
			delete(gbus.actors, source)

			log.Infof("Scheduling shutdown of: %s", source)

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

func (gbus *ProtoBus) Join() (err error) {
	gbus.taskGroup.Join()
	return
}

func (gbus *ProtoBus) RegisterTask(task concurrent.Task) (handle Handle, err error) {
	gbus.taskGroup.Schedule(task)
	return
}

func (gbus *ProtoBus) RegisterActor(name string, actor Actor) (ah ActorHandle, err error) {
	ctx := &ProtoBusActorContext {
		bus: gbus,
	}

	gbus.taskGroup.Schedule(func() (err error) {
		if err := actor.Init(ctx); err != nil {
			log.Errorf("Actor %s failed to initialize: %v.", name, err)
		}

		gbus.actorsContext(func() {
			if _, found := gbus.actors[name]; !found {
				gbus.actors[name] = actor
			} else {
				err = fmt.Errorf("Failed to add actor %s. Reason: actor already registered.", name)
			}
		})

		return
	})

	return
}

func (gbus *ProtoBus) scan() (eventProcessed bool) {
	eventProcessed = false

	for source, sinks := range gbus.Bindings() {
		sourceInst := gbus.Source(source)

		if sourceInst == nil {
			continue
		}

		if sourceReply := sourceInst.Pull(); sourceReply != nil {
			eventProcessed = true
			gbus.dispatch(sourceReply, sinks)
		}
	}

	return
}

func (gbus *ProtoBus) dispatch(event Event, sinks []string) () {
	for _, sink := range sinks {
		sinkInst := gbus.Sink(sink)

		if sinkInst == nil {
			continue
		}

		gbus.taskGroup.Schedule(func() (err error) {
			sinkInst.Push(event)
			return
		})
	}
}