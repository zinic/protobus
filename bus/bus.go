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
	RegisterSource(name string, source Source) (ah ActorHandle, err error)
	RegisterSink(name string, sink Sink) (ah ActorHandle, err error)
}

/*
func shutdownActor(name string, actor Actor, shutdownChan chan int) {
	log.Debugf("Shutting down actor: %s.", name)

	if actor.Shutdown() == nil {
		shutdownChan <- 0
	} else {
		shutdownChan <- 1
	}
}
*/

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
		bindings: make(map[string]*BoundSource),
		bindingsContext: concurrent.NewLockerContext(),

		sources: make(map[string]*RegisteredSource),
		sourcesCtx: concurrent.NewLockerContext(),

		sinks: make(map[string]Sink),
		sinksCtx: concurrent.NewLockerContext(),

	}

	tgConfig := &concurrent.TaskGroupConfig {
		Name: fmt.Sprintf("%s-tg", name),
		MaxQueuedTasks: DEFAULT_MAX_TASKS_QUEUED,
		MaxActiveWorkers: runtime.NumCPU(),
	}

	protobus.taskGroup = concurrent.NewTaskGroup(tgConfig)
	protobus.eventLoop = NewEventLoop(func(){})

	return protobus
}

type RegisteredSource struct {
	instance Source
	incoming chan Event
}

type BoundSource struct {
	RegisteredSource

	sinks map[string]Sink
}

// ===============
type ProtoBus struct {
	eventLoop *EventLoop
	taskGroup *concurrent.TaskGroup

	bindings map[string]*BoundSource
	bindingsContext context.Context

	sources map[string]*RegisteredSource
	sourcesCtx context.Context

	sinks map[string]Sink
	sinksCtx context.Context
}

func (protobus *ProtoBus) Start() (err error) {
	if _, err = protobus.taskGroup.Schedule(protobus.eventLoop.Loop); err == nil {
		err = protobus.taskGroup.Start()
	}

	return
}

func (protobus *ProtoBus) source(name string) (source *RegisteredSource, found bool) {
	protobus.sourcesCtx(func() {
		source, found = protobus.sources[name]
	})

	return
}

func (protobus *ProtoBus) sink(name string) (sink Sink, found bool) {
	protobus.sinksCtx(func () {
		sink, found = protobus.sinks[name]
	})

	return
}

func (protobus *ProtoBus) bindingsCopy() (bindingsCopy map[string]*BoundSource) {
	bindingsCopy = make(map[string]*BoundSource)

	protobus.bindingsContext(func() {
		for k, v := range protobus.bindings {
			bindingsCopy[k] = v
		}
	})

	return
}

func (protobus *ProtoBus) scan() {
	for _, source := range protobus.bindingsCopy() {
		select {
			case event, open := <- source.incoming:
				if open {
					for sinkName, sink := range source.sinks {
						protobus.taskGroup.Schedule(func(event Event, sink Sink) {
							if err := sink.Push(event); err != nil {
								log.Errorf("Failed to push event to sink %s: %v.", sinkName, err)
							}
						}, event, sink)
					}
				} else {
					// channel closed, source is done
					log.Errorf("Reclaiming sources that have closed their channels has not been implemented yet.")
				}

			default:
		}
	}
}

func (protobus *ProtoBus) Bind(sourceName, sinkName string) (err error) {
	if source, found := protobus.source(sourceName); !found {
		err = fmt.Errorf("Unable to bind %s --> %s. No source named %s found.", sourceName, sinkName, sourceName)
	} else if sink, found := protobus.sink(sinkName); !found {
		err = fmt.Errorf("Unable to bind %s --> %s. No sink named %s found.", sourceName, sinkName, sinkName)
	} else {
		protobus.bindingsContext(func() {
			if boundSource, found := protobus.bindings[sourceName]; found {
				boundSource.sinks[sinkName] = sink
			} else {
				boundSource = &BoundSource {
					RegisteredSource: *source,

					sinks: map[string]Sink {
						sinkName: sink,
					},
				}

				protobus.bindings[sourceName] = boundSource
			}
		})
	}

	return
}

func (protobus *ProtoBus) Shutdown() {
	log.Infof("Shutting down ProtoBus %s.", protobus.taskGroup.Config.Name)
	protobus.taskGroup.Schedule(protobus.shutdown, DEFAULT_SHUTDOWN_WAIT_DURATION, SHUTDOWN_POLL_INTERVAL)
}

func (protobus *ProtoBus) shutdown(waitPeriod time.Duration, checkInterval time.Duration) (err error) {
	// Wait for the evloop to exit
	protobus.eventLoop.Stop()

	/*
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
	*/

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

func (protobus *ProtoBus) RegisterSource(name string, source Source) (ah ActorHandle, err error) {
	if _, found := protobus.source(name); found {
		err = fmt.Errorf("Failed to add source %s. Reason: source already registered.", name)
	} else {
		var ctx ActorContext = &ProtoBusActorContext {
			name: name,
		}

		incoming := make(chan Event)

		protobus.sourcesCtx(func() {
			protobus.sources[name] = &RegisteredSource {
				instance: source,
				incoming: incoming,
			}
		})

		protobus.taskGroup.Schedule(func (incoming chan Event, ctx ActorContext) {
			if err := source.Start(incoming, ctx); err != nil {
				log.Errorf("Source %s failed to initialize: %v.", ctx.Name, err)
			}
		}, incoming, ctx)
	}

	return
}

func initSink(protobus *ProtoBus, sink Sink, ctx ActorContext) {
	if err := sink.Init(ctx); err != nil {
		log.Errorf("Sink %s failed to initialize: %v.", ctx.Name, err)
	}

	protobus.sinksCtx(func() {
		protobus.sinks[ctx.Name()] = sink
	})
}

func (protobus *ProtoBus) RegisterSink(name string, sink Sink) (ah ActorHandle, err error) {
	if _, found := protobus.sink(name); found {
		log.Errorf("Failed to add sink %s. Reason: sink already registered.", name)
	} else {
		var ctx ActorContext = &ProtoBusActorContext {
			name: name,
		}

		protobus.taskGroup.Schedule(initSink, protobus, sink, ctx)
	}

	return
}