package bus

import (
	"fmt"
	"time"

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
	Wait() (err error)
}

type Bus interface {
	Daemon

	Bind(source, sink string) (err error)

	RegisterTask(task interface{}) (handle Handle, err error)
	RegisterSource(name string, source Source) (ah ActorHandle, err error)
	RegisterSink(name string, sink Sink) (ah ActorHandle, err error)
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
		MaxActiveWorkers: 1024,
	}

	protobus.taskGroup = concurrent.NewTaskGroup(tgConfig)
	protobus.eventLoop = NewEventLoop(protobus.scan)

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

	protobus.eventLoop.Stop()

	sdgConfig := &concurrent.TaskGroupConfig {
		Name: fmt.Sprintf("%s-shutdown-tg", protobus.taskGroup.Config.Name),
		MaxQueuedTasks: DEFAULT_MAX_TASKS_QUEUED,
		MaxActiveWorkers: 1024,
	}

	sourceGroup := concurrent.NewTaskGroup(sdgConfig)
	sourceGroup.Start()

	for sourceName, registeredSource := range protobus.sources {
		sourceGroup.Schedule(stopSource, sourceName, registeredSource.instance)
	}
	sourceGroup.Join(4 * time.Second)
	sourceGroup.Stop()


	sinkGroup := concurrent.NewTaskGroup(sdgConfig)
	sinkGroup.Start()

	for sinkName, sink := range protobus.sinks {
		sinkGroup.Schedule(shutdownSink, sinkName, sink)
	}
	sinkGroup.Join(4 * time.Second)
	sinkGroup.Stop()

	protobus.taskGroup.Stop()
	return
}


func stopSource(name string, source Source) {
	log.Debugf("Shutting down source: %s.", name)

	if err := source.Stop(); err != nil {
		log.Errorf("Failed to stop source %s: %v", name, err)
	}
}

func shutdownSink(name string, sink Sink) {
	log.Debugf("Shutting down sinks: %s.", name)

	if err := sink.Shutdown(); err != nil {
		log.Errorf("Failed to shutdown sink %s: %v", name, err)
	}
}

func (protobus *ProtoBus) Wait() (err error) {
	protobus.taskGroup.Join(0)
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
}

func (protobus *ProtoBus) RegisterSink(name string, sink Sink) (ah ActorHandle, err error) {
	if _, found := protobus.sink(name); found {
		log.Errorf("Failed to add sink %s. Reason: sink already registered.", name)
	} else {
		var ctx ActorContext = &ProtoBusActorContext {
			name: name,
		}

		protobus.sinksCtx(func() {
			protobus.sinks[ctx.Name()] = sink
		})

		protobus.taskGroup.Schedule(sink.Init, ctx)
	}

	return
}