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
	RegisterSource(name string, actor Actor) (ah ActorHandle, err error)
	RegisterSink(name string, actor Actor) (ah ActorHandle, err error)
}

func NewProtoBus(name string) (bus Bus) {
	protobus := &ProtoBus {
		bindings: make(map[string]*BoundSource),
		bindingsContext: concurrent.NewLockerContext(),

		sources: make(map[string]*RegisteredSource),
		sourcesCtx: concurrent.NewLockerContext(),

		sinks: make(map[string]*RegisteredSink),
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

type RegisteredSink struct {
	instance Sink
	outgoing chan Event
}

type BoundSource struct {
	RegisteredSource
	sinks map[string]*RegisteredSink
}

// ===============
type ProtoBus struct {
	eventLoop *EventLoop
	taskGroup *concurrent.TaskGroup

	bindings map[string]*BoundSource
	bindingsContext context.Context

	sources map[string]*RegisteredSource
	sourcesCtx context.Context

	sinks map[string]*RegisteredSink
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

func (protobus *ProtoBus) sink(name string) (sink *RegisteredSink, found bool) {
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
					for _, sink := range source.sinks {
						protobus.taskGroup.Schedule(func(event Event, outgoing chan Event) {
							outgoing <- event
						}, event, sink.outgoing)
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

					sinks: map[string]*RegisteredSink {
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
		sourceGroup.Schedule(stopSource, sourceName, registeredSource)
	}
	sourceGroup.Join(4 * time.Second)
	sourceGroup.Stop()


	sinkGroup := concurrent.NewTaskGroup(sdgConfig)
	sinkGroup.Start()

	for sinkName, registeredSink := range protobus.sinks {
		sinkGroup.Schedule(stopSink, sinkName, registeredSink)
	}
	sinkGroup.Join(4 * time.Second)
	sinkGroup.Stop()

	protobus.taskGroup.Stop()
	return
}


func stopSource(name string, source *RegisteredSource) {
	log.Debugf("Shutting down source: %s.", name)
	defer close(source.incoming)

	if err := source.instance.Stop(); err != nil {
		log.Errorf("Failed to stop source %s: %v", name, err)
	}
}

func stopSink(name string, sink *RegisteredSink) {
	log.Debugf("Shutting down sink: %s.", name)
	close(sink.outgoing)

	if err := sink.instance.Stop(); err != nil {
		log.Errorf("Failed to stop sink %s: %v", name, err)
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

func (protobus *ProtoBus) RegisterSource(name string, actor Actor) (ah ActorHandle, err error) {
	if source, typeOk := actor.(Source); !typeOk {
		err = fmt.Errorf("Failed to add source %s. Reason: actor passed to method does not answer to the Source interface.", name)
	} else  if _, found := protobus.source(name); found {
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

func (protobus *ProtoBus) RegisterSink(name string, actor Actor) (ah ActorHandle, err error) {
	if sink, typeOk := actor.(Sink); !typeOk {
		err = fmt.Errorf("Failed to add sink %s. Reason: actor passed to method does not answer to the Sink interface.", name)
	} else if _, found := protobus.sink(name); found {
		log.Errorf("Failed to add sink %s. Reason: sink already registered.", name)
	} else {
		var ctx ActorContext = &ProtoBusActorContext {
			name: name,
		}

		outgoing := make(chan Event)

		protobus.sinksCtx(func() {
			protobus.sinks[ctx.Name()] = &RegisteredSink {
				instance: sink,
				outgoing: outgoing,
			}
		})

		protobus.taskGroup.Schedule(func (outgoing chan Event, ctx ActorContext) {
			if err := sink.Start(outgoing, ctx); err != nil {
				log.Errorf("Sink %s failed to initialize: %v.", ctx.Name, err)
			}
		}, outgoing, ctx)
	}

	return
}