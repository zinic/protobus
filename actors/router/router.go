package router

import (
	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/concurrent"
)

func NewRouter(addr string, seedNodes []string) (actor bus.Actor) {
	return &Router {
		addr: addr,
		seeds: seedNodes,
		nodes: make([]*Node, 0),
		state: concurrent.NewReferenceLocker(ROUTER_BOOT),
		eventChannel: make(chan bus.Event, 1024),
	}
}

type NodeState int
type RouterState int

const (
	ROUTER_BOOT RouterState = iota
	ROUTER_DISCOVER RouterState = iota
	ROUTER_READY RouterState = iota
	ROUTER_SHUTDOWN RouterState = iota

	NEW NodeState = iota
	CHECK NodeState = iota
	LIVE NodeState = iota
	STALE NodeState = iota
	DAMAGED NodeState = iota
	OFFLINE NodeState = iota
	RETIRED NodeState = iota
)

type Router struct {
	addr string
	seeds []string
	nodes []*Node

	state concurrent.ReferenceLocker
	eventChannel chan bus.Event
}

type Node struct {
	address string
	state NodeState
}

func (router *Router) Start(outgoing chan<- bus.Event, actx bus.ActorContext) (err error) {
	log.Debug("Initializing Router.")

	for _, seedAddr := range router.seeds {
		router.nodes = append(router.nodes, &Node {
			address: seedAddr,
			state: NEW,
		})
	}

	for done := false; !done; {
		switch router.state.Get().(RouterState) {
			case ROUTER_BOOT:
				router.state.Set(ROUTER_DISCOVER)

				for _, seed := range router.seeds {
					payload := &bus.Message {
						Source: router.addr,
						Destination: seed,
					}

					outgoing <- bus.NewEvent("discover.router", payload)
				}

			case ROUTER_DISCOVER:
				if event, ok := <- router.eventChannel; !ok {
					done = true
				} else {
					log.Debugf("%v", event)
				}

			case ROUTER_READY:
			case ROUTER_SHUTDOWN:
				done = true
		}
	}

	return
}

func (router *Router) Stop() (err error) {
	close(router.eventChannel)
	return
}

func (router *Router) Init(actx bus.ActorContext) (err error) {
	return
}

func (router *Router) Push(message bus.Message) {
	return
}