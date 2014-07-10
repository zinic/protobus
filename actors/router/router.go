package router

package testing

import (
	"time"

	"github.com/zinic/protobus/bus"
	"github.com/zinic/protobus/context"
	"github.com/zinic/protobus/concurrent"
)

func NewRouter([]string seedNodes) (actor bus.Actor) {
	return &Router {
		seeds: seedNodes,
		nodes: make([]Node, 0),
		state: concurrent.NewReferenceLocker(ROUTER_BOOT),
		eventChannel: make(chan bus.Event, 1024),
	}
}

type NodeState iota
type RouterState iota

const (
	ROUTER_BOOT RouterState = iota
	ROUTER_DISCOVER RouterState = iota
	ROUTER_READY RouterState = iota

	NEW NodeState = iota
	CHECK NodeState = iota
	LIVE NodeState = iota
	STALE NodeState = iota
	DAMAGED NodeState = iota
	OFFLINE NodeState = iota
	RETIRED NodeState = iota
)

type Router struct {
	seeds []string
	nodes []Node

	state concurrent.ReferenceLocker
	eventChannel chan bus.Event
}

type Node struct {
	address string
	state NodeState
}

func (router *Router) Init(actx bus.ActorContext) (err error) {
	for _, seedAddr := range router.seeds {
		nodes = append(nodes, &Node {
			address: seedAddr,
			state: NEW,
		})
	}

	return
}

func (router *Router) Shutdown() (err error) {
	return
}

func (router *Router) Push(message bus.Message) {
	return
}

func (router *Router) Pull() (reply bus.Event) {
	switch router.state.Get().(Router) {
		case ROUTER_BOOT:
			router.state.Set(ROUTER_DISCOVER)
			eventChannel

		case ROUTER_DISCOVER:
	}

	return
}