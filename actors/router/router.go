package router

package testing

import (
	"time"

	"github.com/zinic/gbus/bus"
)

func NewRouter([]string seedNodes) (actor bus.Actor) {
	return &Router {
		seeds: seedNodes,
		nodes: make([]Node, 0)
		eventChannel: make(chan bus.Event, 1024),
	}
}

type Router struct {
	seeds []string
	nodes []Node
	eventChannel chan bus.Event
}

type NodeState iota

const (
	NEW NodeState = iota
	CHECK NodeState = iota
	LIVE NodeState = iota
	STALE NodeState = iota
	DAMAGED NodeState = iota
	OFFLINE NodeState = iota
	RETIRED NodeState = iota
)

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
	return
}