package concurrent

import (
	"github.com/zinic/protobus/context"
)

type State int

const (
	ACTIVE State = iota
	INACTIVE State = iota
)

type Tracker interface {
	Checkout() (id int)
	State(id int) (state State)
	Retire(id int)
}

func NewTracker(maxActive int) (tracker Tracker) {
	available := make(chan int, maxActive)
	for id := 0; id < maxActive; id++ {
		available <- id
	}

	return &StateTracker {
		states: make([]State, maxActive),
		available: available,
		context: NewLockerContext(),
	}
}

type StateTracker struct {
	states []State
	available chan int
	context context.Context
}

func (t *StateTracker) grow() {
	// Compute new length
	size := len(t.states)
	growBy := size / 2
	newSize := size + growBy

	// Copy channel contents
	available := make(chan int, newSize)

	done := false
	for !done {
		select {
			case id := <- t.available:
				available <- id
			default:
				done = true
		}
	}

	// Add new state buckets
	for id := size; id < newSize; id++ {
		available <- id
	}

	// Copy states
	states := make([]State, newSize)
	copy(states, t.states)

	// Set references
	t.available = available
	t.states = states
}

func (t *StateTracker) Checkout() (id int) {
	haveId := false

	for !haveId {
		select {
			case id = <- t.available:
				t.states[id] = ACTIVE
				haveId = true

			default:
				t.context(t.grow)
		}
	}

	return
}

func (t *StateTracker) State(id int) (state State) {
	return t.states[id]
}

func (t *StateTracker) Retire(id int) {
	t.context(func() {
		t.states[id] = INACTIVE
		t.available <- id
	})
}
