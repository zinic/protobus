package concurrent

import (
	"github.com/zinic/protobus/context"
)

type TrackedReference struct {
	reference interface{}
}

type Tracker interface {
	CheckIn(ref interface{}) (id int)
	Active() (refs []interface{})
	NumActive() (numActive int)
	Lookup(id int) (ref interface{}, checkedIn bool)
	CheckOut(id int)
}

func NewActivityTracker(maxActive int) (tracker Tracker) {
	available := make(chan int, maxActive)
	for id := 0; id < maxActive; id++ {
		available <- id
	}

	return &ActivityTracker {
		references: make([]*TrackedReference, maxActive),
		numActive: 0,
		available: available,
		context: NewLockerContext(),
	}
}

type ActivityTracker struct {
	references []*TrackedReference
	numActive int
	available chan int
	context context.Context
}

func (t *ActivityTracker) grow() {
	// Compute new length
	size := len(t.references)
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

	// Copy references
	references := make([]*TrackedReference, newSize)
	copy(references, t.references)

	// Set references
	t.available = available
	t.references = references
}

func (t *ActivityTracker) CheckIn(reference interface{}) (id int) {
	haveId := false

	for !haveId {
		select {
			case id = <- t.available:
				t.context(func() {
					t.references[id] = &TrackedReference {
						reference: reference,
					}

					t.numActive += 1
					haveId = true
				})

			default:
				t.context(t.grow)
		}
	}

	return
}

func (t *ActivityTracker) Active() (active []interface{}) {
	active = make([]interface{}, 0)

	t.context(func() {
		for _, tracked := range t.references {
			if tracked != nil {
				active = append(active, tracked.reference)
			}
		}
	})

	return
}

func (t *ActivityTracker) NumActive() (numActive int) {
	t.context(func() {
		numActive = t.numActive
	})

	return
}

func (t *ActivityTracker) Lookup(id int) (reference interface{}, checkedIn bool) {
	if tracked := t.references[id]; tracked != nil {
		reference = tracked.reference
		checkedIn = true
	} else {
		reference = nil
		checkedIn = false
	}

	return
}

func (t *ActivityTracker) CheckOut(id int) {
	t.context(func() {
		t.numActive -= 1
		t.references[id] = nil
		t.available <- id
	})
}
