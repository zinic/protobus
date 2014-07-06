package concurrent

import (
	"github.com/zinic/gbus/context"
)

func NewReferenceLocker(contents interface{}) (rl ReferenceLocker) {
	return &ContextBoundRL {
		contents: &contents,
		context: context.NewLockerContext(),
	}
}

type ReferenceLocker interface {
	Get() (reference interface{})
	Set(reference interface{})
}

type ContextBoundRL struct {
	contents *interface{}
	context context.Context
}

func (cbrl *ContextBoundRL) Get() (reference interface{}) {
	var refCopy *interface{}
	cbrl.context(func() {
		refCopy = cbrl.get()
	})

	return *refCopy
}

func (cbrl *ContextBoundRL) get() (reference *interface{}) {
	return cbrl.contents
}

func (cbrl *ContextBoundRL) Set(reference interface{}) () {
	cbrl.context(func() {
		cbrl.set(&reference)
	})
}

func (cbrl *ContextBoundRL) set(reference *interface{}) {
	cbrl.contents = reference
}