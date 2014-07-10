package concurrent

import (
	"github.com/zinic/protobus/context"
)

func NewReferenceLocker(contents interface{}) (rl ReferenceLocker) {
	return &ContextBoundRL {
		contents: &contents,
		context: NewLockerContext(),
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
		refCopy = cbrl.contents
	})

	return *refCopy
}

func (cbrl *ContextBoundRL) Set(reference interface{}) () {
	cbrl.context(func() {
		cbrl.contents = &reference
	})
}