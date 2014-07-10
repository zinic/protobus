package context

import (
	"github.com/zinic/protobus/meta"
)

type Context func(call interface{}, args... interface{}) (err error)
type ContextBound func(args... interface{}) (err error)
type ContextProvider func(call ContextBound, args... interface{}) (err error)

type BoundContext struct {
	contextProvider ContextProvider
}

func (bc *BoundContext) Wrap(call interface{}, args... interface{}) (err error) {
	ctxBound := func(args... interface{}) (err error) {
		_, err = meta.Call(call, args)
		return
	}

	return bc.contextProvider(ctxBound, args)
}

func Using(contextProvider ContextProvider) (ctx Context) {
	bc := &BoundContext {
		contextProvider: contextProvider,
	}

	return bc.Wrap
}