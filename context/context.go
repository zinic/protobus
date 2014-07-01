package context

import (
	"reflect"
	"sync"
)

type ContextBound func(args... interface{}) (err error)
type ContextProvider func(call ContextBound, args... interface{}) (err error)

type CallContext struct {
	call ContextBound
	context ContextProvider
}

func Using(context ContextProvider) (chainCtx *CallContext) {
	return &CallContext {
		context: context,
	}
}

func (cc *CallContext) Wrap(call interface{}) (chainCtx *CallContext) {
	// Some minor typechecking
	var ctxBound ContextBound
	ctxBoundValue := reflect.ValueOf(ctxBound)
	ctxBoundType := ctxBoundValue.Type()

	callValue := reflect.ValueOf(call)
	callType := callValue.Type()

	fProxy := func(in []reflect.Value) (out []reflect.Value) {
		if callType.NumIn() > 0 {
			out = callValue.Call(in)
		} else {
			out = callValue.Call(make([]reflect.Value, 0))
		}

		if callType.NumOut() == 0 {
			out = []reflect.Value {
				reflect.Zero(ctxBoundType.Out(0)),
			}
		} else {
			out = []reflect.Value {
				out[0],
			}
		}

		return
	}

	// Create the function to handle the type bridging
	ctxBoundFn := reflect.ValueOf(&ctxBound).Elem()
	callWrapper := reflect.MakeFunc(ctxBoundFn.Type(), fProxy)
	ctxBoundFn.Set(callWrapper)

	// Set the new function as our call
	cc.call = ctxBound

	// Return ourself to complete the chain
	return cc
}

func (cc *CallContext) Run(args... interface{}) (err error) {
	return cc.context(cc.call, args)
}

func NewMutexContextProvider() (cp ContextProvider) {
	mutex := &sync.Mutex{}

	return func(call ContextBound, args... interface{}) (err error) {
		mutex.Lock()
		defer mutex.Unlock()

		return call(args)
	}
}