package context

import (
	"reflect"
	"fmt"
	"sync"
)

var (
	INTERFACE_TYPE reflect.Type
	ERROR_TYPE reflect.Type
)

func init() {
	INTERFACE_TYPE = reflect.TypeOf((*interface{})(nil)).Elem()
	ERROR_TYPE = reflect.TypeOf((*error)(nil)).Elem()
}

type Context func(call interface{}) (err error)
type ContextBound func(args... interface{}) (err error)
type ContextProvider func(call ContextBound, args... interface{}) (err error)

func Using(context ContextProvider) (ctx Context) {
	cc := &CallContext {
		context: context,
	}

	return func(call interface{}) (err error) {
		return cc.Wrap(call).Run()
	}
}

type CallContext struct {
	call ContextBound
	context ContextProvider
}

func (cc *CallContext) Wrap(call interface{}) (chainCtx *CallContext) {
	// Some minor typechecking
	callValue := reflect.ValueOf(call)
	callType := callValue.Type()

	fProxy := func(in []reflect.Value) (retvals []reflect.Value) {
		// Correctly invoke the call
		var callOutput []reflect.Value

		if callType.NumIn() > 0 {
			callOutput = callValue.Call(in)
		} else {
			callOutput = callValue.Call(make([]reflect.Value, 0))
		}

		if len(callOutput) == 0 {
			retvals = []reflect.Value {
				reflect.Zero(ERROR_TYPE),
			}
		} else if len(callOutput) == 1 {
			retval := callOutput[0]

			if retval.IsNil() {
				retval = reflect.Zero(retval.Type())
			} else {
				retval = retval.Convert(ERROR_TYPE)
			}

			retvals = []reflect.Value {retval}
		} else {
			panic(fmt.Sprintf("Unable to use type %s as a context target as it returned %n values instead of 1 of type error.", callType, len(callOutput)))
		}

		// Copy return values
		return
	}

	// Create the function to handle the type bridging
	var ctxBound ContextBound
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

func NewLockerContext() (context Context) {
	mutex := &sync.Mutex{}

	return Using(func(call ContextBound, args... interface{}) (err error) {
		mutex.Lock()
		defer mutex.Unlock()

		return call(args)
	})
}