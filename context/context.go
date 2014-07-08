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

type Context func(call interface{}, args... interface{}) (err error)
type ContextBound func(args... interface{}) (err error)
type ContextProvider func(call ContextBound, args... interface{}) (err error)

func Using(context ContextProvider) (ctx Context) {
	return func(call interface{}, args... interface{}) (err error) {
		return bridgeCalls(call)(args)
	}
}

func bridgeCalls(call interface{}) (ctxBound ContextBound) {
	// Some minor typechecking
	callValue := reflect.ValueOf(call)
	callType := callValue.Type()

	fProxy := func(in []reflect.Value) (retvals []reflect.Value) {
		// Correctly invoke the call
		var callOutput []reflect.Value
		if callType.NumIn() == 0 {
			callOutput = callValue.Call(make([]reflect.Value, 0))
		} else {
			if args, typeOk := in[0].Interface().([]interface{}); !typeOk || len(args) != 1 {
				panic("Interface reflect.Value of type []interface{} expected for args.")
			} else if rawArgs, typeOk := args[0].([]interface{}); !typeOk {
				panic("Interface []interface{} expected for args.")
			} else {
				translatedArgs := make([]reflect.Value, len(rawArgs))
				for idx, arg := range rawArgs {
					translatedArgs[idx] = reflect.ValueOf(arg)
				}

				callOutput = callValue.Call(translatedArgs)
			}
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
	ctxBoundFn := reflect.ValueOf(&ctxBound).Elem()
	callWrapper := reflect.MakeFunc(ctxBoundFn.Type(), fProxy)
	ctxBoundFn.Set(callWrapper)
	return
}

func NewLockerContext() (context Context) {
	mutex := &sync.Mutex{}

	return Using(func(call ContextBound, args... interface{}) (err error) {
		mutex.Lock()
		defer mutex.Unlock()

		return call(args)
	})
}