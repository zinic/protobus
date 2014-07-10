package meta

import (
	"reflect"
	"fmt"
)

var (
	INTERFACE_TYPE reflect.Type
	ERROR_TYPE reflect.Type

	EMPTY_VALUE_ARRAY = make([]reflect.Value, 0)
)

func init() {
	INTERFACE_TYPE = reflect.TypeOf((*interface{})(nil)).Elem()
	ERROR_TYPE = reflect.TypeOf((*error)(nil)).Elem()
}

func TypeOf(i interface{}) (t reflect.Type) {
	return reflect.TypeOf(i)
}

func IsFunction(i interface{}) (isFunc bool) {
	iType := reflect.TypeOf(i)
	return IsKindFunc(iType.Kind())
}

func IsKindFunc(k reflect.Kind) (isFunc bool) {
	return k == reflect.Func
}

func Call(call interface{}, args... interface{}) (returned []interface{}, err error) {
	if !IsFunction(call) {
		err = fmt.Errorf("Call must must have a Kind of Func.")
	} else {
		callValue := reflect.ValueOf(call)
		callType := callValue.Type()

		callArgs := EMPTY_VALUE_ARRAY
		if callType.NumIn() > 0 {
			if rawArgs, typeOk := args[0].([]interface{}); !typeOk {
				panic("Interface []interface{} expected for args.")
			} else {
				callArgs = make([]reflect.Value, len(rawArgs))
				for idx, arg := range rawArgs {
					callArgs[idx] = reflect.ValueOf(arg)
				}
			}
		}

		callOutput := callValue.Call(callArgs)
		returned := make([]interface{}, len(callOutput))

		for idx, outValue := range callOutput {
			returned[idx] = outValue.Interface()
		}
	}

	return
}

