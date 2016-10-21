package flow

import (
	"log"
	"reflect"
	"time"
)

// 根据类型返回对应比较函数指针，作为接口类型返回
func _getLessThanComparatorByKeyValue(key reflect.Value) (funcPointer interface{}) {
	dt := key.Type()                     // 获得反射值的类型
	if key.Kind() == reflect.Interface { // 是接口类型，进一步获得接口对应的类型
		dt = reflect.TypeOf(key.Interface())
	}
	if dt.String() == "time.Time" { // 字符符值为时间类型
		return func(a, b time.Time) bool { return a.Before(b) }
	}
	switch dt.Kind() { //具体类型
	case reflect.Int:
		funcPointer = func(a, b int) bool { return a < b }
	case reflect.Int8:
		funcPointer = func(a, b int8) bool { return a < b }
	case reflect.Int16:
		funcPointer = func(a, b int16) bool { return a < b }
	case reflect.Int32:
		funcPointer = func(a, b int32) bool { return a < b }
	case reflect.Int64:
		funcPointer = func(a, b int64) bool { return a < b }
	case reflect.Uint:
		funcPointer = func(a, b uint) bool { return a < b }
	case reflect.Uint8:
		funcPointer = func(a, b uint8) bool { return a < b }
	case reflect.Uint16:
		funcPointer = func(a, b uint16) bool { return a < b }
	case reflect.Uint32:
		funcPointer = func(a, b uint32) bool { return a < b }
	case reflect.Uint64:
		funcPointer = func(a, b uint64) bool { return a < b }
	case reflect.Float32:
		funcPointer = func(a, b float32) bool { return a < b }
	case reflect.Float64:
		funcPointer = func(a, b float64) bool { return a < b }
	case reflect.String:
		funcPointer = func(a, b string) bool { return a < b }
	default:
		log.Panicf("No default less than comparator for type:%s, kind:%s", dt.String(), dt.Kind().String())
	}
	return
}

func getLessThanComparator(datasetType reflect.Type, key reflect.Value,
	functionPointer interface{}) func(a interface{}, b interface{}) bool {
	lessThanFuncValue := reflect.ValueOf(functionPointer)
	if functionPointer == nil {
		v := guessKey(key)
		lessThanFuncValue = reflect.ValueOf(_getLessThanComparatorByKeyValue(v))
	}
	if datasetType == KeyValueType {
		return func(a interface{}, b interface{}) bool { // 返回函数，用于比较两个key大小
			ret := _functionCall(lessThanFuncValue, // 调用反射函数
				a.(KeyValue).Key, //  转化为KeyValue，获取key，作为反射函数的入参
				b.(KeyValue).Key,
			)
			return ret[0].Bool()
		}
	} else {
		return func(a interface{}, b interface{}) bool {
			ret := lessThanFuncValue.Call([]reflect.Value{
				reflect.ValueOf(a),
				reflect.ValueOf(b),
			})
			return ret[0].Bool()
		}
	}
}
