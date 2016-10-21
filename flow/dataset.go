package flow

import (
	"reflect"
)

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

type Dataset struct {
	Id                  int // FlowContext 
	context             *FlowContext
	Type                reflect.Type    //反射类型（type data 两个成员组成）
	Shards              []*DatasetShard // 多个数据分片
	Step                *Step
	ReadingSteps        []*Step         //从多个读取
	
	// External[Input|Output]Chans are channels for reading and outputing
	// data from and to external sources; meaning that they are not managed
	// by other Dataset objects.
	//
	// They are used to setup in-memory input and output for a flow.
	// See doChannel() and AddOutput().
	ExternalInputChans  []reflect.Value
	ExternalOutputChans []reflect.Value

	IsKeyPartitioned bool
	IsKeyLocalSorted bool
}

func NewDataset(context *FlowContext, t reflect.Type) *Dataset {
	d := &Dataset{
		Id:      len(context.Datasets),
		context: context,
		Type:    t,
	}
	context.Datasets = append(context.Datasets, d)
	return d
}

// key value can not use reflect.Value which can not be serailize/deserialze
type KeyValue struct {
	Key   interface{}
	Value interface{}
}

type KeyValueValue struct {
	Key    interface{}
	Value1 interface{}
	Value2 interface{}
}

type KeyValues struct {
	Key    interface{}
	Values interface{}
}

type KeyValuesValues struct {
	Key     interface{}
	Values1 interface{}
	Values2 interface{}
}

var (
	KeyValueType        = reflect.TypeOf(KeyValue{}) // 获得结构体KeyValue的反射类型
	KeyValueValueType   = reflect.TypeOf(KeyValueValue{})
	KeyValuesType       = reflect.TypeOf(KeyValues{})
	KeyValuesValuesType = reflect.TypeOf(KeyValuesValues{})
)
