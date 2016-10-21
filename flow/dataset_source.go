package flow

import (
	"bufio"
	"log"
	"os"
	"reflect"
	"sync"
)

// Source returns a new Dataset which evenly distributes the data items produced by f
// among multiple shards. f must be a function defined in the form func(chan <some_type>).
func (fc *FlowContext) Source(f interface{}, shard int) (ret *Dataset) { // 函数变量用接口表示
	ret = fc.newNextDataset(shard, guessFunctionOutputType(f)) //
	step := fc.AddOneToAllStep(nil, ret)                       //  加入整个流程
	step.Name = "Source"
	step.Function = func(task *Task) { // 设置source 阶段的处理函数，获取输入
		ctype := reflect.ChanOf(reflect.BothDir, ret.Type) //获取chan类型
		outChan := reflect.MakeChan(ctype, 0)              //根据类型创建chan
		fn := reflect.ValueOf(f)                           // f是函数
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer outChan.Close()
			fn.Call([]reflect.Value{outChan}) // 调用本文件第57行的函数
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			var t reflect.Value
			i := 0
			for ok := true; ok; {
				if t, ok = outChan.Recv(); ok { // 从反射建立通道中接收
					task.Outputs[i].WriteChan.Send(t) // 通过反射发送给输出通道
					i++
					if i == shard { // 一个输入转化shard个输出
						i = 0
					}
				}
			}
		}()

		wg.Wait()

	}
	return
}

// TextFile returns a new Dataset which reads the text file fname line by line,
// and distributes them evenly among multiple shards.
func (fc *FlowContext) TextFile(fname string, shard int) (ret *Dataset) {
	fn := func(out chan string) { //从文件中读取字符，输出到通道
		file, err := os.Open(fname)
		if err != nil {
			// FIXME collect errors
			log.Panicf("Can not open file %s: %v", fname, err)
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			out <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Scan file %s: %v", fname, err)
		}
	}
	return fc.Source(fn, shard)
}

// Channel returns a new Dataset which has the input channel as the input and sends the received
// values to tasks.
func (fc *FlowContext) Channel(ch interface{}) (ret *Dataset) {
	chValue, chType := reflect.ValueOf(ch), reflect.TypeOf(ch)

	return fc.doChannel(chValue, chType)
}

func (fc *FlowContext) doChannel(chValue reflect.Value, chType reflect.Type) (ret *Dataset) {
	ret = fc.newNextDataset(1, chType.Elem())
	ret.ExternalInputChans = append(ret.ExternalInputChans, reflect.Indirect(chValue))
	step := fc.AddOneToOneStep(nil, ret)
	step.Name = "Input"
	step.Function = func(task *Task) {
		for t := range task.MergedInputChan() {
			task.Outputs[0].WriteChan.Send(t)
		}
	}
	return
}

// Slice accepts a slice as the input and sends the received values to tasks via Channel().
func (fc *FlowContext) Slice(slice interface{}) (ret *Dataset) {
	sliceValue, sliceType := reflect.ValueOf(slice), reflect.TypeOf(slice)
	sliceLen := sliceValue.Len()
	chType := reflect.ChanOf(reflect.BothDir, sliceType.Elem())
	chValue := reflect.MakeChan(chType, 16)

	go func() {
		for i := 0; i < sliceLen; i++ {
			chValue.Send(sliceValue.Index(i))
		}
		chValue.Close()
	}()

	return fc.doChannel(chValue, chType)
}
