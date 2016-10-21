package flow

import (
	"sync"
)

type Step struct {
	Id       int
	Inputs   []*Dataset  // 输入数据
	Output   *Dataset    // 输出数据
	Function func(*Task) // 任务处理函数
	Tasks    []*Task     // 任务集
	Name     string      //任务名
}

func (s *Step) RunStep() {
	var wg sync.WaitGroup
	for i, t := range s.Tasks {
		wg.Add(1)
		go func(i int, t *Task) {
			defer wg.Done()
			t.RunTask()
		}(i, t)
	}
	wg.Wait()

	return
}
