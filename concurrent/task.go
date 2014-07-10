package concurrent

import (
	"fmt"
	"sync"
	"sync/atomic"
	"runtime/debug"

	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/meta"
	"github.com/zinic/protobus/context"
)

type TaskContext struct {
	Id int64
	status string
	call interface{}
	args []interface{}
	editContext context.Context
}

func (tc *TaskContext) SetStatus(status string) {
	tc.editContext(func() {
		tc.status = status
	})
}

func (tc *TaskContext) Status() (status string) {
	return tc.status
}

func NewTaskGroup(config *TaskGroupConfig) (tg *TaskGroup) {
	return &TaskGroup {
		Config: config,
		Tasks: make(chan *TaskContext, config.MaxQueuedTasks),

		nextTaskId: 0,
		closed: NewReferenceLocker(false),
		waitGroup: &sync.WaitGroup{},
		editContext: NewLockerContext(),
	}
}

type TaskGroupConfig struct {
	Name string
	MaxQueuedTasks int64
	MaxActiveWorkers int
}

type TaskGroup struct {
	Config *TaskGroupConfig
	Tasks chan *TaskContext

	closed ReferenceLocker
	nextTaskId int64
	waitGroup *sync.WaitGroup
	editContext context.Context
}

func (tg *TaskGroup) Stop() {
	tg.closed.Set(true)
	close(tg.Tasks)
}

func (tg *TaskGroup) worker() {
	defer tg.waitGroup.Done()

	for task := range tg.Tasks {
		tg.dispatch(task)
	}
}

func (tg *TaskGroup) dispatch(taskCtx *TaskContext) {
	defer func() {
		if recovery := recover(); recovery != nil {
			log.Errorf("Task %s caused a panic. Reason: %v\nStacktrace of call: %s\n",
				taskCtx.Id, recovery, debug.Stack())
		}
	}()

	if !tg.closed.Get().(bool) {
		if _, err := meta.Call(taskCtx.call, taskCtx.args); err != nil {
			log.Infof("Error caught from task: %v", err)
		}
	}
}

func (tg *TaskGroup) Start() (err error) {
	if !tg.closed.Get().(bool) {
		for wc := 0; wc < tg.Config.MaxActiveWorkers; wc++ {
			tg.waitGroup.Add(1)
			go tg.worker()
		}
	} else {
		err = fmt.Errorf("Failed to start TaskGroup: TaskGroup has been shutdown.")
	}

	return
}

func (tg *TaskGroup) Schedule(call interface{}, args... interface{}) (id int64, err error) {
	if !meta.IsFunction(call) {
		err = fmt.Errorf("Task call must be a function.")
	}

	id = atomic.AddInt64(&tg.nextTaskId, 1)
	newCtx := &TaskContext {
		Id: id,
		call: call,
		args: args,
	}

	if !tg.closed.Get().(bool) {
		select {
			case tg.Tasks <- newCtx:
			default:
				err = fmt.Errorf("Task channel full, please wait.")
		}
	} else {
		panic("Unable to schedule task: TaskGroup has been shutdown.")
	}

	return
}

func (tg *TaskGroup) Join() {
	tg.waitGroup.Wait()
}