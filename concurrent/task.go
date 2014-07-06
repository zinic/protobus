package concurrent

import (
	"fmt"
	"sync"
	"runtime"
	"runtime/debug"

	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/context"
	"github.com/nu7hatch/gouuid"
)

const (
	MAX_TASKS_QUEUED = 32768
)

type Task func() (err error)
type ErrorHandler func(err error)

type TaskContext struct {
	Id int64
	status string
	task Task
	editContext *context.CallContext
}

func (tc *TaskContext) SetStatus(status string) {
	tc.editContext.Wrap(func() {
		tc.status = status
	}).Run()
}

func (tc *TaskContext) Status() (status string) {
	return tc.status
}

func NewTaskGroup(id string) (tg *TaskGroup) {
	return &TaskGroup {
		Id: id,
		Tasks: make(chan *TaskContext, MAX_TASKS_QUEUED),
		closed: false,
		nextTaskId: 0,
		waitGroup: &sync.WaitGroup{},
		editContext: context.NewLockerContext(),
	}
}

type TaskGroup struct {
	Id string
	Tasks chan *TaskContext
	closed bool
	nextTaskId int64
	waitGroup *sync.WaitGroup
	editContext context.Context
}

func (tg *TaskGroup) Stop() {
	tg.editContext(func() {
		tg.closed = true
	})

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

	if !tg.closed {
		if err := taskCtx.task(); err != nil {
			log.Infof("Error caught from task: %v", err)
		}
	}
}

func (tg *TaskGroup) Start() (err error) {
	if tg.closed {
		panic(fmt.Sprintf("Task group %s already closed.", tg.Id))
	}

	for cpu := 0; cpu < runtime.NumCPU() * 2; cpu++ {
		tg.waitGroup.Add(1)
		go tg.worker()
	}

	return
}

func (tg *TaskGroup) Schedule(task Task) (id *uuid.UUID, err error) {
	tg.nextTaskId++
	newCtx := &TaskContext {
		Id: tg.nextTaskId,
		task: task,
	}

	if !tg.closed {
		tg.Tasks <- newCtx
	} else {
		panic("Channel closed already.")
	}

	return
}

func (tg *TaskGroup) Join() {
	tg.waitGroup.Wait()
}