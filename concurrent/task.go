package concurrent

import (
	"runtime/debug"
	"sync"
	"github.com/zinic/gbus/log"
	"github.com/zinic/gbus/context"
	"github.com/nu7hatch/gouuid"
)

const (
	MAX_TASKS_QUEUED = 1024
)

type Task func() (err error)
type ErrorHandler func(err error)

type TaskContext struct {
	Id string
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
		waitGroup: &sync.WaitGroup{},
		tracker: make(map[string]*TaskContext),
		editContext: context.NewLockerContext(),
	}
}

type TaskGroup struct {
	Id string
	Tasks chan *TaskContext
	closed bool
	waitGroup *sync.WaitGroup
	tracker map[string]*TaskContext
	editContext context.Context
}

func (tg *TaskGroup) Stop() {
	tg.editContext(func() {
		if !tg.closed {
			tg.closed = true
			close(tg.Tasks)
		}
	})
}

func (tg *TaskGroup) dispatch(id string, task Task) {
	tg.waitGroup.Add(1)

	go func() {
		defer tg.waitGroup.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Task %s caused a panic. Reason: %v\nStacktrace of call: %s\n", id, r, debug.Stack())
			}
		}()

		if err := task(); err != nil {
			log.Infof("Error caught from task: %v", err)
		}

		log.Debugf("Task %s complete", id)
	}()
}

func (tg *TaskGroup) Start() {
	tg.dispatch(tg.Id, func() (err error) {
		for next := range tg.Tasks {
			if next == nil {
				break
			}

			log.Debugf("Dispatching %s", next.Id)
			tg.dispatch(next.Id, next.task)
		}

		return
	})
}

func (tg *TaskGroup) Schedule(task Task) (err error) {
	var taskId *uuid.UUID

	if taskId, err = uuid.NewV4(); err == nil {
		newCtx := &TaskContext {
			Id: taskId.String(),
			task: task,
		}

		tg.editContext(func() {
			if !tg.closed {
				tg.tracker[newCtx.Id] = newCtx
				tg.Tasks <- newCtx
			} else {
				panic("Channel closed already.")
			}
		})
	}

	return
}

func (tg *TaskGroup) Join() {
	tg.waitGroup.Wait()
}