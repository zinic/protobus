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
	Id *uuid.UUID
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
		tracker: make(map[*uuid.UUID]*TaskContext),
		editContext: context.NewLockerContext(),
	}
}

type TaskGroup struct {
	Id string
	Tasks chan *TaskContext
	closed bool
	waitGroup *sync.WaitGroup
	tracker map[*uuid.UUID]*TaskContext
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

func (tg *TaskGroup) dispatch(taskCtx *TaskContext) {
	tg.waitGroup.Add(1)

	go func() {
		defer func() {
			if recovery := recover(); recovery != nil {
				log.Errorf("Task %s caused a panic. Reason: %v\nStacktrace of call: %s\n",
					taskCtx.Id.String(), recovery, debug.Stack())
			}

			delete(tg.tracker, taskCtx.Id)
			tg.waitGroup.Done()
		}()

		if err := taskCtx.task(); err != nil {
			log.Infof("Error caught from task: %v", err)
		}

		log.Debugf("Task %s complete", taskCtx.Id.String())
	}()
}

func (tg *TaskGroup) Start() (err error) {
	var taskId *uuid.UUID

	if taskId, err = uuid.NewV4(); err == nil {
		newCtx := &TaskContext {
			Id: taskId,
			task: func() (err error) {
				for next := range tg.Tasks {
					if next == nil {
						break
					}

					log.Debugf("Dispatching %s", next.Id.String())
					tg.dispatch(next)
				}

				return
			},
		}

		tg.dispatch(newCtx)
	}

	return
}

func (tg *TaskGroup) Status(id *uuid.UUID) (status string, found bool) {
	tg.editContext(func() {
		found = false
		status = "NOT_FOUND"

		if taskCtx, exists := tg.tracker[id]; exists {
			found = true
			status = taskCtx.status
		}
	})

	return
}

func (tg *TaskGroup) Schedule(task Task) (id *uuid.UUID, err error) {
	var taskId *uuid.UUID

	if taskId, err = uuid.NewV4(); err == nil {
		newCtx := &TaskContext {
			Id: taskId,
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