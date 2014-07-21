package concurrent

import (
	"fmt"
	"time"
	"sync"
	"runtime"

	"github.com/zinic/protobus/log"
	"github.com/zinic/protobus/meta"
	"github.com/zinic/protobus/context"
)

type TrackedTask struct {
	call interface{}
	schedulerStack []byte
}

type TaskContext struct {
	Id int
	call interface{}
	args []interface{}
}

func panicTaskGroupClosed(tgName string) {
	reason := fmt.Sprintf("TaskGroup %s already closed", tgName)
	panic(reason)
}

func cleanupDispatch(tg *TaskGroup, id int) {
	// Handle any panics
	if recovery := recover(); recovery != nil {
		log.Errorf("Task %s caused a panic. Reason: %v", id, recovery)

		if log.Level() == log.DEBUG {
			stackTrace := make([]byte, 1024)

			for {
				if written := runtime.Stack(stackTrace, false); written < len(stackTrace) {
					stackTrace = stackTrace[:written]
					break
				}

				stackTrace = make([]byte, len(stackTrace)*2)
			}

			log.Errorf("Stacktrace of call: %s", stackTrace)
		}
	}

	tg.taskTracker.CheckOut(id)
}

func NewTaskGroup(config *TaskGroupConfig) (tg *TaskGroup) {
	mutex := &sync.Mutex{}

	return &TaskGroup {
		Config: config,
		closed: false,
		taskTracker: NewActivityTracker(config.MaxActiveWorkers),
		taskChan: make(chan *TaskContext, config.MaxQueuedTasks),

		tasksComplete: sync.NewCond(mutex),
		lockCtx: NewMutexContext(mutex),
	}
}

type TaskGroupConfig struct {
	Name string
	MaxQueuedTasks int
	MaxActiveWorkers int
}

type TaskGroup struct {
	Config *TaskGroupConfig

	closed bool
	taskTracker Tracker
	taskChan chan *TaskContext

	tasksComplete *sync.Cond
	lockCtx context.Context
}

func (tg *TaskGroup) Closed() (closed bool) {
	tg.lockCtx(func() {
		closed = tg.closed
	})

	return
}

func (tg *TaskGroup) Stop() {
	if tg.Closed() {
		panicTaskGroupClosed(tg.Config.Name)
	}

	tg.lockCtx(func() {
		tg.closed = true
	})

	close(tg.taskChan)
}

func (tg *TaskGroup) worker() {
	done := false

	for !done {
		if task, ok := <- tg.taskChan; ok {
			go tg.dispatch(task)
		} else {
			done = true
		}
	}
}

func (tg *TaskGroup) dispatch(taskCtx *TaskContext) {
	defer cleanupDispatch(tg, taskCtx.Id)

	if !tg.Closed() {
		if _, err := meta.Call(taskCtx.call, taskCtx.args); err != nil {
			log.Infof("Error caught from task: %v", err)
		}
	}
}

func (tg *TaskGroup) Start() (err error) {
	if tg.Closed() {
		panicTaskGroupClosed(tg.Config.Name)
	}

	go tg.worker()

	return
}

func (tg *TaskGroup) Schedule(call interface{}, args... interface{}) (id int, err error) {
	switch {
		case !meta.IsFunction(call):
			err = fmt.Errorf("Task call must be a function.")

		case tg.Closed():
			panicTaskGroupClosed(tg.Config.Name)

		default:
			stackTrace := []byte("No stack trace recorded. Enable DEBUG.")

			if log.Level() == log.DEBUG {
				stackTrace = make([]byte, 1024)

				for {
					if written := runtime.Stack(stackTrace, false); written < len(stackTrace) {
						stackTrace = stackTrace[:written]
						break
					}

					stackTrace = make([]byte, len(stackTrace)*2)
				}
			}

			// Generate the next task id and submit it
			id = tg.taskTracker.CheckIn(&TrackedTask {
				call: call,
				schedulerStack: stackTrace,
			})

			tg.taskChan <- &TaskContext {
				Id: id,
				call: call,
				args: args,
			}
	}

	return
}

func (tg *TaskGroup) Join(timeout time.Duration) (err error) {
	var waitedNanos int64
	waitForever := timeout == 0

	for waitedNanos = 0; waitForever || waitedNanos < timeout.Nanoseconds(); {
		then := time.Now()

		done := false
		tg.lockCtx(func() {
			active := tg.taskTracker.NumActive()
			done = active == 0
		})

		if done {
			break
		}

		time.Sleep(100 * time.Millisecond)
		waitedNanos += time.Now().Sub(then).Nanoseconds()
	}

	if !waitForever && waitedNanos >= timeout.Nanoseconds() {
		errMsg := fmt.Sprintf("Giving up waiting for TaskGroup: %s to finish execution.\n\nTasks Remaining\n", tg.Config.Name)

		for _, activeRef := range tg.taskTracker.Active() {
			taskInfo := activeRef.(*TrackedTask)
			errMsg += fmt.Sprintf("Task %T\n%s\n\n", taskInfo.call, taskInfo.schedulerStack)
		}

		err = fmt.Errorf(errMsg)
	}

	return
}