package concurrent

import (
	"fmt"
	"time"
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

func NewTaskGroup(config *TaskGroupConfig) (tg *TaskGroup) {
	return &TaskGroup {
		Config: config,
		closed: false,
		activeTasks: NewActivityTracker(config.MaxActiveWorkers),
		taskChan: make(chan *TaskContext, config.MaxQueuedTasks),
		editContext: NewLockerContext(),
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
	activeTasks Tracker
	taskChan chan *TaskContext
	editContext context.Context
}

func (tg *TaskGroup) Closed() (closed bool) {
	tg.editContext(func() {
		closed = tg.closed
	})

	return
}

func (tg *TaskGroup) Stop() {
	tg.editContext(func() {
		if tg.closed {
			panicTaskGroupClosed(tg.Config.Name)
		}

		tg.closed = true
	})

	close(tg.taskChan)
}

func (tg *TaskGroup) worker(id int) {
	defer tg.activeTasks.CheckOut(id)

	for !tg.Closed() {
		if task, ok := <- tg.taskChan; ok {
			go tg.dispatch(task)
		} else {
			break
		}
	}
}

func panicTaskGroupClosed(tgName string) {
	reason := fmt.Sprintf("TaskGroup %s already closed", tgName)
	panic(reason)
}

func cleanupDispatch(tg *TaskGroup, id int) {
	tg.activeTasks.CheckOut(id)

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

	id := tg.activeTasks.CheckIn(&TrackedTask {
		call: tg.worker,
		schedulerStack: stackTrace,
	})

	go tg.worker(id)

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
			id = tg.activeTasks.CheckIn(&TrackedTask {
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

func (tg *TaskGroup) Wait() {
	done := false

	for !done {
		tg.editContext(func() {
			done = tg.activeTasks.NumActive() == 0
		})

		time.Sleep(100 * time.Millisecond)
	}
}

func (tg *TaskGroup) Join(timeout time.Duration) {
	var waitedNanos int64
	waitForever := timeout == 0

	for waitedNanos = 0; waitForever || waitedNanos < timeout.Nanoseconds(); {
		then := time.Now()

		done := false
		tg.editContext(func() {
			done = tg.activeTasks.NumActive() == 0
		})

		if done {
			break
		}

		time.Sleep(100 * time.Millisecond)
		waitedNanos += time.Now().Sub(then).Nanoseconds()
	}

	if !waitForever && waitedNanos >= timeout.Nanoseconds() {
		errMsg := fmt.Sprintf("Giving up waiting for TaskGroup: %s to finish execution.\n\nTasks Remaining\n", tg.Config.Name)

		for _, activeRef := range tg.activeTasks.Active() {
			taskInfo := activeRef.(*TrackedTask)
			errMsg += fmt.Sprintf("Task %T\n%s\n\n", taskInfo.call, taskInfo.schedulerStack)
		}

		log.Error(errMsg)
	}
}