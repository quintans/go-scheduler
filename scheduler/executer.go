package scheduler

import (
	"context"
)

// Executer represents the work to be performed.
type Executer interface {
	// Name identifies the executer. It is persisted with each of its tasks, so that every scheduler instance
	// sharing the store can find the executer to run them, and it is the default slug of its jobs.
	Name() string
	// Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
	// If a nil Task is returned, it will be removed from the scheduler.
	// If an error is returned it will be rescheduled with a backoff.
	Execute(context.Context, *Task) (*Task, error)
}

// ExecuterFunc adapts a function to the Executer interface, under the given name.
func ExecuterFunc(name string, fn func(context.Context, *Task) (*Task, error)) Executer {
	return funcExecuter{name: name, fn: fn}
}

type funcExecuter struct {
	name string
	fn   func(context.Context, *Task) (*Task, error)
}

func (e funcExecuter) Name() string {
	return e.name
}

func (e funcExecuter) Execute(ctx context.Context, st *Task) (*Task, error) {
	return e.fn(ctx, st)
}
