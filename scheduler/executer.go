package scheduler

import (
	"context"
)

// Executer represents the work to be performed.
type Executer interface {
	// Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
	// If a nil Task is returned, it will be removed from the scheduler.
	// If an error is returned it will be rescheduled with a backoff.
	Execute(context.Context, *Task) (*Task, error)
}

type ExecuterFn func(context.Context, *Task) (*Task, error)

func (fn ExecuterFn) Execute(ctx context.Context, st *Task) (*Task, error) {
	return fn(ctx, st)
}
