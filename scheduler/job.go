package scheduler

import (
	"context"
)

// Job represents the work to be performed.
type Job interface {
	// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
	// If a nil StoreTask is returned, it will be removed from the scheduler.
	// If an error is returned it will be rescheduled with a backoff.
	Execute(context.Context, *StoreTask) (*StoreTask, error)
}

type JobFn func(context.Context, *StoreTask) (*StoreTask, error)

func (fn JobFn) Execute(ctx context.Context, st *StoreTask) (*StoreTask, error) {
	return fn(ctx, st)
}
