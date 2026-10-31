package scheduler

import (
	"context"
	"time"
)

// JobStore represents the store for the jobs to be executed
type JobStore interface {
	// Create schedule a new task
	Create(context.Context, *StoreTask) error
	// NextRun finds the next run time
	NextRun(context.Context) (*StoreTask, error)
	// Lock find and locks a the next task to be run
	Lock(context.Context, *StoreTask) (*StoreTask, error)
	// Reschedule releases the acquired lock and updates the data for the next run
	Reschedule(context.Context, *StoreTask) error
	// Suspend marks the task as suspended. A suspended task is ignored by NextRun and cannot be locked,
	// until it is resumed. The lock, if any, is left untouched.
	// Fails with ErrJobNotFound if the task does not exist.
	Suspend(ctx context.Context, slug string) error
	// Resume clears the suspension of the task and sets its next run time to when.
	// The lock, if any, is left untouched.
	// Fails with ErrJobNotFound if the task does not exist.
	Resume(ctx context.Context, slug string, when time.Time) error
	// GetSlugs gets all the slugs
	GetSlugs(context.Context) ([]string, error)
	// Get gets a stored task
	Get(ctx context.Context, slug string) (*StoreTask, error)
	// Delete deletes a stored task
	Delete(ctx context.Context, slug string) error
	// Clear all the tasks
	Clear(context.Context) error
}

type StoreTask struct {
	Slug    string
	Payload []byte
	When    time.Time
	Version int64
	Retry   int
	Result  string
	// Trigger is the string expression of the trigger used to schedule this task, if any.
	// It is set when the task is created and can be parsed with trigger.Parse.
	Trigger string
	// Suspended indicates that the task must not run until it is resumed.
	Suspended bool
}

func (s *StoreTask) IsOK() bool {
	return s.Retry == 0
}
