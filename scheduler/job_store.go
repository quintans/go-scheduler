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
}

func (s *StoreTask) IsOK() bool {
	return s.Retry == 0
}
