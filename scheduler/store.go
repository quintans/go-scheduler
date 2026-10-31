package scheduler

import (
	"context"
	"time"
)

// Store represents the persistent store for the tasks to be executed
type Store interface {
	// Create schedules a new task
	Create(context.Context, *Task) error
	// NextRun finds the next task to be run
	NextRun(context.Context) (*Task, error)
	// Lock finds and locks the next task to be run
	Lock(context.Context, *Task) (*Task, error)
	// Reschedule releases the acquired lock and updates the data for the next run
	Reschedule(context.Context, *Task) error
	// Suspend marks the task as suspended. A suspended task is ignored by NextRun and cannot be locked,
	// until it is resumed. The lock, if any, is left untouched.
	// Fails with ErrTaskNotFound if the task does not exist.
	Suspend(ctx context.Context, slug string) error
	// Resume clears the suspension of the task and sets its next run time to when.
	// The lock, if any, is left untouched.
	// Fails with ErrTaskNotFound if the task does not exist.
	Resume(ctx context.Context, slug string, when time.Time) error
	// GetSlugs gets the slugs of all the tasks
	GetSlugs(context.Context) ([]string, error)
	// Get gets a task by its slug
	Get(ctx context.Context, slug string) (*Task, error)
	// Delete deletes a task
	Delete(ctx context.Context, slug string) error
	// Clear deletes all the tasks
	Clear(context.Context) error
}

// Task is the persisted state of a scheduled run of a Job: its payload, when it must run next,
// how many times it has been retried and the trigger expression used to reschedule it.
// Tasks are created by the Scheduler when a Job is registered or a one-off job is scheduled,
// and are handed to the Executer when they are due.
type Task struct {
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

// IsOK reports whether the last execution of the task succeeded (no retry is pending).
func (s *Task) IsOK() bool {
	return s.Retry == 0
}
