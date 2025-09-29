# Go Scheduler

A simple, durable, and distributed job scheduling library for Go.

## Key Features

- **Distributed Operation**: Run multiple scheduler instances with automatic coordination
- **Pluggable Storage**: Support for in-memory, PostgreSQL, and Firestore backends
- **Cron Support**: Full Unix cron expression support with special shortcuts
- **Error Handling**: Automatic retry with configurable backoff strategies
- **Context Support**: Graceful shutdown with Go's context cancellation
- **Thread-Safe**: Safe for concurrent use across multiple goroutines

## Installation

```bash
go get github.com/quintans/go-scheduler
```

## Quick Start

```go
package main

import (
    "context"
    "time"
    
    "github.com/quintans/go-scheduler/scheduler"
    "github.com/quintans/go-scheduler/store/memory"
    "github.com/quintans/go-scheduler/trigger"
)

func main() {
    // Create a memory store and scheduler
    ctx := context.Background()
    store := memory.New() // there are others
    sched := scheduler.NewScheduler(store)
    
    // Create and register a shell (fictional) job
    shellJob := NewShellJob()
    cronTrigger, _ := trigger.NewCronTrigger("@every 30s")
    // Schedule a job to run a command
    sched.ScheduleJob(ctx, "hello-world", shellJob, cronTrigger)

    // Start the scheduler
    sched.Start(ctx, 1)

    // ...
}
```

## Getting Started

### Core Interfaces

#### Job Interface

The Job interface is responsible for executing the actual work:

```go
type Job interface {
    // Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
    // If a nil StoreTask is returned, it will be removed from the scheduler.
    // If an error is returned it will be rescheduled with a backoff.
    Execute(context.Context, *StoreTask) (*StoreTask, error)
}
```

#### Trigger Interface

The Trigger interface is responsible for computing the next execution time:

```go
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
    // Next returns the next trigger time.
    Next(prev time.Time) time.Time
}
```

#### JobStore Interface

The JobStore interface defines where and how job tasks are persisted:

```go
// JobStore represents persistent storage for scheduled jobs.
type JobStore interface {
	// Create schedules a new task.
	Create(context.Context, *StoreTask) error
	
	// NextRun finds the next task scheduled to run.
	NextRun(context.Context) (*StoreTask, error)
	
	// Lock finds and locks the next task to be executed.
	Lock(context.Context, *StoreTask) (*StoreTask, error)
	
	// Reschedule releases the lock and updates task data for the next run.
	Reschedule(context.Context, *StoreTask) error
	
	// GetSlugs returns all job identifiers.
	GetSlugs(context.Context) ([]string, error)
	
	// Get retrieves a stored task by its identifier.
	Get(ctx context.Context, slug string) (*StoreTask, error)
	
	// Delete removes a stored task.
	Delete(ctx context.Context, slug string) error
	
	// Clear removes all stored tasks.
	Clear(context.Context) error
}
```

Available storage implementations:
- `Memory` - In-memory storage (single instance only)
- `PostgreSQL` - PostgreSQL with advisory locking for distributed coordination
- `Firestore` - Google Cloud Firestore for distributed coordination

## Examples

Consider the job:

```go
// PrintJob implements the scheduler.Job interface.
type PrintJob struct {}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (PrintJob) Execute(_ context.Context, st *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	fmt.Println(string(st.Payload))
	return st, nil
}
```

### Repeating Jobs

```go
ctx := context.Background()
store := memory.New()
sched := scheduler.NewScheduler(store)

// Create a cron trigger that fires every 5 seconds
cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
pj := &PrintJob{}

// If this job is already scheduled by another process, it will be ignored
sched.ScheduleJob(ctx, "print-job", pj, cronTrigger)

sched.Start(ctx)
```

### One-off Jobs

```go
ctx := context.Background()
store := // ...
sched := scheduler.NewScheduler(store)

pj := &PrintJob{}
// Register for one-off execution
sched.RegisterOneOffJob("print-job", pj) 

sched.Start(ctx)

// Schedule a one-off job with a payload
// If already scheduled by another process, it will be ignored
sched.ScheduleOneOffJob(
    ctx, 
    "print-job:"+uuid.NewString(), 
    time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
    []byte("9bfadfd6-8e62-4c58-9b6e-636d666b6643"),
)
```

## Acknowledgements

This library is inspired by [go-quartz](https://github.com/reugn/go-quartz) and has been expanded to support distributed operation across multiple instances, depending on the storage backend implementation.
