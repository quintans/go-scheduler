# Go Scheduler

A simple, durable, and distributed job scheduling library for Go.

## Key Features

- **Distributed Operation**: Run multiple scheduler instances with automatic coordination
- **Pluggable Storage**: Support for in-memory, PostgreSQL, Firestore, and MongoDB backends
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

#### Executer Interface

The Executer interface is responsible for executing the actual work:

```go
type Executer interface {
    // Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
    // If a nil Task is returned, it will be removed from the scheduler.
    // If an error is returned it will be rescheduled with a backoff.
    Execute(context.Context, *Task) (*Task, error)
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

#### Store Interface

The Store interface defines where and how tasks (the persisted state of scheduled jobs) are persisted:

```go
// Store represents persistent storage for the tasks to be executed.
type Store interface {
	// Create schedules a new task.
	Create(context.Context, *Task) error
	
	// NextRun finds the next task scheduled to run.
	NextRun(context.Context) (*Task, error)
	
	// Lock finds and locks the next task to be executed.
	Lock(context.Context, *Task) (*Task, error)
	
	// Reschedule releases the lock and updates the task data for the next run.
	Reschedule(context.Context, *Task) error
	
	// Suspend marks the task as suspended: it is skipped by NextRun and cannot be locked until resumed.
	Suspend(ctx context.Context, slug string) error
	
	// Resume clears the suspension and sets the next run time of the task.
	Resume(ctx context.Context, slug string, when time.Time) error
	
	// GetSlugs returns the slugs of all the tasks.
	GetSlugs(context.Context) ([]string, error)
	
	// Get retrieves a task by its slug.
	Get(ctx context.Context, slug string) (*Task, error)
	
	// Delete removes a task.
	Delete(ctx context.Context, slug string) error
	
	// Clear removes all the tasks.
	Clear(context.Context) error
}
```

Available storage implementations:
- `Memory` - In-memory storage (single instance only)
- `PostgreSQL` - PostgreSQL with advisory locking for distributed coordination
- `Firestore` - Google Cloud Firestore for distributed coordination
- `MongoDB` - MongoDB with versioned atomic updates for distributed coordination (no replica set required)

Every store has a `CreateSchema(ctx)` method that creates whatever the store needs (table, collection, indexes). It is idempotent, so it can be called on every start-up:

```go
store := postgres.New(db) // or mongodb.New(...), firestore.New(...), memory.New()
if err := store.CreateSchema(ctx); err != nil {
    // ...
}
```

- `PostgreSQL` creates the `schedules` table (or the one given with `TableOption`) and an index on `(run_at, locked_until)`.
- `MongoDB` creates the `schedules` collection (or the one given with `CollectionOption`) and an index on `(run_at, locked_until)`.
- `Firestore` and `Memory` need no setup, so it is a no-op.

If you prefer to manage the PostgreSQL schema yourself (e.g. with a migration tool), this is the table that the store expects:

```sql
CREATE TABLE IF NOT EXISTS schedules (
    slug TEXT PRIMARY KEY,
    payload BYTEA,
    run_at TIMESTAMP NOT NULL,
    version INTEGER NOT NULL,
    retry INTEGER NOT NULL,
    result TEXT,
    trigger_expr TEXT,
    suspended BOOLEAN NOT NULL DEFAULT FALSE,
    locked_until TIMESTAMP
);
CREATE INDEX IF NOT EXISTS schedules_run_at_lock_idx ON schedules (run_at, locked_until);
```

## Examples

Consider the job:

```go
// PrintExecuter implements the scheduler.Executer interface.
type PrintExecuter struct {}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
func (PrintExecuter) Execute(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
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
pj := &PrintExecuter{}

// If this job is already scheduled by another process, it will be ignored
sched.ScheduleJob(ctx, "print-job", pj, cronTrigger)

sched.Start(ctx, 1)
```

### Loading the Trigger from the Store

When a job is scheduled, its trigger expression is persisted alongside the job (cron expressions as given, simple triggers as `@every <duration>`).
`LoadScheduleJob` reads that expression back from the store and uses it to schedule the job, so the schedule can be changed directly in the store without a new deployment.
The provided trigger is only used as a default when the job is not in the store yet (or was stored without a trigger).

The stored expression is re-read every time the job runs, so editing it in the store while the scheduler is running takes effect on the next execution (an invalid expression is logged and the default trigger is used instead).
Jobs scheduled with `ScheduleJob` always keep the trigger given in code.

```go
ctx := context.Background()
store := // ...
sched := scheduler.NewScheduler(store)

// Used only the first time, when nothing is stored for "print-job" yet.
// On the next start-up the trigger stored for "print-job" takes precedence.
defaultTrigger, _ := trigger.NewCronTrigger("@hourly")
sched.LoadScheduleJob(ctx, "print-job", &PrintExecuter{}, defaultTrigger)

sched.Start(ctx, 1)
```

For the PostgreSQL store this requires a `trigger_expr TEXT` column in the schedules table. The MongoDB store persists it in the `trigger_expr` field.

### Suspending Jobs

A suspended job stays in the store but does not run until it is resumed. The suspension is persisted alongside the job, so it survives restarts and is honoured by every scheduler instance sharing the store.

```go
// no further runs of "print-job" until it is resumed; a run already in progress is not interrupted
sched.SuspendJob(ctx, "print-job")

sj, _ := sched.GetScheduledJob(ctx, "print-job")
fmt.Println(sj.Suspended) // true

sched.ResumeJob(ctx, "print-job")
```

On resume, if the next run time of the job is already in the past, it is recomputed from the job trigger so the runs missed while suspended are not executed; a one-off job (which has no trigger) keeps its run time and runs right away.

The scheduler never caches the suspension state, it is read from the store on every run. So a job suspended or resumed by another replica, or directly in the database, is picked up without any coordination:
- a suspended job is skipped when looking for the next run, and if a worker was already waiting for it the lock fails and the worker moves on;
- a resumed job is found at the latest on the next heartbeat (see `HeartbeatOption`). When resuming directly in the database, set the next run time (`run_at`) as well, otherwise a stale run time in the past makes the job run immediately.

For the PostgreSQL store this requires a `suspended BOOLEAN NOT NULL DEFAULT FALSE` column in the schedules table. The MongoDB and Firestore stores persist it in the `suspended` field (a missing field means not suspended).

### One-off Jobs

```go
ctx := context.Background()
store := // ...
sched := scheduler.NewScheduler(store)

pj := &PrintExecuter{}
// Register for one-off execution
sched.RegisterOneOffJob("print-job", pj) 

sched.Start(ctx, 1)

// Schedule a one-off job with a payload.
// Each call creates a new job with a unique slug of the form "print-job:<uuid>",
// which is returned so that it can be suspended, resumed or deleted later.
slug, err := sched.ScheduleOneOffJob(
    ctx, 
    "print-job", 
    time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
    []byte("9bfadfd6-8e62-4c58-9b6e-636d666b6643"),
)
```

## Acknowledgements

This library is inspired by [go-quartz](https://github.com/reugn/go-quartz) and has been expanded to support distributed operation across multiple instances, depending on the storage backend implementation.
