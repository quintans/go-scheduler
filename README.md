# Go Scheduler

A simple, durable, and distributed job scheduling library for Go.

## Key Features

- **Distributed Operation**: Run multiple scheduler instances with automatic coordination
- **Named Executers**: Schedule any number of recurring or one-off jobs for the same executer, each with its own slug, trigger and payload
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
    
    // Schedule a job for a shell (fictional) executer: the slug defaults to the executer name
    cronTrigger, _ := trigger.NewCronTrigger("@every 30s")
    sched.ScheduleJob(ctx, NewShellJob(), cronTrigger, scheduler.WithPayload([]byte("echo hello")))

    // Start the scheduler, after everything is registered and scheduled
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
    // Name identifies the executer. It is persisted with each of its tasks and is the default slug of its jobs.
    Name() string
    // Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
    // If a nil Task is returned, it will be removed from the scheduler.
    // If an error is returned it will be rescheduled with a backoff.
    Execute(context.Context, *Task) (*Task, error)
}
```

`scheduler.ExecuterFunc(name, fn)` adapts a function to the interface.

Tasks reference their executer by name, which is persisted with each of them, so any number of jobs can share one executer.

#### Registration

A replica only runs the tasks of the executers it knows, and it must know them **before it starts**. The tasks of any other executer are left alone in the store, so a rollout that adds an executer is safe: the old replicas ignore its tasks and the new ones run them. A task whose trigger kind is unknown is backed off (see [Persisted Triggers](#persisted-triggers)). The tasks of an executer that no longer exists are never run: remove them with `DeleteJob`.
The scheduler learns the executers and the trigger kinds from the calls you make before `Start`:

- **Passing an executer and a trigger to `ScheduleJob` is all it takes.** There is nothing to register for them.
- `RegisterExecuter` and `RegisterTrigger` are for the executers and trigger kinds that this instance does not pass to `ScheduleJob` before `Start`, but whose tasks it must be able to run. That is the case when:
  - the runs are scheduled with `ScheduleOnce`, which refers to the executer by name and needs it registered (`ErrExecuterNotFound` otherwise);
  - the trigger is set with `ChangeJobTrigger`, which needs its kind registered (`ErrUnknownTrigger` otherwise);
  - the tasks are created or changed directly in the store, with no `ScheduleJob` call in the code at all. For example an admin tool sharing the store can create a task for a registered executer, with a payload and a trigger expression of a registered kind, or change the expression of an existing one, and the running replicas pick it up without a deployment;
  - the executer needs options, such as the backoff used to retry its failed runs.

  ```go
  sched.RegisterExecuter(&ReportExecuter{}, scheduler.WithBackoff(backoff))
  sched.RegisterTrigger(StepTrigger{}) // any instance will do, such as the zero value
  ```

Call `Start` after everything is scheduled and registered: nothing can be registered afterwards. Scheduling after `Start` still works with the executers and trigger kinds the scheduler already knows, and fails with `ErrExecuterNotFound` or `ErrUnknownTrigger` otherwise.

#### Trigger Interface

The Trigger interface is responsible for computing the next execution time:

```go
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
    // Name identifies the kind of trigger. It selects the trigger kind that rebuilds it from its Expression.
    Name() string
    // Next returns the next trigger time.
    Next(prev time.Time) time.Time
    // Expression returns the string representation of the trigger, parsable by Parse.
    // It is what gets persisted in the store. An empty string means the trigger cannot be persisted.
    Expression() string
    // Parse rebuilds a trigger of this kind from an expression previously returned by Expression.
    Parse(expr string) (Trigger, error)
}
```

Two triggers are provided, and registered in every scheduler: `trigger.CronTrigger` (name `cron`) and `trigger.SimpleTrigger` (name `every`).
See [Custom Triggers](#custom-triggers) to use your own.

#### Store Interface

The Store interface defines where and how tasks (the persisted state of scheduled jobs) are persisted:

```go
// Store represents persistent storage for the tasks to be executed.
type Store interface {
	// Create schedules a new task.
	Create(context.Context, *Task) error
	
	// NextRun finds the next task scheduled to run among those of the given executers.
	NextRun(ctx context.Context, executers []string) (*Task, error)
	
	// Lock finds and locks the next task to be executed.
	Lock(context.Context, *Task) (*Task, error)
	
	// Reschedule releases the lock and updates the task data for the next run.
	Reschedule(context.Context, *Task) error
	
	// Suspend marks the task as suspended: it is skipped by NextRun and cannot be locked until resumed.
	Suspend(ctx context.Context, slug string) error
	
	// Resume clears the suspension and sets the next run time of the task.
	Resume(ctx context.Context, slug string, when time.Time) error
	
	// UpdateTrigger replaces the trigger expression of the task and sets its next run time.
	UpdateTrigger(ctx context.Context, slug string, expr string, when time.Time) error
	
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
    executer TEXT NOT NULL,
    payload BYTEA,
    run_at TIMESTAMP NOT NULL,
    version INTEGER NOT NULL,
    retry INTEGER NOT NULL,
    result TEXT,
    trigger_expr TEXT,
    suspended BOOLEAN NOT NULL DEFAULT FALSE,
    locked_until TIMESTAMP
);
CREATE INDEX IF NOT EXISTS schedules_executer_run_at_lock_idx ON schedules (executer, run_at, locked_until);
```

## Examples

Consider the job:

```go
// PrintExecuter implements the scheduler.Executer interface.
type PrintExecuter struct {}

func (PrintExecuter) Name() string { return "print" }

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

// The slug (the task id in the store) defaults to the executer name.
// If this job is already scheduled by another process, it will be ignored
sched.ScheduleJob(ctx, &PrintExecuter{}, cronTrigger, scheduler.WithPayload([]byte("hello")))

sched.Start(ctx, 1)
```

### Several Jobs for the Same Executer

An executer can run as many jobs as needed, each with its own slug, trigger and payload.

```go
report := &ReportExecuter{} // named "report"
daily, _ := trigger.NewCronTrigger("@daily")
weekly, _ := trigger.NewCronTrigger("@weekly")
sched.ScheduleJob(ctx, report, daily, scheduler.WithSlug("report-daily"), scheduler.WithPayload([]byte("sales")))
sched.ScheduleJob(ctx, report, weekly, scheduler.WithSlug("report-weekly"), scheduler.WithPayload([]byte("sales")))

sched.Start(ctx, 4)

sj, _ := sched.GetScheduledJob(ctx, "report-weekly")
fmt.Println(sj.Executer, sj.Trigger, sj.NextRunTime) // report cron: @weekly ...
```

Jobs can also be scheduled after `Start`, from any instance sharing the store, as long as that instance already knows the executer and the trigger kind (see [Registration](#registration)).

The first run time defaults to the next trigger time; `WithWhen` or `WithDelay` set another.
Scheduling a job whose slug is already in the store changes nothing (the stored task wins, see below), but fails with `ErrExecuterMismatch` if it was stored for another executer, and with `ErrOneOffJob` if it is a one-off task.

### Persisted Triggers

The trigger persisted in the store always takes precedence over the one given in code.
`ScheduleJob` only uses the given trigger when the job is not in the store yet; in that case it is persisted as `<name>: <expression>` (e.g. `cron: @hourly`, `every: 5m0s`), so the schedule can later be changed in the store without a new deployment.
Every trigger must be persistable: it needs a non-empty `Expression()` (`ErrTriggerNotPersistent`), and its name selects the kind of trigger whose `Parse` rebuilds it when the expression is read back (see [Custom Triggers](#custom-triggers)).

The stored expression is re-read every time the job is due, so a change made while the scheduler is running takes effect on the next execution.
If the stored expression is invalid the job does not run: the error is logged and recorded in the task (`Result`, `Retry`), and it is retried with the executer backoff until the expression is fixed.

```go
ctx := context.Background()
store := // ...
sched := scheduler.NewScheduler(store)

// Used only the first time, when nothing is stored for "print" yet.
// On the next start-up the trigger stored for "print" takes precedence.
hourly, _ := trigger.NewCronTrigger("@hourly")
sched.ScheduleJob(ctx, &PrintExecuter{}, hourly)

sched.Start(ctx, 1)
```

For the PostgreSQL store this requires a `trigger_expr TEXT` column in the schedules table. The MongoDB store persists it in the `trigger_expr` field.

### Changing the Trigger

Instead of editing the store by hand, the trigger of a job can be changed through the scheduler.
The new expression is persisted, so the change survives restarts and is honoured by every scheduler instance sharing the store, and the next run time is recomputed from now with the new trigger.

```go
// from now on "print" runs every 5 minutes
sched.ChangeJobTrigger(ctx, "print", trigger.NewSimpleTrigger(5*time.Minute))

sj, _ := sched.GetScheduledJob(ctx, "print")
fmt.Println(sj.NextRunTime) // now + 5 minutes
```

A few things to keep in mind:
- the executer does not need to be registered in the instance that makes the change (e.g. an admin tool sharing the store); other instances pick the change up at the latest on the next heartbeat;
- a suspended job keeps its suspension, the new trigger is used once it is resumed;
- if a run is in progress, it is rescheduled with the trigger it was started with, the new trigger takes effect on the run after that one;
- the trigger must have a non-empty `Expression()` (as cron and simple triggers do), otherwise it fails with `ErrTriggerNotPersistent`;
- the kind of the trigger must be registered (see [Registration](#registration)), otherwise it fails with `ErrUnknownTrigger`;
- one-off tasks never recur, so changing their trigger fails with `ErrOneOffJob`.

### Custom Triggers

Any type implementing `trigger.Trigger` can be used: its `Parse` method is how the scheduler rebuilds it from the expression it persists.

```go
// fires on every multiple of step seconds, persisted as "step: <step>s"
type StepTrigger struct{ Step int }

func (StepTrigger) Name() string                    { return "step" }
func (t StepTrigger) Expression() string            { return fmt.Sprintf("%ds", t.Step) }
func (t StepTrigger) Next(prev time.Time) time.Time { return prev.Add(time.Duration(t.Step) * time.Second) }
func (StepTrigger) Parse(expr string) (trigger.Trigger, error) {
	step, err := strconv.Atoi(strings.TrimSuffix(expr, "s"))
	if err != nil {
		return nil, err
	}
	return StepTrigger{Step: step}, nil
}

sched := scheduler.NewScheduler(store)
// nothing to register: ScheduleJob learns the "step" kind from the trigger it is given
sched.ScheduleJob(ctx, &PrintExecuter{}, StepTrigger{Step: 30})
sched.Start(ctx, 1)
```

The name must not be empty nor contain `:` or whitespace, and `cron` and `every` are taken by the provided triggers.
A stored expression naming a kind unknown to this scheduler makes `ScheduleJob` fail with `ErrUnknownTrigger`; use `RegisterTrigger` for kinds that only exist in the store (see [Registration](#registration)).

### Suspending Jobs

A suspended job stays in the store but does not run until it is resumed. The suspension is persisted alongside the job, so it survives restarts and is honoured by every scheduler instance sharing the store.

```go
// no further runs of "print" until it is resumed; a run already in progress is not interrupted
sched.SuspendJob(ctx, "print")

sj, _ := sched.GetScheduledJob(ctx, "print")
fmt.Println(sj.Suspended) // true

sched.ResumeJob(ctx, "print")
```

On resume, if the next run time of the job is already in the past, it is recomputed from the job trigger so the runs missed while suspended are not executed; a one-off task (which has no trigger) keeps its run time and runs right away.

The scheduler never caches the suspension state, it is read from the store on every run. So a job suspended or resumed by another replica, or directly in the database, is picked up without any coordination:
- a suspended job is skipped when looking for the next run, and if a worker was already waiting for it the lock fails and the worker moves on;
- a resumed job is found at the latest on the next heartbeat (see `HeartbeatOption`). When resuming directly in the database, set the next run time (`run_at`) as well, otherwise a stale run time in the past makes the job run immediately.

For the PostgreSQL store this requires a `suspended BOOLEAN NOT NULL DEFAULT FALSE` column in the schedules table. The MongoDB and Firestore stores persist it in the `suspended` field (a missing field means not suspended).

### One-off Tasks

`ScheduleOnce` schedules a single run of an executer, given by name. The task executes once, immediately by default or when set by `WithWhen` or `WithDelay`, and is then removed: one-off tasks have no trigger and never recur.

The executer is referred to by its name rather than by an instance because `ScheduleOnce` never registers anything: the executer must be registered already (`ErrExecuterNotFound` otherwise), and the registered instance is what runs. One-off runs are usually scheduled after `Start`, from code that has no reason to hold the executer, such as an HTTP handler, so it is convenient to expose the name as a constant.

```go
const PrintExecuterName = "print"

func (PrintExecuter) Name() string { return PrintExecuterName }
```

```go
ctx := context.Background()
store := // ...
sched := scheduler.NewScheduler(store)

// ScheduleOnce needs the executer registered: every replica must know it before starting,
// otherwise a replica without it would leave the task in the store instead of running it
sched.RegisterExecuter(&PrintExecuter{})
sched.Start(ctx, 1)

// Schedule a one-off run with a payload.
// Each call creates a new task with a unique slug of the form "print:<uuid>",
// which is returned so that it can be suspended, resumed or deleted later.
slug, err := sched.ScheduleOnce(
    ctx,
    PrintExecuterName,
    scheduler.WithWhen(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)),
    scheduler.WithPayload([]byte("9bfadfd6-8e62-4c58-9b6e-636d666b6643")),
)

// With an explicit slug the run is scheduled at most once: a second call fails with ErrTaskAlreadyExists
_, err = sched.ScheduleOnce(ctx, PrintExecuterName, scheduler.WithSlug("order-42"), scheduler.WithPayload([]byte("42")))
```

## Acknowledgements

This library is inspired by [go-quartz](https://github.com/reugn/go-quartz) and has been expanded to support distributed operation across multiple instances, depending on the storage backend implementation.
