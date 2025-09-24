# Go Scheduler

A simple, durable, and distributed job scheduling library for Go.

## Overview

This library is inspired by [go-quartz](https://github.com/reugn/go-quartz) and has been expanded to support distributed operation across multiple instances, depending on the storage backend implementation.

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
    store := memory.New()
    sched := scheduler.NewStdScheduler(store)
    
    // Create and register a shell job
    shellJob := scheduler.NewShellJob()
    cronTrigger, _ := trigger.NewCronTrigger("@every 30s")
    sched.RegisterJob(shellJob, scheduler.WithTrigger(cronTrigger))
    
    // Start the scheduler
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    sched.Start(ctx)
    
    // Schedule a job to run a command
    sched.ScheduleJob(ctx, "hello-world", shellJob, 
        scheduler.WithDelay(time.Second),
        scheduler.WithPayload([]byte("echo 'Hello, World!'")))
    
    // Let it run for a minute
    time.Sleep(time.Minute)
}
```

## Getting Started

### Core Interfaces

#### Job Interface

The Job interface is responsible for executing the actual work:

```go
type Job interface {
    // Kind returns the type of the job.
    Kind() string
    // Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
    // If a nil StoreTask is returned, it will be removed from the scheduler.
    // If an error is returned it will be rescheduled with a backoff.
    Execute(context.Context, *StoreTask) (*StoreTask, error)
}
```

Built-in job implementations:
- `PrintJob` - Simple logging job (for demonstration)
- `ShellJob` - Execute shell commands
- `CurlJob` - Make HTTP requests

> **Note:** These implementations exist primarily for demonstration purposes.

#### Trigger Interface

The Trigger interface is responsible for computing the next execution time:

```go
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
    // NextFireTime returns the next time at which the Trigger is scheduled to fire.
    NextFireTime(prev time.Time) (time.Time, error)
}
```

Available trigger implementations:
- `CronTrigger` - Unix cron-style scheduling
- `SimpleTrigger` - Simple interval-based scheduling

#### Scheduler Interface

The Scheduler interface orchestrates job execution:

```go
// A Scheduler orchestrates job execution.
// Schedulers are responsible for executing Jobs when their associated Triggers fire.
type Scheduler interface {
	// RegisterJob registers a job and its trigger.
	// Returns ErrJobAlreadyExists if the job is already registered.
	// This should be called before starting the scheduler.
	RegisterJob(job Job, options ...RegisterJobOption) error
	
	// Start begins the scheduler's execution loop.
	Start(context.Context)
	
	// ScheduleJob schedules a specific job instance for execution.
	// Returns ErrJobAlreadyScheduled if already scheduled by another process.
	// The job must be registered first and will be picked up by any running instance.
	ScheduleJob(ctx context.Context, slug string, job Job, options ...ScheduleJobOption) error
	
	// GetJobSlugs returns all scheduled job identifiers.
	GetJobSlugs(context.Context) ([]string, error)
	
	// GetScheduledJob retrieves metadata for a scheduled job.
	GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error)
	
	// DeleteJob removes a job from the execution queue.
	DeleteJob(ctx context.Context, slug string) error
	
	// Clear removes all scheduled jobs.
	Clear(context.Context) error
}
```

Available scheduler implementations:
- `StdScheduler` - Standard scheduler with configurable heartbeat and jitter

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

### Repeating Jobs

```go
store := memory.New()
sched := scheduler.NewStdScheduler(store)

// Create a cron trigger that fires every 5 seconds
cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
curlJob, err := scheduler.NewCurlJob(
    "curl-clock",
    http.MethodGet, 
    "http://worldclockapi.com/api/json/est/now", 
    "", 
    nil,
)
if err != nil {
    log.Fatal(err)
}

// Register the job with its trigger
sched.RegisterJob(curlJob, scheduler.WithTrigger(cronTrigger))

ctx, cancel := context.WithCancel(context.Background())
sched.Start(ctx)

// Schedule the job to start after the first delay
delay, _ := cronTrigger.FirstDelay()
// If this job is already scheduled by another process, it will be ignored
sched.ScheduleJob(ctx, "curl-now", curlJob, scheduler.WithDelay(delay))

time.Sleep(time.Second * 10)
cancel()
```

### One-time Jobs

```go
// conn is your database connection
conn := // ... your database connection
store := postgres.New(conn)
sched := scheduler.NewStdScheduler(store)

cancelTxJob := scheduler.NewCancelTransactionJob()
// Register without a trigger for one-time execution
sched.RegisterJob(cancelTxJob) 

ctx, cancel := context.WithCancel(context.Background())
sched.Start(ctx)

// Schedule a one-time job with a payload
// If already scheduled by another process, it will be ignored
err := sched.ScheduleJob(ctx, "cancel-tx", cancelTxJob, 
    scheduler.WithDelay(time.Second),
    scheduler.WithPayload([]byte("9bfadfd6-8e62-4c58-9b6e-636d666b6643")))
if err != nil {
    log.Printf("Failed to schedule job: %v", err)
}

time.Sleep(time.Second * 5)
cancel()
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
