# Go Scheduler - AI Coding Assistant Instructions

## Architecture Overview

This is a **distributed job scheduling library** for Go that allows jobs to run across multiple instances using pluggable storage backends. The core pattern is:

- **Jobs** define work to be done (`Execute()` method)
- **Triggers** calculate next run times (cron or simple intervals)  
- **Stores** persist and coordinate job state across instances
- **Scheduler** orchestrates execution with locking/heartbeat mechanisms

## Key Components & Patterns

### Job Registration vs Scheduling
```go
// Register job types BEFORE starting scheduler
sched.RegisterJob(job, scheduler.WithTrigger(cronTrigger))

// Schedule specific job instances AFTER starting
sched.ScheduleJob(ctx, "unique-slug", job, scheduler.WithDelay(delay))
```

**Critical**: Jobs must be registered on all scheduler instances, but scheduled only once across the distributed system.

### Store Implementations
- `memory/` - In-process priority queue (single instance only)
- `postgres/` - PostgreSQL with advisory locking for distribution
- `firestore/` - Google Firestore for cloud distribution

All stores implement the same `JobStore` interface with `Lock()`, `Reschedule()`, `NextRun()` methods for distributed coordination.

### Error Handling & Backoff
Jobs returning errors are automatically retried using configurable backoff strategies:
- `ExponentialBackoff` (default) - doubles delay up to max
- `FixedBackoff` - predefined retry intervals
- Return `nil` from `Job.Execute()` to permanently remove job

### Distributed Coordination
- Scheduler uses **heartbeat** mechanism (default 1min) to detect next job
- **Jitter** option prevents thundering herd in multi-instance setups
- Jobs are **locked** during execution to prevent duplicate processing
- `StoreTask.Version` field enables optimistic concurrency control

## Development Workflows

### Running Examples
```bash
go run examples/main.go  # Demonstrates both scheduler and job patterns
```

### Testing (includes Docker containers)
```bash
make test  # Runs all tests with race detection, cleans up containers
```

### Storage Integration Testing
Tests use testcontainers-go for real PostgreSQL/Firestore integration. Look at `store/*_test.go` for patterns.

## Job Implementation Patterns

### Standard Job Structure
```go
type MyJob struct {
    kind string
}

func (j *MyJob) Kind() string { return j.kind }

func (j *MyJob) Execute(ctx context.Context, task *scheduler.StoreTask) (*scheduler.StoreTask, error) {
    // Use task.Payload for input data
    // Set task.Result for output/logging
    // Return task to continue, nil to remove, error to retry
}
```

### Built-in Job Types
- `ShellJob` - Execute shell commands (payload = command)
- `CurlJob` - HTTP requests with configurable method/headers
- `PrintJob` (examples only) - Simple logging job

## Cron Expression Support

Uses 6-7 field cron format: `<second> <minute> <hour> <day-of-month> <month> <day-of-week> [<year>]`

Special expressions: `@yearly`, `@monthly`, `@weekly`, `@daily`, `@hourly`

**Important**: Cannot specify both day-of-month AND day-of-week (use `?` wildcard for one)

## Configuration Patterns

### Scheduler Options
```go
scheduler.NewStdScheduler(store,
    scheduler.StdSchedulerHeartbeatOption(30*time.Second),
    scheduler.StdSchedulerJitterOption(5*time.Second),
)
```

### Job Registration Options
```go
sched.RegisterJob(job,
    scheduler.WithTrigger(cronTrigger),
    scheduler.WithBackoff(customBackoff),
)
```

## Common Integration Points

- **Context cancellation** for graceful shutdown
- **Payload serialization** for job parameters (typically JSON in []byte)
- **Result field** for execution output/error logging
- **Slug uniqueness** for job identification across instances