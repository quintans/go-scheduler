# scheduler
Simple durable and distributed scheduling library for go.

## About
inspired by [go-quartz](https://github.com/reugn/go-quartz) and expanded to be able to run in a distributed mode depending on the storage implementation.

## How To

### Job interface

responsible to do the work

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

Implemented examples:
- PrintJob
- ShellJob
- CurlJob

> These exists for demonstration purposes only

### Trigger interface

responsible to compute the next run time

```go
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
    // NextFireTime returns the next time at which the Trigger is scheduled to fire.
    NextFireTime(prev time.Time) (time.Time, error)
}
```

Available implementations:
- CronTrigger
- SimpleTrigger

### Scheduler interface
the orchestrator

```go
// A Scheduler is the Jobs orchestrator.
// Schedulers responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler interface {
	// RegisterJob registers the job and trigger
	// Fails with ErrJobAlreadyScheduled if job already registered.
	//
	// This should be used to register the jobs before starting the scheduler.
	RegisterJob(job Job, options ...RegisterJobOption) error
	// Start starts the scheduler
	Start(context.Context)
	// ScheduleJob will attempt to schedule a job, failing with ErrJobAlreadyScheduled if it was already scheduled by another process
	// and since the job is already registered it will be picked up by one of the concurrent processes.
	ScheduleJob(ctx context.Context, slug string, job Job, options ...ScheduleJobOption) error
	// GetJobSlugs get slugs of all of the scheduled jobs
	GetJobSlugs(context.Context) ([]string, error)
	// GetScheduledJob get the scheduled job metadata
	GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error)
	// DeleteJob remove the job from the execution queue
	DeleteJob(ctx context.Context, slug string) error
	// Clear clear all the scheduled jobs
	Clear(context.Context) error
}
```

Available implementations:
- StdScheduler

### JobStore interface
where to store job tasks

```go
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
```

Available implementations:
- Memory
- PostgreSQL
- Firestore

## Example

### Repeating

```go
    store := memory.New()
    sched := scheduler.NewStdScheduler(store)

    cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
    curlJob, err := scheduler.NewCurlJob(
        "curl-clock",
        http.MethodGet, 
        "http://worldclockapi.com/api/json/est/now", 
        "", 
        nil,
    )

    sched.RegisterJob(curlJob, scheduler.WithTrigger(cronTrigger))

    ctx, cancel := context.WithCancel(context.Background())
    sched.Start(ctx)
    delay, _ := cronTrigger.FirstDelay()
    // if the store already has this schedule, meaning this was already scheduled by another process, it will be ignore
    sched.ScheduleJob(ctx, "curl-now", curlJob, scheduler.WithDelay(delay))
    time.Sleep(time.Second * 2)
    cancel()
```

### Once

```go
    conn = ...
    store := postgres.New(conn)
    sched := scheduler.NewStdScheduler(store)

    cancelTxJob := scheduler.NewCancelTransactionJob()
    // no trigger means non repeatable
    sched.RegisterJob(cancelTxJob) 

    ctx, cancel := context.WithCancel(context.Background())
    sched.Start(ctx)
    // if the store already has this schedule, meaning this was already scheduled by another process, it will be ignore
    sched.ScheduleJob(ctx, "cancel-tx", cancelTxJob, scheduler.WithDelay(time.Second), scheduler.WithPayload([]byte("9bfadfd6-8e62-4c58-9b6e-636d666b6643"))
    time.Sleep(time.Second * 2)
    cancel()
```
