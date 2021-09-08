# scheduler
Simple durable and distributed scheduling library for go.

## About
inspired by [go-quarts](https://github.com/reugn/go-quartz) and expanded to be able to run running in a distributed mode depending on the storage implementation.

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
type Scheduler interface {
    // RegisterJob registers the job and trigger.
    // Fails if job already registered.
    // If trigger is nil, the associated job will only run once.
    RegisterJob(job Job, trigger trigger.Trigger) error
    // start the scheduler
    Start(context.Context)
    // schedule the job with the specified trigger
    ScheduleJob(ctx context.Context, slug string, job Job, payload []byte, firstDelay time.Duration) error
    // get keys of all of the scheduled jobs
    GetJobSlugs(context.Context) ([]string, error)
    // get the scheduled job metadata
    GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error)
    // remove the job from the execution queue
    DeleteJob(ctx context.Context, slug string) error
    // clear all the scheduled jobs
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
    NextRun(context.Context) (time.Time, error)
    // Lock find and locks a the next task to be run
    Lock(context.Context) (*StoreTask, error)
    // Release releases the acquired lock and updates the data for the next run
    Release(context.Context, *StoreTask) error
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
- MemStore
- PgStore

## Example

### Repeating

```go
    store := store.NewMemStore()
    sched := scheduler.NewStdScheduler(store)

    cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
    curlJob, err := scheduler.NewCurlJob(
        "curl-clock",
        http.MethodGet, 
        "http://worldclockapi.com/api/json/est/now", 
        "", 
        nil,
    )

    sched.RegisterJob(curlJob, cronTrigger)

    ctx, cancel := context.WithCancel(context.Background())
    sched.Start(ctx)
    delay, _ := cronTrigger.FirstDelay()
    sched.ScheduleJob(ctx, "curl-now", curlJob, nil, delay)
    time.Sleep(time.Second * 2)
    cancel()
```

### Once

```go
    conn = ...
    store := store.NewPgStore(conn)
    sched := scheduler.NewStdScheduler(store)

    cancelTxJob := scheduler.NewCancelTransactionJob()
    // nil triggers means non repeatable
    sched.RegisterJob(cancelTxJob, nil) 

    ctx, cancel := context.WithCancel(context.Background())
    sched.Start(ctx)
    sched.ScheduleJob(ctx, "cancel-tx", cancelTxJob, "9bfadfd6-8e62-4c58-9b6e-636d666b6643", time.Second)
    time.Sleep(time.Second * 2)
    cancel()
```
