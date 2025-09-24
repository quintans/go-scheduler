package scheduler

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/quintans/go-scheduler/trigger"
)

var (
	ErrJobNotFound         = errors.New("no job with the given Slug was found")
	ErrJobNotLocked        = errors.New("job lock was not acquired")
	ErrJobAlreadyScheduled = errors.New("job already scheduled")
	ErrJobAlreadyExists    = errors.New("job already exists")
)

const waitOnCalculateNextRunError = 10 * time.Second

type knownTasks struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

func newKnownTasks() *knownTasks {
	return &knownTasks{
		tasks: map[string]*Task{},
	}
}

func (m *knownTasks) add(jobKind string, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.tasks[jobKind]
	if ok {
		return ErrJobAlreadyExists
	}
	m.tasks[jobKind] = task
	return nil
}

func (m *knownTasks) get(jobKind string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.tasks[jobKind]
}

// Job represents the work to be performed.
type Job interface {
	// Kind returns the type of the job.
	Kind() string
	// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
	// If a nil StoreTask is returned, it will be removed from the scheduler.
	// If an error is returned it will be rescheduled with a backoff.
	Execute(context.Context, *StoreTask) (*StoreTask, error)
}

type Task struct {
	Job     Job
	Trigger trigger.Trigger
	Backoff trigger.Backoff
}

type StoreTask struct {
	Slug    string
	Kind    string
	Payload []byte
	When    time.Time
	Version int64
	Retry   int
	Result  string
}

func (s *StoreTask) IsOK() bool {
	return s.Retry == 0
}

type ScheduledJob struct {
	Job         Job
	NextRunTime time.Time
}

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

// A Scheduler is the Jobs orchestrator.
// Schedulers responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler interface {
	// RegisterJob registers the job and trigger
	// Fails with ErrJobAlreadyExists if job already registered.
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

type RegisterJobOptions struct {
	Trigger trigger.Trigger
	Backoff trigger.Backoff
}

type RegisterJobOption func(options *RegisterJobOptions)

func WithTrigger(trg trigger.Trigger) RegisterJobOption {
	return func(options *RegisterJobOptions) {
		options.Trigger = trg
	}
}

func WithBackoff(backoff trigger.Backoff) RegisterJobOption {
	return func(options *RegisterJobOptions) {
		options.Backoff = backoff
	}
}

type ScheduleJobOptions struct {
	Delay   time.Duration
	Payload []byte
}

type ScheduleJobOption func(options *ScheduleJobOptions)

func WithDelay(delay time.Duration) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.Delay = delay
	}
}

func WithPayload(payload []byte) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.Payload = payload
	}
}

type Logger interface {
	Error(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Info(format string, args ...interface{})
}

type myLogger struct{}

func (myLogger) Error(format string, args ...interface{}) {
	log.Printf("ERROR "+format, args...)
}

func (myLogger) Warn(format string, args ...interface{}) {
	log.Printf("WARN "+format, args...)
}

func (myLogger) Info(format string, args ...interface{}) {
	log.Printf("INFO "+format, args...)
}

var _ Scheduler = (*StdScheduler)((nil))

// StdScheduler implements the scheduler.Scheduler interface.
type StdScheduler struct {
	store     JobStore
	interrupt chan interface{}
	heartbeat time.Duration
	// to avoid concurrent instances to collide when locking we can add some randomness
	jitter time.Duration

	// tracks registry
	registry *knownTasks

	logger Logger
}

// NewStdScheduler2 returns a new DistScheduler.
func NewStdScheduler(store JobStore, options ...StdSchedulerOption) *StdScheduler {
	s := &StdScheduler{
		store:     store,
		interrupt: make(chan interface{}),
		heartbeat: time.Minute,
		registry:  newKnownTasks(),
		logger:    myLogger{},
	}
	for _, f := range options {
		f(s)
	}

	return s
}

type StdSchedulerOption func(*StdScheduler)

func StdSchedulerHeartbeatOption(heartbeat time.Duration) StdSchedulerOption {
	return func(s *StdScheduler) {
		s.heartbeat = heartbeat
	}
}

func StdSchedulerJitterOption(jitter time.Duration) StdSchedulerOption {
	return func(s *StdScheduler) {
		s.jitter = jitter
	}
}

func (s *StdScheduler) RegisterJob(job Job, options ...RegisterJobOption) error {
	opts := RegisterJobOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}
	return s.registry.add(job.Kind(), &Task{
		Job:     job,
		Trigger: opts.Trigger,
		Backoff: opts.Backoff,
	})
}

// ScheduleJob uses the specified Trigger to schedule the Job.
func (s *StdScheduler) ScheduleJob(ctx context.Context, slug string, job Job, options ...ScheduleJobOption) error {
	defer s.reset()

	opts := ScheduleJobOptions{}
	for _, o := range options {
		o(&opts)
	}
	nextRunTime := time.Now().Add(opts.Delay)
	err := s.store.Create(ctx, &StoreTask{
		Slug:    slug,
		Kind:    job.Kind(),
		Payload: opts.Payload,
		When:    nextRunTime,
	})
	if err != nil {
		return err
	}

	return nil
}

// Start starts the DistScheduler execution loop.
func (s *StdScheduler) Start(ctx context.Context) {
	// start scheduler execution loop
	go s.startExecutionLoop(ctx)

	if s.heartbeat > 0 {
		go s.startHeartbeat(ctx)
	}
}

// GetJobKeys returns the keys of all of the scheduled jobs.
func (s *StdScheduler) GetJobSlugs(ctx context.Context) ([]string, error) {
	return s.store.GetSlugs(ctx)
}

// GetScheduledJob returns the ScheduledJob by the unique key.
func (s *StdScheduler) GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error) {
	storedTask, err := s.store.Get(ctx, slug)
	if err != nil {
		return nil, err
	}

	task := s.registry.get(storedTask.Kind)
	return &ScheduledJob{
		Job:         task.Job,
		NextRunTime: storedTask.When,
	}, nil
}

// DeleteJob removes the job for the specified key from the DistScheduler if present.
func (s *StdScheduler) DeleteJob(ctx context.Context, slug string) error {
	err := s.store.Delete(ctx, slug)
	if err != nil {
		return err
	}
	s.reset()

	return nil
}

// Clear removes all of the scheduled jobs.
func (s *StdScheduler) Clear(ctx context.Context) error {
	defer s.reset()
	// reset the jobs queue
	return s.store.Clear(ctx)
}

func (s *StdScheduler) startExecutionLoop(ctx context.Context) {
	for {
		run, task, err := s.calculateNextRun(ctx)
		if err != nil {
			s.logger.Error("failed to calculate next run: %v", err)
			time.Sleep(waitOnCalculateNextRunError)
		}
		if run == nil {
			select {
			case <-s.interrupt:
			case <-ctx.Done():
				return
			}
		} else {
			select {
			case <-run.C:
				err := s.executeAndReschedule(ctx, task)
				if err != nil && !errors.Is(err, ErrJobNotLocked) {
					s.logger.Error("failed to execute and reschedule task: %+v", err)
				}
			case <-s.interrupt:
				run.Stop()
				continue
			case <-ctx.Done():
				run.Stop()
				return
			}
		}
	}
}

func (s *StdScheduler) calculateNextRun(ctx context.Context) (*time.Timer, *StoreTask, error) {
	task, err := s.store.NextRun(ctx)
	if errors.Is(err, ErrJobNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	park := parkTime(task.When)
	if s.jitter > 0 {
		park = time.Duration(int64(park) + rand.Int63n(int64(s.jitter)))
	}
	return time.NewTimer(park), task, nil
}

func (s *StdScheduler) executeAndReschedule(ctx context.Context, st *StoreTask) error {
	defer s.reset()

	st, err := s.store.Lock(ctx, st)
	if err != nil {
		return err
	}
	task := s.registry.get(st.Kind)
	if task == nil {
		return s.store.Delete(ctx, st.Slug)
	}

	go func() {
		slug := st.Slug
		st2 := s.executeTask(ctx, *task, st)
		if st2 == nil {
			s.logger.Info("Task '%s': deleting task", slug)
			err := s.store.Delete(ctx, slug)
			if err != nil {
				s.logger.Error("Task '%s': failed to delete task: %+v", slug, err)
			}
			return
		}
		err := s.store.Reschedule(ctx, st2)
		if err != nil {
			s.logger.Error("Task '%s': failed to release task: %+v", slug, err)
			return
		}
	}()

	return nil
}

func (s *StdScheduler) executeTask(ctx context.Context, task Task, storeTask *StoreTask) *StoreTask {
	// execute the Job
	storeTask2, err := task.Job.Execute(ctx, storeTask)
	if err != nil {
		s.logger.Error("Task '%s': failed to execute: %+v", storeTask.Slug, err)
		if task.Backoff == nil {
			s.logger.Warn("Task '%s': no retry", storeTask.Slug)
			return nil
		}

		storeTask.Result = err.Error()
		storeTask.Retry++

		when, errRetry := task.Backoff.NextRetryTime(storeTask.When, storeTask.Retry)
		if errRetry != nil {
			// no more attempts will be made
			s.logger.Info("Task '%s': no more retries: %+v", storeTask.Slug, errRetry)
			return nil
		}

		s.logger.Error("Task '%s': Backoff to %s: %+v", storeTask.Slug, when, err)
		storeTask.When = when
		return storeTask
	}

	if storeTask2 == nil {
		return nil
	}

	storeTask2.Retry = 0
	// reschedule the Job
	if task.Trigger == nil {
		// will cause this to be removed from the job queue
		return nil
	}
	storeTask2.When, err = task.Trigger.NextFireTime(storeTask2.When)
	if err != nil {
		// will cause this to be removed from the job queue
		return nil
	}

	return storeTask2
}

func (s *StdScheduler) reset() {
	select {
	case s.interrupt <- struct{}{}:
	default:
	}
}

func (s *StdScheduler) startHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.heartbeat)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			s.reset()
		}
	}
}

func parkTime(ts time.Time) time.Duration {
	now := time.Now()
	if ts.After(now) {
		return ts.Sub(now)
	}
	return 0
}
