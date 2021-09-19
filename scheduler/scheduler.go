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
	ErrJobAlreadyScheduled = errors.New("job already scheduled")
	ErrJobAlreadyExists    = errors.New("job already exists")
)

type KnownTasks struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

func NewKnownTasks() *KnownTasks {
	return &KnownTasks{
		tasks: map[string]*Task{},
	}
}

func (m *KnownTasks) Add(jobKind string, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.tasks[jobKind]
	if ok {
		return ErrJobAlreadyExists
	}
	m.tasks[jobKind] = task
	return nil
}

func (m *KnownTasks) Get(jobKind string) *Task {
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

func (s StoreTask) IsOK() bool {
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

// A Scheduler is the Jobs orchestrator.
// Schedulers responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler interface {
	// RegisterJob registers the job and trigger
	// Fails if job already registered.
	// If trigger is nil, the associated job will only run once.
	RegisterJob(job Job, trigger trigger.Trigger) error
	// Start starts the scheduler
	Start(context.Context)
	// ScheduleJob schedule the job with a initial payload an delay
	ScheduleJob(ctx context.Context, slug string, job Job, payload []byte, delay time.Duration) error
	// GetJobSlugs get slugs of all of the scheduled jobs
	GetJobSlugs(context.Context) ([]string, error)
	// GetScheduledJob get the scheduled job metadata
	GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error)
	// DeleteJob remove the job from the execution queue
	DeleteJob(ctx context.Context, slug string) error
	// Clear clear all the scheduled jobs
	Clear(context.Context) error
}

// StdScheduler implements the scheduler.Scheduler interface.
type StdScheduler struct {
	sync.Mutex
	store      JobStore
	interrupt  chan interface{}
	heartbeat  time.Duration
	incBackoff time.Duration
	maxBackoff time.Duration
	// to avoid concurrent instances to colide when locking we can add some randomness
	jitter time.Duration

	// tracks registry
	registry *KnownTasks
}

// NewStdScheduler2 returns a new DistScheduler.
func NewStdScheduler(store JobStore, options ...StdSchedulerOption) *StdScheduler {
	s := &StdScheduler{
		store:      store,
		interrupt:  make(chan interface{}),
		heartbeat:  10 * time.Second,
		incBackoff: 10 * time.Second,
		maxBackoff: 10 * time.Minute,
		registry:   NewKnownTasks(),
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

func StdSchedulerIncBackoffOption(backoff time.Duration) StdSchedulerOption {
	return func(s *StdScheduler) {
		s.incBackoff = backoff
	}
}

func StdSchedulerMaxBackoffOption(backoff time.Duration) StdSchedulerOption {
	return func(s *StdScheduler) {
		s.maxBackoff = backoff
	}
}

func StdSchedulerJitterOption(jitter time.Duration) StdSchedulerOption {
	return func(s *StdScheduler) {
		s.jitter = jitter
	}
}

func (s *StdScheduler) RegisterJob(job Job, trigger trigger.Trigger) error {
	return s.registry.Add(job.Kind(), &Task{
		Job:     job,
		Trigger: trigger,
	})
}

// ScheduleJob uses the specified Trigger to schedule the Job.
func (s *StdScheduler) ScheduleJob(ctx context.Context, slug string, job Job, payload []byte, delay time.Duration) error {
	nextRunTime := time.Now().Add(delay)
	err := s.store.Create(ctx, &StoreTask{
		Slug:    slug,
		Kind:    job.Kind(),
		Payload: payload,
		When:    nextRunTime,
	})
	if err != nil && !errors.Is(err, ErrJobAlreadyScheduled) {
		return err
	}

	s.reset()

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

	task := s.registry.Get(storedTask.Kind)
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
		run, err := s.calculateNextRun(ctx)
		if err != nil {
			log.Printf("failed to calculate next run: %v", err)
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
				err := s.executeAndReschedule(ctx)
				if err != nil {
					log.Printf("failed to execute and reschedule task: %+v", err)
				}
			case <-s.interrupt:
				run.Stop()
				continue
			case <-ctx.Done():
				run.Stop()
				log.Printf("Exit the execution loop.")
				return
			}
		}
	}
}

func (s *StdScheduler) calculateNextRun(ctx context.Context) (*time.Timer, error) {
	ts, err := s.store.NextRun(ctx)
	if err != nil && !errors.Is(err, ErrJobNotFound) {
		return nil, err
	}
	if (ts == time.Time{}) {
		return nil, nil
	}

	park := parkTime(ts)
	if s.jitter > 0 {
		park = time.Duration(int64(park) + rand.Int63n(int64(s.jitter)))
	}
	return time.NewTimer(park), nil
}

func (s *StdScheduler) executeAndReschedule(ctx context.Context) error {
	st, err := s.store.Lock(ctx)
	s.reset()
	if err != nil {
		return err
	}
	task := s.registry.Get(st.Kind)
	if task == nil {
		return s.store.Delete(ctx, st.Slug)
	}

	go func() {
		slug := st.Slug
		st := s.executeTask(ctx, *task, st)
		if st == nil {
			err := s.store.Delete(ctx, slug)
			if err != nil {
				log.Printf("failed to delete task '%s': %+v", slug, err)
			}
			return
		}
		err := s.store.Release(ctx, st)
		if err != nil {
			log.Printf("failed to release task '%s': %+v", slug, err)
			return
		}
	}()

	return nil
}

func (s *StdScheduler) executeTask(ctx context.Context, task Task, storeTask *StoreTask) *StoreTask {
	// execute the Job
	storeTask2, err := task.Job.Execute(ctx, storeTask)
	if err != nil {
		storeTask.Result = err.Error()
		storeTask.Retry++

		factor := int64(1)
		var backoff int64
		for i := 1; i <= storeTask.Retry; i++ {
			backoff = factor * int64(s.incBackoff)
			if backoff > s.maxBackoff.Nanoseconds() {
				backoff = s.maxBackoff.Nanoseconds()
				break
			}
			factor = factor * 2
		}
		delay := time.Duration(backoff)
		log.Printf("failed to execute task '%s'. Backoff %s: %+v", storeTask.Slug, delay, err)
		storeTask.When = time.Now().Add(delay)
		return storeTask
	}

	if storeTask == nil {
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
