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

func (m *KnownTasks) Add(slug string, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tasks[slug] = task
}

func (m *KnownTasks) Get(slug string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.tasks[slug]
}

func (m *KnownTasks) Delete(slug string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tasks, slug)
}

// Job is the interface to be implemented by structs which represent a 'job'
// to be performed.
type Job interface {
	Slug() string
	// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
	Execute(context.Context) (string, error)
	// Description returns a Job description.
	Description() string
}

type Task struct {
	Job     Job
	Trigger trigger.Trigger
}

type StoreTask struct {
	Slug    string
	When    int64
	Version int64
	Retry   int
	Result  string
}

func (s StoreTask) IsOK() bool {
	return s.Retry == 0
}

type ScheduledJob struct {
	Job                Job
	TriggerDescription string
	NextRunTime        int64
}

type JobStore interface {
	Create(context.Context, *StoreTask) error
	NextRun(context.Context) (int64, error)
	// RunAndReschedule implementation should lock the task that is ready to be executed (timed out), and asynchronously call the handler function.
	// If it returns a StoreTask is non nil, it should update the task. If StoreTask is nil it should delete the task.
	Lock(context.Context) (*StoreTask, error)
	Release(context.Context, *StoreTask) error
	GetSlugs(context.Context) ([]string, error)
	Get(ctx context.Context, slug string) (*StoreTask, error)
	Delete(ctx context.Context, slug string) error
	Clear(context.Context) error
}

// A Scheduler is the Jobs orchestrator.
// Schedulers responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler interface {
	// start the scheduler
	Start(context.Context)
	// schedule the job with the specified trigger
	ScheduleJob(ctx context.Context, job Job, trigger trigger.Trigger) error
	// get keys of all of the scheduled jobs
	GetJobSlugs(context.Context) ([]string, error)
	// get the scheduled job metadata
	GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error)
	// remove the job from the execution queue
	DeleteJob(ctx context.Context, slug string) error
	// clear all the scheduled jobs
	Clear(context.Context) error
}

// StdScheduler implements the scheduler.Scheduler interface.
type StdScheduler struct {
	sync.Mutex
	store      JobStore
	interrupt  chan interface{}
	heartbeat  time.Duration
	minBackoff time.Duration
	maxBackoff time.Duration
	jitter     time.Duration

	// tracks tasks
	tasks *KnownTasks
}

// NewStdScheduler2 returns a new DistScheduler.
func NewStdScheduler(store JobStore, options ...StdSchedulerOption) *StdScheduler {
	s := &StdScheduler{
		store:      store,
		interrupt:  make(chan interface{}),
		heartbeat:  10 * time.Second,
		minBackoff: 10 * time.Second,
		maxBackoff: 10 * time.Minute,
		tasks:      NewKnownTasks(),
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

func StdSchedulerMinBackoffOption(backoff time.Duration) StdSchedulerOption {
	return func(s *StdScheduler) {
		s.minBackoff = backoff
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

// ScheduleJob uses the specified Trigger to schedule the Job.
func (s *StdScheduler) ScheduleJob(ctx context.Context, job Job, trigger trigger.Trigger) error {
	nextRunTime, err := trigger.NextFireTime(NowNano())
	if err != nil {
		return err
	}

	err = s.store.Create(ctx, &StoreTask{
		Slug: job.Slug(),
		When: nextRunTime,
	})
	if err != nil && !errors.Is(err, ErrJobAlreadyScheduled) {
		return err
	}

	s.tasks.Add(job.Slug(), &Task{
		Job:     job,
		Trigger: trigger,
	})
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

	task := s.tasks.Get(storedTask.Slug)
	return &ScheduledJob{
		Job:                task.Job,
		TriggerDescription: task.Trigger.Description(),
		NextRunTime:        storedTask.When,
	}, nil
}

// DeleteJob removes the job for the specified key from the DistScheduler if present.
func (s *StdScheduler) DeleteJob(ctx context.Context, slug string) error {
	err := s.store.Delete(ctx, slug)
	if err != nil {
		return err
	}
	s.tasks.Delete(slug)
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
	if ts == 0 {
		return nil, nil
	}

	park := parkTime(ts)
	if s.jitter > 0 {
		park += rand.Int63n(int64(s.jitter))
	}
	return time.NewTimer(time.Duration(park)), nil
}

func (s *StdScheduler) executeAndReschedule(ctx context.Context) error {
	st, err := s.store.Lock(ctx)
	s.reset()
	if err != nil {
		return err
	}
	task := s.tasks.Get(st.Slug)
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
	var nextRunTime int64
	// execute the Job
	result, err := task.Job.Execute(ctx)
	if err != nil {
		storeTask.Result = err.Error()
		storeTask.Retry++
		backoff := int64(storeTask.Retry) * int64(s.minBackoff)
		if backoff > s.maxBackoff.Nanoseconds() {
			backoff = s.maxBackoff.Nanoseconds()
		}
		nextRunTime = NowNano() + backoff
		log.Printf("failed to execute task '%s'. Backoff %s: %+v", storeTask.Slug, time.Duration(backoff), err)
	} else {
		storeTask.Retry = 0
		storeTask.Result = result
		// reschedule the Job
		nextRunTime, err = task.Trigger.NextFireTime(storeTask.When)
		if err != nil {
			// will cause this to be removed from the job queue
			return nil
		}
	}

	storeTask.When = nextRunTime

	return storeTask
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

func parkTime(ts int64) int64 {
	now := NowNano()
	if ts > now {
		return ts - now
	}
	return 0
}

// NowNano returns the current UTC Unix time in nanoseconds.
func NowNano() int64 {
	return time.Now().UTC().UnixNano()
}
