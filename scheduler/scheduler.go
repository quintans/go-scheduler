package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/quintans/go-scheduler/internal/lib"
	"github.com/quintans/go-scheduler/trigger"
)

var (
	ErrJobNotFound          = errors.New("no job with the given Slug was found")
	ErrJobNotLocked         = errors.New("job lock was not acquired")
	ErrJobAlreadyRegistered = errors.New("job already registered")
	ErrJobAlreadyExists     = errors.New("job already exists")
	ErrInvalidOneOffJobSlug = errors.New("invalid one-off job slug format")
)

const waitOnCalculateNextRunError = 10 * time.Second
const oneOffSlugSeparator = ":"

type knownTasks struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

func newKnownTasks() *knownTasks {
	return &knownTasks{
		tasks: map[string]*Task{},
	}
}

func (m *knownTasks) add(slug string, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tasks[slug]; exists {
		return fmt.Errorf("task with slug '%s' already registered: %w", slug, ErrJobAlreadyRegistered)
	}

	m.tasks[slug] = task
	return nil
}

func (m *knownTasks) get(slug string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.tasks[slug]
}

type Task struct {
	Job     Job
	Trigger trigger.Trigger
	Backoff trigger.Backoff
	OneOff  bool
}

type ScheduledJob struct {
	Job         Job
	NextRunTime time.Time
}

// A Scheduler is the Jobs orchestrator.
// It is responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler struct {
	running   bool
	store     JobStore
	waiters   []*lib.Waiter
	heartbeat time.Duration
	// to avoid concurrent instances to collide when locking we can add some randomness
	jitter time.Duration

	// tracks registry
	registry *knownTasks

	logger Logger
}

// NewScheduler returns a new Scheduler.
func NewScheduler(store JobStore, options ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		store:     store,
		heartbeat: time.Minute,
		registry:  newKnownTasks(),
		logger:    myLogger{},
	}
	for _, f := range options {
		f(s)
	}

	return s
}

type SchedulerOption func(*Scheduler)

func HeartbeatOption(heartbeat time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.heartbeat = heartbeat
	}
}

func JitterOption(jitter time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.jitter = jitter
	}
}

type ScheduleJobOptions struct {
	Backoff trigger.Backoff
	When    time.Time
	Payload []byte
}

type ScheduleJobOption func(options *ScheduleJobOptions)

func WithBackoff(backoff trigger.Backoff) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.Backoff = backoff
	}
}

func WithDelay(delay time.Duration) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.When = time.Now().Add(delay)
	}
}

func WithWhen(when time.Time) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.When = when
	}
}

func WithPayload(payload []byte) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.Payload = payload
	}
}

// ScheduleJob schedules a recurring job to be run.
// This should be used before Start.
func (s *Scheduler) ScheduleJob(ctx context.Context, slug string, job Job, trg trigger.Trigger, options ...ScheduleJobOption) error {
	if s.running {
		return fmt.Errorf("cannot schedule job after scheduler has started job: '%s'", slug)
	}

	opts := ScheduleJobOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}

	if err := s.registry.add(slug, &Task{
		Job:     job,
		Trigger: trg,
		Backoff: opts.Backoff,
	}); err != nil {
		return fmt.Errorf("failed to register job: %w", err)
	}

	when := opts.When
	if when.IsZero() {
		when = trg.Next(time.Now())
	}

	return s.scheduleJob(ctx, slug, when, opts.Payload)
}

func (s *Scheduler) scheduleJob(ctx context.Context, slug string, when time.Time, payload []byte) error {
	if err := s.store.Create(ctx, &StoreTask{
		Slug:    slug,
		Payload: payload,
		When:    when,
	}); err != nil && !errors.Is(err, ErrJobAlreadyExists) {
		return fmt.Errorf("failed to create store task: %w", err)
	}

	return nil
}

type RegisterJobOptions struct {
	Backoff trigger.Backoff
	Trigger trigger.Trigger
}

type RegisterJobOption func(options *RegisterJobOptions)

func WithJobBackoff(backoff trigger.Backoff) RegisterJobOption {
	return func(options *RegisterJobOptions) {
		options.Backoff = backoff
	}
}

func WithJobTrigger(trigger trigger.Trigger) RegisterJobOption {
	return func(options *RegisterJobOptions) {
		options.Trigger = trigger
	}
}

// RegisterOnDemandJob registers the job and trigger
// Fails with ErrJobAlreadyExists if job already registered.
//
// This should be used to register the jobs before starting the scheduler.
func (s *Scheduler) RegisterOneOffJob(slug string, job Job, options ...RegisterJobOption) error {
	if s.running {
		return fmt.Errorf("cannot register one-off job after scheduler has started: '%s'", slug)
	}

	opts := RegisterJobOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}
	if err := s.registry.add(slug, &Task{
		Job:     job,
		Backoff: opts.Backoff,
		Trigger: opts.Trigger,
		OneOff:  true,
	}); err != nil {
		return fmt.Errorf("failed to register one-off job: %w", err)
	}
	return nil
}

// Start starts the scheduler
//
// This should be called after registering all the jobs, either by calling RegisterJob or ScheduleJob.
func (s *Scheduler) Start(ctx context.Context, workers int) {
	if s.running {
		return
	}
	s.running = true

	if workers <= 0 {
		workers = 1
	}

	s.waiters = make([]*lib.Waiter, workers)
	for i := 0; i < workers; i++ {
		s.waiters[i] = lib.NewWaiter()
	}

	// start scheduler execution loop
	for _, waiter := range s.waiters {
		go s.startExecutionLoop(ctx, waiter)

		if s.heartbeat > 0 {
			go s.startHeartbeat(ctx, waiter)
		}
	}
}

func (s *Scheduler) pokeWaiters() {
	for _, waiter := range s.waiters {
		go func() {
			if s.jitter > 0 {
				time.Sleep(time.Duration(rand.Int63n(int64(s.jitter))))
			}
			waiter.Poke()
		}()
	}
}

type ScheduleOneOffJobOptions struct {
	When    time.Time
	Payload []byte
}

// ScheduleOneOffJob schedules short lived jobs
//
// slug will have the form <job-name>:<anything> to ensure uniqueness.
// Tipically <anything> can be a UUID or a timestamp.
func (s *Scheduler) ScheduleOneOffJob(ctx context.Context, slug string, when time.Time, payload []byte) error {
	if !s.running {
		return fmt.Errorf("cannot schedule one-off job before scheduler has started job: '%s'", slug)
	}

	parts := strings.Split(slug, oneOffSlugSeparator)
	if len(parts) != 2 {
		return ErrInvalidOneOffJobSlug
	}
	task := s.registry.get(parts[0])
	if task == nil {
		return fmt.Errorf("job not found: %s", parts[0])
	}
	if !task.OneOff {
		return fmt.Errorf("job is not a one-off job: %s", parts[0])
	}

	defer s.pokeWaiters()

	return s.scheduleJob(ctx, slug, when, payload)
}

// GetJobSlugs returns the slugs of all of the scheduled jobs.
func (s *Scheduler) GetJobSlugs(ctx context.Context) ([]string, error) {
	return s.store.GetSlugs(ctx)
}

// GetScheduledJob get the scheduled job metadata
func (s *Scheduler) GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error) {
	storedTask, err := s.store.Get(ctx, slug)
	if err != nil {
		return nil, err
	}

	task := s.registry.get(storedTask.Slug)
	return &ScheduledJob{
		Job:         task.Job,
		NextRunTime: storedTask.When,
	}, nil
}

// DeleteJob remove the job from the execution queue
func (s *Scheduler) DeleteJob(ctx context.Context, slug string) error {
	err := s.store.Delete(ctx, slug)
	if err != nil {
		return err
	}
	s.pokeWaiters()

	return nil
}

// Clear removes all of the scheduled jobs.
func (s *Scheduler) Clear(ctx context.Context) error {
	defer s.pokeWaiters()
	// reset the jobs queue
	return s.store.Clear(ctx)
}

func (s *Scheduler) startExecutionLoop(ctx context.Context, waiter *lib.Waiter) {
	for {
		run, task, err := s.calculateNextRun(ctx)
		if err != nil {
			s.logger.Error("failed to calculate next run: %v", err)
			time.Sleep(waitOnCalculateNextRunError)
			continue
		}
		if run == nil {
			select {
			case <-waiter.Wait():
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
			case <-waiter.Wait():
				run.Stop()
				continue
			case <-ctx.Done():
				run.Stop()
				return
			}
		}
	}
}

func (s *Scheduler) calculateNextRun(ctx context.Context) (*time.Timer, *StoreTask, error) {
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

func (s *Scheduler) executeAndReschedule(ctx context.Context, st *StoreTask) error {
	defer s.pokeWaiters()

	st, err := s.store.Lock(ctx, st)
	if err != nil {
		return err
	}

	slug := st.Slug
	parts := strings.Split(slug, oneOffSlugSeparator)
	if len(parts) == 2 {
		// one-off job, use the base slug to find the task
		slug = parts[0]
	}
	task := s.registry.get(slug)
	if task == nil {
		s.logger.Info("Task '%s': not found in registry. Removing from store", slug)
		return s.store.Delete(ctx, slug)
	}

	go func() {
		slug := st.Slug
		st2 := s.executeTask(ctx, *task, st)
		if st2 == nil || st2.When.IsZero() {
			s.logger.Info("Task '%s' signaled to be removed: deleting task", slug)
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

func (s *Scheduler) executeTask(ctx context.Context, task Task, storeTask *StoreTask) *StoreTask {
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
	storeTask2.When = task.Trigger.Next(storeTask2.When)
	if storeTask2.When.IsZero() {
		// will cause this to be removed from the job queue
		return nil
	}

	return storeTask2
}

func (s *Scheduler) startHeartbeat(ctx context.Context, waiter *lib.Waiter) {
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			waiter.Poke()
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
