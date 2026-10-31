package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/quintans/go-scheduler/trigger"
)

var (
	ErrJobNotFound          = errors.New("no job with the given Slug was found")
	ErrJobNotLocked         = errors.New("job lock was not acquired")
	ErrJobAlreadyRegistered = errors.New("job already registered")
	ErrJobAlreadyExists     = errors.New("job already exists")
	ErrInvalidOneOffJobSlug = errors.New("invalid one-off job slug format")
)

const (
	waitOnCalculateNextRunError = 10 * time.Second
	oneOffSlugSeparator         = ":"
)

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
	// TriggerFromStore indicates that the trigger expression stored with the task takes precedence over Trigger.
	// It is re-read on every execution, so changes made directly in the store are picked up on the next run.
	TriggerFromStore bool
}

type ScheduledJob struct {
	Job         Job
	NextRunTime time.Time
	Suspended   bool
}

// A Scheduler is the Jobs orchestrator.
// It is responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler struct {
	running bool
	store   JobStore
	// one wake-up channel per worker. Each has a buffer of one, so a poke that arrives
	// while the worker is busy (e.g. querying the store) is remembered and not lost.
	wakes     []chan struct{}
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

// LoadScheduleJob loads a job from the store and schedules it to be run.
// It will parse the trigger from the store and use it to determine when to run the job.
// If the job is not found in the store, or the stored task has no trigger expression,
// it will be scheduled with the provided default trigger.
//
// The stored trigger expression is re-read on every execution, so changing it directly
// in the store updates the job schedule from its next run onwards, without changing the code.
// This should be used before Start.
func (s *Scheduler) LoadScheduleJob(ctx context.Context, slug string, job Job, defaultTrigger trigger.Trigger, options ...ScheduleJobOption) error {
	if s.running {
		return fmt.Errorf("cannot load job after scheduler has started job: '%s'", slug)
	}

	trg := defaultTrigger
	storedTask, err := s.store.Get(ctx, slug)
	switch {
	case errors.Is(err, ErrJobNotFound):
		// not stored yet, the default trigger will be used
	case err != nil:
		return fmt.Errorf("failed to load job '%s' from store: %w", slug, err)
	case storedTask.Trigger != "":
		trg, err = trigger.Parse(storedTask.Trigger)
		if err != nil {
			return fmt.Errorf("failed to parse stored trigger for job '%s': %w", slug, err)
		}
	}

	return s.scheduleTask(ctx, slug, &Task{
		Job:              job,
		Trigger:          trg,
		TriggerFromStore: true,
	}, options...)
}

// ScheduleJob schedules a recurring job to be run.
// This should be used before Start.
func (s *Scheduler) ScheduleJob(ctx context.Context, slug string, job Job, trg trigger.Trigger, options ...ScheduleJobOption) error {
	if s.running {
		return fmt.Errorf("cannot schedule job after scheduler has started job: '%s'", slug)
	}

	return s.scheduleTask(ctx, slug, &Task{
		Job:     job,
		Trigger: trg,
	}, options...)
}

func (s *Scheduler) scheduleTask(ctx context.Context, slug string, task *Task, options ...ScheduleJobOption) error {
	opts := ScheduleJobOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}
	task.Backoff = opts.Backoff

	if err := s.registry.add(slug, task); err != nil {
		return fmt.Errorf("failed to register job: %w", err)
	}

	when := opts.When
	if when.IsZero() {
		when = task.Trigger.Next(time.Now())
	}

	return s.scheduleJob(ctx, &StoreTask{
		Slug:    slug,
		Payload: opts.Payload,
		When:    when,
		Trigger: trigger.Expression(task.Trigger),
	})
}

func (s *Scheduler) scheduleJob(ctx context.Context, st *StoreTask) error {
	if err := s.store.Create(ctx, st); err != nil && !errors.Is(err, ErrJobAlreadyExists) {
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

	s.wakes = make([]chan struct{}, workers)
	for i := range s.wakes {
		s.wakes[i] = make(chan struct{}, 1)
		go s.startExecutionLoop(ctx, s.wakes[i])
	}
}

// pokeWaiters wakes up all workers so that they re-read the store.
// Pokes are coalesced: if a worker already has a pending poke, nothing is added.
func (s *Scheduler) pokeWaiters() {
	for _, wake := range s.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

type ScheduleOneOffJobOptions struct {
	When    time.Time
	Payload []byte
}

// ScheduleOneOffJob schedules short lived jobs
//
// slug will have the form <job-name>:<anything> to ensure uniqueness.
// Typically <anything> can be a UUID or a timestamp.
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

	return s.scheduleJob(ctx, &StoreTask{
		Slug:    slug,
		Payload: payload,
		When:    when,
	})
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

	sj := &ScheduledJob{
		NextRunTime: storedTask.When,
		Suspended:   storedTask.Suspended,
	}
	if task := s.registry.get(baseSlug(storedTask.Slug)); task != nil {
		sj.Job = task.Job
	}
	return sj, nil
}

// SuspendJob suspends the job, so that it will not run until it is resumed with ResumeJob.
// A run already in progress is not interrupted, but no further runs will happen.
// The suspension is persisted in the store, so it survives restarts and is seen by all the scheduler instances.
func (s *Scheduler) SuspendJob(ctx context.Context, slug string) error {
	if err := s.store.Suspend(ctx, slug); err != nil {
		return err
	}
	s.pokeWaiters()

	return nil
}

// ResumeJob resumes a suspended job. It does nothing if the job is not suspended.
// If the next run time of the job is already in the past, it is recomputed from the job trigger,
// so that the runs missed while suspended are not executed. A job without a trigger (one-off) keeps its
// run time and therefore runs right away.
func (s *Scheduler) ResumeJob(ctx context.Context, slug string) error {
	st, err := s.store.Get(ctx, slug)
	if err != nil {
		return err
	}
	if !st.Suspended {
		return nil
	}

	when := st.When
	if now := time.Now(); when.Before(now) {
		if trg := s.triggerFor(st); trg != nil {
			if next := trg.Next(now); !next.IsZero() {
				when = next
			}
		}
	}

	if err := s.store.Resume(ctx, slug, when); err != nil {
		return err
	}
	s.pokeWaiters()

	return nil
}

// triggerFor returns the trigger of the stored task, or nil if it has none.
// It honours the same precedence as the execution: the stored expression for tasks loaded from the store,
// otherwise the registered trigger. If the task is not registered in this instance, the stored expression is used.
func (s *Scheduler) triggerFor(st *StoreTask) trigger.Trigger {
	task := s.registry.get(baseSlug(st.Slug))
	if task != nil {
		return s.resolveTrigger(*task, st)
	}
	if st.Trigger == "" {
		return nil
	}
	trg, err := trigger.Parse(st.Trigger)
	if err != nil {
		s.logger.Warn("Task '%s': invalid stored trigger '%s': %+v", st.Slug, st.Trigger, err)
		return nil
	}
	return trg
}

// baseSlug returns the registry slug of a stored task: one-off tasks are stored as <job-name>:<anything>.
func baseSlug(slug string) string {
	parts := strings.Split(slug, oneOffSlugSeparator)
	if len(parts) == 2 {
		return parts[0]
	}
	return slug
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

func (s *Scheduler) startExecutionLoop(ctx context.Context, wake <-chan struct{}) {
	for {
		task, park, err := s.calculateNextRun(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// the scheduler is shutting down, the store error is a consequence of that
				return
			}
			s.logger.Error("failed to calculate next run: %v", err)
			select {
			case <-time.After(waitOnCalculateNextRunError):
			case <-ctx.Done():
				return
			}
			continue
		}

		// a nil channel blocks forever: no task and no heartbeat means wait for a poke
		var timer *time.Timer
		var due <-chan time.Time
		if park >= 0 {
			timer = time.NewTimer(park)
			due = timer.C
		}

		select {
		case <-due:
			if task == nil {
				// heartbeat: re-read the store
				continue
			}
			err := s.executeAndReschedule(ctx, task)
			if err != nil && !errors.Is(err, ErrJobNotLocked) {
				s.logger.Error("failed to execute and reschedule task: %+v", err)
			}
		case <-wake:
			stopTimer(timer)
		case <-ctx.Done():
			stopTimer(timer)
			return
		}
	}
}

func stopTimer(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

// calculateNextRun returns the next task to run and how long to wait before running it.
// The wait is capped at the heartbeat, so that the store is re-read periodically to pick up
// changes made by other instances.
// If there is no task, the returned task is nil and the wait is the heartbeat.
// A negative wait means wait indefinitely (no task and no heartbeat).
func (s *Scheduler) calculateNextRun(ctx context.Context) (*StoreTask, time.Duration, error) {
	task, err := s.store.NextRun(ctx)
	if errors.Is(err, ErrJobNotFound) {
		if s.heartbeat > 0 {
			return nil, s.heartbeat, nil
		}
		return nil, -1, nil
	}
	if err != nil {
		return nil, 0, err
	}

	park := parkTime(task.When)
	if s.jitter > 0 {
		park += time.Duration(rand.Int63n(int64(s.jitter)))
	}
	if s.heartbeat > 0 && park > s.heartbeat {
		// re-read the store before the task is due, something may have changed in the meantime
		return nil, s.heartbeat, nil
	}
	return task, park, nil
}

func (s *Scheduler) executeAndReschedule(ctx context.Context, st *StoreTask) error {
	defer s.pokeWaiters()

	st, err := s.store.Lock(ctx, st)
	if err != nil {
		return err
	}

	slug := baseSlug(st.Slug)
	task := s.registry.get(slug)
	if task == nil {
		s.logger.Info("Task '%s': not found in registry. Removing from store", st.Slug)
		return s.store.Delete(ctx, st.Slug)
	}

	go func() {
		// poke again once the task has been rescheduled (or removed), otherwise the execution loop
		// would only find the task again on the next heartbeat
		defer s.pokeWaiters()

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
	if storeTask2.Trigger == "" {
		// keep the stored trigger expression, the job is not responsible for carrying it
		storeTask2.Trigger = storeTask.Trigger
	}
	// reschedule the Job
	trg := s.resolveTrigger(task, storeTask)
	if trg == nil {
		// will cause this to be removed from the job queue
		return nil
	}
	storeTask2.When = trg.Next(storeTask2.When)
	if storeTask2.When.IsZero() {
		// will cause this to be removed from the job queue
		return nil
	}

	return storeTask2
}

// resolveTrigger returns the trigger to use to reschedule the task.
// For tasks whose trigger comes from the store, the stored expression (as read when the task was locked)
// takes precedence over the registered one, so changes in the store are honoured on every run.
func (s *Scheduler) resolveTrigger(task Task, storeTask *StoreTask) trigger.Trigger {
	if !task.TriggerFromStore || storeTask.Trigger == "" {
		return task.Trigger
	}

	trg, err := trigger.Parse(storeTask.Trigger)
	if err != nil {
		s.logger.Warn("Task '%s': invalid stored trigger '%s', using the registered trigger: %+v", storeTask.Slug, storeTask.Trigger, err)
		return task.Trigger
	}

	return trg
}

func parkTime(ts time.Time) time.Duration {
	now := time.Now()
	if ts.After(now) {
		return ts.Sub(now)
	}
	return 0
}
