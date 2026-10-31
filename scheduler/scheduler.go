package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/go-scheduler/trigger"
)

var (
	ErrTaskNotFound         = errors.New("no task with the given Slug was found")
	ErrTaskNotLocked        = errors.New("task lock was not acquired")
	ErrJobAlreadyRegistered = errors.New("job already registered")
	ErrTaskAlreadyExists    = errors.New("task already exists")
	ErrInvalidOneOffJobSlug = errors.New("invalid one-off job slug: must not contain ':'")
)

const (
	waitOnCalculateNextRunError = 10 * time.Second
	oneOffSlugSeparator         = ":"
)

type knownJobs struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func newKnownJobs() *knownJobs {
	return &knownJobs{
		jobs: map[string]*Job{},
	}
}

func (m *knownJobs) add(slug string, job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.jobs[slug]; exists {
		return fmt.Errorf("job with slug '%s' already registered: %w", slug, ErrJobAlreadyRegistered)
	}

	m.jobs[slug] = job
	return nil
}

func (m *knownJobs) get(slug string) *Job {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.jobs[slug]
}

type Job struct {
	Executer Executer
	Trigger  trigger.Trigger
	Backoff  trigger.Backoff
	OneOff   bool
	// TriggerFromStore indicates that the trigger expression stored with the job takes precedence over Trigger.
	// It is re-read on every execution, so changes made directly in the store are picked up on the next run.
	TriggerFromStore bool
}

type ScheduledJob struct {
	Executer    Executer
	NextRunTime time.Time
	Suspended   bool
}

// A Scheduler is the Jobs orchestrator.
// It is responsible for executing Jobs when their associated Triggers fire (when their scheduled time arrives).
type Scheduler struct {
	running bool
	store   Store
	// one wake-up channel per worker. Each has a buffer of one, so a poke that arrives
	// while the worker is busy (e.g. querying the store) is remembered and not lost.
	wakes     []chan struct{}
	heartbeat time.Duration
	// to avoid concurrent instances to collide when locking we can add some randomness
	jitter time.Duration

	// tracks registry
	registry *knownJobs

	logger *slog.Logger
}

// NewScheduler returns a new Scheduler.
func NewScheduler(store Store, options ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		store:     store,
		heartbeat: time.Minute,
		registry:  newKnownJobs(),
		logger:    slog.Default(),
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

// LoggerOption sets the logger used by the scheduler. Defaults to slog.Default().
func LoggerOption(logger *slog.Logger) SchedulerOption {
	return func(s *Scheduler) {
		if logger != nil {
			s.logger = logger
		}
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
// If the job is not found in the store, or its task has no trigger expression,
// it will be scheduled with the provided default trigger.
//
// The stored trigger expression is re-read on every execution, so changing it directly
// in the store updates the job schedule from its next run onwards, without changing the code.
// This should be used before Start.
func (s *Scheduler) LoadScheduleJob(ctx context.Context, slug string, executer Executer, defaultTrigger trigger.Trigger, options ...ScheduleJobOption) error {
	if s.running {
		return fmt.Errorf("cannot load job after scheduler has started job: '%s'", slug)
	}

	trg := defaultTrigger
	storedJob, err := s.store.Get(ctx, slug)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		// not stored yet, the default trigger will be used
	case err != nil:
		return fmt.Errorf("failed to load job '%s' from store: %w", slug, err)
	case storedJob.Trigger != "":
		trg, err = trigger.Parse(storedJob.Trigger)
		if err != nil {
			return fmt.Errorf("failed to parse stored trigger for job '%s': %w", slug, err)
		}
	}

	return s.scheduleJob(ctx, slug, &Job{
		Executer:         executer,
		Trigger:          trg,
		TriggerFromStore: true,
	}, options...)
}

// ScheduleJob schedules a recurring job to be run.
// This should be used before Start.
func (s *Scheduler) ScheduleJob(ctx context.Context, slug string, executer Executer, trg trigger.Trigger, options ...ScheduleJobOption) error {
	if s.running {
		return fmt.Errorf("cannot schedule job after scheduler has started job: '%s'", slug)
	}

	return s.scheduleJob(ctx, slug, &Job{
		Executer: executer,
		Trigger:  trg,
	}, options...)
}

func (s *Scheduler) scheduleJob(ctx context.Context, slug string, job *Job, options ...ScheduleJobOption) error {
	opts := ScheduleJobOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}
	job.Backoff = opts.Backoff

	if err := s.registry.add(slug, job); err != nil {
		return fmt.Errorf("failed to register job: %w", err)
	}

	when := opts.When
	if when.IsZero() {
		when = job.Trigger.Next(time.Now())
	}

	return s.createTask(ctx, &Task{
		Slug:    slug,
		Payload: opts.Payload,
		When:    when,
		Trigger: trigger.Expression(job.Trigger),
	})
}

func (s *Scheduler) createTask(ctx context.Context, st *Task) error {
	if err := s.store.Create(ctx, st); err != nil && !errors.Is(err, ErrTaskAlreadyExists) {
		return fmt.Errorf("failed to create task: %w", err)
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

// RegisterOneOffJob registers the job and trigger
// Fails with ErrJobAlreadyRegistered if the job is already registered.
//
// This should be used to register the jobs before starting the scheduler.
func (s *Scheduler) RegisterOneOffJob(slug string, executer Executer, options ...RegisterJobOption) error {
	if s.running {
		return fmt.Errorf("cannot register one-off job after scheduler has started: '%s'", slug)
	}
	if strings.Contains(slug, oneOffSlugSeparator) {
		return fmt.Errorf("failed to register one-off job '%s': %w", slug, ErrInvalidOneOffJobSlug)
	}

	opts := RegisterJobOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}
	if err := s.registry.add(slug, &Job{
		Executer: executer,
		Backoff:  opts.Backoff,
		Trigger:  opts.Trigger,
		OneOff:   true,
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

// ScheduleOneOffJob schedules a single run of a job previously registered with RegisterOneOffJob.
//
// slug is the slug used in RegisterOneOffJob. Each call creates a new scheduled job with
// a unique slug of the form <slug>:<uuid>, which is returned so that it can be used with
// SuspendJob, ResumeJob, DeleteJob or GetScheduledJob.
func (s *Scheduler) ScheduleOneOffJob(ctx context.Context, slug string, when time.Time, payload []byte) (string, error) {
	if !s.running {
		return "", fmt.Errorf("cannot schedule one-off job before scheduler has started job: '%s'", slug)
	}

	if strings.Contains(slug, oneOffSlugSeparator) {
		return "", fmt.Errorf("cannot schedule one-off job '%s': %w", slug, ErrInvalidOneOffJobSlug)
	}
	job := s.registry.get(slug)
	if job == nil {
		return "", fmt.Errorf("job not found: %s", slug)
	}
	if !job.OneOff {
		return "", fmt.Errorf("job is not a one-off job: %s", slug)
	}

	defer s.pokeWaiters()

	fullSlug := slug + oneOffSlugSeparator + uuid.NewString()
	if err := s.createTask(ctx, &Task{
		Slug:    fullSlug,
		Payload: payload,
		When:    when,
	}); err != nil {
		return "", err
	}

	return fullSlug, nil
}

// GetJobSlugs returns the slugs of all of the scheduled jobs.
func (s *Scheduler) GetJobSlugs(ctx context.Context) ([]string, error) {
	return s.store.GetSlugs(ctx)
}

// GetScheduledJob get the scheduled job metadata
func (s *Scheduler) GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error) {
	storedJob, err := s.store.Get(ctx, slug)
	if err != nil {
		return nil, err
	}

	sj := &ScheduledJob{
		NextRunTime: storedJob.When,
		Suspended:   storedJob.Suspended,
	}
	if job := s.registry.get(baseSlug(storedJob.Slug)); job != nil {
		sj.Executer = job.Executer
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

// triggerFor returns the trigger of the task, or nil if it has none.
// It honours the same precedence as the execution: the stored expression for jobs loaded from the store,
// otherwise the registered trigger. If the job is not registered in this instance, the stored expression is used.
func (s *Scheduler) triggerFor(st *Task) trigger.Trigger {
	job := s.registry.get(baseSlug(st.Slug))
	if job != nil {
		return s.resolveTrigger(*job, st)
	}
	if st.Trigger == "" {
		return nil
	}
	trg, err := trigger.Parse(st.Trigger)
	if err != nil {
		s.logger.Warn("invalid stored trigger", "slug", st.Slug, "trigger", st.Trigger, "error", err)
		return nil
	}
	return trg
}

// baseSlug returns the registry slug of a task: one-off tasks are stored as <job-name>:<uuid>.
func baseSlug(slug string) string {
	base, _, _ := strings.Cut(slug, oneOffSlugSeparator)
	return base
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
		job, park, err := s.calculateNextRun(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// the scheduler is shutting down, the store error is a consequence of that
				return
			}
			s.logger.Error("failed to calculate next run", "error", err)
			select {
			case <-time.After(waitOnCalculateNextRunError):
			case <-ctx.Done():
				return
			}
			continue
		}

		// a nil channel blocks forever: no job and no heartbeat means wait for a poke
		var timer *time.Timer
		var due <-chan time.Time
		if park >= 0 {
			timer = time.NewTimer(park)
			due = timer.C
		}

		select {
		case <-due:
			if job == nil {
				// heartbeat: re-read the store
				continue
			}
			err := s.executeAndReschedule(ctx, job)
			if err != nil && !errors.Is(err, ErrTaskNotLocked) {
				s.logger.Error("failed to execute and reschedule job", "slug", job.Slug, "error", err)
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

// calculateNextRun returns the next job to run and how long to wait before running it.
// The wait is capped at the heartbeat, so that the store is re-read periodically to pick up
// changes made by other instances.
// If there is no job, the returned job is nil and the wait is the heartbeat.
// A negative wait means wait indefinitely (no job and no heartbeat).
func (s *Scheduler) calculateNextRun(ctx context.Context) (*Task, time.Duration, error) {
	job, err := s.store.NextRun(ctx)
	if errors.Is(err, ErrTaskNotFound) {
		if s.heartbeat > 0 {
			return nil, s.heartbeat, nil
		}
		return nil, -1, nil
	}
	if err != nil {
		return nil, 0, err
	}

	park := parkTime(job.When)
	if s.jitter > 0 {
		park += time.Duration(rand.Int63n(int64(s.jitter)))
	}
	if s.heartbeat > 0 && park > s.heartbeat {
		// re-read the store before the job is due, something may have changed in the meantime
		return nil, s.heartbeat, nil
	}
	return job, park, nil
}

func (s *Scheduler) executeAndReschedule(ctx context.Context, st *Task) error {
	defer s.pokeWaiters()

	st, err := s.store.Lock(ctx, st)
	if err != nil {
		return err
	}

	slug := baseSlug(st.Slug)
	job := s.registry.get(slug)
	if job == nil {
		s.logger.Info("job not found in registry, removing from store", "slug", st.Slug)
		return s.store.Delete(ctx, st.Slug)
	}

	go func() {
		// poke again once the job has been rescheduled (or removed), otherwise the execution loop
		// would only find the job again on the next heartbeat
		defer s.pokeWaiters()

		slug := st.Slug
		st2 := s.executeJob(ctx, *job, st)
		if st2 == nil || st2.When.IsZero() {
			s.logger.Info("job signaled to be removed, deleting job", "slug", slug)
			err := s.store.Delete(ctx, slug)
			if err != nil {
				s.logger.Error("failed to delete job", "slug", slug, "error", err)
			}
			return
		}
		err := s.store.Reschedule(ctx, st2)
		if err != nil {
			s.logger.Error("failed to release job", "slug", slug, "error", err)
			return
		}
	}()

	return nil
}

func (s *Scheduler) executeJob(ctx context.Context, job Job, task *Task) *Task {
	// execute the Executer
	task2, err := job.Executer.Execute(ctx, task)
	if err != nil {
		s.logger.Error("failed to execute job", "slug", task.Slug, "error", err)
		if job.Backoff == nil {
			s.logger.Warn("no retry configured for job", "slug", task.Slug)
			return nil
		}

		task.Result = err.Error()
		task.Retry++

		when, errRetry := job.Backoff.NextRetryTime(task.When, task.Retry)
		if errRetry != nil {
			// no more attempts will be made
			s.logger.Info("no more retries for job", "slug", task.Slug, "retry", task.Retry, "error", errRetry)
			return nil
		}

		s.logger.Warn("backing off job", "slug", task.Slug, "retry", task.Retry, "when", when, "error", err)
		task.When = when
		return task
	}

	if task2 == nil {
		return nil
	}

	task2.Retry = 0
	if task2.Trigger == "" {
		// keep the stored trigger expression, the job is not responsible for carrying it
		task2.Trigger = task.Trigger
	}
	// reschedule the job
	trg := s.resolveTrigger(job, task)
	if trg == nil {
		// will cause this to be removed from the job queue
		return nil
	}
	task2.When = trg.Next(task2.When)
	if task2.When.IsZero() {
		// will cause this to be removed from the job queue
		return nil
	}

	return task2
}

// resolveTrigger returns the trigger to use to reschedule the job.
// For jobs whose trigger comes from the store, the stored expression (as read when the job was locked)
// takes precedence over the registered one, so changes in the store are honoured on every run.
func (s *Scheduler) resolveTrigger(job Job, task *Task) trigger.Trigger {
	if !job.TriggerFromStore || task.Trigger == "" {
		return job.Trigger
	}

	trg, err := trigger.Parse(task.Trigger)
	if err != nil {
		s.logger.Warn("invalid stored trigger, using the registered trigger", "slug", task.Slug, "trigger", task.Trigger, "error", err)
		return job.Trigger
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
