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
	ErrInvalidJobSlug       = errors.New("invalid job slug: must not contain ':'")
	ErrOneOffJob            = errors.New("one-off jobs have no trigger")
	ErrTriggerNotPersistent = errors.New("trigger has no expression and cannot be persisted")
	ErrTriggerFinished      = errors.New("trigger yields no next run time")
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
	// Trigger is the default trigger of the job, used when the task has no (valid) stored trigger expression.
	// One-off jobs have none.
	Trigger trigger.Trigger
	Backoff trigger.Backoff
	// OneOff jobs run once and are then removed. They never recur, whatever is stored with the task.
	OneOff bool
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

// ScheduleJob schedules a recurring job to be run.
//
// The trigger persisted in the store always takes precedence: defaultTrigger is only used when the job
// is not in the store yet, or was stored without a trigger expression. In both cases its expression is persisted.
// The stored expression is re-read on every execution, so changing it (see ChangeJobTrigger, or directly in the store)
// updates the job schedule from its next run onwards, without changing the code.
// If the stored expression is invalid, this fails. If it becomes invalid later, it is logged and defaultTrigger is used instead.
//
// The slug must not contain ':', which is reserved for the tasks of one-off jobs (ErrInvalidJobSlug).
// This should be used before Start.
func (s *Scheduler) ScheduleJob(ctx context.Context, slug string, executer Executer, defaultTrigger trigger.Trigger, options ...ScheduleJobOption) error {
	if s.running {
		return fmt.Errorf("cannot schedule job after scheduler has started job: '%s'", slug)
	}
	if strings.Contains(slug, oneOffSlugSeparator) {
		return fmt.Errorf("failed to schedule job '%s': %w", slug, ErrInvalidJobSlug)
	}

	storedJob, err := s.store.Get(ctx, slug)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		// not stored yet, the default trigger will be used
	case err != nil:
		return fmt.Errorf("failed to load job '%s' from store: %w", slug, err)
	case storedJob.Trigger != "":
		// fail fast on an unusable stored trigger
		if _, err := trigger.Parse(storedJob.Trigger); err != nil {
			return fmt.Errorf("failed to parse stored trigger for job '%s': %w", slug, err)
		}
	default:
		// stored without a trigger: the default is used from now on, so persist it (keeping the run time)
		if expr := trigger.Expression(defaultTrigger); expr != "" {
			if err := s.store.UpdateTrigger(ctx, slug, expr, storedJob.When); err != nil {
				return fmt.Errorf("failed to persist default trigger for job '%s': %w", slug, err)
			}
		}
	}

	return s.scheduleJob(ctx, slug, &Job{
		Executer: executer,
		Trigger:  defaultTrigger,
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
}

type RegisterJobOption func(options *RegisterJobOptions)

func WithJobBackoff(backoff trigger.Backoff) RegisterJobOption {
	return func(options *RegisterJobOptions) {
		options.Backoff = backoff
	}
}

// RegisterOneOffJob registers a job whose runs are scheduled individually with ScheduleOneOffJob.
// Each run executes once and is then removed: one-off jobs have no trigger and never recur.
// Fails with ErrJobAlreadyRegistered if the job is already registered.
//
// This should be used to register the jobs before starting the scheduler.
func (s *Scheduler) RegisterOneOffJob(slug string, executer Executer, options ...RegisterJobOption) error {
	if s.running {
		return fmt.Errorf("cannot register one-off job after scheduler has started: '%s'", slug)
	}
	if strings.Contains(slug, oneOffSlugSeparator) {
		return fmt.Errorf("failed to register one-off job '%s': %w", slug, ErrInvalidJobSlug)
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
		return "", fmt.Errorf("cannot schedule one-off job '%s': %w", slug, ErrInvalidJobSlug)
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

// ChangeJobTrigger replaces the trigger of a job.
// The new trigger expression is persisted in the store, so it survives restarts and is honoured by
// every scheduler instance sharing the store, and the next run time is recomputed from it (from now).
// A suspended job keeps its suspension; the new trigger is used when it is resumed.
//
// If a run of the job is in progress, the run time it is rescheduled with at the end of the run was already
// computed with the previous trigger, so the new trigger only takes effect on the run after that one.
//
// The trigger must have a string expression (see trigger.Expressioner), otherwise it fails with ErrTriggerNotPersistent.
// Fails with ErrOneOffJob for the task of a one-off job, since those never recur.
// Fails with ErrTaskNotFound if the job is not in the store.
func (s *Scheduler) ChangeJobTrigger(ctx context.Context, slug string, trg trigger.Trigger) error {
	if trg == nil {
		return fmt.Errorf("cannot change trigger of job '%s': trigger is nil", slug)
	}
	if isOneOffSlug(slug) {
		return fmt.Errorf("cannot change trigger of job '%s': %w", slug, ErrOneOffJob)
	}
	expr := trigger.Expression(trg)
	if expr == "" {
		return fmt.Errorf("cannot change trigger of job '%s': %w", slug, ErrTriggerNotPersistent)
	}
	when := trg.Next(time.Now())
	if when.IsZero() {
		return fmt.Errorf("cannot change trigger of job '%s': %w", slug, ErrTriggerFinished)
	}

	if err := s.store.UpdateTrigger(ctx, slug, expr, when); err != nil {
		return err
	}
	s.pokeWaiters()

	return nil
}

// triggerFor returns the trigger of the task, or nil if it has none.
// It honours the same precedence as the execution: the stored expression, otherwise the registered trigger.
// If the job is not registered in this instance, only the stored expression can be used.
func (s *Scheduler) triggerFor(st *Task) trigger.Trigger {
	job := s.registry.get(baseSlug(st.Slug))
	if job != nil {
		return s.resolveTrigger(*job, st)
	}
	if isOneOffSlug(st.Slug) || st.Trigger == "" {
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

// isOneOffSlug reports whether the slug is the one of a one-off task.
// Only those contain the separator, since it is rejected in job slugs.
func isOneOffSlug(slug string) bool {
	return strings.Contains(slug, oneOffSlugSeparator)
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

// resolveTrigger returns the trigger to use to reschedule the job, or nil if it must not run again.
// The stored expression (as read when the job was locked) takes precedence over the registered one,
// so changes in the store are honoured on every run. One-off jobs never recur.
func (s *Scheduler) resolveTrigger(job Job, task *Task) trigger.Trigger {
	if job.OneOff {
		return nil
	}
	if task.Trigger == "" {
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
