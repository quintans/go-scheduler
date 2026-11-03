package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/quintans/go-scheduler/trigger"
)

var (
	ErrTaskNotFound         = errors.New("no task with the given Slug was found")
	ErrTaskNotLocked        = errors.New("task lock was not acquired")
	ErrTaskAlreadyExists    = errors.New("task already exists")
	ErrExecuterRegistered   = errors.New("executer name already registered")
	ErrExecuterNotFound     = errors.New("no executer registered with the given name")
	ErrExecuterMismatch     = errors.New("task is stored with a different executer")
	ErrInvalidExecuterName  = errors.New("invalid executer name: must not be empty")
	ErrOneOffJob            = errors.New("one-off tasks have no trigger")
	ErrTriggerNotPersistent = errors.New("trigger has no expression and cannot be persisted")
	ErrTriggerFinished      = errors.New("trigger yields no next run time")
	ErrUnknownTrigger       = errors.New("no trigger factory registered with the given name")
	ErrTriggerRegistered    = errors.New("trigger name already registered")
	ErrInvalidTriggerName   = errors.New("invalid trigger name: must not be empty nor contain ':' or whitespace")
)

const (
	waitOnCalculateNextRunError = 10 * time.Second
	oneOffSlugSeparator         = ":"
	triggerNameSeparator        = ":"
)

// registeredExecuter is a registered Executer along with the backoff used when its runs fail.
type registeredExecuter struct {
	Executer
	backoff trigger.Backoff
}

type registry struct {
	mu        sync.RWMutex
	executers map[string]*registeredExecuter
}

func newRegistry() *registry {
	return &registry{
		executers: map[string]*registeredExecuter{},
	}
}

func (r *registry) add(name string, e *registeredExecuter) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.executers[name]; exists {
		return fmt.Errorf("executer '%s': %w", name, ErrExecuterRegistered)
	}

	r.executers[name] = e
	return nil
}

func (r *registry) get(name string) *registeredExecuter {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.executers[name]
}

// names returns the names of the registered executers, sorted.
func (r *registry) names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.executers))
	for name := range r.executers {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// ScheduledJob is the metadata of a scheduled job, as read from the store.
type ScheduledJob struct {
	// Executer is the name of the executer that runs the job (see RegisterExecuter).
	Executer string
	// Trigger is the persisted trigger expression ("<name>: <expression>"). Empty for one-off tasks.
	Trigger     string
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

	// executers by name, used to run the tasks read from the store
	registry *registry
	// names of the registered executers, used to read only their tasks from the store.
	// Set by Start, when nothing can be registered any more, so the workers can read it without locking.
	executers []string
	// trigger parsers by trigger name, used to rebuild the triggers persisted in the store.
	// Only written before Start, so the workers can read it without locking.
	triggers map[string]func(expr string) (trigger.Trigger, error)

	logger *slog.Logger
}

// NewScheduler returns a new Scheduler.
// The cron ("cron") and simple ("every") triggers come registered; see RegisterTrigger for custom ones.
func NewScheduler(store Store, options ...SchedulerOption) *Scheduler {
	s := &Scheduler{
		store:     store,
		heartbeat: time.Minute,
		registry:  newRegistry(),
		triggers: map[string]func(expr string) (trigger.Trigger, error){
			trigger.CronName:   trigger.ParseCronTrigger,
			trigger.SimpleName: trigger.ParseSimpleTrigger,
		},
		logger: slog.Default(),
	}
	for _, f := range options {
		f(s)
	}

	return s
}

// RegisterTrigger registers a kind of trigger under its Name(), so that the triggers persisted in the store
// as "<name>: <expression>" can be rebuilt with its Parse method. Any instance will do, e.g. the zero value.
//
// ScheduleJob registers the trigger it is given if it is not registered yet, so this is only needed for the kinds
// of trigger that this instance does not schedule that way before Start: those given to ChangeJobTrigger, those
// scheduled by another instance, or whose expression was edited in the store.
// The name must not be empty nor contain ':' or whitespace (ErrInvalidTriggerName), and cannot be registered
// twice (ErrTriggerRegistered).
//
// This must be used before Start: every replica sharing the store must know the same triggers before any of them
// runs, otherwise a task may be picked up by a replica unable to rebuild its trigger.
func (s *Scheduler) RegisterTrigger(trg trigger.Trigger) error {
	if trg == nil {
		return errors.New("failed to register trigger: trigger is nil")
	}
	name := trg.Name()
	if s.running {
		return fmt.Errorf("cannot register trigger after scheduler has started: '%s'", name)
	}
	if name == "" || strings.Contains(name, triggerNameSeparator) || strings.ContainsFunc(name, unicode.IsSpace) {
		return fmt.Errorf("failed to register trigger '%s': %w", name, ErrInvalidTriggerName)
	}
	if _, exists := s.triggers[name]; exists {
		return fmt.Errorf("failed to register trigger '%s': %w", name, ErrTriggerRegistered)
	}
	s.triggers[name] = trg.Parse
	return nil
}

// ensureTriggerRegistered registers the kind of the trigger if it is not registered yet.
// Fails with ErrUnknownTrigger if it is not registered and the scheduler has started, since it can no longer be.
func (s *Scheduler) ensureTriggerRegistered(trg trigger.Trigger) error {
	name := trg.Name()
	if _, ok := s.triggers[name]; ok {
		return nil
	}
	if s.running {
		return fmt.Errorf("%w: '%s' (triggers cannot be registered after Start)", ErrUnknownTrigger, name)
	}
	return s.RegisterTrigger(trg)
}

// encodeTrigger returns the persisted form of the trigger: "<name>: <expression>".
// Fails with ErrTriggerNotPersistent if the trigger has no expression and with ErrUnknownTrigger
// if its name is not registered, since it could never be rebuilt.
func (s *Scheduler) encodeTrigger(trg trigger.Trigger) (string, error) {
	expr := trg.Expression()
	if expr == "" {
		return "", ErrTriggerNotPersistent
	}
	name := trg.Name()
	if _, ok := s.triggers[name]; !ok {
		return "", fmt.Errorf("%w: '%s'", ErrUnknownTrigger, name)
	}
	return name + triggerNameSeparator + " " + expr, nil
}

// parseTrigger rebuilds a trigger from its persisted form "<name>: <expression>", using the trigger kind registered for name.
func (s *Scheduler) parseTrigger(stored string) (trigger.Trigger, error) {
	name, expr, found := strings.Cut(stored, triggerNameSeparator)
	if !found {
		return nil, fmt.Errorf("invalid trigger '%s': expected '<name>: <expression>'", stored)
	}
	name = strings.TrimSpace(name)
	parse, ok := s.triggers[name]
	if !ok {
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownTrigger, name)
	}
	trg, err := parse(strings.TrimSpace(expr))
	if err != nil {
		return nil, fmt.Errorf("failed to build trigger '%s': %w", name, err)
	}
	return trg, nil
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

type RegisterExecuterOptions struct {
	Backoff trigger.Backoff
}

type RegisterExecuterOption func(options *RegisterExecuterOptions)

// WithBackoff sets the backoff used to retry the failed runs of the executer.
// Defaults to trigger.NewExponentialBackoff(). A nil backoff means failed runs are not retried: the task is removed.
func WithBackoff(backoff trigger.Backoff) RegisterExecuterOption {
	return func(options *RegisterExecuterOptions) {
		options.Backoff = backoff
	}
}

// RegisterExecuter registers an executer under its Name().
// Tasks reference their executer by name, which is persisted with each of them, so any number of jobs can share
// the same executer, and every scheduler instance sharing the store must register the same executers before Start.
//
// ScheduleJob registers the executer it is given if it is not registered yet, so this is only needed to set
// options (see WithBackoff), or for executers whose runs are all scheduled with ScheduleOnce, which refers to
// the executer by name.
// The name must not be empty (ErrInvalidExecuterName) and cannot be registered twice (ErrExecuterRegistered).
//
// This must be used before Start. A replica only runs the tasks of the executers it knows: the tasks of any other
// executer are left in the store for the replicas that know it (e.g. during a rollout that adds an executer).
// The tasks of an executer that no longer exists are never run; remove them with DeleteJob.
func (s *Scheduler) RegisterExecuter(executer Executer, options ...RegisterExecuterOption) error {
	if executer == nil {
		return errors.New("failed to register executer: executer is nil")
	}
	name := executer.Name()
	if s.running {
		return fmt.Errorf("cannot register executer after scheduler has started: '%s'", name)
	}
	if name == "" {
		return fmt.Errorf("failed to register executer '%s': %w", name, ErrInvalidExecuterName)
	}

	opts := RegisterExecuterOptions{
		Backoff: trigger.NewExponentialBackoff(),
	}
	for _, o := range options {
		o(&opts)
	}
	return s.registry.add(name, &registeredExecuter{
		Executer: executer,
		backoff:  opts.Backoff,
	})
}

type ScheduleJobOptions struct {
	Slug    string
	When    time.Time
	Payload []byte
}

type ScheduleJobOption func(options *ScheduleJobOptions)

// WithSlug sets the slug that identifies the task in the store.
// For ScheduleJob it defaults to the executer name, for ScheduleOnce to "<executer>:<uuid>".
func WithSlug(slug string) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.Slug = slug
	}
}

// WithDelay sets the first run time of the task to now plus delay.
func WithDelay(delay time.Duration) ScheduleJobOption {
	return func(options *ScheduleJobOptions) {
		options.When = time.Now().Add(delay)
	}
}

// WithWhen sets the first run time of the task.
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

// ScheduleJob schedules a recurring job, run by the given executer.
// The task is identified by its slug, which defaults to the executer name (see WithSlug), so several jobs
// can be scheduled for the same executer by giving each its own slug.
// The executer and the trigger kind are registered (see RegisterExecuter and RegisterTrigger) if they are not yet.
// If an executer with the same name is already registered, that one runs the job and the given instance is only
// used for its name. Since nothing can be registered after Start, this should be used before Start, unless both
// are registered.
//
// The trigger persisted in the store always takes precedence: trg is only used when the job is not in the store yet,
// in which case its expression is persisted along with the executer name, the payload and the first run time
// (the next trigger time, see WithWhen and WithDelay). If the job is already stored, nothing is changed:
// the stored trigger is validated and the stored executer must be the given one (ErrExecuterMismatch).
// The stored expression is re-read on every execution, so changing it (see ChangeJobTrigger, or directly in the store)
// updates the job schedule from its next run onwards, without changing the code. If it becomes invalid later,
// the job does not run: it is backed off until the expression is fixed.
//
// Triggers are persisted as "<name>: <expression>" and rebuilt by the trigger kind registered for the name,
// so the trigger must have a non-empty Expression() (ErrTriggerNotPersistent).
// Fails with ErrExecuterNotFound or ErrUnknownTrigger if the executer or the trigger kind is not registered and
// the scheduler has started, and with ErrOneOffJob if the slug belongs to a one-off task.
func (s *Scheduler) ScheduleJob(ctx context.Context, exec Executer, trg trigger.Trigger, options ...ScheduleJobOption) error {
	if exec == nil {
		return errors.New("failed to schedule job: executer is nil")
	}
	executer := exec.Name()
	opts := ScheduleJobOptions{
		Slug: executer,
	}
	for _, o := range options {
		o(&opts)
	}
	slug := opts.Slug

	if err := s.ensureRegistered(exec); err != nil {
		return fmt.Errorf("failed to schedule job '%s': %w", slug, err)
	}
	if trg == nil {
		return fmt.Errorf("failed to schedule job '%s': trigger is nil", slug)
	}
	if err := s.ensureTriggerRegistered(trg); err != nil {
		return fmt.Errorf("failed to schedule job '%s': %w", slug, err)
	}
	expression, err := s.encodeTrigger(trg)
	if err != nil {
		return fmt.Errorf("failed to schedule job '%s': %w", slug, err)
	}

	stored, err := s.store.Get(ctx, slug)
	switch {
	case errors.Is(err, ErrTaskNotFound):
		// not stored yet, created below
	case err != nil:
		return fmt.Errorf("failed to load job '%s' from store: %w", slug, err)
	default:
		// the stored task wins, make sure it is usable
		if stored.Executer != executer {
			return fmt.Errorf("failed to schedule job '%s' for executer '%s': %w: '%s'", slug, executer, ErrExecuterMismatch, stored.Executer)
		}
		if stored.Trigger == "" {
			return fmt.Errorf("failed to schedule job '%s': %w", slug, ErrOneOffJob)
		}
		if _, err := s.parseTrigger(stored.Trigger); err != nil {
			return fmt.Errorf("failed to parse stored trigger for job '%s': %w", slug, err)
		}
		return nil
	}

	when := opts.When
	if when.IsZero() {
		when = trg.Next(time.Now())
		if when.IsZero() {
			return fmt.Errorf("failed to schedule job '%s': %w", slug, ErrTriggerFinished)
		}
	}

	defer s.pokeWaiters()

	err = s.store.Create(ctx, &Task{
		Slug:     slug,
		Executer: executer,
		Payload:  opts.Payload,
		When:     when,
		Trigger:  expression,
	})
	if err != nil && !errors.Is(err, ErrTaskAlreadyExists) {
		// already created by another instance in the meantime is fine
		return fmt.Errorf("failed to create task '%s': %w", slug, err)
	}

	return nil
}

// ScheduleOnce schedules a single run of the executer registered under the given name.
// The task runs once, when set by WithWhen or WithDelay (immediately by default), and is then removed:
// one-off tasks have no trigger and never recur.
// The executer is identified by its name (see Executer.Name) because it must be registered already
// (see RegisterExecuter), otherwise it fails with ErrExecuterNotFound: one-off runs are usually scheduled
// after Start, when nothing can be registered any more, and every replica sharing the store must know the
// executer to run the task.
//
// The slug of the task defaults to "<executer>:<uuid>", so each call schedules a new run; it is returned so that
// it can be used with SuspendJob, ResumeJob, DeleteJob or GetScheduledJob. A slug given with WithSlug must not be
// in use (ErrTaskAlreadyExists), which makes it possible to schedule a run at most once for a given key.
func (s *Scheduler) ScheduleOnce(ctx context.Context, executer string, options ...ScheduleJobOption) (string, error) {
	if executer == "" {
		return "", fmt.Errorf("failed to schedule one-off task: %w", ErrInvalidExecuterName)
	}
	opts := ScheduleJobOptions{
		Slug: executer + oneOffSlugSeparator + uuid.NewString(),
		When: time.Now(),
	}
	for _, o := range options {
		o(&opts)
	}
	slug := opts.Slug

	if s.registry.get(executer) == nil {
		return "", fmt.Errorf("failed to schedule one-off task '%s': %w: '%s'", slug, ErrExecuterNotFound, executer)
	}

	defer s.pokeWaiters()

	err := s.store.Create(ctx, &Task{
		Slug:     slug,
		Executer: executer,
		Payload:  opts.Payload,
		When:     opts.When,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create one-off task '%s': %w", slug, err)
	}

	return slug, nil
}

// ensureRegistered registers the executer with the default options if no executer with its name is registered yet.
// Fails with ErrExecuterNotFound if it is not registered and the scheduler has started, since it can no longer be.
func (s *Scheduler) ensureRegistered(exec Executer) error {
	name := exec.Name()
	if s.registry.get(name) != nil {
		return nil
	}
	if s.running {
		return fmt.Errorf("%w: '%s' (executers cannot be registered after Start)", ErrExecuterNotFound, name)
	}
	err := s.RegisterExecuter(exec)
	if errors.Is(err, ErrExecuterRegistered) {
		// registered concurrently in the meantime
		return nil
	}
	return err
}

// Start starts the scheduler.
//
// This should be called after registering the triggers and the executers, and after scheduling the jobs:
// nothing can be registered afterwards, so only known executers and trigger kinds can be scheduled then.
func (s *Scheduler) Start(ctx context.Context, workers int) {
	if s.running {
		return
	}
	s.running = true
	s.executers = s.registry.names()

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

// GetJobSlugs returns the slugs of all of the scheduled jobs.
func (s *Scheduler) GetJobSlugs(ctx context.Context) ([]string, error) {
	return s.store.GetSlugs(ctx)
}

// GetScheduledJob get the scheduled job metadata
func (s *Scheduler) GetScheduledJob(ctx context.Context, slug string) (*ScheduledJob, error) {
	st, err := s.store.Get(ctx, slug)
	if err != nil {
		return nil, err
	}

	return &ScheduledJob{
		Executer:    st.Executer,
		Trigger:     st.Trigger,
		NextRunTime: st.When,
		Suspended:   st.Suspended,
	}, nil
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
// so that the runs missed while suspended are not executed. A task without a trigger (one-off) keeps its
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
		trg, err := s.resolveTrigger(st)
		if err != nil {
			s.logger.Warn("invalid stored trigger, keeping the run time", "slug", st.Slug, "trigger", st.Trigger, "error", err)
		}
		if trg != nil {
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
// The trigger must have a non-empty string expression (see trigger.Trigger.Expression), otherwise it fails with ErrTriggerNotPersistent,
// and its kind must be registered already (see RegisterTrigger), otherwise it fails with ErrUnknownTrigger: the change
// is usually made after Start, when nothing can be registered any more, and every replica sharing the store must know
// the kind to reschedule the job.
// Fails with ErrOneOffJob for a one-off task, since those never recur.
// Fails with ErrTaskNotFound if the job is not in the store.
func (s *Scheduler) ChangeJobTrigger(ctx context.Context, slug string, trg trigger.Trigger) error {
	if trg == nil {
		return fmt.Errorf("cannot change trigger of job '%s': trigger is nil", slug)
	}
	expr, err := s.encodeTrigger(trg)
	if err != nil {
		return fmt.Errorf("cannot change trigger of job '%s': %w", slug, err)
	}
	when := trg.Next(time.Now())
	if when.IsZero() {
		return fmt.Errorf("cannot change trigger of job '%s': %w", slug, ErrTriggerFinished)
	}

	st, err := s.store.Get(ctx, slug)
	if err != nil {
		return err
	}
	if st.Trigger == "" {
		return fmt.Errorf("cannot change trigger of job '%s': %w", slug, ErrOneOffJob)
	}

	if err := s.store.UpdateTrigger(ctx, slug, expr, when); err != nil {
		return err
	}
	s.pokeWaiters()

	return nil
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
	job, err := s.store.NextRun(ctx, s.executers)
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
	// the store only returns the tasks of the registered executers, so this cannot fail unless the store is broken.
	// The task is left alone, since it may belong to a replica that knows the executer.
	exec := s.registry.get(st.Executer)
	if exec == nil {
		return fmt.Errorf("task '%s' has no executer registered: '%s' (the store must only return the tasks of the executers it is given)", st.Slug, st.Executer)
	}

	defer s.pokeWaiters()

	st, err := s.store.Lock(ctx, st)
	if err != nil {
		return err
	}

	go func() {
		// poke again once the job has been rescheduled (or removed), otherwise the execution loop
		// would only find the job again on the next heartbeat
		defer s.pokeWaiters()

		slug := st.Slug
		st2 := s.executeJob(ctx, exec, st)
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

// executeJob runs the task and returns it updated for its next run, or nil if it must be removed.
func (s *Scheduler) executeJob(ctx context.Context, exec *registeredExecuter, task *Task) *Task {
	// make sure the task can be rescheduled before running it: a run that could not be rescheduled
	// would be lost, or would repeat at the backoff pace instead of its intended one
	trg, err := s.resolveTrigger(task)
	if err != nil {
		s.logger.Error("invalid stored trigger, the task will not run until it is fixed", "slug", task.Slug, "trigger", task.Trigger, "error", err)
		return s.backoff(exec, task, err)
	}

	task2, err := exec.Execute(ctx, task)
	if err != nil {
		s.logger.Error("failed to execute job", "slug", task.Slug, "error", err)
		return s.backoff(exec, task, err)
	}

	if task2 == nil {
		return nil
	}

	task2.Retry = 0
	// the identity of the task is not the job's responsibility to carry
	task2.Slug = task.Slug
	task2.Executer = task.Executer
	if task2.Trigger == "" {
		// keep the stored trigger expression
		task2.Trigger = task.Trigger
	}
	if trg == nil {
		// one-off task: will cause this to be removed from the job queue
		return nil
	}
	task2.When = trg.Next(task2.When)
	if task2.When.IsZero() {
		// will cause this to be removed from the job queue
		return nil
	}

	return task2
}

// backoff returns the task scheduled for a retry after cause, or nil if it must not be retried.
func (s *Scheduler) backoff(exec *registeredExecuter, task *Task, cause error) *Task {
	if exec.backoff == nil {
		s.logger.Warn("no retry configured for job", "slug", task.Slug)
		return nil
	}

	task.Result = cause.Error()
	task.Retry++

	when, err := exec.backoff.NextRetryTime(task.When, task.Retry)
	if err != nil {
		// no more attempts will be made
		s.logger.Info("no more retries for job", "slug", task.Slug, "retry", task.Retry, "error", err)
		return nil
	}

	s.logger.Warn("backing off job", "slug", task.Slug, "retry", task.Retry, "when", when, "error", cause)
	task.When = when
	return task
}

// resolveTrigger rebuilds the trigger persisted with the task, or returns nil if it has none (one-off task).
// The stored expression is read on every run, so changes in the store are honoured.
func (s *Scheduler) resolveTrigger(task *Task) (trigger.Trigger, error) {
	if task.Trigger == "" {
		return nil, nil
	}
	return s.parseTrigger(task.Trigger)
}

func parkTime(ts time.Time) time.Duration {
	now := time.Now()
	if ts.After(now) {
		return ts.Sub(now)
	}
	return 0
}
