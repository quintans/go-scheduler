package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/quintans/go-scheduler/scheduler"
	"github.com/quintans/go-scheduler/store/memory"
	"github.com/quintans/go-scheduler/trigger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := memory.New()
		sched := scheduler.NewScheduler(store)

		// ALL executers need to be registered before starting the scheduler,
		// since every instance must be able to run the tasks found in the store
		printJob := &DummyExecuter{name: "print"}
		require.NoError(t, sched.RegisterExecuter(printJob))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cronTrigger, err := trigger.NewCronTrigger("1/3 * * * * *")
		require.NoError(t, err)
		when := cronTrigger.Next(time.Now())
		err = sched.ScheduleJob(
			ctx, printJob,
			cronTrigger,
			scheduler.WithSlug("print-cron"),
			scheduler.WithWhen(when),
			scheduler.WithPayload([]byte("Cron job")),
		)
		require.NoError(t, err)

		sched.Start(ctx, 1)

		// after start we can still schedule runs, as long as the executer is registered
		now := time.Now()
		adHocSlug, err := sched.ScheduleOnce(ctx, "print", scheduler.WithWhen(now.Add(time.Second*5)), scheduler.WithPayload([]byte("Ad hoc Job")))
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(adHocSlug, "print:"))
		firstSlug, err := sched.ScheduleOnce(ctx, "print", scheduler.WithWhen(now.Add(time.Second*12)), scheduler.WithPayload([]byte("First job")))
		require.NoError(t, err)
		secondSlug, err := sched.ScheduleOnce(ctx, "print", scheduler.WithDelay(time.Second*6), scheduler.WithPayload([]byte("Second job")))
		require.NoError(t, err)
		thirdSlug, err := sched.ScheduleOnce(ctx, "print", scheduler.WithDelay(time.Second*3), scheduler.WithPayload([]byte("Third job")))
		require.NoError(t, err)

		slugs, err := sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{adHocSlug, firstSlug, secondSlug, thirdSlug, "print-cron"}, slugs)

		// the cron job runs at 1s, 4s, 7s, 10s...; observe between runs to avoid racing with an execution
		time.Sleep(time.Millisecond * 9500)

		sj, err := sched.GetScheduledJob(ctx, "print-cron")
		require.NoError(t, err)
		assert.Equal(t, "print", sj.Executer)
		assert.Equal(t, "cron: 1/3 * * * * *", sj.Trigger)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{firstSlug, "print-cron"}, slugs)

		st, err := store.Get(ctx, "print-cron")
		require.NoError(t, err)
		assert.Equal(t, "Cron job#3", string(st.Payload))
		assert.Equal(t, "print", st.Executer)

		err = sched.DeleteJob(ctx, "print-cron")
		require.NoError(t, err)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{firstSlug}, slugs)
	})
}

func TestConcurrentScheduling(t *testing.T) {
	store := memory.New()
	sched1 := scheduler.NewScheduler(store)
	sched2 := scheduler.NewScheduler(store)

	printJob := &DummyExecuter{name: "print"}
	err := sched1.ScheduleJob(t.Context(), printJob, trigger.NewSimpleTrigger(time.Millisecond*500))
	require.NoError(t, err)
	err = sched2.ScheduleJob(t.Context(), printJob, trigger.NewSimpleTrigger(time.Millisecond*500))
	require.NoError(t, err)
}

// DummyExecuter implements the scheduler.Executer interface. It is named "job" unless a name is given.
type DummyExecuter struct {
	name string
}

// jobExecuter is the executer used by most tests, named "job".
var jobExecuter = &DummyExecuter{}

func (pj DummyExecuter) Name() string {
	if pj.name == "" {
		return "job"
	}
	return pj.name
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
func (pj DummyExecuter) Execute(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
	splits := strings.Split(string(st.Payload), "#")
	if len(splits) == 1 {
		st.Payload = fmt.Appendf(nil, "%s#1", st.Payload)
	} else {
		count, err := strconv.Atoi(splits[1])
		if err != nil {
			return nil, err
		}
		count++
		st.Payload = fmt.Appendf(nil, "%s#%d", splits[0], count)
	}

	return st, nil
}

// newScheduler returns a scheduler on the store with jobExecuter registered, so that its runs can be scheduled after Start.
func newScheduler(t *testing.T, store scheduler.Store, options ...scheduler.SchedulerOption) *scheduler.Scheduler {
	t.Helper()
	sched := scheduler.NewScheduler(store, options...)
	require.NoError(t, sched.RegisterExecuter(jobExecuter))
	return sched
}

func TestRegisterExecuter(t *testing.T) {
	t.Run("validates the registration", func(t *testing.T) {
		sched := scheduler.NewScheduler(memory.New())

		unnamed := scheduler.ExecuterFunc("", DummyExecuter{}.Execute)
		require.ErrorIs(t, sched.RegisterExecuter(unnamed), scheduler.ErrInvalidExecuterName)
		require.Error(t, sched.RegisterExecuter(nil))

		require.NoError(t, sched.RegisterExecuter(jobExecuter))
		require.ErrorIs(t, sched.RegisterExecuter(&DummyExecuter{}), scheduler.ErrExecuterRegistered)

		sched.Start(t.Context(), 1)
		require.Error(t, sched.RegisterExecuter(&DummyExecuter{name: "late"}))
	})

	t.Run("scheduling registers the executer, until the scheduler is started", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := scheduler.NewScheduler(store)

		// not registered: ScheduleJob registers it
		err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour))
		require.NoError(t, err)
		require.ErrorIs(t, sched.RegisterExecuter(jobExecuter), scheduler.ErrExecuterRegistered)
		// ScheduleOnce does not: it refers to the executer by name, so it must be registered
		other := &DummyExecuter{name: "other"}
		_, err = sched.ScheduleOnce(ctx, "other", scheduler.WithDelay(time.Hour))
		require.ErrorIs(t, err, scheduler.ErrExecuterNotFound)
		require.NoError(t, sched.RegisterExecuter(other))
		_, err = sched.ScheduleOnce(ctx, "other", scheduler.WithDelay(time.Hour))
		require.NoError(t, err)

		sched.Start(ctx, 1)

		// the registered executers can still be scheduled, any other cannot
		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour), scheduler.WithSlug("job-2"))
		require.NoError(t, err)
		_, err = sched.ScheduleOnce(ctx, "other", scheduler.WithDelay(time.Hour))
		require.NoError(t, err)

		late := &DummyExecuter{name: "late"}
		err = sched.ScheduleJob(ctx, late, trigger.NewSimpleTrigger(time.Hour))
		require.ErrorIs(t, err, scheduler.ErrExecuterNotFound)
		_, err = sched.ScheduleOnce(ctx, "late")
		require.ErrorIs(t, err, scheduler.ErrExecuterNotFound)
		_, err = store.Get(ctx, "late")
		require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	})

	t.Run("the task of an executer not registered is left for the replicas that know it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			// a task stored by another replica (e.g. a newer deployment), whose executer is not registered here.
			// It is due before the known one, so it would be picked up first if it were not ignored.
			err := store.Create(ctx, &scheduler.Task{Slug: "unknown:1", Executer: "unknown", When: time.Now().Add(time.Second)})
			require.NoError(t, err)

			sched := scheduler.NewScheduler(store)
			var runs atomic.Int32
			known := scheduler.ExecuterFunc("known", func(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
				runs.Add(1)
				return nil, nil
			})
			require.NoError(t, sched.RegisterExecuter(known))
			_, err = sched.ScheduleOnce(ctx, "known", scheduler.WithDelay(2*time.Second))
			require.NoError(t, err)

			sched.Start(ctx, 1)
			time.Sleep(3 * time.Second)
			synctest.Wait()

			// the unknown task is untouched, and did not block the known one
			st, err := store.Get(ctx, "unknown:1")
			require.NoError(t, err)
			require.Equal(t, "unknown", st.Executer)
			require.Equal(t, int32(1), runs.Load())
		})
	})
}

func TestScheduleJob(t *testing.T) {
	t.Run("several jobs share the same executer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			var mu sync.Mutex
			runs := map[string]int{}
			report := scheduler.ExecuterFunc("report", func(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
				mu.Lock()
				defer mu.Unlock()
				runs[st.Slug+"="+string(st.Payload)]++
				return st, nil
			})

			start := time.Now()
			// the slug defaults to the executer name
			err := sched.ScheduleJob(ctx, report, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(start.Add(time.Second)))
			require.NoError(t, err)
			err = sched.ScheduleJob(ctx, report, trigger.NewSimpleTrigger(2*time.Second),
				scheduler.WithSlug("report-daily"), scheduler.WithWhen(start.Add(time.Second)), scheduler.WithPayload([]byte("daily")))
			require.NoError(t, err)
			err = sched.ScheduleJob(ctx, report, trigger.NewSimpleTrigger(3*time.Second),
				scheduler.WithSlug("report-weekly"), scheduler.WithWhen(start.Add(time.Second)), scheduler.WithPayload([]byte("weekly")))
			require.NoError(t, err)

			slugs, err := sched.GetJobSlugs(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"report", "report-daily", "report-weekly"}, slugs)
			for _, slug := range slugs {
				st, err := store.Get(ctx, slug)
				require.NoError(t, err)
				assert.Equal(t, "report", st.Executer)
			}

			sched.Start(ctx, 1)
			// runs at 1s, 2s, 3s, 4s, 5s, 6s / 1s, 3s, 5s / 1s, 4s
			time.Sleep(6500 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, map[string]int{"report=": 6, "report-daily=daily": 3, "report-weekly=weekly": 2}, runs)
		})
	})

	t.Run("can be scheduled after the scheduler has started", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)
			sched.Start(ctx, 1)
			// the worker is parked, waiting for a poke
			synctest.Wait()

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour), scheduler.WithWhen(firstRun), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)

			time.Sleep(1500 * time.Millisecond)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, firstRun.Add(time.Hour), st.When)
		})
	})

	t.Run("uses the stored trigger instead of the given one", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			// simulate a job previously persisted (possibly edited) in the store
			firstRun := time.Now().Add(time.Second)
			err := store.Create(ctx, &scheduler.Task{
				Slug:     "job",
				Executer: "job",
				Payload:  []byte("Loaded"),
				When:     firstRun,
				Trigger:  "every: 2s",
			})
			require.NoError(t, err)

			err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour))
			require.NoError(t, err)

			// the stored job is kept untouched
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "every: 2s", st.Trigger)
			assert.Equal(t, firstRun, st.When)

			sched.Start(ctx, 1)
			time.Sleep(1500 * time.Millisecond)

			// the job ran and was rescheduled with the stored trigger (2s), not the given one (1h)
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Loaded#1", string(st.Payload))
			assert.Equal(t, firstRun.Add(2*time.Second), st.When)
			assert.Equal(t, "every: 2s", st.Trigger)
		})
	})

	t.Run("picks up a trigger changed in the store after start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(2*time.Second), scheduler.WithWhen(firstRun))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			// let the execution loop park on the first run timer before editing the store
			synctest.Wait()

			// simulate an edit made directly in the store while the scheduler is running
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			require.Equal(t, "every: 2s", st.Trigger)
			editStoredTrigger(t, store, "job", "every: 5s")

			time.Sleep(1500 * time.Millisecond)

			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "#1", string(st.Payload))
			assert.Equal(t, firstRun.Add(5*time.Second), st.When)

			// an invalid edit does not run the task: it is backed off (10s by default) until the trigger is fixed
			editStoredTrigger(t, store, "job", "not a trigger")
			time.Sleep(5 * time.Second)

			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "#1", string(st.Payload), "the task must not run")
			assert.Equal(t, 1, st.Retry)
			assert.Contains(t, st.Result, "not a trigger")
			assert.Equal(t, firstRun.Add(15*time.Second), st.When)

			editStoredTrigger(t, store, "job", "every: 5s")
			time.Sleep(10 * time.Second)

			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "#2", string(st.Payload))
			assert.True(t, st.IsOK())
			assert.Equal(t, firstRun.Add(20*time.Second), st.When)
		})
	})

	t.Run("persists the trigger when the job is not stored", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		cronTrigger, err := trigger.NewCronTrigger("0 0 * * * *")
		require.NoError(t, err)
		when := time.Now().Add(time.Hour)
		err = sched.ScheduleJob(ctx, jobExecuter, cronTrigger, scheduler.WithWhen(when))
		require.NoError(t, err)

		st, err := store.Get(ctx, "job")
		require.NoError(t, err)
		assert.Equal(t, "job", st.Executer)
		assert.Equal(t, "cron: 0 0 * * * *", st.Trigger)
		assert.Equal(t, when, st.When)
	})

	t.Run("fails when the stored task has another executer", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		err := store.Create(ctx, &scheduler.Task{Slug: "job", Executer: "other", When: time.Now(), Trigger: "every: 1s"})
		require.NoError(t, err)

		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second))
		require.ErrorIs(t, err, scheduler.ErrExecuterMismatch)
	})

	t.Run("fails when the slug belongs to a one-off task", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		slug, err := sched.ScheduleOnce(ctx, "job", scheduler.WithDelay(time.Hour))
		require.NoError(t, err)

		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second), scheduler.WithSlug(slug))
		require.ErrorIs(t, err, scheduler.ErrOneOffJob)
	})

	t.Run("fails on an invalid stored trigger", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		err := store.Create(ctx, &scheduler.Task{Slug: "job", Executer: "job", When: time.Now(), Trigger: "not a trigger"})
		require.NoError(t, err)

		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second))
		require.Error(t, err)
	})

	t.Run("fails on a trigger that cannot be persisted", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		require.ErrorIs(t, sched.ScheduleJob(ctx, jobExecuter, opaqueTrigger{}), scheduler.ErrTriggerNotPersistent)
		require.Error(t, sched.ScheduleJob(ctx, jobExecuter, nil))

		_, err := store.Get(ctx, "job")
		require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	})
}

func TestScheduleOnce(t *testing.T) {
	t.Run("runs once, immediately by default, and is then removed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)
			sched.Start(ctx, 1)
			synctest.Wait()

			var mu sync.Mutex
			var ran []string
			executer := scheduler.ExecuterFunc("once", func(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
				mu.Lock()
				defer mu.Unlock()
				ran = append(ran, string(st.Payload))
				return st, nil
			})
			// the executer registry is closed once started, use a second scheduler for the custom executer
			sched2 := scheduler.NewScheduler(store)
			require.NoError(t, sched2.RegisterExecuter(executer))
			sched2.Start(ctx, 1)
			synctest.Wait()

			now, err := sched2.ScheduleOnce(ctx, "once", scheduler.WithPayload([]byte("now")))
			require.NoError(t, err)
			later, err := sched2.ScheduleOnce(ctx, "once", scheduler.WithDelay(time.Second), scheduler.WithPayload([]byte("later")))
			require.NoError(t, err)
			assert.NotEqual(t, now, later)

			synctest.Wait()
			mu.Lock()
			assert.Equal(t, []string{"now"}, ran)
			mu.Unlock()
			_, err = store.Get(ctx, now)
			require.ErrorIs(t, err, scheduler.ErrTaskNotFound)

			st, err := store.Get(ctx, later)
			require.NoError(t, err)
			assert.Equal(t, "once", st.Executer)
			assert.Empty(t, st.Trigger)

			time.Sleep(1500 * time.Millisecond)
			mu.Lock()
			assert.Equal(t, []string{"now", "later"}, ran)
			mu.Unlock()
			_, err = store.Get(ctx, later)
			require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
		})
	})

	t.Run("an explicit slug is scheduled at most once", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		slug, err := sched.ScheduleOnce(ctx, "job", scheduler.WithSlug("order-42"), scheduler.WithDelay(time.Hour))
		require.NoError(t, err)
		assert.Equal(t, "order-42", slug)

		_, err = sched.ScheduleOnce(ctx, "job", scheduler.WithSlug("order-42"), scheduler.WithDelay(time.Hour))
		require.ErrorIs(t, err, scheduler.ErrTaskAlreadyExists)
	})
}

// editStoredTrigger simulates editing the trigger expression directly in the store,
// without touching the entry shared with the scheduler.
// The memory store has no in-place update, so the job is recreated; callers must make sure
// the scheduler is parked on a timer (synctest.Wait) so it does not observe the empty queue in between.
func editStoredTrigger(t *testing.T, store scheduler.Store, slug, expr string) {
	t.Helper()
	ctx := t.Context()

	st, err := store.Get(ctx, slug)
	require.NoError(t, err)
	edited := *st
	edited.Trigger = expr

	require.NoError(t, store.Delete(ctx, slug))
	require.NoError(t, store.Create(ctx, &edited))
}

func TestSuspendResumeJob(t *testing.T) {
	t.Run("a suspended job does not run and resumes from the next trigger time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second),
				scheduler.WithWhen(firstRun), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			synctest.Wait()

			err = sched.SuspendJob(ctx, "job")
			require.NoError(t, err)

			sj, err := sched.GetScheduledJob(ctx, "job")
			require.NoError(t, err)
			assert.True(t, sj.Suspended)

			// well past several trigger times
			time.Sleep(5 * time.Second)

			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.True(t, st.Suspended)
			assert.Equal(t, "Job", string(st.Payload), "a suspended job must not run")
			assert.Equal(t, firstRun, st.When)

			err = sched.ResumeJob(ctx, "job")
			require.NoError(t, err)
			resumedAt := time.Now()

			sj, err = sched.GetScheduledJob(ctx, "job")
			require.NoError(t, err)
			assert.False(t, sj.Suspended)
			// the missed runs are skipped: the next run is computed from the trigger, not from the stale time
			assert.Equal(t, resumedAt.Add(time.Second), sj.NextRunTime)

			// resuming a job that is not suspended does nothing
			err = sched.ResumeJob(ctx, "job")
			require.NoError(t, err)
			sj, err = sched.GetScheduledJob(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, resumedAt.Add(time.Second), sj.NextRunTime)

			time.Sleep(1500 * time.Millisecond)

			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, resumedAt.Add(2*time.Second), st.When)
		})
	})

	t.Run("a resumed job whose run time has not passed keeps it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			firstRun := time.Now().Add(10 * time.Second)
			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(firstRun))
			require.NoError(t, err)

			require.NoError(t, sched.SuspendJob(ctx, "job"))
			time.Sleep(time.Second)
			require.NoError(t, sched.ResumeJob(ctx, "job"))

			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.False(t, st.Suspended)
			assert.Equal(t, firstRun, st.When)
		})
	})

	t.Run("a resumed one-off task keeps its run time and runs right away if due", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)
			sched.Start(ctx, 1)

			due := time.Now().Add(time.Second)
			slug, err := sched.ScheduleOnce(ctx, "job", scheduler.WithWhen(due), scheduler.WithPayload([]byte("Ad hoc")))
			require.NoError(t, err)
			synctest.Wait()

			require.NoError(t, sched.SuspendJob(ctx, slug))
			time.Sleep(3 * time.Second)

			st, err := store.Get(ctx, slug)
			require.NoError(t, err)
			assert.True(t, st.Suspended)
			assert.Equal(t, "Ad hoc", string(st.Payload))

			require.NoError(t, sched.ResumeJob(ctx, slug))
			st, err = store.Get(ctx, slug)
			require.NoError(t, err)
			assert.False(t, st.Suspended)
			assert.Equal(t, due, st.When)

			// it runs immediately, and being a one-off task without trigger it is then removed
			synctest.Wait()
			_, err = store.Get(ctx, slug)
			require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
		})
	})

	t.Run("suspending a running job lets it finish but does not run it again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			started := make(chan struct{})
			release := make(chan struct{})
			executer := scheduler.ExecuterFunc("slow", func(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
				close(started)
				<-release
				st.Payload = []byte("done")
				return st, nil
			})

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, executer, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(firstRun))
			require.NoError(t, err)
			sched.Start(ctx, 1)

			<-started
			require.NoError(t, sched.SuspendJob(ctx, "slow"))
			close(release)
			synctest.Wait()

			// the result of the run is kept and the job is rescheduled, but as suspended
			st, err := store.Get(ctx, "slow")
			require.NoError(t, err)
			assert.Equal(t, "done", string(st.Payload))
			assert.True(t, st.Suspended)
			assert.Equal(t, firstRun.Add(time.Second), st.When)

			// well past the next run time: nothing runs (the job would panic closing the started channel again)
			time.Sleep(5 * time.Second)
			st, err = store.Get(ctx, "slow")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(time.Second), st.When)
		})
	})

	t.Run("picks up a suspension and a resumption made by another replica or directly in the store", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store, scheduler.HeartbeatOption(time.Second))

			start := time.Now()
			firstRun := start.Add(time.Second)
			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second),
				scheduler.WithWhen(firstRun), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			// the worker is now parked on the timer for the first run
			synctest.Wait()

			// suspended behind the scheduler's back: no poke
			require.NoError(t, store.Suspend(ctx, "job"))

			time.Sleep(5 * time.Second)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.True(t, st.Suspended)
			assert.Equal(t, "Job", string(st.Payload), "the worker must not run the job it was parked on")

			// resumed behind the scheduler's back, due in the future
			resumeAt := time.Now().Add(3 * time.Second)
			require.NoError(t, store.Resume(ctx, "job", resumeAt))

			// found on the next heartbeat (at most 1s later), then run when due
			time.Sleep(3500 * time.Millisecond)
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.False(t, st.Suspended)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, resumeAt.Add(time.Second), st.When)
		})
	})

	t.Run("the suspension survives a restart", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()

		sched := newScheduler(t, store)
		err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second))
		require.NoError(t, err)
		require.NoError(t, sched.SuspendJob(ctx, "job"))

		// a new scheduler instance (or the same one after a restart) schedules the job again
		sched = newScheduler(t, store)
		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second))
		require.NoError(t, err)

		sj, err := sched.GetScheduledJob(ctx, "job")
		require.NoError(t, err)
		assert.True(t, sj.Suspended)
	})

	t.Run("fails on an unknown job", func(t *testing.T) {
		ctx := t.Context()
		sched := scheduler.NewScheduler(memory.New())

		require.ErrorIs(t, sched.SuspendJob(ctx, "unknown"), scheduler.ErrTaskNotFound)
		require.ErrorIs(t, sched.ResumeJob(ctx, "unknown"), scheduler.ErrTaskNotFound)
	})
}

func TestChangeJobTrigger(t *testing.T) {
	t.Run("reschedules a loaded job with the new trigger", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour),
				scheduler.WithWhen(time.Now().Add(time.Hour)), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			// the worker is parked, waiting for the run in one hour
			synctest.Wait()

			err = sched.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(time.Second))
			require.NoError(t, err)
			changedAt := time.Now()

			// the next run is recomputed from now with the new trigger and persisted along with its expression
			sj, err := sched.GetScheduledJob(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, changedAt.Add(time.Second), sj.NextRunTime)
			assert.Equal(t, "every: 1s", sj.Trigger)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "every: 1s", st.Trigger)

			// the parked worker was woken up and runs the job at the new time
			time.Sleep(1500 * time.Millisecond)
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, changedAt.Add(2*time.Second), st.When)
			assert.Equal(t, "every: 1s", st.Trigger)

			// a cron trigger works the same way
			cron, err := trigger.NewCronTrigger("0 0 * * * *")
			require.NoError(t, err)
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", cron))
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "cron: 0 0 * * * *", st.Trigger)
			assert.Equal(t, cron.Next(time.Now()), st.When)
		})
	})

	t.Run("a suspended job keeps its suspension and uses the new trigger when resumed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			synctest.Wait()

			require.NoError(t, sched.SuspendJob(ctx, "job"))
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(time.Second)))

			time.Sleep(5 * time.Second)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.True(t, st.Suspended)
			assert.Equal(t, "every: 1s", st.Trigger)
			assert.Equal(t, "Job", string(st.Payload), "a suspended job must not run")

			require.NoError(t, sched.ResumeJob(ctx, "job"))
			resumedAt := time.Now()

			// the stale run time is recomputed with the new trigger
			sj, err := sched.GetScheduledJob(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, resumedAt.Add(time.Second), sj.NextRunTime)

			time.Sleep(1500 * time.Millisecond)
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, resumedAt.Add(2*time.Second), st.When)
		})
	})

	t.Run("a change made by another instance is picked up on the next heartbeat", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store, scheduler.HeartbeatOption(time.Second))

			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			synctest.Wait()

			// e.g. an admin tool sharing the store, where the executer is not registered
			admin := scheduler.NewScheduler(store)
			require.NoError(t, admin.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(2*time.Second)))
			changedAt := time.Now()

			time.Sleep(2500 * time.Millisecond)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, changedAt.Add(4*time.Second), st.When)
			assert.Equal(t, "every: 2s", st.Trigger)
		})
	})

	t.Run("the change survives a restart", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()

			sched := newScheduler(t, store)
			err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(time.Second)))
			changedAt := time.Now()

			// a new scheduler instance (or the same one after a restart) schedules the job again with the same trigger
			sched = newScheduler(t, store)
			err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour))
			require.NoError(t, err)
			sched.Start(ctx, 1)

			time.Sleep(1500 * time.Millisecond)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, "every: 1s", st.Trigger)
			assert.Equal(t, changedAt.Add(2*time.Second), st.When)
		})
	})

	t.Run("a run in progress is rescheduled with the previous trigger, the new one applies afterwards", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, &slowExecuter{duration: 2 * time.Second}, trigger.NewSimpleTrigger(10*time.Second),
				scheduler.WithWhen(firstRun), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)

			// the job is running
			time.Sleep(1500 * time.Millisecond)
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(3*time.Second)))

			// the run finishes and is rescheduled with the trigger read when it was locked
			time.Sleep(2 * time.Second)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, "every: 3s", st.Trigger)
			assert.Equal(t, firstRun.Add(10*time.Second), st.When)

			// from then on the new trigger is used
			time.Sleep(firstRun.Add(10*time.Second).Sub(time.Now()) + 2500*time.Millisecond)
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#2", string(st.Payload))
			assert.Equal(t, firstRun.Add(13*time.Second), st.When)
		})
	})

	t.Run("fails when the trigger cannot be changed", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		err := sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour))
		require.NoError(t, err)
		ooSlug, err := sched.ScheduleOnce(ctx, "job", scheduler.WithDelay(time.Hour))
		require.NoError(t, err)

		second := trigger.NewSimpleTrigger(time.Second)
		// unknown job
		require.ErrorIs(t, sched.ChangeJobTrigger(ctx, "unknown", second), scheduler.ErrTaskNotFound)
		// one-off tasks never recur
		require.ErrorIs(t, sched.ChangeJobTrigger(ctx, ooSlug, second), scheduler.ErrOneOffJob)
		// a trigger without expression cannot be persisted
		require.ErrorIs(t, sched.ChangeJobTrigger(ctx, "job", opaqueTrigger{}), scheduler.ErrTriggerNotPersistent)
		require.Error(t, sched.ChangeJobTrigger(ctx, "job", nil))

		// nothing was changed
		st, err := store.Get(ctx, "job")
		require.NoError(t, err)
		assert.Equal(t, "every: 1h0m0s", st.Trigger)
		st, err = store.Get(ctx, ooSlug)
		require.NoError(t, err)
		assert.Empty(t, st.Trigger)
	})
}

func TestRegisterTrigger(t *testing.T) {
	t.Run("a custom trigger is persisted under its name and rebuilt with its factory", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := newScheduler(t, store)

			// scheduling registers the trigger kind
			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, jobExecuter, stepTrigger{step: 2}, scheduler.WithWhen(firstRun))
			require.NoError(t, err)
			require.ErrorIs(t, sched.RegisterTrigger(stepTrigger{}), scheduler.ErrTriggerRegistered)

			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "step: 2s", st.Trigger)

			// an edit in the store is honoured through the registered factory
			editStoredTrigger(t, store, "job", "step:5s")
			sched.Start(ctx, 1)
			time.Sleep(1500 * time.Millisecond)

			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(5*time.Second), st.When)

			// and so is a change through the scheduler
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", stepTrigger{step: 7}))
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "step: 7s", st.Trigger)
		})
	})

	t.Run("a stored trigger with an unknown name is rebuilt once its kind is registered", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		err := store.Create(ctx, &scheduler.Task{Slug: "job", Executer: "job", When: time.Now(), Trigger: "step: 3s"})
		require.NoError(t, err)

		sched := newScheduler(t, store)
		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second))
		require.ErrorIs(t, err, scheduler.ErrUnknownTrigger)

		sched = newScheduler(t, store)
		require.NoError(t, sched.RegisterTrigger(stepTrigger{}))
		err = sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Second))
		require.NoError(t, err)
	})

	t.Run("a trigger kind not registered before Start fails afterwards", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		require.NoError(t, sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour)))
		sched.Start(ctx, 1)

		err := sched.ScheduleJob(ctx, jobExecuter, stepTrigger{step: 2}, scheduler.WithSlug("step-job"))
		require.ErrorIs(t, err, scheduler.ErrUnknownTrigger)
		_, err = store.Get(ctx, "step-job")
		require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	})

	t.Run("changing the trigger needs its kind registered", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := newScheduler(t, store)

		require.NoError(t, sched.ScheduleJob(ctx, jobExecuter, trigger.NewSimpleTrigger(time.Hour)))
		err := sched.ChangeJobTrigger(ctx, "job", stepTrigger{step: 2})
		require.ErrorIs(t, err, scheduler.ErrUnknownTrigger)
		st, err := store.Get(ctx, "job")
		require.NoError(t, err)
		assert.Equal(t, "every: 1h0m0s", st.Trigger)

		require.NoError(t, sched.RegisterTrigger(stepTrigger{}))
		require.NoError(t, sched.ChangeJobTrigger(ctx, "job", stepTrigger{step: 2}))
		st, err = store.Get(ctx, "job")
		require.NoError(t, err)
		assert.Equal(t, "step: 2s", st.Trigger)
	})

	t.Run("validates the registration", func(t *testing.T) {
		sched := scheduler.NewScheduler(memory.New())

		for _, name := range []string{"", "a:b", "a b", "a\tb"} {
			require.ErrorIs(t, sched.RegisterTrigger(namedTrigger{name: name}), scheduler.ErrInvalidTriggerName, name)
		}
		require.Error(t, sched.RegisterTrigger(nil))
		// the provided triggers are already registered
		require.ErrorIs(t, sched.RegisterTrigger(&trigger.CronTrigger{}), scheduler.ErrTriggerRegistered)
		require.ErrorIs(t, sched.RegisterTrigger(&trigger.SimpleTrigger{}), scheduler.ErrTriggerRegistered)

		require.NoError(t, sched.RegisterTrigger(stepTrigger{}))
		require.ErrorIs(t, sched.RegisterTrigger(stepTrigger{}), scheduler.ErrTriggerRegistered)

		sched.Start(t.Context(), 1)
		require.Error(t, sched.RegisterTrigger(namedTrigger{name: "late"}))
	})
}

// namedTrigger is a trigger with an arbitrary name, to exercise the name validation.
type namedTrigger struct {
	name string
}

func (t namedTrigger) Name() string                          { return t.name }
func (namedTrigger) Next(prev time.Time) time.Time           { return prev.Add(time.Second) }
func (namedTrigger) Expression() string                      { return "1s" }
func (t namedTrigger) Parse(string) (trigger.Trigger, error) { return t, nil }

// opaqueTrigger is a trigger without a string expression.
type opaqueTrigger struct{}

func (opaqueTrigger) Name() string                            { return "opaque" }
func (opaqueTrigger) Next(prev time.Time) time.Time           { return prev.Add(time.Second) }
func (opaqueTrigger) Expression() string                      { return "" }
func (t opaqueTrigger) Parse(string) (trigger.Trigger, error) { return t, nil }

// stepTrigger is a custom trigger: fires at every multiple of step seconds, as a "<step>s" expression.
type stepTrigger struct {
	step int
}

func parseStepTrigger(expr string) (trigger.Trigger, error) {
	step, err := strconv.Atoi(strings.TrimSuffix(expr, "s"))
	if err != nil || step <= 0 {
		return nil, fmt.Errorf("invalid step '%s'", expr)
	}
	return stepTrigger{step: step}, nil
}

func (stepTrigger) Name() string                               { return "step" }
func (stepTrigger) Parse(expr string) (trigger.Trigger, error) { return parseStepTrigger(expr) }
func (t stepTrigger) Next(prev time.Time) time.Time {
	return prev.Add(time.Duration(t.step) * time.Second)
}
func (t stepTrigger) Expression() string { return fmt.Sprintf("%ds", t.step) }

// slowExecuter behaves like DummyExecuter but takes duration to run.
type slowExecuter struct {
	duration time.Duration
}

func (e *slowExecuter) Name() string { return "job" }

func (e *slowExecuter) Execute(ctx context.Context, st *scheduler.Task) (*scheduler.Task, error) {
	time.Sleep(e.duration)
	return DummyExecuter{}.Execute(ctx, st)
}

func TestExecutionLoopStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &failingStore{Store: memory.New()}
		sched := scheduler.NewScheduler(store)

		ctx, cancel := context.WithCancel(t.Context())
		sched.Start(ctx, 1)
		// the loop hits the failing store and parks on the error back-off
		synctest.Wait()
		calls := store.calls()
		require.Equal(t, 1, calls)

		cancel()
		synctest.Wait()

		// a cancelled loop must not keep hitting the store
		time.Sleep(time.Minute)
		assert.Equal(t, calls, store.calls())
	})
}

// failingStore fails every NextRun and counts the calls.
type failingStore struct {
	scheduler.Store
	mu sync.Mutex
	n  int
}

func (f *failingStore) NextRun(context.Context, []string) (*scheduler.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return nil, errors.New("store down")
}

func (f *failingStore) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}
