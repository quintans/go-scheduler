package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
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

		// ALL jobs need to be registered before starting the scheduler
		// otherwise they will not be picked up by any of the concurrent processes
		printJob := &DummyJob{}
		// on demand job, not required to run on startup, therefore not scheduled,
		// but still needs to be registered
		err := sched.RegisterOneOffJob("ad-hoc", printJob)
		require.NoError(t, err)
		err = sched.RegisterOneOffJob("first", printJob)
		require.NoError(t, err)
		err = sched.RegisterOneOffJob("second", printJob)
		require.NoError(t, err)
		err = sched.RegisterOneOffJob("third", printJob)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cronTrigger, err := trigger.NewCronTrigger("1/3 * * * * *")
		require.NoError(t, err)
		when := cronTrigger.Next(time.Now())
		err = sched.ScheduleJob(
			ctx, "print-cron", printJob,
			cronTrigger,
			scheduler.WithWhen(when),
			scheduler.WithPayload([]byte("Cron job")),
		)
		require.NoError(t, err)

		sched.Start(ctx, 1)

		// after start we schedule one-off jobs, but it is essential that the job is already registered

		// we don't specify the trigger here since it is already registered, but we could override it if we wanted to
		// by specifying it here
		// the same applies to backoff, if we wanted to override the registered backoff
		now := time.Now()
		adHocSlug, err := sched.ScheduleOneOffJob(ctx, "ad-hoc", now.Add(time.Second*5), []byte("Ad hoc Job"))
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(adHocSlug, "ad-hoc:"))
		firstSlug, err := sched.ScheduleOneOffJob(ctx, "first", now.Add(time.Second*12), []byte("First job"))
		require.NoError(t, err)
		secondSlug, err := sched.ScheduleOneOffJob(ctx, "second", now.Add(time.Second*6), []byte("Second job"))
		require.NoError(t, err)
		thirdSlug, err := sched.ScheduleOneOffJob(ctx, "third", now.Add(time.Second*3), []byte("Third job"))
		require.NoError(t, err)

		slugs, err := sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{adHocSlug, firstSlug, secondSlug, thirdSlug, "print-cron"}, slugs)

		// the cron job runs at 1s, 4s, 7s, 10s...; observe between runs to avoid racing with an execution
		time.Sleep(time.Millisecond * 9500)

		_, err = sched.GetScheduledJob(ctx, "print-cron")
		require.NoError(t, err)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{firstSlug, "print-cron"}, slugs)

		st, err := store.Get(ctx, "print-cron")
		require.NoError(t, err)
		assert.Equal(t, "Cron job#3", string(st.Payload))

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

	printJob := &DummyJob{}
	err := sched1.ScheduleJob(t.Context(), "print", printJob, trigger.NewSimpleTrigger(time.Millisecond*500))
	require.NoError(t, err)
	err = sched2.ScheduleJob(t.Context(), "print", printJob, trigger.NewSimpleTrigger(time.Millisecond*500))
	require.NoError(t, err)
}

// DummyJob implements the scheduler.Job interface.
type DummyJob struct{}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (pj DummyJob) Execute(_ context.Context, st *scheduler.StoreTask) (*scheduler.StoreTask, error) {
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

func TestLoadScheduleJob(t *testing.T) {
	t.Run("uses the stored trigger instead of the default", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			// simulate a task previously persisted (possibly edited) in the store
			firstRun := time.Now().Add(time.Second)
			err := store.Create(ctx, &scheduler.StoreTask{
				Slug:    "load-job",
				Payload: []byte("Loaded"),
				When:    firstRun,
				Trigger: "@every 2s",
			})
			require.NoError(t, err)

			err = sched.LoadScheduleJob(ctx, "load-job", &DummyJob{}, trigger.NewSimpleTrigger(time.Hour))
			require.NoError(t, err)

			// the stored task is kept untouched
			st, err := store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, "@every 2s", st.Trigger)
			assert.Equal(t, firstRun, st.When)

			sched.Start(ctx, 1)
			time.Sleep(1500 * time.Millisecond)

			// the job ran and was rescheduled with the stored trigger (2s), not the default (1h)
			st, err = store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, "Loaded#1", string(st.Payload))
			assert.Equal(t, firstRun.Add(2*time.Second), st.When)
			assert.Equal(t, "@every 2s", st.Trigger)
		})
	})

	t.Run("picks up a trigger changed in the store after start", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := sched.LoadScheduleJob(ctx, "load-job", &DummyJob{}, trigger.NewSimpleTrigger(2*time.Second), scheduler.WithWhen(firstRun))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			// let the execution loop park on the first run timer before editing the store
			synctest.Wait()

			// simulate an edit made directly in the store while the scheduler is running
			st, err := store.Get(ctx, "load-job")
			require.NoError(t, err)
			require.Equal(t, "@every 2s", st.Trigger)
			editStoredTrigger(t, store, "load-job", "@every 5s")

			time.Sleep(1500 * time.Millisecond)

			st, err = store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(5*time.Second), st.When)

			// an invalid edit falls back to the registered trigger
			editStoredTrigger(t, store, "load-job", "not a cron")
			time.Sleep(5 * time.Second)

			st, err = store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(7*time.Second), st.When)
		})
	})

	t.Run("ScheduleJob keeps the registered trigger even if the store changes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, "code-job", &DummyJob{}, trigger.NewSimpleTrigger(2*time.Second), scheduler.WithWhen(firstRun))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			// let the execution loop park on the first run timer before editing the store
			synctest.Wait()

			editStoredTrigger(t, store, "code-job", "@every 5s")

			time.Sleep(1500 * time.Millisecond)

			st, err := store.Get(ctx, "code-job")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(2*time.Second), st.When)
		})
	})

	t.Run("falls back to the default trigger when the job is not stored", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := scheduler.NewScheduler(store)

		cronTrigger, err := trigger.NewCronTrigger("0 0 * * * *")
		require.NoError(t, err)
		when := time.Now().Add(time.Hour)
		err = sched.LoadScheduleJob(ctx, "load-job", &DummyJob{}, cronTrigger, scheduler.WithWhen(when))
		require.NoError(t, err)

		st, err := store.Get(ctx, "load-job")
		require.NoError(t, err)
		assert.Equal(t, "0 0 * * * *", st.Trigger)
		assert.Equal(t, when, st.When)
	})

	t.Run("falls back to the default trigger when the stored task has no trigger", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := store.Create(ctx, &scheduler.StoreTask{Slug: "load-job", When: firstRun})
			require.NoError(t, err)

			err = sched.LoadScheduleJob(ctx, "load-job", &DummyJob{}, trigger.NewSimpleTrigger(3*time.Second))
			require.NoError(t, err)

			sched.Start(ctx, 1)
			time.Sleep(1500 * time.Millisecond)

			st, err := store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(3*time.Second), st.When)
		})
	})

	t.Run("fails on an invalid stored trigger", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := scheduler.NewScheduler(store)

		err := store.Create(ctx, &scheduler.StoreTask{Slug: "load-job", When: time.Now(), Trigger: "not a cron"})
		require.NoError(t, err)

		err = sched.LoadScheduleJob(ctx, "load-job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second))
		require.Error(t, err)
	})

	t.Run("fails after the scheduler has started", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		sched := scheduler.NewScheduler(memory.New())
		sched.Start(ctx, 1)

		err := sched.LoadScheduleJob(ctx, "load-job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second))
		require.Error(t, err)
	})
}

// editStoredTrigger simulates editing the trigger expression directly in the store,
// without touching the entry shared with the scheduler.
// The memory store has no in-place update, so the task is recreated; callers must make sure
// the scheduler is parked on a timer (synctest.Wait) so it does not observe the empty queue in between.
func editStoredTrigger(t *testing.T, store scheduler.JobStore, slug, expr string) {
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
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, "job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second),
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
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(10 * time.Second)
			err := sched.ScheduleJob(ctx, "job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(firstRun))
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

	t.Run("a resumed one-off job keeps its run time and runs right away if due", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			require.NoError(t, sched.RegisterOneOffJob("ad-hoc", &DummyJob{}))
			sched.Start(ctx, 1)

			due := time.Now().Add(time.Second)
			slug, err := sched.ScheduleOneOffJob(ctx, "ad-hoc", due, []byte("Ad hoc"))
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

			// it runs immediately, and being a one-off job without trigger it is then removed
			synctest.Wait()
			_, err = store.Get(ctx, slug)
			require.ErrorIs(t, err, scheduler.ErrJobNotFound)
		})
	})

	t.Run("suspending a running job lets it finish but does not run it again", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			started := make(chan struct{})
			release := make(chan struct{})
			job := scheduler.JobFn(func(_ context.Context, st *scheduler.StoreTask) (*scheduler.StoreTask, error) {
				close(started)
				<-release
				st.Payload = []byte("done")
				return st, nil
			})

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, "slow", job, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(firstRun))
			require.NoError(t, err)
			sched.Start(ctx, 1)

			<-started
			require.NoError(t, sched.SuspendJob(ctx, "slow"))
			close(release)
			synctest.Wait()

			// the result of the run is kept and the task is rescheduled, but as suspended
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
			sched := scheduler.NewScheduler(store, scheduler.HeartbeatOption(time.Second))

			start := time.Now()
			firstRun := start.Add(time.Second)
			err := sched.ScheduleJob(ctx, "job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second),
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
			assert.Equal(t, "Job", string(st.Payload), "the worker must not run the task it was parked on")

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

		sched := scheduler.NewScheduler(store)
		err := sched.ScheduleJob(ctx, "job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second))
		require.NoError(t, err)
		require.NoError(t, sched.SuspendJob(ctx, "job"))

		// a new scheduler instance (or the same one after a restart) schedules the job again
		sched = scheduler.NewScheduler(store)
		err = sched.ScheduleJob(ctx, "job", &DummyJob{}, trigger.NewSimpleTrigger(time.Second))
		require.NoError(t, err)

		sj, err := sched.GetScheduledJob(ctx, "job")
		require.NoError(t, err)
		assert.True(t, sj.Suspended)
	})

	t.Run("fails on an unknown job", func(t *testing.T) {
		ctx := t.Context()
		sched := scheduler.NewScheduler(memory.New())

		require.ErrorIs(t, sched.SuspendJob(ctx, "unknown"), scheduler.ErrJobNotFound)
		require.ErrorIs(t, sched.ResumeJob(ctx, "unknown"), scheduler.ErrJobNotFound)
	})
}

func TestExecutionLoopStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &failingStore{JobStore: memory.New()}
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

func TestUnregisteredOneOffTaskIsRemoved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		// a one-off task stored by another replica, whose job is not registered here
		err := store.Create(ctx, &scheduler.StoreTask{Slug: "unknown:1", When: time.Now().Add(time.Second)})
		require.NoError(t, err)

		sched := scheduler.NewScheduler(store)
		sched.Start(ctx, 1)
		time.Sleep(1500 * time.Millisecond)

		_, err = store.Get(ctx, "unknown:1")
		require.ErrorIs(t, err, scheduler.ErrJobNotFound)
	})
}

// failingStore fails every NextRun and counts the calls.
type failingStore struct {
	scheduler.JobStore
	mu sync.Mutex
	n  int
}

func (f *failingStore) NextRun(context.Context) (*scheduler.StoreTask, error) {
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
