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
		printJob := &DummyExecuter{}
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

	printJob := &DummyExecuter{}
	err := sched1.ScheduleJob(t.Context(), "print", printJob, trigger.NewSimpleTrigger(time.Millisecond*500))
	require.NoError(t, err)
	err = sched2.ScheduleJob(t.Context(), "print", printJob, trigger.NewSimpleTrigger(time.Millisecond*500))
	require.NoError(t, err)
}

// DummyExecuter implements the scheduler.Executer interface.
type DummyExecuter struct{}

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

func TestScheduleJob(t *testing.T) {
	t.Run("uses the stored trigger instead of the default", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			// simulate a job previously persisted (possibly edited) in the store
			firstRun := time.Now().Add(time.Second)
			err := store.Create(ctx, &scheduler.Task{
				Slug:    "load-job",
				Payload: []byte("Loaded"),
				When:    firstRun,
				Trigger: "@every 2s",
			})
			require.NoError(t, err)

			err = sched.ScheduleJob(ctx, "load-job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour))
			require.NoError(t, err)

			// the stored job is kept untouched
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
			err := sched.ScheduleJob(ctx, "load-job", &DummyExecuter{}, trigger.NewSimpleTrigger(2*time.Second), scheduler.WithWhen(firstRun))
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

	t.Run("falls back to the default trigger when the job is not stored", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := scheduler.NewScheduler(store)

		cronTrigger, err := trigger.NewCronTrigger("0 0 * * * *")
		require.NoError(t, err)
		when := time.Now().Add(time.Hour)
		err = sched.ScheduleJob(ctx, "load-job", &DummyExecuter{}, cronTrigger, scheduler.WithWhen(when))
		require.NoError(t, err)

		st, err := store.Get(ctx, "load-job")
		require.NoError(t, err)
		assert.Equal(t, "0 0 * * * *", st.Trigger)
		assert.Equal(t, when, st.When)
	})

	t.Run("falls back to the default trigger when the stored job has no trigger", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := store.Create(ctx, &scheduler.Task{Slug: "load-job", When: firstRun})
			require.NoError(t, err)

			err = sched.ScheduleJob(ctx, "load-job", &DummyExecuter{}, trigger.NewSimpleTrigger(3*time.Second))
			require.NoError(t, err)

			// the default is persisted, keeping the stored run time
			st, err := store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, "@every 3s", st.Trigger)
			assert.Equal(t, firstRun, st.When)

			sched.Start(ctx, 1)
			time.Sleep(1500 * time.Millisecond)

			st, err = store.Get(ctx, "load-job")
			require.NoError(t, err)
			assert.Equal(t, firstRun.Add(3*time.Second), st.When)
			assert.Equal(t, "@every 3s", st.Trigger)
		})
	})

	t.Run("fails on a slug reserved for one-off tasks", func(t *testing.T) {
		ctx := t.Context()
		sched := scheduler.NewScheduler(memory.New())

		err := sched.ScheduleJob(ctx, "job:1", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second))
		require.ErrorIs(t, err, scheduler.ErrInvalidJobSlug)
	})

	t.Run("fails on an invalid stored trigger", func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		sched := scheduler.NewScheduler(store)

		err := store.Create(ctx, &scheduler.Task{Slug: "load-job", When: time.Now(), Trigger: "not a cron"})
		require.NoError(t, err)

		err = sched.ScheduleJob(ctx, "load-job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second))
		require.Error(t, err)
	})

	t.Run("fails after the scheduler has started", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		sched := scheduler.NewScheduler(memory.New())
		sched.Start(ctx, 1)

		err := sched.ScheduleJob(ctx, "load-job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second))
		require.Error(t, err)
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
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second),
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
			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(firstRun))
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

			require.NoError(t, sched.RegisterOneOffJob("ad-hoc", &DummyExecuter{}))
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
			executer := scheduler.ExecuterFn(func(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
				close(started)
				<-release
				st.Payload = []byte("done")
				return st, nil
			})

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, "slow", executer, trigger.NewSimpleTrigger(time.Second), scheduler.WithWhen(firstRun))
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
			sched := scheduler.NewScheduler(store, scheduler.HeartbeatOption(time.Second))

			start := time.Now()
			firstRun := start.Add(time.Second)
			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second),
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

		sched := scheduler.NewScheduler(store)
		err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second))
		require.NoError(t, err)
		require.NoError(t, sched.SuspendJob(ctx, "job"))

		// a new scheduler instance (or the same one after a restart) schedules the job again
		sched = scheduler.NewScheduler(store)
		err = sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Second))
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
			sched := scheduler.NewScheduler(store)

			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour),
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
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "@every 1s", st.Trigger)

			// the parked worker was woken up and runs the job at the new time
			time.Sleep(1500 * time.Millisecond)
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, changedAt.Add(2*time.Second), st.When)
			assert.Equal(t, "@every 1s", st.Trigger)

			// a cron trigger works the same way
			cron, err := trigger.NewCronTrigger("0 0 * * * *")
			require.NoError(t, err)
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", cron))
			st, err = store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "0 0 * * * *", st.Trigger)
			assert.Equal(t, cron.Next(time.Now()), st.When)
		})
	})

	t.Run("a suspended job keeps its suspension and uses the new trigger when resumed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			synctest.Wait()

			require.NoError(t, sched.SuspendJob(ctx, "job"))
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(time.Second)))

			time.Sleep(5 * time.Second)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.True(t, st.Suspended)
			assert.Equal(t, "@every 1s", st.Trigger)
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
			sched := scheduler.NewScheduler(store, scheduler.HeartbeatOption(time.Second))

			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			sched.Start(ctx, 1)
			synctest.Wait()

			// e.g. an admin tool sharing the store, where the job is not registered
			admin := scheduler.NewScheduler(store)
			require.NoError(t, admin.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(2*time.Second)))
			changedAt := time.Now()

			time.Sleep(2500 * time.Millisecond)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, changedAt.Add(4*time.Second), st.When)
			assert.Equal(t, "@every 2s", st.Trigger)
		})
	})

	t.Run("the change survives a restart", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()

			sched := scheduler.NewScheduler(store)
			err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour), scheduler.WithPayload([]byte("Job")))
			require.NoError(t, err)
			require.NoError(t, sched.ChangeJobTrigger(ctx, "job", trigger.NewSimpleTrigger(time.Second)))
			changedAt := time.Now()

			// a new scheduler instance (or the same one after a restart) loads the job again with the same default
			sched = scheduler.NewScheduler(store)
			err = sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour))
			require.NoError(t, err)
			sched.Start(ctx, 1)

			time.Sleep(1500 * time.Millisecond)
			st, err := store.Get(ctx, "job")
			require.NoError(t, err)
			assert.Equal(t, "Job#1", string(st.Payload))
			assert.Equal(t, "@every 1s", st.Trigger)
			assert.Equal(t, changedAt.Add(2*time.Second), st.When)
		})
	})

	t.Run("a run in progress is rescheduled with the previous trigger, the new one applies afterwards", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)

			firstRun := time.Now().Add(time.Second)
			err := sched.ScheduleJob(ctx, "job", &slowExecuter{duration: 2 * time.Second}, trigger.NewSimpleTrigger(10*time.Second),
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
			assert.Equal(t, "@every 3s", st.Trigger)
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
		sched := scheduler.NewScheduler(store)

		err := sched.ScheduleJob(ctx, "job", &DummyExecuter{}, trigger.NewSimpleTrigger(time.Hour))
		require.NoError(t, err)

		require.NoError(t, sched.RegisterOneOffJob("oo-job", &DummyExecuter{}))
		sched.Start(ctx, 1)
		ooSlug, err := sched.ScheduleOneOffJob(ctx, "oo-job", time.Now().Add(time.Hour), nil)
		require.NoError(t, err)

		second := trigger.NewSimpleTrigger(time.Second)
		// unknown job
		require.ErrorIs(t, sched.ChangeJobTrigger(ctx, "unknown", second), scheduler.ErrTaskNotFound)
		// one-off jobs never recur
		require.ErrorIs(t, sched.ChangeJobTrigger(ctx, ooSlug, second), scheduler.ErrOneOffJob)
		// a trigger without expression cannot be persisted
		require.ErrorIs(t, sched.ChangeJobTrigger(ctx, "job", opaqueTrigger{}), scheduler.ErrTriggerNotPersistent)
		require.Error(t, sched.ChangeJobTrigger(ctx, "job", nil))

		// nothing was changed
		st, err := store.Get(ctx, "job")
		require.NoError(t, err)
		assert.Equal(t, "@every 1h0m0s", st.Trigger)
		st, err = store.Get(ctx, ooSlug)
		require.NoError(t, err)
		assert.Empty(t, st.Trigger)
	})

	t.Run("a one-off task does not recur even with a trigger stored for it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx := t.Context()
			store := memory.New()
			sched := scheduler.NewScheduler(store)
			require.NoError(t, sched.RegisterOneOffJob("oo-job", &DummyExecuter{}))

			// simulate an edit made directly in the store
			err := store.Create(ctx, &scheduler.Task{Slug: "oo-job:1", When: time.Now().Add(time.Second), Trigger: "@every 1s"})
			require.NoError(t, err)
			sched.Start(ctx, 1)

			time.Sleep(1500 * time.Millisecond)
			_, err = store.Get(ctx, "oo-job:1")
			require.ErrorIs(t, err, scheduler.ErrTaskNotFound, "the task must be removed after its single run")
		})
	})
}

// opaqueTrigger is a trigger without a string expression.
type opaqueTrigger struct{}

func (opaqueTrigger) Next(prev time.Time) time.Time { return prev.Add(time.Second) }

// slowExecuter behaves like DummyExecuter but takes duration to run.
type slowExecuter struct {
	duration time.Duration
}

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

func TestUnregisteredOneOffJobIsRemoved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		store := memory.New()
		// a one-off job stored by another replica, whose job is not registered here
		err := store.Create(ctx, &scheduler.Task{Slug: "unknown:1", When: time.Now().Add(time.Second)})
		require.NoError(t, err)

		sched := scheduler.NewScheduler(store)
		sched.Start(ctx, 1)
		time.Sleep(1500 * time.Millisecond)

		_, err = store.Get(ctx, "unknown:1")
		require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	})
}

// failingStore fails every NextRun and counts the calls.
type failingStore struct {
	scheduler.Store
	mu sync.Mutex
	n  int
}

func (f *failingStore) NextRun(context.Context) (*scheduler.Task, error) {
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
