package scheduler_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
		err = sched.ScheduleOneOffJob(ctx, "ad-hoc:1", now.Add(time.Second*5), []byte("Ad hoc Job"))
		require.NoError(t, err)
		err = sched.ScheduleOneOffJob(ctx, "first:1", now.Add(time.Second*12), []byte("First job"))
		require.NoError(t, err)
		err = sched.ScheduleOneOffJob(ctx, "second:1", now.Add(time.Second*6), []byte("Second job"))
		require.NoError(t, err)
		err = sched.ScheduleOneOffJob(ctx, "third:1", now.Add(time.Second*3), []byte("Third job"))
		require.NoError(t, err)

		slugs, err := sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"ad-hoc:1", "first:1", "second:1", "third:1", "print-cron"}, slugs)

		// the cron job runs at 1s, 4s, 7s, 10s...; observe between runs to avoid racing with an execution
		time.Sleep(time.Millisecond * 9500)

		_, err = sched.GetScheduledJob(ctx, "print-cron")
		require.NoError(t, err)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"first:1", "print-cron"}, slugs)

		st, err := store.Get(ctx, "print-cron")
		require.NoError(t, err)
		assert.Equal(t, "Cron job#3", string(st.Payload))

		err = sched.DeleteJob(ctx, "print-cron")
		require.NoError(t, err)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"first:1"}, slugs)
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
