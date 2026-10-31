package store_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quintans/go-scheduler/scheduler"
	"github.com/quintans/go-scheduler/trigger"
	"github.com/stretchr/testify/require"
)

func testScheduler(t *testing.T, store scheduler.JobStore) {
	sched := scheduler.NewScheduler(
		store,
		scheduler.HeartbeatOption(time.Second),
	)

	backoff := trigger.NewExponentialBackoff(trigger.StdSchedulerIncBackoffOption(100 * time.Millisecond))

	goodJob := &DummyJob{kind: "curl-good", err: nil}
	badJob := &DummyJob{kind: "curl-bad", err: errors.New("curl error")}

	ctx := t.Context()
	// for static (repeating) jobs,  we provide a trigger. We usually don't provide payload
	err := sched.ScheduleJob(ctx, "good-job", goodJob, trigger.NewSimpleTrigger(time.Millisecond*700))
	require.NoError(t, err)
	err = sched.ScheduleJob(ctx, "bad-job", badJob, trigger.NewSimpleTrigger(time.Millisecond*800), scheduler.WithBackoff(backoff))
	require.NoError(t, err)
	// one off
	err = sched.RegisterOneOffJob("good-job-oo", goodJob)
	require.NoError(t, err)
	err = sched.RegisterOneOffJob("bad-job-oo", badJob)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	// call start after registering all jobs
	sched.Start(ctx, 1)

	// dynamic jobs, receive execution parameters though the payload argument
	now := time.Now()
	_, err = sched.ScheduleOneOffJob(ctx, "good-job-oo", now.Add(time.Millisecond*700), []byte("OK"))
	require.NoError(t, err)
	badJobOOSlug, err := sched.ScheduleOneOffJob(ctx, "bad-job-oo", now.Add(time.Millisecond), []byte("BAD"))
	require.NoError(t, err)

	time.Sleep(4 * time.Second)
	scheduledJobKeys, err := sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bad-job", "good-job", badJobOOSlug}, scheduledJobKeys)

	_, err = sched.GetScheduledJob(ctx, "good-job")
	require.NoError(t, err)
	st, err := store.Get(ctx, "good-job")
	require.NoError(t, err)
	require.True(t, st.IsOK())
	// the trigger expression must survive the store round trip
	require.Equal(t, "@every 700ms", st.Trigger)

	err = sched.DeleteJob(ctx, "good-job")
	require.NoError(t, err)
	_, err = store.Get(ctx, "good-job")
	require.True(t, errors.Is(err, scheduler.ErrJobNotFound))

	scheduledJobKeys, err = sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bad-job", badJobOOSlug}, scheduledJobKeys)

	cancel()
	ctx = context.Background()

	st, err = store.Get(ctx, "bad-job")
	require.NoError(t, err)
	require.False(t, st.IsOK())

	st, err = store.Get(ctx, badJobOOSlug)
	require.NoError(t, err)
	require.False(t, st.IsOK())
}

// testStoreSuspend exercises the Suspend/Resume operations of the store directly.
func testStoreSuspend(t *testing.T, store scheduler.JobStore) {
	ctx := t.Context()
	require.NoError(t, store.Clear(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)
	err := store.Create(ctx, &scheduler.StoreTask{Slug: "paused", When: now.Add(-time.Second), Trigger: "@every 1s"})
	require.NoError(t, err)
	err = store.Create(ctx, &scheduler.StoreTask{Slug: "later", When: now.Add(time.Hour)})
	require.NoError(t, err)

	// the earliest task is the next to run
	next, err := store.NextRun(ctx)
	require.NoError(t, err)
	require.Equal(t, "paused", next.Slug)

	require.NoError(t, store.Suspend(ctx, "paused"))
	require.NoError(t, store.Suspend(ctx, "paused"), "suspending twice is fine")

	st, err := store.Get(ctx, "paused")
	require.NoError(t, err)
	require.True(t, st.Suspended)
	require.Equal(t, "@every 1s", st.Trigger)

	// a suspended task is skipped, even if it is due
	next, err = store.NextRun(ctx)
	require.NoError(t, err)
	require.Equal(t, "later", next.Slug)
	require.False(t, next.Suspended)

	// and cannot be locked
	_, err = store.Lock(ctx, st)
	require.ErrorIs(t, err, scheduler.ErrJobNotLocked)

	// when it is the only task, there is nothing to run
	require.NoError(t, store.Delete(ctx, "later"))
	_, err = store.NextRun(ctx)
	require.ErrorIs(t, err, scheduler.ErrJobNotFound)

	when := now.Add(time.Minute)
	require.NoError(t, store.Resume(ctx, "paused", when))

	next, err = store.NextRun(ctx)
	require.NoError(t, err)
	require.Equal(t, "paused", next.Slug)
	require.Equal(t, when, next.When)

	st, err = store.Get(ctx, "paused")
	require.NoError(t, err)
	require.False(t, st.Suspended)
	require.Equal(t, when, st.When)
	require.Equal(t, "@every 1s", st.Trigger)

	// a locked (running) task can be suspended; the lock is kept
	locked, err := store.Lock(ctx, next)
	require.NoError(t, err)
	require.NoError(t, store.Suspend(ctx, "paused"))
	_, err = store.NextRun(ctx)
	require.ErrorIs(t, err, scheduler.ErrJobNotFound)
	// once rescheduled at the end of the run, the task keeps the run result but stays suspended
	locked.Payload = []byte("ran")
	locked.When = when.Add(time.Minute)
	require.NoError(t, store.Reschedule(ctx, locked))
	st, err = store.Get(ctx, "paused")
	require.NoError(t, err)
	require.True(t, st.Suspended)
	require.Equal(t, "ran", string(st.Payload))
	require.Equal(t, when.Add(time.Minute), st.When)
	_, err = store.NextRun(ctx)
	require.ErrorIs(t, err, scheduler.ErrJobNotFound)

	require.ErrorIs(t, store.Suspend(ctx, "unknown"), scheduler.ErrJobNotFound)
	require.ErrorIs(t, store.Resume(ctx, "unknown", when), scheduler.ErrJobNotFound)

	require.NoError(t, store.Clear(ctx))
}

// DummyJob implements the scheduler.Job interface.
type DummyJob struct {
	kind string
	err  error
}

func (pj DummyJob) Kind() string {
	return pj.kind
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (pj DummyJob) Execute(_ context.Context, st *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	if pj.err != nil {
		return nil, pj.err
	}
	if string(st.Payload) == "BAD" {
		return nil, errors.New("bad command")
	}

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
