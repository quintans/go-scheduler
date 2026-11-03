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

func testScheduler(t *testing.T, store scheduler.Store) {
	sched := scheduler.NewScheduler(
		store,
		scheduler.HeartbeatOption(time.Second),
	)

	backoff := trigger.NewExponentialBackoff(trigger.StdSchedulerIncBackoffOption(100 * time.Millisecond))

	goodExecuter := &DummyExecuter{name: "good", err: nil}
	badExecuter := &DummyExecuter{name: "bad", err: errors.New("curl error")}

	ctx := t.Context()
	// registering is only needed to set options; ScheduleJob registers the executers it is given
	err := sched.RegisterExecuter(badExecuter, scheduler.WithBackoff(backoff))
	require.NoError(t, err)

	// for recurring jobs we provide a trigger. We usually don't provide payload
	err = sched.ScheduleJob(ctx, goodExecuter, trigger.NewSimpleTrigger(time.Millisecond*700), scheduler.WithSlug("good-job"))
	require.NoError(t, err)
	err = sched.ScheduleJob(ctx, badExecuter, trigger.NewSimpleTrigger(time.Millisecond*800), scheduler.WithSlug("bad-job"))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx, 1)

	// one-off runs, receiving execution parameters though the payload
	now := time.Now()
	_, err = sched.ScheduleOnce(ctx, "good", scheduler.WithWhen(now.Add(time.Millisecond*700)), scheduler.WithPayload([]byte("OK")))
	require.NoError(t, err)
	badJobOOSlug, err := sched.ScheduleOnce(ctx, "bad", scheduler.WithWhen(now.Add(time.Millisecond)), scheduler.WithPayload([]byte("BAD")))
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
	// the executer and the trigger expression must survive the store round trip
	require.Equal(t, "good", st.Executer)
	require.Equal(t, "every: 700ms", st.Trigger)

	err = sched.DeleteJob(ctx, "good-job")
	require.NoError(t, err)
	_, err = store.Get(ctx, "good-job")
	require.True(t, errors.Is(err, scheduler.ErrTaskNotFound))

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
	require.Equal(t, "bad", st.Executer)
}

// testStoreSuspend exercises the Suspend/Resume operations of the store directly.
func testStoreSuspend(t *testing.T, store scheduler.Store) {
	ctx := t.Context()
	require.NoError(t, store.Clear(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)
	err := store.Create(ctx, &scheduler.Task{Slug: "paused", Executer: "exec", When: now.Add(-time.Second), Trigger: "every: 1s"})
	require.NoError(t, err)
	err = store.Create(ctx, &scheduler.Task{Slug: "later", Executer: "exec", When: now.Add(time.Hour)})
	require.NoError(t, err)

	// the earliest job is the next to run
	next, err := store.NextRun(ctx, []string{"exec"})
	require.NoError(t, err)
	require.Equal(t, "paused", next.Slug)
	require.Equal(t, "exec", next.Executer)

	require.NoError(t, store.Suspend(ctx, "paused"))
	require.NoError(t, store.Suspend(ctx, "paused"), "suspending twice is fine")

	st, err := store.Get(ctx, "paused")
	require.NoError(t, err)
	require.True(t, st.Suspended)
	require.Equal(t, "every: 1s", st.Trigger)

	// a suspended job is skipped, even if it is due
	next, err = store.NextRun(ctx, []string{"exec"})
	require.NoError(t, err)
	require.Equal(t, "later", next.Slug)
	require.False(t, next.Suspended)

	// and cannot be locked
	_, err = store.Lock(ctx, st)
	require.ErrorIs(t, err, scheduler.ErrTaskNotLocked)

	// when it is the only job, there is nothing to run
	require.NoError(t, store.Delete(ctx, "later"))
	_, err = store.NextRun(ctx, []string{"exec"})
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)

	when := now.Add(time.Minute)
	require.NoError(t, store.Resume(ctx, "paused", when))

	next, err = store.NextRun(ctx, []string{"exec"})
	require.NoError(t, err)
	require.Equal(t, "paused", next.Slug)
	require.Equal(t, when, next.When)

	st, err = store.Get(ctx, "paused")
	require.NoError(t, err)
	require.False(t, st.Suspended)
	require.Equal(t, when, st.When)
	require.Equal(t, "every: 1s", st.Trigger)

	// a locked (running) job can be suspended; the lock is kept
	locked, err := store.Lock(ctx, next)
	require.NoError(t, err)
	require.Equal(t, "exec", locked.Executer)
	require.NoError(t, store.Suspend(ctx, "paused"))
	_, err = store.NextRun(ctx, []string{"exec"})
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	// once rescheduled at the end of the run, the job keeps the run result but stays suspended
	locked.Payload = []byte("ran")
	locked.When = when.Add(time.Minute)
	require.NoError(t, store.Reschedule(ctx, locked))
	st, err = store.Get(ctx, "paused")
	require.NoError(t, err)
	require.True(t, st.Suspended)
	require.Equal(t, "ran", string(st.Payload))
	require.Equal(t, "exec", st.Executer)
	require.Equal(t, when.Add(time.Minute), st.When)
	_, err = store.NextRun(ctx, []string{"exec"})
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)

	require.ErrorIs(t, store.Suspend(ctx, "unknown"), scheduler.ErrTaskNotFound)
	require.ErrorIs(t, store.Resume(ctx, "unknown", when), scheduler.ErrTaskNotFound)

	require.NoError(t, store.Clear(ctx))
}

// testStoreUpdateTrigger exercises the UpdateTrigger operation of the store directly.
func testStoreUpdateTrigger(t *testing.T, store scheduler.Store) {
	ctx := t.Context()
	require.NoError(t, store.Clear(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)
	err := store.Create(ctx, &scheduler.Task{Slug: "first", Executer: "exec", When: now.Add(time.Minute), Trigger: "every: 1m", Payload: []byte("p")})
	require.NoError(t, err)
	err = store.Create(ctx, &scheduler.Task{Slug: "second", Executer: "exec", When: now.Add(time.Hour), Trigger: "every: 1h"})
	require.NoError(t, err)

	// moving the run time changes the order of the queue
	when := now.Add(time.Second)
	require.NoError(t, store.UpdateTrigger(ctx, "second", "every: 1s", when))

	next, err := store.NextRun(ctx, []string{"exec"})
	require.NoError(t, err)
	require.Equal(t, "second", next.Slug)
	require.Equal(t, when, next.When)

	st, err := store.Get(ctx, "second")
	require.NoError(t, err)
	require.Equal(t, "every: 1s", st.Trigger)
	require.Equal(t, when, st.When)
	require.False(t, st.Suspended)

	// the other fields are left untouched
	require.NoError(t, store.UpdateTrigger(ctx, "first", "every: 2m", now.Add(2*time.Minute)))
	st, err = store.Get(ctx, "first")
	require.NoError(t, err)
	require.Equal(t, "every: 2m", st.Trigger)
	require.Equal(t, now.Add(2*time.Minute), st.When)
	require.Equal(t, "p", string(st.Payload))

	// a suspended task keeps its suspension
	require.NoError(t, store.Suspend(ctx, "first"))
	require.NoError(t, store.UpdateTrigger(ctx, "first", "every: 3m", now.Add(3*time.Minute)))
	st, err = store.Get(ctx, "first")
	require.NoError(t, err)
	require.True(t, st.Suspended)
	require.Equal(t, "every: 3m", st.Trigger)
	next, err = store.NextRun(ctx, []string{"exec"})
	require.NoError(t, err)
	require.Equal(t, "second", next.Slug)

	// a locked (running) task can have its trigger changed; the lock is kept and the change survives the reschedule
	locked, err := store.Lock(ctx, next)
	require.NoError(t, err)
	require.NoError(t, store.UpdateTrigger(ctx, "second", "every: 5s", when.Add(5*time.Second)))
	_, err = store.NextRun(ctx, []string{"exec"})
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	locked.Payload = []byte("ran")
	locked.When = when.Add(time.Second)
	require.NoError(t, store.Reschedule(ctx, locked))
	st, err = store.Get(ctx, "second")
	require.NoError(t, err)
	require.Equal(t, "every: 5s", st.Trigger)
	require.Equal(t, "ran", string(st.Payload))
	require.Equal(t, when.Add(time.Second), st.When)

	require.ErrorIs(t, store.UpdateTrigger(ctx, "unknown", "every: 1s", when), scheduler.ErrTaskNotFound)

	require.NoError(t, store.Clear(ctx))
}

// DummyExecuter implements the scheduler.Executer interface.
type DummyExecuter struct {
	name string
	err  error
}

func (pj DummyExecuter) Name() string {
	return pj.name
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Executer.
func (pj DummyExecuter) Execute(_ context.Context, st *scheduler.Task) (*scheduler.Task, error) {
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

func testStoreNextRunByExecuter(t *testing.T, store scheduler.Store) {
	ctx := t.Context()
	require.NoError(t, store.Clear(ctx))

	now := time.Now().UTC().Truncate(time.Millisecond)
	// the earliest task belongs to an executer this instance does not know (e.g. added by a newer deployment)
	err := store.Create(ctx, &scheduler.Task{Slug: "new:1", Executer: "new", When: now.Add(-time.Minute)})
	require.NoError(t, err)
	err = store.Create(ctx, &scheduler.Task{Slug: "old:1", Executer: "old", When: now.Add(-time.Second)})
	require.NoError(t, err)
	err = store.Create(ctx, &scheduler.Task{Slug: "other:1", Executer: "other", When: now.Add(time.Hour)})
	require.NoError(t, err)

	// only the tasks of the given executers are returned, the earliest first
	next, err := store.NextRun(ctx, []string{"old", "other"})
	require.NoError(t, err)
	require.Equal(t, "old:1", next.Slug)
	require.Equal(t, "old", next.Executer)

	next, err = store.NextRun(ctx, []string{"other"})
	require.NoError(t, err)
	require.Equal(t, "other:1", next.Slug)

	next, err = store.NextRun(ctx, []string{"new", "old", "other"})
	require.NoError(t, err)
	require.Equal(t, "new:1", next.Slug)

	// the task of an unknown executer is skipped, not removed
	_, err = store.NextRun(ctx, []string{"none"})
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	_, err = store.NextRun(ctx, nil)
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	_, err = store.NextRun(ctx, []string{})
	require.ErrorIs(t, err, scheduler.ErrTaskNotFound)
	st, err := store.Get(ctx, "new:1")
	require.NoError(t, err)
	require.Equal(t, "new", st.Executer)

	// a locked task is skipped in favour of the next one of the wanted executers
	locked, err := store.Lock(ctx, next)
	require.NoError(t, err)
	next, err = store.NextRun(ctx, []string{"new", "old"})
	require.NoError(t, err)
	require.Equal(t, "old:1", next.Slug)
	require.NoError(t, store.Reschedule(ctx, locked))
}
