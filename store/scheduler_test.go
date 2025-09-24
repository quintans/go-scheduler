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
	sched := scheduler.NewStdScheduler(
		store,
		scheduler.StdSchedulerHeartbeatOption(time.Second),
	)

	backoff := trigger.NewExponentialBackoff(trigger.StdSchedulerIncBackoffOption(100 * time.Millisecond))

	shellJob := &DummyJob{kind: "sh"}
	goodCurlJob := &DummyJob{kind: "curl-good", err: nil}
	badCurlJob := &DummyJob{kind: "curl-bad", err: errors.New("curl error")}

	// for repeating jobs, we provide a trigger
	err := sched.RegisterJob(shellJob, scheduler.WithTrigger(trigger.NewSimpleTrigger(time.Millisecond*700)))
	require.NoError(t, err)
	err = sched.RegisterJob(badCurlJob, scheduler.WithTrigger(trigger.NewSimpleTrigger(time.Millisecond*800)), scheduler.WithBackoff(backoff))
	require.NoError(t, err)
	// to make a job run only once, we don't provide a trigger option
	err = sched.RegisterJob(goodCurlJob)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	// call start after registering all jobs
	sched.Start(ctx)
	// dynamic jobs, receive execution parameters though the payload argument
	err = sched.ScheduleJob(ctx, "sh-good", shellJob, scheduler.WithDelay(time.Millisecond*700), scheduler.WithPayload([]byte("OK")))
	require.NoError(t, err)
	err = sched.ScheduleJob(ctx, "sh-bad", shellJob, scheduler.WithDelay(time.Millisecond), scheduler.WithPayload([]byte("BAD")))
	require.NoError(t, err)

	// static jobs, that execute always the same task, usually don't provide payload
	err = sched.ScheduleJob(ctx, "curl-good", goodCurlJob, scheduler.WithDelay(time.Millisecond))
	require.NoError(t, err)
	err = sched.ScheduleJob(ctx, "curl-bad", badCurlJob, scheduler.WithDelay(time.Millisecond*800))
	require.NoError(t, err)

	time.Sleep(4 * time.Second)
	scheduledJobKeys, err := sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"sh-bad", "curl-bad", "sh-good"}, scheduledJobKeys)

	_, err = sched.GetScheduledJob(ctx, "sh-good")
	require.NoError(t, err)
	st, err := store.Get(ctx, "sh-good")
	require.NoError(t, err)
	require.True(t, st.IsOK())

	err = sched.DeleteJob(ctx, "sh-good")
	require.NoError(t, err)
	_, err = store.Get(ctx, "sh-good")
	require.True(t, errors.Is(err, scheduler.ErrJobNotFound))

	scheduledJobKeys, err = sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"curl-bad", "sh-bad"}, scheduledJobKeys)

	cancel()
	ctx = context.Background()

	st, err = store.Get(ctx, "sh-bad")
	require.NoError(t, err)
	require.False(t, st.IsOK())

	st, err = store.Get(ctx, "curl-bad")
	require.NoError(t, err)
	require.False(t, st.IsOK())
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
