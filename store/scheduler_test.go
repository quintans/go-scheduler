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
	err = sched.ScheduleOneOffJob(ctx, "good-job-oo:1", now.Add(time.Millisecond*700), []byte("OK"))
	require.NoError(t, err)
	err = sched.ScheduleOneOffJob(ctx, "bad-job-oo:1", now.Add(time.Millisecond), []byte("BAD"))
	require.NoError(t, err)

	time.Sleep(4 * time.Second)
	scheduledJobKeys, err := sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bad-job", "good-job", "bad-job-oo:1"}, scheduledJobKeys)

	_, err = sched.GetScheduledJob(ctx, "good-job")
	require.NoError(t, err)
	st, err := store.Get(ctx, "good-job")
	require.NoError(t, err)
	require.True(t, st.IsOK())

	err = sched.DeleteJob(ctx, "good-job")
	require.NoError(t, err)
	_, err = store.Get(ctx, "good-job")
	require.True(t, errors.Is(err, scheduler.ErrJobNotFound))

	scheduledJobKeys, err = sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"bad-job", "bad-job-oo:1"}, scheduledJobKeys)

	cancel()
	ctx = context.Background()

	st, err = store.Get(ctx, "bad-job")
	require.NoError(t, err)
	require.False(t, st.IsOK())

	st, err = store.Get(ctx, "bad-job-oo:1")
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
