package store_test

import (
	"context"
	"errors"
	"net/http"
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

	shellJob := scheduler.NewShellJob()

	curlJob, err := scheduler.NewCurlJob("curl-good", http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	require.NoError(t, err)

	errCurlJob, err := scheduler.NewCurlJob("curl-bad", http.MethodGet, "http://", "", nil)
	require.NoError(t, err)
	// for repeating jobs, we provide a trigger
	sched.RegisterJob(shellJob, scheduler.WithTrigger(trigger.NewSimpleTrigger(time.Millisecond*700)))
	sched.RegisterJob(errCurlJob, scheduler.WithTrigger(trigger.NewSimpleTrigger(time.Millisecond*800)), scheduler.WithBackoff(backoff))
	// to make a job run only once, we don't provide a trigger option
	sched.RegisterJob(curlJob)

	ctx, cancel := context.WithCancel(context.Background())
	// call start after registering all jobs
	sched.Start(ctx)
	// dynamic jobs, receive execution parameters though the payload argument
	sched.ScheduleJob(ctx, "sh-good", shellJob, time.Millisecond*700, scheduler.WithPayload([]byte("ls -la")))
	sched.ScheduleJob(ctx, "sh-bad", shellJob, time.Millisecond, scheduler.WithPayload([]byte("ls -z")))

	// static jobs, that execute always the same task, usually don't provide payload
	sched.ScheduleJob(ctx, "curl-good", curlJob, time.Millisecond)
	sched.ScheduleJob(ctx, "curl-bad", errCurlJob, time.Millisecond*800)

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
