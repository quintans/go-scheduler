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
		scheduler.StdSchedulerIncBackoffOption(100*time.Millisecond),
		scheduler.StdSchedulerHeartbeatOption(time.Second),
	)

	shellJob := scheduler.NewShellJob("sh-good")

	curlJob, err := scheduler.NewCurlJob("curl-good", http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	require.NoError(t, err)

	errShellJob := scheduler.NewShellJob("sh-bad")

	errCurlJob, err := scheduler.NewCurlJob("curl-bad", http.MethodGet, "http://", "", nil)
	require.NoError(t, err)

	sched.RegisterJob(shellJob, trigger.NewSimpleTrigger(time.Millisecond*700))
	sched.RegisterJob(curlJob, nil)     // since it run only once no trigger is needed
	sched.RegisterJob(errShellJob, nil) // since it run only once no trigger is needed
	sched.RegisterJob(errCurlJob, trigger.NewSimpleTrigger(time.Millisecond*800))

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	sched.ScheduleJob(ctx, shellJob, []byte("ls -la"), time.Millisecond*700)
	sched.ScheduleJob(ctx, curlJob, nil, time.Millisecond)
	sched.ScheduleJob(ctx, errShellJob, []byte("ls -z"), time.Millisecond)
	sched.ScheduleJob(ctx, errCurlJob, nil, time.Millisecond*800)

	time.Sleep(4 * time.Second)
	scheduledJobKeys, err := sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"sh-bad", "curl-bad", "sh-good"}, scheduledJobKeys)

	_, err = sched.GetScheduledJob(ctx, shellJob.Slug())
	require.NoError(t, err)
	st, err := store.Get(ctx, shellJob.Slug())
	require.NoError(t, err)
	require.True(t, st.IsOK())

	err = sched.DeleteJob(ctx, shellJob.Slug())
	require.NoError(t, err)
	_, err = store.Get(ctx, shellJob.Slug())
	require.True(t, errors.Is(err, scheduler.ErrJobNotFound))

	scheduledJobKeys, err = sched.GetJobSlugs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"curl-bad", "sh-bad"}, scheduledJobKeys)

	cancel()
	ctx = context.Background()

	st, err = store.Get(ctx, errShellJob.Slug())
	require.NoError(t, err)
	require.False(t, st.IsOK())

	st, err = store.Get(ctx, errCurlJob.Slug())
	require.NoError(t, err)
	require.False(t, st.IsOK())
}
