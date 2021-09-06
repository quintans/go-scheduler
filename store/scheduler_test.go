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
	sched := scheduler.NewStdScheduler(store, scheduler.StdSchedulerMinBackoffOption(100*time.Millisecond))

	shellJob := scheduler.NewShellJob("sh-good", "ls -la")
	shellJob.Description()

	curlJob, err := scheduler.NewCurlJob("curl-good", http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	require.NoError(t, err)
	curlJob.Description()

	errShellJob := scheduler.NewShellJob("sh-bad", "ls -z")

	errCurlJob, err := scheduler.NewCurlJob("curl-bad", http.MethodGet, "http://", "", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	sched.ScheduleJob(ctx, shellJob, trigger.NewSimpleTrigger(time.Millisecond*700))
	sched.ScheduleJob(ctx, curlJob, trigger.NewRunOnceTrigger(time.Millisecond))
	sched.ScheduleJob(ctx, errShellJob, trigger.NewRunOnceTrigger(time.Millisecond))
	sched.ScheduleJob(ctx, errCurlJob, trigger.NewSimpleTrigger(time.Millisecond*800))
	sched.Start(ctx)

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
