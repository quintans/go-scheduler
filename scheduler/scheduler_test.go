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

		time.Sleep(time.Second * 10)

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
