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
		sched := scheduler.NewStdScheduler(store)

		printJob := &DummyJob{kind: "print-once"}
		sched.RegisterJob(printJob)

		cronTrigger, _ := trigger.NewCronTrigger("1/3 * * * * *")
		cronJob := &DummyJob{kind: "print-cron"}
		err := sched.RegisterJob(cronJob, scheduler.WithTrigger(cronTrigger))
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sched.Start(ctx)

		sched.ScheduleJob(ctx, "ad-hoc", printJob, scheduler.WithDelay(time.Second*5), scheduler.WithPayload([]byte("Ad hoc Job")))
		sched.ScheduleJob(ctx, "first", printJob, scheduler.WithDelay(time.Second*12), scheduler.WithPayload([]byte("First job")))
		sched.ScheduleJob(ctx, "second", printJob, scheduler.WithDelay(time.Second*6), scheduler.WithPayload([]byte("Second job")))
		sched.ScheduleJob(ctx, "third", printJob, scheduler.WithDelay(time.Second*3), scheduler.WithPayload([]byte("Third job")))
		delay, err := cronTrigger.FirstDelay()
		require.NoError(t, err)
		sched.ScheduleJob(ctx, "print-cron", cronJob, scheduler.WithDelay(delay), scheduler.WithPayload([]byte("Cron job")))

		slugs, err := sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"ad-hoc", "first", "second", "third", "print-cron"}, slugs)

		time.Sleep(time.Second * 10)

		_, err = sched.GetScheduledJob(ctx, "print-cron")
		require.NoError(t, err)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"first", "print-cron"}, slugs)

		st, err := store.Get(ctx, "print-cron")
		require.NoError(t, err)
		assert.Equal(t, "Cron job#3", string(st.Payload))

		err = sched.DeleteJob(ctx, "print-cron")
		require.NoError(t, err)

		slugs, err = sched.GetJobSlugs(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"first"}, slugs)
	})
}

// DummyJob implements the scheduler.Job interface.
type DummyJob struct {
	kind string
}

func (pj DummyJob) Kind() string {
	return pj.kind
}

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
