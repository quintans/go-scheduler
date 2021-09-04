package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/quintans/go-scheduler/scheduler"
	"github.com/quintans/go-scheduler/store"
	"github.com/quintans/go-scheduler/trigger"
)

// demo main
func main() {
	wg := new(sync.WaitGroup)
	wg.Add(2)

	go demoJobs(wg)
	go demoScheduler(wg)

	wg.Wait()
}

func demoScheduler(wg *sync.WaitGroup) {
	store := store.NewMemStore()
	var sched scheduler.Scheduler = scheduler.NewStdScheduler(store)
	cronTrigger, _ := trigger.NewCronTrigger("1/3 * * * * *")
	cronJob := PrintJob{"print", "Cron job"}

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)

	sched.ScheduleJob(ctx, &PrintJob{"ad-hoc", "Ad hoc Job"}, trigger.NewRunOnceTrigger(time.Second*5))
	sched.ScheduleJob(ctx, &PrintJob{"first", "First job"}, trigger.NewSimpleTrigger(time.Second*12))
	sched.ScheduleJob(ctx, &PrintJob{"second", "Second job"}, trigger.NewSimpleTrigger(time.Second*6))
	sched.ScheduleJob(ctx, &PrintJob{"third", "Third job"}, trigger.NewSimpleTrigger(time.Second*3))
	sched.ScheduleJob(ctx, &cronJob, cronTrigger)

	time.Sleep(time.Second * 10)

	j, err := sched.GetScheduledJob(ctx, cronJob.Slug())
	mustNoError(err)
	fmt.Println(j.TriggerDescription)
	slugs, err := sched.GetJobSlugs(ctx)
	mustNoError(err)
	fmt.Println("Before delete: ", slugs)
	err = sched.DeleteJob(ctx, cronJob.Slug())
	mustNoError(err)
	slugs, err = sched.GetJobSlugs(ctx)
	mustNoError(err)
	fmt.Println("After delete: ", slugs)

	time.Sleep(time.Second * 2)
	cancel()
	wg.Done()
}

func demoJobs(wg *sync.WaitGroup) {
	store := store.NewMemStore()
	sched := scheduler.NewStdScheduler(store)
	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
	shellJob := scheduler.NewShellJob("list-all", "ls -la")
	curlJob, err := scheduler.NewCurlJob("curl-clock", http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	mustNoError(err)
	err = sched.ScheduleJob(ctx, shellJob, cronTrigger)
	mustNoError(err)
	err = sched.ScheduleJob(ctx, curlJob, trigger.NewSimpleTrigger(time.Second*7))
	mustNoError(err)

	time.Sleep(time.Second * 10)

	slugs, err := sched.GetJobSlugs(ctx)
	mustNoError(err)
	fmt.Println("slugs:", slugs)
	sj, err := store.Get(ctx, shellJob.Slug())
	mustNoError(err)
	fmt.Println("Result:", sj.Result)
	fmt.Println("OK:", sj.IsOK())

	time.Sleep(time.Second * 2)
	cancel()
	wg.Done()
}

func mustNoError(err error) {
	if err != nil {
		panic(err)
	}
}
