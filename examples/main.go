package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/quintans/go-scheduler/scheduler"
	"github.com/quintans/go-scheduler/store/memory"
	"github.com/quintans/go-scheduler/trigger"
)

// demo main
func main() {
	wg := new(sync.WaitGroup)

	wg.Add(1)
	go demoJobs(wg)

	wg.Add(1)
	go demoScheduler(wg)

	wg.Wait()
}

func demoScheduler(wg *sync.WaitGroup) {
	store := memory.New()
	sched := scheduler.NewStdScheduler(store)

	printJob := &PrintJob{"print-once"}
	sched.RegisterJob(printJob)

	cronTrigger, _ := trigger.NewCronTrigger("1/3 * * * * *")
	cronJob := &PrintJob{"print-cron"}
	err := sched.RegisterJob(cronJob, scheduler.WithTrigger(cronTrigger))
	mustNoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)

	sched.ScheduleJob(ctx, "ad-hoc", printJob, scheduler.WithDelay(time.Second*5), scheduler.WithPayload([]byte("Ad hoc Job")))
	sched.ScheduleJob(ctx, "first", printJob, scheduler.WithDelay(time.Second*12), scheduler.WithPayload([]byte("First job")))
	sched.ScheduleJob(ctx, "second", printJob, scheduler.WithDelay(time.Second*6), scheduler.WithPayload([]byte("Second job")))
	sched.ScheduleJob(ctx, "third", printJob, scheduler.WithDelay(time.Second*3), scheduler.WithPayload([]byte("Third job")))
	delay, err := cronTrigger.FirstDelay()
	mustNoError(err)
	sched.ScheduleJob(ctx, "print-cron", cronJob, scheduler.WithDelay(delay), scheduler.WithPayload([]byte("Cron job")))

	time.Sleep(time.Second * 10)

	_, err = sched.GetScheduledJob(ctx, "print-cron")
	mustNoError(err)
	slugs, err := sched.GetJobSlugs(ctx)
	mustNoError(err)
	fmt.Println("Before delete: ", slugs)
	err = sched.DeleteJob(ctx, "print-cron")
	mustNoError(err)
	slugs, err = sched.GetJobSlugs(ctx)
	mustNoError(err)
	fmt.Println("After delete: ", slugs)

	time.Sleep(time.Second * 2)
	cancel()
	wg.Done()
}

func demoJobs(wg *sync.WaitGroup) {
	store := memory.New()
	sched := scheduler.NewStdScheduler(store)

	cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
	shellJob := scheduler.NewShellJob()
	curlJob, err := scheduler.NewCurlJob("curl-clock", http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	mustNoError(err)

	sched.RegisterJob(shellJob, scheduler.WithTrigger(cronTrigger))
	sched.RegisterJob(curlJob, scheduler.WithTrigger(trigger.NewSimpleTrigger(time.Second*7)))

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	delay, err := cronTrigger.FirstDelay()
	mustNoError(err)
	err = sched.ScheduleJob(ctx, "shell-list", shellJob, scheduler.WithDelay(delay), scheduler.WithPayload([]byte("ls -la")))
	mustNoError(err)
	err = sched.ScheduleJob(ctx, "curl-clock", curlJob, scheduler.WithDelay(time.Second*7))
	mustNoError(err)

	time.Sleep(time.Second * 10)

	slugs, err := sched.GetJobSlugs(ctx)
	mustNoError(err)
	fmt.Println("slugs:", slugs)
	sj, err := store.Get(ctx, "shell-list")
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
