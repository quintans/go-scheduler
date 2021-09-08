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

	printJob := &PrintJob{"print-once"}
	sched.RegisterJob(printJob, nil)

	cronTrigger, _ := trigger.NewCronTrigger("1/3 * * * * *")
	cronJob := &PrintJob{"print-cron"}
	sched.RegisterJob(cronJob, cronTrigger)

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)

	sched.ScheduleJob(ctx, "ad-hoc", printJob, []byte("Ad hoc Job"), time.Second*5)
	sched.ScheduleJob(ctx, "first", printJob, []byte("First job"), time.Second*12)
	sched.ScheduleJob(ctx, "second", printJob, []byte("Second job"), time.Second*6)
	sched.ScheduleJob(ctx, "third", printJob, []byte("Third job"), time.Second*3)
	delay, err := cronTrigger.FirstDelay()
	mustNoError(err)
	sched.ScheduleJob(ctx, "cron", cronJob, []byte("Cron job"), delay)

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
	store := store.NewMemStore()
	sched := scheduler.NewStdScheduler(store)

	cronTrigger, _ := trigger.NewCronTrigger("1/5 * * * * *")
	shellJob := scheduler.NewShellJob()
	curlJob, err := scheduler.NewCurlJob("curl-clock", http.MethodGet, "http://worldclockapi.com/api/json/est/now", "", nil)
	mustNoError(err)

	sched.RegisterJob(shellJob, cronTrigger)
	sched.RegisterJob(curlJob, trigger.NewSimpleTrigger(time.Second*7))

	ctx, cancel := context.WithCancel(context.Background())
	sched.Start(ctx)
	delay, err := cronTrigger.FirstDelay()
	mustNoError(err)
	err = sched.ScheduleJob(ctx, "shell-list", shellJob, []byte("ls -la"), delay)
	mustNoError(err)
	err = sched.ScheduleJob(ctx, "curl-clock", curlJob, nil, time.Second*7)
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
