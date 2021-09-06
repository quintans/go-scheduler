package main

import (
	"context"
	"fmt"

	"github.com/quintans/go-scheduler/scheduler"
)

// PrintJob implements the scheduler.Job interface.
type PrintJob struct {
	slug string
}

func (pj PrintJob) Slug() string {
	return pj.slug
}

func (pj PrintJob) Kind() string {
	return pj.slug
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (pj PrintJob) Execute(_ context.Context, st *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	fmt.Println("Executing " + string(st.Payload))
	return st, nil
}
