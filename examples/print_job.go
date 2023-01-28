package main

import (
	"context"
	"fmt"

	"github.com/quintans/go-scheduler/scheduler"
)

// PrintJob implements the scheduler.Job interface.
type PrintJob struct {
	kind string
}

func (pj PrintJob) Kind() string {
	return pj.kind
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (pj PrintJob) Execute(_ context.Context, st *scheduler.StoreTask) (*scheduler.StoreTask, error) {
	fmt.Println("PrintJob: executing " + string(st.Payload))
	return st, nil
}
