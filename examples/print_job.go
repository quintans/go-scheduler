package main

import (
	"context"
	"fmt"
)

// PrintJob implements the scheduler.Job interface.
type PrintJob struct {
	slug string
	desc string
}

// Description returns a PrintJob description.
func (pj PrintJob) Description() string {
	return pj.desc
}

// Key returns a PrintJob unique key.
func (pj PrintJob) Slug() string {
	return pj.slug
}

// Execute Called by the Scheduler when a Trigger fires that is associated with the Job.
func (pj PrintJob) Execute(_ context.Context) (string, error) {
	fmt.Println("Executing " + pj.Description())
	return "", nil
}
