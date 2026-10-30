package trigger

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type CronTrigger struct {
	expr     string
	schedule cron.Schedule
}

// NewCronTrigger returns a new CronTrigger.
func NewCronTrigger(expr string) (*CronTrigger, error) {
	parser := cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
	schedule, err := parser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cron expression: %w", err)
	}

	return &CronTrigger{expr: expr, schedule: schedule}, nil
}

func (ct *CronTrigger) Next(prev time.Time) time.Time {
	return ct.schedule.Next(prev)
}

// Expression returns the cron expression used to build the trigger.
func (ct *CronTrigger) Expression() string {
	return ct.expr
}
