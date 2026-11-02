package trigger

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrFinished = errors.New("trigger has finished")

const (
	// SimpleName is the name under which SimpleTrigger is registered in the scheduler.
	SimpleName = "every"
	// CronName is the name under which CronTrigger is registered in the scheduler.
	CronName = "cron"
)

// Trigger is the Triggers interface.
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
	// Name identifies the kind of trigger. It selects the Factory used to rebuild the trigger
	// from its Expression, so it must match the name the factory was registered with in the scheduler.
	Name() string
	// Next returns the next trigger time.
	Next(prev time.Time) time.Time
	// Expression returns the string representation of the trigger, parsable by the Factory registered under Name.
	// The expression is what gets persisted in the store and later rebuilt into a Trigger.
	// Triggers that cannot be represented as an expression return an empty string; such triggers cannot be persisted.
	Expression() string
}

// SimpleTrigger implements the scheduler.Trigger interface; uses a time.Duration interval.
type SimpleTrigger struct {
	Interval time.Duration
}

// NewSimpleTrigger returns a new SimpleTrigger.
func NewSimpleTrigger(interval time.Duration) *SimpleTrigger {
	return &SimpleTrigger{interval}
}

// ParseSimpleTrigger is the Factory of SimpleTrigger: it builds one from a duration expression (e.g. "1h30m").
func ParseSimpleTrigger(expr string) (Trigger, error) {
	interval, err := time.ParseDuration(strings.TrimSpace(expr))
	if err != nil {
		return nil, fmt.Errorf("failed to parse simple trigger expression '%s': %w", expr, err)
	}
	return NewSimpleTrigger(interval), nil
}

// Name returns SimpleName.
func (st *SimpleTrigger) Name() string {
	return SimpleName
}

// Next returns the next time at which the SimpleTrigger is scheduled to fire.
func (st *SimpleTrigger) Next(prev time.Time) time.Time {
	return prev.Add(st.Interval)
}

// Expression returns the interval as a duration string (e.g. "1h30m0s").
func (st *SimpleTrigger) Expression() string {
	return st.Interval.String()
}
