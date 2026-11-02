package trigger

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrFinished = errors.New("trigger has finished")

const everyPrefix = "@every "

// Trigger is the Triggers interface.
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
	// Next returns the next trigger time.
	Next(prev time.Time) time.Time
	// Expression returns the string representation of the trigger, parsable by Parse.
	// The expression is what gets persisted in the store and later rebuilt into a Trigger.
	// Triggers that cannot be represented as an expression return an empty string; such triggers cannot be persisted.
	Expression() string
}

// Parse builds a Trigger from its string expression.
//
// Expressions in the form "@every <duration>" (e.g. "@every 1h30m") produce a SimpleTrigger.
// Any other expression is parsed as a cron expression and produces a CronTrigger.
func Parse(expr string) (Trigger, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, errors.New("empty trigger expression")
	}

	if after, ok := strings.CutPrefix(expr, everyPrefix); ok {
		interval, err := time.ParseDuration(strings.TrimSpace(after))
		if err != nil {
			return nil, fmt.Errorf("failed to parse trigger expression '%s': %w", expr, err)
		}
		return NewSimpleTrigger(interval), nil
	}

	return NewCronTrigger(expr)
}

// SimpleTrigger implements the scheduler.Trigger interface; uses a time.Duration interval.
type SimpleTrigger struct {
	Interval time.Duration
}

// NewSimpleTrigger returns a new SimpleTrigger.
func NewSimpleTrigger(interval time.Duration) *SimpleTrigger {
	return &SimpleTrigger{interval}
}

// Next returns the next time at which the SimpleTrigger is scheduled to fire.
func (st *SimpleTrigger) Next(prev time.Time) time.Time {
	return prev.Add(st.Interval)
}

// Expression returns the trigger expression in the form "@every <duration>".
func (st *SimpleTrigger) Expression() string {
	return everyPrefix + st.Interval.String()
}
