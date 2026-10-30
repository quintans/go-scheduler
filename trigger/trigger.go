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
}

// Expressioner is implemented by triggers that can be represented as a string expression.
// The expression can be persisted and later rebuilt into a Trigger with Parse.
type Expressioner interface {
	// Expression returns the string representation of the trigger, parsable by Parse.
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

	if strings.HasPrefix(expr, everyPrefix) {
		interval, err := time.ParseDuration(strings.TrimSpace(strings.TrimPrefix(expr, everyPrefix)))
		if err != nil {
			return nil, fmt.Errorf("failed to parse trigger expression '%s': %w", expr, err)
		}
		return NewSimpleTrigger(interval), nil
	}

	return NewCronTrigger(expr)
}

// Expression returns the string expression of the trigger if it implements Expressioner,
// otherwise it returns an empty string.
func Expression(trg Trigger) string {
	if e, ok := trg.(Expressioner); ok {
		return e.Expression()
	}
	return ""
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
