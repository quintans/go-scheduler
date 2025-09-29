package trigger

import (
	"errors"
	"time"
)

var ErrFinished = errors.New("trigger has finished")

// Trigger is the Triggers interface.
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
	// Next returns the next trigger time.
	Next(prev time.Time) time.Time
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
