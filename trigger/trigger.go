package trigger

import (
	"errors"
	"time"
)

var ErrFinished = errors.New("trigger has finished")

// Trigger is the Triggers interface.
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {
	// NextFireTime returns the next time at which the Trigger is scheduled to fire.
	NextFireTime(prev time.Time) (time.Time, error)
}

// SimpleTrigger implements the scheduler.Trigger interface; uses a time.Duration interval.
type SimpleTrigger struct {
	Interval time.Duration
}

// NewSimpleTrigger returns a new SimpleTrigger.
func NewSimpleTrigger(interval time.Duration) *SimpleTrigger {
	return &SimpleTrigger{interval}
}

// NextFireTime returns the next time at which the SimpleTrigger is scheduled to fire.
func (st *SimpleTrigger) NextFireTime(prev time.Time) (time.Time, error) {
	return prev.Add(st.Interval), nil
}
