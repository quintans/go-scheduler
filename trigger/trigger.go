package trigger

import (
	"errors"
	"fmt"
	"time"
)

var ErrExpired = errors.New("trigger has expired")

// Trigger is the Triggers interface.
// Triggers are the 'mechanism' by which Jobs are scheduled.
type Trigger interface {

	// NextFireTime returns the next time at which the Trigger is scheduled to fire.
	NextFireTime(prev time.Time) (time.Time, error)

	// Description returns a Trigger description.
	Description() string
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

// Description returns a SimpleTrigger description.
func (st *SimpleTrigger) Description() string {
	return fmt.Sprintf("SimpleTrigger with the interval %d.", st.Interval)
}

// RunOnceTrigger implements the scheduler.Trigger interface. Could be triggered only once.
type RunOnceTrigger struct {
	Delay   time.Duration
	expired bool
}

// NewRunOnceTrigger returns a new RunOnceTrigger.
func NewRunOnceTrigger(delay time.Duration) *RunOnceTrigger {
	return &RunOnceTrigger{delay, false}
}

// NextFireTime returns the next time at which the RunOnceTrigger is scheduled to fire.
// Sets expired to true afterwards.
func (st *RunOnceTrigger) NextFireTime(prev time.Time) (time.Time, error) {
	if !st.expired {
		next := prev.Add(st.Delay)
		st.expired = true
		return next, nil
	}

	return time.Time{}, ErrExpired
}

// Description returns a RunOnceTrigger description.
func (st *RunOnceTrigger) Description() string {
	status := "valid"
	if st.expired {
		status = "expired"
	}

	return fmt.Sprintf("RunOnceTrigger (%s).", status)
}
