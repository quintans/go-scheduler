package trigger

import "time"

// Backoff is the Backoff interface to calculate the next fire time after a failed execution
type Backoff interface {
	// NextRetryTime returns the next time at which the retry should happen.
	NextRetryTime(prev time.Time, retry int) (time.Time, error)
}

type ExponentialBackoff struct {
	incBackoff time.Duration
	maxBackoff time.Duration
}

func NewExponentialBackoff(options ...ExponentialBackoffOption) ExponentialBackoff {
	b := ExponentialBackoff{
		incBackoff: 10 * time.Second,
		maxBackoff: 10 * time.Minute,
	}
	for _, o := range options {
		o(&b)
	}

	return b
}

func (b ExponentialBackoff) NextRetryTime(prev time.Time, retry int) (time.Time, error) {
	factor := int64(1)
	var backoff int64
	for i := 1; i <= retry; i++ {
		backoff = factor * int64(b.incBackoff)
		if backoff > b.maxBackoff.Nanoseconds() {
			backoff = b.maxBackoff.Nanoseconds()
			break
		}
		factor = factor * 2
	}
	delay := time.Duration(backoff)
	return time.Now().Add(delay), nil
}

type ExponentialBackoffOption func(*ExponentialBackoff)

func StdSchedulerIncBackoffOption(backoff time.Duration) ExponentialBackoffOption {
	return func(s *ExponentialBackoff) {
		s.incBackoff = backoff
	}
}

func StdSchedulerMaxBackoffOption(backoff time.Duration) ExponentialBackoffOption {
	return func(s *ExponentialBackoff) {
		s.maxBackoff = backoff
	}
}

type FixedBackoff struct {
	retries []time.Duration
}

func (b FixedBackoff) NextRetryTime(prev time.Time, retry int) (time.Time, error) {
	if retry > 0 && retry <= len(b.retries) {
		return prev.Add(b.retries[retry-1]), nil
	}
	return time.Time{}, ErrFinished
}
