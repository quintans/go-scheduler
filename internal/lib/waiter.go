package lib

import (
	"sync"
)

// Waiter is a small synchronization primitive that supports:
//   - Wait(): clear busy and return a channel that will be closed by the next Poke().
//   - Poke(): if a waiter exists and lock is not busy, release (close) the waiter channel.
type Waiter struct {
	mu     sync.Mutex
	waiter chan struct{} // non-nil when there is an active waiter to be signalled
}

// NewWaiter creates an initially not-busy Waiter.
func NewWaiter() *Waiter {
	return &Waiter{}
}

// Wait clears the "busy" state and returns a receive-only channel that will be closed
// by the next Poke() (if any). If Wait is called multiple times before a Poke,
// the same channel is returned (only the first subsequent Poke will close it).
func (p *Waiter) Wait() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	// create a waiter channel if none exists yet
	if p.waiter == nil {
		p.waiter = make(chan struct{})
	}

	return p.waiter
}

// Poke signals a waiter if one exists and the lock is not busy.
// If there is no waiter or the lock is busy, Poke does nothing.
func (p *Waiter) Poke() {
	p.mu.Lock()
	// if busy or no waiter, do nothing
	if p.waiter == nil {
		p.mu.Unlock()
		return
	}
	// consume the waiter and close channel
	close(p.waiter)
	// mark as busy
	p.waiter = nil
	p.mu.Unlock()
}
