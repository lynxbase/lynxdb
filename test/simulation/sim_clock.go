// Package simulation provides a deterministic cluster simulation framework
// for property-based and integration testing. It wraps existing LynxDB
// interfaces (ClockProvider, ObjectStore) with fault injection and
// controllable time.
package simulation

import (
	"sync"
	"time"
)

// SimClock implements cluster.ClockProvider with deterministic, manually
// advanced time. All goroutines sharing a SimClock see the same time,
// enabling reproducible tests without real wall-clock delays.
type SimClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewSimClock creates a SimClock set to t.
func NewSimClock(t time.Time) *SimClock {
	return &SimClock{now: t}
}

// Now returns the current simulated time.
func (c *SimClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.now
}

// Advance moves the clock forward by d. Panics if d is negative.
func (c *SimClock) Advance(d time.Duration) {
	if d < 0 {
		panic("SimClock.Advance: negative duration")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

// Set sets the clock to an absolute time.
func (c *SimClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = t
}
