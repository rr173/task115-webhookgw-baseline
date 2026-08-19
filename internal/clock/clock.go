// Package clock abstracts time so business logic can be exercised with a
// deterministic fake clock in tests and smoke checks.
package clock

import "time"

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// RealClock uses the system wall clock.
type RealClock struct{}

// Now implements Clock.
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock is a manually advanced clock used in tests.
type FakeClock struct {
	t time.Time
}

// NewFakeClock builds a FakeClock positioned at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{t: t}
}

// Now implements Clock.
func (c *FakeClock) Now() time.Time { return c.t }

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// Set positions the clock at t.
func (c *FakeClock) Set(t time.Time) { c.t = t }
