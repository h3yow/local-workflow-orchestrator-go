package engine

import (
	"sync"
	"time"
)

// fakeClock is a manually advanced clock; After returns a channel notified
// when Advance crosses the requested deadline.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []fakeTimer
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := fakeTimer{at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t.ch
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	fired := make([]fakeTimer, 0)
	rest := c.timers[:0]
	for _, t := range c.timers {
		if !t.at.After(c.now) {
			fired = append(fired, t)
		} else {
			rest = append(rest, t)
		}
	}
	c.timers = rest
	c.mu.Unlock()
	for _, t := range fired {
		select {
		case t.ch <- c.now:
		default:
		}
	}
}
