package engine

import "time"

// Clock abstracts time so retry timing is testable and deterministic.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return time.After(0)
	}
	return time.After(d)
}

// SystemClock returns the wall-clock implementation.
func SystemClock() Clock { return systemClock{} }
