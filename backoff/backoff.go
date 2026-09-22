// Package backoff provides deterministic retry backoff policies.
//
// No jitter is ever applied: identical (policy, attempt index) inputs always
// yield identical delays, which is required for reproducible scheduling.
package backoff

import "time"

// Kind selects the backoff shape.
type Kind string

const (
	Fixed       Kind = "fixed"
	Linear      Kind = "linear"
	Exponential Kind = "exponential"
)

// Policy computes the delay before the given retry.
//
// Attempt is the zero-based retry index (0 = delay before the first retry,
// i.e. after the first failed attempt).
type Policy struct {
	Kind   Kind
	Base   time.Duration
	Max    time.Duration // 0 means uncapped
	Factor float64       // multiplier for linear/exponential (defaults: 1 / 2)
}

// DefaultPolicy is used when nodes/engine specify none: fixed 10ms, capped 1s.
func DefaultPolicy() Policy {
	return Policy{Kind: Fixed, Base: 10 * time.Millisecond, Max: time.Second}
}

// Normalize fills defaults and never mutates the receiver.
func (p Policy) Normalize() Policy {
	if p.Kind == "" {
		p.Kind = Fixed
	}
	switch p.Kind {
	case Linear:
		if p.Factor <= 0 {
			p.Factor = 1
		}
	case Exponential:
		if p.Factor <= 0 {
			p.Factor = 2
		}
	}
	return p
}

// Delay returns the (deterministic) wait before retry number `attempt`.
// Negative attempt indices are treated as 0.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	q := p.Normalize()
	var d time.Duration
	switch q.Kind {
	case Fixed:
		d = q.Base
	case Linear:
		d = time.Duration(float64(q.Base) * q.Factor * float64(attempt+1))
	case Exponential:
		d = q.Base
		for i := 0; i < attempt; i++ {
			d = time.Duration(float64(d) * q.Factor)
		}
	default:
		d = q.Base
	}
	if q.Max > 0 && d > q.Max {
		d = q.Max
	}
	if d < 0 {
		d = 0
	}
	return d
}
