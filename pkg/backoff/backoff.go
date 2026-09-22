// Package backoff defines retry backoff policies.
package backoff

import (
	"errors"
	"math"
	"time"
)

// Kind enumerates supported backoff strategies.
type Kind string

const (
	// KindNone means no delay between retries.
	KindNone Kind = "none"
	// KindFixed always waits Base.
	KindFixed Kind = "fixed"
	// KindLinear waits Base * (attempt+1).
	KindLinear Kind = "linear"
	// KindExponent waits Base * Factor^attempt.
	KindExponent Kind = "exponential"
)

// Policy describes how delays between retries are computed.
// The zero value is a valid no-wait policy.
type Policy struct {
	Kind       Kind
	Base       time.Duration
	Factor     float64
	MaxDelay   time.Duration
	JitterSeed int64 // reserved: deterministic engines do not apply random jitter
}

// Delay returns the wait before attempt number attempt (0-based: attempt 0 is
// the wait before the first retry). Delays are deterministic for identical
// policies so repeated runs schedule identically.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	var d time.Duration
	switch p.Kind {
	case "", KindNone:
		d = 0
	case KindFixed:
		d = p.Base
	case KindLinear:
		d = p.Base * time.Duration(attempt+1)
	case KindExponent:
		d = time.Duration(float64(p.Base) * math.Pow(p.Factor, float64(attempt)))
	default:
		d = 0
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	if d < 0 {
		d = 0
	}
	return d
}

// Validate checks the policy fields and returns a plain error describing the
// first illegal value.
func (p Policy) Validate() error {
	if p.Base < 0 {
		return errors.New("backoff base must be >= 0")
	}
	if p.MaxDelay < 0 {
		return errors.New("backoff max delay must be >= 0")
	}
	switch p.Kind {
	case "", KindNone, KindFixed:
		return nil
	case KindLinear:
		if p.Factor != 0 && p.Factor < 1 {
			return errors.New("linear backoff factor must be >= 1 when set")
		}
		return nil
	case KindExponent:
		if p.Factor < 1 {
			return errors.New("exponential backoff factor must be >= 1")
		}
		return nil
	default:
		return errors.New("unknown backoff kind: " + string(p.Kind))
	}
}
