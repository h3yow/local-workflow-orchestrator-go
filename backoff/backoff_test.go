package backoff

import (
	"testing"
	"time"
)

func TestDelayDeterministic(t *testing.T) {
	p := Policy{Kind: Exponential, Base: 10 * time.Millisecond, Max: time.Second}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range want {
		got := p.Delay(i)
		if got != w {
			t.Fatalf("exp attempt=%d got=%s want=%s", i, got, w)
		}
		// No jitter: repeated calls must be identical.
		if p.Delay(i) != got {
			t.Fatalf("non-deterministic delay at attempt=%d", i)
		}
	}
}

func TestDelayCap(t *testing.T) {
	p := Policy{Kind: Exponential, Base: time.Second, Max: 250 * time.Millisecond}
	if got := p.Delay(10); got != 250*time.Millisecond {
		t.Fatalf("cap got=%s", got)
	}
}

func TestDelayLinearAndFixed(t *testing.T) {
	fixed := Policy{Kind: Fixed, Base: 5 * time.Millisecond}
	if fixed.Delay(3) != 5*time.Millisecond {
		t.Fatal("fixed wrong")
	}
	lin := Policy{Kind: Linear, Base: 5 * time.Millisecond}
	if got := lin.Delay(2); got != 15*time.Millisecond {
		t.Fatalf("linear got=%s", got)
	}
}
