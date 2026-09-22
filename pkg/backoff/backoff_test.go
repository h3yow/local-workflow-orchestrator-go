package backoff

import (
	"testing"
	"time"
)

func TestDelays(t *testing.T) {
	cases := []struct {
		p   Policy
		seq []time.Duration
	}{
		{Policy{Kind: KindNone}, []time.Duration{0, 0, 0}},
		{Policy{Kind: KindFixed, Base: 10 * time.Millisecond}, []time.Duration{10, 10, 10}},
		{Policy{Kind: KindLinear, Base: 10 * time.Millisecond}, []time.Duration{10, 20, 30}},
		{Policy{Kind: KindExponent, Base: 10 * time.Millisecond, Factor: 2}, []time.Duration{10, 20, 40}},
		{Policy{Kind: KindExponent, Base: 10 * time.Millisecond, Factor: 2, MaxDelay: 15 * time.Millisecond}, []time.Duration{10, 15, 15}},
	}
	for _, c := range cases {
		for i, want := range c.seq {
			if got := c.p.Delay(i); got != want*time.Millisecond {
				t.Fatalf("%s attempt %d = %s want %s", c.p.Kind, i, got, want)
			}
		}
	}
}

func TestValidate(t *testing.T) {
	if err := (Policy{Kind: KindExponent, Factor: 0.5}).Validate(); err == nil {
		t.Fatal("expected factor validation error")
	}
	if err := (Policy{Kind: KindFixed, Base: -1}).Validate(); err == nil {
		t.Fatal("expected negative base error")
	}
	if err := (Policy{Kind: "weird"}).Validate(); err == nil {
		t.Fatal("expected unknown kind error")
	}
	if err := (Policy{Kind: KindLinear, Base: time.Millisecond}).Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
}
