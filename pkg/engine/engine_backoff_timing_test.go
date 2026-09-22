package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// TestRetryWaitsForBackoffWithFakeClock proves the second attempt does not
// start until the backoff interval elapses, using a manually advanced clock.
func TestRetryWaitsForBackoffWithFakeClock(t *testing.T) {
	clk := newFakeClock()
	s := runSpec(specNode("a", 3, backoff.Policy{Kind: backoff.KindFixed, Base: 50 * time.Millisecond}))
	logSpec(t, "[FAKECLK]", s)
	var calls int32
	reg := StaticResolver(func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return nil, errors.New("not ready")
		}
		return &ExecResult{}, nil
	})
	eng := New(newMemStore(), reg, Config{}).WithClock(clk)
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)

	// Pump: advance clock in small steps while polling attempt count.
	attemptSeen := func(want int32) bool {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if atomic.LoadInt32(&calls) >= want {
				return true
			}
			clk.Advance(5 * time.Millisecond)
			time.Sleep(time.Millisecond)
		}
		return false
	}
	if !attemptSeen(1) {
		t.Fatal("first attempt never started")
	}
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("second attempt started early: calls=%d", got)
	}
	if !attemptSeen(2) {
		t.Fatal("retry after backoff never started")
	}
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("third attempt started before second backoff: calls=%d", got)
	}
	if !attemptSeen(3) {
		t.Fatal("final attempt never ran")
	}
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[FAKECLK]", rec)
	if rec.Status.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("state=%s", rec.Status.Nodes["a"].State)
	}
}
