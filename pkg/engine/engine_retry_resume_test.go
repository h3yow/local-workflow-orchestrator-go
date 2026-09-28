package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

// waitForCalls polls while advancing a fake clock until calls reaches want.
func waitForCalls(t *testing.T, clk *fakeClock, calls *int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(calls) >= want {
			return
		}
		clk.Advance(5 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("calls=%d never reached %d", atomic.LoadInt32(calls), want)
}

// TestBackoffResumesRemainingWaitAfterRestart: with a 100ms fixed backoff, the
// process is killed 40ms into the wait. After resume (same clock) the retry
// must fire only after the remaining 60ms: advancing 30ms must not trigger it,
// advancing to 100ms must — never immediately, never after a fresh full wait.
func TestBackoffResumesRemainingWaitAfterRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	clk := newFakeClock()
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 2,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 100 * time.Millisecond}})
	logSpec(t, "[BO-RESUME]", s)

	var calls int32
	failing := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("boom")
	}

	// First process: first attempt fails, deadline parked at now+100ms.
	eng1 := New(st, ResolverFunc(func(string) (Task, bool) { return failing, true }), Config{}).WithClock(clk)
	runID, err := eng1.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, clk, &calls, 1)
	// Let the failure transition + parked snapshot land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec, _ := eng1.GetRun(context.Background(), runID); rec != nil {
			n := rec.Status.Nodes["a"]
			if n.State == model.StateReady && !n.RetryNotBefore.IsZero() {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	clk.Advance(40 * time.Millisecond) // 40ms of the wait elapsed
	time.Sleep(10 * time.Millisecond)
	raw, _ := st.LoadStatus(context.Background(), runID)
	if raw.Nodes["a"].State != model.StateReady {
		t.Fatalf("state at kill=%s want ready", raw.Nodes["a"].State)
	}
	storedDeadline := raw.Nodes["a"].RetryNotBefore
	if storedDeadline.IsZero() {
		t.Fatal("retry deadline was not persisted")
	}
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("attempts before restart=%d want 1", got)
	}

	// Reopen store (new process) with the same clock state.
	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := New(st2, ResolverFunc(func(string) (Task, bool) { return failing, true }), Config{}).WithClock(clk)
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	// Remaining wait: advancing 30ms (70ms total) must NOT retry early.
	clk.Advance(30 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("retry fired early after restart: calls=%d", got)
	}
	// Advancing the remaining 30ms (100ms total) must trigger exactly once.
	waitForCalls(t, clk, &calls, 2)

	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[BO-RESUME]", rec)
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("attempts=%d want 2", got)
	}
	n := rec.Status.Nodes["a"]
	if n.State != model.StateFailed || n.FailCode != model.FailAttemptsExhausted {
		t.Fatalf("a=%s/%s", n.State, n.FailCode)
	}
	if !n.RetryNotBefore.IsZero() {
		t.Fatalf("terminal node must clear deadline, got %v", n.RetryNotBefore)
	}
}

// TestBackoffSurvivesMultipleRestarts: the deadline must stay anchored to the
// original retry instant across several kills inside the wait: total waited
// before retry is exactly the backoff, not backoff per restart.
func TestBackoffSurvivesMultipleRestarts(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	clk := newFakeClock()
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 2,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 90 * time.Millisecond}})
	logSpec(t, "[BO-MULTI]", s)

	var calls int32
	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return nil, errors.New("boom")
		}
		return &ExecResult{}, nil
	}

	var runID string
	restart := func() (*Engine, store.Store) {
		eng := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{}).WithClock(clk)
		if runID == "" {
			var err error
			runID, err = eng.Submit(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
		} else if err := eng.Resume(context.Background(), runID); err != nil {
			t.Fatal(err)
		}
		return eng, st
	}

	eng, _ := restart()
	waitForCalls(t, clk, &calls, 1)
	// Wait until deadline parked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		raw, _ := st.LoadStatus(context.Background(), runID)
		if raw != nil && raw.Nodes["a"].State == model.StateReady {
			break
		}
		time.Sleep(time.Millisecond)
	}
	originalDeadline := func() time.Time {
		raw, _ := st.LoadStatus(context.Background(), runID)
		return raw.Nodes["a"].RetryNotBefore
	}()
	if originalDeadline.IsZero() {
		t.Fatal("deadline not persisted")
	}

	// Kill three times during the wait, advancing 20ms each (60ms of 90ms).
	for i := 0; i < 3; i++ {
		clk.Advance(20 * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		if err := eng.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := atomic.LoadInt32(&calls); got != 1 {
			t.Fatalf("retry early after restart %d: calls=%d", i+1, got)
		}
		raw, _ := st.LoadStatus(context.Background(), runID)
		if !raw.Nodes["a"].RetryNotBefore.Equal(originalDeadline) {
			t.Fatalf("deadline drifted on restart %d: %v != %v",
				i+1, raw.Nodes["a"].RetryNotBefore, originalDeadline)
		}
		eng, _ = restart()
	}

	// 30ms remain: 20ms is still not enough, then the last 10ms triggers.
	clk.Advance(20 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("retry before original deadline: calls=%d", got)
	}
	waitForCalls(t, clk, &calls, 2)
	rec, err := eng.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[BO-MULTI]", rec)
	if rec.Status.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("a=%s", rec.Status.Nodes["a"].State)
	}
	eng.Shutdown(context.Background())
}

// TestBackoffPastDeadlineRetriesImmediately: if the wait was fully elapsed
// while down, resume must retry immediately (remaining wait is zero), not
// wait another full backoff.
func TestBackoffPastDeadlineRetriesImmediately(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	clk := newFakeClock()
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 2,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 100 * time.Millisecond}})
	logSpec(t, "[BO-PAST]", s)

	var calls int32
	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return nil, errors.New("boom")
		}
		return &ExecResult{}, nil
	}
	eng1 := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{}).WithClock(clk)
	runID, _ := eng1.Submit(context.Background(), s)
	waitForCalls(t, clk, &calls, 1)
	time.Sleep(20 * time.Millisecond)
	_ = eng1.Shutdown(context.Background())

	// Entire backoff elapses while the process is down.
	clk.Advance(150 * time.Millisecond)
	st2, _ := store.NewFileStore(dir)
	eng2 := New(st2, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{}).WithClock(clk)
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	// Immediate retry without advancing the clock further.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected immediate retry after elapsed deadline, calls=%d", got)
	}
	rec, _ := eng2.Wait(context.Background(), runID)
	logTransitions(t, "[BO-PAST]", rec)
	if rec.Status.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("a=%s", rec.Status.Nodes["a"].State)
	}
}
