package engine

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

// TestRecoveryRunningNotMarkedSucceeded crashes while node "b" is executing:
// after resume "a" must not re-run, "b" must be re-executed (never marked
// successful), and the history must not duplicate earlier events.
func TestRecoveryRunningNotMarkedSucceeded(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	s := runSpec(
		nn("a", 1),
		nn("b", 1, "a"),
		nn("c", 1, "b"),
	)
	logSpec(t, "[RECOVER]", s)

	var aCalls int32
	bStarted := make(chan struct{})
	blockB := make(chan struct{})
	reg := ResolverFunc(func(id string) (Task, bool) {
		switch id {
		case "a":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				atomic.AddInt32(&aCalls, 1)
				return &ExecResult{Value: "a"}, nil
			}, true
		case "b":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				close(bStarted)
				<-blockB
				return &ExecResult{Value: "b"}, nil
			}, true
		case "c":
			return taskOK, true
		}
		return nil, false
	})

	// ---- first process: submit, wait until b is executing, then kill ----
	eng1 := New(st, reg, Config{})
	runID, err := eng1.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	<-bStarted
	time.Sleep(20 * time.Millisecond) // ensure running snapshot hit the disk

	mid, _ := eng1.GetRun(context.Background(), runID)
	t.Logf("[RECOVER] state at interruption: a=%s b=%s c=%s",
		mid.Status.Nodes["a"].State, mid.Status.Nodes["b"].State, mid.Status.Nodes["c"].State)
	if mid.Status.Nodes["b"].State != model.StateRunning {
		t.Fatalf("b should be running at interruption, got %s", mid.Status.Nodes["b"].State)
	}
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Persisted snapshot must still show b as running (never succeeded).
	raw, err := st.LoadStatus(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Nodes["b"].State != model.StateRunning {
		t.Fatalf("durable b state=%s at crash", raw.Nodes["b"].State)
	}
	if raw.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("durable a state=%s at crash", raw.Nodes["a"].State)
	}
	hist1, _ := st.LoadHistory(context.Background(), runID)
	t.Logf("[RECOVER] pre-resume history events=%d", len(hist1.Events))

	// ---- second process: resume from the same store ----
	var bCallsAfter int32
	reg2 := ResolverFunc(func(id string) (Task, bool) {
		switch id {
		case "a":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				atomic.AddInt32(&aCalls, 1)
				return &ExecResult{}, nil
			}, true
		case "b":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				atomic.AddInt32(&bCallsAfter, 1)
				return &ExecResult{Value: "b2"}, nil
			}, true
		case "c":
			return taskOK, true
		}
		return nil, false
	})
	eng2 := New(st, reg2, Config{})
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[RECOVER]", rec)

	if aCalls != 1 {
		t.Fatalf("succeeded node a re-ran: total a calls=%d", aCalls)
	}
	if bCallsAfter != 1 {
		t.Fatalf("interrupted b executed %d times after resume, want 1", bCallsAfter)
	}
	for _, id := range []string{"a", "b", "c"} {
		if rec.Status.Nodes[id].State != model.StateSucceeded {
			t.Fatalf("%s final state=%s", id, rec.Status.Nodes[id].State)
		}
	}
	// b must carry the recovery annotation and must never have been recorded
	// as succeeded before its replay attempt.
	var sawBRecovery bool
	for _, ev := range rec.History.Events {
		if ev.NodeID == "b" && ev.Detail != "" && containsAll(ev.Detail, "replayed") {
			sawBRecovery = true
		}
	}
	if !sawBRecovery {
		t.Fatal("no recovered_running annotation recorded for b")
	}
	// Event Seq numbers must be strictly increasing (no duplicated records).
	prev := int64(-1)
	for _, ev := range rec.History.Events {
		if ev.Seq <= prev {
			t.Fatalf("history seq not strictly increasing: %d after %d", ev.Seq, prev)
		}
		prev = ev.Seq
	}
}

// TestRecoveryRetriesAfterCrash: a node failing before crash keeps its attempt
// count after resume and eventually fails when attempts are exhausted.
func TestRecoveryMidRetry(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	s := runSpec(
		spec.NodeSpec{ID: "a", MaxAttempts: 2, Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 2 * time.Second}},
		nn("b", 1, "a"),
	)
	logSpec(t, "[RECOV2]", s)

	// First process: a fails its first attempt; shut down while waiting retry.
	var calls int32
	eng1 := New(st, ResolverFunc(func(id string) (Task, bool) {
		if id == "a" {
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				atomic.AddInt32(&calls, 1)
				return nil, errors.New("fail before crash")
			}, true
		}
		return taskOK, true
	}), Config{})
	runID, _ := eng1.Submit(context.Background(), s)
	time.Sleep(60 * time.Millisecond)
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 attempt before crash, got %d", calls)
	}

	eng2 := New(st, ResolverFunc(func(id string) (Task, bool) {
		if id == "a" {
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				atomic.AddInt32(&calls, 1)
				return nil, errors.New("fail after crash")
			}, true
		}
		return taskOK, true
	}), Config{})
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[RECOV2]", rec)
	if calls != 2 {
		t.Fatalf("total attempts=%d want 2 (no reset, no extra)", calls)
	}
	a := rec.Status.Nodes["a"]
	if a.State != model.StateFailed || a.FailCode != model.FailAttemptsExhausted {
		t.Fatalf("a=%s/%s", a.State, a.FailCode)
	}
	if rec.Status.Nodes["b"].State != model.StateSkipped {
		t.Fatalf("b=%s want skipped", rec.Status.Nodes["b"].State)
	}
}

func containsAll(s string, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
