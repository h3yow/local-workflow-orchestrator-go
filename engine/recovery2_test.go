package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
	"github.com/h3yow/local-workflow-orchestrator-go/store"
)

// TestRecoveryFromCraftedRunningSnapshot hand-crafts the exact on-disk state
// a hard process kill would leave behind (a succeeded 'a', a running 'b', a
// pending 'c') and asserts Resume continues without re-running 'a' and
// without declaring 'b' successful.
func TestRecoveryFromCraftedRunningSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	runID := "run-crash"
	now := time.Now().UTC().Add(-time.Second)
	crafted := &state.RunState{
		RunID: runID, SpecID: "crash", Status: state.Running, Seq: 3,
		Nodes: map[string]*state.NodeState{
			"a": {
				ID: "a", Slots: 1, Status: state.Succeeded, OrderKey: 1,
				AttemptCount: 1, Result: "a-ok", UpdatedAt: now,
				Attempts: []state.Attempt{{Index: 1, Outcome: state.OutcomeSuccess,
					StartedAt: now, FinishedAt: now}},
			},
			"b": {
				ID: "b", Deps: []string{"a"}, Slots: 1, Status: state.Running,
				OrderKey: 2, AttemptCount: 1, UpdatedAt: now,
			},
			"c": {
				ID: "c", Deps: []string{"b"}, Slots: 1, Status: state.Pending,
				OrderKey: 3, UpdatedAt: now,
			},
		},
	}
	if err := st.SaveSnapshot(ctx, crafted); err != nil {
		t.Fatal(err)
	}
	// Simulate the durable effect 'a' had emitted before the crash.
	if _, err := st.AppendEvent(ctx, runID, store.Event{
		Key: "effect:a:fx-a", Kind: "effect", Detail: "fx-a",
	}); err != nil {
		t.Fatal(err)
	}

	// A brand new engine resumes from the same store.
	st2, _ := store.NewFileStore(dir)
	log := &logBuf{}
	e, err := New(Config{MaxRunning: 1, MaxQueue: 8, Store: st2,
		Backoff: backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond}, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var ranA, ranB, ranC int
	exec := func(ctx context.Context, rid, id string) (string, []string, error) {
		switch id {
		case "a":
			ranA++
		case "b":
			ranB++
		case "c":
			ranC++
		}
		return id, []string{"fx-" + id}, nil
	}
	if err := e.Resume(ctx, runID, exec, nil); err != nil {
		t.Fatal(err)
	}
	rs, err := e.Wait(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RECOVERED: a=%s b=%s c=%s run=%s\n%s",
		rs.Nodes["a"].Status, rs.Nodes["b"].Status, rs.Nodes["c"].Status,
		rs.Status, log.String())
	if ranA != 0 {
		t.Fatalf("succeeded a re-ran %d times", ranA)
	}
	if ranB != 1 || ranC != 1 {
		t.Fatalf("ran b=%d c=%d (want 1 each)", ranB, ranC)
	}
	if rs.Nodes["b"].Attempts[0].Outcome != state.OutcomeInterrupted {
		t.Fatalf("b first attempt outcome=%s want interrupted",
			rs.Nodes["b"].Attempts[0].Outcome)
	}
	if rs.Nodes["b"].Status != state.Succeeded || rs.Nodes["b"].AttemptCount != 2 {
		t.Fatalf("b status=%s attempts=%d", rs.Nodes["b"].Status,
			rs.Nodes["b"].AttemptCount)
	}
	fx, _ := e.Effects(ctx, runID)
	seen := map[string]bool{}
	for _, k := range fx {
		if k != "fx-a" && k != "fx-b" && k != "fx-c" {
			t.Fatalf("unexpected effect %q", k)
		}
		if seen[k] {
			t.Fatalf("duplicate effect %q after recovery", k)
		}
		seen[k] = true
	}
	if len(fx) != 3 {
		t.Fatalf("effects=%v want exactly 3 unique", fx)
	}
}
