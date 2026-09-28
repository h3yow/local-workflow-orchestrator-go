package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

// classify reads the LAST transition of a node from the replayed history and
// returns its terminal classification, exactly as a replay-only observer
// would derive it.
func historyTerminal(rec *model.RunRecord, node string) model.NodeState {
	var st model.NodeState
	for _, ev := range rec.History.Events {
		if ev.NodeID == node {
			st = ev.To
		}
	}
	return st
}

// TestTerminalSuccessCleanAfterRecovery: the hardest case for classification
// cleanliness — a node interrupted mid-attempt (recovered_running annotation)
// that later succeeds must present a bare success: no failure code, no message,
// no skip hint, no parked retry deadline. The intermediate annotation remains
// visible in history, but never in the terminal classification.
func TestTerminalSuccessCleanAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 2,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: time.Millisecond}})
	logSpec(t, "[TERMOK]", s)

	started := make(chan struct{})
	block := make(chan struct{})
	eng1 := New(st, ResolverFunc(func(id string) (Task, bool) {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			close(started)
			<-block
			return &ExecResult{}, nil
		}, true
	}), Config{})
	runID, _ := eng1.Submit(context.Background(), s)
	<-started
	time.Sleep(20 * time.Millisecond)
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	eng2 := New(st, StaticResolver(func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		return &ExecResult{Value: "done"}, nil
	}), Config{})
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[TERMOK]", rec)

	n := rec.Status.Nodes["a"]
	if n.State != model.StateSucceeded {
		t.Fatalf("state=%s", n.State)
	}
	if n.FailCode != model.FailNone {
		t.Fatalf("success carries fail code %s", n.FailCode)
	}
	if n.FailMessage != "" || n.SkippedBecause != "" {
		t.Fatalf("success carries leftovers: msg=%q skippedBecause=%q", n.FailMessage, n.SkippedBecause)
	}
	if n.RetryReadyAt != nil {
		t.Fatalf("success still parked at %v", n.RetryReadyAt)
	}
	// Intermediate recovery information survived in history but the final
	// classification derived from replay is unambiguously succeeded.
	var sawRecovered bool
	for _, ev := range rec.History.Events {
		if ev.NodeID == "a" && ev.Code == model.FailRecoveredRunning {
			sawRecovered = true
		}
	}
	if !sawRecovered {
		t.Fatal("recovery annotation missing from observable history")
	}
	if got := historyTerminal(rec, "a"); got != model.StateSucceeded {
		t.Fatalf("history replay classifies node as %s, want succeeded", got)
	}
}

// TestTerminalClassesMutuallyExclusive covers all three terminal classes in
// one run and asserts each carries exactly its own code set with no bleed.
func TestTerminalClassesMutuallyExclusive(t *testing.T) {
	s := runSpec(
		nn("ok", 1),
		spec.NodeSpec{ID: "bad", MaxAttempts: 1},
		nn("down", 1, "bad"),
		nn("indirect", 1, "down"),
	)
	logSpec(t, "[TERMEX]", s)
	reg := MapRegistry{
		"ok": taskOK,
		"bad": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			return nil, errors.New("boom")
		},
		"down":     taskOK,
		"indirect": taskOK,
	}
	eng := New(newMemStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[TERMEX]", rec)

	if rec.Status.State != model.RunFailed {
		t.Fatalf("run=%s want failed", rec.Status.State)
	}
	ok := rec.Status.Nodes["ok"]
	if ok.State != model.StateSucceeded || ok.FailCode != model.FailNone ||
		ok.FailMessage != "" || ok.SkippedBecause != "" {
		t.Fatalf("succeeded node polluted: %+v", ok)
	}
	bad := rec.Status.Nodes["bad"]
	if bad.State != model.StateFailed || bad.FailCode != model.FailAttemptsExhausted ||
		bad.SkippedBecause != "" || bad.FailMessage == "" {
		t.Fatalf("failed node inconsistent: %+v", bad)
	}
	for _, id := range []string{"down", "indirect"} {
		n := rec.Status.Nodes[id]
		if n.State != model.StateSkipped || n.FailCode != model.FailUpstreamFailed ||
			n.SkippedBecause == "" || len(n.Attempts) != 0 {
			t.Fatalf("skipped node %s inconsistent: %+v", id, n)
		}
		if got := historyTerminal(rec, id); got != model.StateSkipped {
			t.Fatalf("history classifies %s as %s", id, got)
		}
	}
	// Strict mutual exclusion: exactly one of the three terminal predicates.
	for id, n := range rec.Status.Nodes {
		term := 0
		if n.State == model.StateSucceeded {
			term++
		}
		if n.State == model.StateFailed {
			term++
		}
		if n.State == model.StateSkipped {
			term++
		}
		if term != 1 {
			t.Fatalf("node %s belongs to %d terminal classes", id, term)
		}
	}
}

// TestTerminalClassificationConcurrentReaders hammers GetRun while a run with
// retries executes: every read must show state and reason self-consistent
// (a state/code pair that can only be produced together), and all readers
// converge on the same clean terminal classification.
func TestTerminalClassificationConcurrentReaders(t *testing.T) {
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 3,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: time.Millisecond}})
	var calls int32
	reg := StaticResolver(func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		if atomic.AddInt32(&calls, 1) < 3 {
			return nil, errors.New("flaky")
		}
		return &ExecResult{}, nil
	})
	eng := New(newMemStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec, err := eng.GetRun(context.Background(), id)
				if err != nil {
					t.Errorf("GetRun: %v", err)
					return
				}
				n := rec.Status.Nodes["a"]
				if n == nil {
					continue
				}
				switch n.State {
				case model.StateSucceeded:
					if n.FailCode != model.FailNone || n.FailMessage != "" || n.RetryReadyAt != nil {
						t.Errorf("inconsistent success view: %+v", n)
						return
					}
				case model.StateFailed:
					if n.FailCode == model.FailNone {
						t.Errorf("failed without code: %+v", n)
						return
					}
				case model.StateSkipped:
					if n.FailCode != model.FailUpstreamFailed || n.SkippedBecause == "" {
						t.Errorf("inconsistent skip view: %+v", n)
						return
					}
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}
	final, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	n := final.Status.Nodes["a"]
	if n.State != model.StateSucceeded || n.FailCode != model.FailNone || n.FailMessage != "" {
		t.Fatalf("terminal view = %+v", n)
	}
	if got := historyTerminal(final, "a"); got != model.StateSucceeded {
		t.Fatalf("history replay gives %s", got)
	}
}

// TestTerminalReplayStableAcrossRuns runs the same failing+skip graph twice
// with fresh run ids and asserts the terminal classification each node gets
// from replayed history is identical between independent runs.
func TestTerminalReplayStableAcrossRuns(t *testing.T) {
	st := newMemStore()
	s := runSpec(nn("a", 1), nn("b", 1, "a"))
	reg := ResolverFunc(func(id string) (Task, bool) {
		if id == "a" {
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				return nil, errors.New("always")
			}, true
		}
		return taskOK, true
	})
	classify := func() map[string]model.NodeState {
		eng := New(st, reg, Config{})
		id, err := eng.Submit(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := eng.Wait(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		eng.Shutdown(context.Background())
		out := map[string]model.NodeState{}
		for _, nid := range []string{"a", "b"} {
			out[nid] = historyTerminal(rec, nid)
		}
		return out
	}
	first := classify()
	for i := 0; i < 3; i++ {
		got := classify()
		if got["a"] != first["a"] || got["b"] != first["b"] {
			t.Fatalf("replay classification drifted: %v vs %v", got, first)
		}
		if got["a"] != model.StateFailed || got["b"] != model.StateSkipped {
			t.Fatalf("unexpected classification: %v", got)
		}
	}
}

// TestTerminalSuccessCleanAfterQueuedCrash: a node that was admitted (queued)
// when the process died is reset to pending with the recovery annotation; its
// later success must still be a clean terminal success.
func TestTerminalSuccessCleanAfterQueuedCrash(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	// Four independent blockers ("w1".."w4" sort before "z5") occupy every
	// parallel slot; "z5" is dependency-eligible and admitted into the queue,
	// but head-of-line blocking keeps it queued when the process is killed.
	s := runSpec(
		nn("w1", 1), nn("w2", 1), nn("w3", 1), nn("w4", 1), nn("z5", 1),
	)
	logSpec(t, "[TERMQ]", s)

	started := make(chan struct{})
	var startedOnce sync.Once
	block := make(chan struct{})
	eng1 := New(st, ResolverFunc(func(id string) (Task, bool) {
		if id == "z5" {
			return taskOK, true
		}
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			startedOnce.Do(func() { close(started) })
			<-block
			return &ExecResult{}, nil
		}, true
	}), Config{})
	runID, _ := eng1.Submit(context.Background(), s)
	<-started
	// Give the scheduler time to admit "z5" into the bounded queue.
	waitUntil(t, func() bool {
		r, _ := st.LoadStatus(context.Background(), runID)
		return r.Nodes["z5"].State == model.StateQueued
	})
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	eng2 := New(st, StaticResolver(taskOK), Config{})
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"w1", "w2", "w3", "w4", "z5"} {
		n := rec.Status.Nodes[id]
		if n.State != model.StateSucceeded || n.FailCode != model.FailNone ||
			n.FailMessage != "" || n.SkippedBecause != "" || n.RetryReadyAt != nil {
			t.Fatalf("node %s terminal view polluted: %+v", id, n)
		}
		if got := historyTerminal(rec, id); got != model.StateSucceeded {
			t.Fatalf("history classifies %s as %s", id, got)
		}
	}
}
