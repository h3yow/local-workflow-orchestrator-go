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

// assertCleanTerminal verifies the three terminal states never borrow each
// other's markers: success has no failure/recovery/skip marks; failed/skipped
// carry exactly their own reason.
func assertCleanTerminal(t *testing.T, n *model.NodeStatus) {
	t.Helper()
	switch n.State {
	case model.StateSucceeded:
		if n.FailCode != model.FailNone {
			t.Errorf("succeeded %s carries fail code %s", n.ID, n.FailCode)
		}
		if n.FailMessage != "" {
			t.Errorf("succeeded %s carries fail message %q", n.ID, n.FailMessage)
		}
		if n.SkippedBecause != "" {
			t.Errorf("succeeded %s carries skip reason %q", n.ID, n.SkippedBecause)
		}
	case model.StateFailed:
		if n.FailCode == model.FailNone {
			t.Errorf("failed %s has no fail code", n.ID)
		}
		if n.SkippedBecause != "" {
			t.Errorf("failed %s carries skip reason %q", n.ID, n.SkippedBecause)
		}
	case model.StateSkipped:
		if n.SkippedBecause == "" {
			t.Errorf("skipped %s has no blocking node", n.ID)
		}
		if n.FailCode != model.FailUpstreamFailed {
			t.Errorf("skipped %s code=%s want upstream_failed", n.ID, n.FailCode)
		}
	default:
		t.Errorf("node %s non-terminal: %s", n.ID, n.State)
	}
}

// TestSuccessCleanAfterInterruptionAndRetries: node fails twice, is killed
// mid-execution (recovered_running annotation), then succeeds. Its terminal
// state must be a clean success with zero failure markers, while the failed
// attempts and recovery evidence remain observable in Attempts/History.
func TestSuccessCleanAfterInterruptionAndRetries(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 5,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 5 * time.Millisecond}})
	logSpec(t, "[TERM-CLEAN]", s)

	var calls int32
	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		n := atomic.AddInt32(&calls, 1)
		switch {
		case n == 1:
			return nil, errors.New("flaky")
		case n == 2:
			<-ctx.Done() // killed mid-attempt in the second process
			return nil, ctx.Err()
		default:
			return &ExecResult{Value: "ok"}, nil
		}
	}

	eng1 := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{})
	runID, _ := eng1.Submit(context.Background(), s)
	// Allow first failure then the second attempt to be running.
	time.Sleep(40 * time.Millisecond)
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	eng2 := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{})
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[TERM-CLEAN]", rec)

	n := rec.Status.Nodes["a"]
	assertCleanTerminal(t, n)
	if n.State != model.StateSucceeded {
		t.Fatalf("state=%s want succeeded", n.State)
	}
	// Attempt counts started attempts (the killed one included); attempt
	// numbering survived the restart (history shows attempts 1..3).
	if n.Attempt != 3 {
		t.Fatalf("started attempts=%d want 3", n.Attempt)
	}
	// Intermediate evidence survives: the recovery annotation is in history.
	var sawRecovery, sawFailure bool
	for _, ev := range rec.History.Events {
		if ev.NodeID == "a" && ev.Code == model.FailRecoveredRunning {
			sawRecovery = true
		}
		if ev.NodeID == "a" && ev.To == model.StateReady {
			sawFailure = true
		}
	}
	if !sawRecovery {
		t.Error("recovered_running annotation missing from history")
	}
	if !sawFailure {
		t.Error("retry-park transition missing from history")
	}
	// The completed failed attempt and the final success are recorded; the
	// attempt killed mid-execution has no result, but its replay is evidenced
	// by the counter jump and the recovered_running event above.
	if len(n.Attempts) < 2 {
		t.Errorf("attempt records=%d want >=2 (completed failure + success)", len(n.Attempts))
	}
	var sawFailedAttempt bool
	for _, a := range n.Attempts {
		if a.Err != "" {
			sawFailedAttempt = true
		}
	}
	if !sawFailedAttempt {
		t.Error("failed attempt detail must remain observable in Attempts")
	}
}

// TestTerminalClassesMutuallyExclusive: failed, skipped and succeeded nodes in
// one run all carry only their own classification markers.
func TestTerminalClassesMutuallyExclusive(t *testing.T) {
	s := runSpec(
		nn("ok", 1),
		nn("bad", 1, "ok"),
		nn("down", 1, "bad"),
	)
	logSpec(t, "[TERM-EXCL]", s)
	reg := MapRegistry{
		"ok": taskOK,
		"bad": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			return nil, errors.New("nope")
		},
		"down": taskOK,
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[TERM-EXCL]", rec)
	assertCleanTerminal(t, rec.Status.Nodes["ok"])
	assertCleanTerminal(t, rec.Status.Nodes["bad"])
	assertCleanTerminal(t, rec.Status.Nodes["down"])
	if rec.Status.State != model.RunFailed {
		t.Fatalf("run=%s want failed", rec.Status.State)
	}
}

// TestTerminalClassificationStableUnderConcurrentReads: many concurrent
// GetRun readers must always observe (a) a self-consistent snapshot where every
// node is terminal with clean markers, and (b) a history replay classification
// identical to the snapshot classification.
func TestTerminalClassificationStableUnderConcurrentReads(t *testing.T) {
	s := runSpec(
		nn("a", 1),
		spec.NodeSpec{ID: "b", MaxAttempts: 3,
			Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: time.Millisecond}},
		nn("c", 1, "a"),
		nn("d", 1, "b", "c"),
	)
	logSpec(t, "[TERM-RACE]", s)

	var calls int32
	reg := ResolverFunc(func(id string) (Task, bool) {
		switch id {
		case "b":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				if atomic.AddInt32(&calls, 1) < 3 {
					return nil, errors.New("retry me")
				}
				return &ExecResult{}, nil
			}, true
		default:
			return taskOK, true
		}
	})

	st := store.NewMemoryStore()
	eng := New(st, reg, Config{})
	defer eng.Shutdown(context.Background())
	runID, _ := eng.Submit(context.Background(), s)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var readerErr atomic.Value
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec, err := eng.GetRun(context.Background(), runID)
				if err != nil {
					readerErr.Store(err)
					return
				}
				// During the run non-terminal nodes are allowed; but whenever a
				// node IS terminal its markers must already be self-consistent.
				for _, id := range rec.Status.NodeOrder {
					n := rec.Status.Nodes[id]
					if n.State.IsTerminal() {
						switch n.State {
						case model.StateSucceeded:
							if n.FailCode != model.FailNone || n.SkippedBecause != "" {
								readerErr.Store(errors.New("dirty succeeded node observed: " + id))
								return
							}
						case model.StateFailed:
							if n.FailCode == model.FailNone || n.SkippedBecause != "" {
								readerErr.Store(errors.New("dirty failed node observed: " + id))
								return
							}
						case model.StateSkipped:
							if n.FailCode != model.FailUpstreamFailed || n.SkippedBecause == "" {
								readerErr.Store(errors.New("dirty skipped node observed: " + id))
								return
							}
						}
					}
				}
				// Any terminal node must agree with history replay classification.
				replayed := model.ClassifyTerminal(rec.History)
				for _, id := range rec.Status.NodeOrder {
					n := rec.Status.Nodes[id]
					if !n.State.IsTerminal() {
						continue
					}
					rc, ok := replayed[id]
					if !ok {
						continue // terminal transition event not yet flushed
					}
					if rc.State != n.State {
						readerErr.Store(errors.New("replay/snapshot state mismatch for " + id))
						return
					}
					// Failure code only compared for failed/skipped: success is
					// normalised to none in the snapshot while the succeeded
					// event also carries none, so they match as well.
					if rc.Code != n.FailCode {
						readerErr.Store(errors.New("replay/snapshot code mismatch for " + id))
						return
					}
				}
			}
		}()
	}

	rec, err := eng.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if v := readerErr.Load(); v != nil {
		t.Fatal(v)
	}
	logTransitions(t, "[TERM-RACE]", rec)

	// Final cross-check: snapshot classification == history replay for all.
	replayed := model.ClassifyTerminal(rec.History)
	for _, id := range rec.Status.NodeOrder {
		n := rec.Status.Nodes[id]
		assertCleanTerminal(t, n)
		rc := replayed[id]
		if rc.State != n.State || rc.Code != n.FailCode {
			t.Fatalf("%s replay %s/%s != snapshot %s/%s",
				id, rc.State, rc.Code, n.State, n.FailCode)
		}
	}
}

// TestClassifyTerminalIgnoresIntermediateTransitions is a focused unit check
// that recovery/retry annotations can never change terminal classification.
func TestClassifyTerminalIgnoresIntermediateTransitions(t *testing.T) {
	h := &model.History{RunID: "r", Events: []model.SnapshotEvent{
		{Seq: 1, NodeID: "x", To: model.StateRunning, Code: model.FailNone},
		{Seq: 2, NodeID: "x", To: model.StatePending, Code: model.FailRecoveredRunning},
		{Seq: 3, NodeID: "x", To: model.StateReady, Code: model.FailNone},
		{Seq: 4, NodeID: "x", To: model.StateSucceeded, Code: model.FailNone},
	}}
	cls := model.ClassifyTerminal(h)
	if cls["x"] != (model.TerminalClass{State: model.StateSucceeded, Code: model.FailNone}) {
		t.Fatalf("classification=%v", cls["x"])
	}
}
