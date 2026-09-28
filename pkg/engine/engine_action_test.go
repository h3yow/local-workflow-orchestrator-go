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

// TestActionUniqueWithinRunAcrossRetry: a task records a business action on
// every attempt; after a failed attempt followed by a successful retry exactly
// one record must survive.
func TestActionUniqueWithinRunAcrossRetry(t *testing.T) {
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 3,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 2 * time.Millisecond}})
	logSpec(t, "[ACT-RETRY]", s)

	var effects int32
	reg := ResolverFunc(func(id string) (Task, bool) {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			inserted, err := in.Actions.Record(ctx, "order-1", "payload")
			if err != nil {
				t.Errorf("record: %v", err)
				return nil, err
			}
			if inserted {
				atomic.AddInt32(&effects, 1)
			}
			if in.Attempt < 1 {
				return nil, errors.New("transient")
			}
			return &ExecResult{Value: "ok"}, nil
		}, true
	})
	st := store.NewMemoryStore()
	eng := New(st, reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[ACT-RETRY]", rec)
	if got := atomic.LoadInt32(&effects); got != 1 {
		t.Fatalf("external effect ran %d times, want 1", got)
	}
	if len(rec.Actions) != 1 || rec.Actions[0].Key != "order-1" {
		t.Fatalf("actions=%v, want single order-1", rec.Actions)
	}
	if rec.Status.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("state=%s", rec.Status.Nodes["a"].State)
	}
}

// TestActionUniqueAcrossCrashWindow: the action is recorded (before the
// external effect), then the process is killed while the attempt is still
// running. After Resume the attempt is replayed, Record returns false, the
// effect is not applied a second time, and the durable action set contains
// exactly one record — observable identically through the engine API and by
// reopening the file store.
func TestActionUniqueAcrossCrashWindow(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := runSpec(nn("a", 3))
	logSpec(t, "[ACT-CRASH]", s)

	var effects int32
	started := make(chan struct{})
	block := make(chan struct{})

	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		inserted, err := in.Actions.Record(ctx, "send-message-7", "hello")
		if err != nil {
			return nil, err
		}
		if inserted {
			atomic.AddInt32(&effects, 1)
		}
		if in.Attempt == 0 {
			close(started)
			<-block // killed here in the first process
		}
		return &ExecResult{Value: "done"}, nil
	}

	eng1 := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{})
	runID, err := eng1.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(block)

	if got := atomic.LoadInt32(&effects); got != 1 {
		t.Fatalf("effect count at crash=%d want 1", got)
	}
	// The record survived the kill even though the attempt did not finish.
	acts, err := st.LoadActions(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(acts) != 1 || acts[0].Key != "send-message-7" || acts[0].NodeID != "a" || acts[0].Attempt != 0 {
		t.Fatalf("durable actions at crash=%v", acts)
	}

	// Restart from a freshly opened store (simulating a new process).
	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := New(st2, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{})
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[ACT-CRASH]", rec)
	if got := atomic.LoadInt32(&effects); got != 1 {
		t.Fatalf("external effect applied %d times total, want 1", got)
	}
	if len(rec.Actions) != 1 || rec.Actions[0].Key != "send-message-7" {
		t.Fatalf("actions after resume=%v, want one record", rec.Actions)
	}
	if rec.Actions[0].Attempt != 0 {
		t.Fatalf("record must describe the first committer attempt, got %d", rec.Actions[0].Attempt)
	}
	if rec.Status.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("a=%s", rec.Status.Nodes["a"].State)
	}
	// History replay and direct store reads agree on the identical set.
	again, err := eng2.ListActions(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].Key != rec.Actions[0].Key {
		t.Fatalf("ListActions mismatch: %v vs %v", again, rec.Actions)
	}
}

// TestActionUniqueAcrossMultipleRestarts: two kills mid-attempt still leave a
// single effective action.
func TestActionUniqueAcrossMultipleRestarts(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := runSpec(nn("a", 5))
	logSpec(t, "[ACT-2CRASH]", s)

	var effects int32
	gate := make(chan struct{})
	var gateOnce sync.Once

	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		inserted, err := in.Actions.Record(ctx, "k", "")
		if err != nil {
			return nil, err
		}
		if inserted {
			atomic.AddInt32(&effects, 1)
		}
		if in.Attempt < 2 {
			gateOnce.Do(func() { close(gate) })
			<-ctx.Done() // engine shutdown cancels attempt context
			return nil, ctx.Err()
		}
		return &ExecResult{}, nil
	}

	var runID string
	for proc := 0; proc < 3; proc++ {
		eng := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{})
		if proc == 0 {
			runID, err = eng.Submit(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			<-gate
			time.Sleep(10 * time.Millisecond)
		} else {
			if err := eng.Resume(context.Background(), runID); err != nil {
				t.Fatal(err)
			}
			if proc < 2 {
				time.Sleep(30 * time.Millisecond)
			}
		}
		if proc < 2 {
			if err := eng.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			continue
		}
		rec, err := eng.Wait(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		logTransitions(t, "[ACT-2CRASH]", rec)
		defer eng.Shutdown(context.Background())
		if len(rec.Actions) != 1 {
			t.Fatalf("actions after %d processes=%v, want 1", proc+1, rec.Actions)
		}
	}
	if got := atomic.LoadInt32(&effects); got != 1 {
		t.Fatalf("external effects=%d want 1 after multiple restarts", got)
	}
}

// TestActionKeysScopedToRun: the same key in different runs is independent.
func TestActionKeysScopedToRun(t *testing.T) {
	s := runSpec(nn("a", 1))
	logSpec(t, "[ACT-SCOPE]", s)
	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		inserted, err := in.Actions.Record(ctx, "shared-key", in.RunID)
		if err != nil {
			return nil, err
		}
		if !inserted {
			t.Error("key must be free in an independent run")
		}
		return &ExecResult{}, nil
	}
	st := store.NewMemoryStore()
	eng := New(st, ResolverFunc(func(string) (Task, bool) { return task, true }), Config{})
	defer eng.Shutdown(context.Background())

	id1, _ := eng.Submit(context.Background(), s)
	id2, _ := eng.Submit(context.Background(), s)
	r1, err := eng.Wait(context.Background(), id1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := eng.Wait(context.Background(), id2)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Actions) != 1 || len(r2.Actions) != 1 {
		t.Fatalf("each run should hold one record: %v / %v", r1.Actions, r2.Actions)
	}
	if r1.Actions[0].RunID == r2.Actions[0].RunID {
		t.Fatal("actions leaked across runs")
	}
	if r1.Actions[0].Value == r2.Actions[0].Value {
		t.Fatal("record payloads collide across runs")
	}
}

// TestActionConcurrentRecordOnce: many goroutines recording the same key
// concurrently within one attempt get exactly one first committer.
func TestActionConcurrentRecordOnce(t *testing.T) {
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 1})
	logSpec(t, "[ACT-RACE]", s)
	const N = 32
	reg := ResolverFunc(func(string) (Task, bool) {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			start := make(chan struct{})
			var wg sync.WaitGroup
			var inserts int32
			wg.Add(N)
			for i := 0; i < N; i++ {
				go func() {
					defer wg.Done()
					<-start
					ok, err := in.Actions.Record(ctx, "race-key", "x")
					if err != nil {
						t.Errorf("record: %v", err)
						return
					}
					if ok {
						atomic.AddInt32(&inserts, 1)
					}
				}()
			}
			close(start)
			wg.Wait()
			if inserts != 1 {
				t.Errorf("first committers=%d want 1", inserts)
			}
			has, err := in.Actions.Has(ctx, "race-key")
			if err != nil || !has {
				t.Errorf("Has after race: %v %v", has, err)
			}
			return &ExecResult{}, nil
		}, true
	})
	st := store.NewMemoryStore()
	eng := New(st, reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[ACT-RACE]", rec)
	if len(rec.Actions) != 1 {
		t.Fatalf("actions=%v want 1", rec.Actions)
	}
}
