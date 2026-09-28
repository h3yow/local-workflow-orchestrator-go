package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

// TestActionExactlyOnceAcrossCrash: a node commits a business action and is
// killed before its result is processed. On resume the attempt is replayed;
// the external effect guarded by Commit runs exactly once, the durable action
// set contains exactly one record, and the node ends in a clean success.
func TestActionExactlyOnceAcrossCrash(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	clk := newFakeClock()
	s := runSpec(nn("place-order", 3))
	logSpec(t, "[ACTION]", s)

	var effects int32
	var commitsAttempt2 int32

	task := func(markCommitted chan<- struct{}) Task {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			rec, committed, err := in.Actions.Commit(ctx, "order-42", "payload-v1")
			if err != nil {
				return nil, err
			}
			if committed {
				// Only the first (durable) commit performs the real effect.
				atomic.AddInt32(&effects, 1)
				if markCommitted != nil {
					close(markCommitted)
				}
			}
			if in.Attempt == 1 {
				atomic.AddInt32(&commitsAttempt2, 1)
				// Replay after crash: the action already existed.
				if rec.Attempt != 0 || committed {
					t.Errorf("replayed commit reported committed=true or wrong origin attempt=%d", rec.Attempt)
				}
				return &ExecResult{Value: "ok-after-replay"}, nil
			}
			if markCommitted != nil {
				// First process: block forever so the process is killed with
				// the action durably committed but the node still "running".
				<-ctx.Done()
			}
			return &ExecResult{Value: "ok"}, nil
		}
	}

	committed := make(chan struct{})
	eng1 := New(st, ResolverFunc(func(id string) (Task, bool) {
		if id == "place-order" {
			return task(committed), true
		}
		return nil, false
	}), Config{}).WithClock(clk)
	runID, err := eng1.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	<-committed
	// The record must already be durable while the node is still running.
	acts, err := st.LoadActions(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(acts.Actions) != 1 || acts.Actions[0].Key != "order-42" {
		t.Fatalf("durable action mid-execution = %+v", acts.Actions)
	}
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Second process: replay the interrupted attempt.
	eng2 := New(st, ResolverFunc(func(id string) (Task, bool) {
		if id == "place-order" {
			return task(nil), true
		}
		return nil, false
	}), Config{}).WithClock(clk)
	defer eng2.Shutdown(context.Background())
	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[ACTION]", rec)

	if got := atomic.LoadInt32(&effects); got != 1 {
		t.Fatalf("external effect ran %d times, want exactly 1", got)
	}
	if atomic.LoadInt32(&commitsAttempt2) != 1 {
		t.Fatalf("replay attempt did not observe the pre-existing action")
	}
	if rec.Actions == nil || len(rec.Actions.Actions) != 1 {
		t.Fatalf("final action set = %+v, want exactly 1 record", rec.Actions)
	}
	ar := rec.Actions.Actions[0]
	if ar.Key != "order-42" || ar.NodeID != "place-order" || ar.Attempt != 0 {
		t.Fatalf("surviving record = %+v", ar)
	}
	if ar.Value != "payload-v1" {
		t.Fatalf("replay payload overwrote the original: %q", ar.Value)
	}
	n := rec.Status.Nodes["place-order"]
	if n.State != model.StateSucceeded {
		t.Fatalf("node state=%s want succeeded", n.State)
	}
	// The intermediate recovered_running annotation must not pollute success.
	if n.FailCode != model.FailNone || n.FailMessage != "" {
		t.Fatalf("success carries failure leftovers: code=%s msg=%q", n.FailCode, n.FailMessage)
	}
}

// TestActionScopedPerRun: identical keys in different runs (and different
// nodes) never dedup against each other.
func TestActionScopedPerRun(t *testing.T) {
	st := newMemStore()
	mk := func(runIDLabel string) *spec.Spec { return runSpec(nn("n", 1)) }
	var mu sync.Mutex
	seen := map[string]int{}
	reg := ResolverFunc(func(id string) (Task, bool) {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			_, committed, err := in.Actions.Commit(ctx, "same-key", "")
			if err != nil {
				return nil, err
			}
			mu.Lock()
			seen[in.RunID]++
			mu.Unlock()
			if !committed {
				t.Errorf("run %s first commit not reported as committed", in.RunID)
			}
			return &ExecResult{}, nil
		}, true
	})
	var runIDs []string
	for i := 0; i < 2; i++ {
		eng := New(st, reg, Config{})
		id, err := eng.Submit(context.Background(), mk("r"))
		if err != nil {
			t.Fatal(err)
		}
		runIDs = append(runIDs, id)
		if _, err := eng.Wait(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		eng.Shutdown(context.Background())
	}
	for _, id := range runIDs {
		log, err := st.LoadActions(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if len(log.Actions) != 1 || log.Actions[0].Key != "same-key" {
			t.Fatalf("run %s actions = %+v", id, log.Actions)
		}
	}
	if len(seen) != 2 {
		t.Fatalf("runs collided on the action key: %v", seen)
	}
}

// TestActionConcurrentSameKey: many goroutines commit the same key at once;
// exactly one wins, the set is one record and everyone saw the same winner.
func TestActionConcurrentSameKey(t *testing.T) {
	st := newMemStore()
	s := runSpec(nn("fan", 1))
	var winners int32
	reg := StaticResolver(func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		const g = 32
		var wg sync.WaitGroup
		wg.Add(g)
		start := make(chan struct{})
		for i := 0; i < g; i++ {
			go func() {
				defer wg.Done()
				<-start
				_, committed, err := in.Actions.Commit(ctx, "k", "")
				if err != nil {
					t.Error(err)
					return
				}
				if committed {
					atomic.AddInt32(&winners, 1)
				}
			}()
		}
		close(start)
		wg.Wait()
		return &ExecResult{}, nil
	})
	eng := New(st, reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	if _, err := eng.Wait(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	log, err := eng.Actions(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(log.Actions) != 1 {
		t.Fatalf("committed records=%d want 1", len(log.Actions))
	}
	if winners != 1 {
		t.Fatalf("concurrent winners=%d want 1", winners)
	}
	// Repeated reads are deterministic and ordered.
	log2, _ := eng.Actions(context.Background(), id)
	if log2.Actions[0].Seq != log.Actions[0].Seq {
		t.Fatal("action seq differs between reads")
	}
}

// TestActionReplayAcrossReopenedStore covers the file store specifically: the
// action log survives a full process simulation (new FileStore on the same
// directory) and duplicate commits stay suppressed.
func TestActionReplayAcrossReopenedStore(t *testing.T) {
	dir := t.TempDir()
	st1, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := st1.SaveStatus(ctx, &model.RunStatus{RunID: "run-x", State: model.RunRunning,
		Nodes: map[string]*model.NodeStatus{"a": {ID: "a"}}, NodeOrder: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	rec1, committed, err := st1.CommitAction(ctx, &model.ActionRecord{
		RunID: "run-x", NodeID: "a", Key: "m-1", CommittedAt: time.Now()})
	if err != nil || !committed || rec1.Seq != 1 {
		t.Fatalf("first commit: rec=%+v committed=%v err=%v", rec1, committed, err)
	}

	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec2, committed, err := st2.CommitAction(ctx, &model.ActionRecord{
		RunID: "run-x", NodeID: "a", Key: "m-1", Value: "later", CommittedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if committed {
		t.Fatal("duplicate commit after reopen reported committed=true")
	}
	if rec2.Seq != 1 || rec2.Value != "" {
		t.Fatalf("duplicate changed the winning record: %+v", rec2)
	}
	// A new key continues the sequence above the restart.
	rec3, committed, err := st2.CommitAction(ctx, &model.ActionRecord{
		RunID: "run-x", NodeID: "a", Key: "m-2", CommittedAt: time.Now()})
	if err != nil || !committed || rec3.Seq != 2 {
		t.Fatalf("post-restart commit: rec=%+v committed=%v err=%v", rec3, committed, err)
	}
	log, err := st2.LoadActions(ctx, "run-x")
	if err != nil {
		t.Fatal(err)
	}
	if len(log.Actions) != 2 || log.Actions[0].Key != "m-1" || log.Actions[1].Key != "m-2" {
		t.Fatalf("replayed action set = %+v", log.Actions)
	}
}
