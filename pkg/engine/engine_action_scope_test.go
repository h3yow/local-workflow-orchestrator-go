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

// sharedKeyEffects is the side-effect gate used by the scope tests.
type sharedKeyEffects struct {
	effects int32
}

func (e *sharedKeyEffects) commit(ctx context.Context, in TaskInput) (*model.ActionRecord, bool, error) {
	rec, committed, err := in.Actions.Commit(ctx, "order-42", "payload-"+in.NodeID)
	if err != nil {
		return nil, false, err
	}
	if committed {
		// The effect gate: only the run-wide first committer may act.
		atomic.AddInt32(&e.effects, 1)
	}
	return rec, committed, nil
}

// TestActionSameKeyTwoNodesOneEffect: two different nodes in ONE run register
// the same business key (beta depends on alpha so alpha commits first). The
// second node receives the first node's record, performs no side effect, and
// the run ends with exactly one action record and both nodes succeeded.
func TestActionSameKeyTwoNodesOneEffect(t *testing.T) {
	st := newMemStore()
	eff := &sharedKeyEffects{}
	s := runSpec(nn("alpha", 1), nn("beta", 1, "alpha"))
	logSpec(t, "[SCOPE]", s)

	reg := ResolverFunc(func(id string) (Task, bool) {
		switch id {
		case "alpha":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				if _, committed, err := eff.commit(ctx, in); err != nil {
					return nil, err
				} else if !committed {
					t.Errorf("alpha unexpectedly lost the run-wide key race")
				}
				return &ExecResult{Value: "alpha-ok"}, nil
			}, true
		case "beta":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				rec, committed, err := eff.commit(ctx, in)
				if err != nil {
					return nil, err
				}
				if committed {
					t.Errorf("beta reported committed=true for a key already committed by alpha")
				}
				if rec.NodeID != "alpha" || rec.Attempt != 0 || rec.Value != "payload-alpha" {
					t.Errorf("beta got foreign/new record instead of alpha's: %+v", rec)
				}
				return &ExecResult{Value: "beta-ok"}, nil
			}, true
		}
		return nil, false
	})

	eng := New(st, reg, Config{})
	defer eng.Shutdown(context.Background())
	id, err := eng.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[SCOPE]", rec)

	if got := atomic.LoadInt32(&eff.effects); got != 1 {
		t.Fatalf("external effect ran %d times, want exactly 1", got)
	}
	if rec.Actions == nil || len(rec.Actions.Actions) != 1 {
		t.Fatalf("final action set = %+v, want exactly 1 record", rec.Actions)
	}
	r := rec.Actions.Actions[0]
	if r.Key != "order-42" || r.NodeID != "alpha" || r.Attempt != 0 || r.Seq != 1 {
		t.Fatalf("surviving record = %+v", r)
	}
	for _, nid := range []string{"alpha", "beta"} {
		if rec.Status.Nodes[nid].State != model.StateSucceeded {
			t.Fatalf("node %s state=%s want succeeded", nid, rec.Status.Nodes[nid].State)
		}
	}
}

// TestActionSameKeyTwoNodesRacing: two independent nodes commit the same key
// concurrently; regardless of which node wins, exactly one effect runs and
// exactly one record survives. Concurrent readers always agree on the set.
func TestActionSameKeyTwoNodesRacing(t *testing.T) {
	st := newMemStore()
	eff := &sharedKeyEffects{}
	s := runSpec(nn("alpha", 1), nn("beta", 1))
	logSpec(t, "[SCOPE]", s)

	start := make(chan struct{})
	task := func(node string) Task {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			<-start // release both nodes at once
			if _, _, err := eff.commit(ctx, in); err != nil {
				return nil, err
			}
			return &ExecResult{Value: node + "-ok"}, nil
		}
	}
	eng := New(st, ResolverFunc(func(id string) (Task, bool) {
		return task(id), true
	}), Config{})
	defer eng.Shutdown(context.Background())
	id, err := eng.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent readers during the run must never observe two records for
	// the one key nor an unstable winner.
	stopReaders := make(chan struct{})
	var readerWG sync.WaitGroup
	var readerErr int32
	for i := 0; i < 8; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			var prev *model.ActionLog
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				got, err := st.LoadActions(context.Background(), id)
				if err != nil {
					// The run row may not exist for the first few iterations;
					// anything other than ErrNotFound is real.
					if err != model.ErrNotFound {
						t.Error(err)
						return
					}
					continue
				}
				if len(got.Actions) > 1 {
					atomic.StoreInt32(&readerErr, 1)
					return
				}
				if prev != nil && len(prev.Actions) == 1 && len(got.Actions) == 1 {
					if prev.Actions[0] != got.Actions[0] {
						atomic.StoreInt32(&readerErr, 2)
						return
					}
				}
				prev = got.Clone()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	close(start)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	close(stopReaders)
	readerWG.Wait()
	logTransitions(t, "[SCOPE]", rec)

	if code := atomic.LoadInt32(&readerErr); code != 0 {
		t.Fatalf("concurrent readers disagreed on the action set (code=%d)", code)
	}
	if got := atomic.LoadInt32(&eff.effects); got != 1 {
		t.Fatalf("external effect ran %d times, want exactly 1", got)
	}
	if len(rec.Actions.Actions) != 1 {
		t.Fatalf("final action set = %+v, want exactly 1 record", rec.Actions.Actions)
	}
	winner := rec.Actions.Actions[0].NodeID
	if winner != "alpha" && winner != "beta" {
		t.Fatalf("winner node = %q", winner)
	}
	// Repeated reads after the run finish are identical and replay-stable.
	a, _ := eng.Actions(context.Background(), id)
	b, _ := eng.Actions(context.Background(), id)
	if len(a.Actions) != 1 || a.Actions[0] != b.Actions[0] {
		t.Fatalf("replayed action sets diverged: %+v vs %+v", a.Actions, b.Actions)
	}
}

// reference spec used by the cross-node restart test, exposed to keep the
// construction identical between both processes.
func crossNodeRestartSpec() *spec.Spec {
	return runSpec(nn("alpha", 3), nn("beta", 1, "alpha"))
}

// TestActionSameKeyAcrossCrashAndNodes: process 1 has alpha durably commit
// the key and is killed while alpha is still running. Process 2 resumes;
// alpha's attempt is replayed and its dependent beta runs fresh. Both of them
// re-register the same key after the restart: neither may perform the effect,
// no second record may appear, and what every reader observes before and
// after the restart is one stable record originating at alpha/attempt-0.
func TestActionSameKeyAcrossCrashAndNodes(t *testing.T) {
	dir := t.TempDir()
	st1, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Use the real wall clock: after restart the run makes progress across
	// multiple scheduling waves (alpha replay, then beta) and each wave relies
	// on the scheduler's safety timer, which a never-advanced fake clock would
	// never fire.
	s := crossNodeRestartSpec()
	logSpec(t, "[SCOPE]", s)

	var effects int32

	// makeTask builds the node task. killOnFirstAttempt only applies in the
	// first process: alpha's attempt 0 commits the action durably and is then
	// killed before its result is processed. In the restarted process that
	// same attempt is replayed and MUST return normally.
	makeTask := func(killOnFirstAttempt bool, markCommitted chan<- struct{}) Task {
		return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			rec, committed, err := in.Actions.Commit(ctx, "order-42", "payload-alpha")
			if err != nil {
				return nil, err
			}
			if committed {
				atomic.AddInt32(&effects, 1)
				if markCommitted != nil {
					close(markCommitted)
				}
			} else {
				// Losers (alpha replay + beta) must see the exact pre-crash
				// record, not a node-local substitute.
				if rec.NodeID != "alpha" || rec.Attempt != 0 || rec.Value != "payload-alpha" || rec.Seq != 1 {
					t.Errorf("%s attempt %d saw foreign record: %+v", in.NodeID, in.Attempt, rec)
				}
			}
			if killOnFirstAttempt && in.NodeID == "alpha" && in.Attempt == 0 {
				// First process: the durable commit exists, but the result is
				// never processed — simulate kill -9.
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &ExecResult{Value: in.NodeID + "-ok"}, nil
		}
	}

	committed := make(chan struct{})
	eng1 := New(st1, ResolverFunc(func(id string) (Task, bool) {
		if id == "alpha" {
			return makeTask(true, committed), true
		}
		return nil, false // beta never starts before the kill
	}), Config{})
	runID, err := eng1.Submit(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	<-committed

	// Pre-kill read: one durable record already, stable under repeat reads.
	pre, err := st1.LoadActions(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pre.Actions) != 1 || pre.Actions[0].NodeID != "alpha" || pre.Actions[0].Seq != 1 {
		t.Fatalf("pre-kill action set = %+v", pre.Actions)
	}
	if err := eng1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Process 2: brand new FileStore on the same directory (real restart).
	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := New(st2, ResolverFunc(func(id string) (Task, bool) {
		return makeTask(false, nil), true // both alpha (replay) and beta resolve
	}), Config{})
	defer eng2.Shutdown(context.Background())

	// While replay scheduling proceeds, concurrent readers reopening the log
	// must keep seeing the same single record, unchanged across the restart.
	stopReaders := make(chan struct{})
	var readerWG sync.WaitGroup
	var readerErr int32
	for i := 0; i < 4; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				got, err := st2.LoadActions(context.Background(), runID)
				if err != nil {
					atomic.StoreInt32(&readerErr, 1)
					return
				}
				if len(got.Actions) != 1 || got.Actions[0] != pre.Actions[0] {
					atomic.StoreInt32(&readerErr, 1)
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}

	if err := eng2.Resume(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	rec, err := eng2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	close(stopReaders)
	readerWG.Wait()
	logTransitions(t, "[SCOPE]", rec)

	if atomic.LoadInt32(&readerErr) != 0 {
		t.Fatal("a reader observed the record set change across the restart")
	}
	if got := atomic.LoadInt32(&effects); got != 1 {
		t.Fatalf("external effect ran %d times, want exactly 1", got)
	}
	if rec.Actions == nil || len(rec.Actions.Actions) != 1 {
		t.Fatalf("post-restart action set = %+v, want exactly 1 record", rec.Actions)
	}
	r := rec.Actions.Actions[0]
	if r.Key != "order-42" || r.NodeID != "alpha" || r.Attempt != 0 || r.Seq != 1 ||
		r.Value != "payload-alpha" {
		t.Fatalf("surviving record = %+v", r)
	}
	for _, nid := range []string{"alpha", "beta"} {
		n := rec.Status.Nodes[nid]
		if n.State != model.StateSucceeded {
			t.Fatalf("node %s state=%s want succeeded", nid, n.State)
		}
		if n.FailCode != model.FailNone || n.FailMessage != "" {
			t.Fatalf("node %s carries failure leftovers: code=%s msg=%q", nid, n.FailCode, n.FailMessage)
		}
	}
}
