package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

func runSpec(nodes ...spec.NodeSpec) *spec.Spec {
	return &spec.Spec{Name: "t", MaxParallel: 4, MaxQueue: 8, Nodes: nodes}
}
func nn(id string, attempts int, deps ...string) spec.NodeSpec {
	return spec.NodeSpec{ID: id, Deps: deps, MaxAttempts: attempts}
}

func logSpec(t *testing.T, tag string, s *spec.Spec) {
	t.Helper()
	t.Logf("%s INPUT SPEC: maxParallel=%d maxQueue=%d", tag, s.MaxParallel, s.MaxQueue)
	for _, n := range s.Nodes {
		t.Logf("%s   node=%s deps=%v attempts=%d timeout=%s backoff=%s base=%s",
			tag, n.ID, n.Deps, n.MaxAttempts, n.Timeout, n.Backoff.Kind, n.Backoff.Base)
	}
}

func logTransitions(t *testing.T, tag string, rec *model.RunRecord) {
	t.Helper()
	for _, ev := range rec.History.Events {
		t.Logf("%s TRANSITION seq=%d %s: %s->%s attempt=%d code=%s detail=%q",
			tag, ev.Seq, ev.NodeID, ev.From, ev.To, ev.Attempt, ev.Code, ev.Detail)
	}
	for _, id := range rec.Status.NodeOrder {
		n := rec.Status.Nodes[id]
		t.Logf("%s FINAL node=%s state=%s attempts=%d code=%s skippedBecause=%s msg=%q",
			tag, id, n.State, len(n.Attempts), n.FailCode, n.SkippedBecause, n.FailMessage)
	}
}

func TestDependencyFailureSkipsDownstream(t *testing.T) {
	s := runSpec(
		nn("a", 1),
		nn("b", 1, "a"),
		nn("c", 1, "b"),
		nn("d", 1, "a"),
	)
	logSpec(t, "[FAILPROP]", s)
	reg := MapRegistry{
		"a": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			return nil, errors.New("boom")
		},
		"b": taskOK, "c": taskOK, "d": taskOK,
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[FAILPROP]", rec)

	if rec.Status.State != model.RunFailed {
		t.Fatalf("run state=%s want failed", rec.Status.State)
	}
	if got := rec.Status.Nodes["a"]; got.State != model.StateFailed || got.FailCode != model.FailAttemptsExhausted {
		t.Fatalf("a = %s/%s", got.State, got.FailCode)
	}
	for _, id := range []string{"b", "c"} {
		n := rec.Status.Nodes[id]
		if n.State != model.StateSkipped || n.FailCode != model.FailUpstreamFailed {
			t.Fatalf("%s = %s/%s, want skipped/upstream_failed", id, n.State, n.FailCode)
		}
		if len(n.Attempts) != 0 {
			t.Fatalf("%s was executed %d times despite being skipped", id, len(n.Attempts))
		}
	}
	// d also depends solely on failed a: skipped, not executed.
	if rec.Status.Nodes["d"].State != model.StateSkipped {
		t.Fatalf("d = %s want skipped", rec.Status.Nodes["d"].State)
	}
}

func TestRetryExhaustionAndPermanent(t *testing.T) {
	var calls int32
	s := runSpec(
		spec.NodeSpec{ID: "retry", MaxAttempts: 3, Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: time.Millisecond}},
		spec.NodeSpec{ID: "perm", MaxAttempts: 5},
	)
	logSpec(t, "[RETRY]", s)
	reg := MapRegistry{
		"retry": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			atomic.AddInt32(&calls, 1)
			return nil, errors.New("flaky forever")
		},
		"perm": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			return nil, Permanent(errors.New("cannot recover"))
		},
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[RETRY]", rec)

	if calls != 3 {
		t.Fatalf("retry node attempts=%d want 3", calls)
	}
	rn := rec.Status.Nodes["retry"]
	if rn.State != model.StateFailed || rn.FailCode != model.FailAttemptsExhausted {
		t.Fatalf("retry = %s/%s", rn.State, rn.FailCode)
	}
	if len(rn.Attempts) != 3 {
		t.Fatalf("retry attempt records=%d", len(rn.Attempts))
	}
	pn := rec.Status.Nodes["perm"]
	if pn.State != model.StateFailed || pn.FailCode != model.FailPermanent {
		t.Fatalf("perm = %s/%s want failed/permanent", pn.State, pn.FailCode)
	}
	if len(pn.Attempts) != 1 {
		t.Fatalf("permanent attempts=%d want 1 (no retries)", len(pn.Attempts))
	}
}

func TestRetrySucceedsAfterBackoff(t *testing.T) {
	var calls int32
	s := runSpec(spec.NodeSpec{ID: "a", MaxAttempts: 4,
		Backoff: backoff.Policy{Kind: backoff.KindFixed, Base: 5 * time.Millisecond}})
	logSpec(t, "[RETRYOK]", s)
	reg := MapRegistry{
		"a": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			if atomic.AddInt32(&calls, 1) < 3 {
				return nil, errors.New("not yet")
			}
			return &ExecResult{Value: "done"}, nil
		},
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[RETRYOK]", rec)
	if calls != 3 {
		t.Fatalf("attempts=%d want 3", calls)
	}
	if rec.Status.Nodes["a"].State != model.StateSucceeded {
		t.Fatalf("state=%s", rec.Status.Nodes["a"].State)
	}
	// The recorded ready->queued transition must cite the backoff delay.
	found := false
	for _, ev := range rec.History.Events {
		if strings.Contains(ev.Detail, "retry scheduled") {
			found = true
		}
	}
	if !found {
		t.Fatal("no retry scheduling evidence in history")
	}
}

func TestTimeoutFailsNode(t *testing.T) {
	s := runSpec(spec.NodeSpec{ID: "slow", MaxAttempts: 2, Timeout: 20 * time.Millisecond,
		Backoff: backoff.Policy{Kind: backoff.KindNone}})
	logSpec(t, "[TIMEOUT]", s)
	reg := MapRegistry{
		"slow": func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[TIMEOUT]", rec)
	n := rec.Status.Nodes["slow"]
	if n.State != model.StateFailed || n.FailCode != model.FailTimeout {
		t.Fatalf("slow = %s/%s want failed/timeout", n.State, n.FailCode)
	}
	for _, a := range n.Attempts {
		if !a.TimedOut {
			t.Fatalf("attempt %d not marked timed out", a.Attempt)
		}
	}
}

func taskOK(ctx context.Context, in TaskInput) (*ExecResult, error) {
	return &ExecResult{Value: in.NodeID}, nil
}
