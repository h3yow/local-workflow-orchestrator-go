package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

// TestDynamicExpansion: the expander hook attaches new successors after a
// node succeeds; the new graph runs to completion.
func TestDynamicExpansion(t *testing.T) {
	e, _ := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "dyn", Nodes: []spec.NodeSpec{node("a")}}
	dumpSpec(t, s)

	var sawExpanded int32
	var mu sync.Mutex
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		mu.Lock()
		if id == "b1" || id == "b2" {
			sawExpanded++
		}
		mu.Unlock()
		return id, nil, nil
	}
	exp := func(runID, parentID, result string) []spec.NodeSpec {
		if parentID == "a" {
			return []spec.NodeSpec{
				{ID: "b1", Deps: []string{"a"}, Slots: 1},
				{ID: "b2", Deps: []string{"a"}, Slots: 1},
			}
		}
		return nil
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-dyn", Expander: exp})
	rs, _ := e.Wait(context.Background(), id)
	for _, n := range []string{"a", "b1", "b2"} {
		if rs.Nodes[n] == nil || rs.Nodes[n].Status != state.Succeeded {
			t.Fatalf("node %s status=%v", n, rs.Nodes[n])
		}
		if n != "a" && rs.Nodes[n].ExpandedBy != "a" {
			t.Fatalf("node %s ExpandedBy=%q", n, rs.Nodes[n].ExpandedBy)
		}
	}
	if sawExpanded != 2 {
		t.Fatalf("expanded nodes executed=%d want 2", sawExpanded)
	}
}

// TestExpansionDuplicateProducesOnce: producing the same node twice (same
// hook on two parents + explicit Expand call) can only take effect once.
func TestExpansionDuplicateProducesOnce(t *testing.T) {
	e, _ := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "dup", Nodes: []spec.NodeSpec{node("a"), node("b")}}
	dumpSpec(t, s)
	exec := func(ctx context.Context, runID, id string) (string, []string, error) { return id, nil, nil }
	exp := func(runID, parentID, result string) []spec.NodeSpec {
		// both a and b claim to produce the same node id
		return []spec.NodeSpec{{ID: "x", Deps: []string{parentID}, Slots: 1}}
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-dup", Expander: exp})
	rs, _ := e.Wait(context.Background(), id)
	// Whichever parent commits first wins; the second expansion is rejected
	// but the run must still terminate. The node exists exactly once.
	if _, ok := rs.Nodes["x"]; !ok {
		t.Fatal("x should exist once")
	}
	if len(rs.Nodes) != 3 {
		t.Fatalf("nodes=%d want 3 (a,b,x); duplicate production must not create a copy", len(rs.Nodes))
	}
	// Explicit attempt to produce x again is a distinguishable error.
	err := e.Expand(context.Background(), id, "a", []spec.NodeSpec{{ID: "x", Deps: []string{"a"}, Slots: 1}})
	if !errors.Is(err, errDuplicateNode) {
		t.Fatalf("err=%v want errDuplicateNode", err)
	}
}

// TestExpansionRejectsCycle: dynamically added edges that close a cycle are
// rejected with the distinguishable cycle cause.
func TestExpansionRejectsCycle(t *testing.T) {
	e, _ := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "cyc", Nodes: []spec.NodeSpec{
		node("a"), node("b", "a"),
	}}
	dumpSpec(t, s)
	exec := func(ctx context.Context, runID, id string) (string, []string, error) { return id, nil, nil }
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-cyc"})
	_, _ = e.Wait(context.Background(), id)
	// new node c: c depends on b; then a2 that makes a depend on c is illegal
	// because existing node deps are immutable -> emulate cycle with new nodes
	// c (deps b) and d (deps c) plus e (deps nothing) where... simpler: c deps b,
	// and add node that declares dep on a node that would depend back is not
	// expressible without editing old edges; validate via two-node new cycle
	// where one new node depends on another and that other depends on the first.
	err := e.Expand(context.Background(), id, "b", []spec.NodeSpec{
		{ID: "c", Deps: []string{"b", "d"}, Slots: 1},
		{ID: "d", Deps: []string{"c"}, Slots: 1},
	})
	if !errors.Is(err, errExpansionCycle) {
		t.Fatalf("err=%v want errExpansionCycle", err)
	}
}

// TestExpansionDanglingDependency is rejected with a distinct cause.
func TestExpansionDanglingDependency(t *testing.T) {
	e, _ := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "dang", Nodes: []spec.NodeSpec{node("a")}}
	dumpSpec(t, s)
	exec := func(ctx context.Context, runID, id string) (string, []string, error) { return id, nil, nil }
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-dang"})
	_, _ = e.Wait(context.Background(), id)
	err := e.Expand(context.Background(), id, "a", []spec.NodeSpec{
		{ID: "c", Deps: []string{"ghost"}, Slots: 1},
	})
	if !errors.Is(err, errDanglingDependency) {
		t.Fatalf("err=%v want errDanglingDependency", err)
	}
}

// TestExpansionBackpressure: queue-saturating expansion is refused atomically
// and nothing partially takes effect.
func TestExpansionBackpressure(t *testing.T) {
	e, _ := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "xpb", Nodes: []spec.NodeSpec{node("a")}}
	dumpSpec(t, s)
	exec := func(ctx context.Context, runID, id string) (string, []string, error) { return id, nil, nil }
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-xpb"})
	_, _ = e.Wait(context.Background(), id)

	// Build a second tiny-queue engine and expand there through API.
	e2, _ := engineWithLog(t, 1, 2, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s2 := &spec.Spec{ID: "xpb2", Nodes: []spec.NodeSpec{node("a")}}
	id2, _ := e2.Create(context.Background(), s2, exec, Option{RunID: "run-xpb2"})
	_, _ = e2.Wait(context.Background(), id2)
	batch := []spec.NodeSpec{
		{ID: "n1", Deps: []string{"a"}, Slots: 1},
		{ID: "n2", Deps: []string{"a"}, Slots: 1},
		{ID: "n3", Deps: []string{"a"}, Slots: 1},
	}
	err := e2.Expand(context.Background(), id2, "a", batch)
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err=%v want ErrBackpressure", err)
	}
	view, _ := e2.View(id2)
	for _, n := range []string{"n1", "n2", "n3"} {
		if _, ok := view.Nodes[n]; ok {
			t.Fatalf("node %s must not partially exist after refused expansion", n)
		}
	}
}

// TestConcurrentViewsConsistent: observers concurrently View while running;
// every observed snapshot must be internally consistent (statuses valid and
// the succeeded set monotonic, no half-updated structures).
func TestConcurrentViewsConsistent(t *testing.T) {
	e, _ := engineWithLog(t, 4, 32, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	ns := make([]spec.NodeSpec, 0, 20)
	for i := 0; i < 20; i++ {
		id := nodeName(i)
		var deps []string
		if i > 0 {
			deps = []string{nodeName(i - 1)}
		}
		ns = append(ns, spec.NodeSpec{ID: id, Deps: deps, Slots: 1})
	}
	s := &spec.Spec{ID: "obs", Nodes: ns}
	dumpSpec(t, s)
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		time.Sleep(2 * time.Millisecond)
		return id, nil, nil
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-obs"})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prevSucceeded := map[string]bool{}
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, err := e.View(id)
				if err != nil {
					t.Errorf("view: %v", err)
					return
				}
				if len(v.Nodes) != len(ns) {
					t.Errorf("half-updated view: nodes=%d", len(v.Nodes))
					return
				}
				for nid, nd := range v.Nodes {
					switch nd.Status {
					case state.Pending, state.Queued, state.Running,
						state.Succeeded, state.Failed, state.Skipped:
					default:
						t.Errorf("invalid status %q for %s", nd.Status, nid)
						return
					}
					if prevSucceeded[nid] && nd.Status != state.Succeeded {
						t.Errorf("succeeded node %s regressed to %s", nid, nd.Status)
						return
					}
					if nd.Status == state.Succeeded {
						prevSucceeded[nid] = true
					}
				}
			}
		}()
	}
	rs, _ := e.Wait(context.Background(), id)
	close(stop)
	wg.Wait()
	if rs.Status != state.Succeeded {
		t.Fatalf("status=%s", rs.Status)
	}
}

func nodeName(i int) string {
	return "n" + string(rune('a'+i%26)) + itoa(i/26)
}

func itoa(i int) string {
	if i == 0 {
		return ""
	}
	b := []byte{}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
