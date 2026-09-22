package engine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

// TestMultiRunIsolation: concurrent runs do not pollute each other's
// statuses, effects or histories, and both remain reproducible.
func TestMultiRunIsolation(t *testing.T) {
	e, _ := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	mk := func(prefix string, failNode string) *spec.Spec {
		return &spec.Spec{ID: "multi", Nodes: []spec.NodeSpec{
			node(prefix + "-a"),
			node(prefix+"-b", prefix+"-a"),
		}}
	}
	var execs sync.Map // runID -> count
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		key := runID + "/" + id
		v, _ := execs.LoadOrStore(key, new(int32))
		atomic.AddInt32(v.(*int32), 1)
		return id, []string{"fx:" + key}, nil
	}

	const N = 4
	var wg sync.WaitGroup
	results := make(chan string, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			prefix := fmt.Sprintf("r%d", i)
			rid := "run-" + prefix
			s := mk(prefix, "")
			id, err := e.Create(context.Background(), s, exec, Option{RunID: rid})
			if err != nil {
				results <- fmt.Sprintf("create err: %v", err)
				return
			}
			rs, err := e.Wait(context.Background(), id)
			if err != nil {
				results <- fmt.Sprintf("wait err: %v", err)
				return
			}
			results <- string(rs.Status)
		}(i)
	}
	wg.Wait()
	close(results)
	for r := range results {
		if r != string(state.Succeeded) {
			t.Fatalf("unexpected run result: %s", r)
		}
	}
	ids, _ := e.ListRuns(context.Background())
	if len(ids) < N {
		t.Fatalf("historical runs=%d want >= %d", len(ids), N)
	}
	// Every node executed exactly once per run; effects stay run-scoped.
	for i := 0; i < N; i++ {
		prefix := fmt.Sprintf("r%d", i)
		rid := "run-" + prefix
		rs, _ := e.View(rid)
		for _, nid := range []string{prefix + "-a", prefix + "-b"} {
			v, ok := execs.Load(rid + "/" + nid)
			if !ok || atomic.LoadInt32(v.(*int32)) != 1 {
				t.Fatalf("node %s/%s executions=%v (want 1)", rid, nid, v)
			}
			if rs.Nodes[nid].Status != state.Succeeded {
				t.Fatalf("%s/%s status=%s", rid, nid, rs.Nodes[nid].Status)
			}
		}
		fx, _ := e.Effects(context.Background(), rid)
		if len(fx) != 2 {
			t.Fatalf("run %s effects=%v want 2 run-scoped effects", rid, fx)
		}
	}
}

// TestReproducibleScheduling runs the identical spec+behavior twice with the
// same deterministic backoff and compares node outcomes, attempt counts and
// effect sets.
func TestReproducibleScheduling(t *testing.T) {
	s := &spec.Spec{ID: "rep", Nodes: []spec.NodeSpec{
		node("a"),
		node("b", "a"),
		{ID: "c", Deps: []string{"a"}, Slots: 1, MaxRetries: 3},
		node("d", "b", "c"),
		node("e", "b"),
	}}
	dumpSpec(t, s)
	flaky := map[string]int32{"c": 2} // c fails twice then succeeds
	runOnce := func(rid string) map[string]string {
		e, _ := engineWithLog(t, 3, 16,
			backoff.Policy{Kind: backoff.Exponential, Base: time.Millisecond, Max: 10 * time.Millisecond})
		defer e.Close()
		var mu sync.Mutex
		counts := map[string]int32{}
		exec := func(ctx context.Context, runID, id string) (string, []string, error) {
			mu.Lock()
			counts[id]++
			n := counts[id]
			mu.Unlock()
			if need := flaky[id]; n <= need {
				return "", nil, fmt.Errorf("flaky %s attempt %d", id, n)
			}
			return id + "-ok", []string{"fx:" + id}, nil
		}
		id, err := e.Create(context.Background(), s, exec, Option{RunID: rid})
		if err != nil {
			t.Fatal(err)
		}
		rs, _ := e.Wait(context.Background(), id)
		out := map[string]string{}
		for nid, nd := range rs.Nodes {
			out[nid] = fmt.Sprintf("%s/%d", nd.Status, nd.AttemptCount)
		}
		out["__run__"] = string(rs.Status)
		fx, _ := e.Effects(context.Background(), id)
		sort.Strings(fx)
		out["__effects__"] = join(fx)
		return out
	}
	r1 := runOnce("rep-1")
	r2 := runOnce("rep-2")
	for k := range r1 {
		if r1[k] != r2[k] {
			t.Fatalf("non-reproducible key %s: %q vs %q", k, r1[k], r2[k])
		}
	}
	if r1["__run__"] != string(state.Succeeded) {
		t.Fatalf("run outcome=%s", r1["__run__"])
	}
	if r1["c"] != "succeeded/3" {
		t.Fatalf("c=%s want succeeded/3", r1["c"])
	}
}

func join(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

// TestNoDuplicateExecutionUnderContention: many ready nodes with a single
// slot must each execute exactly once (no double dispatch, no starvation).
func TestNoDuplicateExecutionUnderContention(t *testing.T) {
	e, _ := engineWithLog(t, 1, 4, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	const M = 12
	nodes := make([]spec.NodeSpec, M)
	for i := 0; i < M; i++ {
		nodes[i] = node(fmt.Sprintf("n%02d", i))
	}
	s := &spec.Spec{ID: "cont", Nodes: nodes}
	dumpSpec(t, s)
	var counts sync.Map
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { // hammer View concurrently to stress the state lock
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, _ = e.View("run-cont")
			}
		}()
	}
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		v, _ := counts.LoadOrStore(id, new(int32))
		atomic.AddInt32(v.(*int32), 1)
		time.Sleep(time.Millisecond)
		return id, nil, nil
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-cont"})
	rs, _ := e.Wait(context.Background(), id)
	wg.Wait()
	if rs.Status != state.Succeeded {
		t.Fatalf("status=%s", rs.Status)
	}
	for i := 0; i < M; i++ {
		nid := fmt.Sprintf("n%02d", i)
		v, ok := counts.Load(nid)
		if !ok || atomic.LoadInt32(v.(*int32)) != 1 {
			t.Fatalf("node %s executed %v times (want exactly 1)", nid, v)
		}
	}
}
