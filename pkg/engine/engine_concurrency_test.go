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

// TestConcurrencyCap enforces that at most MaxParallel nodes run at once.
func TestConcurrencyCap(t *testing.T) {
	const N, Cap = 8, 2
	nodes := make([]spec.NodeSpec, 0, N)
	for i := 0; i < N; i++ {
		id := "n" + itoa(i)
		nodes = append(nodes, spec.NodeSpec{ID: id, MaxAttempts: 1})
	}
	s := &spec.Spec{Name: "cap", MaxParallel: Cap, MaxQueue: N, Nodes: nodes}
	logSpec(t, "[CAP]", s)

	var inFlight, maxSeen int32
	var mu sync.Mutex
	release := make(chan struct{})
	reg := MapRegistry{}
	for i := 0; i < N; i++ {
		id := "n" + itoa(i)
		reg[id] = func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			cur := atomic.AddInt32(&inFlight, 1)
			for {
				old := atomic.LoadInt32(&maxSeen)
				if cur <= old || atomic.CompareAndSwapInt32(&maxSeen, old, cur) {
					break
				}
			}
			<-release
			atomic.AddInt32(&inFlight, -1)
			mu.Lock()
			mu.Unlock()
			return &ExecResult{}, nil
		}
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)

	// Give the scheduler time to fill the cap.
	time.Sleep(60 * time.Millisecond)
	if got := atomic.LoadInt32(&maxSeen); got > Cap {
		t.Fatalf("max in flight=%d > cap %d", got, Cap)
	}
	if got := atomic.LoadInt32(&inFlight); got != Cap {
		t.Fatalf("expected %d tasks parked, got %d", Cap, got)
	}
	close(release)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[CAP]", rec)
	if maxSeen != Cap {
		t.Fatalf("max in flight=%d want %d (cap underutilised or violated)", maxSeen, Cap)
	}
	for i := 0; i < N; i++ {
		if rec.Status.Nodes["n"+itoa(i)].State != model.StateSucceeded {
			t.Fatalf("n%d did not succeed", i)
		}
	}
}

// TestDeterministicRepeatedRuns runs the same spec twice and asserts the
// transition stream (node identity, states and codes, in order) is identical.
func TestDeterministicRepeatedRuns(t *testing.T) {
	s := runSpec(nn("a", 1), nn("b", 1, "a"), nn("c", 1, "a"), nn("d", 1, "b", "c"))
	logSpec(t, "[DET]", s)
	var counter int32
	reg := MapRegistry{}
	for _, id := range []string{"a", "b", "c", "d"} {
		id := id
		reg[id] = func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			atomic.AddInt32(&counter, 1)
			time.Sleep(time.Duration(1+int(in.Attempt)) * 100 * time.Microsecond)
			return &ExecResult{Value: id}, nil
		}
	}
	sig := func(rec *model.RunRecord) []string {
		out := make([]string, 0, len(rec.History.Events))
		for _, ev := range rec.History.Events {
			out = append(out, ev.NodeID+":"+string(ev.From)+"->"+string(ev.To)+":"+string(ev.Code))
		}
		return out
	}
	runOnce := func() []string {
		eng := New(store.NewMemoryStore(), reg, Config{})
		defer eng.Shutdown(context.Background())
		id, err := eng.Submit(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := eng.Wait(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		return sig(rec)
	}
	first := runOnce()
	second := runOnce()
	if len(first) != len(second) {
		t.Fatalf("transition lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("non-reproducible transition at %d:\n%s\nvs\n%s", i, first[i], second[i])
		}
	}
	t.Logf("[DET] deterministic transition signature: %v", first)
}

// TestBackpressureBoundedQueue proves surplus ready nodes are not admitted past
// MaxQueue while they still eventually execute (never silently dropped).
func TestBackpressureBoundedQueue(t *testing.T) {
	const N, Q = 6, 2
	nodes := make([]spec.NodeSpec, 0, N)
	for i := 0; i < N; i++ {
		nodes = append(nodes, spec.NodeSpec{ID: "n" + itoa(i), MaxAttempts: 1})
	}
	s := &spec.Spec{Name: "bp", MaxParallel: 1, MaxQueue: Q, Nodes: nodes}
	logSpec(t, "[BP]", s)

	var done int32
	gate := make(chan struct{})
	reg := MapRegistry{}
	for i := 0; i < N; i++ {
		reg["n"+itoa(i)] = func(ctx context.Context, in TaskInput) (*ExecResult, error) {
			atomic.AddInt32(&done, 1)
			if in.Attempt == 0 {
				<-gate
			}
			return &ExecResult{}, nil
		}
	}
	eng := New(store.NewMemoryStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)

	time.Sleep(60 * time.Millisecond)
	rec0, _ := eng.GetRun(context.Background(), id)
	queued, running, pending := 0, 0, 0
	for _, n := range rec0.Status.Nodes {
		switch n.State {
		case model.StateQueued:
			queued++
		case model.StateRunning:
			running++
		case model.StatePending:
			pending++
		}
	}
	t.Logf("[BP] mid-run: running=%d queued=%d pending=%d", running, queued, pending)
	if running > 1 {
		t.Fatalf("running=%d > MaxParallel 1", running)
	}
	if queued > Q {
		t.Fatalf("queued=%d exceeds MaxQueue %d: backpressure violated", queued, Q)
	}
	if running+queued+pending != N {
		t.Fatalf("node accounting broken: %d+%d+%d != %d", running, queued, pending, N)
	}
	close(gate)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if done != N {
		t.Fatalf("only %d/%d nodes ran: a valid task was dropped", done, N)
	}
	for i := 0; i < N; i++ {
		if rec.Status.Nodes["n"+itoa(i)].State != model.StateSucceeded {
			t.Fatalf("n%d missing terminal success", i)
		}
	}
}

// TestGlobalCapAcrossRuns verifies GlobalMaxParallel bounds in-flight nodes
// even when multiple runs are scheduled concurrently.
func TestGlobalCapAcrossRuns(t *testing.T) {
	var inFlight, maxSeen int32
	gate := make(chan struct{})
	task := func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxSeen)
			if cur <= old || atomic.CompareAndSwapInt32(&maxSeen, old, cur) {
				break
			}
		}
		<-gate
		atomic.AddInt32(&inFlight, -1)
		return &ExecResult{}, nil
	}
	mk := func() *spec.Spec {
		nodes := []spec.NodeSpec{
			{ID: "a", MaxAttempts: 1}, {ID: "b", MaxAttempts: 1}, {ID: "c", MaxAttempts: 1},
		}
		return &spec.Spec{Name: "g", MaxParallel: 3, MaxQueue: 3, Nodes: nodes}
	}
	eng := New(store.NewMemoryStore(), StaticResolver(task), Config{GlobalMaxParallel: 2})
	defer eng.Shutdown(context.Background())
	id1, err := eng.Submit(context.Background(), mk())
	if err != nil {
		t.Fatal(err)
	}
	id2, err := eng.Submit(context.Background(), mk())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	if got := atomic.LoadInt32(&maxSeen); got > 2 {
		t.Fatalf("global in flight peaked at %d > 2", got)
	}
	close(gate)
	for _, id := range []string{id1, id2} {
		rec, err := eng.Wait(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if rec.Status.State != model.RunSucceeded {
			t.Fatalf("run %s state=%s", id, rec.Status.State)
		}
	}
	t.Logf("[GCAP] observed peak in flight across runs=%d", maxSeen)
}
