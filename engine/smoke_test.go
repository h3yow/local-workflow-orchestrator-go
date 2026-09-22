package engine

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
	"github.com/h3yow/local-workflow-orchestrator-go/store"
)

func newTestEngine(t *testing.T, maxRun, maxQueue int) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(Config{MaxRunning: maxRun, MaxQueue: maxQueue, Store: st})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, dir
}

func TestSmokeDiamond(t *testing.T) {
	e, _ := newTestEngine(t, 2, 16)
	s := &spec.Spec{ID: "diamond", Nodes: []spec.NodeSpec{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"a"}},
		{ID: "d", Deps: []string{"b", "c"}},
	}}
	var ran int32
	exec := func(ctx context.Context, runID, nodeID string) (string, []string, error) {
		atomic.AddInt32(&ran, 1)
		return nodeID + "-ok", []string{"fx-" + nodeID}, nil
	}
	id, err := e.Create(context.Background(), s, exec, Option{RunID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	rs, err := e.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Status != state.Succeeded {
		t.Fatalf("status=%s", rs.Status)
	}
	if ran != 4 {
		t.Fatalf("ran=%d", ran)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		if rs.Nodes[n].AttemptCount != 1 {
			t.Fatalf("node %s attempts=%d", n, rs.Nodes[n].AttemptCount)
		}
	}
	fx, _ := e.Effects(context.Background(), id)
	if len(fx) != 4 {
		t.Fatalf("effects=%v", fx)
	}
}

var _ = time.Second
