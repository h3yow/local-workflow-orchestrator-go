package engine

import (
	"context"
	"testing"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

func TestSmokeDiamond(t *testing.T) {
	s := &spec.Spec{
		Name: "diamond", MaxParallel: 2, MaxQueue: 4,
		Nodes: []spec.NodeSpec{
			{ID: "a", MaxAttempts: 1},
			{ID: "b", Deps: []string{"a"}, MaxAttempts: 1},
			{ID: "c", Deps: []string{"a"}, MaxAttempts: 1},
			{ID: "d", Deps: []string{"b", "c"}, MaxAttempts: 1},
		},
	}
	eng := New(store.NewMemoryStore(), StaticResolver(func(ctx context.Context, in TaskInput) (*ExecResult, error) {
		return &ExecResult{Value: in.NodeID}, nil
	}), Config{})
	defer eng.Shutdown(context.Background())

	runID, err := eng.Submit(context.Background(), s)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rec, err := eng.Wait(context.Background(), runID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if rec.Status.State != model.RunSucceeded {
		t.Fatalf("run state=%s want succeeded", rec.Status.State)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if got := rec.Status.Nodes[id].State; got != model.StateSucceeded {
			t.Fatalf("node %s state=%s", id, got)
		}
	}
	if len(rec.History.Events) == 0 {
		t.Fatal("history empty")
	}
}
