package engine

import (
	"context"
	"testing"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
)

func TestDynamicSpawnAppendsFollowups(t *testing.T) {
	s := runSpec(nn("seed", 1))
	logSpec(t, "[SPAWN]", s)
	reg := ResolverFunc(func(id string) (Task, bool) {
		switch id {
		case "seed":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				return &ExecResult{Spawn: []SpawnNode{
					{Node: spec.NodeSpec{ID: "x", Deps: []string{"seed"}, MaxAttempts: 1}},
					{Node: spec.NodeSpec{ID: "y", Deps: []string{"x"}, MaxAttempts: 1}},
					// duplicate within the batch: must only take effect once
					{Node: spec.NodeSpec{ID: "x", Deps: []string{"seed"}, MaxAttempts: 1}},
					// duplicate of an existing node: ignored
					{Node: spec.NodeSpec{ID: "seed", MaxAttempts: 1}},
				}}, nil
			}, true
		case "x":
			return taskOK, true
		case "y":
			return taskOK, true
		}
		return nil, false
	})
	eng := New(newMemStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[SPAWN]", rec)
	for _, want := range []string{"seed", "x", "y"} {
		n := rec.Status.Nodes[want]
		if n == nil || n.State != model.StateSucceeded {
			t.Fatalf("node %s missing or state=%v", want, n)
		}
	}
	// Exactly one "dynamically spawned" event per unique new node.
	spawnEvents := 0
	for _, ev := range rec.History.Events {
		if ev.Detail == "dynamically spawned" {
			spawnEvents++
			if ev.NodeID == "seed" {
				t.Fatal("existing node must not be recorded as spawned twice")
			}
		}
	}
	if spawnEvents != 2 {
		t.Fatalf("spawn events=%d want 2 (x, y)", spawnEvents)
	}
	if got := len(rec.Status.Nodes["seed"].Spawned); got != 2 {
		t.Fatalf("seed.Spawned=%v want [x y]", rec.Status.Nodes["seed"].Spawned)
	}
}

func TestSpawnInvalidCycleFailsProducingNode(t *testing.T) {
	s := runSpec(nn("seed", 1))
	logSpec(t, "[SPAWNBAD]", s)
	reg := ResolverFunc(func(id string) (Task, bool) {
		if id == "seed" {
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				// x depends on itself -> invalid batch
				return &ExecResult{Spawn: []SpawnNode{
					{Node: spec.NodeSpec{ID: "x", Deps: []string{"x"}, MaxAttempts: 1}},
				}}, nil
			}, true
		}
		return taskOK, true
	})
	eng := New(newMemStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, err := eng.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[SPAWNBAD]", rec)
	n := rec.Status.Nodes["seed"]
	if n.State != model.StateFailed || n.FailCode != model.FailInvalidSpawn {
		t.Fatalf("seed=%s/%s want failed/invalid_spawn", n.State, n.FailCode)
	}
	if _, exists := rec.Status.Nodes["x"]; exists {
		t.Fatal("rejected spawn node x must not enter the graph")
	}
	if rec.Status.State != model.RunFailed {
		t.Fatalf("run=%s want failed", rec.Status.State)
	}
}

func TestSpawnDanglingRejected(t *testing.T) {
	s := runSpec(nn("seed", 1))
	reg := ResolverFunc(func(id string) (Task, bool) {
		if id == "seed" {
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				return &ExecResult{Spawn: []SpawnNode{
					{Node: spec.NodeSpec{ID: "x", Deps: []string{"ghost"}, MaxAttempts: 1}},
				}}, nil
			}, true
		}
		return taskOK, true
	})
	eng := New(newMemStore(), reg, Config{})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, _ := eng.Wait(context.Background(), id)
	logTransitions(t, "[SPAWNDAN]", rec)
	if rec.Status.Nodes["seed"].FailCode != model.FailInvalidSpawn {
		t.Fatalf("seed code=%s", rec.Status.Nodes["seed"].FailCode)
	}
	if got := rec.Status.Nodes["seed"].FailMessage; !contains(got, "dangling") {
		t.Fatalf("failure reason should mention dangling dependency, got %q", got)
	}
}

func TestSpawnCapRejectsUnboundedGrowth(t *testing.T) {
	s := runSpec(nn("seed", 1))
	reg := ResolverFunc(func(id string) (Task, bool) {
		if id == "seed" {
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				return &ExecResult{Spawn: []SpawnNode{
					{Node: spec.NodeSpec{ID: "x", MaxAttempts: 1}},
					{Node: spec.NodeSpec{ID: "y", MaxAttempts: 1}},
				}}, nil
			}, true
		}
		return taskOK, true
	})
	eng := New(newMemStore(), reg, Config{MaxNodesPerRun: 2})
	defer eng.Shutdown(context.Background())
	id, _ := eng.Submit(context.Background(), s)
	rec, _ := eng.Wait(context.Background(), id)
	logTransitions(t, "[SPAWNCAP]", rec)
	if rec.Status.Nodes["seed"].FailCode != model.FailInvalidSpawn {
		t.Fatalf("expected cap rejection, seed=%s", rec.Status.Nodes["seed"].State)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
