package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

func TestSnapshotRoundTrip(t *testing.T) {
	st, err := NewFileStore(filepath.Join(t.TempDir(), "s"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rs := &state.RunState{RunID: "r1", Status: state.Running,
		Nodes: map[string]*state.NodeState{"a": {ID: "a", Status: state.Succeeded}}}
	if err := st.SaveSnapshot(ctx, rs); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadSnapshot(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Nodes["a"].Status != state.Succeeded {
		t.Fatalf("status=%s", got.Nodes["a"].Status)
	}
	ids, _ := st.ListRuns(ctx)
	if len(ids) != 1 || ids[0] != "r1" {
		t.Fatalf("ids=%v", ids)
	}
}

func TestEventDedup(t *testing.T) {
	st, err := NewFileStore(filepath.Join(t.TempDir(), "s"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ev := Event{Key: "effect:a:x", Kind: "effect", Detail: "x"}
	ok, err := st.AppendEvent(ctx, "r1", ev)
	if err != nil || !ok {
		t.Fatalf("first append ok=%v err=%v", ok, err)
	}
	ok2, err := st.AppendEvent(ctx, "r1", ev)
	if err != nil || ok2 {
		t.Fatalf("duplicate append must be no-op ok=false, got ok=%v err=%v", ok2, err)
	}
	evs, _ := st.ListEvents(ctx, "r1")
	if len(evs) != 1 {
		t.Fatalf("events=%d", len(evs))
	}
}

func TestLoadNotFound(t *testing.T) {
	st, _ := NewFileStore(filepath.Join(t.TempDir(), "s"))
	if _, err := st.LoadSnapshot(context.Background(), "nope"); err != ErrNotFound {
		t.Fatalf("err=%v", err)
	}
}
