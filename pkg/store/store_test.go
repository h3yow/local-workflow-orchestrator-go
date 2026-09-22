package store

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
)

func mkStatus(id string, rev int64) *model.RunStatus {
	return &model.RunStatus{
		RunID: id, SpecName: "s", State: model.RunRunning, Revision: rev,
		UpdatedAt: time.Unix(int64(rev), 0),
		Nodes: map[string]*model.NodeStatus{
			"a": {ID: "a", State: model.StateSucceeded, Attempt: 1,
				Attempts: []model.AttemptRecord{{Attempt: 0}}},
		},
		NodeOrder: []string{"a"},
		Spec:      &spec.Spec{Name: "s", MaxParallel: 1, Nodes: []spec.NodeSpec{{ID: "a", MaxAttempts: 1}}},
	}
}

func storeCases(t *testing.T) map[string]Store {
	mem := NewMemoryStore()
	file, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return map[string]Store{"memory": mem, "file": file}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			in := mkStatus("run1", 1)
			if err := st.SaveStatus(ctx, in); err != nil {
				t.Fatal(err)
			}
			out, err := st.LoadStatus(ctx, "run1")
			if err != nil {
				t.Fatal(err)
			}
			if out.Revision != 1 || out.Nodes["a"].State != model.StateSucceeded || out.Spec.MaxParallel != 1 {
				t.Fatalf("round trip mismatch: %+v", out)
			}
			// Mutating the returned copy must not affect stored state.
			out.Nodes["a"].State = model.StateFailed
			out2, _ := st.LoadStatus(ctx, "run1")
			if out2.Nodes["a"].State != model.StateSucceeded {
				t.Fatal("store leaked mutable state to readers")
			}
			if _, err := st.LoadStatus(ctx, "missing"); err != model.ErrNotFound {
				t.Fatalf("want ErrNotFound, got %v", err)
			}
		})
	}
}

func TestEventLogIdempotentAndOrdered(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_ = st.SaveStatus(ctx, mkStatus("r9", 1))
			ev := func(seq int64, node string) *model.SnapshotEvent {
				return &model.SnapshotEvent{Seq: seq, RunID: "r9", NodeID: node, At: time.Now()}
			}
			for _, e := range []*model.SnapshotEvent{ev(1, "a"), ev(2, "b"), ev(3, "c")} {
				if err := st.AppendEvent(ctx, e); err != nil {
					t.Fatal(err)
				}
			}
			// Replays (same Seq) must not duplicate records.
			if err := st.AppendEvent(ctx, ev(2, "b")); err != nil {
				t.Fatal(err)
			}
			h, err := st.LoadHistory(ctx, "r9")
			if err != nil {
				t.Fatal(err)
			}
			if len(h.Events) != 3 {
				t.Fatalf("history len=%d want 3 (duplicate append leaked)", len(h.Events))
			}
			// File store reopened: history survives a restart.
			if name == "file" {
				dir := st.(*FileStore).dir
				reopened, err := NewFileStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				h2, err := reopened.LoadHistory(ctx, "r9")
				if err != nil {
					t.Fatal(err)
				}
				if len(h2.Events) != 3 {
					t.Fatalf("reopened history len=%d", len(h2.Events))
				}
				s2, err := reopened.LoadStatus(ctx, "r9")
				if err != nil || s2.Revision != 1 {
					t.Fatalf("reopened snapshot: %+v err=%v", s2, err)
				}
			}
		})
	}
}

func TestListRunsRetained(t *testing.T) {
	for name, st := range storeCases(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_ = st.SaveStatus(ctx, mkStatus("zeta", 1))
			_ = st.SaveStatus(ctx, mk("alpha", 1))
			ids, err := st.ListRuns(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// Memory preserves insertion order; file is lexicographic; just
			// require both runs retained.
			if len(ids) != 2 {
				t.Fatalf("retained runs=%v", ids)
			}
		})
	}
}

func mk(id string, rev int64) *model.RunStatus {
	s := mkStatus(id, rev)
	s.Nodes = map[string]*model.NodeStatus{"a": {ID: "a", State: model.StatePending}}
	return s
}
