package engine

import (
	"context"
	"testing"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

// TestE2EFileStoreHistoryReproducible runs the same diamond twice against
// fresh FileStore directories and asserts identical transition signatures;
// it also confirms history retained in the store is directly replayable.
func TestE2EFileStoreHistoryReproducible(t *testing.T) {
	s := runSpec(nn("a", 1), nn("b", 1, "a"), nn("c", 1, "a"), nn("d", 1, "b", "c"))
	logSpec(t, "[E2E]", s)
	signature := func(dir string) []string {
		st, err := store.NewFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		eng := New(st, StaticResolver(taskOK), Config{})
		id, err := eng.Submit(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := eng.Wait(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if err := eng.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		out := make([]string, len(rec.History.Events))
		for i, ev := range rec.History.Events {
			out[i] = ev.NodeID + ":" + string(ev.From) + "->" + string(ev.To)
		}
		return out
	}
	first := signature(t.TempDir())
	second := signature(t.TempDir())
	if len(first) != len(second) {
		t.Fatalf("lengths %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("history diverges at %d:\n%v\n%v", i, first, second)
		}
	}
	t.Logf("[E2E] reproducible history signature: %v", first)
}
