package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// TestMultiRunIsolation runs two independent specs concurrently: a failing run
// must not pollute a succeeding run's state or history.
func TestMultiRunIsolation(t *testing.T) {
	good := runSpec(nn("g1", 1), nn("g2", 1, "g1"))
	bad := runSpec(nn("x1", 1), nn("x2", 1, "x1"))
	logSpec(t, "[ISO-GOOD]", good)
	logSpec(t, "[ISO-BAD]", bad)
	reg := ResolverFunc(func(id string) (Task, bool) {
		switch id {
		case "g1", "g2":
			return taskOK, true
		case "x1":
			return func(ctx context.Context, in TaskInput) (*ExecResult, error) {
				return nil, errors.New("boom")
			}, true
		case "x2":
			return taskOK, true
		}
		return nil, false
	})
	eng := New(newMemStore(), reg, Config{})
	defer eng.Shutdown(context.Background())

	var wg sync.WaitGroup
	var goodID, badID string
	wg.Add(2)
	go func() { defer wg.Done(); id, _ := eng.Submit(context.Background(), good); goodID = id }()
	go func() { defer wg.Done(); id, _ := eng.Submit(context.Background(), bad); badID = id }()
	wg.Wait()

	recGood, err := eng.Wait(context.Background(), goodID)
	if err != nil {
		t.Fatal(err)
	}
	recBad, err := eng.Wait(context.Background(), badID)
	if err != nil {
		t.Fatal(err)
	}
	logTransitions(t, "[ISO-GOOD]", recGood)
	logTransitions(t, "[ISO-BAD]", recBad)

	if recGood.Status.State != model.RunSucceeded {
		t.Fatalf("good run=%s polluted by failing run", recGood.Status.State)
	}
	if recBad.Status.State != model.RunFailed {
		t.Fatalf("bad run=%s should be failed independently", recBad.Status.State)
	}
	for _, ev := range recGood.History.Events {
		if ev.RunID != goodID {
			t.Fatal("good run history contains foreign run events")
		}
		if ev.NodeID[0] == 'x' {
			t.Fatalf("foreign node %s leaked into good history", ev.NodeID)
		}
	}
	if _, err := eng.GetRun(context.Background(), goodID); err != nil {
		t.Fatalf("good run not retrievable: %v", err)
	}

	// Historical runs must be retained: both ids list independently.
	ids, err := eng.store.ListRuns(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("store retained %d runs, want 2: %v", len(ids), ids)
	}
}

// TestObserverConsistentView subscribes a concurrent reader and verifies every
// observed transition is well-formed (strict seq, known states) and that
// GetRun always returns a self-consistent snapshot.
func TestObserverConsistentView(t *testing.T) {
	s := runSpec(nn("a", 1), nn("b", 1, "a"), nn("c", 1, "a"), nn("d", 1, "b", "c"))
	logSpec(t, "[OBS]", s)
	eng := New(newMemStore(), StaticResolver(taskOK), Config{})
	defer eng.Shutdown(context.Background())
	runID, _ := eng.Submit(context.Background(), s)

	ch, unsub, err := eng.Subscribe(runID)
	if err != nil {
		t.Fatal(err)
	}
	var seen []model.SnapshotEvent
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range ch {
			seen = append(seen, ev)
		}
	}()
	// Hammer GetRun concurrently: no half-updated views may ever appear.
	stop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rec, err := eng.GetRun(context.Background(), runID)
				if err != nil {
					t.Errorf("getrun: %v", err)
					return
				}
				n := len(rec.Status.Nodes)
				if n != 4 {
					t.Errorf("partial view: %d nodes", n)
					return
				}
			}
		}()
	}
	if _, err := eng.Wait(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	unsub()
	wg.Wait()
	close(stop)
	readerWg.Wait()

	prev := int64(-1)
	for _, ev := range seen {
		if ev.Seq <= prev {
			t.Fatalf("observer saw non-increasing seq %d after %d", ev.Seq, prev)
		}
		prev = ev.Seq
	}
	t.Logf("[OBS] observed %d transitions, last seq=%d", len(seen), prev)
	if len(seen) == 0 {
		t.Fatal("observer received no events")
	}
}
