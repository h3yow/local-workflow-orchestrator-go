// Command orchestrator runs a small demonstration workflow that exercises
// dependencies, concurrency limits, retries, timeout-less failure, skip
// propagation and dynamic expansion, persisting to a local directory.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/engine"
	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/store"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

func main() {
	dir := filepath.Join(os.TempDir(), "lwo-demo")
	st, err := store.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}
	e, err := engine.New(engine.Config{
		MaxRunning: 2,
		MaxQueue:   8,
		Store:      st,
		Backoff:    backoff.Policy{Kind: backoff.Exponential, Base: 5 * time.Millisecond, Max: 100 * time.Millisecond},
		Logger:     stdLogger{},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()

	// a -> b(flaky, retries) -> d (skipped because b eventually fails)
	// a -> c ----------------/
	s := &spec.Spec{
		ID: "demo",
		Nodes: []spec.NodeSpec{
			{ID: "a", Slots: 1},
			{ID: "b", Deps: []string{"a"}, Slots: 1, MaxRetries: 2},
			{ID: "c", Deps: []string{"a"}, Slots: 1},
			{ID: "d", Deps: []string{"b", "c"}, Slots: 1},
		},
	}

	var bAttempts int
	exec := func(ctx context.Context, runID, nodeID string) (string, []string, error) {
		if nodeID == "b" {
			bAttempts++
			return "", nil, fmt.Errorf("b always fails (attempt %d)", bAttempts)
		}
		return nodeID + "-result", []string{"effect:" + nodeID}, nil
	}

	ctx := context.Background()
	runID, err := e.Create(ctx, s, exec, engine.Option{RunID: "demo-run"})
	if err != nil {
		log.Fatalf("create: %v", err)
	}
	rs, err := e.Wait(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== Final node states ===")
	for _, id := range []string{"a", "b", "c", "d"} {
		nd := rs.Nodes[id]
		fmt.Printf("node=%-2s status=%-9s attempts=%d reason=%q\n",
			id, nd.Status, nd.AttemptCount, nd.FailureReason)
	}
	fmt.Printf("run=%s\n", rs.Status)

	if fx, err := e.Effects(ctx, runID); err == nil {
		fmt.Printf("effects=%v (deduplicated, persisted)\n", fx)
	}
}
