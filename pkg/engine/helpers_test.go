package engine

import (
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

func newMemStore() *store.MemoryStore { return store.NewMemoryStore() }

func specNode(id string, attempts int, bo backoff.Policy) spec.NodeSpec {
	return spec.NodeSpec{ID: id, MaxAttempts: attempts, Backoff: bo}
}
