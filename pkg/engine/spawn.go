package engine

import "github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"

// SpawnNode is a node dynamically produced by a completed node.
type SpawnNode struct {
	Node spec.NodeSpec
}

// SpawnOutcome reports how a batch of spawned nodes was applied.
type SpawnOutcome struct {
	Applied []string // node ids accepted into the graph
	Skipped []string // duplicate spawns already present: only first effective
}
