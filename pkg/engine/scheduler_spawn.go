package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
)

// applySpawns validates and integrates a batch of dynamically produced nodes.
// Semantics:
//   - duplicate ids (within the batch or already in the graph) take effect at
//     most once: the first occurrence wins, later ones are reported as Skipped;
//   - the expanded graph is re-validated (numeric values, dangling deps,
//     cycles); any structural problem rejects the ENTIRE batch, so the graph
//     never becomes partially expanded;
//   - the hard engine node cap bounds memory growth: overflow is an explicit
//     failure (FailInvalidSpawn), never a silent drop.
func (sc *scheduler) applySpawns(ctx context.Context, batch []SpawnNode) (*SpawnOutcome, error) {
	out := &SpawnOutcome{}

	sc.mu.Lock()
	existing := make(map[string]struct{}, len(sc.status.Nodes))
	for id := range sc.status.Nodes {
		existing[id] = struct{}{}
	}
	seen := make(map[string]struct{})
	uniq := make([]spec.NodeSpec, 0, len(batch))
	for _, sp := range batch {
		id := sp.Node.ID
		if _, dup := existing[id]; dup {
			out.Skipped = append(out.Skipped, id)
			continue
		}
		if _, dup := seen[id]; dup {
			out.Skipped = append(out.Skipped, id)
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, sp.Node)
	}
	if len(existing)+len(uniq) > sc.engine.cfg.MaxNodesPerRun {
		sc.mu.Unlock()
		return nil, fmt.Errorf("spawn rejected: node count %d exceeds engine cap %d",
			len(existing)+len(uniq), sc.engine.cfg.MaxNodesPerRun)
	}
	cur := sc.graph.Spec()
	candidate := &spec.Spec{
		Name:        cur.Name,
		MaxParallel: cur.MaxParallel,
		MaxQueue:    cur.MaxQueue,
		Nodes:       make([]spec.NodeSpec, 0, len(cur.Nodes)+len(uniq)),
	}
	candidate.Nodes = append(candidate.Nodes, cur.Nodes...)
	candidate.Nodes = append(candidate.Nodes, uniq...)
	sc.mu.Unlock()

	// Full re-validation of the expanded graph, giving distinguishable reject
	// reasons (duplicate/dangling/cycle/invalid values).
	newGraph, err := spec.Build(candidate)
	if err != nil {
		return nil, fmt.Errorf("spawn validation failed: %w", err)
	}

	sc.mu.Lock()
	now := sc.engine.clock.Now()
	added := make([]string, 0, len(uniq))
	for _, n := range uniq {
		out.Applied = append(out.Applied, n.ID)
		sc.status.Nodes[n.ID] = &model.NodeStatus{ID: n.ID, State: model.StatePending}
		added = append(added, n.ID)
	}
	sc.graph = newGraph
	sc.status.Spec = candidate
	sc.status.NodeOrder = newGraph.IDs()
	sc.status.UpdatedAt = now
	sc.status.Revision++
	sort.Strings(added)
	for _, id := range added {
		sc.pending = append(sc.pending, pendingEvent{
			nodeID: id, from: model.StatePending, to: model.StatePending,
			detail: "dynamically spawned",
		})
	}
	snapshot := sc.status.Clone()
	sc.mu.Unlock()

	if err := sc.engine.store.SaveStatus(ctx, snapshot); err != nil {
		return nil, err
	}
	return out, nil
}
