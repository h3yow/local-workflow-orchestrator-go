package engine

import (
	"context"
	"sort"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// flushEvents assigns deterministic Seq numbers to buffered transitions.
// Events are normalised globally by node id while preserving each node's own
// chronological order, removing the non-determinism of concurrent completions
// (independent nodes finishing in either order): identical specifications
// yield identical event streams. Flushing is batched by the loop at stable
// barriers (see loop), so events of one scheduling wave are ordered together.
func (sc *scheduler) flushEvents(ctx context.Context) {
	sc.mu.Lock()
	if len(sc.pending) == 0 {
		sc.mu.Unlock()
		return
	}
	events := make([]model.SnapshotEvent, 0, len(sc.pending))
	now := sc.engine.clock.Now()
	sc.mu.Unlock()

	sc.mu.Lock()
	byNode := map[string][]pendingEvent{}
	for _, p := range sc.pending {
		byNode[p.nodeID] = append(byNode[p.nodeID], p)
	}
	order := make([]string, 0, len(byNode))
	for id := range byNode {
		order = append(order, id)
	}
	sort.Strings(order)
	for _, id := range order {
		for _, p := range byNode[id] {
			sc.seq++
			events = append(events, model.SnapshotEvent{
				Seq: sc.seq, RunID: sc.entry.id, At: now,
				NodeID: p.nodeID, From: p.from, To: p.to,
				Attempt: p.attempt, Code: p.code, Detail: p.detail,
			})
		}
	}
	sc.pending = sc.pending[:0]
	sc.mu.Unlock()

	for i := range events {
		// Idempotent append: a Seq already stored (crash/replay) is ignored.
		if err := sc.engine.store.AppendEvent(ctx, &events[i]); err != nil {
			return
		}
		sc.engine.publish(sc.entry.id, events[i])
	}
}
