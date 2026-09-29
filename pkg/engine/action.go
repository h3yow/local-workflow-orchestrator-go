package engine

import (
	"context"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// ActionRecorder is handed to a task attempt so business side effects (placing
// an order, sending a message, writing a result file) can be registered under
// a stable business key.
//
// Dedup scope and guarantee:
//
//   - the key is unique within the whole run: whether the same key is
//     committed by two different nodes, by the same node on a later attempt,
//     after a crash-and-resume replay, or from racing goroutines, it takes
//     effect exactly once. The first committer performs the side effect;
//     every later committer receives the already-stored record;
//
//   - different runs never collide, even when they happen to use the same key
//     string; within one run, by contrast, every node shares one key space;
//
//   - Commit is durable before it returns committed=true, so callers can use
//     the result as an effect gate:
//
//     rec, committed, err := in.Actions.Commit(ctx, "order-42", payload)
//     if err != nil { return nil, err }
//     if committed {
//     // only this caller performs the external side effect
//     }
//     // rec is the single surviving record, queryable and replayable later.
type ActionRecorder interface {
	// Commit records the action identified by key with an optional replay
	// payload. It returns the stored record and true on the first commit of
	// the key; if the key was already committed it returns the pre-existing
	// record and false without mutating anything.
	Commit(ctx context.Context, key, value string) (*model.ActionRecord, bool, error)
}

// actionRecorder is the attempt-scoped implementation.
type actionRecorder struct {
	engine  *Engine
	runID   string
	nodeID  string
	attempt int
}

func (r *actionRecorder) Commit(ctx context.Context, key, value string) (*model.ActionRecord, bool, error) {
	rec := &model.ActionRecord{
		RunID:       r.runID,
		NodeID:      r.nodeID,
		Key:         key,
		Attempt:     r.attempt,
		Value:       value,
		CommittedAt: r.engine.clock.Now(),
	}
	return r.engine.store.CommitAction(ctx, rec)
}

var _ ActionRecorder = (*actionRecorder)(nil)
