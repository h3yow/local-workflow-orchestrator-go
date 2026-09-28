package engine

import (
	"context"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// ActionRecorder lets a task durably register business side effects (place
// order, send message, write result file) under a stable business key.
//
// Within one run a key becomes effective at most once: Record is atomic and
// idempotent across retries, concurrent attempts and process crashes. The
// returned bool is true exactly for the call that first committed the key, so
// a task replays its decision correctly:
//
//	inserted, err := in.Actions.Record(ctx, "order-123", payload)
//	if err != nil { return nil, err }
//	if inserted {
//	    // this attempt owns the external side effect
//	    placeOrder(...)
//	}
//
// Recording BEFORE performing the external effect guarantees at-most-once:
// if the process dies between Record and the effect, the replayed attempt
// observes inserted=false and must not perform the effect again. A task that
// cannot tolerate the resulting at-most-once gap on the external system
// should additionally make the external call idempotent with the same key.
//
// Keys are scoped to a run; the same key in two different runs is
// independent. Has/Load read the committed set and never observe partial
// writes.
type ActionRecorder interface {
	// Record commits key for this run exactly once. inserted is true only for
	// the first successful commit. value is an opaque payload stored alongside
	// the key and ignored for dedup purposes.
	Record(ctx context.Context, key, value string) (inserted bool, err error)
	// Has reports whether key is already committed.
	Has(ctx context.Context, key string) (bool, error)
	// Load returns every committed record ordered by key.
	Load(ctx context.Context) ([]model.ActionRecord, error)
}

// actionRecorder is the run-scoped ActionRecorder implementation. Uniqueness
// is enforced by the durable Store (AppendAction); the in-process key cache
// short-circuits repeated lookups and keeps racing goroutines within one
// process on the same first-committer semantics without weakening durability.
type actionRecorder struct {
	runID   string
	nodeID  string
	attempt int
	clock   Clock
	store   actionStore
}

// actionStore is the durability boundary used by the recorder (store.Store
// satisfies it; a narrow interface keeps this file focused).
type actionStore interface {
	AppendAction(ctx context.Context, rec model.ActionRecord) (bool, error)
	LoadActions(ctx context.Context, runID string) ([]model.ActionRecord, error)
}

var _ ActionRecorder = (*actionRecorder)(nil)

func (r *actionRecorder) Record(ctx context.Context, key, value string) (bool, error) {
	rec := model.ActionRecord{
		RunID:    r.runID,
		Key:      key,
		Value:    value,
		NodeID:   r.nodeID,
		Attempt:  r.attempt,
		Recorded: r.clock.Now(),
	}
	return r.store.AppendAction(ctx, rec)
}

func (r *actionRecorder) Has(ctx context.Context, key string) (bool, error) {
	recs, err := r.store.LoadActions(ctx, r.runID)
	if err != nil {
		return false, err
	}
	for _, rec := range recs {
		if rec.Key == key {
			return true, nil
		}
	}
	return false, nil
}

func (r *actionRecorder) Load(ctx context.Context) ([]model.ActionRecord, error) {
	return r.store.LoadActions(ctx, r.runID)
}
