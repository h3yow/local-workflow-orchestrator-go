// Package store persists run snapshots and transition history.
package store

import (
	"context"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// Store is the durability boundary for run state and history.
// Implementations must be safe for concurrent use and must never expose
// partially written state to readers.
type Store interface {
	// SaveStatus atomically stores a full run snapshot.
	SaveStatus(ctx context.Context, status *model.RunStatus) error
	// LoadStatus reads one run snapshot.
	LoadStatus(ctx context.Context, runID string) (*model.RunStatus, error)
	// ListRuns returns known run ids in creation order.
	ListRuns(ctx context.Context) ([]string, error)
	// AppendEvent durably records one transition; event Seq must be strictly
	// increasing per run and duplicate Seq values must not be stored twice.
	AppendEvent(ctx context.Context, ev *model.SnapshotEvent) error
	// LoadHistory replays the transition log of a run.
	LoadHistory(ctx context.Context, runID string) (*model.History, error)
	// CommitAction durably records one business side effect. Within a run the
	// business Key is unique ACROSS ALL NODES: the first call for a key (no
	// matter which node, attempt or post-restart replay registered it) inserts
	// and returns the stored record and true; later calls with the same key in
	// the same run change nothing and return the existing record and false.
	// Different runs never collide. The insert, sequence assignment and dedup
	// check must be one atomic operation so concurrent commits of the same key
	// can never both report true.
	CommitAction(ctx context.Context, rec *model.ActionRecord) (*model.ActionRecord, bool, error)
	// LoadActions replays the committed action set of a run in a deterministic
	// order (Seq ascending), identical before and after a restart and under
	// concurrent readers. An unknown run returns model.ErrNotFound.
	LoadActions(ctx context.Context, runID string) (*model.ActionLog, error)
}
