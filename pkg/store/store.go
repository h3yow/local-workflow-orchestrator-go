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
	// AppendAction durably records a business action. The (RunID, Key) pair is
	// idempotent: if Key was already recorded for the run the existing record
	// wins and inserted reports false. A true result means this call's record
	// was committed for the first time, i.e. the action became effective. The
	// operation is atomic under concurrency, so racing attempts of one Key
	// produce exactly one committed record.
	AppendAction(ctx context.Context, rec model.ActionRecord) (inserted bool, err error)
	// LoadActions returns all action records for a run ordered by Key, so all
	// observers (including after restart) reproduce the identical action set.
	LoadActions(ctx context.Context, runID string) ([]model.ActionRecord, error)
}
