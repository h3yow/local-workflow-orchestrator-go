// Package model holds workflow run state, events and historical records.
package model

import (
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
)

// NodeState is the lifecycle state of a node within a run.
type NodeState string

const (
	StatePending   NodeState = "pending"
	StateReady     NodeState = "ready"
	StateQueued    NodeState = "queued"
	StateRunning   NodeState = "running"
	StateSucceeded NodeState = "succeeded"
	StateFailed    NodeState = "failed"
	StateSkipped   NodeState = "skipped"
)

// IsTerminal reports whether a node state never changes again.
func (s NodeState) IsTerminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateSkipped
}

// Run-level states reuse succeeded/failed; running and pending as well.
const (
	RunPending   = StatePending
	RunRunning   = StateRunning
	RunSucceeded = StateSucceeded
	RunFailed    = StateFailed
)

// FailureCode distinguishes terminal failure/skip reasons.
type FailureCode string

const (
	FailNone              FailureCode = ""
	FailAttemptsExhausted FailureCode = "attempts_exhausted"
	FailTimeout           FailureCode = "timeout"
	FailPermanent         FailureCode = "permanent"
	FailInvalidSpawn      FailureCode = "invalid_spawn"
	FailUpstreamFailed    FailureCode = "upstream_failed"
	FailRecoveredRunning  FailureCode = "recovered_running"
)

// NodeStatus is the persisted status of one node.
type NodeStatus struct {
	ID             string
	State          NodeState
	Attempt        int // number of attempts already started (0 before first)
	FailCode       FailureCode
	FailMessage    string
	SkippedBecause string
	StartedAt      time.Time
	FinishedAt     time.Time
	Spawned        []string
	Attempts       []AttemptRecord
	// RetryReadyAt is the absolute wall-clock time at which a node parked in
	// StateReady becomes eligible for its next attempt. It is persisted so a
	// restart during the backoff wait resumes with the REMAINING wait instead
	// of retrying immediately or starting a fresh wait. It is nil unless the
	// node is currently parked waiting for a retry.
	RetryReadyAt *time.Time
}

// Clone returns a deep copy.
func (n *NodeStatus) Clone() *NodeStatus {
	if n == nil {
		return nil
	}
	cp := *n
	if n.Spawned != nil {
		cp.Spawned = append([]string(nil), n.Spawned...)
	}
	if n.Attempts != nil {
		cp.Attempts = append([]AttemptRecord(nil), n.Attempts...)
	}
	if n.RetryReadyAt != nil {
		t := *n.RetryReadyAt
		cp.RetryReadyAt = &t
	}
	return &cp
}

// AttemptRecord captures one execution attempt of a node.
type AttemptRecord struct {
	Attempt  int
	Started  time.Time
	Finished time.Time
	TimedOut bool
	Err      string
	Result   string
}

// RunStatus is the persisted state of one workflow run.
type RunStatus struct {
	RunID     string
	SpecName  string
	Spec      *spec.Spec // full declarative spec, refreshed on dynamic spawn
	State     NodeState
	Nodes     map[string]*NodeStatus
	NodeOrder []string
	CreatedAt time.Time
	UpdatedAt time.Time
	Revision  int64
}

// Clone returns a deep copy safe for observers to read without holding locks.
func (r *RunStatus) Clone() *RunStatus {
	if r == nil {
		return nil
	}
	cp := *r
	cp.NodeOrder = append([]string(nil), r.NodeOrder...)
	if r.Spec != nil {
		cp.Spec = cloneSpec(r.Spec)
	}
	cp.Nodes = make(map[string]*NodeStatus, len(r.Nodes))
	for k, v := range r.Nodes {
		cp.Nodes[k] = v.Clone()
	}
	return &cp
}

// SnapshotEvent marks a durable state transition.
type SnapshotEvent struct {
	Seq     int64
	RunID   string
	At      time.Time
	NodeID  string
	From    NodeState
	To      NodeState
	Attempt int
	Code    FailureCode
	Detail  string
}

// History is an ordered, append-only list of transitions for a run.
type History struct {
	RunID  string
	Events []SnapshotEvent
}

// Clone returns a deep copy.
func (h *History) Clone() *History {
	if h == nil {
		return nil
	}
	cp := *h
	cp.Events = append([]SnapshotEvent(nil), h.Events...)
	return &cp
}

// RunRecord is the reproducible record of a finished or running run.
type RunRecord struct {
	Status  *RunStatus
	History *History
	Actions *ActionLog
	Spec    *spec.Spec
}

// ActionRecord is the durable, deduped record of one business side effect.
// Within a run the business Key is unique across all nodes: the first
// successful commit for a key wins and every later commit with the same key
// (from another node, a retried attempt or a post-restart/recovery replay)
// returns Committed=false and the pre-existing record without inserting
// another one. Records survive process restarts and are replayable; they never
// interact across runs (the run id is the storage namespace).
type ActionRecord struct {
	// Seq is the run-scoped, strictly increasing commit order (assigned by the
	// store atomically with the insert).
	Seq int64
	// RunID scopes the deduplication domain.
	RunID string
	// NodeID identifies the node whose execution committed the action.
	NodeID string
	// Key is the stable business identifier of the action (e.g. order id).
	Key string
	// Attempt is the 0-based attempt on which the action first took effect.
	Attempt int
	// Value is an optional, business-defined replay payload.
	Value string
	// CommittedAt is when the unique record was durably stored.
	CommittedAt time.Time
}

// Clone returns a deep copy.
func (a *ActionRecord) Clone() *ActionRecord {
	if a == nil {
		return nil
	}
	cp := *a
	return &cp
}

// ActionLog is the replayable set of a run's committed business actions.
type ActionLog struct {
	RunID   string
	Actions []ActionRecord
}

// Clone returns a deep copy.
func (l *ActionLog) Clone() *ActionLog {
	if l == nil {
		return nil
	}
	cp := *l
	cp.Actions = append([]ActionRecord(nil), l.Actions...)
	return &cp
}

// Lookup returns the record committed with key and true, or nil/false.
func (l *ActionLog) Lookup(key string) (*ActionRecord, bool) {
	for i := range l.Actions {
		if l.Actions[i].Key == key {
			return &l.Actions[i], true
		}
	}
	return nil, false
}

// cloneSpec deep-copies a spec (the spec package owns validation; mirroring
// the struct here keeps model free of import cycles).
func cloneSpec(s *spec.Spec) *spec.Spec {
	cp := *s
	cp.Nodes = make([]spec.NodeSpec, len(s.Nodes))
	for i, n := range s.Nodes {
		cn := n
		if n.Deps != nil {
			cn.Deps = append([]string(nil), n.Deps...)
		}
		cp.Nodes[i] = cn
	}
	return &cp
}
