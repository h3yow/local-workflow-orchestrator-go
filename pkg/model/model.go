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
	Spec    *spec.Spec
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
