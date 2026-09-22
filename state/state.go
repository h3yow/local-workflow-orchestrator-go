// Package state defines the durable, externally observable state model of
// workflow runs and their nodes.
package state

import "time"

// Status is the externally visible lifecycle state of a node or run.
type Status string

const (
	// Pending: not yet eligible (dependencies unmet) or not admitted to
	// the bounded ready queue.
	Pending Status = "pending"
	// Queued: eligible and admitted; waiting for a concurrency slot
	// (also used while backing off between attempts).
	Queued Status = "queued"
	// Running: an attempt is in progress.
	Running Status = "running"
	// Succeeded: terminal success.
	Succeeded Status = "succeeded"
	// Failed: terminal failure (retries exhausted or hard timeout).
	Failed Status = "failed"
	// Skipped: terminal; an upstream dependency failed/was skipped.
	Skipped Status = "skipped"
)

// AttemptOutcome classifies how a single attempt ended.
type AttemptOutcome string

const (
	OutcomeSuccess     AttemptOutcome = "success"
	OutcomeError       AttemptOutcome = "error"
	OutcomeTimeout     AttemptOutcome = "timeout"
	OutcomeInterrupted AttemptOutcome = "interrupted" // process died mid-attempt
)

// Attempt is the immutable record of one execution attempt.
type Attempt struct {
	Index      int            `json:"index"`
	StartedAt  time.Time      `json:"started_at"`
	FinishedAt time.Time      `json:"finished_at"`
	Outcome    AttemptOutcome `json:"outcome"`
	Reason     string         `json:"reason,omitempty"`
}

// NodeState is the persisted state of one node.
type NodeState struct {
	ID         string   `json:"id"`
	Deps       []string `json:"deps"`
	Slots      int      `json:"slots"`
	Timeout    int64    `json:"timeout_ns"`
	MaxRetries int      `json:"max_retries"`

	Status        Status    `json:"status"`
	AttemptCount  int       `json:"attempt_count"`
	Attempts      []Attempt `json:"attempts,omitempty"`
	Result        string    `json:"result,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	OrderKey      int64     `json:"order_key"` // stable scheduling tie-break
	ExpandedBy    string    `json:"expanded_by,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// RunState is the persisted state of one run.
type RunState struct {
	RunID     string                `json:"run_id"`
	SpecID    string                `json:"spec_id"`
	Status    Status                `json:"status"`
	Nodes     map[string]*NodeState `json:"nodes"`
	Effects   []string              `json:"effects,omitempty"` // deduped effect keys
	CreatedAt time.Time             `json:"created_at"`
	UpdatedAt time.Time             `json:"updated_at"`
	// Seq is the monotonic counter used to assign stable OrderKeys to
	// dynamically expanded nodes.
	Seq int64 `json:"seq"`
}

// Snapshot is an immutable deep copy of a run state safe for observers.
type Snapshot = RunState

// Terminal reports whether the status is one of the terminal states.
func (s Status) Terminal() bool {
	return s == Succeeded || s == Failed || s == Skipped
}

// Clone returns a deep copy safe to hand to concurrent observers.
func (rs *RunState) Clone() *RunState {
	if rs == nil {
		return nil
	}
	cp := *rs
	cp.Nodes = make(map[string]*NodeState, len(rs.Nodes))
	for k, v := range rs.Nodes {
		nc := *v
		nc.Deps = append([]string(nil), v.Deps...)
		if v.Attempts != nil {
			nc.Attempts = append([]Attempt(nil), v.Attempts...)
		}
		cp.Nodes[k] = &nc
	}
	if rs.Effects != nil {
		cp.Effects = append([]string(nil), rs.Effects...)
	}
	return &cp
}

// Terminal reports whether the run has reached a terminal status.
func (rs *RunState) Terminal() bool { return rs.Status.Terminal() }

// Count returns how many nodes currently have the given status.
func (rs *RunState) Count(s Status) int {
	n := 0
	for _, nd := range rs.Nodes {
		if nd.Status == s {
			n++
		}
	}
	return n
}
