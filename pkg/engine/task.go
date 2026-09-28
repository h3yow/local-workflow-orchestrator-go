package engine

import (
	"context"
	"errors"
)

// permanentErr marks a task error as non-retryable.
type permanentErr struct{ cause error }

func (e *permanentErr) Error() string {
	if e.cause == nil {
		return ErrPermanent.Error()
	}
	return ErrPermanent.Error() + ": " + e.cause.Error()
}
func (e *permanentErr) Unwrap() []error { return []error{ErrPermanent, e.cause} }

// ErrPermanent wraps an execution error so that the scheduler fails the node
// immediately instead of retrying.
var ErrPermanent = errors.New("permanent task failure")

// Permanent wraps err as non-retryable. A nil err yields the sentinel.
func Permanent(err error) error {
	return &permanentErr{cause: err}
}

// IsPermanent reports whether err must not be retried.
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }

// TaskInput is handed to every task invocation.
type TaskInput struct {
	RunID   string
	NodeID  string
	Attempt int // 0-based attempt number
	// Actions registers externally-visible business effects with run-scoped
	// exactly-once dedup; it is always non-nil during engine execution.
	Actions ActionRecorder
}

// Task is the executable unit bound to a node id.
type Task func(ctx context.Context, in TaskInput) (*ExecResult, error)

// ExecResult is what a task returns.
type ExecResult struct {
	Value string
	// Spawn declares follow-up nodes produced by this completion.
	Spawn []SpawnNode
}

// Resolver binds node ids (including dynamically spawned ones) to tasks.
type Resolver interface {
	Resolve(nodeID string) (Task, bool)
}

// ResolverFunc adapts a function to Resolver.
type ResolverFunc func(nodeID string) (Task, bool)

func (f ResolverFunc) Resolve(nodeID string) (Task, bool) { return f(nodeID) }

// MapRegistry is a static id -> Task resolver.
type MapRegistry map[string]Task

// Resolve looks up a task.
func (m MapRegistry) Resolve(nodeID string) (Task, bool) {
	t, ok := m[nodeID]
	return t, ok
}

// StaticResolver returns a resolver that always resolves a node to t.
func StaticResolver(t Task) Resolver {
	return ResolverFunc(func(string) (Task, bool) { return t, true })
}
