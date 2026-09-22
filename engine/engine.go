// Package engine implements the concurrent, retry-aware, recoverable and
// dynamically extensible workflow scheduler.
//
// Concurrency model: each run is driven by a single serialized event loop
// (goroutine). State transitions therefore never race and observers always
// see a consistent snapshot. User task code runs on a bounded pool of worker
// goroutines; workers only report results back as events.
package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
	"github.com/h3yow/local-workflow-orchestrator-go/store"
)

// ExecFunc executes one node attempt. Returned effect keys are recorded
// exactly once across retries/recovery (idempotency anchors).
type ExecFunc func(ctx context.Context, runID, nodeID string) (result string, effectKeys []string, err error)

// ExpanderFunc is invoked after a node succeeds and may return new nodes
// to attach to the graph (dynamic expansion).
type ExpanderFunc func(runID, nodeID, result string) []spec.NodeSpec

// ErrBackpressure is returned when an expansion cannot be admitted because
// the bounded pending/ready queue is full.
var ErrBackpressure = errors.New("engine: queue full (backpressure)")

// ErrRunExists is returned on duplicate run creation.
var ErrRunExists = errors.New("engine: run already exists")

// ErrClosed is returned after the engine was closed.
var ErrClosed = errors.New("engine: closed")

// Config configures an Engine.
type Config struct {
	// MaxRunning bounds the sum of concurrently running node Slots. >= 1.
	MaxRunning int
	// MaxQueue bounds admitted ready/backing-off nodes (backpressure). >= 1.
	MaxQueue int
	// Store persists snapshots/events.
	Store store.Store
	// Backoff is the default retry policy for nodes.
	Backoff backoff.Policy
	// Logger receives one line per scheduling decision with its reason.
	// nil disables logging.
	Logger Logger
}

// Logger is the minimal logging surface used by the engine.
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Engine owns scheduling for any number of concurrently active runs.
type Engine struct {
	cfg    Config
	mu     sync.Mutex
	runs   map[string]*runCtx
	closed bool
	wg     sync.WaitGroup
}

// New constructs an Engine.
func New(cfg Config) (*Engine, error) {
	if cfg.MaxRunning < 1 {
		return nil, errors.New("engine: Config.MaxRunning must be >= 1")
	}
	if cfg.MaxQueue < 1 {
		return nil, errors.New("engine: Config.MaxQueue must be >= 1")
	}
	if cfg.Store == nil {
		return nil, errors.New("engine: Config.Store is required")
	}
	cfg.Backoff = cfg.Backoff.Normalize()
	if cfg.Logger == nil {
		cfg.Logger = discardLogger{}
	}
	return &Engine{cfg: cfg, runs: make(map[string]*runCtx)}, nil
}

// Close drains running loops and workers.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	runs := make([]*runCtx, 0, len(e.runs))
	for _, rc := range e.runs {
		runs = append(runs, rc)
	}
	e.mu.Unlock()
	for _, rc := range runs {
		rc.cancel()
	}
	e.wg.Wait()
	return nil
}

// Option tunes run creation.
type Option struct {
	// RunID pins the run id (useful for reproducible tests).
	RunID string
	// Expander produces dynamic successors on success, if any.
	Expander ExpanderFunc
}

// Create validates the spec, persists an initial snapshot and starts a run.
func (e *Engine) Create(ctx context.Context, s *spec.Spec, exec ExecFunc, opt Option) (string, error) {
	if exec == nil {
		return "", errors.New("engine: ExecFunc is required")
	}
	s.Normalize()
	if err := s.Validate(); err != nil {
		return "", err
	}
	runID := strings.TrimSpace(opt.RunID)
	if runID == "" {
		runID = fmt.Sprintf("run-%d", time.Now().UnixNano())
	}

	now := time.Now().UTC()
	rs := &state.RunState{
		RunID:     runID,
		SpecID:    s.ID,
		Status:    state.Pending,
		Nodes:     make(map[string]*state.NodeState, len(s.Nodes)),
		CreatedAt: now,
		UpdatedAt: now,
	}
	for i := range s.Nodes {
		n := &s.Nodes[i]
		rs.Nodes[n.ID] = &state.NodeState{
			ID:         n.ID,
			Deps:       append([]string(nil), n.Deps...),
			Slots:      n.Slots,
			Timeout:    int64(n.Timeout),
			MaxRetries: n.MaxRetries,
			Status:     state.Pending,
			OrderKey:   int64(i + 1),
			UpdatedAt:  now,
		}
		rs.Seq++
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return "", ErrClosed
	}
	if _, exists := e.runs[runID]; exists {
		e.mu.Unlock()
		return "", fmt.Errorf("%w: %s", ErrRunExists, runID)
	}
	if _, err := e.cfg.Store.LoadSnapshot(ctx, runID); err == nil {
		e.mu.Unlock()
		return "", fmt.Errorf("%w: %s", ErrRunExists, runID)
	} else if !errors.Is(err, store.ErrNotFound) {
		e.mu.Unlock()
		return "", err
	}
	rc := newRunCtx(runID, e, exec, opt.Expander)
	e.runs[runID] = rc
	e.mu.Unlock()

	if err := e.cfg.Store.SaveSnapshot(ctx, rs); err != nil {
		e.forget(runID)
		return "", err
	}
	rc.bootstrap(rs)
	e.wg.Add(1)
	go e.loop(rc)
	return runID, nil
}

// Resume continues a previously persisted run after a process restart.
// Nodes captured in the running state are NOT marked succeeded: their last
// attempt is recorded as interrupted and re-attempted.
func (e *Engine) Resume(ctx context.Context, runID string, exec ExecFunc, expander ExpanderFunc) error {
	if exec == nil {
		return errors.New("engine: ExecFunc is required")
	}
	rs, err := e.cfg.Store.LoadSnapshot(ctx, runID)
	if err != nil {
		return err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	if _, exists := e.runs[runID]; exists {
		e.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrRunExists, runID)
	}
	rc := newRunCtx(runID, e, exec, expander)
	e.runs[runID] = rc
	e.mu.Unlock()

	// Recovery: replay event log (effects dedupe) and reconcile running nodes.
	events, err := e.cfg.Store.ListEvents(ctx, runID)
	if err != nil {
		e.forget(runID)
		return err
	}
	now := time.Now().UTC()
	for _, ev := range events {
		if ev.Kind == kindEffect && !contains(rs.Effects, ev.Detail) {
			rs.Effects = append(rs.Effects, ev.Detail)
		}
		if ev.Seq > rs.Seq {
			rs.Seq = ev.Seq
		}
	}
	recovered := 0
	for _, nd := range rs.Nodes {
		if nd.Status == state.Running {
			// An in-flight attempt cannot be known to have completed.
			// Record it as interrupted (NOT success) without consuming the
			// retry budget; the node is re-attempted after requeue.
			nd.Attempts = append(nd.Attempts, state.Attempt{
				Index:      nd.AttemptCount,
				StartedAt:  nd.UpdatedAt,
				FinishedAt: now,
				Outcome:    state.OutcomeInterrupted,
				Reason:     "process interrupted while attempt in progress",
			})
			nd.Status = state.Queued
			nd.NextAttemptAt = time.Time{}
			nd.UpdatedAt = now
			recovered++
			e.cfg.Logger.Printf("[run=%s] recovery: node=%s was running at crash -> recorded interrupted, re-queued (not marked success)",
				runID, nd.ID)
		} else if nd.Status == state.Queued {
			nd.UpdatedAt = now
		}
	}
	rs.Status = state.Pending
	rs.UpdatedAt = now
	if err := e.cfg.Store.SaveSnapshot(ctx, rs); err != nil {
		e.forget(runID)
		return err
	}
	rc.bootstrap(rs)
	rc.recovered = recovered
	e.wg.Add(1)
	go e.loop(rc)
	return nil
}

func (e *Engine) forget(runID string) {
	e.mu.Lock()
	delete(e.runs, runID)
	e.mu.Unlock()
}

// Wait blocks until the run reaches a terminal state and returns its snapshot.
func (e *Engine) Wait(ctx context.Context, runID string) (*state.RunState, error) {
	e.mu.Lock()
	rc := e.runs[runID]
	e.mu.Unlock()
	if rc == nil {
		return e.cfg.Store.LoadSnapshot(ctx, runID)
	}
	select {
	case <-rc.terminalChan():
		return e.View(runID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// View returns a consistent deep-copy snapshot of the run for observers.
func (e *Engine) View(runID string) (*state.RunState, error) {
	e.mu.Lock()
	rc := e.runs[runID]
	e.mu.Unlock()
	if rc != nil {
		rc.mu.Lock()
		rs := rc.state.Clone()
		rc.mu.Unlock()
		return rs, nil
	}
	return e.cfg.Store.LoadSnapshot(context.Background(), runID)
}

// ListRuns returns the ids of all runs known to the engine, including
// historical (persisted) runs.
func (e *Engine) ListRuns(ctx context.Context) ([]string, error) {
	ids, err := e.cfg.Store.ListRuns(ctx)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	for id := range e.runs {
		if !contains(ids, id) {
			ids = append(ids, id)
		}
	}
	e.mu.Unlock()
	return ids, nil
}

// Expand dynamically attaches nodes produced by an already completed node.
func (e *Engine) Expand(ctx context.Context, runID, parentID string, nodes []spec.NodeSpec) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	rc := e.runs[runID]
	e.mu.Unlock()
	if rc == nil {
		return fmt.Errorf("%w: %s", store.ErrNotFound, runID)
	}
	return rc.requestExpand(ctx, parentID, nodes, false)
}

// Effects returns the deduplicated effect-key log of a run.
func (e *Engine) Effects(ctx context.Context, runID string) ([]string, error) {
	e.mu.Lock()
	rc := e.runs[runID]
	e.mu.Unlock()
	if rc != nil {
		rc.mu.Lock()
		out := append([]string(nil), rc.state.Effects...)
		rc.mu.Unlock()
		return out, nil
	}
	events, err := e.cfg.Store.ListEvents(ctx, runID)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, ev := range events {
		if ev.Kind == kindEffect {
			out = append(out, ev.Detail)
		}
	}
	return out, nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// cancelAll is a test-only helper that cancels all run contexts.
func (e *Engine) cancelAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, rc := range e.runs {
		rc.cancel()
	}
}
