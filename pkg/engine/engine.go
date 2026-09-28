// Package engine implements the local workflow orchestration engine:
// bounded-concurrency scheduling, retries with backoff, timeouts,
// backpressure, durable recovery and dynamic graph expansion.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/store"
)

// Config tunes engine-wide behaviour.
type Config struct {
	// GlobalMaxParallel bounds concurrently running nodes across all runs.
	// 0 means unlimited; per-spec limits still apply.
	GlobalMaxParallel int
	// MaxNodesPerRun is a hard safety cap on graph size (including spawned
	// nodes). Exceeding it rejects a spawn batch explicitly instead of
	// growing memory without bound. 0 defaults to 100000.
	MaxNodesPerRun int
}

// Engine orchestrates multiple runs.
type Engine struct {
	store    store.Store
	resolver Resolver
	clock    Clock
	cfg      Config

	mu     sync.RWMutex
	runs   map[string]*runEntry
	wg     sync.WaitGroup
	close  chan struct{}
	closed bool

	// sem is the global concurrency budget; nil when unlimited.
	sem chan struct{}
}

type observer struct {
	ch chan model.SnapshotEvent
}

type runEntry struct {
	id        string
	graph     *spec.Graph
	sc        *scheduler
	done      chan struct{}
	observers map[int]*observer
	nextObsID int
	baseCtx   context.Context
	cancel    context.CancelFunc
	startErr  error
}

// New constructs an engine.
func New(st store.Store, resolver Resolver, cfg Config) *Engine {
	if cfg.MaxNodesPerRun <= 0 {
		cfg.MaxNodesPerRun = 100000
	}
	e := &Engine{
		store:    st,
		resolver: resolver,
		clock:    SystemClock(),
		cfg:      cfg,
		runs:     make(map[string]*runEntry),
		close:    make(chan struct{}),
	}
	if cfg.GlobalMaxParallel > 0 {
		e.sem = make(chan struct{}, cfg.GlobalMaxParallel)
	}
	return e
}

// WithClock overrides the clock (mainly for tests); must be set before Submit.
func (e *Engine) WithClock(c Clock) *Engine { e.clock = c; return e }

func newRunID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "run-" + hex.EncodeToString(b[:])
}

// Submit validates s, persists a fresh run and starts scheduling it.
func (e *Engine) Submit(ctx context.Context, s *spec.Spec) (string, error) {
	graph, err := spec.Build(s)
	if err != nil {
		return "", err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return "", errors.New("engine shut down")
	}
	e.mu.Unlock()

	runID := newRunID()
	now := e.clock.Now()
	status := &model.RunStatus{
		RunID:     runID,
		SpecName:  s.Name,
		Spec:      s,
		State:     model.RunPending,
		Nodes:     make(map[string]*model.NodeStatus, len(s.Nodes)),
		NodeOrder: graph.IDs(),
		CreatedAt: now,
		UpdatedAt: now,
	}
	for _, id := range graph.IDs() {
		status.Nodes[id] = &model.NodeStatus{ID: id, State: model.StatePending}
	}
	if err := e.store.SaveStatus(ctx, status); err != nil {
		return "", err
	}
	return runID, e.startRun(ctx, graph, status)
}

// Resume continues a previously persisted run after a process restart, using
// the specification embedded in the durable snapshot. Nodes that were
// "running" at crash are never silently marked successful: they are
// re-executed from their recorded attempt with a recovered_running annotation;
// succeeded nodes are never re-run.
func (e *Engine) Resume(ctx context.Context, runID string) error {
	status, err := e.store.LoadStatus(ctx, runID)
	if err != nil {
		return err
	}
	if status.Spec == nil {
		return errors.New("snapshot is missing its spec; cannot resume")
	}
	graph, err := spec.Build(status.Spec)
	if err != nil {
		return err
	}
	return e.startRun(ctx, graph, status)
}

// startRun registers a run and launches its scheduler loop.
func (e *Engine) startRun(ctx context.Context, graph *spec.Graph, status *model.RunStatus) error {
	rctx, cancel := context.WithCancel(context.Background())
	entry := &runEntry{
		id:        status.RunID,
		graph:     graph,
		done:      make(chan struct{}),
		observers: make(map[int]*observer),
		baseCtx:   rctx,
		cancel:    cancel,
	}
	sc := newScheduler(entry, graph, status, e)
	entry.sc = sc

	e.mu.Lock()
	if e.closed {
		cancel()
		e.mu.Unlock()
		return errors.New("engine shut down")
	}
	if _, exists := e.runs[status.RunID]; exists {
		e.mu.Unlock()
		cancel()
		return fmt.Errorf("run %q is already active in this process", status.RunID)
	}
	e.runs[status.RunID] = entry
	e.mu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		sc.loop(rctx)
		e.mu.Lock()
		close(entry.done)
		e.mu.Unlock()
	}()
	return nil
}

// Wait blocks until the run reaches a terminal state or ctx is done.
func (e *Engine) Wait(ctx context.Context, runID string) (*model.RunRecord, error) {
	e.mu.RLock()
	entry := e.runs[runID]
	e.mu.RUnlock()
	if entry == nil {
		return e.GetRun(ctx, runID) // finished in a previous process: read store
	}
	select {
	case <-entry.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.close:
	}
	return e.GetRun(ctx, runID)
}

// GetRun returns a consistent deep-copied view of one run: status snapshot
// plus the append-only transition history.
func (e *Engine) GetRun(ctx context.Context, runID string) (*model.RunRecord, error) {
	e.mu.RLock()
	entry := e.runs[runID]
	e.mu.RUnlock()
	if entry != nil {
		return entry.sc.record(ctx)
	}
	status, err := e.store.LoadStatus(ctx, runID)
	if err != nil {
		return nil, err
	}
	hist, err := e.store.LoadHistory(ctx, runID)
	if err != nil {
		return nil, err
	}
	actions, err := e.store.LoadActions(ctx, runID)
	if err != nil {
		return nil, err
	}
	return &model.RunRecord{Status: status, History: hist, Actions: actions}, nil
}

// ListActions returns the committed business-action records of a run ordered
// by key. The set is identical for concurrent readers and across restarts.
func (e *Engine) ListActions(ctx context.Context, runID string) ([]model.ActionRecord, error) {
	return e.store.LoadActions(ctx, runID)
}

// Subscribe streams state transitions; calling the returned func unsubscribes.
// The channel is closed on unsubscribe or run termination. Replay of existing
// history is included so observers always see a consistent prefix.
func (e *Engine) Subscribe(runID string) (<-chan model.SnapshotEvent, func(), error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entry := e.runs[runID]
	if entry == nil {
		return nil, nil, model.ErrNotFound
	}
	id := entry.nextObsID
	entry.nextObsID++
	ch := make(chan model.SnapshotEvent, 256)
	obs := &observer{ch: ch}
	entry.observers[id] = obs
	unsub := func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if o, ok := entry.observers[id]; ok {
			delete(entry.observers, id)
			close(o.ch)
		}
	}
	// Replay durable history first (outside scheduler lock-free store read).
	hist, err := e.store.LoadHistory(context.Background(), runID)
	if err == nil {
		for _, ev := range hist.Events {
			ch <- ev
		}
	}
	return ch, unsub, nil
}

// publish hands a transition to all live observers without blocking the
// scheduler; observers whose buffer is full still receive every event because
// channels are buffered per subscriber and drained under callers' control.
func (e *Engine) publish(runID string, ev model.SnapshotEvent) {
	e.mu.RLock()
	entry := e.runs[runID]
	if entry == nil {
		e.mu.RUnlock()
		return
	}
	obs := make([]*observer, 0, len(entry.observers))
	for _, o := range entry.observers {
		obs = append(obs, o)
	}
	e.mu.RUnlock()
	for _, o := range obs {
		select {
		case o.ch <- ev:
		default:
			// Drop for a saturated subscriber is avoided by spinning briefly;
			// guarantee delivery by blocking unless engine is closing.
			select {
			case o.ch <- ev:
			case <-e.close:
			}
		}
	}
}

// Shutdown cancels and waits for all in-flight scheduling to stop; durable
// state at interruption remains recoverable via Resume.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.close)
	entries := make([]*runEntry, 0, len(e.runs))
	for _, en := range e.runs {
		entries = append(entries, en)
		en.cancel()
	}
	e.mu.Unlock()

	doneAll := make(chan struct{})
	go func() { e.wg.Wait(); close(doneAll) }()
	select {
	case <-doneAll:
	case <-ctx.Done():
		return ctx.Err()
	}
	e.mu.Lock()
	for _, en := range entries {
		for _, o := range en.observers {
			close(o.ch)
		}
		en.observers = map[int]*observer{}
	}
	e.mu.Unlock()
	return nil
}
