package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
)

// scheduler drives one run. All bookkeeping happens on a single goroutine
// (loop), so admission and dispatch are race-free by construction: one node
// transitions to running exactly once per attempt, and a ready node can never
// be picked twice or forgotten.
type scheduler struct {
	entry  *runEntry
	graph  *spec.Graph
	engine *Engine

	mu     sync.RWMutex // guards status only (readers: GetRun/observers)
	status *model.RunStatus

	// scheduling bookkeeping (goroutine-confined to loop)
	waiting   map[string]bool      // nodes not yet admitted (backpressure spillover)
	inflight  map[string]bool      // attempts currently executing
	readyAt   map[string]time.Time // retrying nodes: earliest dispatch time
	terminals int

	results chan attemptResult // attempt goroutines report back here
	wake    chan struct{}      // completion nudge (never carries payload)
	pending []pendingEvent     // transitions awaiting deterministic flushing
	seq     int64
}

func newScheduler(entry *runEntry, graph *spec.Graph, status *model.RunStatus, eng *Engine) *scheduler {
	sc := &scheduler{
		entry:    entry,
		graph:    graph,
		engine:   eng,
		status:   status,
		waiting:  make(map[string]bool),
		inflight: make(map[string]bool),
		readyAt:  make(map[string]time.Time),
		results:  make(chan attemptResult, 256),
		wake:     make(chan struct{}, 1),
	}
	return sc
}

// record returns a consistent cloned view for concurrent readers.
func (sc *scheduler) record(ctx context.Context) (*model.RunRecord, error) {
	sc.mu.RLock()
	status := sc.status.Clone()
	sc.mu.RUnlock()
	hist, err := sc.engine.store.LoadHistory(ctx, sc.entry.id)
	if err != nil {
		return nil, err
	}
	return &model.RunRecord{Status: status, History: hist, Spec: sc.graph.Spec()}, nil
}

// pendingEvent is a state transition collected during one tick; its Seq is
// assigned deterministically at flush time.
type pendingEvent struct {
	nodeID  string
	from    model.NodeState
	to      model.NodeState
	attempt int
	code    model.FailureCode
	detail  string
}

// transition is the ONLY way node states change: it mutates the in-memory
// snapshot and persists it immediately (crash safety), buffering a
// corresponding event. Durable event Seqs are assigned when the scheduler
// flushes at a stable barrier, where events are normalised by node id so
// concurrent completions produce identical histories across runs.
func (sc *scheduler) transition(ctx context.Context, nodeID string, to model.NodeState, code model.FailureCode, detail string) error {
	now := sc.engine.clock.Now()

	sc.mu.Lock()
	node := sc.status.Nodes[nodeID]
	from := node.State
	attempt := node.Attempt
	node.State = to
	switch code {
	case model.FailNone:
	default:
		node.FailCode = code
		node.FailMessage = detail
	}
	if to == model.StateRunning {
		if node.StartedAt.IsZero() {
			node.StartedAt = now
		}
	}
	if to.IsTerminal() {
		node.FinishedAt = now
	}
	sc.status.UpdatedAt = now
	sc.status.Revision++
	snapshot := sc.status.Clone()
	sc.pending = append(sc.pending, pendingEvent{
		nodeID: nodeID, from: from, to: to, attempt: attempt, code: code, detail: detail,
	})
	sc.mu.Unlock()

	// Snapshot first so an interruption never loses the terminal result;
	// flushEvents (before the next blocking wait) makes the event log match.
	if err := sc.engine.store.SaveStatus(ctx, snapshot); err != nil {
		return err
	}
	return nil
}

// setRunState updates run-level state under lock and persists.
func (sc *scheduler) setRunState(ctx context.Context, st model.NodeState) error {
	sc.mu.Lock()
	if sc.status.State == st {
		sc.mu.Unlock()
		return nil
	}
	sc.status.State = st
	sc.status.UpdatedAt = sc.engine.clock.Now()
	sc.status.Revision++
	snapshot := sc.status.Clone()
	sc.mu.Unlock()
	return sc.engine.store.SaveStatus(ctx, snapshot)
}

// recover inspects the persisted snapshot and returns true if the run is
// already terminal. Non-terminal state is repaired: running/queued/ready
// nodes fall back to pending and their attempt is replayed (never marked
// successful); retrying nodes become immediately eligible again.
func (sc *scheduler) recover(ctx context.Context) (bool, error) {
	// Highest recorded event seq becomes our counter base so post-recovery
	// events never reuse a Seq.
	if hist, err := sc.engine.store.LoadHistory(ctx, sc.entry.id); err == nil {
		for _, ev := range hist.Events {
			if ev.Seq > sc.seq {
				sc.seq = ev.Seq
			}
		}
	}

	sc.mu.Lock()
	alreadyTerminal := sc.status.State.IsTerminal()
	repaired := make([]pendingEvent, 0)
	changed := false
	if !alreadyTerminal {
		for _, id := range sc.status.NodeOrder {
			n := sc.status.Nodes[id]
			if n.State == model.StateRunning || n.State == model.StateQueued || n.State == model.StateReady {
				repaired = append(repaired, pendingEvent{
					nodeID: id, from: n.State, to: model.StatePending,
					attempt: n.Attempt, code: model.FailRecoveredRunning,
					detail: "interrupted while " + string(n.State) + "; attempt replayed",
				})
				n.State = model.StatePending
				n.FailCode = model.FailRecoveredRunning
				n.FailMessage = "interrupted while non-terminal; attempt replayed"
				changed = true
			}
		}
		if changed {
			sc.status.State = model.RunPending
			sc.status.UpdatedAt = sc.engine.clock.Now()
		}
	}
	sc.countTerminalsLocked()
	sc.pending = append(repaired, sc.pending...)
	snapshot := sc.status.Clone()
	sc.mu.Unlock()

	if changed {
		if err := sc.engine.store.SaveStatus(ctx, snapshot); err != nil {
			return false, err
		}
		sc.flushEvents(ctx)
	}
	return alreadyTerminal, nil
}

// countTerminalsLocked recounts terminal nodes (used after recovery).
func (sc *scheduler) countTerminalsLocked() {
	t := 0
	for _, n := range sc.status.Nodes {
		if n.State.IsTerminal() {
			t++
		}
	}
	sc.terminals = t
}

// loop is the single scheduler goroutine.
func (sc *scheduler) loop(ctx context.Context) {
	if terminal, err := sc.recover(ctx); err != nil || terminal {
		return
	}
	_ = sc.setRunState(ctx, model.RunRunning)

	for {
		if ctx.Err() != nil {
			return // interrupted; durable state allows Resume
		}
		wait := sc.tick(ctx)
		// Flush only at stable barriers: no unconsumed completion results and
		// no in-flight attempts (or nothing running at all). While a wave of
		// concurrent nodes is executing, transitions buffer so the whole wave
		// can be normalised into one deterministic ordering; snapshots are
		// already durable per transition, so crashes remain recoverable.
		if len(sc.results) == 0 && len(sc.inflight) == 0 {
			sc.flushEvents(ctx)
		}
		if sc.done() {
			sc.finish(ctx)
			return
		}
		// Wait for: an attempt finishing, the soonest retry backoff expiring,
		// the global budget freeing up, or shutdown. A short safety timer also
		// covers the case where the global pool is held by other runs.
		if wait <= 0 {
			wait = 2 * time.Millisecond
		} else if wait > 20*time.Millisecond {
			wait = 20 * time.Millisecond // cap so retry deadline is rechecked promptly
		}
		select {
		case <-sc.wake:
		case <-sc.engine.clock.After(wait):
		case <-ctx.Done():
			return
		}
	}
}

func (sc *scheduler) done() bool {
	return sc.terminals >= len(sc.status.Nodes)
}

func (sc *scheduler) finish(ctx context.Context) {
	sc.mu.RLock()
	st := model.RunSucceeded
	for _, n := range sc.status.Nodes {
		if n.State == model.StateFailed || n.State == model.StateSkipped {
			st = model.RunFailed
			break
		}
	}
	sc.mu.RUnlock()
	_ = sc.setRunState(ctx, st)
}

// tick performs one deterministic scheduling pass and returns how long the
// loop should sleep before the next pass (0 = work available / immediate).
func (sc *scheduler) tick(ctx context.Context) time.Duration {
	specLimit := sc.graph.Spec().MaxParallel

	// 1) Skip propagation: pending nodes whose deps include failed/skipped.
	sc.propagateSkips(ctx)

	// 2) Mark dependency-satisfied pending nodes ready; maintain bounded queue.
	sc.admit(ctx)

	// 3) Dispatch eligible queued nodes in canonical order (strict FIFO: head
	//    blocking guarantees a later node can never starve an earlier one).
	sc.dispatch(ctx, specLimit)

	return sc.nextWake()
}

// propagateSkips marks every still-pending node skipped when any dependency
// is failed or skipped; the reason names the blocking upstream node.
func (sc *scheduler) propagateSkips(ctx context.Context) {
	for {
		progress := false
		for _, id := range sc.graph.IDs() {
			sc.mu.RLock()
			n := sc.status.Nodes[id]
			if n.State != model.StatePending {
				sc.mu.RUnlock()
				continue
			}
			blocked := ""
			node, _ := sc.graph.Node(id)
			for _, dep := range node.Deps {
				ds := sc.status.Nodes[dep].State
				if ds == model.StateFailed || ds == model.StateSkipped {
					blocked = dep
					break
				}
			}
			sc.mu.RUnlock()
			if blocked != "" {
				sc.mu.Lock()
				n := sc.status.Nodes[id]
				n.SkippedBecause = blocked
				sc.terminals++
				sc.waiting[id] = false
				sc.mu.Unlock()
				_ = sc.transition(ctx, id, model.StateSkipped, model.FailUpstreamFailed,
					fmt.Sprintf("upstream %q did not succeed", blocked))
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// admit moves pending nodes with all deps succeeded first to ready, then into
// the bounded queue; surplus nodes remain in the deterministic waiting set
// (bounded by graph size — no task is ever silently dropped).
func (sc *scheduler) admit(ctx context.Context) {
	limit := sc.graph.Spec().MaxQueue
	sc.mu.RLock()
	queued := 0
	for _, n := range sc.status.Nodes {
		if n.State == model.StateQueued {
			queued++
		}
	}
	sc.mu.RUnlock()

	for _, id := range sc.graph.IDs() {
		sc.mu.RLock()
		n := sc.status.Nodes[id]
		if n.State != model.StatePending && n.State != model.StateReady {
			sc.mu.RUnlock()
			continue
		}
		eligible := true
		node, _ := sc.graph.Node(id)
		for _, dep := range node.Deps {
			if sc.status.Nodes[dep].State != model.StateSucceeded {
				eligible = false
				break
			}
		}
		if _, wait := sc.readyAt[id]; wait && sc.engine.clock.Now().Before(sc.readyAt[id]) {
			eligible = false
		}
		sc.mu.RUnlock()
		if !eligible {
			continue
		}
		if queued >= limit && limit > 0 {
			sc.waiting[id] = true // backpressure: stay where it is, revisited
			continue
		}
		if err := sc.transition(ctx, id, model.StateQueued, model.FailNone, "admitted to ready queue"); err != nil {
			continue
		}
		delete(sc.waiting, id)
		queued++
	}
}

// nextWake returns the time until the soonest retry becomes due.
func (sc *scheduler) nextWake() time.Duration {
	now := sc.engine.clock.Now()
	soonest := time.Duration(1<<63 - 1)
	for _, t := range sc.readyAt {
		if d := t.Sub(now); d < soonest {
			soonest = d
		}
	}
	if soonest == 1<<63-1 {
		return 0
	}
	if soonest < 0 {
		return 0
	}
	return soonest
}
