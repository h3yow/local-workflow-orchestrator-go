package engine

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

const (
	kindEffect = "effect"
)

// event is the only way worker goroutines / timers talk to the event loop.
type event struct {
	kind    string
	nodeID  string
	attempt int
	result  string
	effects []string
	err     error
	outcome state.AttemptOutcome
	at      time.Time
	// dynamic expansion request
	expandReq *expandRequest
}

type expandRequest struct {
	parentID string
	nodes    []spec.NodeSpec
	fromHook bool
	reply    chan error
	ctx      context.Context
}

// runCtx is the serialized control block of one run.
type runCtx struct {
	id       string
	engine   *Engine
	exec     ExecFunc
	expander ExpanderFunc

	mu        sync.Mutex // guards state for external View calls
	state     *state.RunState
	ready     []string // admitted, waiting for a slot (ordered by OrderKey)
	waiting   map[string]*time.Timer
	running   map[string]bool // node attempts in flight
	used      int             // sum of Slots of running nodes
	recovered int
	fatalErr  error // infrastructure error forcing run failure

	events chan event
	done   chan struct{} // closed when the loop exits (shutdown/fatal)
	// terminalMu guards terminalCh: terminalCh is closed each time the run
	// settles in a terminal state and replaced when it becomes active again.
	terminalMu sync.Mutex
	terminalCh chan struct{}
	cancel     context.CancelFunc
	ctx        context.Context
}

// terminalChan returns the current terminal-notification channel.
func (rc *runCtx) terminalChan() chan struct{} {
	rc.terminalMu.Lock()
	defer rc.terminalMu.Unlock()
	return rc.terminalCh
}

// signalTerminal closes the current terminal channel (idempotent per episode).
func (rc *runCtx) signalTerminal() {
	rc.terminalMu.Lock()
	defer rc.terminalMu.Unlock()
	select {
	case <-rc.terminalCh:
		// already closed for this episode
	default:
		close(rc.terminalCh)
	}
}

// armTerminalEpisode installs a fresh open terminal channel, indicating the
// run became active after having been terminal (dynamic expansion).
func (rc *runCtx) armTerminalEpisode() {
	rc.terminalMu.Lock()
	defer rc.terminalMu.Unlock()
	select {
	case <-rc.terminalCh:
		rc.terminalCh = make(chan struct{})
	default:
		// still open: nothing to do
	}
}

func newRunCtx(id string, e *Engine, exec ExecFunc, expander ExpanderFunc) *runCtx {
	return &runCtx{id: id, engine: e, exec: exec, expander: expander}
}

// logf emits a scheduling-decision line (state-transition rationale).
func (rc *runCtx) logf(format string, args ...any) {
	rc.engine.cfg.Logger.Printf("[run=%s] "+format, append([]any{rc.id}, args...)...)
}

// bootstrap wires loop channels and the cancellation context.
// Called before the event loop starts; the initial state is already
// persisted by Create/Resume.
func (rc *runCtx) bootstrap(rs *state.RunState) {
	ctx, cancel := context.WithCancel(context.Background())
	rc.ctx = ctx
	rc.cancel = cancel
	rc.events = make(chan event, 64)
	rc.done = make(chan struct{})
	rc.terminalCh = make(chan struct{})

	// Hold the view lock while wiring mutable state so concurrent View calls
	// never observe a half-initialized runCtx.
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.state = rs
	rc.waiting = make(map[string]*time.Timer)
	rc.running = make(map[string]bool)

	// Re-admit nodes that are already eligible: persisted Queued nodes
	// (including recovered interrupted attempts) plus initially-ready nodes.
	now := time.Now().UTC()
	for _, nd := range rs.Nodes {
		switch nd.Status {
		case state.Queued:
			if !nd.NextAttemptAt.IsZero() && nd.NextAttemptAt.After(now) {
				continue // timer is (re)armed by armPendingBackoffs in the loop
			}
			nd.NextAttemptAt = time.Time{}
			rc.ready = append(rc.ready, nd.ID)
		case state.Pending:
			if rc.depsResolved(nd) {
				rc.admit(nd)
			}
		}
	}
	rc.sortReady()
}

// armPendingBackoffs (re)creates wait timers for queued nodes still inside
// their backoff window after process restart.
func (rc *runCtx) armPendingBackoffs() {
	now := time.Now().UTC()
	for _, nd := range rc.state.Nodes {
		if nd.Status != state.Queued || nd.NextAttemptAt.IsZero() {
			continue
		}
		d := nd.NextAttemptAt.Sub(now)
		if d > 0 {
			rc.armBackoff(nd, d)
		} else if !contains(rc.ready, nd.ID) {
			nd.NextAttemptAt = time.Time{}
			rc.ready = append(rc.ready, nd.ID)
		}
	}
	rc.sortReady()
}

// depsResolved reports whether every dependency has reached a terminal state.
func (rc *runCtx) depsResolved(nd *state.NodeState) bool {
	for _, d := range nd.Deps {
		dn, ok := rc.state.Nodes[d]
		if !ok || !dn.Status.Terminal() {
			return false
		}
	}
	return true
}

// admit moves an eligible pending node into the bounded ready queue.
// Returns false when backpressure refuses admission; the node stays Pending
// and is re-evaluated on the next pass (never silently dropped).
func (rc *runCtx) admit(nd *state.NodeState) bool {
	if len(rc.ready) >= rc.engine.cfg.MaxQueue {
		rc.logf("node=%s not admitted: backlog %d/%d (backpressure; stays pending)",
			nd.ID, len(rc.ready), rc.engine.cfg.MaxQueue)
		return false
	}
	nd.Status = state.Queued
	nd.UpdatedAt = time.Now().UTC()
	rc.ready = append(rc.ready, nd.ID)
	rc.logf("node=%s admitted to ready queue (backlog now %d/%d)",
		nd.ID, len(rc.ready), rc.engine.cfg.MaxQueue)
	return true
}

func (rc *runCtx) sortReady() {
	// Deterministic order: (NextAttemptAt, OrderKey, ID).
	for i := 1; i < len(rc.ready); i++ {
		for j := i; j > 0 && rc.readyLess(rc.ready[j], rc.ready[j-1]); j-- {
			rc.ready[j], rc.ready[j-1] = rc.ready[j-1], rc.ready[j]
		}
	}
}

func (rc *runCtx) readyLess(a, b string) bool {
	na, nb := rc.state.Nodes[a], rc.state.Nodes[b]
	if !na.NextAttemptAt.Equal(nb.NextAttemptAt) {
		return na.NextAttemptAt.Before(nb.NextAttemptAt)
	}
	if na.OrderKey != nb.OrderKey {
		return na.OrderKey < nb.OrderKey
	}
	return a < b
}

// requestExpand enqueues a dynamic-expansion request to the event loop.
// It either succeeds (all nodes admitted/registered) or fails atomically;
// duplicate ids never take effect twice. Expansion is allowed even when the
// run previously reached a terminal state: newly admitted nodes make the run
// runnable again.
func (rc *runCtx) requestExpand(ctx context.Context, parentID string, nodes []spec.NodeSpec, fromHook bool) error {
	req := &expandRequest{
		parentID: parentID,
		nodes:    append([]spec.NodeSpec(nil), nodes...),
		fromHook: fromHook,
		reply:    make(chan error, 1),
		ctx:      ctx,
	}
	select {
	case rc.events <- event{kind: "expand", expandReq: req}:
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.ctx.Done():
		return ErrClosed
	}
	select {
	case err := <-req.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.ctx.Done():
		return ErrClosed
	}
}
