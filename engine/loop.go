package engine

import (
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

// shutdownGrace bounds how long shutdown waits for cancelled in-flight
// attempts to deliver their terminal events before persisting survivors as
// re-runnable. It is small by design (process shutdown must stay prompt).
const shutdownGrace = 250 * time.Millisecond

// loop is the serialized scheduler event loop for one run.
//
// Exactly one goroutine mutates rc.state / rc.ready / rc.running. Every
// externally visible transition is snapshot-persisted before the loop goes
// back to waiting, so an observer (or a crash + Resume) never sees a
// half-applied update and successful nodes are never re-run.
//
// The loop stays alive after the run reaches a terminal state so that
// post-completion dynamic expansion can resume scheduling; it only exits on
// engine shutdown or a fatal persistence error.
func (e *Engine) loop(rc *runCtx) {
	defer e.wg.Done()
	defer close(rc.done)

	persist := func(reason string) {
		rc.mu.Lock()
		rc.state.UpdatedAt = time.Now().UTC()
		rs := rc.state.Clone()
		rc.mu.Unlock()
		if err := e.cfg.Store.SaveSnapshot(rc.ctx, rs); err != nil {
			rc.fatalErr = err
		}
	}

	// Initial drive: re-arm any surviving backoff timers, admit + dispatch.
	rc.mu.Lock()
	rc.armPendingBackoffs()
	rc.schedule(time.Now().UTC())
	rc.reconcile()
	wasTerminal := rc.state.Status.Terminal() && len(rc.running) == 0
	rc.mu.Unlock()
	persist("init")

	for {
		if rc.fatalErr != nil {
			rc.terminateFatal()
			return
		}

		rc.mu.Lock()
		terminal := rc.state.Status.Terminal() && len(rc.running) == 0
		rc.mu.Unlock()
		if terminal && !wasTerminal {
			persist("terminal")
			rc.signalTerminal()
			wasTerminal = true
		}

		if !terminal {
			// Work is in flight: wait for the next reported event.
			select {
			case ev := <-rc.events:
				if ev.kind == "expand" {
					rc.armTerminalEpisode()
					wasTerminal = false
				}
				rc.handleEvent(ev)
				persist(ev.kind)
			case <-rc.ctx.Done():
				rc.drainAndShutdown(persist)
				return
			}
			continue
		}

		// Terminal and idle: stay resident to accept expansions.
		select {
		case ev := <-rc.events:
			if ev.kind != "expand" {
				continue // nothing else is expected after completion
			}
			rc.armTerminalEpisode()
			wasTerminal = false
			rc.handleEvent(ev)
			persist(ev.kind)
		case <-rc.ctx.Done():
			rc.drainAndShutdown(persist)
			return
		}
	}
}

// drainAndShutdown gives in-flight attempts (whose contexts were cancelled)
// a bounded grace window to deliver their terminal events, so a node that in
// fact succeeded is persisted as succeeded rather than rewritten to queued.
// Only nodes STILL running afterwards are reconciled for re-execution.
func (rc *runCtx) drainAndShutdown(persist func(string)) {
	deadline := time.Now().Add(shutdownGrace)
	for {
		select {
		case ev := <-rc.events:
			rc.handleEvent(ev)
		default:
		}
		rc.mu.Lock()
		remaining := len(rc.running)
		rc.mu.Unlock()
		if remaining == 0 || time.Now().After(deadline) {
			break
		}
		select {
		case ev := <-rc.events:
			rc.handleEvent(ev)
		case <-time.After(2 * time.Millisecond):
		}
	}
	rc.shutdownPersist(persist)
}

func (rc *runCtx) handleEvent(ev event) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	switch ev.kind {
	case "attempt":
		rc.finishAttempt(ev)
	case "due":
		nd := rc.state.Nodes[ev.nodeID]
		if nd != nil {
			delete(rc.waiting, nd.ID)
			nd.NextAttemptAt = time.Time{}
			if nd.Status == state.Queued && !contains(rc.ready, nd.ID) {
				rc.ready = append(rc.ready, nd.ID)
			}
		}
	case "expand":
		req := ev.expandReq
		err := rc.applyExpand(req.parentID, req.nodes, req.fromHook)
		req.reply <- err
	}
	rc.schedule(time.Now().UTC())
	rc.reconcile()
}

func (rc *runCtx) shutdownPersist(persist func(string)) {
	rc.mu.Lock()
	for id := range rc.running {
		if nd := rc.state.Nodes[id]; nd != nil && nd.Status == state.Running {
			nd.Status = state.Queued
		}
	}
	// Stop all backoff timers; they are re-created on Resume.
	for id, t := range rc.waiting {
		t.Stop()
		delete(rc.waiting, id)
	}
	rc.mu.Unlock()
	persist("shutdown")
}

// terminateFatal forces the run to Failed using the recorded infrastructure
// error; already-succeeded nodes stay succeeded.
func (rc *runCtx) terminateFatal() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.state.Status = state.Failed
	now := time.Now().UTC()
	for _, nd := range rc.state.Nodes {
		if !nd.Status.Terminal() && nd.Status != state.Succeeded {
			nd.Status = state.Failed
			if nd.FailureReason == "" {
				nd.FailureReason = "run aborted: " + rc.fatalErr.Error()
			}
			nd.UpdatedAt = now
		}
	}
	rc.state.UpdatedAt = now
	rs := rc.state.Clone()
	_ = rc.engine.cfg.Store.SaveSnapshot(rc.ctx, rs)
}
