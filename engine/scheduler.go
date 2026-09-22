package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

// schedule is the only place that transitions nodes between Pending,
// Queued and Running. It is invoked exclusively from the event loop.
//
// It performs, repeatedly and in deterministic order:
//  1. admission: eligible Pending nodes enter the bounded queue (P=backlog);
//  2. skip propagation: nodes whose deps failed/skipped become Skipped;
//  3. dispatch: due Queued nodes start while Slots remain.
//
// Nodes whose NextAttemptAt is in the future stay Queued (backing off).
func (rc *runCtx) schedule(now time.Time) {
	changed := true
	for changed {
		changed = false

		// (1)+(2) visit nodes in stable OrderKey order.
		ids := rc.orderedIDs()
		for _, id := range ids {
			nd := rc.state.Nodes[id]
			if nd.Status != state.Pending {
				continue
			}
			if !rc.depsResolved(nd) {
				continue
			}
			if rc.hasBadDep(nd) {
				rc.markSkipped(nd)
				changed = true
				continue
			}
			// Eligible: attempt bounded admission (backpressure).
			if rc.admit(nd) {
				changed = true
			}
		}

		// (3) dispatch due nodes.
		rc.sortReady()
		var stillQueued []string
		for _, id := range rc.ready {
			nd := rc.state.Nodes[id]
			if nd.Status != state.Queued {
				continue // stale entry
			}
			// Defensive: a dependency can never flip terminal->bad after a
			// node is queued, but keep skip classification consistent.
			if rc.hasBadDep(nd) {
				rc.markSkipped(nd)
				changed = true
				continue
			}
			if !nd.NextAttemptAt.IsZero() && nd.NextAttemptAt.After(now) {
				stillQueued = append(stillQueued, id)
				continue
			}
			if !rc.canStart(nd) {
				stillQueued = append(stillQueued, id)
				continue
			}
			rc.start(nd)
			changed = true
		}
		rc.ready = stillQueued
	}
}

// orderedIDs returns node ids sorted by OrderKey (then id).
func (rc *runCtx) orderedIDs() []string {
	ids := make([]string, 0, len(rc.state.Nodes))
	for id := range rc.state.Nodes {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0; j-- {
			a, b := rc.state.Nodes[ids[j]], rc.state.Nodes[ids[j-1]]
			less := a.OrderKey < b.OrderKey || (a.OrderKey == b.OrderKey && a.ID < b.ID)
			if !less {
				break
			}
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	return ids
}

func (rc *runCtx) hasBadDep(nd *state.NodeState) bool {
	for _, d := range nd.Deps {
		switch dn := rc.state.Nodes[d]; dn.Status {
		case state.Failed, state.Skipped:
			return true
		}
	}
	return false
}

func (rc *runCtx) canStart(nd *state.NodeState) bool {
	return rc.used+nd.Slots <= rc.engine.cfg.MaxRunning
}

func (rc *runCtx) markSkipped(nd *state.NodeState) {
	bad := ""
	for _, d := range nd.Deps {
		if dn := rc.state.Nodes[d]; dn != nil &&
			(dn.Status == state.Failed || dn.Status == state.Skipped) {
			bad = d
			break
		}
	}
	nd.Status = state.Skipped
	nd.FailureReason = "skipped: upstream dependency failed or was skipped"
	nd.UpdatedAt = time.Now().UTC()
	rc.logf("node=%s -> skipped (dep %q is failed/skipped)", nd.ID, bad)
}

// start launches one attempt on a worker goroutine and records Running.
func (rc *runCtx) start(nd *state.NodeState) {
	nd.AttemptCount++
	nd.Status = state.Running
	nd.NextAttemptAt = time.Time{}
	nd.UpdatedAt = time.Now().UTC()
	rc.used += nd.Slots
	rc.running[nd.ID] = true

	attempt := nd.AttemptCount
	timeout := time.Duration(nd.Timeout)
	go rc.runAttempt(nd.ID, attempt, timeout)
	rc.logf("node=%s -> running attempt=%d slots=%d (used %d/%d, timeout=%s)",
		nd.ID, attempt, nd.Slots, rc.used, rc.engine.cfg.MaxRunning, timeout)
}

// runAttempt runs user code with per-attempt timeout and posts exactly one
// result event. It never mutates run state directly.
func (rc *runCtx) runAttempt(id string, attempt int, timeout time.Duration) {
	ctx, cancel := context.WithCancel(rc.ctx)
	if timeout > 0 {
		var c2 context.CancelFunc
		ctx, c2 = context.WithTimeout(ctx, timeout)
		cancel()
		cancel = c2
	}
	defer cancel()

	type res struct {
		result  string
		effects []string
		err     error
	}
	out := make(chan res, 1)
	start := time.Now().UTC()
	go func() {
		r, fx, err := rc.exec(ctx, rc.id, id)
		out <- res{r, fx, err}
	}()

	ev := event{kind: "attempt", nodeID: id, attempt: attempt, at: start}
	select {
	case o := <-out:
		ev.result = o.result
		ev.effects = o.effects
		ev.err = o.err
		if o.err != nil {
			ev.outcome = state.OutcomeError
		} else {
			ev.outcome = state.OutcomeSuccess
		}
	case <-ctx.Done():
		ev.err = ctx.Err()
		if rc.ctx.Err() == nil && timeout > 0 {
			ev.outcome = state.OutcomeTimeout
		} else {
			ev.outcome = state.OutcomeInterrupted
		}
	}
	select {
	case rc.events <- ev:
	case <-rc.ctx.Done():
	}
}

// finishAttempt applies an attempt result within the event loop.
func (rc *runCtx) finishAttempt(ev event) {
	nd, ok := rc.state.Nodes[ev.nodeID]
	if !ok {
		return
	}
	// Stale event for a node no longer running (cannot happen with current
	// transitions, but guard deterministically).
	if !rc.running[nd.ID] || nd.AttemptCount != ev.attempt {
		return
	}
	delete(rc.running, nd.ID)
	rc.used -= nd.Slots
	if rc.used < 0 {
		rc.used = 0
	}
	now := time.Now().UTC()

	a := state.Attempt{
		Index:      ev.attempt,
		StartedAt:  ev.at,
		FinishedAt: now,
		Outcome:    ev.outcome,
	}
	if ev.err != nil {
		a.Reason = ev.err.Error()
	}
	nd.Attempts = append(nd.Attempts, a)

	if ev.outcome == state.OutcomeSuccess {
		nd.Status = state.Succeeded
		nd.Result = ev.result
		nd.FailureReason = ""
		nd.NextAttemptAt = time.Time{}
		nd.UpdatedAt = now
		rc.logf("node=%s -> succeeded after attempt=%d (effects=%d)",
			nd.ID, ev.attempt, len(ev.effects))
		// Record effect keys exactly once (dedupe anchored on key).
		for _, fx := range ev.effects {
			rc.recordEffect(nd.ID, fx)
		}
		// Dynamic expansion produced by the completed node.
		if rc.expander != nil {
			extra := rc.expander(rc.id, nd.ID, nd.Result)
			if len(extra) > 0 {
				if err := rc.applyExpand(nd.ID, extra, true); err != nil {
					// Expansion failure fails the parent node explicitly;
					// the successful attempt remains visible in history.
					nd.Status = state.Failed
					nd.FailureReason = "expansion rejected: " + err.Error()
				}
			}
		}
		return
	}

	// Failure or timeout. Interrupted attempts do NOT consume the retry
	// budget (a crash is neither a task failure nor a timeout); only
	// error/timeout outcomes count against MaxRetries.
	failCount := 0
	for _, a := range nd.Attempts {
		if a.Outcome == state.OutcomeError || a.Outcome == state.OutcomeTimeout {
			failCount++
		}
	}
	if failCount <= nd.MaxRetries {
		delay := rc.engine.cfg.Backoff.Delay(failCount - 1)
		nd.Status = state.Queued
		nd.NextAttemptAt = now.Add(delay)
		nd.FailureReason = fmt.Sprintf("attempt %d %s: %v", ev.attempt, ev.outcome, ev.err)
		nd.UpdatedAt = now
		rc.ready = append(rc.ready, nd.ID)
		rc.armBackoff(nd, delay)
		rc.logf("node=%s attempt=%d outcome=%s: retry %d/%d after %s (reason: %v)",
			nd.ID, ev.attempt, ev.outcome, failCount, nd.MaxRetries, delay, ev.err)
		return
	}

	nd.Status = state.Failed
	nd.NextAttemptAt = time.Time{}
	nd.FailureReason = fmt.Sprintf("retries exhausted after %d failed attempt(s); last=%s: %v",
		failCount, ev.outcome, ev.err)
	nd.UpdatedAt = now
	rc.logf("node=%s -> failed: retries exhausted (%d/%d, last outcome=%s)",
		nd.ID, failCount, nd.MaxRetries, ev.outcome)
}

// armBackoff schedules the event that makes a backing-off node due.
func (rc *runCtx) armBackoff(nd *state.NodeState, delay time.Duration) {
	if t, ok := rc.waiting[nd.ID]; ok {
		t.Stop()
	}
	t := time.AfterFunc(delay, func() {
		select {
		case rc.events <- event{kind: "due", nodeID: nd.ID}:
		case <-rc.ctx.Done():
		}
	})
	rc.waiting[nd.ID] = t
}

// reconcile computes the run-level status from node statuses.
func (rc *runCtx) reconcile() {
	allTerminal := true
	anyFail, anySkip := false, false
	for _, nd := range rc.state.Nodes {
		switch {
		case !nd.Status.Terminal():
			allTerminal = false
		case nd.Status == state.Failed:
			anyFail = true
		case nd.Status == state.Skipped:
			anySkip = true
		}
	}
	if !allTerminal {
		rc.state.Status = state.Running
		return
	}
	switch {
	case anyFail:
		rc.state.Status = state.Failed
	case anySkip:
		rc.state.Status = state.Skipped
	default:
		rc.state.Status = state.Succeeded
	}
	rc.logf("run -> %s (failed=%d skipped=%d succeeded=%d)",
		rc.state.Status,
		rc.state.Count(state.Failed),
		rc.state.Count(state.Skipped),
		rc.state.Count(state.Succeeded))
}
