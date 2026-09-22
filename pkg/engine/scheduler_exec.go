package engine

import (
	"context"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
	"github.com/h3yow/local-workflow-orchestrator-go/pkg/spec"
)

// attemptResult is delivered from an executing goroutine to the scheduler loop.
type attemptResult struct {
	nodeID   string
	attempt  int
	res      *ExecResult
	err      error
	timedOut bool
	started  time.Time
	finished time.Time
}

// loop is extended (see scheduler.go); drainResults is invoked every tick.

// dispatch drains finished attempts, then starts queued nodes in canonical
// order while both the per-spec and global concurrency budgets allow it.
// Traversal stops at the first node that cannot start: strict FIFO admission
// prevents a later cheap node from repeatedly overtaking an earlier one.
func (sc *scheduler) dispatch(ctx context.Context, specLimit int) {
	sc.drainResults(ctx)
	running := len(sc.inflight)

	for _, id := range sc.graph.IDs() {
		sc.mu.RLock()
		st := sc.status.Nodes[id].State
		sc.mu.RUnlock()
		if st != model.StateQueued || sc.inflight[id] {
			continue
		}
		if running >= specLimit {
			return // head-of-line waits; determinism preserved
		}
		// Global budget: non-blocking acquisition. A busy global pool parks
		// the whole run fairly without ever exceeding the bound.
		if sc.engine.sem != nil {
			select {
			case sc.engine.sem <- struct{}{}:
			default:
				return
			}
		}
		sc.startAttempt(ctx, id)
		running++
	}
}

// startAttempt launches exactly one execution attempt of a node.
func (sc *scheduler) startAttempt(ctx context.Context, id string) {
	node, ok := sc.graph.Node(id)
	if !ok {
		return
	}
	task, ok := sc.engine.resolver.Resolve(id)
	if !ok || task == nil {
		// Unresolvable node: permanent failure, no retries.
		sc.terminals++
		_ = sc.transition(ctx, id, model.StateFailed, model.FailPermanent, "no task registered for node")
		return
	}

	sc.mu.Lock()
	ns := sc.status.Nodes[id]
	ns.Attempt++
	attempt := ns.Attempt - 1
	sc.inflight[id] = true
	delete(sc.readyAt, id)
	sc.mu.Unlock()

	_ = sc.transition(ctx, id, model.StateRunning, model.FailNone, "attempt started")

	attemptCtx, cancel := context.WithCancel(sc.entry.baseCtx)
	if node.Timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(sc.entry.baseCtx, node.Timeout)
	}
	go func() {
		defer cancel()
		start := sc.engine.clock.Now()
		res, err := task(attemptCtx, TaskInput{RunID: sc.entry.id, NodeID: id, Attempt: attempt})
		finish := sc.engine.clock.Now()
		timedOut := attemptCtx.Err() == context.DeadlineExceeded
		if err == nil && timedOut {
			err = context.DeadlineExceeded
		}
		select {
		case sc.results <- attemptResult{nodeID: id, attempt: attempt, res: res, err: err, timedOut: timedOut, started: start, finished: finish}:
			select {
			case sc.wake <- struct{}{}:
			default:
			}
		case <-sc.entry.baseCtx.Done():
			// Engine shutting down before the result was consumed. The
			// scheduler loop is dead and will never release the global token,
			// so release it here to avoid leaking semaphore capacity; durable
			// state remains whatever was last persisted and Resume replays the
			// attempt safely (attempt goroutines still own their own cleanup).
			if sc.engine.sem != nil {
				select {
				case <-sc.engine.sem:
				default:
				}
			}
		}
	}()
}

// drainResults processes every completed attempt without blocking.
func (sc *scheduler) drainResults(ctx context.Context) {
	for {
		select {
		case r := <-sc.results:
			if sc.engine.sem != nil {
				<-sc.engine.sem
			}
			sc.finishAttempt(ctx, r)
		default:
			return
		}
	}
}

// finishAttempt records the attempt and applies success/failure/retry/spawn.
func (sc *scheduler) finishAttempt(ctx context.Context, r attemptResult) {
	sc.mu.Lock()
	if !sc.inflight[r.nodeID] {
		// Stale result after interruption: ignore, nothing is marked terminal.
		sc.mu.Unlock()
		return
	}
	delete(sc.inflight, r.nodeID)
	node, _ := sc.graph.Node(r.nodeID)
	ns := sc.status.Nodes[r.nodeID]
	rec := modelAttemptRecord(r)
	ns.Attempts = append(ns.Attempts, rec)
	sc.mu.Unlock()

	switch {
	case r.err == nil:
		var applied []string
		if r.res != nil && len(r.res.Spawn) > 0 {
			outcome, err := sc.applySpawns(ctx, r.res.Spawn)
			if err != nil {
				sc.failNode(ctx, r.nodeID, model.FailInvalidSpawn, err.Error(), node)
				return
			}
			applied = outcome.Applied
		}
		if len(applied) > 0 {
			sc.mu.Lock()
			sc.status.Nodes[r.nodeID].Spawned = append(sc.status.Nodes[r.nodeID].Spawned, applied...)
			sc.mu.Unlock()
		}
		sc.terminals++
		_ = sc.transition(ctx, r.nodeID, model.StateSucceeded, model.FailNone, "attempt succeeded")
	case sc.shouldRetry(node, r):
		// Schedule another attempt with the configured deterministic backoff.
		delay := node.Backoff.Delay(r.attempt)
		sc.mu.Lock()
		sc.readyAt[r.nodeID] = sc.engine.clock.Now().Add(delay)
		sc.mu.Unlock()
		_ = sc.transition(ctx, r.nodeID, model.StateReady, model.FailNone,
			"attempt "+itoa(r.attempt+1)+" failed; retry scheduled after "+delay.String()+": "+r.err.Error())
	default:
		code := model.FailAttemptsExhausted
		if r.timedOut {
			code = model.FailTimeout
		} else if IsPermanent(r.err) {
			code = model.FailPermanent
		}
		sc.failNode(ctx, r.nodeID, code, r.err.Error(), node)
	}
}

func (sc *scheduler) failNode(ctx context.Context, id string, code model.FailureCode, msg string, node spec.NodeSpec) {
	sc.terminals++
	_ = sc.transition(ctx, id, model.StateFailed, code, msg)
}

// shouldRetry reports whether another attempt is allowed for this failure.
func (sc *scheduler) shouldRetry(node spec.NodeSpec, r attemptResult) bool {
	if r.err == nil {
		return false
	}
	if IsPermanent(r.err) {
		return false
	}
	return r.attempt+1 < node.MaxAttempts
}

func modelAttemptRecord(r attemptResult) model.AttemptRecord {
	return model.AttemptRecord{
		Attempt:  r.attempt,
		Started:  r.started,
		Finished: r.finished,
		TimedOut: r.timedOut,
		Err:      errString(r.err),
		Result:   resultValue(r.res),
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func resultValue(res *ExecResult) string {
	if res == nil {
		return ""
	}
	return res.Value
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
