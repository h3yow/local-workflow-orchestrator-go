package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
	"github.com/h3yow/local-workflow-orchestrator-go/store"
)

// Distinguishable expansion failure causes.
var (
	errInvalidExpansion   = errors.New("invalid expansion")
	errDuplicateNode      = errors.New("duplicate expanded node")
	errExpansionCycle     = errors.New("expansion introduces a cycle")
	errDanglingDependency = errors.New("expansion introduces dangling dependency")
)

// recordEffect appends an effect event exactly once (idempotent key).
// The store is the dedup authority: even a crash between event append and
// snapshot persist cannot duplicate the effect record on recovery.
func (rc *runCtx) recordEffect(nodeID, effectKey string) {
	if effectKey == "" {
		return
	}
	if contains(rc.state.Effects, effectKey) {
		return
	}
	rc.state.Seq++
	ev := store.Event{
		Seq:    rc.state.Seq,
		Key:    fmt.Sprintf("effect:%s:%s", nodeID, effectKey),
		Kind:   kindEffect,
		At:     time.Now().UTC().UnixNano(),
		Detail: effectKey,
	}
	ok, err := rc.engine.cfg.Store.AppendEvent(rc.ctx, rc.id, ev)
	if err != nil {
		rc.fatalErr = errors.New("persist effect for node " + nodeID + ": " + err.Error())
		return
	}
	if ok {
		rc.state.Effects = append(rc.state.Effects, effectKey)
	}
}

// applyExpand validates the enlarged graph and atomically admits new nodes.
// Called from the event loop only.
//
// Semantics:
//   - every new node id must be new to the run ("produced at most once");
//   - after merge every dependency must exist, the graph must stay acyclic
//     and all numeric values must be legal;
//   - the parent must have succeeded;
//   - a new node depending on a failed/skipped node is immediately Skipped;
//   - admission is atomic: if the immediately-eligible new nodes do not all
//     fit the bounded queue backlog, nothing takes effect (ErrBackpressure).
func (rc *runCtx) applyExpand(parentID string, batch []spec.NodeSpec, fromHook bool) error {
	if len(batch) == 0 {
		return nil
	}

	// Duplicate production is checked first: a node id that already exists
	// (or repeats within the batch) can never take effect twice, regardless
	// of the parent's state. This gives a stable, distinguishable rejection.
	batchIndex := make(map[string]int)
	for i := range batch {
		n := &batch[i]
		if n.Slots == 0 {
			n.Slots = 1
		}
		if _, exists := rc.state.Nodes[n.ID]; exists {
			return fmt.Errorf("%w: %q already exists in run", errDuplicateNode, n.ID)
		}
		if _, dup := batchIndex[n.ID]; dup {
			return fmt.Errorf("%w: %q listed twice in the same expansion", errDuplicateNode, n.ID)
		}
		batchIndex[n.ID] = i
	}

	parent, ok := rc.state.Nodes[parentID]
	if !ok || !parent.Status.Terminal() || parent.Status != state.Succeeded {
		return fmt.Errorf("%w: parent node %q must have succeeded before expansion",
			errInvalidExpansion, parentID)
	}

	// 1) Build the merged spec strictly for validation.
	merged := &spec.Spec{ID: rc.state.SpecID}
	for _, id := range rc.orderedIDs() {
		nd := rc.state.Nodes[id]
		merged.Nodes = append(merged.Nodes, spec.NodeSpec{
			ID:         nd.ID,
			Deps:       append([]string(nil), nd.Deps...),
			Timeout:    time.Duration(nd.Timeout),
			MaxRetries: nd.MaxRetries,
			Slots:      nd.Slots,
		})
	}
	for i := range batch {
		merged.Nodes = append(merged.Nodes, batch[i])
	}
	if err := merged.Validate(); err != nil {
		if re, ok := err.(*spec.RejectError); ok {
			switch re.Reason {
			case spec.ReasonCycle:
				return fmt.Errorf("%w: %v", errExpansionCycle, err)
			case spec.ReasonDanglingDependency:
				return fmt.Errorf("%w: %v", errDanglingDependency, err)
			}
		}
		return fmt.Errorf("%w: %v", errInvalidExpansion, err)
	}

	// 2) Determine prospective status of each new node using only current
	//    (committed) node states. New nodes cannot depend on terminals within
	//    the same batch, so such deps simply keep the node Pending.
	type planned struct {
		node             *spec.NodeSpec
		eligible, badDep bool
	}
	plans := make([]planned, 0, len(batch))
	need := 0
	for i := range batch {
		n := &batch[i]
		eligible, badDep := true, false
		for _, d := range n.Deps {
			if dn, old := rc.state.Nodes[d]; old {
				if !dn.Status.Terminal() {
					eligible = false
					break
				}
				if dn.Status == state.Failed || dn.Status == state.Skipped {
					badDep = true
				}
				continue
			}
			// dep is a brand-new node in this batch: not terminal yet
			eligible = false
			break
		}
		plans = append(plans, planned{n, eligible, badDep})
		if eligible && !badDep {
			need++
		}
	}

	// 3) Bounded backlog admission, atomic.
	if len(rc.ready)+need > rc.engine.cfg.MaxQueue {
		return fmt.Errorf("%w: expansion needs %d slot(s), backlog %d/%d",
			ErrBackpressure, need, len(rc.ready), rc.engine.cfg.MaxQueue)
	}

	// 4) Commit: register nodes in batch order with monotonic OrderKeys.
	now := time.Now().UTC()
	for _, p := range plans {
		rc.state.Seq++
		n := p.node
		nd := &state.NodeState{
			ID:         n.ID,
			Deps:       append([]string(nil), n.Deps...),
			Slots:      n.Slots,
			Timeout:    int64(n.Timeout),
			MaxRetries: n.MaxRetries,
			OrderKey:   rc.state.Seq,
			ExpandedBy: parentID,
			UpdatedAt:  now,
		}
		switch {
		case p.badDep:
			nd.Status = state.Skipped
			nd.FailureReason = "skipped: upstream dependency failed or was skipped"
			rc.logf("expansion: new node=%s -> skipped (bad upstream dep)", n.ID)
		case p.eligible:
			nd.Status = state.Queued
			rc.ready = append(rc.ready, nd.ID)
			rc.logf("expansion: new node=%s admitted (parent=%s)", n.ID, parentID)
		default:
			nd.Status = state.Pending
			rc.logf("expansion: new node=%s pending (waiting on deps, parent=%s)", n.ID, parentID)
		}
		rc.state.Nodes[n.ID] = nd
	}
	rc.sortReady()
	return nil
}
