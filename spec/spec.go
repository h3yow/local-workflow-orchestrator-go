// Package spec defines the declarative task-graph specification and its
// static validation rules.
package spec

import (
	"fmt"
	"strings"
	"time"
)

// NodeSpec declares one task node in the graph.
type NodeSpec struct {
	// ID is the unique node identifier within a spec. Must be non-empty.
	ID string

	// Deps lists node IDs that must reach a terminal state before this node
	// may be scheduled. Every referenced ID must exist in the same spec
	// (or be introduced via dynamic expansion).
	Deps []string

	// Timeout bounds a single attempt. Zero means no timeout. Must be >= 0.
	Timeout time.Duration

	// MaxRetries is the number of retries AFTER the first attempt.
	// 0 means fail on the first error. Must be >= 0.
	MaxRetries int

	// Slots is the concurrency capacity this node occupies while running
	// (the node's declared concurrency requirement). Must be >= 1.
	Slots int
}

// Spec is a declarative task graph.
type Spec struct {
	// ID identifies the graph definition. Must be non-empty.
	ID string

	// Nodes are declared in a stable order that participates in
	// deterministic scheduling tie-breaking.
	Nodes []NodeSpec
}

// RejectReason is the distinguishable machine-readable reason a spec is rejected.
type RejectReason string

const (
	ReasonEmptyID            RejectReason = "empty_id"
	ReasonDuplicateID        RejectReason = "duplicate_id"
	ReasonDanglingDependency RejectReason = "dangling_dependency"
	ReasonCycle              RejectReason = "cycle"
	ReasonInvalidValue       RejectReason = "invalid_value"
)

// RejectError reports why a specification was refused.
type RejectError struct {
	Reason  RejectReason
	Subject string // node ID or field involved, when applicable
	Message string
}

func (e *RejectError) Error() string { return e.Message }

// Normalize fills defaults (Slots==0 -> 1). It does not validate.
func (s *Spec) Normalize() {
	for i := range s.Nodes {
		if s.Nodes[i].Slots == 0 {
			s.Nodes[i].Slots = 1
		}
	}
}

func reject(reason RejectReason, subject, format string, args ...any) error {
	return &RejectError{Reason: reason, Subject: subject, Message: fmt.Sprintf(format, args...)}
}

// Validate checks the static graph: duplicate ids, dangling dependencies,
// cycles and illegal numeric values.
//
// Nodes are visited in declaration order, so the first reported defect is
// stable for a given spec (deterministic rejection reason).
func (s *Spec) Validate() error {
	if strings.TrimSpace(s.ID) == "" {
		return reject(ReasonEmptyID, "<spec>", "spec id must not be empty")
	}
	if len(s.Nodes) == 0 {
		return reject(ReasonInvalidValue, "<spec>", "spec %q has no nodes", s.ID)
	}

	seen := make(map[string]int, len(s.Nodes))
	for i := range s.Nodes {
		n := &s.Nodes[i]
		if strings.TrimSpace(n.ID) == "" {
			return reject(ReasonEmptyID, fmt.Sprintf("nodes[%d]", i),
				"node id must not be empty (spec %q, index %d)", s.ID, i)
		}
		if first, dup := seen[n.ID]; dup {
			return reject(ReasonDuplicateID, n.ID,
				"duplicate node id %q (first at index %d, again at %d)", n.ID, first, i)
		}
		seen[n.ID] = i
		if n.Timeout < 0 {
			return reject(ReasonInvalidValue, n.ID,
				"node %q has negative timeout %s", n.ID, n.Timeout)
		}
		if n.MaxRetries < 0 {
			return reject(ReasonInvalidValue, n.ID,
				"node %q has negative max_retries %d", n.ID, n.MaxRetries)
		}
		if n.Slots < 1 {
			return reject(ReasonInvalidValue, n.ID,
				"node %q has illegal slots %d (must be >= 1; 0 normalizes to 1)", n.ID, n.Slots)
		}
	}

	// Dangling dependencies and repeated/self edges.
	for i := range s.Nodes {
		n := &s.Nodes[i]
		edge := make(map[string]bool, len(n.Deps))
		for _, d := range n.Deps {
			if _, ok := seen[d]; !ok {
				return reject(ReasonDanglingDependency, d,
					"node %q depends on undeclared node %q", n.ID, d)
			}
			if d == n.ID {
				return reject(ReasonCycle, n.ID,
					"cycle detected at node %q (self dependency)", n.ID)
			}
			if edge[d] {
				return reject(ReasonInvalidValue, n.ID,
					"node %q lists dependency %q more than once", n.ID, d)
			}
			edge[d] = true
		}
	}

	// Iterative DFS cycle detection with deterministic exploration order:
	// roots and edges follow declaration order.
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(s.Nodes))
	byID := make(map[string]*NodeSpec, len(s.Nodes))
	for i := range s.Nodes {
		byID[s.Nodes[i].ID] = &s.Nodes[i]
	}

	for i := range s.Nodes {
		root := s.Nodes[i].ID
		if color[root] != white {
			continue
		}
		type frame struct {
			id    string
			depIx int
		}
		stack := []frame{{id: root}}
		path := []string{root}
		color[root] = gray
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			node := byID[top.id]
			if top.depIx < len(node.Deps) {
				d := node.Deps[top.depIx]
				top.depIx++
				switch color[d] {
				case white:
					color[d] = gray
					path = append(path, d)
					stack = append(stack, frame{id: d})
				case gray:
					start := 0
					for k, v := range path {
						if v == d {
							start = k
							break
						}
					}
					cycle := append(append([]string{}, path[start:]...), d)
					return reject(ReasonCycle, d,
						"cycle detected: %s", strings.Join(cycle, " -> "))
				}
				continue
			}
			color[top.id] = black
			stack = stack[:len(stack)-1]
			path = path[:len(path)-1]
		}
	}
	return nil
}

// Topological returns node ids in a deterministic topological order
// (declaration order among nodes whose deps are already emitted).
// The caller must have validated the spec.
func (s *Spec) Topological() []string {
	indeg := make(map[string]int, len(s.Nodes))
	children := make(map[string][]string)
	for i := range s.Nodes {
		n := &s.Nodes[i]
		indeg[n.ID] = len(n.Deps)
		for _, d := range n.Deps {
			children[d] = append(children[d], n.ID)
		}
	}
	ready := make([]string, 0)
	for i := range s.Nodes {
		if indeg[s.Nodes[i].ID] == 0 {
			ready = append(ready, s.Nodes[i].ID)
		}
	}
	order := make([]string, 0, len(s.Nodes))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for _, k := range children[id] {
			indeg[k]--
			if indeg[k] == 0 {
				ready = append(ready, k)
			}
		}
	}
	return order
}
