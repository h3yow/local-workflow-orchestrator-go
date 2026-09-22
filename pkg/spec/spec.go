// Package spec defines the declarative workflow specification and its validation.
package spec

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
)

// RejectCode identifies a distinct reason for rejecting a specification.
type RejectCode string

const (
	RejectEmptyGraph         RejectCode = "empty_graph"
	RejectDuplicateNode      RejectCode = "duplicate_node"
	RejectDanglingDep        RejectCode = "dangling_dependency"
	RejectSelfDependency     RejectCode = "self_dependency"
	RejectCycle              RejectCode = "cycle"
	RejectInvalidConcurrency RejectCode = "invalid_concurrency"
	RejectInvalidRetry       RejectCode = "invalid_retry"
	RejectInvalidTimeout     RejectCode = "invalid_timeout"
	RejectInvalidBackoff     RejectCode = "invalid_backoff"
	RejectInvalidQueueSize   RejectCode = "invalid_queue_size"
	RejectInvalidNodeID      RejectCode = "invalid_node_id"
	RejectDuplicateDep       RejectCode = "duplicate_dependency"
)

// ValidationError describes one validation problem.
type ValidationError struct {
	Code   RejectCode
	NodeID string
	Msg    string
}

func (e *ValidationError) Error() string {
	if e.NodeID != "" {
		return fmt.Sprintf("%s: node=%q: %s", e.Code, e.NodeID, e.Msg)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

// ValidationErrors aggregates every problem found in a specification so that
// callers can distinguish and display each rejection reason.
type ValidationErrors []*ValidationError

func (es ValidationErrors) Error() string {
	parts := make([]string, 0, len(es))
	for _, e := range es {
		parts = append(parts, e.Error())
	}
	return "invalid workflow spec: " + strings.Join(parts, "; ")
}

// Has reports whether any aggregated error uses the given code.
func (es ValidationErrors) Has(code RejectCode) bool {
	for _, e := range es {
		if e.Code == code {
			return true
		}
	}
	return false
}

// AsValidationErrors extracts aggregated validation errors.
func AsValidationErrors(err error) (ValidationErrors, bool) {
	var ve ValidationErrors
	if errors.As(err, &ve) {
		return ve, true
	}
	return nil, false
}

// NodeSpec declares one task node.
type NodeSpec struct {
	// ID is the unique node identifier; non-empty.
	ID string
	// Deps lists ids of nodes that must succeed before this node runs.
	Deps []string
	// Timeout bounds a single attempt; 0 means no timeout.
	Timeout time.Duration
	// MaxAttempts is >= 1; values above 1 enable retry.
	MaxAttempts int
	// Backoff configures waits between attempts.
	Backoff backoff.Policy
}

// Spec is a declarative task graph.
type Spec struct {
	Name  string
	Nodes []NodeSpec
	// MaxParallel bounds nodes running at once; must be >= 1.
	MaxParallel int
	// MaxQueue bounds admitted ready nodes waiting for a slot; 0 means that
	// only nodes already eligible for dispatch are held (fully backpressured).
	MaxQueue int
}

// Validate checks identity, topology and numeric constraints, returning a
// ValidationErrors value describing every problem found.
func (s *Spec) Validate() error {
	var errs ValidationErrors

	if len(s.Nodes) == 0 {
		errs = append(errs, &ValidationError{Code: RejectEmptyGraph, Msg: "spec has no nodes"})
		return errs
	}
	if s.MaxParallel < 1 {
		errs = append(errs, &ValidationError{Code: RejectInvalidConcurrency,
			Msg: fmt.Sprintf("max parallel must be >= 1, got %d", s.MaxParallel)})
	}
	if s.MaxQueue < 0 {
		errs = append(errs, &ValidationError{Code: RejectInvalidQueueSize,
			Msg: fmt.Sprintf("max queue must be >= 0, got %d", s.MaxQueue)})
	}

	seen := make(map[string]int, len(s.Nodes))
	for i, n := range s.Nodes {
		if strings.TrimSpace(n.ID) == "" {
			errs = append(errs, &ValidationError{Code: RejectInvalidNodeID,
				Msg: fmt.Sprintf("node at index %d has empty id", i)})
			continue
		}
		if prev, ok := seen[n.ID]; ok {
			errs = append(errs, &ValidationError{Code: RejectDuplicateNode, NodeID: n.ID,
				Msg: fmt.Sprintf("duplicated at indices %d and %d", prev, i)})
			continue
		}
		seen[n.ID] = i

		if n.MaxAttempts < 1 {
			errs = append(errs, &ValidationError{Code: RejectInvalidRetry, NodeID: n.ID,
				Msg: fmt.Sprintf("max attempts must be >= 1, got %d", n.MaxAttempts)})
		}
		if n.Timeout < 0 {
			errs = append(errs, &ValidationError{Code: RejectInvalidTimeout, NodeID: n.ID,
				Msg: fmt.Sprintf("timeout must be >= 0, got %s", n.Timeout)})
		}
		if err := n.Backoff.Validate(); err != nil {
			errs = append(errs, &ValidationError{Code: RejectInvalidBackoff, NodeID: n.ID, Msg: err.Error()})
		}
	}

	// Dependency-level checks (identity, dangling, self, duplicate edges).
	indeg := make(map[string]int, len(seen))
	depSetOk := make(map[string]map[string]struct{}, len(seen))
	for _, n := range s.Nodes {
		if _, known := seen[n.ID]; !known {
			continue
		}
		indeg[n.ID] = 0
	}
	for _, n := range s.Nodes {
		if _, known := seen[n.ID]; !known {
			continue
		}
		edges := make(map[string]struct{})
		for _, dep := range n.Deps {
			if dep == n.ID {
				errs = append(errs, &ValidationError{Code: RejectSelfDependency, NodeID: n.ID,
					Msg: "node lists itself as dependency"})
				continue
			}
			if _, ok := seen[dep]; !ok {
				errs = append(errs, &ValidationError{Code: RejectDanglingDep, NodeID: n.ID,
					Msg: fmt.Sprintf("dependency %q does not exist", dep)})
				continue
			}
			if _, dup := edges[dep]; dup {
				errs = append(errs, &ValidationError{Code: RejectDuplicateDep, NodeID: n.ID,
					Msg: fmt.Sprintf("dependency %q listed more than once", dep)})
				continue
			}
			edges[dep] = struct{}{}
		}
		depSetOk[n.ID] = edges
	}

	// Topology check only with well-formed edges so a cycle error is not
	// masked by earlier structural problems.
	if len(errs) == 0 {
		for id, edges := range depSetOk {
			indeg[id] = len(edges)
		}
		ready := make([]string, 0)
		for id, d := range indeg {
			if d == 0 {
				ready = append(ready, id)
			}
		}
		sortStrings(ready)
		remaining := len(indeg)
		dependents := make(map[string][]string)
		for id, edges := range depSetOk {
			for dep := range edges {
				dependents[dep] = append(dependents[dep], id)
			}
		}
		for _, list := range dependents {
			sortStrings(list)
		}
		for len(ready) > 0 {
			id := ready[0]
			ready = ready[1:]
			remaining--
			for _, ch := range dependents[id] {
				indeg[ch]--
				if indeg[ch] == 0 {
					ready = insertSorted(ready, ch)
				}
			}
		}
		if remaining > 0 {
			cycleNodes := make([]string, 0)
			for id, d := range indeg {
				if d > 0 {
					cycleNodes = append(cycleNodes, id)
				}
			}
			sortStrings(cycleNodes)
			errs = append(errs, &ValidationError{Code: RejectCycle,
				Msg: fmt.Sprintf("dependency cycle involves nodes: %s", strings.Join(cycleNodes, ", "))})
		}
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func sortStrings(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j-1] > x[j]; j-- {
			x[j-1], x[j] = x[j], x[j-1]
		}
	}
}

func insertSorted(x []string, v string) []string {
	i := 0
	for i < len(x) && x[i] < v {
		i++
	}
	x = append(x, "")
	copy(x[i+1:], x[i:])
	x[i] = v
	return x
}
