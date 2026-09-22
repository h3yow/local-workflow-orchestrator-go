package spec

import (
	"strings"
	"testing"
	"time"
)

// ns builds a node with default concurrency slot (raw Validate rejects 0).
func ns(id string, deps ...string) NodeSpec {
	return NodeSpec{ID: id, Deps: deps, Slots: 1}
}

func reason(t *testing.T, s *Spec) RejectReason {
	t.Helper()
	err := s.Validate()
	if err == nil {
		t.Fatalf("expected rejection, got nil")
	}
	re, ok := err.(*RejectError)
	if !ok {
		t.Fatalf("expected *RejectError, got %T: %v", err, err)
	}
	t.Logf("input spec id=%q nodes=%d -> rejected reason=%s subject=%q: %v",
		s.ID, len(s.Nodes), re.Reason, re.Subject, err)
	return re.Reason
}

func TestValidate_OK(t *testing.T) {
	s := &Spec{ID: "ok", Nodes: []NodeSpec{
		{ID: "a", Slots: 1, Timeout: time.Millisecond, MaxRetries: 2},
		{ID: "b", Deps: []string{"a"}, Slots: 2},
		{ID: "c", Deps: []string{"a", "b"}, Slots: 1},
	}}
	if err := s.Validate(); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	if got := s.Topological(); strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("topo=%v", got)
	}
}

func TestValidate_EmptySpecID(t *testing.T) {
	if r := reason(t, &Spec{Nodes: []NodeSpec{{ID: "a"}}}); r != ReasonEmptyID {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_EmptyNodeID(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{ns("a"), {ID: "", Slots: 1}}}
	if r := reason(t, s); r != ReasonEmptyID {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_DuplicateID(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{ns("a"), ns("a")}}
	if r := reason(t, s); r != ReasonDuplicateID {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_DanglingDependency(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{
		ns("a"), {ID: "b", Deps: []string{"ghost"}, Slots: 1},
	}}
	if r := reason(t, s); r != ReasonDanglingDependency {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_SelfCycle(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{{ID: "a", Deps: []string{"a"}, Slots: 1}}}
	if r := reason(t, s); r != ReasonCycle {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_Cycle(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{
		ns("a", "c"), ns("b", "a"), ns("c", "b"),
	}}
	r := reason(t, s)
	if r != ReasonCycle {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_CycleReasonContainsPath(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{
		ns("a", "b"), ns("b", "a"),
	}}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "a -> b -> a") &&
		!strings.Contains(err.Error(), "b -> a -> b") {
		t.Fatalf("cycle path missing from error: %v", err)
	}
}

func TestValidate_IllegalValues(t *testing.T) {
	cases := []struct {
		name string
		node NodeSpec
	}{
		{"negative timeout", NodeSpec{ID: "a", Slots: 1, Timeout: -1}},
		{"negative retries", NodeSpec{ID: "a", Slots: 1, MaxRetries: -1}},
		{"zero slots", NodeSpec{ID: "a", Slots: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Spec{ID: "s", Nodes: []NodeSpec{tc.node}}
			// zero slots: raw Validate rejects; Normalize promotes to 1.
			if tc.node.Slots == 0 {
				if err := s.Validate(); err == nil {
					t.Fatal("zero slots must be rejected before normalization")
				}
				s.Normalize()
				if err := s.Validate(); err != nil {
					t.Fatalf("normalized spec must validate: %v", err)
				}
				return
			}
			if r := reason(t, s); r != ReasonInvalidValue {
				t.Fatalf("reason=%s", r)
			}
		})
	}
}

func TestValidate_EmptyGraph(t *testing.T) {
	if r := reason(t, &Spec{ID: "s"}); r != ReasonInvalidValue {
		t.Fatalf("reason=%s", r)
	}
}

func TestValidate_RepeatedDepEdge(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{
		ns("a"), {ID: "b", Deps: []string{"a", "a"}, Slots: 1},
	}}
	if r := reason(t, s); r != ReasonInvalidValue {
		t.Fatalf("reason=%s", r)
	}
}

func TestTopological_Deterministic(t *testing.T) {
	s := &Spec{ID: "s", Nodes: []NodeSpec{
		ns("d", "b", "c"), ns("c", "a"), ns("b", "a"), ns("a"),
	}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	first := strings.Join(s.Topological(), ",")
	for i := 0; i < 5; i++ {
		if got := strings.Join(s.Topological(), ","); got != first {
			t.Fatalf("non-deterministic topo: %q vs %q", got, first)
		}
	}
	if first != "a,c,b,d" && first != "a,b,c,d" {
		// declaration order: a is index 3 so ready seeds in declaration order
		// of traversal; accept documented deterministic outputs only.
		t.Fatalf("unexpected topo=%q", first)
	}
}
