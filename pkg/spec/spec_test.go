package spec

import (
	"strings"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/backoff"
)

func base(nodes ...NodeSpec) *Spec {
	return &Spec{Name: "t", MaxParallel: 2, MaxQueue: 4, Nodes: nodes}
}
func n(id string, deps ...string) NodeSpec {
	return NodeSpec{ID: id, Deps: deps, MaxAttempts: 1, Timeout: time.Second}
}

func TestValidateRejectReasons(t *testing.T) {
	cases := []struct {
		name string
		spec *Spec
		want RejectCode
	}{
		{"empty graph", base(), RejectEmptyGraph},
		{"duplicate node", base(n("a"), n("a")), RejectDuplicateNode},
		{"dangling dependency", base(n("a", "ghost")), RejectDanglingDep},
		{"self dependency", base(n("a", "a")), RejectSelfDependency},
		{"cycle", base(n("a", "b"), n("b", "a")), RejectCycle},
		{"three node cycle", base(n("a", "c"), n("b", "a"), n("c", "b")), RejectCycle},
		{"bad concurrency", func() *Spec { s := base(n("a")); s.MaxParallel = 0; return s }(), RejectInvalidConcurrency},
		{"bad retry", base(NodeSpec{ID: "a", MaxAttempts: 0}), RejectInvalidRetry},
		{"bad timeout", base(NodeSpec{ID: "a", MaxAttempts: 1, Timeout: -1}), RejectInvalidTimeout},
		{"bad backoff", base(NodeSpec{ID: "a", MaxAttempts: 1, Backoff: backoff.Policy{Kind: backoff.KindExponent, Factor: 0.5}}), RejectInvalidBackoff},
		{"bad queue", func() *Spec { s := base(n("a")); s.MaxQueue = -1; return s }(), RejectInvalidQueueSize},
		{"empty id", base(NodeSpec{MaxAttempts: 1}), RejectInvalidNodeID},
		{"duplicate dependency edge", base(n("a"), NodeSpec{ID: "b", Deps: []string{"a", "a"}, MaxAttempts: 1}), RejectDuplicateDep},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("INPUT SPEC: name=%q maxParallel=%d maxQueue=%d nodes=%v",
				tc.spec.Name, tc.spec.MaxParallel, tc.spec.MaxQueue, summarize(tc.spec))
			err := tc.spec.Validate()
			if err == nil {
				t.Fatalf("expected rejection %s, got nil", tc.want)
			}
			ves, ok := AsValidationErrors(err)
			if !ok {
				t.Fatalf("expected ValidationErrors, got %T: %v", err, err)
			}
			t.Logf("REJECTION BASIS: %v", ves)
			if !ves.Has(tc.want) {
				t.Fatalf("expected reject code %s, got %v", tc.want, ves)
			}
			// The full aggregated error string must also carry the code so
			// reasons remain distinguishable at the API boundary.
			if !strings.Contains(err.Error(), string(tc.want)) {
				t.Fatalf("error string %q missing code %s", err.Error(), tc.want)
			}
		})
	}
}

func TestValidateAggregatesMultipleProblems(t *testing.T) {
	s := base(n("a", "ghost"), NodeSpec{ID: "a", MaxAttempts: 0})
	t.Logf("INPUT SPEC: %v", summarize(s))
	err := s.Validate()
	ves, ok := AsValidationErrors(err)
	if !ok || len(ves) < 2 {
		t.Fatalf("expected >=2 aggregated errors, got %v", err)
	}
	t.Logf("REJECTIONS: %v", ves)
}

func TestValidSpecBuildsCanonicalOrder(t *testing.T) {
	s := base(n("d", "b", "c"), n("c", "a"), n("b", "a"), n("a"))
	g, err := Build(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ids := g.IDs()
	t.Logf("CANONICAL ORDER: %v", ids)
	want := []string{"a", "b", "c", "d"}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order=%v want %v", ids, want)
		}
	}
}

func summarize(s *Spec) []string {
	out := make([]string, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		out = append(out, n.ID+"<-["+strings.Join(n.Deps, ",")+"]")
	}
	return out
}
