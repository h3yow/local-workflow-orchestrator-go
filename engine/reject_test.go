package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/local-workflow-orchestrator-go/spec"
)

func TestCreateRejectsInvalidSpec(t *testing.T) {
	e, _ := newTestEngine(t, 2, 8)
	noop := func(ctx context.Context, runID, id string) (string, []string, error) {
		return "", nil, nil
	}
	cases := []struct {
		name string
		s    *spec.Spec
		want spec.RejectReason
	}{
		{"cycle", &spec.Spec{ID: "s", Nodes: []spec.NodeSpec{
			{ID: "a", Deps: []string{"b"}, Slots: 1},
			{ID: "b", Deps: []string{"a"}, Slots: 1},
		}}, spec.ReasonCycle},
		{"dangling", &spec.Spec{ID: "s", Nodes: []spec.NodeSpec{
			{ID: "a", Deps: []string{"x"}, Slots: 1},
		}}, spec.ReasonDanglingDependency},
		{"duplicate", &spec.Spec{ID: "s", Nodes: []spec.NodeSpec{
			{ID: "a", Slots: 1}, {ID: "a", Slots: 1},
		}}, spec.ReasonDuplicateID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.Create(context.Background(), tc.s, noop, Option{RunID: "rej-" + tc.name})
			var re *spec.RejectError
			if !errors.As(err, &re) {
				t.Fatalf("err=%v want *spec.RejectError", err)
			}
			if re.Reason != tc.want {
				t.Fatalf("reason=%s want %s", re.Reason, tc.want)
			}
		})
	}
}
