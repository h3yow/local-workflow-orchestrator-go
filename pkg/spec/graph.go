package spec

import "sort"

// Graph is a validated, indexed view over a Spec.
type Graph struct {
	spec       *Spec
	index      map[string]int
	dependents map[string][]string
	order      []string // lexicographically smallest topological order
}

// Build validates s and returns an indexed Graph.
func Build(s *Spec) (*Graph, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	g := &Graph{
		spec:       s,
		index:      make(map[string]int, len(s.Nodes)),
		dependents: make(map[string][]string, len(s.Nodes)),
	}
	for i, n := range s.Nodes {
		g.index[n.ID] = i
	}
	indeg := make(map[string]int, len(s.Nodes))
	for _, n := range s.Nodes {
		indeg[n.ID] = len(n.Deps)
		for _, dep := range n.Deps {
			g.dependents[dep] = append(g.dependents[dep], n.ID)
		}
	}
	for id := range g.dependents {
		sort.Strings(g.dependents[id])
	}
	// Kahn with lexicographic ordering gives one canonical schedule order.
	ready := make([]string, 0)
	for id, d := range indeg {
		if d == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		g.order = append(g.order, id)
		for _, ch := range g.dependents[id] {
			indeg[ch]--
			if indeg[ch] == 0 {
				ready = insertSorted(ready, ch)
			}
		}
	}
	return g, nil
}

// Spec returns the underlying specification.
func (g *Graph) Spec() *Spec { return g.spec }

// Node returns a node spec by id.
func (g *Graph) Node(id string) (NodeSpec, bool) {
	if i, ok := g.index[id]; ok {
		return g.spec.Nodes[i], true
	}
	return NodeSpec{}, false
}

// Dependents returns node ids that depend on id (sorted).
func (g *Graph) Dependents(id string) []string {
	out := g.dependents[id]
	cp := make([]string, len(out))
	copy(cp, out)
	return cp
}

// IDs returns all node ids in the canonical deterministic order.
func (g *Graph) IDs() []string {
	out := make([]string, len(g.order))
	copy(out, g.order)
	return out
}
