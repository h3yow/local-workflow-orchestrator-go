package store

import (
	"context"
	"sync"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// MemoryStore is an in-memory Store useful for tests. All reads return deep
// copies so concurrent observers never see partially updated state.
type MemoryStore struct {
	mu      sync.RWMutex
	status  map[string]*model.RunStatus
	order   []string
	history map[string]*model.History
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		status:  make(map[string]*model.RunStatus),
		history: make(map[string]*model.History),
	}
}

func (m *MemoryStore) SaveStatus(ctx context.Context, status *model.RunStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.status[status.RunID]; !ok {
		m.order = append(m.order, status.RunID)
	}
	m.status[status.RunID] = status.Clone()
	return nil
}

func (m *MemoryStore) LoadStatus(ctx context.Context, runID string) (*model.RunStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.status[runID]
	if !ok {
		return nil, model.ErrNotFound
	}
	return s.Clone(), nil
}

func (m *MemoryStore) ListRuns(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]string(nil), m.order...), nil
}

func (m *MemoryStore) AppendEvent(ctx context.Context, ev *model.SnapshotEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.history[ev.RunID]
	if h == nil {
		h = &model.History{RunID: ev.RunID}
		m.history[ev.RunID] = h
	}
	// Idempotent: a replayed append (e.g. after crash) with the same Seq is
	// not recorded twice, so no duplicate side-effect history is produced.
	for _, e := range h.Events {
		if e.Seq == ev.Seq {
			return nil
		}
	}
	h.Events = append(h.Events, *ev)
	return nil
}

func (m *MemoryStore) LoadHistory(ctx context.Context, runID string) (*model.History, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.history[runID]
	if !ok {
		if _, exists := m.status[runID]; exists {
			return &model.History{RunID: runID}, nil
		}
		return nil, model.ErrNotFound
	}
	return h.Clone(), nil
}
