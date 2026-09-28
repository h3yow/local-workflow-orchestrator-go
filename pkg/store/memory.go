package store

import (
	"context"
	"sort"
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
	actions map[string]map[string]model.ActionRecord
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		status:  make(map[string]*model.RunStatus),
		history: make(map[string]*model.History),
		actions: make(map[string]map[string]model.ActionRecord),
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

func (m *MemoryStore) AppendAction(ctx context.Context, rec model.ActionRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	byKey := m.actions[rec.RunID]
	if byKey == nil {
		byKey = make(map[string]model.ActionRecord)
		m.actions[rec.RunID] = byKey
	}
	if _, exists := byKey[rec.Key]; exists {
		// First committer wins: the business action is effective exactly once.
		return false, nil
	}
	byKey[rec.Key] = rec
	return true, nil
}

func (m *MemoryStore) LoadActions(ctx context.Context, runID string) ([]model.ActionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	byKey, ok := m.actions[runID]
	if !ok {
		if _, exists := m.status[runID]; exists {
			return []model.ActionRecord{}, nil
		}
		return nil, model.ErrNotFound
	}
	out := make([]model.ActionRecord, 0, len(byKey))
	for _, rec := range byKey {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
