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
	mu        sync.RWMutex
	status    map[string]*model.RunStatus
	order     []string
	history   map[string]*model.History
	actions   map[string]*model.ActionLog
	actionSeq map[string]int64
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		status:    make(map[string]*model.RunStatus),
		history:   make(map[string]*model.History),
		actions:   make(map[string]*model.ActionLog),
		actionSeq: make(map[string]int64),
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

func (m *MemoryStore) CommitAction(ctx context.Context, rec *model.ActionRecord) (*model.ActionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	log := m.actions[rec.RunID]
	if log == nil {
		log = &model.ActionLog{RunID: rec.RunID}
		m.actions[rec.RunID] = log
	}
	// First writer for Key within the run wins (which node or which attempt
	// registered it is irrelevant); later commits return the stored record
	// unchanged so the side effect is counted exactly once per run.
	for i := range log.Actions {
		if log.Actions[i].Key == rec.Key {
			stored := log.Actions[i]
			return &stored, false, nil
		}
	}
	stored := *rec
	m.actionSeq[rec.RunID]++
	stored.Seq = m.actionSeq[rec.RunID]
	log.Actions = append(log.Actions, stored)
	return &stored, true, nil
}

func (m *MemoryStore) LoadActions(ctx context.Context, runID string) (*model.ActionLog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	log, ok := m.actions[runID]
	if !ok {
		if _, exists := m.status[runID]; exists {
			return &model.ActionLog{RunID: runID}, nil
		}
		return nil, model.ErrNotFound
	}
	out := log.Clone()
	sortActions(out.Actions)
	return out, nil
}

// sortActions orders records by commit Seq (the only order records are ever
// appended in; sorting makes the guarantee explicit and reproducible).
func sortActions(as []model.ActionRecord) {
	sort.Slice(as, func(i, j int) bool { return as[i].Seq < as[j].Seq })
}
