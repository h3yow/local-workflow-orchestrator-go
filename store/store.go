// Package store provides durable persistence of run snapshots and an
// append-only, idempotently-keyed event log.
//
// Layout on disk (one directory per store):
//
//	<dir>/<runID>/snapshot.json  – latest full run state (atomic temp+rename)
//	<dir>/<runID>/events.log     – append-only JSON-lines history
//
// Observers reading a snapshot always observe either the previous or the new
// complete file, never a half-written one.
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/h3yow/local-workflow-orchestrator-go/state"
)

// ErrNotFound is returned when no run exists for the requested id.
var ErrNotFound = errors.New("store: run not found")

// Event is one append-only, idempotently-keyed record.
type Event struct {
	Seq    int64  `json:"seq"`
	Key    string `json:"key"` // unique within a run (dedup anchor)
	Kind   string `json:"kind"`
	At     int64  `json:"at_ns"`
	Detail string `json:"detail,omitempty"`
}

// Store persists run state and event history.
type Store interface {
	// SaveSnapshot atomically persists the full run snapshot.
	SaveSnapshot(ctx context.Context, rs *state.RunState) error
	// LoadSnapshot returns the most recently persisted run snapshot.
	LoadSnapshot(ctx context.Context, runID string) (*state.RunState, error)
	// ListRuns returns the ids of all historically persisted runs.
	ListRuns(ctx context.Context) ([]string, error)

	// AppendEvent appends an event; a duplicate Key is a no-op and
	// reports ok=false without error.
	AppendEvent(ctx context.Context, runID string, ev Event) (ok bool, err error)
	// ListEvents returns the full event log in append order.
	ListEvents(ctx context.Context, runID string) ([]Event, error)
}

// FileStore is a directory-backed JSON store with atomic writes.
type FileStore struct {
	dir string
	mu  sync.Mutex
	// keys[runID] holds the already-persisted event keys of that run.
	keys map[string]map[string]struct{}
}

// NewFileStore creates (or opens) a file-backed store in dir.
func NewFileStore(dir string) (*FileStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("store: dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir: %w", err)
	}
	fs := &FileStore{dir: dir, keys: make(map[string]map[string]struct{})}
	return fs, nil
}

func (fs *FileStore) runDir(runID string) string {
	return filepath.Join(fs.dir, safeName(runID))
}

func safeName(s string) string {
	// Run ids are used as path components; keep the on-disk layout safe.
	r := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", "..", "_")
	return r.Replace(s)
}

// writeFileAtomic writes to a temp file and renames into place.
func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// SaveSnapshot implements Store.
func (fs *FileStore) SaveSnapshot(ctx context.Context, rs *state.RunState) error {
	if rs == nil || rs.RunID == "" {
		return errors.New("store: snapshot requires a run id")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal snapshot: %w", err)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return writeFileAtomic(filepath.Join(fs.runDir(rs.RunID), "snapshot.json"), data)
}

// LoadSnapshot implements Store.
func (fs *FileStore) LoadSnapshot(ctx context.Context, runID string) (*state.RunState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(fs.runDir(runID), "snapshot.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("store: read snapshot: %w", err)
	}
	var rs state.RunState
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("store: decode snapshot: %w", err)
	}
	if rs.Nodes == nil {
		rs.Nodes = make(map[string]*state.NodeState)
	}
	return &rs, nil
}

// ListRuns implements Store.
func (fs *FileStore) ListRuns(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(fs.dir)
	if err != nil {
		return nil, fmt.Errorf("store: list runs: %w", err)
	}
	var ids []string
	for _, en := range entries {
		if !en.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(fs.dir, en.Name(), "snapshot.json")); err == nil {
			ids = append(ids, en.Name())
		}
	}
	return ids, nil
}

// eventKeys lazily rebuilds the dedup index from the on-disk log.
func (fs *FileStore) eventKeys(runID string) (map[string]struct{}, error) {
	if k, ok := fs.keys[runID]; ok {
		return k, nil
	}
	events, err := fs.readLog(runID)
	if err != nil {
		return nil, err
	}
	k := make(map[string]struct{}, len(events))
	for _, ev := range events {
		k[ev.Key] = struct{}{}
	}
	fs.keys[runID] = k
	return k, nil
}

func (fs *FileStore) readLog(runID string) ([]Event, error) {
	f, err := os.Open(filepath.Join(fs.runDir(runID), "events.log"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: open event log: %w", err)
	}
	defer f.Close()
	var events []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var ev Event
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			return nil, fmt.Errorf("store: corrupt event log %s line %d: %w", runID, line, err)
		}
		events = append(events, ev)
	}
	return events, sc.Err()
}

// AppendEvent implements Store; duplicate keys are idempotent no-ops.
func (fs *FileStore) AppendEvent(ctx context.Context, runID string, ev Event) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ev.Key == "" {
		return false, errors.New("store: event requires a key")
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	keys, err := fs.eventKeys(runID)
	if err != nil {
		return false, err
	}
	if _, dup := keys[ev.Key]; dup {
		return false, nil
	}
	if ev.Seq <= 0 {
		ev.Seq = int64(len(keys)) + 1
	}
	line, err := json.Marshal(ev)
	if err != nil {
		return false, err
	}
	line = append(line, '\n')

	path := filepath.Join(fs.runDir(runID), "events.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return false, fmt.Errorf("store: append event: %w", err)
	}
	if _, err := f.Write(line); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	keys[ev.Key] = struct{}{}
	return true, nil
}

// ListEvents implements Store.
func (fs *FileStore) ListEvents(ctx context.Context, runID string) ([]Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.readLog(runID)
}
