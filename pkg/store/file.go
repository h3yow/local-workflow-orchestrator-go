package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/h3yow/local-workflow-orchestrator-go/pkg/model"
)

// FileStore persists each run as a JSON snapshot plus an append-only JSON-line
// event log under a directory. Writes are made durable via temp-file + rename
// (and fsync) so readers never observe a half-written file.
type FileStore struct {
	dir string

	mu        sync.Mutex
	logMu     map[string]*sync.Mutex
	lastSeq   map[string]int64
	loaded    map[string]bool
	actionMu  map[string]*sync.Mutex
	actionSet map[string]map[string]bool
}

// NewFileStore opens (creating if needed) a file-backed store in dir.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(dir, "runs"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "actions"), 0o755); err != nil {
		return nil, err
	}
	return &FileStore{
		dir:       dir,
		logMu:     make(map[string]*sync.Mutex),
		lastSeq:   make(map[string]int64),
		loaded:    make(map[string]bool),
		actionMu:  make(map[string]*sync.Mutex),
		actionSet: make(map[string]map[string]bool),
	}, nil
}

func safeName(id string) error {
	if id == "" || id == "." || id == ".." {
		return fmt.Errorf("unsafe store id %q", id)
	}
	if strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("unsafe store id %q", id)
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return fmt.Errorf("unsafe store id %q", id)
		}
	}
	return nil
}

func (f *FileStore) statusPath(runID string) string {
	return filepath.Join(f.dir, "runs", runID+".json")
}
func (f *FileStore) eventPath(runID string) string {
	return filepath.Join(f.dir, "events", runID+".log")
}
func (f *FileStore) actionPath(runID string) string {
	return filepath.Join(f.dir, "actions", runID+".log")
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (f *FileStore) SaveStatus(ctx context.Context, status *model.RunStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safeName(status.RunID); err != nil {
		return err
	}
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	buf.Write(data)
	buf.WriteByte('\n')
	return writeFileAtomic(f.statusPath(status.RunID), buf.Bytes())
}

func (f *FileStore) LoadStatus(ctx context.Context, runID string) (*model.RunStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := safeName(runID); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(f.statusPath(runID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, model.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var rs model.RunStatus
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, fmt.Errorf("corrupt snapshot for run %q: %w", runID, err)
	}
	return &rs, nil
}

func (f *FileStore) ListRuns(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(f.dir, "runs"))
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, en := range entries {
		if en.IsDir() || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(en.Name(), ".json"))
	}
	sort.Strings(ids)
	return ids, nil
}

// logLock returns the mutex serialising writes for one run's event log.
func (f *FileStore) logLock(runID string) *sync.Mutex {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.logMu[runID]
	if m == nil {
		m = &sync.Mutex{}
		f.logMu[runID] = m
	}
	return m
}

// ensureLoaded scans an existing log once to learn the highest stored Seq so
// duplicate appends after a restart are suppressed.
func (f *FileStore) ensureLoadedLocked(runID string) error {
	if f.loaded[runID] {
		return nil
	}
	path := f.eventPath(runID)
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		f.loaded[runID] = true
		f.lastSeq[runID] = -1
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	maxSeq := int64(-1)
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev model.SnapshotEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("corrupt event log for run %q: %w", runID, err)
		}
		if ev.Seq > maxSeq {
			maxSeq = ev.Seq
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	f.loaded[runID] = true
	f.lastSeq[runID] = maxSeq
	return nil
}

func (f *FileStore) AppendEvent(ctx context.Context, ev *model.SnapshotEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safeName(ev.RunID); err != nil {
		return err
	}
	lock := f.logLock(ev.RunID)
	lock.Lock()
	defer lock.Unlock()

	f.mu.Lock()
	if err := f.ensureLoadedLocked(ev.RunID); err != nil {
		f.mu.Unlock()
		return err
	}
	if ev.Seq <= f.lastSeq[ev.RunID] {
		// Replayed or out-of-order append: history stays append-only and the
		// record is not duplicated.
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()

	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	fp, err := os.OpenFile(f.eventPath(ev.RunID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := fp.Write(data); err != nil {
		_ = fp.Close()
		return err
	}
	if err := fp.Sync(); err != nil {
		_ = fp.Close()
		return err
	}
	if err := fp.Close(); err != nil {
		return err
	}
	f.mu.Lock()
	f.lastSeq[ev.RunID] = ev.Seq
	f.mu.Unlock()
	return nil
}

func (f *FileStore) LoadHistory(ctx context.Context, runID string) (*model.History, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := safeName(runID); err != nil {
		return nil, err
	}
	h := &model.History{RunID: runID}
	file, err := os.Open(f.eventPath(runID))
	if errors.Is(err, fs.ErrNotExist) {
		if _, statErr := os.Stat(f.statusPath(runID)); statErr != nil {
			return nil, model.ErrNotFound
		}
		return h, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev model.SnapshotEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("corrupt event log for run %q: %w", runID, err)
		}
		h.Events = append(h.Events, ev)
	}
	return h, sc.Err()
}

// actionLock returns the mutex serialising action appends for one run.
func (f *FileStore) actionLock(runID string) *sync.Mutex {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.actionMu[runID]
	if m == nil {
		m = &sync.Mutex{}
		f.actionMu[runID] = m
	}
	return m
}

// ensureActionsLoadedLocked scans an existing action log once to build the
// committed-key set, so duplicate appends after a restart (or racing
// appends in this process) are suppressed.
func (f *FileStore) ensureActionsLoadedLocked(runID string) error {
	if f.actionSet[runID] != nil {
		return nil
	}
	keys := map[string]bool{}
	path := f.actionPath(runID)
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		f.actionSet[runID] = keys
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec model.ActionRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("corrupt action log for run %q: %w", runID, err)
		}
		keys[rec.Key] = true
	}
	if err := sc.Err(); err != nil {
		return err
	}
	f.actionSet[runID] = keys
	return nil
}

func (f *FileStore) AppendAction(ctx context.Context, rec model.ActionRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := safeName(rec.RunID); err != nil {
		return false, err
	}
	lock := f.actionLock(rec.RunID)
	lock.Lock()
	defer lock.Unlock()

	f.mu.Lock()
	if err := f.ensureActionsLoadedLocked(rec.RunID); err != nil {
		f.mu.Unlock()
		return false, err
	}
	if f.actionSet[rec.RunID][rec.Key] {
		// First committer wins: replaying an attempt after a crash can never
		// make a business action effective twice.
		f.mu.Unlock()
		return false, nil
	}
	f.mu.Unlock()

	data, err := json.Marshal(rec)
	if err != nil {
		return false, err
	}
	data = append(data, '\n')
	fp, err := os.OpenFile(f.actionPath(rec.RunID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	if _, err := fp.Write(data); err != nil {
		_ = fp.Close()
		return false, err
	}
	if err := fp.Sync(); err != nil {
		_ = fp.Close()
		return false, err
	}
	if err := fp.Close(); err != nil {
		return false, err
	}
	f.mu.Lock()
	if f.actionSet[rec.RunID] == nil {
		f.actionSet[rec.RunID] = map[string]bool{}
	}
	f.actionSet[rec.RunID][rec.Key] = true
	f.mu.Unlock()
	return true, nil
}

func (f *FileStore) LoadActions(ctx context.Context, runID string) ([]model.ActionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := safeName(runID); err != nil {
		return nil, err
	}
	out := make([]model.ActionRecord, 0)
	file, err := os.Open(f.actionPath(runID))
	if errors.Is(err, fs.ErrNotExist) {
		if _, statErr := os.Stat(f.statusPath(runID)); statErr != nil {
			return nil, model.ErrNotFound
		}
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec model.ActionRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("corrupt action log for run %q: %w", runID, err)
		}
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// Canonical ordering by key: every reader reproduces the same action set
	// regardless of append timing across attempts.
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
