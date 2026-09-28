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

	mu      sync.Mutex
	logMu   map[string]*sync.Mutex
	lastSeq map[string]int64
	loaded  map[string]bool

	// actionMu serialises per-run action-log appends; action state
	// (lastActionSeq/keys/actionLoaded) is guarded by mu.
	actionMu      map[string]*sync.Mutex
	lastActionSeq map[string]int64
	actionKeys    map[string]map[string]struct{}
	actionLoaded  map[string]bool
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
		dir:           dir,
		logMu:         make(map[string]*sync.Mutex),
		lastSeq:       make(map[string]int64),
		loaded:        make(map[string]bool),
		actionMu:      make(map[string]*sync.Mutex),
		lastActionSeq: make(map[string]int64),
		actionKeys:    make(map[string]map[string]struct{}),
		actionLoaded:  make(map[string]bool),
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

// actionLock returns the mutex serialising appends to one run's action log.
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

// ensureActionsLoadedLocked scans an existing action log once after (re)start
// to learn the committed key set and the highest Seq, so a replay after a
// crash can neither duplicate a record nor reuse a Seq.
func (f *FileStore) ensureActionsLoadedLocked(runID string) error {
	if f.actionLoaded[runID] {
		return nil
	}
	path := f.actionPath(runID)
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		f.actionLoaded[runID] = true
		f.lastActionSeq[runID] = 0
		f.actionKeys[runID] = map[string]struct{}{}
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	keys := map[string]struct{}{}
	maxSeq := int64(0)
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
		keys[actionDedupKey(rec.NodeID, rec.Key)] = struct{}{}
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	f.actionLoaded[runID] = true
	f.lastActionSeq[runID] = maxSeq
	f.actionKeys[runID] = keys
	return nil
}

// actionDedupKey namespaces the business key by node within one run.
func actionDedupKey(nodeID, key string) string {
	return nodeID + "\x00" + key
}

func (f *FileStore) CommitAction(ctx context.Context, rec *model.ActionRecord) (*model.ActionRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := safeName(rec.RunID); err != nil {
		return nil, false, err
	}
	lock := f.actionLock(rec.RunID)
	lock.Lock()
	defer lock.Unlock()

	f.mu.Lock()
	if err := f.ensureActionsLoadedLocked(rec.RunID); err != nil {
		f.mu.Unlock()
		return nil, false, err
	}
	dk := actionDedupKey(rec.NodeID, rec.Key)
	if _, dup := f.actionKeys[rec.RunID][dk]; dup {
		// Duplicate (retry/replay/recovered attempt): load and return the
		// original winning record; nothing is appended.
		f.mu.Unlock()
		log, err := f.readActionLogLocked(rec.RunID)
		if err != nil {
			return nil, false, err
		}
		for i := range log.Actions {
			if actionDedupKey(log.Actions[i].NodeID, log.Actions[i].Key) == dk {
				stored := log.Actions[i]
				return &stored, false, nil
			}
		}
		return nil, false, fmt.Errorf("action key %q tracked but record missing", dk)
	}
	stored := *rec
	f.lastActionSeq[rec.RunID]++
	stored.Seq = f.lastActionSeq[rec.RunID]
	data, err := json.Marshal(&stored)
	if err != nil {
		f.mu.Unlock()
		return nil, false, err
	}
	f.mu.Unlock()

	data = append(data, '\n')
	fp, err := os.OpenFile(f.actionPath(rec.RunID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, false, err
	}
	if _, err := fp.Write(data); err != nil {
		_ = fp.Close()
		return nil, false, err
	}
	if err := fp.Sync(); err != nil {
		_ = fp.Close()
		return nil, false, err
	}
	if err := fp.Close(); err != nil {
		return nil, false, err
	}

	// Record the claim only after the durable append succeeds, so a crashed
	// write cannot poison the in-memory key set for the rest of the process.
	f.mu.Lock()
	f.actionKeys[rec.RunID][dk] = struct{}{}
	f.mu.Unlock()
	return &stored, true, nil
}

// readActionLogLocked parses the whole action log; ordering is normalised on
// read so every replay (and every concurrent reader) sees the same set and
// order regardless of how records arrived on disk.
func (f *FileStore) readActionLogLocked(runID string) (*model.ActionLog, error) {
	file, err := os.Open(f.actionPath(runID))
	if errors.Is(err, fs.ErrNotExist) {
		return &model.ActionLog{RunID: runID}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	log := &model.ActionLog{RunID: runID}
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
		log.Actions = append(log.Actions, rec)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	sort.Slice(log.Actions, func(i, j int) bool { return log.Actions[i].Seq < log.Actions[j].Seq })
	return log, nil
}

func (f *FileStore) LoadActions(ctx context.Context, runID string) (*model.ActionLog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := safeName(runID); err != nil {
		return nil, err
	}
	// A run may legitimately have an empty action log; a missing status file
	// means the run id itself is unknown.
	if _, err := os.Stat(f.statusPath(runID)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	lock := f.actionLock(runID)
	lock.Lock()
	defer lock.Unlock()
	return f.readActionLogLocked(runID)
}
