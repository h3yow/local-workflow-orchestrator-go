package engine

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/local-workflow-orchestrator-go/backoff"
	"github.com/h3yow/local-workflow-orchestrator-go/spec"
	"github.com/h3yow/local-workflow-orchestrator-go/state"
	"github.com/h3yow/local-workflow-orchestrator-go/store"
)

// logBuf is a thread-safe Logger capturing scheduling rationale lines.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.b, format+"\n", args...)
}
func (l *logBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func dumpSpec(t *testing.T, s *spec.Spec) {
	t.Helper()
	var b strings.Builder
	b.WriteString(fmt.Sprintf("INPUT SPEC id=%q nodes=%d\n", s.ID, len(s.Nodes)))
	for i := range s.Nodes {
		n := &s.Nodes[i]
		fmt.Fprintf(&b, "  [%d] id=%s deps=%v slots=%d timeout=%s retries=%d\n",
			i, n.ID, n.Deps, n.Slots, n.Timeout, n.MaxRetries)
	}
	t.Log(b.String())
}

func engineWithLog(t *testing.T, maxRun, maxQueue int, bo backoff.Policy) (*Engine, *logBuf) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.NewFileStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	log := &logBuf{}
	e, err := New(Config{MaxRunning: maxRun, MaxQueue: maxQueue, Store: st,
		Backoff: bo, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, log
}

func node(id string, deps ...string) spec.NodeSpec {
	return spec.NodeSpec{ID: id, Deps: deps, Slots: 1}
}

// TestFailureAndSkipClassification: a failing node is Failed after retries;
// downstream nodes become Skipped (not Failed, not Succeeded).
func TestFailureAndSkipClassification(t *testing.T) {
	e, log := engineWithLog(t, 4, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "failgraph", Nodes: []spec.NodeSpec{
		node("a"),
		node("b", "a"),
		node("c", "a"),
		node("d", "b", "c"),
	}}
	dumpSpec(t, s)
	var execs int32
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		atomic.AddInt32(&execs, 1)
		if id == "b" {
			return "", nil, fmt.Errorf("boom at %s", id)
		}
		return "ok", nil, nil
	}
	id, err := e.Create(context.Background(), s, exec, Option{RunID: "run-fail"})
	if err != nil {
		t.Fatal(err)
	}
	rs, err := e.Wait(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FINAL STATUSES: a=%s b=%s c=%s d=%s run=%s\n%s",
		rs.Nodes["a"].Status, rs.Nodes["b"].Status, rs.Nodes["c"].Status,
		rs.Nodes["d"].Status, rs.Status, log.String())

	if rs.Status != state.Failed {
		t.Fatalf("run status=%s want failed", rs.Status)
	}
	if rs.Nodes["a"].Status != state.Succeeded {
		t.Fatal("a must succeed")
	}
	if rs.Nodes["b"].Status != state.Failed {
		t.Fatalf("b=%s want failed", rs.Nodes["b"].Status)
	}
	if rs.Nodes["b"].FailureReason == "" || !strings.Contains(rs.Nodes["b"].FailureReason, "boom") {
		t.Fatalf("b failure reason not distinguishable: %q", rs.Nodes["b"].FailureReason)
	}
	// c succeeded independently; d is skipped because b failed.
	if rs.Nodes["c"].Status != state.Succeeded {
		t.Fatalf("c=%s want succeeded (independent branch)", rs.Nodes["c"].Status)
	}
	if rs.Nodes["d"].Status != state.Skipped {
		t.Fatalf("d=%s want skipped", rs.Nodes["d"].Status)
	}
	if !strings.Contains(rs.Nodes["d"].FailureReason, "upstream") {
		t.Fatalf("d skip reason=%q", rs.Nodes["d"].FailureReason)
	}
}

// TestRetryExhausted verifies attempts == MaxRetries+1 and backoff applied.
func TestRetryExhausted(t *testing.T) {
	e, log := engineWithLog(t, 2, 8, backoff.Policy{Kind: backoff.Fixed, Base: 2 * time.Millisecond})
	s := &spec.Spec{ID: "retry", Nodes: []spec.NodeSpec{
		{ID: "a", Slots: 1, MaxRetries: 2},
	}}
	dumpSpec(t, s)
	var n int32
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		atomic.AddInt32(&n, 1)
		return "", nil, fmt.Errorf("always fails")
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-retry"})
	rs, _ := e.Wait(context.Background(), id)
	t.Logf("attempts=%d reason=%q\n%s", rs.Nodes["a"].AttemptCount,
		rs.Nodes["a"].FailureReason, log.String())
	if rs.Nodes["a"].Status != state.Failed {
		t.Fatalf("status=%s", rs.Nodes["a"].Status)
	}
	if rs.Nodes["a"].AttemptCount != 3 {
		t.Fatalf("attempts=%d want 3 (1 + 2 retries)", rs.Nodes["a"].AttemptCount)
	}
	if n != 3 {
		t.Fatalf("exec calls=%d want 3", n)
	}
}

// TestRetryThenSucceed verifies a transient failure is retried and succeeds.
func TestRetryThenSucceed(t *testing.T) {
	e, _ := engineWithLog(t, 2, 8, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "retryok", Nodes: []spec.NodeSpec{{ID: "a", Slots: 1, MaxRetries: 3}}}
	dumpSpec(t, s)
	var n int32
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		if atomic.AddInt32(&n, 1) < 3 {
			return "", nil, fmt.Errorf("transient")
		}
		return "done", []string{"fx-a"}, nil
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-retryok"})
	rs, _ := e.Wait(context.Background(), id)
	if rs.Nodes["a"].Status != state.Succeeded {
		t.Fatalf("status=%s", rs.Nodes["a"].Status)
	}
	if rs.Nodes["a"].AttemptCount != 3 {
		t.Fatalf("attempts=%d want 3", rs.Nodes["a"].AttemptCount)
	}
	fx, _ := e.Effects(context.Background(), id)
	if len(fx) != 1 || fx[0] != "fx-a" {
		t.Fatalf("effects=%v", fx)
	}
}

// TestTimeoutFailsNode: attempt exceeding per-node timeout becomes a timeout
// outcome and, with no retries left, a Failed node.
func TestTimeoutFailsNode(t *testing.T) {
	e, log := engineWithLog(t, 2, 8, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "to", Nodes: []spec.NodeSpec{
		{ID: "slow", Slots: 1, Timeout: 20 * time.Millisecond, MaxRetries: 0},
	}}
	dumpSpec(t, s)
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		<-ctx.Done()
		return "", nil, ctx.Err()
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-to"})
	rs, _ := e.Wait(context.Background(), id)
	t.Logf("status=%s reason=%q\n%s", rs.Nodes["slow"].Status,
		rs.Nodes["slow"].FailureReason, log.String())
	if rs.Nodes["slow"].Status != state.Failed {
		t.Fatalf("status=%s", rs.Nodes["slow"].Status)
	}
	if rs.Nodes["slow"].Attempts[0].Outcome != state.OutcomeTimeout {
		t.Fatalf("outcome=%s want timeout", rs.Nodes["slow"].Attempts[0].Outcome)
	}
}

// TestConcurrencyCap ensures running slots never exceed MaxRunning.
func TestConcurrencyCap(t *testing.T) {
	e, log := engineWithLog(t, 2, 16, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	nodes := []spec.NodeSpec{
		{ID: "a", Slots: 1},
		{ID: "b", Slots: 1},
		{ID: "c", Slots: 1},
		{ID: "d", Slots: 1},
	}
	s := &spec.Spec{ID: "cap", Nodes: nodes}
	dumpSpec(t, s)
	var inFlight, maxInFlight int32
	var mu sync.Mutex
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		cur := atomic.AddInt32(&inFlight, 1)
		mu.Lock()
		if cur > maxInFlight {
			maxInFlight = cur
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		return id, nil, nil
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-cap"})
	rs, _ := e.Wait(context.Background(), id)
	t.Logf("max concurrent=%d\n%s", atomic.LoadInt32(&maxInFlight), log.String())
	if rs.Status != state.Succeeded {
		t.Fatalf("status=%s", rs.Status)
	}
	if got := atomic.LoadInt32(&maxInFlight); got != 2 {
		t.Fatalf("max in flight=%d want 2", got)
	}
}

// TestBackpressure: with MaxRunning=1 and MaxQueue=1, at most one eligible
// node is admitted at a time; nothing is dropped and all nodes eventually run.
func TestBackpressure(t *testing.T) {
	e, log := engineWithLog(t, 1, 1, backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond})
	s := &spec.Spec{ID: "bp", Nodes: []spec.NodeSpec{
		node("a"), node("b"), node("c"),
	}}
	dumpSpec(t, s)
	var ran int32
	exec := func(ctx context.Context, runID, id string) (string, []string, error) {
		atomic.AddInt32(&ran, 1)
		time.Sleep(15 * time.Millisecond)
		return id, nil, nil
	}
	id, _ := e.Create(context.Background(), s, exec, Option{RunID: "run-bp"})
	rs, _ := e.Wait(context.Background(), id)
	t.Logf("ran=%d status=%s\n%s", atomic.LoadInt32(&ran), rs.Status, log.String())
	if rs.Status != state.Succeeded {
		t.Fatalf("status=%s", rs.Status)
	}
	if ran != 3 {
		t.Fatalf("ran=%d want 3 (no task dropped)", ran)
	}
	if !strings.Contains(log.String(), "backpressure") {
		t.Fatal("expected backpressure rationale in log")
	}
}

// TestRecoveryMidExecution simulates a process crash while a node attempt is
// in flight: the snapshot captured in Running state is resumed by a NEW
// engine. The interrupted node must NOT be marked success, must be retried,
// and successful nodes must not re-run; effect records stay unique.
func TestRecoveryMidExecution(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	st0, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := &logBuf{}

	entered := make(chan struct{})
	release := make(chan struct{})

	// Phase 1: run a->b->c; block 'b' the first time once it has entered
	// Running (snapshot persisted), then kill the engine mid-execution.
	attemptB := int32(0)
	var ranA1 int32
	var ranC1 int32

	exec1 := func(ctx context.Context, runID, id string) (string, []string, error) {
		switch id {
		case "a":
			atomic.AddInt32(&ranA1, 1)
			return "a-ok", []string{"effect-a"}, nil
		case "b":
			if atomic.AddInt32(&attemptB, 1) == 1 {
				close(entered)
				<-release
				return "", nil, ctx.Err() // killed while running
			}
			return "b-ok", []string{"effect-b"}, nil
		case "c":
			atomic.AddInt32(&ranC1, 1)
			return "c-ok", []string{"effect-c"}, nil
		}
		return "", nil, nil
	}

	e1, err := New(Config{MaxRunning: 1, MaxQueue: 8, Store: st0,
		Backoff: backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond}, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	s := &spec.Spec{ID: "rec", Nodes: []spec.NodeSpec{
		node("a"), node("b", "a"), node("c", "b"),
	}}
	dumpSpec(t, s)
	runID := "run-rec"
	if _, err := e1.Create(context.Background(), s, exec1, Option{RunID: runID}); err != nil {
		t.Fatal(err)
	}
	<-entered
	// Synchronize on a DURABLE scheduling point before killing the process:
	// wait until the snapshot with a=succeeded, b=running is persisted. This
	// pins the crash to "b in execution with a durably committed".
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap, err := st0.LoadSnapshot(context.Background(), runID)
		if err == nil && snap.Nodes["a"].Status == state.Succeeded &&
			snap.Nodes["b"].Status == state.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never reached durable crash point: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	// Crash simulation: cancel the run contexts (process death) and release
	// the blocked worker after cancellation.
	go func() {
		e1.cancelAll()
		close(release)
	}()
	e1.wg.Wait()

	// Phase 2: brand new engine, same store, Resume.
	st1, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	var ranA2, ranC2 int32
	var bAttemptsAfter int32
	exec2 := func(ctx context.Context, rid, id string) (string, []string, error) {
		switch id {
		case "a":
			ranA2++
			t.Error("already-succeeded node a must not re-run")
			return "a-ok", []string{"effect-a"}, nil
		case "b":
			bAttemptsAfter++
			return "b-ok", []string{"effect-b"}, nil
		case "c":
			ranC2++
			return "c-ok", []string{"effect-c"}, nil
		}
		return "", nil, nil
	}
	e2, err := New(Config{MaxRunning: 1, MaxQueue: 8, Store: st1,
		Backoff: backoff.Policy{Kind: backoff.Fixed, Base: time.Millisecond}, Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.Resume(context.Background(), runID, exec2, nil); err != nil {
		t.Fatal(err)
	}
	rs, err := e2.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("AFTER RESUME: statuses a=%s b=%s c=%s run=%s\n%s",
		rs.Nodes["a"].Status, rs.Nodes["b"].Status, rs.Nodes["c"].Status,
		rs.Status, log.String())

	if rs.Status != state.Succeeded {
		t.Fatalf("run=%s want succeeded", rs.Status)
	}
	if ranA2 != 0 {
		t.Fatal("a re-executed after recovery")
	}
	if bAttemptsAfter != 1 {
		t.Fatalf("b re-attempts after resume=%d want 1", bAttemptsAfter)
	}
	if ranC2 != 1 {
		t.Fatalf("c executions after resume=%d want 1", ranC2)
	}
	if rs.Nodes["b"].Attempts[0].Outcome != state.OutcomeInterrupted {
		t.Fatalf("first b attempt outcome=%s want interrupted", rs.Nodes["b"].Attempts[0].Outcome)
	}
	fx, _ := e2.Effects(context.Background(), runID)
	if len(fx) != 3 {
		t.Fatalf("effects=%v want exactly 3 unique (no duplicates)", fx)
	}
}
