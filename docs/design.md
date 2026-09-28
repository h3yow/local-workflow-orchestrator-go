# Orchestration engine — design and semantics

## 1. Declarative specification

```go
spec.Spec{
    Name        string         // human label, stored in snapshots
    MaxParallel int            // >= 1: per-run running-node cap
    MaxQueue    int            // >= 0: bounded ready-queue capacity (0 = fully backpressured)
    Nodes       []spec.NodeSpec{
        ID          string
        Deps        []string     // ids of required-successful predecessors
        Timeout     time.Duration // per-attempt cap, 0 = unbounded
        MaxAttempts int           // >= 1; retries = MaxAttempts - 1
        Backoff     backoff.Policy
    },
}
```

### 1.1 Validation and distinguishable rejection reasons

`Spec.Validate()` collects **every** problem and returns
`spec.ValidationErrors` (use `spec.AsValidationErrors(err)` and
`ValidationErrors.Has(code)`). Reject codes:

| Code | Meaning |
|---|---|
| `empty_graph` | no nodes declared |
| `duplicate_node` | same node id appears twice |
| `duplicate_dependency` | same edge listed twice on one node |
| `dangling_dependency` | dependency id does not exist |
| `self_dependency` | a node lists itself in Deps |
| `cycle` | dependency graph is not a DAG; error names every node still on a cycle |
| `invalid_concurrency` | `MaxParallel < 1` |
| `invalid_queue_size` | `MaxQueue < 0` |
| `invalid_retry` | `MaxAttempts < 1` |
| `invalid_timeout` | `Timeout < 0` |
| `invalid_backoff` | negative base/max delay, factor < 1, unknown kind |
| `invalid_node_id` | empty node id |

Topological cycle detection (Kahn's algorithm) runs after structural checks,
so a cycle is reported as a cycle rather than being masked by dangling edges.

### 1.2 Canonical order

`spec.Build` returns an indexed `Graph`. The node order is the
**lexicographically smallest topological ordering** (Kahn with a sorted
frontier). This is the single source of truth for admission and dispatch
ordering.

## 2. Node and run lifecycle

```
pending ──deps satisfied & admitted──▶ queued ──slot available──▶ running
   ▲                                     │                         │
   │                                  (not used)             success│
   │  retry backoff not due / sp.ready◀──┤                         ▼
   └──────────────────────────────┘      │ failure & retries left  succeeded
                                         ▼
                                       ready (retry parked)
                       terminal failure ▼ failed
            upstream failed/skipped ▶ skipped
```

- **failed** — the node's own attempts all failed, it timed out, it returned a
  permanent error, or it produced an invalid spawn batch.
- **skipped** — at least one dependency is `failed` or `skipped`. Skip
  propagation is a fixpoint: indirect downstream nodes are skipped too. The
  node is never executed (`Attempts` stays empty) and records
  `code=upstream_failed`, `SkippedBecause=<blocking node>`.
- **pending** nodes are provably distinguishable from both failed and skipped;
  after a run terminates, any non-terminal node would indicate an engine bug
  (the scheduler only terminates when all nodes are terminal).

**Terminal classification is mutually exclusive and clean.** The three
terminal states never borrow each other's markers:

| Final state | `FailCode` | `FailMessage` | `SkippedBecause` |
|---|---|---|---|
| `succeeded` | cleared (`""`) | cleared | cleared |
| `failed` | the failure reason | the detail | cleared |
| `skipped` | always `upstream_failed` | the detail | blocking node id |

Intermediate information stays fully observable without affecting terminal
classification: failed attempt details remain in `NodeStatus.Attempts`, and
`recovered_running` annotations, retry-park (`ready`) and running transitions
remain in the append-only history. `model.ClassifyTerminal(history)` re-derives
each node's classification as its **last terminal transition**, so a history
replay classifies nodes exactly as the live snapshot does. Reaching
`succeeded` after interruptions/retries therefore wipes failure/recovery
markers from the final state while preserving their evidence in history.
Snapshots written for every transition make this invariant hold for every
concurrent observer at every read.

**Compatibility scope.** `recovered_running` remains a valid code but is by
construction a *non-terminal* annotation: it appears only on intermediate
states (and history), never on a final `succeeded`/`failed`/`skipped` node, so
it cannot be confused with a terminal failure reason. Snapshots written by
older versions without `RetryNotBefore` (zero value) resume with immediate
retry eligibility — the pre-existing behaviour — and a missing `actions/` log
is treated as an empty action set. Code consuming only the documented terminal
states needs no changes.

Failure codes (`model.FailureCode`):
`attempts_exhausted`, `timeout`, `permanent`, `invalid_spawn`,
`upstream_failed`, `recovered_running`.

A run is `succeeded` iff every node is `succeeded`; presence of any
`failed`/`skipped` node makes the run `failed`.

## 3. Scheduling and concurrency

### 3.1 Single-writer loop

Each run is driven by exactly one scheduler goroutine:

1. **skip propagation** — mark nodes blocked by failed/skipped deps;
2. **admission** — pending (or retry-parked `ready`) nodes whose deps all
   succeeded enter the bounded queue, in canonical order;
3. **dispatch** — queued nodes start in canonical order while
   `running < Spec.MaxParallel` and the global semaphore (if configured) has a
   token; traversal stops at the first node that cannot start (strict FIFO
   head-of-line), so a later node can never repeatedly overtake and starve an
   earlier one;
4. **completion** — attempt goroutines deliver results on a buffered channel;
   the loop drains all available results each pass;
5. flush buffered transitions at a **stable barrier** (no unconsumed result and
   no in-flight attempt), then sleep until a retry deadline, a completion
   nudge, or shutdown.

Because admission/dispatch bookkeeping is single-threaded, a node can be
marked running at most once per attempt: duplicate execution and lost
ready-nodes are impossible by construction.

### 3.2 Determinism and reproducibility

- Canonical graph order governs admission and dispatch.
- Wall-clock timings of independent tasks vary, so the *history* would
  otherwise vary. Transitions produced within one scheduling wave are
  therefore buffered and, at a stable barrier, **normalised by node id** while
  preserving each node's own chronological transition order, then assigned
  strictly increasing `Seq` numbers. Two runs of the same spec (with the same
  task outcomes) produce the identical ordered event stream.
- Attempt numbering starts at 0 and is monotonic; it survives recovery.

### 3.3 Retries, backoff and timeout

- A task returns either a result or an error. `engine.Permanent(err)` fails
  the node immediately (`permanent`), bypassing remaining attempts.
- A per-attempt `context.WithTimeout` bounds execution. Timeout cancellation
  is respected cooperatively; when the deadline expires the attempt records
  `TimedOut=true` and the terminal code (after exhausting attempts) is
  `timeout`.
- Retry gaps come from `backoff.Policy.Delay(attempt)`:
  `none (0)`, `fixed Base`, `linear Base*(attempt+1)`,
  `exponential Base*Factor^attempt`, all optionally capped by `MaxDelay`.
  Policies have no randomness — delays are reproducible.

### 3.5 Retry waits survive restarts

When an attempt fails with retries remaining, the scheduler computes the
absolute deadline `now + Delay(attempt)` and persists it as
`NodeStatus.RetryNotBefore` together with the transition to `ready` (the
snapshot is fsynced before the loop blocks). The in-memory wait is anchored to
that same instant.

If the process is killed during the wait, `Resume` restores the deadline
instead of re-running the backoff:

- remaining wait = `RetryNotBefore - now_at_resume`; the retry fires only when
  that remainder elapses — never immediately (unless the deadline is already
  in the past, in which case the remaining wait is zero) and never after a
  fresh full `Delay`;
- any number of restarts converge on the **same original retry instant**: the
  stored deadline is reused verbatim and never re-anchored, so waits cannot be
  stretched by repeated kills or fired prematurely;
- elapsed backoff does not reset the attempt counter (attempt numbering is
  monotonic across recovery);
- once the next attempt starts (or the node becomes terminal) the deadline is
  cleared from the snapshot.

Policies stay deterministic (no jitter), so the wall-clock retry instant of a
run that never restarted and a run restarted mid-wait is the same.

### 3.4 Backpressure

Two bounds apply:

- **`Spec.MaxQueue`** limits admitted ready nodes. Surplus eligible nodes stay
  in the scheduler's waiting set, which is a subset of the (already
  instantiated) graph — its memory is bounded by graph size, never by an
  unbounded queue, and no eligible task is ever dropped; they are admitted as
  slots drain.
- **`Config.MaxNodesPerRun`** (default 100000) bounds dynamic graph growth: a
  spawn batch that would exceed it is rejected explicitly with
  `invalid_spawn`, instead of growing memory silently.
- **`Config.GlobalMaxParallel`** adds a process-wide semaphore shared by all
  runs (0 = unlimited). Token acquisition at dispatch is non-blocking: when
  other runs hold the pool, a run parks until a token frees; at no point do
  more than the cap nodes execute.

## 4. Persistence and recovery

### 4.1 Storage layout (`FileStore`)

```
<dir>/
  runs/<runID>.json      # full snapshot, written temp-file + fsync + rename
  events/<runID>.log     # append-only JSON Lines, one SnapshotEvent per line
  actions/<runID>.log    # append-only JSON Lines, one ActionRecord per line
```

- Snapshots are rewritten atomically (rename); readers never see torn state.
  All store reads return deep copies.
- `AppendEvent` is idempotent for a `(RunID, Seq)`: on restart the existing
  log is scanned for the max Seq, so a replayed append cannot duplicate a
  history record.
- `AppendAction` is idempotent for a `(RunID, Key)`: on restart the action log
  is scanned into a committed-key set, so a business action replayed after a
  crash (or racing a concurrent attempt) cannot take effect twice. See §8.

### 4.2 Write ordering

1. A transition mutates the in-memory snapshot and writes the snapshot
   immediately (terminal results survive an immediate kill).
2. Events are buffered and flushed at stable barriers (see 3.1) with strictly
   increasing Seq, then published to observers.

A crash between a snapshot write and its event flush means the event log may
temporarily lag the snapshot; recovery rebuilds state from the snapshot and
resumes, and later events continue above the last stored Seq. Snapshots never
announce success before the in-memory transition is authoritative.

### 4.3 Recovery rules

`Engine.Resume(runID)` loads the snapshot (which embeds the full, possibly
expanded `Spec`), rebuilds the `Graph`, and:

- an already terminal run is returned as-is;
- a node in `running`/`queued` is reset to `pending`, annotated with
  `recovered_running` and an explanatory history event
  (`"interrupted while <state>; attempt replayed"`), and its recorded attempt
  index is preserved;
- a node parked in `ready` (retry backoff wait) **keeps its state**: the
  persisted absolute deadline `NodeStatus.RetryNotBefore` is restored into the
  scheduler, so the wait resumes for the *remaining* interval (see §3.5);
- `succeeded` nodes are never re-executed;
- on resume the max stored event Seq seeds the scheduler counter so new events
  never reuse or duplicate a Seq.

**Side effects**: the engine guarantees *at-most-once dispatch per committed
state*; a task killed mid-execution by definition cannot be made effect-free by
an orchestrator alone. §8 provides the durable business-action facility that
makes externally visible effects take effect at most once across crashes and
retries, and tasks may also use `TaskInput{RunID, NodeID, Attempt}` as an
idempotency key; the same `(run, node, attempt)` is replayed at most once after
a crash.

## 5. Business actions: exactly-once-effect records

Tasks that produce external side effects (place an order, send a message,
write a result file) register them through the recorder handed to every task:

```go
task := func(ctx context.Context, in engine.TaskInput) (*engine.ExecResult, error) {
    inserted, err := in.Actions.Record(ctx, "order-1234", payload) // stable business key
    if err != nil { return nil, err }
    if inserted {
        placeOrder(...) // this attempt is the unique owner of the effect
    }
    return &engine.ExecResult{}, nil
}
```

### 5.1 Identity and scope

- The **key is a stable business identifier** chosen by the task. Within one
  run a key becomes effective **at most once**: the committed record set is
  `set<Key>` per run, never a multiset.
- Scope is the run: the same key in two different runs is fully independent
  (neither blocks nor dedupes the other). Within a run keys are shared across
  all nodes/attempts, so two nodes claiming `"order-1"` also collapse to one.
- `Record` returns `inserted=true` for exactly one caller — the first
  committer — and `false` for every duplicate (a retried attempt, an attempt
  replayed after crash, or a racing goroutine). The first committer's record
  (node id, attempt, payload, timestamp) wins and is never overwritten.

### 5.2 Crash window semantics

Records are durable before `Record` returns (appended + fsynced to
`actions/<runID>.log`). The safe usage is **record-before-effect**:

1. `Record` commits the key;
2. the task performs the external effect.

If the process is killed between 1 and 2 (or mid-attempt), `Resume` replays
the attempt, `Record` returns `false`, and the replayed attempt must not
perform the effect again: the final durable action set contains exactly one
record for the key, before and after every number of restarts. Killing the
process cannot lose a committed record or duplicate one. (If the external
system itself needs "exactly once" rather than the engine's at-most-once
guarantee, make the external call idempotent under the same key.)

### 5.3 Querying and replay

- `in.Actions.Has(key)` / `in.Actions.Load()` inside a task, and
  `engine.ListActions(runID)` / `RunRecord.Actions` for observers, return the
  committed set. `Load`/`ListActions` order records by key, so the set has one
  canonical representation.
- Because records are append-only JSONL loaded into a committed-key set (and
  all reads are copy-on-read), pre/post-restart readers and concurrent readers
  always observe the same committed set — never a partial or duplicated view.
  Records are independent of node-state snapshots and survive regardless of
  how the owning node later transitions.

## 6. Dynamic expansion

A successful task result may contain `SpawnNode`s (`NodeSpec`). Application is
atomic per batch:

1. ids already in the graph or repeated inside the batch are deduped (first
   occurrence wins) and reported in `SpawnOutcome.Skipped`;
2. exceeding `MaxNodesPerRun` rejects the batch;
3. the candidate graph (existing nodes + new nodes, deterministically ordered)
   is rebuilt through `spec.Build`, enforcing all rules including new cycles
   and dangling deps;
4. only when validation succeeds are nodes inserted, each marked `pending`,
   the embedded Spec replaced, a snapshot persisted, and one
   `"dynamically spawned"` event published per new node.

A rejected batch fails the **producing** node with `invalid_spawn` and a
message carrying the underlying validation reason; the graph is untouched, so
its downstream rules apply normally.

## 7. Multi-run consistency and observers

- Runs are independent state machines over the same store; their status maps,
  event logs, action logs, revision counters and node ids never overlap. A
  failing run does not mutate another run's snapshot, history or action set
  (covered by `TestMultiRunIsolation`, `TestActionKeysScopedToRun`).
- `GetRun` returns a deep-cloned `RunRecord` (snapshot + replayed history +
  key-ordered action set); concurrent readers never observe a half-updated map
  (the code passes `-race`).
- `Subscribe` replays durable history then streams live transitions on a
  buffered channel; sequence numbers are strictly increasing per run.
- Store writes are mutually excluded (per-run event/action log locks; full-map
  locks for the memory store) and reads are copy-on-read.

## 8. Verification

```bash
go test ./... -race -count=3
```

Test matrix (each test logs the input spec and transition rationale):

| Area | Test |
|---|---|
| cycle / dangling / duplicate / illegal values | `pkg/spec/spec_test.go` |
| backoff arithmetic & validation | `pkg/backoff/backoff_test.go` |
| snapshot/event durability, idempotent replay, restart | `pkg/store/store_test.go` |
| action-log at-most-once, first-writer-wins, restart replay, per-run scoping | `TestActionLogAtMostOnce`, `TestActionLogSurvivesRestart`, `TestActionKeysScopedPerRun` |
| dependency failure → correct failed vs skipped classification | `TestDependencyFailureSkipsDownstream` |
| retry exhaustion, permanent shortcut | `TestRetryExhaustionAndPermanent` |
| successful retry after backoff | `TestRetrySucceedsAfterBackoff` |
| timeout → `timeout` classification | `TestTimeoutFailsNode` |
| per-run and global concurrency caps | `TestConcurrencyCap`, `TestGlobalCapAcrossRuns` |
| no starvation / no duplicate, reproducible history | `TestDeterministicRepeatedRuns` |
| bounded queue, no dropped work | `TestBackpressureBoundedQueue` |
| interrupted while running: no silent success, no re-run of success, no duplicate records | `TestRecoveryRunningNotMarkedSucceeded` |
| interrupted while retrying: attempt count preserved | `TestRecoveryMidRetry` |
| business action uniqueness across retries / crash window / multiple restarts / races, run scoping | `TestActionUniqueWithinRunAcrossRetry`, `TestActionUniqueAcrossCrashWindow`, `TestActionUniqueAcrossMultipleRestarts`, `TestActionConcurrentRecordOnce`, `TestActionKeysScopedToRun` |
| retry wait resumes remaining delay after restart, stable across multiple restarts, zero-remainder immediate retry | `TestBackoffResumesRemainingWaitAfterRestart`, `TestBackoffSurvivesMultipleRestarts`, `TestBackoffPastDeadlineRetriesImmediately` |
| clean terminal state after interrupt+retry, mutual exclusivity, self-consistency under concurrent reads, replay classification | `TestSuccessCleanAfterInterruptionAndRetries`, `TestTerminalClassesMutuallyExclusive`, `TestTerminalClassificationStableUnderConcurrentReads`, `TestClassifyTerminalIgnoresIntermediateTransitions` |
| dynamic spawn, dedup-once, cycle/dangling/cap rejection | `pkg/engine/engine_spawn_test.go` |
| cross-run isolation, concurrent reader consistency | `TestMultiRunIsolation`, `TestObserverConsistentView` |
