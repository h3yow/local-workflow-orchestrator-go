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
```

- Snapshots are rewritten atomically (rename); readers never see torn state.
  All store reads return deep copies.
- `AppendEvent` is idempotent for a `(RunID, Seq)`: on restart the existing
  log is scanned for the max Seq, so a replayed append cannot duplicate a
  history record.

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
- a node in `running`/`queued`/`ready` is reset to `pending`, annotated with
  `recovered_running` and an explanatory history event
  (`"interrupted while <state>; attempt replayed"`), and its recorded attempt
  index is preserved;
- `succeeded` nodes are never re-executed;
- on resume the max stored event Seq seeds the scheduler counter so new events
  never reuse or duplicate a Seq.

**Side effects**: the engine guarantees *at-most-once dispatch per attempt
per committed state*, but a task killed mid-execution by definition cannot be
made effect-free by an orchestrator. Tasks with external side effects should
therefore be idempotent and may use `TaskInput{RunID, NodeID, Attempt}` as an
idempotency key; the same `(run, node, attempt)` is replayed at most once
after a crash.

## 5. Dynamic expansion

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

## 6. Multi-run consistency and observers

- Runs are independent state machines over the same store; their status maps,
  event logs, revision counters and node ids never overlap. A failing run does
  not mutate another run's snapshot or history (covered by
  `TestMultiRunIsolation`).
- `GetRun` returns a deep-cloned `RunRecord` (snapshot + replayed history);
  concurrent readers never observe a half-updated map (the code passes
  `-race`).
- `Subscribe` replays durable history then streams live transitions on a
  buffered channel; sequence numbers are strictly increasing per run.
- Store writes are mutually excluded (per-run event log locks; full-map locks
  for the memory store) and reads are copy-on-read.

## 7. Verification

```bash
go test ./... -race -count=3
```

Test matrix (each test logs the input spec and transition rationale):

| Area | Test |
|---|---|
| cycle / dangling / duplicate / illegal values | `pkg/spec/spec_test.go` |
| backoff arithmetic & validation | `pkg/backoff/backoff_test.go` |
| snapshot/event durability, idempotent replay, restart | `pkg/store/store_test.go` |
| dependency failure → correct failed vs skipped classification | `TestDependencyFailureSkipsDownstream` |
| retry exhaustion, permanent shortcut | `TestRetryExhaustionAndPermanent` |
| successful retry after backoff | `TestRetrySucceedsAfterBackoff` |
| timeout → `timeout` classification | `TestTimeoutFailsNode` |
| per-run and global concurrency caps | `TestConcurrencyCap`, `TestGlobalCapAcrossRuns` |
| no starvation / no duplicate, reproducible history | `TestDeterministicRepeatedRuns` |
| bounded queue, no dropped work | `TestBackpressureBoundedQueue` |
| interrupted while running: no silent success, no re-run of success, no duplicate records | `TestRecoveryRunningNotMarkedSucceeded` |
| interrupted while retrying: attempt count preserved | `TestRecoveryMidRetry` |
| dynamic spawn, dedup-once, cycle/dangling/cap rejection | `pkg/engine/engine_spawn_test.go` |
| cross-run isolation, concurrent reader consistency | `TestMultiRunIsolation`, `TestObserverConsistentView` |
