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

### 2.1 Terminal classification is clean and mutually exclusive

The three terminal states are strict alternatives; exactly one holds for any
terminal node, and a node's terminal fields are written atomically with the
state transition:

- entering **succeeded** clears `FailCode` (to `""`), `FailMessage`,
  `SkippedBecause` and `RetryReadyAt`. A node that was interrupted mid-attempt
  and carried the observable `recovered_running` intermediate annotation ends
  with a bare, clean success — the annotation is retained in *history* but is
  absent from the terminal status;
- entering **failed** sets `FailCode`/`FailMessage` to the terminal reason and
  clears skip hints and parked deadlines;
- entering **skipped** sets `code=upstream_failed`, the explanatory message and
  `SkippedBecause`, and clears failure/backoff leftovers.

Non-terminal transitions (parked `ready`, recovery repairs) keep their
intermediate markers on purpose: they remain observable while the run is
incomplete, but the final classification is decided solely by the terminal
transition. Because terminal fields are persisted inside the same atomic
snapshot write as the state change, concurrent readers always observe a
self-consistent state/reason pair, and replaying history (whose last event per
node is the terminal transition) reproduces the identical classification.

### 2.2 Business actions: run-scoped exactly-once records

Tasks with external side effects (placing an order, sending a message,
writing a result file) register them through the recorder handed in
`TaskInput.Actions`:

```go
rec, committed, err := in.Actions.Commit(ctx, "order-42", replayPayload)
if err != nil { return nil, err }
if committed {
    // exactly one caller across attempts and crashes performs the effect
}
```

Rules:

- **dedup scope**: the business key is unique per `(run, node)`. The first
  durable commit wins; retries, recovered replays, and racing goroutines
  committing the same key all receive the original record with
  `committed=false` and perform nothing. Different runs never interfere, even
  with identical keys; different nodes within one run also have separate key
  spaces;
- **crash window**: `Commit` is durable before returning. A process killed
  after the commit but while the node is still `running` finds the record on
  resume; the replayed attempt sees `committed=false`, so the final action set
  contains exactly one record and the effect is not counted twice. A crash
  *during* an uncommitted external call follows the usual replay semantics —
  gate the effect on `committed` (or make the business call itself idempotent
  with the same key);
- **query and replay**: `Engine.Actions(runID)` and
  `RunRecord.Actions` return the committed set in deterministic commit order
  (`Seq` ascending); the view is identical before and after restart and under
  concurrent readers. The winning record's `Attempt`, `Value` and
  `CommittedAt` are preserved; losing commits mutate nothing.

Storage is a per-run append-only action log (`actions/<runID>.log` in
`FileStore`) scanned on open to rebuild the committed key set and sequence
counter; the check, sequence assignment and append are one atomic operation.

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

#### 3.3.1 Backoff waits survive restarts

When a failed attempt is scheduled for retry, the scheduler computes the
**absolute** retry deadline `now + Delay(attempt)` and writes it into the node
snapshot (`NodeStatus.RetryReadyAt`) in the same durable transition that moves
the node to `ready`. On `Resume`:

- a `ready` node carrying `RetryReadyAt` stays `ready`; the in-memory timer is
  re-armed with the *same* absolute deadline, so the process sleeps only for
  the **remaining** wait — it does not retry immediately and does not start a
  fresh (therefore longer) wait;
- if the deadline is already in the past at restart, the node is eligible
  immediately;
- the deadline is anchored to the original failure, so any number of restarts
  within the wait keep the final retry instant identical to the uninterrupted
  policy; a history event records each preserved deadline
  (`"resumed inside backoff wait; retry deadline … preserved"`) without
  changing the node state;
- starting an attempt clears `RetryReadyAt`; every terminal state clears it as
  well, so a finished node never looks parked.

Intermediate information (the parked wait, retry events) stays observable in
history; it never alters terminal classification (see §2.1).

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
  actions/<runID>.log    # append-only JSON Lines, one deduped ActionRecord per line
```

- Snapshots are rewritten atomically (rename); readers never see torn state.
  All store reads return deep copies.
- `AppendEvent` is idempotent for a `(RunID, Seq)`: on restart the existing
  log is scanned for the max Seq, so a replayed append cannot duplicate a
  history record.
- `CommitAction` is idempotent for a `(RunID, NodeID, Key)`: on restart the
  action log is scanned to rebuild the committed key set and max action Seq,
  so replays cannot duplicate a business action or reuse its sequence.
  `RetryReadyAt` deadlines live inside the snapshot JSON and therefore need no
  separate write.

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
  index is preserved; a legacy `ready` snapshot without a durable deadline is
  repaired the same defensive way;
- a node in `ready` **with** a persisted `RetryReadyAt` keeps its parked state
  and the same absolute retry deadline (§3.3.1): only the remaining backoff is
  waited after restart, repeatedly across multiple restarts;
- `succeeded` nodes are never re-executed;
- on resume the max stored event Seq seeds the scheduler counter so new events
  never reuse or duplicate a Seq.

**Side effects**: the engine guarantees *at-most-once dispatch per attempt
per committed state*, but a task killed mid-execution by definition cannot be
made effect-free by an orchestrator alone. Business actions registered with
`TaskInput.Actions.Commit` get run-scoped **exactly-once records**: the key
commits once durably, retries/replays after a crash see the existing record,
and the committed set is queryable and replayable (§2.2). Tasks that prefer
end-to-end idempotency may additionally use `TaskInput{RunID, NodeID,
Attempt}` as an idempotency key against their own external system; the same
`(run, node, attempt)` is replayed at most once after a crash.

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
  event logs, action logs, revision counters and node ids never overlap. A
  failing run does not mutate another run's snapshot, history or action set
  (covered by `TestMultiRunIsolation` and `TestActionScopedPerRun`).
- `GetRun` returns a deep-cloned `RunRecord` (snapshot + replayed history +
  committed action set); concurrent readers never observe a half-updated map
  (the code passes `-race`) and the state/reason pair is always
  self-consistent (§2.1).
- `Subscribe` replays durable history then streams live transitions on a
  buffered channel; sequence numbers are strictly increasing per run.
- Store writes are mutually excluded (per-run event/action log locks; full-map
  locks for the memory store) and reads are copy-on-read. The same
  `(run, node, action-key)` therefore has exactly one winner even under
  concurrent commits (`TestActionConcurrentSameKey`), and action replays are
  ordered by commit `Seq`.

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
| interrupted during a backoff wait: absolute deadline preserved, remaining wait only, survives repeated restarts | `TestRecoveryMidRetry` |
| business action exactly-once across the crash window, retries and racing commits; per-run scoping; durable replay | `TestActionExactlyOnceAcrossCrash`, `TestActionScopedPerRun`, `TestActionConcurrentSameKey`, `TestActionReplayAcrossReopenedStore` |
| store-level action log: dedup, sequence continuation, isolation, concurrency | `pkg/store/action_test.go` |
| clean terminal success after recovery; three terminal classes mutually exclusive; concurrent readers self-consistent; replay classification stable | `TestTerminalSuccessCleanAfterRecovery`, `TestTerminalClassesMutuallyExclusive`, `TestTerminalClassificationConcurrentReaders`, `TestTerminalReplayStableAcrossRuns` |
| dynamic spawn, dedup-once, cycle/dangling/cap rejection | `pkg/engine/engine_spawn_test.go` |
| cross-run isolation, concurrent reader consistency | `TestMultiRunIsolation`, `TestObserverConsistentView` |
