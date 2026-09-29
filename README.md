# Local Workflow Orchestrator (Go)

A local, single-binary workflow orchestration engine that schedules
**declarative task graphs** with dependencies, concurrency limits, retries,
timeouts, backpressure, durable recovery after process restarts and dynamic
runtime expansion.

Only the Go standard library is used (`go 1.25`).

## Packages

| Package | Responsibility |
|---|---|
| `pkg/spec` | Declarative specification, validation (cycles / dangling deps / duplicate ids / illegal values) with distinguishable reject codes, and the canonical topological order |
| `pkg/backoff` | Retry backoff policies (`none`, `fixed`, `linear`, `exponential`, capped) |
| `pkg/model` | Run/node states, failure/skip codes, attempt records, transition events, deep-copy snapshots for concurrent readers |
| `pkg/store` | Durability boundary: `MemoryStore` (tests) and `FileStore` (atomic JSON snapshots + append-only JSONL event and business-action logs) |
| `pkg/engine` | Single-writer scheduler: bounded dispatch, FIFO admission, retries/timeouts, skip propagation, dynamic spawn validation, recovery and observers |

## Quick start

```go
s := &spec.Spec{
    Name: "build-and-test",
    MaxParallel: 4, // at most 4 nodes running at once
    MaxQueue:    16, // bounded ready-node queue (backpressure)
    Nodes: []spec.NodeSpec{
        {ID: "build", MaxAttempts: 1},
        {ID: "unit", Deps: []string{"build"}, MaxAttempts: 3, Timeout: 5 * time.Minute,
         Backoff: backoff.Policy{Kind: backoff.KindExponent, Base: 100 * time.Millisecond, Factor: 2, MaxDelay: 2 * time.Second}},
        {ID: "integration", Deps: []string{"build"}, MaxAttempts: 2, Timeout: 10 * time.Minute},
        {ID: "report", Deps: []string{"unit", "integration"}, MaxAttempts: 1},
    },
}

st, _ := store.NewFileStore("./.wf-data")
eng := engine.New(st, engine.MapRegistry{
    "build":       buildTask,
    "unit":        unitTask,
    "integration": integrationTask,
    "report":      reportTask,
}, engine.Config{GlobalMaxParallel: 8})

runID, err := eng.Submit(context.Background(), s)
if err != nil {
    // err is spec.ValidationErrors with per-problem RejectCode values
}
rec, err := eng.Wait(context.Background(), runID)
```

After a crash, continue without re-running completed work:

```go
eng2 := engine.New(st, resolver, cfg)
if err := eng2.Resume(context.Background(), runID); err != nil { ... }
rec, _ := eng2.Wait(context.Background(), runID)
```

## Semantics summary

- **Node states**: `pending → queued → running → succeeded | failed`;
  failed nodes make all transitive dependents `skipped` (distinct from both
  `failed` and unexecuted `pending`), annotated with `upstream_failed` and the
  blocking node id.
- **Terminal classification**: `succeeded` / `failed` / `skipped` are mutually
  exclusive. Entering a terminal state writes the reason atomically with the
  state and clears leftovers — a node that was interrupted and replayed ends
  with a bare success (no `recovered_running` code or message), a failed node
  never carries a skip hint, and a parked retry deadline is always cleared.
  Intermediate recovery/retry information stays observable in history but
  never changes the final classification; history replay and concurrent
  readers derive the same state/reason pair.
- **Business actions (exactly-once per run)**: tasks register externally
  visible effects through `in.Actions.Commit(ctx, businessKey, payload)`.
  The key takes effect exactly once **within the whole run, regardless of
  node**: the first durable commit — whether from node A or node B — returns
  `committed=true` (the signal to perform the effect); every later commit of
  the same key, including one from a *different* node, a retry, a
  crash-recovered replay, or a racing goroutine, gets the original record with
  `committed=false` and inserts no second record. The surviving record keeps
  the originating node's identity and attempt. The committed set survives
  restarts (also when recovery re-runs the interrupted node and another node
  registers the same key after the restart), is queryable via
  `eng.Actions(runID)` / `RunRecord.Actions`, replays in deterministic order,
  and never interacts across runs (identical keys in different runs are
  independent).
- **Failure codes**: `attempts_exhausted`, `timeout`, `permanent`,
  `invalid_spawn`, `upstream_failed`, `recovered_running` (the last one is an
  intermediate annotation only and never appears on a terminal node).
- **Retries**: each failing attempt is retried up to `MaxAttempts-1` times
  using the node's deterministic backoff policy; `engine.Permanent(err)`
  short-circuits retries; a timeout counts as a failed attempt. The retry
  deadline is an **absolute timestamp persisted with the `ready` state**, so a
  restart during the wait resumes with the remaining time — never an immediate
  retry and never a fresh wait; repeated restarts keep the original deadline.
- **Concurrency & determinism**: one scheduler goroutine per run owns all
  bookkeeping; dispatch follows the canonical (lexicographically smallest)
  topological order with strict FIFO admission, so no ready node can be
  executed twice or permanently starved. Transition history is normalised by
  node id at stable scheduling barriers, so identical specs produce identical
  event streams regardless of goroutine timing.
- **Backpressure**: at most `MaxQueue` ready nodes are admitted; the rest stay
  in a bounded, deterministically ordered waiting set (bounded by graph size)
  and are admitted later — nothing valid is silently dropped.
  `GlobalMaxParallel` additionally bounds in-flight nodes across runs, and
  `MaxNodesPerRun` bounds dynamically spawned graph growth.
- **Recovery**: every transition immediately persists an atomic snapshot;
  transition events are flushed at barriers with strictly increasing,
  idempotent sequence numbers. On restart, `succeeded` nodes are never re-run;
  a node found `running/queued` is reset to `pending`, annotated with
  `recovered_running`, and its attempt is replayed — it is never silently
  marked successful, and replayed events cannot duplicate history records. A
  node found mid-backoff keeps its absolute retry deadline (see above).
- **Dynamic expansion**: a successful task may return `SpawnNode`s. The
  expanded graph is fully re-validated (ids, values, dangling edges, cycles);
  any structural problem rejects the **whole batch** and fails the producing
  node with `invalid_spawn`. A duplicate id (in the batch or the graph) takes
  effect exactly once.

### Compatibility

Snapshots gained the optional `NodeStatus.RetryReadyAt` field (an absolute
time, nil for older/intermediate snapshots); action logs live in a separate
`actions/` directory. Older snapshots without the field recover exactly as
before (a parked `ready` node is replayed immediately). The `Store` interface
gained `CommitAction`/`LoadActions`; custom implementations must provide both
(the engine requires them).

See [`docs/design.md`](docs/design.md) for full semantics, recovery rules,
storage layout and verification procedure.

## Testing

```bash
go test ./... -race -count=3
```

To verify business-action uniqueness specifically (same key across two nodes,
concurrent single winner, crash/restart replay):

```bash
go test ./pkg/store/  -run TestAction -race -count=10
go test ./pkg/engine/ -run TestAction -race -count=10
```

Test logs print the input specification and the state-transition rationale
(reject codes, retry/backoff decisions, skip reasons, recovery annotations).
