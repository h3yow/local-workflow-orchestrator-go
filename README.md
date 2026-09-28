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
| `pkg/model` | Run/node states, failure/skip codes, attempt records, transition events, business-action records, terminal classification, deep-copy snapshots for concurrent readers |
| `pkg/store` | Durability boundary: `MemoryStore` (tests) and `FileStore` (atomic JSON snapshots + append-only JSONL event and action logs) |
| `pkg/engine` | Single-writer scheduler: bounded dispatch, FIFO admission, retries/timeouts with restart-resumable waits, at-most-once business actions, skip propagation, dynamic spawn validation, recovery and observers |

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

Register an externally-visible action so it survives retries and crashes
exactly once within the run:

```go
orderTask := func(ctx context.Context, in engine.TaskInput) (*engine.ExecResult, error) {
    inserted, err := in.Actions.Record(ctx, "order-1234", `{"amount":10}`)
    if err != nil { return nil, err }
    if inserted { // only the first committer performs the side effect
        if err := placeOrder(ctx); err != nil { return nil, err }
    }
    return &engine.ExecResult{Value: "placed"}, nil
}
```

## Semantics summary

- **Node states**: `pending → queued → running → succeeded | failed`;
  failed nodes make all transitive dependents `skipped` (distinct from both
  `failed` and unexecuted `pending`), annotated with `upstream_failed` and the
  blocking node id.
- **Failure codes**: `attempts_exhausted`, `timeout`, `permanent`,
  `invalid_spawn`, `upstream_failed`, `recovered_running`.
- **Retries**: each failing attempt is retried up to `MaxAttempts-1` times
  using the node's deterministic backoff policy; `engine.Permanent(err)`
  short-circuits retries; a timeout counts as a failed attempt. The absolute
  retry deadline (`RetryNotBefore`) is persisted, so a restart during the wait
  resumes only the **remaining** delay — never immediately, never a fresh full
  backoff; repeated restarts converge on the same retry instant.
- **Business actions**: tasks register external side effects via
  `in.Actions.Record(ctx, businessKey, payload)`; within a run a key takes
  effect **at most once** across retries, crashes (record is fsynced before the
  effect) and concurrent attempts — the first committer performs the effect,
  duplicates get `inserted=false`. Keys are run-scoped; the committed set is
  queryable (`engine.ListActions`, `RunRecord.Actions`) and identical for all
  readers and across restarts.
- **Terminal classification**: `succeeded` / `failed` / `skipped` are mutually
  exclusive with clean markers (a success carries no failure/recovery/skip
  marks). Recovery and retry evidence remains observable in `Attempts` and
  history; `model.ClassifyTerminal(history)` reproduces the live snapshot's
  classification, including under concurrent reads.
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
  node found in `ready` keeps its parked retry deadline (see above).
- **Dynamic expansion**: a successful task may return `SpawnNode`s. The
  expanded graph is fully re-validated (ids, values, dangling edges, cycles);
  any structural problem rejects the **whole batch** and fails the producing
  node with `invalid_spawn`. A duplicate id (in the batch or the graph) takes
  effect exactly once.

See [`docs/design.md`](docs/design.md) for full semantics, recovery rules,
storage layout and verification procedure.

## Testing

```bash
go test ./... -race -count=3
```

Test logs print the input specification and the state-transition rationale
(reject codes, retry/backoff decisions, skip reasons, recovery annotations).
