# Execution mode guide

Choose a protocol from its actual atomicity boundary:

| Mode | Atomicity / isolation | Durability and retry | Cancellation / recovery | Observable outcomes |
| --- | --- | --- | --- | --- |
| Atomic | One backend native transaction; backend isolation | Effects and receipt commit together | Before dispatch can abort; uncertain attempt must reconcile receipt | Indeterminate, Committed, Aborted |
| Chunked | Each chunk only; readers can see partial effects | Stable ordered chunk keys; atomic per-chunk receipts; monotonic checkpoints | Pause between chunks; execute resumes; recover honors pause | Pending, PartiallyApplied, Paused, Indeterminate, Rejected, Complete |
| StagedPublish | Hidden immutable version; one generation CAS changes visibility | Pointer and publication receipt commit together | Pre-CAS cleanup; after dispatch reconcile, never unpublish | Staging, Staged, Validated, Publishing, Aborting, Published, Conflict, Aborted |
| 2PC | Durable decision across prepared participants; participant isolation | Journal decision barrier before finalization; durable acknowledgements | Abort undecided; redeliver recorded decision | Committed, Aborted, InDoubt with durable decision and pending participants |

`Mode::guarantees()` exposes these boundaries programmatically. A partial batch
is not a 2PC `InDoubt` or a global commit. `ChunkStatus::outcome()` exposes the
batch classification; `completed` always reports the acknowledged durable prefix.
An uncertain next chunk may have committed beyond it. A rejected batch preserves
its successful prefix and is terminal; a paused batch remains resumable.
`Publishing` means the CAS result is indeterminate, while `Published` permanently
means it succeeded, even if a later publisher supersedes it.

## API

Import `pactum_core::execution::{Engine, Execution, Request, OperationId, ...}`.
Open a journal and a transactional backend with stable identities, then call:

```rust,ignore
let mut engine = Engine::open(journal, backend, ExecutionPolicy::default())?;
let cancellation = Cancellation::default();
let recovered = engine.recover(&cancellation).await?;
let key = OperationId::new("import-2026-11")?;
let status = engine.execute(key.clone(), Request::Chunked(ChunkedRequest {
    chunks: deterministic_chunks,
}), &cancellation).await?;
let observed = engine.status(&key)?;
let paused = engine.cancel(&key).await?;
```

Requests are immutable, including chunk order and publish precondition. Retry a
key with identical work. Use a new key for new work or to retry a definitively
rejected operation after changing its input. Backend receipts additionally reject
key/payload mismatches. The engine serializes work under one owner; keep its
backend and journal together across restart. SQLite enforces same-path advisory
locks and FULL/WAL durability. A new database with the same namespace has a new
incarnation and cannot recover an old journal.

Backend failures return a nonterminal mode status with a local `last_error`;
retry or recover safely using receipts. Journal failures return `Error::InDoubt`
and require dropping/reopening the engine and backend before even reading status.
Neither failure is a definite rollback. Backend receipt integrity and durable
filesystem behavior are prerequisites. There is no distributed fencing service.

For 2PC, `Execution::execute(&mut coordinator, tx, TwoPhaseRequest { enlistments },
&cancellation).await` uses the same participant protocol as the original inherent
`Coordinator::execute`. Common `cancel` aborts undecided work and delivers that
abort, or continues delivery of an existing decision. It never reverses commit.
The original cancellation token behavior and v0.1 API remain available unchanged.

## Reference backend and examples

`pactum_sqlite::SqliteBackend` applies JSON `Vec<Value>` payloads. Atomic and
Chunked interpret values as counter deltas, with checked arithmetic and full-unit
rollback on malformed payload, duplicate/empty keys or overflow. StagedPublish
interprets values as a complete immutable snapshot. Stage writes remain hidden
until validation and CAS succeed. Read published snapshots with `visible(target)`;
it joins the pointer and artifact in one statement. Namespace creation, receipts,
staged artifacts and visible pointers all live in the backend database. Engine
progress lives in a separate `SqliteJournal` database.

Standalone examples take an optional persistent data directory:

```sh
cargo run -p pactum-demo --example atomic -- ./atomic-data
cargo run -p pactum-demo --example chunked -- ./chunked-data
cargo run -p pactum-demo --example staged_publish -- ./publish-data
cargo run -p pactum-demo --example multi_mode -- ./all-modes-data
```

The chunk example deliberately limits each pass to one chunk, pauses and closes
both handles, reopens, then resumes three chunks totalling `10`. The combined
example also applies Atomic `+7`, publishes snapshot `42` at generation `1`, and
uses the original durable 2PC participants to commit `a=10, b=20`. Its counter
total is `17`. Repeat the same command/directory to verify stable totals and
idempotent publication. The standalone atomic total is `7`; staged snapshots do
not modify the counter table. Existing `pactum-demo demo/recover` remains valid.

The SQLite payload language is a small reference adapter, not a general SQL
transaction runner. Supply your own `Backend` for application units while
preserving its receipt, validation, publish and cleanup contracts.
