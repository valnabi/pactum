# ADR 0003: Multi-mode execution and receipt-protected recovery

Status: accepted for v0.2. Implements #11 and its contract/journal work (#12–#13).

## Decision

Keep the v0.1 `Coordinator`, `Participant`, version-1 records and failure behavior
unchanged. Add `pactum_core::execution::Engine` for native Atomic, Chunked and
StagedPublish protocols. `Execution` is a lifecycle trait with associated key,
request and status types. Its implementation on `Coordinator` adapts
`TwoPhaseRequest` without changing participant adapters. No new facade crate or
network/framework dependencies are introduced. `pactum-journal` remains an
independent ordered byte journal.

`OperationId` identifies a native operation; `TransactionId` identifies a 2PC
transaction. Native requests are typed variants. Each stable operation key binds
permanently to its complete request: mode, payload, chunk order, target and CAS
precondition. Namespace changes and replacement databases must not silently reuse
old work. The engine journal's first flushed record binds it to the backend's
persisted identity (namespace plus database incarnation). SQLite creates a random
128-bit incarnation once and retains it with namespace metadata.

## Durable records

Native execution uses a dedicated version-2 stream: `Header`, mode-tagged `Begin`
and mode-tagged `Progress` records. Every write crosses append and flush before
publishing local state or dispatching the next external action. Replay checks
contiguous sequences, versions, header identity, immutable IDs, unique begins,
mode consistency and legal transitions. Unknown/malformed records and impossible
histories fail closed. Any append/flush failure poisons the engine: status and
recovery refuse further use until all handles are dropped and reopened.

2PC and native mode journals deliberately use separate files. Opening one with
the other engine fails; records are never silently skipped or reinterpreted.
This preserves existing v0.1 replay without introducing mixed-stream ownership
or changing its durability decisions. Multiple new modes can share a native
engine journal. Backend policy does not enter the generic byte-journal contract.

## Commit/checkpoint gap

A journal and a backend database do not share a transaction. Intent is flushed
before dispatch, but backend effects can commit before the next journal barrier.
Therefore every backend native unit must atomically commit its effects and a
permanent receipt. The key is `(operation, chunk-index)` for chunks and
`(operation, atomic)` for native atomic work. Requests are bound in the same
backend database, including every chunk, so changed input cannot replay old
receipts. A definite rejection also has a receipt. The SQLite reference adapter
uses an explicit savepoint commit/rollback for effects and commits the result
receipt in the outer transaction.

Atomic begin is conservatively indeterminate until a committed or aborted receipt
is acknowledged. Chunk attempt is flushed before dispatch; its uncertain bit
remains set until a checkpoint acknowledges the receipt. Recovery can replay the
same work after either a lost backend acknowledgement or journal barrier.

Publishing flushes `Publishing` before CAS. The backend switches pointer,
increments monotonic generation and writes its permanent publication receipt in
one transaction. Pointer equality alone is insufficient: a newer publication
might supersede it before recovery. Receipts retain the earlier publisher's
success. Generations prevent ABA and stale publishers from overwriting newer
versions. Published data and receipts are retained, even after supersession.

## Cancellation and cleanup

Atomic cancellation is terminal only before dispatch; after an indeterminate
attempt, cancellation cannot claim rollback. Recovery reconciles the receipt.
A cancellation token stops future work at synchronous protocol boundaries.
`cancel` pauses Chunked durably; explicit `execute` resumes it. `recover` honors
that pause. A rejected chunk is terminal with its acknowledged prefix retained;
there is no global rollback or compensation.

Before publish dispatch, `cancel` flushes `Aborting`, performs idempotent cleanup,
then records `Aborted`. Validation failure uses the same sequence. The durable
cleanup intent is essential when death occurs after deletion but before the abort
record. After `Publishing`, cancellation leaves the CAS outcome indeterminate;
recovery must reconcile its receipt. A CAS conflict has a permanent rejection
receipt and permits cleanup. Cleanup refuses any once-published artifact.

## Consequences and limits

The backend contract requires transactional receipts; arbitrary external side
effects are not made atomic by this engine. Reference native units accept JSON
counter deltas, while staged payloads represent absolute snapshot values.
Applications can implement `Backend` for other local transactional units.

`ExecutionPolicy` limits admitted unit bytes and chunks per pass. Callers split
finite batches deterministically before admission; streaming manifests and
compaction are deferred. Work is sequential. Reopening with a new policy does not
rewrite the immutable batch; unit byte limits are admission limits, while the
chunk budget applies to each execute/recover pass.

Backend calls and journal barriers are synchronous, bounded local work. Async
applications should host this engine on a blocking worker; cancellation cannot
preempt a native transaction midway through commit. There is no distributed
leadership, consensus, cross-store Atomic mode or global chunk isolation.

## Compatibility

No v0.1 consumer migration is required. Existing imports and coordinator calls
continue to work. Opting into `Execution` on a coordinator uses fully qualified
trait calls where its inherent `execute` has the original signature. Native modes
need new dedicated journal and backend paths; do not migrate an existing 2PC
journal by changing its version tag. Workspace release version becomes 0.2.0.
