# Pactum v0.1 architecture

Implemented for [epic #1](https://github.com/valnabi/pactum/issues/1).

## Boundaries

```text
application -> pactum-core -> pactum-journal
application -> pactum-sqlite -> pactum-journal
pactum-demo -> core + sqlite (example consumer and resource adapters)
```

Journal stores versioned opaque bytes and has no core, SQLite, or application
dependency. Core interprets version-1 JSON protocol records; unknown versions,
fields, invalid identifiers and illegal transitions fail replay. SQLite provides
the independently reusable storage adapter. Demo is an ordinary consumer; it is
not imported by core. See [ADR 0001](adr/0001-workspace-boundaries.md) and
[ADR 0002](adr/0002-durable-protocol.md).

## State machine and durable ordering

Every record is appended and flushed before the next protocol action:

1. `Begin` binds a globally unique transaction ID to sorted unique participant
   identities and immutable opaque payloads. It is durable before prepare dispatch.
2. `Prepared` stores each participant's durable opaque token (1..=65536 bytes).
3. `Decided(Commit)` is legal only after every participant has a prepared record.
   A rejection, failed prepare, timeout or pre-decision cancellation chooses abort.
4. `Decided(Abort)` is legal while no decision exists. Both decision variants
   cross the durability barrier before finalization dispatch.
5. `Acknowledged` records a participant's successful durable finalization. All
   enlisted resources must acknowledge, including resources never asked to prepare
   when aborting. This fences delayed or unacknowledged prepares.

Decisions are immutable; duplicate/conflicting decision records are invalid.
Completion is derived from the durable decision and all acknowledgements, so an
extra completion record and its durability boundary are unnecessary. A response
may be lost after any write or resource operation. Duplicate delivery is normal.

An append/flush failure might have persisted. The coordinator poisons its handle,
stops dispatch, returns `Error::InDoubt` and refuses further status/recovery calls.
Drop and reopen to learn the durable prefix. `diagnostics` remains available with
the last acknowledged sequence and a reopen-required flag. No in-memory guess
can override an uncertain write.

## Recovery

`Coordinator::open` restores validated checkpoint state and streams the durable tail.
It does not dispatch participant work. Applications call `recover` before new
execution, and again as needed. Each pass visits transactions and participants in
stable lexicographic ID order. There is no automatic background task.

An undecided transaction receives a durable abort, even if all votes were prepared.
A flushed commit record is always preserved. Recovery redelivers only missing
acknowledgements and never repeats prepare. An unavailable registration produces
pending status; resources can be registered later. Duplicate registrations fail
instead of replacing a resource silently. Resources themselves must bind their
stable IDs to the same durable state; demo persists and checks that binding.

Repeated `execute` with identical input reconciles existing work. If a previous
future was dropped without a decision, reconciliation aborts it. Different input
under the same transaction ID fails without dispatch. IDs are never recycled.
No heuristic abort is permitted at a prepared resource; coordinator unavailability
can block the resource indefinitely.

## Async policy, cancellation and diagnostics

Participant operations are asynchronous and object-safe through `async_trait`.
Tokio applies a configurable per-attempt timeout, bounded attempt count, and fixed
retry backoff (defaults: 5 seconds, 3 attempts, 25 milliseconds). Prepare rejection
is a vote and is not retried; transport errors and timeouts are retried. Finalize
errors retain the durable decision and pending identity for later recovery.

Cancellation before the decision chooses abort; a cancelled execution stops
finalization and returns pending status. Cancellation after a durable commit
never changes that commit. Use a fresh cancellation handle to reconcile. Dropping
an execution future can lose an operation response but cannot interleave with a
synchronous append+flush barrier. Participants must serialize/fence late work,
persist abort tombstones even for unknown prepares, and tolerate retries after a
dropped future. Core does not assume transport cancellation rolls back a resource.

Timeouts require participant futures to yield. Synchronous journal barriers are
not preemptible and should run on a dedicated orchestration thread/runtime if
storage latency would block unrelated application work. Demo SQLite operations
use `spawn_blocking`; dropped jobs can finish later, safely serialized with abort
by the resource mutex and SQLite transaction. Physical I/O hangs still require
operational intervention; async cancellation does not terminate OS I/O.

`Status` distinguishes preparing, finalizing and complete, and exposes the durable
decision, pending identities, typed outcome and last local error. Local errors
are not persisted across restart. `Diagnostics` exposes durable sequence, retained
transaction count, pending transaction count, recovery passes and journal health.
An `Aborted` or `Committed` outcome requires every durable acknowledgement.

## Journal storage and failure assumptions

`append` stages an ordered record, `flush` atomically persists all staged records,
and `replay` returns only the durable tail after any checkpoint. An unflushed batch may disappear on
crash. Barrier errors are ambiguous regardless of whether the backend reports
failure before or after an actual commit. Each backend needs exclusive ownership.

SQLite uses a sidecar advisory file lock held for the journal lifetime, then WAL,
synchronous=FULL, and `BEGIN IMMEDIATE` / `COMMIT` per flush batch. Opening verifies
settings, schema version and `quick_check`. Sequence, consumer version, payload
and SHA-256 checksum are stored per row; replay rejects gaps and mismatches.
SQLite handles incomplete physical WAL tails atomically. Checksums detect accidental
record corruption; a checksummed durable head also rejects missing tail rows and
checkpoint generations. They do not provide an external integrity authority. Physical truncation/corruption fails closed when
SQLite cannot recover its committed prefix. No repair silently changes a decision.

Use a local filesystem that supports SQLite WAL locks and correctly honors sync.
Do not remove an active sidecar lock, use hard-link aliases to evade it, mutate
open databases through another connection, or restore the journal independently
of resource state. No distributed fencing/consensus is claimed. Storage loss,
malicious modification, lying hardware, inconsistent backup restores, and arbitrary
nontransactional resource behavior are outside the guarantees. Retain database,
WAL and associated files together using SQLite-supported backup procedures.

## Reference resources and limitations

Demo provides two independent SQLite counters, each with durable prepared deltas,
immutable payload/token binding, abort tombstones, and atomic value/state commits.
Prepare reserves numeric capacity across outstanding deltas so commit cannot
later reject for overflow. Nonnegative additions commute; rejection of a negative
delta demonstrates abort. The two databases have independent durability domains.

There is no generic resource isolation layer, global atomic read snapshot, network
transport, distributed coordinator consensus, active-active failover,
Aquanet/ContentGraph runtime, or FoundationDB backend. Active history is streamed
after a validated checkpoint; see [journal lifecycle](journal-lifecycle.md).
Adapters can be added by consumers through `Participant`, and other storage through `Journal`.

## v0.2 native execution

`pactum-core::execution` adds a lifecycle trait shared with the existing
coordinator and a native `Engine` for Atomic, Chunked and StagedPublish. Native
work has a dedicated version-2 journal bound to a backend incarnation. The v0.1
2PC journal remains version 1; neither stream is silently interpreted as the
other. Backend-native receipts resolve the effect/checkpoint gap, and publishing
uses a monotonic generation CAS plus a permanent result receipt.

See [mode guide](execution-modes.md) for API usage and guarantees, and
[ADR 0003](adr/0003-multi-mode-execution.md) for transitions, cancellation,
recovery, cleanup and compatibility decisions. The engine retains one-owner local
coordination and does not add consensus or cross-store Atomic transactions.

## Optional Sagas for non-2PC resources

`pactum-core::saga` adds typed forward and compensation requests, permanent
direction-specific receipt keys, durable action budgets and reverse dependency
compensation. It reuses transaction-plan DAG validation and the existing recovery
service, while keeping format-5 Saga history separate from mode/2PC journals.
It does not change the coordinator's decision rules. Ambiguous effects must be
reconciled before compensation; permanent compensation failure and irreversible
boundaries require explicit manual remediation. Compensation is not atomic
rollback and a completed Saga is not an ACID commit. See [Saga contracts](sagas.md).

## Optional distributed 2PC

See [ADR 0004](adr/0004-distributed-ownership.md) and the
[distributed guide](distributed-coordination.md). The separate optional crate
requires enforceable grant validation at journal and participant effect boundaries;
lease expiry alone never permits takeover of unfenced local resources.
