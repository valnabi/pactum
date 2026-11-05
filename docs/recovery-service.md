# Optional recovery service

`pactum_core::recovery` wraps the existing Coordinator, multi-mode Engine, bounded PlanEngine
and optional [SagaEngine](sagas.md). Saga manual failures remain discoverable;
operator Resume can renew an exhausted action budget but cannot bypass permanent
rejections or irreversible boundaries. Compensation never implies atomic rollback.
It is optional: direct `execute`, `recover`, `recover_pending` and status APIs
keep their existing behavior. It uses Tokio, already required by core, and has
no Aquanet, ContentGraph, transport, worker or hosting dependency.

## Start, submit work, inspect, stop

Create one runner per protocol journal. Give it a **separate** schedule journal
and a permanent application namespace identifying that exact protocol journal.
Keep both databases together across restarts. Never point the two owners at the
same journal or reuse a namespace/schedule database for a different engine.

```rust,ignore
use pactum_core::recovery::{Config, Control, Runner};
use pactum_sqlite::SqliteJournal;

// `engine` is an opened Coordinator, Engine or PlanEngine with durable work.
let runner = Runner::open(
    engine,
    SqliteJournal::open("recovery-schedule.db")?,
    "tenant-42/transactions-v1",
    Config::default(),
)?.observe(|trace| {
    // Forward to an application's structured tracing sink; keep this bounded.
    eprintln!("{} {} {:?}", trace.action, trace.key, trace.schedule);
});
let (handle, task) = runner.start();

// Use this same exclusive owner for requests and participant registration.
{
    let mut engine = handle.target().lock().await;
    // engine.execute(...).await?;
    // coordinator.register(returned_participant)?;
}
handle.wake();
let snapshot = handle.query().await?;
for pending in snapshot.pending {
    println!("{} {:?} {:?}", pending.work.key(), pending.work, pending.schedule);
}
// For an existing pending key:
handle.control("operation-key", Control::Pause).await?;
handle.control("operation-key", Control::Resume).await?;

handle.stop();
let runner = task.await??; // Inspect errors; wait before reopening journal owners.
drop(runner);
drop(handle); // Releases the shared protocol owner when no other handles remain.
```

`start` moves the runner, preventing a second task for that runner. A clean stop
returns the runner through its join handle; it can be started again. Dropping all
handles closes its command queue and stops it after the current bounded pass.
For prompt shutdown, call `stop`, then await the task. Stop drops cooperative
recovery futures even while waiting for the target mutex. It does not invoke
protocol cancellation or request an abort. Journal writes contain no await
between append, flush and publishing local state. A dropped finalization is
retried using the existing durable decision/receipt on the next run.
Synchronous backend and journal calls must be bounded by the implementation;
Tokio cannot interrupt a blocking call. Resource call retries/timeouts remain
controlled by `RetryPolicy`; set `ExecutionPolicy::max_chunks` to a finite value
when bounding a native batch's work per attempt matters.

Do not hold `target()`'s mutex while awaiting query/control commands. All engine
operations, participant registration and runner attempts serialize on that owner.
A returning participant can be registered through the handle, followed by `wake`.
Wake coalesces discovery notifications and respects persisted backoff and pauses;
use `Retry` to request immediate dispatch of a specific key.

## Scheduling and safety

Only indexed pending protocol work is recovered. The runner reserves an attempt
in its scheduling journal **before** dispatching resource work. That reservation
includes count, last attempt, first observation and next eligible Unix millisecond.
An interrupted attempt therefore retains backoff on restart. The schedule log is
versioned and permanently bound by its first flushed namespace record. Replay
rejects sequence gaps, unsupported versions and namespace mismatches.

Retry delay grows exponentially from `initial_backoff`, capped by `max_backoff`,
with equal jitter in the upper half of the current delay range (minimum 1 ms).
`jitter_seed` defaults to current time; choose distinct deployment seeds if needed.
Transient resource failures remain retryable indefinitely, with bounded per-pass
work and capped delays. The poll interval can add latency after an eligible
deadline. Clock adjustments affect retry timing, never protocol outcomes.

`max_per_tick` bounds keys dispatched per pass. A round-robin cursor moves past
the last attempted key, preventing a failing low-sorted key from starving later
work. It resets on restart; all pending keys are rediscovered. Backoff, successful
terminal cleanup and scheduler pauses are independent per key. Discovery and
queries enumerate pending work, so memory/cost scale with pending count. The
bounded command queue applies backpressure to administrative callers; bounded
command draining prevents a query flood from starving recovery.

| Observation | Runner behavior |
| --- | --- |
| Resource unavailable or ambiguous resource response | Preserve typed protocol uncertainty; retain error and back off |
| `Error::Invalid` from per-key recovery | Persist `Reason::Permanent`; require operator retry/resume after repair |
| `Error::Corrupt` | Stop and report the error through the join result |
| `Error::InDoubt` from either journal | Stop all dispatch immediately; drop handles/owners, reopen and replay |
| Native chunk pause | Persist `ProtocolPaused`; wait for explicit resume |
| Terminal protocol result | Remove scheduling state; preserve all protocol retry bindings/receipts |

The scheduler never decides whether a transaction commits or aborts. In 2PC,
existing recovery may durably abort **undecided** work, exactly as before. A durable
commit is only redelivered. A journal write with unknown durability requires
reopen, never a compensating abort. Native modes retain their own receipt,
partial-application and publication semantics. External direct recovery through
the target remains possible; a scheduling pause governs this runner's dispatch,
not other application calls.

## Operator controls and observability

`query` reads authoritative pending/in-doubt statuses, durable schedule reasons,
last errors/attempts, both acknowledged journal positions, and metrics. A missing
schedule means newly observed work before its first persisted attempt/control.
A successful query means both the runner is alive and the target accepts status
reads. A failed query/channel means the caller must inspect the task result;
it is not evidence that a transaction aborted.

- `Pause` persists a scheduling pause, acknowledged after any in-flight attempt.
  It cannot retract already dispatched work or alter protocol decisions.
- `Retry` clears a scheduling pause/permanent-error stop and makes the key due
  immediately. It retains any native chunk pause and preserves attempt history.
- `Resume` also clears a native chunk pause through the Engine's journal barrier.
  It never clears chunk rejection, replaces immutable requests or reverses a
  commit/abort/publication result. The scheduling intent is durable first; after
  an interrupted resume, query and repeat the command if necessary.

Controls reject unknown/terminal keys. All acknowledged controls have durable
`operator:*` audit events. Authorization, actor identity, administrative permissions,
audit export and retention are application responsibilities. The schedule journal
is append-only; this initial service does not compact it or accept schedule
checkpoints. Plan storage retention accordingly; deleting it loses scheduling
pauses, backoff and audit history, although the protocol journal retains decisions.

`Snapshot::metrics` exposes process attempts, retries, completions, pending count,
oldest observed pending age and acknowledged controls. Per-key attempt counts and
timestamps survive restart. `observe` receives structured `Trace` records after
each acknowledged scheduling write; export these into the application's tracing
and metrics system. The callback must not block or panic. Callback delivery is
best effort across process death; the journal is the durable audit source.

SQLite enforces advisory single-owner journal locks. The runner adds no distributed
leadership mechanism: future distributed ownership must fence both owners before
dispatch. Keep the returned handles/target mutex within that ownership boundary.

## Verification

Scheduler unit tests cover bounded queues, command backpressure, round-robin
fairness, retry caps/jitter, persistent pause and timing, configuration errors,
ambiguous schedule barriers, wakeups, and cooperative shutdown/repeated startup.
SQLite integration tests cover returning 2PC participants without decision
reversal, native pause/resume across owner restart, uncertain protocol barriers
and receipt reconciliation, and child-process exits after every scheduling
append/flush for Atomic, Chunked and StagedPublish. Existing protocol boundary
crash tests continue to validate the transaction authority independently.
