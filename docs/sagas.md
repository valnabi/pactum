# Durable Sagas for non-2PC operations

`pactum_core::saga` implements epic #22 and tasks #36–#38. A Saga performs
forward business operations, then explicitly compensates confirmed successes
after a definite failure. **Compensation is not atomic rollback.** Readers can
see intermediate effects; compensation itself can fail, be ambiguous, or stop
at an irreversible boundary. `Completed` is not an ACID commit and `Compensated`
does not prove that the world returned to its original state.

Use Atomic for a backend-local transaction, and 2PC only with participants that
can durably prepare. Sagas are optional for cooperating resources that cannot
prepare. The engine does not change or duplicate the 2PC coordinator. It shares
transaction-plan step/adapter IDs and deterministic DAG validation, and implements
the existing recovery-service interface. Core has no SQLite, Aquanet or
ContentGraph dependency.

## Example

```sh
cargo run -p pactum-demo --example saga -- ./saga-data
```

The standalone SQLite-backed external-like stub has no prepare protocol. It
persists each business operation atomically with a receipt. The example runs
successful forward execution, successful compensation after rejection,
permanent compensator failure, and an explicitly allowed irreversible boundary.
The retained balance is `50`: `30` successful work plus `10` uncompensated effects
from each intervention case. Repeating the command leaves it unchanged.
`crates/pactum-demo/examples/support/saga.rs` supplies reusable adapter and
construction code.

## Immutable contracts and cooperating adapters

Construct a `Saga { version: 1, id, revision, steps, irreversible, retry_limit }`.
Each step binds a `StepId`, prerequisites, a forward `Operation` and either a
compensation `Operation` or `Compensation::Irreversible`. Operations bind the
adapter and opaque payload; compensators can use a different adapter. Empty
graphs, duplicate IDs, cycles, missing dependencies, zero revision/budget,
unsupported versions, more than 1024 steps and specs exceeding 16 MiB encoded
JSON are rejected before dispatch. Choose `IrreversiblePolicy::Reject` to forbid
irreversible work; `Allow` is explicit permission to include a boundary.

Step-list ordering does not affect identity. The SHA-256 fingerprint binds the
entire canonical specification, including compensations, dependencies, revision,
irreversible policy and retry limit. Reusing a Saga ID with changed work fails.
Every forward and compensator adapter's durable resource incarnation is bound
before the first call. Changed adapter identities fail before further dispatch.
Status/replay work without adapters online; dispatch requires all bound adapters.

`SagaAdapter` must durably bind each key to its immutable payload and atomically
store its effect and permanent receipt. Repeated and delayed calls reconcile
the same receipt without repeating effects. Retain receipts for the lifetime
of the keys. A deployment namespace, full spec fingerprint, step identity and
direction derive each `OperationId`; attempt numbers never enter the key.
Forward and compensation use different keys, even with identical payloads.
Separate Saga journals must use separate permanent deployment namespaces.

| Response | Promise and engine behavior |
| --- | --- |
| `Applied` | Effect and receipt are durable; unlock forward dependents or the next compensation |
| `Rejected(reason)` | Permanent no-effect tombstone, including fencing earlier delayed attempts; forward rejection triggers compensation, compensator rejection requires intervention |
| `Retryable(reason)` | This entire keyed operation has no possible effect from any earlier call; another attempt may succeed |
| `Uncertain(reason)` or transport `Err` | Effect may exist; reconcile the same key, never infer no-effect |

`Retryable` and `Rejected` are statements about **all** earlier attempts, not just
the latest network request. If an API cannot atomically deduplicate or determine
whether a request applied, report uncertainty and require manual reconciliation.
The journal cannot make an arbitrary remote API exactly-once. Never detach
untracked effects. Adapter futures must yield to Tokio; synchronous local units
and journal barriers must be bounded.

## Durable execution and compensation ordering

Use `SagaEngine::open(journal, namespace, adapters, policy)` with a dedicated
format-5 journal, separate from plan, mode, coordinator and schedule journals.
Opening replays only; it never calls resources. `execute` binds or resumes a
specification, `resume` uses its retained spec, and `recover_pending` visits
unfinished Sagas in stable ID order.

Execution is serial in deterministic topological order, breaking ties by step ID.
This also serializes independent branches. A definite forward failure stops all
further forward work. A durable `Compensate` record crosses a barrier before the
first compensation. Only steps with acknowledged forward `Applied` results are
compensated, in reverse topological order. Unresolved forward attempts must first
be reconciled; earlier successes are not compensated while a later forward call
is ambiguous. An ambiguous compensator blocks compensating its prerequisites.

Every `Attempt` is flushed before dispatch and every `Report` before exposing
success or dispatching subsequent work. A crash after a resource effect but
before its report retries the same key. Replay rejects future formats, sequence
gaps, malformed records, duplicate bindings/reports, wrong-direction calls,
out-of-order steps, early compensation and illegal operator retry records.
Completed acknowledgements are never dispatched again.

Any journal append/flush failure poisons the engine. All fallible APIs then
return `Error::InDoubt`: drop the engine and journal, reopen and replay. Lost
barrier acknowledgements may have persisted. Never compensate based on a write
error alone. Saga journals retain all history and permanent retry bindings;
Saga compaction/checkpoints are unsupported and rejected on open. Single-owner
storage and existing journal durability assumptions apply. There is no
distributed fencing or isolation.

## Outcomes, retry limits and intervention

`SagaStatus` retains forward and compensation reports, lifetime attempt counts,
attempt-budget consumption, the durable compensation switch and diagnostic
reasons. Outcomes distinguish `Running`, `Completed`, `Compensating`,
`Compensated`, `NeedsIntervention` and `Uncertain`. Uncertainty takes precedence
when the current operation may have effects. `needs_intervention` is also true
when uncertainty has exhausted the action budget. Only Completed and Compensated
are terminal for recovery discovery; manual failures remain visible.

`SagaPolicy` bounds total calls per pass and provides cooperative timeout and
retry backoff. The spec's positive `retry_limit` bounds attempts per action
durably across restarts, including interrupted attempts. Exhausted definite
no-effect forward retries trigger compensation. Permanent compensator rejection,
exhausted no-effect compensator retries and irreversible boundaries require
intervention. Exhausted ambiguous attempts remain Uncertain, never definite
failure. Cancellation is a process-local pause, not a compensation request.
Dropping or panicking inside a call leaves a durable uncertain attempt for
restart recovery. Cancellation intent is not persisted.

When reverse traversal reaches a confirmed irreversible step, it stops. Already
compensated descendants stay compensated; that step and any earlier effects stay
visible. Automatic traversal never bypasses the boundary. Business-specific
manual remediation remains outside the engine; the journal retains unresolved
status rather than claiming rollback.

After repairing an unavailable cooperating adapter, call
`retry(&id, "operator remediation reason")` to durably authorize one more
bounded budget for the currently blocked action, then `resume`. This preserves
the payload, lifetime attempt count, receipt key and ambiguous report. The reason
is audited in the journal. Retry cannot bypass an irreversible boundary or a
permanent rejection tombstone, and cannot declare a failed Saga compensated.
Permanent failures require an application-specific remediation and audit process;
never edit receipts/journals or delete state to force success.

`RecoveryTarget` exposes `Work::Saga`. The existing recovery runner supplies
backoff, inspection and durable scheduling controls. Manual blocks return
`Error::Invalid` to persist a Permanent scheduling reason while staying in
pending queries. `Control::Retry` changes scheduling only; it does not renew a
Saga action budget. `Control::Resume` authorizes an additional bounded budget
with an audited reason through `resume_work`, provided the block is retryable.
Neither control bypasses permanent rejection or irreversible boundaries.
Application authorization and operator identity are embedding responsibilities.

## Verification

`crates/pactum-demo/tests/saga.rs` checks DAG validation and fan-in, immutable
identities, reverse ordering, duplicate callbacks, receipt conflicts, budgets
across restart, operator retry, cancellation, timeout, dropped futures, adapter
panic, permanent compensation failure, irreversible boundaries, recovery-service
integration and illegal replay histories. Lost acknowledgements and child-process
exits are injected before and after every Saga append/flush and adapter call for
successful, compensated, permanent-failure, irreversible and operator-retry scenarios. Reopening
the same SQLite files preserves effects exactly once and requires intervention
where compensation cannot complete. Process-death tests do not simulate failures
inside every SQLite/VFS instruction or hardware power loss.

Run the workspace build, format, strict Clippy, tests and rustdoc checks from the
root README. No GitHub-hosted CI is needed.

## Saga operational failures

See the [structured error contract](error-handling.md) for stable codes,
source/context propagation, safe retry guidance and migration details.
