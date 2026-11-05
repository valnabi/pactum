# Structured errors and recovery safety

Implemented for [epic #51](https://github.com/valnabi/pactum/issues/51), after the
distributed ownership and recovery-service epics. `pactum-errors` provides the
shared vocabulary without depending on an engine, database, async runtime,
Aquanet or ContentGraph. SNAFU preserves typed source chains at call boundaries.
Existing protocol state machines and the recovery runner remain the authorities
for decisions and dispatch.

## A failed call does not decide the transaction

Read `Status::transaction_outcome()` for 2PC knowledge:

| Knowledge | Meaning and required behavior |
| --- | --- |
| `Committed` | Commit and every final acknowledgement are durable. |
| `Aborted` | Abort and every final acknowledgement are durable. |
| `PendingRecovery { decision }` | Keep that durable decision; reconcile missing final acknowledgements. |
| `Unknown` | No durable decision is known. Read authoritative state before making a claim. |

`Outcome::InDoubt` remains available for existing callers; inspect `decision` or
use `transaction_outcome()` to distinguish pending delivery from an unknown
decision. An operational `Error`, its class and its message are never a substitute
for this status. After any journal write/barrier/checkpoint error, status from the
poisoned owner is unavailable: drop all affected handles, reopen, replay, and then
recover. A lost acknowledgement may hide an already durable commit.

Prepare errors can lead the authorized 2PC state machine to record a durable
abort before a decision exists. It must still deliver abort to every participant,
including a participant whose prepare response was lost. After commit is durable,
errors, cancellation, deadlines and operator controls cannot switch it to abort.
Participants retain prepared work until the coordinator's final decision.

Native modes keep `Progress` and their receipt semantics. Plans keep mode-specific
`StepResult` outputs and an aggregate `PlanOutcome`; a completed plan is not a
global transaction. Saga `ActionResult::Rejected` is a definitive, durable
no-effect response. A Saga adapter `Err`, including a permanent configuration
error, remains an operational failure with unknown effects; it never becomes a
rejection vote or automatic compensation instruction. Outbox send uncertainty
likewise never proves transport non-acceptance.

## Layers and sources

| Boundary | Error contract |
| --- | --- |
| Journal | `JournalError` wraps a SNAFU `Diagnostic`; write errors require reopen. |
| Coordinator, plan, native engine and recovery runner | `pactum_core::Error::{Invalid, Corrupt, InDoubt, Operational}` retain structured causes and context. |
| Participant/resource | `ParticipantError`; definitive votes and final acknowledgements stay separate return values. |
| Native backend | `BackendError`; ambiguous calls reconcile using permanent receipts. |
| Saga adapter | `SagaError`; `ActionResult` carries confirmed effects or rejection separately. |
| Messaging storage/validation | `pactum_messaging::Error`; storage sources default to ambiguous classification. |
| Outbox transport | `SendError`; only `rejected` permits definitive permanent non-acceptance. Timeout and unknown constructors preserve uncertainty. |
| Remote exchange | `TransportError`; correlated `RemoteFailure` carries stable server codes across serialization. |
| SQLite reference adapters | `AdapterError` retains driver, I/O, serialization, range and engine/journal sources. |

Use `from_source`, `classified` or `from_diagnostic` to retain original errors;
use `message` only for an explicitly safe validation/invariant explanation. Do
not turn a driver or transport error into `message(error.to_string())`. The
SNAFU selectors are implementation details; public codes do not depend on their
names or generated display text. Sources are cloneable through a shared handle.
Diagnostic equality compares code, context and safe detail, not source identity.

`std::error::Error::source` lets a local caller inspect/downcast the original
cause (for example `rusqlite::Error` or `std::io::Error`). Source messages may
contain paths, SQL or credentials, so they are redacted from automatic diagnostic
`Display`, `Debug`, tracing and wire metadata. Explicit source inspection is a
local privileged diagnostic operation. Legacy/static `message` constructors do
not sanitize text supplied by the caller; pass only safe explanations.

## Stable classification and contextual guidance

`ErrorCode::as_str()` and its serialized snake-case value are stable. Never parse
`Display` or `last_error` to classify a failure. The base categories are:

| Codes | `FailureClass` |
| --- | --- |
| `invalid_input`, `rejected` | `Permanent` |
| `unavailable`, `ownership_busy` | `Transient` |
| `durability_unknown`, `storage`, `timeout`, `cancelled`, `adapter_unknown`, `transport_unknown` | `Ambiguous` |
| `corrupt_history`, `stale_owner`, `protocol_mismatch`, `operator_required` | `OperatorActionRequired` |

Classification alone is insufficient for a safe next action. Supply confirmed
phase, decision, authority and idempotency knowledge through `RetryContext`, or
use the context supplied by a valid coordinator status:

```rust,ignore
if let Some(failure) = status.diagnostic.as_ref() {
    let next = failure.retry_disposition(status.retry_context());
    // Route by next and status.transaction_outcome(), never by failure.to_string().
}
```

| `RetryDisposition` | Action |
| --- | --- |
| `Retry` | Repeat the same idempotent operation while authority is valid. |
| `ReconcileBeforeRetry` | Inspect/reopen durable state or reconcile the same receipt key before retrying. |
| `Permanent` | Repair input/configuration; do not dispatch automatic retries. |
| `OperatorIntervention` | Stop the affected session or block the key for explicit remediation. |

Unknown authority defaults to intervention. Stale owners, corrupt history and
protocol mismatches cannot become retryable by changing diagnostic wording.
Journal reads preserve the underlying storage or fencing code; a failed I/O
read alone does not prove corruption. Validated schema/checksum/state-machine
failures use `corrupt_history`. Any journal write or `JournalWrite` phase requires reconciliation/reopen even
when the underlying source would otherwise be transient. A read-only idempotent
storage query can be retried. A transient operation without a retained immutable
idempotency key must first be reconciled. Permanent failure while delivering a
durable decision requires intervention, not a different decision.

`decision_advice(context)` separately reports `AbortBeforeDecision`,
`RetainCommit`, `RetainAbort` or `Reconcile`. Abort advice is limited to an
explicitly undecided, authorized, idempotent 2PC prepare outside a journal write.
It is permission for the existing protocol to evaluate/record abort, never a
claim that abort is already durable or complete. Defaults never authorize abort.

The bounded participant retry loop stops retrying permanent and operator failures.
For ambiguous calls, repeating the identical participant operation is itself
receipt/tombstone reconciliation under the Participant contract. Timeouts and
cancellation do not undo effects or authorize a new request identity.

## Durable recovery and operator controls

`Schedule::failure` stores `FailureMetadata { code, disposition, context }` in the
existing format-20 schedule journal. It contains no error source or Display text.
Older schedules without that optional field remain readable. `last_error` remains
a diagnostic field and is never used to choose a transaction decision. Replayed
failure metadata is validated before dispatch; impossible code/disposition pairs
fail closed.

Outbox `Delivery::failure` exposes the same typed metadata after reopening, so
operators do not parse `last_error` text. SQLite messaging schema v2 adds this
nullable field; opening v1 atomically migrates it while retaining every envelope,
receipt, attempt count, ordering binding and delivery state. Older rows have no
structured failure metadata. Expired send budgets remain uncertain dead letters
requiring operator action; source text is never stored.

The existing runner backs off unresolved idempotent resource work, persists
permanent/manual blocks, and stops on corrupt history, stale authority or an
ambiguous journal operation. It does not introduce another retry engine or
transaction authority. `Control::Retry` requests another scheduled attempt;
`Control::Resume` also invokes the protocol's explicit resume authorization.
Neither control reverses a decision or changes an immutable payload.

Plan and Saga operational reports persist code-based metadata separately from
protocol results. Their `diagnostic` source fields are process-local and absent
after replay. A permanent/manual plan failure requires `PlanEngine::retry_step`
with an audit reason, or recovery-service `Resume`. Authorization is journaled,
survives restart and is consumed by the next attempt. Saga `retry`/operator
`Resume` renews the existing bounded action budget after repair; unknown effects
remain unknown until the same receipt key is reconciled. It cannot bypass a
definitive rejection, irreversible boundary or permanent payload binding.

## Context, tracing and remote protocol

`ErrorContext` carries applicable transaction/operation/plan/Saga identity,
participant, step, phase, journal/adapter operation, fencing epoch and the digest
of a remote request identity. It excludes payloads, prepared tokens, grant
credentials and connection URLs. Boundary enrichment retains existing fields
unless a more specific boundary supplies them.

`Diagnostic::trace()` emits `error_code`, `classification`, `transaction`,
`participant`, `step`, `phase`, `operation`, `fencing_epoch` and `request` through
`tracing`; it does not format source errors or payloads. Pactum installs no global
subscriber. Applications choose their own bounded sink and access policy.

The remote envelope is now protocol **v2**. `Response::result` carries
`RemoteFailure { code, context }` instead of an arbitrary server error string.
Correlated requests retain transaction, participant, phase, request digest and
epoch even when a timeout drops the exchange. Server error codes survive the JSON
boundary; a source chain cannot be transported as an in-process Rust object, so
server driver sources remain server-local. The client retains its transport
source or the typed wire failure. Unknown versions/fields and mismatched replies
fail closed before a reply can be interpreted as a vote or acknowledgement.
Update both endpoints together; v1 envelopes are deliberately rejected.

## Migration and validation

This unpublished v0.2 workspace retains coordinator/participant method signatures
and existing outcome models, but error construction and some diagnostics are
source-breaking:

- Replace tuple constructors such as `JournalError(text)` and `ParticipantError(text)`
  with `::message(text)` for safe static text, or source-preserving constructors.
- Match core errors as `Error::Invalid { .. }`, `Corrupt { .. }`, `InDoubt { .. }`
  and `Operational { .. }`; prefer stable codes when classifying.
- Replace tuple `.0` access with `diagnostic()`, `code()`, `context()` or explicit
  source traversal. Existing snapshot literals need the optional `diagnostic` field;
  `Delivery` literals also need the optional structured `failure` field.
- Saga adapters return `Result<ActionResult, SagaError>` rather than a string error.
  Outbox adapters construct `SendError::rejected`, `unknown`, `from_source` or
  `timeout`, and inspect `permanent()`/`code()` instead of mutable public fields.
- SQLite messaging databases migrate atomically from schema v1 to v2 on open;
  older binaries reject v2. Keep a backup and update readers/writers together.
- Format-4 plan and format-5 Saga journals accept the new operational-report
  variants and explicit plan retry event. Existing records remain readable; older
  binaries reject these new variants. Upgrade all readers together and retain
  the original databases and immutable receipt stores during migration.

The contract regressions in `pactum-errors/tests/contract.rs`,
`pactum-demo/tests/error_contract.rs`, the plan/Saga tests, recovery unit tests and
distributed tests cover stable codes, source chains, redaction/tracing,
phase-aware dispositions, lost durable acknowledgements, partial finalization,
operator authorization across restart, stale epochs, remote partitions/timeouts
and version rejection. The existing fault-injection and real process-kill matrices
remain required: no durable commit may reverse and no ambiguous write may become
a confirmed abort. Process-kill tests do not emulate every physical power-loss,
filesystem or device failure.

Run the documented workspace build, formatting, Clippy, tests and documentation
gates from the README. No hosted CI, network broker or application runtime is
required by the error model.
