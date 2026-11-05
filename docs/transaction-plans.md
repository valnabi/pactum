# Durable transaction plans

`pactum_core::plan` implements epic #21 and tasks #32–#35. A transaction plan is
an immutable, bounded DAG of existing Pactum transactions. Each step has its own
atomicity and recovery boundary. `PlanOutcome::Complete` means every step has an
acknowledged successful result; **it does not mean global atomic commit**.
Failure or cancellation can leave earlier effects applied. There is no automatic
rollback or compensation of successful steps.

## Run the example

```sh
cargo run -p pactum-demo --example transaction_plan -- ./transaction-plan-data
```

The standalone example runs Atomic → Chunked → StagedPublish. The chunk engine
allows one chunk per pass, so the example explicitly resumes incomplete work.
It prints the durable step outputs, counter `17`, and published snapshot `42`.
Repeating it with the same directory does not repeat effects. The reusable
construction code is in `crates/pactum-demo/examples/support/plan.rs`.

## Specification and identity

`TransactionPlan { version: 1, id, revision, steps }` is permanently bound to its
`PlanId`. A step contains its unique `StepId`, `AdapterId`, prerequisite IDs,
exclusive resource scopes and `StepWork`. Work is either a native `Request`
(Atomic, Chunked, StagedPublish) or immutable 2PC enlistments. Empty plans,
duplicate identities, unknown prerequisites, cycles, empty chunk requests and
invalid resource names are rejected before dispatch. Version 1 allows at most
1024 steps and 16 MiB of JSON-encoded specification. Resource names are 1–256
bytes without control characters. No runtime DAG expansion is supported.

Steps and 2PC enlistments are canonicalized by identity; dependencies and resource
scopes are ordered sets. `validate()` returns deterministic topological order,
with ties broken by step ID. `fingerprint()` hashes the canonical entire spec
with SHA-256, including revision, payloads, mode, dependencies and adapter names.
A changed revision, payload, dependency, adapter or step set requires a **new
PlanId**. Reordering steps or enlistments leaves the binding unchanged.

Supply a globally unique, permanent deployment namespace to `PlanEngine::open`.
The runner derives backend receipt keys from this namespace, the plan fingerprint
and step identity, never the attempt number. `TransactionPlan::operation_key()`
returns the namespace-independent seed, not the final backend key. Separate plan
journals must not share a namespace. Retain the journal and all underlying mode
journals, backend receipts and participant state for the lifetime of retry keys.

## Adapters and outputs

Register `Arc<dyn StepAdapter>` by `AdapterId` in a `BTreeMap` when opening the
runner. All work and adapter registrations are checked before binding a new plan.
`NativeAdapter::new(engine)` uses an existing native mode `Engine`; its identity
includes the backend's durable incarnation. `TwoPhaseAdapter::new(namespace,
coordinator)` uses the existing coordinator and participant protocols. Its namespace
must identify the retained coordinator journal incarnation, and participant IDs
must include their permanent resource namespaces. Changing an adapter's durable
identity fails before dispatch, including when reopening an existing plan. Replay
and status inspection do not require adapters to be online.

Each adapter owns and serializes its underlying engine. Register separate engines
for useful parallelism across independent resources. The caller must ensure a
single owner for each journal/backend pair; the SQLite journal enforces local
same-path ownership. Do not open separate coordinators for the same resources.

Custom adapters must implement `identity`, `accepts`, and `execute`, preserve the
selected mode semantics, and atomically persist effects with permanent receipts
bound to the full immutable request. Calls can be dropped, timed out, repeated or
crashed at any point. Adapters must not spawn detached effects and async calls must
yield to Tokio. The runner cannot make an arbitrary remote API idempotent.

A durable `Report::Result` retains `StepResult::Native(Progress)` or the 2PC decision
and completion acknowledgement. These typed values are the plan's outputs; there
is no mutable application output store or payload templating between steps.
`guarantees(plan, step)` exposes the selected existing mode guarantee. Only Atomic
Committed, all chunks completed, Published, or fully acknowledged 2PC Commit
unlock dependents. Aborted, rejected and publish-conflict results remain terminal
failures. A durable 2PC Commit awaiting acknowledgements is still unresolved.
Timeouts, adapter errors, panics and dropped calls never become definite failures.

## Journal invariants and restart

Use a **dedicated plan journal**; record format 4 is distinct from mode format 2,
coordinator format 1 and recovery scheduling format 20. The header binds the
deployment namespace. A Begin record stores the canonical immutable spec and
adapter resource bindings. An Attempt is flushed before dispatch; a Report is
flushed before exposing outputs or dispatching dependents. The state machine
rejects sequence gaps, future versions, malformed records, duplicate begins,
illegal attempts, result/mode mismatches, duplicate reports and attempts preceding
prerequisite success. Opening a runner only replays and never dispatches work.

A crash after a resource effect but before the plan Report leaves an unresolved
attempt. `resume(id, fresh_cancellation)` retries it with the **same** backend key,
allowing the underlying protocol/receipt to reconcile it. A successful durable
Report is immutable and never dispatched again. The 2PC coordinator retains its
existing rule: an interrupted undecided transaction is reconciled by abort,
without inventing a new commit decision.

After any plan journal append/flush error, all fallible operations reject use of
the poisoned runner. Drop it and reopen storage; acknowledgement loss may mean the
write persisted. If a mode adapter reports journal uncertainty, its underlying
engine must also be dropped/reopened before further useful recovery. Reopening
with a new backend or deleting retained receipt state is not recovery.

Plan journals currently retain their entire history and permanent retry bindings;
plan compaction is not implemented. Opening a checkpointed plan journal fails
closed. Underlying mode/coordinator compaction remains supported independently.
Storage durability assumes the existing journal and participant contracts; process
kill testing does not simulate every filesystem's power-loss behavior.

## Scheduling, cancellation and partial failures

The [scheduler policy guide](transaction-scheduler.md) describes the optional
`TransactionScheduler` / `SchedulingPolicy` seam, default and priority-aging
implementations, queue admission, adapter/plan limits, resource quotas, local pause
controls and pending-reason diagnostics. All dispatches retain the existing
dependency, durability, cancellation and receipt checks.

`PlanPolicy` bounds concurrency, total dispatches per pass, attempts per step per
pass, cooperative timeout and retry backoff. All bounds except backoff must be
positive; concurrency is limited to 1024. Ready steps are selected by fewest durable attempts, then step ID.
A retrying low-ID step cannot starve untouched ready siblings across bounded
passes. A finite DAG, finite attempts, backoff, and explicit resume provide
backpressure without an unbounded work queue. A pass may return with work pending.

Exclusive resource scopes are acquired together in sorted order; steps with an
overlapping scope cannot run concurrently. Concurrency and scope guards remain
held until interrupted adapter futures are actually destroyed, including when an
execution future is dropped and immediately resumed. These are local scheduling
constraints, not distributed locks, isolation from external writers, or protection
against a delayed effect escaping a noncompliant adapter.

Cancellation is a process-local pause: it stops new dispatches and drops in-flight
cooperative calls, reporting them as uncertain. It never erases effects, reverses
a commit decision, or declares a plan aborted. An already-cancelled token binds
the plan without dispatch. Resume with a fresh token. Dropping `execute` also
leaves durable attempts that a subsequent pass can reconcile. Cancellation intent
is not persisted by the plan runner; use recovery-service operator controls if
persistent scheduling suspension is needed.

Timeouts require cooperative async operations. Native backend/journal calls are
synchronous bounded local units and cannot be preempted; use a suitable worker
runtime for these operations. Timeout expiry means reconciliation is needed,
not rollback. Panicking tasks leave their in-flight steps unresolved.

`PlanStatus` exposes every step's attempt count and report, unsatisfied `blocked`
prerequisites, and `failed_dependencies` containing descendants of known failures.
Plan outcomes are Complete, PartiallyFailed, Indeterminate and Pending; uncertainty
takes precedence over a known failure. `is_terminal()` also accounts for steps
permanently blocked by failed ancestors. Independent runnable branches may finish
after another branch fails. Permanently blocked descendants are not dispatched
or retried forever. Partial effects remain visible through the underlying mode.

`resume` reconciles one existing plan; `recover_pending` visits unfinished plans in
ID order. `PlanEngine` implements the existing `RecoveryTarget`; `Work::Plan`
allows the recovery service's durable backoff, status and operator scheduling to
be reused. Each service pass uses the bounded plan policy. Scopes and concurrency
limits apply within one runner; this is not a general workflow engine, arbitrary
agent scheduler, saga framework, or replacement for application orchestration.
Core remains independent of Aquanet, ContentGraph and SQLite.

For non-2PC operations requiring explicit business compensation, the optional
[Saga engine](sagas.md) shares the DAG validator and recovery interface. Ordinary
transaction plans retain their existing per-step guarantees and failure behavior.

## Verification

`crates/pactum-demo/tests/plan.rs` covers DAG validation, canonical identities,
fan-out/fan-in, concurrency, resource exclusion, retry fairness/backpressure,
partial failure, cancellation before/during dispatch, timeout, panic, wrong-mode
results, interrupted-future resume, replaced resource identities, native and 2PC
adapters, recovery-target integration and illegal replay histories. Fault tests
inject lost acknowledgements and terminate child processes before/after every
plan append/flush and mode adapter call, reopen the same SQLite databases and
verify exactly-once effects, immutable completed outputs and dependency gating.
Existing mode tests cover their internal protocol crash boundaries.

Run the workspace's documented local build, format, Clippy, tests and docs checks;
no GitHub-hosted CI service is required.

## Plan operational failures

See the [structured error contract](error-handling.md) for stable codes,
source/context propagation, safe retry guidance and migration details.

The optional [transaction planner](transaction-planner.md) generates validated DAGs
from declarative intent and frozen provider capabilities without executing work.
