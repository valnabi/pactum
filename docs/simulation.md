# Deterministic simulation and state-machine verification

Implemented for [epic #70](https://github.com/valnabi/pactum/issues/70) (#83, #84, #85, #86).

`pactum_core::simulation` provides an in-memory deterministic simulation harness,
bounded state-machine interleaving explorer, trace shrinker, and fixture replay
facility for Pactum's transaction protocols. It exercises the exact production
`Coordinator`, native `Engine`, `SagaEngine`, and `RecoveryTarget` state machines
through existing trait boundaries (`pactum_journal::Journal`,
`pactum_core::Participant`, `pactum_core::execution::Backend`,
`pactum_core::saga::SagaAdapter`, and `pactum_distributed::reference::Clock`)
without introducing a parallel decision authority or separate execution engine.

## Simulated primitives and fault barriers (#83)

| Type | Role |
| --- | --- |
| `SimClock` | Deterministic monotonic millisecond clock with explicit `advance_millis`, backward-time rejection, one-shot `ErrorCode` fault injection, and `pactum_distributed::reference::Clock` integration |
| `SimAuthority` / `SimGrant` | Deterministic lease/epoch authority backed by `SimClock`; enforces single live owner per scope, monotonic epochs across expiry/release, and `ErrorCode::StaleOwner` / `ErrorCode::OwnershipBusy` diagnostics |
| `SimController` | Step-numbered boundary interceptor and trace recorder evaluating `FaultRule`s at `BoundaryPhase::BeforeEffect` and `BoundaryPhase::AfterEffect` |
| `SimJournalStore` / `SimJournal` | Reusable `pactum_journal::Journal` implementation with exclusive unfenced owner locks, optional `SimGrant` fencing, unflushed staging discard on crash, SHA-256 entry/checkpoint verification, `Compact`/`Archive` retention, and corruption injection |
| `SimParticipantStore` / `SimParticipant` | Durable 2PC counter resource implementing `Participant` with immutable payload/token binding, abort tombstones, optional `SimGrant` fencing, duplicate delivery, and seeded message reorder queues |
| `SimBackendStore` / `SimBackend` | Native mode backend implementing `execution::Backend` for `Atomic`, `Chunked`, and `StagedPublish` with transactional unit receipts, monotonic generation CAS, and orphan artifact tracking |
| `SimSagaStore` / `SimSagaAdapter` | Non-2PC adapter implementing `saga::SagaAdapter` with permanent direction-specific (`Forward` / `Compensate`) receipts, configurable uncertainty, and business rejections |

Every journal, participant, backend, and Saga adapter operation enters two
deterministic boundary phases:

1. `BoundaryPhase::BeforeEffect`: evaluated before any durable state mutation.
   Supports `FaultAction::FailBeforeEffect`, `FaultAction::CrashProcess`
   (discarding unflushed journal staging), `FaultAction::DuplicateDelivery`,
   `FaultAction::DelayMillis`, and `FaultAction::ExpireLease`.
2. `BoundaryPhase::AfterEffect`: evaluated after the durable mutation succeeds
   but before the caller receives the acknowledgement. Supports
   `FaultAction::LoseAck` (returning `ErrorCode::DurabilityUnknown`,
   `ErrorCode::TransportUnknown`, or `ErrorCode::AdapterUnknown` after the effect
   has committed), `FaultAction::CrashProcess`, `FaultAction::DelayMillis`, and
   `FaultAction::ExpireLease`.

## Bounded state-machine exploration and invariants (#84)

`StateExplorer` explores single-boundary crash/lost-ack sweeps and seeded
multi-action schedules (`SimSchedule`) across six protocol workloads
(`SimWorkload::canonical_suite()`):

- `SimWorkload::TwoPhaseCommit`: commit, participant prepare rejection, and
  pre-decision cancellation, optionally followed by journal compaction and
  reopen.
- `SimWorkload::NativeAtomic`: valid atomic commit and invalid/overflow atomic
  rollback across compaction and restart.
- `SimWorkload::NativeChunked`: multi-chunk batch progress, pause/resume, and
  terminal chunk rejection with preserved prior chunk receipts.
- `SimWorkload::NativeStagedPublish`: generation-0 publish, stale-generation
  conflict, and validation rejection with staged artifact cleanup.
- `SimWorkload::SagaExecution`: forward completion, step-2 rejection with
  reverse compensation, ambiguous forward reconciliation, and `Irreversible`
  policy boundaries requiring operator intervention.
- `SimWorkload::FencedRecovery`: lease expiration and epoch takeover both before
  and after the durable 2PC decision barrier, paired with delayed stale-owner
  journal and participant calls.

Every schedule execution checks six state-machine invariants (`Invariant`):

1. `ImmutableDecision`: a durable 2PC decision or terminal mode/Saga outcome
   never flips across crashes, lost acknowledgements, recovery passes, or
   compaction.
2. `DecisionBarrierPrecedesFinalization`: no participant receives `commit` or
   `abort` before the matching `Decided` record is durably flushed, and
   `Decided(Commit)` never flushes unless all enlisted participants are prepared.
3. `ExactlyOnceEffects`: committed resource counters, chunk deltas, published
   snapshots, and Saga step/compensation receipts apply at most once per
   idempotency key despite duplicate deliveries, lost replies, and message
   reordering.
4. `DurableReceiptConsistency`: converged status after recovery matches durable
   journal watermarks, checkpoints, and resource state with no unresolved
   `Outcome::InDoubt` or orphan staged artifacts.
5. `FencingSafety`: once a higher epoch is acquired on `SimAuthority`, delayed
   journal flushes, compactions, prepares, commits, or aborts carrying a stale
   `SimGrant` are rejected with `ErrorCode::StaleOwner` without mutating state.
6. `PoisonedOnUncertainty`: any journal write/flush error returns
   `Error::InDoubt`, sets `reopen_required`, and blocks further status or
   recovery calls until the handle is dropped and reopened.

## Reproducible failure traces and shrinking (#85)

A `SimSchedule` combines a 64-bit `seed`, a `SimWorkload`, and an ordered vector
of `ScheduleAction`s (`InjectFault`, `AdvanceClock`,
`ReorderParticipantMessages`, `CrashAndReopen`, `ExtraRecoveryPass`,
`CompactJournal`).

- **JSON fixtures**: `SimSchedule::to_json()` and `SimSchedule::from_json()`
  serialize and validate human-readable fixtures (`#[serde(deny_unknown_fields)]`)
  for regression tests.
- **Deterministic replay**: re-running `StateExplorer::run_schedule` with the
  same `SimSchedule` produces an identical `SimTrace` (ordered `TraceEvent`s,
  `SimOutcomeSummary`, and `InvariantViolation`).
- **Delta-debugging shrinker**: when `StateExplorer::explore` or a custom test
  detects a violation, `StateExplorer::shrink_violation` (or `shrink_with`)
  reduces the action sequence via chunk removal, single-action elimination, and
  fault parameter simplification (reducing `DuplicateDelivery` counts and
  `DelayMillis` values) while preserving the failure. `ShrinkResult` and
  `SimTrace` implement `Display` for human-readable diagnostics.

## SQLite and child-process crash cross-checking (#86)

`crates/pactum-demo/tests/simulation.rs` and
`crates/pactum-distributed/tests/distributed.rs` cross-check representative
`SimSchedule` fixtures against real SQLite adapters and OS child-process kills:

- `ControlledSqliteJournal`, `ControlledSqliteParticipant`, and
  `ControlledSqliteBackend` wrap `SqliteJournal`, `DurableCounter`, and
  `SqliteBackend` with `SimController::enter_boundary`, sharing the exact step
  numbering and `FaultRule` evaluation of the in-memory simulator.
- When `FaultAction::CrashProcess` triggers in child-process mode
  (`simulation_crash_child`), the child process signals readiness at that exact
  boundary step and parks while the parent terminates it with `Child::kill`,
  skipping all Rust destructors and relying on SQLite WAL rollback/recovery on
  reopen.
- After recovery in the parent process, assertions verify that SQLite and the
  simulator converge to identical 2PC decisions, outcomes, native `Progress`
  states, committed counter/snapshot values, durable sequences, and fencing
  epochs.

## Model vs. hardware failure limitations

The deterministic simulator and the SQLite cross-check suite have distinct,
complementary boundaries:

- **What the model verifies**: protocol state transitions, durability barrier
  ordering, idempotency key and token bindings, duplicate/reordered message
  handling, handle poisoning on ambiguous writes, checkpoint/compaction state
  retention, and epoch fencing across coordinator failover.
- **What requires SQLite / child-process tests**: SQL phrasing, SQLite WAL and
  `synchronous=FULL` transaction rollback on abrupt process death (`SIGKILL`),
  OS file descriptor and sidecar lock release, and schema/pragma validation on
  reopen.
- **Outside both model and process-kill tests**: physical storage media
  corruption beyond SHA-256/SQLite checksum detection, storage devices or
  filesystems that acknowledge `fsync` before media persistence (write-cache
  loss on power failure), kernel/hardware memory bit flips, clock drift across
  unsynchronized physical hosts without an external fencing authority, and
  out-of-band manual edits to SQLite database or WAL files.
