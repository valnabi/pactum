# Isolation contracts and optimistic validation

Epic #69 (#79–#82) adds `pactum_core::isolation`, an optional layer over the
existing planner, DAG runner, coordinator and recovery service. Atomicity and
isolation are separate contracts. None of the original modes, constructors,
Participant/Backend/StepAdapter methods, accepted planner recipes or journal
formats change. Existing adapters acquire no isolation capabilities implicitly.
There are no Aquanet or ContentGraph dependencies.

## Guarantees and boundaries

| Contract | Meaning | Does not establish |
| --- | --- | --- |
| Unspecified | Original mode/participant behavior | Additional anomaly prevention |
| ReadCommitted | No dirty reads in the resource transaction | Repeatable reads, predicate protection |
| Snapshot | One participant transaction's snapshot | Write-skew prevention or a shared timestamp |
| Serializable | Serial execution equivalence within one participant | A cross-resource serial order |
| Reads validation | Compare key/absence versions at the resource barrier | Predicate completeness or a global snapshot |
| Writes validation | Compare expected write versions at that barrier | Protection for unreported writes |
| Predicates validation | Compare provider range/query tokens including insertions/deletions | Automatic extraction of predicates from opaque payloads |

Levels are explicit protocol capabilities, not an ordered ladder. Core never
promotes a report to another level. Every requested validation kind and fencing
requirement must be advertised. VersionStamp is a nonempty opaque provider token
(up to 4096 bytes), not a globally comparable timestamp. Absence must have an
explicit provider token. `None` in a write footprint explicitly requests a blind
write; acceptance requires `blind_writes` support. Blind writes do not imply
lost-update protection.

`IsolationScope::GlobalSnapshot` and `GlobalSerializable` always fail closed,
including when every individual participant advertises Serializable. Local
scheduler resource scopes are process-local exclusion, not distributed locks.
2PC supplies its existing durable decision; it never becomes serializable merely
because optimistic checks run before commit. Snapshot and footprint completeness
are adapter contracts which core cannot verify against opaque application data.

Mode boundaries remain unchanged:

- Atomic: one backend transaction, including validation, effects and receipt.
- Chunked: each chunk has its own transaction and snapshot. A single frozen
  footprint cannot safely represent all chunks; validation-bearing Chunked
  requests are refused. Use the existing planner to partition into individual
  Atomic DAG steps and attach a separate footprint to each. This is still partial
  execution, without operation-wide repeatable reads or serializability.
- StagedPublish: validation must cover the manifest and existing generation CAS
  at publication. Local isolation does not turn hidden writes or pointer readers
  into a shared read snapshot. Stage transactions need their own contracts.
- TwoPhase: contracts name exactly every enlisted participant. Any requested
  isolation level beyond Unspecified, or any validation, requires advertised
  `prepared_protection`. Validate atomically during durable prepare and retain
  reservations/conflict protection until the existing final decision. A separate
  pre-prepare or post-decision version check is insufficient.

## Accept, persist and execute

1. Resolve adapter capabilities and choose explicit per-resource requirements.
   Use stable resource identities (ParticipantId, including incarnation/namespace),
   provider snapshot/configuration versions and complete conflict footprints.
2. Build the existing TransactionPlan, or use
   `planner::ExecutionPlan::transaction_plan().clone()`. Call
   `IsolationPlan::accept(plan, contracts_by_step)`. Coverage is exact: one contract
   per step, exactly one resource for native work, and exactly the enlisted
   participants for 2PC. Unsupported combinations fail before execution.
3. Persist the accepted IsolationPlan using serde alongside the retained plan
   journal. Deserialization revalidates schema version, DAG bounds, reports,
   footprints, negotiation and exact coverage. The total recipe is bounded to
   16 MiB; each footprint has at most 65536 entries. Resource/key/predicate names
   are 1–256 bytes without controls. Foreign/unknown fields are refused.
4. Implement the opt-in `IsolationExecutor` extension on an adapter which can
   enforce its contract inside the resource transaction or prepare boundary.
   Register executors by the ORIGINAL adapter IDs in `IsolationPlan::adapters`.
   Existing NativeAdapter and TwoPhaseAdapter cannot be automatically wrapped
   with a version check outside their effect barriers.
5. Pass the returned adapters to the ordinary PlanEngine::open, then execute
   `isolated.transaction_plan().clone()`. The existing scheduler, journal
   Attempt/Report barriers, cancellation, timeout and retry logic execute it.
   RecoveryTarget and recovery-service operator controls remain usable unchanged.

Each generated adapter is bound to one step's work and full isolation recipe.
Its ID hashes the canonical recipe and step identity. The journal retains these
IDs and the underlying durable resource identity plus contract hash. Consequently
requirements, evidence, snapshot versions, work and planner fingerprints affect
both accepted DAG identity and permanent receipt keys. Reordering steps or
2PC enlistments does not change identity. Any changed input under a bound PlanId
is refused. Use a new PlanId for new work, never silently replan a pending key.

Generated adapter IDs are exposed in `transaction_plan().steps`. Configure any
per-adapter scheduler budgets using those IDs: a generated adapter is per step,
so use the existing global/plan limit and shared resource scopes for aggregate
local resource concurrency limits. Dependencies and explicit resource names are
preserved. A contract is not enforcement if a host deliberately registers an
unrelated StepAdapter under one of these IDs; adapter compliance is part of the
existing trusted host boundary.

Keep the accepted recipe, journals, original adapter/coordinator incarnations,
participant state and receipts for the lifetime of retry keys. On restart,
deserialize the recipe, rebuild adapters with it, open the existing PlanEngine,
and call resume/recover_pending. Opening only replays. Missing executors or
changed durable resource identities fail closed. There is no additional journal,
commit authority, rollback protocol or parallel executor.

## Validation, uncertainty and decision barriers

The wrapper first asks `IsolationExecutor::reconcile` to resolve the SAME permanent
key, work and contract. This must inspect authoritative retained receipts or the
existing coordinator state. It returns Some for known terminal or unresolved
protocol progress. None is allowed only if the key has never been admitted and
no delayed effects are possible. Lookup failure or uncertainty must return an
error, never None. Receipts must bind the complete work AND isolation contract.

Only after reconciliation proves fresh work does core compare live reports to
the exact frozen reports. It then calls execute_validated. The adapter MUST
repeat that comparison and check authority, footprint completeness, versions and
predicates atomically with effects and the permanent receipt (or during durable
prepare). The initial core comparison does not close a validation-to-write race.
No detached effect may survive dropped calls outside the resource's fencing and
idempotency contract. Sync resource operations must be bounded; normal plan
cancellation and cooperative timeout semantics still apply.

A committed receipt wins over current version conflicts, capability drift or an
expired ownership epoch. A retained rejection remains rejected even if versions
later match. Unknown completion requires reconciliation, not a new key or an
inferred abort. An interrupted undecided 2PC operation follows existing recovery
and abort rules; a durable Commit may only be redelivered, never revalidated into
Abort. Fencing must reject stale NEW writes, while finalization follows the
existing fenced authority protocol. Core cannot renew authority.

`IsolationFailure` exposes typed adapter errors without including payloads:
Conflict and CapabilityChanged map to InvalidInput, Fenced to StaleOwner, and
Unknown to conservative uncertain completion. Errors are not commit outcomes.
Plan Reports retain the existing operational blocks/retry guidance; a validation
conflict may instead return the original mode's definitive aborted/rejected
result after atomically retaining its rejection receipt. Operator retry cannot
change frozen work, bypass fencing or erase a durable decision.

## Regression evidence

`crates/pactum-demo/tests/isolation.rs` exercises public APIs and a durable SQLite
validation fixture. SQLite serializes compare/effect/receipt in one immediate
transaction; it is a test adapter, not a new public backend implementation.

| Case | Expected evidence |
| --- | --- |
| Lost update | Second expected write version rejects |
| Write skew | Changed observed peer version rejects |
| Phantom/absent key | Predicate generation or absence token changes reject |
| Independent runners | Resource barrier admits one competing write; local scopes cannot protect them |
| Snapshot without read validation | Both disjoint writes may succeed; no global invariant is claimed |
| Mixed participants | Exact reports and requested levels required; missing prepare protection rejects |
| Capability mismatch/race | No effect, including change between preflight and resource barrier |
| Stale fence | StaleOwner and zero new effects |
| Unknown lookup | No fresh dispatch |
| Lost effect acknowledgement/restart | Same retained receipt resolves despite stale versions/reports |
| Duplicate delivery/conflict | Effect once; permanent binding and rejection survive retries |
| 2PC unknown acknowledgement/restart | Existing coordinator Commit remains final after capability drift |
| Process death | Every before/after plan append/flush and after effect/receipt commit recovers once, with default and priority schedulers |

The existing protocol suites continue to cover internal native/2PC/distributed
crash, prepare, decision and fencing boundaries. Process exit is not a simulation
of power loss on every filesystem. No production adapter gains new backend
isolation or distributed concurrency control merely by enabling these types.

Run the workspace build, fmt, clippy, test and Rust API documentation commands
listed in README.md. No hosted CI or live external provider is required.

Reads performed by the host before the resource transaction are outside its advertised isolation level. Include their versions/predicates in complete validation evidence when work depends on them; declaring Serializable alone cannot protect an earlier unreported observation.
