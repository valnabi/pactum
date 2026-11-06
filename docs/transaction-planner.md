# Transaction planner

`pactum_core::planner` is an optional, synchronous, side-effect-free planning
layer over the existing `plan::TransactionPlan` and `PlanEngine`. It does not
open a journal, call a backend, prepare participants, commit, schedule runtime
admission, or alter recovery decisions. Planning is deterministic for identical
serialized inputs. Provider estimates and advertised guarantees are contracts
supplied by the host, not facts the planner can verify against a live backend.

## Use and extension

1. Resolve each adapter's dynamic `CapabilityProvider::capabilities()` report.
   Put those reports in `PlanningContext::capabilities`; freeze them for the
   entire planning call. `snapshot` identifies the provider incarnation/version.
2. Build a `TransactionIntent` with a permanent `PlanId`, positive revision,
   adapter identity, explicit `Consistency`, and ordered indivisible `WorkUnit`s
   or existing 2PC enlistments. Never reuse a PlanId for different work.
3. Call `TransactionPlanner::plan(&DefaultTransactionPlanner, &intent, &context)`.
   `ExecutionPlan::explain()` is the dry-run result: selected strategy, actual
   effective budgets, estimates, warnings, and the guarantee boundary.
4. Review or persist the accepted result using serde. When ready, submit
   `accepted.transaction_plan().clone()` to the existing `PlanEngine::execute`.
   Use its existing mode-specific results and recovery service thereafter.

The object-safe `TransactionPlanner: Send + Sync` trait permits third-party
implementations. Hosts can hold `Arc<dyn TransactionPlanner>` directly or register
named implementations in a `PlannerRegistry`. Custom planners choose a strategy
and exclusive cumulative partition ends via `ExecutionPlan::accept`; this public
constructor validates all hard constraints, consistency, capabilities, publication,
and existing DAG bounds. The registry additionally checks that custom results
bind the exact requested intent and frozen context. A custom planner may use a
different partitioning algorithm, but cannot bypass accepted-plan validation.
`crates/pactum-demo/tests/planner.rs` exercises both default and third-party
implementations against the same public contract.

## Strategies and guarantees

| Requirement | Default strategy | Guarantee |
| --- | --- | --- |
| `NativeAtomic` | One Atomic request; refuse if too large | One backend transaction |
| `PartialAllowed` | Atomic when feasible, otherwise Chunked | Individual chunks can remain applied after failure |
| `StagedVisibility` | Ordered hidden staging transactions, then a dependent StagedPublish request | One fenced visibility CAS, only for readers through the pointer |
| `TwoPhaseDecision` | One existing TwoPhase step with unchanged enlistments | Existing durable prepared-participant decision; no global snapshot |

Native atomicity never falls back to chunking or staged visibility. No mode is
inferred from another advertised mode. Staging also requires advertised Atomic
support. A finished DAG is not a distributed ACID transaction. 2PC enlistments
are indivisible and are passed directly to the existing coordinator adapter.

## Adapter payload contract

Native strategies require explicit `planner_envelope_v1` opt-in. Their payloads
are JSON-encoded `PlannerEnvelope` values, not arbitrary concatenations of host
operations. Existing opaque-payload adapters do not automatically implement this
protocol. A host adapter must decode and honor version 1:

- `Apply`: apply the ordered opaque units in one receipt-protected transaction.
- `Stage`: store immutable, hidden units under `(artifact, index)`, using the
  existing permanent receipt binding. Duplicate work must verify identical data.
- `Publish`: validate the exact hidden chunk set and host manifest, then use the
  existing StagedPublish protocol and generation precondition to switch the pointer.
  Refuse missing or failed chunks. Readers must resolve data through that pointer.

The artifact namespace hashes the frozen intent and context. Publication depends
on successful, durable acknowledgements of all staging work through a serial DAG
chain. Hidden artifact storage, validation, retention, cleanup, isolation and
publication correctness remain the adapter's responsibility. Publication manifest
estimates must bound its complete stage/validation/CAS work, including internal
backend overhead beyond the one envelope overhead charged by the planner.
No reader may treat hidden staging writes as live mutations.

## Budgets and estimates

Provider `ResourceCapabilities::budget` constrains every backend unit. Context
budgets constrain the whole operation. Both must hold; a host cannot override a
provider hard limit. All values are inclusive integer bounds. Dimensions are
bytes, operations, duration in milliseconds, memory bytes, concurrency and
provider-defined conflict units. `extensions` preserves opaque provider data in
the frozen fingerprint; providers must translate any enforceable extension
constraints into these budgets or implement additional host validation.

Hard limits reserve `safety_margin_bps` (0 through 10000) with integer floor
rounding, using overflow-safe arithmetic. Missing estimates are unknown; a hard
bound on an unknown cost fails closed. Soft bounds prefer smaller partitions
and emit `soft_budget:<scope>:<dimension>` warnings for unknown or excessive
costs. Unknown soft estimates alone do not force splits. An indivisible unit
may exceed a soft target. It may never exceed a hard limit. The default chooses
maximal contiguous partitions with a linear pass over the units; custom planners
can choose different feasible partitions.

Provider estimates include logical/external backend data, operations, time and
conflict footprint, excluding the planner envelope. Serialized envelope bytes
are added to known byte and memory estimates, conservatively accounting for
request/receipt storage and buffers alongside backend work. JSON expansion and
fixed provider overhead therefore count. An omitted
byte estimate remains unknown, since the payload may reference external data.
Omitted overhead dimensions declare zero additional overhead; they do not make
unknown workload estimates known. Overhead is added once per backend unit.
Memory sums within each transaction and peaks across sequential transactions;
concurrency peaks, with at least one active unit. Other costs sum. These are
conservative estimates, not measured durations or runtime enforcement. Runtime
scheduler/admission policy remains separate; dependency ordering keeps generated
staging work sequential regardless of runner concurrency.

## Accepted identity and persistence

`ExecutionPlan` exposes immutable borrows. Its version 1 serde representation is
an accepted recipe containing intent, frozen context, strategy and partition ends.
Deserialization checks the version, rebuilds the DAG, and reruns validation;
it cannot create an accepted value with an unchecked hard-budget overrun.
`fingerprint()` hashes the entire recipe with SHA-256. Generated step identities
also bind that hash, so changed budgets, capabilities, payloads, partitions or
consistency change the existing DAG fingerprint and receipt keys. The existing
engine refuses resubmission of those changes under an already bound PlanId.
Export `transaction_plan()` separately when consumers need the executable DAG.

The existing version 1 DAG format and journal records are unchanged. The existing
1024-step/16 MiB serialized DAG bounds still apply. This planner conservatively
limits a recipe to 1023 work partitions (plus publication). Oversized payloads
must use host-defined immutable external artifact references with complete logical
cost estimates; the planner never uploads or dereferences them. Accepted plans
are never adaptively mutated. Recovery executes the bound DAG; changed live
capabilities require host handling and any future replanning needs explicit
journaled authorization under a new identity.

## Validation and FoundationDB-style fixture

The planner tests include 100,000 immutable record references representing
100,000,000 logical bytes, provider-reported 10 MB / 5 second / conflict limits,
and a 10% safety reserve. They verify stable staging partitions and a final
publication without claiming native ACID for the whole dataset. Smaller durable
SQLite fixtures test unknown chunk commit, unknown publication, failed chunk
blocking across restart, generation conflict, and permanent input bindings.

There is no FoundationDB adapter or runtime in this workspace. The fixture uses
backend-neutral reports; it introduces no FDB types or dependencies into core.
A future optional #24 adapter can report its installed provider limits and opt
into the envelope contract after implementing its hidden generation storage,
receipt and CAS guarantees. Live FDB validation remains conditional on that
adapter and an installed runtime.

Run locally: `cargo fmt --all -- --check`, `cargo lint`, and `cargo test-all`.

Optional [isolation contracts](isolation.md) negotiate per-participant guarantees and conflict evidence over accepted DAG output, bind frozen reports into execution identity, and retain the existing mode and recovery boundaries.
