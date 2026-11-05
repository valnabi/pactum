# Pactum

Pactum v0.2 is an independent Rust transaction engine with Atomic, Chunked,
StagedPublish, durable two-phase commit (2PC), and optional Saga execution.
It provides typed mode-specific outcomes, stable idempotency keys and deterministic restart recovery.
The coordinator and participant protocols remain supported; source migrations for
structured errors are documented in the [error-handling guide](docs/error-handling.md).

In 2PC, a commit or abort decision crosses a durable journal barrier before any resource
receives finalization. Successful completion requires durable acknowledgements
from every participant. An unavailable resource or uncertain storage operation
is reported explicitly; recovery never reverses a recorded decision.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `pactum-errors` | Stable error codes, SNAFU source chains, safe context and recovery classification |
| `pactum-core` | Stable IDs, multi-mode execution, 2PC, durable transaction DAGs, Sagas, recovery and cancellation |
| `pactum-journal` | Independent ordered byte journal and explicit durability barrier |
| `pactum-sqlite` | SQLite WAL/FULL journal plus transactional receipt and publish reference backend |
| `pactum-messaging` | Optional backend-neutral outbox/inbox contracts, dispatcher and fake transport |
| `pactum-distributed` | Opt-in fenced 2PC ownership, durable reference authority and remote participant protocol |
| `pactum-demo` | Durable 2PC counters, mode and Saga examples, and crash tests |

Core depends on the journal contract and shared error vocabulary, not SQLite or
the demo. Journal can be used without the engine and has no database or async
runtime dependency. There are no Aquanet or ContentGraph dependencies. Application adapters belong to their consumers.
A facade crate is deferred until it adds value.

## Structured errors

The [error-handling guide](docs/error-handling.md) documents stable codes,
phase-aware retry and decision guidance, source-preserving diagnostics, safe
tracing, operator controls and the remote protocol v2 migration. A failed call
never replaces the authoritative protocol outcome or reverses a durable decision.

## Multi-mode execution

See the [mode selection and API guide](docs/execution-modes.md) and
[ADR 0003](docs/adr/0003-multi-mode-execution.md) for guarantees and recovery.
Run all four protocols in one sample (safe to repeat with the same directory):

```sh
cargo run -p pactum-demo --example multi_mode -- ./all-modes-data
```

The sample commits a local atomic update, resumes a three-chunk job after
closing/reopening storage, publishes a snapshot with a fenced visible-version
switch, and commits the existing 2PC operation. Native counter total is `17`;
published snapshot value is `42`; 2PC counters are `10` and `20`.
Atomic, Chunked and StagedPublish also have standalone examples.

## Durable transaction plans

The [transaction plan guide](docs/transaction-plans.md) describes versioned DAGs,
immutable payload bindings, bounded concurrent scheduling, exclusive resource scopes
and durable restart reconciliation. Steps invoke Atomic, Chunked, StagedPublish or
2PC with explicit per-step guarantees. A completed plan means every step succeeded;
plans do not provide global atomicity or rollback previously completed effects.

```sh
cargo run -p pactum-demo --example transaction_plan -- ./transaction-plan-data
```

## Optional transaction planning

The [transaction planner guide](docs/transaction-planner.md) describes the public
`TransactionPlanner` trait, default and custom planner selection, frozen provider
capabilities, hard/soft budgets, deterministic partitioning, immutable accepted
plans and dry-run inspection. Plans reuse the existing DAG engine and retain
explicit native atomic, partial chunk, staged visibility or 2PC guarantees.

## Sagas for non-2PC resources

The optional [Saga engine](docs/sagas.md) runs immutable forward operations and
explicit compensations, with durable retry keys, reverse dependency ordering
and restart recovery. Compensation is a business operation, not atomic rollback.
Uncertain effects are reconciled before compensation; permanent failures and
irreversible boundaries remain visible as manual intervention requirements.

```sh
cargo run -p pactum-demo --example saga -- ./saga-data
```

The example demonstrates completion, successful compensation, permanent
compensator failure and an irreversible boundary with retained effects.

## Reliable integration events

The optional [outbox/inbox guide](docs/messaging.md) covers native transaction
boundaries, leased at-least-once publication, bounded retry/dead letters,
transactionally deduplicated handlers and per-key ordering. No broker is required
for the reference producer/consumer demonstration:

```sh
cargo run -p pactum-demo --example messaging -- ./messaging-data
```

## Run the 2PC demonstration

```sh
cargo run -p pactum-demo -- demo ./demo-data
cargo run -p pactum-demo -- recover ./demo-data
cargo run -p pactum-demo -- demo ./demo-data
```

The first command commits `+10` and `+20` on independent durable resources, then
aborts another transaction when one resource rejects a negative delta. Values
are `a=10, b=20`. Repeating the command reuses the same transaction keys and
leaves those values unchanged. `recover` only reconciles existing work.
Use a fresh directory for a fresh demonstration.

## Using the libraries

Create a journal with `SqliteJournal::open(path)` and register durable
`Arc<dyn Participant>` resources with `Coordinator::open`. Run `recover` at startup,
then `execute(transaction_id, enlistments, cancellation)`. The executable in
[main.rs](crates/pactum-demo/src/main.rs) is a complete usage example.

Transaction IDs are caller-assigned global idempotency keys, permanently bound
to the same sorted set of participant IDs and payloads. Reusing a key with
changed work is an error. Every participant must retain a stable identity and
resource namespace across restarts.

Inspect `Status` for `outcome`, durable `decision`, `phase`, `pending` identities,
and the latest local error. `Outcome::InDoubt` means delivery or the decision
is unresolved. `Error::InDoubt` means a journal write may have persisted: drop the
coordinator and journal, reopen, then recover. Do not issue a compensating abort.
`diagnostics()` exposes durable-record, recovery-pass and pending-work counters.

## Optional distributed 2PC

The [distributed coordination guide](docs/distributed-coordination.md) describes
opt-in lease/epoch authority, atomic fencing, safe shared-journal takeover and
versioned remote participants. The SQLite authority and loopback transport are
single-host reference implementations; production multi-host use requires an
external durable authority and adapters enforcing the documented fencing contract.

```sh
cargo run -p pactum-distributed --example distributed -- ./distributed.db
```

## Failure model and limitations

The default local API assumes one coordinator owns the journal and all transaction decisions.
The SQLite adapter enforces a same-path advisory owner lock, not distributed
leadership fencing. Use local filesystems honoring SQLite locking and sync,
retain journal and resource databases, and never modify them behind the engine.

Participants must durably prepare, atomically persist effects and commit state,
retain abort tombstones, handle duplicate and delayed calls, and remain available
for eventual reconciliation. A prepared participant must not time itself out.
A coordinator journal cannot make a nontransactional API atomic. Cross-resource
isolation is limited to participant guarantees; there is no global read snapshot.

Storage uses WAL and synchronous=FULL, verified on open. Physical corruption,
checksum mismatches, sequence gaps, future versions and illegal protocol records
fail closed. This is not protection against malicious database editing or a
storage device that lies about synchronization. Process-kill tests do not emulate
power loss on every filesystem or device.

Journal checkpoints compact or archive covered events while permanently retaining
retry bindings, decisions and pending state. SQLite streams the remaining tail;
`recover_pending` uses a pending-work index. See the
[journal lifecycle guide](docs/journal-lifecycle.md) for APIs, crash guarantees,
retention limits and replay benchmarks. Mode/coordinator recovery visits keys in deterministic ID order. Plan steps use bounded concurrent DAG scheduling. There is
no built-in consensus or active-active decision authority. The optional distributed
crate adds fenced 2PC takeover and a remote transport contract.
An optional [recovery service](docs/recovery-service.md) schedules subsequent
recovery passes with durable backoff, operator controls and observability.
See [architecture](docs/architecture.md) and [fault matrix](docs/fault-matrix.md).

## Development

Rust 1.98.0, edition 2024; rustfmt and Clippy are pinned in `rust-toolchain.toml`.
From the workspace root:

```sh
cargo build --workspace --all-targets --all-features --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --no-deps --locked
```

Plain `cargo fmt --check`, `cargo clippy -- -D warnings`, and `cargo test` also
cover the workspace. Local aliases: `cargo check-all`, `cargo lint`, `cargo test-all`.
The tests terminate child processes around protocol boundaries and reopen the
same SQLite files; no hosted CI is required. `Cargo.lock` pins dependencies.
Unsafe Rust is forbidden. Public APIs are documented. Publishing is disabled;
no project license has been selected yet. Work is tracked by
[v0.1 epic #1](https://github.com/valnabi/pactum/issues/1) and [v0.2 epic #11](https://github.com/valnabi/pactum/issues/11).
