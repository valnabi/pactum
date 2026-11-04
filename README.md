# Pactum

Pactum v0.2 is an independent Rust transaction engine with Atomic, Chunked,
StagedPublish and durable two-phase commit (2PC) execution. It provides typed
mode-specific outcomes, stable idempotency keys and deterministic restart recovery.
The existing v0.1 coordinator and participant API remains supported.

In 2PC, a commit or abort decision crosses a durable journal barrier before any resource
receives finalization. Successful completion requires durable acknowledgements
from every participant. An unavailable resource or uncertain storage operation
is reported explicitly; recovery never reverses a recorded decision.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `pactum-core` | Stable IDs, multi-mode execution, 2PC, recovery, cancellation and typed status |
| `pactum-journal` | Independent ordered byte journal and explicit durability barrier |
| `pactum-sqlite` | SQLite WAL/FULL journal plus transactional receipt and publish reference backend |
| `pactum-demo` | Durable 2PC counters, mode examples and crash tests |

Core depends on the journal contract, not SQLite or the demo. Journal has no
external dependencies and can be used without the engine. There are no Aquanet
or ContentGraph dependencies. Application adapters belong to their consumers.
A facade crate is deferred until it adds value.

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

## Failure model and limitations

This MVP assumes one coordinator owns the journal and all transaction decisions.
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

History and terminal tombstones are retained without compaction. Replay reads
all history into memory. Work is sequential in deterministic ID order. There is
no consensus, active-active failover, remote transport, saga engine or automatic
background recovery scheduler. Applications schedule subsequent recovery passes.
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
