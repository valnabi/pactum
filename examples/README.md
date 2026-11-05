# Examples

The standalone executable lives in `crates/pactum-demo/src/main.rs` and its durable
reference participant in `crates/pactum-demo/src/lib.rs`.

```sh
cargo run -p pactum-demo -- demo ./demo-data
cargo run -p pactum-demo -- recover ./demo-data
cargo run -p pactum-demo -- demo ./demo-data
```

Commit adds 10 to counter A and 20 to counter B. Abort attempts another addition
but counter B rejects a negative delta. Restart/repetition preserves values 10
and 20 through fixed idempotency keys. A fresh directory starts a new demo.

Applications depend on `pactum-core` and a journal adapter, implement `Participant`
for their own resources, and pass those instances to `Coordinator::open`.
Call `recover` before new work, retain stable IDs and databases, and schedule
recovery again when pending resources become available. Never treat in-doubt as
an abort. See the root README and architecture for the participant obligations.

## v0.2 mode demonstrations

The executable library examples in `crates/pactum-demo/examples` can be run
standalone with persistent data directories:

```sh
cargo run -p pactum-demo --example atomic -- ./atomic-data
cargo run -p pactum-demo --example chunked -- ./chunked-data
cargo run -p pactum-demo --example staged_publish -- ./publish-data
cargo run -p pactum-demo --example multi_mode -- ./all-modes-data
```

See [execution modes](../docs/execution-modes.md) for expected values, lifecycle
usage and backend contracts. The combined example runs all four protocols.

## Saga compensation

```sh
cargo run -p pactum-demo --example saga -- ./saga-data
```

Runs successful work, compensation after rejection, permanent compensator failure
and an irreversible boundary. The retained balance is `50` on repeated runs.
See [Saga contracts and intervention](../docs/sagas.md); compensation is not
atomic rollback. Crash-injected versions are exercised by `tests/saga.rs`.
