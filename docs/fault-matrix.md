# Fault and restart matrix

Run `cargo test --workspace --all-features --locked`. Tests need local SQLite files
and permission to spawn/terminate their own child processes. No network is used.
The matrix is exhaustive over the listed deterministic boundary sites; it is not
a hardware power-failure simulator or a distributed-system model checker.

| Boundary/fault | Verified behavior |
| --- | --- |
| Before/after each append and flush for Begin, both Prepared records, decision, both acknowledgements | 24 injected journal errors poison the live coordinator; reopening resolves the actual durable prefix |
| Flush succeeds, acknowledgement is lost | Durable decision is preserved, never replaced by guessed abort |
| Before/after prepare dispatch and durable vote response | Process kill leaves either unknown or prepared resource; undecided recovery durably aborts all enlisted resources |
| Before/after commit or abort delivery and acknowledgement recording | Repeated delivery changes effects once; every durable decision is preserved |
| Before/after replay and each recovery write/delivery | Killing recovery itself still permits subsequent deterministic reconciliation |
| Prepared/no-decision recovery | Durable abort precedes all finalization, even when every vote was prepared |
| Durable-commit recovery, partially finalized | Commit retained until all participants acknowledge |
| Hanging prepare after durable resource prepare | Bounded retry then abort, including tokenless resolution and late-prepare fencing |
| Hanging finalization | In-doubt result with durable commit and pending identity; later recovery completes |
| Cancellation before prepare, during prepare, after decision | No new prepare when already cancelled; abort before decision, retain commit after decision |
| Dropped execution future | Durable prepared work remains discoverable and resolves through recovery |
| Missing participant registration | Remains pending until the resource is registered; no false success |
| Duplicate execution/delivery, changed payload, wrong token, opposite decision | Effects once; conflicting work and invalid transitions fail |
| SQLite reopen/unflushed staging/second owner | Flushed prefix survives, unflushed transaction rolls back, second owner fails |
| Counter reservation capacity/reopen | Prepared token survives restart; overflow rejects at prepare, not commit |
| Corrupt checksum, future storage/protocol version, sequence gap, malformed/unknown protocol record, illegal transition | Replay fails closed |

`pactum-demo/tests/protocol.rs` launches its own test executable as a child, pauses
at each selected boundary, and calls `Child::kill`. This skips Rust destructors
and reopens the same files. Both the commit path and rejection/abort path are
covered, followed by crashes inside commit recovery and undecided/abort recovery.
After restart, assertions compare the original durable decision, final resource
states, committed counter values, and a second idempotent recovery pass.

SQLite adapter unit tests supply the reusable journal conformance check. Core
unit tests verify all legal commit/abort prefixes and reject impossible histories,
including a second decision. The executable is the compile-checked API usage
example. Local `cargo fmt`, strict Clippy, build, test and rustdoc commands are in
the root README.

The test suite intentionally retains no production fault hooks. Fault wrappers
sit at journal and participant interfaces. SQLite's atomic transactions implement
intra-operation rollback; process kills on unflushed journal transactions exercise
that storage behavior. Device write-cache loss and failures inside every internal
SQLite/VFS instruction are outside this test matrix.

## v0.2 native mode matrix

`crates/pactum-demo/tests/execution.rs` injects errors and real child-process exits
before/after every append, flush and backend protocol call. The test scenarios
cover Atomic, a three-chunk batch, successful publish, stale-generation conflict,
validation rejection and cancellation cleanup. Reopening reconciles receipt gaps
and verifies stable totals, hidden orphan cleanup, no false successful completion
and permanent terminal outcomes. A separate test loses acknowledgements after
actual backend commits and proves status remains indeterminate until recovery;
cancellation never undoes committed native work or a publish CAS.

| Boundary | Durable state / required restart behavior |
| --- | --- |
| Before/after atomic transaction commit | Begin may be indeterminate; replay permanent receipt; never apply delta twice |
| Before/after chunk attempt/checkpoint barriers | Checkpoint prefix is monotonic; uncertain next chunk retries its stable key |
| Between chunks / durable pause | Prior chunks remain visible; recovery honors pause, explicit execute resumes |
| Before/after stage write or validation | Hidden immutable artifact may exist; replay stage/validate safely |
| Before/after publish-intent barrier | No CAS dispatched until Publishing is durable |
| Before/after generation CAS | Read permanent receipt by version, even when visible pointer later advances |
| Stale generation | Conflict receipt persists; newer visible version remains intact |
| Before/after cleanup and abort barrier | Aborting intent resumes idempotent deletion; never delete published data |
| Journal acknowledgement lost | Poison engine; drop/reopen before reading status or dispatching anything |
| Namespace/database incarnation changes | Reject opening the journal against a different backend |
| Schema mismatch, malformed records, impossible transition | Fail closed without dispatching backend work |

`pactum-core` additionally tests legal replay prefixes and rejects impossible
mode transitions, checkpoint jumps, terminal reversals, missing begins and
repeated headers. Existing v0.1 protocol and process-death tests run unchanged.
These process exits model crash boundaries, not arbitrary hardware power loss.
