# Distributed ownership and remote participants

`pactum-distributed` opts durable 2PC into shared ownership and fenced remote
resources. Existing `Coordinator`, native modes, plans, Sagas and messaging
retain their current behavior. Read [ADR 0004](adr/0004-distributed-ownership.md)
before implementing an adapter.

## Reference demonstration

```sh
cargo run -p pactum-distributed --example distributed -- ./distributed.db
```

The example acquires a durable epoch, opens shared history, recovers pending
work, and executes through the versioned loopback protocol. It prints
`Committed; counter=7`. Repeating it leaves the value at `7` through permanent
transaction idempotency. A clean run releases its grant; after a process crash,
wait for the service-owned lease to expire before acquiring a new epoch.

The SQLite reference is a single-host authority/storage service. It supports
multiple connections/processes on a local filesystem honoring SQLite locking
and FULL sync. It is not a production multi-host consensus backend or a promise
that SQLite can safely fail over on network storage. External implementations
of the contracts enable multi-host operation without adding consensus to Pactum.

## Contracts and lifecycle

1. `LeaseAuthority::acquire` issues a fresh `Grant` for an entire journal scope.
   A grant binds persisted authority incarnation, namespace, owner and epoch.
   `renew` extends only a live grant; `release` invalidates it permanently.
2. `JournalAuthority::open_journal` opens a fresh fenced session. Shared replay
   includes all prior durable decisions and checkpoints. A journal must validate
   the live grant atomically with append/flush/compaction, including staged batches.
3. `FencedResource` requires the existing participant durability and idempotency
   guarantees plus atomic fencing of every effect against ownership transfer.
   Report the same authority domain as the journal. There is deliberately no
   adapter that declares an arbitrary local `Participant` or journal fenced.
4. `DistributedCoordinator::open` accepts only these contracts. Call `recover`
   before admitting new work. Undecided history aborts durably; decided history
   redelivers exactly that decision. Missing resources remain pending.
5. Renew through the authority while work is active. Renewal is an application
   scheduling concern; the coordinator does not renew automatically. On ownership
   loss or journal error, drop the coordinator, acquire a new grant, and open/replay
   a fresh session. Cached status/diagnostics do not validate current ownership.

Expiry alone never authorizes unsafe resource access. A conforming adapter must
serialize the old owner's effects with ownership transfer. Merely checking a
lease before awaiting a local resource, or remembering the greatest epoch seen
at a participant, leaves a stale-writer race before that participant sees the
new epoch. If the authority/storage cannot provide this enforcement, takeover
must be refused. Applications must authenticate ownership requests in production.

The reference enforces fencing inside `BEGIN IMMEDIATE` transactions shared by
lease rows, durable journal entries and counter operations. Journal sessions use
an exact durable-head comparison so even two sessions sharing a grant cannot
append conflicting prefixes. Epochs survive process death and release. Do not
roll back the database or independently restore ownership, journal and resources.

`DistributedCoordinator` also implements the existing `RecoveryTarget` trait,
so the recovery service can schedule pending transaction retries. Its scheduling
journal remains separate from the protocol journal and holds no decisions;
applications still manage lease renewal and replacement of expired sessions.

## Remote protocol

`Transport::exchange` carries serializable protocol-v2 `Request` and `Response`
envelopes. Responses carry typed `RemoteFailure { code, context }` on failure;
server source text is never serialized. Version-1 peers must be upgraded together.
See the [error contract and migration](error-handling.md).
Requests include protocol version, exact grant, resource identity, transaction,
operation, a stable SHA-256 request identity and an attempt deadline. Identity
binds all immutable arguments; the deadline can change between attempts. A new
epoch changes request identity while the transaction's payload binding remains.
Responses echo version, request identity, resource and transaction; mismatches
are uncertainty. `Endpoint` validates these fields, message size and deadline
before dispatch. The actual resource still fences inside its effect transaction.

`RemoteResource` enforces a cooperative per-attempt timeout. The coordinator's
`RetryPolicy` controls bounded attempts/backoff; identical operations retry with
the same identity. Errors, timeouts, cancellation, expired deadlines and lost
responses never become a definite rejection vote. Prepare uncertainty can lead
the core to record an abort, which is then delivered idempotently. A recorded
commit always remains commit, with delivery pending until reconciliation.
Prepared work never expires itself; abort tombstones reject delayed prepares.

`Loopback` crosses the JSON serialization boundary and supports
`set_partitioned(true)` and `lose_replies(n)`. It demonstrates the protocol and
uncertainty contract without a network dependency. Real transports must provide
authentication, integrity, routing to the stable resource, bounded decoding and
server-side deadlines. Client/server clock skew can affect deadline availability;
only the authority clock determines ownership. An operation already executing
may finish after its deadline, so callers must reconcile uncertain responses.

## Verification and limits

Tests inject competing owners, expiry, stale staged batches, stale finalization
before a new owner contacts a resource, payload/identity changes, partitions,
lost acknowledgements, checkpoints and process death after prepare, decision and
effect. The SQLite reference counter reserves nonnegative i64 capacity during
prepare, retains payload bindings/tokens and abort tombstones, and atomically
persists effects plus terminal state. It is a reference participant, not a generic
transactional wrapper for arbitrary external side effects.

Storage corruption fails closed on journal checksum, sequence/head and protocol
validation. Restore rollback, malicious adapter implementations and storage that
lies about sync are outside the model. The API enforces explicit opt-in contracts;
it cannot prove that an external adapter implements linearizable fencing.
