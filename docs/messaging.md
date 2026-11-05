# Transactional outbox and inbox

Epic [#23](https://github.com/valnabi/pactum/issues/23), tasks #39–#42.
`pactum-messaging` is optional and independent of the core engine, SQLite and
brokers. It defines immutable envelopes, `OutboxStore`, `InboxStore`, `Transport`,
retry policy and a bounded asynchronous `Dispatcher`. `pactum-sqlite::messaging`
provides a reference native database adapter. Existing v0.1/v0.2 APIs are unchanged.
No Aquanet, ContentGraph, broker installation or hosted CI is required.

## Atomicity contract

`SqliteMessaging::produce(message, policy, mutation)` opens an immediate native
SQLite transaction, inserts the outbox record and invokes the mutation with
that same `rusqlite::Transaction`. It commits both or neither. An identical
retry skips the mutation; changed envelope bytes or retry policy fail before
mutation. A commit error may mean the commit succeeded: reopen the database and
retry the same ID and immutable envelope. Do not invent a new ID on uncertainty.

For an application-owned connection, initialize storage with `from_connection`.
The adapter exposes `connection()` for application schema setup and reads. An
application can also call `SqliteMessaging::enqueue(&transaction, ...)` inside
its native transaction after schema initialization, executing the domain mutation
only if enqueue returns true and propagating every error to rollback. Initialize
every writing connection with WAL/FULL; keep all domain/outbox data in that same
database. Do not put the outbox in a separate journal/database transaction.

This is the local resource boundary used by Atomic mode (#14); applications
can use enqueue in their own `Backend::unit` Atomic implementation alongside its
native operation receipt. The existing counter reference backend is unchanged.
No cross-resource atomicity is implied. Staged publication requires an adapter
that atomically promotes the visible domain version and its outbox records;
calling produce after a separate publish transaction leaves a loss window.
No staged-publish integration is claimed by this reference implementation.

Handlers are trusted application code. They must only mutate through the
supplied transaction, propagate database errors, and leave transaction boundaries,
messaging tables and durability pragmas alone. A remote HTTP call or another
database write is outside these guarantees. SQLite does not stop a malicious
callback from issuing raw COMMIT or modifying protocol tables.

## Envelope and retention

`Message` contains a stable application-assigned ID, logical route, positive
payload schema version and up to 16 MiB of opaque bytes. Optional ordering key
and positive signed-64 sequence must be supplied together. Names are 1–256 bytes
with no control characters. Validation happens before any effect. IDs must be
globally stable within an outbox; each inbox scopes identity by consumer name.
Applications sharing a transport must namespace IDs across independent producers.
The adapter compares the entire serialized envelope, including route, sequence
and version: ID reuse for changed work is an error, even after delivery.

Outbox delivered/dead records, inbox receipts/poison records and stream sequence
bindings are retained indefinitely. There is no automatic deletion API. Outbox
inspection uses `delivery(id)` or bounded `deliveries(after_id, limit)` pages;
`inbox_status(consumer,id)` exposes failures and attempts. This retains full
payloads, so plan storage capacity and sensitive data retention accordingly.
Deleting receipts or restoring an older database weakens deduplication; an
application-specific migration must preserve identity bindings and define the
maximum replay horizon before discarding any history. Journal compaction does
not remove these records; messaging lives in the resource database.

## Dispatch, leases and recovery

Run `Dispatcher::dispatch` at startup and periodically when work is due. Each
pass atomically claims at most its batch size (maximum 10,000), commits leases
before sending, then records acceptance or failure after each send. Independent
SQLite connections serialize writers with BEGIN IMMEDIATE and a five-second
busy timeout. Claims increment durable tokens; expired tokens cannot acknowledge
or fail a record after a newer claim. A token can finish after its deadline if
it has not been reclaimed. Leases prevent normal overlap; they cannot prevent a
paused sender from resuming after another worker reclaims its record.

Supply nonnegative Unix milliseconds from reasonably synchronized clocks.
Leases use absolute persisted deadlines. Clock jumps forward may create early
redelivery; jumps backward delay recovery. No distributed clock fencing is
claimed. Use sensible lease lengths relative to batch size, send timeouts and
worker scheduling. Sequential batches may outlive their lease: reduce batches
or increase lease time to reduce duplicate attempts. Every send has a timeout.
Dropping/cancelling a pass leaves claimed work recoverable at lease expiry.

Retry policies are bound at enqueue. Claims, including process deaths before
send, consume the per-cycle attempt budget. Failed sends use capped exponential
backoff. Permanent rejection or budget exhaustion creates a retained dead letter;
an expired final claim is explicitly marked acceptance-unknown. Each pass also
examines a bounded batch of expired exhausted records. Repeated passes eventually
surface all of them. Nothing silently deletes recorded work.

`replay(id, now)` explicitly retries a dead letter with its original envelope,
resetting the cycle budget while keeping the monotonic token. Old acknowledgments
remain fenced after replay. Tokens stop at u32::MAX; exhausted tokens require
operator intervention/migration and cannot be replayed. `DispatchReport` counts
this pass's send successes/failures; inspect durable state to distinguish retries
from dead letters or leases. Storage failures stop the pass and require retry or
reopen/reconciliation; do not turn ambiguous local acknowledgement errors into
a new message identity.

A transport adapter must forward the identity unchanged and define durable
acceptance. Success lets the outbox mark Delivered. Lost acknowledgement,
timeout, uncertain send errors and crashes after acceptance cause redelivery.
Mark an error permanent only for definitive rejection. This is **at-least-once
delivery with bounded automatic retries and explicit dead letters**, conditional
on eventual destination availability and operator replay when budgets exhaust.
Delivered means destination acceptance, not consumer application. External
exactly-once side effects are never promised.

## Consume, ordering and poison messages

`InboxStore::consume` takes a stable consumer name, immutable message, ordering
policy, per-message failure budget and a native transaction handler. SQLite
serializes duplicates and atomically commits the effect and Applied receipt.
An identical Applied envelope returns Duplicate without running the handler;
both results allow acknowledging the broker delivery. A crash before commit
rolls back both; a crash after commit but before ack redelivers safely. A handler
error rolls its changes back to a savepoint and commits only the failure count.
After the budget, Dead retains the poison record; further deliveries skip its
handler. Process death during a handler rolls back the attempt count too: monitor
repeated worker crashes externally, since no successful transaction can record
that failure. `replay_inbox` explicitly resets a poison record for another try.

Unordered consumers process unique arrivals without stream ordering. Strict
consumers require key+sequence and start each key at sequence one. Only the next
contiguous sequence may apply. Gaps and poison messages block later messages on
that key; other keys can progress. OutOfOrder persists identity/sequence binding
but does not charge a poison attempt. A different ID cannot reuse a sequence.
Policy is permanently bound per consumer, failure budget per message. Replay
does not erase successful receipts or rewind watermarks. Restoring projections
requires a new consumer namespace or a coordinated data migration.

Retry, OutOfOrder and Dead are not successful acknowledgements. The broker
adapter/application owns redelivery delay, buffering and operator queue policy.
The store does not poll the broker or automatically drain out-of-order inbox
records. Redeliver them when preceding messages finish. A backend-specific
inbox implementation must offer the same effect/receipt transaction boundary;
dedup records in a separate store cannot make arbitrary handlers atomic.

## Demo and local validation

```sh
cargo run -p pactum-demo --example messaging -- ./messaging-data
cargo test -p pactum-demo --test messaging --locked
```

The demo uses two SQLite databases for producer and consumer, with a volatile
`FakeTransport` between them. It injects acceptance with a lost ack followed by
duplicate delivery; producer and consumer counters both end at one, including
repeated runs with the same directory. The fake's acceptance lasts only in this
process and is not a durable production broker. Its scripted Partition, Drop,
LoseAck, Duplicate and Reject faults model explicit failure cases; Drop returns
failure rather than falsely acknowledging a discarded message.

Integration tests reopen independent connections, race producers/consumers and
claimers, exercise bounded retries, leases, stale acknowledgments, timeout and
cancellation, poison rollback, ordering gaps, replay and immutable identities.
The subprocess matrix kills paused children from the parent without Rust
destructors before producer commit,
after producer commit, before send, inside consumer effects, after durable
acceptance before local ack, inside the local ack transaction, and after local
ack. A separate durable consumer
database represents accepted delivery across sender restarts. Each restart
replays the same identities and verifies exactly one local effect. These are
process-failure tests, not simulations of storage devices lying about sync.

SQLite storage requires a local filesystem honoring WAL locking and FULL sync.
Retain resource files and prevent replacement while workers are active. No
consensus, cross-database transactions or distributed leader election is added.
