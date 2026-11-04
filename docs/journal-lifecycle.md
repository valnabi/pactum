# Journal lifecycle

Implements epic #19 and tasks #26–#28. Core and the independent byte journal
remain usable without SQLite, Aquanet or ContentGraph.

## Public API

Call `Coordinator::compact(Retention::Compact)` or
`Engine::compact(Retention::Compact)` while holding the engine's exclusive owner.
The synchronous call installs a checkpoint of the exact last acknowledged durable
sequence. No resource calls occur during compaction. Any error returns
`Error::InDoubt` and poisons the engine: drop and reopen before further status,
execution or recovery. Success preserves the current state and sequence.

`Retention::Compact` discards covered event rows. `Retention::Archive` moves them
into SQLite's archive table in the same transaction. Use
`SqliteJournal::export_archive(&mut visitor)` to stream checked events to an
application-owned export destination. Export callbacks may fail; retrying export
can repeat events, so deduplicate by journal identity and sequence. Archive may
have gaps when earlier generations used Compact. Archive is optional audit data,
never a recovery source. It grows until the application rotates a complete store;
there is no implicit archive expiry or external upload.

Both choices permanently retain every transaction/operation ID and its complete
immutable request, prepare tokens, decisions, acknowledgements, abort tombstones,
and nonterminal progress. Resource/backend receipts must also remain permanent.
There is deliberately no TTL or key-deletion policy: an expiring policy cannot
satisfy Pactum's permanent changed-payload retry rejection contract. Compaction
bounds event history by retained state, not by a constant number of bytes; the
number and size of distinct keys and immutable requests still grow. In particular,
2PC retains a canonical event set for every transaction and participant.

`Journal::checkpoint()` returns the validated active envelope, or None.
`Journal::visit(&mut visitor)` streams only the durable tail after its watermark;
`replay()` collects that same tail for compatibility. A consumer must load the
checkpoint before applying tail entries. Legacy Journal implementations continue
to compile: checkpoint defaults to None, visit defaults to replay, and compact
returns unsupported. Wrappers around a checkpoint-capable backend must forward
checkpoint and tail operations together. Only an override of visit provides
streaming storage; SQLite reads one row at a time using its sequence primary key.

`Coordinator::recover_pending` (async) and `Engine::recover_pending` (synchronous)
visit the deterministic in-memory BTreeSet of nonterminal IDs, rebuilt from
validated state on open and updated after each durable transition. A write clones
only the affected transaction state. Paused batches remain paused. The existing
`recover` API still returns all statuses, including terminal outcomes, and must
enumerate retained keys to do so. No work is dispatched during open.

## Format and validation

SQLite journal schema 2 migrates schema 0/1 transactionally, preserving record
sequences and payload versions. Older binaries reject schema 2. Record versions
remain 1 (2PC) and 2 (multi-mode); journals for the two engines stay separate.

The single active checkpoint has envelope schema 1, generation starting at 1,
watermark, consumer version and opaque payload. SHA-256 covers a domain tag
`pactum-checkpoint-v1`, big-endian u64 generation, big-endian u64 watermark,
big-endian u32 consumer version and payload bytes. A separate head row stores the
last durable sequence and generation, with SHA-256 over `pactum-head-v1` and those
two big-endian u64 values. All metadata and records live in the same SQLite file.

The 2PC payload (consumer version 1) is a JSON array of canonical Begin, Prepared,
Decided and Acknowledged records in transaction-ID order. Restore runs the existing
strict transition validator: commit without all prepare tokens, duplicate bindings,
conflicting decisions and unknown acknowledgements are rejected. The canonical
record count cannot exceed the watermark. No local error is persisted.

The multi-mode payload (consumer version 2) is a strict JSON object with `backend`
(namespace/incarnation identity) and `states`, an array of [ID, request, progress]
triples in ID order. Restore validates identifiers, request constraints, unique
IDs, backend identity, matching mode and reachable progress. Chunk totals must
match immutable requests, completed must be in range, rejected cannot be paused
or uncertain, and completed batches cannot have pending flags. The state count
plus header cannot exceed the watermark. Atomic and publication enum variants
are all reachable within their own modes; restored uncertain phases require
receipt reconciliation, never an invented success or decision.

Open checks SQLite integrity, checkpoint schema/checksum, head checksum and
matching generation. Tail sequences must be contiguous from watermark + 1 through
the durable head; record checksums and engine transitions must validate. A missing
snapshot, missing tail, incompatible version or corrupt payload fails closed.
There is no automatic fallback to archive or partial history. Checksums are for
accidental corruption, not malicious rewriting or rollback of the entire store.

## Atomic cutover and crash boundaries

Under the existing same-path owner lock and WAL/FULL configuration:

1. Require no staged appends and an exact durable watermark.
2. Begin a SQLite transaction and write the next checkpoint generation.
3. Optionally copy covered rows to the archive.
4. Delete covered active rows and update the durable head generation.
5. Commit using SQLite's FULL synchronization barrier, then acknowledge.

The head's sequence does not change during compaction; subsequent appends continue
at watermark + 1. SQLite supplies the atomic generation switch and fsync ordering
through its WAL commit; there is no separate filesystem manifest or rename window.
Before commit, reopen sees the old checkpoint and complete old tail. After commit,
it sees the new checkpoint and remaining tail. Lost acknowledgement requires reopen.
Failed writes poison the journal. Applications must not unlink databases, locks,
WAL files or backend receipts. Freed pages are reused by SQLite; compaction does
not promise immediate filesystem shrinkage or run VACUUM automatically.

Tests exit a real child without destructors before begin, after snapshot insert,
after archive copy, after history removal, before commit and after commit. Both
first and replacement generations, and both retention choices, are exercised.
Existing process-crash matrices checkpoint/reopen every reachable recovered 2PC
and multi-mode prefix before reconciling; corruption and changed-payload retries
are covered separately. Process death tests exercise SQLite recovery, not arbitrary
device power-loss behavior; the documented filesystem/sync assumptions still apply.

## Reproduce benchmarks

Run `cargo run --release -p pactum-demo --example replay_benchmark --locked`.
The CSV reports terminal-key count, raw history growth, full replay/open time,
checkpoint bytes, checkpoint replay/open time, one pending recovery latency and
mean empty pending-index latency over 10,000 calls. Histories of 100, 1,000 and
10,000 terminal operations retain one pending operation. Every run uses fresh
temporary local SQLite stores and verifies that compaction removes the active tail.
Timings include storage open/integrity checks and are machine/cache dependent;
use repeated runs for comparisons. Snapshot bytes intentionally grow with permanent
retry bindings. Benchmarks have no timing assertions and require no hosted CI.

Example single run on Windows with the pinned Rust toolchain (2026-11-04):

| Terminal keys | History records | Full open ms | Snapshot bytes | Snapshot open ms | One pending recovery us |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 202 | 11.732 | 8,549 | 11.118 | 1,553.2 |
| 1,000 | 2,002 | 16.689 | 84,149 | 16.325 | 997.0 |
| 10,000 | 20,002 | 98.233 | 840,149 | 67.240 | 1,527.3 |

Empty pending-index calls averaged 216–313 ns in that run. These are observations,
not latency guarantees; short-history timings can be dominated by storage noise.
