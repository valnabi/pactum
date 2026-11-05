# ADR 0004: Opt-in distributed 2PC ownership and remote resources

Status: accepted. Implements #25 and tasks #47–#50.

## Authority and admission

Distributed coordination uses a separate opt-in crate and a single ownership
scope for an entire 2PC journal. An authority issues strictly increasing epochs
on acquire, including reacquisition by the same owner. Renew never revives an
expired grant. Release invalidates the grant and retains the epoch high-water
mark permanently. Owner names are diagnostic identities, not authentication.
A grant binds authority identity, journal namespace, owner and epoch.

The authority must be durable and linearizable. Its clock alone determines
expiration; clients never decide ownership from their own clocks. A clock jump
may change availability but cannot permit two fenced effects: each effect and
its grant validation serialize with acquire/renew/release. A production external
authority may use consensus/quorums; Pactum implements no consensus. Loss of the
authority quorum must fail closed, including participant finalization.

## Enforceable fencing

Every journal append/flush/checkpoint and participant prepare/commit/abort must
atomically validate the exact current unexpired grant with its effect. A local
preflight lease check, or a participant's remembered maximum epoch alone, is
insufficient: an old request can arrive before the new owner contacts that
participant. Implementations must serialize ownership transfer with effects,
or provide an equivalent fencing barrier before admitting the new owner.
No wrapper converts an arbitrary local journal or Participant into a fenced
resource. Separate traits explicitly require this stronger backend contract.

Takeover opens a fresh fenced journal session only after acquire and replays the
shared durable prefix. It then uses the existing coordinator recovery rules:
undecided work aborts durably; recorded decisions never change. An old owner may
retain a cached decision, but can neither append nor finalize after transfer.
Old staged batches are session-local and cannot flush in the new epoch. Reads
used for replay must share the same authority and durable namespace; stale
replicas and journal copies are unsupported. A lease grant never authorizes
replacing or discarding a journal. Restore authority, journal, resource state
and permanent tombstones together; rollback of an epoch high-water mark is unsafe.

## Reference implementation scope

SQLite ReferenceStore runs as a single-host authority/storage service on a
local filesystem with WAL and FULL synchronization. Separate connections and
processes serialize with BEGIN IMMEDIATE. Grant validation, journal durability
and reference counter effects are in the same SQLite database transaction.
This demonstrates the enforcement contract and crash recovery, not multi-host
SQLite/NFS failover. A server can expose these contracts to remote clients;
production multi-host deployments require a suitable external durable authority
and storage/participant implementations honoring the same atomicity contract.

The reference counter is a durable, idempotent resource, not a generic unsafe
fencing wrapper. Its permanent payload bindings, prepare tokens, terminal state
and counter effects survive service restart. All participants and journal must
report the same authority domain. Legacy local resources cannot be registered
with the distributed constructor. Missing resources remain pending on recovery.
Native modes, plans, Sagas and messaging keep their current local invariants;
this epic opts 2PC into distribution and makes no distributed guarantee for those
other protocols.

## Remote protocol and uncertainty

Versioned requests bind authority grant, resource identity, transaction,
operation and immutable arguments. Request identity hashes this whole binding;
retries preserve it. A new epoch changes request identity while retaining the
transaction's durable idempotency binding. Servers validate resource, version,
identity and deadline before dispatch. Participants still atomically fence effects;
a deadline check cannot cancel a request already executing.

Transport failures, lost replies, timeouts and cancellation mean uncertain
completion, never a negative vote. The coordinator's bounded retry policy retries
identical requests and recovery reconciles durable state. Abort tombstones fence
late prepare. Loopback is a reference transport with before-dispatch partitions
and after-effect reply loss; it is not an authenticated network protocol.
Production adapters must authenticate callers, protect integrity, limit message
sizes and enforce server-side deadlines. A client clock is used only to express
deadlines, never to authorize ownership.

## Proof obligations and validation

Let transfer T serialize against every mutation M at the authority. If M is
before T, its durable result is visible to the new owner's replay. If M is after
T, the old epoch fails validation before its effect. The same argument applies
to participant effects, including delayed finalization; thus an old owner cannot
publish a different decision or effect after transfer. Existing 2PC barriers
ensure the new owner repeats only the durable decision. This proof assumes
honest conforming adapters, permanent transaction identities, non-rollbackable
authority storage and storage that honors synchronization.

Tests cover expired/released/renewed grants, competing connections, stale staged
writes, old-owner finalization, partition/reply loss, immutable remote identities,
undecided and decided takeover, checkpoints and process death/reopen. Existing
workspace suites continue to validate v0.1/v0.2 compatibility. Power loss on
lying storage, malicious clients/adapters and independently restored databases
are outside this failure model. No GitHub-hosted CI is required.
