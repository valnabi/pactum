# ADR 0002: Durable protocol and reference runtime

Status: accepted for v0.1.

Use a sequential single-owner coordinator, versioned strict JSON protocol records,
caller-assigned stable IDs, durable begin/enlistment, prepared tokens, immutable
decisions, and final acknowledgements. A durable decision is distinct from
completion. Missing decisions abort on recovery; recorded decisions never change.
Storage uncertainty requires reopening rather than guessing. No distributed
leadership or fencing is implied.

Use Tokio with object-safe async participant methods, per-call bounded retry,
timeout and cooperative cancellation. Keep generic journal methods synchronous so
append+flush+state publication has no async cancellation boundary. Consumers can
isolate blocking orchestration from latency-sensitive runtime work. Do not hold
resource/application locks in the coordinator.

Use SQLite WAL/FULL with a process-lifetime sidecar owner lock and record checksums.
Journal bytes remain independent of core types. No unsafe sync mode is exposed.
Add one consumer crate, `pactum-demo`, for durable resource fixtures, the executable
and end-to-end fault tests. Core and journal do not depend on that consumer. This
extends ADR 0001's three-library bootstrap without adding a facade or unused layers.

Retain full journal history and resource terminal tombstones. Compaction, schema
migration, coordinator consensus and remote transports require separate designs.
