# ADR 0001: Minimal workspace with an independent journal

Status: accepted for the bootstrap.

## Context

Pactum needs an independent transaction engine and a journal that other systems
can reuse without importing transaction execution. Epic #1 and bootstrap #2 call
for a small crate split and an initial SQLite adapter.

## Decision

Use a virtual Cargo workspace with three libraries under `crates/`:
`pactum-core`, `pactum-journal`, and `pactum-sqlite`. Core and SQLite depend on
journal; journal depends on neither. Keep participant adapters in consuming
systems. Defer a convenience facade until it provides a useful ergonomic API.

Use edition 2024 and explicitly select resolver 3 at the workspace root. Inherit
package metadata, dependency paths, and lints from that manifest. Pin the verified
development toolchain and keep a shared lockfile. Disable publishing pending API
and license decisions. Add external dependencies and feature flags only when an
implementation needs them; do not select an async runtime during bootstrap.

## Consequences

Journal consumers do not pull in core or SQLite. Core can be tested against other
journal implementations. Three crates establish the necessary seams without
creating empty layers for policy, recovery, or diagnostics. The libraries start
with documentation only; downstream issues must define real contracts and test
their behavior before any durability claims can be made.
