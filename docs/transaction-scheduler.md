# Transaction scheduling policies

The scheduler implements epic #58 (#64–#68) as a policy seam in the existing
bounded `PlanEngine` dispatcher. It adds no executor, journal format or durable
decision authority. `PlanEngine::open` retains the default ordering: fewest durable
attempts, then step ID. Existing `PlanPolicy` construction and journals remain compatible.

## Configuration

After opening a runner, configure its process-local policy and budgets:

```rust
use pactum_core::plan::{AdapterId, StepId};
use pactum_core::scheduler::{PriorityFairScheduler, SchedulerLimits};
use std::{collections::BTreeMap, sync::Arc};

// engine is an already-open PlanEngine.
engine.configure_scheduler(
    Arc::new(PriorityFairScheduler {
        priorities: BTreeMap::from([(StepId::new("interactive")?, 10)]),
    }),
    SchedulerLimits {
        pending_steps: 256,
        per_plan_concurrency: 4,
        per_adapter_concurrency: BTreeMap::from([(AdapterId::new("database")?, 2)]),
        resource_dispatch_quota: BTreeMap::from([("catalog".into(), 8)]),
    },
)?;
```

`PlanPolicy::concurrency` remains the global ceiling for this runner. A plan's
ceiling is the smaller of that value and `per_plan_concurrency`. Adapter limits
are checked before selection and enforced with owned permits. Exclusive resource
scopes retain concurrency one; independent scopes can run concurrently. Scope
acquisition remains sorted. Global, plan, adapter and scope guards survive dropped
execution futures until their aborted calls are actually destroyed. Permits cover
backoff and adapter execution. Reconfiguration is rejected until all interrupted
task futures are destroyed, including never-polled tasks; let them drain before
replacing policy or budgets.

`pending_steps` counts outstanding steps across bound plans, including dependencies
and uncertain attempts, excluding terminal steps and descendants of definite
failures. An oversized new plan is rejected before its Begin record; retry admission
later with the same identity. Existing durable work always remains resumable,
including after restart with smaller limits. Defaults preserve existing admission
behavior; set a finite limit to bound outstanding work across plans. Candidate sets
remain bounded by the existing 1024-step plan limit, with no separate unbounded
work queue. Retained completed journal history is outside the pending-work budget.

Resource quotas count dispatch attempts per pass, including uncertain attempts.
A step naming several scopes consumes one unit from each. A zero quota defers
that scope while independent work continues. Quotas reset on each explicit pass;
they are local burst budgets, not durable wall-clock rate limits.

## Policy contract and fairness

Implement `TransactionScheduler` (also exported as `SchedulingPolicy`). The
synchronous `select` receives immutable `SchedulingContext`: current plan,
`ReadyStep` candidates, active count, remaining dispatch budget and local metrics.
Return `SchedulingDecision::Dispatch(step_id)` or `Defer`; return an error to
reject the scheduling call explicitly. A host quota/rate-limit hook can retain
synchronized state and defer until a later pass. Policies must complete promptly:
synchronous policy code cannot be timed out or preempted. Unwinding panics are
caught; `panic=abort` still terminates the process. Policies must not perform effects.

The engine constructs eligibility before calling the policy and validates the
returned identity against that set. Dependencies need durable successful Reports;
terminal, active, budget-exhausted, operator-blocked and conflicting scope steps
are excluded. Policies cannot change work, receipt keys, consistency or resource
scopes. Invalid choices, errors and unwinding panics fail closed without fallback.
Outstanding calls dropped on that error retain uncertain durable Attempts; resume
or reopen reconciles them under the existing adapter receipt contract.

`DefaultScheduler` preserves ordering by attempts and ID. `PriorityFairScheduler`
selects largest `priority + wait_rounds`, then fewest attempts and step ID.
Unspecified priorities are zero. Each eligible selection round adds one age point;
dispatch resets age. With continued eligible selection, finite priorities cannot
permanently starve waiting siblings. Aging survives bounded passes in one process
and resets on restart. It cannot override dependencies, scope contention, quotas,
operator blocks or a custom policy that continually defers. Policies operate within
one plan; existing recovery traversal visits plans serially by ID. There is no
cross-runner or cross-plan weighted fairness guarantee.

## Pause, recovery and ownership

Clone `engine.scheduler_control()` before starting execution. `pause()` stops new
dispatches at the next scheduling boundary and drains active calls, preserving
their results. Call `resume()` on the handle and invoke another execution pass.
Cancellation retains its existing behavior: stop dispatch and interrupt cooperative
calls, reporting uncertainty. Neither action reverses decisions or rolls back
effects. Pause is process-local; recovery-service controls provide durable suspension.

Timeouts, adapter panics and uncertain responses retain existing classified Reports,
permanent receipt keys and bounded retry/backoff. Permanent and operator failures
remain excluded until `retry_step` or recovery operator controls authorize retry.
Due retries, durable backoff and reconciliation remain owned by recovery #20.
`recover_pending` and `RecoveryTarget` use the same policy seam, without another
retry scheduler.

All budgets and scope locks are local. They neither acquire leases nor authorize
distributed writes. Fenced adapters must still validate authority at their existing
resource boundaries under #25. Stale-owner failures remain operator blocks. Policies
cannot renew expired authority or turn uncertainty into success. `TwoPhaseAdapter`
wraps the local coordinator; custom remote adapters retain their fencing contract.

## Visibility and verification

`queued_steps()` exposes outstanding queue depth. `scheduler_metrics()` exposes
admissions, capacity rejections, deferrals, policy errors, durable dispatches,
acknowledged reports, uncertainty, maximum waiting-round age, maximum time since
first eligibility and aggregate dispatch-to-report elapsed time. Wait time includes
time between passes; it is neither a durable timestamp nor an SLA.
`scheduling_snapshot()` exposes pending reasons at the last observed boundary:
dependencies, failed ancestors, operator block, pause, cancellation, pass budget,
concurrency, adapter limit, scope conflict/quota, policy deferral/error, eligible or
active. These are advisory process-local signals; `PlanStatus` remains authoritative.
Debug dispatch traces include plan ID, step ID and attempt, without payloads.

Reusable integration contracts run default, priority and external policies through
fan-in, saturation, pass budgets, scope exclusion and smaller restart limits.
Adversarial tests cover invalid/dependency-blocked selection, duplicate dispatch,
policy errors/panics, deferral, aging, rejection before binding, quotas, pause draining
and immediate resume after dropping a pass. Both built-in policies run through
acknowledgement loss and process death at every existing plan journal barrier and
native adapter boundary. Existing native, 2PC and distributed tests verify protocol
internals. Run the documented workspace fmt, Clippy, tests, build and docs checks;
no hosted CI is required.
