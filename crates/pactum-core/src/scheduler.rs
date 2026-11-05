//! Process-local dispatch policy for the existing durable DAG executor.
//! Policies choose only from engine-validated candidates and never own outcomes.
use crate::Error;
use crate::plan::{AdapterId, PlanId, Step, StepId};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

/// One dependency-ready, retry-eligible, resource-safe dispatch candidate.
#[derive(Debug)]
pub struct ReadyStep<'a> {
    /// Immutable specification, including host-assigned resource scopes.
    pub step: &'a Step,
    /// Durable attempts, including uncertain attempts needing reconciliation.
    pub attempts: u64,
    /// Eligible selection rounds spent waiting during this process lifetime.
    pub wait_rounds: u64,
    /// Local wall-clock time since first eligible observation, reset on dispatch.
    pub wait_duration: Duration,
}
/// Read-only runtime signals. Counters are local and never replayed as decisions.
#[derive(Clone, Debug, Default)]
pub struct SchedulerMetrics {
    /// Successfully bound new plans.
    pub admitted: u64,
    /// Plans rejected by the queue budget before journal binding.
    pub rejected: u64,
    /// Durable attempts dispatched in this process.
    pub dispatched: u64,
    /// Policy deferrals (quota/rate-limit hooks may defer until a later pass).
    pub deferred: u64,
    /// Failed, panicking or invalid policy responses.
    pub policy_errors: u64,
    /// Reports acknowledged in this process.
    pub completed: u64,
    /// Acknowledged uncertain or operational reports.
    pub uncertain: u64,
    /// Largest eligible selection-round wait observed.
    pub max_wait_rounds: u64,
    /// Largest time since first eligible observation, including time between passes.
    pub max_wait_duration: Duration,
    /// Aggregate time from durable attempt to acknowledged report.
    pub dispatch_elapsed: Duration,
}
/// Reason a nonterminal step was not dispatched at the latest observed boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingReason {
    /// All checks passed; a policy may select this step.
    Eligible,
    /// Waiting for durable prerequisite success.
    Dependencies,
    /// A prerequisite permanently failed.
    FailedDependencies,
    /// Classified failure requires explicit operator authorization.
    OperatorIntervention,
    /// Local pause control is set.
    Paused,
    /// Caller cancellation stopped dispatch.
    Cancelled,
    /// Per-pass dispatch or per-step attempt budget exhausted.
    PassBudget,
    /// Global or plan concurrency exhausted.
    Concurrency,
    /// An adapter is at its concurrency ceiling.
    AdapterConcurrency,
    /// Another active step owns an exclusive scope.
    ResourceBusy,
    /// A resource's per-pass quota is exhausted.
    ResourceQuota,
    /// Policy deferred this otherwise eligible work.
    PolicyDeferred,
    /// A policy returned an error, panicked, or selected an invalid identity.
    PolicyError,
    /// A durable attempt is in flight.
    Active,
}
/// Snapshot of the last pass, bounded by the immutable plan's 1024-step limit.
/// It is advisory local state; use PlanStatus for authoritative durable outcomes.
#[derive(Clone, Debug)]
pub struct SchedulingSnapshot {
    /// Plan observed in the most recent pass.
    pub plan: PlanId,
    /// Nonterminal step identities and their latest pending reasons.
    pub pending: BTreeMap<StepId, PendingReason>,
}
/// Immutable context for one bounded, synchronous policy call.
pub struct SchedulingContext<'a> {
    /// Plan currently being reconciled; plans are visited serially by the runner.
    pub plan: &'a PlanId,
    /// Only these identities may be dispatched. At most 1024 candidates.
    pub ready: &'a [ReadyStep<'a>],
    /// In-flight steps in this pass.
    pub active: usize,
    /// Remaining dispatch budget for this pass.
    pub remaining_dispatches: usize,
    /// Optional adaptive input; a policy can ignore these local signals.
    pub metrics: &'a SchedulerMetrics,
}
/// A dispatch preference, never a transaction decision or permanent step outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchedulingDecision {
    /// Select one identity from the supplied candidates.
    Dispatch(StepId),
    /// Stop dispatching until an active result arrives or the host resumes later.
    Defer,
}
/// Pluggable prioritization/admission seam, independent of journals and adapters.
/// Calls must be bounded and synchronous. Errors and unwinding panics fail closed;
/// no fallback dispatch is made. In-flight attempts remain reconcilable on resume.
pub trait TransactionScheduler: Send + Sync {
    /// Choose an eligible step or defer, for example for a host quota/rate limit.
    fn select(&self, context: &SchedulingContext<'_>) -> Result<SchedulingDecision, Error>;
}
/// Alternate name for the dispatch policy boundary.
pub use TransactionScheduler as SchedulingPolicy;

/// Existing behavior: fewest durable attempts, then step identity.
#[derive(Debug, Default)]
pub struct DefaultScheduler;
impl TransactionScheduler for DefaultScheduler {
    fn select(&self, context: &SchedulingContext<'_>) -> Result<SchedulingDecision, Error> {
        Ok(context
            .ready
            .iter()
            .min_by_key(|s| (s.attempts, &s.step.id))
            .map_or(SchedulingDecision::Defer, |s| {
                SchedulingDecision::Dispatch(s.step.id.clone())
            }))
    }
}
/// Sample priority policy with deterministic aging. Larger priorities run first.
/// Each waiting round adds one point, eventually overtaking any finite priority.
/// Aging survives bounded passes, but resets on restart; it cannot guarantee
/// progress for a step continuously excluded by dependencies, quotas or scopes.
#[derive(Debug, Default)]
pub struct PriorityFairScheduler {
    /// Host priorities; unspecified steps have priority zero. Plans share these IDs.
    pub priorities: BTreeMap<StepId, i32>,
}
impl TransactionScheduler for PriorityFairScheduler {
    fn select(&self, context: &SchedulingContext<'_>) -> Result<SchedulingDecision, Error> {
        Ok(context
            .ready
            .iter()
            .min_by_key(|s| {
                let score = i128::from(self.priorities.get(&s.step.id).copied().unwrap_or(0))
                    + i128::from(s.wait_rounds);
                (-score, s.attempts, &s.step.id)
            })
            .map_or(SchedulingDecision::Defer, |s| {
                SchedulingDecision::Dispatch(s.step.id.clone())
            }))
    }
}
/// Engine-enforced admission limits, independent of the selected policy.
#[derive(Clone, Debug)]
pub struct SchedulerLimits {
    /// Maximum outstanding steps across bound plans. Rejects new plans before Begin;
    /// existing durable work always remains resumable, including after restart.
    pub pending_steps: usize,
    /// Maximum simultaneous steps of the current plan (also bounded by PlanPolicy).
    pub per_plan_concurrency: usize,
    /// Optional adapter concurrency ceilings; absent adapters use the global bound.
    pub per_adapter_concurrency: BTreeMap<AdapterId, usize>,
    /// Optional maximum dispatches per resource scope per pass. Zero defers a scope.
    /// Exclusive scopes always have concurrency one, regardless of this quota.
    pub resource_dispatch_quota: BTreeMap<String, usize>,
}
impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            pending_steps: usize::MAX,
            per_plan_concurrency: 1024,
            per_adapter_concurrency: BTreeMap::new(),
            resource_dispatch_quota: BTreeMap::new(),
        }
    }
}
/// Cloneable local pause control. Pausing drains active calls; cancellation instead
/// interrupts cooperative calls. Resume control, then invoke the runner again.
#[derive(Clone, Debug, Default)]
pub struct SchedulerControl(Arc<AtomicBool>);
impl SchedulerControl {
    /// Stop new dispatches at the next engine scheduling boundary.
    pub fn pause(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    /// Allow dispatch on a subsequent scheduling boundary/pass.
    pub fn resume(&self) {
        self.0.store(false, Ordering::SeqCst);
    }
    /// Whether new dispatch is currently paused.
    pub fn is_paused(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}
