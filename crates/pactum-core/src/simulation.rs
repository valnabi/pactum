//! Deterministic simulation, fault barriers, and state-machine verification.
//!
//! This module provides controlled time, simulated journals, participants,
//! native-mode backends, Saga adapters, fenced epoch authorities, bounded
//! interleaving exploration, invariant verification, and shrinkable replay
//! traces over existing Pactum traits without introducing a second execution
//! or scheduling engine.
use crate::{
    Cancellation, Coordinator, Decision, Enlistment, Error, ErrorCode, ErrorContext, Outcome,
    Participant, ParticipantError, ParticipantId, ParticipantStatus, RetryPolicy, TransactionId,
    Vote,
    execution::{
        AtomicRequest, AtomicStatus, Backend, BackendError, ChunkStatus, ChunkedRequest, Engine,
        Execution, ExecutionPolicy, OperationId, OperationStatus, Progress, PublishRequest,
        PublishResult, PublishStatus, Request, UnitResult,
    },
    plan::{AdapterId, StepId},
    recovery::RecoveryTarget,
    saga::{
        ActionResult, Compensation, IrreversiblePolicy, Operation as SagaOperation, Saga,
        SagaAdapter, SagaEngine, SagaError, SagaId, SagaOutcome, SagaPolicy, Step as SagaStep,
    },
};
use async_trait::async_trait;
use pactum_journal::{Checkpoint, Entry, Journal, JournalError, Retention, Sequence};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

fn invalid(detail: impl Into<String>) -> Error {
    Error::invalid(detail.into())
}

fn valid_label(value: &str, name: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(invalid(format!("invalid {name} identifier")));
    }
    Ok(())
}

/// Deterministic SplitMix64 pseudo-random generator for reproducible schedules.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimRng {
    state: u64,
}

impl SimRng {
    /// Create a generator from an explicit seed.
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    /// Generate the next deterministic 64-bit word.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Deterministically pick an index in `0..bound` (returns `0` when `bound <= 1`).
    pub fn next_index(&mut self, bound: usize) -> usize {
        if bound <= 1 {
            0
        } else {
            (self.next_u64() as usize) % bound
        }
    }
}

#[derive(Debug)]
struct ClockInner {
    now_ms: u64,
    fault: Option<ErrorCode>,
}

/// Monotonic simulated clock shared by simulated storage, leases, and messages.
#[derive(Clone, Debug)]
pub struct SimClock {
    inner: Arc<Mutex<ClockInner>>,
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new(1_000)
    }
}

impl SimClock {
    /// Construct a simulated clock starting at `initial_millis`.
    pub fn new(initial_millis: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ClockInner {
                now_ms: initial_millis,
                fault: None,
            })),
        }
    }

    /// Read the simulated timestamp, failing closed if a clock fault is armed.
    pub fn now_millis(&self) -> Result<u64, Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated clock mutex poisoned".into()))?;
        if let Some(code) = guard.fault.take() {
            return Err(Error::operational(crate::Diagnostic::message(
                code,
                "injected simulated clock failure".into(),
            )));
        }
        Ok(guard.now_ms)
    }

    /// Inspect current simulated milliseconds without consuming an armed fault.
    pub fn current_millis(&self) -> u64 {
        self.inner.lock().map(|g| g.now_ms).unwrap_or(0)
    }

    /// Advance the clock by a duration; fails closed on overflow.
    pub fn advance(&self, delta: Duration) -> Result<u64, Error> {
        let millis = u64::try_from(delta.as_millis())
            .map_err(|_| invalid("clock advance duration overflow"))?;
        self.advance_millis(millis)
    }

    /// Advance the clock by `delta_millis`; fails closed on overflow.
    pub fn advance_millis(&self, delta_millis: u64) -> Result<u64, Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated clock mutex poisoned".into()))?;
        let next = guard
            .now_ms
            .checked_add(delta_millis)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| invalid("simulated clock overflow"))?;
        guard.now_ms = next;
        Ok(next)
    }

    /// Set the clock to an exact monotonic target timestamp; rejects backwards jumps.
    pub fn set_millis(&self, target_millis: u64) -> Result<u64, Error> {
        if target_millis > i64::MAX as u64 {
            return Err(invalid("simulated clock target exceeds limit"));
        }
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated clock mutex poisoned".into()))?;
        if target_millis < guard.now_ms {
            return Err(invalid("simulated clock cannot move backwards"));
        }
        guard.now_ms = target_millis;
        Ok(guard.now_ms)
    }

    /// Arm or clear a one-shot clock read failure.
    pub fn inject_fault(&self, code: Option<ErrorCode>) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.fault = code;
        }
    }
}

/// Fenced ownership grant issued by [`SimAuthority`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimGrant {
    /// Durable authority namespace.
    pub namespace: String,
    /// Diagnostic owner identifier.
    pub owner: String,
    /// Strictly increasing fencing epoch.
    pub epoch: u64,
}

#[derive(Debug)]
struct AuthorityInner {
    namespace: String,
    epoch: u64,
    owner: Option<String>,
    expires_ms: u64,
    stale_rejections: usize,
}

/// Deterministic single-domain lease and epoch authority backed by [`SimClock`].
#[derive(Clone, Debug)]
pub struct SimAuthority {
    clock: SimClock,
    inner: Arc<Mutex<AuthorityInner>>,
}

impl SimAuthority {
    /// Create a simulated authority bound to `namespace` and `clock`.
    pub fn new(namespace: impl Into<String>, clock: SimClock) -> Result<Self, Error> {
        let namespace = namespace.into();
        valid_label(&namespace, "authority namespace")?;
        Ok(Self {
            clock,
            inner: Arc::new(Mutex::new(AuthorityInner {
                namespace,
                epoch: 0,
                owner: None,
                expires_ms: 0,
                stale_rejections: 0,
            })),
        })
    }

    /// Durable namespace for this authority.
    pub fn namespace(&self) -> String {
        self.inner
            .lock()
            .map(|g| g.namespace.clone())
            .unwrap_or_default()
    }

    /// Acquire a fresh epoch when no unexpired owner holds the scope.
    pub fn acquire(&self, owner: &str, ttl_millis: u64) -> Result<SimGrant, Error> {
        valid_label(owner, "owner")?;
        if ttl_millis == 0 {
            return Err(invalid("lease TTL must be positive"));
        }
        let now = self.clock.now_millis()?;
        let expires_ms = now
            .checked_add(ttl_millis)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| invalid("lease expiration overflow"))?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated authority poisoned".into()))?;
        if guard.owner.is_some() && now < guard.expires_ms {
            return Err(Error::operational(crate::Diagnostic::message(
                ErrorCode::OwnershipBusy,
                "simulated scope already owned".into(),
            )));
        }
        let epoch = guard
            .epoch
            .checked_add(1)
            .ok_or_else(|| invalid("simulated epoch exhausted"))?;
        guard.epoch = epoch;
        guard.owner = Some(owner.into());
        guard.expires_ms = expires_ms;
        Ok(SimGrant {
            namespace: guard.namespace.clone(),
            owner: owner.into(),
            epoch,
        })
    }

    /// Extend a live grant without ever shortening its expiration.
    pub fn renew(&self, grant: &SimGrant, ttl_millis: u64) -> Result<(), Error> {
        if ttl_millis == 0 {
            return Err(invalid("lease TTL must be positive"));
        }
        let now = self.clock.now_millis()?;
        let candidate = now
            .checked_add(ttl_millis)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| invalid("lease expiration overflow"))?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated authority poisoned".into()))?;
        Self::check_grant(&mut guard, now, grant)?;
        guard.expires_ms = guard.expires_ms.max(candidate);
        Ok(())
    }

    /// Release a live grant while preserving the monotonic epoch high-water mark.
    pub fn release(&self, grant: &SimGrant) -> Result<(), Error> {
        let now = self.clock.now_millis()?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated authority poisoned".into()))?;
        Self::check_grant(&mut guard, now, grant)?;
        guard.owner = None;
        guard.expires_ms = 0;
        Ok(())
    }

    /// Validate that `grant` is the live unexpired owner on `clock`.
    pub fn validate(&self, grant: &SimGrant) -> Result<(), Error> {
        let now = self.clock.now_millis()?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated authority poisoned".into()))?;
        Self::check_grant(&mut guard, now, grant)
    }

    fn check_grant(guard: &mut AuthorityInner, now: u64, grant: &SimGrant) -> Result<(), Error> {
        if grant.namespace != guard.namespace
            || grant.epoch != guard.epoch
            || guard.owner.as_deref() != Some(grant.owner.as_str())
            || now >= guard.expires_ms
        {
            guard.stale_rejections += 1;
            return Err(Error::operational(
                crate::Diagnostic::message(
                    ErrorCode::StaleOwner,
                    "stale, released or expired simulated grant".into(),
                )
                .with_context(ErrorContext {
                    epoch: Some(grant.epoch),
                    operation: Some("validate_grant".into()),
                    ..Default::default()
                }),
            ));
        }
        Ok(())
    }

    /// High-water epoch issued by this authority.
    pub fn current_epoch(&self) -> u64 {
        self.inner.lock().map(|g| g.epoch).unwrap_or(0)
    }

    /// Number of operations rejected due to stale or expired grants.
    pub fn stale_rejections(&self) -> usize {
        self.inner.lock().map(|g| g.stale_rejections).unwrap_or(0)
    }
}

/// Component boundary where a trace event or fault rule applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SimComponent {
    /// Journal append, flush, replay, checkpoint or compact boundary.
    Journal,
    /// Two-phase commit participant prepare, commit, abort or status boundary.
    Participant,
    /// Native multi-mode backend unit, stage, validate, publish or cleanup boundary.
    Backend,
    /// Saga adapter forward or compensation execution boundary.
    SagaAdapter,
    /// Lease/fencing authority boundary.
    Authority,
    /// Simulation harness control action (crash, recovery, clock or compaction).
    Harness,
}

impl fmt::Display for SimComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Journal => "journal",
            Self::Participant => "participant",
            Self::Backend => "backend",
            Self::SagaAdapter => "saga_adapter",
            Self::Authority => "authority",
            Self::Harness => "harness",
        };
        f.write_str(name)
    }
}

/// Whether a boundary occurs before durable state mutation or after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum BoundaryPhase {
    /// Before applying the durable effect or storage mutation.
    BeforeEffect,
    /// After the durable effect succeeded, before returning acknowledgement.
    AfterEffect,
}

impl fmt::Display for BoundaryPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeEffect => f.write_str("before_effect"),
            Self::AfterEffect => f.write_str("after_effect"),
        }
    }
}

/// Deterministic fault or message perturbation injected at a boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum FaultAction {
    /// Allow the operation to proceed without error injection.
    Pass,
    /// Fail before the durable effect is applied.
    FailBeforeEffect {
        /// Machine-readable error code returned to the caller.
        code: ErrorCode,
    },
    /// Apply the durable effect, then lose the acknowledgement with `code`.
    LoseAck {
        /// Machine-readable error code returned after persistence.
        code: ErrorCode,
    },
    /// Simulate abrupt process death: discard unflushed staging and poison live handles.
    CrashProcess,
    /// Deliver an idempotent adapter/participant call multiple times before returning.
    DuplicateDelivery {
        /// Additional deliveries (`1..=16`) beyond the primary call.
        extra_deliveries: u32,
    },
    /// Advance [`SimClock`] at the boundary to model message or I/O latency.
    DelayMillis {
        /// Milliseconds to advance.
        millis: u64,
    },
    /// Advance [`SimClock`] past lease expiry and acquire a new epoch mid-flight.
    ExpireLease {
        /// Milliseconds to advance the authority clock.
        advance_millis: u64,
        /// New owner acquiring the next epoch.
        takeover_owner: String,
        /// Lease TTL for the takeover grant.
        ttl_millis: u64,
    },
}

impl FaultAction {
    /// Validate bounds on fault parameters.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Pass | Self::CrashProcess => Ok(()),
            Self::FailBeforeEffect { .. } | Self::LoseAck { .. } => Ok(()),
            Self::DuplicateDelivery { extra_deliveries } => {
                if *extra_deliveries == 0 || *extra_deliveries > 16 {
                    return Err(invalid("duplicate delivery count must be in 1..=16"));
                }
                Ok(())
            }
            Self::DelayMillis { millis } => {
                if *millis == 0 || *millis > 86_400_000 {
                    return Err(invalid("delay milliseconds must be in 1..=86_400_000"));
                }
                Ok(())
            }
            Self::ExpireLease {
                advance_millis,
                takeover_owner,
                ttl_millis,
            } => {
                valid_label(takeover_owner, "takeover owner")?;
                if *advance_millis == 0 || *ttl_millis == 0 {
                    return Err(invalid("lease expiry advance and TTL must be positive"));
                }
                Ok(())
            }
        }
    }
}

/// Rule matching a specific boundary step or component operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultRule {
    /// 1-based boundary step index, or `0` to match the next matching component/operation.
    pub step: usize,
    /// Optional component filter.
    pub component: Option<SimComponent>,
    /// Optional operation name filter (`append`, `flush`, `prepare`, `commit`, etc.).
    pub operation: Option<String>,
    /// Boundary phase where the rule fires.
    pub phase: BoundaryPhase,
    /// Fault action to execute.
    pub action: FaultAction,
}

impl FaultRule {
    /// Validate rule fields and nested action bounds.
    pub fn validate(&self) -> Result<(), Error> {
        if let Some(op) = &self.operation {
            valid_label(op, "operation filter")?;
        }
        if matches!(self.phase, BoundaryPhase::AfterEffect)
            && matches!(self.action, FaultAction::FailBeforeEffect { .. })
        {
            return Err(invalid(
                "FailBeforeEffect cannot be scheduled at AfterEffect phase",
            ));
        }
        if matches!(self.phase, BoundaryPhase::BeforeEffect)
            && matches!(self.action, FaultAction::LoseAck { .. })
        {
            return Err(invalid("LoseAck cannot be scheduled at BeforeEffect phase"));
        }
        self.action.validate()
    }
}

/// Recorded execution event in a deterministic simulation trace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceEvent {
    /// Monotonic 1-based boundary step counter.
    pub step: usize,
    /// Simulated clock timestamp in milliseconds.
    pub time_ms: u64,
    /// Component that emitted the event.
    pub component: SimComponent,
    /// Target identifier (journal namespace, participant ID, backend or saga ID).
    pub target: String,
    /// Operation name.
    pub operation: String,
    /// Boundary phase.
    pub phase: BoundaryPhase,
    /// Injected fault action, if one fired at this step.
    pub fault: Option<FaultAction>,
    /// Deterministic summary of the outcome (`ok`, error code, vote, etc.).
    pub outcome: String,
    /// Last acknowledged durable journal sequence at this point.
    pub durable_sequence: u64,
    /// Active fencing epoch if applicable.
    pub epoch: Option<u64>,
}

impl fmt::Display for TraceEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[step {:03} @ {}ms] {}:{} ({}/{}) seq={}",
            self.step,
            self.time_ms,
            self.component,
            self.target,
            self.operation,
            self.phase,
            self.durable_sequence
        )?;
        if let Some(epoch) = self.epoch {
            write!(f, " epoch={epoch}")?;
        }
        if let Some(fault) = &self.fault {
            write!(f, " fault={fault:?}")?;
        }
        write!(f, " -> {}", self.outcome)
    }
}

#[derive(Debug)]
struct ControllerInner {
    step: usize,
    rules: VecDeque<FaultRule>,
    events: Vec<TraceEvent>,
    crashed: bool,
    crashes_triggered: usize,
    durable_sequence: u64,
    latest_takeover: Option<SimGrant>,
}

/// Shared simulation controller coordinating time, fault barriers, and trace events.
#[derive(Clone, Debug)]
pub struct SimController {
    clock: SimClock,
    authority: Option<SimAuthority>,
    inner: Arc<Mutex<ControllerInner>>,
}

impl Default for SimController {
    fn default() -> Self {
        Self::new(SimClock::default(), None)
    }
}

impl SimController {
    /// Create a controller with a simulated clock and optional lease authority.
    pub fn new(clock: SimClock, authority: Option<SimAuthority>) -> Self {
        Self {
            clock,
            authority,
            inner: Arc::new(Mutex::new(ControllerInner {
                step: 0,
                rules: VecDeque::new(),
                events: Vec::new(),
                crashed: false,
                crashes_triggered: 0,
                durable_sequence: 0,
                latest_takeover: None,
            })),
        }
    }

    /// Borrow the shared simulated clock.
    pub fn clock(&self) -> &SimClock {
        &self.clock
    }

    /// Borrow the optional lease authority.
    pub fn authority(&self) -> Option<&SimAuthority> {
        self.authority.as_ref()
    }

    /// Enqueue a validated fault rule.
    pub fn add_rule(&self, rule: FaultRule) -> Result<(), Error> {
        rule.validate()?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulation controller poisoned".into()))?;
        guard.rules.push_back(rule);
        Ok(())
    }

    /// Clear any active crash latch when reopening simulated processes.
    pub fn clear_crash(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.crashed = false;
        }
    }

    /// Clear crash state and disarm remaining fault rules before clean recovery convergence.
    pub fn reset_for_recovery(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.crashed = false;
            guard.rules.clear();
        }
    }

    /// Whether a simulated process crash is currently latched.
    pub fn is_crashed(&self) -> bool {
        self.inner.lock().map(|g| g.crashed).unwrap_or(false)
    }

    /// Total simulated crashes triggered so far.
    pub fn crashes_triggered(&self) -> usize {
        self.inner.lock().map(|g| g.crashes_triggered).unwrap_or(0)
    }

    /// Latest grant acquired by an [`FaultAction::ExpireLease`] fault, if any.
    pub fn latest_takeover(&self) -> Option<SimGrant> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.latest_takeover.clone())
    }

    /// Snapshot all recorded trace events in deterministic step order.
    pub fn events(&self) -> Vec<TraceEvent> {
        self.inner
            .lock()
            .map(|g| g.events.clone())
            .unwrap_or_default()
    }

    /// Current boundary step count.
    pub fn step_count(&self) -> usize {
        self.inner.lock().map(|g| g.step).unwrap_or(0)
    }

    fn set_durable_sequence(&self, seq: u64) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.durable_sequence = seq;
        }
    }

    /// Record an explicit harness event (e.g. crash, reopen, recovery, compaction).
    pub fn record_harness(&self, target: &str, operation: &str, outcome: &str) {
        let time_ms = self.clock.current_millis();
        let epoch = self.authority.as_ref().map(SimAuthority::current_epoch);
        if let Ok(mut guard) = self.inner.lock() {
            guard.step += 1;
            let step = guard.step;
            let seq = guard.durable_sequence;
            guard.events.push(TraceEvent {
                step,
                time_ms,
                component: SimComponent::Harness,
                target: target.into(),
                operation: operation.into(),
                phase: BoundaryPhase::BeforeEffect,
                fault: None,
                outcome: outcome.into(),
                durable_sequence: seq,
                epoch,
            });
        }
    }

    /// Enter a deterministic protocol boundary, increment the step counter, and evaluate
    /// any matching armed [`FaultRule`].
    pub fn enter_boundary(
        &self,
        component: SimComponent,
        target: &str,
        operation: &str,
        phase: BoundaryPhase,
        epoch: Option<u64>,
    ) -> (usize, Option<FaultAction>) {
        let mut guard = self.inner.lock().expect("simulation controller lock");
        guard.step += 1;
        let step = guard.step;
        if guard.crashed {
            let seq = guard.durable_sequence;
            guard.events.push(TraceEvent {
                step,
                time_ms: self.clock.current_millis(),
                component,
                target: target.into(),
                operation: operation.into(),
                phase,
                fault: Some(FaultAction::CrashProcess),
                outcome: "crashed_session".into(),
                durable_sequence: seq,
                epoch,
            });
            return (step, Some(FaultAction::CrashProcess));
        }
        let matched_idx = guard.rules.iter().position(|r| {
            let step_ok = r.step == 0 || r.step == step;
            let comp_ok = r.component.is_none_or(|c| c == component);
            let op_ok = r.operation.as_deref().is_none_or(|o| o == operation);
            let phase_ok = r.phase == phase;
            step_ok && comp_ok && op_ok && phase_ok
        });
        let fault = matched_idx.and_then(|idx| guard.rules.remove(idx).map(|r| r.action));
        drop(guard);

        if let Some(action) = &fault {
            match action {
                FaultAction::DelayMillis { millis } => {
                    let _ = self.clock.advance_millis(*millis);
                }
                FaultAction::ExpireLease {
                    advance_millis,
                    takeover_owner,
                    ttl_millis,
                } => {
                    let _ = self.clock.advance_millis(*advance_millis);
                    if let Some(authority) = &self.authority
                        && let Ok(grant) = authority.acquire(takeover_owner, *ttl_millis)
                        && let Ok(mut guard) = self.inner.lock()
                    {
                        guard.latest_takeover = Some(grant);
                    }
                }
                FaultAction::CrashProcess => {
                    if let Ok(mut guard) = self.inner.lock() {
                        guard.crashed = true;
                        guard.crashes_triggered += 1;
                    }
                }
                _ => {}
            }
        }
        (step, fault)
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_boundary(
        &self,
        step: usize,
        component: SimComponent,
        target: &str,
        operation: &str,
        phase: BoundaryPhase,
        fault: Option<FaultAction>,
        outcome: impl Into<String>,
        epoch: Option<u64>,
    ) {
        let time_ms = self.clock.current_millis();
        if let Ok(mut guard) = self.inner.lock() {
            let seq = guard.durable_sequence;
            guard.events.push(TraceEvent {
                step,
                time_ms,
                component,
                target: target.into(),
                operation: operation.into(),
                phase,
                fault,
                outcome: outcome.into(),
                durable_sequence: seq,
                epoch,
            });
        }
    }
}

fn sha256_bytes(data: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(data).to_vec()
}

#[derive(Clone, Debug)]
struct StoredEntry {
    entry: Entry,
    checksum: Vec<u8>,
}

#[derive(Clone, Debug)]
struct StoredCheckpoint {
    checkpoint: Checkpoint,
    checksum: Vec<u8>,
}

#[derive(Debug)]
struct JournalStoreInner {
    namespace: String,
    entries: Vec<StoredEntry>,
    archive: Vec<StoredEntry>,
    checkpoint: Option<StoredCheckpoint>,
    head: Sequence,
    active_unfenced_owner: bool,
    active_staged: Vec<Entry>,
    discarded_staged_records: usize,
    corrupt_reason: Option<String>,
    observed_decisions: BTreeMap<TransactionId, Decision>,
    decision_conflict: Option<String>,
    illegal_decision_without_prepare: Option<String>,
}

/// Shared durable backing store for [`SimJournal`] across crashes and reopens.
#[derive(Clone, Debug)]
pub struct SimJournalStore {
    controller: SimController,
    inner: Arc<Mutex<JournalStoreInner>>,
}

impl SimJournalStore {
    /// Create a fresh simulated journal store bound to `namespace` and `controller`.
    pub fn new(namespace: impl Into<String>, controller: SimController) -> Result<Self, Error> {
        let namespace = namespace.into();
        valid_label(&namespace, "journal namespace")?;
        Ok(Self {
            controller,
            inner: Arc::new(Mutex::new(JournalStoreInner {
                namespace,
                entries: Vec::new(),
                archive: Vec::new(),
                checkpoint: None,
                head: 0,
                active_unfenced_owner: false,
                active_staged: Vec::new(),
                discarded_staged_records: 0,
                corrupt_reason: None,
                observed_decisions: BTreeMap::new(),
                decision_conflict: None,
                illegal_decision_without_prepare: None,
            })),
        })
    }

    /// Open an exclusive unfenced journal session. Fails if another unfenced owner is live.
    pub fn open(&self) -> Result<SimJournal, Error> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated journal store poisoned".into()))?;
        if guard.active_unfenced_owner {
            return Err(Error::operational(crate::Diagnostic::message(
                ErrorCode::OwnershipBusy,
                "simulated journal already has an active owner".into(),
            )));
        }
        guard.active_unfenced_owner = true;
        guard.active_staged.clear();
        let next_sequence = guard.head;
        drop(guard);
        self.controller.clear_crash();
        Ok(SimJournal {
            store: self.clone(),
            grant: None,
            staged: Vec::new(),
            next_sequence,
            crashed: false,
        })
    }

    /// Open a fenced journal session bound to `grant`. Validates ownership immediately.
    pub fn open_fenced(&self, grant: SimGrant) -> Result<SimJournal, Error> {
        let authority = self
            .controller
            .authority()
            .ok_or_else(|| invalid("fenced journal requires a SimAuthority"))?;
        authority.validate(&grant)?;
        let guard = self
            .inner
            .lock()
            .map_err(|_| Error::in_doubt("simulated journal store poisoned".into()))?;
        if guard.namespace != grant.namespace {
            return Err(invalid("journal and grant namespace mismatch"));
        }
        let next_sequence = guard.head;
        drop(guard);
        self.controller.clear_crash();
        Ok(SimJournal {
            store: self.clone(),
            grant: Some(grant),
            staged: Vec::new(),
            next_sequence,
            crashed: false,
        })
    }

    /// Simulate an abrupt crash of any active session, discarding unflushed staged writes.
    pub fn crash_active_session(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.discarded_staged_records += guard.active_staged.len();
            guard.active_staged.clear();
            guard.active_unfenced_owner = false;
        }
        self.controller
            .record_harness(&self.namespace(), "crash_session", "unflushed_discarded");
    }

    /// Inject durable corruption so subsequent replay/checkpoint reads fail closed.
    pub fn inject_corruption(&self, reason: impl Into<String>) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.corrupt_reason = Some(reason.into());
        }
    }

    /// Durable namespace of this journal store.
    pub fn namespace(&self) -> String {
        self.inner
            .lock()
            .map(|g| g.namespace.clone())
            .unwrap_or_default()
    }

    /// Last durably flushed sequence watermark.
    pub fn head_sequence(&self) -> Sequence {
        self.inner.lock().map(|g| g.head).unwrap_or(0)
    }

    /// Number of staged records discarded by crashes or dropped sessions.
    pub fn discarded_staged_records(&self) -> usize {
        self.inner
            .lock()
            .map(|g| g.discarded_staged_records)
            .unwrap_or(0)
    }

    /// Snapshot of currently retained durable tail entries.
    pub fn durable_entries(&self) -> Vec<Entry> {
        self.inner
            .lock()
            .map(|g| g.entries.iter().map(|s| s.entry.clone()).collect())
            .unwrap_or_default()
    }

    /// Snapshot of archived entries retained by [`Retention::Archive`].
    pub fn archived_entries(&self) -> Vec<Entry> {
        self.inner
            .lock()
            .map(|g| g.archive.iter().map(|s| s.entry.clone()).collect())
            .unwrap_or_default()
    }

    /// Active durable checkpoint, if installed.
    pub fn active_checkpoint(&self) -> Option<Checkpoint> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.checkpoint.as_ref().map(|c| c.checkpoint.clone()))
    }

    /// Durable 2PC decision observed for `tx` across all flushes and compactions.
    pub fn observed_decision(&self, tx: &TransactionId) -> Option<Decision> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.observed_decisions.get(tx).copied())
    }

    fn inspect_flushed_entry(guard: &mut JournalStoreInner, entry: &Entry) {
        if entry.version != 1 {
            return;
        }
        if let Ok(record) = serde_json::from_slice::<crate::Record>(&entry.payload)
            && let crate::Record::Decided { tx, decision } = record
        {
            if let Some(prev) = guard.observed_decisions.get(&tx).copied()
                && prev != decision
            {
                guard.decision_conflict = Some(format!(
                    "transaction {tx} changed durable decision from {prev:?} to {decision:?}"
                ));
            }
            if decision == Decision::Commit {
                // Verify all enlisted requests had durable Prepared records in history/checkpoint.
                let mut enlisted = 0usize;
                let mut prepared = BTreeSet::new();
                if let Some(cp) = &guard.checkpoint
                    && let Ok(records) =
                        serde_json::from_slice::<Vec<crate::Record>>(&cp.checkpoint.payload)
                {
                    for r in records {
                        match r {
                            crate::Record::Begin {
                                tx: ref t,
                                ref requests,
                            } if t == &tx => {
                                enlisted = requests.len();
                            }
                            crate::Record::Prepared {
                                tx: ref t,
                                participant,
                                ..
                            } if t == &tx => {
                                prepared.insert(participant);
                            }
                            _ => {}
                        }
                    }
                }
                for stored in &guard.entries {
                    if let Ok(r) = serde_json::from_slice::<crate::Record>(&stored.entry.payload) {
                        match r {
                            crate::Record::Begin {
                                tx: ref t,
                                ref requests,
                            } if t == &tx => {
                                enlisted = requests.len();
                            }
                            crate::Record::Prepared {
                                tx: ref t,
                                participant,
                                ..
                            } if t == &tx => {
                                prepared.insert(participant);
                            }
                            _ => {}
                        }
                    }
                }
                if enlisted == 0 || prepared.len() != enlisted {
                    guard.illegal_decision_without_prepare = Some(format!(
                        "transaction {tx} flushed Commit with {}/{enlisted} prepared participants",
                        prepared.len()
                    ));
                }
            }
            guard.observed_decisions.insert(tx, decision);
        }
    }
}

/// Simulated journal handle implementing [`pactum_journal::Journal`].
#[derive(Debug)]
pub struct SimJournal {
    store: SimJournalStore,
    grant: Option<SimGrant>,
    staged: Vec<Entry>,
    next_sequence: Sequence,
    crashed: bool,
}

impl Drop for SimJournal {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.store.inner.lock() {
            guard.discarded_staged_records += guard.active_staged.len();
            guard.active_staged.clear();
            if self.grant.is_none() {
                guard.active_unfenced_owner = false;
            }
        }
    }
}

impl SimJournal {
    fn epoch(&self) -> Option<u64> {
        self.grant.as_ref().map(|g| g.epoch)
    }

    fn check_live_grant(&self) -> Result<(), JournalError> {
        if let Some(grant) = &self.grant {
            let authority = self.store.controller.authority().ok_or_else(|| {
                JournalError::from_diagnostic(crate::Diagnostic::message(
                    ErrorCode::StaleOwner,
                    "missing authority for fenced journal".into(),
                ))
            })?;
            authority
                .validate(grant)
                .map_err(|e| JournalError::from_diagnostic(e.diagnostic().clone()))?;
        }
        Ok(())
    }

    fn handle_before_fault(
        &mut self,
        operation: &str,
    ) -> Result<(usize, Option<FaultAction>), JournalError> {
        let target = self.store.namespace();
        let (step, fault) = self.store.controller.enter_boundary(
            SimComponent::Journal,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            self.epoch(),
        );
        if let Some(action) = &fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    let code = *code;
                    let err = JournalError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        format!("simulated journal {operation} failed before effect"),
                    ));
                    self.store.controller.finish_boundary(
                        step,
                        SimComponent::Journal,
                        &target,
                        operation,
                        BoundaryPhase::BeforeEffect,
                        fault,
                        code.as_str(),
                        self.epoch(),
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    self.crashed = true;
                    let discarded = self.staged.len();
                    self.staged.clear();
                    if let Ok(mut guard) = self.store.inner.lock() {
                        guard.discarded_staged_records += discarded;
                        guard.active_staged.clear();
                        if self.grant.is_none() {
                            guard.active_unfenced_owner = false;
                        }
                    }
                    let err = JournalError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::DurabilityUnknown,
                        format!("simulated process crash before journal {operation}"),
                    ));
                    self.store.controller.finish_boundary(
                        step,
                        SimComponent::Journal,
                        &target,
                        operation,
                        BoundaryPhase::BeforeEffect,
                        fault,
                        "crashed_before_effect",
                        self.epoch(),
                    );
                    return Err(err);
                }
                _ => {}
            }
        }
        self.store.controller.finish_boundary(
            step,
            SimComponent::Journal,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            fault.clone(),
            "ok",
            self.epoch(),
        );
        Ok((step, fault))
    }

    fn handle_after_fault(&mut self, operation: &str) -> Result<(), JournalError> {
        let target = self.store.namespace();
        let (step, fault) = self.store.controller.enter_boundary(
            SimComponent::Journal,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            self.epoch(),
        );
        if let Some(action) = &fault {
            match action {
                FaultAction::LoseAck { code } => {
                    let code = *code;
                    let err = JournalError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        format!(
                            "simulated journal {operation} acknowledgement lost after persistence"
                        ),
                    ));
                    self.store.controller.finish_boundary(
                        step,
                        SimComponent::Journal,
                        &target,
                        operation,
                        BoundaryPhase::AfterEffect,
                        fault,
                        format!("lost_ack:{}", code.as_str()),
                        self.epoch(),
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    self.crashed = true;
                    let discarded = self.staged.len();
                    self.staged.clear();
                    if let Ok(mut guard) = self.store.inner.lock() {
                        guard.discarded_staged_records += discarded;
                        guard.active_staged.clear();
                        if self.grant.is_none() {
                            guard.active_unfenced_owner = false;
                        }
                    }
                    let err = JournalError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::DurabilityUnknown,
                        format!("simulated process crash after journal {operation}"),
                    ));
                    self.store.controller.finish_boundary(
                        step,
                        SimComponent::Journal,
                        &target,
                        operation,
                        BoundaryPhase::AfterEffect,
                        fault,
                        "crashed_after_effect",
                        self.epoch(),
                    );
                    return Err(err);
                }
                _ => {}
            }
        }
        self.store.controller.finish_boundary(
            step,
            SimComponent::Journal,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            fault,
            "ok",
            self.epoch(),
        );
        Ok(())
    }
}

impl Journal for SimJournal {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        if self.crashed {
            return Err(JournalError::message(
                "simulated journal session is crashed".into(),
            ));
        }
        self.handle_before_fault("append")?;
        let sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| JournalError::message("journal sequence overflow".into()))?;
        let entry = Entry {
            sequence,
            version,
            payload: payload.to_vec(),
        };
        self.staged.push(entry.clone());
        self.next_sequence = sequence;
        if let Ok(mut guard) = self.store.inner.lock() {
            guard.active_staged = self.staged.clone();
        }
        self.handle_after_fault("append")?;
        Ok(sequence)
    }

    fn flush(&mut self) -> Result<Sequence, JournalError> {
        if self.crashed {
            return Err(JournalError::message(
                "simulated journal session is crashed".into(),
            ));
        }
        self.handle_before_fault("flush")?;
        self.check_live_grant()?;
        let durable_head = {
            let mut guard =
                self.store.inner.lock().map_err(|_| {
                    JournalError::message("simulated journal store poisoned".into())
                })?;
            if let Some(first) = self.staged.first()
                && Some(first.sequence) != guard.head.checked_add(1)
            {
                return Err(JournalError::from_diagnostic(crate::Diagnostic::message(
                    ErrorCode::StaleOwner,
                    "staged journal sequence conflicts with concurrent durable head".into(),
                )));
            }
            for entry in self.staged.drain(..) {
                let mut material = entry.sequence.to_be_bytes().to_vec();
                material.extend(entry.version.to_be_bytes());
                material.extend(&entry.payload);
                let checksum = sha256_bytes(&material);
                guard.head = entry.sequence;
                guard.entries.push(StoredEntry {
                    entry: entry.clone(),
                    checksum,
                });
                SimJournalStore::inspect_flushed_entry(&mut guard, &entry);
            }
            guard.active_staged.clear();
            guard.head
        };
        self.store.controller.set_durable_sequence(durable_head);
        self.handle_after_fault("flush")?;
        Ok(durable_head)
    }

    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        if self.crashed {
            return Err(JournalError::message(
                "simulated journal session is crashed".into(),
            ));
        }
        self.handle_before_fault("replay")?;
        self.check_live_grant()?;
        let entries = {
            let guard =
                self.store.inner.lock().map_err(|_| {
                    JournalError::message("simulated journal store poisoned".into())
                })?;
            if let Some(reason) = &guard.corrupt_reason {
                return Err(JournalError::corrupt(reason.clone()));
            }
            let mut expected = guard
                .checkpoint
                .as_ref()
                .map_or(0, |c| c.checkpoint.watermark);
            let mut out = Vec::with_capacity(guard.entries.len());
            for stored in &guard.entries {
                if Some(stored.entry.sequence) != expected.checked_add(1) {
                    return Err(JournalError::corrupt(
                        "simulated journal sequence gap".into(),
                    ));
                }
                let mut material = stored.entry.sequence.to_be_bytes().to_vec();
                material.extend(stored.entry.version.to_be_bytes());
                material.extend(&stored.entry.payload);
                if sha256_bytes(&material) != stored.checksum {
                    return Err(JournalError::corrupt(
                        "simulated journal entry checksum mismatch".into(),
                    ));
                }
                expected = stored.entry.sequence;
                out.push(stored.entry.clone());
            }
            if expected != guard.head {
                return Err(JournalError::corrupt(
                    "simulated journal head watermark mismatch".into(),
                ));
            }
            out
        };
        self.handle_after_fault("replay")?;
        Ok(entries)
    }

    fn checkpoint(&mut self) -> Result<Option<Checkpoint>, JournalError> {
        if self.crashed {
            return Err(JournalError::message(
                "simulated journal session is crashed".into(),
            ));
        }
        self.handle_before_fault("checkpoint")?;
        self.check_live_grant()?;
        let cp = {
            let guard =
                self.store.inner.lock().map_err(|_| {
                    JournalError::message("simulated journal store poisoned".into())
                })?;
            if let Some(reason) = &guard.corrupt_reason {
                return Err(JournalError::corrupt(reason.clone()));
            }
            if let Some(stored) = &guard.checkpoint {
                let mut material = stored.checkpoint.generation.to_be_bytes().to_vec();
                material.extend(stored.checkpoint.watermark.to_be_bytes());
                material.extend(stored.checkpoint.version.to_be_bytes());
                material.extend(&stored.checkpoint.payload);
                if stored.checkpoint.generation == 0 || sha256_bytes(&material) != stored.checksum {
                    return Err(JournalError::corrupt(
                        "simulated checkpoint checksum mismatch".into(),
                    ));
                }
                Some(stored.checkpoint.clone())
            } else {
                None
            }
        };
        self.handle_after_fault("checkpoint")?;
        Ok(cp)
    }

    fn compact(
        &mut self,
        watermark: Sequence,
        version: u32,
        payload: &[u8],
        retention: Retention,
    ) -> Result<(), JournalError> {
        if self.crashed {
            return Err(JournalError::message(
                "simulated journal session is crashed".into(),
            ));
        }
        self.handle_before_fault("compact")?;
        self.check_live_grant()?;
        if !self.staged.is_empty() {
            return Err(JournalError::message(
                "cannot compact with unflushed staged records".into(),
            ));
        }
        {
            let mut guard =
                self.store.inner.lock().map_err(|_| {
                    JournalError::message("simulated journal store poisoned".into())
                })?;
            let current_watermark = guard
                .checkpoint
                .as_ref()
                .map_or(0, |c| c.checkpoint.watermark);
            if watermark == 0 || watermark < current_watermark || watermark > guard.head {
                return Err(JournalError::message("invalid compaction watermark".into()));
            }
            let generation = guard
                .checkpoint
                .as_ref()
                .map_or(1, |c| c.checkpoint.generation + 1);
            let checkpoint = Checkpoint {
                generation,
                watermark,
                version,
                payload: payload.to_vec(),
            };
            let mut material = checkpoint.generation.to_be_bytes().to_vec();
            material.extend(checkpoint.watermark.to_be_bytes());
            material.extend(checkpoint.version.to_be_bytes());
            material.extend(&checkpoint.payload);
            let checksum = sha256_bytes(&material);
            let mut remaining = Vec::new();
            let drained = std::mem::take(&mut guard.entries);
            for entry in drained {
                if entry.entry.sequence <= watermark {
                    if retention == Retention::Archive {
                        guard.archive.push(entry);
                    }
                } else {
                    remaining.push(entry);
                }
            }
            guard.entries = remaining;
            guard.checkpoint = Some(StoredCheckpoint {
                checkpoint,
                checksum,
            });
        }
        self.handle_after_fault("compact")?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ParticipantRecord {
    Prepared { payload: Vec<u8>, token: Vec<u8> },
    Committed { payload: Vec<u8>, token: Vec<u8> },
    Aborted { token: Option<Vec<u8>> },
}

/// Queued participant operation for deterministic message reorder and duplicate tests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
pub enum SimParticipantMessage {
    /// Prepare `tx` with `payload`.
    Prepare {
        /// Transaction key.
        tx: TransactionId,
        /// Operation payload.
        payload: Vec<u8>,
    },
    /// Commit `tx` with `token`.
    Commit {
        /// Transaction key.
        tx: TransactionId,
        /// Prepared token.
        token: Vec<u8>,
    },
    /// Abort `tx` with optional `token`.
    Abort {
        /// Transaction key.
        tx: TransactionId,
        /// Optional prepared token.
        token: Option<Vec<u8>>,
    },
}

#[derive(Debug)]
struct ParticipantStoreInner {
    id: ParticipantId,
    value: i64,
    work: BTreeMap<TransactionId, ParticipantRecord>,
    commit_effect_counts: BTreeMap<TransactionId, usize>,
    premature_finalization: Option<String>,
    reorder_queue: Vec<SimParticipantMessage>,
}

/// Durable simulated 2PC counter resource store surviving process restarts.
#[derive(Clone, Debug)]
pub struct SimParticipantStore {
    controller: SimController,
    journal_observer: Option<SimJournalStore>,
    inner: Arc<Mutex<ParticipantStoreInner>>,
}

impl SimParticipantStore {
    /// Create a durable simulated participant resource with nonnegative additive deltas.
    pub fn new(
        id: ParticipantId,
        controller: SimController,
        journal_observer: Option<SimJournalStore>,
    ) -> Self {
        Self {
            controller,
            journal_observer,
            inner: Arc::new(Mutex::new(ParticipantStoreInner {
                id,
                value: 0,
                work: BTreeMap::new(),
                commit_effect_counts: BTreeMap::new(),
                premature_finalization: None,
                reorder_queue: Vec::new(),
            })),
        }
    }

    /// Create an unfenced [`SimParticipant`] handle sharing this durable store.
    pub fn handle(&self) -> Arc<SimParticipant> {
        Arc::new(SimParticipant {
            store: self.clone(),
            grant: None,
        })
    }

    /// Create a fenced [`SimParticipant`] handle bound to `grant`.
    pub fn fenced_handle(&self, grant: SimGrant) -> Arc<SimParticipant> {
        Arc::new(SimParticipant {
            store: self.clone(),
            grant: Some(grant),
        })
    }

    /// Stable participant identity.
    pub fn id(&self) -> ParticipantId {
        self.inner.lock().expect("participant lock").id.clone()
    }

    /// Read the committed counter value.
    pub fn value(&self) -> i64 {
        self.inner.lock().map(|g| g.value).unwrap_or(0)
    }

    /// Inspect durable status for `tx` directly from storage.
    pub fn durable_status(&self, tx: &TransactionId) -> ParticipantStatus {
        let guard = self.inner.lock().expect("participant lock");
        match guard.work.get(tx) {
            None => ParticipantStatus::Unknown,
            Some(ParticipantRecord::Prepared { token, .. }) => {
                ParticipantStatus::Prepared(token.clone())
            }
            Some(ParticipantRecord::Committed { .. }) => ParticipantStatus::Committed,
            Some(ParticipantRecord::Aborted { .. }) => ParticipantStatus::Aborted,
        }
    }

    /// Number of times `tx` mutated committed counter state (must never exceed `1`).
    pub fn commit_effect_count(&self, tx: &TransactionId) -> usize {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.commit_effect_counts.get(tx).copied())
            .unwrap_or(0)
    }

    /// Enqueue a message for deterministic reordering before delivery.
    pub fn enqueue_message(&self, msg: SimParticipantMessage) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.reorder_queue.push(msg);
        }
    }

    /// Reorder and deliver all queued messages using deterministic `seed`.
    pub async fn drain_reordered(&self, seed: u64) -> Vec<Result<(), ParticipantError>> {
        let mut messages = {
            let mut guard = self.inner.lock().expect("participant lock");
            std::mem::take(&mut guard.reorder_queue)
        };
        let mut rng = SimRng::new(seed);
        for i in (1..messages.len()).rev() {
            let j = rng.next_index(i + 1);
            messages.swap(i, j);
        }
        let handle = self.handle();
        let mut results = Vec::with_capacity(messages.len());
        for msg in messages {
            let res = match msg {
                SimParticipantMessage::Prepare { tx, payload } => {
                    handle.prepare(&tx, &payload).await.map(|_| ())
                }
                SimParticipantMessage::Commit { tx, token } => handle.commit(&tx, &token).await,
                SimParticipantMessage::Abort { tx, token } => {
                    handle.abort(&tx, token.as_deref()).await
                }
            };
            results.push(res);
        }
        results
    }
}

/// Simulated 2PC participant adapter implementing [`crate::Participant`].
#[derive(Clone, Debug)]
pub struct SimParticipant {
    store: SimParticipantStore,
    grant: Option<SimGrant>,
}

impl SimParticipant {
    fn epoch(&self) -> Option<u64> {
        self.grant.as_ref().map(|g| g.epoch)
    }

    fn check_live_grant(&self) -> Result<(), ParticipantError> {
        if let Some(grant) = &self.grant {
            let authority = self.store.controller.authority().ok_or_else(|| {
                ParticipantError::from_diagnostic(crate::Diagnostic::message(
                    ErrorCode::StaleOwner,
                    "missing authority for fenced participant".into(),
                ))
            })?;
            authority
                .validate(grant)
                .map_err(|e| ParticipantError::from_diagnostic(e.diagnostic().clone()))?;
        }
        Ok(())
    }

    fn execute_with_barriers<T, F>(
        &self,
        operation: &str,
        mut apply: F,
    ) -> Result<T, ParticipantError>
    where
        F: FnMut(&mut ParticipantStoreInner) -> Result<T, ParticipantError>,
    {
        let target = self.store.id().as_str().to_owned();
        let (before_step, before_fault) = self.store.controller.enter_boundary(
            SimComponent::Participant,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            self.epoch(),
        );
        let mut repetitions = 1u32;
        if let Some(action) = &before_fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    let code = *code;
                    let err = ParticipantError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        format!("simulated participant {operation} failed before effect"),
                    ));
                    self.store.controller.finish_boundary(
                        before_step,
                        SimComponent::Participant,
                        &target,
                        operation,
                        BoundaryPhase::BeforeEffect,
                        before_fault,
                        code.as_str(),
                        self.epoch(),
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    let err = ParticipantError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::AdapterUnknown,
                        format!("simulated crash before participant {operation}"),
                    ));
                    self.store.controller.finish_boundary(
                        before_step,
                        SimComponent::Participant,
                        &target,
                        operation,
                        BoundaryPhase::BeforeEffect,
                        before_fault,
                        "crashed_before_effect",
                        self.epoch(),
                    );
                    return Err(err);
                }
                FaultAction::DuplicateDelivery { extra_deliveries } => {
                    repetitions = 1 + *extra_deliveries;
                }
                _ => {}
            }
        }
        self.store.controller.finish_boundary(
            before_step,
            SimComponent::Participant,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            before_fault,
            "ok",
            self.epoch(),
        );

        self.check_live_grant()?;
        let mut last_result = None;
        for _ in 0..repetitions {
            let mut guard = self.store.inner.lock().map_err(|_| {
                ParticipantError::message("simulated participant mutex poisoned".into())
            })?;
            last_result = Some(apply(&mut guard));
        }
        let result = last_result.expect("at least one execution");

        let (after_step, after_fault) = self.store.controller.enter_boundary(
            SimComponent::Participant,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            self.epoch(),
        );
        if let Some(action) = &after_fault {
            match action {
                FaultAction::LoseAck { code } => {
                    let code = *code;
                    let err = ParticipantError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        format!("simulated participant {operation} reply lost after effect"),
                    ));
                    self.store.controller.finish_boundary(
                        after_step,
                        SimComponent::Participant,
                        &target,
                        operation,
                        BoundaryPhase::AfterEffect,
                        after_fault,
                        format!("lost_ack:{}", code.as_str()),
                        self.epoch(),
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    let err = ParticipantError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::AdapterUnknown,
                        format!("simulated crash after participant {operation}"),
                    ));
                    self.store.controller.finish_boundary(
                        after_step,
                        SimComponent::Participant,
                        &target,
                        operation,
                        BoundaryPhase::AfterEffect,
                        after_fault,
                        "crashed_after_effect",
                        self.epoch(),
                    );
                    return Err(err);
                }
                _ => {}
            }
        }
        let summary = match &result {
            Ok(_) => "ok".to_owned(),
            Err(e) => e.code().as_str().to_owned(),
        };
        self.store.controller.finish_boundary(
            after_step,
            SimComponent::Participant,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            after_fault,
            summary,
            self.epoch(),
        );
        result
    }
}

#[async_trait]
impl Participant for SimParticipant {
    fn id(&self) -> ParticipantId {
        self.store.id()
    }

    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError> {
        tokio::task::yield_now().await;
        self.execute_with_barriers("prepare", |guard| {
            if let Some(existing) = guard.work.get(tx) {
                return match existing {
                    ParticipantRecord::Aborted { .. } => {
                        Ok(Vote::Rejected("durable abort tombstone".into()))
                    }
                    ParticipantRecord::Prepared {
                        payload: old,
                        token,
                    }
                    | ParticipantRecord::Committed {
                        payload: old,
                        token,
                    } => {
                        if old.as_slice() != payload {
                            Err(ParticipantError::from_diagnostic(
                                crate::Diagnostic::message(
                                    ErrorCode::InvalidInput,
                                    "transaction payload mismatch".into(),
                                ),
                            ))
                        } else {
                            Ok(Vote::Prepared(token.clone()))
                        }
                    }
                };
            }
            let delta: i64 = std::str::from_utf8(payload)
                .map_err(|_| ParticipantError::message("invalid UTF-8 delta".into()))?
                .parse()
                .map_err(|_| ParticipantError::message("invalid integer delta".into()))?;
            if delta < 0 {
                return Ok(Vote::Rejected("negative delta".into()));
            }
            let mut reserved = guard.value;
            for record in guard.work.values() {
                if let ParticipantRecord::Prepared { payload: p, .. } = record {
                    let amount: i64 = std::str::from_utf8(p)
                        .map_err(|_| ParticipantError::message("corrupt prepared payload".into()))?
                        .parse()
                        .map_err(|_| ParticipantError::message("corrupt prepared delta".into()))?;
                    reserved = reserved.checked_add(amount).ok_or_else(|| {
                        ParticipantError::message("reservation capacity overflow".into())
                    })?;
                }
            }
            if reserved.checked_add(delta).is_none() {
                return Ok(Vote::Rejected("counter capacity exhausted".into()));
            }
            let token = tx.as_str().as_bytes().to_vec();
            guard.work.insert(
                tx.clone(),
                ParticipantRecord::Prepared {
                    payload: payload.to_vec(),
                    token: token.clone(),
                },
            );
            Ok(Vote::Prepared(token))
        })
    }

    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError> {
        tokio::task::yield_now().await;
        let journal_decision = self
            .store
            .journal_observer
            .as_ref()
            .and_then(|j| j.observed_decision(tx));
        self.execute_with_barriers("commit", |guard| {
            if self.store.journal_observer.is_some() && journal_decision != Some(Decision::Commit) {
                guard.premature_finalization = Some(format!(
                    "participant {} received commit for {tx} while journal decision is {journal_decision:?}",
                    guard.id
                ));
            }
            let Some(existing) = guard.work.get(tx).cloned() else {
                return Err(ParticipantError::from_diagnostic(
                    crate::Diagnostic::message(
                        ErrorCode::ProtocolMismatch,
                        "commit for unknown transaction".into(),
                    ),
                ));
            };
            match existing {
                ParticipantRecord::Committed {
                    token: stored_token,
                    ..
                } => {
                    if stored_token.as_slice() != token {
                        return Err(ParticipantError::from_diagnostic(
                            crate::Diagnostic::message(
                                ErrorCode::ProtocolMismatch,
                                "commit token mismatch on committed transaction".into(),
                            ),
                        ));
                    }
                    Ok(())
                }
                ParticipantRecord::Aborted { .. } => Err(ParticipantError::from_diagnostic(
                    crate::Diagnostic::message(
                        ErrorCode::ProtocolMismatch,
                        "cannot commit aborted transaction".into(),
                    ),
                )),
                ParticipantRecord::Prepared {
                    payload,
                    token: stored_token,
                } => {
                    if stored_token.as_slice() != token {
                        return Err(ParticipantError::from_diagnostic(
                            crate::Diagnostic::message(
                                ErrorCode::ProtocolMismatch,
                                "commit token mismatch".into(),
                            ),
                        ));
                    }
                    let delta: i64 = std::str::from_utf8(&payload)
                        .map_err(|_| ParticipantError::message("invalid payload".into()))?
                        .parse()
                        .map_err(|_| ParticipantError::message("invalid delta".into()))?;
                    guard.value = guard.value.checked_add(delta).ok_or_else(|| {
                        ParticipantError::message("counter overflow on commit".into())
                    })?;
                    *guard.commit_effect_counts.entry(tx.clone()).or_insert(0) += 1;
                    guard.work.insert(
                        tx.clone(),
                        ParticipantRecord::Committed {
                            payload,
                            token: stored_token,
                        },
                    );
                    Ok(())
                }
            }
        })
    }

    async fn abort(
        &self,
        tx: &TransactionId,
        token: Option<&[u8]>,
    ) -> Result<(), ParticipantError> {
        tokio::task::yield_now().await;
        let journal_decision = self
            .store
            .journal_observer
            .as_ref()
            .and_then(|j| j.observed_decision(tx));
        self.execute_with_barriers("abort", |guard| {
            if self.store.journal_observer.is_some() && journal_decision != Some(Decision::Abort) {
                guard.premature_finalization = Some(format!(
                    "participant {} received abort for {tx} while journal decision is {journal_decision:?}",
                    guard.id
                ));
            }
            match guard.work.get(tx).cloned() {
                None => {
                    guard.work.insert(
                        tx.clone(),
                        ParticipantRecord::Aborted {
                            token: token.map(<[u8]>::to_vec),
                        },
                    );
                    Ok(())
                }
                Some(ParticipantRecord::Committed { .. }) => Err(
                    ParticipantError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::ProtocolMismatch,
                        "cannot abort committed resource".into(),
                    )),
                ),
                Some(ParticipantRecord::Aborted { .. }) => Ok(()),
                Some(ParticipantRecord::Prepared {
                    token: stored_token,
                    ..
                }) => {
                    if let Some(supplied) = token
                        && supplied != stored_token.as_slice()
                    {
                        return Err(ParticipantError::from_diagnostic(
                            crate::Diagnostic::message(
                                ErrorCode::ProtocolMismatch,
                                "abort token mismatch".into(),
                            ),
                        ));
                    }
                    guard.work.insert(
                        tx.clone(),
                        ParticipantRecord::Aborted {
                            token: Some(stored_token),
                        },
                    );
                    Ok(())
                }
            }
        })
    }

    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        tokio::task::yield_now().await;
        self.execute_with_barriers("status", |guard| {
            Ok(match guard.work.get(tx) {
                None => ParticipantStatus::Unknown,
                Some(ParticipantRecord::Prepared { token, .. }) => {
                    ParticipantStatus::Prepared(token.clone())
                }
                Some(ParticipantRecord::Committed { .. }) => ParticipantStatus::Committed,
                Some(ParticipantRecord::Aborted { .. }) => ParticipantStatus::Aborted,
            })
        })
    }
}

/// Key/value delta or staged snapshot item compatible with `pactum_sqlite::Value`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimValue {
    /// Nonempty unique key within a unit or staged artifact.
    pub key: String,
    /// Signed delta for Atomic/Chunked, or snapshot value for StagedPublish.
    pub value: i64,
}

/// Visible published snapshot and monotonic CAS generation in [`SimBackendStore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimVisibleVersion {
    /// Monotonic visible generation (starts at `1` on first publication).
    pub generation: u64,
    /// Operation ID that published this snapshot.
    pub version: OperationId,
    /// Decoded snapshot values.
    pub values: Vec<SimValue>,
}

fn decode_sim_values(payload: &[u8]) -> Option<Vec<SimValue>> {
    let values: Vec<SimValue> = serde_json::from_slice(payload).ok()?;
    let mut keys = BTreeSet::new();
    values
        .iter()
        .all(|v| !v.key.is_empty() && keys.insert(&v.key))
        .then_some(values)
}

#[derive(Debug)]
struct BackendStoreInner {
    identity: String,
    operations: BTreeMap<OperationId, (Vec<u8>, String)>,
    units: BTreeMap<(OperationId, i64), UnitResult>,
    unit_effect_counts: BTreeMap<(OperationId, i64), usize>,
    values: BTreeMap<String, i64>,
    artifacts: BTreeMap<OperationId, Vec<u8>>,
    visible: BTreeMap<OperationId, (u64, OperationId)>,
    publish_effect_counts: BTreeMap<OperationId, usize>,
}

/// Durable simulated native-mode backend store for Atomic, Chunked, and StagedPublish.
#[derive(Clone, Debug)]
pub struct SimBackendStore {
    controller: SimController,
    inner: Arc<Mutex<BackendStoreInner>>,
}

impl SimBackendStore {
    /// Create a simulated native backend store with stable `identity`.
    pub fn new(identity: impl Into<String>, controller: SimController) -> Result<Self, Error> {
        let identity = identity.into();
        valid_label(&identity, "backend identity")?;
        Ok(Self {
            controller,
            inner: Arc::new(Mutex::new(BackendStoreInner {
                identity,
                operations: BTreeMap::new(),
                units: BTreeMap::new(),
                unit_effect_counts: BTreeMap::new(),
                values: BTreeMap::new(),
                artifacts: BTreeMap::new(),
                visible: BTreeMap::new(),
                publish_effect_counts: BTreeMap::new(),
            })),
        })
    }

    /// Open a [`SimBackend`] adapter sharing this durable store.
    pub fn open(&self) -> SimBackend {
        SimBackend {
            store: self.clone(),
        }
    }

    /// Read a committed counter value (`0` if never updated).
    pub fn value(&self, key: &str) -> i64 {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.values.get(key).copied())
            .unwrap_or(0)
    }

    /// Read the currently visible version for `target`, if published.
    pub fn visible(&self, target: &OperationId) -> Option<SimVisibleVersion> {
        let guard = self.inner.lock().ok()?;
        let (generation, version) = guard.visible.get(target)?.clone();
        let payload = guard.artifacts.get(&version)?;
        let values = decode_sim_values(payload)?;
        Some(SimVisibleVersion {
            generation,
            version,
            values,
        })
    }

    /// Check whether an artifact is currently stored for `id`.
    pub fn has_artifact(&self, id: &OperationId) -> bool {
        self.inner
            .lock()
            .map(|g| g.artifacts.contains_key(id))
            .unwrap_or(false)
    }

    /// Snapshot all committed counter values.
    pub fn all_values(&self) -> BTreeMap<String, i64> {
        self.inner
            .lock()
            .map(|g| g.values.clone())
            .unwrap_or_default()
    }
}

/// Simulated native-mode backend implementing [`crate::execution::Backend`].
#[derive(Clone, Debug)]
pub struct SimBackend {
    store: SimBackendStore,
}

impl SimBackend {
    /// Borrow the underlying durable store.
    pub fn store(&self) -> &SimBackendStore {
        &self.store
    }

    fn execute_with_barriers<T, F>(
        &mut self,
        operation: &str,
        mut apply: F,
    ) -> Result<T, BackendError>
    where
        F: FnMut(&mut BackendStoreInner) -> Result<T, BackendError>,
    {
        let target = self.identity();
        let (before_step, before_fault) = self.store.controller.enter_boundary(
            SimComponent::Backend,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            None,
        );
        let mut repetitions = 1u32;
        if let Some(action) = &before_fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    let code = *code;
                    let err = BackendError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        format!("simulated backend {operation} failed before effect"),
                    ));
                    self.store.controller.finish_boundary(
                        before_step,
                        SimComponent::Backend,
                        &target,
                        operation,
                        BoundaryPhase::BeforeEffect,
                        before_fault,
                        code.as_str(),
                        None,
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    let err = BackendError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::AdapterUnknown,
                        format!("simulated crash before backend {operation}"),
                    ));
                    self.store.controller.finish_boundary(
                        before_step,
                        SimComponent::Backend,
                        &target,
                        operation,
                        BoundaryPhase::BeforeEffect,
                        before_fault,
                        "crashed_before_effect",
                        None,
                    );
                    return Err(err);
                }
                FaultAction::DuplicateDelivery { extra_deliveries } => {
                    repetitions = 1 + *extra_deliveries;
                }
                _ => {}
            }
        }
        self.store.controller.finish_boundary(
            before_step,
            SimComponent::Backend,
            &target,
            operation,
            BoundaryPhase::BeforeEffect,
            before_fault,
            "ok",
            None,
        );

        let mut last: Option<Result<T, BackendError>> = None;
        for _ in 0..repetitions {
            let mut guard =
                self.store.inner.lock().map_err(|_| {
                    BackendError::message("simulated backend store poisoned".into())
                })?;
            last = Some(apply(&mut guard));
        }
        let result = last.expect("at least one backend execution");

        let (after_step, after_fault) = self.store.controller.enter_boundary(
            SimComponent::Backend,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            None,
        );
        if let Some(action) = &after_fault {
            match action {
                FaultAction::LoseAck { code } => {
                    let code = *code;
                    let err = BackendError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        format!("simulated backend {operation} acknowledgement lost"),
                    ));
                    self.store.controller.finish_boundary(
                        after_step,
                        SimComponent::Backend,
                        &target,
                        operation,
                        BoundaryPhase::AfterEffect,
                        after_fault,
                        format!("lost_ack:{}", code.as_str()),
                        None,
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    let err = BackendError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::AdapterUnknown,
                        format!("simulated crash after backend {operation}"),
                    ));
                    self.store.controller.finish_boundary(
                        after_step,
                        SimComponent::Backend,
                        &target,
                        operation,
                        BoundaryPhase::AfterEffect,
                        after_fault,
                        "crashed_after_effect",
                        None,
                    );
                    return Err(err);
                }
                _ => {}
            }
        }
        let summary = match &result {
            Ok(_) => "ok".to_owned(),
            Err(e) => e.code().as_str().to_owned(),
        };
        self.store.controller.finish_boundary(
            after_step,
            SimComponent::Backend,
            &target,
            operation,
            BoundaryPhase::AfterEffect,
            after_fault,
            summary,
            None,
        );
        result
    }

    fn bind(
        guard: &mut BackendStoreInner,
        id: &OperationId,
        request: &Request,
    ) -> Result<(), BackendError> {
        let bytes = serde_json::to_vec(request)
            .map_err(|e| BackendError::classified(ErrorCode::InvalidInput, e))?;
        if let Some((existing, _)) = guard.operations.get(id) {
            if existing != &bytes {
                return Err(BackendError::from_diagnostic(crate::Diagnostic::message(
                    ErrorCode::InvalidInput,
                    "backend operation key/payload/mode mismatch".into(),
                )));
            }
        } else {
            guard.operations.insert(id.clone(), (bytes, "new".into()));
        }
        Ok(())
    }
}

impl Backend for SimBackend {
    fn identity(&self) -> String {
        self.store
            .inner
            .lock()
            .map(|g| g.identity.clone())
            .unwrap_or_default()
    }

    fn unit(
        &mut self,
        id: &OperationId,
        request: &Request,
        chunk: Option<usize>,
    ) -> Result<UnitResult, BackendError> {
        let (payload, index) = match (request, chunk) {
            (Request::Atomic(r), None) => (r.payload.clone(), -1_i64),
            (Request::Chunked(r), Some(i)) => {
                let bytes = r
                    .chunks
                    .get(i)
                    .ok_or_else(|| BackendError::message("invalid chunk index".into()))?
                    .clone();
                let idx = i64::try_from(i)
                    .map_err(|_| BackendError::message("chunk index overflow".into()))?;
                (bytes, idx)
            }
            _ => return Err(BackendError::message("unit mode mismatch".into())),
        };
        self.execute_with_barriers("unit", |guard| {
            Self::bind(guard, id, request)?;
            if let Some(receipt) = guard.units.get(&(id.clone(), index)).copied() {
                return Ok(receipt);
            }
            let mut accepted = false;
            if let Some(values) = decode_sim_values(&payload) {
                let mut staged = guard.values.clone();
                accepted = true;
                for item in values {
                    let old = staged.get(&item.key).copied().unwrap_or(0);
                    let Some(next) = old.checked_add(item.value) else {
                        accepted = false;
                        break;
                    };
                    staged.insert(item.key, next);
                }
                if accepted {
                    guard.values = staged;
                    *guard
                        .unit_effect_counts
                        .entry((id.clone(), index))
                        .or_insert(0) += 1;
                }
            }
            let outcome = if accepted {
                UnitResult::Committed
            } else {
                UnitResult::RolledBack
            };
            guard.units.insert((id.clone(), index), outcome);
            Ok(outcome)
        })
    }

    fn stage(&mut self, id: &OperationId, request: &PublishRequest) -> Result<(), BackendError> {
        let wrapped = Request::StagedPublish(request.clone());
        self.execute_with_barriers("stage", |guard| {
            Self::bind(guard, id, &wrapped)?;
            let state = guard.operations[id].1.as_str();
            match state {
                "new" | "staged" => {
                    guard
                        .artifacts
                        .entry(id.clone())
                        .or_insert_with(|| request.payload.clone());
                    guard.operations.get_mut(id).unwrap().1 = "staged".into();
                    Ok(())
                }
                "published" => Ok(()),
                _ => Err(BackendError::message(
                    "staging was cleaned or rejected".into(),
                )),
            }
        })
    }

    fn validate(
        &mut self,
        id: &OperationId,
        request: &PublishRequest,
    ) -> Result<bool, BackendError> {
        let wrapped = Request::StagedPublish(request.clone());
        self.execute_with_barriers("validate", |guard| {
            Self::bind(guard, id, &wrapped)?;
            let Some(payload) = guard.artifacts.get(id) else {
                return Err(BackendError::message("missing staged artifact".into()));
            };
            if payload != &request.payload {
                return Err(BackendError::from_diagnostic(crate::Diagnostic::message(
                    ErrorCode::CorruptHistory,
                    "staged artifact changed".into(),
                )));
            }
            Ok(decode_sim_values(payload).is_some())
        })
    }

    fn publish(
        &mut self,
        id: &OperationId,
        request: &PublishRequest,
    ) -> Result<PublishResult, BackendError> {
        let wrapped = Request::StagedPublish(request.clone());
        self.execute_with_barriers("publish", |guard| {
            Self::bind(guard, id, &wrapped)?;
            match guard.operations[id].1.as_str() {
                "published" => return Ok(PublishResult::Published),
                "conflict" => return Ok(PublishResult::Conflict),
                "staged" => {}
                _ => return Err(BackendError::message("artifact is not staged".into())),
            }
            let Some(payload) = guard.artifacts.get(id) else {
                return Err(BackendError::message("missing staged artifact".into()));
            };
            if payload != &request.payload || decode_sim_values(payload).is_none() {
                return Err(BackendError::from_diagnostic(crate::Diagnostic::message(
                    ErrorCode::CorruptHistory,
                    "invalid staged artifact".into(),
                )));
            }
            let generation = guard
                .visible
                .get(&request.target)
                .map_or(0, |(gen_num, _)| *gen_num);
            let published =
                generation == request.expected_generation && generation < i64::MAX as u64;
            if published {
                guard
                    .visible
                    .insert(request.target.clone(), (generation + 1, id.clone()));
                *guard.publish_effect_counts.entry(id.clone()).or_insert(0) += 1;
            }
            guard.operations.get_mut(id).unwrap().1 = if published {
                "published".into()
            } else {
                "conflict".into()
            };
            Ok(if published {
                PublishResult::Published
            } else {
                PublishResult::Conflict
            })
        })
    }

    fn cleanup(&mut self, id: &OperationId, request: &PublishRequest) -> Result<(), BackendError> {
        let wrapped = Request::StagedPublish(request.clone());
        self.execute_with_barriers("cleanup", |guard| {
            Self::bind(guard, id, &wrapped)?;
            let prior = guard.operations[id].1.clone();
            if prior == "published" {
                return Err(BackendError::message(
                    "cannot clean a published artifact".into(),
                ));
            }
            guard.artifacts.remove(id);
            if prior != "conflict" {
                guard.operations.get_mut(id).unwrap().1 = "cleaned".into();
            }
            Ok(())
        })
    }
}

/// Configured deterministic response behavior for a [`SimSagaAdapter`] payload or step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "behavior", deny_unknown_fields)]
pub enum SimSagaBehavior {
    /// Apply the effect and record a permanent `Applied` receipt.
    Apply,
    /// Reject permanently with a durable no-effect tombstone.
    Reject {
        /// Rejection reason.
        reason: String,
    },
    /// Return `Retryable` for the first `failures` calls, then `Applied`.
    RetryableThenApply {
        /// Number of initial retryable responses.
        failures: u32,
    },
    /// Return `Uncertain` for the first `uncertainties` calls, then reconcile to `Applied`.
    UncertainThenApply {
        /// Number of initial uncertain responses.
        uncertainties: u32,
    },
}

#[derive(Debug)]
struct SagaStoreInner {
    identity: String,
    receipts: BTreeMap<OperationId, (Vec<u8>, ActionResult)>,
    call_counts: BTreeMap<OperationId, u32>,
    effect_counts: BTreeMap<OperationId, usize>,
    behaviors: BTreeMap<Vec<u8>, SimSagaBehavior>,
    net_value: i64,
}

/// Durable simulated Saga adapter store surviving process restarts.
#[derive(Clone, Debug)]
pub struct SimSagaStore {
    controller: SimController,
    inner: Arc<Mutex<SagaStoreInner>>,
}

impl SimSagaStore {
    /// Create a simulated Saga resource store with stable `identity`.
    pub fn new(identity: impl Into<String>, controller: SimController) -> Result<Self, Error> {
        let identity = identity.into();
        valid_label(&identity, "saga adapter identity")?;
        Ok(Self {
            controller,
            inner: Arc::new(Mutex::new(SagaStoreInner {
                identity,
                receipts: BTreeMap::new(),
                call_counts: BTreeMap::new(),
                effect_counts: BTreeMap::new(),
                behaviors: BTreeMap::new(),
                net_value: 0,
            })),
        })
    }

    /// Configure deterministic behavior for a specific operation `payload`.
    pub fn set_behavior(&self, payload: Vec<u8>, behavior: SimSagaBehavior) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.behaviors.insert(payload, behavior);
        }
    }

    /// Create an `Arc<SimSagaAdapter>` handle backed by this store.
    pub fn handle(&self) -> Arc<SimSagaAdapter> {
        Arc::new(SimSagaAdapter {
            store: self.clone(),
        })
    }

    /// Current net value across applied forward (`+delta`) and compensating (`-delta`) operations.
    pub fn net_value(&self) -> i64 {
        self.inner.lock().map(|g| g.net_value).unwrap_or(0)
    }

    /// Number of times `key` applied its business effect (must never exceed `1`).
    pub fn effect_count(&self, key: &OperationId) -> usize {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.effect_counts.get(key).copied())
            .unwrap_or(0)
    }
}

/// Simulated cooperating Saga adapter implementing [`crate::saga::SagaAdapter`].
#[derive(Clone, Debug)]
pub struct SimSagaAdapter {
    store: SimSagaStore,
}

#[async_trait]
impl SagaAdapter for SimSagaAdapter {
    fn identity(&self) -> String {
        self.store
            .inner
            .lock()
            .map(|g| g.identity.clone())
            .unwrap_or_default()
    }

    fn accepts(&self, payload: &[u8]) -> bool {
        !payload.is_empty()
            && std::str::from_utf8(payload)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .is_some()
    }

    async fn execute(&self, key: OperationId, payload: Vec<u8>) -> Result<ActionResult, SagaError> {
        tokio::task::yield_now().await;
        let target = self.identity();
        let (before_step, before_fault) = self.store.controller.enter_boundary(
            SimComponent::SagaAdapter,
            &target,
            "execute",
            BoundaryPhase::BeforeEffect,
            None,
        );
        let mut repetitions = 1u32;
        if let Some(action) = &before_fault {
            match action {
                FaultAction::FailBeforeEffect { code } => {
                    let code = *code;
                    let err = SagaError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        "simulated saga adapter failed before effect".into(),
                    ));
                    self.store.controller.finish_boundary(
                        before_step,
                        SimComponent::SagaAdapter,
                        &target,
                        "execute",
                        BoundaryPhase::BeforeEffect,
                        before_fault,
                        code.as_str(),
                        None,
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    let err = SagaError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::AdapterUnknown,
                        "simulated crash before saga adapter execute".into(),
                    ));
                    self.store.controller.finish_boundary(
                        before_step,
                        SimComponent::SagaAdapter,
                        &target,
                        "execute",
                        BoundaryPhase::BeforeEffect,
                        before_fault,
                        "crashed_before_effect",
                        None,
                    );
                    return Err(err);
                }
                FaultAction::DuplicateDelivery { extra_deliveries } => {
                    repetitions = 1 + *extra_deliveries;
                }
                _ => {}
            }
        }
        self.store.controller.finish_boundary(
            before_step,
            SimComponent::SagaAdapter,
            &target,
            "execute",
            BoundaryPhase::BeforeEffect,
            before_fault,
            "ok",
            None,
        );

        let delta: i64 = std::str::from_utf8(&payload)
            .map_err(|_| SagaError::message("invalid UTF-8 saga payload".into()))?
            .parse()
            .map_err(|_| SagaError::message("invalid integer saga delta".into()))?;

        let mut last: Option<Result<ActionResult, SagaError>> = None;
        for _ in 0..repetitions {
            let mut guard = self
                .store
                .inner
                .lock()
                .map_err(|_| SagaError::message("simulated saga store poisoned".into()))?;
            if let Some((bound_payload, receipt)) = guard.receipts.get(&key).cloned() {
                if bound_payload != payload {
                    return Err(SagaError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::InvalidInput,
                        "saga operation key reused with different payload".into(),
                    )));
                }
                last = Some(Ok(receipt));
                continue;
            }
            let call_num = {
                let count = guard.call_counts.entry(key.clone()).or_insert(0);
                *count += 1;
                *count
            };
            let behavior = guard
                .behaviors
                .get(&payload)
                .cloned()
                .unwrap_or(SimSagaBehavior::Apply);
            let outcome = match behavior {
                SimSagaBehavior::Apply => {
                    guard.net_value = guard
                        .net_value
                        .checked_add(delta)
                        .ok_or_else(|| SagaError::message("saga net value overflow".into()))?;
                    *guard.effect_counts.entry(key.clone()).or_insert(0) += 1;
                    let res = ActionResult::Applied;
                    guard
                        .receipts
                        .insert(key.clone(), (payload.clone(), res.clone()));
                    res
                }
                SimSagaBehavior::Reject { reason } => {
                    let res = ActionResult::Rejected(reason);
                    guard
                        .receipts
                        .insert(key.clone(), (payload.clone(), res.clone()));
                    res
                }
                SimSagaBehavior::RetryableThenApply { failures } => {
                    if call_num <= failures {
                        ActionResult::Retryable("simulated transient rejection".into())
                    } else {
                        guard.net_value = guard
                            .net_value
                            .checked_add(delta)
                            .ok_or_else(|| SagaError::message("saga net value overflow".into()))?;
                        *guard.effect_counts.entry(key.clone()).or_insert(0) += 1;
                        let res = ActionResult::Applied;
                        guard
                            .receipts
                            .insert(key.clone(), (payload.clone(), res.clone()));
                        res
                    }
                }
                SimSagaBehavior::UncertainThenApply { uncertainties } => {
                    // Persist effect on the first attempt, but report Uncertain until reconciled!
                    guard.net_value = guard
                        .net_value
                        .checked_add(delta)
                        .ok_or_else(|| SagaError::message("saga net value overflow".into()))?;
                    *guard.effect_counts.entry(key.clone()).or_insert(0) += 1;
                    let res = ActionResult::Applied;
                    guard
                        .receipts
                        .insert(key.clone(), (payload.clone(), res.clone()));
                    if call_num <= uncertainties {
                        ActionResult::Uncertain("simulated ambiguous completion".into())
                    } else {
                        res
                    }
                }
            };
            last = Some(Ok(outcome));
        }
        let result = last.expect("at least one saga execution");

        let (after_step, after_fault) = self.store.controller.enter_boundary(
            SimComponent::SagaAdapter,
            &target,
            "execute",
            BoundaryPhase::AfterEffect,
            None,
        );
        if let Some(action) = &after_fault {
            match action {
                FaultAction::LoseAck { code } => {
                    let code = *code;
                    let err = SagaError::from_diagnostic(crate::Diagnostic::message(
                        code,
                        "simulated saga adapter acknowledgement lost".into(),
                    ));
                    self.store.controller.finish_boundary(
                        after_step,
                        SimComponent::SagaAdapter,
                        &target,
                        "execute",
                        BoundaryPhase::AfterEffect,
                        after_fault,
                        format!("lost_ack:{}", code.as_str()),
                        None,
                    );
                    return Err(err);
                }
                FaultAction::CrashProcess => {
                    let err = SagaError::from_diagnostic(crate::Diagnostic::message(
                        ErrorCode::AdapterUnknown,
                        "simulated crash after saga adapter execute".into(),
                    ));
                    self.store.controller.finish_boundary(
                        after_step,
                        SimComponent::SagaAdapter,
                        &target,
                        "execute",
                        BoundaryPhase::AfterEffect,
                        after_fault,
                        "crashed_after_effect",
                        None,
                    );
                    return Err(err);
                }
                _ => {}
            }
        }
        let summary = match &result {
            Ok(r) => format!("{r:?}"),
            Err(e) => e.code().as_str().to_owned(),
        };
        self.store.controller.finish_boundary(
            after_step,
            SimComponent::SagaAdapter,
            &target,
            "execute",
            BoundaryPhase::AfterEffect,
            after_fault,
            summary,
            None,
        );
        result
    }
}

/// Protocol state-machine invariant checked during simulation and after recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Invariant {
    /// Durable 2PC decisions and terminal mode/Saga outcomes never reverse across recovery or compaction.
    ImmutableDecision,
    /// No participant receives commit or abort before a durable decision barrier; commit requires all prepares.
    DecisionBarrierPrecedesFinalization,
    /// Committed effects execute at most once per idempotency key; aborted work leaves zero net effect.
    ExactlyOnceEffects,
    /// Coordinator/engine terminal status matches participant/backend/adapter receipts and counter totals.
    DurableReceiptConsistency,
    /// Stale-epoch owners cannot mutate journal or participant state after lease expiry and takeover.
    FencingSafety,
    /// Uncertain journal writes poison the live coordinator/engine and require reopen before further calls.
    PoisonedOnUncertainty,
}

impl fmt::Display for Invariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::ImmutableDecision => "ImmutableDecision",
            Self::DecisionBarrierPrecedesFinalization => "DecisionBarrierPrecedesFinalization",
            Self::ExactlyOnceEffects => "ExactlyOnceEffects",
            Self::DurableReceiptConsistency => "DurableReceiptConsistency",
            Self::FencingSafety => "FencingSafety",
            Self::PoisonedOnUncertainty => "PoisonedOnUncertainty",
        };
        f.write_str(label)
    }
}

/// Human-readable, machine-serializable state-machine invariant violation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvariantViolation {
    /// Violated invariant classification.
    pub invariant: Invariant,
    /// Boundary step counter where the violation was detected.
    pub step: usize,
    /// Human-readable explanation of the invariant failure.
    pub detail: String,
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invariant {} violated at step {}: {}",
            self.invariant, self.step, self.detail
        )
    }
}

impl std::error::Error for InvariantViolation {}

/// Protocol workload explored by [`StateExplorer`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "workload", deny_unknown_fields)]
pub enum SimWorkload {
    /// Two-phase commit across two durable counter participants (`a` and `b`).
    TwoPhaseCommit {
        /// Whether participant `b` rejects prepare with a negative delta.
        reject_second: bool,
        /// Whether cancellation is requested before prepare/decision.
        cancel_before_decision: bool,
        /// Whether to compact the journal and reopen after recovery.
        compact_after_recovery: bool,
    },
    /// Native `Atomic` execution over [`SimBackend`].
    NativeAtomic {
        /// Whether the atomic payload commits (`true`) or rolls back on overflow (`false`).
        valid_payload: bool,
        /// Whether to compact the journal and reopen after recovery.
        compact_after_recovery: bool,
    },
    /// Native `Chunked` batch execution over [`SimBackend`].
    NativeChunked {
        /// Number of deterministic chunks (`1..=8`).
        chunks: usize,
        /// Whether the final chunk is invalid and rejected after earlier chunks commit.
        reject_last: bool,
        /// Whether to pause after the first chunk and explicitly resume.
        pause_after_first: bool,
    },
    /// Native `StagedPublish` execution over [`SimBackend`].
    NativeStagedPublish {
        /// Expected visible pointer generation (`0` succeeds on fresh store; `>0` conflicts).
        expected_generation: u64,
        /// Whether staged payload passes validation (`true`) or aborts and cleans up (`false`).
        valid_payload: bool,
    },
    /// Non-2PC Saga execution and reverse dependency compensation over [`SimSagaAdapter`].
    SagaExecution {
        /// Whether step 2 forward operation rejects, triggering reverse compensation of step 1.
        fail_second_forward: bool,
        /// Whether step 1 forward operation reports initial uncertainty before reconciling.
        uncertain_first_forward: bool,
        /// Whether step 1 is marked `Irreversible` under `IrreversiblePolicy::Allow`.
        irreversible_first: bool,
    },
    /// Fenced 2PC execution with lease expiration, epoch takeover, and stale-owner rejection.
    FencedRecovery {
        /// Whether lease takeover happens before the first owner records a durable decision.
        takeover_before_decision: bool,
        /// Whether the stale first owner attempts delayed journal/participant calls after takeover.
        delayed_stale_calls: bool,
    },
}

impl SimWorkload {
    /// Validate workload parameters.
    pub fn validate(&self) -> Result<(), Error> {
        if let Self::NativeChunked { chunks, .. } = self
            && (*chunks == 0 || *chunks > 8)
        {
            return Err(invalid("simulated chunked workload requires 1..=8 chunks"));
        }
        Ok(())
    }

    /// Canonical set of workloads covering 2PC, native modes, Sagas, and fenced recovery.
    pub fn canonical_suite() -> Vec<Self> {
        vec![
            Self::TwoPhaseCommit {
                reject_second: false,
                cancel_before_decision: false,
                compact_after_recovery: true,
            },
            Self::TwoPhaseCommit {
                reject_second: true,
                cancel_before_decision: false,
                compact_after_recovery: true,
            },
            Self::TwoPhaseCommit {
                reject_second: false,
                cancel_before_decision: true,
                compact_after_recovery: false,
            },
            Self::NativeAtomic {
                valid_payload: true,
                compact_after_recovery: true,
            },
            Self::NativeAtomic {
                valid_payload: false,
                compact_after_recovery: true,
            },
            Self::NativeChunked {
                chunks: 3,
                reject_last: false,
                pause_after_first: true,
            },
            Self::NativeChunked {
                chunks: 3,
                reject_last: true,
                pause_after_first: false,
            },
            Self::NativeStagedPublish {
                expected_generation: 0,
                valid_payload: true,
            },
            Self::NativeStagedPublish {
                expected_generation: 9,
                valid_payload: true,
            },
            Self::NativeStagedPublish {
                expected_generation: 0,
                valid_payload: false,
            },
            Self::SagaExecution {
                fail_second_forward: false,
                uncertain_first_forward: true,
                irreversible_first: false,
            },
            Self::SagaExecution {
                fail_second_forward: true,
                uncertain_first_forward: true,
                irreversible_first: false,
            },
            Self::SagaExecution {
                fail_second_forward: true,
                uncertain_first_forward: false,
                irreversible_first: true,
            },
            Self::FencedRecovery {
                takeover_before_decision: true,
                delayed_stale_calls: true,
            },
            Self::FencedRecovery {
                takeover_before_decision: false,
                delayed_stale_calls: true,
            },
        ]
    }
}

/// Deterministic action in a [`SimSchedule`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub enum ScheduleAction {
    /// Arm a boundary fault rule before running the workload.
    InjectFault {
        /// Fault rule to arm.
        rule: FaultRule,
    },
    /// Advance simulated clock by `millis` before reconciliation.
    AdvanceClock {
        /// Milliseconds to advance.
        millis: u64,
    },
    /// Deliver duplicate or reordered participant messages with `seed`.
    ReorderParticipantMessages {
        /// Seed for deterministic Fisher-Yates shuffle of queued messages.
        seed: u64,
    },
    /// Crash the active session, discard unflushed staging, and reopen for recovery.
    CrashAndReopen,
    /// Execute an additional idempotent recovery pass through [`RecoveryTarget`].
    ExtraRecoveryPass,
    /// Compact the journal using `Retention::Compact` or `Retention::Archive`.
    CompactJournal {
        /// When `true`, retain covered events in the archive; otherwise compact.
        archive: bool,
    },
}

impl ScheduleAction {
    /// Validate action parameters.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::InjectFault { rule } => rule.validate(),
            Self::AdvanceClock { millis } => {
                if *millis == 0 || *millis > 86_400_000 {
                    return Err(invalid("clock advance must be in 1..=86_400_000 ms"));
                }
                Ok(())
            }
            Self::ReorderParticipantMessages { .. }
            | Self::CrashAndReopen
            | Self::ExtraRecoveryPass
            | Self::CompactJournal { .. } => Ok(()),
        }
    }
}

/// Seeded, serializable, and shrinkable simulation schedule fixture.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimSchedule {
    /// Deterministic seed identifying this schedule.
    pub seed: u64,
    /// Target protocol workload.
    pub workload: SimWorkload,
    /// Ordered fault and interleaving actions.
    pub actions: Vec<ScheduleAction>,
}

impl SimSchedule {
    /// Validate workload and all actions (`<= 256` actions per schedule).
    pub fn validate(&self) -> Result<(), Error> {
        if self.actions.len() > 256 {
            return Err(invalid("schedule exceeds 256 actions"));
        }
        self.workload.validate()?;
        for action in &self.actions {
            action.validate()?;
        }
        Ok(())
    }

    /// Serialize this schedule as a deterministic JSON fixture.
    pub fn to_json(&self) -> Result<String, Error> {
        self.validate()?;
        serde_json::to_string_pretty(self).map_err(Error::invalid_source)
    }

    /// Parse and validate a JSON schedule fixture.
    pub fn from_json(json: &str) -> Result<Self, Error> {
        let schedule: Self = serde_json::from_str(json).map_err(Error::invalid_source)?;
        schedule.validate()?;
        Ok(schedule)
    }
}

/// Final protocol and resource state after running a [`SimSchedule`] and converging recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimOutcomeSummary {
    /// Executed workload.
    pub workload: SimWorkload,
    /// Final 2PC durable decision, if applicable.
    pub two_phase_decision: Option<Decision>,
    /// Final 2PC outcome after recovery, if applicable.
    pub two_phase_outcome: Option<Outcome>,
    /// Final native mode progress, if applicable.
    pub native_progress: Option<Progress>,
    /// Final Saga outcome, if applicable.
    pub saga_outcome: Option<SagaOutcome>,
    /// Committed resource values keyed by resource or counter name.
    pub resource_values: BTreeMap<String, i64>,
    /// Final durable journal sequence watermark.
    pub durable_sequence: u64,
    /// Final authority epoch (`0` for unfenced workloads).
    pub final_epoch: u64,
    /// Number of simulated crashes triggered and recovered.
    pub crashes_recovered: usize,
    /// Number of stale-owner operations rejected by fencing.
    pub stale_operations_rejected: usize,
}

/// Complete reproducible trace produced by executing a [`SimSchedule`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimTrace {
    /// Executed schedule fixture.
    pub schedule: SimSchedule,
    /// Recorded boundary and harness events in deterministic order.
    pub events: Vec<TraceEvent>,
    /// Final converged protocol and resource summary.
    pub summary: SimOutcomeSummary,
    /// First state-machine invariant violation detected, if any.
    pub violation: Option<InvariantViolation>,
}

impl fmt::Display for SimTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "SimTrace(seed={}, workload={:?}, actions={}, events={})",
            self.schedule.seed,
            self.schedule.workload,
            self.schedule.actions.len(),
            self.events.len()
        )?;
        if let Some(v) = &self.violation {
            writeln!(f, "VIOLATION: {v}")?;
        } else {
            writeln!(f, "STATUS: all invariants satisfied")?;
        }
        writeln!(
            f,
            "SUMMARY: decision={:?} outcome={:?} native={:?} saga={:?} values={:?} seq={} epoch={}",
            self.summary.two_phase_decision,
            self.summary.two_phase_outcome,
            self.summary.native_progress,
            self.summary.saga_outcome,
            self.summary.resource_values,
            self.summary.durable_sequence,
            self.summary.final_epoch,
        )?;
        for event in &self.events {
            writeln!(f, "  {event}")?;
        }
        Ok(())
    }
}

/// Result of shrinking a failing [`SimSchedule`] to a minimal reproducing trace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShrinkResult {
    /// Original failing schedule before minimization.
    pub original_schedule: SimSchedule,
    /// Minimized schedule that still reproduces the violation.
    pub minimal_schedule: SimSchedule,
    /// Number of candidate schedules evaluated during shrinking.
    pub shrink_attempts: usize,
    /// Reproducible trace of the minimal schedule.
    pub trace: SimTrace,
    /// Violated invariant preserved by shrinking.
    pub violation: InvariantViolation,
}

impl fmt::Display for ShrinkResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "ShrinkResult: reduced {} -> {} actions in {} attempts ({})",
            self.original_schedule.actions.len(),
            self.minimal_schedule.actions.len(),
            self.shrink_attempts,
            self.violation
        )?;
        writeln!(f, "Minimal actions: {:?}", self.minimal_schedule.actions)?;
        write!(f, "{}", self.trace)
    }
}

/// Configuration bounds for [`StateExplorer`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExplorationConfig {
    /// Base seed for deterministic schedule generation.
    pub seed: u64,
    /// Maximum randomized schedules per workload (`1..=10_000`).
    pub max_schedules: usize,
    /// Maximum actions generated per randomized schedule (`1..=64`).
    pub max_actions_per_schedule: usize,
    /// Whether to include exhaustive single-boundary fault sweeps per workload.
    pub include_boundary_sweep: bool,
}

impl Default for ExplorationConfig {
    fn default() -> Self {
        Self {
            seed: 0x5041_4354_554d_0070,
            max_schedules: 16,
            max_actions_per_schedule: 6,
            include_boundary_sweep: true,
        }
    }
}

impl ExplorationConfig {
    /// Validate explorer bounds.
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_schedules == 0
            || self.max_schedules > 10_000
            || self.max_actions_per_schedule == 0
            || self.max_actions_per_schedule > 64
        {
            return Err(invalid(
                "invalid exploration bounds: require 1..=10_000 schedules and 1..=64 actions",
            ));
        }
        Ok(())
    }
}

/// Summary report from running [`StateExplorer::explore`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExplorationReport {
    /// Total schedules executed across all workloads.
    pub schedules_explored: usize,
    /// Total boundary trace events observed across all schedules.
    pub total_events: usize,
    /// Total simulated crashes triggered and recovered.
    pub total_crashes_recovered: usize,
    /// Total stale-owner calls rejected by fencing.
    pub total_stale_rejections: usize,
    /// First shrunk invariant failure if any schedule violated an invariant.
    pub failure: Option<ShrinkResult>,
}

/// Bounded state-machine and interleaving explorer over Pactum protocols.
#[derive(Clone, Debug)]
pub struct StateExplorer {
    config: ExplorationConfig,
}

impl StateExplorer {
    /// Create a validated state-machine explorer.
    pub fn new(config: ExplorationConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Borrow the explorer configuration.
    pub fn config(&self) -> &ExplorationConfig {
        &self.config
    }

    /// Deterministically generate a schedule for `workload` and `index`.
    pub fn generate_schedule(&self, workload: SimWorkload, index: usize) -> SimSchedule {
        let schedule_seed = self
            .config
            .seed
            .wrapping_add((index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut rng = SimRng::new(schedule_seed);
        let action_count = 1 + rng.next_index(self.config.max_actions_per_schedule);
        let mut actions = Vec::with_capacity(action_count);
        for _ in 0..action_count {
            let kind = rng.next_index(8);
            let step = 1 + rng.next_index(14);
            let action = match kind {
                0 => ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step,
                        component: None,
                        operation: None,
                        phase: BoundaryPhase::BeforeEffect,
                        action: FaultAction::FailBeforeEffect {
                            code: ErrorCode::DurabilityUnknown,
                        },
                    },
                },
                1 => ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step,
                        component: None,
                        operation: None,
                        phase: BoundaryPhase::AfterEffect,
                        action: FaultAction::LoseAck {
                            code: ErrorCode::DurabilityUnknown,
                        },
                    },
                },
                2 => ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step,
                        component: None,
                        operation: None,
                        phase: if rng.next_index(2) == 0 {
                            BoundaryPhase::BeforeEffect
                        } else {
                            BoundaryPhase::AfterEffect
                        },
                        action: FaultAction::CrashProcess,
                    },
                },
                3 => ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step: 0,
                        component: Some(match &workload {
                            SimWorkload::TwoPhaseCommit { .. }
                            | SimWorkload::FencedRecovery { .. } => SimComponent::Participant,
                            SimWorkload::SagaExecution { .. } => SimComponent::SagaAdapter,
                            _ => SimComponent::Backend,
                        }),
                        operation: None,
                        phase: BoundaryPhase::BeforeEffect,
                        action: FaultAction::DuplicateDelivery {
                            extra_deliveries: 1 + (rng.next_index(3) as u32),
                        },
                    },
                },
                4 => ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step,
                        component: None,
                        operation: None,
                        phase: BoundaryPhase::BeforeEffect,
                        action: FaultAction::DelayMillis {
                            millis: 50 + (rng.next_index(200) as u64),
                        },
                    },
                },
                5 => ScheduleAction::ReorderParticipantMessages {
                    seed: rng.next_u64(),
                },
                6 => ScheduleAction::CrashAndReopen,
                _ => ScheduleAction::CompactJournal {
                    archive: rng.next_index(2) == 0,
                },
            };
            actions.push(action);
        }
        SimSchedule {
            seed: schedule_seed,
            workload,
            actions,
        }
    }

    /// Explore boundary sweeps and seeded interleavings across `workloads`.
    pub async fn explore(&self, workloads: &[SimWorkload]) -> Result<ExplorationReport, Error> {
        if workloads.is_empty() {
            return Err(invalid("at least one workload is required for exploration"));
        }
        for w in workloads {
            w.validate()?;
        }
        let mut schedules_explored = 0usize;
        let mut total_events = 0usize;
        let mut total_crashes_recovered = 0usize;
        let mut total_stale_rejections = 0usize;

        for (w_idx, workload) in workloads.iter().enumerate() {
            let mut candidates = Vec::new();
            if self.config.include_boundary_sweep {
                let baseline = self
                    .run_schedule(&SimSchedule {
                        seed: self.config.seed.wrapping_add(w_idx as u64),
                        workload: workload.clone(),
                        actions: Vec::new(),
                    })
                    .await?;
                let max_step = baseline.events.len().min(20);
                for step in 1..=max_step {
                    candidates.push(SimSchedule {
                        seed: self.config.seed ^ (step as u64),
                        workload: workload.clone(),
                        actions: vec![ScheduleAction::InjectFault {
                            rule: FaultRule {
                                step,
                                component: None,
                                operation: None,
                                phase: BoundaryPhase::BeforeEffect,
                                action: FaultAction::CrashProcess,
                            },
                        }],
                    });
                    candidates.push(SimSchedule {
                        seed: self.config.seed ^ ((step as u64) << 8),
                        workload: workload.clone(),
                        actions: vec![ScheduleAction::InjectFault {
                            rule: FaultRule {
                                step,
                                component: None,
                                operation: None,
                                phase: BoundaryPhase::AfterEffect,
                                action: FaultAction::LoseAck {
                                    code: ErrorCode::DurabilityUnknown,
                                },
                            },
                        }],
                    });
                }
            }
            for idx in 0..self.config.max_schedules {
                candidates.push(
                    self.generate_schedule(
                        workload.clone(),
                        w_idx * self.config.max_schedules + idx,
                    ),
                );
            }

            for schedule in candidates {
                let trace = self.run_schedule(&schedule).await?;
                schedules_explored += 1;
                total_events += trace.events.len();
                total_crashes_recovered += trace.summary.crashes_recovered;
                total_stale_rejections += trace.summary.stale_operations_rejected;
                if trace.violation.is_some() {
                    let shrunk = self
                        .shrink_violation(&schedule)
                        .await?
                        .expect("violation reproduces during shrinking");
                    return Ok(ExplorationReport {
                        schedules_explored,
                        total_events,
                        total_crashes_recovered,
                        total_stale_rejections,
                        failure: Some(shrunk),
                    });
                }
            }
        }

        Ok(ExplorationReport {
            schedules_explored,
            total_events,
            total_crashes_recovered,
            total_stale_rejections,
            failure: None,
        })
    }

    /// Execute one [`SimSchedule`] deterministically and verify all protocol invariants.
    pub async fn run_schedule(&self, schedule: &SimSchedule) -> Result<SimTrace, Error> {
        schedule.validate()?;
        match &schedule.workload {
            SimWorkload::TwoPhaseCommit {
                reject_second,
                cancel_before_decision,
                compact_after_recovery,
            } => {
                self.run_two_phase(
                    schedule,
                    *reject_second,
                    *cancel_before_decision,
                    *compact_after_recovery,
                )
                .await
            }
            SimWorkload::NativeAtomic {
                valid_payload,
                compact_after_recovery,
            } => {
                self.run_native_atomic(schedule, *valid_payload, *compact_after_recovery)
                    .await
            }
            SimWorkload::NativeChunked {
                chunks,
                reject_last,
                pause_after_first,
            } => {
                self.run_native_chunked(schedule, *chunks, *reject_last, *pause_after_first)
                    .await
            }
            SimWorkload::NativeStagedPublish {
                expected_generation,
                valid_payload,
            } => {
                self.run_native_publish(schedule, *expected_generation, *valid_payload)
                    .await
            }
            SimWorkload::SagaExecution {
                fail_second_forward,
                uncertain_first_forward,
                irreversible_first,
            } => {
                self.run_saga(
                    schedule,
                    *fail_second_forward,
                    *uncertain_first_forward,
                    *irreversible_first,
                )
                .await
            }
            SimWorkload::FencedRecovery {
                takeover_before_decision,
                delayed_stale_calls,
            } => {
                self.run_fenced_recovery(schedule, *takeover_before_decision, *delayed_stale_calls)
                    .await
            }
        }
    }

    /// Shrink a schedule that triggers a built-in [`InvariantViolation`].
    pub async fn shrink_violation(
        &self,
        schedule: &SimSchedule,
    ) -> Result<Option<ShrinkResult>, Error> {
        self.shrink_with(schedule, |trace| trace.violation.clone())
            .await
    }

    /// Deterministically shrink `schedule` to a minimal action sequence satisfying `predicate`.
    pub async fn shrink_with<F>(
        &self,
        schedule: &SimSchedule,
        mut predicate: F,
    ) -> Result<Option<ShrinkResult>, Error>
    where
        F: FnMut(&SimTrace) -> Option<InvariantViolation>,
    {
        schedule.validate()?;
        let initial_trace = self.run_schedule(schedule).await?;
        let Some(mut best_violation) = predicate(&initial_trace) else {
            return Ok(None);
        };
        let mut best_schedule = schedule.clone();
        let mut best_trace = initial_trace;
        let mut attempts = 0usize;

        // Delta-debugging chunk removal followed by single-action elimination.
        let mut chunk_size = best_schedule.actions.len() / 2;
        while chunk_size > 0 {
            let mut idx = 0;
            while idx + chunk_size <= best_schedule.actions.len() {
                let mut candidate_actions = best_schedule.actions.clone();
                candidate_actions.drain(idx..idx + chunk_size);
                let candidate = SimSchedule {
                    seed: best_schedule.seed,
                    workload: best_schedule.workload.clone(),
                    actions: candidate_actions,
                };
                attempts += 1;
                let trace = self.run_schedule(&candidate).await?;
                if let Some(v) = predicate(&trace) {
                    best_schedule = candidate;
                    best_trace = trace;
                    best_violation = v;
                } else {
                    idx += 1;
                }
            }
            chunk_size /= 2;
        }

        let mut idx = 0;
        while idx < best_schedule.actions.len() {
            let mut candidate_actions = best_schedule.actions.clone();
            candidate_actions.remove(idx);
            let candidate = SimSchedule {
                seed: best_schedule.seed,
                workload: best_schedule.workload.clone(),
                actions: candidate_actions,
            };
            attempts += 1;
            let trace = self.run_schedule(&candidate).await?;
            if let Some(v) = predicate(&trace) {
                best_schedule = candidate;
                best_trace = trace;
                best_violation = v;
            } else {
                idx += 1;
            }
        }

        // Simplify duplicate counts or delays in remaining actions.
        for i in 0..best_schedule.actions.len() {
            let simplified = match &best_schedule.actions[i] {
                ScheduleAction::InjectFault {
                    rule:
                        FaultRule {
                            step,
                            component,
                            operation,
                            phase,
                            action: FaultAction::DuplicateDelivery { extra_deliveries },
                        },
                } if *extra_deliveries > 1 => Some(ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step: *step,
                        component: *component,
                        operation: operation.clone(),
                        phase: *phase,
                        action: FaultAction::DuplicateDelivery {
                            extra_deliveries: 1,
                        },
                    },
                }),
                _ => None,
            };
            if let Some(sim_action) = simplified {
                let mut candidate = best_schedule.clone();
                candidate.actions[i] = sim_action;
                attempts += 1;
                let trace = self.run_schedule(&candidate).await?;
                if let Some(v) = predicate(&trace) {
                    best_schedule = candidate;
                    best_trace = trace;
                    best_violation = v;
                }
            }
        }

        Ok(Some(ShrinkResult {
            original_schedule: schedule.clone(),
            minimal_schedule: best_schedule,
            shrink_attempts: attempts,
            trace: best_trace,
            violation: best_violation,
        }))
    }
}

impl StateExplorer {
    fn fast_retry_policy() -> RetryPolicy {
        RetryPolicy {
            attempts: 2,
            timeout: Duration::from_millis(100),
            backoff: Duration::ZERO,
        }
    }

    async fn run_two_phase(
        &self,
        schedule: &SimSchedule,
        reject_second: bool,
        cancel_before_decision: bool,
        compact_after_recovery: bool,
    ) -> Result<SimTrace, Error> {
        let clock = SimClock::new(1_000);
        let controller = SimController::new(clock.clone(), None);
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone())?;
            }
        }
        let journal_store = SimJournalStore::new("sim-2pc", controller.clone())?;
        let part_a = SimParticipantStore::new(
            ParticipantId::new("a")?,
            controller.clone(),
            Some(journal_store.clone()),
        );
        let part_b = SimParticipantStore::new(
            ParticipantId::new("b")?,
            controller.clone(),
            Some(journal_store.clone()),
        );
        let tx = TransactionId::new("sim-tx")?;
        let requests = vec![
            Enlistment {
                participant: part_b.id(),
                payload: if reject_second {
                    b"-1".to_vec()
                } else {
                    b"10".to_vec()
                },
            },
            Enlistment {
                participant: part_a.id(),
                payload: b"10".to_vec(),
            },
        ];

        let mut violation: Option<InvariantViolation> = None;

        // Primary execution pass (may encounter injected faults/crashes).
        if let Ok(journal) = journal_store.open()
            && let Ok(mut coord) = Coordinator::open(
                journal,
                vec![part_a.handle(), part_b.handle()],
                Self::fast_retry_policy(),
            )
        {
            let cancel = Cancellation::default();
            if cancel_before_decision {
                cancel.cancel();
            }
            let exec_res = coord.execute(tx.clone(), requests.clone(), &cancel).await;
            if matches!(exec_res, Err(Error::InDoubt { .. }))
                && (!coord.diagnostics().reopen_required
                    || !matches!(coord.status(&tx), Err(Error::InDoubt { .. })))
            {
                violation = Some(InvariantViolation {
                    invariant: Invariant::PoisonedOnUncertainty,
                    step: controller.step_count(),
                    detail: "coordinator did not poison handle after uncertain journal write"
                        .into(),
                });
            }
        }
        journal_store.crash_active_session();
        controller.clear_crash();

        // Execute post-fault schedule actions (clock advance, message reorder, crash/reopen).
        for action in &schedule.actions {
            match action {
                ScheduleAction::AdvanceClock { millis } => {
                    let _ = clock.advance_millis(*millis);
                }
                ScheduleAction::ReorderParticipantMessages { seed } => {
                    let existing_decision = journal_store.observed_decision(&tx);
                    if existing_decision == Some(Decision::Commit) {
                        let token = tx.as_str().as_bytes().to_vec();
                        part_a.enqueue_message(SimParticipantMessage::Commit {
                            tx: tx.clone(),
                            token: token.clone(),
                        });
                        part_a.enqueue_message(SimParticipantMessage::Prepare {
                            tx: tx.clone(),
                            payload: b"10".to_vec(),
                        });
                    } else if existing_decision == Some(Decision::Abort) {
                        part_a.enqueue_message(SimParticipantMessage::Abort {
                            tx: tx.clone(),
                            token: None,
                        });
                        part_a.enqueue_message(SimParticipantMessage::Prepare {
                            tx: tx.clone(),
                            payload: b"10".to_vec(),
                        });
                    }
                    let _ = part_a.drain_reordered(*seed).await;
                }
                ScheduleAction::CrashAndReopen => {
                    journal_store.crash_active_session();
                    controller.clear_crash();
                }
                _ => {}
            }
        }

        // Clean recovery pass on reopened journal.
        controller.reset_for_recovery();
        let decision_before_recovery = journal_store.observed_decision(&tx);
        let mut coordinator = Coordinator::open(
            journal_store.open()?,
            vec![part_a.handle(), part_b.handle()],
            Self::fast_retry_policy(),
        )?;
        let statuses = coordinator.recover(&Cancellation::default()).await?;
        let status = coordinator.status(&tx)?;

        if (compact_after_recovery
            || schedule
                .actions
                .iter()
                .any(|a| matches!(a, ScheduleAction::CompactJournal { .. })))
            && journal_store.head_sequence() > 0
        {
            let archive = schedule
                .actions
                .iter()
                .any(|a| matches!(a, ScheduleAction::CompactJournal { archive: true }));
            coordinator.compact(if archive {
                Retention::Archive
            } else {
                Retention::Compact
            })?;
            drop(coordinator);
            coordinator = Coordinator::open(
                journal_store.open()?,
                vec![part_a.handle(), part_b.handle()],
                Self::fast_retry_policy(),
            )?;
            // Second recovery via RecoveryTarget trait if any work remained or to verify idempotency.
            let pending = RecoveryTarget::pending_work(&coordinator)?;
            if !pending.is_empty() && violation.is_none() {
                violation = Some(InvariantViolation {
                    invariant: Invariant::DurableReceiptConsistency,
                    step: controller.step_count(),
                    detail: "pending work remained after clean 2PC recovery and compaction".into(),
                });
            }
            let _ = coordinator.recover(&Cancellation::default()).await?;
        }

        // Check journal/participant recorded invariant violations.
        if violation.is_none() {
            let j_guard = journal_store.inner.lock().unwrap();
            if let Some(conflict) = &j_guard.decision_conflict {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ImmutableDecision,
                    step: controller.step_count(),
                    detail: conflict.clone(),
                });
            } else if let Some(illegal) = &j_guard.illegal_decision_without_prepare {
                violation = Some(InvariantViolation {
                    invariant: Invariant::DecisionBarrierPrecedesFinalization,
                    step: controller.step_count(),
                    detail: illegal.clone(),
                });
            }
        }
        if violation.is_none() {
            for store in [&part_a, &part_b] {
                let p_guard = store.inner.lock().unwrap();
                if let Some(premature) = &p_guard.premature_finalization {
                    violation = Some(InvariantViolation {
                        invariant: Invariant::DecisionBarrierPrecedesFinalization,
                        step: controller.step_count(),
                        detail: premature.clone(),
                    });
                    break;
                }
            }
        }

        let final_status = coordinator.status(&tx)?;
        let final_decision = final_status.as_ref().and_then(|s| s.decision);
        let final_outcome = final_status.as_ref().map(|s| s.outcome.clone());

        if violation.is_none()
            && let Some(prev) = decision_before_recovery
            && final_decision != Some(prev)
        {
            violation = Some(InvariantViolation {
                invariant: Invariant::ImmutableDecision,
                step: controller.step_count(),
                detail: format!(
                    "recovery changed durable decision from {prev:?} to {final_decision:?}"
                ),
            });
        }

        // Verify ExactlyOnceEffects and DurableReceiptConsistency.
        let expected_value = if final_decision == Some(Decision::Commit) {
            10
        } else {
            0
        };
        if violation.is_none() {
            if part_a.commit_effect_count(&tx) > 1 || part_b.commit_effect_count(&tx) > 1 {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ExactlyOnceEffects,
                    step: controller.step_count(),
                    detail: format!(
                        "participant commit applied multiple times: a={}, b={}",
                        part_a.commit_effect_count(&tx),
                        part_b.commit_effect_count(&tx)
                    ),
                });
            } else if part_a.value() != expected_value || part_b.value() != expected_value {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ExactlyOnceEffects,
                    step: controller.step_count(),
                    detail: format!(
                        "counter values ({}, {}) do not match expected {} for decision {:?}",
                        part_a.value(),
                        part_b.value(),
                        expected_value,
                        final_decision
                    ),
                });
            } else if let Some(s) = &status
                && s.outcome == Outcome::InDoubt
            {
                violation = Some(InvariantViolation {
                    invariant: Invariant::DurableReceiptConsistency,
                    step: controller.step_count(),
                    detail: format!(
                        "transaction remained InDoubt after clean recovery: {statuses:?}"
                    ),
                });
            }
        }

        let summary = SimOutcomeSummary {
            workload: schedule.workload.clone(),
            two_phase_decision: final_decision,
            two_phase_outcome: final_outcome,
            native_progress: None,
            saga_outcome: None,
            resource_values: BTreeMap::from([
                ("a".into(), part_a.value()),
                ("b".into(), part_b.value()),
            ]),
            durable_sequence: journal_store.head_sequence(),
            final_epoch: 0,
            crashes_recovered: controller.crashes_triggered(),
            stale_operations_rejected: 0,
        };
        Ok(SimTrace {
            schedule: schedule.clone(),
            events: controller.events(),
            summary,
            violation,
        })
    }

    async fn run_native_atomic(
        &self,
        schedule: &SimSchedule,
        valid_payload: bool,
        compact_after_recovery: bool,
    ) -> Result<SimTrace, Error> {
        let clock = SimClock::new(1_000);
        let controller = SimController::new(clock, None);
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone())?;
            }
        }
        let journal_store = SimJournalStore::new("sim-native-atomic", controller.clone())?;
        let backend_store = SimBackendStore::new("sim-backend:v1", controller.clone())?;
        let op_id = OperationId::new("atomic-op")?;
        let payload = if valid_payload {
            serde_json::to_vec(&vec![SimValue {
                key: "counter".into(),
                value: 7,
            }])
            .unwrap()
        } else {
            serde_json::to_vec(&vec![
                SimValue {
                    key: "counter".into(),
                    value: 7,
                },
                SimValue {
                    key: "overflow".into(),
                    value: i64::MAX,
                },
                // Duplicate key triggers decode rejection -> definite rollback
                SimValue {
                    key: "overflow".into(),
                    value: 1,
                },
            ])
            .unwrap()
        };
        let request = Request::Atomic(AtomicRequest { payload });
        let mut violation = None;

        if let Ok(journal) = journal_store.open()
            && let Ok(mut engine) =
                Engine::open(journal, backend_store.open(), ExecutionPolicy::default())
        {
            let res = engine
                .execute(op_id.clone(), request.clone(), &Cancellation::default())
                .await;
            if matches!(res, Err(Error::InDoubt { .. }))
                && !matches!(engine.status(&op_id), Err(Error::InDoubt { .. }))
            {
                violation = Some(InvariantViolation {
                    invariant: Invariant::PoisonedOnUncertainty,
                    step: controller.step_count(),
                    detail: "native engine did not poison handle on journal write error".into(),
                });
            }
        }
        journal_store.crash_active_session();
        controller.reset_for_recovery();

        let mut engine = Engine::open(
            journal_store.open()?,
            backend_store.open(),
            ExecutionPolicy::default(),
        )?;
        let _ = engine.recover(&Cancellation::default()).await?;
        let status = engine
            .execute(op_id.clone(), request, &Cancellation::default())
            .await?;
        if compact_after_recovery {
            engine.compact(Retention::Compact)?;
            drop(engine);
            engine = Engine::open(
                journal_store.open()?,
                backend_store.open(),
                ExecutionPolicy::default(),
            )?;
            let after_compact = engine.status(&op_id)?.expect("retained after compact");
            if after_compact.progress != status.progress && violation.is_none() {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ImmutableDecision,
                    step: controller.step_count(),
                    detail: "native atomic progress changed across compaction".into(),
                });
            }
        }

        let expected = if valid_payload { 7 } else { 0 };
        let expected_progress = Progress::Atomic(if valid_payload {
            AtomicStatus::Committed
        } else {
            AtomicStatus::Aborted
        });
        if violation.is_none() {
            if status.progress != expected_progress {
                violation = Some(InvariantViolation {
                    invariant: Invariant::DurableReceiptConsistency,
                    step: controller.step_count(),
                    detail: format!(
                        "unexpected atomic progress {:?}, expected {:?}",
                        status.progress, expected_progress
                    ),
                });
            } else if backend_store.value("counter") != expected {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ExactlyOnceEffects,
                    step: controller.step_count(),
                    detail: format!(
                        "atomic counter value {} != expected {}",
                        backend_store.value("counter"),
                        expected
                    ),
                });
            }
        }

        let summary = SimOutcomeSummary {
            workload: schedule.workload.clone(),
            two_phase_decision: None,
            two_phase_outcome: None,
            native_progress: Some(status.progress),
            saga_outcome: None,
            resource_values: BTreeMap::from([("counter".into(), backend_store.value("counter"))]),
            durable_sequence: journal_store.head_sequence(),
            final_epoch: 0,
            crashes_recovered: controller.crashes_triggered(),
            stale_operations_rejected: 0,
        };
        Ok(SimTrace {
            schedule: schedule.clone(),
            events: controller.events(),
            summary,
            violation,
        })
    }

    async fn run_native_chunked(
        &self,
        schedule: &SimSchedule,
        chunks: usize,
        reject_last: bool,
        pause_after_first: bool,
    ) -> Result<SimTrace, Error> {
        let clock = SimClock::new(1_000);
        let controller = SimController::new(clock, None);
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone())?;
            }
        }
        let journal_store = SimJournalStore::new("sim-native-chunked", controller.clone())?;
        let backend_store = SimBackendStore::new("sim-backend:v1", controller.clone())?;
        let op_id = OperationId::new("chunked-op")?;

        let mut chunk_vec = Vec::with_capacity(chunks);
        let mut expected_total = 0i64;
        for idx in 0..chunks {
            if reject_last && idx + 1 == chunks {
                chunk_vec.push(b"invalid-chunk-payload".to_vec());
            } else {
                let delta = (idx as i64) + 2;
                expected_total += delta;
                chunk_vec.push(
                    serde_json::to_vec(&vec![SimValue {
                        key: "counter".into(),
                        value: delta,
                    }])
                    .unwrap(),
                );
            }
        }
        let request = Request::Chunked(ChunkedRequest { chunks: chunk_vec });
        let policy = ExecutionPolicy {
            max_chunks: if pause_after_first { 1 } else { usize::MAX },
            ..ExecutionPolicy::default()
        };

        if let Ok(journal) = journal_store.open()
            && let Ok(mut engine) = Engine::open(journal, backend_store.open(), policy)
        {
            let _ = engine
                .execute(op_id.clone(), request.clone(), &Cancellation::default())
                .await;
            if pause_after_first {
                let _ = engine.cancel(&op_id).await;
            }
        }
        journal_store.crash_active_session();
        controller.reset_for_recovery();

        let mut engine = Engine::open(
            journal_store.open()?,
            backend_store.open(),
            ExecutionPolicy::default(),
        )?;
        let _ = engine.recover(&Cancellation::default()).await?;
        let status: OperationStatus = engine
            .execute(op_id.clone(), request, &Cancellation::default())
            .await?;

        let mut violation = None;
        let expected_progress = Progress::Chunked(ChunkStatus {
            completed: if reject_last { chunks - 1 } else { chunks },
            total: chunks,
            uncertain: false,
            paused: false,
            rejected: reject_last,
        });
        if status.progress != expected_progress {
            violation = Some(InvariantViolation {
                invariant: Invariant::DurableReceiptConsistency,
                step: controller.step_count(),
                detail: format!(
                    "chunked progress {:?} != expected {:?}",
                    status.progress, expected_progress
                ),
            });
        } else if backend_store.value("counter") != expected_total {
            violation = Some(InvariantViolation {
                invariant: Invariant::ExactlyOnceEffects,
                step: controller.step_count(),
                detail: format!(
                    "chunked counter {} != expected {}",
                    backend_store.value("counter"),
                    expected_total
                ),
            });
        }

        let summary = SimOutcomeSummary {
            workload: schedule.workload.clone(),
            two_phase_decision: None,
            two_phase_outcome: None,
            native_progress: Some(status.progress),
            saga_outcome: None,
            resource_values: BTreeMap::from([("counter".into(), backend_store.value("counter"))]),
            durable_sequence: journal_store.head_sequence(),
            final_epoch: 0,
            crashes_recovered: controller.crashes_triggered(),
            stale_operations_rejected: 0,
        };
        Ok(SimTrace {
            schedule: schedule.clone(),
            events: controller.events(),
            summary,
            violation,
        })
    }

    async fn run_native_publish(
        &self,
        schedule: &SimSchedule,
        expected_generation: u64,
        valid_payload: bool,
    ) -> Result<SimTrace, Error> {
        let clock = SimClock::new(1_000);
        let controller = SimController::new(clock, None);
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone())?;
            }
        }
        let journal_store = SimJournalStore::new("sim-native-publish", controller.clone())?;
        let backend_store = SimBackendStore::new("sim-backend:v1", controller.clone())?;
        let op_id = OperationId::new("publish-op")?;
        let target_id = OperationId::new("visible")?;
        let payload = if valid_payload {
            serde_json::to_vec(&vec![SimValue {
                key: "snapshot".into(),
                value: 42,
            }])
            .unwrap()
        } else {
            b"invalid-artifact".to_vec()
        };
        let request = Request::StagedPublish(PublishRequest {
            target: target_id.clone(),
            expected_generation,
            payload,
        });

        if let Ok(journal) = journal_store.open()
            && let Ok(mut engine) =
                Engine::open(journal, backend_store.open(), ExecutionPolicy::default())
        {
            let _ = engine
                .execute(op_id.clone(), request.clone(), &Cancellation::default())
                .await;
        }
        journal_store.crash_active_session();
        controller.reset_for_recovery();

        let mut engine = Engine::open(
            journal_store.open()?,
            backend_store.open(),
            ExecutionPolicy::default(),
        )?;
        let _ = engine.recover(&Cancellation::default()).await?;
        let status = engine
            .execute(op_id.clone(), request, &Cancellation::default())
            .await?;

        let expected_status = if !valid_payload {
            PublishStatus::Aborted
        } else if expected_generation == 0 {
            PublishStatus::Published
        } else {
            PublishStatus::Conflict
        };
        let mut violation = None;
        if status.progress != Progress::StagedPublish(expected_status) {
            violation = Some(InvariantViolation {
                invariant: Invariant::DurableReceiptConsistency,
                step: controller.step_count(),
                detail: format!(
                    "publish progress {:?} != expected {:?}",
                    status.progress, expected_status
                ),
            });
        } else if expected_status == PublishStatus::Published {
            if backend_store.visible(&target_id).is_none() || !backend_store.has_artifact(&op_id) {
                violation = Some(InvariantViolation {
                    invariant: Invariant::DurableReceiptConsistency,
                    step: controller.step_count(),
                    detail: "published artifact missing from visible pointer or storage".into(),
                });
            }
        } else if backend_store.visible(&target_id).is_some() || backend_store.has_artifact(&op_id)
        {
            violation = Some(InvariantViolation {
                invariant: Invariant::ExactlyOnceEffects,
                step: controller.step_count(),
                detail: "aborted or conflicted publish left visible state or orphan artifact"
                    .into(),
            });
        }

        let visible_val = backend_store
            .visible(&target_id)
            .and_then(|v| v.values.first().map(|item| item.value))
            .unwrap_or(0);
        let summary = SimOutcomeSummary {
            workload: schedule.workload.clone(),
            two_phase_decision: None,
            two_phase_outcome: None,
            native_progress: Some(status.progress),
            saga_outcome: None,
            resource_values: BTreeMap::from([("visible".into(), visible_val)]),
            durable_sequence: journal_store.head_sequence(),
            final_epoch: 0,
            crashes_recovered: controller.crashes_triggered(),
            stale_operations_rejected: 0,
        };
        Ok(SimTrace {
            schedule: schedule.clone(),
            events: controller.events(),
            summary,
            violation,
        })
    }

    async fn run_saga(
        &self,
        schedule: &SimSchedule,
        fail_second_forward: bool,
        uncertain_first_forward: bool,
        irreversible_first: bool,
    ) -> Result<SimTrace, Error> {
        let clock = SimClock::new(1_000);
        let controller = SimController::new(clock, None);
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone())?;
            }
        }
        let journal_store = SimJournalStore::new("sim-saga", controller.clone())?;
        let saga_store = SimSagaStore::new("sim-saga-adapter:v1", controller.clone())?;
        if uncertain_first_forward {
            saga_store.set_behavior(
                b"10".to_vec(),
                SimSagaBehavior::UncertainThenApply { uncertainties: 1 },
            );
        }
        if fail_second_forward {
            saga_store.set_behavior(
                b"20".to_vec(),
                SimSagaBehavior::Reject {
                    reason: "step 2 business rejection".into(),
                },
            );
        }
        let adapter_id = AdapterId::new("ledger")?;
        let step1 = StepId::new("step-1")?;
        let step2 = StepId::new("step-2")?;
        let spec = Saga {
            version: 1,
            id: SagaId::new("saga-1")?,
            revision: 1,
            steps: vec![
                SagaStep {
                    id: step1.clone(),
                    dependencies: BTreeSet::new(),
                    forward: SagaOperation {
                        adapter: adapter_id.clone(),
                        payload: b"10".to_vec(),
                    },
                    compensation: if irreversible_first {
                        Compensation::Irreversible
                    } else {
                        Compensation::Operation(SagaOperation {
                            adapter: adapter_id.clone(),
                            payload: b"-10".to_vec(),
                        })
                    },
                },
                SagaStep {
                    id: step2,
                    dependencies: BTreeSet::from([step1.clone()]),
                    forward: SagaOperation {
                        adapter: adapter_id.clone(),
                        payload: b"20".to_vec(),
                    },
                    compensation: Compensation::Operation(SagaOperation {
                        adapter: adapter_id.clone(),
                        payload: b"-20".to_vec(),
                    }),
                },
            ],
            irreversible: if irreversible_first {
                IrreversiblePolicy::Allow
            } else {
                IrreversiblePolicy::Reject
            },
            retry_limit: 4,
        };
        let namespace = SagaId::new("sim-saga-ns")?;
        let policy = SagaPolicy {
            max_dispatches: 64,
            timeout: Duration::from_millis(100),
            retry_backoff: Duration::ZERO,
        };

        if let Ok(journal) = journal_store.open()
            && let Ok(mut engine) = SagaEngine::open(
                journal,
                namespace.clone(),
                BTreeMap::from([(
                    adapter_id.clone(),
                    saga_store.handle() as Arc<dyn SagaAdapter>,
                )]),
                policy.clone(),
            )
        {
            let _ = engine.execute(spec.clone(), &Cancellation::default()).await;
        }
        journal_store.crash_active_session();
        controller.reset_for_recovery();

        let mut engine = SagaEngine::open(
            journal_store.open()?,
            namespace,
            BTreeMap::from([(adapter_id, saga_store.handle() as Arc<dyn SagaAdapter>)]),
            policy,
        )?;
        let _ = engine.recover_pending(&Cancellation::default()).await?;
        let mut status = engine
            .execute(spec.clone(), &Cancellation::default())
            .await?;
        if status.outcome == SagaOutcome::Uncertain {
            let _ = engine.retry(&spec.id, "sim reconcile");
            status = engine.execute(spec, &Cancellation::default()).await?;
        }

        let expected_outcome = if !fail_second_forward {
            SagaOutcome::Completed
        } else if irreversible_first {
            SagaOutcome::NeedsIntervention
        } else {
            SagaOutcome::Compensated
        };
        let expected_net = if !fail_second_forward {
            30
        } else if irreversible_first {
            10
        } else {
            0
        };
        let mut violation = None;
        if status.outcome != expected_outcome {
            violation = Some(InvariantViolation {
                invariant: Invariant::DurableReceiptConsistency,
                step: controller.step_count(),
                detail: format!(
                    "saga outcome {:?} != expected {:?}",
                    status.outcome, expected_outcome
                ),
            });
        } else if saga_store.net_value() != expected_net {
            violation = Some(InvariantViolation {
                invariant: Invariant::ExactlyOnceEffects,
                step: controller.step_count(),
                detail: format!(
                    "saga net value {} != expected {}",
                    saga_store.net_value(),
                    expected_net
                ),
            });
        }

        let summary = SimOutcomeSummary {
            workload: schedule.workload.clone(),
            two_phase_decision: None,
            two_phase_outcome: None,
            native_progress: None,
            saga_outcome: Some(status.outcome),
            resource_values: BTreeMap::from([("net".into(), saga_store.net_value())]),
            durable_sequence: journal_store.head_sequence(),
            final_epoch: 0,
            crashes_recovered: controller.crashes_triggered(),
            stale_operations_rejected: 0,
        };
        Ok(SimTrace {
            schedule: schedule.clone(),
            events: controller.events(),
            summary,
            violation,
        })
    }

    async fn run_fenced_recovery(
        &self,
        schedule: &SimSchedule,
        takeover_before_decision: bool,
        delayed_stale_calls: bool,
    ) -> Result<SimTrace, Error> {
        let clock = SimClock::new(1_000);
        let authority = SimAuthority::new("fenced-scope", clock.clone())?;
        let controller = SimController::new(clock.clone(), Some(authority.clone()));
        for action in &schedule.actions {
            if let ScheduleAction::InjectFault { rule } = action {
                controller.add_rule(rule.clone())?;
            }
        }
        if takeover_before_decision {
            // Expire owner-1 right after participant prepare (before Decided is flushed).
            controller.add_rule(FaultRule {
                step: 0,
                component: Some(SimComponent::Participant),
                operation: Some("prepare".into()),
                phase: BoundaryPhase::AfterEffect,
                action: FaultAction::ExpireLease {
                    advance_millis: 1_000,
                    takeover_owner: "owner-2".into(),
                    ttl_millis: 5_000,
                },
            })?;
        } else {
            // Expire owner-1 right after Decided(Commit) is flushed (before finalization ack).
            controller.add_rule(FaultRule {
                step: 0,
                component: Some(SimComponent::Participant),
                operation: Some("commit".into()),
                phase: BoundaryPhase::BeforeEffect,
                action: FaultAction::ExpireLease {
                    advance_millis: 1_000,
                    takeover_owner: "owner-2".into(),
                    ttl_millis: 5_000,
                },
            })?;
        }

        let journal_store = SimJournalStore::new("fenced-scope", controller.clone())?;
        let part_store = SimParticipantStore::new(
            ParticipantId::new("counter")?,
            controller.clone(),
            Some(journal_store.clone()),
        );
        let tx = TransactionId::new("fenced-tx")?;
        let requests = vec![Enlistment {
            participant: part_store.id(),
            payload: b"7".to_vec(),
        }];

        let grant1 = authority.acquire("owner-1", 500)?;
        let mut stale_journal = journal_store.open_fenced(grant1.clone())?;
        let stale_part = part_store.fenced_handle(grant1.clone());

        if let Ok(j1) = journal_store.open_fenced(grant1.clone())
            && let Ok(mut coord1) = Coordinator::open(
                j1,
                vec![part_store.fenced_handle(grant1.clone())],
                Self::fast_retry_policy(),
            )
        {
            let _ = coord1
                .execute(tx.clone(), requests.clone(), &Cancellation::default())
                .await;
        }
        controller.reset_for_recovery();

        // Acquire or retrieve the takeover grant.
        let grant2 = if let Some(taken) = controller.latest_takeover() {
            authority.renew(&taken, 5_000)?;
            taken
        } else {
            clock.advance_millis(1_000)?;
            authority.acquire("owner-2", 5_000)?
        };

        let mut violation = None;
        if delayed_stale_calls {
            // Delayed stale owner attempts must all be rejected with StaleOwner.
            let _ = stale_journal.append(1, b"stale staged record");
            if stale_journal.flush().is_ok()
                || stale_part.prepare(&tx, b"7").await.is_ok()
                || stale_part.commit(&tx, tx.as_str().as_bytes()).await.is_ok()
                || stale_part.abort(&tx, None).await.is_ok()
            {
                violation = Some(InvariantViolation {
                    invariant: Invariant::FencingSafety,
                    step: controller.step_count(),
                    detail: "stale-epoch owner succeeded in mutating journal or participant state"
                        .into(),
                });
            }
        }

        let decision_before_takeover = journal_store.observed_decision(&tx);
        let mut coord2 = Coordinator::open(
            journal_store.open_fenced(grant2.clone())?,
            vec![part_store.fenced_handle(grant2)],
            Self::fast_retry_policy(),
        )?;
        let _ = coord2.recover(&Cancellation::default()).await?;
        let final_status = coord2.status(&tx)?;
        let final_decision = final_status.as_ref().and_then(|s| s.decision);
        let final_outcome = final_status.as_ref().map(|s| s.outcome.clone());

        if violation.is_none() {
            if let Some(prev) = decision_before_takeover
                && final_decision != Some(prev)
            {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ImmutableDecision,
                    step: controller.step_count(),
                    detail: format!(
                        "fenced takeover changed durable decision from {prev:?} to {final_decision:?}"
                    ),
                });
            }
            let expected_val = if final_decision == Some(Decision::Commit) {
                7
            } else {
                0
            };
            if part_store.value() != expected_val || part_store.commit_effect_count(&tx) > 1 {
                violation = Some(InvariantViolation {
                    invariant: Invariant::ExactlyOnceEffects,
                    step: controller.step_count(),
                    detail: format!(
                        "fenced counter value {} != expected {} (commits={})",
                        part_store.value(),
                        expected_val,
                        part_store.commit_effect_count(&tx)
                    ),
                });
            }
        }

        let summary = SimOutcomeSummary {
            workload: schedule.workload.clone(),
            two_phase_decision: final_decision,
            two_phase_outcome: final_outcome,
            native_progress: None,
            saga_outcome: None,
            resource_values: BTreeMap::from([("counter".into(), part_store.value())]),
            durable_sequence: journal_store.head_sequence(),
            final_epoch: authority.current_epoch(),
            crashes_recovered: controller.crashes_triggered(),
            stale_operations_rejected: authority.stale_rejections(),
        };
        Ok(SimTrace {
            schedule: schedule.clone(),
            events: controller.events(),
            summary,
            violation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_clock_and_authority_enforce_monotonic_time_and_epoch_fencing() {
        let clock = SimClock::new(500);
        assert_eq!(clock.now_millis().unwrap(), 500);
        assert!(clock.set_millis(499).is_err());
        assert_eq!(clock.advance_millis(250).unwrap(), 750);
        clock.inject_fault(Some(ErrorCode::Unavailable));
        assert!(clock.now_millis().is_err());
        assert_eq!(clock.now_millis().unwrap(), 750);

        let auth = SimAuthority::new("scope", clock.clone()).unwrap();
        let g1 = auth.acquire("owner-1", 100).unwrap();
        assert_eq!(g1.epoch, 1);
        assert!(auth.acquire("owner-2", 100).is_err());
        clock.advance_millis(100).unwrap();
        assert!(auth.validate(&g1).is_err());
        let g2 = auth.acquire("owner-2", 200).unwrap();
        assert_eq!(g2.epoch, 2);
        assert!(auth.renew(&g1, 100).is_err());
        auth.renew(&g2, 500).unwrap();
        auth.release(&g2).unwrap();
        assert!(auth.validate(&g2).is_err());
        assert!(auth.stale_rejections() >= 3);
    }

    #[test]
    fn sim_journal_satisfies_conformance_and_discards_unflushed_staging_on_crash() {
        let controller = SimController::default();
        let store = SimJournalStore::new("conformance", controller).unwrap();
        let mut journal = store.open().unwrap();
        pactum_journal::check_conformance(&mut journal).unwrap();
        assert!(store.open().is_err()); // Exclusive unfenced owner lock.
        journal.append(1, b"unflushed").unwrap();
        store.crash_active_session();
        drop(journal);
        assert_eq!(store.discarded_staged_records(), 1);
        let mut reopened = store.open().unwrap();
        assert_eq!(reopened.replay().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn state_explorer_verifies_canonical_suite_and_shrinks_synthetic_violation() {
        let explorer = StateExplorer::new(ExplorationConfig {
            seed: 42,
            max_schedules: 4,
            max_actions_per_schedule: 4,
            include_boundary_sweep: true,
        })
        .unwrap();
        let report = explorer
            .explore(&SimWorkload::canonical_suite())
            .await
            .unwrap();
        assert!(report.failure.is_none(), "{:?}", report.failure);
        assert!(report.schedules_explored > 50);

        // Verify deterministic replay and shrinking against a synthetic predicate.
        let schedule = SimSchedule {
            seed: 99,
            workload: SimWorkload::TwoPhaseCommit {
                reject_second: false,
                cancel_before_decision: false,
                compact_after_recovery: true,
            },
            actions: vec![
                ScheduleAction::AdvanceClock { millis: 10 },
                ScheduleAction::InjectFault {
                    rule: FaultRule {
                        step: 1,
                        component: None,
                        operation: None,
                        phase: BoundaryPhase::BeforeEffect,
                        action: FaultAction::CrashProcess,
                    },
                },
                ScheduleAction::ExtraRecoveryPass,
                ScheduleAction::CompactJournal { archive: false },
            ],
        };
        let json = schedule.to_json().unwrap();
        let decoded = SimSchedule::from_json(&json).unwrap();
        assert_eq!(schedule, decoded);
        let trace1 = explorer.run_schedule(&schedule).await.unwrap();
        let trace2 = explorer.run_schedule(&decoded).await.unwrap();
        assert_eq!(trace1, trace2);

        let shrunk = explorer
            .shrink_with(&schedule, |t| {
                (t.summary.crashes_recovered > 0).then(|| InvariantViolation {
                    invariant: Invariant::ImmutableDecision,
                    step: 2,
                    detail: "synthetic crash detector".into(),
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(shrunk.minimal_schedule.actions.len(), 1);
        assert!(shrunk.to_string().contains("synthetic crash detector"));
    }
}
