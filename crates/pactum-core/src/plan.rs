//! Bounded durable transaction DAGs. Each step retains its own mode guarantee;
//! a completed plan is never a distributed atomic commit.
use crate::execution::{
    AtomicStatus, Backend, Engine, Execution, Guarantees, Mode, OperationId, Progress,
    PublishStatus, Request,
};
use crate::{Cancellation, Coordinator, Decision, Enlistment, Error, Outcome, TransactionId};
use async_trait::async_trait;
use pactum_journal::Journal;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
};

identifier!(
    PlanId,
    "Permanent plan identity within a dedicated plan journal."
);
identifier!(StepId, "Immutable step identity within a plan.");
identifier!(
    AdapterId,
    "Stable adapter incarnation and resource namespace."
);

/// Immutable work using the existing execution protocols.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StepWork {
    /// Atomic, Chunked or StagedPublish request.
    Native(Request),
    /// Existing 2PC participant enlistments.
    TwoPhase(Vec<Enlistment>),
}
impl StepWork {
    /// Protocol selected by this request.
    pub fn mode(&self) -> Mode {
        match self {
            Self::Native(r) => r.mode(),
            Self::TwoPhase(_) => Mode::TwoPhase,
        }
    }
}
/// One immutable step. Resource scopes are exclusive within this plan runner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// Identity, independent of list position.
    pub id: StepId,
    /// Registered adapter; must preserve its durable namespace on restart.
    pub adapter: AdapterId,
    /// All prerequisites must have durable successful results.
    pub dependencies: BTreeSet<StepId>,
    /// Application-assigned exclusive resource names, acquired together.
    pub resources: BTreeSet<String>,
    /// Permanently bound payload and protocol.
    pub work: StepWork,
}
/// Version 1 bounded DAG. Revisions are metadata, not permission to reuse a key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionPlan {
    /// Supported schema version: 1.
    pub version: u32,
    /// Permanent retry identity.
    pub id: PlanId,
    /// Positive immutable application revision; changes require a new PlanId.
    pub revision: u64,
    /// At most 1024 steps, canonicalized by identity before binding.
    pub steps: Vec<Step>,
}
impl TransactionPlan {
    /// Validate size, requests, unique identities, dependencies and acyclicity.
    /// Returns deterministic topological order, breaking ties by step ID.
    pub fn validate(&self) -> Result<Vec<StepId>, Error> {
        if self.version != 1
            || self.revision == 0
            || self.steps.is_empty()
            || self.steps.len() > 1024
        {
            return Err(Error::invalid(
                "unsupported, empty or oversized plan".into(),
            ));
        }
        if serde_json::to_vec(self)
            .map_err(Error::invalid_source)?
            .len()
            > 16 * 1024 * 1024
        {
            return Err(Error::invalid("plan exceeds 16 MiB".into()));
        }
        let steps: BTreeMap<_, _> = self.steps.iter().map(|s| (s.id.clone(), s)).collect();
        if steps.len() != self.steps.len() {
            return Err(Error::invalid("duplicate step ID".into()));
        }
        for s in &self.steps {
            if s.dependencies.iter().any(|d| !steps.contains_key(d))
                || s.resources
                    .iter()
                    .any(|r| r.is_empty() || r.len() > 256 || r.chars().any(char::is_control))
            {
                return Err(Error::invalid(
                    "unknown dependency or invalid resource scope".into(),
                ));
            }
            match &s.work {
                StepWork::Native(r) => r.validate()?,
                StepWork::TwoPhase(r) => {
                    let mut requests = r.clone();
                    requests.sort_by(|a, b| a.participant.cmp(&b.participant));
                    crate::validate_requests(&requests)?;
                }
            }
        }
        dependency_order(self.steps.iter().map(|s| (&s.id, &s.dependencies)))
    }
    fn canonical(mut self) -> Result<Self, Error> {
        self.validate()?;
        self.steps.sort_by(|a, b| a.id.cmp(&b.id));
        for s in &mut self.steps {
            if let StepWork::TwoPhase(r) = &mut s.work {
                r.sort_by(|a, b| a.participant.cmp(&b.participant));
            }
        }
        Ok(self)
    }
    /// SHA-256 of canonical immutable spec, including revision and payloads.
    pub fn fingerprint(&self) -> Result<String, Error> {
        let spec = self.clone().canonical()?;
        Ok(crate::sha256_hex(
            serde_json::to_vec(&spec).map_err(Error::invalid_source)?,
        ))
    }
    /// Namespace-independent receipt seed shared by every attempt of a step.
    /// The engine hashes this with its deployment namespace for the backend key.
    pub fn operation_key(&self, step: &StepId) -> Result<OperationId, Error> {
        if !self.steps.iter().any(|s| &s.id == step) {
            return Err(Error::invalid("unknown step".into()));
        }
        Self::receipt_seed(&self.fingerprint()?, step)
    }
    fn receipt_seed(fingerprint: &str, step: &StepId) -> Result<OperationId, Error> {
        let bytes = serde_json::to_vec(&(fingerprint, step)).map_err(Error::invalid_source)?;
        OperationId::new(format!("plan-v1-{}", crate::sha256_hex(bytes)))
    }
}

// Shared deterministic DAG validation for plans and optional Saga execution.
pub(crate) fn dependency_order<'a>(
    steps: impl Iterator<Item = (&'a StepId, &'a BTreeSet<StepId>)>,
) -> Result<Vec<StepId>, Error> {
    let mut graph = BTreeMap::new();
    for (id, dependencies) in steps {
        if graph.insert(id, dependencies).is_some() {
            return Err(Error::invalid("duplicate step ID".into()));
        }
    }
    if graph
        .values()
        .any(|deps| deps.iter().any(|d| !graph.contains_key(d)))
    {
        return Err(Error::invalid("unknown dependency".into()));
    }
    let mut done = BTreeSet::new();
    let mut order = Vec::new();
    while done.len() < graph.len() {
        let next = graph
            .iter()
            .find(|(id, deps)| !done.contains(**id) && deps.is_subset(&done));
        let Some((id, _)) = next else {
            return Err(Error::invalid("dependency cycle".into()));
        };
        done.insert((*id).clone());
        order.push((*id).clone());
    }
    Ok(order)
}
/// Durable mode-specific step result. Completed values are immutable outputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StepResult {
    /// Existing native-mode progress.
    Native(Progress),
    /// Durable 2PC decision and whether all participants acknowledged it.
    TwoPhase {
        /// None means no acknowledged decision.
        decision: Option<Decision>,
        /// True only after every final acknowledgement.
        complete: bool,
    },
}
impl StepResult {
    /// Whether dependents may run.
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            Self::Native(Progress::Atomic(AtomicStatus::Committed))
                | Self::Native(Progress::StagedPublish(PublishStatus::Published))
                | Self::TwoPhase {
                    decision: Some(Decision::Commit),
                    complete: true
                }
        ) || matches!(self, Self::Native(Progress::Chunked(s)) if s.is_complete() && !s.rejected && !s.paused)
    }
    /// Whether the underlying protocol is terminal, including definite failure.
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Native(p) => p.is_terminal(),
            Self::TwoPhase { complete, .. } => *complete,
        }
    }
    fn validate(&self, work: &StepWork) -> Result<(), Error> {
        let valid = match (self, work) {
            (Self::Native(Progress::Atomic(_)), StepWork::Native(Request::Atomic(_))) => true,
            (Self::Native(Progress::Chunked(s)), StepWork::Native(Request::Chunked(r))) => {
                s.total == r.chunks.len()
                    && s.completed <= s.total
                    && !(s.completed == s.total && (s.uncertain || s.paused || s.rejected))
            }
            (
                Self::Native(Progress::StagedPublish(_)),
                StepWork::Native(Request::StagedPublish(_)),
            ) => true,
            (Self::TwoPhase { decision, complete }, StepWork::TwoPhase(_)) => {
                !complete || decision.is_some()
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(Error::corrupt("adapter result does not match step".into()))
        }
    }
}
/// Attempt acknowledgement, retaining ambiguity rather than inventing failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Report {
    /// Typed protocol output.
    Result(StepResult),
    /// Timeout, dropped call, adapter error or panic; retry the same receipt key.
    Uncertain(String),
    /// Failed operational call, with no confirmed protocol outcome.
    Operational(pactum_errors::FailureMetadata),
}
/// Durable per-step progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepStatus {
    /// Number of attempts reserved durably before dispatch.
    pub attempts: u64,
    /// None with attempts > 0 means an interrupted/running attempt.
    pub report: Option<Report>,
    /// Process-local source chain. Durable guidance is carried by Report::Operational.
    #[serde(skip)]
    pub diagnostic: Option<pactum_errors::Diagnostic>,
    /// Explicit operator authorization, consumed by the next attempt.
    #[serde(default)]
    pub retry_authorized: bool,
}
impl StepStatus {
    /// Successful durable prerequisite acknowledgement.
    pub fn is_success(&self) -> bool {
        matches!(&self.report, Some(Report::Result(r)) if r.is_success())
    }
    fn terminal(&self) -> bool {
        matches!(&self.report, Some(Report::Result(r)) if r.is_terminal())
    }
}
/// Plan completion never implies global atomicity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanOutcome {
    /// All steps have durable successful mode-specific results.
    Complete,
    /// A step failed definitively; other step effects may remain.
    PartiallyFailed,
    /// An attempt may have effects beyond the plan journal acknowledgement.
    Indeterminate,
    /// Undispatched or incomplete work remains, possibly paused by cancellation.
    Pending,
}
/// Snapshot including typed permanent outputs and blocked prerequisites.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanStatus {
    /// Stable plan identity.
    pub id: PlanId,
    /// Canonical immutable payload fingerprint.
    pub fingerprint: String,
    /// Aggregate classification, never a global commit decision.
    pub outcome: PlanOutcome,
    /// Deterministic step snapshots.
    pub steps: BTreeMap<StepId, StepStatus>,
    /// Steps whose dependencies have not all completed successfully.
    pub blocked: BTreeSet<StepId>,
    /// Steps permanently blocked by a definite failed ancestor.
    pub failed_dependencies: BTreeSet<StepId>,
}
impl PlanStatus {
    /// All work is settled or permanently blocked by a failed prerequisite.
    pub fn is_terminal(&self) -> bool {
        self.steps
            .iter()
            .all(|(id, s)| s.terminal() || self.failed_dependencies.contains(id))
    }
}
impl State {
    fn failed_dependencies(&self) -> BTreeSet<StepId> {
        let mut failed: BTreeSet<_> = self
            .steps
            .iter()
            .filter(|(_, s)| s.terminal() && !s.is_success())
            .map(|(id, _)| id.clone())
            .collect();
        let mut blocked = BTreeSet::new();
        loop {
            let before = failed.len();
            for s in &self.spec.steps {
                if !s.dependencies.is_disjoint(&failed) {
                    blocked.insert(s.id.clone());
                    failed.insert(s.id.clone());
                }
            }
            if failed.len() == before {
                return blocked;
            }
        }
    }
}
/// Adapter contract: permanent receipts bind key to work atomically with effects.
/// Calls can be dropped or repeated after any crash/error. Never spawn detached
/// effects. Keep identity and namespace stable and retain receipts forever.
#[async_trait]
pub trait StepAdapter: Send + Sync {
    /// Stable durable resource incarnation, checked against the journal before dispatch.
    fn identity(&self) -> String;
    /// Check supported work before the plan is journaled or any step dispatched.
    fn accepts(&self, work: &StepWork) -> bool;
    /// Execute/reconcile the same immutable request. Async operations must yield.
    async fn execute(
        &self,
        key: OperationId,
        work: StepWork,
        cancellation: &Cancellation,
    ) -> Result<StepResult, Error>;
}
/// Adapter for the existing Atomic, Chunked and StagedPublish engine.
/// Each adapter serializes its engine; distinct adapters can execute concurrently.
/// Native backend calls are synchronous bounded units and cannot be preempted.
pub struct NativeAdapter<J: Journal, B: Backend> {
    engine: Mutex<Engine<J, B>>,
    identity: String,
}
impl<J: Journal, B: Backend> NativeAdapter<J, B> {
    /// Wrap an already opened, exclusively owned engine and its receipt backend.
    pub fn new(engine: Engine<J, B>) -> Self {
        Self {
            identity: engine.backend().identity(),
            engine: Mutex::new(engine),
        }
    }
}
#[async_trait]
impl<J: Journal, B: Backend> StepAdapter for NativeAdapter<J, B> {
    fn identity(&self) -> String {
        self.identity.clone()
    }
    fn accepts(&self, work: &StepWork) -> bool {
        matches!(work, StepWork::Native(_))
    }
    async fn execute(
        &self,
        key: OperationId,
        work: StepWork,
        cancellation: &Cancellation,
    ) -> Result<StepResult, Error> {
        let StepWork::Native(request) = work else {
            return Err(Error::invalid("native adapter requires native work".into()));
        };
        Ok(StepResult::Native(
            self.engine
                .lock()
                .await
                .execute(key, request, cancellation)
                .await?
                .progress,
        ))
    }
}
/// Adapter to the existing durable 2PC coordinator; no new decision rules.
pub struct TwoPhaseAdapter<J: Journal> {
    coordinator: Mutex<Coordinator<J>>,
    identity: String,
}
impl<J: Journal> TwoPhaseAdapter<J> {
    /// Wrap an exclusively owned coordinator. Namespace must identify the retained
    /// coordinator journal incarnation, not merely its pathname. Never replace it.
    pub fn new(namespace: AdapterId, coordinator: Coordinator<J>) -> Self {
        let identity = format!(
            "{}:{:?}",
            namespace,
            coordinator.participants.keys().collect::<Vec<_>>()
        );
        Self {
            identity,
            coordinator: Mutex::new(coordinator),
        }
    }
}
#[async_trait]
impl<J: Journal> StepAdapter for TwoPhaseAdapter<J> {
    fn identity(&self) -> String {
        self.identity.clone()
    }
    fn accepts(&self, work: &StepWork) -> bool {
        matches!(work, StepWork::TwoPhase(_))
    }
    async fn execute(
        &self,
        key: OperationId,
        work: StepWork,
        cancellation: &Cancellation,
    ) -> Result<StepResult, Error> {
        let StepWork::TwoPhase(requests) = work else {
            return Err(Error::invalid("2PC adapter requires enlistments".into()));
        };
        let s = self
            .coordinator
            .lock()
            .await
            .execute(TransactionId::new(key.as_str())?, requests, cancellation)
            .await?;
        Ok(StepResult::TwoPhase {
            decision: s.decision,
            complete: s.outcome != Outcome::InDoubt,
        })
    }
}
/// Bounded scheduling policy, independent of immutable work identity.
#[derive(Clone, Debug)]
pub struct PlanPolicy {
    /// Maximum in-flight adapter futures (1..=1024).
    pub concurrency: usize,
    /// Maximum attempts per step in one pass (positive).
    pub attempts_per_step: usize,
    /// Maximum total dispatches in one pass (positive); provides backpressure.
    pub max_dispatches: usize,
    /// Positive cooperative timeout; elapsed calls are indeterminate.
    pub timeout: Duration,
    /// Delay before retrying unresolved work in the same pass.
    pub retry_backoff: Duration,
}
impl Default for PlanPolicy {
    fn default() -> Self {
        Self {
            concurrency: 4,
            attempts_per_step: 1,
            max_dispatches: 1024,
            timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(25),
        }
    }
}
#[derive(Clone)]
struct State {
    spec: TransactionPlan,
    bindings: BTreeMap<AdapterId, String>,
    steps: BTreeMap<StepId, StepStatus>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
enum Record {
    Header {
        namespace: PlanId,
    },
    Begin {
        spec: TransactionPlan,
        bindings: BTreeMap<AdapterId, String>,
    },
    Attempt {
        plan: PlanId,
        step: StepId,
        attempt: u64,
    },
    Retry {
        plan: PlanId,
        step: StepId,
        reason: String,
    },
    Report {
        plan: PlanId,
        step: StepId,
        attempt: u64,
        report: Report,
    },
}
fn failure_report(
    error: Error,
    context: crate::ErrorContext,
) -> (Report, Option<crate::Diagnostic>) {
    let diagnostic = error.diagnostic().clone().with_context(context);
    diagnostic.trace();
    let metadata = diagnostic.metadata(crate::RetryContext {
        authority_valid: true,
        idempotent: true,
        ..Default::default()
    });
    (Report::Operational(metadata), Some(diagnostic))
}
/// Single-owner plan journal and bounded scheduler. Use a dedicated journal
/// (record schema 4), separate from mode/coordinator/recovery-service journals.
/// Any journal write error poisons the runner until it is dropped and reopened.
pub struct PlanEngine<J: Journal> {
    journal: J,
    namespace: PlanId,
    adapters: BTreeMap<AdapterId, Arc<dyn StepAdapter>>,
    policy: PlanPolicy,
    states: BTreeMap<PlanId, State>,
    sequence: u64,
    poisoned: bool,
    permits: Arc<Semaphore>,
    scopes: BTreeMap<String, Arc<Mutex<()>>>,
}
impl<J: Journal> PlanEngine<J> {
    /// Replay without dispatch, rejecting corrupt history and foreign journals.
    /// Namespace is a permanent deployment identity used in backend receipt keys.
    pub fn open(
        mut journal: J,
        namespace: PlanId,
        adapters: BTreeMap<AdapterId, Arc<dyn StepAdapter>>,
        policy: PlanPolicy,
    ) -> Result<Self, Error> {
        if policy.concurrency == 0
            || policy.concurrency > 1024
            || policy.attempts_per_step == 0
            || policy.max_dispatches == 0
            || policy.timeout.is_zero()
        {
            return Err(Error::invalid(
                "positive plan scheduling bounds required".into(),
            ));
        }
        if journal
            .checkpoint()
            .map_err(|source| Error::journal_read(source, "checkpoint"))?
            .is_some()
        {
            return Err(Error::corrupt("plan checkpoints are unsupported".into()));
        }
        let history = journal
            .replay()
            .map_err(|source| Error::journal_read(source, "replay"))?;
        let permits = Arc::new(Semaphore::new(policy.concurrency));
        let mut engine = Self {
            journal,
            namespace,
            adapters,
            policy,
            states: BTreeMap::new(),
            sequence: 0,
            poisoned: false,
            permits,
            scopes: BTreeMap::new(),
        };
        for entry in history {
            if entry.version != 4 || entry.sequence != engine.sequence + 1 {
                return Err(Error::corrupt(
                    "invalid plan record version/sequence".into(),
                ));
            }
            let r: Record =
                serde_json::from_slice(&entry.payload).map_err(Error::corrupt_source)?;
            if engine.sequence == 0 {
                if !matches!(r, Record::Header { ref namespace } if namespace == &engine.namespace)
                {
                    return Err(Error::corrupt("foreign plan namespace/header".into()));
                }
            } else {
                engine.apply(&r)?;
            }
            engine.sequence = entry.sequence;
        }
        if engine.sequence == 0 {
            engine.write(&Record::Header {
                namespace: engine.namespace.clone(),
            })?;
        }
        Ok(engine)
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::in_doubt(
                "drop and reopen plan engine after journal uncertainty".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn apply(&mut self, record: &Record) -> Result<(), Error> {
        let corrupt = || Error::corrupt("illegal plan journal transition".into());
        match record {
            Record::Header { .. } => return Err(corrupt()),
            Record::Begin { spec, bindings } => {
                let canonical = spec.clone().canonical().map_err(|_| corrupt())?;
                if &canonical != spec
                    || self.states.contains_key(&spec.id)
                    || bindings.keys().collect::<BTreeSet<_>>()
                        != spec.steps.iter().map(|s| &s.adapter).collect()
                    || bindings.values().any(|v| v.is_empty() || v.len() > 65536)
                {
                    return Err(corrupt());
                }
                self.states.insert(
                    spec.id.clone(),
                    State {
                        spec: spec.clone(),
                        bindings: bindings.clone(),
                        steps: spec
                            .steps
                            .iter()
                            .map(|s| {
                                (
                                    s.id.clone(),
                                    StepStatus {
                                        attempts: 0,
                                        report: None,
                                        diagnostic: None,
                                        retry_authorized: false,
                                    },
                                )
                            })
                            .collect(),
                    },
                );
            }
            Record::Attempt {
                plan,
                step,
                attempt,
            } => {
                let state = self.states.get_mut(plan).ok_or_else(corrupt)?;
                let spec = state
                    .spec
                    .steps
                    .iter()
                    .find(|s| &s.id == step)
                    .ok_or_else(corrupt)?;
                if !spec
                    .dependencies
                    .iter()
                    .all(|d| state.steps[d].is_success())
                {
                    return Err(corrupt());
                }
                let s = state.steps.get_mut(step).ok_or_else(corrupt)?;
                if s.terminal() || s.attempts.checked_add(1) != Some(*attempt) {
                    return Err(corrupt());
                }
                if !s.retry_authorized
                    && matches!(&s.report, Some(Report::Operational(f)) if matches!(f.disposition, crate::RetryDisposition::Permanent | crate::RetryDisposition::OperatorIntervention))
                {
                    return Err(corrupt());
                }
                s.attempts = *attempt;
                s.retry_authorized = false;
                s.report = None;
                s.diagnostic = None;
            }
            Record::Retry { plan, step, reason } => {
                let state = self.states.get_mut(plan).ok_or_else(corrupt)?;
                let step = state.steps.get_mut(step).ok_or_else(corrupt)?;
                if reason.is_empty()
                    || reason.len() > 4096
                    || step.retry_authorized
                    || !matches!(&step.report, Some(Report::Operational(f)) if matches!(f.disposition, crate::RetryDisposition::Permanent | crate::RetryDisposition::OperatorIntervention))
                {
                    return Err(corrupt());
                }
                step.retry_authorized = true;
            }
            Record::Report {
                plan,
                step,
                attempt,
                report,
            } => {
                let state = self.states.get_mut(plan).ok_or_else(corrupt)?;
                let spec = state
                    .spec
                    .steps
                    .iter()
                    .find(|s| &s.id == step)
                    .ok_or_else(corrupt)?;
                if let Report::Result(r) = report {
                    r.validate(&spec.work)?;
                }
                if matches!(report, Report::Operational(f) if !f.is_valid()) {
                    return Err(corrupt());
                }
                let s = state.steps.get_mut(step).ok_or_else(corrupt)?;
                if *attempt == 0 || s.attempts != *attempt || s.report.is_some() {
                    return Err(corrupt());
                }
                s.report = Some(report.clone());
            }
        }
        Ok(())
    }
    fn write(&mut self, record: &Record) -> Result<(), Error> {
        self.healthy()?;
        let bytes = serde_json::to_vec(record).map_err(Error::invalid_source)?;
        let context = match record {
            Record::Header { namespace } => crate::ErrorContext {
                transaction: Some(namespace.as_str().into()),
                phase: Some("plan_header".into()),
                ..Default::default()
            },
            Record::Begin { spec, .. } => crate::ErrorContext {
                transaction: Some(spec.id.as_str().into()),
                phase: Some("plan_begin".into()),
                ..Default::default()
            },
            Record::Attempt { plan, step, .. }
            | Record::Report { plan, step, .. }
            | Record::Retry { plan, step, .. } => crate::ErrorContext {
                transaction: Some(plan.as_str().into()),
                step: Some(step.as_str().into()),
                phase: Some("plan_journal".into()),
                ..Default::default()
            },
        };
        self.poisoned = true;
        let sequence = self.journal.append(4, &bytes).map_err(|e| {
            let error = Error::journal(e, "append").with_context(context.clone());
            error.trace();
            error
        })?;
        let durable = self.journal.flush().map_err(|e| {
            let error = Error::journal(e, "flush").with_context(context.clone());
            error.trace();
            error
        })?;
        if self.sequence.checked_add(1) != Some(sequence) || durable != sequence {
            return Err(Error::in_doubt("plan barrier did not cover record".into()));
        }
        self.sequence = sequence;
        self.poisoned = false;
        Ok(())
    }
    fn persist(&mut self, record: Record) -> Result<(), Error> {
        self.write(&record)?;
        if let Err(e) = self.apply(&record) {
            self.poisoned = true;
            return Err(e);
        }
        Ok(())
    }
    /// Last acknowledged durable record position, including replay.
    pub fn replay_position(&self) -> u64 {
        self.sequence
    }
    /// Read acknowledged outputs without executing adapters.
    pub fn status(&self, id: &PlanId) -> Result<Option<PlanStatus>, Error> {
        self.healthy()?;
        self.states
            .get(id)
            .map(|s| {
                let outcome = if s.steps.values().all(StepStatus::is_success) {
                    PlanOutcome::Complete
                } else if s.steps.values().any(|s| {
                    s.attempts > 0
                        && (s.report.is_none()
                            || matches!(
                                s.report,
                                Some(Report::Uncertain(_) | Report::Operational(_))
                            ))
                }) {
                    PlanOutcome::Indeterminate
                } else if s.steps.values().any(|s| s.terminal() && !s.is_success()) {
                    PlanOutcome::PartiallyFailed
                } else if s
                    .steps
                    .values()
                    .any(|s| matches!(&s.report, Some(Report::Result(r)) if !r.is_terminal()))
                {
                    PlanOutcome::Indeterminate
                } else {
                    PlanOutcome::Pending
                };
                Ok(PlanStatus {
                    id: id.clone(),
                    fingerprint: s.spec.fingerprint()?,
                    outcome,
                    steps: s.steps.clone(),
                    failed_dependencies: s.failed_dependencies(),
                    blocked: s
                        .spec
                        .steps
                        .iter()
                        .filter(|step| !step.dependencies.iter().all(|d| s.steps[d].is_success()))
                        .map(|s| s.id.clone())
                        .collect(),
                })
            })
            .transpose()
    }
    /// Per-step guarantees remain those of its selected Pactum mode.
    pub fn guarantees(&self, id: &PlanId, step: &StepId) -> Result<Guarantees, Error> {
        self.healthy()?;
        self.states
            .get(id)
            .and_then(|s| s.spec.steps.iter().find(|s| &s.id == step))
            .map(|s| s.work.mode().guarantees())
            .ok_or_else(|| Error::invalid("unknown plan step".into()))
    }
    /// Bind or resume immutable work. Cancellation stops dispatch and drops
    /// outstanding cooperative calls as uncertain; a fresh token resumes them.
    /// Ready work is ordered by fewest durable attempts then step ID, preventing
    /// a retrying low-ID step from starving other ready work across bounded passes.
    pub async fn execute(
        &mut self,
        spec: TransactionPlan,
        cancellation: &Cancellation,
    ) -> Result<PlanStatus, Error> {
        self.healthy()?;
        let spec = spec.canonical()?;
        if let Some(old) = self.states.get(&spec.id)
            && old.spec != spec
        {
            return Err(Error::invalid(
                "plan ID reused with changed revision, steps or payloads".into(),
            ));
        }
        for s in &spec.steps {
            if !self
                .adapters
                .get(&s.adapter)
                .is_some_and(|a| a.accepts(&s.work))
            {
                return Err(Error::invalid(format!(
                    "missing/incompatible adapter {}",
                    s.adapter
                )));
            }
        }
        let bindings: BTreeMap<_, _> = spec
            .steps
            .iter()
            .map(|s| (s.adapter.clone(), self.adapters[&s.adapter].identity()))
            .collect();
        if bindings.values().any(|v| v.is_empty() || v.len() > 65536) {
            return Err(Error::invalid("invalid adapter resource identity".into()));
        }
        if let Some(old) = self.states.get(&spec.id)
            && old.bindings != bindings
        {
            return Err(Error::invalid(
                "adapter resource incarnation changed".into(),
            ));
        }
        if !self.states.contains_key(&spec.id) {
            self.persist(Record::Begin {
                spec: spec.clone(),
                bindings,
            })?;
        }
        let fingerprint = spec.fingerprint()?;
        let mut tasks = JoinSet::new();
        let mut active: BTreeMap<StepId, BTreeSet<String>> = BTreeMap::new();
        let mut counts: BTreeMap<StepId, usize> = BTreeMap::new();
        let mut dispatched = 0;
        loop {
            while !cancellation.is_cancelled()
                && tasks.len() < self.policy.concurrency
                && dispatched < self.policy.max_dispatches
            {
                let state = &self.states[&spec.id];
                let used: BTreeSet<_> = active.values().flat_map(|s| s.iter().cloned()).collect();
                let next = spec
                    .steps
                    .iter()
                    .filter(|s| {
                        !active.contains_key(&s.id)
                            && !state.steps[&s.id].terminal()
                            && (state.steps[&s.id].retry_authorized || !matches!(&state.steps[&s.id].report, Some(Report::Operational(f)) if matches!(f.disposition, crate::RetryDisposition::Permanent | crate::RetryDisposition::OperatorIntervention)))
                            && counts.get(&s.id).copied().unwrap_or(0)
                                < self.policy.attempts_per_step
                            && s.resources.is_disjoint(&used)
                            && s.dependencies.iter().all(|d| state.steps[d].is_success())
                    })
                    .min_by_key(|s| (state.steps[&s.id].attempts, s.id.clone()))
                    .cloned();
                let Some(step) = next else {
                    break;
                };
                let previous = state.steps[&step.id].attempts;
                let attempt = previous
                    .checked_add(1)
                    .ok_or_else(|| Error::invalid("step attempt counter exhausted".into()))?;
                self.persist(Record::Attempt {
                    plan: spec.id.clone(),
                    step: step.id.clone(),
                    attempt,
                })?;
                active.insert(step.id.clone(), step.resources.clone());
                *counts.entry(step.id.clone()).or_default() += 1;
                dispatched += 1;
                let adapter = self.adapters[&step.adapter].clone();
                // Namespace avoids collisions when independent plan journals share a backend.
                let key = OperationId::new(format!(
                    "plan-v1-{}",
                    crate::sha256_hex(
                        serde_json::to_vec(&(
                            self.namespace.as_str(),
                            TransactionPlan::receipt_seed(&fingerprint, &step.id)?.as_str()
                        ))
                        .map_err(Error::invalid_source)?
                    )
                ))?;
                let token = cancellation.clone();
                let timeout = self.policy.timeout;
                let backoff = if previous > 0 {
                    self.policy.retry_backoff
                } else {
                    Duration::ZERO
                };
                let permits = self.permits.clone();
                let scopes: Vec<_> = step
                    .resources
                    .iter()
                    .map(|r| {
                        self.scopes
                            .entry(r.clone())
                            .or_insert_with(|| Arc::new(Mutex::new(())))
                            .clone()
                    })
                    .collect();
                let plan_id = spec.id.clone();
                tasks.spawn(async move {
                    let context = crate::ErrorContext { transaction: Some(plan_id.as_str().into()), step: Some(step.id.as_str().into()), phase: Some("plan_step".into()), ..Default::default() };
                    let call = async {
                        // Guards survive a dropped execution future until its aborted
                        // tasks are actually destroyed, bounding overlapping resumes.
                        let _permit = permits
                            .acquire_owned()
                            .await
                            .expect("private semaphore remains open");
                        let mut guards = Vec::new();
                        for scope in scopes {
                            guards.push(scope.lock_owned().await);
                        }
                        if !backoff.is_zero() {
                            tokio::time::sleep(backoff).await;
                        }
                        match tokio::time::timeout(
                            timeout,
                            adapter.execute(key, step.work.clone(), &token),
                        )
                        .await
                        {
                            Ok(Ok(result)) => match result.validate(&step.work) {
                                Ok(()) => (Report::Result(result), None),
                                Err(e) => failure_report(e, context.clone()),
                            },
                            Ok(Err(e)) => failure_report(e, context.clone()),
                            Err(e) => failure_report(Error::operational(crate::Diagnostic::caused(crate::ErrorCode::Timeout, e)), context.clone()),
                        }
                    };
                    let report = tokio::select! {
                        biased;
                        _ = token.cancelled() => failure_report(Error::operational(crate::Diagnostic::message(crate::ErrorCode::Cancelled, "cancelled in flight".into())), context),
                        report = call => report,
                    };
                    (step.id, attempt, report.0, report.1)
                });
            }
            if tasks.is_empty() {
                break;
            }
            match tasks.join_next().await.expect("nonempty task set") {
                Ok((step, attempt, report, diagnostic)) => {
                    self.persist(Record::Report {
                        plan: spec.id.clone(),
                        step: step.clone(),
                        attempt,
                        report,
                    })?;
                    self.states
                        .get_mut(&spec.id)
                        .unwrap()
                        .steps
                        .get_mut(&step)
                        .unwrap()
                        .diagnostic = diagnostic;
                    active.remove(&step);
                }
                Err(e) => {
                    let diagnostic = crate::Diagnostic::caused(crate::ErrorCode::AdapterUnknown, e);
                    diagnostic.trace();
                    // A panic invalidates all in-flight acknowledgements; dropping
                    // their futures is safe by the adapter receipt contract.
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    for step in active.keys() {
                        let attempt = self.states[&spec.id].steps[step].attempts;
                        let (report, local) = failure_report(
                            Error::operational(diagnostic.clone()),
                            crate::ErrorContext {
                                transaction: Some(spec.id.as_str().into()),
                                step: Some(step.as_str().into()),
                                phase: Some("plan_step".into()),
                                ..Default::default()
                            },
                        );
                        self.persist(Record::Report {
                            plan: spec.id.clone(),
                            step: step.clone(),
                            attempt,
                            report,
                        })?;
                        self.states
                            .get_mut(&spec.id)
                            .unwrap()
                            .steps
                            .get_mut(step)
                            .unwrap()
                            .diagnostic = local;
                    }
                    break;
                }
            }
        }
        self.status(&spec.id)?
            .ok_or_else(|| Error::corrupt("plan disappeared".into()))
    }
    /// Authorize one retry after repairing a classified operational failure.
    /// Keeps the immutable specification, receipt key and prior uncertainty.
    pub fn retry_step(
        &mut self,
        plan: &PlanId,
        step: &StepId,
        reason: impl Into<String>,
    ) -> Result<(), Error> {
        self.healthy()?;
        let reason = reason.into();
        let status = self
            .states
            .get(plan)
            .and_then(|s| s.steps.get(step))
            .ok_or_else(|| Error::invalid("unknown plan step".into()))?;
        if reason.is_empty()
            || reason.len() > 4096
            || status.retry_authorized
            || !matches!(&status.report, Some(Report::Operational(f)) if matches!(f.disposition, crate::RetryDisposition::Permanent | crate::RetryDisposition::OperatorIntervention))
        {
            return Err(Error::invalid(
                "step is not manually blocked or retry reason is invalid".into(),
            ));
        }
        self.persist(Record::Retry {
            plan: plan.clone(),
            step: step.clone(),
            reason,
        })
    }
    /// Reconcile an existing plan using its durable immutable spec.
    pub async fn resume(
        &mut self,
        id: &PlanId,
        cancellation: &Cancellation,
    ) -> Result<PlanStatus, Error> {
        self.healthy()?;
        let spec = self
            .states
            .get(id)
            .ok_or_else(|| Error::invalid("unknown plan".into()))?
            .spec
            .clone();
        self.execute(spec, cancellation).await
    }
    /// Reconcile every nonterminal plan once in deterministic ID order.
    pub async fn recover_pending(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Vec<PlanStatus>, Error> {
        self.healthy()?;
        let ids: Vec<_> = self
            .states
            .iter()
            .filter(|(_, s)| {
                let blocked = s.failed_dependencies();
                s.steps
                    .iter()
                    .any(|(id, s)| !s.terminal() && !blocked.contains(id))
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut results = Vec::new();
        for id in ids {
            if cancellation.is_cancelled() {
                break;
            }
            results.push(self.resume(&id, cancellation).await?);
        }
        Ok(results)
    }
}

#[async_trait]
impl<J: Journal> crate::recovery::RecoveryTarget for PlanEngine<J> {
    fn pending_work(&self) -> Result<Vec<crate::recovery::Work>, Error> {
        self.healthy()?;
        self.states
            .keys()
            .filter_map(|id| match self.status(id) {
                Ok(Some(s)) if !s.is_terminal() => Some(Ok(crate::recovery::Work::Plan(s))),
                Err(e) => Some(Err(e)),
                _ => None,
            })
            .collect()
    }
    fn replay_position(&self) -> u64 {
        self.sequence
    }
    fn resume_work(&mut self, key: &str) -> Result<(), Error> {
        self.healthy()?;
        let plan = PlanId::new(key)?;
        let state = self
            .states
            .get(&plan)
            .ok_or_else(|| Error::invalid("unknown plan".into()))?;
        let blocked: Vec<_> = state.steps.iter().filter_map(|(step, status)| {
            (!status.retry_authorized && matches!(&status.report, Some(Report::Operational(f)) if matches!(f.disposition, crate::RetryDisposition::Permanent | crate::RetryDisposition::OperatorIntervention))).then_some(step.clone())
        }).collect();
        for step in blocked {
            self.retry_step(&plan, &step, "recovery service operator resume")?;
        }
        Ok(())
    }
    async fn recover_one(
        &mut self,
        key: &str,
        cancellation: &Cancellation,
    ) -> Result<crate::recovery::Work, Error> {
        self.resume(&PlanId::new(key)?, cancellation)
            .await
            .map(crate::recovery::Work::Plan)
    }
}
