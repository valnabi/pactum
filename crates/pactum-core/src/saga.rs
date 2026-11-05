//! Optional durable Sagas for non-2PC resources. Compensation is a new business
//! operation, never ACID rollback. Effects can remain visible or need intervention.
use crate::{
    Cancellation, Error,
    execution::OperationId,
    plan::{AdapterId, StepId, dependency_order},
};
use async_trait::async_trait;
use pactum_journal::Journal;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
    time::Duration,
};

identifier!(
    SagaId,
    "Permanent Saga identity within a dedicated journal."
);

/// Immutable request to a cooperating non-2PC adapter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    /// Stable adapter registration, including its durable resource namespace.
    pub adapter: AdapterId,
    /// Application payload retained for all retries, including compensation.
    pub payload: Vec<u8>,
}
/// An explicit compensation contract or a consciously irreversible boundary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compensation {
    /// Independent operation, possibly on a different adapter.
    Operation(Operation),
    /// Effects cannot be compensated by this engine.
    Irreversible,
}
/// Permission to include irreversible work; never permission to claim rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IrreversiblePolicy {
    /// Reject any irreversible step before binding or dispatch.
    Reject,
    /// Allow it; subsequent failure stops compensation at this boundary.
    Allow,
}
/// One Saga step in the same deterministic dependency model as transaction plans.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Step {
    /// Stable step identity.
    pub id: StepId,
    /// Prerequisites requiring acknowledged forward success.
    pub dependencies: BTreeSet<StepId>,
    /// Non-2PC forward operation.
    pub forward: Operation,
    /// Explicit compensability and immutable undo request.
    pub compensation: Compensation,
}
/// Immutable, bounded Saga DAG. Any change requires a new ID.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Saga {
    /// Supported specification version: 1.
    pub version: u32,
    /// Permanent retry binding.
    pub id: SagaId,
    /// Positive immutable application revision.
    pub revision: u64,
    /// At most 1024 steps, executed serially in deterministic topological order.
    pub steps: Vec<Step>,
    /// Explicit treatment of irreversible boundaries.
    pub irreversible: IrreversiblePolicy,
    /// Positive durable attempt budget per action, including interrupted calls.
    /// Exhaustion of an ambiguous call never becomes a definite failure.
    pub retry_limit: u32,
}
impl Saga {
    /// Validate bounds, identities, dependencies and irreversible-step policy.
    pub fn validate(&self) -> Result<Vec<StepId>, Error> {
        if self.version != 1
            || self.revision == 0
            || self.retry_limit == 0
            || self.steps.is_empty()
            || self.steps.len() > 1024
            || serde_json::to_vec(self)
                .map_err(Error::invalid_source)?
                .len()
                > 16 * 1024 * 1024
        {
            return Err(Error::invalid(
                "unsupported, empty or oversized Saga".into(),
            ));
        }
        if self.irreversible == IrreversiblePolicy::Reject
            && self
                .steps
                .iter()
                .any(|s| s.compensation == Compensation::Irreversible)
        {
            return Err(Error::invalid(
                "irreversible Saga step requires explicit Allow policy".into(),
            ));
        }
        dependency_order(self.steps.iter().map(|s| (&s.id, &s.dependencies)))
    }
    fn canonical(mut self) -> Result<Self, Error> {
        self.validate()?;
        self.steps.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(self)
    }
    /// SHA-256 of the entire canonical immutable spec.
    pub fn fingerprint(&self) -> Result<String, Error> {
        Ok(crate::sha256_hex(
            serde_json::to_vec(&self.clone().canonical()?).map_err(Error::invalid_source)?,
        ))
    }
    fn operations(&self) -> impl Iterator<Item = &Operation> {
        self.steps.iter().flat_map(|s| {
            std::iter::once(&s.forward).chain(match &s.compensation {
                Compensation::Operation(op) => Some(op),
                Compensation::Irreversible => None,
            })
        })
    }
}
/// Direction is part of every receipt key. Attempts never change receipt keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// Apply the requested business effect.
    Forward,
    /// Apply a separately defined compensating business effect.
    Compensation,
}
/// Authoritative response about the entire keyed operation, including earlier
/// delayed calls. Rejected/Retryable must fence any earlier ambiguous effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionResult {
    /// Durable effect and permanent receipt persisted atomically.
    Applied,
    /// Permanent rejection with durable no-effect tombstone.
    Rejected(String),
    /// No effect is possible from earlier calls; a later call may succeed.
    Retryable(String),
    /// Success is unknown; reconcile the SAME key before compensating anything.
    Uncertain(String),
    /// Operational failure retaining uncertainty independently of its error class.
    Operational(pactum_errors::FailureMetadata),
}
/// Saga adapter call failure. This never proves the operation was rejected.
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
#[snafu(transparent)]
pub struct SagaError {
    source: pactum_errors::Diagnostic,
}
impl SagaError {
    /// Construct a safe legacy diagnostic; completion remains conservative.
    pub fn message(detail: String) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::message(
            pactum_errors::ErrorCode::AdapterUnknown,
            detail,
        ))
    }
    /// Preserve a typed boundary source using the conservative default code.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::classified(pactum_errors::ErrorCode::AdapterUnknown, source)
    }
    /// Preserve a typed boundary source with an explicit machine-readable code.
    pub fn classified(
        code: pactum_errors::ErrorCode,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::caused(code, source))
    }
    /// Wrap an existing structured diagnostic without losing its classification.
    pub fn from_diagnostic(source: pactum_errors::Diagnostic) -> Self {
        Self { source }
    }
    /// Add allowlisted boundary identifiers, preserving prior metadata and sources.
    pub fn with_context(self, context: pactum_errors::ErrorContext) -> Self {
        Self {
            source: self.source.with_context(context),
        }
    }
    /// Inspect the structured diagnostic.
    pub fn diagnostic(&self) -> &pactum_errors::Diagnostic {
        &self.source
    }
}
impl std::ops::Deref for SagaError {
    type Target = pactum_errors::Diagnostic;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

impl From<String> for SagaError {
    fn from(detail: String) -> Self {
        Self::message(detail)
    }
}
impl From<&str> for SagaError {
    fn from(detail: &str) -> Self {
        Self::message(detail.into())
    }
}
/// Cooperating external-like adapter. Persist effects and immutable key/payload
/// receipts atomically, reject conflicting reuse, and retain receipts forever.
/// Repeated calls reconcile previous effects and never repeat them. Timeouts,
/// cancellation, panics and dropped futures are ambiguous. Never detach effects.
/// Identity must name the retained resource incarnation across process restarts.
#[async_trait]
pub trait SagaAdapter: Send + Sync {
    /// Stable durable resource incarnation (1..=65536 bytes).
    fn identity(&self) -> String;
    /// Check request support before binding the Saga.
    fn accepts(&self, payload: &[u8]) -> bool;
    /// Execute or reconcile immutable work. Async calls must yield to Tokio.
    /// Transport errors are always uncertain, never definitive rejection.
    async fn execute(&self, key: OperationId, payload: Vec<u8>) -> Result<ActionResult, SagaError>;
}
/// Bounded process-local dispatch policy; durable retry budgets live in the spec.
#[derive(Clone, Debug)]
pub struct SagaPolicy {
    /// Maximum total adapter calls per execution/recovery pass, positive.
    pub max_dispatches: usize,
    /// Positive cooperative per-call timeout.
    pub timeout: Duration,
    /// Delay before a repeated action within a pass.
    pub retry_backoff: Duration,
}
impl Default for SagaPolicy {
    fn default() -> Self {
        Self {
            max_dispatches: 1024,
            timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(25),
        }
    }
}
/// Durable action progress. Missing report after an attempt means uncertainty.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionStatus {
    /// Lifetime reserved attempts; never reset by intervention.
    pub attempts: u64,
    /// Attempts since the last explicitly audited retry authorization.
    pub budget_used: u32,
    /// Latest durable acknowledgement, if any.
    pub report: Option<ActionResult>,
    /// Local typed cause; never serialized into authoritative records.
    #[serde(skip)]
    pub diagnostic: Option<pactum_errors::Diagnostic>,
}
impl ActionStatus {
    fn applied(&self) -> bool {
        self.report == Some(ActionResult::Applied)
    }
    fn uncertain(&self) -> bool {
        self.attempts > 0
            && matches!(
                self.report,
                None | Some(ActionResult::Uncertain(_) | ActionResult::Operational(_))
            )
    }
    fn error(&self) -> Option<&str> {
        match &self.report {
            Some(
                ActionResult::Rejected(s) | ActionResult::Retryable(s) | ActionResult::Uncertain(s),
            ) => Some(s),
            Some(ActionResult::Operational(failure)) => Some(failure.code.as_str()),
            _ => None,
        }
    }
}
/// Forward/compensation acknowledgements remain available after completion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StepStatus {
    /// Forward progress.
    pub forward: ActionStatus,
    /// Compensator progress, empty for irreversible work.
    pub compensation: ActionStatus,
}
/// Explicit Saga classification. None of these promises atomic rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SagaOutcome {
    /// Undispatched forward work remains.
    Running,
    /// All forward operations succeeded; no ACID guarantee is implied.
    Completed,
    /// A definite forward failure triggered durable compensation intent.
    Compensating,
    /// Every confirmed forward success has a confirmed compensating effect.
    Compensated,
    /// Permanent/exhausted compensation failure or irreversible boundary.
    NeedsIntervention,
    /// A forward/compensating effect is ambiguous; no rollback claim is safe.
    Uncertain,
}
/// Authoritative durable progress and operator guidance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaStatus {
    /// Stable Saga key.
    pub id: SagaId,
    /// Immutable spec fingerprint.
    pub fingerprint: String,
    /// Aggregate state, retaining uncertainty before failure classifications.
    pub outcome: SagaOutcome,
    /// Durable direction switch; once true, never runs new forward work.
    pub compensating: bool,
    /// Lifetime action progress in stable step order.
    pub steps: BTreeMap<StepId, StepStatus>,
    /// Explicit manual action required; automatic recovery will not bypass it.
    pub needs_intervention: bool,
    /// Current action failure or irreversible-boundary explanation.
    pub last_error: Option<String>,
}
impl SagaStatus {
    /// Only fully completed or compensated work leaves the recovery pending set.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.outcome,
            SagaOutcome::Completed | SagaOutcome::Compensated
        )
    }
}
#[derive(Clone)]
struct State {
    spec: Saga,
    order: Vec<StepId>,
    bindings: BTreeMap<AdapterId, String>,
    steps: BTreeMap<StepId, StepStatus>,
    compensating: bool,
}
enum Next {
    Call(StepId, Direction),
    Compensate,
    Settled(SagaOutcome),
    Blocked(SagaOutcome, String),
}
impl State {
    fn action(&self, step: &StepId, direction: Direction) -> &ActionStatus {
        match direction {
            Direction::Forward => &self.steps[step].forward,
            Direction::Compensation => &self.steps[step].compensation,
        }
    }
    fn action_mut(&mut self, step: &StepId, direction: Direction) -> &mut ActionStatus {
        let s = self.steps.get_mut(step).expect("validated step");
        match direction {
            Direction::Forward => &mut s.forward,
            Direction::Compensation => &mut s.compensation,
        }
    }
    fn next(&self) -> Next {
        let order: Box<dyn Iterator<Item = &StepId> + '_> = if self.compensating {
            Box::new(self.order.iter().rev())
        } else {
            Box::new(self.order.iter())
        };
        for id in order {
            let step = self
                .spec
                .steps
                .iter()
                .find(|s| &s.id == id)
                .expect("validated DAG");
            if self.compensating && !self.steps[id].forward.applied() {
                continue;
            }
            if self.compensating && step.compensation == Compensation::Irreversible {
                return Next::Blocked(
                    SagaOutcome::NeedsIntervention,
                    format!(
                        "irreversible boundary at {id}; retained effects need manual remediation"
                    ),
                );
            }
            let direction = if self.compensating {
                Direction::Compensation
            } else {
                Direction::Forward
            };
            let action = self.action(id, direction);
            if action.applied() {
                continue;
            }
            if action.budget_used > 0
                && matches!(&action.report, Some(ActionResult::Operational(f)) if matches!(f.disposition, crate::RetryDisposition::Permanent | crate::RetryDisposition::OperatorIntervention))
            {
                return Next::Blocked(
                    SagaOutcome::Uncertain,
                    "operational failure requires reconciliation or operator action".into(),
                );
            }
            let exhausted = action.budget_used >= self.spec.retry_limit;
            if matches!(action.report, Some(ActionResult::Rejected(_)))
                || (exhausted && matches!(action.report, Some(ActionResult::Retryable(_))))
            {
                return if self.compensating {
                    Next::Blocked(
                        SagaOutcome::NeedsIntervention,
                        format!(
                            "compensation failed at {id}: {}",
                            action.error().unwrap_or("attempt budget exhausted")
                        ),
                    )
                } else {
                    Next::Compensate
                };
            }
            if exhausted {
                return Next::Blocked(
                    SagaOutcome::Uncertain,
                    format!(
                        "{direction:?} at {id} exhausted its attempt budget with unknown effects; reconcile the same key"
                    ),
                );
            }
            return Next::Call(id.clone(), direction);
        }
        Next::Settled(if self.compensating {
            SagaOutcome::Compensated
        } else {
            SagaOutcome::Completed
        })
    }
    fn operation(&self, id: &StepId, direction: Direction) -> &Operation {
        let step = self
            .spec
            .steps
            .iter()
            .find(|s| &s.id == id)
            .expect("validated step");
        match direction {
            Direction::Forward => &step.forward,
            Direction::Compensation => match &step.compensation {
                Compensation::Operation(op) => op,
                _ => unreachable!(),
            },
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
enum Record {
    Header {
        namespace: SagaId,
    },
    Begin {
        spec: Saga,
        bindings: BTreeMap<AdapterId, String>,
    },
    Attempt {
        saga: SagaId,
        step: StepId,
        direction: Direction,
        attempt: u64,
    },
    Report {
        saga: SagaId,
        step: StepId,
        direction: Direction,
        attempt: u64,
        result: ActionResult,
    },
    Compensate {
        saga: SagaId,
    },
    Retry {
        saga: SagaId,
        step: StepId,
        direction: Direction,
        reason: String,
    },
}
fn saga_failure(
    diagnostic: crate::Diagnostic,
    context: crate::ErrorContext,
) -> (ActionResult, Option<crate::Diagnostic>) {
    let diagnostic = diagnostic.with_context(context);
    diagnostic.trace();
    let metadata = diagnostic.metadata(crate::RetryContext {
        authority_valid: true,
        idempotent: true,
        ..Default::default()
    });
    (ActionResult::Operational(metadata), Some(diagnostic))
}
/// Single-owner Saga engine with dedicated format-5 journal. Uses the plan DAG
/// validator and existing recovery service, without changing the 2PC coordinator.
/// All journal write failures poison the engine; drop/reopen before recovery.
pub struct SagaEngine<J: Journal> {
    journal: J,
    namespace: SagaId,
    adapters: BTreeMap<AdapterId, Arc<dyn SagaAdapter>>,
    policy: SagaPolicy,
    states: BTreeMap<SagaId, State>,
    sequence: u64,
    poisoned: bool,
}
impl<J: Journal> SagaEngine<J> {
    /// Replay without resource calls. A namespace permanently identifies this
    /// exact journal deployment and is included in all backend receipt keys.
    pub fn open(
        mut journal: J,
        namespace: SagaId,
        adapters: BTreeMap<AdapterId, Arc<dyn SagaAdapter>>,
        policy: SagaPolicy,
    ) -> Result<Self, Error> {
        if policy.max_dispatches == 0 || policy.timeout.is_zero() {
            return Err(Error::invalid(
                "positive Saga dispatch bound and timeout required".into(),
            ));
        }
        if journal
            .checkpoint()
            .map_err(|source| Error::journal_read(source, "checkpoint"))?
            .is_some()
        {
            return Err(Error::corrupt("Saga checkpoints unsupported".into()));
        }
        let history = journal
            .replay()
            .map_err(|source| Error::journal_read(source, "replay"))?;
        let mut engine = Self {
            journal,
            namespace,
            adapters,
            policy,
            states: BTreeMap::new(),
            sequence: 0,
            poisoned: false,
        };
        for entry in history {
            if entry.version != 5 || engine.sequence.checked_add(1) != Some(entry.sequence) {
                return Err(Error::corrupt(
                    "invalid Saga record version/sequence".into(),
                ));
            }
            let r: Record =
                serde_json::from_slice(&entry.payload).map_err(Error::corrupt_source)?;
            if engine.sequence == 0 {
                if !matches!(r, Record::Header { ref namespace } if namespace == &engine.namespace)
                {
                    return Err(Error::corrupt("foreign Saga namespace/header".into()));
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
                "drop and reopen Saga engine after journal uncertainty".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn apply(&mut self, record: &Record) -> Result<(), Error> {
        let corrupt = || Error::corrupt("illegal Saga journal transition".into());
        match record {
            Record::Header { .. } => return Err(corrupt()),
            Record::Begin { spec, bindings } => {
                let canonical = spec.clone().canonical().map_err(|_| corrupt())?;
                if &canonical != spec
                    || self.states.contains_key(&spec.id)
                    || bindings.keys().collect::<BTreeSet<_>>()
                        != spec.operations().map(|op| &op.adapter).collect()
                    || bindings.values().any(|s| s.is_empty() || s.len() > 65536)
                {
                    return Err(corrupt());
                }
                self.states.insert(
                    spec.id.clone(),
                    State {
                        spec: spec.clone(),
                        order: spec.validate().map_err(|_| corrupt())?,
                        bindings: bindings.clone(),
                        steps: spec
                            .steps
                            .iter()
                            .map(|s| (s.id.clone(), StepStatus::default()))
                            .collect(),
                        compensating: false,
                    },
                );
            }
            Record::Compensate { saga } => {
                let state = self.states.get_mut(saga).ok_or_else(corrupt)?;
                if !matches!(state.next(), Next::Compensate) {
                    return Err(corrupt());
                }
                state.compensating = true;
            }
            Record::Attempt {
                saga,
                step,
                direction,
                attempt,
            } => {
                let state = self.states.get_mut(saga).ok_or_else(corrupt)?;
                if !matches!(state.next(), Next::Call(ref s, d) if s == step && d == *direction) {
                    return Err(corrupt());
                }
                let action = state.action_mut(step, *direction);
                if action.attempts.checked_add(1) != Some(*attempt) {
                    return Err(corrupt());
                }
                action.attempts = *attempt;
                action.budget_used += 1;
                action.report = None;
                action.diagnostic = None;
            }
            Record::Report {
                saga,
                step,
                direction,
                attempt,
                result,
            } => {
                let state = self.states.get_mut(saga).ok_or_else(corrupt)?;
                let Some(progress) = state.steps.get(step) else {
                    return Err(corrupt());
                };
                let action = match direction {
                    Direction::Forward => &progress.forward,
                    Direction::Compensation => &progress.compensation,
                };
                // Reports must belong to the current direction and unresolved attempt.
                if state.compensating != (*direction == Direction::Compensation)
                    || *attempt == 0
                    || action.budget_used == 0
                    || action.attempts != *attempt
                    || action.report.is_some()
                {
                    return Err(corrupt());
                }
                if matches!(result, ActionResult::Operational(f) if !f.is_valid()) {
                    return Err(corrupt());
                }
                state.action_mut(step, *direction).report = Some(result.clone());
            }
            Record::Retry {
                saga,
                step,
                direction,
                reason,
            } => {
                let state = self.states.get_mut(saga).ok_or_else(corrupt)?;
                if reason.is_empty()
                    || reason.len() > 4096
                    || !matches!(state.next(), Next::Blocked(_, _))
                {
                    return Err(corrupt());
                }
                let (expected, d) = Self::retry_action(state).map_err(|_| corrupt())?;
                if &expected != step || d != *direction {
                    return Err(corrupt());
                }
                let action = state.action_mut(step, *direction);
                action.budget_used = 0;
                // Preserve the last report: retry authority does not resolve ambiguity.
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
                phase: Some("saga_header".into()),
                ..Default::default()
            },
            Record::Begin { spec, .. } => crate::ErrorContext {
                transaction: Some(spec.id.as_str().into()),
                phase: Some("saga_begin".into()),
                ..Default::default()
            },
            Record::Compensate { saga } => crate::ErrorContext {
                transaction: Some(saga.as_str().into()),
                phase: Some("compensation".into()),
                ..Default::default()
            },
            Record::Attempt {
                saga,
                step,
                direction,
                ..
            }
            | Record::Report {
                saga,
                step,
                direction,
                ..
            }
            | Record::Retry {
                saga,
                step,
                direction,
                ..
            } => crate::ErrorContext {
                transaction: Some(saga.as_str().into()),
                step: Some(step.as_str().into()),
                phase: Some(
                    match direction {
                        Direction::Forward => "forward",
                        Direction::Compensation => "compensation",
                    }
                    .into(),
                ),
                ..Default::default()
            },
        };
        self.poisoned = true;
        let sequence = self.journal.append(5, &bytes).map_err(|e| {
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
            return Err(Error::in_doubt("Saga barrier did not cover record".into()));
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
    /// Last acknowledged durable journal position, including replay.
    pub fn replay_position(&self) -> u64 {
        self.sequence
    }
    /// Inspect durable progress without contacting any resource.
    pub fn status(&self, id: &SagaId) -> Result<Option<SagaStatus>, Error> {
        self.healthy()?;
        self.states
            .get(id)
            .map(|state| {
                let (outcome, needs_intervention, last_error) = match state.next() {
                    Next::Settled(outcome) => (outcome, false, None),
                    Next::Blocked(outcome, error) => (outcome, true, Some(error)),
                    Next::Compensate => (
                        SagaOutcome::Compensating,
                        false,
                        state
                            .steps
                            .values()
                            .find_map(|s| s.forward.error())
                            .map(str::to_owned),
                    ),
                    Next::Call(step, direction) => {
                        let a = state.action(&step, direction);
                        let outcome = if a.uncertain() {
                            SagaOutcome::Uncertain
                        } else if state.compensating {
                            SagaOutcome::Compensating
                        } else {
                            SagaOutcome::Running
                        };
                        (outcome, false, a.error().map(str::to_owned))
                    }
                };
                Ok(SagaStatus {
                    id: id.clone(),
                    fingerprint: state.spec.fingerprint()?,
                    outcome,
                    compensating: state.compensating,
                    steps: state.steps.clone(),
                    needs_intervention,
                    last_error,
                })
            })
            .transpose()
    }
    /// Stable direction-specific resource receipt key, unchanged by retry/restart.
    pub fn operation_key(
        &self,
        id: &SagaId,
        step: &StepId,
        direction: Direction,
    ) -> Result<OperationId, Error> {
        self.healthy()?;
        let state = self
            .states
            .get(id)
            .ok_or_else(|| Error::invalid("unknown Saga".into()))?;
        if !state.steps.contains_key(step) {
            return Err(Error::invalid("unknown Saga step".into()));
        }
        let bytes =
            serde_json::to_vec(&(&self.namespace, state.spec.fingerprint()?, step, direction))
                .map_err(Error::invalid_source)?;
        OperationId::new(format!("saga-v1-{}", crate::sha256_hex(bytes)))
    }
    fn check_adapters(&self, spec: &Saga) -> Result<BTreeMap<AdapterId, String>, Error> {
        let mut bindings = BTreeMap::new();
        for op in spec.operations() {
            let a = self
                .adapters
                .get(&op.adapter)
                .filter(|a| a.accepts(&op.payload))
                .ok_or_else(|| {
                    Error::invalid(format!("missing/incompatible Saga adapter {}", op.adapter))
                })?;
            let identity = a.identity();
            if identity.is_empty() || identity.len() > 65536 {
                return Err(Error::invalid("invalid Saga adapter identity".into()));
            }
            bindings.insert(op.adapter.clone(), identity);
        }
        Ok(bindings)
    }
    /// Bind immutable work or resume it. Cancellation is a pause, never failure.
    /// Forward failures start compensation only after definitive no-effect proof.
    /// Calls are serial, bounding resource ownership even if this future is dropped.
    pub async fn execute(
        &mut self,
        spec: Saga,
        cancellation: &Cancellation,
    ) -> Result<SagaStatus, Error> {
        self.healthy()?;
        let spec = spec.canonical()?;
        if let Some(old) = self.states.get(&spec.id)
            && old.spec != spec
        {
            return Err(Error::invalid("Saga ID reused with changed work".into()));
        }
        let bindings = self.check_adapters(&spec)?;
        if let Some(old) = self.states.get(&spec.id) {
            if old.bindings != bindings {
                return Err(Error::invalid(
                    "Saga adapter resource incarnation changed".into(),
                ));
            }
        } else {
            self.persist(Record::Begin {
                spec: spec.clone(),
                bindings,
            })?;
        }
        let mut calls = 0;
        loop {
            if cancellation.is_cancelled() {
                break;
            }
            let (step, direction) = match self.states[&spec.id].next() {
                Next::Compensate => {
                    self.persist(Record::Compensate {
                        saga: spec.id.clone(),
                    })?;
                    continue;
                }
                Next::Call(step, direction) if calls < self.policy.max_dispatches => {
                    (step, direction)
                }
                _ => break,
            };
            let action = self.states[&spec.id].action(&step, direction);
            let attempt = action
                .attempts
                .checked_add(1)
                .ok_or_else(|| Error::invalid("Saga attempt overflow".into()))?;
            if action.budget_used > 0 {
                tokio::select! { biased; _ = cancellation.cancelled() => break, _ = tokio::time::sleep(self.policy.retry_backoff) => {} }
            }
            let op = self.states[&spec.id].operation(&step, direction).clone();
            let adapter = self.adapters[&op.adapter].clone();
            let key = self.operation_key(&spec.id, &step, direction)?;
            self.persist(Record::Attempt {
                saga: spec.id.clone(),
                step: step.clone(),
                direction,
                attempt,
            })?;
            calls += 1;
            let context = crate::ErrorContext {
                transaction: Some(spec.id.as_str().into()),
                step: Some(step.as_str().into()),
                phase: Some(
                    match direction {
                        Direction::Forward => "forward",
                        Direction::Compensation => "compensation",
                    }
                    .into(),
                ),
                ..Default::default()
            };
            let (result, diagnostic) = tokio::select! {
                biased;
                _ = cancellation.cancelled() => saga_failure(crate::Diagnostic::message(crate::ErrorCode::Cancelled, "cancelled in flight".into()), context),
                result = tokio::time::timeout(self.policy.timeout, adapter.execute(key, op.payload)) => match result {
                    Ok(Ok(r)) => (r, None),
                    Ok(Err(e)) => saga_failure(e.diagnostic().clone(), context),
                    Err(e) => saga_failure(crate::Diagnostic::caused(crate::ErrorCode::Timeout, e), context),
                }
            };
            self.persist(Record::Report {
                saga: spec.id.clone(),
                step: step.clone(),
                direction,
                attempt,
                result,
            })?;
            self.states
                .get_mut(&spec.id)
                .unwrap()
                .action_mut(&step, direction)
                .diagnostic = diagnostic;
        }
        self.status(&spec.id)?
            .ok_or_else(|| Error::corrupt("Saga disappeared".into()))
    }
    /// Reconcile using the permanently retained specification and receipt keys.
    pub async fn resume(
        &mut self,
        id: &SagaId,
        cancellation: &Cancellation,
    ) -> Result<SagaStatus, Error> {
        self.healthy()?;
        let spec = self
            .states
            .get(id)
            .ok_or_else(|| Error::invalid("unknown Saga".into()))?
            .spec
            .clone();
        self.execute(spec, cancellation).await
    }
    fn retry_action(state: &State) -> Result<(StepId, Direction), Error> {
        let direction = if state.compensating {
            Direction::Compensation
        } else {
            Direction::Forward
        };
        let mut ids = state.order.clone();
        if state.compensating {
            ids.reverse();
        }
        for id in ids {
            if state.compensating && !state.steps[&id].forward.applied() {
                continue;
            }
            if !state.action(&id, direction).applied() {
                if state.compensating
                    && matches!(
                        state
                            .spec
                            .steps
                            .iter()
                            .find(|s| s.id == id)
                            .expect("validated DAG")
                            .compensation,
                        Compensation::Irreversible
                    )
                {
                    return Err(Error::invalid(
                        "irreversible boundary needs external remediation; cannot retry it away"
                            .into(),
                    ));
                }
                if matches!(
                    state.action(&id, direction).report,
                    Some(ActionResult::Rejected(_))
                ) {
                    return Err(Error::invalid("permanent rejection needs external remediation; retained tombstone cannot be retried away".into()));
                }
                return Ok((id, direction));
            }
        }
        Err(Error::invalid("no failed Saga action to retry".into()))
    }
    /// Explicit operator authorization for one additional bounded retry budget.
    /// Persist an audit reason after repairing the SAME adapter/receipt store.
    /// Cannot erase ambiguity, bypass irreversible effects or change any payload.
    pub fn retry(&mut self, id: &SagaId, reason: impl Into<String>) -> Result<(), Error> {
        self.healthy()?;
        let state = self
            .states
            .get(id)
            .ok_or_else(|| Error::invalid("unknown Saga".into()))?;
        if !matches!(state.next(), Next::Blocked(_, _)) {
            return Err(Error::invalid("Saga does not require intervention".into()));
        }
        let reason = reason.into();
        if reason.is_empty() || reason.len() > 4096 {
            return Err(Error::invalid(
                "retry requires an audit reason of 1..=4096 bytes".into(),
            ));
        }
        let (step, direction) = Self::retry_action(state)?;
        self.persist(Record::Retry {
            saga: id.clone(),
            step,
            direction,
            reason,
        })
    }
    /// Visit unfinished Sagas once in ID order; manual blocks remain inspectable.
    pub async fn recover_pending(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Vec<SagaStatus>, Error> {
        self.healthy()?;
        let ids: Vec<_> = self
            .states
            .keys()
            .filter_map(|id| match self.status(id) {
                Ok(Some(s)) if !s.is_terminal() => Some(Ok(id.clone())),
                Err(e) => Some(Err(e)),
                _ => None,
            })
            .collect::<Result<_, _>>()?;
        let mut statuses = Vec::new();
        for id in ids {
            if cancellation.is_cancelled() {
                break;
            }
            statuses.push(self.resume(&id, cancellation).await?);
        }
        Ok(statuses)
    }
}
#[async_trait]
impl<J: Journal> crate::recovery::RecoveryTarget for SagaEngine<J> {
    fn pending_work(&self) -> Result<Vec<crate::recovery::Work>, Error> {
        self.healthy()?;
        self.states
            .keys()
            .filter_map(|id| match self.status(id) {
                Ok(Some(s)) if !s.is_terminal() => Some(Ok(crate::recovery::Work::Saga(s))),
                Err(e) => Some(Err(e)),
                _ => None,
            })
            .collect()
    }
    fn replay_position(&self) -> u64 {
        self.sequence
    }
    async fn recover_one(
        &mut self,
        key: &str,
        cancellation: &Cancellation,
    ) -> Result<crate::recovery::Work, Error> {
        let s = self.resume(&SagaId::new(key)?, cancellation).await?;
        if s.needs_intervention {
            return Err(Error::operational(crate::Diagnostic::message(
                crate::ErrorCode::OperatorRequired,
                "Saga needs explicit intervention".into(),
            ))
            .with_context(crate::ErrorContext {
                transaction: Some(key.into()),
                phase: Some("saga_recovery".into()),
                ..Default::default()
            }));
        }
        Ok(crate::recovery::Work::Saga(s))
    }
    fn resume_work(&mut self, key: &str) -> Result<(), Error> {
        let id = SagaId::new(key)?;
        let s = self
            .status(&id)?
            .ok_or_else(|| Error::invalid("unknown Saga".into()))?;
        if s.needs_intervention {
            self.retry(&id, "recovery-service operator Resume")?;
        }
        Ok(())
    }
}
