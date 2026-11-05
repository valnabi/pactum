//! Optional pure planning over frozen provider capabilities. See docs/transaction-planner.md.
use crate::execution::{AtomicRequest, ChunkedRequest, Mode, OperationId, PublishRequest, Request};
use crate::plan::{AdapterId, PlanId, Step, StepId, StepWork, TransactionPlan};
use crate::{Enlistment, Error};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Budget dimensions. Durations are milliseconds; bytes include provider overhead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Dimension {
    /// Effective backend bytes.
    Bytes,
    /// Backend operations.
    Operations,
    /// Estimated duration in milliseconds.
    Duration,
    /// Peak working memory in bytes.
    Memory,
    /// Simultaneous backend work units.
    Concurrency,
    /// Provider-estimated conflict footprint (provider-defined units).
    Conflicts,
}
/// Missing dimensions are unknown, never zero. Estimates are not runtime deadlines.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEstimate(pub BTreeMap<Dimension, u64>);
/// Hard constraints fail closed on unknown estimates; soft constraints produce warnings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceBudget {
    /// Mandatory upper bounds, inclusive.
    pub hard: BTreeMap<Dimension, u64>,
    /// Preferred upper bounds, inclusive; used as partition targets.
    pub soft: BTreeMap<Dimension, u64>,
}
/// Explicit operation-wide consistency; staged visibility never satisfies native ACID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Consistency {
    /// One backend transaction.
    NativeAtomic,
    /// Partial application is allowed, including already completed chunks.
    PartialAllowed,
    /// Hidden chunks with one fenced pointer publication; readers must use that pointer.
    StagedVisibility,
    /// Existing prepared-participant durable decision, without global snapshot isolation.
    TwoPhaseDecision,
}
/// An indivisible host-encoded operation or immutable external artifact reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkUnit {
    /// Adapter-defined data embedded in the planner envelope.
    pub payload: Vec<u8>,
    /// Provider backend-work estimates, including external data, excluding the planner envelope.
    pub estimate: PlanEstimate,
}
/// Publication metadata; payload must describe the artifact assembled from hidden chunks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    /// Visible pointer.
    pub target: OperationId,
    /// Fenced expected generation.
    pub expected_generation: u64,
    /// Host manifest interpreted by the opt-in adapter.
    pub manifest: WorkUnit,
}
/// Declarative work. The planner never decodes application operations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum IntentWork {
    /// Ordered units for a planner-envelope-aware native adapter.
    Native {
        /// Indivisible units in semantic order.
        units: Vec<WorkUnit>,
        /// Required for staged visibility.
        publication: Option<Publication>,
    },
    /// Already encoded immutable enlistments; never split a 2PC decision.
    TwoPhase {
        /// Existing coordinator work.
        enlistments: Vec<Enlistment>,
        /// Aggregate estimates across prepared participants.
        estimate: PlanEstimate,
    },
}
/// Immutable declarative transaction intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionIntent {
    /// Permanent execution identity; changed input requires a new ID.
    pub id: PlanId,
    /// Positive host revision.
    pub revision: u64,
    /// Registered execution adapter.
    pub adapter: AdapterId,
    /// Required operation-wide guarantee.
    pub consistency: Consistency,
    /// Immutable work.
    pub work: IntentWork,
}
/// Frozen provider report. Limits describe one backend transaction, not the whole plan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCapabilities {
    /// Provider incarnation/version; included in accepted identity.
    pub snapshot: String,
    /// Explicit supported protocols.
    pub modes: Vec<Mode>,
    /// Adapter implements PlannerEnvelope v1 semantics (including hidden staging).
    pub planner_envelope_v1: bool,
    /// Per-transaction constraints; hard limits take precedence over soft preferences.
    pub budget: ResourceBudget,
    /// Fixed cost of each backend transaction; unknown dimensions stay unknown.
    pub overhead: PlanEstimate,
    /// Reserve this many basis points of hard limits (0..=10000).
    pub safety_margin_bps: u16,
    /// Provider-specific opaque data, frozen and fingerprinted without core interpretation.
    pub extensions: BTreeMap<String, Vec<u8>>,
}
/// Host resolves dynamic providers before planning. No adapter is called by the planner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanningContext {
    /// Frozen reports by adapter identity.
    pub capabilities: BTreeMap<AdapterId, ResourceCapabilities>,
    /// Operation-wide constraints; memory/concurrency are peaks, other dimensions additive.
    pub budget: ResourceBudget,
}
/// Dynamic capability resolution is deliberately separate from pure planning.
pub trait CapabilityProvider {
    /// Capture one report. Hosts freeze all reports before invoking a planner.
    fn capabilities(&self) -> Result<ResourceCapabilities, PlanningError>;
}
/// Stable planner failure categories, never execution or commit outcomes.
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
pub enum PlanningError {
    /// Malformed input or unsupported schema.
    #[snafu(display("invalid planning input: {detail}"))]
    Invalid {
        /// Validation detail.
        detail: String,
    },
    /// Requested guarantee cannot be provided by the advertised protocols.
    #[snafu(display("requested consistency is unsupported"))]
    UnsupportedConsistency,
    /// An unknown estimate or excessive estimate violates a mandatory constraint.
    #[snafu(display("hard budget cannot be satisfied: {dimension:?}"))]
    HardBudget {
        /// Failed dimension.
        dimension: Dimension,
    },
    /// No bounded plan can represent the work.
    #[snafu(display("no feasible bounded plan"))]
    Infeasible,
}
fn invalid(detail: impl Into<String>) -> PlanningError {
    PlanningError::Invalid {
        detail: detail.into(),
    }
}
fn engine_error(e: Error) -> PlanningError {
    invalid(e.to_string())
}

/// Object-safe pure extension point. Use ExecutionPlan::accept to validate custom partitions.
pub trait TransactionPlanner: Send + Sync {
    /// Produce a validated immutable plan from exactly these frozen inputs.
    fn plan(
        &self,
        intent: &TransactionIntent,
        context: &PlanningContext,
    ) -> Result<ExecutionPlan, PlanningError>;
}
/// Deterministic built-in strategy selection and contiguous greedy partitioning.
#[derive(Default)]
pub struct DefaultTransactionPlanner;
/// Host-owned named planner registration; no global mutable registry.
#[derive(Default)]
pub struct PlannerRegistry(BTreeMap<String, Arc<dyn TransactionPlanner>>);
impl PlannerRegistry {
    /// Register a unique host-selected name. Duplicate names are rejected.
    pub fn register(
        &mut self,
        name: String,
        planner: Arc<dyn TransactionPlanner>,
    ) -> Result<(), PlanningError> {
        if name.is_empty() || self.0.contains_key(&name) {
            return Err(invalid("duplicate or empty planner name"));
        }
        self.0.insert(name, planner);
        Ok(())
    }
    /// Select and dry-run a registered planner, checking its frozen input binding.
    pub fn plan(
        &self,
        name: &str,
        intent: &TransactionIntent,
        context: &PlanningContext,
    ) -> Result<ExecutionPlan, PlanningError> {
        let result = self
            .0
            .get(name)
            .ok_or_else(|| invalid("unknown planner"))?
            .plan(intent, context)?;
        if &result.recipe.intent != intent || &result.recipe.context != context {
            return Err(invalid("planner returned a different input binding"));
        }
        Ok(result)
    }
}
/// Adapter payload protocol. Opt-in adapters must implement this envelope, not concatenate bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum PlannerEnvelope {
    /// Apply ordered units in one native transaction.
    Apply {
        /// Schema version: 1.
        version: u32,
        /// Original opaque unit payloads.
        units: Vec<Vec<u8>>,
    },
    /// Write immutable hidden chunks; never expose these as live writes.
    Stage {
        /// Schema version: 1.
        version: u32,
        /// Frozen-input artifact namespace, shared with Publish.
        artifact: String,
        /// Stable ordered chunk index.
        index: usize,
        /// Original opaque unit payloads.
        units: Vec<Vec<u8>>,
    },
    /// Validate all hidden chunks, then publish through the existing fenced CAS protocol.
    Publish {
        /// Schema version: 1.
        version: u32,
        /// Hidden artifact namespace.
        artifact: String,
        /// Exact expected chunk count; validation must refuse missing/failed chunks.
        chunks: usize,
        /// Host-supplied artifact manifest.
        manifest: Vec<u8>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Recipe {
    version: u32,
    intent: TransactionIntent,
    context: PlanningContext,
    mode: Mode,
    ends: Vec<usize>,
}
/// Serializable dry-run explanation. Guarantees retain their mode-specific boundaries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanExplanation {
    /// Selected strategy.
    pub strategy: Mode,
    /// Aggregate estimated cost.
    pub estimate: PlanEstimate,
    /// Effective per-transaction hard budget after safety reserve.
    pub transaction_budget: ResourceBudget,
    /// Operation-wide budget.
    pub budget: ResourceBudget,
    /// Stable soft-budget reason codes by scope and dimension.
    pub warnings: Vec<String>,
    /// Explicit atomicity boundary.
    pub guarantee: String,
}
/// Immutable accepted plan. Deserialization revalidates the recipe and rebuilds the DAG.
/// The serialized recipe includes frozen inputs and partition boundaries, never runtime state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Recipe", into = "Recipe")]
pub struct ExecutionPlan {
    recipe: Recipe,
    plan: TransactionPlan,
    explanation: PlanExplanation,
}
impl From<ExecutionPlan> for Recipe {
    fn from(p: ExecutionPlan) -> Self {
        p.recipe
    }
}
impl TryFrom<Recipe> for ExecutionPlan {
    type Error = PlanningError;
    fn try_from(r: Recipe) -> Result<Self, Self::Error> {
        if r.version != 1 {
            return Err(invalid("unsupported planner version"));
        }
        Self::accept(&r.intent, &r.context, r.mode, r.ends)
    }
}
impl ExecutionPlan {
    /// Validate a custom strategy and exclusive cumulative partition ends. No writes occur.
    /// Atomic/2PC require a single partition; staged publication adds a dependent final step.
    pub fn accept(
        intent: &TransactionIntent,
        context: &PlanningContext,
        mode: Mode,
        ends: Vec<usize>,
    ) -> Result<Self, PlanningError> {
        let caps = context
            .capabilities
            .get(&intent.adapter)
            .ok_or_else(|| invalid("missing capabilities"))?;
        if caps.snapshot.is_empty() || caps.safety_margin_bps > 10000 {
            return Err(invalid("invalid capability snapshot or margin"));
        }
        let allowed = match intent.consistency {
            Consistency::NativeAtomic => mode == Mode::Atomic,
            Consistency::PartialAllowed => matches!(mode, Mode::Atomic | Mode::Chunked),
            Consistency::StagedVisibility => mode == Mode::StagedPublish,
            Consistency::TwoPhaseDecision => mode == Mode::TwoPhase,
        };
        if !allowed || !caps.modes.contains(&mode) {
            return Err(PlanningError::UnsupportedConsistency);
        }
        let mut budget = caps.budget.clone();
        for value in budget.hard.values_mut() {
            *value = ((*value as u128 * (10000 - caps.safety_margin_bps as u128)) / 10000) as u64;
        }
        let recipe = Recipe {
            version: 1,
            intent: intent.clone(),
            context: context.clone(),
            mode,
            ends: ends.clone(),
        };
        let artifact = crate::sha256_hex(
            serde_json::to_vec(&(intent, context)).map_err(|e| invalid(e.to_string()))?,
        );
        let mut steps = Vec::new();
        let mut costs = Vec::new();
        let mut warnings = Vec::new();
        match &intent.work {
            IntentWork::TwoPhase {
                enlistments,
                estimate,
            } => {
                if mode != Mode::TwoPhase || ends != [1] {
                    return Err(PlanningError::UnsupportedConsistency);
                }
                let bytes = serde_json::to_vec(enlistments)
                    .map_err(|e| invalid(e.to_string()))?
                    .len();
                costs.push(effective(estimate.clone(), &caps.overhead, bytes)?);
                steps.push(make_step(
                    intent,
                    0,
                    BTreeSet::new(),
                    StepWork::TwoPhase(enlistments.clone()),
                )?);
            }
            IntentWork::Native { units, publication } => {
                if mode == Mode::TwoPhase || !caps.planner_envelope_v1 {
                    return Err(PlanningError::UnsupportedConsistency);
                }
                if units.is_empty()
                    || ends.is_empty()
                    || ends.len() > 1023
                    || ends.last() != Some(&units.len())
                    || ends.windows(2).any(|w| w[0] >= w[1])
                    || ends[0] == 0
                    || (mode == Mode::Atomic && ends.len() != 1)
                {
                    return Err(PlanningError::Infeasible);
                }
                let mut start = 0;
                let mut chunks = Vec::new();
                let mut previous = BTreeSet::new();
                for (index, &end) in ends.iter().enumerate() {
                    let group = units.get(start..end).ok_or(PlanningError::Infeasible)?;
                    let payloads = group.iter().map(|u| u.payload.clone()).collect();
                    let envelope = if mode == Mode::StagedPublish {
                        PlannerEnvelope::Stage {
                            version: 1,
                            artifact: artifact.clone(),
                            index,
                            units: payloads,
                        }
                    } else {
                        PlannerEnvelope::Apply {
                            version: 1,
                            units: payloads,
                        }
                    };
                    let bytes = encode(&envelope)?;
                    costs.push(effective(
                        aggregate_units(group.iter().map(|u| &u.estimate))?,
                        &caps.overhead,
                        bytes.len(),
                    )?);
                    if mode == Mode::StagedPublish {
                        let step = make_step(
                            intent,
                            index,
                            previous,
                            StepWork::Native(Request::Atomic(AtomicRequest { payload: bytes })),
                        )?;
                        previous = BTreeSet::from([step.id.clone()]);
                        steps.push(step);
                    } else {
                        chunks.push(bytes);
                    }
                    start = end;
                }
                if mode == Mode::StagedPublish {
                    // Staging uses native atomic transactions even though the operation guarantee is visibility.
                    if !caps.modes.contains(&Mode::Atomic) {
                        return Err(PlanningError::UnsupportedConsistency);
                    }
                    let publication = publication
                        .as_ref()
                        .ok_or_else(|| invalid("missing publication"))?;
                    let payload = encode(&PlannerEnvelope::Publish {
                        version: 1,
                        artifact,
                        chunks: ends.len(),
                        manifest: publication.manifest.payload.clone(),
                    })?;
                    costs.push(effective(
                        publication.manifest.estimate.clone(),
                        &caps.overhead,
                        payload.len(),
                    )?);
                    steps.push(make_step(
                        intent,
                        ends.len(),
                        previous,
                        StepWork::Native(Request::StagedPublish(PublishRequest {
                            target: publication.target.clone(),
                            expected_generation: publication.expected_generation,
                            payload,
                        })),
                    )?);
                } else {
                    let request = if mode == Mode::Atomic {
                        Request::Atomic(AtomicRequest {
                            payload: chunks.remove(0),
                        })
                    } else {
                        Request::Chunked(ChunkedRequest { chunks })
                    };
                    steps.push(make_step(
                        intent,
                        0,
                        BTreeSet::new(),
                        StepWork::Native(request),
                    )?);
                }
            }
        }
        for cost in &costs {
            check_budget(cost, &budget, "transaction", &mut warnings)?;
        }
        let estimate = aggregate(costs.iter())?;
        check_budget(&estimate, &context.budget, "plan", &mut warnings)?;
        warnings.sort();
        warnings.dedup();
        // Bind execution receipts to every accepted input, including the frozen budgets.
        let binding =
            crate::sha256_hex(serde_json::to_vec(&recipe).map_err(|e| invalid(e.to_string()))?);
        for step in &mut steps {
            step.id = StepId::new(format!("{}-{binding}", step.id)).map_err(engine_error)?;
            step.dependencies = step
                .dependencies
                .iter()
                .map(|id| StepId::new(format!("{id}-{binding}")).map_err(engine_error))
                .collect::<Result<_, _>>()?;
        }
        let plan = TransactionPlan {
            version: 1,
            id: intent.id.clone(),
            revision: intent.revision,
            steps,
        };
        plan.validate().map_err(engine_error)?;
        Ok(Self {
            recipe,
            plan,
            explanation: PlanExplanation {
                strategy: mode,
                estimate,
                transaction_budget: budget,
                budget: context.budget.clone(),
                warnings,
                guarantee: mode.guarantees().atomicity.into(),
            },
        })
    }
    /// Borrow the validated DAG for the existing PlanEngine. Clone to submit; accepted data stays immutable.
    pub fn transaction_plan(&self) -> &TransactionPlan {
        &self.plan
    }
    /// Inspect strategy, effective budgets, estimates, warnings and guarantee without writes.
    pub fn explain(&self) -> &PlanExplanation {
        &self.explanation
    }
    /// Stable versioned identity binding intent, frozen snapshot, budgets and partition decisions.
    pub fn fingerprint(&self) -> String {
        crate::sha256_hex(serde_json::to_vec(&self.recipe).expect("serializable planner recipe"))
    }
}
fn encode(envelope: &PlannerEnvelope) -> Result<Vec<u8>, PlanningError> {
    serde_json::to_vec(envelope).map_err(|e| invalid(e.to_string()))
}
fn make_step(
    intent: &TransactionIntent,
    index: usize,
    dependencies: BTreeSet<StepId>,
    work: StepWork,
) -> Result<Step, PlanningError> {
    Ok(Step {
        id: StepId::new(format!("planner-{index:06}")).map_err(engine_error)?,
        adapter: intent.adapter.clone(),
        dependencies,
        resources: BTreeSet::new(),
        work,
    })
}
fn aggregate<'a>(
    estimates: impl Iterator<Item = &'a PlanEstimate>,
) -> Result<PlanEstimate, PlanningError> {
    let values: Vec<_> = estimates.collect();
    let mut result = BTreeMap::new();
    for dimension in [
        Dimension::Bytes,
        Dimension::Operations,
        Dimension::Duration,
        Dimension::Memory,
        Dimension::Concurrency,
        Dimension::Conflicts,
    ] {
        let mut total = 0u64;
        if values.iter().all(|e| e.0.contains_key(&dimension)) {
            for e in &values {
                let v = e.0[&dimension];
                total = if matches!(dimension, Dimension::Memory | Dimension::Concurrency) {
                    total.max(v)
                } else {
                    total.checked_add(v).ok_or(PlanningError::Infeasible)?
                };
            }
            result.insert(dimension, total);
        }
    }
    Ok(PlanEstimate(result))
}
// All units in a transaction may occupy memory together. Across sequential transactions
// memory is a peak; concurrency is a peak in both cases.
fn aggregate_units<'a>(
    estimates: impl Iterator<Item = &'a PlanEstimate>,
) -> Result<PlanEstimate, PlanningError> {
    let values: Vec<_> = estimates.collect();
    let mut result = aggregate(values.iter().copied())?;
    if values.iter().all(|e| e.0.contains_key(&Dimension::Memory)) {
        let mut memory = 0u64;
        for e in values {
            memory = memory
                .checked_add(e.0[&Dimension::Memory])
                .ok_or(PlanningError::Infeasible)?;
        }
        result.0.insert(Dimension::Memory, memory);
    }
    Ok(result)
}
fn effective(
    mut estimate: PlanEstimate,
    overhead: &PlanEstimate,
    encoded_bytes: usize,
) -> Result<PlanEstimate, PlanningError> {
    // Unknown provider overhead is zero by declaration; unknown workload costs remain unknown.
    for (&dimension, &value) in &overhead.0 {
        if let Some(current) = estimate.0.get_mut(&dimension) {
            *current = current
                .checked_add(value)
                .ok_or(PlanningError::Infeasible)?;
        }
    }
    // Charge the encoded request in addition to backend work: adapters may retain
    // it in receipts while writing the referenced logical data in the same transaction.
    for dimension in [Dimension::Bytes, Dimension::Memory] {
        if let Some(value) = estimate.0.get_mut(&dimension) {
            *value = value
                .checked_add(encoded_bytes as u64)
                .ok_or(PlanningError::Infeasible)?;
        }
    }
    let concurrency = estimate.0.entry(Dimension::Concurrency).or_insert(1);
    *concurrency = (*concurrency).max(1);
    Ok(estimate)
}
fn check_budget(
    estimate: &PlanEstimate,
    budget: &ResourceBudget,
    scope: &str,
    warnings: &mut Vec<String>,
) -> Result<(), PlanningError> {
    for (&dimension, &limit) in &budget.hard {
        if estimate.0.get(&dimension).is_none_or(|&v| v > limit) {
            return Err(PlanningError::HardBudget { dimension });
        }
    }
    for (&dimension, &limit) in &budget.soft {
        if estimate.0.get(&dimension).is_none_or(|&v| v > limit) {
            warnings.push(format!("soft_budget:{scope}:{dimension:?}"));
        }
    }
    Ok(())
}
impl DefaultTransactionPlanner {
    fn plan_with_targets(
        &self,
        intent: &TransactionIntent,
        context: &PlanningContext,
    ) -> Result<ExecutionPlan, PlanningError> {
        let mode = match intent.consistency {
            Consistency::NativeAtomic => Mode::Atomic,
            Consistency::PartialAllowed => Mode::Chunked,
            Consistency::StagedVisibility => Mode::StagedPublish,
            Consistency::TwoPhaseDecision => Mode::TwoPhase,
        };
        let IntentWork::Native { units, .. } = &intent.work else {
            return ExecutionPlan::accept(intent, context, mode, vec![1]);
        };
        // Prefer one native transaction; unknown soft estimates cannot be improved by splitting.
        if intent.consistency == Consistency::PartialAllowed
            && let Ok(plan) =
                ExecutionPlan::accept(intent, context, Mode::Atomic, vec![units.len()])
            && (plan.explanation.warnings.is_empty()
                || !context.capabilities[&intent.adapter]
                    .modes
                    .contains(&Mode::Chunked)
                || !context.capabilities[&intent.adapter]
                    .budget
                    .soft
                    .iter()
                    .any(|(d, limit)| {
                        plan.explanation
                            .estimate
                            .0
                            .get(d)
                            .is_some_and(|v| v > limit)
                    }))
        {
            return Ok(plan);
        }
        if mode == Mode::Atomic {
            return ExecutionPlan::accept(intent, context, mode, vec![units.len()]);
        }
        let caps = context
            .capabilities
            .get(&intent.adapter)
            .ok_or_else(|| invalid("missing capabilities"))?;
        if !caps.modes.contains(&mode) || !caps.planner_envelope_v1 {
            return Err(PlanningError::UnsupportedConsistency);
        }
        // Greedy maximal contiguous batches, using the exact envelope and provider estimates.
        // Soft limits guide splits, but an indivisible unit may exceed a soft limit.
        let artifact = crate::sha256_hex(
            serde_json::to_vec(&(intent, context)).map_err(|e| invalid(e.to_string()))?,
        );
        let mut hard = caps.budget.clone();
        if caps.safety_margin_bps > 10000 {
            return Err(invalid("invalid margin"));
        }
        for v in hard.hard.values_mut() {
            *v = ((*v as u128 * (10000 - caps.safety_margin_bps as u128)) / 10000) as u64;
        }
        let mut ends = Vec::new();
        let mut start = 0;
        let mut end = 0;
        let mut running: Option<PlanEstimate> = None;
        let mut payload_bytes = 0usize;
        while end < units.len() {
            let unit = &units[end];
            let cost = match &running {
                Some(previous) => aggregate_units([previous, &unit.estimate].into_iter())?,
                None => unit.estimate.clone(),
            };
            let candidate_bytes = payload_bytes
                .checked_add(
                    serde_json::to_vec(&unit.payload)
                        .map_err(|e| invalid(e.to_string()))?
                        .len(),
                )
                .and_then(|v| v.checked_add(usize::from(end > start)))
                .ok_or(PlanningError::Infeasible)?;
            let envelope = if mode == Mode::StagedPublish {
                PlannerEnvelope::Stage {
                    version: 1,
                    artifact: artifact.clone(),
                    index: ends.len(),
                    units: vec![],
                }
            } else {
                PlannerEnvelope::Apply {
                    version: 1,
                    units: vec![],
                }
            };
            let encoded_bytes = encode(&envelope)?
                .len()
                .checked_add(candidate_bytes)
                .ok_or(PlanningError::Infeasible)?;
            let effective_cost = effective(cost.clone(), &caps.overhead, encoded_bytes)?;
            let mut warnings = Vec::new();
            let result = check_budget(&effective_cost, &hard, "transaction", &mut warnings);
            if result.is_err()
                || (hard
                    .soft
                    .iter()
                    .any(|(d, limit)| effective_cost.0.get(d).is_some_and(|v| v > limit))
                    && end > start)
            {
                if end == start {
                    result?;
                } else {
                    ends.push(end);
                    start = end;
                    running = None;
                    payload_bytes = 0;
                    if ends.len() > 1023 {
                        return Err(PlanningError::Infeasible);
                    }
                    continue;
                }
            }
            running = Some(cost);
            payload_bytes = candidate_bytes;
            end += 1;
        }
        ends.push(end);
        ExecutionPlan::accept(intent, context, mode, ends)
    }
}

impl TransactionPlanner for DefaultTransactionPlanner {
    fn plan(
        &self,
        intent: &TransactionIntent,
        context: &PlanningContext,
    ) -> Result<ExecutionPlan, PlanningError> {
        match self.plan_with_targets(intent, context) {
            Ok(plan) => Ok(plan),
            Err(original) => {
                // Soft targets must never make a hard-feasible plan fail (for example,
                // extra per-chunk overhead exceeding an operation-wide byte budget).
                if context
                    .capabilities
                    .get(&intent.adapter)
                    .is_none_or(|c| c.budget.soft.is_empty())
                {
                    return Err(original);
                }
                let mut hard_context = context.clone();
                hard_context
                    .capabilities
                    .get_mut(&intent.adapter)
                    .expect("checked capability")
                    .budget
                    .soft
                    .clear();
                match self.plan_with_targets(intent, &hard_context) {
                    Ok(plan) => {
                        ExecutionPlan::accept(intent, context, plan.recipe.mode, plan.recipe.ends)
                    }
                    Err(_) => Err(original),
                }
            }
        }
    }
}
