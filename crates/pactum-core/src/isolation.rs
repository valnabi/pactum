//! Optional negotiated participant isolation and optimistic validation.
//!
//! Isolation is enforced by opt-in adapters at existing resource transaction or
//! prepare barriers, never by a check-then-execute call in the plan runner.
//! See `docs/isolation.md` for guarantees, recovery rules and unsupported claims.
use crate::execution::{Mode, OperationId, Request};
use crate::plan::{AdapterId, StepAdapter, StepId, StepResult, StepWork, TransactionPlan};
use crate::{Cancellation, Error, ParticipantId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Isolation within one participant transaction. No ordering or promotion is inferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Adapter exposes no additional isolation guarantee.
    Unspecified,
    /// No dirty reads; repeatable reads and phantoms are not excluded.
    ReadCommitted,
    /// One participant snapshot; write skew can remain possible.
    Snapshot,
    /// Serializable within one participant, including predicate conflicts.
    Serializable,
}
/// Scope of a requested guarantee. Global scopes are currently unsupported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationScope {
    /// Each participant transaction independently; chunks have separate boundaries.
    ParticipantTransaction,
    /// A shared read snapshot across resources (unsupported).
    GlobalSnapshot,
    /// A serial order across resources (unsupported).
    GlobalSerializable,
}
/// Optional conflict evidence that an adapter must validate at its effect barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ValidationKind {
    /// Validate observed key versions, including absence tokens.
    Reads,
    /// Validate expected write versions; blind writes must be explicitly supported.
    Writes,
    /// Validate predicate/range tokens to detect insertions, deletions and phantoms.
    Predicates,
}
/// Opaque provider version, not a globally ordered timestamp.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct VersionStamp(Vec<u8>);
impl VersionStamp {
    /// Validate a nonempty provider token of at most 4096 bytes.
    pub fn new(bytes: Vec<u8>) -> Result<Self, Error> {
        if bytes.is_empty() || bytes.len() > 4096 {
            return Err(Error::invalid("invalid isolation version stamp".into()));
        }
        Ok(Self(bytes))
    }
    /// Borrow provider bytes; only the originating adapter may interpret them.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl TryFrom<Vec<u8>> for VersionStamp {
    type Error = Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Error> {
        Self::new(bytes)
    }
}
impl From<VersionStamp> for Vec<u8> {
    fn from(value: VersionStamp) -> Self {
        value.0
    }
}
/// Optional bounded conflict footprint for one resource. Keys and predicates are
/// provider-defined identities; the adapter must verify completeness against work.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConflictFootprint {
    /// Observed key or absence versions.
    pub reads: BTreeMap<String, VersionStamp>,
    /// Expected versions for writes; None explicitly declares a blind write.
    pub writes: BTreeMap<String, Option<VersionStamp>>,
    /// Range/query identities and observed predicate versions.
    pub predicates: BTreeMap<String, VersionStamp>,
}
/// Frozen advertisement for a single resource incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolationCapabilities {
    /// Stable provider incarnation/configuration version, not a read timestamp.
    pub snapshot: String,
    /// Explicit supported levels; stronger levels are never inferred.
    pub levels: BTreeSet<IsolationLevel>,
    /// Supported atomic validation kinds.
    pub validation: BTreeSet<ValidationKind>,
    /// Provider can safely validate explicit blind writes.
    pub blind_writes: bool,
    /// Validation protection survives prepare until the final durable 2PC decision.
    pub prepared_protection: bool,
    /// Authority is checked atomically with all relevant writes and prepare.
    pub fencing: bool,
}
/// Immutable guarantee requested for a resource, independent of commit atomicity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IsolationRequirement {
    /// Per-participant guarantee.
    pub level: IsolationLevel,
    /// Requested scope; global claims fail closed.
    pub scope: IsolationScope,
    /// Evidence kinds to enforce; footprint presence does not silently enable them.
    pub validation: BTreeSet<ValidationKind>,
    /// Require adapter authority fencing at the transaction/prepare barrier.
    pub fencing: bool,
}
/// One resource's accepted contract and immutable conflict evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceIsolation {
    /// Explicit requested guarantees.
    pub requirement: IsolationRequirement,
    /// Frozen provider report.
    pub capabilities: IsolationCapabilities,
    /// Optional evidence, interpreted and checked for completeness by the adapter.
    pub footprint: ConflictFootprint,
}
/// Isolation contract for one existing plan step. Native work names exactly one
/// resource; 2PC work must name every enlisted participant, without extra entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepIsolation {
    /// Stable resource identities, identical to participant IDs for 2PC.
    pub resources: BTreeMap<ParticipantId, ResourceIsolation>,
}
impl StepIsolation {
    /// Negotiate explicit guarantees without interpreting application payloads.
    pub fn validate(&self, work: &StepWork) -> Result<(), Error> {
        let expected: BTreeSet<_> = match work {
            StepWork::TwoPhase(enlistments) => {
                enlistments.iter().map(|e| e.participant.clone()).collect()
            }
            StepWork::Native(_) => {
                if self.resources.len() != 1 {
                    return Err(invalid("native work needs one resource"));
                }
                self.resources.keys().cloned().collect()
            }
        };
        if self.resources.is_empty()
            || self.resources.keys().cloned().collect::<BTreeSet<_>>() != expected
        {
            return Err(invalid(
                "isolation must cover exactly the enlisted participants",
            ));
        }
        for resource in self.resources.values() {
            let r = &resource.requirement;
            let c = &resource.capabilities;
            valid_name(&c.snapshot)?;
            if r.scope != IsolationScope::ParticipantTransaction
                || !c.levels.contains(&r.level)
                || !r.validation.is_subset(&c.validation)
                || (r.fencing && !c.fencing)
                || (work.mode() == Mode::TwoPhase
                    && (r.level != IsolationLevel::Unspecified || !r.validation.is_empty())
                    && !c.prepared_protection)
            {
                return Err(invalid(
                    "unsupported isolation guarantee or decision barrier",
                ));
            }
            let f = &resource.footprint;
            if (!f.reads.is_empty() && !r.validation.contains(&ValidationKind::Reads))
                || (!f.writes.is_empty() && !r.validation.contains(&ValidationKind::Writes))
                || (!f.predicates.is_empty() && !r.validation.contains(&ValidationKind::Predicates))
                || (f.writes.values().any(Option::is_none) && !c.blind_writes)
                || f.reads
                    .len()
                    .saturating_add(f.writes.len())
                    .saturating_add(f.predicates.len())
                    > 65536
            {
                return Err(invalid("unsupported or oversized conflict footprint"));
            }
            for key in f
                .reads
                .keys()
                .chain(f.writes.keys())
                .chain(f.predicates.keys())
            {
                valid_name(key)?;
            }
            // A single preobserved footprint cannot describe several independent
            // chunk transactions. Planners must attach evidence to each Atomic step.
            if matches!(work, StepWork::Native(Request::Chunked(_))) && !r.validation.is_empty() {
                return Err(invalid(
                    "split validated chunks into individual atomic plan steps",
                ));
            }
        }
        Ok(())
    }
    /// The exact reports required at dispatch and atomically at the effect barrier.
    pub fn capabilities(&self) -> BTreeMap<ParticipantId, IsolationCapabilities> {
        self.resources
            .iter()
            .map(|(id, r)| (id.clone(), r.capabilities.clone()))
            .collect()
    }
}
fn invalid(detail: &str) -> Error {
    Error::invalid(detail.into())
}
fn valid_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
        return Err(invalid("invalid isolation identity"));
    }
    Ok(())
}

/// Safe failures for optional validation adapters. A conflict is definitive only
/// when the adapter proves no effects and retains a rejection receipt for the key.
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
pub enum IsolationFailure {
    /// Definite validation rejection, with no effects for this key.
    #[snafu(display("isolation conflict"))]
    Conflict,
    /// Frozen capabilities no longer match; no fresh work was dispatched.
    #[snafu(display("isolation capability snapshot changed"))]
    CapabilityChanged,
    /// Stale authority; must never authorize an effect or reinterpret a decision.
    #[snafu(display("isolation authority is stale"))]
    Fenced,
    /// Effects, validation or commit status cannot be proven. Reconcile the same key.
    #[snafu(display("isolation outcome is unknown"))]
    Unknown,
}
impl From<IsolationFailure> for Error {
    fn from(value: IsolationFailure) -> Self {
        match value {
            IsolationFailure::Conflict | IsolationFailure::CapabilityChanged => {
                Error::invalid_source(value)
            }
            IsolationFailure::Fenced => Error::operational(crate::Diagnostic::caused(
                crate::ErrorCode::StaleOwner,
                value,
            )),
            IsolationFailure::Unknown => Error::operational(crate::Diagnostic::caused(
                crate::ErrorCode::AdapterUnknown,
                value,
            )),
        }
    }
}
/// Opt-in adapter extension; existing Participant, Backend and StepAdapter APIs
/// remain unchanged. Core cannot implement atomic validation over opaque work.
#[async_trait]
pub trait IsolationExecutor: Send + Sync {
    /// Existing durable resource/coordinator incarnation, stable across restarts.
    fn identity(&self) -> String;
    /// Advertise supported work without effects.
    fn accepts(&self, work: &StepWork) -> bool;
    /// Live reports, used before fresh dispatch. Must not acquire decision authority.
    fn capabilities(&self) -> Result<BTreeMap<ParticipantId, IsolationCapabilities>, Error>;
    /// Reconcile retained receipts or existing coordinator state BEFORE checking
    /// current validation, versions or authority. Existing decisions remain final.
    /// Some includes incomplete/uncertain protocol progress. None is permitted only
    /// when this key has never been admitted and no delayed effects are possible.
    /// Unknown lookup must return an error, never None. Receipt bindings must cover
    /// the full work AND contract, including frozen reports and conflict evidence.
    async fn reconcile(
        &self,
        key: &OperationId,
        work: &StepWork,
        contract: &StepIsolation,
        cancellation: &Cancellation,
    ) -> Result<Option<StepResult>, Error>;
    /// Atomically recheck exact frozen reports, authority, complete footprints and
    /// requested isolation with effects and the permanent receipt. There must be
    /// no validation-to-write race. For 2PC, validate during durable prepare and
    /// retain conflict protection until decision; use the existing coordinator,
    /// journal and recovery rules. Never validate again to reverse a commit.
    /// StagedPublish validation includes manifest/generation CAS; Snapshot applies
    /// only to its local transaction. Never expose staging as a shared snapshot.
    /// Dropped calls, timeouts and crashes must reconcile under the same key.
    async fn execute_validated(
        &self,
        key: OperationId,
        work: StepWork,
        contract: &StepIsolation,
        cancellation: &Cancellation,
    ) -> Result<StepResult, Error>;
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Recipe {
    version: u32,
    plan: TransactionPlan,
    contracts: BTreeMap<StepId, StepIsolation>,
}
/// Accepted optional isolation recipe over an existing DAG (including planner
/// output). Serde revalidates all contracts. Persist this alongside the plan
/// journal; rebuilding adapters from it preserves recovery without new records.
///
/// ```
/// use std::{collections::BTreeMap, sync::Arc};
/// use pactum_core::{Error, isolation::{IsolationPlan, IsolationExecutor, StepIsolation},
///     plan::{AdapterId, PlanEngine, PlanId, PlanPolicy, StepId}, planner::ExecutionPlan};
/// use pactum_journal::Journal;
///
/// fn open_isolated<J: Journal>(
///     journal: J, accepted: &ExecutionPlan,
///     contracts: BTreeMap<StepId, StepIsolation>,
///     executors: BTreeMap<AdapterId, Arc<dyn IsolationExecutor>>,
/// ) -> Result<(IsolationPlan, PlanEngine<J>), Error> {
///     let isolated = IsolationPlan::accept(accepted.transaction_plan().clone(), contracts)?;
///     // Persist `isolated` before execution, and retain it for restart.
///     let adapters = isolated.adapters(&executors)?;
///     let runner = PlanEngine::open(journal, PlanId::new("deployment-v1")?,
///         adapters, PlanPolicy::default())?;
///     Ok((isolated, runner))
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Recipe", into = "Recipe")]
pub struct IsolationPlan {
    recipe: Recipe,
    plan: TransactionPlan,
}
impl From<IsolationPlan> for Recipe {
    fn from(value: IsolationPlan) -> Self {
        value.recipe
    }
}
impl TryFrom<Recipe> for IsolationPlan {
    type Error = Error;
    fn try_from(value: Recipe) -> Result<Self, Error> {
        if value.version != 1 {
            return Err(invalid("unsupported isolation recipe version"));
        }
        Self::accept(value.plan, value.contracts)
    }
}
impl IsolationPlan {
    /// Negotiate per-step contracts over a validated DAG, with exact step coverage.
    /// Use `accepted.transaction_plan().clone()` from the public planner as input.
    /// Changed contracts require a new PlanId, as with changed payloads.
    pub fn accept(
        mut plan: TransactionPlan,
        contracts: BTreeMap<StepId, StepIsolation>,
    ) -> Result<Self, Error> {
        plan.validate()?;
        plan.steps.sort_by(|a, b| a.id.cmp(&b.id));
        for step in &mut plan.steps {
            if let StepWork::TwoPhase(enlistments) = &mut step.work {
                enlistments.sort_by(|a, b| a.participant.cmp(&b.participant));
            }
        }
        if contracts.keys().collect::<BTreeSet<_>>() != plan.steps.iter().map(|s| &s.id).collect() {
            return Err(invalid(
                "isolation contracts must cover exactly the plan steps",
            ));
        }
        for step in &plan.steps {
            contracts[&step.id].validate(&step.work)?;
        }
        let recipe = Recipe {
            version: 1,
            plan: plan.clone(),
            contracts,
        };
        let bytes = serde_json::to_vec(&recipe).map_err(Error::invalid_source)?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(invalid("isolation recipe exceeds 16 MiB"));
        }
        let fingerprint = crate::sha256_hex(bytes);
        for step in &mut plan.steps {
            step.adapter = bound_adapter(&fingerprint, &step.id)?;
        }
        plan.validate()?;
        Ok(Self { recipe, plan })
    }
    /// Existing executable DAG, with contract-bound adapter identities and receipt
    /// seeds. Register only the adapters returned by `adapters` for these IDs.
    pub fn transaction_plan(&self) -> &TransactionPlan {
        &self.plan
    }
    /// Immutable accepted contract snapshots, keyed by original step identity.
    pub fn contracts(&self) -> &BTreeMap<StepId, StepIsolation> {
        &self.recipe.contracts
    }
    /// SHA-256 of the canonical full recipe, including every frozen capability.
    pub fn fingerprint(&self) -> Result<String, Error> {
        Ok(crate::sha256_hex(
            serde_json::to_vec(&self.recipe).map_err(Error::invalid_source)?,
        ))
    }
    /// Bind opt-in executors registered by ORIGINAL adapter ID to accepted work.
    /// Feed the result to the existing PlanEngine::open. The journal retains the
    /// contract hash in both executable adapter IDs and durable resource bindings.
    /// Missing adapters fail before execution; live drift is checked only after
    /// receipt reconciliation, so it cannot reverse an existing durable decision.
    pub fn adapters(
        &self,
        executors: &BTreeMap<AdapterId, Arc<dyn IsolationExecutor>>,
    ) -> Result<BTreeMap<AdapterId, Arc<dyn StepAdapter>>, Error> {
        let mut result = BTreeMap::new();
        let fingerprint = self.fingerprint()?;
        for step in &self.recipe.plan.steps {
            let executor = executors
                .get(&step.adapter)
                .ok_or_else(|| invalid("missing isolation executor"))?
                .clone();
            if !executor.accepts(&step.work) {
                return Err(invalid("isolation executor rejects work"));
            }
            let identity = executor.identity();
            if identity.is_empty() || identity.len() > 65000 {
                return Err(invalid("invalid isolation executor identity"));
            }
            let bound = bound_adapter(&fingerprint, &step.id)?;
            result.insert(
                bound.clone(),
                Arc::new(BoundAdapter {
                    identity: format!("isolation-v1:{}:{}", identity, bound),
                    executor,
                    work: step.work.clone(),
                    contract: self.recipe.contracts[&step.id].clone(),
                }) as Arc<dyn StepAdapter>,
            );
        }
        Ok(result)
    }
}
fn bound_adapter(fingerprint: &str, step: &StepId) -> Result<AdapterId, Error> {
    AdapterId::new(format!(
        "isolation-v1-{}",
        crate::sha256_hex(serde_json::to_vec(&(fingerprint, step)).map_err(Error::invalid_source)?)
    ))
}
struct BoundAdapter {
    identity: String,
    executor: Arc<dyn IsolationExecutor>,
    work: StepWork,
    contract: StepIsolation,
}
#[async_trait]
impl StepAdapter for BoundAdapter {
    fn identity(&self) -> String {
        self.identity.clone()
    }
    fn accepts(&self, work: &StepWork) -> bool {
        work == &self.work && self.executor.accepts(work)
    }
    async fn execute(
        &self,
        key: OperationId,
        work: StepWork,
        cancellation: &Cancellation,
    ) -> Result<StepResult, Error> {
        if !self.accepts(&work) {
            return Err(invalid("isolation work binding changed"));
        }
        if let Some(result) = self
            .executor
            .reconcile(&key, &work, &self.contract, cancellation)
            .await?
        {
            return Ok(result);
        }
        if self.executor.capabilities()? != self.contract.capabilities() {
            return Err(IsolationFailure::CapabilityChanged.into());
        }
        self.executor
            .execute_validated(key, work, &self.contract, cancellation)
            .await
    }
}
