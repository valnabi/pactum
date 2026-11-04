//! Multi-mode execution, with backend receipts closing commit/checkpoint gaps.
//!
//! Version 2 journals are separate from v0.1 coordinator journals. Backend calls
//! are synchronous bounded local units; async applications should run the engine
//! on a blocking worker. See the mode guarantees and backend contracts below.
use crate::{Cancellation, Coordinator, Enlistment, Error, Status, TransactionId};
use async_trait::async_trait;
use pactum_journal::Journal;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

identifier!(
    OperationId,
    "Stable operation key, distinct from a 2PC transaction ID."
);

/// Execution protocol and its atomicity boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode {
    /// One native backend transaction.
    Atomic,
    /// Independently atomic chunks, without global rollback.
    Chunked,
    /// Hidden staging followed by a fenced visibility switch.
    StagedPublish,
    /// Durable coordinator decision across prepared participants.
    TwoPhase,
}
/// Explicit guarantees; backend contracts are prerequisites, not engine magic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Guarantees {
    /// Scope of atomicity.
    pub atomicity: &'static str,
    /// Reader isolation boundary.
    pub isolation: &'static str,
    /// Required persistence contract.
    pub durability: &'static str,
    /// Required duplicate suppression.
    pub idempotency: &'static str,
    /// Restart behavior.
    pub recovery: &'static str,
}
impl Mode {
    /// Guarantees required of compliant adapters.
    pub fn guarantees(self) -> Guarantees {
        Guarantees {
            atomicity: match self {
                Self::Atomic => "one backend transaction",
                Self::Chunked => "one chunk; operation may be partially applied",
                Self::StagedPublish => "one visibility CAS; staging is hidden",
                Self::TwoPhase => "durable decision across prepared participants",
            },
            isolation: match self {
                Self::TwoPhase => "participant reservations; no global snapshot",
                Self::Chunked => "backend isolation within each chunk",
                Self::Atomic => "backend transaction isolation",
                Self::StagedPublish => "read through the atomically switched pointer",
            },
            durability: "durable backend receipts and journal flush barriers",
            idempotency: "stable namespace/key permanently bound to immutable request",
            recovery: match self {
                Self::TwoPhase => "abort undecided; redeliver durable decision",
                _ => "replay receipt-protected work; retain terminal outcomes",
            },
        }
    }
}
/// One opaque backend unit of work.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtomicRequest {
    /// Adapter-defined immutable payload.
    pub payload: Vec<u8>,
}
/// Finite deterministic chunks; index is the stable chunk key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkedRequest {
    /// Ordered immutable units, already split to fit backend limits.
    pub chunks: Vec<Vec<u8>>,
}
/// Immutable staged artifact and fenced publish precondition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishRequest {
    /// Stable visible pointer name.
    pub target: OperationId,
    /// Monotonic pointer generation expected by CAS (zero means absent).
    pub expected_generation: u64,
    /// Adapter-defined staged artifact; version identity is the operation ID.
    pub payload: Vec<u8>,
}
/// Mode-tagged request; protocol data is never interpreted as 2PC prepare data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", content = "request", deny_unknown_fields)]
pub enum Request {
    /// One native transaction.
    Atomic(AtomicRequest),
    /// Checkpointed batch.
    Chunked(ChunkedRequest),
    /// Stage, validate and publish.
    StagedPublish(PublishRequest),
}
impl Request {
    /// Selected protocol.
    pub fn mode(&self) -> Mode {
        match self {
            Self::Atomic(_) => Mode::Atomic,
            Self::Chunked(_) => Mode::Chunked,
            Self::StagedPublish(_) => Mode::StagedPublish,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if matches!(self, Self::Chunked(r) if r.chunks.is_empty()) {
            return Err(Error::Invalid("at least one chunk required".into()));
        }
        if matches!(self, Self::StagedPublish(r) if r.expected_generation >= i64::MAX as u64) {
            return Err(Error::Invalid("publish generation exhausted".into()));
        }
        Ok(())
    }
}
/// Native transaction result, including a definite rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AtomicStatus {
    /// No receipt has yet been reconciled; commit may have happened.
    Indeterminate,
    /// Durable effects and receipt.
    Committed,
    /// Definite rollback or cancellation before dispatch.
    Aborted,
}
/// Observable batch outcome; partial application is never a global commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// No chunks have been dispatched.
    Pending,
    /// A strict prefix has completed and more work remains.
    PartiallyApplied,
    /// A chunk may have committed beyond the durable checkpoint.
    Indeterminate,
    /// Cancellation paused a retryable batch.
    Paused,
    /// A definite rejection stopped the batch; prior chunks remain applied.
    Rejected,
    /// Every chunk completed durably.
    Complete,
}
/// Chunk progress is distinct from 2PC InDoubt and global commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkStatus {
    /// Durable acknowledged prefix length.
    pub completed: usize,
    /// Immutable total chunks.
    pub total: usize,
    /// Next chunk may have committed beyond the acknowledged prefix.
    pub uncertain: bool,
    /// Cancellation pauses between chunks; execute explicitly resumes.
    pub paused: bool,
    /// Next chunk was definitely rejected; prior effects remain applied.
    pub rejected: bool,
}
impl ChunkStatus {
    /// Classify completion without hiding uncertainty behind partial progress.
    pub fn outcome(&self) -> ChunkOutcome {
        if self.uncertain {
            ChunkOutcome::Indeterminate
        } else if self.rejected {
            ChunkOutcome::Rejected
        } else if self.is_complete() {
            ChunkOutcome::Complete
        } else if self.paused {
            ChunkOutcome::Paused
        } else if self.completed > 0 {
            ChunkOutcome::PartiallyApplied
        } else {
            ChunkOutcome::Pending
        }
    }
    /// Whether all chunks have acknowledged durable receipts.
    pub fn is_complete(&self) -> bool {
        self.completed == self.total && !self.uncertain
    }
}
/// Staged publication phase. Publishing is an indeterminate CAS outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishStatus {
    /// Stage write may need receipt-protected replay.
    Staging,
    /// Hidden artifact exists; validation is pending.
    Staged,
    /// Validation succeeded; CAS not dispatched yet.
    Validated,
    /// CAS may have succeeded; reconcile its permanent receipt.
    Publishing,
    /// Published successfully, even if a newer version supersedes it later.
    Published,
    /// CAS precondition failed; hidden orphan cleaned up.
    Conflict,
    /// Cleanup intent is durable; replay cleanup before reporting abort.
    Aborting,
    /// Validation failed or cancellation cleaned up unpublished staging.
    Aborted,
}
/// Mode-specific durable outcome; no conversion to a generic success boolean.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Progress {
    /// Native atomic completion.
    Atomic(AtomicStatus),
    /// Partial application, pause or completion.
    Chunked(ChunkStatus),
    /// Staging, uncertainty or terminal publication.
    StagedPublish(PublishStatus),
}
impl Progress {
    fn initial(request: &Request) -> Self {
        match request {
            Request::Atomic(_) => Self::Atomic(AtomicStatus::Indeterminate),
            Request::Chunked(r) => Self::Chunked(ChunkStatus {
                completed: 0,
                total: r.chunks.len(),
                uncertain: false,
                paused: false,
                rejected: false,
            }),
            Request::StagedPublish(_) => Self::StagedPublish(PublishStatus::Staging),
        }
    }
    /// Whether protocol work is terminal (a paused batch is resumable).
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Atomic(s) => *s != AtomicStatus::Indeterminate,
            Self::Chunked(s) => s.is_complete() || s.rejected,
            Self::StagedPublish(s) => matches!(
                s,
                PublishStatus::Published | PublishStatus::Aborted | PublishStatus::Conflict
            ),
        }
    }
}
/// Durable operation snapshot and process-local diagnostic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationStatus {
    /// Stable key.
    pub operation: OperationId,
    /// Mode-specific progress.
    pub progress: Progress,
    /// Latest local backend failure; not a durable audit log.
    pub last_error: Option<String>,
}
/// Backend call failed. All errors conservatively permit an uncertain outcome;
/// recovery retries using durable receipts. A definite rejection is a typed result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendError(pub String);
impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for BackendError {}
/// Native transaction has committed or definitely rolled back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnitResult {
    /// Effects and receipt are durably atomic.
    Committed,
    /// No effects; rejection receipt is durable.
    RolledBack,
}
/// Publish CAS result, persisted forever alongside the pointer switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishResult {
    /// CAS succeeded and permanent publication receipt exists.
    Published,
    /// Generation precondition failed; retry must return the same rejection.
    Conflict,
}
/// Local backend protocols. Implementations must bind every operation key to
/// the full Request (including mode, chunks and publish precondition), reject
/// mismatches, and atomically persist effects with permanent result receipts.
/// Calls can be repeated after any error or process death. No external effects
/// may escape the native transaction. Stage/validate/cleanup must be idempotent;
/// cleanup must refuse published artifacts. Receipts survive superseding writes.
/// One owner must exclusively coordinate this backend and journal pair.
pub trait Backend: Send {
    /// Persisted database incarnation plus namespace, never just its pathname.
    fn identity(&self) -> String;
    /// Commit or explicitly roll back one native unit. `chunk` is None for
    /// Atomic, otherwise the deterministic zero-based chunk index.
    fn unit(
        &mut self,
        id: &OperationId,
        request: &Request,
        chunk: Option<usize>,
    ) -> Result<UnitResult, BackendError>;
    /// Durably write an immutable hidden version, keyed by operation ID.
    fn stage(&mut self, id: &OperationId, request: &PublishRequest) -> Result<(), BackendError>;
    /// Validate all staged data; false rejects publication.
    fn validate(
        &mut self,
        id: &OperationId,
        request: &PublishRequest,
    ) -> Result<bool, BackendError>;
    /// Atomically switch visibility only at the requested generation, increment
    /// it, and persist the Published/Conflict receipt in the same transaction.
    fn publish(
        &mut self,
        id: &OperationId,
        request: &PublishRequest,
    ) -> Result<PublishResult, BackendError>;
    /// Remove only unpublished staging, retaining identity and rejection receipts.
    fn cleanup(&mut self, id: &OperationId, request: &PublishRequest) -> Result<(), BackendError>;
}
/// Per-pass limits, not part of immutable operation identity.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionPolicy {
    /// Maximum chunks dispatched in one pass (positive).
    pub max_chunks: usize,
    /// Maximum encoded bytes per native unit / staged artifact (positive).
    pub max_unit_bytes: usize,
}
impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            max_chunks: usize::MAX,
            max_unit_bytes: 1024 * 1024,
        }
    }
}
/// Common lifecycle with associated protocol request, key and status types.
/// 2PC retains its original transaction/status model through this adapter.
#[async_trait]
pub trait Execution {
    /// Stable key type.
    type Id: Sync + Send;
    /// Immutable request type.
    type Request: Send;
    /// Explicit protocol status.
    type Status: Send;
    /// Start or reconcile the same immutable work.
    async fn execute(
        &mut self,
        id: Self::Id,
        request: Self::Request,
        cancellation: &Cancellation,
    ) -> Result<Self::Status, Error>;
    /// Read acknowledged durable state; reopen after journal uncertainty.
    fn status(&self, id: &Self::Id) -> Result<Option<Self::Status>, Error>;
    /// Cancel at protocol boundaries. Never reverse a durable decision or CAS.
    async fn cancel(&mut self, id: &Self::Id) -> Result<Option<Self::Status>, Error>;
    /// Reconcile persisted work without inventing new requests.
    async fn recover(&mut self, cancellation: &Cancellation) -> Result<Vec<Self::Status>, Error>;
}
/// Immutable 2PC request for the common lifecycle API.
#[derive(Clone, Debug)]
pub struct TwoPhaseRequest {
    /// Existing v0.1 enlistments; participant adapters need no changes.
    pub enlistments: Vec<Enlistment>,
}
#[async_trait]
impl<J: Journal> Execution for Coordinator<J> {
    type Id = TransactionId;
    type Request = TwoPhaseRequest;
    type Status = Status;
    async fn execute(
        &mut self,
        id: TransactionId,
        request: TwoPhaseRequest,
        cancellation: &Cancellation,
    ) -> Result<Status, Error> {
        Coordinator::execute(self, id, request.enlistments, cancellation).await
    }
    fn status(&self, id: &TransactionId) -> Result<Option<Status>, Error> {
        Coordinator::status(self, id)
    }
    async fn cancel(&mut self, id: &TransactionId) -> Result<Option<Status>, Error> {
        self.healthy()?;
        if !self.states.contains_key(id) {
            return Ok(None);
        }
        Ok(Some(self.reconcile(id, &Cancellation::default()).await?))
    }
    async fn recover(&mut self, cancellation: &Cancellation) -> Result<Vec<Status>, Error> {
        Coordinator::recover(self, cancellation).await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", deny_unknown_fields)]
enum Record {
    Header { backend: String },
    Begin { id: OperationId, request: Request },
    Progress { id: OperationId, progress: Progress },
}
#[derive(Clone)]
struct State {
    request: Request,
    progress: Progress,
    last_error: Option<String>,
}
fn transition(old: &Progress, next: &Progress) -> bool {
    match (old, next) {
        (
            Progress::Atomic(AtomicStatus::Indeterminate),
            Progress::Atomic(AtomicStatus::Committed | AtomicStatus::Aborted),
        ) => true,
        (Progress::Chunked(a), Progress::Chunked(b)) => {
            if b.rejected {
                return !a.rejected
                    && a.uncertain
                    && a.total == b.total
                    && b.completed == a.completed
                    && !b.uncertain
                    && !b.paused;
            }
            a.total == b.total
                && b.completed <= b.total
                && !a.is_complete()
                && !a.rejected
                && ((!a.uncertain
                    && !a.paused
                    && b.completed == a.completed
                    && b.uncertain
                    && !b.paused)
                    || (a.uncertain && b.completed == a.completed + 1 && !b.uncertain && !b.paused)
                    || (b.completed == a.completed
                        && b.uncertain == a.uncertain
                        && b.paused != a.paused))
        }
        (Progress::StagedPublish(a), Progress::StagedPublish(b)) => matches!(
            (a, b),
            (
                PublishStatus::Staging,
                PublishStatus::Staged | PublishStatus::Aborting
            ) | (
                PublishStatus::Staged,
                PublishStatus::Validated | PublishStatus::Aborting
            ) | (
                PublishStatus::Validated,
                PublishStatus::Publishing | PublishStatus::Aborting
            ) | (
                PublishStatus::Publishing,
                PublishStatus::Published | PublishStatus::Conflict
            ) | (PublishStatus::Aborting, PublishStatus::Aborted)
        ),
        _ => false,
    }
}
fn apply(states: &mut BTreeMap<OperationId, State>, record: &Record) -> Result<(), Error> {
    let corrupt = || Error::Corrupt("illegal multi-mode transition".into());
    match record {
        Record::Begin { id, request } => {
            request
                .validate()
                .map_err(|e| Error::Corrupt(e.to_string()))?;
            if states.contains_key(id) {
                return Err(corrupt());
            }
            states.insert(
                id.clone(),
                State {
                    request: request.clone(),
                    progress: Progress::initial(request),
                    last_error: None,
                },
            );
        }
        Record::Progress { id, progress } => {
            let state = states.get_mut(id).ok_or_else(corrupt)?;
            if !transition(&state.progress, progress) {
                return Err(corrupt());
            }
            state.progress = progress.clone();
            state.last_error = None;
        }
        Record::Header { .. } => return Err(corrupt()),
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    backend: String,
    states: Vec<(OperationId, Request, Progress)>,
}
// Validate reachability from the initial state, without replaying every chunk.
fn validate_snapshot_progress(request: &Request, progress: &Progress) -> Result<(), Error> {
    request
        .validate()
        .map_err(|e| Error::Corrupt(e.to_string()))?;
    let valid = match (request, progress) {
        (Request::Atomic(_), Progress::Atomic(_)) => true,
        (Request::StagedPublish(_), Progress::StagedPublish(_)) => true,
        (Request::Chunked(r), Progress::Chunked(s)) => {
            s.total == r.chunks.len()
                && s.completed <= s.total
                && !(s.rejected && (s.uncertain || s.paused))
                && (s.completed < s.total || (!s.rejected && !s.uncertain && !s.paused))
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Corrupt("unreachable checkpoint progress".into()))
    }
}
/// Single-owner engine for Atomic, Chunked and StagedPublish. Uses a dedicated
/// version-2 journal, leaving the v0.1 Coordinator and its journal unchanged.
/// Backend identity is bound in the first flushed record before any work.
pub struct Engine<J: Journal, B: Backend> {
    journal: J,
    backend: B,
    states: BTreeMap<OperationId, State>,
    pending: BTreeSet<OperationId>,
    policy: ExecutionPolicy,
    poisoned: bool,
    sequence: u64,
}
impl<J: Journal, B: Backend> Engine<J, B> {
    /// Open and strictly replay. Namespace/incarnation mismatch fails closed.
    pub fn open(mut journal: J, backend: B, policy: ExecutionPolicy) -> Result<Self, Error> {
        if policy.max_chunks == 0 || policy.max_unit_bytes == 0 {
            return Err(Error::Invalid("positive execution limits required".into()));
        }
        let mut states = BTreeMap::new();
        let mut sequence = 0;
        if let Some(checkpoint) = journal
            .checkpoint()
            .map_err(|e| Error::Corrupt(e.to_string()))?
        {
            if checkpoint.version != 2 || checkpoint.watermark == 0 {
                return Err(Error::Corrupt("unsupported multi-mode checkpoint".into()));
            }
            let snapshot: Snapshot = serde_json::from_slice(&checkpoint.payload)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
            if snapshot.backend != backend.identity()
                || snapshot.states.len() as u64 >= checkpoint.watermark
            {
                return Err(Error::Corrupt(
                    "checkpoint namespace or watermark mismatch".into(),
                ));
            }
            for (id, request, progress) in snapshot.states {
                validate_snapshot_progress(&request, &progress)?;
                if states
                    .insert(
                        id,
                        State {
                            request,
                            progress,
                            last_error: None,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Corrupt("duplicate checkpoint key".into()));
                }
            }
            sequence = checkpoint.watermark;
        }
        journal.visit(&mut |entry| {
            let result = (|| {
                if Some(entry.sequence) != sequence.checked_add(1) || entry.version != 2 {
                    return Err(Error::Corrupt("sequence gap or unsupported multi-mode schema".into()));
                }
                let record: Record = serde_json::from_slice(&entry.payload).map_err(|e| Error::Corrupt(e.to_string()))?;
                if sequence == 0 {
                    if !matches!(record, Record::Header { backend: ref identity } if *identity == backend.identity()) {
                        return Err(Error::Corrupt("backend namespace/incarnation mismatch".into()));
                    }
                } else { apply(&mut states, &record)?; }
                sequence = entry.sequence;
                Ok(())
            })();
            result.map_err(|e: Error| pactum_journal::JournalError(e.to_string()))
        }).map_err(|e| Error::Corrupt(e.to_string()))?;
        let mut engine = Self {
            journal,
            backend,
            pending: states
                .iter()
                .filter(|(_, s)| !s.progress.is_terminal())
                .map(|(id, _)| id.clone())
                .collect(),
            states,
            policy,
            poisoned: false,
            sequence,
        };
        if sequence == 0 {
            engine.write(&Record::Header {
                backend: engine.backend.identity(),
            })?;
        }
        Ok(engine)
    }
    /// Atomically checkpoint every mode, pending phase and permanent request
    /// binding, then compact or archive covered events. Backend receipts and
    /// publication tombstones must also be retained by the backend forever.
    pub fn compact(&mut self, retention: pactum_journal::Retention) -> Result<(), Error> {
        self.healthy()?;
        let snapshot = Snapshot {
            backend: self.backend.identity(),
            states: self
                .states
                .iter()
                .map(|(id, s)| (id.clone(), s.request.clone(), s.progress.clone()))
                .collect(),
        };
        let payload = serde_json::to_vec(&snapshot).map_err(|e| Error::Invalid(e.to_string()))?;
        self.poisoned = true;
        self.journal
            .compact(self.sequence, 2, &payload, retention)
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        self.poisoned = false;
        Ok(())
    }
    /// Recover indexed nonterminal operations in ID order. Paused chunks remain
    /// paused. Terminal request bindings are retained without enumerating them.
    pub fn recover_pending(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Vec<OperationStatus>, Error> {
        self.healthy()?;
        let ids: Vec<_> = self.pending.iter().cloned().collect();
        ids.iter()
            .map(|id| self.advance(id, cancellation))
            .collect()
    }
    /// Read backend diagnostics and values without bypassing engine writes.
    pub fn backend(&self) -> &B {
        &self.backend
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::InDoubt(
                "journal uncertainty; drop and reopen engine/backend".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn write(&mut self, record: &Record) -> Result<(), Error> {
        self.healthy()?;
        let payload = serde_json::to_vec(record).map_err(|e| Error::Invalid(e.to_string()))?;
        self.poisoned = true;
        let sequence = self
            .journal
            .append(2, &payload)
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        let durable = self
            .journal
            .flush()
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        if sequence != self.sequence + 1 || durable != sequence {
            return Err(Error::InDoubt("invalid durability barrier".into()));
        }
        self.sequence = sequence;
        self.poisoned = false;
        Ok(())
    }
    fn persist(&mut self, record: Record) -> Result<(), Error> {
        let id = match &record {
            Record::Begin { id, .. } | Record::Progress { id, .. } => id,
            Record::Header { .. } => unreachable!(),
        }
        .clone();
        let mut next = BTreeMap::new();
        if let Some(state) = self.states.get(&id) {
            next.insert(id.clone(), state.clone());
        }
        apply(&mut next, &record)?;
        self.write(&record)?;
        let state = next
            .remove(&id)
            .expect("validated transition retains state");
        if state.progress.is_terminal() {
            self.pending.remove(&id);
        } else {
            self.pending.insert(id.clone());
        }
        self.states.insert(id, state);
        Ok(())
    }
    fn progress(&mut self, id: &OperationId, progress: Progress) -> Result<(), Error> {
        self.persist(Record::Progress {
            id: id.clone(),
            progress,
        })
    }
    fn snapshot(&self, id: &OperationId) -> OperationStatus {
        let s = &self.states[id];
        OperationStatus {
            operation: id.clone(),
            progress: s.progress.clone(),
            last_error: s.last_error.clone(),
        }
    }
    fn failure(&mut self, id: &OperationId, error: BackendError) -> OperationStatus {
        self.states.get_mut(id).unwrap().last_error = Some(error.to_string());
        self.snapshot(id)
    }
    fn advance(
        &mut self,
        id: &OperationId,
        cancellation: &Cancellation,
    ) -> Result<OperationStatus, Error> {
        self.healthy()?;
        let request = self.states[id].request.clone();
        let mut dispatched = 0;
        loop {
            let progress = self.states[id].progress.clone();
            if progress.is_terminal() || cancellation.is_cancelled() {
                return Ok(self.snapshot(id));
            }
            match (&request, progress) {
                (Request::Atomic(_), Progress::Atomic(_)) => {
                    let result = match self.backend.unit(id, &request, None) {
                        Ok(r) => r,
                        Err(e) => return Ok(self.failure(id, e)),
                    };
                    self.progress(
                        id,
                        Progress::Atomic(match result {
                            UnitResult::Committed => AtomicStatus::Committed,
                            UnitResult::RolledBack => AtomicStatus::Aborted,
                        }),
                    )?;
                }
                (Request::Chunked(_), Progress::Chunked(mut s)) => {
                    if s.paused || dispatched == self.policy.max_chunks {
                        return Ok(self.snapshot(id));
                    }
                    if !s.uncertain {
                        s.uncertain = true;
                        self.progress(id, Progress::Chunked(s.clone()))?;
                    }
                    match self.backend.unit(id, &request, Some(s.completed)) {
                        Ok(UnitResult::Committed) => {
                            s.completed += 1;
                            s.uncertain = false;
                            dispatched += 1;
                            self.progress(id, Progress::Chunked(s))?;
                        }
                        Ok(UnitResult::RolledBack) => {
                            s.uncertain = false;
                            s.rejected = true;
                            self.progress(id, Progress::Chunked(s))?;
                            return Ok(self.failure(
                                id,
                                BackendError(
                                    "chunk rejected; immutable operation remains partial".into(),
                                ),
                            ));
                        }
                        Err(e) => return Ok(self.failure(id, e)),
                    }
                }
                (Request::StagedPublish(r), Progress::StagedPublish(s)) => {
                    let next = match s {
                        PublishStatus::Staging => {
                            self.backend.stage(id, r).map(|()| PublishStatus::Staged)
                        }
                        PublishStatus::Staged => match self.backend.validate(id, r) {
                            Ok(true) => Ok(PublishStatus::Validated),
                            Ok(false) => Ok(PublishStatus::Aborting),
                            Err(e) => Err(e),
                        },
                        PublishStatus::Aborting => {
                            self.backend.cleanup(id, r).map(|()| PublishStatus::Aborted)
                        }
                        PublishStatus::Validated => Ok(PublishStatus::Publishing),
                        PublishStatus::Publishing => match self.backend.publish(id, r) {
                            Ok(PublishResult::Published) => Ok(PublishStatus::Published),
                            Ok(PublishResult::Conflict) => self
                                .backend
                                .cleanup(id, r)
                                .map(|()| PublishStatus::Conflict),
                            Err(e) => Err(e),
                        },
                        _ => unreachable!(),
                    };
                    match next {
                        Ok(s) => self.progress(id, Progress::StagedPublish(s))?,
                        Err(e) => return Ok(self.failure(id, e)),
                    }
                }
                _ => unreachable!("replay checks mode transitions"),
            }
        }
    }
}
#[async_trait]
impl<J: Journal, B: Backend> Execution for Engine<J, B> {
    type Id = OperationId;
    type Request = Request;
    type Status = OperationStatus;
    async fn execute(
        &mut self,
        id: OperationId,
        request: Request,
        cancellation: &Cancellation,
    ) -> Result<OperationStatus, Error> {
        self.healthy()?;
        request.validate()?;
        let oversized = match &request {
            Request::Atomic(r) => r.payload.len() > self.policy.max_unit_bytes,
            Request::Chunked(r) => r
                .chunks
                .iter()
                .any(|c| c.len() > self.policy.max_unit_bytes),
            Request::StagedPublish(r) => r.payload.len() > self.policy.max_unit_bytes,
        };
        if oversized {
            return Err(Error::Invalid(
                "unit exceeds configured byte limit; split into smaller chunks".into(),
            ));
        }
        if let Some(state) = self.states.get(&id) {
            if state.request != request {
                return Err(Error::Invalid(
                    "operation key reused for different payload/mode/precondition".into(),
                ));
            }
            if let Progress::Chunked(mut s) = state.progress.clone()
                && s.paused
            {
                s.paused = false;
                self.progress(&id, Progress::Chunked(s))?;
            }
        } else {
            self.persist(Record::Begin {
                id: id.clone(),
                request,
            })?;
            if cancellation.is_cancelled() && matches!(self.states[&id].request, Request::Atomic(_))
            {
                self.progress(&id, Progress::Atomic(AtomicStatus::Aborted))?;
            }
        }
        self.advance(&id, cancellation)
    }
    fn status(&self, id: &OperationId) -> Result<Option<OperationStatus>, Error> {
        self.healthy()?;
        Ok(self.states.contains_key(id).then(|| self.snapshot(id)))
    }
    async fn cancel(&mut self, id: &OperationId) -> Result<Option<OperationStatus>, Error> {
        self.healthy()?;
        let Some(state) = self.states.get(id).cloned() else {
            return Ok(None);
        };
        match (&state.request, state.progress) {
            (_, p) if p.is_terminal() => {}
            (Request::Chunked(_), Progress::Chunked(mut s)) if !s.paused => {
                s.paused = true;
                self.progress(id, Progress::Chunked(s))?;
            }
            (
                Request::StagedPublish(_),
                Progress::StagedPublish(
                    PublishStatus::Staging | PublishStatus::Staged | PublishStatus::Validated,
                ),
            ) => {
                self.progress(id, Progress::StagedPublish(PublishStatus::Aborting))?;
                return self.advance(id, &Cancellation::default()).map(Some);
            }
            (Request::StagedPublish(_), Progress::StagedPublish(PublishStatus::Aborting)) => {
                return self.advance(id, &Cancellation::default()).map(Some);
            }
            _ => {} // Atomic/CAS already dispatched may have committed; never undo.
        }
        Ok(Some(self.snapshot(id)))
    }
    async fn recover(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Vec<OperationStatus>, Error> {
        self.recover_pending(cancellation)?;
        Ok(self.states.keys().map(|id| self.snapshot(id)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pactum_journal::{Entry, JournalError, Sequence};
    struct History(Vec<Entry>);
    impl Journal for History {
        fn append(&mut self, _: u32, _: &[u8]) -> Result<Sequence, JournalError> {
            unreachable!()
        }
        fn flush(&mut self) -> Result<Sequence, JournalError> {
            unreachable!()
        }
        fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
            Ok(self.0.clone())
        }
    }
    struct Offline;
    impl Backend for Offline {
        fn identity(&self) -> String {
            "incarnation".into()
        }
        fn unit(
            &mut self,
            _: &OperationId,
            _: &Request,
            _: Option<usize>,
        ) -> Result<UnitResult, BackendError> {
            unreachable!()
        }
        fn stage(&mut self, _: &OperationId, _: &PublishRequest) -> Result<(), BackendError> {
            unreachable!()
        }
        fn validate(&mut self, _: &OperationId, _: &PublishRequest) -> Result<bool, BackendError> {
            unreachable!()
        }
        fn publish(
            &mut self,
            _: &OperationId,
            _: &PublishRequest,
        ) -> Result<PublishResult, BackendError> {
            unreachable!()
        }
        fn cleanup(&mut self, _: &OperationId, _: &PublishRequest) -> Result<(), BackendError> {
            unreachable!()
        }
    }
    fn id() -> OperationId {
        OperationId::new("test").unwrap()
    }
    fn begin(request: Request) -> Record {
        Record::Begin { id: id(), request }
    }
    fn progress(progress: Progress) -> Record {
        Record::Progress { id: id(), progress }
    }
    fn replay(records: Vec<Record>) -> Result<Engine<History, Offline>, Error> {
        let records = std::iter::once(Record::Header {
            backend: "incarnation".into(),
        })
        .chain(records);
        let history = History(
            records
                .enumerate()
                .map(|(i, r)| Entry {
                    sequence: i as u64 + 1,
                    version: 2,
                    payload: serde_json::to_vec(&r).unwrap(),
                })
                .collect(),
        );
        Engine::open(history, Offline, ExecutionPolicy::default())
    }
    fn atomic() -> Request {
        Request::Atomic(AtomicRequest { payload: vec![1] })
    }
    fn publish() -> Request {
        Request::StagedPublish(PublishRequest {
            target: id(),
            expected_generation: 0,
            payload: vec![1],
        })
    }
    fn chunk(n: usize, uncertain: bool, paused: bool, rejected: bool) -> Record {
        progress(Progress::Chunked(ChunkStatus {
            completed: n,
            total: 2,
            uncertain,
            paused,
            rejected,
        }))
    }
    #[test]
    fn replay_validates_each_mode_without_dispatch_and_rejects_impossible_histories() {
        let histories = vec![
            vec![
                begin(atomic()),
                progress(Progress::Atomic(AtomicStatus::Committed)),
            ],
            vec![
                begin(atomic()),
                progress(Progress::Atomic(AtomicStatus::Aborted)),
            ],
            vec![
                begin(Request::Chunked(ChunkedRequest {
                    chunks: vec![vec![1], vec![2]],
                })),
                chunk(0, true, false, false),
                chunk(1, false, false, false),
                chunk(1, false, true, false),
                chunk(1, false, false, false),
                chunk(1, true, false, false),
                chunk(2, false, false, false),
            ],
            vec![
                begin(Request::Chunked(ChunkedRequest {
                    chunks: vec![vec![1], vec![2]],
                })),
                chunk(0, true, false, false),
                chunk(0, false, false, true),
            ],
            vec![
                begin(publish()),
                progress(Progress::StagedPublish(PublishStatus::Staged)),
                progress(Progress::StagedPublish(PublishStatus::Validated)),
                progress(Progress::StagedPublish(PublishStatus::Publishing)),
                progress(Progress::StagedPublish(PublishStatus::Published)),
            ],
            vec![
                begin(publish()),
                progress(Progress::StagedPublish(PublishStatus::Aborting)),
                progress(Progress::StagedPublish(PublishStatus::Aborted)),
            ],
        ];
        for history in histories {
            for len in 0..=history.len() {
                assert!(replay(history[..len].to_vec()).is_ok());
            }
        }
        for history in [
            vec![begin(atomic()), begin(atomic())],
            vec![
                begin(atomic()),
                progress(Progress::StagedPublish(PublishStatus::Published)),
            ],
            vec![
                begin(atomic()),
                progress(Progress::Atomic(AtomicStatus::Committed)),
                progress(Progress::Atomic(AtomicStatus::Aborted)),
            ],
            vec![
                begin(publish()),
                progress(Progress::StagedPublish(PublishStatus::Published)),
            ],
            vec![
                begin(publish()),
                progress(Progress::StagedPublish(PublishStatus::Publishing)),
            ],
            vec![begin(Request::Chunked(ChunkedRequest { chunks: vec![] }))],
            vec![
                begin(Request::Chunked(ChunkedRequest {
                    chunks: vec![vec![1], vec![2]],
                })),
                chunk(1, false, false, false),
            ],
            vec![
                begin(Request::Chunked(ChunkedRequest {
                    chunks: vec![vec![1], vec![2]],
                })),
                chunk(0, false, false, true),
            ],
            vec![
                begin(atomic()),
                Record::Header {
                    backend: "incarnation".into(),
                },
            ],
            vec![progress(Progress::Atomic(AtomicStatus::Committed))],
        ] {
            assert!(matches!(replay(history), Err(Error::Corrupt(_))));
        }
    }
    #[test]
    fn status_models_and_guarantees_keep_success_partial_and_uncertainty_distinct() {
        assert_ne!(
            Mode::Atomic.guarantees().atomicity,
            Mode::Chunked.guarantees().atomicity
        );
        assert!(!Progress::Atomic(AtomicStatus::Indeterminate).is_terminal());
        assert!(!Progress::StagedPublish(PublishStatus::Publishing).is_terminal());
        assert!(Progress::StagedPublish(PublishStatus::Published).is_terminal());
        assert!(
            !Progress::Chunked(ChunkStatus {
                completed: 1,
                total: 2,
                uncertain: false,
                paused: true,
                rejected: false
            })
            .is_terminal()
        );
    }
}
