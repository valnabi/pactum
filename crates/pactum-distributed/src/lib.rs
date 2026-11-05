//! Opt-in distributed 2PC. Adapters must enforce fencing atomically with effects.
//!
//! See ADR 0004. The SQLite reference is a single-host authority service, not
//! distributed consensus. Local Pactum APIs and other execution modes are unchanged.
use async_trait::async_trait;
use pactum_core::{
    Cancellation, Coordinator, Diagnostics, Enlistment, Error, Participant, ParticipantError,
    ParticipantId, ParticipantStatus, RetryPolicy, Status, TransactionId, Vote,
};
use pactum_journal::{Checkpoint, Entry, Journal, JournalError, Retention, Sequence};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

/// Durable single-host reference authority and fenced resource implementation.
pub mod reference;
/// Versioned transport-independent requests and fault-injectable loopback adapter.
pub mod transport;

/// Authority incarnation plus journal namespace; never reuse after storage loss.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Domain {
    /// Stable authority incarnation persisted with its database.
    pub authority: String,
    /// Exclusive ownership scope for the entire journal.
    pub namespace: String,
}
/// Exact grant; possession does not replace server-side validation/authentication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    /// Bound authority and journal identity.
    pub domain: Domain,
    /// Diagnostic owner identity.
    pub owner: String,
    /// Strictly increasing, permanently retained fencing epoch.
    pub epoch: u64,
}
/// Linearizable durable epoch authority. No consensus implementation is implied.
///
/// Acquire excludes an unexpired owner; every successful acquire advances epoch.
/// Renew/release require an exact current unexpired grant. Authority time alone
/// controls expiration. Uncertainty must return an error, never assume ownership.
pub trait LeaseAuthority: Send + Sync {
    /// Stable authority domain.
    fn domain(&self) -> Domain;
    /// Acquire a fresh epoch, failing when a live grant exists.
    fn acquire(&self, owner: &str, ttl: Duration) -> Result<Grant, Error>;
    /// Extend the current live grant; never resurrect an expired owner.
    fn renew(&self, grant: &Grant, ttl: Duration) -> Result<(), Error>;
    /// Invalidate a live grant without resetting the epoch high-water mark.
    fn release(&self, grant: &Grant) -> Result<(), Error>;
}
/// Factory for shared, durable, enforced journal sessions.
///
/// All journal mutations (including flush and compaction) must validate the exact
/// current live grant atomically with their durable effects. Replay must see all
/// preceding durable writes. Local preflight checks do not meet this contract.
/// Grant transfer must serialize with every mutation, including participant calls.
pub trait JournalAuthority: Send + Sync {
    /// Durable domain shared with the lease authority and all resources.
    fn domain(&self) -> Domain;
    /// Open a fresh session, rejecting stale grants. Never migrate staged writes.
    fn open_journal(&self, grant: &Grant) -> Result<OwnedJournal, Error>;
}
/// Boxed fenced journal session returned by a conforming authority.
pub struct OwnedJournal(Box<dyn Journal>);
impl OwnedJournal {
    /// Construct from a journal satisfying JournalAuthority's fencing contract.
    pub fn new(journal: impl Journal + 'static) -> Self {
        Self(Box::new(journal))
    }
}
impl Journal for OwnedJournal {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        self.0.append(version, payload)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.0.flush()
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.0.replay()
    }
    fn visit(
        &mut self,
        visitor: &mut dyn FnMut(Entry) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.0.visit(visitor)
    }
    fn checkpoint(&mut self) -> Result<Option<Checkpoint>, JournalError> {
        self.0.checkpoint()
    }
    fn compact(
        &mut self,
        watermark: Sequence,
        version: u32,
        payload: &[u8],
        retention: Retention,
    ) -> Result<(), JournalError> {
        self.0.compact(watermark, version, payload, retention)
    }
}
/// Durable participant operation. Transaction identity remains stable across epochs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Operation {
    /// Bind and durably reserve immutable work.
    Prepare(Vec<u8>),
    /// Apply the prepared effects exactly once.
    Commit(Vec<u8>),
    /// Persist an abort tombstone, even if prepare has not arrived.
    Abort(Option<Vec<u8>>),
    /// Inspect durable state.
    Status,
}
impl Operation {
    /// Safe phase label, independent of payload/token contents.
    pub fn phase(&self) -> &'static str {
        match self {
            Self::Prepare(_) => "prepare",
            Self::Commit(_) => "commit",
            Self::Abort(_) => "abort",
            Self::Status => "status",
        }
    }
}
/// Version-independent typed resource response; invalid operation/response pairs fail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Reply {
    /// Durable prepared token (also used for status).
    Prepared(Vec<u8>),
    /// Definite prepare refusal, never used for transport uncertainty.
    Rejected(String),
    /// Durable finalization acknowledgement.
    Acknowledged,
    /// Unknown durable transaction.
    Unknown,
    /// Durable committed state.
    Committed,
    /// Durable abort tombstone.
    Aborted,
}
/// Resource-side enforcement contract, deliberately separate from local Participant.
///
/// The Participant durability/idempotency contract applies. In addition, grant
/// validation and every operation must serialize atomically with ownership
/// transfer. Checking a lease before awaiting an unfenced local resource is unsafe.
/// Resources must share the journal's authority domain and preserve all retry keys.
#[async_trait]
pub trait FencedResource: Send + Sync {
    /// Stable durable resource identity.
    fn id(&self) -> ParticipantId;
    /// Shared authority and journal scope.
    fn domain(&self) -> Domain;
    /// Execute under an exact live grant, or return uncertain/error without voting no.
    async fn call(
        &self,
        grant: &Grant,
        tx: &TransactionId,
        operation: Operation,
    ) -> Result<Reply, ParticipantError>;
}
struct BoundResource {
    resource: Arc<dyn FencedResource>,
    grant: Grant,
}
fn invalid_reply() -> ParticipantError {
    ParticipantError::from_diagnostic(pactum_errors::Diagnostic::message(
        pactum_errors::ErrorCode::ProtocolMismatch,
        "invalid resource response".into(),
    ))
}
#[async_trait]
impl Participant for BoundResource {
    fn id(&self) -> ParticipantId {
        self.resource.id()
    }
    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError> {
        match self
            .resource
            .call(&self.grant, tx, Operation::Prepare(payload.to_vec()))
            .await?
        {
            Reply::Prepared(token) => Ok(Vote::Prepared(token)),
            Reply::Rejected(reason) => Ok(Vote::Rejected(reason)),
            _ => Err(invalid_reply()),
        }
    }
    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError> {
        match self
            .resource
            .call(&self.grant, tx, Operation::Commit(token.to_vec()))
            .await?
        {
            Reply::Acknowledged => Ok(()),
            _ => Err(invalid_reply()),
        }
    }
    async fn abort(
        &self,
        tx: &TransactionId,
        token: Option<&[u8]>,
    ) -> Result<(), ParticipantError> {
        match self
            .resource
            .call(&self.grant, tx, Operation::Abort(token.map(<[u8]>::to_vec)))
            .await?
        {
            Reply::Acknowledged => Ok(()),
            _ => Err(invalid_reply()),
        }
    }
    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        match self
            .resource
            .call(&self.grant, tx, Operation::Status)
            .await?
        {
            Reply::Prepared(token) => Ok(ParticipantStatus::Prepared(token)),
            Reply::Unknown => Ok(ParticipantStatus::Unknown),
            Reply::Committed => Ok(ParticipantStatus::Committed),
            Reply::Aborted => Ok(ParticipantStatus::Aborted),
            _ => Err(invalid_reply()),
        }
    }
}
/// Exclusive epoch-bound 2PC coordinator. No unfenced registration escape hatch.
///
/// Acquire first, then open/replay and recover before admitting new work. Renew
/// through the authority while running; expired/stale handles cannot be reused.
/// Drop and open a fresh coordinator after any journal error or ownership loss.
pub struct DistributedCoordinator {
    inner: Coordinator<OwnedJournal>,
    grant: Grant,
}
impl DistributedCoordinator {
    /// Open shared history under a grant. Legacy journals/resources cannot be used.
    pub fn open(
        store: &dyn JournalAuthority,
        grant: Grant,
        resources: Vec<Arc<dyn FencedResource>>,
        policy: RetryPolicy,
    ) -> Result<Self, Error> {
        if store.domain() != grant.domain || resources.iter().any(|r| r.domain() != grant.domain) {
            return Err(Error::invalid(
                "journal/resource authority domain mismatch".into(),
            ));
        }
        let journal = store.open_journal(&grant)?;
        let participants = resources
            .into_iter()
            .map(|resource| {
                Arc::new(BoundResource {
                    resource,
                    grant: grant.clone(),
                }) as Arc<dyn Participant>
            })
            .collect();
        Ok(Self {
            inner: Coordinator::open(journal, participants, policy)?,
            grant,
        })
    }
    /// Current immutable session grant.
    pub fn grant(&self) -> &Grant {
        &self.grant
    }
    /// Execute immutable work using existing durable 2PC barriers and bounded retries.
    pub async fn execute(
        &mut self,
        tx: TransactionId,
        requests: Vec<Enlistment>,
        cancellation: &Cancellation,
    ) -> Result<Status, Error> {
        self.inner.execute(tx, requests, cancellation).await
    }
    /// Replay pending decisions; undecided work is durably aborted before delivery.
    pub async fn recover(&mut self, cancellation: &Cancellation) -> Result<Vec<Status>, Error> {
        self.inner.recover(cancellation).await
    }
    /// Inspect cached acknowledged history; this is not an ownership validation.
    pub fn status(&self, tx: &TransactionId) -> Result<Option<Status>, Error> {
        self.inner.status(tx)
    }
    /// Operational counters; cached even after ownership loss.
    pub fn diagnostics(&self) -> Diagnostics {
        self.inner.diagnostics()
    }
    /// Fenced checkpoint retaining all permanent bindings and decisions.
    pub fn compact(&mut self, retention: Retention) -> Result<(), Error> {
        self.inner.compact(retention)
    }
}

/// Delegate scheduled recovery without exposing unfenced resource registration.
#[async_trait]
impl pactum_core::recovery::RecoveryTarget for DistributedCoordinator {
    fn pending_work(&self) -> Result<Vec<pactum_core::recovery::Work>, Error> {
        pactum_core::recovery::RecoveryTarget::pending_work(&self.inner)
    }
    fn replay_position(&self) -> u64 {
        pactum_core::recovery::RecoveryTarget::replay_position(&self.inner)
    }
    async fn recover_one(
        &mut self,
        key: &str,
        cancellation: &Cancellation,
    ) -> Result<pactum_core::recovery::Work, Error> {
        pactum_core::recovery::RecoveryTarget::recover_one(&mut self.inner, key, cancellation).await
    }
}
