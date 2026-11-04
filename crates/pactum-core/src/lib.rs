//! Durable single-owner two-phase commit with deterministic replay.
//!
//! See `Coordinator`, `Participant`, and the repository failure model. Tokio is
//! used for participant timeouts; journal barriers are synchronous and must be
//! bounded by the backend. No coordinator consensus or cross-resource isolation
//! is provided. A dropped execution future can be resumed with `recover`.
use async_trait::async_trait;
use pactum_journal::Journal;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    sync::Arc,
    time::Duration,
};
use tokio::sync::watch;

macro_rules! identifier {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            /// Validate a stable caller-assigned identifier (1..=256 bytes).
            pub fn new(value: impl Into<String>) -> Result<Self, Error> {
                let value = value.into();
                if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                    return Err(Error::Invalid(
                        "identifier must be 1..=256 bytes without controls".into(),
                    ));
                }
                Ok(Self(value))
            }
            /// Borrow the stable identifier.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = Error;
            fn try_from(v: String) -> Result<Self, Error> {
                Self::new(v)
            }
        }
        impl From<$name> for String {
            fn from(v: $name) -> Self {
                v.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}
identifier!(
    TransactionId,
    "Globally unique durable idempotency key. Never reuse for different work."
);
identifier!(
    ParticipantId,
    "Stable resource identity; registration must survive restart unchanged."
);

/// A participant's opaque, durably recoverable prepare handle.
pub type PrepareToken = Vec<u8>;

/// Immutable resource request persisted before any participant is called.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Enlistment {
    /// Stable participant identity.
    pub participant: ParticipantId,
    /// Opaque application operation, bound permanently to this transaction ID.
    pub payload: Vec<u8>,
}

/// Durable terminal decision, distinct from successful delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    /// All participants voted prepared.
    Commit,
    /// No commit decision exists; release or fence all enlisted resources.
    Abort,
}

/// Result of a prepare request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Vote {
    /// Resource and token are durable before this response.
    Prepared(PrepareToken),
    /// Resource refuses the operation; coordinator must abort.
    Rejected(String),
}

/// Participant-side durable state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantStatus {
    /// No known request or terminal tombstone.
    Unknown,
    /// Durable reservation awaiting the coordinator decision.
    Prepared(PrepareToken),
    /// Durable commit and resource effects.
    Committed,
    /// Durable abort tombstone; future prepares must refuse.
    Aborted,
}

/// Participant transport/resource failure (including ambiguous completion).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantError(pub String);
impl fmt::Display for ParticipantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ParticipantError {}

/// Durable, idempotent resource adapter. All calls use the transaction as their
/// idempotency key. Duplicate prepare must verify the same payload and token;
/// duplicate finalization must not repeat effects. Conflicting decisions fail.
///
/// Prepared responses promise durable state, isolation/reservations, and eventual
/// resolution after restart. Commit persists effects and terminal state atomically.
/// Abort must persist a tombstone even when prepare is unknown (including late
/// prepare after a timed-out/dropped call). Never expire prepared work unilaterally.
/// Futures may be dropped at any await; retries and abort must fence delayed work.
/// Methods must yield to Tokio; blocking calls cannot be preempted by timeouts.
#[async_trait]
pub trait Participant: Send + Sync {
    /// Stable resource identity, including its durable namespace.
    fn id(&self) -> ParticipantId;
    /// Durably prepare immutable work, or reject it.
    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError>;
    /// Apply durable prepared work exactly once, verifying its token.
    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError>;
    /// Release/fence work. Missing token is valid after an unacknowledged prepare.
    async fn abort(&self, tx: &TransactionId, token: Option<&[u8]>)
    -> Result<(), ParticipantError>;
    /// Inspect durable participant state for operational diagnosis.
    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError>;
}

/// Engine errors. Journal uncertainty always requires dropping and reopening the
/// coordinator and journal; do not interpret it as a failed/aborted transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Invalid input or registration.
    Invalid(String),
    /// Journal schema or protocol history violates the state machine.
    Corrupt(String),
    /// Durability is uncertain. No further finalization is dispatched.
    InDoubt(String),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

/// Completion known from durable coordinator acknowledgements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Durable commit and every participant acknowledged it.
    Committed,
    /// Durable abort and every participant acknowledged it.
    Aborted,
    /// Work needs reconciliation; consult status for the durable decision.
    InDoubt,
}

/// Observable coordinator phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    /// No durable decision yet.
    Preparing,
    /// A decision is durable but delivery is incomplete.
    Finalizing,
    /// Every enlisted resource acknowledged the durable decision.
    Complete,
}

/// Transaction snapshot. Diagnostics are local to this process; decisions and
/// acknowledgements are durable. Missing participants remain pending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    /// Stable transaction key.
    pub transaction: TransactionId,
    /// Completion result.
    pub outcome: Outcome,
    /// Current protocol phase.
    pub phase: Phase,
    /// Durable decision, if known. Never overridden by cancellation.
    pub decision: Option<Decision>,
    /// Participants without durable final acknowledgements, in stable order.
    pub pending: Vec<ParticipantId>,
    /// Latest local operation error; not a durable diagnostic log.
    pub last_error: Option<String>,
}

/// Bounded per-operation retry settings. Attempts must be positive.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum attempts per participant operation per execution/recovery pass.
    pub attempts: u32,
    /// Timeout for each cooperative async operation.
    pub timeout: Duration,
    /// Fixed delay between failed attempts.
    pub backoff: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            timeout: Duration::from_secs(5),
            backoff: Duration::from_millis(25),
        }
    }
}

/// Cloneable cooperative cancellation. Before a decision this requests abort;
/// after a decision it stops delivery and leaves recovery work pending.
#[derive(Clone, Debug)]
pub struct Cancellation(watch::Sender<bool>);
impl Default for Cancellation {
    fn default() -> Self {
        Self(watch::channel(false).0)
    }
}
impl Cancellation {
    /// Request cancellation permanently.
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    /// Whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        if *receiver.borrow_and_update() {
            return;
        }
        let _ = receiver.wait_for(|value| *value).await;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Record {
    Begin {
        tx: TransactionId,
        requests: Vec<Enlistment>,
    },
    Prepared {
        tx: TransactionId,
        participant: ParticipantId,
        token: PrepareToken,
    },
    Decided {
        tx: TransactionId,
        decision: Decision,
    },
    Acknowledged {
        tx: TransactionId,
        participant: ParticipantId,
    },
}
#[derive(Clone, Debug)]
struct State {
    requests: Vec<Enlistment>,
    prepared: BTreeMap<ParticipantId, PrepareToken>,
    decision: Option<Decision>,
    acknowledged: BTreeSet<ParticipantId>,
    last_error: Option<String>,
}
fn validate_requests(requests: &[Enlistment]) -> Result<(), Error> {
    if requests.is_empty()
        || requests
            .windows(2)
            .any(|w| w[0].participant >= w[1].participant)
    {
        return Err(Error::Invalid(
            "enlist at least one resource, sorted uniquely by identity".into(),
        ));
    }
    Ok(())
}
fn apply(states: &mut BTreeMap<TransactionId, State>, record: Record) -> Result<(), Error> {
    let corrupt = || Error::Corrupt("illegal transaction transition".into());
    if let Record::Begin { tx, requests } = record {
        validate_requests(&requests).map_err(|e| Error::Corrupt(e.to_string()))?;
        if states.contains_key(&tx) {
            return Err(corrupt());
        }
        states.insert(
            tx,
            State {
                requests,
                prepared: BTreeMap::new(),
                decision: None,
                acknowledged: BTreeSet::new(),
                last_error: None,
            },
        );
        return Ok(());
    }
    let tx = match &record {
        Record::Prepared { tx, .. }
        | Record::Decided { tx, .. }
        | Record::Acknowledged { tx, .. } => tx,
        Record::Begin { .. } => unreachable!(),
    };
    let state = states.get_mut(tx).ok_or_else(corrupt)?;
    match record {
        Record::Prepared {
            participant, token, ..
        } => {
            if state.decision.is_some()
                || token.is_empty()
                || token.len() > 65536
                || state.prepared.contains_key(&participant)
                || !state.requests.iter().any(|r| r.participant == participant)
            {
                return Err(corrupt());
            }
            state.prepared.insert(participant, token);
        }
        Record::Decided { decision, .. } => {
            if state.decision.is_some()
                || (decision == Decision::Commit && state.prepared.len() != state.requests.len())
            {
                return Err(corrupt());
            }
            state.decision = Some(decision);
        }
        Record::Acknowledged { participant, .. } => {
            if state.decision.is_none()
                || !state.requests.iter().any(|r| r.participant == participant)
                || !state.acknowledged.insert(participant)
            {
                return Err(corrupt());
            }
        }
        Record::Begin { .. } => unreachable!(),
    }
    Ok(())
}

/// Operational counters and last-known durable journal position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostics {
    /// Last acknowledged durable journal sequence, including replayed history.
    pub durable_records: u64,
    /// Number of recovery passes invoked in this process.
    pub recovery_passes: u64,
    /// Number of permanent transaction bindings in recovered state.
    pub transactions: usize,
    /// Number of transactions still lacking every final acknowledgement.
    pub pending_transactions: usize,
    /// Journal uncertainty requires reopening before using transaction status.
    pub reopen_required: bool,
}

/// Single-owner coordinator. Keep its journal and participant identities bound to
/// the same durable namespace across restarts. `open` validates replay; `recover`
/// aborts undecided work and redelivers existing decisions. Ownership/fencing
/// across machines is the application's responsibility, not distributed consensus.
pub struct Coordinator<J: Journal> {
    journal: J,
    participants: BTreeMap<ParticipantId, Arc<dyn Participant>>,
    states: BTreeMap<TransactionId, State>,
    pending: BTreeSet<TransactionId>,
    policy: RetryPolicy,
    poisoned: bool,
    durable_sequence: u64,
    recovery_passes: u64,
}
impl<J: Journal> Coordinator<J> {
    /// Replay all history and validate registrations. Unavailable participants may
    /// be omitted and registered later; their transactions remain explicitly pending.
    pub fn open(
        mut journal: J,
        participants: Vec<Arc<dyn Participant>>,
        policy: RetryPolicy,
    ) -> Result<Self, Error> {
        if policy.attempts == 0 || policy.timeout.is_zero() {
            return Err(Error::Invalid(
                "positive attempts and timeout required".into(),
            ));
        }
        let mut states = BTreeMap::new();
        let mut durable_sequence = 0;
        if let Some(checkpoint) = journal
            .checkpoint()
            .map_err(|e| Error::Corrupt(e.to_string()))?
        {
            if checkpoint.version != 1 {
                return Err(Error::Corrupt("unsupported 2PC checkpoint".into()));
            }
            let records: Vec<Record> = serde_json::from_slice(&checkpoint.payload)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
            if records.len() as u64 > checkpoint.watermark {
                return Err(Error::Corrupt("checkpoint exceeds watermark".into()));
            }
            for record in records {
                apply(&mut states, record)?;
            }
            durable_sequence = checkpoint.watermark;
        }
        journal
            .visit(&mut |entry| {
                if Some(entry.sequence) != durable_sequence.checked_add(1) || entry.version != 1 {
                    return Err(pactum_journal::JournalError(
                        "sequence gap or unsupported protocol version".into(),
                    ));
                }
                let record = serde_json::from_slice(&entry.payload)
                    .map_err(|e| pactum_journal::JournalError(e.to_string()))?;
                apply(&mut states, record)
                    .map_err(|e| pactum_journal::JournalError(e.to_string()))?;
                durable_sequence = entry.sequence;
                Ok(())
            })
            .map_err(|e| Error::Corrupt(e.to_string()))?;
        let mut coordinator = Self {
            journal,
            participants: BTreeMap::new(),
            pending: states
                .iter()
                .filter(|(_, s)| s.acknowledged.len() != s.requests.len())
                .map(|(id, _)| id.clone())
                .collect(),
            states,
            policy,
            poisoned: false,
            durable_sequence,
            recovery_passes: 0,
        };
        for participant in participants {
            coordinator.register(participant)?;
        }
        Ok(coordinator)
    }
    /// Checkpoint all durable state and compact covered events. Permanent key
    /// bindings, prepare tokens, decisions and acknowledgements are retained.
    /// On any installation error, drop and reopen before using this coordinator.
    pub fn compact(&mut self, retention: pactum_journal::Retention) -> Result<(), Error> {
        self.healthy()?;
        let mut records = Vec::new();
        for (tx, state) in &self.states {
            records.push(Record::Begin {
                tx: tx.clone(),
                requests: state.requests.clone(),
            });
            for (participant, token) in &state.prepared {
                records.push(Record::Prepared {
                    tx: tx.clone(),
                    participant: participant.clone(),
                    token: token.clone(),
                });
            }
            if let Some(decision) = state.decision {
                records.push(Record::Decided {
                    tx: tx.clone(),
                    decision,
                });
            }
            for participant in &state.acknowledged {
                records.push(Record::Acknowledged {
                    tx: tx.clone(),
                    participant: participant.clone(),
                });
            }
        }
        let payload = serde_json::to_vec(&records).map_err(|e| Error::Invalid(e.to_string()))?;
        self.poisoned = true;
        self.journal
            .compact(self.durable_sequence, 1, &payload, retention)
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        self.poisoned = false;
        Ok(())
    }
    /// Add an available resource. Never silently replace an existing identity.
    pub fn register(&mut self, participant: Arc<dyn Participant>) -> Result<(), Error> {
        let id = participant.id();
        if self.participants.contains_key(&id) {
            return Err(Error::Invalid(format!("duplicate participant: {id}")));
        }
        self.participants.insert(id, participant);
        Ok(())
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::InDoubt(
                "journal uncertainty; drop and reopen".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn persist(&mut self, record: Record) -> Result<(), Error> {
        self.healthy()?;
        let tx = match &record {
            Record::Begin { tx, .. }
            | Record::Prepared { tx, .. }
            | Record::Decided { tx, .. }
            | Record::Acknowledged { tx, .. } => tx,
        }
        .clone();
        let mut next = BTreeMap::new();
        if let Some(state) = self.states.get(&tx) {
            next.insert(tx.clone(), state.clone());
        }
        apply(&mut next, record.clone())?;
        let bytes = serde_json::to_vec(&record).map_err(|e| Error::Invalid(e.to_string()))?;
        // No await between append, barrier, and publishing state.
        self.poisoned = true;
        let sequence = self
            .journal
            .append(1, &bytes)
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        let durable = self
            .journal
            .flush()
            .map_err(|e| Error::InDoubt(e.to_string()))?;
        if Some(sequence) != self.durable_sequence.checked_add(1) || durable != sequence {
            return Err(Error::InDoubt(
                "barrier did not cover appended record".into(),
            ));
        }
        let state = next
            .remove(&tx)
            .expect("validated transition retains state");
        if state.acknowledged.len() == state.requests.len() {
            self.pending.remove(&tx);
        } else {
            self.pending.insert(tx.clone());
        }
        self.states.insert(tx, state);
        self.durable_sequence = durable;
        self.poisoned = false;
        Ok(())
    }
    /// Inspect counters even when a journal error requires reopening. The
    /// durable position is only the last *acknowledged* barrier in that case.
    pub fn diagnostics(&self) -> Diagnostics {
        Diagnostics {
            durable_records: self.durable_sequence,
            recovery_passes: self.recovery_passes,
            transactions: self.states.len(),
            pending_transactions: self
                .states
                .values()
                .filter(|s| s.acknowledged.len() != s.requests.len())
                .count(),
            reopen_required: self.poisoned,
        }
    }
    /// Inspect the last known durable state. After a journal error, reopen first.
    pub fn status(&self, tx: &TransactionId) -> Result<Option<Status>, Error> {
        self.healthy()?;
        Ok(self.states.get(tx).map(|s| {
            let pending: Vec<_> = s
                .requests
                .iter()
                .filter(|r| !s.acknowledged.contains(&r.participant))
                .map(|r| r.participant.clone())
                .collect();
            let outcome = match (s.decision, pending.is_empty()) {
                (Some(Decision::Commit), true) => Outcome::Committed,
                (Some(Decision::Abort), true) => Outcome::Aborted,
                _ => Outcome::InDoubt,
            };
            let phase = if s.decision.is_none() {
                Phase::Preparing
            } else if pending.is_empty() {
                Phase::Complete
            } else {
                Phase::Finalizing
            };
            Status {
                transaction: tx.clone(),
                outcome,
                phase,
                decision: s.decision,
                pending,
                last_error: s.last_error.clone(),
            }
        }))
    }
    /// Begin/enlist and execute in participant-ID order. Repeated identical input
    /// resumes the original transaction; conflicting reuse fails before dispatch.
    /// If an earlier call was dropped with no decision, resumption safely aborts.
    pub async fn execute(
        &mut self,
        tx: TransactionId,
        mut requests: Vec<Enlistment>,
        cancellation: &Cancellation,
    ) -> Result<Status, Error> {
        self.healthy()?;
        requests.sort_by(|a, b| a.participant.cmp(&b.participant));
        validate_requests(&requests)?;
        if let Some(state) = self.states.get(&tx) {
            if state.requests != requests {
                return Err(Error::Invalid(
                    "transaction ID reused for different work".into(),
                ));
            }
            return self.reconcile(&tx, cancellation).await;
        }
        if requests
            .iter()
            .any(|r| !self.participants.contains_key(&r.participant))
        {
            return Err(Error::Invalid("unregistered participant".into()));
        }
        self.persist(Record::Begin {
            tx: tx.clone(),
            requests: requests.clone(),
        })?;
        let mut decision = Decision::Commit;
        for request in requests {
            let participant = self.participants[&request.participant].clone();
            let result = retry(&self.policy, cancellation, || {
                participant.prepare(&tx, &request.payload)
            })
            .await;
            match result {
                Ok(Vote::Prepared(token)) if !token.is_empty() && token.len() <= 65536 => {
                    self.persist(Record::Prepared {
                        tx: tx.clone(),
                        participant: request.participant,
                        token,
                    })?;
                }
                other => {
                    decision = Decision::Abort;
                    self.states.get_mut(&tx).unwrap().last_error =
                        Some(format!("prepare {}: {other:?}", request.participant));
                    break;
                }
            }
        }
        if cancellation.is_cancelled() {
            decision = Decision::Abort;
        }
        self.persist(Record::Decided {
            tx: tx.clone(),
            decision,
        })?;
        self.finalize(&tx, cancellation).await
    }
    /// Reconcile every transaction in transaction-ID order. No new prepare is
    /// issued during recovery. Undecided work gets a durable abort first.
    pub async fn recover(&mut self, cancellation: &Cancellation) -> Result<Vec<Status>, Error> {
        self.recover_pending(cancellation).await?;
        self.states
            .keys()
            .map(|id| self.status(id).map(Option::unwrap))
            .collect()
    }
    /// Reconcile only the indexed pending transactions, returning their statuses.
    /// Cost excludes enumerating permanent terminal retry bindings.
    pub async fn recover_pending(
        &mut self,
        cancellation: &Cancellation,
    ) -> Result<Vec<Status>, Error> {
        self.healthy()?;
        self.recovery_passes += 1;
        let ids: Vec<_> = self.pending.iter().cloned().collect();
        let mut statuses = Vec::new();
        for tx in ids {
            statuses.push(self.reconcile(&tx, cancellation).await?);
        }
        Ok(statuses)
    }
    async fn reconcile(
        &mut self,
        tx: &TransactionId,
        cancellation: &Cancellation,
    ) -> Result<Status, Error> {
        if self.states[tx].decision.is_none() {
            self.persist(Record::Decided {
                tx: tx.clone(),
                decision: Decision::Abort,
            })?;
        }
        self.finalize(tx, cancellation).await
    }
    async fn finalize(
        &mut self,
        tx: &TransactionId,
        cancellation: &Cancellation,
    ) -> Result<Status, Error> {
        let snapshot = self.status(tx)?.unwrap();
        let decision = snapshot
            .decision
            .expect("finalize requires durable decision");
        for id in snapshot.pending {
            if cancellation.is_cancelled() {
                self.states.get_mut(tx).unwrap().last_error =
                    Some("cancelled; durable decision retained".into());
                break;
            }
            let Some(participant) = self.participants.get(&id).cloned() else {
                self.states.get_mut(tx).unwrap().last_error =
                    Some(format!("participant unavailable: {id}"));
                continue;
            };
            let token = self.states[tx].prepared.get(&id).cloned();
            let result = retry(&self.policy, cancellation, || async {
                match decision {
                    Decision::Commit => {
                        participant
                            .commit(tx, token.as_deref().expect("validated commit token"))
                            .await
                    }
                    Decision::Abort => participant.abort(tx, token.as_deref()).await,
                }
            })
            .await;
            match result {
                Ok(()) => self.persist(Record::Acknowledged {
                    tx: tx.clone(),
                    participant: id,
                })?,
                Err(e) => {
                    self.states.get_mut(tx).unwrap().last_error =
                        Some(format!("finalize {id}: {e}"))
                }
            }
        }
        if self.states[tx].acknowledged.len() == self.states[tx].requests.len() {
            self.states.get_mut(tx).unwrap().last_error = None;
        }
        Ok(self.status(tx)?.unwrap())
    }
}
async fn retry<T, F, Fut>(
    policy: &RetryPolicy,
    cancellation: &Cancellation,
    mut operation: F,
) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ParticipantError>>,
{
    let mut last = String::new();
    for attempt in 0..policy.attempts {
        if cancellation.is_cancelled() {
            return Err("cancelled".into());
        }
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err("cancelled".into()),
            result = tokio::time::timeout(policy.timeout, operation()) => result,
        };
        match result {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(e)) => last = e.to_string(),
            Err(_) => last = "timeout".into(),
        }
        if attempt + 1 < policy.attempts {
            tokio::select! {
                _ = cancellation.cancelled() => return Err("cancelled".into()),
                _ = tokio::time::sleep(policy.backoff) => {},
            }
        }
    }
    Err(last)
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
    fn history(records: Vec<Record>) -> History {
        History(
            records
                .into_iter()
                .enumerate()
                .map(|(i, r)| Entry {
                    sequence: i as u64 + 1,
                    version: 1,
                    payload: serde_json::to_vec(&r).unwrap(),
                })
                .collect(),
        )
    }
    fn tx() -> TransactionId {
        TransactionId::new("t").unwrap()
    }
    fn participant() -> ParticipantId {
        ParticipantId::new("p").unwrap()
    }
    fn begin() -> Record {
        Record::Begin {
            tx: tx(),
            requests: vec![Enlistment {
                participant: participant(),
                payload: b"work".to_vec(),
            }],
        }
    }
    fn prepared() -> Record {
        Record::Prepared {
            tx: tx(),
            participant: participant(),
            token: b"token".to_vec(),
        }
    }
    fn decision(decision: Decision) -> Record {
        Record::Decided { tx: tx(), decision }
    }
    fn ack() -> Record {
        Record::Acknowledged {
            tx: tx(),
            participant: participant(),
        }
    }
    #[test]
    fn replay_accepts_legal_prefixes_and_rejects_conflicting_or_impossible_history() {
        let commit = vec![begin(), prepared(), decision(Decision::Commit), ack()];
        let abort = vec![begin(), decision(Decision::Abort), ack()];
        for records in [&commit, &abort] {
            for prefix in 0..=records.len() {
                let coordinator = Coordinator::open(
                    history(records[..prefix].to_vec()),
                    vec![],
                    RetryPolicy::default(),
                )
                .unwrap();
                if prefix > 0 {
                    assert!(coordinator.status(&tx()).unwrap().is_some());
                }
            }
        }
        for records in [
            vec![prepared()],
            vec![begin(), begin()],
            vec![begin(), decision(Decision::Commit)],
            vec![begin(), ack()],
            vec![begin(), prepared(), prepared()],
            vec![begin(), decision(Decision::Abort), prepared()],
            vec![
                begin(),
                prepared(),
                decision(Decision::Commit),
                decision(Decision::Abort),
            ],
            vec![
                begin(),
                decision(Decision::Abort),
                decision(Decision::Commit),
            ],
            vec![begin(), decision(Decision::Abort), ack(), ack()],
            vec![
                begin(),
                Record::Prepared {
                    tx: tx(),
                    participant: ParticipantId::new("other").unwrap(),
                    token: vec![1],
                },
            ],
            vec![
                begin(),
                Record::Prepared {
                    tx: tx(),
                    participant: participant(),
                    token: vec![],
                },
            ],
        ] {
            assert!(matches!(
                Coordinator::open(history(records), vec![], RetryPolicy::default()),
                Err(Error::Corrupt(_))
            ));
        }
    }
    #[test]
    fn corrupt_schema_gaps_unknown_fields_and_invalid_ids_fail_closed() {
        let mut valid = history(vec![begin()]);
        valid.0[0].version = 2;
        assert!(Coordinator::open(valid, vec![], RetryPolicy::default()).is_err());
        let mut valid = history(vec![begin()]);
        valid.0[0].sequence = 2;
        assert!(Coordinator::open(valid, vec![], RetryPolicy::default()).is_err());
        for payload in [
            br#"{"kind":"Begin","tx":"","requests":[]}"#.as_slice(),
            br#"{"kind":"Begin","tx":"t","requests":[],"unknown":true}"#,
            br#"{"kind":"FutureRecord","tx":"t"}"#,
            br#"{"kind":"Begin""#,
        ] {
            let journal = History(vec![Entry {
                sequence: 1,
                version: 1,
                payload: payload.to_vec(),
            }]);
            assert!(Coordinator::open(journal, vec![], RetryPolicy::default()).is_err());
        }
        assert!(TransactionId::new("").is_err());
        assert!(ParticipantId::new("line\nbreak").is_err());
        assert!(TransactionId::new("x".repeat(257)).is_err());
        let policy = RetryPolicy {
            attempts: 0,
            ..RetryPolicy::default()
        };
        assert!(matches!(
            Coordinator::open(history(vec![]), vec![], policy),
            Err(Error::Invalid(_))
        ));
    }
}

/// Multi-mode lifecycle, backend contracts, and 2PC compatibility.
pub mod execution;
