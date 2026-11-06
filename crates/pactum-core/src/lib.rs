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

fn sha256_hex(data: impl AsRef<[u8]>) -> String {
    use sha2::{Digest, Sha256};

    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
#[test]
fn sha256_hex_preserves_persisted_encoding() {
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

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
                    return Err(Error::invalid(
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
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
#[snafu(transparent)]
pub struct ParticipantError {
    source: pactum_errors::Diagnostic,
}
impl ParticipantError {
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
impl std::ops::Deref for ParticipantError {
    type Target = pactum_errors::Diagnostic;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

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
pub use pactum_errors::{
    DecisionAdvice, DecisionState, Diagnostic, ErrorCode, ErrorContext, ErrorPhase, FailureClass,
    RetryContext, RetryDisposition,
};

/// Layered engine errors; variants describe the failed call, never a final outcome.
#[derive(Debug, Clone, PartialEq, Eq, snafu::Snafu)]
#[snafu(module)]
pub enum Error {
    /// Invalid immutable input or configuration; never a transaction abort result.
    #[snafu(display("{source}"))]
    Invalid {
        /// Structured cause.
        source: Diagnostic,
    },
    /// Invalid durable history; automatic dispatch must stop.
    #[snafu(display("{source}"))]
    Corrupt {
        /// Structured cause.
        source: Diagnostic,
    },
    /// Ambiguous journal durability; drop/reopen before further decisions.
    #[snafu(display("{source}"))]
    InDoubt {
        /// Structured cause.
        source: Diagnostic,
    },
    /// Classified adapter, ownership or recovery failure.
    #[snafu(display("{source}"))]
    Operational {
        /// Structured cause.
        source: Diagnostic,
    },
}
impl Error {
    /// Safe validation message. Display is not a stable contract.
    pub fn invalid(detail: String) -> Self {
        Self::Invalid {
            source: Diagnostic::message(ErrorCode::InvalidInput, detail),
        }
    }
    /// Safe durable history invariant message.
    pub fn corrupt(detail: String) -> Self {
        Self::Corrupt {
            source: Diagnostic::message(ErrorCode::CorruptHistory, detail),
        }
    }
    /// Safe journal uncertainty message.
    pub fn in_doubt(detail: String) -> Self {
        Self::InDoubt {
            source: Diagnostic::message(ErrorCode::DurabilityUnknown, detail),
        }
    }
    /// Preserve a validation source without logging its text.
    pub fn invalid_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Invalid {
            source: Diagnostic::caused(ErrorCode::InvalidInput, source),
        }
    }
    /// Preserve a replay source without treating its Display as protocol history.
    pub fn corrupt_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Corrupt {
            source: Diagnostic::caused(ErrorCode::CorruptHistory, source),
        }
    }
    /// Preserve an ambiguous write source. No final decision may be inferred.
    pub fn in_doubt_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::InDoubt {
            source: Diagnostic::caused(ErrorCode::DurabilityUnknown, source),
        }
    }
    /// Annotate an actual journal write boundary, retaining stale-owner codes.
    pub fn journal(source: pactum_journal::JournalError, operation: &str) -> Self {
        let code = if source.code() == ErrorCode::StaleOwner {
            ErrorCode::StaleOwner
        } else {
            ErrorCode::DurabilityUnknown
        };
        let context = source.context().clone();
        Self::InDoubt {
            source: Diagnostic::caused(code, source)
                .with_context(context)
                .with_context(ErrorContext::operation(operation)),
        }
    }
    /// Preserve the actual storage/fencing code at a read-only journal boundary.
    /// A failed read alone does not establish corrupt history or a transaction outcome.
    pub fn journal_read(source: pactum_journal::JournalError, operation: &str) -> Self {
        let code = source.code();
        let context = source.context().clone();
        let source = Diagnostic::caused(code, source)
            .with_context(context)
            .with_context(ErrorContext {
                phase: Some("journal_read".into()),
                operation: Some(operation.into()),
                ..Default::default()
            });
        if code == ErrorCode::CorruptHistory {
            Self::Corrupt { source }
        } else {
            Self::Operational { source }
        }
    }
    /// Wrap a classified failure at an adapter/authority/recovery boundary.
    pub fn operational(source: Diagnostic) -> Self {
        Self::Operational { source }
    }
    /// Inspect stable code and structured context.
    pub fn diagnostic(&self) -> &Diagnostic {
        match self {
            Self::Invalid { source }
            | Self::Corrupt { source }
            | Self::InDoubt { source }
            | Self::Operational { source } => source,
        }
    }
    /// Add structured boundary identifiers.
    pub fn with_context(self, context: ErrorContext) -> Self {
        match self {
            Self::Invalid { source } => Self::Invalid {
                source: source.with_context(context),
            },
            Self::Corrupt { source } => Self::Corrupt {
                source: source.with_context(context),
            },
            Self::InDoubt { source } => Self::InDoubt {
                source: source.with_context(context),
            },
            Self::Operational { source } => Self::Operational {
                source: source.with_context(context),
            },
        }
    }
}
impl std::ops::Deref for Error {
    type Target = Diagnostic;
    fn deref(&self) -> &Self::Target {
        self.diagnostic()
    }
}

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

/// Unambiguous transaction knowledge, separate from a failed operational call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// Durable commit and all final acknowledgements.
    Committed,
    /// Durable abort and all final acknowledgements.
    Aborted,
    /// Durable decision exists; delivery still needs recovery.
    PendingRecovery {
        /// Decision that recovery must retain.
        decision: Decision,
    },
    /// No durable decision is known, including after an ambiguous journal write.
    Unknown,
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
    /// Process-local typed failure; source chains are never journaled.
    pub diagnostic: Option<pactum_errors::Diagnostic>,
}

impl Status {
    /// Read transaction knowledge from durable status, never from Error alone.
    pub fn transaction_outcome(&self) -> TransactionOutcome {
        match self.outcome {
            Outcome::Committed => TransactionOutcome::Committed,
            Outcome::Aborted => TransactionOutcome::Aborted,
            Outcome::InDoubt => match self.decision {
                Some(decision) => TransactionOutcome::PendingRecovery { decision },
                None => TransactionOutcome::Unknown,
            },
        }
    }
    /// Classification context for an existing idempotent reconciliation call.
    pub fn retry_context(&self) -> RetryContext {
        RetryContext {
            phase: if self.decision.is_some() {
                ErrorPhase::Finalize
            } else {
                ErrorPhase::Prepare
            },
            decision: match self.decision {
                Some(Decision::Commit) => DecisionState::Commit,
                Some(Decision::Abort) => DecisionState::Abort,
                None => DecisionState::Undecided,
            },
            authority_valid: true,
            idempotent: true,
            journal_write: false,
        }
    }
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
    diagnostic: Option<pactum_errors::Diagnostic>,
}
fn validate_requests(requests: &[Enlistment]) -> Result<(), Error> {
    if requests.is_empty()
        || requests
            .windows(2)
            .any(|w| w[0].participant >= w[1].participant)
    {
        return Err(Error::invalid(
            "enlist at least one resource, sorted uniquely by identity".into(),
        ));
    }
    Ok(())
}
fn apply(states: &mut BTreeMap<TransactionId, State>, record: Record) -> Result<(), Error> {
    let corrupt = || Error::corrupt("illegal transaction transition".into());
    if let Record::Begin { tx, requests } = record {
        validate_requests(&requests).map_err(Error::corrupt_source)?;
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
                diagnostic: None,
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
            return Err(Error::invalid(
                "positive attempts and timeout required".into(),
            ));
        }
        let mut states = BTreeMap::new();
        let mut durable_sequence = 0;
        if let Some(checkpoint) = journal
            .checkpoint()
            .map_err(|source| Error::journal_read(source, "checkpoint"))?
        {
            if checkpoint.version != 1 {
                return Err(Error::corrupt("unsupported 2PC checkpoint".into()));
            }
            let records: Vec<Record> =
                serde_json::from_slice(&checkpoint.payload).map_err(Error::corrupt_source)?;
            if records.len() as u64 > checkpoint.watermark {
                return Err(Error::corrupt("checkpoint exceeds watermark".into()));
            }
            for record in records {
                apply(&mut states, record)?;
            }
            durable_sequence = checkpoint.watermark;
        }
        journal
            .visit(&mut |entry| {
                if Some(entry.sequence) != durable_sequence.checked_add(1) || entry.version != 1 {
                    return Err(pactum_journal::JournalError::corrupt(
                        "sequence gap or unsupported protocol version".into(),
                    ));
                }
                let record = serde_json::from_slice(&entry.payload).map_err(|e| {
                    pactum_journal::JournalError::classified(crate::ErrorCode::CorruptHistory, e)
                })?;
                apply(&mut states, record).map_err(|e| {
                    pactum_journal::JournalError::classified(crate::ErrorCode::CorruptHistory, e)
                })?;
                durable_sequence = entry.sequence;
                Ok(())
            })
            .map_err(|source| Error::journal_read(source, "replay"))?;
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
        let payload = serde_json::to_vec(&records).map_err(Error::invalid_source)?;
        self.poisoned = true;
        self.journal
            .compact(self.durable_sequence, 1, &payload, retention)
            .map_err(|e| Error::journal(e, "compact"))?;
        self.poisoned = false;
        Ok(())
    }
    /// Add an available resource. Never silently replace an existing identity.
    pub fn register(&mut self, participant: Arc<dyn Participant>) -> Result<(), Error> {
        let id = participant.id();
        if self.participants.contains_key(&id) {
            return Err(Error::invalid(format!("duplicate participant: {id}")));
        }
        self.participants.insert(id, participant);
        Ok(())
    }
    fn healthy(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::in_doubt(
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
        let bytes = serde_json::to_vec(&record).map_err(Error::invalid_source)?;
        let context = ErrorContext {
            transaction: Some(tx.as_str().into()),
            phase: Some(
                match &record {
                    Record::Begin { .. } => "begin",
                    Record::Prepared { .. } => "prepare",
                    Record::Decided { .. } => "decision",
                    Record::Acknowledged { .. } => "acknowledge",
                }
                .into(),
            ),
            ..Default::default()
        };
        // No await between append, barrier, and publishing state.
        self.poisoned = true;
        let sequence = self.journal.append(1, &bytes).map_err(|e| {
            let error = Error::journal(e, "append").with_context(context.clone());
            error.trace();
            error
        })?;
        let durable = self.journal.flush().map_err(|e| {
            let error = Error::journal(e, "flush").with_context(context.clone());
            error.trace();
            error
        })?;
        if Some(sequence) != self.durable_sequence.checked_add(1) || durable != sequence {
            return Err(
                Error::in_doubt("barrier did not cover appended record".into())
                    .with_context(context),
            );
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
                diagnostic: s.diagnostic.clone(),
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
                return Err(Error::invalid(
                    "transaction ID reused for different work".into(),
                ));
            }
            return self.reconcile(&tx, cancellation).await;
        }
        if requests
            .iter()
            .any(|r| !self.participants.contains_key(&r.participant))
        {
            return Err(Error::invalid("unregistered participant".into()));
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
                    let diagnostic = match other {
                        Err(e) => e.diagnostic().clone(),
                        Ok(Vote::Rejected(_)) => {
                            Diagnostic::message(ErrorCode::Rejected, "prepare refused".into())
                        }
                        Ok(Vote::Prepared(_)) => Diagnostic::message(
                            ErrorCode::ProtocolMismatch,
                            "prepare returned an invalid token".into(),
                        ),
                    }
                    .with_context(ErrorContext {
                        transaction: Some(tx.as_str().into()),
                        participant: Some(request.participant.as_str().into()),
                        phase: Some("prepare".into()),
                        ..ErrorContext::default()
                    });
                    diagnostic.trace();
                    let state = self.states.get_mut(&tx).unwrap();
                    state.last_error = Some(diagnostic.to_string());
                    state.diagnostic = Some(diagnostic);
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
                let diagnostic = Diagnostic::message(
                    ErrorCode::Cancelled,
                    "cancelled; durable decision retained".into(),
                )
                .with_context(ErrorContext {
                    transaction: Some(tx.as_str().into()),
                    phase: Some("finalize".into()),
                    ..Default::default()
                });
                let state = self.states.get_mut(tx).unwrap();
                state.last_error = Some(diagnostic.to_string());
                state.diagnostic = Some(diagnostic);
                break;
            }
            let Some(participant) = self.participants.get(&id).cloned() else {
                let state = self.states.get_mut(tx).unwrap();
                let diagnostic =
                    Diagnostic::message(ErrorCode::Unavailable, "participant unavailable".into())
                        .with_context(ErrorContext {
                            transaction: Some(tx.as_str().into()),
                            participant: Some(id.as_str().into()),
                            phase: Some("finalize".into()),
                            ..Default::default()
                        });
                state.last_error = Some(diagnostic.to_string());
                state.diagnostic = Some(diagnostic);
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
                    let diagnostic = e.diagnostic().clone().with_context(ErrorContext {
                        transaction: Some(tx.as_str().into()),
                        participant: Some(id.as_str().into()),
                        phase: Some(
                            match decision {
                                Decision::Commit => "commit",
                                Decision::Abort => "abort",
                            }
                            .into(),
                        ),
                        ..ErrorContext::default()
                    });
                    diagnostic.trace();
                    let state = self.states.get_mut(tx).unwrap();
                    state.last_error = Some(diagnostic.to_string());
                    state.diagnostic = Some(diagnostic);
                }
            }
        }
        if self.states[tx].acknowledged.len() == self.states[tx].requests.len() {
            self.states.get_mut(tx).unwrap().last_error = None;
            self.states.get_mut(tx).unwrap().diagnostic = None;
        }
        Ok(self.status(tx)?.unwrap())
    }
}
async fn retry<T, F, Fut>(
    policy: &RetryPolicy,
    cancellation: &Cancellation,
    mut operation: F,
) -> Result<T, ParticipantError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ParticipantError>>,
{
    let mut last = ParticipantError::message("participant retry budget exhausted".into());
    for attempt in 0..policy.attempts {
        if cancellation.is_cancelled() {
            return Err(ParticipantError::from_diagnostic(Diagnostic::message(
                ErrorCode::Cancelled,
                "cancelled".into(),
            )));
        }
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParticipantError::from_diagnostic(Diagnostic::message(ErrorCode::Cancelled, "cancelled".into()))),
            result = tokio::time::timeout(policy.timeout, operation()) => result,
        };
        match result {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(e)) => {
                if matches!(
                    e.classification(),
                    FailureClass::Permanent | FailureClass::OperatorActionRequired
                ) {
                    return Err(e);
                }
                last = e;
            }
            Err(e) => last = ParticipantError::classified(ErrorCode::Timeout, e),
        }
        if attempt + 1 < policy.attempts {
            tokio::select! {
                _ = cancellation.cancelled() => return Err(ParticipantError::from_diagnostic(Diagnostic::message(ErrorCode::Cancelled, "cancelled".into()))),
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
                Err(Error::Corrupt { .. })
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
            Err(Error::Invalid { .. })
        ));
    }
}

/// Multi-mode lifecycle, backend contracts, and 2PC compatibility.
pub mod execution;

/// Optional host-neutral recovery scheduling and operator controls.
pub mod recovery;

/// Durable bounded transaction plans and mode adapters.
pub mod plan;

/// Durable forward operations and explicit best-effort compensations.
pub mod saga;

/// Optional pure transaction planning over frozen provider capabilities.
pub mod planner;

/// Pluggable dispatch policies and engine-enforced admission budgets.
pub mod scheduler;

/// Optional negotiated participant isolation and conflict validation contracts.
pub mod isolation;
