//! Shared diagnostic vocabulary, independent of engines, databases and runtimes.
//! Codes and metadata are public contracts; Display and source text are not.
use serde::{Deserialize, Serialize};
use snafu::{IntoError, Snafu};
use std::{error::Error, fmt, sync::Arc};

/// Stable machine-readable failure code. Never classify by Display text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Invalid immutable input or configuration.
    InvalidInput,
    /// Corrupt, incompatible or impossible durable history.
    CorruptHistory,
    /// A write or durability acknowledgement may have been lost.
    DurabilityUnknown,
    /// Storage operation failed; effects may have persisted.
    Storage,
    /// Resource is temporarily unavailable before confirmed completion.
    Unavailable,
    /// Deadline elapsed; completion may be unknown.
    Timeout,
    /// Cooperative cancellation; prior effects remain possible.
    Cancelled,
    /// Owner no longer has authority. Stop using the old session.
    StaleOwner,
    /// A live owner prevents acquisition; retry acquisition later.
    OwnershipBusy,
    /// Remote response or request violates the correlation protocol.
    ProtocolMismatch,
    /// Adapter completion is unknown.
    AdapterUnknown,
    /// Transport acceptance/completion is unknown.
    TransportUnknown,
    /// Definitive resource rejection, separate from transport failure.
    Rejected,
    /// Retry budget exhausted or manual remediation required.
    OperatorRequired,
}
impl ErrorCode {
    /// Stable identifier suitable for logs, APIs and persisted scheduling metadata.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::CorruptHistory => "corrupt_history",
            Self::DurabilityUnknown => "durability_unknown",
            Self::Storage => "storage",
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::StaleOwner => "stale_owner",
            Self::OwnershipBusy => "ownership_busy",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::AdapterUnknown => "adapter_unknown",
            Self::TransportUnknown => "transport_unknown",
            Self::Rejected => "rejected",
            Self::OperatorRequired => "operator_required",
        }
    }
    /// Conservative classification, independent of message wording.
    pub const fn classification(self) -> FailureClass {
        match self {
            Self::InvalidInput | Self::Rejected => FailureClass::Permanent,
            Self::CorruptHistory
            | Self::StaleOwner
            | Self::ProtocolMismatch
            | Self::OperatorRequired => FailureClass::OperatorActionRequired,
            Self::Unavailable | Self::OwnershipBusy => FailureClass::Transient,
            _ => FailureClass::Ambiguous,
        }
    }
}
impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
/// Failure classification, never a transaction outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// May succeed later without changing immutable work.
    Transient,
    /// Same invalid work will not succeed automatically.
    Permanent,
    /// Durable effects or decision are unknown.
    Ambiguous,
    /// Requires explicit remediation or renewed authority.
    OperatorActionRequired,
}
/// Safe next action for this call; does not authorize a new protocol decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryDisposition {
    /// Repeat the same idempotent operation under valid authority.
    Retry,
    /// Inspect durable state/receipts before retry; retain immutable keys.
    ReconcileBeforeRetry,
    /// Fix invalid work/configuration; do not retry automatically.
    Permanent,
    /// Stop this session and request explicit intervention.
    OperatorIntervention,
}
/// Decision knowledge supplied by the protocol, never inferred from an error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DecisionState {
    /// Durable state must first be read or reopened.
    #[default]
    Unknown,
    /// No durable decision has been made.
    Undecided,
    /// Commit is durable and cannot be reversed.
    Commit,
    /// Abort is durable and must still be delivered.
    Abort,
}
/// Operation phase supplied by the caller, independent of diagnostic wording.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ErrorPhase {
    /// Validate work before dispatch.
    Validation,
    /// Idempotent 2PC prepare; a durable abort may fence an uncertain prepare.
    Prepare,
    /// Deliver an existing durable decision.
    Finalize,
    /// Append, flush or install a journal checkpoint.
    JournalWrite,
    /// Inspect durable state without changing it.
    Read,
    /// Reconcile existing work and receipts.
    #[default]
    Recovery,
}
/// Decision guidance; only the protocol state machine may record a decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionAdvice {
    /// An authorized 2PC coordinator may record abort before a decision exists.
    AbortBeforeDecision,
    /// Continue reconciliation of the durable commit; never switch to abort.
    RetainCommit,
    /// Continue delivery of the durable abort.
    RetainAbort,
    /// Read/reopen authoritative state before making any decision.
    Reconcile,
}
/// Call-site knowledge needed to safely interpret a diagnostic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetryContext {
    /// Phase of the failed operation.
    pub phase: ErrorPhase,
    /// Last confirmed durable decision.
    pub decision: DecisionState,
    /// This call may have changed journal durability; reopen before decisions.
    pub journal_write: bool,
    /// Current session is still authorized to issue effects.
    pub authority_valid: bool,
    /// Repeated operation uses a retained immutable idempotency key.
    pub idempotent: bool,
}
/// Diagnostic identifiers only. Never put payloads, tokens, credentials or URLs here.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorContext {
    /// Transaction, native operation, plan or Saga key.
    pub transaction: Option<String>,
    /// Durable participant identity.
    pub participant: Option<String>,
    /// Plan/Saga step identity.
    pub step: Option<String>,
    /// Protocol phase (for example prepare, commit or compensation).
    pub phase: Option<String>,
    /// Journal or adapter operation (for example append or flush).
    pub operation: Option<String>,
    /// Current fencing epoch; never the grant's authentication material.
    pub epoch: Option<u64>,
    /// Hex digest of the immutable remote request identity.
    pub request: Option<String>,
}
impl ErrorContext {
    /// Context for a journal/adapter operation.
    pub fn operation(operation: impl Into<String>) -> Self {
        Self {
            operation: Some(operation.into()),
            ..Self::default()
        }
    }
}
/// Serializable recovery guidance. Contains no source/display text and no decisions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureMetadata {
    /// Stable failure code.
    pub code: ErrorCode,
    /// Safe action established by the call boundary.
    pub disposition: RetryDisposition,
    /// Allowlisted correlation fields.
    pub context: Box<ErrorContext>,
}
impl FailureMetadata {
    /// Reject impossible code/disposition combinations in replayed metadata.
    pub fn is_valid(&self) -> bool {
        use RetryDisposition::*;
        match self.code.classification() {
            FailureClass::OperatorActionRequired => self.disposition == OperatorIntervention,
            FailureClass::Permanent => matches!(self.disposition, Permanent | OperatorIntervention),
            FailureClass::Ambiguous => {
                matches!(
                    self.disposition,
                    ReconcileBeforeRetry | OperatorIntervention
                ) || (self.code == ErrorCode::Storage && self.disposition == Retry)
            }
            FailureClass::Transient => matches!(
                self.disposition,
                Retry | ReconcileBeforeRetry | OperatorIntervention
            ),
        }
    }
    /// Reconstruct code/context after restart; local source chains are unavailable.
    pub fn diagnostic(&self) -> Diagnostic {
        Diagnostic::message(self.code, "recovered failure metadata".into())
            .with_context((*self.context).clone())
    }
}
/// Cloneable source handle. Automatic formatting redacts the underlying message.
#[derive(Clone)]
pub struct SharedSource(Arc<dyn Error + Send + Sync>);
impl SharedSource {
    /// Retain a typed source. Inspect explicitly through Error::source.
    pub fn new(source: impl Error + Send + Sync + 'static) -> Self {
        Self(Arc::new(source))
    }
}
impl fmt::Debug for SharedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted source]")
    }
}
impl fmt::Display for SharedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operation failed")
    }
}
impl Error for SharedSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
/// SNAFU diagnostic preserving source chains without exposing source text in logs.
#[derive(Clone, Debug, Snafu)]
#[snafu(module)]
pub enum Diagnostic {
    /// Explicit safe explanation, intended for static validation/invariant messages.
    #[snafu(display("{code}: {detail}"))]
    Message {
        /// Stable code.
        code: ErrorCode,
        /// Safe explanation; never copy arbitrary source text here.
        detail: String,
        /// Structured identifiers.
        context: Box<ErrorContext>,
    },
    /// Source-bearing boundary failure. Sources remain local and are not serialized.
    #[snafu(display("{code}: operation failed"))]
    Caused {
        /// Stable code independent of the source's wording.
        code: ErrorCode,
        /// Structured identifiers.
        context: Box<ErrorContext>,
        /// Retained, redacted source handle.
        source: SharedSource,
    },
}
// Diagnostic equality describes public metadata, never arbitrary source identity/text.
impl PartialEq for Diagnostic {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
            && self.context() == other.context()
            && self.safe_detail() == other.safe_detail()
    }
}
impl Eq for Diagnostic {}
impl Diagnostic {
    /// Construct a static/safe message; callers must not pass secrets.
    pub fn message(code: ErrorCode, detail: String) -> Self {
        diagnostic::MessageSnafu {
            code,
            detail,
            context: Box::<ErrorContext>::default(),
        }
        .build()
    }
    /// Preserve the original typed source without copying its display text.
    pub fn caused(code: ErrorCode, source: impl Error + Send + Sync + 'static) -> Self {
        diagnostic::CausedSnafu {
            code,
            context: Box::<ErrorContext>::default(),
        }
        .into_error(SharedSource::new(source))
    }
    /// Stable code.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Message { code, .. } | Self::Caused { code, .. } => *code,
        }
    }
    /// Failure category.
    pub fn classification(&self) -> FailureClass {
        self.code().classification()
    }
    /// Safe structured metadata.
    pub fn context(&self) -> &ErrorContext {
        match self {
            Self::Message { context, .. } | Self::Caused { context, .. } => context,
        }
    }
    /// Attach context, retaining any existing fields not supplied by this boundary.
    pub fn with_context(mut self, added: ErrorContext) -> Self {
        let target = match &mut self {
            Self::Message { context, .. } | Self::Caused { context, .. } => context,
        };
        macro_rules! merge { ($($field:ident),*) => { $(if added.$field.is_some() { target.$field = added.$field; })* }; }
        merge!(
            transaction,
            participant,
            step,
            phase,
            operation,
            epoch,
            request
        );
        self
    }
    /// Safe human-facing detail; callers must use codes for decisions.
    pub fn safe_detail(&self) -> &str {
        match self {
            Self::Message { detail, .. } => detail,
            Self::Caused { .. } => "operation failed",
        }
    }
    /// Context-sensitive guidance. A durable decision always survives a failed call.
    pub fn retry_disposition(&self, context: RetryContext) -> RetryDisposition {
        if !context.authority_valid
            || matches!(
                self.code(),
                ErrorCode::StaleOwner
                    | ErrorCode::CorruptHistory
                    | ErrorCode::ProtocolMismatch
                    | ErrorCode::OperatorRequired
            )
        {
            return RetryDisposition::OperatorIntervention;
        }
        if context.journal_write
            || context.phase == ErrorPhase::JournalWrite
            || self.code() == ErrorCode::DurabilityUnknown
        {
            return RetryDisposition::ReconcileBeforeRetry;
        }
        if self.code() == ErrorCode::Storage
            && context.phase == ErrorPhase::Read
            && context.idempotent
        {
            return RetryDisposition::Retry;
        }
        match self.classification() {
            FailureClass::Ambiguous => RetryDisposition::ReconcileBeforeRetry,
            FailureClass::OperatorActionRequired => RetryDisposition::OperatorIntervention,
            FailureClass::Transient if context.idempotent => RetryDisposition::Retry,
            FailureClass::Transient => RetryDisposition::ReconcileBeforeRetry,
            FailureClass::Permanent
                if matches!(
                    context.decision,
                    DecisionState::Commit | DecisionState::Abort
                ) =>
            {
                RetryDisposition::OperatorIntervention
            }
            FailureClass::Permanent => RetryDisposition::Permanent,
        }
    }
    /// Guidance never treats a failed call as a confirmed transaction outcome.
    pub fn decision_advice(&self, context: RetryContext) -> DecisionAdvice {
        match context.decision {
            DecisionState::Commit => DecisionAdvice::RetainCommit,
            DecisionState::Abort => DecisionAdvice::RetainAbort,
            DecisionState::Undecided
                if context.phase == ErrorPhase::Prepare
                    && context.authority_valid
                    && context.idempotent
                    && !context.journal_write
                    && !matches!(
                        self.code(),
                        ErrorCode::DurabilityUnknown
                            | ErrorCode::CorruptHistory
                            | ErrorCode::StaleOwner
                            | ErrorCode::ProtocolMismatch
                            | ErrorCode::OperatorRequired
                    ) =>
            {
                DecisionAdvice::AbortBeforeDecision
            }
            _ => DecisionAdvice::Reconcile,
        }
    }
    /// Strip source and display text for durable recovery/operator metadata.
    pub fn metadata(&self, context: RetryContext) -> FailureMetadata {
        FailureMetadata {
            code: self.code(),
            disposition: self.retry_disposition(context),
            context: Box::new(self.context().clone()),
        }
    }
    /// Emit stable codes and allowlisted identifiers, excluding source and payload text.
    pub fn trace(&self) {
        let c = self.context();
        tracing::warn!(error_code = self.code().as_str(), classification = ?self.classification(), transaction = c.transaction.as_deref(), participant = c.participant.as_deref(), step = c.step.as_deref(), phase = c.phase.as_deref(), operation = c.operation.as_deref(), fencing_epoch = c.epoch, request = c.request.as_deref(), "Pactum operation failed");
    }
}
