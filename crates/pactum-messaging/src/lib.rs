//! Optional reliable messaging contracts, independent of databases and brokers.
//! See `docs/messaging.md` for atomicity, retention and adapter obligations.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, time::Duration};

/// Messaging validation, storage or fencing error. A storage error at commit may
/// be ambiguous: reopen and retry with the same immutable message identity.
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
#[snafu(transparent)]
pub struct Error {
    source: pactum_errors::Diagnostic,
}
impl Error {
    /// Construct a safe legacy diagnostic; completion remains conservative.
    pub fn message(detail: String) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::message(
            pactum_errors::ErrorCode::InvalidInput,
            detail,
        ))
    }
    /// Preserve a typed boundary source using the conservative default code.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::classified(pactum_errors::ErrorCode::Storage, source)
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
impl std::ops::Deref for Error {
    type Target = pactum_errors::Diagnostic;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

/// Immutable, versioned envelope. IDs are unique within an outbox and within a
/// consumer's inbox. Route, order and bytes are part of the permanent binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Stable application-assigned idempotency key; preserve through redelivery.
    pub id: String,
    /// Logical destination understood by the transport adapter.
    pub route: String,
    /// Optional ordered stream. Both key and sequence must be present together.
    pub ordering_key: Option<String>,
    /// Positive contiguous sequence for strict inboxes, starting at one.
    pub sequence: Option<u64>,
    /// Application payload schema version, greater than zero.
    pub version: u32,
    /// Opaque application bytes; not interpreted by the dispatcher.
    pub payload: Vec<u8>,
}
/// Validate a durable name (1..=256 bytes, no control characters).
pub fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
        return Err(Error::message("invalid messaging name".into()));
    }
    Ok(())
}
impl Message {
    /// Validate before either enqueue or consume, including SQLite integer range.
    pub fn validate(&self) -> Result<(), Error> {
        validate_name(&self.id)?;
        validate_name(&self.route)?;
        if self.version == 0 || self.payload.len() > 16 * 1024 * 1024 {
            return Err(Error::message(
                "invalid version or payload exceeds 16 MiB".into(),
            ));
        }
        match (&self.ordering_key, self.sequence) {
            (None, None) => Ok(()),
            (Some(key), Some(1..=0x7fff_ffff_ffff_ffff)) => validate_name(key),
            _ => Err(Error::message(
                "ordering requires a key and positive signed-64 sequence".into(),
            )),
        }
    }
}

/// Current durable publication state. Delivered means transport accepted it,
/// not that every consumer has applied it. Dead letters remain inspectable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryState {
    /// Ready now or waiting for retry time.
    Pending,
    /// Claimed until a durable lease deadline.
    Leased,
    /// Transport acceptance durably acknowledged locally.
    Delivered,
    /// Budget exhausted or a permanent send failure; operator action required.
    Dead,
}
/// Inspectable durable outbox record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    /// Full immutable envelope, retained even for terminal records.
    pub message: Message,
    /// Current state.
    pub state: DeliveryState,
    /// Total claims, including claims abandoned by a crash.
    pub attempts: u32,
    /// Next retry time or current lease deadline, Unix milliseconds.
    pub available_at: i64,
    /// Most recent human-facing failure or expiry diagnostic.
    pub last_error: Option<String>,
    /// Stable metadata for the last failed send/expired budget; no source text.
    pub failure: Option<pactum_errors::FailureMetadata>,
}
/// Durable fenced claim. Only the current attempt may acknowledge or fail it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    /// Immutable envelope to send.
    pub message: Message,
    /// Monotonic per-message generation; never reset on dead-letter replay.
    pub token: u32,
}
/// Bounded retry policy. Persist it with each record so worker settings cannot
/// silently change the retry budget for previously recorded messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Maximum claims per replay cycle, including interrupted sends.
    pub max_attempts: u32,
    /// Initial retry delay in milliseconds; doubles with each failed claim.
    pub backoff_ms: i64,
    /// Saturating upper bound on retry delay.
    pub max_backoff_ms: i64,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            backoff_ms: 100,
            max_backoff_ms: 60_000,
        }
    }
}
impl RetryPolicy {
    /// Reject unusable policies before work is recorded.
    pub fn validate(self) -> Result<(), Error> {
        if self.max_attempts == 0 || self.backoff_ms < 0 || self.max_backoff_ms < self.backoff_ms {
            return Err(Error::message("invalid messaging retry policy".into()));
        }
        Ok(())
    }
    /// Saturating delay after the given one-based attempt within a replay cycle.
    pub fn delay(self, attempt: u32) -> i64 {
        self.backoff_ms
            .saturating_mul(
                1_i64
                    .checked_shl(attempt.saturating_sub(1))
                    .filter(|factor| *factor > 0)
                    .unwrap_or(i64::MAX),
            )
            .min(self.max_backoff_ms)
    }
}

/// Outbox adapter contract. Enqueue must share a native transaction with domain
/// mutation; generic dispatch alone cannot create that atomicity. Claims must
/// atomically increment a durable fencing token and account for expired leases.
/// Expired final attempts become retained dead letters, never disappear.
pub trait OutboxStore {
    /// Claim at most `limit` due records, using a positive lease duration.
    /// Time is nonnegative Unix milliseconds from a reasonably synchronized clock.
    fn claim(&mut self, now: i64, lease_ms: i64, limit: usize) -> Result<Vec<Claim>, Error>;
    /// Persist transport acceptance, or reject an obsolete token.
    fn acknowledge(&mut self, claim: &Claim) -> Result<(), Error>;
    /// Persist a failed/uncertain send, using the record's bounded retry policy.
    fn fail(&mut self, claim: &Claim, now: i64, failure: &SendError) -> Result<(), Error>;
    /// Inspect a retained record by immutable identity.
    fn delivery(&self, id: &str) -> Result<Option<Delivery>, Error>;
    /// Explicit operator replay of a dead letter, preserving identity and token.
    fn replay(&mut self, id: &str, now: i64) -> Result<(), Error>;
}

/// Transport failure classification; uncertain acceptance must remain retryable.
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
#[snafu(display("{source}"))]
pub struct SendError {
    source: pactum_errors::Diagnostic,
}
impl SendError {
    /// Definitive rejection before acceptance, safe to dead-letter.
    pub fn rejected(detail: String) -> Self {
        Self {
            source: pactum_errors::Diagnostic::message(pactum_errors::ErrorCode::Rejected, detail),
        }
    }
    /// Acceptance is unknown; only the same immutable message may be retried.
    pub fn unknown(detail: String) -> Self {
        Self {
            source: pactum_errors::Diagnostic::message(
                pactum_errors::ErrorCode::TransportUnknown,
                detail,
            ),
        }
    }
    /// Preserve a typed transport source. A failed send never proves rejection.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            source: pactum_errors::Diagnostic::caused(
                pactum_errors::ErrorCode::TransportUnknown,
                source,
            ),
        }
    }
    /// Preserve an elapsed send deadline; transport acceptance is still unknown.
    pub fn timeout(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            source: pactum_errors::Diagnostic::caused(pactum_errors::ErrorCode::Timeout, source),
        }
    }
    /// Only a definitive rejection may be treated as permanent non-acceptance.
    pub fn permanent(&self) -> bool {
        self.source.code() == pactum_errors::ErrorCode::Rejected
    }
    /// Stable code.
    pub fn code(&self) -> pactum_errors::ErrorCode {
        self.source.code()
    }
    /// Structured local diagnostic, including original typed source.
    pub fn diagnostic(&self) -> &pactum_errors::Diagnostic {
        &self.source
    }
    /// Attach the immutable message identity and safe dispatch phase.
    pub fn with_context(self, context: pactum_errors::ErrorContext) -> Self {
        Self {
            source: self.source.with_context(context),
        }
    }
}
/// Broker-neutral sender. Success must mean durable acceptance by the adapter's
/// destination. Forward stable IDs unchanged. Cancellation/timeouts can happen
/// after acceptance; retries may duplicate, including after a lease expires.
#[async_trait]
pub trait Transport: Send {
    /// Send an envelope; do not claim exactly-once external effects.
    async fn send(&mut self, message: &Message) -> Result<(), SendError>;
}
/// One bounded dispatcher pass. Run again at startup and while work is due.
/// Dropping this future leaves claims recoverable after their leases expire.
pub struct Dispatcher {
    /// Maximum records claimed per pass (1..=10,000).
    pub batch_size: usize,
    /// Lease length, in milliseconds. Long sends may overlap another worker.
    pub lease_ms: i64,
    /// Per-send timeout; acceptance on timeout is unknown and retryable.
    pub send_timeout: Duration,
}
impl Default for Dispatcher {
    fn default() -> Self {
        Self {
            batch_size: 100,
            lease_ms: 30_000,
            send_timeout: Duration::from_secs(5),
        }
    }
}
/// Counts for this pass; durable per-message details live in the store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DispatchReport {
    /// Successful transport acceptances acknowledged in storage.
    pub delivered: usize,
    /// Failed or uncertain send attempts durably retained for retry/dead letter.
    pub failed: usize,
}
impl Dispatcher {
    /// Storage errors stop the pass, leaving remaining claims for lease recovery.
    /// `clock` is sampled before claiming and after sends for retry scheduling.
    pub async fn dispatch<S: OutboxStore, T: Transport>(
        &self,
        store: &mut S,
        transport: &mut T,
        mut clock: impl FnMut() -> i64,
    ) -> Result<DispatchReport, Error> {
        if self.batch_size == 0
            || self.batch_size > 10_000
            || self.lease_ms <= 0
            || self.send_timeout.is_zero()
        {
            return Err(Error::message("invalid dispatcher bounds".into()));
        }
        let claims = store.claim(clock(), self.lease_ms, self.batch_size)?;
        let mut report = DispatchReport::default();
        for claim in claims {
            let result = tokio::time::timeout(self.send_timeout, transport.send(&claim.message))
                .await
                .unwrap_or_else(|error| Err(SendError::timeout(error)));
            match result {
                Ok(()) => {
                    store.acknowledge(&claim)?;
                    report.delivered += 1;
                }
                Err(failure) => {
                    let failure = failure.with_context(pactum_errors::ErrorContext {
                        transaction: Some(claim.message.id.clone()),
                        phase: Some("outbox_send".into()),
                        ..Default::default()
                    });
                    failure.diagnostic().trace();
                    store.fail(&claim, clock(), &failure)?;
                    report.failed += 1;
                }
            }
        }
        Ok(report)
    }
}

/// Immutable consumer ordering policy. Strict ordering blocks at gaps and poison
/// records; unordered consumers suppress duplicates without imposing stream order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderingPolicy {
    /// Apply unique messages in arrival order; sequence fields are informational.
    Unordered,
    /// Require a key and exactly watermark+1, initially one, for each stream.
    Strict,
}
/// Durable consume result. Only Applied/Duplicate permit a successful broker ack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumeResult {
    /// Effect and receipt committed together.
    Applied,
    /// Same envelope already committed; handler was not invoked.
    Duplicate,
    /// Earlier sequence missing or blocked; retry without counting as poison.
    OutOfOrder,
    /// Handler rolled back; retry until its durable budget is exhausted.
    Retry(String),
    /// Retained poison message; explicitly replay or route to an operator queue.
    Dead(String),
}
/// Generic native-transaction inbox contract. Implementations must serialize
/// competing duplicates, permanently bind consumer+ID to the envelope, and commit
/// receipt, ordered watermark and handler effects atomically. On handler error,
/// roll back its effects before recording a bounded failure. Handler must touch
/// only the supplied transaction; external effects cannot be deduplicated here.
pub trait InboxStore {
    /// Backend's borrowed native transaction handle.
    type Transaction<'a>
    where
        Self: 'a;
    /// Consume under the consumer's immutable policy and poison-attempt budget.
    fn consume<F>(
        &mut self,
        consumer: &str,
        message: &Message,
        ordering: OrderingPolicy,
        max_attempts: u32,
        handler: F,
    ) -> Result<ConsumeResult, Error>
    where
        F: for<'a> FnOnce(&Self::Transaction<'a>) -> Result<(), Error>;
    /// Explicit operator retry of a retained poison record, without changing its
    /// identity, ordering policy or successful receipt history.
    fn replay_inbox(&mut self, consumer: &str, id: &str) -> Result<(), Error>;
}

/// Deterministic fault injected on the next fake transport send.
#[derive(Clone, Debug)]
pub enum Fault {
    /// Reject without accepting; models a partition/unavailable broker.
    Partition,
    /// Lose the attempt before acceptance and return a retryable error.
    Drop,
    /// Accept but lose the acknowledgement; retry will redeliver.
    LoseAck,
    /// Accept two identical deliveries and acknowledge.
    Duplicate,
    /// Definitive permanent rejection.
    Reject,
}
/// Volatile local fake transport for examples and fault tests, not a durable
/// broker. Success means acceptance into `deliveries` for this process lifetime.
#[derive(Default, Debug)]
pub struct FakeTransport {
    /// Script of faults, consumed once per send; empty means normal delivery.
    pub faults: VecDeque<Fault>,
    /// Accepted envelopes, including injected duplicates.
    pub deliveries: VecDeque<Message>,
}
#[async_trait]
impl Transport for FakeTransport {
    async fn send(&mut self, message: &Message) -> Result<(), SendError> {
        match self.faults.pop_front() {
            Some(Fault::Partition | Fault::Drop) => {
                Err(SendError::unknown("not accepted (partition/drop)".into()))
            }
            Some(Fault::Reject) => Err(SendError::rejected("permanent rejection".into())),
            Some(Fault::LoseAck) => {
                self.deliveries.push_back(message.clone());
                Err(SendError::unknown("accepted; acknowledgement lost".into()))
            }
            fault => {
                self.deliveries.push_back(message.clone());
                if matches!(fault, Some(Fault::Duplicate)) {
                    self.deliveries.push_back(message.clone());
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_saturates_at_large_attempts_without_wrapping_or_turning_negative() {
        let policy = RetryPolicy {
            max_attempts: u32::MAX,
            backoff_ms: 1,
            max_backoff_ms: i64::MAX,
        };
        assert_eq!(policy.delay(1), 1);
        assert_eq!(policy.delay(3), 4);
        assert_eq!(policy.delay(63), 1_i64 << 62);
        assert_eq!(policy.delay(64), i64::MAX);
        assert_eq!(policy.delay(u32::MAX), i64::MAX);
        assert_eq!(
            RetryPolicy {
                backoff_ms: 0,
                ..policy
            }
            .delay(u32::MAX),
            0
        );
        assert_eq!(
            RetryPolicy {
                max_backoff_ms: 3,
                ..policy
            }
            .delay(3),
            3
        );
        assert_eq!(
            RetryPolicy {
                backoff_ms: i64::MAX,
                ..policy
            }
            .delay(2),
            i64::MAX
        );
    }

    #[test]
    fn envelope_names_and_ordering_reject_ambiguous_identity_inputs() {
        let valid = Message {
            id: "event".into(),
            route: "route".into(),
            ordering_key: None,
            sequence: None,
            version: 1,
            payload: vec![],
        };
        assert!(valid.validate().is_ok());
        assert!(validate_name(&"a".repeat(256)).is_ok());
        for invalid in ["", "line\nbreak", &"a".repeat(257)] {
            assert!(validate_name(invalid).is_err());
        }
        for invalid in [
            Message {
                ordering_key: Some("stream".into()),
                ..valid.clone()
            },
            Message {
                sequence: Some(1),
                ..valid.clone()
            },
            Message {
                ordering_key: Some("stream".into()),
                sequence: Some(0),
                ..valid.clone()
            },
            Message {
                ordering_key: Some("stream".into()),
                sequence: Some(i64::MAX as u64 + 1),
                ..valid.clone()
            },
            Message {
                version: 0,
                ..valid.clone()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
        assert!(
            Message {
                ordering_key: Some("stream".into()),
                sequence: Some(i64::MAX as u64),
                ..valid
            }
            .validate()
            .is_ok()
        );
    }
}
