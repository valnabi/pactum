//! Stable codes, phase-aware guidance, source/redaction and durable metadata contracts.
use pactum_errors::*;
use std::{
    error::Error,
    sync::{Arc, Mutex},
};

fn context() -> RetryContext {
    RetryContext {
        authority_valid: true,
        idempotent: true,
        ..Default::default()
    }
}
#[test]
fn codes_and_serialization_do_not_depend_on_display() {
    let codes = [
        (ErrorCode::InvalidInput, "invalid_input"),
        (ErrorCode::CorruptHistory, "corrupt_history"),
        (ErrorCode::DurabilityUnknown, "durability_unknown"),
        (ErrorCode::Storage, "storage"),
        (ErrorCode::Unavailable, "unavailable"),
        (ErrorCode::Timeout, "timeout"),
        (ErrorCode::Cancelled, "cancelled"),
        (ErrorCode::StaleOwner, "stale_owner"),
        (ErrorCode::OwnershipBusy, "ownership_busy"),
        (ErrorCode::ProtocolMismatch, "protocol_mismatch"),
        (ErrorCode::AdapterUnknown, "adapter_unknown"),
        (ErrorCode::TransportUnknown, "transport_unknown"),
        (ErrorCode::Rejected, "rejected"),
        (ErrorCode::OperatorRequired, "operator_required"),
    ];
    for (code, stable) in codes {
        assert_eq!(code.as_str(), stable);
        assert_eq!(serde_json::to_value(code).unwrap(), stable);
        let a = Diagnostic::message(code, "first wording".into());
        let b = Diagnostic::message(code, "changed wording".into());
        assert_eq!(
            a.retry_disposition(context()),
            b.retry_disposition(context())
        );
        assert_eq!(a.metadata(context()), b.metadata(context()));
        assert!(a.metadata(context()).is_valid());
    }
}
#[test]
fn phase_decision_authority_and_idempotency_control_safe_actions() {
    let c = context();
    let invalid = Diagnostic::message(ErrorCode::InvalidInput, "invalid".into());
    assert_eq!(invalid.retry_disposition(c), RetryDisposition::Permanent);
    let prepare = RetryContext {
        phase: ErrorPhase::Prepare,
        decision: DecisionState::Undecided,
        ..c
    };
    assert_eq!(
        invalid.decision_advice(prepare),
        DecisionAdvice::AbortBeforeDecision
    );
    for code in [
        ErrorCode::Timeout,
        ErrorCode::Storage,
        ErrorCode::TransportUnknown,
    ] {
        let failure = Diagnostic::message(code, "unknown completion".into());
        assert_eq!(
            failure.retry_disposition(c),
            RetryDisposition::ReconcileBeforeRetry
        );
        for (decision, advice) in [
            (DecisionState::Commit, DecisionAdvice::RetainCommit),
            (DecisionState::Abort, DecisionAdvice::RetainAbort),
        ] {
            assert_eq!(
                failure.decision_advice(RetryContext {
                    decision,
                    ..prepare
                }),
                advice
            );
        }
    }
    let available = Diagnostic::message(ErrorCode::Unavailable, "offline".into());
    assert_eq!(available.retry_disposition(c), RetryDisposition::Retry);
    assert_eq!(
        available.retry_disposition(RetryContext {
            idempotent: false,
            ..c
        }),
        RetryDisposition::ReconcileBeforeRetry
    );
    assert_eq!(
        available.retry_disposition(RetryContext {
            journal_write: true,
            ..c
        }),
        RetryDisposition::ReconcileBeforeRetry
    );
    assert_eq!(
        available.retry_disposition(RetryContext {
            phase: ErrorPhase::JournalWrite,
            ..c
        }),
        RetryDisposition::ReconcileBeforeRetry
    );
    assert_eq!(
        available.retry_disposition(RetryContext {
            authority_valid: false,
            ..c
        }),
        RetryDisposition::OperatorIntervention
    );
    assert_eq!(
        Diagnostic::message(ErrorCode::Storage, "read".into()).retry_disposition(RetryContext {
            phase: ErrorPhase::Read,
            ..c
        }),
        RetryDisposition::Retry
    );
    for code in [
        ErrorCode::StaleOwner,
        ErrorCode::CorruptHistory,
        ErrorCode::ProtocolMismatch,
        ErrorCode::DurabilityUnknown,
    ] {
        let failure = Diagnostic::message(code, "stop".into());
        assert_eq!(failure.decision_advice(prepare), DecisionAdvice::Reconcile);
        assert_eq!(
            failure.decision_advice(RetryContext {
                decision: DecisionState::Commit,
                ..prepare
            }),
            DecisionAdvice::RetainCommit
        );
    }
    assert_eq!(
        invalid.retry_disposition(RetryContext {
            decision: DecisionState::Commit,
            ..c
        }),
        RetryDisposition::OperatorIntervention
    );
}
#[test]
fn sources_are_inspectable_but_not_formatted_or_serialized() {
    let failure = Diagnostic::caused(ErrorCode::Storage, std::io::Error::other("password=SECRET"))
        .with_context(ErrorContext {
            transaction: Some("tx-safe".into()),
            epoch: Some(7),
            ..Default::default()
        });
    assert!(!format!("{failure:?} {failure}").contains("SECRET"));
    let mut cause = failure.source();
    let mut original = false;
    while let Some(error) = cause {
        original |= error.downcast_ref::<std::io::Error>().is_some();
        cause = error.source();
    }
    assert!(original);
    let metadata = failure.metadata(context());
    let bytes = serde_json::to_vec(&metadata).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("SECRET"));
    assert_eq!(
        serde_json::from_slice::<FailureMetadata>(&bytes).unwrap(),
        metadata
    );
    let mut unsafe_metadata = metadata;
    unsafe_metadata.code = ErrorCode::StaleOwner;
    unsafe_metadata.disposition = RetryDisposition::Retry;
    assert!(!unsafe_metadata.is_valid());
}
#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<String>>>);
impl tracing::field::Visit for Capture {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .lock()
            .unwrap()
            .push(format!("{}={value:?}", field.name()));
    }
}
impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut self.clone());
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
#[test]
fn tracing_emits_codes_and_identifiers_without_source_text() {
    let capture = Capture(Arc::default());
    let failure = Diagnostic::caused(
        ErrorCode::TransportUnknown,
        std::io::Error::other("Bearer SECRET"),
    )
    .with_context(ErrorContext {
        transaction: Some("tx-safe".into()),
        participant: Some("resource-safe".into()),
        epoch: Some(9),
        ..Default::default()
    });
    tracing::subscriber::with_default(capture.clone(), || failure.trace());
    let fields = capture.0.lock().unwrap().join(" ");
    assert!(
        fields.contains("transport_unknown")
            && fields.contains("tx-safe")
            && fields.contains("resource-safe")
    );
    assert!(fields.contains("fencing_epoch=9"));
    assert!(!fields.contains("SECRET") && !fields.contains("Bearer"));
}
