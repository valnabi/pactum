//! Error classification never substitutes for a durable transaction outcome.
use async_trait::async_trait;
use pactum_core::{
    Cancellation, Coordinator, Decision, Enlistment, ErrorCode, Participant, ParticipantError,
    ParticipantId, ParticipantStatus, RetryContext, RetryDisposition, RetryPolicy, TransactionId,
    TransactionOutcome, Vote,
};
use pactum_demo::DurableCounter;
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use pactum_sqlite::SqliteJournal;
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn id() -> TransactionId {
    TransactionId::new("error-contract-tx").unwrap()
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        attempts: 1,
        timeout: Duration::from_secs(1),
        backoff: Duration::ZERO,
    }
}
fn enlist(resource: &dyn Participant) -> Vec<Enlistment> {
    vec![Enlistment {
        participant: resource.id(),
        payload: b"7".to_vec(),
    }]
}
fn has_source<T: Error + 'static>(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<T>() {
            return true;
        }
        current = error.source();
    }
    false
}
struct LostDecisionAck {
    journal: SqliteJournal,
    decision: bool,
}
impl Journal for LostDecisionAck {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        let record: serde_json::Value = serde_json::from_slice(payload).unwrap();
        self.decision = record.get("kind").and_then(serde_json::Value::as_str) == Some("Decided");
        self.journal.append(version, payload)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        let durable = self.journal.flush()?;
        if self.decision {
            return Err(JournalError::from_source(std::io::Error::other(
                "credential=SECRET",
            )));
        }
        Ok(durable)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.journal.replay()
    }
}
#[tokio::test]
async fn lost_decision_barrier_keeps_code_source_context_and_recovers_commit() {
    let dir = tempfile::tempdir().unwrap();
    let journal_path = dir.path().join("journal.db");
    let resource = Arc::new(
        DurableCounter::open(
            dir.path().join("resource.db"),
            ParticipantId::new("counter").unwrap(),
        )
        .unwrap(),
    );
    let mut coordinator = Coordinator::open(
        LostDecisionAck {
            journal: SqliteJournal::open(&journal_path).unwrap(),
            decision: false,
        },
        vec![resource.clone()],
        policy(),
    )
    .unwrap();
    let failure = coordinator
        .execute(id(), enlist(&*resource), &Cancellation::default())
        .await
        .unwrap_err();
    assert_eq!(failure.code(), ErrorCode::DurabilityUnknown);
    assert_eq!(
        failure.context().transaction.as_deref(),
        Some(id().as_str())
    );
    assert_eq!(failure.context().phase.as_deref(), Some("decision"));
    assert_eq!(failure.context().operation.as_deref(), Some("flush"));
    assert!(has_source::<std::io::Error>(&failure));
    assert!(!format!("{failure:?} {failure}").contains("SECRET"));
    assert_eq!(
        failure.retry_disposition(RetryContext {
            authority_valid: true,
            journal_write: true,
            ..Default::default()
        }),
        RetryDisposition::ReconcileBeforeRetry
    );
    assert!(coordinator.status(&id()).is_err());
    assert_eq!(resource.value().await.unwrap(), 0);
    drop(coordinator);
    let mut journal = SqliteJournal::open(&journal_path).unwrap();
    for entry in journal.replay().unwrap() {
        assert!(!String::from_utf8_lossy(&entry.payload).contains("SECRET"));
    }
    let mut reopened = Coordinator::open(journal, vec![resource.clone()], policy()).unwrap();
    let pending = reopened.status(&id()).unwrap().unwrap();
    assert_eq!(
        pending.transaction_outcome(),
        TransactionOutcome::PendingRecovery {
            decision: Decision::Commit
        }
    );
    let result = reopened
        .recover(&Cancellation::default())
        .await
        .unwrap()
        .remove(0);
    assert_eq!(result.transaction_outcome(), TransactionOutcome::Committed);
    assert_eq!(resource.value().await.unwrap(), 7);
    reopened.recover(&Cancellation::default()).await.unwrap();
    assert_eq!(resource.value().await.unwrap(), 7);
}
struct LostResourceAck {
    resource: Arc<DurableCounter>,
    lose: AtomicBool,
}
#[async_trait]
impl Participant for LostResourceAck {
    fn id(&self) -> ParticipantId {
        self.resource.id()
    }
    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError> {
        self.resource.prepare(tx, payload).await
    }
    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError> {
        self.resource.commit(tx, token).await?;
        if self.lose.load(Ordering::SeqCst) {
            Err(ParticipantError::classified(
                ErrorCode::Unavailable,
                std::io::Error::other("token=SECRET"),
            ))
        } else {
            Ok(())
        }
    }
    async fn abort(
        &self,
        tx: &TransactionId,
        token: Option<&[u8]>,
    ) -> Result<(), ParticipantError> {
        self.resource.abort(tx, token).await
    }
    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        self.resource.status(tx).await
    }
}
#[tokio::test]
async fn failed_finalization_retains_commit_and_typed_source_until_reconciled() {
    let dir = tempfile::tempdir().unwrap();
    let resource = Arc::new(LostResourceAck {
        resource: Arc::new(
            DurableCounter::open(
                dir.path().join("resource.db"),
                ParticipantId::new("counter").unwrap(),
            )
            .unwrap(),
        ),
        lose: AtomicBool::new(true),
    });
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(dir.path().join("journal.db")).unwrap(),
        vec![resource.clone()],
        policy(),
    )
    .unwrap();
    let status = coordinator
        .execute(id(), enlist(&*resource), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(
        status.transaction_outcome(),
        TransactionOutcome::PendingRecovery {
            decision: Decision::Commit
        }
    );
    let failure = status.diagnostic.as_ref().unwrap();
    assert_eq!(failure.code(), ErrorCode::Unavailable);
    assert_eq!(failure.context().participant.as_deref(), Some("counter"));
    assert_eq!(failure.context().phase.as_deref(), Some("commit"));
    assert!(has_source::<std::io::Error>(failure));
    assert_eq!(
        failure.retry_disposition(status.retry_context()),
        RetryDisposition::Retry
    );
    assert_eq!(resource.resource.value().await.unwrap(), 7);
    drop(coordinator);
    resource.lose.store(false, Ordering::SeqCst);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(dir.path().join("journal.db")).unwrap(),
        vec![resource.clone()],
        policy(),
    )
    .unwrap();
    let result = coordinator
        .recover(&Cancellation::default())
        .await
        .unwrap()
        .remove(0);
    assert_eq!(result.transaction_outcome(), TransactionOutcome::Committed);
    assert!(result.diagnostic.is_none());
    assert_eq!(resource.resource.value().await.unwrap(), 7);
}
#[test]
fn adapter_sources_are_typed_and_default_formatting_redacts_paths() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing-parent").join("journal.db");
    let failure = SqliteJournal::open(&path).err().unwrap();
    assert_eq!(failure.code(), ErrorCode::Storage);
    assert!(has_source::<std::io::Error>(&failure));
    assert!(!format!("{failure:?} {failure}").contains("missing-parent"));
    let driver = pactum_sqlite::AdapterError::from(rusqlite::Error::InvalidQuery).diagnostic();
    assert!(has_source::<rusqlite::Error>(&driver));
}

#[test]
fn direct_journal_corruption_has_an_operator_code_before_coordinator_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt.db");
    let mut journal = SqliteJournal::open(&path).unwrap();
    journal.append(1, b"immutable record").unwrap();
    journal.flush().unwrap();
    drop(journal);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("UPDATE pactum_records SET checksum=zeroblob(32)", [])
        .unwrap();
    drop(connection);
    let failure = SqliteJournal::open(&path).err().unwrap();
    assert_eq!(failure.code(), ErrorCode::CorruptHistory);
    assert_eq!(
        failure.retry_disposition(RetryContext {
            authority_valid: true,
            idempotent: true,
            ..Default::default()
        }),
        RetryDisposition::OperatorIntervention
    );
}
#[test]
fn direct_poisoned_journal_reports_unknown_durability_after_failed_flush() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("poison.db");
    let mut journal = SqliteJournal::open(&path).unwrap();
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute("DROP TABLE pactum_head", []).unwrap();
    drop(connection);
    journal.append(1, b"staged record").unwrap();
    let storage = journal.flush().unwrap_err();
    assert_eq!(storage.code(), ErrorCode::Storage);
    assert!(has_source::<rusqlite::Error>(&storage));
    let poisoned = journal.append(1, b"must not dispatch").unwrap_err();
    assert_eq!(poisoned.code(), ErrorCode::DurabilityUnknown);
    assert_eq!(
        poisoned.retry_disposition(RetryContext {
            authority_valid: true,
            idempotent: true,
            ..Default::default()
        }),
        RetryDisposition::ReconcileBeforeRetry
    );
}

struct FailedRead {
    code: ErrorCode,
    checkpoint: bool,
}
impl FailedRead {
    fn error(&self) -> JournalError {
        JournalError::classified(self.code, std::io::Error::other("credential=SECRET"))
            .with_context(pactum_errors::ErrorContext {
                epoch: Some(7),
                ..Default::default()
            })
    }
}
impl Journal for FailedRead {
    fn append(&mut self, _: u32, _: &[u8]) -> Result<Sequence, JournalError> {
        panic!("read failure must not dispatch writes")
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        panic!("read failure must not dispatch writes")
    }
    fn checkpoint(&mut self) -> Result<Option<pactum_journal::Checkpoint>, JournalError> {
        if self.checkpoint {
            Err(self.error())
        } else {
            Ok(None)
        }
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        Err(self.error())
    }
}
#[test]
fn journal_read_failures_preserve_storage_fencing_and_corruption_codes() {
    for checkpoint in [false, true] {
        for (code, expected) in [
            (ErrorCode::Storage, RetryDisposition::Retry),
            (
                ErrorCode::StaleOwner,
                RetryDisposition::OperatorIntervention,
            ),
            (
                ErrorCode::CorruptHistory,
                RetryDisposition::OperatorIntervention,
            ),
            (
                ErrorCode::DurabilityUnknown,
                RetryDisposition::ReconcileBeforeRetry,
            ),
        ] {
            let failure = Coordinator::open(FailedRead { code, checkpoint }, vec![], policy())
                .err()
                .unwrap();
            assert_eq!(failure.code(), code);
            assert_eq!(
                failure.context().operation.as_deref(),
                Some(if checkpoint { "checkpoint" } else { "replay" })
            );
            assert_eq!(failure.context().epoch, Some(7));
            assert!(has_source::<std::io::Error>(&failure));
            assert!(!format!("{failure:?} {failure}").contains("SECRET"));
            assert_eq!(
                failure.retry_disposition(RetryContext {
                    phase: pactum_errors::ErrorPhase::Read,
                    authority_valid: true,
                    idempotent: true,
                    ..Default::default()
                }),
                expected
            );
        }
    }
}
struct MalformedHistory;
impl Journal for MalformedHistory {
    fn append(&mut self, _: u32, _: &[u8]) -> Result<Sequence, JournalError> {
        panic!("invalid history must not dispatch writes")
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        panic!("invalid history must not dispatch writes")
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        Ok(vec![Entry {
            sequence: 1,
            version: 2,
            payload: b"{invalid SECRET".to_vec(),
        }])
    }
}
#[test]
fn native_replay_preserves_original_codec_source() {
    let dir = tempfile::tempdir().unwrap();
    let backend =
        pactum_sqlite::SqliteBackend::open(dir.path().join("backend.db"), "codec-contract")
            .unwrap();
    let failure = pactum_core::execution::Engine::open(
        MalformedHistory,
        backend,
        pactum_core::execution::ExecutionPolicy::default(),
    )
    .err()
    .unwrap();
    assert_eq!(failure.code(), ErrorCode::CorruptHistory);
    assert!(has_source::<serde_json::Error>(&failure));
    assert!(!format!("{failure:?} {failure}").contains("SECRET"));
}

struct InvalidPrepareToken {
    resource: Arc<DurableCounter>,
    abort_entered: tokio::sync::Notify,
}
#[async_trait]
impl Participant for InvalidPrepareToken {
    fn id(&self) -> ParticipantId {
        self.resource.id()
    }
    async fn prepare(&self, _: &TransactionId, _: &[u8]) -> Result<Vote, ParticipantError> {
        Ok(Vote::Prepared(Vec::new()))
    }
    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError> {
        self.resource.commit(tx, token).await
    }
    async fn abort(&self, _: &TransactionId, _: Option<&[u8]>) -> Result<(), ParticipantError> {
        self.abort_entered.notify_one();
        std::future::pending().await
    }
    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        self.resource.status(tx).await
    }
}
#[tokio::test]
async fn malformed_prepare_vote_is_a_protocol_failure_and_recovery_retains_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid-token-journal.db");
    let resource = Arc::new(
        DurableCounter::open(
            dir.path().join("invalid-token-resource.db"),
            ParticipantId::new("counter").unwrap(),
        )
        .unwrap(),
    );
    let adapter = Arc::new(InvalidPrepareToken {
        resource: resource.clone(),
        abort_entered: tokio::sync::Notify::new(),
    });
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(&path).unwrap(),
        vec![adapter.clone()],
        policy(),
    )
    .unwrap();
    {
        let cancellation = Cancellation::default();
        let execution = coordinator.execute(id(), enlist(&*adapter), &cancellation);
        tokio::pin!(execution);
        tokio::select! {
            _ = &mut execution => panic!("abort delivery should remain pending"),
            _ = adapter.abort_entered.notified() => {},
        }
    }
    let status = coordinator.status(&id()).unwrap().unwrap();
    assert_eq!(
        status.transaction_outcome(),
        TransactionOutcome::PendingRecovery {
            decision: Decision::Abort
        }
    );
    let failure = status.diagnostic.as_ref().unwrap();
    assert_eq!(failure.code(), ErrorCode::ProtocolMismatch);
    assert_eq!(failure.context().phase.as_deref(), Some("prepare"));
    assert_eq!(failure.context().participant.as_deref(), Some("counter"));
    assert_eq!(
        failure.retry_disposition(status.retry_context()),
        RetryDisposition::OperatorIntervention
    );
    drop(coordinator);
    let mut reopened = Coordinator::open(
        SqliteJournal::open(&path).unwrap(),
        vec![resource.clone()],
        policy(),
    )
    .unwrap();
    assert_eq!(
        reopened
            .recover(&Cancellation::default())
            .await
            .unwrap()
            .remove(0)
            .transaction_outcome(),
        TransactionOutcome::Aborted
    );
    assert_eq!(resource.value().await.unwrap(), 0);
}
