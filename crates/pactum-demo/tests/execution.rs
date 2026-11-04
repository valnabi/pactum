//! Multi-mode restart and fault matrix; child exits skip destructors.
use pactum_core::{Cancellation, Error, execution::*};
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use pactum_sqlite::{SqliteBackend, SqliteJournal, Value};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn id(s: &str) -> OperationId {
    OperationId::new(s).unwrap()
}
fn payload(value: i64) -> Vec<u8> {
    serde_json::to_vec(&vec![Value {
        key: "counter".into(),
        value,
    }])
    .unwrap()
}
fn request(scenario: &str) -> Request {
    match scenario {
        "atomic" => Request::Atomic(AtomicRequest {
            payload: payload(7),
        }),
        "chunked" => Request::Chunked(ChunkedRequest {
            chunks: vec![payload(2), payload(3), payload(5)],
        }),
        "publish" | "conflict" | "invalid" | "cancel" => Request::StagedPublish(PublishRequest {
            target: id("visible"),
            expected_generation: if scenario == "conflict" { 9 } else { 0 },
            payload: if scenario == "invalid" {
                b"invalid".to_vec()
            } else {
                payload(42)
            },
        }),
        _ => panic!("unknown scenario"),
    }
}
fn open(path: &Path, policy: ExecutionPolicy) -> Engine<SqliteJournal, SqliteBackend> {
    Engine::open(
        SqliteJournal::open(path.join("journal.db")).unwrap(),
        SqliteBackend::open(path.join("backend.db"), "test").unwrap(),
        policy,
    )
    .unwrap()
}
#[tokio::test]
async fn atomic_rollback_idempotency_and_immutable_binding() {
    let temp = tempfile::tempdir().unwrap();
    let mut e = open(temp.path(), ExecutionPolicy::default());
    let c = Cancellation::default();
    for _ in 0..2 {
        assert_eq!(
            e.execute(id("a"), request("atomic"), &c)
                .await
                .unwrap()
                .progress,
            Progress::Atomic(AtomicStatus::Committed)
        );
    }
    assert_eq!(e.backend().value("counter").unwrap(), 7);
    assert!(matches!(
        e.execute(id("a"), request("chunked"), &c).await,
        Err(Error::Invalid(_))
    ));
    // First delta succeeds in the savepoint; overflow on the second rolls it all back.
    let work = vec![
        Value {
            key: "other".into(),
            value: 9,
        },
        Value {
            key: "counter".into(),
            value: i64::MAX,
        },
    ];
    let r = Request::Atomic(AtomicRequest {
        payload: serde_json::to_vec(&work).unwrap(),
    });
    assert_eq!(
        e.execute(id("rollback"), r.clone(), &c)
            .await
            .unwrap()
            .progress,
        Progress::Atomic(AtomicStatus::Aborted)
    );
    assert_eq!(e.backend().value("other").unwrap(), 0);
    drop(e);
    let mut e = open(temp.path(), ExecutionPolicy::default());
    assert_eq!(
        e.execute(id("rollback"), r, &c).await.unwrap().progress,
        Progress::Atomic(AtomicStatus::Aborted)
    );
    assert_eq!(e.backend().value("counter").unwrap(), 7);
    let cancelled = Cancellation::default();
    cancelled.cancel();
    assert_eq!(
        e.execute(id("cancelled"), request("atomic"), &cancelled)
            .await
            .unwrap()
            .progress,
        Progress::Atomic(AtomicStatus::Aborted)
    );
    e.recover(&c).await.unwrap();
    assert_eq!(e.backend().value("counter").unwrap(), 7);
}
#[tokio::test]
async fn partial_chunks_pause_restart_and_resume_with_stable_total() {
    let temp = tempfile::tempdir().unwrap();
    let policy = ExecutionPolicy {
        max_chunks: 1,
        ..ExecutionPolicy::default()
    };
    let mut e = open(temp.path(), policy);
    let c = Cancellation::default();
    let result = e.execute(id("job"), request("chunked"), &c).await.unwrap();
    assert!(matches!(
        result.progress,
        Progress::Chunked(ChunkStatus {
            completed: 1,
            uncertain: false,
            ..
        })
    ));
    e.cancel(&id("job")).await.unwrap();
    drop(e);
    let mut e = open(temp.path(), policy);
    assert!(matches!(
        e.recover(&c).await.unwrap()[0].progress,
        Progress::Chunked(ChunkStatus {
            completed: 1,
            paused: true,
            ..
        })
    ));
    assert_eq!(e.backend().value("counter").unwrap(), 2);
    e.execute(id("job"), request("chunked"), &c).await.unwrap();
    let result = e.recover(&c).await.unwrap().remove(0);
    assert!(matches!(result.progress,Progress::Chunked(ref s) if s.is_complete()));
    assert_eq!(e.backend().value("counter").unwrap(), 10);
    let bad = Request::Chunked(ChunkedRequest {
        chunks: vec![payload(1), b"bad".to_vec(), payload(100)],
    });
    e.execute(id("rejected"), bad, &c).await.unwrap();
    let statuses = e.recover(&c).await.unwrap();
    assert!(matches!(
        statuses[1].progress,
        Progress::Chunked(ChunkStatus {
            completed: 1,
            rejected: true,
            uncertain: false,
            ..
        })
    ));
    e.recover(&c).await.unwrap();
    assert_eq!(e.backend().value("counter").unwrap(), 11);
}
#[tokio::test]
async fn staged_visibility_fencing_receipts_and_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let mut e = open(temp.path(), ExecutionPolicy::default());
    let c = Cancellation::default();
    let cancelled = Cancellation::default();
    cancelled.cancel();
    e.execute(id("cancel"), request("cancel"), &cancelled)
        .await
        .unwrap();
    assert!(e.backend().visible(&id("visible")).unwrap().is_none());
    assert_eq!(
        e.cancel(&id("cancel")).await.unwrap().unwrap().progress,
        Progress::StagedPublish(PublishStatus::Aborted)
    );
    assert!(!e.backend().has_artifact(&id("cancel")).unwrap());
    assert_eq!(
        e.execute(id("invalid"), request("invalid"), &c)
            .await
            .unwrap()
            .progress,
        Progress::StagedPublish(PublishStatus::Aborted)
    );
    assert_eq!(
        e.execute(id("first"), request("publish"), &c)
            .await
            .unwrap()
            .progress,
        Progress::StagedPublish(PublishStatus::Published)
    );
    let next = Request::StagedPublish(PublishRequest {
        target: id("visible"),
        expected_generation: 1,
        payload: payload(99),
    });
    e.execute(id("second"), next, &c).await.unwrap();
    assert_eq!(
        e.execute(id("stale"), request("publish"), &c)
            .await
            .unwrap()
            .progress,
        Progress::StagedPublish(PublishStatus::Conflict)
    );
    assert!(!e.backend().has_artifact(&id("stale")).unwrap());
    let visible = e.backend().visible(&id("visible")).unwrap().unwrap();
    assert_eq!(visible.generation, 2);
    assert_eq!(visible.version, id("second"));
    assert_eq!(visible.values[0].value, 99);
    drop(e);
    let mut backend = SqliteBackend::open(temp.path().join("backend.db"), "test").unwrap();
    let Request::StagedPublish(first) = request("publish") else {
        unreachable!()
    };
    // A superseded publisher retains success; cleanup cannot remove a once-published version.
    assert_eq!(
        backend.publish(&id("first"), &first).unwrap(),
        PublishResult::Published
    );
    assert!(backend.cleanup(&id("first"), &first).is_err());
    assert!(
        backend
            .unit(&id("first"), &request("atomic"), None)
            .is_err()
    );
    assert_eq!(backend.visible(&id("visible")).unwrap().unwrap(), visible);
}
#[tokio::test]
async fn identity_limits_and_separate_journal_schemas_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let mut e = open(
        temp.path(),
        ExecutionPolicy {
            max_unit_bytes: 1,
            ..ExecutionPolicy::default()
        },
    );
    assert!(
        e.execute(id("a"), request("atomic"), &Cancellation::default())
            .await
            .is_err()
    );
    assert!(e.status(&id("a")).unwrap().is_none());
    drop(e);
    assert!(SqliteBackend::open(temp.path().join("backend.db"), "changed").is_err());
    assert!(
        Engine::open(
            SqliteJournal::open(temp.path().join("journal.db")).unwrap(),
            SqliteBackend::open(temp.path().join("replacement.db"), "test").unwrap(),
            ExecutionPolicy::default()
        )
        .is_err()
    );
    assert!(
        pactum_core::Coordinator::open(
            SqliteJournal::open(temp.path().join("journal.db")).unwrap(),
            vec![],
            pactum_core::RetryPolicy::default()
        )
        .is_err()
    );
    for (i, version, payload) in [
        (0, 3, b"{}".as_slice()),
        (1, 2, b"{truncated"),
        (2, 2, b"{\"event\":\"Unknown\"}"),
    ] {
        let p = temp.path().join(format!("corrupt{i}.db"));
        let mut j = SqliteJournal::open(&p).unwrap();
        j.append(version, payload).unwrap();
        j.flush().unwrap();
        assert!(
            Engine::open(
                j,
                SqliteBackend::open(temp.path().join("backend.db"), "test").unwrap(),
                ExecutionPolicy::default()
            )
            .is_err()
        );
    }
}

#[derive(Clone)]
struct Fault {
    count: Arc<AtomicUsize>,
    target: usize,
    kill: bool,
}
impl Fault {
    fn tick(&self) -> Result<(), BackendError> {
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.target {
            if self.kill {
                std::process::exit(86);
            }
            return Err(BackendError("injected lost acknowledgement".into()));
        }
        Ok(())
    }
}
struct FaultJournal {
    inner: SqliteJournal,
    fault: Fault,
}
impl Journal for FaultJournal {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        self.fault.tick().map_err(|e| JournalError(e.0))?;
        let s = self.inner.append(version, payload)?;
        self.fault.tick().map_err(|e| JournalError(e.0))?;
        Ok(s)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.fault.tick().map_err(|e| JournalError(e.0))?;
        let s = self.inner.flush()?;
        self.fault.tick().map_err(|e| JournalError(e.0))?;
        Ok(s)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.inner.replay()
    }
}
struct FaultBackend {
    inner: SqliteBackend,
    fault: Fault,
}
impl Backend for FaultBackend {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn unit(
        &mut self,
        id: &OperationId,
        r: &Request,
        chunk: Option<usize>,
    ) -> Result<UnitResult, BackendError> {
        self.fault.tick()?;
        let result = self.inner.unit(id, r, chunk)?;
        self.fault.tick()?;
        Ok(result)
    }
    fn stage(&mut self, id: &OperationId, r: &PublishRequest) -> Result<(), BackendError> {
        self.fault.tick()?;
        self.inner.stage(id, r)?;
        self.fault.tick()
    }
    fn validate(&mut self, id: &OperationId, r: &PublishRequest) -> Result<bool, BackendError> {
        self.fault.tick()?;
        let result = self.inner.validate(id, r)?;
        self.fault.tick()?;
        Ok(result)
    }
    fn publish(
        &mut self,
        id: &OperationId,
        r: &PublishRequest,
    ) -> Result<PublishResult, BackendError> {
        self.fault.tick()?;
        let result = self.inner.publish(id, r)?;
        self.fault.tick()?;
        Ok(result)
    }
    fn cleanup(&mut self, id: &OperationId, r: &PublishRequest) -> Result<(), BackendError> {
        self.fault.tick()?;
        self.inner.cleanup(id, r)?;
        self.fault.tick()
    }
}
async fn attempt(path: &Path, scenario: &str, target: usize, kill: bool) -> usize {
    let fault = Fault {
        count: Arc::new(AtomicUsize::new(0)),
        target,
        kill,
    };
    let j = FaultJournal {
        inner: SqliteJournal::open(path.join("journal.db")).unwrap(),
        fault: fault.clone(),
    };
    let b = FaultBackend {
        inner: SqliteBackend::open(path.join("backend.db"), "test").unwrap(),
        fault: fault.clone(),
    };
    if let Ok(mut e) = Engine::open(j, b, ExecutionPolicy::default()) {
        let c = Cancellation::default();
        if scenario == "cancel" {
            c.cancel();
        }
        let result = e.execute(id("work"), request(scenario), &c).await;
        if matches!(result, Err(Error::InDoubt(_))) {
            assert!(e.status(&id("work")).is_err());
            assert!(e.recover(&Cancellation::default()).await.is_err());
        }
        if let Ok(s) = result {
            match s.progress {
                Progress::Atomic(AtomicStatus::Committed) => {
                    assert_eq!(e.backend().inner.value("counter").unwrap(), 7)
                }
                Progress::Chunked(ref s) if s.is_complete() => {
                    assert_eq!(e.backend().inner.value("counter").unwrap(), 10)
                }
                Progress::StagedPublish(PublishStatus::Published) => {
                    assert!(e.backend().inner.visible(&id("visible")).unwrap().is_some())
                }
                _ => {}
            }
        }
        if scenario == "cancel" {
            let _ = e.cancel(&id("work")).await;
        }
    }
    fault.count.load(Ordering::SeqCst)
}
async fn verify(path: &Path, scenario: &str) {
    let mut e = open(path, ExecutionPolicy::default());
    let c = Cancellation::default();
    if scenario == "cancel" {
        // Repeat cancellation intent if death preceded the durable intent record.
        let paused = Cancellation::default();
        paused.cancel();
        e.execute(id("work"), request(scenario), &paused)
            .await
            .unwrap();
        e.cancel(&id("work")).await.unwrap();
    } else {
        e.recover(&c).await.unwrap();
        e.execute(id("work"), request(scenario), &c).await.unwrap();
    }
    let s = e.status(&id("work")).unwrap().unwrap();
    assert!(s.progress.is_terminal(), "{scenario}: {s:?}");
    let expected = match scenario {
        "atomic" => 7,
        "chunked" => 10,
        _ => 0,
    };
    assert_eq!(e.backend().value("counter").unwrap(), expected);
    let visible = e.backend().visible(&id("visible")).unwrap();
    if scenario == "publish" {
        assert_eq!(
            s.progress,
            Progress::StagedPublish(PublishStatus::Published)
        );
        assert_eq!(visible.unwrap().values[0].value, 42);
    } else if matches!(scenario, "conflict" | "invalid" | "cancel") {
        assert!(visible.is_none());
        assert!(!e.backend().has_artifact(&id("work")).unwrap());
    }
    assert_eq!(e.recover(&c).await.unwrap()[0].progress, s.progress);
}
#[tokio::test]
async fn uncertain_writes_and_lost_backend_responses_at_every_boundary() {
    for scenario in [
        "atomic", "chunked", "publish", "conflict", "invalid", "cancel",
    ] {
        let baseline = tempfile::tempdir().unwrap();
        let count = attempt(baseline.path(), scenario, 0, false).await;
        for target in 1..=count {
            let temp = tempfile::tempdir().unwrap();
            attempt(temp.path(), scenario, target, false).await;
            verify(temp.path(), scenario).await;
        }
    }
}
#[tokio::test]
async fn crash_child() {
    let Ok(path) = std::env::var("PACTUM_MULTI_PATH") else {
        return;
    };
    let scenario = std::env::var("PACTUM_MULTI_SCENARIO").unwrap();
    let target = std::env::var("PACTUM_MULTI_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let count = attempt(Path::new(&path), &scenario, target, true).await;
    println!("BOUNDARIES:{count}");
}
fn child(path: &Path, scenario: &str, target: usize) -> usize {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("PACTUM_MULTI_PATH", path)
        .env("PACTUM_MULTI_SCENARIO", scenario)
        .env("PACTUM_MULTI_TARGET", target.to_string())
        .output()
        .unwrap();
    if target != 0 {
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return 0;
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|l| l.strip_prefix("BOUNDARIES:"))
        .unwrap()
        .parse()
        .unwrap()
}
#[tokio::test]
async fn process_death_restart_at_every_new_mode_boundary() {
    for scenario in [
        "atomic", "chunked", "publish", "conflict", "invalid", "cancel",
    ] {
        let baseline = tempfile::tempdir().unwrap();
        let count = child(baseline.path(), scenario, 0);
        assert!(count >= 14);
        for target in 1..=count {
            let temp = tempfile::tempdir().unwrap();
            child(temp.path(), scenario, target);
            verify(temp.path(), scenario).await;
        }
    }
}

#[tokio::test]
async fn original_coordinator_uses_common_lifecycle_without_participant_changes() {
    use pactum_core::{
        Coordinator, Enlistment, Outcome, Participant, ParticipantId, RetryPolicy, TransactionId,
    };
    use pactum_demo::DurableCounter;
    let temp = tempfile::tempdir().unwrap();
    let counter = Arc::new(
        DurableCounter::open(
            temp.path().join("counter.db"),
            ParticipantId::new("counter").unwrap(),
        )
        .unwrap(),
    );
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("2pc.db")).unwrap(),
        vec![counter.clone()],
        RetryPolicy::default(),
    )
    .unwrap();
    let tx = TransactionId::new("common").unwrap();
    let status = Execution::execute(
        &mut coordinator,
        tx.clone(),
        TwoPhaseRequest {
            enlistments: vec![Enlistment {
                participant: counter.id(),
                payload: b"4".to_vec(),
            }],
        },
        &Cancellation::default(),
    )
    .await
    .unwrap();
    assert_eq!(status.outcome, Outcome::Committed);
    assert_eq!(
        Execution::status(&coordinator, &tx).unwrap().unwrap(),
        status
    );
    assert_eq!(
        Execution::cancel(&mut coordinator, &tx)
            .await
            .unwrap()
            .unwrap()
            .outcome,
        Outcome::Committed
    );
    assert_eq!(
        Execution::recover(&mut coordinator, &Cancellation::default())
            .await
            .unwrap()[0]
            .outcome,
        Outcome::Committed
    );
    assert_eq!(counter.value().await.unwrap(), 4);
}

#[tokio::test]
async fn uncertain_backend_commit_is_never_reported_as_success_or_undone_by_cancel() {
    for (scenario, target) in [("atomic", 10), ("chunked", 14), ("publish", 26)] {
        let temp = tempfile::tempdir().unwrap();
        let fault = Fault {
            count: Arc::new(AtomicUsize::new(0)),
            target,
            kill: false,
        };
        let j = FaultJournal {
            inner: SqliteJournal::open(temp.path().join("journal.db")).unwrap(),
            fault: fault.clone(),
        };
        let b = FaultBackend {
            inner: SqliteBackend::open(temp.path().join("backend.db"), "test").unwrap(),
            fault,
        };
        let mut e = Engine::open(j, b, ExecutionPolicy::default()).unwrap();
        let s = e
            .execute(id("work"), request(scenario), &Cancellation::default())
            .await
            .unwrap();
        assert!(!s.progress.is_terminal());
        assert!(s.last_error.is_some());
        match s.progress {
            Progress::Atomic(AtomicStatus::Indeterminate) => {
                assert_eq!(e.backend().inner.value("counter").unwrap(), 7)
            }
            Progress::Chunked(s) => {
                assert_eq!(s.completed, 0);
                assert_eq!(s.outcome(), ChunkOutcome::Indeterminate);
                assert_eq!(e.backend().inner.value("counter").unwrap(), 2);
            }
            Progress::StagedPublish(PublishStatus::Publishing) => {
                assert!(e.backend().inner.visible(&id("visible")).unwrap().is_some())
            }
            _ => panic!("wrong uncertain status"),
        }
        e.cancel(&id("work")).await.unwrap();
        if scenario == "publish" {
            assert!(e.backend().inner.has_artifact(&id("work")).unwrap());
        }
        drop(e);
        // Explicit execute resumes a paused batch; all receipts suppress reapplication.
        verify(temp.path(), scenario).await;
    }
}
