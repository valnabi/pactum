//! Recovery service integration with real durable resources and process death.
use async_trait::async_trait;
use pactum_core::{
    Cancellation, Coordinator, Decision, Enlistment, Error, Outcome, Participant, ParticipantError,
    ParticipantId, ParticipantStatus, RetryPolicy, TransactionId, Vote, execution::*, recovery::*,
};
use pactum_demo::DurableCounter;
use pactum_journal::{Entry, Journal, JournalError};
use pactum_sqlite::{SqliteBackend, SqliteJournal, Value};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn config() -> Config {
    Config {
        poll_interval: Duration::from_millis(1),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        max_per_tick: 1,
        ..Config::default()
    }
}
fn id(key: &str) -> OperationId {
    OperationId::new(key).unwrap()
}
fn payload(n: i64) -> Vec<u8> {
    serde_json::to_vec(&vec![Value {
        key: "counter".into(),
        value: n,
    }])
    .unwrap()
}
fn request(mode: &str) -> Request {
    match mode {
        "atomic" => Request::Atomic(AtomicRequest {
            payload: payload(7),
        }),
        "chunked" => Request::Chunked(ChunkedRequest {
            chunks: vec![payload(3), payload(4)],
        }),
        "publish" => Request::StagedPublish(PublishRequest {
            target: id("visible"),
            expected_generation: 0,
            payload: payload(7),
        }),
        _ => unreachable!(),
    }
}
struct Offline(SqliteBackend);
impl Backend for Offline {
    fn identity(&self) -> String {
        self.0.identity()
    }
    fn unit(
        &mut self,
        _: &OperationId,
        _: &Request,
        _: Option<usize>,
    ) -> Result<UnitResult, BackendError> {
        Err(BackendError::message("offline".into()))
    }
    fn stage(&mut self, _: &OperationId, _: &PublishRequest) -> Result<(), BackendError> {
        Err(BackendError::message("offline".into()))
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
fn open(path: &Path) -> Engine<SqliteJournal, SqliteBackend> {
    Engine::open(
        SqliteJournal::open(path.join("engine.db")).unwrap(),
        SqliteBackend::open(path.join("backend.db"), "recovery-test").unwrap(),
        ExecutionPolicy {
            max_chunks: 1,
            ..ExecutionPolicy::default()
        },
    )
    .unwrap()
}
async fn seed(path: &Path, mode: &str) {
    let mut engine = Engine::open(
        SqliteJournal::open(path.join("engine.db")).unwrap(),
        Offline(SqliteBackend::open(path.join("backend.db"), "recovery-test").unwrap()),
        ExecutionPolicy::default(),
    )
    .unwrap();
    let status = engine
        .execute(id("work"), request(mode), &Cancellation::default())
        .await
        .unwrap();
    assert!(!status.progress.is_terminal());
}
async fn drained<T: RecoveryTarget + 'static>(handle: &Handle<T>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if handle.query().await.unwrap().pending.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
struct Trip {
    inner: SqliteJournal,
    hits: Arc<AtomicUsize>,
    target: usize,
}
impl Trip {
    fn hit(&self) {
        let hit = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
        if hit == self.target {
            std::process::exit(86);
        }
    }
}
impl Journal for Trip {
    fn append(&mut self, v: u32, p: &[u8]) -> Result<u64, JournalError> {
        let r = self.inner.append(v, p)?;
        self.hit();
        Ok(r)
    }
    fn flush(&mut self) -> Result<u64, JournalError> {
        let r = self.inner.flush()?;
        self.hit();
        Ok(r)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.inner.replay()
    }
}
#[tokio::test]
async fn recovery_crash_child() {
    let Ok(path) = std::env::var("PACTUM_RECOVERY_CHILD") else {
        return;
    };
    let path = Path::new(&path);
    let mode = std::env::var("PACTUM_RECOVERY_MODE").unwrap();
    let target = std::env::var("PACTUM_RECOVERY_CRASH")
        .unwrap()
        .parse()
        .unwrap();
    seed(path, &mode).await;
    let hits = Arc::new(AtomicUsize::new(0));
    let journal = Trip {
        inner: SqliteJournal::open(path.join("schedule.db")).unwrap(),
        hits: hits.clone(),
        target,
    };
    let runner = Runner::open(open(path), journal, "engine", config()).unwrap();
    let (handle, join) = runner.start();
    drained(&handle).await;
    handle.stop();
    drop(join.await.unwrap().unwrap());
    drop(handle);
    println!("BOUNDARIES={}", hits.load(Ordering::SeqCst));
}
fn child(path: &Path, mode: &str, target: usize) -> usize {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recovery_crash_child", "--nocapture"])
        .env("PACTUM_RECOVERY_CHILD", path)
        .env("PACTUM_RECOVERY_MODE", mode)
        .env("PACTUM_RECOVERY_CRASH", target.to_string())
        .output()
        .unwrap();
    if target > 0 {
        assert_eq!(
            output.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return target;
    }
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("BOUNDARIES="))
        .unwrap()
        .parse()
        .unwrap()
}
#[tokio::test]
async fn restart_at_every_scheduler_write_boundary_preserves_all_native_modes() {
    for mode in ["atomic", "chunked", "publish"] {
        let baseline = tempfile::tempdir().unwrap();
        let count = child(baseline.path(), mode, 0);
        assert!(count >= 6);
        for boundary in 1..=count {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path();
            child(path, mode, boundary);
            let runner = Runner::open(
                open(path),
                SqliteJournal::open(path.join("schedule.db")).unwrap(),
                "engine",
                config(),
            )
            .unwrap();
            let (handle, join) = runner.start();
            drained(&handle).await;
            {
                let mut engine = handle.target().lock().await;
                assert!(
                    engine
                        .status(&id("work"))
                        .unwrap()
                        .unwrap()
                        .progress
                        .is_terminal()
                );
                // Duplicate application requests must keep the original receipts.
                engine
                    .execute(id("work"), request(mode), &Cancellation::default())
                    .await
                    .unwrap();
                if mode == "publish" {
                    assert_eq!(
                        engine
                            .backend()
                            .visible(&id("visible"))
                            .unwrap()
                            .unwrap()
                            .generation,
                        1
                    );
                } else {
                    assert_eq!(engine.backend().value("counter").unwrap(), 7);
                }
            }
            handle.stop();
            drop(join.await.unwrap().unwrap());
            drop(handle);
        }
    }
}

#[tokio::test]
async fn chunk_pause_resume_survives_owner_restart_and_single_owner_lock() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let mut engine = open(path);
    engine
        .execute(id("work"), request("chunked"), &Cancellation::default())
        .await
        .unwrap();
    engine.cancel(&id("work")).await.unwrap();
    let journal = SqliteJournal::open(path.join("schedule.db")).unwrap();
    assert!(SqliteJournal::open(path.join("schedule.db")).is_err());
    let runner = Runner::open(engine, journal, "engine", config()).unwrap();
    let (handle, join) = runner.start();
    handle.control("work", Control::Pause).await.unwrap();
    handle.stop();
    drop(join.await.unwrap().unwrap());
    drop(handle);
    let runner = Runner::open(
        open(path),
        SqliteJournal::open(path.join("schedule.db")).unwrap(),
        "engine",
        config(),
    )
    .unwrap();
    let (handle, join) = runner.start();
    let snapshot = handle.query().await.unwrap();
    assert_eq!(
        snapshot.pending[0].schedule.as_ref().unwrap().reason,
        Reason::Paused
    );
    assert!(snapshot.pending[0].work.is_paused());
    handle.control("work", Control::Resume).await.unwrap();
    drained(&handle).await;
    assert_eq!(
        handle
            .target()
            .lock()
            .await
            .backend()
            .value("counter")
            .unwrap(),
        7
    );
    handle.stop();
    drop(join.await.unwrap().unwrap());
    drop(handle);
}

struct NoCommit(Arc<DurableCounter>);
#[async_trait]
impl Participant for NoCommit {
    fn id(&self) -> ParticipantId {
        self.0.id()
    }
    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError> {
        self.0.prepare(tx, payload).await
    }
    async fn commit(&self, _: &TransactionId, _: &[u8]) -> Result<(), ParticipantError> {
        Err(ParticipantError::message("unavailable".into()))
    }
    async fn abort(
        &self,
        tx: &TransactionId,
        token: Option<&[u8]>,
    ) -> Result<(), ParticipantError> {
        self.0.abort(tx, token).await
    }
    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        self.0.status(tx).await
    }
}
#[tokio::test]
async fn participant_return_and_manual_controls_never_reverse_commit() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let participant = Arc::new(
        DurableCounter::open(path.join("resource.db"), ParticipantId::new("p").unwrap()).unwrap(),
    );
    let policy = RetryPolicy {
        attempts: 1,
        timeout: Duration::from_millis(100),
        backoff: Duration::ZERO,
    };
    let tx = TransactionId::new("committed").unwrap();
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(path.join("2pc.db")).unwrap(),
        vec![Arc::new(NoCommit(participant.clone()))],
        policy.clone(),
    )
    .unwrap();
    let status = coordinator
        .execute(
            tx.clone(),
            vec![Enlistment {
                participant: participant.id(),
                payload: b"9".to_vec(),
            }],
            &Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(status.decision, Some(Decision::Commit));
    assert_eq!(status.outcome, Outcome::InDoubt);
    drop(coordinator);
    let coordinator = Coordinator::open(
        SqliteJournal::open(path.join("2pc.db")).unwrap(),
        vec![],
        policy,
    )
    .unwrap();
    let runner = Runner::open(
        coordinator,
        SqliteJournal::open(path.join("schedule.db")).unwrap(),
        "2pc",
        config(),
    )
    .unwrap();
    let (handle, join) = runner.start();
    handle.control("committed", Control::Pause).await.unwrap();
    handle
        .target()
        .lock()
        .await
        .register(participant.clone())
        .unwrap();
    handle.control("committed", Control::Resume).await.unwrap();
    handle.wake();
    drained(&handle).await;
    let status = handle.target().lock().await.status(&tx).unwrap().unwrap();
    assert_eq!(status.decision, Some(Decision::Commit));
    assert_eq!(status.outcome, Outcome::Committed);
    assert_eq!(participant.value().await.unwrap(), 9);
    assert!(matches!(
        handle.control("committed", Control::Retry).await,
        Err(Error::Invalid { .. })
    ));
    handle.stop();
    drop(join.await.unwrap().unwrap());
    drop(handle);
}

struct Ambiguous(SqliteJournal);
impl Journal for Ambiguous {
    fn append(&mut self, v: u32, p: &[u8]) -> Result<u64, JournalError> {
        self.0.append(v, p)
    }
    fn flush(&mut self) -> Result<u64, JournalError> {
        self.0.flush()?;
        Err(JournalError::message(
            "lost protocol barrier acknowledgement".into(),
        ))
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.0.replay()
    }
}
#[tokio::test]
async fn uncertain_protocol_write_stops_runner_and_reopen_reconciles_receipts() {
    for mode in ["atomic", "chunked", "publish"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path();
        seed(path, mode).await;
        let engine = Engine::open(
            Ambiguous(SqliteJournal::open(path.join("engine.db")).unwrap()),
            SqliteBackend::open(path.join("backend.db"), "recovery-test").unwrap(),
            ExecutionPolicy::default(),
        )
        .unwrap();
        let runner = Runner::open(
            engine,
            SqliteJournal::open(path.join("schedule.db")).unwrap(),
            "engine",
            config(),
        )
        .unwrap();
        let (handle, join) = runner.start();
        assert!(matches!(join.await.unwrap(), Err(Error::InDoubt { .. })));
        assert!(handle.target().lock().await.status(&id("work")).is_err());
        drop(handle);
        let traces = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = traces.clone();
        let runner = Runner::open(
            open(path),
            SqliteJournal::open(path.join("schedule.db")).unwrap(),
            "engine",
            config(),
        )
        .unwrap()
        .observe(move |trace| {
            sink.lock().unwrap().push(trace.clone());
        });
        let (handle, join) = runner.start();
        drained(&handle).await;
        let engine = handle.target().lock().await;
        if mode == "publish" {
            assert_eq!(
                engine
                    .backend()
                    .visible(&id("visible"))
                    .unwrap()
                    .unwrap()
                    .generation,
                1
            );
        } else {
            assert_eq!(engine.backend().value("counter").unwrap(), 7);
        }
        drop(engine);
        assert!(!traces.lock().unwrap().is_empty());
        handle.stop();
        drop(join.await.unwrap().unwrap());
        drop(handle);
    }
}
