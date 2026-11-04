//! Protocol fault matrix including real child-process termination without unwind.
use async_trait::async_trait;
use pactum_core::{
    Cancellation, Coordinator, Decision, Enlistment, Error, Outcome, Participant, ParticipantError,
    ParticipantId, ParticipantStatus, RetryPolicy, TransactionId, Vote,
};
use pactum_demo::DurableCounter;
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use pactum_sqlite::SqliteJournal;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

fn tx() -> TransactionId {
    TransactionId::new("fault-transaction").unwrap()
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        attempts: 2,
        timeout: Duration::from_millis(200),
        backoff: Duration::from_millis(1),
    }
}
fn resources(path: &Path) -> (Arc<DurableCounter>, Arc<DurableCounter>) {
    (
        Arc::new(
            DurableCounter::open(path.join("a.db"), ParticipantId::new("a").unwrap()).unwrap(),
        ),
        Arc::new(
            DurableCounter::open(path.join("b.db"), ParticipantId::new("b").unwrap()).unwrap(),
        ),
    )
}
fn enlist(a: &dyn Participant, b: &dyn Participant, reject: bool) -> Vec<Enlistment> {
    // Reverse order deliberately: coordinator must sort this deterministically.
    vec![
        Enlistment {
            participant: b.id(),
            payload: if reject {
                b"-1".to_vec()
            } else {
                b"10".to_vec()
            },
        },
        Enlistment {
            participant: a.id(),
            payload: b"10".to_vec(),
        },
    ]
}

#[tokio::test]
async fn durable_commit_abort_duplicates_identity_and_conflicting_input() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    let (a, b) = resources(path);
    assert!(DurableCounter::open(path.join("a.db"), b.id()).is_err());
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        vec![a.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    let cancel = Cancellation::default();
    let requests = enlist(&*a, &*b, false);
    assert_eq!(
        coordinator
            .execute(tx(), requests.clone(), &cancel)
            .await
            .unwrap()
            .outcome,
        Outcome::Committed
    );
    assert_eq!(
        coordinator
            .execute(tx(), requests, &cancel)
            .await
            .unwrap()
            .outcome,
        Outcome::Committed
    );
    assert!(matches!(
        coordinator
            .execute(tx(), enlist(&*a, &*b, true), &cancel)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(a.value().await.unwrap(), 10);
    assert_eq!(b.value().await.unwrap(), 10);
    assert!(a.abort(&tx(), None).await.is_err());
    assert!(a.commit(&tx(), b"wrong-token").await.is_err());
    a.commit(&tx(), tx().as_str().as_bytes()).await.unwrap();
    assert_eq!(a.value().await.unwrap(), 10);
    let abort = TransactionId::new("abort").unwrap();
    assert_eq!(
        coordinator
            .execute(abort.clone(), enlist(&*a, &*b, true), &cancel)
            .await
            .unwrap()
            .outcome,
        Outcome::Aborted
    );
    assert_eq!(a.status(&abort).await.unwrap(), ParticipantStatus::Aborted);
    assert_eq!(b.status(&abort).await.unwrap(), ParticipantStatus::Aborted);
    assert!(matches!(
        a.prepare(&abort, b"10").await.unwrap(),
        Vote::Rejected(_)
    ));
    assert!(a.commit(&abort, b"abort").await.is_err());
    assert_eq!(a.value().await.unwrap(), 10);
    coordinator
        .compact(pactum_journal::Retention::Compact)
        .unwrap();
    drop(coordinator);
    drop(a);
    drop(b);
    let (a, b) = resources(path);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        vec![a.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    assert!(
        coordinator
            .recover(&cancel)
            .await
            .unwrap()
            .iter()
            .all(|s| s.outcome != Outcome::InDoubt)
    );
    assert_eq!(a.value().await.unwrap(), 10);
    assert_eq!(b.value().await.unwrap(), 10);
    assert!(matches!(
        coordinator
            .execute(tx(), enlist(&*a, &*b, true), &cancel)
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        coordinator
            .execute(tx(), enlist(&*a, &*b, false), &cancel)
            .await
            .unwrap()
            .outcome,
        Outcome::Committed
    );
    assert_eq!(
        coordinator
            .execute(abort, enlist(&*a, &*b, true), &cancel)
            .await
            .unwrap()
            .outcome,
        Outcome::Aborted
    );
}

/// Journal failures both before persistence and after a successful barrier.
struct FailJournal {
    inner: SqliteJournal,
    step: usize,
    fail_at: usize,
}
impl FailJournal {
    fn tick(&mut self) -> Result<(), JournalError> {
        self.step += 1;
        if self.step == self.fail_at {
            Err(JournalError("injected uncertain write".into()))
        } else {
            Ok(())
        }
    }
}
impl Journal for FailJournal {
    fn append(&mut self, v: u32, p: &[u8]) -> Result<Sequence, JournalError> {
        self.tick()?;
        let result = self.inner.append(v, p)?;
        self.tick()?;
        Ok(result)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.tick()?;
        let result = self.inner.flush()?;
        self.tick()?;
        Ok(result)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.inner.replay()
    }
}
#[tokio::test]
async fn every_write_and_flush_error_requires_reopen_and_never_reverses_decision() {
    // Begin + two votes + decision + two acks; four fault sites per record.
    for fail_at in 1..=24 {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path();
        let (a, b) = resources(path);
        let journal = FailJournal {
            inner: SqliteJournal::open(path.join("j.db")).unwrap(),
            step: 0,
            fail_at,
        };
        let mut coordinator =
            Coordinator::open(journal, vec![a.clone(), b.clone()], policy()).unwrap();
        let cancel = Cancellation::default();
        assert!(
            matches!(
                coordinator
                    .execute(tx(), enlist(&*a, &*b, false), &cancel)
                    .await,
                Err(Error::InDoubt(_))
            ),
            "fault {fail_at}"
        );
        assert!(matches!(
            coordinator.recover(&cancel).await,
            Err(Error::InDoubt(_))
        ));
        drop(coordinator);
        let mut coordinator = Coordinator::open(
            SqliteJournal::open(path.join("j.db")).unwrap(),
            vec![a.clone(), b.clone()],
            policy(),
        )
        .unwrap();
        let before = coordinator.status(&tx()).unwrap();
        let recovered = coordinator.recover(&cancel).await.unwrap();
        if let Some(before) = before {
            let after = &recovered[0];
            if let Some(decision) = before.decision {
                assert_eq!(after.decision, Some(decision), "fault {fail_at}");
            }
            let value = if after.decision == Some(Decision::Commit) {
                10
            } else {
                0
            };
            assert_eq!(a.value().await.unwrap(), value);
            assert_eq!(b.value().await.unwrap(), value);
            assert_ne!(after.outcome, Outcome::InDoubt);
        } else {
            assert!(recovered.is_empty());
        }
    }
}

struct Controlled {
    resource: Arc<DurableCounter>,
    hang_prepare: AtomicBool,
    hang_commit: AtomicBool,
    fail_commit: AtomicBool,
    lose_commit_response: AtomicBool,
    prepare_started: tokio::sync::Notify,
    commit_started: tokio::sync::Notify,
    prepare_calls: AtomicUsize,
}
impl Controlled {
    fn new(resource: Arc<DurableCounter>) -> Self {
        Self {
            resource,
            hang_prepare: AtomicBool::new(false),
            hang_commit: AtomicBool::new(false),
            fail_commit: AtomicBool::new(false),
            lose_commit_response: AtomicBool::new(false),
            prepare_started: tokio::sync::Notify::new(),
            commit_started: tokio::sync::Notify::new(),
            prepare_calls: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl Participant for Controlled {
    fn id(&self) -> ParticipantId {
        self.resource.id()
    }
    async fn prepare(&self, t: &TransactionId, p: &[u8]) -> Result<Vote, ParticipantError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        let vote = self.resource.prepare(t, p).await?;
        self.prepare_started.notify_one();
        if self.hang_prepare.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        Ok(vote)
    }
    async fn commit(&self, t: &TransactionId, p: &[u8]) -> Result<(), ParticipantError> {
        self.commit_started.notify_one();
        if self.hang_commit.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(ParticipantError("offline".into()));
        }
        self.resource.commit(t, p).await?;
        if self.lose_commit_response.swap(false, Ordering::SeqCst) {
            return Err(ParticipantError("commit response lost".into()));
        }
        Ok(())
    }
    async fn abort(&self, t: &TransactionId, p: Option<&[u8]>) -> Result<(), ParticipantError> {
        self.resource.abort(t, p).await
    }
    async fn status(&self, t: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        self.resource.status(t).await
    }
}
#[tokio::test]
async fn timeout_lost_prepare_response_aborts_every_resource_and_fences_late_prepare() {
    let temp = tempfile::tempdir().unwrap();
    let (a, b) = resources(temp.path());
    let controlled = Arc::new(Controlled::new(a.clone()));
    controlled.hang_prepare.store(true, Ordering::SeqCst);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("j.db")).unwrap(),
        vec![controlled.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    let result = coordinator
        .execute(tx(), enlist(&*a, &*b, false), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(result.outcome, Outcome::Aborted);
    assert_eq!(controlled.prepare_calls.load(Ordering::SeqCst), 2);
    assert_eq!(a.status(&tx()).await.unwrap(), ParticipantStatus::Aborted);
    assert_eq!(b.status(&tx()).await.unwrap(), ParticipantStatus::Aborted);
    assert!(matches!(
        a.prepare(&tx(), b"10").await.unwrap(),
        Vote::Rejected(_)
    ));
    assert_eq!(a.value().await.unwrap(), 0);
}
#[tokio::test]
async fn cancellation_before_and_after_decision_and_dropped_future_are_safe() {
    for commit in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (a, b) = resources(temp.path());
        let controlled = Arc::new(Controlled::new(a.clone()));
        controlled.hang_prepare.store(!commit, Ordering::SeqCst);
        controlled.hang_commit.store(commit, Ordering::SeqCst);
        let mut coordinator = Coordinator::open(
            SqliteJournal::open(temp.path().join("j.db")).unwrap(),
            vec![controlled.clone(), b.clone()],
            policy(),
        )
        .unwrap();
        let cancel = Cancellation::default();
        let requester = cancel.clone();
        let requests = enlist(&*a, &*b, false);
        let (result, ()) = tokio::join!(coordinator.execute(tx(), requests, &cancel), async {
            if commit {
                controlled.commit_started.notified().await;
            } else {
                controlled.prepare_started.notified().await;
            }
            requester.cancel();
        });
        let result = result.unwrap();
        assert_eq!(result.outcome, Outcome::InDoubt);
        assert_eq!(
            result.decision,
            Some(if commit {
                Decision::Commit
            } else {
                Decision::Abort
            })
        );
        controlled.hang_commit.store(false, Ordering::SeqCst);
        let result = coordinator
            .recover(&Cancellation::default())
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            result.outcome,
            if commit {
                Outcome::Committed
            } else {
                Outcome::Aborted
            }
        );
    }
    let temp = tempfile::tempdir().unwrap();
    let (a, b) = resources(temp.path());
    let controlled = Arc::new(Controlled::new(a.clone()));
    controlled.hang_prepare.store(true, Ordering::SeqCst);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("j.db")).unwrap(),
        vec![controlled.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    let cancel = Cancellation::default();
    {
        let future = coordinator.execute(tx(), enlist(&*a, &*b, false), &cancel);
        tokio::pin!(future);
        tokio::select! { _ = &mut future => panic!("should be preparing"), _ = controlled.prepare_started.notified() => {} }
        // Dropping a caller's future must leave durable work recoverable.
    }
    assert_eq!(coordinator.status(&tx()).unwrap().unwrap().decision, None);
    assert_eq!(
        coordinator.recover(&cancel).await.unwrap()[0].outcome,
        Outcome::Aborted
    );
}
#[tokio::test]
async fn durable_commit_survives_timeout_and_missing_registration() {
    let temp = tempfile::tempdir().unwrap();
    let (a, b) = resources(temp.path());
    let controlled = Arc::new(Controlled::new(b.clone()));
    controlled.hang_commit.store(true, Ordering::SeqCst);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("j.db")).unwrap(),
        vec![a.clone(), controlled],
        policy(),
    )
    .unwrap();
    let cancel = Cancellation::default();
    let result = coordinator
        .execute(tx(), enlist(&*a, &*b, false), &cancel)
        .await
        .unwrap();
    assert_eq!(result.decision, Some(Decision::Commit));
    assert_eq!(result.outcome, Outcome::InDoubt);
    assert!(result.last_error.unwrap().contains("timeout"));
    drop(coordinator);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("j.db")).unwrap(),
        vec![a.clone()],
        policy(),
    )
    .unwrap();
    let result = coordinator.recover(&cancel).await.unwrap().remove(0);
    assert_eq!(result.decision, Some(Decision::Commit));
    assert_eq!(result.pending, vec![b.id()]);
    coordinator.register(b.clone()).unwrap();
    assert!(coordinator.register(b.clone()).is_err());
    assert_eq!(
        coordinator.recover(&cancel).await.unwrap()[0].outcome,
        Outcome::Committed
    );
    assert_eq!(a.value().await.unwrap(), 10);
    assert_eq!(b.value().await.unwrap(), 10);
}

struct Gate {
    counter: AtomicUsize,
    target: usize,
    directory: PathBuf,
}
impl Gate {
    fn tick(&self) {
        let step = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        if step == self.target {
            std::fs::write(self.directory.join("ready"), step.to_string()).unwrap();
            // Parent uses Child::kill here: no resource/journal destructors run.
            loop {
                std::thread::park();
            }
        }
    }
}
struct CrashJournal {
    inner: SqliteJournal,
    gate: Arc<Gate>,
}
impl Journal for CrashJournal {
    fn append(&mut self, v: u32, p: &[u8]) -> Result<Sequence, JournalError> {
        self.gate.tick();
        let r = self.inner.append(v, p);
        self.gate.tick();
        r
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.gate.tick();
        let r = self.inner.flush();
        self.gate.tick();
        r
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.gate.tick();
        let r = self.inner.replay();
        self.gate.tick();
        r
    }
}
struct CrashParticipant {
    resource: Arc<DurableCounter>,
    gate: Arc<Gate>,
}
#[async_trait]
impl Participant for CrashParticipant {
    fn id(&self) -> ParticipantId {
        self.resource.id()
    }
    async fn prepare(&self, t: &TransactionId, p: &[u8]) -> Result<Vote, ParticipantError> {
        self.gate.tick();
        let r = self.resource.prepare(t, p).await;
        self.gate.tick();
        r
    }
    async fn commit(&self, t: &TransactionId, p: &[u8]) -> Result<(), ParticipantError> {
        self.gate.tick();
        let r = self.resource.commit(t, p).await;
        self.gate.tick();
        r
    }
    async fn abort(&self, t: &TransactionId, p: Option<&[u8]>) -> Result<(), ParticipantError> {
        self.gate.tick();
        let r = self.resource.abort(t, p).await;
        self.gate.tick();
        r
    }
    async fn status(&self, t: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        self.resource.status(t).await
    }
}
#[test]
fn crash_child() {
    let Some(directory) = std::env::var_os("PACTUM_CHILD_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let gate = Arc::new(Gate {
        counter: AtomicUsize::new(0),
        target: std::env::var("PACTUM_CRASH_AT").unwrap().parse().unwrap(),
        directory: directory.clone(),
    });
    let (a, b) = resources(&directory);
    let participants: Vec<Arc<dyn Participant>> = vec![
        Arc::new(CrashParticipant {
            resource: a.clone(),
            gate: gate.clone(),
        }),
        Arc::new(CrashParticipant {
            resource: b.clone(),
            gate: gate.clone(),
        }),
    ];
    let mut coordinator = Coordinator::open(
        CrashJournal {
            inner: SqliteJournal::open(directory.join("j.db")).unwrap(),
            gate: gate.clone(),
        },
        participants,
        policy(),
    )
    .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let cancel = Cancellation::default();
        if std::env::var("PACTUM_SCENARIO").unwrap() == "recovery" {
            coordinator.recover(&cancel).await.unwrap();
        } else {
            coordinator
                .execute(
                    tx(),
                    enlist(
                        &*a,
                        &*b,
                        std::env::var("PACTUM_SCENARIO").unwrap() == "abort",
                    ),
                    &cancel,
                )
                .await
                .unwrap();
        }
    });
    std::fs::write(
        directory.join("done"),
        gate.counter.load(Ordering::SeqCst).to_string(),
    )
    .unwrap();
}
fn run_child(directory: &Path, target: usize, scenario: &str) -> usize {
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("PACTUM_CHILD_DIR", directory)
        .env("PACTUM_CRASH_AT", target.to_string())
        .env("PACTUM_SCENARIO", scenario)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        if directory.join("ready").exists() {
            child.kill().unwrap();
            child.wait().unwrap();
            return target;
        }
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child failed at {target} ({scenario})");
            assert_eq!(
                target, 0,
                "child never reached requested crash site ({scenario})"
            );
            return std::fs::read_to_string(directory.join("done"))
                .unwrap()
                .parse()
                .unwrap();
        }
        if start.elapsed() > Duration::from_secs(20) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child stuck at {target} ({scenario})");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
async fn reconcile_and_verify(path: &Path) {
    let (a, b) = resources(path);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        vec![a.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    let before = coordinator.status(&tx()).unwrap();
    coordinator
        .compact(pactum_journal::Retention::Archive)
        .unwrap();
    drop(coordinator);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        vec![a.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    assert_eq!(before, coordinator.status(&tx()).unwrap());
    let after = coordinator.recover(&Cancellation::default()).await.unwrap();
    if let Some(before) = before {
        assert_eq!(after.len(), 1);
        let after = &after[0];
        if before.decision.is_some() {
            assert_eq!(before.decision, after.decision);
        }
        assert_ne!(after.outcome, Outcome::InDoubt);
        let value = if after.decision == Some(Decision::Commit) {
            10
        } else {
            0
        };
        assert_eq!(a.value().await.unwrap(), value);
        assert_eq!(b.value().await.unwrap(), value);
        let state = if value == 10 {
            ParticipantStatus::Committed
        } else {
            ParticipantStatus::Aborted
        };
        assert_eq!(a.status(&tx()).await.unwrap(), state);
        assert_eq!(b.status(&tx()).await.unwrap(), state);
        assert!(
            coordinator
                .recover_pending(&Cancellation::default())
                .await
                .unwrap()
                .is_empty()
        );
        coordinator
            .compact(pactum_journal::Retention::Compact)
            .unwrap();
        assert_eq!(
            coordinator.recover(&Cancellation::default()).await.unwrap()[0],
            *after
        );
    } else {
        assert!(after.is_empty());
        assert_eq!(a.value().await.unwrap(), 0);
        assert_eq!(b.value().await.unwrap(), 0);
    }
}
#[tokio::test]
async fn kill_restart_at_every_protocol_boundary() {
    for scenario in ["commit", "abort"] {
        let baseline = tempfile::tempdir().unwrap();
        let count = run_child(baseline.path(), 0, scenario);
        assert!(count >= 30);
        for target in 1..=count {
            let temp = tempfile::tempdir().unwrap();
            run_child(temp.path(), target, scenario);
            reconcile_and_verify(temp.path()).await;
        }
    }
}
#[tokio::test]
async fn kill_restart_during_recovery_repeatedly_preserves_commit() {
    // Leave a durable decision with an unavailable second participant.
    let baseline = tempfile::tempdir().unwrap();
    seed_pending_commit(baseline.path()).await;
    let count = run_child(baseline.path(), 0, "recovery");
    for target in 1..=count {
        let temp = tempfile::tempdir().unwrap();
        seed_pending_commit(temp.path()).await;
        run_child(temp.path(), target, "recovery");
        reconcile_and_verify(temp.path()).await;
    }
}
async fn seed_pending_commit(path: &Path) {
    let (a, b) = resources(path);
    let controlled = Arc::new(Controlled::new(b.clone()));
    controlled.fail_commit.store(true, Ordering::SeqCst);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        vec![a.clone(), controlled],
        policy(),
    )
    .unwrap();
    assert_eq!(
        coordinator
            .execute(tx(), enlist(&*a, &*b, false), &Cancellation::default())
            .await
            .unwrap()
            .decision,
        Some(Decision::Commit)
    );
}

#[tokio::test]
async fn cancelled_before_prepare_creates_only_abort_tombstones() {
    let temp = tempfile::tempdir().unwrap();
    let (a, b) = resources(temp.path());
    let controlled = Arc::new(Controlled::new(a.clone()));
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("j.db")).unwrap(),
        vec![controlled.clone(), b.clone()],
        policy(),
    )
    .unwrap();
    let cancel = Cancellation::default();
    cancel.cancel();
    let result = coordinator
        .execute(tx(), enlist(&*a, &*b, false), &cancel)
        .await
        .unwrap();
    assert_eq!(result.decision, Some(Decision::Abort));
    assert_eq!(result.outcome, Outcome::InDoubt);
    assert_eq!(controlled.prepare_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        coordinator.recover(&Cancellation::default()).await.unwrap()[0].outcome,
        Outcome::Aborted
    );
    assert_eq!(a.value().await.unwrap(), 0);
}

#[tokio::test]
async fn kill_restart_during_undecided_recovery_preserves_abort() {
    let baseline = tempfile::tempdir().unwrap();
    seed_undecided(baseline.path()).await;
    let count = run_child(baseline.path(), 0, "recovery");
    for target in 1..=count {
        let temp = tempfile::tempdir().unwrap();
        seed_undecided(temp.path()).await;
        run_child(temp.path(), target, "recovery");
        reconcile_and_verify(temp.path()).await;
    }
}
async fn seed_undecided(path: &Path) {
    let (a, b) = resources(path);
    let journal = FailJournal {
        inner: SqliteJournal::open(path.join("j.db")).unwrap(),
        step: 0,
        fail_at: 13,
    };
    let mut coordinator = Coordinator::open(journal, vec![a.clone(), b.clone()], policy()).unwrap();
    assert!(matches!(
        coordinator
            .execute(tx(), enlist(&*a, &*b, false), &Cancellation::default())
            .await,
        Err(Error::InDoubt(_))
    ));
    assert!(matches!(
        a.status(&tx()).await.unwrap(),
        ParticipantStatus::Prepared(_)
    ));
    assert!(matches!(
        b.status(&tx()).await.unwrap(),
        ParticipantStatus::Prepared(_)
    ));
}

#[tokio::test]
async fn participant_capacity_reservations_and_restart_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("resource.db");
    let id = ParticipantId::new("capacity").unwrap();
    let a = DurableCounter::open(&path, id.clone()).unwrap();
    let first = TransactionId::new("first").unwrap();
    let second = TransactionId::new("second").unwrap();
    let payload = i64::MAX.to_string();
    let Vote::Prepared(token) = a.prepare(&first, payload.as_bytes()).await.unwrap() else {
        panic!("must prepare");
    };
    assert_eq!(
        a.prepare(&first, payload.as_bytes()).await.unwrap(),
        Vote::Prepared(token.clone())
    );
    assert!(a.prepare(&first, b"1").await.is_err());
    assert!(matches!(
        a.prepare(&second, b"1").await.unwrap(),
        Vote::Rejected(_)
    ));
    drop(a);
    let a = DurableCounter::open(path, id).unwrap();
    assert_eq!(
        a.status(&first).await.unwrap(),
        ParticipantStatus::Prepared(token.clone())
    );
    a.commit(&first, &token).await.unwrap();
    a.commit(&first, &token).await.unwrap();
    assert_eq!(a.value().await.unwrap(), i64::MAX);
    assert!(matches!(
        a.prepare(&second, b"1").await.unwrap(),
        Vote::Rejected(_)
    ));
}

#[tokio::test]
async fn retry_after_lost_commit_response_changes_value_once() {
    let temp = tempfile::tempdir().unwrap();
    let (a, b) = resources(temp.path());
    let controlled = Arc::new(Controlled::new(b.clone()));
    controlled
        .lose_commit_response
        .store(true, Ordering::SeqCst);
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("j.db")).unwrap(),
        vec![a.clone(), controlled.clone()],
        policy(),
    )
    .unwrap();
    let status = coordinator
        .execute(tx(), enlist(&*a, &*b, false), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(status.outcome, Outcome::Committed);
    assert!(!controlled.lose_commit_response.load(Ordering::SeqCst));
    assert_eq!(b.value().await.unwrap(), 10);
    assert_eq!(coordinator.diagnostics().pending_transactions, 0);
    assert!(!coordinator.diagnostics().reopen_required);
}

#[test]
fn checkpoint_does_not_invent_commit_without_prepares_or_accept_duplicate_bindings() {
    let begin = serde_json::json!({"kind":"Begin", "tx":"t", "requests":[{"participant":"a", "payload":[49]}]});
    for records in [
        serde_json::json!([begin, {"kind":"Decided", "tx":"t", "decision":"Commit"}]),
        serde_json::json!([begin, begin]),
        serde_json::json!([{"kind":"Acknowledged", "tx":"t", "participant":"a"}]),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut j = SqliteJournal::open(temp.path().join("j.db")).unwrap();
        for _ in 0..10 {
            j.append(1, b"covered").unwrap();
        }
        j.flush().unwrap();
        j.compact(
            10,
            1,
            &serde_json::to_vec(&records).unwrap(),
            pactum_journal::Retention::Compact,
        )
        .unwrap();
        assert!(Coordinator::open(j, vec![], policy()).is_err());
    }
}

#[tokio::test]
async fn uncertain_checkpoint_acknowledgement_requires_reopen_and_preserves_pending_commit() {
    struct LostCheckpoint {
        inner: SqliteJournal,
        after_commit: bool,
    }
    impl Journal for LostCheckpoint {
        fn append(&mut self, v: u32, p: &[u8]) -> Result<Sequence, JournalError> {
            self.inner.append(v, p)
        }
        fn flush(&mut self) -> Result<Sequence, JournalError> {
            self.inner.flush()
        }
        fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
            self.inner.replay()
        }
        fn checkpoint(&mut self) -> Result<Option<pactum_journal::Checkpoint>, JournalError> {
            self.inner.checkpoint()
        }
        fn compact(
            &mut self,
            w: Sequence,
            v: u32,
            p: &[u8],
            r: pactum_journal::Retention,
        ) -> Result<(), JournalError> {
            if self.after_commit {
                self.inner.compact(w, v, p, r)?;
            }
            Err(JournalError("lost checkpoint acknowledgement".into()))
        }
    }
    for after_commit in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        seed_pending_commit(temp.path()).await;
        let journal = LostCheckpoint {
            inner: SqliteJournal::open(temp.path().join("j.db")).unwrap(),
            after_commit,
        };
        let mut coordinator = Coordinator::open(journal, vec![], policy()).unwrap();
        let before = coordinator.status(&tx()).unwrap();
        assert!(matches!(
            coordinator.compact(pactum_journal::Retention::Compact),
            Err(Error::InDoubt(_))
        ));
        assert!(matches!(coordinator.status(&tx()), Err(Error::InDoubt(_))));
        drop(coordinator);
        let coordinator = Coordinator::open(
            SqliteJournal::open(temp.path().join("j.db")).unwrap(),
            vec![],
            policy(),
        )
        .unwrap();
        assert_eq!(before, coordinator.status(&tx()).unwrap());
        drop(coordinator);
        reconcile_and_verify(temp.path()).await;
    }
}
