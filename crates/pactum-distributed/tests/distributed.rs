//! Ownership, partition, remote identity and real process-crash boundaries.
use async_trait::async_trait;
use pactum_core::{
    Cancellation, Decision, Enlistment, Error, Outcome, ParticipantError, ParticipantId,
    RetryPolicy, TransactionId,
};
use pactum_distributed::{
    DistributedCoordinator, Domain, FencedResource, Grant, JournalAuthority, LeaseAuthority,
    Operation, Reply,
    reference::{Clock, ReferenceCounter, ReferenceStore},
    transport::{Endpoint, Loopback, RemoteResource, Request, Response, Transport, TransportError},
};
use pactum_journal::{Journal, Retention};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

struct TestClock(AtomicU64);
impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(1000)))
    }
    fn advance(&self) {
        self.0.fetch_add(1000, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn now_millis(&self) -> Result<u64, Error> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
fn ttl() -> Duration {
    Duration::from_millis(500)
}
fn pid() -> ParticipantId {
    ParticipantId::new("counter").unwrap()
}
fn tid() -> TransactionId {
    TransactionId::new("transaction").unwrap()
}
fn requests() -> Vec<Enlistment> {
    vec![Enlistment {
        participant: pid(),
        payload: b"7".to_vec(),
    }]
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        attempts: 2,
        timeout: Duration::from_millis(200),
        backoff: Duration::ZERO,
    }
}
fn coordinator(
    store: &ReferenceStore,
    grant: Grant,
    resource: Arc<dyn FencedResource>,
) -> DistributedCoordinator {
    DistributedCoordinator::open(store, grant, vec![resource], policy()).unwrap()
}
fn store(path: &std::path::Path, clock: Arc<TestClock>) -> ReferenceStore {
    ReferenceStore::open_with_clock(path, "scope", clock).unwrap()
}

#[test]
fn epochs_survive_reopen_and_stale_batches_cannot_flush_or_compact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("service.db");
    let clock = TestClock::new();
    let first = store(&path, clock.clone());
    let grant = first.acquire("one", ttl()).unwrap();
    assert!(first.acquire("two", ttl()).is_err());
    assert!(first.acquire("one", ttl()).is_err());
    assert!(first.acquire("", ttl()).is_err());
    let mut old = first.open_journal(&grant).unwrap();
    old.append(1, b"staged but never durable").unwrap();
    clock.advance();
    assert!(first.renew(&grant, ttl()).is_err());
    assert!(first.release(&grant).is_err());
    let second = store(&path, clock.clone());
    let next = second.acquire("two", ttl()).unwrap();
    assert!(next.epoch > grant.epoch);
    assert!(old.flush().is_err());
    assert!(old.replay().is_err());
    assert!(first.open_journal(&grant).is_err());
    let mut fresh = second.open_journal(&next).unwrap();
    assert!(fresh.replay().unwrap().is_empty());
    fresh.append(1, b"durable").unwrap();
    fresh.flush().unwrap();
    second.renew(&next, Duration::from_millis(1500)).unwrap();
    clock.advance();
    assert!(second.acquire("three", ttl()).is_err());
    second.release(&next).unwrap();
    assert!(fresh.compact(1, 1, b"state", Retention::Compact).is_err());
    drop(first);
    drop(second);
    drop(fresh);
    drop(old);
    let reopened = store(&path, clock);
    let final_grant = reopened.acquire("two", ttl()).unwrap();
    assert!(final_grant.epoch > next.epoch);
    assert_eq!(
        reopened
            .open_journal(&final_grant)
            .unwrap()
            .replay()
            .unwrap()[0]
            .payload,
        b"durable"
    );
    assert!(ReferenceStore::open(&path, "different").is_err());
}

#[test]
fn simultaneous_connections_have_one_winner_and_same_epoch_sessions_use_cas() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("service.db");
    let clock = TestClock::new();
    let a = store(&path, clock.clone());
    let b = store(&path, clock);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let barrier2 = barrier.clone();
    let winner = std::thread::spawn(move || {
        barrier2.wait();
        a.acquire("a", ttl())
    });
    barrier.wait();
    let other = b.acquire("b", ttl());
    let winner = winner.join().unwrap();
    assert_ne!(winner.is_ok(), other.is_ok());
    let grant = winner.or(other).unwrap();
    let mut one = b.open_journal(&grant).unwrap();
    let mut two = b.open_journal(&grant).unwrap();
    one.append(1, b"one").unwrap();
    two.append(1, b"two").unwrap();
    one.flush().unwrap();
    assert!(two.flush().is_err());
    assert_eq!(
        b.open_journal(&grant).unwrap().replay().unwrap()[0].payload,
        b"one"
    );
}

#[tokio::test]
async fn delayed_old_resource_calls_fail_before_new_owner_contacts_resource() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock.clone());
    let counter = db.counter(pid()).unwrap();
    let old = db.acquire("old", ttl()).unwrap();
    let Reply::Prepared(token) = counter
        .call(&old, &tid(), Operation::Prepare(b"7".to_vec()))
        .await
        .unwrap()
    else {
        panic!()
    };
    clock.advance();
    let next = db.acquire("new", ttl()).unwrap();
    // No participant epoch installation has happened. Global enforcement still rejects old calls.
    for op in [
        Operation::Commit(token.clone()),
        Operation::Abort(None),
        Operation::Prepare(b"7".to_vec()),
        Operation::Status,
    ] {
        assert!(counter.call(&old, &tid(), op).await.is_err());
    }
    assert_eq!(counter.value().unwrap(), 0);
    counter
        .call(&next, &tid(), Operation::Abort(None))
        .await
        .unwrap();
    assert!(matches!(
        counter
            .call(&next, &tid(), Operation::Prepare(b"7".to_vec()))
            .await
            .unwrap(),
        Reply::Rejected(_)
    ));
    assert!(
        counter
            .call(&next, &tid(), Operation::Commit(token))
            .await
            .is_err()
    );
    assert_eq!(counter.value().unwrap(), 0);
}

#[tokio::test]
async fn remote_lost_replies_retry_idempotently_and_checkpoints_survive_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let path = dir.path().join("service.db");
    let db = store(&path, clock.clone());
    let counter = db.counter(pid()).unwrap();
    let link = Arc::new(Loopback::new(counter.clone()));
    link.lose_replies(1);
    let remote = Arc::new(
        RemoteResource::new(
            pid(),
            LeaseAuthority::domain(&db),
            link,
            Duration::from_millis(100),
        )
        .unwrap(),
    );
    let grant = db.acquire("old", ttl()).unwrap();
    let mut owner = coordinator(&db, grant, remote.clone());
    assert_eq!(
        owner
            .execute(tid(), requests(), &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        Outcome::Committed
    );
    assert_eq!(counter.value().unwrap(), 7);
    owner.compact(Retention::Archive).unwrap();
    clock.advance();
    let reopened = store(&path, clock);
    let grant = reopened.acquire("new", ttl()).unwrap();
    let mut takeover = coordinator(&reopened, grant, remote);
    assert_eq!(
        takeover.recover(&Cancellation::default()).await.unwrap()[0].outcome,
        Outcome::Committed
    );
    assert_eq!(
        takeover
            .execute(tid(), requests(), &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        Outcome::Committed
    );
    let mut changed = requests();
    changed[0].payload = b"8".to_vec();
    assert!(
        takeover
            .execute(tid(), changed, &Cancellation::default())
            .await
            .is_err()
    );
    assert_eq!(counter.value().unwrap(), 7);
}

struct Intercept {
    inner: Arc<ReferenceCounter>,
    pause_prepare: bool,
    pause_commit: bool,
    reached: Notify,
    resume: Notify,
    commit_reply_loss: AtomicBool,
}
impl Intercept {
    fn new(inner: Arc<ReferenceCounter>, prepare: bool, commit: bool) -> Arc<Self> {
        Arc::new(Self {
            inner,
            pause_prepare: prepare,
            pause_commit: commit,
            reached: Notify::new(),
            resume: Notify::new(),
            commit_reply_loss: AtomicBool::new(false),
        })
    }
}
#[async_trait]
impl FencedResource for Intercept {
    fn id(&self) -> ParticipantId {
        self.inner.id()
    }
    fn domain(&self) -> Domain {
        self.inner.domain()
    }
    async fn call(
        &self,
        grant: &Grant,
        tx: &TransactionId,
        op: Operation,
    ) -> Result<Reply, ParticipantError> {
        if matches!(op, Operation::Prepare(_)) && self.pause_prepare {
            let result = self.inner.call(grant, tx, op).await;
            self.reached.notify_one();
            self.resume.notified().await;
            return result;
        }
        if matches!(op, Operation::Commit(_)) {
            if self.pause_commit {
                self.reached.notify_one();
                self.resume.notified().await;
            }
            let result = self.inner.call(grant, tx, op).await;
            if self.commit_reply_loss.load(Ordering::SeqCst) {
                return Err(ParticipantError::message("reply lost after effect".into()));
            }
            return result;
        }
        self.inner.call(grant, tx, op).await
    }
}

#[tokio::test]
async fn takeover_fences_old_owner_waiting_on_prepare_and_preserves_abort() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock.clone());
    let counter = db.counter(pid()).unwrap();
    let gate = Intercept::new(counter.clone(), true, false);
    let old = db.acquire("old", ttl()).unwrap();
    let mut owner = coordinator(&db, old, gate.clone());
    let executing = tokio::spawn(async move {
        owner
            .execute(tid(), requests(), &Cancellation::default())
            .await
    });
    gate.reached.notified().await;
    clock.advance();
    let new = db.acquire("new", ttl()).unwrap();
    let mut takeover = coordinator(&db, new, counter.clone());
    let result = takeover.recover(&Cancellation::default()).await.unwrap();
    assert_eq!(result[0].decision, Some(Decision::Abort));
    assert_eq!(result[0].outcome, Outcome::Aborted);
    gate.resume.notify_one();
    assert!(matches!(
        executing.await.unwrap(),
        Err(Error::InDoubt { .. })
    ));
    assert_eq!(counter.value().unwrap(), 0);
    assert_eq!(
        takeover.status(&tid()).unwrap().unwrap().decision,
        Some(Decision::Abort)
    );
}

#[tokio::test]
async fn takeover_repeats_durable_commit_and_rejects_delayed_old_commit() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock.clone());
    let counter = db.counter(pid()).unwrap();
    let gate = Intercept::new(counter.clone(), false, true);
    let old = db.acquire("old", ttl()).unwrap();
    let mut owner = coordinator(&db, old, gate.clone());
    let executing = tokio::spawn(async move {
        owner
            .execute(tid(), requests(), &Cancellation::default())
            .await
    });
    gate.reached.notified().await;
    clock.advance();
    let new = db.acquire("new", ttl()).unwrap();
    let mut takeover = coordinator(&db, new, counter.clone());
    assert_eq!(
        takeover.recover(&Cancellation::default()).await.unwrap()[0].outcome,
        Outcome::Committed
    );
    gate.resume.notify_one();
    // Old finalization is rejected; old acknowledgement cannot cross the fenced journal barrier.
    let result = executing.await.unwrap();
    assert!(result.is_err() || result.unwrap().outcome == Outcome::InDoubt);
    assert_eq!(counter.value().unwrap(), 7);
    assert_eq!(
        takeover.status(&tid()).unwrap().unwrap().decision,
        Some(Decision::Commit)
    );
}

#[tokio::test]
async fn commit_effect_with_lost_ack_and_partitions_remains_pending_until_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock.clone());
    let counter = db.counter(pid()).unwrap();
    let lost = Intercept::new(counter.clone(), false, false);
    lost.commit_reply_loss.store(true, Ordering::SeqCst);
    let old = db.acquire("old", ttl()).unwrap();
    let mut owner = coordinator(&db, old, lost);
    let pending = owner
        .execute(tid(), requests(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(pending.decision, Some(Decision::Commit));
    assert_eq!(pending.outcome, Outcome::InDoubt);
    assert_eq!(counter.value().unwrap(), 7);
    clock.advance();
    let new = db.acquire("new", ttl()).unwrap();
    let link = Arc::new(Loopback::new(counter.clone()));
    link.set_partitioned(true);
    let remote = Arc::new(
        RemoteResource::new(
            pid(),
            LeaseAuthority::domain(&db),
            link.clone(),
            Duration::from_millis(100),
        )
        .unwrap(),
    );
    let mut takeover = coordinator(&db, new, remote);
    assert_eq!(
        takeover.recover(&Cancellation::default()).await.unwrap()[0].outcome,
        Outcome::InDoubt
    );
    link.set_partitioned(false);
    use pactum_core::recovery::RecoveryTarget;
    assert_eq!(takeover.pending_work().unwrap().len(), 1);
    assert!(takeover.replay_position() > 0);
    assert!(
        takeover
            .recover_one(tid().as_str(), &Cancellation::default())
            .await
            .unwrap()
            .is_terminal()
    );
    assert!(takeover.pending_work().unwrap().is_empty());
    assert_eq!(
        takeover.recover(&Cancellation::default()).await.unwrap()[0].outcome,
        Outcome::Committed
    );
    assert_eq!(counter.value().unwrap(), 7);
}

#[tokio::test]
async fn protocol_rejects_tampering_deadlines_wrong_domains_and_delayed_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock.clone());
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("owner", ttl()).unwrap();
    let endpoint = Endpoint::new(counter.clone());
    let request = Request::new(
        grant.clone(),
        pid(),
        tid(),
        Operation::Prepare(b"7".to_vec()),
        u64::MAX,
    )
    .unwrap();
    let mut retry = request.clone();
    retry.deadline_millis -= 1;
    assert_eq!(retry.identity, request.identity);
    let mut bad = request.clone();
    bad.version = 900;
    assert!(endpoint.handle(bad).await.is_err());
    let mut bad = request.clone();
    bad.operation = Operation::Prepare(b"99".to_vec());
    assert!(endpoint.handle(bad).await.is_err());
    let mut bad = request.clone();
    bad.deadline_millis = 0;
    assert!(endpoint.handle(bad).await.is_err());
    let bad = Request::new(
        grant.clone(),
        ParticipantId::new("wrong").unwrap(),
        tid(),
        Operation::Status,
        u64::MAX,
    )
    .unwrap();
    assert!(endpoint.handle(bad).await.is_err());
    assert_eq!(counter.value().unwrap(), 0);
    counter
        .call(&grant, &tid(), Operation::Abort(None))
        .await
        .unwrap();
    assert!(matches!(
        endpoint.handle(request).await.unwrap().result,
        Ok(Reply::Rejected(_))
    ));
    let other =
        ReferenceStore::open_with_clock(dir.path().join("other.db"), "scope", clock).unwrap();
    assert!(
        DistributedCoordinator::open(&other, grant.clone(), vec![counter.clone()], policy())
            .is_err()
    );
    let other_resource = other.counter(pid()).unwrap();
    assert!(DistributedCoordinator::open(&db, grant, vec![other_resource], policy()).is_err());
}

struct CorruptTransport(Arc<Loopback>);
#[async_trait]
impl Transport for CorruptTransport {
    async fn exchange(&self, request: Request) -> Result<Response, TransportError> {
        let mut result = self.0.exchange(request).await?;
        result.identity.clear();
        Ok(result)
    }
}
#[tokio::test]
async fn mismatched_response_is_uncertain_even_when_effect_was_durable() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock);
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("owner", ttl()).unwrap();
    let link = Arc::new(CorruptTransport(Arc::new(Loopback::new(counter.clone()))));
    let remote = RemoteResource::new(pid(), counter.domain(), link, ttl()).unwrap();
    assert!(
        remote
            .call(&grant, &tid(), Operation::Prepare(b"7".to_vec()))
            .await
            .is_err()
    );
    assert!(matches!(
        counter
            .call(&grant, &tid(), Operation::Status)
            .await
            .unwrap(),
        Reply::Prepared(_)
    ));
    assert_eq!(counter.value().unwrap(), 0);
}

// A real process exits without destructors after the selected protocol boundary.
struct CrashResource {
    inner: Arc<ReferenceCounter>,
    boundary: String,
}
#[async_trait]
impl FencedResource for CrashResource {
    fn id(&self) -> ParticipantId {
        self.inner.id()
    }
    fn domain(&self) -> Domain {
        self.inner.domain()
    }
    async fn call(
        &self,
        grant: &Grant,
        tx: &TransactionId,
        op: Operation,
    ) -> Result<Reply, ParticipantError> {
        let prepare = matches!(op, Operation::Prepare(_));
        let commit = matches!(op, Operation::Commit(_));
        if commit && self.boundary == "decision" {
            std::process::exit(73);
        }
        let reply = self.inner.call(grant, tx, op).await;
        if (prepare && self.boundary == "prepare") || (commit && self.boundary == "effect") {
            std::process::exit(73);
        }
        reply
    }
}
#[test]
fn crash_worker() {
    let Ok(path) = std::env::var("PACTUM_DISTRIBUTED_CRASH_DB") else {
        return;
    };
    let boundary = std::env::var("PACTUM_DISTRIBUTED_CRASH_BOUNDARY").unwrap();
    let clock = TestClock::new();
    let db = store(std::path::Path::new(&path), clock);
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("crashing", ttl()).unwrap();
    if boundary == "acquire" {
        std::process::exit(73);
    }
    if boundary == "staged" {
        let mut journal = db.open_journal(&grant).unwrap();
        journal.append(1, b"must never be replayed").unwrap();
        std::process::exit(73);
    }
    let checkpoint = boundary == "checkpoint";
    let resource = Arc::new(CrashResource {
        inner: counter,
        boundary,
    });
    let mut owner = coordinator(&db, grant, resource);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        owner
            .execute(tid(), requests(), &Cancellation::default())
            .await
            .unwrap();
    });
    if checkpoint {
        owner.compact(Retention::Compact).unwrap();
        std::process::exit(73);
    }
    panic!("crash hook was not reached");
}
#[test]
fn actual_process_death_reopens_authority_journal_and_resources_without_reversing_decisions() {
    for boundary in [
        "acquire",
        "staged",
        "prepare",
        "decision",
        "effect",
        "checkpoint",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.db");
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_worker", "--nocapture"])
            .env("PACTUM_DISTRIBUTED_CRASH_DB", &path)
            .env("PACTUM_DISTRIBUTED_CRASH_BOUNDARY", boundary)
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(73), "{result:?}");
        let clock = TestClock::new();
        clock.advance();
        let db = store(&path, clock);
        let counter = db.counter(pid()).unwrap();
        let grant = db.acquire("recovery", ttl()).unwrap();
        assert_eq!(grant.epoch, 2);
        let mut owner = coordinator(&db, grant, counter.clone());
        let result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(owner.recover(&Cancellation::default()))
            .unwrap();
        if boundary == "acquire" || boundary == "staged" {
            assert!(result.is_empty());
            assert_eq!(counter.value().unwrap(), 0);
            continue;
        }
        assert_eq!(
            result[0].outcome,
            if boundary == "prepare" {
                Outcome::Aborted
            } else {
                Outcome::Committed
            }
        );
        assert_eq!(
            counter.value().unwrap(),
            if boundary == "prepare" { 0 } else { 7 }
        );
    }
}

struct SlowReply(Arc<Loopback>);
#[async_trait]
impl Transport for SlowReply {
    async fn exchange(&self, request: Request) -> Result<Response, TransportError> {
        let prepare = matches!(request.operation, Operation::Prepare(_));
        let response = self.0.exchange(request).await?;
        if prepare {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(response)
    }
}
#[tokio::test]
async fn deadline_after_effect_is_uncertain_and_abort_tombstone_fences_late_prepare() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("service.db"), clock);
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("owner", ttl()).unwrap();
    let remote = Arc::new(
        RemoteResource::new(
            pid(),
            counter.domain(),
            Arc::new(SlowReply(Arc::new(Loopback::new(counter.clone())))),
            Duration::from_millis(10),
        )
        .unwrap(),
    );
    let mut owner = coordinator(&db, grant.clone(), remote);
    let result = owner
        .execute(tid(), requests(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(result.decision, Some(Decision::Abort));
    assert_eq!(result.outcome, Outcome::Aborted);
    assert_eq!(counter.value().unwrap(), 0);
    assert_eq!(
        counter
            .call(&grant, &tid(), Operation::Status)
            .await
            .unwrap(),
        Reply::Aborted
    );
    assert!(matches!(
        counter
            .call(&grant, &tid(), Operation::Prepare(b"7".to_vec()))
            .await
            .unwrap(),
        Reply::Rejected(_)
    ));
}

#[tokio::test]
async fn prepare_reserves_capacity_and_terminal_bindings_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("service.db");
    let clock = TestClock::new();
    let db = store(&path, clock.clone());
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("owner", ttl()).unwrap();
    let max = i64::MAX.to_string().into_bytes();
    let Reply::Prepared(token) = counter
        .call(&grant, &tid(), Operation::Prepare(max.clone()))
        .await
        .unwrap()
    else {
        panic!()
    };
    let second = TransactionId::new("second").unwrap();
    assert!(matches!(
        counter
            .call(&grant, &second, Operation::Prepare(b"1".to_vec()))
            .await
            .unwrap(),
        Reply::Rejected(_)
    ));
    assert!(
        counter
            .call(&grant, &tid(), Operation::Prepare(b"2".to_vec()))
            .await
            .is_err()
    );
    assert!(
        counter
            .call(&grant, &tid(), Operation::Commit(b"wrong".to_vec()))
            .await
            .is_err()
    );
    counter
        .call(&grant, &tid(), Operation::Commit(token.clone()))
        .await
        .unwrap();
    assert!(
        counter
            .call(&grant, &tid(), Operation::Abort(None))
            .await
            .is_err()
    );
    clock.advance();
    let reopened = store(&path, clock);
    let next = reopened.acquire("new", ttl()).unwrap();
    let resource = reopened.counter(pid()).unwrap();
    resource
        .call(&next, &tid(), Operation::Commit(token))
        .await
        .unwrap();
    assert_eq!(resource.value().unwrap(), i64::MAX);
    assert!(matches!(
        resource
            .call(&next, &tid(), Operation::Prepare(max))
            .await
            .unwrap(),
        Reply::Prepared(_)
    ));
}

#[test]
fn journal_truncation_checksum_and_future_schema_fail_closed() {
    for fault in [
        "DELETE FROM distributed_entries",
        "UPDATE distributed_entries SET payload=x'00'",
        "UPDATE distributed_meta SET format=999",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.db");
        let clock = TestClock::new();
        let db = store(&path, clock.clone());
        let grant = db.acquire("owner", ttl()).unwrap();
        let mut journal = db.open_journal(&grant).unwrap();
        journal.append(1, b"value").unwrap();
        journal.flush().unwrap();
        db.release(&grant).unwrap();
        drop(journal);
        drop(db);
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute(fault, [])
            .unwrap();
        let reopened = ReferenceStore::open_with_clock(&path, "scope", clock);
        if fault.contains("format") {
            assert!(reopened.is_err());
            continue;
        }
        let reopened = reopened.unwrap();
        let grant = reopened.acquire("new", ttl()).unwrap();
        assert!(reopened.open_journal(&grant).is_err());
    }
}

#[tokio::test]
async fn remote_codes_and_correlation_survive_partitions_lost_replies_and_stale_epochs() {
    use pactum_core::{ErrorCode, RetryContext, RetryDisposition};
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new();
    let db = store(&dir.path().join("typed-errors.db"), clock.clone());
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("owner", ttl()).unwrap();
    let link = Arc::new(Loopback::new(counter.clone()));
    let remote = RemoteResource::new(pid(), counter.domain(), link.clone(), ttl()).unwrap();
    link.set_partitioned(true);
    let error = remote
        .call(&grant, &tid(), Operation::Prepare(b"7".to_vec()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.context().transaction.as_deref(), Some(tid().as_str()));
    assert_eq!(error.context().phase.as_deref(), Some("prepare"));
    assert_eq!(error.context().request.as_ref().unwrap().len(), 64);
    assert_eq!(error.context().epoch, Some(grant.epoch));
    link.set_partitioned(false);
    link.lose_replies(1);
    let error = remote
        .call(&grant, &tid(), Operation::Prepare(b"7".to_vec()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::TransportUnknown);
    assert_eq!(
        error.retry_disposition(RetryContext {
            authority_valid: true,
            idempotent: true,
            ..Default::default()
        }),
        RetryDisposition::ReconcileBeforeRetry
    );
    assert!(matches!(
        remote
            .call(&grant, &tid(), Operation::Prepare(b"7".to_vec()))
            .await
            .unwrap(),
        Reply::Prepared(_)
    ));
    clock.advance();
    let next = db.acquire("new-owner", ttl()).unwrap();
    let error = remote
        .call(&grant, &tid(), Operation::Status)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::StaleOwner);
    assert_eq!(error.context().epoch, Some(grant.epoch));
    assert_eq!(error.context().request.as_ref().unwrap().len(), 64);
    assert_eq!(
        error.retry_disposition(RetryContext {
            authority_valid: true,
            idempotent: true,
            ..Default::default()
        }),
        RetryDisposition::OperatorIntervention
    );
    assert!(matches!(
        remote.call(&next, &tid(), Operation::Status).await.unwrap(),
        Reply::Prepared(_)
    ));
}
#[tokio::test]
async fn typed_remote_timeout_retains_request_identity_and_version_one_fails_closed() {
    use pactum_core::ErrorCode;
    let dir = tempfile::tempdir().unwrap();
    let db = store(&dir.path().join("timeout-errors.db"), TestClock::new());
    let counter = db.counter(pid()).unwrap();
    let grant = db.acquire("owner", ttl()).unwrap();
    let remote = RemoteResource::new(
        pid(),
        counter.domain(),
        Arc::new(SlowReply(Arc::new(Loopback::new(counter.clone())))),
        Duration::from_millis(10),
    )
    .unwrap();
    let error = remote
        .call(&grant, &tid(), Operation::Prepare(b"7".to_vec()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(error.context().request.as_ref().unwrap().len(), 64);
    assert!(matches!(
        counter
            .call(&grant, &tid(), Operation::Status)
            .await
            .unwrap(),
        Reply::Prepared(_)
    ));
    let mut request = Request::new(grant, pid(), tid(), Operation::Status, u64::MAX).unwrap();
    request.version = 1;
    let error = Endpoint::new(counter).handle(request).await.unwrap_err();
    assert_eq!(error.code(), ErrorCode::ProtocolMismatch);
}
