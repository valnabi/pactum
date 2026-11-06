//! Isolation negotiation, concurrency anomalies and durable receipt recovery.
use async_trait::async_trait;
use pactum_core::{Cancellation, Error, ParticipantId, execution::*, isolation::*, plan::*};
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use pactum_sqlite::SqliteJournal;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

fn pid(value: &str) -> ParticipantId {
    ParticipantId::new(value).unwrap()
}
fn aid() -> AdapterId {
    AdapterId::new("db").unwrap()
}
fn sid(value: &str) -> StepId {
    StepId::new(value).unwrap()
}
fn stamp(value: i64) -> VersionStamp {
    VersionStamp::new(value.to_le_bytes().to_vec()).unwrap()
}
fn caps() -> IsolationCapabilities {
    IsolationCapabilities {
        snapshot: "db-incarnation-v1".into(),
        levels: BTreeSet::from([
            IsolationLevel::Unspecified,
            IsolationLevel::ReadCommitted,
            IsolationLevel::Snapshot,
            IsolationLevel::Serializable,
        ]),
        validation: BTreeSet::from([
            ValidationKind::Reads,
            ValidationKind::Writes,
            ValidationKind::Predicates,
        ]),
        blind_writes: false,
        prepared_protection: true,
        fencing: true,
    }
}
fn resource() -> ResourceIsolation {
    ResourceIsolation {
        requirement: IsolationRequirement {
            level: IsolationLevel::Serializable,
            scope: IsolationScope::ParticipantTransaction,
            validation: BTreeSet::from([
                ValidationKind::Reads,
                ValidationKind::Writes,
                ValidationKind::Predicates,
            ]),
            fencing: true,
        },
        capabilities: caps(),
        footprint: ConflictFootprint::default(),
    }
}
fn contract() -> StepIsolation {
    StepIsolation {
        resources: BTreeMap::from([(pid("db"), resource())]),
    }
}
fn spec(id: &str, key: &str) -> TransactionPlan {
    TransactionPlan {
        version: 1,
        id: PlanId::new(id).unwrap(),
        revision: 1,
        steps: vec![Step {
            id: sid("write"),
            adapter: aid(),
            dependencies: BTreeSet::new(),
            resources: BTreeSet::from(["db".into()]),
            work: StepWork::Native(Request::Atomic(AtomicRequest {
                payload: key.as_bytes().to_vec(),
            })),
        }],
    }
}
fn accept(id: &str, key: &str, c: StepIsolation) -> IsolationPlan {
    IsolationPlan::accept(spec(id, key), BTreeMap::from([(sid("write"), c)])).unwrap()
}
fn compiled(p: TransactionPlan, c: StepIsolation) -> Result<IsolationPlan, Error> {
    IsolationPlan::accept(p, BTreeMap::from([(sid("write"), c)]))
}
#[test]
fn negotiation_exact_coverage_and_invalid_evidence_fail_closed() {
    let work = spec("x", "a").steps.remove(0).work;
    let c = contract();
    c.validate(&work).unwrap();
    for scope in [
        IsolationScope::GlobalSnapshot,
        IsolationScope::GlobalSerializable,
    ] {
        let mut bad = c.clone();
        bad.resources.get_mut(&pid("db")).unwrap().requirement.scope = scope;
        assert!(bad.validate(&work).is_err());
    }
    let mut bad = c.clone();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .capabilities
        .levels
        .remove(&IsolationLevel::Serializable);
    assert!(bad.validate(&work).is_err()); // No implicit promotion.
    let mut bad = c.clone();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .capabilities
        .validation
        .remove(&ValidationKind::Predicates);
    assert!(bad.validate(&work).is_err());
    let mut bad = c.clone();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .capabilities
        .fencing = false;
    assert!(bad.validate(&work).is_err());
    let mut bad = c.clone();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .capabilities
        .snapshot
        .clear();
    assert!(bad.validate(&work).is_err());
    let mut bad = c.clone();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .footprint
        .writes
        .insert("a".into(), None);
    assert!(bad.validate(&work).is_err());
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .capabilities
        .blind_writes = true;
    bad.validate(&work).unwrap();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .requirement
        .validation
        .clear();
    assert!(bad.validate(&work).is_err());
    let mut bad = c.clone();
    bad.resources
        .get_mut(&pid("db"))
        .unwrap()
        .footprint
        .reads
        .insert("\n".into(), stamp(0));
    assert!(bad.validate(&work).is_err());
    let chunk = StepWork::Native(Request::Chunked(ChunkedRequest {
        chunks: vec![vec![1]],
    }));
    assert!(c.validate(&chunk).is_err());
    let mut simple = c.clone();
    simple
        .resources
        .get_mut(&pid("db"))
        .unwrap()
        .requirement
        .validation
        .clear();
    simple.validate(&chunk).unwrap(); // Level is per chunk, never across chunks.
    let publish = StepWork::Native(Request::StagedPublish(PublishRequest {
        target: OperationId::new("target").unwrap(),
        expected_generation: 0,
        payload: vec![1],
    }));
    c.validate(&publish).unwrap();
    assert!(IsolationPlan::accept(spec("x", "a"), BTreeMap::new()).is_err());
    assert!(VersionStamp::new(vec![]).is_err());
    assert!(VersionStamp::new(vec![0; 4097]).is_err());
    assert!(serde_json::from_str::<VersionStamp>("[]").is_err());
}
#[test]
fn mixed_participants_require_exact_reports_and_durable_prepare_protection() {
    let work = StepWork::TwoPhase(vec![
        pactum_core::Enlistment {
            participant: pid("a"),
            payload: vec![1],
        },
        pactum_core::Enlistment {
            participant: pid("b"),
            payload: vec![2],
        },
    ]);
    let mut c = StepIsolation {
        resources: BTreeMap::from([(pid("a"), resource()), (pid("b"), resource())]),
    };
    c.validate(&work).unwrap();
    c.resources
        .get_mut(&pid("b"))
        .unwrap()
        .capabilities
        .prepared_protection = false;
    assert!(c.validate(&work).is_err());
    c.resources
        .get_mut(&pid("b"))
        .unwrap()
        .capabilities
        .prepared_protection = true;
    c.resources.get_mut(&pid("b")).unwrap().capabilities.levels =
        BTreeSet::from([IsolationLevel::ReadCommitted]);
    assert!(c.validate(&work).is_err());
    c.resources.get_mut(&pid("b")).unwrap().requirement.level = IsolationLevel::ReadCommitted;
    c.validate(&work).unwrap(); // Explicit mixed guarantees are permitted.
    c.resources.insert(pid("extra"), resource());
    assert!(c.validate(&work).is_err());
    c.resources.remove(&pid("extra"));
    c.resources.remove(&pid("b"));
    assert!(c.validate(&work).is_err());
}
#[test]
fn accepted_identity_roundtrip_and_untrusted_recipe_validation() {
    let a = accept("identity", "a", contract());
    let bytes = serde_json::to_vec(&a).unwrap();
    assert_eq!(a, serde_json::from_slice(&bytes).unwrap());
    let mut changed = contract();
    changed
        .resources
        .get_mut(&pid("db"))
        .unwrap()
        .footprint
        .reads
        .insert("a".into(), stamp(0));
    let b = accept("identity", "a", changed);
    assert_ne!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    assert_ne!(
        a.transaction_plan().operation_key(&sid("write")).unwrap(),
        b.transaction_plan().operation_key(&sid("write")).unwrap()
    );
    let mut changed = contract();
    changed
        .resources
        .get_mut(&pid("db"))
        .unwrap()
        .capabilities
        .snapshot = "v2".into();
    assert_ne!(
        a.transaction_plan().fingerprint().unwrap(),
        accept("identity", "a", changed)
            .transaction_plan()
            .fingerprint()
            .unwrap()
    );
    let mut raw = serde_json::to_value(&a).unwrap();
    raw["version"] = 2.into();
    assert!(serde_json::from_value::<IsolationPlan>(raw).is_err());
    let mut raw = serde_json::to_value(&a).unwrap();
    raw["contracts"]["write"]["resources"]["db"]["requirement"]["scope"] =
        "GlobalSerializable".into();
    assert!(serde_json::from_value::<IsolationPlan>(raw).is_err());
    let mut raw = serde_json::to_value(&a).unwrap();
    raw["extra"] = true.into();
    assert!(serde_json::from_value::<IsolationPlan>(raw).is_err());
}

// Real local transaction barrier: compare evidence, effect, and receipt under one
// SQLite immediate transaction. This is a test adapter, not a generic SQL adapter.
struct Database {
    connection: Mutex<Connection>,
    reports: Mutex<BTreeMap<ParticipantId, IsolationCapabilities>>,
    lose_ack: AtomicBool,
    stale_before_barrier: AtomicBool,
    unreadable: AtomicBool,
}
fn sql(error: rusqlite::Error) -> Error {
    Error::in_doubt_source(error)
}
impl Database {
    fn open(path: &Path) -> Self {
        let c = Connection::open(path).unwrap();
        c.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS data(key TEXT PRIMARY KEY, value INTEGER NOT NULL, version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY, generation INTEGER NOT NULL, epoch INTEGER NOT NULL);
            INSERT OR IGNORE INTO meta VALUES(1,0,1);
            CREATE TABLE IF NOT EXISTS receipts(key TEXT PRIMARY KEY, binding BLOB NOT NULL, applied INTEGER NOT NULL);
            INSERT OR IGNORE INTO data VALUES('a',0,0),('b',0,0);").unwrap();
        Self {
            connection: Mutex::new(c),
            reports: Mutex::new(BTreeMap::from([(pid("db"), caps())])),
            lose_ack: AtomicBool::new(false),
            stale_before_barrier: AtomicBool::new(false),
            unreadable: AtomicBool::new(false),
        }
    }
    fn value(&self, key: &str) -> i64 {
        self.connection
            .lock()
            .unwrap()
            .query_row("SELECT value FROM data WHERE key=?1", [key], |r| r.get(0))
            .unwrap()
    }
    fn external_write(&self, key: &str) {
        let mut c = self.connection.lock().unwrap();
        let tx = c.transaction().unwrap();
        tx.execute("INSERT INTO data VALUES(?1,1,1) ON CONFLICT(key) DO UPDATE SET value=value+1,version=version+1",[key]).unwrap();
        tx.execute("UPDATE meta SET generation=generation+1", [])
            .unwrap();
        tx.commit().unwrap();
    }
    fn receipt(
        &self,
        key: &OperationId,
        work: &StepWork,
        c: &StepIsolation,
    ) -> Result<Option<StepResult>, Error> {
        if self.unreadable.load(Ordering::SeqCst) {
            return Err(IsolationFailure::Unknown.into());
        }
        let binding = serde_json::to_vec(&(work, c)).unwrap();
        let found: Option<(Vec<u8>, bool)> = self
            .connection
            .lock()
            .unwrap()
            .query_row(
                "SELECT binding,applied FROM receipts WHERE key=?1",
                [key.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        match found {
            None => Ok(None),
            Some((b, applied)) => {
                if b != binding {
                    return Err(Error::invalid("receipt binding changed".into()));
                }
                Ok(Some(result(applied)))
            }
        }
    }
}
fn result(applied: bool) -> StepResult {
    StepResult::Native(Progress::Atomic(if applied {
        AtomicStatus::Committed
    } else {
        AtomicStatus::Aborted
    }))
}
#[async_trait]
impl IsolationExecutor for Database {
    fn identity(&self) -> String {
        "database-incarnation-v1".into()
    }
    fn accepts(&self, w: &StepWork) -> bool {
        matches!(w, StepWork::Native(Request::Atomic(_)))
    }
    fn capabilities(&self) -> Result<BTreeMap<ParticipantId, IsolationCapabilities>, Error> {
        Ok(self.reports.lock().unwrap().clone())
    }
    async fn reconcile(
        &self,
        key: &OperationId,
        work: &StepWork,
        c: &StepIsolation,
        _: &Cancellation,
    ) -> Result<Option<StepResult>, Error> {
        self.receipt(key, work, c)
    }
    async fn execute_validated(
        &self,
        key: OperationId,
        work: StepWork,
        c: &StepIsolation,
        _: &Cancellation,
    ) -> Result<StepResult, Error> {
        // A race after the wrapper's preflight is checked again INSIDE the effect barrier.
        if self.stale_before_barrier.swap(false, Ordering::SeqCst) {
            self.reports
                .lock()
                .unwrap()
                .get_mut(&pid("db"))
                .unwrap()
                .snapshot = "changed".into();
        }
        let reports = self.reports.lock().unwrap();
        let mut connection = self.connection.lock().unwrap();
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql)?;
        if *reports != c.capabilities() {
            return Err(IsolationFailure::CapabilityChanged.into());
        }
        let r = &c.resources[&pid("db")];
        let epoch: i64 = tx
            .query_row("SELECT epoch FROM meta", [], |row| row.get(0))
            .map_err(sql)?;
        if r.requirement.fencing && epoch != 1 {
            return Err(IsolationFailure::Fenced.into());
        }
        let binding = serde_json::to_vec(&(&work, c)).unwrap();
        // Another delivery may have committed between lookup and acquiring the lock.
        let old: Option<(Vec<u8>, bool)> = tx
            .query_row(
                "SELECT binding,applied FROM receipts WHERE key=?1",
                [key.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(sql)?;
        if let Some((b, applied)) = old {
            if b != binding {
                return Err(Error::invalid("receipt binding changed".into()));
            }
            return Ok(result(applied));
        }
        let mut valid = true;
        for (key, expected) in r.footprint.reads.iter().chain(
            r.footprint
                .writes
                .iter()
                .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
        ) {
            let current: i64 = tx
                .query_row("SELECT version FROM data WHERE key=?1", [key], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(sql)?
                .unwrap_or(-1);
            valid &= expected == &stamp(current);
        }
        for (predicate, expected) in &r.footprint.predicates {
            if predicate != "all" {
                return Err(Error::invalid("unknown predicate".into()));
            }
            let generation: i64 = tx
                .query_row("SELECT generation FROM meta", [], |row| row.get(0))
                .map_err(sql)?;
            valid &= expected == &stamp(generation);
        }
        let StepWork::Native(Request::Atomic(request)) = &work else {
            unreachable!()
        };
        let target = std::str::from_utf8(&request.payload).unwrap();
        // The fixture's operation is one increment. Reject incomplete write evidence.
        if r.requirement.validation.contains(&ValidationKind::Writes)
            && !r.footprint.writes.contains_key(target)
        {
            return Err(Error::invalid("incomplete write footprint".into()));
        }
        if valid {
            tx.execute(
                "UPDATE data SET value=value+1, version=version+1 WHERE key=?1",
                [target],
            )
            .map_err(sql)?;
            tx.execute("UPDATE meta SET generation=generation+1", [])
                .map_err(sql)?;
        }
        tx.execute(
            "INSERT INTO receipts VALUES(?1,?2,?3)",
            params![key.as_str(), binding, valid],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)?;
        if self.lose_ack.swap(false, Ordering::SeqCst) {
            return Err(IsolationFailure::Unknown.into());
        }
        if std::env::var_os("PACTUM_ISOLATION_KILL_AFTER_EFFECT").is_some() {
            std::process::exit(87);
        }
        Ok(result(valid))
    }
}
fn writes(mut c: StepIsolation, key: &str, version: i64) -> StepIsolation {
    c.resources
        .get_mut(&pid("db"))
        .unwrap()
        .footprint
        .writes
        .insert(key.into(), Some(stamp(version)));
    c
}
fn adapters(p: &IsolationPlan, db: Arc<Database>) -> BTreeMap<AdapterId, Arc<dyn StepAdapter>> {
    p.adapters(&BTreeMap::from([(aid(), db as Arc<dyn IsolationExecutor>)]))
        .unwrap()
}
fn engine(path: &Path, p: &IsolationPlan, db: Arc<Database>) -> PlanEngine<SqliteJournal> {
    PlanEngine::open(
        SqliteJournal::open(path).unwrap(),
        PlanId::new("deployment-v1").unwrap(),
        adapters(p, db),
        PlanPolicy::default(),
    )
    .unwrap()
}
async fn run(path: &Path, p: &IsolationPlan, db: Arc<Database>) -> PlanStatus {
    engine(path, p, db)
        .execute(p.transaction_plan().clone(), &Cancellation::default())
        .await
        .unwrap()
}
#[tokio::test]
async fn lost_update_write_skew_phantoms_and_absence_are_rejected() {
    // Deterministic interleavings: both transactions observe v0 before the first commits.
    for anomaly in ["lost_update", "write_skew", "phantom", "absence"] {
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(&temp.path().join("resource.db")));
        let first = accept("first", "a", writes(contract(), "a", 0));
        let target = if anomaly == "lost_update" { "a" } else { "b" };
        let mut second = writes(contract(), target, 0);
        let f = &mut second.resources.get_mut(&pid("db")).unwrap().footprint;
        match anomaly {
            "write_skew" => {
                f.reads.insert("a".into(), stamp(0));
            }
            "phantom" => {
                f.predicates.insert("all".into(), stamp(0));
            }
            "absence" => {
                f.reads.insert("missing".into(), stamp(-1));
            }
            _ => (),
        }
        let second = accept("second", target, second);
        assert_eq!(
            run(&temp.path().join("first.db"), &first, db.clone())
                .await
                .outcome,
            PlanOutcome::Complete
        );
        if anomaly == "absence" {
            db.external_write("missing");
        }
        let status = run(&temp.path().join("second.db"), &second, db.clone()).await;
        assert_eq!(status.outcome, PlanOutcome::PartiallyFailed, "{anomaly}");
        assert_eq!(db.value(target), if target == "a" { 1 } else { 0 });
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_runners_compete_at_resource_barrier_without_local_scope_protection() {
    let temp = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(&temp.path().join("resource.db")));
    let a = accept("a", "a", writes(contract(), "a", 0));
    let b = accept("b", "a", writes(contract(), "a", 0));
    let independent = Arc::new(Database::open(&temp.path().join("resource.db")));
    let path_a = temp.path().join("a.db");
    let path_b = temp.path().join("b.db");
    let (a, b) = tokio::join!(run(&path_a, &a, db.clone()), run(&path_b, &b, independent));
    assert_eq!(
        [a.outcome, b.outcome]
            .iter()
            .filter(|o| **o == PlanOutcome::Complete)
            .count(),
        1
    );
    assert_eq!(db.value("a"), 1);
}
#[tokio::test]
async fn unknown_commit_restart_and_capability_drift_reconcile_same_receipt_first() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("plan.db");
    let resource_path = temp.path().join("resource.db");
    let db = Arc::new(Database::open(&resource_path));
    let p = accept("unknown", "a", writes(contract(), "a", 0));
    db.lose_ack.store(true, Ordering::SeqCst);
    assert_eq!(
        run(&path, &p, db.clone()).await.outcome,
        PlanOutcome::Indeterminate
    );
    assert_eq!(db.value("a"), 1);
    drop(db);
    let db = Arc::new(Database::open(&resource_path));
    db.external_write("a"); // Old validation is stale, but the committed receipt wins.
    db.connection
        .lock()
        .unwrap()
        .execute("UPDATE meta SET epoch=2", [])
        .unwrap();
    db.reports
        .lock()
        .unwrap()
        .get_mut(&pid("db"))
        .unwrap()
        .snapshot = "v2".into();
    let mut e = engine(&path, &p, db.clone());
    assert_eq!(
        e.resume(&p.transaction_plan().id, &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        PlanOutcome::Complete
    );
    assert_eq!(db.value("a"), 2);
    let changed = accept("unknown", "a", writes(contract(), "a", 2));
    assert!(
        e.execute(changed.transaction_plan().clone(), &Cancellation::default())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn capability_races_stale_fences_and_unknown_lookup_cannot_dispatch() {
    for failure in ["drift", "race", "fence", "lookup", "incomplete"] {
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::open(&temp.path().join("resource.db")));
        let p = accept(
            failure,
            "a",
            if failure == "incomplete" {
                contract()
            } else {
                writes(contract(), "a", 0)
            },
        );
        match failure {
            "drift" => {
                db.reports
                    .lock()
                    .unwrap()
                    .get_mut(&pid("db"))
                    .unwrap()
                    .snapshot = "v2".into()
            }
            "race" => db.stale_before_barrier.store(true, Ordering::SeqCst),
            "fence" => {
                db.connection
                    .lock()
                    .unwrap()
                    .execute("UPDATE meta SET epoch=2", [])
                    .unwrap();
            }
            "lookup" => db.unreadable.store(true, Ordering::SeqCst),
            _ => (),
        }
        let status = run(&temp.path().join("plan.db"), &p, db.clone()).await;
        assert_eq!(status.outcome, PlanOutcome::Indeterminate);
        assert_eq!(db.value("a"), 0, "{failure}");
        if failure == "fence" {
            assert!(
                matches!(&status.steps[&sid("write")].report,Some(Report::Operational(f)) if f.code == pactum_core::ErrorCode::StaleOwner)
            );
        }
    }
}
#[tokio::test]
async fn duplicate_delivery_binding_and_permanent_conflict_receipts() {
    let temp = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(&temp.path().join("resource.db")));
    let p = accept("duplicate", "a", writes(contract(), "a", 0));
    let a = adapters(&p, db.clone()).into_values().next().unwrap();
    let key = OperationId::new("permanent").unwrap();
    let work = p.transaction_plan().steps[0].work.clone();
    assert_eq!(
        a.execute(key.clone(), work.clone(), &Cancellation::default())
            .await
            .unwrap(),
        result(true)
    );
    assert_eq!(
        a.execute(key.clone(), work.clone(), &Cancellation::default())
            .await
            .unwrap(),
        result(true)
    );
    assert_eq!(db.value("a"), 1);
    assert!(
        db.receipt(&key, &work, &writes(contract(), "a", 1))
            .is_err()
    );
    let q = accept("rejected", "a", writes(contract(), "a", 0));
    let b = adapters(&q, db.clone()).into_values().next().unwrap();
    let rejected = OperationId::new("rejected").unwrap();
    assert_eq!(
        b.execute(rejected.clone(), work.clone(), &Cancellation::default())
            .await
            .unwrap(),
        result(false)
    );
    // Even if versions could match again, the retained rejection remains final.
    db.connection
        .lock()
        .unwrap()
        .execute("UPDATE data SET version=0", [])
        .unwrap();
    assert_eq!(
        b.execute(rejected, work, &Cancellation::default())
            .await
            .unwrap(),
        result(false)
    );
    assert_eq!(db.value("a"), 1);
}

// Faults at all before/after append/flush barriers, with actual process exit.
struct CrashJournal {
    inner: SqliteJournal,
    target: usize,
    count: usize,
}
impl CrashJournal {
    fn tick(&mut self) {
        self.count += 1;
        if self.count == self.target {
            std::process::exit(86);
        }
    }
}
impl Journal for CrashJournal {
    fn append(&mut self, v: u32, b: &[u8]) -> Result<Sequence, JournalError> {
        self.tick();
        let s = self.inner.append(v, b)?;
        self.tick();
        Ok(s)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.tick();
        let s = self.inner.flush()?;
        self.tick();
        Ok(s)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.inner.replay()
    }
}
#[tokio::test]
async fn isolation_crash_child() {
    let Some(path) = std::env::var_os("PACTUM_ISOLATION_CHILD") else {
        return;
    };
    let path = Path::new(&path);
    let db = Arc::new(Database::open(&path.join("resource.db")));
    let p = accept("crash", "a", writes(contract(), "a", 0));
    let target = std::env::var("PACTUM_ISOLATION_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let mut e = PlanEngine::open(
        CrashJournal {
            inner: SqliteJournal::open(path.join("plan.db")).unwrap(),
            target,
            count: 0,
        },
        PlanId::new("deployment-v1").unwrap(),
        adapters(&p, db),
        PlanPolicy::default(),
    )
    .unwrap();
    if std::env::var_os("PACTUM_ISOLATION_PRIORITY").is_some() {
        e.configure_scheduler(
            Arc::new(pactum_core::scheduler::PriorityFairScheduler::default()),
            pactum_core::scheduler::SchedulerLimits::default(),
        )
        .unwrap();
    }
    e.execute(p.transaction_plan().clone(), &Cancellation::default())
        .await
        .unwrap();
    panic!("fault point was not reached");
}
#[tokio::test]
async fn process_death_at_plan_barriers_and_effect_receipt_boundary() {
    for priority in [false, true] {
        for target in 1..=17 {
            let temp = tempfile::tempdir().unwrap();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "isolation_crash_child", "--nocapture"])
                .env("PACTUM_ISOLATION_CHILD", temp.path())
                .env(
                    "PACTUM_ISOLATION_TARGET",
                    if target == 17 { 0 } else { target }.to_string(),
                );
            if target == 17 {
                command.env("PACTUM_ISOLATION_KILL_AFTER_EFFECT", "1");
            }
            if priority {
                command.env("PACTUM_ISOLATION_PRIORITY", "1");
            }
            let output = command.output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(if target == 17 { 87 } else { 86 }),
                "target {target}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let db = Arc::new(Database::open(&temp.path().join("resource.db")));
            let p = accept("crash", "a", writes(contract(), "a", 0));
            assert_eq!(
                run(&temp.path().join("plan.db"), &p, db.clone())
                    .await
                    .outcome,
                PlanOutcome::Complete
            );
            assert_eq!(db.value("a"), 1, "target {target}");
        }
    }
}
#[test]
fn existing_planner_output_can_be_negotiated_without_changing_its_contracts() {
    use pactum_core::planner::*;
    let intent = TransactionIntent {
        id: PlanId::new("planner-isolation").unwrap(),
        revision: 1,
        adapter: aid(),
        consistency: Consistency::NativeAtomic,
        work: IntentWork::Native {
            units: vec![WorkUnit {
                payload: vec![1],
                estimate: PlanEstimate::default(),
            }],
            publication: None,
        },
    };
    let context = PlanningContext {
        capabilities: BTreeMap::from([(
            aid(),
            ResourceCapabilities {
                snapshot: "planner-provider-v1".into(),
                modes: vec![Mode::Atomic],
                planner_envelope_v1: true,
                budget: ResourceBudget::default(),
                overhead: PlanEstimate::default(),
                safety_margin_bps: 0,
                extensions: BTreeMap::new(),
            },
        )]),
        budget: ResourceBudget::default(),
    };
    let planned = DefaultTransactionPlanner.plan(&intent, &context).unwrap();
    let contracts = planned
        .transaction_plan()
        .steps
        .iter()
        .map(|s| (s.id.clone(), contract()))
        .collect();
    let isolated = IsolationPlan::accept(planned.transaction_plan().clone(), contracts).unwrap();
    assert_eq!(
        isolated.transaction_plan().steps[0].work,
        planned.transaction_plan().steps[0].work
    );
    assert_ne!(
        isolated.transaction_plan().fingerprint().unwrap(),
        planned.transaction_plan().fingerprint().unwrap()
    );
}
#[tokio::test]
async fn snapshot_without_read_validation_does_not_claim_write_skew_prevention() {
    let temp = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(&temp.path().join("resource.db")));
    let mut c = contract();
    let r = c.resources.get_mut(&pid("db")).unwrap();
    r.requirement.level = IsolationLevel::Snapshot;
    r.requirement.validation.clear();
    // Both callers can have observed the old a+b state. Independent writes do
    // not conflict under this negotiated contract, so no global invariant follows.
    let a = accept("snapshot-a", "a", c.clone());
    let b = accept("snapshot-b", "b", c);
    assert_eq!(
        run(&temp.path().join("a.db"), &a, db.clone()).await.outcome,
        PlanOutcome::Complete
    );
    assert_eq!(
        run(&temp.path().join("b.db"), &b, db.clone()).await.outcome,
        PlanOutcome::Complete
    );
    assert_eq!(db.value("a") + db.value("b"), 2);
}

// The isolation extension routes 2PC through the existing coordinator. This
// fixture requests only guarantees of the original additive counter; it does
// not retrofit optimistic validation or a global snapshot onto that adapter.
struct TwoPhaseExecutor {
    coordinator: tokio::sync::Mutex<pactum_core::Coordinator<SqliteJournal>>,
    contract: StepIsolation,
    work: StepWork,
    drift: AtomicBool,
    lose_ack: AtomicBool,
}
impl TwoPhaseExecutor {
    fn open(
        path: &Path,
        c: StepIsolation,
        w: StepWork,
        participants: Vec<Arc<dyn pactum_core::Participant>>,
    ) -> Self {
        Self {
            coordinator: tokio::sync::Mutex::new(
                pactum_core::Coordinator::open(
                    SqliteJournal::open(path).unwrap(),
                    participants,
                    pactum_core::RetryPolicy::default(),
                )
                .unwrap(),
            ),
            contract: c,
            work: w,
            drift: AtomicBool::new(false),
            lose_ack: AtomicBool::new(false),
        }
    }
    fn binding(&self, w: &StepWork, c: &StepIsolation) -> Result<(), Error> {
        if w != &self.work || c != &self.contract {
            return Err(Error::invalid("2PC isolation binding changed".into()));
        }
        Ok(())
    }
    fn result(status: pactum_core::Status) -> StepResult {
        StepResult::TwoPhase {
            decision: status.decision,
            complete: status.outcome != pactum_core::Outcome::InDoubt,
        }
    }
}
#[async_trait]
impl IsolationExecutor for TwoPhaseExecutor {
    fn identity(&self) -> String {
        "retained-coordinator-incarnation-v1".into()
    }
    fn accepts(&self, w: &StepWork) -> bool {
        w == &self.work
    }
    fn capabilities(&self) -> Result<BTreeMap<ParticipantId, IsolationCapabilities>, Error> {
        let mut c = self.contract.capabilities();
        if self.drift.load(Ordering::SeqCst) {
            for r in c.values_mut() {
                r.snapshot = "restarted-provider-v2".into();
            }
        }
        Ok(c)
    }
    async fn reconcile(
        &self,
        key: &OperationId,
        w: &StepWork,
        c: &StepIsolation,
        token: &Cancellation,
    ) -> Result<Option<StepResult>, Error> {
        self.binding(w, c)?;
        let mut coordinator = self.coordinator.lock().await;
        let id = pactum_core::TransactionId::new(key.as_str())?;
        if coordinator.status(&id)?.is_none() {
            return Ok(None);
        }
        // Existing recovery owns decision reconciliation, including interrupted
        // undecided work. Validation may not invent a replacement decision.
        coordinator.recover(token).await?;
        Ok(coordinator.status(&id)?.map(Self::result))
    }
    async fn execute_validated(
        &self,
        key: OperationId,
        w: StepWork,
        c: &StepIsolation,
        token: &Cancellation,
    ) -> Result<StepResult, Error> {
        self.binding(&w, c)?;
        let StepWork::TwoPhase(requests) = w else {
            unreachable!()
        };
        if self.capabilities()? != c.capabilities() {
            return Err(IsolationFailure::CapabilityChanged.into());
        }
        let status = self
            .coordinator
            .lock()
            .await
            .execute(
                pactum_core::TransactionId::new(key.as_str())?,
                requests,
                token,
            )
            .await?;
        if self.lose_ack.swap(false, Ordering::SeqCst) {
            return Err(IsolationFailure::Unknown.into());
        }
        Ok(Self::result(status))
    }
}
#[tokio::test]
async fn existing_two_phase_decision_survives_lost_ack_restart_and_changed_capabilities() {
    use pactum_core::Participant;
    let temp = tempfile::tempdir().unwrap();
    let a =
        Arc::new(pactum_demo::DurableCounter::open(temp.path().join("a.db"), pid("a")).unwrap());
    let b =
        Arc::new(pactum_demo::DurableCounter::open(temp.path().join("b.db"), pid("b")).unwrap());
    let participants = vec![
        a.clone() as Arc<dyn Participant>,
        b.clone() as Arc<dyn Participant>,
    ];
    let mut c = StepIsolation {
        resources: BTreeMap::new(),
    };
    for (id, level) in [
        ("a", IsolationLevel::ReadCommitted),
        ("b", IsolationLevel::Unspecified),
    ] {
        let mut r = resource();
        r.requirement.level = level;
        r.requirement.validation.clear();
        r.requirement.fencing = false;
        r.capabilities.validation.clear();
        r.capabilities.fencing = false;
        r.capabilities.levels =
            BTreeSet::from([IsolationLevel::Unspecified, IsolationLevel::ReadCommitted]);
        c.resources.insert(pid(id), r);
    }
    let mut p = spec("2pc", "a");
    let work = StepWork::TwoPhase(vec![
        pactum_core::Enlistment {
            participant: pid("a"),
            payload: b"1".to_vec(),
        },
        pactum_core::Enlistment {
            participant: pid("b"),
            payload: b"2".to_vec(),
        },
    ]);
    p.steps[0].work = work.clone();
    let p = compiled(p, c.clone()).unwrap();
    let coordinator_path = temp.path().join("coordinator.db");
    let plan_path = temp.path().join("plan.db");
    let first = Arc::new(TwoPhaseExecutor::open(
        &coordinator_path,
        c.clone(),
        work.clone(),
        participants.clone(),
    ));
    first.lose_ack.store(true, Ordering::SeqCst);
    let mut e = PlanEngine::open(
        SqliteJournal::open(&plan_path).unwrap(),
        PlanId::new("deployment-v1").unwrap(),
        p.adapters(&BTreeMap::from([(
            aid(),
            first as Arc<dyn IsolationExecutor>,
        )]))
        .unwrap(),
        PlanPolicy::default(),
    )
    .unwrap();
    assert_eq!(
        e.execute(p.transaction_plan().clone(), &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        PlanOutcome::Indeterminate
    );
    drop(e);
    let resumed = Arc::new(TwoPhaseExecutor::open(
        &coordinator_path,
        c,
        work,
        participants,
    ));
    resumed.drift.store(true, Ordering::SeqCst);
    let mut e = PlanEngine::open(
        SqliteJournal::open(&plan_path).unwrap(),
        PlanId::new("deployment-v1").unwrap(),
        p.adapters(&BTreeMap::from([(
            aid(),
            resumed as Arc<dyn IsolationExecutor>,
        )]))
        .unwrap(),
        PlanPolicy::default(),
    )
    .unwrap();
    let status = e
        .resume(&p.transaction_plan().id, &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(status.outcome, PlanOutcome::Complete);
    assert_eq!(a.value().await.unwrap(), 1);
    assert_eq!(b.value().await.unwrap(), 2);
    assert!(matches!(
        status.steps[&sid("write")].report,
        Some(Report::Result(StepResult::TwoPhase {
            decision: Some(pactum_core::Decision::Commit),
            complete: true
        }))
    ));
}
