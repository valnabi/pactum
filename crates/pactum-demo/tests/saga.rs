//! Saga contracts, restart reconciliation, failure boundaries and process death.
#[path = "../examples/support/saga.rs"]
mod support;
use async_trait::async_trait;
use pactum_core::{
    Cancellation, Error,
    execution::OperationId,
    recovery::{RecoveryTarget, Work},
    saga::*,
};
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use pactum_sqlite::SqliteJournal;
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::{ExternalStub, aid, ns, operation, sid, spec};

#[test]
fn validates_dags_bounds_versions_and_immutable_compensation_bindings() {
    let p = spec("p", true, false, false);
    assert_eq!(
        p.validate().unwrap(),
        vec![sid("reserve"), sid("ship"), sid("notify")]
    );
    let mut q = p.clone();
    q.steps.reverse();
    assert_eq!(p.fingerprint().unwrap(), q.fingerprint().unwrap());
    q.steps[0].compensation = Compensation::Operation(operation("add:123"));
    assert_ne!(p.fingerprint().unwrap(), q.fingerprint().unwrap());
    let invalid = |p: Saga| assert!(p.validate().is_err());
    q = p.clone();
    q.version = 2;
    invalid(q);
    q = p.clone();
    q.revision = 0;
    invalid(q);
    q = p.clone();
    q.retry_limit = 0;
    invalid(q);
    q = p.clone();
    q.steps.clear();
    invalid(q);
    q = p.clone();
    q.steps.push(q.steps[0].clone());
    invalid(q);
    q = p.clone();
    q.steps[0].dependencies.insert(sid("missing"));
    invalid(q);
    q = p.clone();
    q.steps[0].dependencies.insert(sid("notify"));
    invalid(q);
    q = p.clone();
    q.steps[0].compensation = Compensation::Irreversible;
    invalid(q);
    q = p.clone();
    q.steps = (0..1025)
        .map(|n| {
            let mut s = p.steps[0].clone();
            s.id = sid(&format!("s{n}"));
            s
        })
        .collect();
    invalid(q);
    q = p.clone();
    q.steps[0].forward.payload = vec![255; 5 * 1024 * 1024];
    invalid(q);
    assert!(spec("p", true, false, true).validate().is_ok());
}

#[tokio::test]
async fn forward_completion_and_reverse_dependency_compensation_are_idempotent() {
    for failure in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
        let mut engine = support::open(dir.path(), stub.clone());
        let p = spec("p", failure, false, false);
        let status = engine
            .execute(p.clone(), &Cancellation::default())
            .await
            .unwrap();
        assert_eq!(
            status.outcome,
            if failure {
                SagaOutcome::Compensated
            } else {
                SagaOutcome::Completed
            }
        );
        assert_eq!(
            stub.effects(),
            if failure {
                vec![10, 20, -20, -10]
            } else {
                vec![10, 20]
            }
        );
        assert_ne!(
            engine
                .operation_key(&p.id, &sid("reserve"), Direction::Forward)
                .unwrap(),
            engine
                .operation_key(&p.id, &sid("reserve"), Direction::Compensation)
                .unwrap()
        );
        assert_eq!(
            engine
                .execute(p.clone(), &Cancellation::default())
                .await
                .unwrap(),
            status
        );
        assert!(engine.pending_work().unwrap().is_empty());
        drop(engine);
        let mut engine = support::open(dir.path(), stub.clone());
        assert_eq!(engine.status(&p.id).unwrap().unwrap(), status);
        assert_eq!(
            engine
                .resume(&p.id, &Cancellation::default())
                .await
                .unwrap(),
            status
        );
        assert_eq!(stub.balance(), if failure { 0 } else { 30 });
    }
}

#[tokio::test]
async fn fan_in_compensates_in_reverse_topological_order_without_dispatching_descendants() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let mut engine = support::open(dir.path(), stub.clone());
    let step = |name: &str, deps: &[&str], value: i64| Step {
        id: sid(name),
        dependencies: deps.iter().map(|s| sid(s)).collect(),
        forward: operation(&format!("add:{value}")),
        compensation: Compensation::Operation(operation(&format!("add:{}", -value))),
    };
    let mut p = spec("p", false, false, false);
    p.steps = vec![
        step("root", &[], 1),
        step("left", &["root"], 2),
        step("right", &["root"], 3),
        step("join", &["left", "right"], 4),
        step("fail", &["join"], 0),
        step("blocked", &["fail"], 99),
    ];
    p.steps[4].forward = operation("reject");
    let s = engine.execute(p, &Cancellation::default()).await.unwrap();
    assert_eq!(s.outcome, SagaOutcome::Compensated);
    assert_eq!(s.steps[&sid("blocked")].forward.attempts, 0);
    assert_eq!(stub.effects(), vec![1, 2, 3, 4, -4, -3, -2, -1]);
}

#[tokio::test]
async fn permanent_compensation_failure_and_irreversible_boundary_require_manual_remediation() {
    for irreversible in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
        let p = spec("p", true, !irreversible, irreversible);
        let mut e = support::open(dir.path(), stub.clone());
        let s = e
            .execute(p.clone(), &Cancellation::default())
            .await
            .unwrap();
        assert_eq!(s.outcome, SagaOutcome::NeedsIntervention);
        assert!(s.needs_intervention && !s.is_terminal());
        assert_eq!(stub.effects(), vec![10, 20, -20]);
        assert_eq!(stub.balance(), 10);
        assert_eq!(
            s.steps[&sid("reserve")].compensation.attempts,
            if irreversible { 0 } else { 1 }
        );
        assert!(s.last_error.as_ref().unwrap().contains(if irreversible {
            "irreversible"
        } else {
            "compensation failed"
        }));
        assert!(e.retry(&p.id, "cannot bypass permanent boundary").is_err());
        let failure = e
            .recover_one("p", &Cancellation::default())
            .await
            .unwrap_err();
        assert_eq!(failure.code(), pactum_core::ErrorCode::OperatorRequired);
        assert_eq!(failure.context().transaction.as_deref(), Some("p"));
        assert_eq!(
            failure.retry_disposition(pactum_core::RetryContext {
                authority_valid: true,
                idempotent: true,
                ..Default::default()
            }),
            pactum_core::RetryDisposition::OperatorIntervention
        );
        assert_eq!(e.pending_work().unwrap().len(), 1);
        drop(e);
        let mut e = support::open(dir.path(), stub.clone());
        assert_eq!(
            e.recover_pending(&Cancellation::default()).await.unwrap(),
            vec![s]
        );
        assert_eq!(stub.effects(), vec![10, 20, -20]);
    }
}

#[tokio::test]
async fn exhausted_compensator_retries_require_durable_operator_authorization() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    stub.available(false);
    let mut e = support::open(dir.path(), stub.clone());
    let p = spec("p", true, false, false);
    let s = e
        .execute(p.clone(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(s.outcome, SagaOutcome::NeedsIntervention);
    assert_eq!(s.steps[&sid("reserve")].compensation.attempts, 3);
    assert_eq!(e.resume(&p.id, &Cancellation::default()).await.unwrap(), s);
    stub.available(true);
    assert_eq!(e.resume(&p.id, &Cancellation::default()).await.unwrap(), s);
    assert!(e.retry(&p.id, "").is_err());
    e.retry(&p.id, "operator repaired external service")
        .unwrap();
    drop(e);
    let mut e = support::open(dir.path(), stub.clone());
    let Work::Saga(s) = e.recover_one("p", &Cancellation::default()).await.unwrap() else {
        panic!()
    };
    assert_eq!(s.outcome, SagaOutcome::Compensated);
    assert_eq!(s.steps[&sid("reserve")].compensation.attempts, 4);
    assert_eq!(s.steps[&sid("reserve")].compensation.budget_used, 1);
    assert_eq!(stub.effects(), vec![10, 20, -20, -10]);
    assert!(e.pending_work().unwrap().is_empty());
}

struct Ambiguous {
    inner: Arc<ExternalStub>,
    calls: AtomicUsize,
    faults: Mutex<BTreeMap<Vec<u8>, usize>>,
}
#[async_trait]
impl SagaAdapter for Ambiguous {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn accepts(&self, p: &[u8]) -> bool {
        self.inner.accepts(p)
    }
    async fn execute(
        &self,
        key: OperationId,
        payload: Vec<u8>,
    ) -> Result<ActionResult, pactum_core::saga::SagaError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.execute(key, payload.clone()).await?;
        let mut faults = self.faults.lock().unwrap();
        if let Some(n) = faults.get_mut(&payload)
            && *n > 0
        {
            *n -= 1;
            return Err("lost external acknowledgement".into());
        }
        Ok(result)
    }
}
fn custom_engine(
    dir: &Path,
    adapter: Arc<dyn SagaAdapter>,
    policy: SagaPolicy,
) -> SagaEngine<SqliteJournal> {
    SagaEngine::open(
        SqliteJournal::open(dir.join("saga.db")).unwrap(),
        ns(),
        BTreeMap::from([(aid(), adapter)]),
        policy,
    )
    .unwrap()
}
#[tokio::test]
async fn ambiguous_forward_never_starts_compensation_and_reconciles_same_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let adapter = Arc::new(Ambiguous {
        inner: stub.clone(),
        calls: AtomicUsize::new(0),
        faults: Mutex::new(BTreeMap::from([(b"add:20".to_vec(), 3)])),
    });
    let mut e = custom_engine(dir.path(), adapter.clone(), SagaPolicy::default());
    let p = spec("p", true, false, false);
    let s = e
        .execute(p.clone(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(s.outcome, SagaOutcome::Uncertain);
    assert!(!s.compensating && s.needs_intervention);
    assert_eq!(stub.effects(), vec![10, 20]);
    assert_eq!(s.steps[&sid("notify")].forward.attempts, 0);
    drop(e);
    let mut e = custom_engine(dir.path(), adapter, SagaPolicy::default());
    let mut durable = s;
    assert!(durable.steps[&sid("ship")].forward.diagnostic.is_some());
    for step in durable.steps.values_mut() {
        step.forward.diagnostic = None;
        step.compensation.diagnostic = None;
    }
    assert_eq!(
        e.resume(&p.id, &Cancellation::default()).await.unwrap(),
        durable
    );
    RecoveryTarget::resume_work(&mut e, "p").unwrap();
    let s = e.resume(&p.id, &Cancellation::default()).await.unwrap();
    assert_eq!(s.outcome, SagaOutcome::Compensated);
    assert_eq!(stub.effects(), vec![10, 20, -20, -10]);
    assert_eq!(s.steps[&sid("ship")].forward.attempts, 4);
}

#[tokio::test]
async fn ambiguous_compensation_is_uncertain_and_never_compensates_its_prerequisites_early() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let adapter = Arc::new(Ambiguous {
        inner: stub.clone(),
        calls: AtomicUsize::new(0),
        faults: Mutex::new(BTreeMap::from([(b"add:-20".to_vec(), 3)])),
    });
    let mut e = custom_engine(dir.path(), adapter.clone(), SagaPolicy::default());
    let p = spec("p", true, false, false);
    let s = e
        .execute(p.clone(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(s.outcome, SagaOutcome::Uncertain);
    assert!(s.compensating && s.needs_intervention);
    assert_eq!(s.steps[&sid("reserve")].compensation.attempts, 0);
    assert_eq!(stub.effects(), vec![10, 20, -20]);
    e.retry(&p.id, "reconcile lost compensation acknowledgement")
        .unwrap();
    assert_eq!(
        e.resume(&p.id, &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        SagaOutcome::Compensated
    );
    assert_eq!(stub.effects(), vec![10, 20, -20, -10]);
}

struct Slow {
    inner: Arc<ExternalStub>,
    behavior: AtomicUsize,
}
#[async_trait]
impl SagaAdapter for Slow {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn accepts(&self, p: &[u8]) -> bool {
        self.inner.accepts(p)
    }
    async fn execute(
        &self,
        key: OperationId,
        p: Vec<u8>,
    ) -> Result<ActionResult, pactum_core::saga::SagaError> {
        let r = self.inner.execute(key, p).await?;
        match self.behavior.swap(0, Ordering::SeqCst) {
            1 => tokio::time::sleep(Duration::from_secs(60)).await,
            2 => panic!("injected adapter panic after effect"),
            _ => {}
        }
        Ok(r)
    }
}
#[tokio::test]
async fn cancellation_timeout_and_dropped_future_pause_with_uncertainty_then_resume() {
    for cause in ["cancel", "timeout", "drop"] {
        let dir = tempfile::tempdir().unwrap();
        let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
        let adapter = Arc::new(Slow {
            inner: stub.clone(),
            behavior: AtomicUsize::new(1),
        });
        let mut e = custom_engine(
            dir.path(),
            adapter,
            SagaPolicy {
                max_dispatches: 1,
                timeout: Duration::from_millis(20),
                ..SagaPolicy::default()
            },
        );
        let p = spec("p", true, false, false);
        let c = Cancellation::default();
        c.cancel();
        let s = e.execute(p.clone(), &c).await.unwrap();
        assert_eq!(s.outcome, SagaOutcome::Running);
        assert!(stub.effects().is_empty());
        let c = Cancellation::default();
        match cause {
            "drop" => {
                assert!(
                    tokio::time::timeout(Duration::from_millis(5), e.resume(&p.id, &c))
                        .await
                        .is_err()
                );
            }
            "cancel" => {
                let cancel = c.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    cancel.cancel();
                });
                e.resume(&p.id, &c).await.unwrap();
            }
            _ => {
                e.resume(&p.id, &c).await.unwrap();
            }
        }
        let s = e.status(&p.id).unwrap().unwrap();
        assert_eq!(s.outcome, SagaOutcome::Uncertain);
        assert_eq!(stub.effects(), vec![10]);
        assert!(!s.compensating);
        for _ in 0..8 {
            if e.resume(&p.id, &Cancellation::default())
                .await
                .unwrap()
                .is_terminal()
            {
                break;
            }
        }
        assert_eq!(
            e.status(&p.id).unwrap().unwrap().outcome,
            SagaOutcome::Compensated
        );
        assert_eq!(stub.effects(), vec![10, 20, -20, -10]);
    }
}
#[tokio::test]
async fn adapter_panic_leaves_durable_uncertain_attempt_for_restart() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let adapter = Arc::new(Slow {
        inner: stub.clone(),
        behavior: AtomicUsize::new(2),
    });
    let mut e = custom_engine(dir.path(), adapter, SagaPolicy::default());
    let p = spec("p", true, false, false);
    let join = tokio::spawn(async move { e.execute(p, &Cancellation::default()).await });
    assert!(join.await.unwrap_err().is_panic());
    let mut e = support::open(dir.path(), stub.clone());
    assert_eq!(
        e.status(&SagaId::new("p").unwrap())
            .unwrap()
            .unwrap()
            .outcome,
        SagaOutcome::Uncertain
    );
    assert_eq!(
        e.resume(&SagaId::new("p").unwrap(), &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        SagaOutcome::Compensated
    );
    assert_eq!(stub.effects(), vec![10, 20, -20, -10]);
}

#[tokio::test]
async fn changed_payload_compensator_identity_namespace_and_invalid_policy_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let p = spec("p", false, false, false);
    let mut e = support::open(dir.path(), stub.clone());
    let c = Cancellation::default();
    c.cancel();
    e.execute(p.clone(), &c).await.unwrap();
    let mut changed = p.clone();
    changed.steps[0].compensation = Compensation::Operation(operation("add:-11"));
    assert!(matches!(
        e.execute(changed, &Cancellation::default()).await,
        Err(Error::Invalid { .. })
    ));
    let key = e
        .operation_key(&p.id, &sid("reserve"), Direction::Forward)
        .unwrap();
    drop(e);
    assert!(
        SagaEngine::open(
            SqliteJournal::open(dir.path().join("saga.db")).unwrap(),
            SagaId::new("foreign").unwrap(),
            support::adapters(stub.clone()),
            SagaPolicy::default()
        )
        .is_err()
    );
    let replacement = Arc::new(ExternalStub::open(&dir.path().join("replacement.db")));
    let mut e = custom_engine(dir.path(), replacement.clone(), SagaPolicy::default());
    assert!(matches!(
        e.resume(&p.id, &Cancellation::default()).await,
        Err(Error::Invalid { .. })
    ));
    assert!(replacement.effects().is_empty());
    drop(e);
    let e = support::open(dir.path(), stub.clone());
    assert_eq!(
        e.operation_key(&p.id, &sid("reserve"), Direction::Forward)
            .unwrap(),
        key
    );
    drop(e);
    for policy in [
        SagaPolicy {
            max_dispatches: 0,
            ..SagaPolicy::default()
        },
        SagaPolicy {
            timeout: Duration::ZERO,
            ..SagaPolicy::default()
        },
    ] {
        assert!(
            SagaEngine::open(
                SqliteJournal::open(dir.path().join("saga.db")).unwrap(),
                ns(),
                support::adapters(stub.clone()),
                policy
            )
            .is_err()
        );
    }
    let mut e = SagaEngine::open(
        SqliteJournal::open(dir.path().join("saga.db")).unwrap(),
        ns(),
        BTreeMap::new(),
        SagaPolicy::default(),
    )
    .unwrap();
    assert!(e.resume(&p.id, &Cancellation::default()).await.is_err());
    assert!(stub.effects().is_empty());
}

#[tokio::test]
async fn cooperating_resource_rejects_conflicting_payload_and_duplicate_callbacks() {
    let dir = tempfile::tempdir().unwrap();
    let stub = ExternalStub::open(&dir.path().join("external.db"));
    let key = OperationId::new("external-test-key").unwrap();
    for _ in 0..3 {
        assert_eq!(
            stub.execute(key.clone(), b"add:10".to_vec()).await.unwrap(),
            ActionResult::Applied
        );
    }
    assert!(stub.execute(key, b"add:11".to_vec()).await.is_err());
    assert_eq!(stub.effects(), vec![10]);
}

#[derive(Clone)]
struct Fault {
    count: Arc<AtomicUsize>,
    target: usize,
    kill: bool,
}
impl Fault {
    fn tick(&self) -> Result<(), JournalError> {
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.target {
            if self.kill {
                std::process::exit(86);
            }
            return Err(JournalError::message("lost acknowledgement".into()));
        }
        Ok(())
    }
}
struct FaultJournal {
    inner: SqliteJournal,
    fault: Fault,
}
impl Journal for FaultJournal {
    fn append(&mut self, v: u32, p: &[u8]) -> Result<Sequence, JournalError> {
        self.fault.tick()?;
        let s = self.inner.append(v, p)?;
        self.fault.tick()?;
        Ok(s)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.fault.tick()?;
        let s = self.inner.flush()?;
        self.fault.tick()?;
        Ok(s)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.inner.replay()
    }
}
struct FaultAdapter {
    inner: Arc<ExternalStub>,
    fault: Fault,
}
#[async_trait]
impl SagaAdapter for FaultAdapter {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn accepts(&self, p: &[u8]) -> bool {
        self.inner.accepts(p)
    }
    async fn execute(
        &self,
        key: OperationId,
        p: Vec<u8>,
    ) -> Result<ActionResult, pactum_core::saga::SagaError> {
        self.fault
            .tick()
            .map_err(pactum_core::saga::SagaError::from_source)?;
        let result = self.inner.execute(key, p).await?;
        self.fault
            .tick()
            .map_err(pactum_core::saga::SagaError::from_source)?;
        Ok(result)
    }
}
async fn attempt(dir: &Path, target: usize, kill: bool, scenario: &str) -> usize {
    let fault = Fault {
        count: Arc::new(AtomicUsize::new(0)),
        target,
        kill,
    };
    let stub = Arc::new(ExternalStub::open(&dir.join("external.db")));
    if scenario == "retry" {
        stub.available(false);
    }
    let adapter = Arc::new(FaultAdapter {
        inner: stub.clone(),
        fault: fault.clone(),
    });
    let journal = FaultJournal {
        inner: SqliteJournal::open(dir.join("saga.db")).unwrap(),
        fault: fault.clone(),
    };
    if let Ok(mut e) = SagaEngine::open(
        journal,
        ns(),
        BTreeMap::from([(aid(), adapter as Arc<dyn SagaAdapter>)]),
        SagaPolicy::default(),
    ) {
        let p = spec(
            "p",
            scenario != "success",
            scenario == "permanent",
            scenario == "irreversible",
        );
        let result = async {
            let s = e.execute(p.clone(), &Cancellation::default()).await?;
            if scenario == "retry" && s.needs_intervention {
                stub.available(true);
                e.retry(&p.id, "repair compensator during fault test")?;
                e.resume(&p.id, &Cancellation::default()).await?;
            }
            Ok::<_, Error>(())
        }
        .await;
        if result.is_err() {
            assert!(matches!(e.status(&p.id), Err(Error::InDoubt { .. })));
            assert!(matches!(
                e.resume(&p.id, &Cancellation::default()).await,
                Err(Error::InDoubt { .. })
            ));
        }
    }
    fault.count.load(Ordering::SeqCst)
}
async fn verify(dir: &Path, scenario: &str) {
    let stub = Arc::new(ExternalStub::open(&dir.join("external.db")));
    if scenario == "retry" {
        stub.available(true);
    }
    let mut e = support::open(dir, stub.clone());
    let p = spec(
        "p",
        scenario != "success",
        scenario == "permanent",
        scenario == "irreversible",
    );
    let mut s = e
        .execute(p.clone(), &Cancellation::default())
        .await
        .unwrap();
    if scenario == "retry" && s.needs_intervention {
        e.retry(&p.id, "operator reconciles interrupted remediation")
            .unwrap();
        s = e.resume(&p.id, &Cancellation::default()).await.unwrap();
    }
    let (outcome, effects) = match scenario {
        "success" => (SagaOutcome::Completed, vec![10, 20]),
        "compensated" | "retry" => (SagaOutcome::Compensated, vec![10, 20, -20, -10]),
        _ => (SagaOutcome::NeedsIntervention, vec![10, 20, -20]),
    };
    assert_eq!(s.outcome, outcome);
    assert_eq!(stub.effects(), effects);
    assert_eq!(e.execute(p, &Cancellation::default()).await.unwrap(), s);
}
#[tokio::test]
async fn lost_acknowledgements_at_every_saga_barrier_and_adapter_boundary() {
    for scenario in [
        "success",
        "compensated",
        "permanent",
        "irreversible",
        "retry",
    ] {
        let baseline = tempfile::tempdir().unwrap();
        let count = attempt(baseline.path(), 0, false, scenario).await;
        for target in 1..=count {
            let dir = tempfile::tempdir().unwrap();
            attempt(dir.path(), target, false, scenario).await;
            verify(dir.path(), scenario).await;
        }
    }
}
#[tokio::test]
async fn saga_crash_child() {
    let Ok(dir) = std::env::var("PACTUM_SAGA_PATH") else {
        return;
    };
    let target = std::env::var("PACTUM_SAGA_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    let scenario = std::env::var("PACTUM_SAGA_SCENARIO").unwrap();
    attempt(Path::new(&dir), target, true, &scenario).await;
    panic!("crash point was not reached");
}
#[tokio::test]
async fn process_death_at_every_saga_barrier_and_adapter_boundary() {
    for scenario in [
        "success",
        "compensated",
        "permanent",
        "irreversible",
        "retry",
    ] {
        let baseline = tempfile::tempdir().unwrap();
        let count = attempt(baseline.path(), 0, false, scenario).await;
        for target in 1..=count {
            let dir = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "saga_crash_child", "--nocapture"])
                .env("PACTUM_SAGA_PATH", dir.path())
                .env("PACTUM_SAGA_TARGET", target.to_string())
                .env("PACTUM_SAGA_SCENARIO", scenario)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{scenario} target={target}: {}",
                String::from_utf8_lossy(&output.stdout)
            );
            verify(dir.path(), scenario).await;
        }
    }
}

struct Replay(Vec<Entry>);
impl Journal for Replay {
    fn append(&mut self, _: u32, _: &[u8]) -> Result<Sequence, JournalError> {
        panic!("no append on replay")
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        panic!("no flush on replay")
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        Ok(self.0.clone())
    }
}
fn replay(entries: Vec<Entry>) -> Result<SagaEngine<Replay>, Error> {
    SagaEngine::open(
        Replay(entries),
        ns(),
        BTreeMap::new(),
        SagaPolicy::default(),
    )
}

#[tokio::test]
async fn recovery_service_keeps_manual_failure_visible_and_resume_renews_budget() {
    use pactum_core::recovery::{Config, Control, Reason, Runner};
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    stub.available(false);
    let mut e = support::open(dir.path(), stub.clone());
    let c = Cancellation::default();
    c.cancel();
    e.execute(spec("p", true, false, false), &c).await.unwrap();
    let runner = Runner::open(
        e,
        SqliteJournal::open(dir.path().join("schedule.db")).unwrap(),
        "saga-recovery-v1",
        Config {
            poll_interval: Duration::from_millis(1),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            max_per_tick: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let (handle, task) = runner.start();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = handle.query().await.unwrap();
            if snapshot.pending.first().is_some_and(|p| {
                p.schedule
                    .as_ref()
                    .is_some_and(|s| matches!(s.reason, Reason::Permanent(_)))
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let snapshot = handle.query().await.unwrap();
    let Work::Saga(s) = &snapshot.pending[0].work else {
        panic!()
    };
    assert!(s.needs_intervention);
    assert_eq!(stub.balance(), 10);
    stub.available(true);
    handle.control("p", Control::Retry).await.unwrap();
    // Scheduling Retry alone cannot reset a protocol attempt budget.
    assert_eq!(
        handle
            .target()
            .lock()
            .await
            .status(&SagaId::new("p").unwrap())
            .unwrap()
            .unwrap()
            .steps[&sid("reserve")]
            .compensation
            .attempts,
        3
    );
    handle.control("p", Control::Resume).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !handle.query().await.unwrap().pending.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(stub.effects(), vec![10, 20, -20, -10]);
    handle.stop();
    drop(task.await.unwrap().unwrap());
    drop(handle);
    let entries = SqliteJournal::open(dir.path().join("saga.db"))
        .unwrap()
        .replay()
        .unwrap();
    assert!(entries.iter().any(|e| {
        let value: serde_json::Value = serde_json::from_slice(&e.payload).unwrap();
        value["event"] == "Retry" && value["reason"] == "recovery-service operator Resume"
    }));
}

#[tokio::test]
async fn definite_forward_retry_exhaustion_compensates_prior_success() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    stub.available(false);
    let mut e = support::open(dir.path(), stub.clone());
    let mut p = spec("p", false, false, false);
    p.steps[0].compensation = Compensation::Operation(operation("add:-10"));
    p.steps[1].forward = operation("retry:20");
    let s = e.execute(p, &Cancellation::default()).await.unwrap();
    assert_eq!(s.outcome, SagaOutcome::Compensated);
    assert_eq!(s.steps[&sid("ship")].forward.attempts, 3);
    assert_eq!(s.steps[&sid("ship")].compensation.attempts, 0);
    assert_eq!(stub.effects(), vec![10, -10]);
}
#[tokio::test]
async fn replay_rejects_illegal_transitions_versions_sequences_and_early_compensation() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let mut e = support::open(dir.path(), stub);
    e.execute(spec("p", true, false, false), &Cancellation::default())
        .await
        .unwrap();
    drop(e);
    let entries = SqliteJournal::open(dir.path().join("saga.db"))
        .unwrap()
        .replay()
        .unwrap();
    for n in 1..=entries.len() {
        assert!(replay(entries[..n].to_vec()).is_ok(), "prefix={n}");
    }
    let cases = [
        (
            0,
            serde_json::json!({"event":"Header","namespace":"foreign"}),
        ),
        (
            1,
            serde_json::json!({"event":"Begin","spec":{"version":99},"bindings":{}}),
        ),
        (2, serde_json::json!({"event":"Compensate","saga":"p"})),
        (
            2,
            serde_json::json!({"event":"Attempt","saga":"p","step":"ship","direction":"Forward","attempt":1}),
        ),
        (
            2,
            serde_json::json!({"event":"Attempt","saga":"p","step":"reserve","direction":"Compensation","attempt":1}),
        ),
        (
            2,
            serde_json::json!({"event":"Attempt","saga":"p","step":"reserve","direction":"Forward","attempt":2}),
        ),
        (3, serde_json::json!({"event":"Compensate","saga":"p"})),
        (
            3,
            serde_json::json!({"event":"Report","saga":"p","step":"reserve","direction":"Compensation","attempt":1,"result":"Applied"}),
        ),
        (
            3,
            serde_json::json!({"event":"Report","saga":"p","step":"reserve","direction":"Forward","attempt":2,"result":"Applied"}),
        ),
        (
            4,
            serde_json::json!({"event":"Report","saga":"p","step":"reserve","direction":"Forward","attempt":1,"result":"Applied"}),
        ),
        (
            4,
            serde_json::json!({"event":"Attempt","saga":"p","step":"reserve","direction":"Forward","attempt":2}),
        ),
        (
            4,
            serde_json::json!({"event":"Retry","saga":"p","step":"reserve","direction":"Forward","reason":"bypass"}),
        ),
        (
            4,
            serde_json::json!({"event":"Header","namespace":"saga-demo-deployment-v1"}),
        ),
        (4, serde_json::json!({"event":"Unknown"})),
    ];
    for (index, value) in cases {
        let mut bad = entries[..=index].to_vec();
        bad[index].payload = serde_json::to_vec(&value).unwrap();
        assert!(matches!(replay(bad), Err(Error::Corrupt { .. })), "{value}");
    }
    let mut bad = entries.clone();
    bad[0].version = 99;
    assert!(replay(bad).is_err());
    let mut bad = entries.clone();
    bad[1].sequence += 1;
    assert!(replay(bad).is_err());
    let mut bad = entries;
    bad[1].payload = b"{".to_vec();
    assert!(replay(bad).is_err());
}

struct ClassifiedSaga {
    inner: Arc<ExternalStub>,
    failing: std::sync::atomic::AtomicBool,
    calls: AtomicUsize,
}
#[async_trait]
impl SagaAdapter for ClassifiedSaga {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn accepts(&self, payload: &[u8]) -> bool {
        self.inner.accepts(payload)
    }
    async fn execute(&self, key: OperationId, payload: Vec<u8>) -> Result<ActionResult, SagaError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failing.load(Ordering::SeqCst) {
            Err(SagaError::classified(
                pactum_core::ErrorCode::InvalidInput,
                std::io::Error::other("credential=SECRET"),
            ))
        } else {
            self.inner.execute(key, payload).await
        }
    }
}
#[tokio::test]
async fn permanent_saga_call_error_is_unknown_effect_and_never_a_rejection_vote() {
    let dir = tempfile::tempdir().unwrap();
    let stub = Arc::new(ExternalStub::open(&dir.path().join("external.db")));
    let adapter = Arc::new(ClassifiedSaga {
        inner: stub.clone(),
        failing: std::sync::atomic::AtomicBool::new(true),
        calls: AtomicUsize::new(0),
    });
    let mut engine = custom_engine(dir.path(), adapter.clone(), SagaPolicy::default());
    let spec = spec("typed-saga", false, false, false);
    let status = engine
        .execute(spec.clone(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(status.outcome, SagaOutcome::Uncertain);
    assert!(status.needs_intervention && !status.compensating);
    assert!(stub.effects().is_empty());
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let action = &status.steps[&sid("reserve")].forward;
    assert!(action.diagnostic.is_some());
    let Some(ActionResult::Operational(metadata)) = &action.report else {
        panic!("expected code-bearing uncertain effect")
    };
    assert_eq!(metadata.code, pactum_core::ErrorCode::InvalidInput);
    assert!(!serde_json::to_string(action).unwrap().contains("SECRET"));
    drop(engine);
    adapter.failing.store(false, Ordering::SeqCst);
    let mut engine = custom_engine(dir.path(), adapter.clone(), SagaPolicy::default());
    let status = engine
        .resume(&spec.id, &Cancellation::default())
        .await
        .unwrap();
    assert!(status.needs_intervention && !status.compensating);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    engine
        .retry(
            &spec.id,
            "repaired adapter configuration; retained receipt store",
        )
        .unwrap();
    drop(engine);
    let mut engine = custom_engine(dir.path(), adapter, SagaPolicy::default());
    assert_eq!(
        engine
            .resume(&spec.id, &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        SagaOutcome::Completed
    );
    assert_eq!(stub.effects(), vec![10, 20]);
}
