//! Transaction plan correctness: DAG validation, concurrency and crash/restart.
#[path = "../examples/support/plan.rs"]
mod support;
use async_trait::async_trait;
use pactum_core::{Cancellation, Error, execution::*, plan::*};
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use pactum_sqlite::{SqliteBackend, SqliteJournal};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn sid(s: &str) -> StepId {
    StepId::new(s).unwrap()
}
fn ns() -> PlanId {
    PlanId::new("plan-example-v1").unwrap()
}
fn step(name: &str, dependencies: &[&str], value: u8, scope: &[&str]) -> Step {
    Step {
        id: sid(name),
        adapter: AdapterId::new("probe").unwrap(),
        dependencies: dependencies.iter().map(|s| sid(s)).collect(),
        resources: scope.iter().map(|s| s.to_string()).collect(),
        work: StepWork::Native(Request::Atomic(AtomicRequest {
            payload: vec![value],
        })),
    }
}
fn plan(steps: Vec<Step>) -> TransactionPlan {
    TransactionPlan {
        version: 1,
        id: PlanId::new("p").unwrap(),
        revision: 1,
        steps,
    }
}
#[test]
fn validates_dag_bounds_versions_and_canonical_fingerprints() {
    let p = plan(vec![step("z", &["a"], 1, &[]), step("a", &[], 2, &[])]);
    assert_eq!(p.validate().unwrap(), vec![sid("a"), sid("z")]);
    let mut reversed = p.clone();
    reversed.steps.reverse();
    assert_eq!(p.fingerprint().unwrap(), reversed.fingerprint().unwrap());
    assert_eq!(
        p.operation_key(&sid("z")).unwrap(),
        reversed.operation_key(&sid("z")).unwrap()
    );
    assert_ne!(
        p.operation_key(&sid("z")).unwrap(),
        p.operation_key(&sid("a")).unwrap()
    );
    {
        let mut bad = p.clone();
        bad.steps[0].dependencies.insert(sid("z"));
        assert!(bad.validate().is_err());
    }
    let mut bad = p.clone();
    bad.steps[1].dependencies.insert(sid("z"));
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.steps[0].dependencies.insert(sid("missing"));
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.steps.push(bad.steps[0].clone());
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.version = 2;
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.revision = 0;
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.steps.clear();
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.steps[0].work = StepWork::Native(Request::Chunked(ChunkedRequest { chunks: vec![] }));
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.steps[0].resources.insert("".into());
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.steps[0].work = StepWork::TwoPhase(vec![]);
    assert!(bad.validate().is_err());
    bad = p.clone();
    bad.revision += 1;
    assert_ne!(bad.fingerprint().unwrap(), p.fingerprint().unwrap());
    bad = p.clone();
    bad.steps[0].work = StepWork::Native(Request::Atomic(AtomicRequest { payload: vec![9] }));
    assert_ne!(bad.fingerprint().unwrap(), p.fingerprint().unwrap());
}
#[derive(Default)]
struct Probe {
    active: AtomicUsize,
    peak: AtomicUsize,
    calls: Mutex<Vec<u8>>,
    scopes: Mutex<BTreeSet<u8>>,
}
struct Guard<'a> {
    probe: &'a Probe,
    scope: u8,
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.probe.active.fetch_sub(1, Ordering::SeqCst);
        self.probe.scopes.lock().unwrap().remove(&self.scope);
    }
}
#[async_trait]
impl StepAdapter for Probe {
    fn identity(&self) -> String {
        "probe-v1".into()
    }
    fn accepts(&self, work: &StepWork) -> bool {
        matches!(work, StepWork::Native(Request::Atomic(_)))
    }
    async fn execute(
        &self,
        _: OperationId,
        work: StepWork,
        _: &Cancellation,
    ) -> Result<StepResult, Error> {
        let StepWork::Native(Request::Atomic(r)) = work else {
            unreachable!()
        };
        let value = r.payload[0];
        self.calls.lock().unwrap().push(value);
        let scope = if matches!(value, 3 | 4) { 3 } else { value };
        assert!(
            self.scopes.lock().unwrap().insert(scope),
            "overlapping exclusive scope"
        );
        let _guard = Guard { probe: self, scope };
        let n = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(n, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(if value == 99 { 200 } else { 10 })).await;
        match value {
            0 => Err(Error::InDoubt("offline".into())),
            8 => Ok(StepResult::Native(Progress::Atomic(AtomicStatus::Aborted))),
            9 => panic!("injected adapter panic"),
            7 => Ok(StepResult::Native(Progress::StagedPublish(
                PublishStatus::Published,
            ))),
            _ => Ok(StepResult::Native(Progress::Atomic(
                AtomicStatus::Committed,
            ))),
        }
    }
}
fn probe_engine(path: &Path, probe: Arc<Probe>, policy: PlanPolicy) -> PlanEngine<SqliteJournal> {
    PlanEngine::open(
        SqliteJournal::open(path.join("plan.db")).unwrap(),
        ns(),
        BTreeMap::from([(
            AdapterId::new("probe").unwrap(),
            probe as Arc<dyn StepAdapter>,
        )]),
        policy,
    )
    .unwrap()
}
#[tokio::test]
async fn fan_out_fan_in_concurrency_scopes_and_immutable_outputs() {
    let temp = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let mut e = probe_engine(
        temp.path(),
        probe.clone(),
        PlanPolicy {
            concurrency: 2,
            ..PlanPolicy::default()
        },
    );
    let p = plan(vec![
        step("a", &[], 1, &[]),
        step("b", &["a"], 2, &[]),
        step("c", &["a"], 3, &["x"]),
        step("d", &["a"], 4, &["x"]),
        step("e", &["b", "c", "d"], 5, &[]),
    ]);
    let s = e
        .execute(p.clone(), &Cancellation::default())
        .await
        .unwrap();
    assert_eq!(s.outcome, PlanOutcome::Complete);
    assert!(s.is_terminal());
    assert!(s.blocked.is_empty());
    assert_eq!(probe.peak.load(Ordering::SeqCst), 2);
    let calls = probe.calls.lock().unwrap().clone();
    assert_eq!(calls.first(), Some(&1));
    assert_eq!(calls.last(), Some(&5));
    assert_eq!(s, e.resume(&p.id, &Cancellation::default()).await.unwrap());
    assert_eq!(probe.calls.lock().unwrap().len(), 5);
    let mut changed = p.clone();
    changed.revision += 1;
    assert!(e.execute(changed, &Cancellation::default()).await.is_err());
    assert_eq!(
        e.guarantees(&p.id, &sid("a")).unwrap(),
        Mode::Atomic.guarantees()
    );
    drop(e);
    let mut e = probe_engine(temp.path(), probe.clone(), PlanPolicy::default());
    assert_eq!(e.status(&p.id).unwrap().unwrap(), s);
    assert!(
        e.recover_pending(&Cancellation::default())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(probe.calls.lock().unwrap().len(), 5);
}
#[tokio::test]
async fn fair_bounded_retries_failure_gating_timeout_and_cancellation() {
    let temp = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let policy = PlanPolicy {
        concurrency: 1,
        max_dispatches: 1,
        attempts_per_step: 3,
        timeout: Duration::from_millis(50),
        ..PlanPolicy::default()
    };
    let p = plan(vec![step("a", &[], 0, &[]), step("b", &[], 1, &[])]);
    let mut e = probe_engine(temp.path(), probe.clone(), policy);
    assert_eq!(
        e.execute(p.clone(), &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        PlanOutcome::Indeterminate
    );
    let s = e.resume(&p.id, &Cancellation::default()).await.unwrap();
    assert!(s.steps[&sid("b")].is_success());
    assert_eq!(*probe.calls.lock().unwrap(), vec![0, 1]);
    let mut fail = plan(vec![
        step("a", &[], 8, &[]),
        step("b", &["a"], 2, &[]),
        step("c", &["b"], 5, &[]),
    ]);
    fail.id = PlanId::new("failed").unwrap();
    let s = e.execute(fail, &Cancellation::default()).await.unwrap();
    assert_eq!(s.outcome, PlanOutcome::PartiallyFailed);
    assert!(s.is_terminal());
    assert_eq!(s.failed_dependencies, BTreeSet::from([sid("b"), sid("c")]));
    let mut slow = plan(vec![step("a", &[], 99, &[]), step("b", &["a"], 6, &[])]);
    slow.id = PlanId::new("slow").unwrap();
    let s = e.execute(slow, &Cancellation::default()).await.unwrap();
    assert_eq!(s.outcome, PlanOutcome::Indeterminate);
    assert_eq!(s.steps[&sid("b")].attempts, 0);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    let cancelled = Cancellation::default();
    cancelled.cancel();
    let mut p = plan(vec![step("a", &[], 1, &[])]);
    p.id = PlanId::new("cancelled").unwrap();
    let s = e.execute(p.clone(), &cancelled).await.unwrap();
    assert_eq!(s.outcome, PlanOutcome::Pending);
    assert_eq!(s.steps[&sid("a")].attempts, 0);
    assert_eq!(
        e.resume(&p.id, &Cancellation::default())
            .await
            .unwrap()
            .outcome,
        PlanOutcome::Complete
    );
}
#[tokio::test]
async fn cancel_during_call_and_panic_or_wrong_mode_never_unlock_dependents() {
    for value in [99, 9, 7] {
        let temp = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let mut e = probe_engine(temp.path(), probe.clone(), PlanPolicy::default());
        let p = plan(vec![step("a", &[], value, &[]), step("b", &["a"], 6, &[])]);
        let token = Cancellation::default();
        let other = token.clone();
        let cancel = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            other.cancel();
        });
        let s = e.execute(p, &token).await.unwrap();
        cancel.await.unwrap();
        assert_eq!(s.outcome, PlanOutcome::Indeterminate);
        assert_eq!(s.steps[&sid("b")].attempts, 0);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn mixed_native_modes_resume_after_reopen_and_never_repeat_effects() {
    let temp = tempfile::tempdir().unwrap();
    let c = Cancellation::default();
    let mut e = support::open(temp.path());
    let s = e.execute(support::spec(), &c).await.unwrap();
    assert_ne!(s.outcome, PlanOutcome::Complete);
    assert_eq!(s.steps[&sid("publish")].attempts, 0);
    drop(e);
    let mut e = support::open(temp.path());
    let s = support::finish(&mut e).await;
    assert_eq!(s.outcome, PlanOutcome::Complete);
    drop(e);
    let mut e = support::open(temp.path());
    assert_eq!(support::finish(&mut e).await, s);
    drop(e);
    let b = SqliteBackend::open(temp.path().join("backend.db"), "plan-example").unwrap();
    assert_eq!(b.value("counter").unwrap(), 17);
    assert_eq!(
        b.visible(&OperationId::new("catalog").unwrap())
            .unwrap()
            .unwrap()
            .values[0]
            .value,
        42
    );
}
#[tokio::test]
async fn two_phase_step_uses_existing_coordinator_and_recovery_target() {
    use pactum_core::{
        Coordinator, Enlistment, Participant, ParticipantId, RetryPolicy,
        recovery::{RecoveryTarget, Work},
    };
    let temp = tempfile::tempdir().unwrap();
    let counter = Arc::new(
        pactum_demo::DurableCounter::open(
            temp.path().join("counter.db"),
            ParticipantId::new("counter").unwrap(),
        )
        .unwrap(),
    );
    let coordinator = Coordinator::open(
        SqliteJournal::open(temp.path().join("2pc.db")).unwrap(),
        vec![counter.clone()],
        RetryPolicy::default(),
    )
    .unwrap();
    let mut p = plan(vec![step("a", &[], 1, &[])]);
    p.steps[0].work = StepWork::TwoPhase(vec![Enlistment {
        participant: counter.id(),
        payload: b"13".to_vec(),
    }]);
    let adapters = BTreeMap::from([(
        AdapterId::new("probe").unwrap(),
        Arc::new(TwoPhaseAdapter::new(
            AdapterId::new("2pc-journal-v1").unwrap(),
            coordinator,
        )) as Arc<dyn StepAdapter>,
    )]);
    let mut e = PlanEngine::open(
        SqliteJournal::open(temp.path().join("plan.db")).unwrap(),
        ns(),
        adapters,
        PlanPolicy::default(),
    )
    .unwrap();
    let paused = Cancellation::default();
    paused.cancel();
    e.execute(p.clone(), &paused).await.unwrap();
    assert_eq!(e.pending_work().unwrap().len(), 1);
    let Work::Plan(s) = e
        .recover_one(p.id.as_str(), &Cancellation::default())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(s.outcome, PlanOutcome::Complete);
    assert!(e.pending_work().unwrap().is_empty());
    assert_eq!(counter.value().await.unwrap(), 13);
    e.resume(&p.id, &Cancellation::default()).await.unwrap();
    assert_eq!(counter.value().await.unwrap(), 13);
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
        if self.target == n {
            if self.kill {
                std::process::exit(86);
            }
            return Err(JournalError("lost acknowledgement".into()));
        }
        Ok(())
    }
}
struct FaultJournal {
    inner: SqliteJournal,
    fault: Fault,
}
impl Journal for FaultJournal {
    fn append(&mut self, v: u32, b: &[u8]) -> Result<Sequence, JournalError> {
        self.fault.tick()?;
        let s = self.inner.append(v, b)?;
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
    inner: Arc<dyn StepAdapter>,
    fault: Fault,
}
#[async_trait]
impl StepAdapter for FaultAdapter {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn accepts(&self, w: &StepWork) -> bool {
        self.inner.accepts(w)
    }
    async fn execute(
        &self,
        key: OperationId,
        work: StepWork,
        c: &Cancellation,
    ) -> Result<StepResult, Error> {
        self.fault.tick().map_err(|e| Error::InDoubt(e.0))?;
        let result = self.inner.execute(key, work, c).await?;
        self.fault.tick().map_err(|e| Error::InDoubt(e.0))?;
        Ok(result)
    }
}
async fn attempt(directory: &Path, target: usize, kill: bool) -> usize {
    let fault = Fault {
        count: Arc::new(AtomicUsize::new(0)),
        target,
        kill,
    };
    let adapters = support::adapters(directory)
        .into_iter()
        .map(|(id, inner)| {
            (
                id,
                Arc::new(FaultAdapter {
                    inner,
                    fault: fault.clone(),
                }) as Arc<dyn StepAdapter>,
            )
        })
        .collect();
    let journal = FaultJournal {
        inner: SqliteJournal::open(directory.join("plan.db")).unwrap(),
        fault: fault.clone(),
    };
    if let Ok(mut e) = PlanEngine::open(journal, ns(), adapters, PlanPolicy::default()) {
        for _ in 0..5 {
            match e.execute(support::spec(), &Cancellation::default()).await {
                Ok(s) if s.is_terminal() => break,
                Ok(_) => {}
                Err(_) => {
                    assert!(matches!(
                        e.status(&support::spec().id),
                        Err(Error::InDoubt(_))
                    ));
                    break;
                }
            }
        }
    }
    fault.count.load(Ordering::SeqCst)
}
async fn verify(directory: &Path) {
    let mut e = support::open(directory);
    let s = support::finish(&mut e).await;
    assert_eq!(s.outcome, PlanOutcome::Complete);
    assert!(s.blocked.is_empty());
    let outputs = s.clone();
    assert_eq!(support::finish(&mut e).await, outputs);
    drop(e);
    let b = SqliteBackend::open(directory.join("backend.db"), "plan-example").unwrap();
    assert_eq!(b.value("counter").unwrap(), 17);
    assert_eq!(
        b.visible(&OperationId::new("catalog").unwrap())
            .unwrap()
            .unwrap()
            .values[0]
            .value,
        42
    );
}
#[tokio::test]
async fn lost_acknowledgements_at_every_plan_barrier_and_adapter_boundary() {
    let baseline = tempfile::tempdir().unwrap();
    let count = attempt(baseline.path(), 0, false).await;
    for target in 1..=count {
        let temp = tempfile::tempdir().unwrap();
        attempt(temp.path(), target, false).await;
        verify(temp.path()).await;
    }
}
#[tokio::test]
async fn plan_crash_child() {
    let Ok(directory) = std::env::var("PACTUM_PLAN_PATH") else {
        return;
    };
    let target = std::env::var("PACTUM_PLAN_TARGET")
        .unwrap()
        .parse()
        .unwrap();
    attempt(Path::new(&directory), target, true).await;
    panic!("crash point was not reached");
}
#[tokio::test]
async fn process_death_at_every_plan_barrier_and_adapter_boundary() {
    let baseline = tempfile::tempdir().unwrap();
    let count = attempt(baseline.path(), 0, false).await;
    for target in 1..=count {
        let temp = tempfile::tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "plan_crash_child", "--nocapture"])
            .env("PACTUM_PLAN_PATH", temp.path())
            .env("PACTUM_PLAN_TARGET", target.to_string())
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "target={target}: {}",
            String::from_utf8_lossy(&result.stdout)
        );
        verify(temp.path()).await;
    }
}

struct Replay(Vec<Entry>);
impl Journal for Replay {
    fn append(&mut self, _: u32, _: &[u8]) -> Result<Sequence, JournalError> {
        panic!("replay must not append")
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        panic!("replay must not flush")
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        Ok(self.0.clone())
    }
}
fn replay(entries: Vec<Entry>) -> Result<PlanEngine<Replay>, Error> {
    PlanEngine::open(
        Replay(entries),
        ns(),
        BTreeMap::new(),
        PlanPolicy::default(),
    )
}
#[tokio::test]
async fn replay_fails_closed_on_illegal_histories_and_never_dispatches() {
    let temp = tempfile::tempdir().unwrap();
    let mut e = probe_engine(
        temp.path(),
        Arc::new(Probe::default()),
        PlanPolicy::default(),
    );
    let p = plan(vec![step("a", &[], 1, &[]), step("b", &["a"], 2, &[])]);
    e.execute(p, &Cancellation::default()).await.unwrap();
    drop(e);
    let entries = SqliteJournal::open(temp.path().join("plan.db"))
        .unwrap()
        .replay()
        .unwrap();
    for n in 1..=entries.len() {
        assert!(replay(entries[..n].to_vec()).is_ok());
    }
    let cases = [
        (
            0,
            serde_json::json!({"event":"Header","namespace":"foreign"}),
        ),
        (
            1,
            serde_json::json!({"event":"Begin","spec":{"version":99,"id":"p","revision":1,"steps":[]}}),
        ),
        (
            2,
            serde_json::json!({"event":"Attempt","plan":"p","step":"b","attempt":1}),
        ),
        (
            2,
            serde_json::json!({"event":"Attempt","plan":"p","step":"a","attempt":2}),
        ),
        (
            3,
            serde_json::json!({"event":"Report","plan":"p","step":"a","attempt":2,"report":{"Uncertain":"x"}}),
        ),
        (
            3,
            serde_json::json!({"event":"Report","plan":"p","step":"a","attempt":1,"report":{"Result":{"Native":{"StagedPublish":"Published"}}}}),
        ),
        (
            4,
            serde_json::json!({"event":"Attempt","plan":"p","step":"a","attempt":2}),
        ),
        (
            4,
            serde_json::json!({"event":"Report","plan":"p","step":"a","attempt":1,"report":{"Uncertain":"duplicate"}}),
        ),
        (
            4,
            serde_json::json!({"event":"Header","namespace":"plan-example-v1"}),
        ),
        (4, serde_json::json!({"event":"Unknown"})),
    ];
    for (index, value) in cases {
        let mut bad = entries[..=index].to_vec();
        bad[index].payload = serde_json::to_vec(&value).unwrap();
        assert!(matches!(replay(bad), Err(Error::Corrupt(_))), "{value}");
    }
    let mut bad = entries.clone();
    bad[0].version = 1;
    assert!(replay(bad).is_err());
    let mut bad = entries.clone();
    bad[2].sequence += 1;
    assert!(replay(bad).is_err());
    let mut bad = entries;
    bad[1].payload = b"{".to_vec();
    assert!(replay(bad).is_err());
}
#[tokio::test]
async fn dropped_future_can_resume_with_same_bounds_and_receipt_keys() {
    let temp = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let mut e = probe_engine(
        temp.path(),
        probe.clone(),
        PlanPolicy {
            concurrency: 1,
            ..PlanPolicy::default()
        },
    );
    let p = plan(vec![
        step("a", &[], 99, &["x"]),
        step("b", &["a"], 1, &["x"]),
    ]);
    let c = Cancellation::default();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), e.execute(p.clone(), &c))
            .await
            .is_err()
    );
    assert_eq!(
        e.status(&p.id).unwrap().unwrap().outcome,
        PlanOutcome::Indeterminate
    );
    assert_eq!(
        e.resume(&p.id, &c).await.unwrap().outcome,
        PlanOutcome::Complete
    );
    assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn invalid_registration_and_policy_fail_before_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    for policy in [
        PlanPolicy {
            concurrency: 0,
            ..PlanPolicy::default()
        },
        PlanPolicy {
            attempts_per_step: 0,
            ..PlanPolicy::default()
        },
        PlanPolicy {
            max_dispatches: 0,
            ..PlanPolicy::default()
        },
        PlanPolicy {
            timeout: Duration::ZERO,
            ..PlanPolicy::default()
        },
    ] {
        assert!(
            PlanEngine::open(
                SqliteJournal::open(temp.path().join("plan.db")).unwrap(),
                ns(),
                BTreeMap::new(),
                policy
            )
            .is_err()
        );
    }
    let mut e = PlanEngine::open(
        SqliteJournal::open(temp.path().join("plan.db")).unwrap(),
        ns(),
        BTreeMap::new(),
        PlanPolicy::default(),
    )
    .unwrap();
    let p = plan(vec![step("a", &[], 1, &[])]);
    assert!(
        e.execute(p.clone(), &Cancellation::default())
            .await
            .is_err()
    );
    assert!(e.status(&p.id).unwrap().is_none());
}
#[tokio::test]
async fn retries_within_a_pass_are_bounded_and_dont_starve_ready_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let mut e = probe_engine(
        temp.path(),
        probe.clone(),
        PlanPolicy {
            concurrency: 1,
            attempts_per_step: 3,
            max_dispatches: 4,
            ..PlanPolicy::default()
        },
    );
    let s = e
        .execute(
            plan(vec![step("a", &[], 0, &[]), step("b", &[], 1, &[])]),
            &Cancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(s.steps[&sid("a")].attempts, 3);
    assert!(s.steps[&sid("b")].is_success());
    assert_eq!(*probe.calls.lock().unwrap(), vec![0, 1, 0, 0]);
}
#[tokio::test]
async fn replacement_backend_cannot_rebind_an_existing_plan() {
    let temp = tempfile::tempdir().unwrap();
    let c = Cancellation::default();
    let mut e = support::open(temp.path());
    e.execute(support::spec(), &c).await.unwrap();
    drop(e);
    let replacement = Engine::open(
        SqliteJournal::open(temp.path().join("replacement-modes.db")).unwrap(),
        SqliteBackend::open(temp.path().join("replacement-backend.db"), "plan-example").unwrap(),
        ExecutionPolicy::default(),
    )
    .unwrap();
    let adapters = BTreeMap::from([(
        AdapterId::new("local-v1").unwrap(),
        Arc::new(NativeAdapter::new(replacement)) as Arc<dyn StepAdapter>,
    )]);
    let mut e = PlanEngine::open(
        SqliteJournal::open(temp.path().join("plan.db")).unwrap(),
        ns(),
        adapters,
        PlanPolicy::default(),
    )
    .unwrap();
    assert!(matches!(
        e.resume(&support::spec().id, &c).await,
        Err(Error::Invalid(_))
    ));
    drop(e);
    let b =
        SqliteBackend::open(temp.path().join("replacement-backend.db"), "plan-example").unwrap();
    assert_eq!(b.value("counter").unwrap(), 0);
}
#[test]
fn encoded_size_and_step_count_are_bounded_and_2pc_order_is_canonical() {
    let mut p = plan(
        (0..1025)
            .map(|i| step(&format!("s{i}"), &[], 1, &[]))
            .collect(),
    );
    assert!(p.validate().is_err());
    p = plan(vec![step("a", &[], 1, &[])]);
    p.steps[0].work = StepWork::Native(Request::Atomic(AtomicRequest {
        payload: vec![255; 4 * 1024 * 1024 + 1],
    }));
    assert!(p.validate().is_err());
    p.steps[0].work = StepWork::TwoPhase(vec![
        pactum_core::Enlistment {
            participant: pactum_core::ParticipantId::new("z").unwrap(),
            payload: vec![2],
        },
        pactum_core::Enlistment {
            participant: pactum_core::ParticipantId::new("a").unwrap(),
            payload: vec![1],
        },
    ]);
    let hash = p.fingerprint().unwrap();
    let StepWork::TwoPhase(r) = &mut p.steps[0].work else {
        panic!()
    };
    r.reverse();
    assert_eq!(p.fingerprint().unwrap(), hash);
}
