//! Public planner contracts, frozen budgets, and provider-style large workload.
use pactum_core::{Enlistment, ParticipantId, execution::*, plan::*, planner::*};
use std::{collections::BTreeMap, sync::Arc};
fn estimate(bytes: u64, operations: u64) -> PlanEstimate {
    PlanEstimate(BTreeMap::from([
        (Dimension::Bytes, bytes),
        (Dimension::Operations, operations),
        (Dimension::Duration, operations),
        (Dimension::Memory, bytes),
        (Dimension::Concurrency, 1),
        (Dimension::Conflicts, operations),
    ]))
}
fn fixture(count: usize, consistency: Consistency) -> (TransactionIntent, PlanningContext) {
    let adapter = AdapterId::new("fixture").unwrap();
    let intent = TransactionIntent {
        id: PlanId::new("fixture-v1").unwrap(),
        revision: 1,
        adapter: adapter.clone(),
        consistency,
        work: IntentWork::Native {
            units: (0..count)
                .map(|i| WorkUnit {
                    payload: (i as u64).to_le_bytes().to_vec(),
                    estimate: estimate(1000, 1),
                })
                .collect(),
            publication: Some(Publication {
                target: OperationId::new("visible-generation").unwrap(),
                expected_generation: 0,
                manifest: WorkUnit {
                    payload: b"immutable-source-v1".to_vec(),
                    estimate: estimate(100, 1),
                },
            }),
        },
    };
    let capabilities = ResourceCapabilities {
        snapshot: "provider-v1".into(),
        modes: vec![
            Mode::Atomic,
            Mode::Chunked,
            Mode::StagedPublish,
            Mode::TwoPhase,
        ],
        planner_envelope_v1: true,
        budget: ResourceBudget {
            hard: BTreeMap::from([(Dimension::Bytes, 3000)]),
            soft: BTreeMap::new(),
        },
        overhead: estimate(10, 0),
        safety_margin_bps: 0,
        extensions: BTreeMap::new(),
    };
    (
        intent,
        PlanningContext {
            capabilities: BTreeMap::from([(adapter, capabilities)]),
            budget: ResourceBudget::default(),
        },
    )
}
fn caps<'a>(
    intent: &TransactionIntent,
    ctx: &'a mut PlanningContext,
) -> &'a mut ResourceCapabilities {
    ctx.capabilities.get_mut(&intent.adapter).unwrap()
}
struct ThirdParty;
impl TransactionPlanner for ThirdParty {
    fn plan(
        &self,
        i: &TransactionIntent,
        c: &PlanningContext,
    ) -> Result<ExecutionPlan, PlanningError> {
        let IntentWork::Native { units, .. } = &i.work else {
            unreachable!()
        };
        ExecutionPlan::accept(i, c, Mode::Chunked, (1..=units.len()).collect())
    }
}
#[test]
fn default_and_third_party_pass_contract_and_round_trip() {
    let (i, c) = fixture(5, Consistency::PartialAllowed);
    let mut registry = PlannerRegistry::default();
    registry
        .register("default".into(), Arc::new(DefaultTransactionPlanner))
        .unwrap();
    registry
        .register("custom".into(), Arc::new(ThirdParty))
        .unwrap();
    assert!(
        registry
            .register("custom".into(), Arc::new(ThirdParty))
            .is_err()
    );
    for name in ["default", "custom"] {
        let plan = registry.plan(name, &i, &c).unwrap();
        assert_eq!(plan, registry.plan(name, &i, &c).unwrap());
        plan.transaction_plan().validate().unwrap();
        let encoded = serde_json::to_vec(&plan).unwrap();
        let decoded: ExecutionPlan = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, plan);
        assert_eq!(decoded.fingerprint(), plan.fingerprint());
        let mut bad: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        bad["version"] = 2.into();
        assert!(serde_json::from_value::<ExecutionPlan>(bad).is_err());
        let mut changed = c.clone();
        caps(&i, &mut changed).snapshot = "provider-v2".into();
        let other = registry.plan(name, &i, &changed).unwrap();
        assert_ne!(plan.fingerprint(), other.fingerprint());
        assert_ne!(
            plan.transaction_plan().fingerprint().unwrap(),
            other.transaction_plan().fingerprint().unwrap()
        );
    }
}
#[test]
fn rejects_impossible_guarantees_unknowns_and_hard_overruns() {
    let (mut i, mut c) = fixture(5, Consistency::NativeAtomic);
    assert!(matches!(
        DefaultTransactionPlanner.plan(&i, &c),
        Err(PlanningError::HardBudget {
            dimension: Dimension::Bytes
        })
    ));
    assert!(ExecutionPlan::accept(&i, &c, Mode::Chunked, vec![2, 4, 5]).is_err());
    i.consistency = Consistency::StagedVisibility;
    caps(&i, &mut c).modes = vec![Mode::Atomic, Mode::Chunked];
    assert_eq!(
        DefaultTransactionPlanner.plan(&i, &c),
        Err(PlanningError::UnsupportedConsistency)
    );
    caps(&i, &mut c).modes.push(Mode::StagedPublish);
    let IntentWork::Native { units, .. } = &mut i.work else {
        unreachable!()
    };
    units[0].estimate.0.remove(&Dimension::Duration);
    caps(&i, &mut c)
        .budget
        .hard
        .insert(Dimension::Duration, 100);
    assert_eq!(
        DefaultTransactionPlanner.plan(&i, &c),
        Err(PlanningError::HardBudget {
            dimension: Dimension::Duration
        })
    );
    caps(&i, &mut c).budget.hard.remove(&Dimension::Duration);
    c.budget.hard.insert(Dimension::Duration, 1000);
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
    c.budget.hard.clear();
    caps(&i, &mut c)
        .budget
        .hard
        .insert(Dimension::Concurrency, 0);
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
}
#[test]
fn overhead_safety_time_conflicts_and_soft_limits_drive_partitions() {
    let (i, mut c) = fixture(6, Consistency::PartialAllowed);
    caps(&i, &mut c).safety_margin_bps = 1000;
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    let StepWork::Native(Request::Chunked(r)) = &p.transaction_plan().steps[0].work else {
        panic!()
    };
    assert_eq!(r.chunks.len(), 3);
    assert_eq!(p.explain().transaction_budget.hard[&Dimension::Bytes], 2700);
    caps(&i, &mut c).budget.hard.insert(Dimension::Duration, 2);
    caps(&i, &mut c).budget.hard.insert(Dimension::Conflicts, 2);
    // With a 10% safety reserve, integer limits of two permit only one.
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    let StepWork::Native(Request::Chunked(r)) = &p.transaction_plan().steps[0].work else {
        panic!()
    };
    assert_eq!(r.chunks.len(), 6);
    caps(&i, &mut c).budget.soft.insert(Dimension::Bytes, 1);
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    assert!(!p.explain().warnings.is_empty());
    c.budget.hard.insert(Dimension::Operations, 5);
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
}
#[test]
fn encoded_bytes_memory_overflow_and_publication_are_bounded() {
    let (mut i, mut c) = fixture(1, Consistency::NativeAtomic);
    let IntentWork::Native { units, .. } = &mut i.work else {
        unreachable!()
    };
    units[0].payload = vec![255; 1000]; // JSON wire bytes exceed the declared logical cost.
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
    let (j, d) = fixture(1, Consistency::StagedVisibility);
    i = j;
    c = d;
    let IntentWork::Native { publication, .. } = &mut i.work else {
        unreachable!()
    };
    publication.as_mut().unwrap().manifest.estimate = estimate(4000, 1);
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
    let IntentWork::Native { publication, .. } = &mut i.work else {
        unreachable!()
    };
    publication.as_mut().unwrap().manifest.estimate = estimate(100, 1);
    publication.as_mut().unwrap().expected_generation = u64::MAX;
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
    let (i, mut c) = fixture(2, Consistency::NativeAtomic);
    caps(&i, &mut c).budget.hard.insert(Dimension::Memory, 1500);
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
    caps(&i, &mut c).budget.hard.clear();
    caps(&i, &mut c)
        .overhead
        .0
        .insert(Dimension::Bytes, u64::MAX);
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
}
#[test]
fn two_phase_is_one_existing_coordinator_step() {
    let (mut i, c) = fixture(1, Consistency::TwoPhaseDecision);
    i.work = IntentWork::TwoPhase {
        enlistments: vec![Enlistment {
            participant: ParticipantId::new("p").unwrap(),
            payload: vec![1],
        }],
        estimate: estimate(100, 1),
    };
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    assert!(matches!(
        p.transaction_plan().steps[0].work,
        StepWork::TwoPhase(_)
    ));
    i.consistency = Consistency::NativeAtomic;
    assert!(DefaultTransactionPlanner.plan(&i, &c).is_err());
}
#[test]
fn synthetic_100mb_100k_records_fit_provider_size_and_time_budgets() {
    // Payloads are immutable external record references. Logical data is 100 MB;
    // the existing journal intentionally cannot inline that much data (16 MiB bound).
    let (i, mut c) = fixture(100_000, Consistency::StagedVisibility);
    let capability = caps(&i, &mut c);
    capability.budget.hard = BTreeMap::from([
        (Dimension::Bytes, 10_000_000),
        (Dimension::Duration, 5000),
        (Dimension::Conflicts, 5000),
    ]);
    capability.safety_margin_bps = 1000;
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    assert!(p.explain().estimate.0[&Dimension::Bytes] >= 100_000_000);
    assert_eq!(p.transaction_plan().steps.len(), 24); // 23 stage transactions + publish
    assert_eq!(p.explain().strategy, Mode::StagedPublish);
    assert_eq!(
        p.explain().guarantee,
        "one visibility CAS; staging is hidden"
    );
    p.transaction_plan().validate().unwrap();
}

// A durable reference adapter for the opt-in envelope protocol. It deliberately
// loses acknowledgements after committing receipts, then is reopened from disk.
struct EnvelopeAdapter {
    db: std::sync::Mutex<rusqlite::Connection>,
    fault: std::sync::atomic::AtomicUsize,
}
impl EnvelopeAdapter {
    fn open(path: &std::path::Path, fault: usize) -> Self {
        let db = rusqlite::Connection::open(path).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS receipts(k TEXT PRIMARY KEY, work BLOB NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS chunks(artifact TEXT, idx INTEGER, PRIMARY KEY(artifact,idx));
            CREATE TABLE IF NOT EXISTS visible(target TEXT PRIMARY KEY, generation INTEGER NOT NULL, artifact TEXT NOT NULL);").unwrap();
        Self {
            db: std::sync::Mutex::new(db),
            fault: std::sync::atomic::AtomicUsize::new(fault),
        }
    }
    fn visible(&self) -> u64 {
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM visible", [], |r| r.get(0))
            .unwrap()
    }
}
#[async_trait::async_trait]
impl StepAdapter for EnvelopeAdapter {
    fn identity(&self) -> String {
        "durable-envelope-fixture-v1".into()
    }
    fn accepts(&self, work: &StepWork) -> bool {
        matches!(
            work,
            StepWork::Native(Request::Atomic(_) | Request::StagedPublish(_))
        )
    }
    async fn execute(
        &self,
        key: OperationId,
        work: StepWork,
        _: &pactum_core::Cancellation,
    ) -> Result<StepResult, pactum_core::Error> {
        use rusqlite::OptionalExtension;
        use std::sync::atomic::Ordering;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction().unwrap();
        let encoded = serde_json::to_vec(&work).unwrap();
        let receipt: Option<(Vec<u8>, String)> = tx
            .query_row(
                "SELECT work,result FROM receipts WHERE k=?1",
                [key.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .unwrap();
        if let Some((old, result)) = receipt {
            assert_eq!(old, encoded);
            return Ok(serde_json::from_str(&result).unwrap());
        }
        let mut lose_ack = false;
        let result = match &work {
            StepWork::Native(Request::Atomic(r)) => {
                let PlannerEnvelope::Stage {
                    artifact, index, ..
                } = serde_json::from_slice(&r.payload).unwrap()
                else {
                    panic!()
                };
                if self.fault.load(Ordering::SeqCst) == 2 && index == 1 {
                    StepResult::Native(Progress::Atomic(AtomicStatus::Aborted))
                } else {
                    tx.execute(
                        "INSERT INTO chunks VALUES(?1,?2)",
                        rusqlite::params![artifact, index],
                    )
                    .unwrap();
                    lose_ack = self
                        .fault
                        .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();
                    StepResult::Native(Progress::Atomic(AtomicStatus::Committed))
                }
            }
            StepWork::Native(Request::StagedPublish(r)) => {
                let PlannerEnvelope::Publish {
                    artifact, chunks, ..
                } = serde_json::from_slice(&r.payload).unwrap()
                else {
                    panic!()
                };
                let actual: usize = tx
                    .query_row(
                        "SELECT count(*) FROM chunks WHERE artifact=?1",
                        [&artifact],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(actual, chunks, "never publish incomplete staging");
                let generation: u64 = tx
                    .query_row(
                        "SELECT generation FROM visible WHERE target=?1",
                        [r.target.as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .unwrap()
                    .unwrap_or(0);
                if generation != r.expected_generation {
                    StepResult::Native(Progress::StagedPublish(PublishStatus::Conflict))
                } else {
                    tx.execute(
                        "INSERT OR REPLACE INTO visible VALUES(?1,?2,?3)",
                        rusqlite::params![r.target.as_str(), generation + 1, artifact],
                    )
                    .unwrap();
                    lose_ack = self
                        .fault
                        .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();
                    StepResult::Native(Progress::StagedPublish(PublishStatus::Published))
                }
            }
            _ => panic!(),
        };
        tx.execute(
            "INSERT INTO receipts VALUES(?1,?2,?3)",
            rusqlite::params![
                key.as_str(),
                encoded,
                serde_json::to_string(&result).unwrap()
            ],
        )
        .unwrap();
        tx.commit().unwrap();
        if lose_ack {
            return Err(pactum_core::Error::operational(
                pactum_core::Diagnostic::message(
                    pactum_core::ErrorCode::Unavailable,
                    "lost acknowledgement after backend commit".into(),
                ),
            ));
        }
        Ok(result)
    }
}
fn engine(
    path: &std::path::Path,
    adapter: Arc<EnvelopeAdapter>,
) -> PlanEngine<pactum_sqlite::SqliteJournal> {
    PlanEngine::open(
        pactum_sqlite::SqliteJournal::open(path).unwrap(),
        PlanId::new("planner-test-journal").unwrap(),
        BTreeMap::from([(
            AdapterId::new("fixture").unwrap(),
            adapter as Arc<dyn StepAdapter>,
        )]),
        PlanPolicy::default(),
    )
    .unwrap()
}
#[tokio::test]
async fn generated_staging_recovers_unknown_chunk_and_unknown_publish_without_duplicate_effects() {
    for fault in [1, 3] {
        let dir = tempfile::tempdir().unwrap();
        let (i, c) = fixture(5, Consistency::StagedVisibility);
        let accepted = DefaultTransactionPlanner.plan(&i, &c).unwrap();
        let adapter = Arc::new(EnvelopeAdapter::open(&dir.path().join("backend.db"), fault));
        let mut e = engine(&dir.path().join("plan.db"), adapter.clone());
        let first = e
            .execute(
                accepted.transaction_plan().clone(),
                &pactum_core::Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(first.outcome, PlanOutcome::Indeterminate);
        assert_eq!(adapter.visible(), u64::from(fault == 3));
        drop(e);
        drop(adapter);
        let adapter = Arc::new(EnvelopeAdapter::open(&dir.path().join("backend.db"), 0));
        let mut e = engine(&dir.path().join("plan.db"), adapter.clone());
        let status = e
            .recover_pending(&pactum_core::Cancellation::default())
            .await
            .unwrap();
        assert_eq!(status[0].outcome, PlanOutcome::Complete);
        assert_eq!(adapter.visible(), 1);
        assert_eq!(
            e.resume(&i.id, &pactum_core::Cancellation::default())
                .await
                .unwrap()
                .outcome,
            PlanOutcome::Complete
        );
        let mut changed = c.clone();
        changed.budget.soft.insert(Dimension::Bytes, 1);
        let altered = DefaultTransactionPlanner.plan(&i, &changed).unwrap();
        assert!(
            e.execute(
                altered.transaction_plan().clone(),
                &pactum_core::Cancellation::default()
            )
            .await
            .is_err()
        );
    }
}
#[tokio::test]
async fn failed_chunk_blocks_publication_across_restart_and_generation_conflicts_stay_explicit() {
    for fault in [0, 2] {
        let dir = tempfile::tempdir().unwrap();
        let (mut i, c) = fixture(5, Consistency::StagedVisibility);
        if fault == 0 {
            let IntentWork::Native { publication, .. } = &mut i.work else {
                unreachable!()
            };
            publication.as_mut().unwrap().expected_generation = 1;
        }
        let accepted = DefaultTransactionPlanner.plan(&i, &c).unwrap();
        let adapter = Arc::new(EnvelopeAdapter::open(&dir.path().join("backend.db"), fault));
        let mut e = engine(&dir.path().join("plan.db"), adapter.clone());
        let status = e
            .execute(
                accepted.transaction_plan().clone(),
                &pactum_core::Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(status.outcome, PlanOutcome::PartiallyFailed);
        assert_eq!(adapter.visible(), 0);
        drop(e);
        drop(adapter);
        let adapter = Arc::new(EnvelopeAdapter::open(&dir.path().join("backend.db"), 0));
        let mut e = engine(&dir.path().join("plan.db"), adapter.clone());
        assert!(
            e.recover_pending(&pactum_core::Cancellation::default())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(adapter.visible(), 0);
    }
}

#[test]
fn soft_preferences_cannot_overrule_hard_feasibility_and_unknowns_do_not_force_splits() {
    let (i, mut c) = fixture(5, Consistency::PartialAllowed);
    caps(&i, &mut c).budget.soft.insert(Dimension::Bytes, 1500);
    // Per-transaction overhead makes five singleton chunks exceed the operation limit.
    caps(&i, &mut c).overhead.0.insert(Dimension::Operations, 1);
    c.budget.hard.insert(Dimension::Operations, 8);
    let plan = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    assert_eq!(plan.explain().estimate.0[&Dimension::Operations], 8);
    assert!(!plan.explain().warnings.is_empty());
    let (mut i, mut c) = fixture(5, Consistency::PartialAllowed);
    let IntentWork::Native { units, .. } = &mut i.work else {
        unreachable!()
    };
    for unit in units {
        unit.estimate.0.remove(&Dimension::Duration);
    }
    caps(&i, &mut c).budget.soft.insert(Dimension::Duration, 1);
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    let StepWork::Native(Request::Chunked(r)) = &p.transaction_plan().steps[0].work else {
        panic!()
    };
    assert_eq!(r.chunks.len(), 3);
    assert!(
        p.explain()
            .warnings
            .iter()
            .any(|w| w == "soft_budget:transaction:Duration")
    );
}
#[test]
fn serialized_recipes_and_custom_partitions_cannot_bypass_validation() {
    let (i, c) = fixture(5, Consistency::PartialAllowed);
    for ends in [
        vec![],
        vec![0, 5],
        vec![3, 2, 5],
        vec![5, 5],
        vec![6],
        vec![1],
    ] {
        assert!(ExecutionPlan::accept(&i, &c, Mode::Chunked, ends).is_err());
    }
    let p = DefaultTransactionPlanner.plan(&i, &c).unwrap();
    let mut recipe = serde_json::to_value(p).unwrap();
    recipe["ends"] = serde_json::json!([5]); // Entire workload exceeds per-unit hard limit.
    assert!(serde_json::from_value::<ExecutionPlan>(recipe).is_err());
    struct WrongInput;
    impl TransactionPlanner for WrongInput {
        fn plan(
            &self,
            i: &TransactionIntent,
            c: &PlanningContext,
        ) -> Result<ExecutionPlan, PlanningError> {
            let mut altered = i.clone();
            altered.revision += 1;
            DefaultTransactionPlanner.plan(&altered, c)
        }
    }
    let mut registry = PlannerRegistry::default();
    registry
        .register("wrong".into(), Arc::new(WrongInput))
        .unwrap();
    assert!(matches!(
        registry.plan("wrong", &i, &c),
        Err(PlanningError::Invalid { .. })
    ));
}
