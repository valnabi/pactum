#![allow(dead_code)]
use pactum_core::{Cancellation, execution::*, plan::*};
use pactum_sqlite::{SqliteBackend, SqliteJournal, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

pub fn payload(value: i64) -> Vec<u8> {
    serde_json::to_vec(&vec![Value {
        key: "counter".into(),
        value,
    }])
    .unwrap()
}
pub fn spec() -> TransactionPlan {
    let step = |name: &str, dependency: Option<&str>, work| Step {
        id: StepId::new(name).unwrap(),
        adapter: AdapterId::new("local-v1").unwrap(),
        dependencies: dependency
            .into_iter()
            .map(|s| StepId::new(s).unwrap())
            .collect(),
        resources: BTreeSet::from(["catalog".into()]),
        work: StepWork::Native(work),
    };
    TransactionPlan {
        version: 1,
        id: PlanId::new("catalog-import-v1").unwrap(),
        revision: 1,
        steps: vec![
            step(
                "load",
                None,
                Request::Atomic(AtomicRequest {
                    payload: payload(7),
                }),
            ),
            step(
                "batch",
                Some("load"),
                Request::Chunked(ChunkedRequest {
                    chunks: vec![payload(2), payload(3), payload(5)],
                }),
            ),
            step(
                "publish",
                Some("batch"),
                Request::StagedPublish(PublishRequest {
                    target: OperationId::new("catalog").unwrap(),
                    expected_generation: 0,
                    payload: payload(42),
                }),
            ),
        ],
    }
}
pub fn adapters(directory: &Path) -> BTreeMap<AdapterId, Arc<dyn StepAdapter>> {
    let engine = Engine::open(
        SqliteJournal::open(directory.join("modes.db")).unwrap(),
        SqliteBackend::open(directory.join("backend.db"), "plan-example").unwrap(),
        ExecutionPolicy {
            max_chunks: 1,
            ..ExecutionPolicy::default()
        },
    )
    .unwrap();
    BTreeMap::from([(
        AdapterId::new("local-v1").unwrap(),
        Arc::new(NativeAdapter::new(engine)) as Arc<dyn StepAdapter>,
    )])
}
pub fn open(directory: &Path) -> PlanEngine<SqliteJournal> {
    PlanEngine::open(
        SqliteJournal::open(directory.join("plan.db")).unwrap(),
        PlanId::new("plan-example-v1").unwrap(),
        adapters(directory),
        PlanPolicy::default(),
    )
    .unwrap()
}
pub async fn finish(engine: &mut PlanEngine<impl pactum_journal::Journal>) -> PlanStatus {
    let c = Cancellation::default();
    let mut status = engine.execute(spec(), &c).await.unwrap();
    for _ in 0..8 {
        if status.is_terminal() {
            return status;
        }
        status = engine.resume(&spec().id, &c).await.unwrap();
    }
    panic!("plan did not settle: {status:?}")
}
