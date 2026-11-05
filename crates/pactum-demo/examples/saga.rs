//! Run with a retained directory to demonstrate idempotent business compensation.
#[path = "support/saga.rs"]
mod support;
use pactum_core::{Cancellation, saga::SagaOutcome};
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() {
    let directory = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "saga-data".into()),
    );
    std::fs::create_dir_all(&directory).unwrap();
    let stub = Arc::new(support::ExternalStub::open(&directory.join("external.db")));
    let mut engine = support::open(&directory, stub.clone());
    for (name, fail, permanent, irreversible, expected) in [
        ("success", false, false, false, SagaOutcome::Completed),
        ("compensated", true, false, false, SagaOutcome::Compensated),
        (
            "permanent-failure",
            true,
            true,
            false,
            SagaOutcome::NeedsIntervention,
        ),
        (
            "irreversible",
            true,
            false,
            true,
            SagaOutcome::NeedsIntervention,
        ),
    ] {
        let s = engine
            .execute(
                support::spec(name, fail, permanent, irreversible),
                &Cancellation::default(),
            )
            .await
            .unwrap();
        assert_eq!(s.outcome, expected);
        println!(
            "{name}: {:?}, manual={}, detail={:?}",
            s.outcome, s.needs_intervention, s.last_error
        );
    }
    println!(
        "Retained balance: {} (30 successful + 10 failed compensation + 10 irreversible). Compensation is not atomic rollback.",
        stub.balance()
    );
    assert_eq!(stub.balance(), 50);
}
