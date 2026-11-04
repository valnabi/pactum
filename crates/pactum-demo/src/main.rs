//! Standalone commit/abort/restart demonstration. See `--help`.
use pactum_core::{
    Cancellation, Coordinator, Enlistment, Participant, ParticipantId, RetryPolicy, TransactionId,
};
use pactum_demo::DurableCounter;
use pactum_sqlite::SqliteJournal;
use std::{path::PathBuf, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || !matches!(args[0].as_str(), "demo" | "recover") {
        println!(
            "Usage: pactum-demo <demo|recover> <data-directory>\nRepeat demo safely; recover reconciles only previously started work."
        );
        return Ok(());
    }
    let directory = PathBuf::from(&args[1]);
    std::fs::create_dir_all(&directory)?;
    let a = Arc::new(DurableCounter::open(
        directory.join("a.db"),
        ParticipantId::new("counter-a")?,
    )?);
    let b = Arc::new(DurableCounter::open(
        directory.join("b.db"),
        ParticipantId::new("counter-b")?,
    )?);
    let participants: Vec<Arc<dyn Participant>> = vec![a.clone(), b.clone()];
    let mut coordinator = Coordinator::open(
        SqliteJournal::open(directory.join("journal.db"))?,
        participants,
        RetryPolicy::default(),
    )?;
    let cancellation = Cancellation::default();
    for status in coordinator.recover(&cancellation).await? {
        println!("recovered: {status:?}");
    }
    if args[0] == "demo" {
        for (id, delta_b) in [("demo-commit", "20"), ("demo-abort", "-1")] {
            let status = coordinator
                .execute(
                    TransactionId::new(id)?,
                    vec![
                        Enlistment {
                            participant: a.id(),
                            payload: b"10".to_vec(),
                        },
                        Enlistment {
                            participant: b.id(),
                            payload: delta_b.as_bytes().to_vec(),
                        },
                    ],
                    &cancellation,
                )
                .await?;
            println!("{status:?}");
        }
    }
    println!(
        "durable counter values: a={}, b={}",
        a.value().await?,
        b.value().await?
    );
    Ok(())
}
