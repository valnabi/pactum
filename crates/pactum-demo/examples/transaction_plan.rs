//! Standalone durable Atomic -> Chunked -> StagedPublish transaction plan.
#[path = "support/plan.rs"]
mod support;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "transaction-plan-data".into()),
    );
    std::fs::create_dir_all(&directory)?;
    let mut engine = support::open(&directory);
    println!("{:?}", support::finish(&mut engine).await);
    drop(engine);
    let backend = pactum_sqlite::SqliteBackend::open(directory.join("backend.db"), "plan-example")?;
    println!(
        "counter={}, published={:?}",
        backend.value("counter")?,
        backend.visible(&pactum_core::execution::OperationId::new("catalog")?)?
    );
    Ok(())
}
