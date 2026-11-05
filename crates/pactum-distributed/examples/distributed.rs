//! Optional fenced 2PC over the loopback remote protocol; repeat with the same DB.
use pactum_core::{Cancellation, Enlistment, ParticipantId, RetryPolicy, TransactionId};
use pactum_distributed::{
    DistributedCoordinator, FencedResource, LeaseAuthority,
    reference::ReferenceStore,
    transport::{Loopback, RemoteResource},
};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "distributed.db".into());
    let store = ReferenceStore::open(path, "distributed-example")?;
    let id = ParticipantId::new("counter")?;
    let counter = store.counter(id.clone())?;
    let transport = Arc::new(Loopback::new(counter.clone()));
    let remote = Arc::new(RemoteResource::new(
        id.clone(),
        counter.domain(),
        transport,
        Duration::from_secs(1),
    )?);
    let grant = store.acquire("example-owner", Duration::from_secs(60))?;
    let mut coordinator =
        DistributedCoordinator::open(&store, grant.clone(), vec![remote], RetryPolicy::default())?;
    coordinator.recover(&Cancellation::default()).await?;
    let result = coordinator
        .execute(
            TransactionId::new("add-seven")?,
            vec![Enlistment {
                participant: id,
                payload: b"7".to_vec(),
            }],
            &Cancellation::default(),
        )
        .await?;
    println!("{:?}; counter={}", result.outcome, counter.value()?);
    store.release(&grant)?;
    Ok(())
}
