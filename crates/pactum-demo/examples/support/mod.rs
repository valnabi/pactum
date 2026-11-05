#![allow(dead_code)]
use pactum_core::{
    Cancellation, Coordinator, Enlistment, Participant, ParticipantId, RetryPolicy, TransactionId,
    execution::*,
};
use pactum_demo::DurableCounter;
use pactum_sqlite::{SqliteBackend, SqliteJournal, Value};
use std::{path::PathBuf, sync::Arc};

fn id(s: &str) -> OperationId {
    OperationId::new(s).unwrap()
}
fn payload(value: i64) -> Vec<u8> {
    serde_json::to_vec(&vec![Value {
        key: "counter".into(),
        value,
    }])
    .unwrap()
}
// The same lifecycle works for Engine and the unchanged v0.1 Coordinator.
async fn run<E: Execution>(
    engine: &mut E,
    id: E::Id,
    request: E::Request,
) -> Result<E::Status, pactum_core::Error> {
    Execution::execute(engine, id, request, &Cancellation::default()).await
}
pub async fn demo(mode: &str) -> Result<(), Box<dyn std::error::Error>> {
    let directory = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| format!("{mode}-data")),
    );
    std::fs::create_dir_all(&directory)?;
    let selected = |name| mode == name || mode == "multi_mode";
    {
        let open = || {
            Engine::open(
                SqliteJournal::open(directory.join("modes-journal.db"))
                    .map_err(pactum_core::Error::in_doubt_source)?,
                SqliteBackend::open(directory.join("modes-backend.db"), "demo")
                    .map_err(pactum_core::Error::invalid_source)?,
                ExecutionPolicy {
                    max_chunks: 1,
                    ..ExecutionPolicy::default()
                },
            )
        };
        // SqliteJournal errors are mapped explicitly to preserve the engine error boundary.
        let mut e = open()?;
        let c = Cancellation::default();
        for s in e.recover(&c).await? {
            println!("recovered: {s:?}");
        }
        if selected("atomic") {
            println!(
                "{:?}",
                run(
                    &mut e,
                    id("atomic"),
                    Request::Atomic(AtomicRequest {
                        payload: payload(7)
                    })
                )
                .await?
            );
        }
        if selected("chunked") {
            let r = Request::Chunked(ChunkedRequest {
                chunks: vec![payload(2), payload(3), payload(5)],
            });
            println!("first pass: {:?}", run(&mut e, id("job"), r.clone()).await?);
            Execution::cancel(&mut e, &id("job")).await?;
            drop(e); // Close both database handles and reopen, preserving durable progress.
            e = open()?;
            println!(
                "paused after restart: {:?}",
                Execution::status(&e, &id("job"))?
            );
            run(&mut e, id("job"), r).await?;
            for s in e.recover(&c).await? {
                println!("resumed: {s:?}");
            }
        }
        if selected("staged_publish") {
            println!(
                "{:?}",
                run(
                    &mut e,
                    id("version-one"),
                    Request::StagedPublish(PublishRequest {
                        target: id("catalog"),
                        expected_generation: 0,
                        payload: payload(42)
                    })
                )
                .await?
            );
            println!(
                "visible snapshot: {:?}",
                e.backend().visible(&id("catalog"))?
            );
        }
        println!("counter total: {}", e.backend().value("counter")?);
    }
    if selected("two_phase") {
        let a = Arc::new(DurableCounter::open(
            directory.join("a.db"),
            ParticipantId::new("a")?,
        )?);
        let b = Arc::new(DurableCounter::open(
            directory.join("b.db"),
            ParticipantId::new("b")?,
        )?);
        let mut coordinator = Coordinator::open(
            SqliteJournal::open(directory.join("2pc-journal.db"))?,
            vec![a.clone(), b.clone()],
            RetryPolicy::default(),
        )?;
        Execution::recover(&mut coordinator, &Cancellation::default()).await?;
        let s = run(
            &mut coordinator,
            TransactionId::new("2pc")?,
            TwoPhaseRequest {
                enlistments: vec![
                    Enlistment {
                        participant: a.id(),
                        payload: b"10".to_vec(),
                    },
                    Enlistment {
                        participant: b.id(),
                        payload: b"20".to_vec(),
                    },
                ],
            },
        )
        .await?;
        println!("2PC: {s:?}; a={}, b={}", a.value().await?, b.value().await?);
    }
    Ok(())
}
