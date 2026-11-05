//! Native transaction invariants, broker faults, concurrent workers and process
//! exits at producer/send/inbox/ack boundaries, followed by durable reopen.
use async_trait::async_trait;
use pactum_messaging::*;
use pactum_sqlite::messaging::SqliteMessaging;
use rusqlite::params;
use std::{
    path::Path,
    sync::{Arc, Barrier},
    time::Duration,
};

fn err(e: impl Into<pactum_sqlite::AdapterError>) -> Error {
    Error::from_diagnostic(e.into().diagnostic())
}
fn message(id: &str) -> Message {
    Message {
        id: id.into(),
        route: "counters".into(),
        ordering_key: None,
        sequence: None,
        version: 1,
        payload: vec![7],
    }
}
fn ordered(id: &str, sequence: u64) -> Message {
    Message {
        ordering_key: Some("account".into()),
        sequence: Some(sequence),
        ..message(id)
    }
}
fn policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 3,
        backoff_ms: 10,
        max_backoff_ms: 20,
    }
}
fn open(path: &Path) -> SqliteMessaging {
    let store = SqliteMessaging::open(path).unwrap();
    store
        .connection()
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS effects(key TEXT PRIMARY KEY, value INTEGER NOT NULL);",
        )
        .unwrap();
    store
}
fn effect(tx: &rusqlite::Transaction<'_>, key: &str) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO effects VALUES(?1,1) ON CONFLICT(key) DO UPDATE SET value=value+1",
        [key],
    )
    .map_err(err)?;
    Ok(())
}
fn value(store: &SqliteMessaging, key: &str) -> i64 {
    store
        .connection()
        .query_row(
            "SELECT COALESCE((SELECT value FROM effects WHERE key=?1),0)",
            [key],
            |r| r.get(0),
        )
        .unwrap()
}

#[test]
fn producer_atomicity_native_hook_and_permanent_binding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("producer.db");
    let mut s = open(&path);
    let m = message("one");
    assert!(
        s.produce(&m, policy(), |tx| {
            effect(tx, "producer")?;
            Err(err("rollback"))
        })
        .is_err()
    );
    assert_eq!(value(&s, "producer"), 0);
    assert!(s.delivery(&m.id).unwrap().is_none());
    assert!(
        s.produce(&m, policy(), |tx| effect(tx, "producer"))
            .unwrap()
    );
    drop(s);
    let mut s = open(&path);
    assert!(
        !s.produce(&m, policy(), |_| panic!("duplicate mutation"))
            .unwrap()
    );
    for changed in [
        Message {
            payload: vec![8],
            ..m.clone()
        },
        Message {
            route: "other".into(),
            ..m.clone()
        },
        Message {
            version: 2,
            ..m.clone()
        },
    ] {
        assert!(
            s.produce(&changed, policy(), |_| panic!("conflicting mutation"))
                .is_err()
        );
    }
    assert!(s.produce(&m, RetryPolicy::default(), |_| Ok(())).is_err());
    assert_eq!(value(&s, "producer"), 1);
    drop(s);
    // Applications can insert into their own pre-existing native transaction.
    let mut connection = rusqlite::Connection::open(&path).unwrap();
    let tx = connection.transaction().unwrap();
    assert!(SqliteMessaging::enqueue(&tx, &message("native"), policy()).unwrap());
    effect(&tx, "native").unwrap();
    tx.rollback().unwrap();
    let s = open(&path);
    assert!(s.delivery("native").unwrap().is_none());
    assert_eq!(value(&s, "native"), 0);
}

#[test]
fn leases_are_exclusive_fenced_and_restart_safe_with_bounded_crash_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("leases.db");
    let mut first = open(&path);
    first
        .produce(&message("one"), policy(), |_| Ok(()))
        .unwrap();
    let old = first.claim(0, 10, 1).unwrap().remove(0);
    let mut second = open(&path);
    assert!(second.claim(9, 10, 1).unwrap().is_empty());
    let current = second.claim(10, 10, 1).unwrap().remove(0);
    assert_eq!(current.token, old.token + 1);
    assert!(first.acknowledge(&old).is_err());
    assert!(
        first
            .fail(&old, 10, &SendError::rejected("stale".into()))
            .is_err()
    );
    drop(first);
    drop(second);
    let mut s = open(&path);
    assert!(s.claim(19, 10, 1).unwrap().is_empty());
    let final_claim = s.claim(20, 10, 1).unwrap().remove(0);
    assert_eq!(final_claim.token, 3);
    drop(s);
    let mut s = open(&path);
    assert!(s.claim(30, 10, 1).unwrap().is_empty());
    let dead = s.delivery("one").unwrap().unwrap();
    assert_eq!(dead.state, DeliveryState::Dead);
    assert!(dead.last_error.unwrap().contains("unknown"));
    assert!(s.acknowledge(&final_claim).is_err());
    s.replay("one", 30).unwrap();
    let replay = s.claim(30, 10, 1).unwrap().remove(0);
    assert_eq!(replay.token, 4);
    assert!(s.acknowledge(&final_claim).is_err());
    s.acknowledge(&replay).unwrap();
    assert_eq!(
        s.delivery("one").unwrap().unwrap().state,
        DeliveryState::Delivered
    );
    assert!(s.claim(100, 10, 1).unwrap().is_empty());
    assert!(s.replay("one", 100).is_err());
}

#[tokio::test]
async fn fault_transport_backoff_dead_letters_replay_and_duplicate_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("faults.db");
    let mut s = open(&path);
    s.produce(&message("event"), policy(), |tx| effect(tx, "producer"))
        .unwrap();
    let dispatcher = Dispatcher {
        batch_size: 1,
        ..Dispatcher::default()
    };
    let mut transport = FakeTransport::default();
    transport
        .faults
        .extend([Fault::Partition, Fault::Drop, Fault::LoseAck]);
    for (now, next) in [(0, 10), (10, 30), (30, 50)] {
        assert_eq!(
            dispatcher
                .dispatch(&mut s, &mut transport, || now)
                .await
                .unwrap()
                .failed,
            1
        );
        assert_eq!(s.delivery("event").unwrap().unwrap().available_at, next);
        assert_eq!(
            dispatcher
                .dispatch(&mut s, &mut transport, || now)
                .await
                .unwrap(),
            DispatchReport::default()
        );
    }
    assert_eq!(
        s.delivery("event").unwrap().unwrap().state,
        DeliveryState::Dead
    );
    s.replay("event", 50).unwrap();
    transport.faults.push_back(Fault::Duplicate);
    assert_eq!(
        dispatcher
            .dispatch(&mut s, &mut transport, || 50)
            .await
            .unwrap()
            .delivered,
        1
    );
    assert_eq!(transport.deliveries.len(), 3);
    while let Some(m) = transport.deliveries.pop_front() {
        let result = s
            .consume("consumer", &m, OrderingPolicy::Unordered, 3, |tx| {
                effect(tx, "consumer")
            })
            .unwrap();
        assert!(matches!(
            result,
            ConsumeResult::Applied | ConsumeResult::Duplicate
        ));
    }
    assert_eq!(value(&s, "producer"), 1);
    assert_eq!(value(&s, "consumer"), 1);
    s.produce(&message("reject"), policy(), |_| Ok(())).unwrap();
    transport.faults.push_back(Fault::Reject);
    dispatcher
        .dispatch(&mut s, &mut transport, || 50)
        .await
        .unwrap();
    assert_eq!(
        s.delivery("reject").unwrap().unwrap().state,
        DeliveryState::Dead
    );
    assert_eq!(s.deliveries("", 10).unwrap().len(), 2);
}

struct Hanging;
#[async_trait]
impl Transport for Hanging {
    async fn send(&mut self, _: &Message) -> Result<(), SendError> {
        std::future::pending().await
    }
}
#[tokio::test]
async fn timeout_and_cancelled_pass_leave_recoverable_messages() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(&dir.path().join("timeout.db"));
    s.produce(&message("one"), policy(), |_| Ok(())).unwrap();
    let d = Dispatcher {
        send_timeout: Duration::from_millis(1),
        ..Dispatcher::default()
    };
    assert_eq!(
        d.dispatch(&mut s, &mut Hanging, || 0).await.unwrap().failed,
        1
    );
    let timed_out = s.delivery("one").unwrap().unwrap();
    assert_eq!(timed_out.state, DeliveryState::Pending);
    let failure = timed_out.failure.unwrap();
    assert_eq!(failure.code, pactum_errors::ErrorCode::Timeout);
    assert_eq!(
        failure.disposition,
        pactum_errors::RetryDisposition::ReconcileBeforeRetry
    );
    let d = Dispatcher {
        send_timeout: Duration::from_secs(60),
        lease_ms: 10,
        ..d
    };
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            d.dispatch(&mut s, &mut Hanging, || 10)
        )
        .await
        .is_err()
    );
    assert_eq!(
        s.delivery("one").unwrap().unwrap().state,
        DeliveryState::Leased
    );
    let report = d
        .dispatch(&mut s, &mut FakeTransport::default(), || 20)
        .await
        .unwrap();
    assert_eq!(report.delivered, 1);
}

#[test]
fn inbox_poison_rollback_ordering_gaps_replay_and_identity_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inbox.db");
    let mut s = open(&path);
    let one = ordered("one", 1);
    let two = ordered("two", 2);
    assert_eq!(
        s.consume("strict", &two, OrderingPolicy::Strict, 2, |_| panic!(
            "gap handler"
        ))
        .unwrap(),
        ConsumeResult::OutOfOrder
    );
    assert_eq!(s.inbox_status("strict", "two").unwrap().unwrap().1, 0);
    assert!(
        s.consume(
            "strict",
            &ordered("other", 2),
            OrderingPolicy::Strict,
            2,
            |_| Ok(())
        )
        .is_err()
    );
    for expected in [
        ConsumeResult::Retry("poison".into()),
        ConsumeResult::Dead("poison".into()),
    ] {
        assert_eq!(
            s.consume("strict", &one, OrderingPolicy::Strict, 2, |tx| {
                effect(tx, "consumer")?;
                Err(err("poison"))
            })
            .unwrap(),
            expected
        );
        assert_eq!(value(&s, "consumer"), 0);
    }
    drop(s);
    let mut s = open(&path);
    assert!(matches!(
        s.consume("strict", &one, OrderingPolicy::Strict, 2, |_| panic!(
            "poison handler"
        ))
        .unwrap(),
        ConsumeResult::Dead(_)
    ));
    assert_eq!(
        s.consume("strict", &two, OrderingPolicy::Strict, 2, |_| panic!(
            "blocked handler"
        ))
        .unwrap(),
        ConsumeResult::OutOfOrder
    );
    s.replay_inbox("strict", "one").unwrap();
    for m in [&one, &two] {
        assert_eq!(
            s.consume("strict", m, OrderingPolicy::Strict, 2, |tx| effect(
                tx, "consumer"
            ))
            .unwrap(),
            ConsumeResult::Applied
        );
    }
    drop(s);
    let mut s = open(&path);
    assert_eq!(
        s.consume("strict", &one, OrderingPolicy::Strict, 2, |_| panic!(
            "replayed effect"
        ))
        .unwrap(),
        ConsumeResult::Duplicate
    );
    assert!(
        s.consume(
            "strict",
            &Message {
                payload: vec![8],
                ..one.clone()
            },
            OrderingPolicy::Strict,
            2,
            |_| Ok(())
        )
        .is_err()
    );
    assert!(
        s.consume("strict", &one, OrderingPolicy::Unordered, 2, |_| Ok(()))
            .is_err()
    );
    assert!(s.replay_inbox("strict", "one").is_err());
    assert_eq!(value(&s, "consumer"), 2);
    // Independent subscribers and stream keys have independent effects/watermarks.
    assert_eq!(
        s.consume("other", &one, OrderingPolicy::Strict, 2, |tx| effect(
            tx, "consumer"
        ))
        .unwrap(),
        ConsumeResult::Applied
    );
    let other_key = Message {
        id: "other-key".into(),
        ordering_key: Some("another".into()),
        ..one
    };
    assert_eq!(
        s.consume(
            "strict",
            &other_key,
            OrderingPolicy::Strict,
            2,
            |tx| effect(tx, "consumer")
        )
        .unwrap(),
        ConsumeResult::Applied
    );
}

#[test]
fn independent_connections_serialize_duplicate_producers_consumers_and_claims() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("concurrent.db");
    drop(open(&path));
    let barrier = Arc::new(Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut s = open(&path);
                barrier.wait();
                let m = message("same");
                s.produce(&m, policy(), |tx| effect(tx, "producer"))
                    .unwrap();
                let consumed = s
                    .consume("c", &m, OrderingPolicy::Unordered, 3, |tx| {
                        effect(tx, "consumer")
                    })
                    .unwrap();
                let claims = s.claim(0, 100, 1).unwrap();
                (consumed, claims.len())
            })
        })
        .collect();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert_eq!(
        results
            .iter()
            .filter(|(r, _)| *r == ConsumeResult::Applied)
            .count(),
        1
    );
    assert_eq!(results.iter().map(|(_, n)| n).sum::<usize>(), 1);
    let s = open(&path);
    assert_eq!(value(&s, "producer"), 1);
    assert_eq!(value(&s, "consumer"), 1);
}

#[test]
fn validation_and_future_schema_fail_before_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("validation.db");
    let mut s = open(&path);
    for m in [
        Message {
            id: "".into(),
            ..message("a")
        },
        Message {
            version: 0,
            ..message("a")
        },
        Message {
            sequence: Some(1),
            ..message("a")
        },
    ] {
        assert!(
            s.produce(&m, policy(), |_| panic!("invalid producer"))
                .is_err()
        );
        assert!(
            s.consume("c", &m, OrderingPolicy::Unordered, 3, |_| panic!(
                "invalid consumer"
            ))
            .is_err()
        );
    }
    assert!(s.claim(-1, 10, 1).is_err());
    assert!(s.claim(i64::MAX, 10, 1).is_err());
    assert!(s.claim(0, 0, 1).is_err());
    assert!(s.claim(0, 10, 0).is_err());
    assert!(
        s.consume("c", &message("a"), OrderingPolicy::Strict, 3, |_| Ok(()))
            .is_err()
    );
    assert!(
        s.produce(
            &message("a"),
            RetryPolicy {
                max_attempts: 0,
                ..policy()
            },
            |_| Ok(())
        )
        .is_err()
    );
    assert_eq!(policy().delay(u32::MAX), 20);
    s.connection()
        .execute("UPDATE pactum_message_meta SET version=3", [])
        .unwrap();
    drop(s);
    assert!(SqliteMessaging::open(&path).is_err());
}

// Parent kills bypass all Rust destructors, leaving WAL recovery
// responsible for uncommitted producer/consumer transactions. The "broker" is
// a separate durable SQLite inbox, so accepted messages survive sender death.
fn crash(point: &str, target: &str) {
    if point == target {
        let directory = std::env::var("PACTUM_MESSAGE_CRASH_DIR").unwrap();
        std::fs::write(Path::new(&directory).join("ready"), point).unwrap();
        loop {
            std::thread::park_timeout(Duration::from_secs(1));
        }
    }
}
#[test]
fn messaging_crash_child() {
    let Ok(directory) = std::env::var("PACTUM_MESSAGE_CRASH_DIR") else {
        return;
    };
    let target = std::env::var("PACTUM_MESSAGE_CRASH_POINT").unwrap();
    let path = Path::new(&directory);
    let mut producer = open(&path.join("producer.db"));
    let m = message("event");
    producer
        .produce(&m, policy(), |tx| {
            effect(tx, "producer")?;
            crash("producer_uncommitted", &target);
            Ok(())
        })
        .unwrap();
    crash("producer_committed", &target);
    let claim = producer.claim(0, 10, 1).unwrap().remove(0);
    crash("before_send", &target);
    let mut consumer = open(&path.join("consumer.db"));
    consumer
        .consume("c", &m, OrderingPolicy::Unordered, 3, |tx| {
            effect(tx, "consumer")?;
            crash("consumer_uncommitted", &target);
            Ok(())
        })
        .unwrap();
    crash("after_send_before_ack", &target);
    if target == "ack_uncommitted" {
        producer
            .connection()
            .execute_batch(
                "BEGIN IMMEDIATE; UPDATE pactum_outbox SET state='delivered' WHERE id='event'",
            )
            .unwrap();
        crash("ack_uncommitted", &target);
    }
    producer.acknowledge(&claim).unwrap();
    crash("after_ack", &target);
    panic!("unrecognized crash point");
}
#[test]
fn process_crash_matrix_recovers_committed_events_without_double_effects() {
    for target in [
        "producer_uncommitted",
        "producer_committed",
        "before_send",
        "consumer_uncommitted",
        "after_send_before_ack",
        "ack_uncommitted",
        "after_ack",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "messaging_crash_child", "--nocapture"])
            .env("PACTUM_MESSAGE_CRASH_DIR", dir.path())
            .env("PACTUM_MESSAGE_CRASH_POINT", target)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !dir.path().join("ready").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before {target}: {status}");
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("child did not reach {target}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success(), "{target}");
        let mut producer = open(&dir.path().join("producer.db"));
        let mut consumer = open(&dir.path().join("consumer.db"));
        if target == "producer_uncommitted" {
            assert!(producer.delivery("event").unwrap().is_none());
            assert_eq!(value(&producer, "producer"), 0);
        }
        let m = message("event");
        producer
            .produce(&m, policy(), |tx| effect(tx, "producer"))
            .unwrap();
        let claims = producer.claim(10, 10, 10).unwrap();
        for claim in claims {
            consumer
                .consume("c", &claim.message, OrderingPolicy::Unordered, 3, |tx| {
                    effect(tx, "consumer")
                })
                .unwrap();
            producer.acknowledge(&claim).unwrap();
        }
        // Model a delayed duplicate delivery even after local publication ack.
        consumer
            .consume("c", &m, OrderingPolicy::Unordered, 3, |tx| {
                effect(tx, "consumer")
            })
            .unwrap();
        assert_eq!(value(&producer, "producer"), 1, "{target}");
        assert_eq!(value(&consumer, "consumer"), 1, "{target}");
        assert_eq!(
            producer.delivery("event").unwrap().unwrap().state,
            DeliveryState::Delivered,
            "{target}"
        );
        assert!(producer.claim(100, 10, 10).unwrap().is_empty());
        // Validate receipt is in the same committed database as the effect.
        assert_eq!(
            consumer
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM pactum_inbox WHERE consumer=?1 AND state='applied'",
                    params!["c"],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }
}

#[test]
fn typed_send_failure_survives_restart_and_v1_outbox_migration_preserves_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("metadata.db");
    let mut store = open(&path);
    let message = message("metadata-message");
    store.produce(&message, policy(), |_| Ok(())).unwrap();
    let claim = store.claim(0, 10, 1).unwrap().remove(0);
    let failure = SendError::from_source(std::io::Error::other("token=SECRET"));
    store.fail(&claim, 0, &failure).unwrap();
    let saved = store.delivery(&message.id).unwrap().unwrap();
    let metadata = saved.failure.as_ref().unwrap();
    assert_eq!(metadata.code, pactum_errors::ErrorCode::TransportUnknown);
    assert_eq!(
        metadata.context.transaction.as_deref(),
        Some(message.id.as_str())
    );
    let raw: String = store
        .connection()
        .query_row(
            "SELECT last_failure FROM pactum_outbox WHERE id=?1",
            [&message.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!raw.contains("SECRET"));
    drop(store);
    let store = open(&path);
    assert_eq!(store.delivery(&message.id).unwrap().unwrap(), saved);
    // Recreate the original v1 schema around an existing retained envelope.
    store.connection().execute_batch("ALTER TABLE pactum_outbox DROP COLUMN last_failure; UPDATE pactum_message_meta SET version=1;").unwrap();
    drop(store);
    let mut migrated = open(&path);
    let old = migrated.delivery(&message.id).unwrap().unwrap();
    assert_eq!(old.message, saved.message);
    assert_eq!(old.state, saved.state);
    assert_eq!(old.attempts, saved.attempts);
    assert!(old.failure.is_none());
    assert!(
        !migrated
            .produce(&message, policy(), |_| panic!(
                "must retain the original producer receipt"
            ))
            .unwrap()
    );
    let version: u32 = migrated
        .connection()
        .query_row("SELECT version FROM pactum_message_meta", [], |r| r.get(0))
        .unwrap();
    assert_eq!(version, 2);
}
