//! Reproducible replay/history-growth benchmark, without a hosted service.
use pactum_core::{Cancellation, execution::*};
use pactum_journal::{Journal, Retention};
use pactum_sqlite::{SqliteBackend, SqliteJournal};
use std::{hint::black_box, time::Instant};

fn event(journal: &mut SqliteJournal, value: serde_json::Value) {
    journal
        .append(2, &serde_json::to_vec(&value).unwrap())
        .unwrap();
}
fn open(path: &std::path::Path) -> Engine<SqliteJournal, SqliteBackend> {
    Engine::open(
        SqliteJournal::open(path.join("j.db")).unwrap(),
        SqliteBackend::open(path.join("b.db"), "benchmark").unwrap(),
        ExecutionPolicy::default(),
    )
    .unwrap()
}
fn main() {
    println!(
        "terminal_keys,history_records,full_open_ms,snapshot_bytes,snapshot_open_ms,one_pending_us,empty_pending_avg_ns"
    );
    for count in [100, 1_000, 10_000] {
        let temp = tempfile::tempdir().unwrap();
        let backend = SqliteBackend::open(temp.path().join("b.db"), "benchmark").unwrap();
        let identity = backend.identity();
        drop(backend);
        let mut j = SqliteJournal::open(temp.path().join("j.db")).unwrap();
        event(
            &mut j,
            serde_json::json!({"event":"Header", "backend":identity}),
        );
        // Completed aborted atomic operations have no backend effect. Their keys
        // and payload bindings remain permanent and measurable after compaction.
        for n in 0..count {
            let id = format!("terminal-{n:06}");
            event(
                &mut j,
                serde_json::json!({"event":"Begin", "id":id, "request":{"mode":"Atomic", "request":{"payload":[]}}}),
            );
            event(
                &mut j,
                serde_json::json!({"event":"Progress", "id":id, "progress":{"Atomic":"Aborted"}}),
            );
        }
        event(
            &mut j,
            serde_json::json!({"event":"Begin", "id":"pending", "request":{"mode":"Atomic", "request":{"payload":[]}}}),
        );
        j.flush().unwrap();
        drop(j);
        let start = Instant::now();
        let mut engine = open(temp.path());
        let full = start.elapsed();
        engine.compact(Retention::Compact).unwrap();
        drop(engine);
        let mut j = SqliteJournal::open(temp.path().join("j.db")).unwrap();
        let bytes = j.checkpoint().unwrap().unwrap().payload.len();
        assert!(j.replay().unwrap().is_empty());
        drop(j);
        let start = Instant::now();
        let mut engine = open(temp.path());
        let compact = start.elapsed();
        let start = Instant::now();
        assert_eq!(
            engine
                .recover_pending(&Cancellation::default())
                .unwrap()
                .len(),
            1
        );
        let pending = start.elapsed();
        let start = Instant::now();
        for _ in 0..10_000 {
            assert!(
                black_box(engine.recover_pending(&Cancellation::default()).unwrap()).is_empty()
            );
        }
        let empty = start.elapsed().as_nanos() / 10_000;
        println!(
            "{count},{},{:.3},{bytes},{:.3},{:.3},{empty}",
            count * 2 + 2,
            full.as_secs_f64() * 1000.0,
            compact.as_secs_f64() * 1000.0,
            pending.as_secs_f64() * 1_000_000.0
        );
    }
}
