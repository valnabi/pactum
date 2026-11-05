#![allow(dead_code)]
//! External-like receipt resource: no prepare/commit protocol. Every keyed
//! business effect (including compensation) is atomic with its own receipt.
use async_trait::async_trait;
use pactum_core::{
    execution::OperationId,
    plan::{AdapterId, StepId},
    saga::*,
};
use pactum_sqlite::SqliteJournal;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

pub struct ExternalStub {
    db: Mutex<Connection>,
    identity: String,
}
impl ExternalStub {
    pub fn open(path: &Path) -> Self {
        let db = Connection::open(path).unwrap();
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS resource(identity TEXT NOT NULL, balance INTEGER NOT NULL, available INTEGER NOT NULL);
            INSERT INTO resource SELECT lower(hex(randomblob(16))),0,1 WHERE NOT EXISTS (SELECT 1 FROM resource);
            CREATE TABLE IF NOT EXISTS receipts(key TEXT PRIMARY KEY, payload BLOB NOT NULL, result TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS effects(position INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE, delta INTEGER NOT NULL);").unwrap();
        let identity = db
            .query_row("SELECT identity FROM resource", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap();
        Self {
            db: Mutex::new(db),
            identity,
        }
    }
    pub fn balance(&self) -> i64 {
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT balance FROM resource", [], |r| r.get(0))
            .unwrap()
    }
    pub fn effects(&self) -> Vec<i64> {
        let db = self.db.lock().unwrap();
        let mut stmt = db
            .prepare("SELECT delta FROM effects ORDER BY position")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
    pub fn available(&self, available: bool) {
        self.db
            .lock()
            .unwrap()
            .execute("UPDATE resource SET available=?1", [available])
            .unwrap();
    }
}
#[async_trait]
impl SagaAdapter for ExternalStub {
    fn identity(&self) -> String {
        format!("external-stub/{}", self.identity)
    }
    fn accepts(&self, payload: &[u8]) -> bool {
        let Ok(s) = std::str::from_utf8(payload) else {
            return false;
        };
        s == "reject"
            || s.strip_prefix("add:")
                .or_else(|| s.strip_prefix("retry:"))
                .is_some_and(|n| n.parse::<i64>().is_ok())
    }
    async fn execute(
        &self,
        key: OperationId,
        payload: Vec<u8>,
    ) -> Result<ActionResult, pactum_core::saga::SagaError> {
        let run = || -> Result<ActionResult, pactum_sqlite::AdapterError> {
            let mut db = self.db.lock().unwrap();
            let tx = db.transaction()?;
            let old: Option<(Vec<u8>, String)> = tx
                .query_row(
                    "SELECT payload,result FROM receipts WHERE key=?1",
                    [key.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((bound, result)) = old {
                if bound != payload {
                    return Err("receipt payload conflict".into());
                }
                return Ok(serde_json::from_str(&result)?);
            }
            let s = std::str::from_utf8(&payload)?;
            let result = if s == "reject" {
                ActionResult::Rejected("permanent external refusal".into())
            } else {
                if s.starts_with("retry:")
                    && !tx.query_row("SELECT available FROM resource", [], |r| {
                        r.get::<_, bool>(0)
                    })?
                {
                    return Ok(ActionResult::Retryable(
                        "external compensator offline".into(),
                    ));
                }
                let delta = s
                    .split_once(':')
                    .ok_or("invalid payload")?
                    .1
                    .parse::<i64>()?;
                tx.execute("UPDATE resource SET balance=balance+?1", [delta])?;
                tx.execute(
                    "INSERT INTO effects(key,delta) VALUES(?1,?2)",
                    params![key.as_str(), delta],
                )?;
                ActionResult::Applied
            };
            tx.execute(
                "INSERT INTO receipts(key,payload,result) VALUES(?1,?2,?3)",
                params![key.as_str(), payload, serde_json::to_string(&result)?],
            )?;
            tx.commit()?;
            Ok(result)
        };
        run().map_err(|e| pactum_core::saga::SagaError::from_diagnostic(e.diagnostic()))
    }
}
pub fn aid() -> AdapterId {
    AdapterId::new("external-v1").unwrap()
}
pub fn sid(s: &str) -> StepId {
    StepId::new(s).unwrap()
}
pub fn ns() -> SagaId {
    SagaId::new("saga-demo-deployment-v1").unwrap()
}
pub fn operation(payload: &str) -> Operation {
    Operation {
        adapter: aid(),
        payload: payload.as_bytes().to_vec(),
    }
}
pub fn spec(name: &str, failure: bool, permanent: bool, irreversible: bool) -> Saga {
    let mut steps = vec![
        Step {
            id: sid("reserve"),
            dependencies: Default::default(),
            forward: operation("add:10"),
            compensation: if irreversible {
                Compensation::Irreversible
            } else {
                Compensation::Operation(operation(if permanent { "reject" } else { "retry:-10" }))
            },
        },
        Step {
            id: sid("ship"),
            dependencies: [sid("reserve")].into(),
            forward: operation("add:20"),
            compensation: Compensation::Operation(operation("add:-20")),
        },
    ];
    if failure {
        steps.push(Step {
            id: sid("notify"),
            dependencies: [sid("ship")].into(),
            forward: operation("reject"),
            compensation: Compensation::Operation(operation("add:0")),
        });
    }
    Saga {
        version: 1,
        id: SagaId::new(name).unwrap(),
        revision: 1,
        steps,
        irreversible: if irreversible {
            IrreversiblePolicy::Allow
        } else {
            IrreversiblePolicy::Reject
        },
        retry_limit: 3,
    }
}
pub fn adapters(stub: Arc<ExternalStub>) -> BTreeMap<AdapterId, Arc<dyn SagaAdapter>> {
    BTreeMap::from([(aid(), stub as Arc<dyn SagaAdapter>)])
}
pub fn open(directory: &Path, stub: Arc<ExternalStub>) -> SagaEngine<SqliteJournal> {
    SagaEngine::open(
        SqliteJournal::open(directory.join("saga.db")).unwrap(),
        ns(),
        adapters(stub),
        SagaPolicy::default(),
    )
    .unwrap()
}
