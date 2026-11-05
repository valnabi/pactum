//! Reference durable counter resource, separate from the reusable engine.
//! Each transaction adds a nonnegative i64 delta; negative deltas reject prepare.
//! SQLite atomically persists effects and terminal state. Prepared rows and abort
//! tombstones never expire. Different transactions commute; no cross-counter
//! isolation or reads of a global atomic snapshot are promised.
use async_trait::async_trait;
use pactum_core::{
    Participant, ParticipantError, ParticipantId, ParticipantStatus, TransactionId, Vote,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
fn error(e: impl Into<pactum_sqlite::AdapterError>) -> ParticipantError {
    ParticipantError::from_diagnostic(e.into().diagnostic())
}

/// Durable additive counter with an identity permanently stored in its database.
#[derive(Clone)]
pub struct DurableCounter {
    id: ParticipantId,
    connection: Arc<Mutex<Connection>>,
}
impl DurableCounter {
    /// Open/create a resource. Reopening it with a different identity fails.
    pub fn open(path: impl AsRef<Path>, id: ParticipantId) -> Result<Self, ParticipantError> {
        let connection = Connection::open(path).map_err(error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(error)?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
          BEGIN IMMEDIATE;
          CREATE TABLE IF NOT EXISTS resource (singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity TEXT NOT NULL, value INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS work (tx TEXT PRIMARY KEY, payload BLOB, token BLOB, state INTEGER NOT NULL CHECK(state IN (1,2,3)));
          COMMIT;").map_err(error)?;
        connection
            .execute(
                "INSERT OR IGNORE INTO resource VALUES (1, ?1, 0)",
                [id.as_str()],
            )
            .map_err(error)?;
        let stored: String = connection
            .query_row("SELECT identity FROM resource WHERE singleton=1", [], |r| {
                r.get(0)
            })
            .map_err(error)?;
        if stored != id.as_str() {
            return Err(error("resource identity mismatch"));
        }
        Ok(Self {
            id,
            connection: Arc::new(Mutex::new(connection)),
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T, ParticipantError> + Send + 'static,
    ) -> Result<T, ParticipantError> {
        let connection = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = connection
                .lock()
                .map_err(|_| error("resource mutex poisoned"))?;
            operation(&mut connection)
        })
        .await
        .map_err(ParticipantError::from_source)?
    }
    /// Read this resource's local committed value.
    pub async fn value(&self) -> Result<i64, ParticipantError> {
        self.run(|c| {
            c.query_row("SELECT value FROM resource WHERE singleton=1", [], |r| {
                r.get(0)
            })
            .map_err(error)
        })
        .await
    }
}
#[async_trait]
impl Participant for DurableCounter {
    fn id(&self) -> ParticipantId {
        self.id.clone()
    }
    async fn prepare(&self, tx: &TransactionId, payload: &[u8]) -> Result<Vote, ParticipantError> {
        let tx = tx.as_str().to_owned();
        let payload = payload.to_vec();
        self.run(move |c| {
            let transaction = c
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(error)?;
            let existing = transaction
                .query_row(
                    "SELECT payload, token, state FROM work WHERE tx=?1",
                    [&tx],
                    |r| {
                        Ok((
                            r.get::<_, Option<Vec<u8>>>(0)?,
                            r.get::<_, Option<Vec<u8>>>(1)?,
                            r.get::<_, i32>(2)?,
                        ))
                    },
                )
                .optional()
                .map_err(error)?;
            if let Some((old, token, state)) = existing {
                if state == 3 {
                    return Ok(Vote::Rejected("durable abort tombstone".into()));
                }
                if old.as_deref() != Some(payload.as_slice()) {
                    return Err(error("transaction payload mismatch"));
                }
                return Ok(Vote::Prepared(token.ok_or_else(|| error("missing token"))?));
            }
            let delta: i64 = std::str::from_utf8(&payload)
                .map_err(error)?
                .parse()
                .map_err(error)?;
            if delta < 0 {
                return Ok(Vote::Rejected("negative delta".into()));
            }
            // Reserve enough numeric capacity for all prepared deltas. This makes
            // commit infallible with respect to counter overflow after prepare.
            let value: i64 = transaction
                .query_row("SELECT value FROM resource WHERE singleton=1", [], |r| {
                    r.get(0)
                })
                .map_err(error)?;
            let mut reserved = value;
            {
                let mut statement = transaction
                    .prepare("SELECT payload FROM work WHERE state=1")
                    .map_err(error)?;
                let rows = statement
                    .query_map([], |r| r.get::<_, Vec<u8>>(0))
                    .map_err(error)?;
                for row in rows {
                    let bytes = row.map_err(error)?;
                    let amount: i64 = std::str::from_utf8(&bytes)
                        .map_err(error)?
                        .parse()
                        .map_err(error)?;
                    reserved = reserved
                        .checked_add(amount)
                        .ok_or_else(|| error("invalid reservation capacity"))?;
                }
            }
            if reserved.checked_add(delta).is_none() {
                return Ok(Vote::Rejected("counter capacity exhausted".into()));
            }
            let token = tx.as_bytes().to_vec();
            transaction
                .execute(
                    "INSERT INTO work VALUES (?1,?2,?3,1)",
                    params![tx, payload, token],
                )
                .map_err(error)?;
            transaction.commit().map_err(error)?;
            Ok(Vote::Prepared(token))
        })
        .await
    }
    async fn commit(&self, tx: &TransactionId, token: &[u8]) -> Result<(), ParticipantError> {
        let tx = tx.as_str().to_owned();
        let token = token.to_vec();
        self.run(move |c| {
            let transaction = c
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(error)?;
            let (payload, stored, state): (Option<Vec<u8>>, Option<Vec<u8>>, i32) = transaction
                .query_row(
                    "SELECT payload,token,state FROM work WHERE tx=?1",
                    [&tx],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(error)?;
            if stored.as_deref() != Some(token.as_slice()) || state == 3 {
                return Err(error("commit conflicts with durable state/token"));
            }
            if state == 2 {
                return Ok(());
            }
            let payload = payload.ok_or_else(|| error("missing payload"))?;
            let delta: i64 = std::str::from_utf8(&payload)
                .map_err(error)?
                .parse()
                .map_err(error)?;
            let value: i64 = transaction
                .query_row("SELECT value FROM resource WHERE singleton=1", [], |r| {
                    r.get(0)
                })
                .map_err(error)?;
            let value = value
                .checked_add(delta)
                .ok_or_else(|| error("counter overflow"))?;
            transaction
                .execute("UPDATE resource SET value=?1 WHERE singleton=1", [value])
                .map_err(error)?;
            transaction
                .execute("UPDATE work SET state=2 WHERE tx=?1", [&tx])
                .map_err(error)?;
            transaction.commit().map_err(error)
        })
        .await
    }
    async fn abort(
        &self,
        tx: &TransactionId,
        token: Option<&[u8]>,
    ) -> Result<(), ParticipantError> {
        let tx = tx.as_str().to_owned();
        let token = token.map(Vec::from);
        self.run(move |c| {
            let transaction = c
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(error)?;
            let existing = transaction
                .query_row("SELECT token,state FROM work WHERE tx=?1", [&tx], |r| {
                    Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, i32>(1)?))
                })
                .optional()
                .map_err(error)?;
            if let Some((stored, state)) = existing {
                if state == 2 {
                    return Err(error("cannot abort committed resource"));
                }
                if state == 3 {
                    return Ok(());
                }
                if token.is_some() && token != stored {
                    return Err(error("abort token mismatch"));
                }
                transaction
                    .execute("UPDATE work SET state=3 WHERE tx=?1", [&tx])
                    .map_err(error)?;
            } else {
                transaction
                    .execute("INSERT INTO work VALUES (?1,NULL,NULL,3)", [&tx])
                    .map_err(error)?;
            }
            transaction.commit().map_err(error)
        })
        .await
    }
    async fn status(&self, tx: &TransactionId) -> Result<ParticipantStatus, ParticipantError> {
        let tx = tx.as_str().to_owned();
        self.run(move |c| {
            let row = c
                .query_row("SELECT token,state FROM work WHERE tx=?1", [&tx], |r| {
                    Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, i32>(1)?))
                })
                .optional()
                .map_err(error)?;
            match row {
                None => Ok(ParticipantStatus::Unknown),
                Some((Some(token), 1)) => Ok(ParticipantStatus::Prepared(token)),
                Some((_, 2)) => Ok(ParticipantStatus::Committed),
                Some((_, 3)) => Ok(ParticipantStatus::Aborted),
                _ => Err(error("invalid durable resource state")),
            }
        })
        .await
    }
}
