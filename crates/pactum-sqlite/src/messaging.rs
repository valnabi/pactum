//! Concurrent SQLite WAL/FULL messaging storage. Domain tables and messaging
//! records must live in this same database; no coordinator journal is required.
use pactum_messaging::{
    Claim, ConsumeResult, Delivery, DeliveryState, Error, InboxStore, Message, OrderingPolicy,
    OutboxStore, RetryPolicy, SendError, validate_name,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::{path::Path, time::Duration};

fn error(e: impl std::fmt::Display) -> Error {
    Error(e.to_string())
}
fn bytes(message: &Message) -> Result<Vec<u8>, Error> {
    message.validate()?;
    serde_json::to_vec(message).map_err(error)
}
fn time(now: i64) -> Result<(), Error> {
    if now < 0 {
        return Err(error("negative messaging timestamp"));
    }
    Ok(())
}
fn state(value: &str) -> Result<DeliveryState, Error> {
    match value {
        "pending" => Ok(DeliveryState::Pending),
        "leased" => Ok(DeliveryState::Leased),
        "delivered" => Ok(DeliveryState::Delivered),
        "dead" => Ok(DeliveryState::Dead),
        _ => Err(error("invalid delivery state")),
    }
}
type DeliveryRow = (Vec<u8>, String, u32, i64, Option<String>);
fn decode_delivery(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeliveryRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}
fn delivery(row: DeliveryRow) -> Result<Delivery, Error> {
    let message: Message = serde_json::from_slice(&row.0).map_err(error)?;
    message.validate()?;
    Ok(Delivery {
        message,
        state: state(&row.1)?,
        attempts: row.2,
        available_at: row.3,
        last_error: row.4,
    })
}

/// Native transaction storage, supporting independent connections/workers with
/// SQLite write serialization and durable claim fencing. Busy writers return an
/// error after five seconds. Do not open this through `SqliteJournal`.
pub struct SqliteMessaging {
    connection: Connection,
}
impl SqliteMessaging {
    /// Open a local file database with WAL and FULL synchronization.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::from_connection(Connection::open(path).map_err(error)?)
    }
    /// Adopt an application's connection. No transaction may already be active.
    /// Configures and verifies WAL/FULL and installs versioned messaging tables.
    pub fn from_connection(connection: Connection) -> Result<Self, Error> {
        if !connection.is_autocommit() {
            return Err(error("connection has an active transaction"));
        }
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(error)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(error)?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(error)?;
        let sync: u32 = connection
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .map_err(error)?;
        if mode != "wal" || sync != 2 {
            return Err(error("messaging requires WAL/FULL on a file database"));
        }
        connection.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS pactum_message_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL);
            INSERT OR IGNORE INTO pactum_message_meta VALUES(1,1);
            COMMIT;").map_err(error)?;
        let version: u32 = connection
            .query_row(
                "SELECT version FROM pactum_message_meta WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(error)?;
        if version != 1 {
            return Err(error("unsupported messaging schema"));
        }
        connection.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS pactum_outbox(
                id TEXT PRIMARY KEY, envelope BLOB NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','leased','delivered','dead')),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 4294967295),
                cycle_attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                backoff INTEGER NOT NULL, max_backoff INTEGER NOT NULL,
                available_at INTEGER NOT NULL DEFAULT 0, last_error TEXT);
            CREATE INDEX IF NOT EXISTS pactum_outbox_due ON pactum_outbox(state,available_at,id);
            CREATE TABLE IF NOT EXISTS pactum_consumers(consumer TEXT PRIMARY KEY, ordering TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pactum_inbox(
                consumer TEXT NOT NULL, id TEXT NOT NULL, envelope BLOB NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','applied','dead')),
                attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL, last_error TEXT,
                PRIMARY KEY(consumer,id));
            CREATE TABLE IF NOT EXISTS pactum_streams(consumer TEXT NOT NULL, key TEXT NOT NULL,
                watermark INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(consumer,key));
            CREATE TABLE IF NOT EXISTS pactum_stream_slots(consumer TEXT NOT NULL, key TEXT NOT NULL,
                sequence INTEGER NOT NULL, id TEXT NOT NULL, PRIMARY KEY(consumer,key,sequence));
            COMMIT;").map_err(error)?;
        Ok(Self { connection })
    }
    /// Application access for schema setup and reads. Do not alter messaging
    /// tables, transaction boundaries or durability pragmas behind the adapter.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Enqueue inside the application's native transaction. Returns false for an
    /// identical existing binding; reject changed payload or retry policy. Only
    /// run the domain mutation when true. Caller owns commit/rollback, and must
    /// propagate errors (including errors in subsequent domain mutation).
    pub fn enqueue(
        tx: &Transaction<'_>,
        message: &Message,
        policy: RetryPolicy,
    ) -> Result<bool, Error> {
        policy.validate()?;
        let envelope = bytes(message)?;
        let old: Option<(Vec<u8>, u32, i64, i64)> = tx
            .query_row(
                "SELECT envelope,max_attempts,backoff,max_backoff FROM pactum_outbox WHERE id=?1",
                [&message.id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .map_err(error)?;
        if let Some(old) = old {
            if old
                != (
                    envelope,
                    policy.max_attempts,
                    policy.backoff_ms,
                    policy.max_backoff_ms,
                )
            {
                return Err(error("outbox identity or retry policy conflict"));
            }
            return Ok(false);
        }
        tx.execute("INSERT INTO pactum_outbox(id,envelope,state,max_attempts,backoff,max_backoff) VALUES(?1,?2,'pending',?3,?4,?5)",
            params![message.id,envelope,policy.max_attempts,policy.backoff_ms,policy.max_backoff_ms]).map_err(error)?;
        Ok(true)
    }
    /// Atomic producer boundary: enqueue plus domain mutation in one immediate
    /// transaction. Identical retries skip mutation. Handler must use only this
    /// transaction and must not commit it, change pragmas or perform external IO.
    /// A commit error can be ambiguous; reopen and retry the identical envelope.
    pub fn produce<F>(
        &mut self,
        message: &Message,
        policy: RetryPolicy,
        mutation: F,
    ) -> Result<bool, Error>
    where
        F: FnOnce(&Transaction<'_>) -> Result<(), Error>,
    {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        let inserted = Self::enqueue(&tx, message, policy)?;
        if inserted {
            mutation(&tx)?;
        }
        tx.commit().map_err(error)?;
        Ok(inserted)
    }
    /// Inspect a bounded page of records, lexically after the supplied ID.
    /// Filter by `DeliveryState` in the application; terminal records are retained.
    pub fn deliveries(&self, after: &str, limit: usize) -> Result<Vec<Delivery>, Error> {
        if limit == 0 || limit > 10_000 {
            return Err(error("invalid inspection limit"));
        }
        let mut query = self.connection.prepare("SELECT envelope,state,attempts,available_at,last_error FROM pactum_outbox WHERE id>?1 ORDER BY id LIMIT ?2").map_err(error)?;
        query
            .query_map(params![after, limit as i64], decode_delivery)
            .map_err(error)?
            .map(|r| delivery(r.map_err(error)?))
            .collect()
    }
    /// Inspect the durable consume outcome and failure count. Pending records
    /// report Retry (including records waiting for a missing sequence).
    pub fn inbox_status(
        &self,
        consumer: &str,
        id: &str,
    ) -> Result<Option<(ConsumeResult, u32)>, Error> {
        let row: Option<(String, u32, Option<String>)> = self
            .connection
            .query_row(
                "SELECT state,attempts,last_error FROM pactum_inbox WHERE consumer=?1 AND id=?2",
                params![consumer, id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(error)?;
        row.map(|(s, attempts, detail)| {
            let result = match s.as_str() {
                "applied" => ConsumeResult::Duplicate,
                "pending" => ConsumeResult::Retry(detail.unwrap_or_default()),
                "dead" => ConsumeResult::Dead(detail.unwrap_or_default()),
                _ => return Err(error("invalid inbox state")),
            };
            Ok((result, attempts))
        })
        .transpose()
    }
}

impl OutboxStore for SqliteMessaging {
    fn claim(&mut self, now: i64, lease_ms: i64, limit: usize) -> Result<Vec<Claim>, Error> {
        time(now)?;
        if lease_ms <= 0 || limit == 0 || limit > 10_000 {
            return Err(error("invalid claim bounds"));
        }
        let until = now
            .checked_add(lease_ms)
            .ok_or_else(|| error("lease deadline overflow"))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        // Expired sends may already have been accepted. Never label them delivered;
        // budget exhaustion retains an explicit uncertain dead letter instead.
        tx.execute("UPDATE pactum_outbox SET state='dead',last_error='claim budget exhausted; acceptance may be unknown' WHERE id IN
            (SELECT id FROM pactum_outbox WHERE state IN ('pending','leased') AND available_at<=?1 AND (cycle_attempts>=max_attempts OR attempts=4294967295) ORDER BY available_at,id LIMIT ?2)",params![now,limit as i64]).map_err(error)?;
        let mut query = tx.prepare("SELECT envelope,attempts FROM pactum_outbox WHERE state IN ('pending','leased') AND available_at<=?1 AND cycle_attempts<max_attempts AND attempts<4294967295 ORDER BY available_at,id LIMIT ?2").map_err(error)?;
        let rows = query
            .query_map(params![now, limit as i64], |r| {
                Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, u32>(1)?))
            })
            .map_err(error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        drop(query);
        let mut claims = Vec::new();
        for (envelope, attempts) in rows {
            let message: Message = serde_json::from_slice(&envelope).map_err(error)?;
            message.validate()?;
            tx.execute("UPDATE pactum_outbox SET state='leased',attempts=attempts+1,cycle_attempts=cycle_attempts+1,available_at=?2 WHERE id=?1",params![message.id,until]).map_err(error)?;
            claims.push(Claim {
                message,
                token: attempts + 1,
            });
        }
        tx.commit().map_err(error)?;
        Ok(claims)
    }
    fn acknowledge(&mut self, claim: &Claim) -> Result<(), Error> {
        let changed = self.connection.execute("UPDATE pactum_outbox SET state='delivered',last_error=NULL WHERE id=?1 AND state='leased' AND attempts=?2",params![claim.message.id,claim.token]).map_err(error)?;
        if changed != 1 {
            return Err(error("obsolete outbox claim"));
        }
        Ok(())
    }
    fn fail(&mut self, claim: &Claim, now: i64, failure: &SendError) -> Result<(), Error> {
        time(now)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        let (attempt, max_attempts, backoff_ms, max_backoff_ms): (u32,u32,i64,i64) = tx.query_row("SELECT cycle_attempts,max_attempts,backoff,max_backoff FROM pactum_outbox WHERE id=?1 AND state='leased' AND attempts=?2",params![claim.message.id,claim.token], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(error)?;
        let policy = RetryPolicy {
            max_attempts,
            backoff_ms,
            max_backoff_ms,
        };
        policy.validate()?;
        let next = now.saturating_add(policy.delay(attempt));
        tx.execute(
            "UPDATE pactum_outbox SET state=?2,available_at=?3,last_error=?4 WHERE id=?1",
            params![
                claim.message.id,
                if failure.permanent || attempt >= max_attempts {
                    "dead"
                } else {
                    "pending"
                },
                next,
                failure.detail
            ],
        )
        .map_err(error)?;
        tx.commit().map_err(error)
    }
    fn delivery(&self, id: &str) -> Result<Option<Delivery>, Error> {
        self.connection.query_row("SELECT envelope,state,attempts,available_at,last_error FROM pactum_outbox WHERE id=?1",[id],decode_delivery).optional().map_err(error)?.map(delivery).transpose()
    }
    fn replay(&mut self, id: &str, now: i64) -> Result<(), Error> {
        time(now)?;
        let changed = self.connection.execute("UPDATE pactum_outbox SET state='pending',cycle_attempts=0,available_at=?2 WHERE id=?1 AND state='dead' AND attempts<4294967295",params![id,now]).map_err(error)?;
        if changed != 1 {
            return Err(error("replay requires a nonexhausted dead letter"));
        }
        Ok(())
    }
}

impl InboxStore for SqliteMessaging {
    type Transaction<'a> = Transaction<'a>;
    fn consume<F>(
        &mut self,
        consumer: &str,
        message: &Message,
        ordering: OrderingPolicy,
        max_attempts: u32,
        handler: F,
    ) -> Result<ConsumeResult, Error>
    where
        F: for<'a> FnOnce(&Transaction<'a>) -> Result<(), Error>,
    {
        validate_name(consumer)?;
        if max_attempts == 0 {
            return Err(error("inbox attempt budget must be positive"));
        }
        let envelope = bytes(message)?;
        let policy = match ordering {
            OrderingPolicy::Unordered => "unordered",
            OrderingPolicy::Strict => "strict",
        };
        if ordering == OrderingPolicy::Strict && message.sequence.is_none() {
            return Err(error("strict inbox requires ordering metadata"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        tx.execute(
            "INSERT OR IGNORE INTO pactum_consumers VALUES(?1,?2)",
            params![consumer, policy],
        )
        .map_err(error)?;
        let stored: String = tx
            .query_row(
                "SELECT ordering FROM pactum_consumers WHERE consumer=?1",
                [consumer],
                |r| r.get(0),
            )
            .map_err(error)?;
        if stored != policy {
            return Err(error("consumer ordering policy conflict"));
        }
        tx.execute("INSERT OR IGNORE INTO pactum_inbox(consumer,id,envelope,state,max_attempts) VALUES(?1,?2,?3,'pending',?4)",params![consumer,message.id,envelope,max_attempts]).map_err(error)?;
        let (old, s, attempts, budget, detail): (Vec<u8>,String,u32,u32,Option<String>) = tx.query_row("SELECT envelope,state,attempts,max_attempts,last_error FROM pactum_inbox WHERE consumer=?1 AND id=?2",params![consumer,message.id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).map_err(error)?;
        if old != envelope || budget != max_attempts {
            return Err(error("inbox identity or budget conflict"));
        }
        if s == "applied" {
            return Ok(ConsumeResult::Duplicate);
        }
        if s == "dead" {
            return Ok(ConsumeResult::Dead(detail.unwrap_or_default()));
        }
        if s != "pending" {
            return Err(error("invalid inbox state"));
        }
        if ordering == OrderingPolicy::Strict {
            let key = message.ordering_key.as_deref().expect("validated key");
            let sequence = message.sequence.expect("validated sequence");
            tx.execute(
                "INSERT OR IGNORE INTO pactum_stream_slots VALUES(?1,?2,?3,?4)",
                params![consumer, key, sequence, message.id],
            )
            .map_err(error)?;
            let bound: String = tx.query_row("SELECT id FROM pactum_stream_slots WHERE consumer=?1 AND key=?2 AND sequence=?3",params![consumer,key,sequence], |r| r.get(0)).map_err(error)?;
            if bound != message.id {
                return Err(error("stream sequence already bound to a different ID"));
            }
            tx.execute(
                "INSERT OR IGNORE INTO pactum_streams VALUES(?1,?2,0)",
                params![consumer, key],
            )
            .map_err(error)?;
            let watermark: u64 = tx
                .query_row(
                    "SELECT watermark FROM pactum_streams WHERE consumer=?1 AND key=?2",
                    params![consumer, key],
                    |r| r.get(0),
                )
                .map_err(error)?;
            if sequence != watermark + 1 {
                tx.commit().map_err(error)?;
                return Ok(ConsumeResult::OutOfOrder);
            }
        }
        tx.execute_batch("SAVEPOINT pactum_inbox_effect")
            .map_err(error)?;
        let result = match handler(&tx) {
            Ok(()) => {
                tx.execute_batch("RELEASE pactum_inbox_effect")
                    .map_err(error)?;
                tx.execute("UPDATE pactum_inbox SET state='applied',attempts=attempts+1,last_error=NULL WHERE consumer=?1 AND id=?2",params![consumer,message.id]).map_err(error)?;
                if ordering == OrderingPolicy::Strict {
                    tx.execute(
                        "UPDATE pactum_streams SET watermark=?3 WHERE consumer=?1 AND key=?2",
                        params![consumer, message.ordering_key, message.sequence],
                    )
                    .map_err(error)?;
                }
                ConsumeResult::Applied
            }
            Err(failure) => {
                tx.execute_batch("ROLLBACK TO pactum_inbox_effect; RELEASE pactum_inbox_effect")
                    .map_err(error)?;
                let dead = attempts + 1 >= budget;
                tx.execute("UPDATE pactum_inbox SET state=?3,attempts=attempts+1,last_error=?4 WHERE consumer=?1 AND id=?2",params![consumer,message.id,if dead {"dead"} else {"pending"},failure.0]).map_err(error)?;
                if dead {
                    ConsumeResult::Dead(failure.0)
                } else {
                    ConsumeResult::Retry(failure.0)
                }
            }
        };
        tx.commit().map_err(error)?;
        Ok(result)
    }
    fn replay_inbox(&mut self, consumer: &str, id: &str) -> Result<(), Error> {
        let changed = self.connection.execute("UPDATE pactum_inbox SET state='pending',attempts=0 WHERE consumer=?1 AND id=?2 AND state='dead'",params![consumer,id]).map_err(error)?;
        if changed != 1 {
            return Err(error("inbox replay requires a poison record"));
        }
        Ok(())
    }
}
