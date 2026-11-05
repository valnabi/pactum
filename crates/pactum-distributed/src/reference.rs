//! Single-host SQLite authority service with atomic fencing at every effect.
//!
//! Host synchronous calls on a blocking worker in latency-sensitive applications.
//! Never share this SQLite file through a network filesystem. It is not consensus.
use crate::{
    Domain, FencedResource, Grant, JournalAuthority, LeaseAuthority, Operation, OwnedJournal, Reply,
};
use async_trait::async_trait;
use pactum_core::{Error, ParticipantError, ParticipantId, TransactionId};
use pactum_journal::{Checkpoint, Entry, Journal, JournalError, Retention, Sequence};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn failure(e: impl std::fmt::Display) -> Error {
    Error::InDoubt(e.to_string())
}
fn invalid(s: &str) -> Error {
    Error::Invalid(s.into())
}
fn journal_error(e: impl std::fmt::Display) -> JournalError {
    JournalError(e.to_string())
}
fn hash(sequence: u64, version: u32, payload: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(sequence.to_be_bytes());
    digest.update(version.to_be_bytes());
    digest.update(payload);
    digest.finalize().to_vec()
}
/// Authority-owned time source. Clients never supply their own time for fencing.
pub trait Clock: Send + Sync {
    /// Milliseconds on the authority's clock. Clock jumps affect liveness only.
    fn now_millis(&self) -> Result<u64, Error>;
}
/// Wall clock used by the reference service; all validation is serialized in SQLite.
pub struct SystemClock;
impl Clock for SystemClock {
    fn now_millis(&self) -> Result<u64, Error> {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(failure)?
                .as_millis(),
        )
        .map_err(failure)
    }
}
/// Shared durable service. Clones share one connection; independent opens/processes
/// coordinate through SQLite write transactions, not process-local mutexes.
#[derive(Clone)]
pub struct ReferenceStore {
    connection: Arc<Mutex<Connection>>,
    domain: Domain,
    clock: Arc<dyn Clock>,
}
impl ReferenceStore {
    /// Open or create the service database, permanently binding its namespace.
    pub fn open(path: impl AsRef<Path>, namespace: &str) -> Result<Self, Error> {
        Self::open_with_clock(path, namespace, Arc::new(SystemClock))
    }
    /// Use a service-owned clock (primarily for deterministic expiry injection).
    /// All connections to one service must use the same clock semantics.
    pub fn open_with_clock(
        path: impl AsRef<Path>,
        namespace: &str,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        TransactionId::new(namespace)?;
        let mut connection = Connection::open(path).map_err(failure)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(failure)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(failure)?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(failure)?;
        let sync: u32 = connection
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .map_err(failure)?;
        if mode != "wal" || sync != 2 {
            return Err(invalid("WAL and FULL required"));
        }
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        tx.execute_batch("
            CREATE TABLE IF NOT EXISTS distributed_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), format INTEGER NOT NULL,
                authority TEXT NOT NULL, namespace TEXT NOT NULL, epoch INTEGER NOT NULL,
                owner TEXT, expires INTEGER NOT NULL, head INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS distributed_entries (
                sequence INTEGER PRIMARY KEY, version INTEGER NOT NULL, payload BLOB NOT NULL, hash BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS distributed_archive (
                sequence INTEGER PRIMARY KEY, version INTEGER NOT NULL, payload BLOB NOT NULL, hash BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS distributed_checkpoint (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), generation INTEGER NOT NULL,
                watermark INTEGER NOT NULL, version INTEGER NOT NULL, payload BLOB NOT NULL, hash BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS distributed_counters (resource TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS distributed_work (
                resource TEXT NOT NULL, tx TEXT NOT NULL, payload BLOB, token BLOB, state INTEGER NOT NULL,
                PRIMARY KEY(resource,tx));
        ").map_err(failure)?;
        tx.execute("INSERT OR IGNORE INTO distributed_meta VALUES (1,1,lower(hex(randomblob(16))),?1,0,NULL,0,0)", [namespace]).map_err(failure)?;
        let (format, authority, stored): (u32, String, String) = tx
            .query_row(
                "SELECT format,authority,namespace FROM distributed_meta WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(failure)?;
        if format != 1 || stored != namespace {
            return Err(invalid("unsupported format or changed namespace"));
        }
        tx.commit().map_err(failure)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            domain: Domain {
                authority,
                namespace: stored,
            },
            clock,
        })
    }
    fn transaction<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>, u64) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut connection = self.connection.lock().map_err(failure)?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(failure)?;
        // Read time after obtaining the serialization lock, never before waiting.
        let now = self.clock.now_millis()?;
        let result = operation(&tx, now)?;
        tx.commit().map_err(failure)?;
        Ok(result)
    }
    fn validate(&self, tx: &Transaction<'_>, now: u64, grant: &Grant) -> Result<(), Error> {
        if grant.domain != self.domain {
            return Err(invalid("authority domain mismatch"));
        }
        let (epoch, owner, expires): (u64, Option<String>, u64) = tx
            .query_row(
                "SELECT epoch,owner,expires FROM distributed_meta WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(failure)?;
        if epoch != grant.epoch || owner.as_deref() != Some(&grant.owner) || now >= expires {
            return Err(invalid("stale, released or expired grant"));
        }
        Ok(())
    }
    fn fenced<T>(
        &self,
        grant: &Grant,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.transaction(|tx, now| {
            self.validate(tx, now, grant)?;
            operation(tx)
        })
    }
    /// Create/reopen a durable fenced nonnegative-delta counter resource.
    pub fn counter(&self, id: ParticipantId) -> Result<Arc<ReferenceCounter>, Error> {
        self.transaction(|tx, _| {
            tx.execute(
                "INSERT OR IGNORE INTO distributed_counters VALUES (?1,0)",
                [id.as_str()],
            )
            .map_err(failure)?;
            Ok(())
        })?;
        Ok(Arc::new(ReferenceCounter {
            store: self.clone(),
            id,
        }))
    }
}
fn expiry(now: u64, ttl: Duration) -> Result<u64, Error> {
    let millis = u64::try_from(ttl.as_millis()).map_err(failure)?;
    if millis == 0 {
        return Err(invalid("TTL must be at least one millisecond"));
    }
    now.checked_add(millis)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or_else(|| invalid("lease expiry exhausted"))
}
impl LeaseAuthority for ReferenceStore {
    fn domain(&self) -> Domain {
        self.domain.clone()
    }
    fn acquire(&self, owner: &str, ttl: Duration) -> Result<Grant, Error> {
        TransactionId::new(owner)?;
        self.transaction(|tx, now| {
            let (epoch, current, until): (u64, Option<String>, u64) = tx
                .query_row(
                    "SELECT epoch,owner,expires FROM distributed_meta WHERE singleton=1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .map_err(failure)?;
            if current.is_some() && now < until {
                return Err(invalid("scope already owned"));
            }
            let epoch = epoch
                .checked_add(1)
                .filter(|e| *e <= i64::MAX as u64)
                .ok_or_else(|| invalid("epoch exhausted"))?;
            let until = expiry(now, ttl)?;
            tx.execute(
                "UPDATE distributed_meta SET epoch=?1,owner=?2,expires=?3 WHERE singleton=1",
                params![epoch, owner, until],
            )
            .map_err(failure)?;
            Ok(Grant {
                domain: self.domain.clone(),
                owner: owner.into(),
                epoch,
            })
        })
    }
    fn renew(&self, grant: &Grant, ttl: Duration) -> Result<(), Error> {
        self.transaction(|tx, now| {
            self.validate(tx, now, grant)?;
            // A renewal must not shorten the existing grant.
            tx.execute(
                "UPDATE distributed_meta SET expires=MAX(expires,?1) WHERE singleton=1",
                [expiry(now, ttl)?],
            )
            .map_err(failure)?;
            Ok(())
        })
    }
    fn release(&self, grant: &Grant) -> Result<(), Error> {
        self.fenced(grant, |tx| {
            tx.execute(
                "UPDATE distributed_meta SET owner=NULL,expires=0 WHERE singleton=1",
                [],
            )
            .map_err(failure)?;
            Ok(())
        })
    }
}
fn head(tx: &Transaction<'_>) -> Result<u64, Error> {
    tx.query_row(
        "SELECT head FROM distributed_meta WHERE singleton=1",
        [],
        |r| r.get(0),
    )
    .map_err(failure)
}
fn checkpoint(tx: &Transaction<'_>) -> Result<Option<Checkpoint>, Error> {
    let data = tx.query_row("SELECT generation,watermark,version,payload,hash FROM distributed_checkpoint WHERE singleton=1", [], |r| Ok((Checkpoint { generation:r.get(0)?,watermark:r.get(1)?,version:r.get(2)?,payload:r.get(3)? }, r.get::<_,Vec<u8>>(4)?))).optional().map_err(failure)?;
    data.map(|(c, digest)| {
        let mut bytes = c.generation.to_be_bytes().to_vec();
        bytes.extend(&c.payload);
        if c.generation == 0 || digest != hash(c.watermark, c.version, &bytes) {
            return Err(Error::Corrupt("checkpoint checksum mismatch".into()));
        }
        Ok(c)
    })
    .transpose()
}
fn entries(tx: &Transaction<'_>) -> Result<Vec<Entry>, Error> {
    let mut expected = checkpoint(tx)?.map_or(0, |c| c.watermark);
    let mut statement = tx
        .prepare("SELECT sequence,version,payload,hash FROM distributed_entries ORDER BY sequence")
        .map_err(failure)?;
    let mut rows = statement.query([]).map_err(failure)?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(failure)? {
        let entry = Entry {
            sequence: row.get(0).map_err(failure)?,
            version: row.get(1).map_err(failure)?,
            payload: row.get(2).map_err(failure)?,
        };
        let digest: Vec<u8> = row.get(3).map_err(failure)?;
        if Some(entry.sequence) != expected.checked_add(1)
            || digest != hash(entry.sequence, entry.version, &entry.payload)
        {
            return Err(Error::Corrupt("journal gap or checksum mismatch".into()));
        }
        expected = entry.sequence;
        result.push(entry);
    }
    if expected != head(tx)? {
        return Err(Error::Corrupt("journal head mismatch".into()));
    }
    Ok(result)
}
impl JournalAuthority for ReferenceStore {
    fn domain(&self) -> Domain {
        self.domain.clone()
    }
    fn open_journal(&self, grant: &Grant) -> Result<OwnedJournal, Error> {
        let base = self.fenced(grant, |tx| {
            entries(tx)?;
            head(tx)
        })?;
        Ok(OwnedJournal::new(ReferenceJournal {
            store: self.clone(),
            grant: grant.clone(),
            base,
            staged: Vec::new(),
            poisoned: false,
        }))
    }
}
struct ReferenceJournal {
    store: ReferenceStore,
    grant: Grant,
    base: u64,
    staged: Vec<Entry>,
    poisoned: bool,
}
impl ReferenceJournal {
    fn healthy(&self) -> Result<(), JournalError> {
        if self.poisoned {
            Err(journal_error("drop and reopen journal after write failure"))
        } else {
            Ok(())
        }
    }
}
impl Journal for ReferenceJournal {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        self.healthy()?;
        self.poisoned = true;
        self.store
            .fenced(&self.grant, |tx| {
                if head(tx)? != self.base {
                    return Err(invalid("journal session superseded"));
                }
                Ok(())
            })
            .map_err(journal_error)?;
        let sequence = self
            .base
            .checked_add(self.staged.len() as u64)
            .and_then(|n| n.checked_add(1))
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| journal_error("sequence exhausted"))?;
        self.staged.push(Entry {
            sequence,
            version,
            payload: payload.to_vec(),
        });
        self.poisoned = false;
        Ok(sequence)
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.healthy()?;
        self.poisoned = true;
        let durable = self
            .store
            .fenced(&self.grant, |tx| {
                if head(tx)? != self.base {
                    return Err(invalid("journal session superseded"));
                }
                for e in &self.staged {
                    tx.execute(
                        "INSERT INTO distributed_entries VALUES (?1,?2,?3,?4)",
                        params![
                            e.sequence,
                            e.version,
                            e.payload,
                            hash(e.sequence, e.version, &e.payload)
                        ],
                    )
                    .map_err(failure)?;
                }
                let last = self.staged.last().map_or(self.base, |e| e.sequence);
                tx.execute(
                    "UPDATE distributed_meta SET head=?1 WHERE singleton=1",
                    [last],
                )
                .map_err(failure)?;
                Ok(last)
            })
            .map_err(journal_error)?;
        self.base = durable;
        self.staged.clear();
        self.poisoned = false;
        Ok(durable)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.healthy()?;
        self.store
            .fenced(&self.grant, entries)
            .map_err(journal_error)
    }
    fn checkpoint(&mut self) -> Result<Option<Checkpoint>, JournalError> {
        self.healthy()?;
        self.store
            .fenced(&self.grant, checkpoint)
            .map_err(journal_error)
    }
    fn compact(
        &mut self,
        watermark: Sequence,
        version: u32,
        payload: &[u8],
        retention: Retention,
    ) -> Result<(), JournalError> {
        self.healthy()?;
        self.poisoned = true;
        if !self.staged.is_empty() {
            return Err(journal_error("staged writes forbid compaction"));
        }
        self.store.fenced(&self.grant, |tx| {
            if watermark != self.base || head(tx)? != watermark { return Err(invalid("checkpoint requires exact durable head")); }
            let generation = checkpoint(tx)?.map_or(0,|c| c.generation).checked_add(1).filter(|g| *g <= i64::MAX as u64).ok_or_else(|| invalid("generation exhausted"))?;
            let mut bytes = generation.to_be_bytes().to_vec(); bytes.extend(payload);
            tx.execute("INSERT OR REPLACE INTO distributed_checkpoint VALUES (1,?1,?2,?3,?4,?5)",params![generation,watermark,version,payload,hash(watermark,version,&bytes)]).map_err(failure)?;
            if retention == Retention::Archive {
                tx.execute("INSERT INTO distributed_archive SELECT * FROM distributed_entries WHERE sequence<=?1", [watermark]).map_err(failure)?;
            }
            tx.execute("DELETE FROM distributed_entries WHERE sequence<=?1", [watermark]).map_err(failure)?;
            Ok(())
        }).map_err(journal_error)?;
        self.poisoned = false;
        Ok(())
    }
}
/// Durable reference counter. UTF-8 nonnegative i64 deltas are accepted at prepare.
/// Grant validation and state/effects commit in the same authority transaction.
pub struct ReferenceCounter {
    store: ReferenceStore,
    id: ParticipantId,
}
impl ReferenceCounter {
    /// Inspect current effects for diagnostics (does not authorize decisions).
    pub fn value(&self) -> Result<i64, Error> {
        self.store.transaction(|tx, _| {
            tx.query_row(
                "SELECT value FROM distributed_counters WHERE resource=?1",
                [self.id.as_str()],
                |r| r.get(0),
            )
            .map_err(failure)
        })
    }
}
#[async_trait]
impl FencedResource for ReferenceCounter {
    fn id(&self) -> ParticipantId {
        self.id.clone()
    }
    fn domain(&self) -> Domain {
        self.store.domain.clone()
    }
    async fn call(
        &self,
        grant: &Grant,
        id: &TransactionId,
        operation: Operation,
    ) -> Result<Reply, ParticipantError> {
        self.store
            .fenced(grant, |tx| self.apply(tx, id, operation))
            .map_err(|e| ParticipantError(e.to_string()))
    }
}

type ResourceWork = (Option<Vec<u8>>, Option<Vec<u8>>, u32);

impl ReferenceCounter {
    fn apply(
        &self,
        tx: &Transaction<'_>,
        id: &TransactionId,
        operation: Operation,
    ) -> Result<Reply, Error> {
        let work: Option<ResourceWork> = tx
            .query_row(
                "SELECT payload,token,state FROM distributed_work WHERE resource=?1 AND tx=?2",
                params![self.id.as_str(), id.as_str()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(failure)?;
        let state = work.as_ref().map(|w| w.2);
        if state.is_some_and(|s| s > 2) {
            return Err(Error::Corrupt("invalid resource state".into()));
        }
        match operation {
            Operation::Status => Ok(match work {
                None => Reply::Unknown,
                Some((_, Some(token), 0)) => Reply::Prepared(token),
                Some((_, _, 1)) => Reply::Committed,
                Some((_, _, 2)) => Reply::Aborted,
                _ => return Err(Error::Corrupt("missing prepare token".into())),
            }),
            Operation::Prepare(payload) => {
                if let Some((bound, token, state)) = work {
                    if state == 2 {
                        return Ok(Reply::Rejected("aborted".into()));
                    }
                    if bound.as_ref() != Some(&payload) {
                        return Err(invalid("transaction payload changed"));
                    }
                    return Ok(Reply::Prepared(
                        token.ok_or_else(|| Error::Corrupt("missing token".into()))?,
                    ));
                }
                let delta = std::str::from_utf8(&payload)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok());
                let Some(delta) = delta.filter(|d| *d >= 0) else {
                    return Ok(Reply::Rejected("expected nonnegative i64 delta".into()));
                };
                // Reserve enough capacity for every outstanding prepared delta.
                // A prepared vote must remain committable in any delivery order.
                let mut reserved: i64 = tx
                    .query_row(
                        "SELECT value FROM distributed_counters WHERE resource=?1",
                        [self.id.as_str()],
                        |r| r.get(0),
                    )
                    .map_err(failure)?;
                let mut statement = tx
                    .prepare("SELECT payload FROM distributed_work WHERE resource=?1 AND state=0")
                    .map_err(failure)?;
                let mut rows = statement.query([self.id.as_str()]).map_err(failure)?;
                while let Some(row) = rows.next().map_err(failure)? {
                    let payload: Vec<u8> = row.get(0).map_err(failure)?;
                    let pending: i64 = std::str::from_utf8(&payload)
                        .map_err(failure)?
                        .parse()
                        .map_err(failure)?;
                    reserved = reserved
                        .checked_add(pending)
                        .ok_or_else(|| Error::Corrupt("invalid capacity reservation".into()))?;
                }
                if reserved.checked_add(delta).is_none() {
                    return Ok(Reply::Rejected("counter capacity exhausted".into()));
                }
                let binding = serde_json::to_vec(&(
                    self.store.domain.clone(),
                    self.id.clone(),
                    id.clone(),
                    &payload,
                ))
                .map_err(failure)?;
                let token = Sha256::digest(binding).to_vec();
                tx.execute(
                    "INSERT INTO distributed_work VALUES (?1,?2,?3,?4,0)",
                    params![self.id.as_str(), id.as_str(), payload, token],
                )
                .map_err(failure)?;
                Ok(Reply::Prepared(token))
            }
            Operation::Commit(token) => {
                let Some((Some(payload), Some(stored), state)) = work else {
                    return Err(invalid("no durable prepare"));
                };
                if state == 2 || stored != token {
                    return Err(invalid("conflicting decision or token"));
                }
                if state == 0 {
                    let delta: i64 = std::str::from_utf8(&payload)
                        .map_err(failure)?
                        .parse()
                        .map_err(failure)?;
                    let value: i64 = tx
                        .query_row(
                            "SELECT value FROM distributed_counters WHERE resource=?1",
                            [self.id.as_str()],
                            |r| r.get(0),
                        )
                        .map_err(failure)?;
                    let value = value
                        .checked_add(delta)
                        .ok_or_else(|| invalid("counter overflow; manual intervention required"))?;
                    tx.execute(
                        "UPDATE distributed_counters SET value=?1 WHERE resource=?2",
                        params![value, self.id.as_str()],
                    )
                    .map_err(failure)?;
                    tx.execute(
                        "UPDATE distributed_work SET state=1 WHERE resource=?1 AND tx=?2",
                        params![self.id.as_str(), id.as_str()],
                    )
                    .map_err(failure)?;
                }
                Ok(Reply::Acknowledged)
            }
            Operation::Abort(token) => {
                if state == Some(1) {
                    return Err(invalid("cannot reverse committed decision"));
                }
                if let (Some(given), Some((_, Some(stored), _))) = (&token, &work)
                    && given != stored
                {
                    return Err(invalid("prepare token changed"));
                }
                tx.execute("INSERT INTO distributed_work VALUES (?1,?2,NULL,NULL,2) ON CONFLICT(resource,tx) DO UPDATE SET state=2",params![self.id.as_str(),id.as_str()]).map_err(failure)?;
                Ok(Reply::Acknowledged)
            }
        }
    }
}
