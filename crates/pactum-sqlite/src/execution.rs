//! SQLite reference backend: transactional counters and staged immutable snapshots.
use crate::SqliteJournal;
use pactum_core::execution::{
    Backend, BackendError, OperationId, PublishRequest, PublishResult, Request, UnitResult,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::Path};

fn error(e: impl std::fmt::Display) -> BackendError {
    BackendError(e.to_string())
}
/// A counter delta for native units; a snapshot value for staged artifacts.
/// Encode a vector of these as JSON for the reference backend.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Value {
    /// Nonempty unique key in a unit or artifact.
    pub key: String,
    /// Signed delta for Atomic/Chunked, absolute value for StagedPublish.
    pub value: i64,
}
/// A visible immutable version and monotonic fencing generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibleVersion {
    /// Current generation; CAS uses this even if the visible content is unchanged.
    pub generation: u64,
    /// Operation ID is the stable artifact/version ID.
    pub version: OperationId,
    /// Complete decoded snapshot read in one SQLite statement.
    pub values: Vec<Value>,
}
/// Single-owner WAL/FULL reference backend. Effects, bound requests and receipts
/// share one SQLite database. Keep this file separate from the engine journal.
/// The retained sidecar lock and random persisted incarnation prevent accidental
/// concurrent owners and replacement-database recovery respectively.
pub struct SqliteBackend {
    storage: SqliteJournal,
    identity: String,
}
fn decode(payload: &[u8]) -> Option<Vec<Value>> {
    let values: Vec<Value> = serde_json::from_slice(payload).ok()?;
    let mut keys = BTreeSet::new();
    values
        .iter()
        .all(|v| !v.key.is_empty() && keys.insert(&v.key))
        .then_some(values)
}
fn bind(tx: &Transaction<'_>, id: &OperationId, request: &Request) -> Result<(), BackendError> {
    let bytes = serde_json::to_vec(request).map_err(error)?;
    let existing: Option<Vec<u8>> = tx
        .query_row(
            "SELECT request FROM pactum_operations WHERE id=?1",
            [id.as_str()],
            |r| r.get(0),
        )
        .optional()
        .map_err(error)?;
    if let Some(existing) = existing {
        if existing != bytes {
            return Err(error("backend operation key/payload/mode mismatch"));
        }
    } else {
        tx.execute(
            "INSERT INTO pactum_operations(id,request,state) VALUES (?1,?2,'new')",
            params![id.as_str(), bytes],
        )
        .map_err(error)?;
    }
    Ok(())
}
fn state(tx: &Transaction<'_>, id: &OperationId) -> Result<String, BackendError> {
    tx.query_row(
        "SELECT state FROM pactum_operations WHERE id=?1",
        [id.as_str()],
        |r| r.get(0),
    )
    .map_err(error)
}
fn set_state(tx: &Transaction<'_>, id: &OperationId, state: &str) -> Result<(), BackendError> {
    tx.execute(
        "UPDATE pactum_operations SET state=?2 WHERE id=?1",
        params![id.as_str(), state],
    )
    .map_err(error)?;
    Ok(())
}
impl SqliteBackend {
    /// Create/open, checking the caller's stable namespace against durable metadata.
    pub fn open(path: impl AsRef<Path>, namespace: &str) -> Result<Self, BackendError> {
        OperationId::new(namespace).map_err(error)?;
        let storage = SqliteJournal::open(path).map_err(error)?;
        storage.connection.execute_batch("BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS pactum_backend_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), namespace TEXT NOT NULL, incarnation TEXT NOT NULL, schema_version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS pactum_operations(id TEXT PRIMARY KEY, request BLOB NOT NULL, state TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pactum_units(id TEXT NOT NULL, chunk INTEGER NOT NULL, result TEXT NOT NULL, PRIMARY KEY(id,chunk));
            CREATE TABLE IF NOT EXISTS pactum_values(key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS pactum_artifacts(id TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS pactum_visible(target TEXT PRIMARY KEY, generation INTEGER NOT NULL CHECK(generation>0), version TEXT NOT NULL);
            COMMIT;").map_err(error)?;
        storage.connection.execute("INSERT OR IGNORE INTO pactum_backend_meta VALUES (1,?1,lower(hex(randomblob(16))),1)", [namespace]).map_err(error)?;
        let (stored, incarnation, version): (String,String,u32) = storage.connection.query_row("SELECT namespace,incarnation,schema_version FROM pactum_backend_meta WHERE singleton=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(error)?;
        if stored != namespace || version != 1 {
            return Err(error("backend namespace or schema mismatch"));
        }
        Ok(Self {
            storage,
            identity: format!("{stored}:{incarnation}"),
        })
    }
    /// Read a reference counter (zero before its first update).
    pub fn value(&self, key: &str) -> Result<i64, BackendError> {
        Ok(self
            .storage
            .connection
            .query_row("SELECT value FROM pactum_values WHERE key=?1", [key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(error)?
            .unwrap_or(0))
    }
    /// Read pointer and artifact together, so concurrent publications cannot mix versions.
    pub fn visible(&self, target: &OperationId) -> Result<Option<VisibleVersion>, BackendError> {
        let row: Option<(u64,String,Vec<u8>)> = self.storage.connection.query_row("SELECT v.generation,v.version,a.payload FROM pactum_visible v JOIN pactum_artifacts a ON a.id=v.version WHERE v.target=?1", [target.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(error)?;
        row.map(|(generation, version, payload)| {
            Ok(VisibleVersion {
                generation,
                version: OperationId::new(version).map_err(error)?,
                values: decode(&payload).ok_or_else(|| error("invalid published artifact"))?,
            })
        })
        .transpose()
    }
    /// Inspect whether unpublished staging remains (published artifacts are retained).
    pub fn has_artifact(&self, id: &OperationId) -> Result<bool, BackendError> {
        self.storage
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pactum_artifacts WHERE id=?1)",
                [id.as_str()],
                |r| r.get(0),
            )
            .map_err(error)
    }
}
impl Backend for SqliteBackend {
    fn identity(&self) -> String {
        self.identity.clone()
    }
    fn unit(
        &mut self,
        id: &OperationId,
        request: &Request,
        chunk: Option<usize>,
    ) -> Result<UnitResult, BackendError> {
        let (payload, index) = match (request, chunk) {
            (Request::Atomic(r), None) => (&r.payload, -1_i64),
            (Request::Chunked(r), Some(i)) => (
                r.chunks
                    .get(i)
                    .ok_or_else(|| error("invalid chunk index"))?,
                i64::try_from(i).map_err(error)?,
            ),
            _ => return Err(error("unit mode mismatch")),
        };
        let mut tx = self.storage.connection.transaction().map_err(error)?;
        bind(&tx, id, request)?;
        let receipt: Option<String> = tx
            .query_row(
                "SELECT result FROM pactum_units WHERE id=?1 AND chunk=?2",
                params![id.as_str(), index],
                |r| r.get(0),
            )
            .optional()
            .map_err(error)?;
        if let Some(receipt) = receipt {
            return match receipt.as_str() {
                "committed" => Ok(UnitResult::Committed),
                "aborted" => Ok(UnitResult::RolledBack),
                _ => Err(error("invalid unit receipt")),
            };
        }
        let mut accepted = false;
        if let Some(values) = decode(payload) {
            let mut savepoint = tx.savepoint().map_err(error)?;
            accepted = true;
            for value in values {
                let old: i64 = savepoint
                    .query_row(
                        "SELECT value FROM pactum_values WHERE key=?1",
                        [&value.key],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(error)?
                    .unwrap_or(0);
                let Some(next) = old.checked_add(value.value) else {
                    accepted = false;
                    break;
                };
                savepoint.execute("INSERT INTO pactum_values VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![value.key,next]).map_err(error)?;
            }
            if accepted {
                savepoint.commit().map_err(error)?;
            } else {
                savepoint.rollback().map_err(error)?;
            }
        }
        tx.execute(
            "INSERT INTO pactum_units VALUES (?1,?2,?3)",
            params![
                id.as_str(),
                index,
                if accepted { "committed" } else { "aborted" }
            ],
        )
        .map_err(error)?;
        tx.commit().map_err(error)?;
        Ok(if accepted {
            UnitResult::Committed
        } else {
            UnitResult::RolledBack
        })
    }
    fn stage(&mut self, id: &OperationId, request: &PublishRequest) -> Result<(), BackendError> {
        let tx = self.storage.connection.transaction().map_err(error)?;
        bind(&tx, id, &Request::StagedPublish(request.clone()))?;
        match state(&tx, id)?.as_str() {
            "new" | "staged" => {
                tx.execute(
                    "INSERT OR IGNORE INTO pactum_artifacts VALUES (?1,?2)",
                    params![id.as_str(), request.payload],
                )
                .map_err(error)?;
                set_state(&tx, id, "staged")?;
            }
            "published" => {}
            _ => return Err(error("staging was cleaned or rejected")),
        }
        tx.commit().map_err(error)
    }
    fn validate(
        &mut self,
        id: &OperationId,
        request: &PublishRequest,
    ) -> Result<bool, BackendError> {
        let tx = self.storage.connection.transaction().map_err(error)?;
        bind(&tx, id, &Request::StagedPublish(request.clone()))?;
        let payload: Vec<u8> = tx
            .query_row(
                "SELECT payload FROM pactum_artifacts WHERE id=?1",
                [id.as_str()],
                |r| r.get(0),
            )
            .map_err(error)?;
        if payload != request.payload {
            return Err(error("staged artifact changed"));
        }
        Ok(decode(&payload).is_some())
    }
    fn publish(
        &mut self,
        id: &OperationId,
        request: &PublishRequest,
    ) -> Result<PublishResult, BackendError> {
        let tx = self.storage.connection.transaction().map_err(error)?;
        bind(&tx, id, &Request::StagedPublish(request.clone()))?;
        match state(&tx, id)?.as_str() {
            "published" => return Ok(PublishResult::Published),
            "conflict" => return Ok(PublishResult::Conflict),
            "staged" => {}
            _ => return Err(error("artifact is not staged")),
        }
        // Validate again inside the visibility transaction, even if called directly.
        let payload: Vec<u8> = tx
            .query_row(
                "SELECT payload FROM pactum_artifacts WHERE id=?1",
                [id.as_str()],
                |r| r.get(0),
            )
            .map_err(error)?;
        if payload != request.payload || decode(&payload).is_none() {
            return Err(error("invalid staged artifact"));
        }
        let generation: u64 = tx
            .query_row(
                "SELECT generation FROM pactum_visible WHERE target=?1",
                [request.target.as_str()],
                |r| r.get(0),
            )
            .optional()
            .map_err(error)?
            .unwrap_or(0);
        let published = generation == request.expected_generation && generation < i64::MAX as u64;
        if published {
            tx.execute("INSERT INTO pactum_visible VALUES (?1,?2,?3) ON CONFLICT(target) DO UPDATE SET generation=excluded.generation,version=excluded.version", params![request.target.as_str(),generation+1,id.as_str()]).map_err(error)?;
        }
        set_state(&tx, id, if published { "published" } else { "conflict" })?;
        tx.commit().map_err(error)?;
        Ok(if published {
            PublishResult::Published
        } else {
            PublishResult::Conflict
        })
    }
    fn cleanup(&mut self, id: &OperationId, request: &PublishRequest) -> Result<(), BackendError> {
        let tx = self.storage.connection.transaction().map_err(error)?;
        bind(&tx, id, &Request::StagedPublish(request.clone()))?;
        let prior = state(&tx, id)?;
        if prior == "published" {
            return Err(error("cannot clean a published artifact"));
        }
        tx.execute("DELETE FROM pactum_artifacts WHERE id=?1", [id.as_str()])
            .map_err(error)?;
        if prior != "conflict" {
            set_state(&tx, id, "cleaned")?;
        }
        tx.commit().map_err(error)
    }
}
