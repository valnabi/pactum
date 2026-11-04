//! SQLite durable byte journal: WAL, synchronous=FULL, atomic flush batches.
//! A sidecar lock prevents concurrent owners of the same canonical path.
//! Use local storage honoring SQLite locks and sync. Do not unlink lock files,
//! use hard-link aliases, or access an open database through other writers.
use fs2::FileExt;
use pactum_journal::{Entry, Journal, JournalError, Sequence};
use rusqlite::{Connection, params};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    path::Path,
};
fn error(e: impl std::fmt::Display) -> JournalError {
    JournalError(e.to_string())
}

/// Exclusive SQLite journal. Drop rolls back unflushed records.
pub struct SqliteJournal {
    connection: Connection,
    _lock: File,
    durable: Sequence,
    staged: Sequence,
    poisoned: bool,
}
fn checksum(sequence: u64, version: u32, payload: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(sequence.to_be_bytes());
    digest.update(version.to_be_bytes());
    digest.update(payload);
    digest.finalize().to_vec()
}
impl SqliteJournal {
    /// Open/create with FULL synchronization and exclusive ownership.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = parent.canonicalize().map_err(error)?;
        let name = path
            .file_name()
            .ok_or_else(|| error("journal needs a filename"))?;
        let path = parent.join(name);
        let path = if path.exists() {
            path.canonicalize().map_err(error)?
        } else {
            path
        };
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_name)
            .map_err(error)?;
        FileExt::try_lock_exclusive(&lock).map_err(error)?;
        let connection = Connection::open(path).map_err(error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(error)?;
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
            .map_err(error)?;
        let mode: String = connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(error)?;
        let synchronization: u32 = connection
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .map_err(error)?;
        if mode != "wal" || synchronization != 2 {
            return Err(error("WAL and FULL synchronization are required"));
        }
        let schema: u32 = connection
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(error)?;
        if schema > 1 {
            return Err(error("unsupported SQLite journal schema"));
        }
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
          CREATE TABLE IF NOT EXISTS pactum_records (
            sequence INTEGER PRIMARY KEY CHECK(sequence > 0), version INTEGER NOT NULL,
            payload BLOB NOT NULL, checksum BLOB NOT NULL);
          PRAGMA user_version=1; COMMIT;",
            )
            .map_err(error)?;
        let integrity: String = connection
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .map_err(error)?;
        if integrity != "ok" {
            return Err(error(integrity));
        }
        let mut journal = Self {
            connection,
            _lock: lock,
            durable: 0,
            staged: 0,
            poisoned: false,
        };
        journal.durable = journal.replay()?.last().map_or(0, |e| e.sequence);
        journal.staged = journal.durable;
        Ok(journal)
    }
    fn healthy(&self) -> Result<(), JournalError> {
        if self.poisoned {
            Err(error("journal write failed; drop and reopen"))
        } else {
            Ok(())
        }
    }
}
impl Journal for SqliteJournal {
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError> {
        self.healthy()?;
        let result = (|| {
            if self.staged == self.durable {
                self.connection
                    .execute_batch("BEGIN IMMEDIATE")
                    .map_err(error)?;
            }
            let sequence = self
                .staged
                .checked_add(1)
                .filter(|s| *s <= i64::MAX as u64)
                .ok_or_else(|| error("sequence exhausted"))?;
            self.connection
                .execute(
                    "INSERT INTO pactum_records VALUES (?1, ?2, ?3, ?4)",
                    params![
                        sequence,
                        version,
                        payload,
                        checksum(sequence, version, payload)
                    ],
                )
                .map_err(error)?;
            self.staged = sequence;
            Ok(sequence)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn flush(&mut self) -> Result<Sequence, JournalError> {
        self.healthy()?;
        if self.staged != self.durable {
            if let Err(e) = self.connection.execute_batch("COMMIT") {
                self.poisoned = true;
                return Err(error(e));
            }
            self.durable = self.staged;
        }
        Ok(self.durable)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        self.healthy()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence, version, payload, checksum FROM pactum_records ORDER BY sequence",
            )
            .map_err(error)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, u64>(0)?,
                    r.get::<_, u32>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                ))
            })
            .map_err(error)?;
        let mut entries = Vec::new();
        for row in rows {
            let (sequence, version, payload, hash) = row.map_err(error)?;
            if self.staged != self.durable && sequence > self.durable {
                break;
            }
            if sequence != entries.len() as u64 + 1 || hash != checksum(sequence, version, &payload)
            {
                return Err(error("journal sequence gap or checksum mismatch"));
            }
            entries.push(Entry {
                sequence,
                version,
                payload,
            });
        }
        Ok(entries)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conformance_reopen_rollback_and_exclusive_owner() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("journal.db");
        let mut journal = SqliteJournal::open(&path).unwrap();
        pactum_journal::check_conformance(&mut journal).unwrap();
        assert!(SqliteJournal::open(&path).is_err());
        journal.append(9, b"unflushed").unwrap();
        drop(journal);
        let mut journal = SqliteJournal::open(&path).unwrap();
        assert_eq!(journal.replay().unwrap().len(), 2);
        assert_eq!(journal.append(9, b"replacement").unwrap(), 3);
        journal.flush().unwrap();
        drop(journal);
        assert_eq!(
            SqliteJournal::open(&path).unwrap().replay().unwrap().len(),
            3
        );
    }
    #[test]
    fn corruption_and_future_schema_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("journal.db");
        let mut journal = SqliteJournal::open(&path).unwrap();
        journal.append(1, b"decision").unwrap();
        journal.flush().unwrap();
        drop(journal);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("UPDATE pactum_records SET payload=x'00'", [])
            .unwrap();
        assert!(SqliteJournal::open(&path).is_err());
        connection.execute_batch("PRAGMA user_version=2").unwrap();
        assert!(SqliteJournal::open(&path).is_err());
    }
}

/// Backend-native execution and staged publication reference adapter.
pub mod execution;
pub use execution::{SqliteBackend, Value, VisibleVersion};
