//! SQLite durable byte journal: WAL, synchronous=FULL, atomic flush batches.
//! A sidecar lock prevents concurrent owners of the same canonical path.
//! Use local storage honoring SQLite locks and sync. Do not unlink lock files,
//! use hard-link aliases, or access an open database through other writers.
use fs2::FileExt;
/// Optional native-transaction outbox/inbox reference adapter.
pub mod messaging;
use pactum_journal::{Checkpoint, Entry, Journal, JournalError, Retention, Sequence};
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
    generation: u64,
    poisoned: bool,
}
fn checksum(sequence: u64, version: u32, payload: &[u8]) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(sequence.to_be_bytes());
    digest.update(version.to_be_bytes());
    digest.update(payload);
    digest.finalize().to_vec()
}
fn head_checksum(sequence: u64, generation: u64) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"pactum-head-v1");
    digest.update(sequence.to_be_bytes());
    digest.update(generation.to_be_bytes());
    digest.finalize().to_vec()
}
fn checkpoint_checksum(c: &Checkpoint) -> Vec<u8> {
    let mut digest = Sha256::new();
    digest.update(b"pactum-checkpoint-v1");
    digest.update(c.generation.to_be_bytes());
    digest.update(c.watermark.to_be_bytes());
    digest.update(c.version.to_be_bytes());
    digest.update(&c.payload);
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
        if schema > 2 {
            return Err(error("unsupported SQLite journal schema"));
        }
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
          CREATE TABLE IF NOT EXISTS pactum_records (
            sequence INTEGER PRIMARY KEY CHECK(sequence > 0), version INTEGER NOT NULL,
            payload BLOB NOT NULL, checksum BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS pactum_checkpoint (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), schema INTEGER NOT NULL,
            generation INTEGER NOT NULL, watermark INTEGER NOT NULL, version INTEGER NOT NULL,
            payload BLOB NOT NULL, checksum BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS pactum_archive (
            sequence INTEGER PRIMARY KEY, version INTEGER NOT NULL,
            payload BLOB NOT NULL, checksum BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS pactum_head (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), sequence INTEGER NOT NULL,
            generation INTEGER NOT NULL, checksum BLOB NOT NULL);",
            )
            .map_err(error)?;
        if schema < 2 {
            let sequence: u64 = connection
                .query_row(
                    "SELECT COALESCE(MAX(sequence),0) FROM pactum_records",
                    [],
                    |r| r.get(0),
                )
                .map_err(error)?;
            connection
                .execute(
                    "INSERT INTO pactum_head VALUES (1, ?1, 0, ?2)",
                    params![sequence, head_checksum(sequence, 0)],
                )
                .map_err(error)?;
        }
        connection
            .execute_batch("PRAGMA user_version=2; COMMIT;")
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
            generation: 0,
            poisoned: false,
        };
        let mut last = journal.checkpoint()?.map_or(0, |c| c.watermark);
        journal.visit(&mut |e| {
            last = e.sequence;
            Ok(())
        })?;
        let (head, generation, hash): (u64, u64, Vec<u8>) = journal
            .connection
            .query_row(
                "SELECT sequence, generation, checksum FROM pactum_head WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(error)?;
        if head != last
            || generation != journal.checkpoint()?.map_or(0, |c| c.generation)
            || hash != head_checksum(head, generation)
        {
            return Err(error("journal head/checkpoint continuity mismatch"));
        }
        journal.generation = generation;
        journal.durable = last;
        journal.staged = journal.durable;
        Ok(journal)
    }
    /// Stream archived events for external export, validating their checksums.
    /// Archive sequences can have gaps when earlier generations used Compact.
    pub fn export_archive(
        &mut self,
        visitor: &mut dyn FnMut(Entry) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.healthy()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence, version, payload, checksum FROM pactum_archive ORDER BY sequence",
            )
            .map_err(error)?;
        let mut rows = statement.query([]).map_err(error)?;
        while let Some(row) = rows.next().map_err(error)? {
            let entry = Entry {
                sequence: row.get(0).map_err(error)?,
                version: row.get(1).map_err(error)?,
                payload: row.get(2).map_err(error)?,
            };
            let hash: Vec<u8> = row.get(3).map_err(error)?;
            if hash != checksum(entry.sequence, entry.version, &entry.payload) {
                return Err(error("archive checksum mismatch"));
            }
            visitor(entry)?;
        }
        Ok(())
    }
    fn compact_with_barrier(
        &mut self,
        watermark: Sequence,
        version: u32,
        payload: &[u8],
        retention: Retention,
        barrier: &mut dyn FnMut(&str),
    ) -> Result<(), JournalError> {
        self.healthy()?;
        if self.staged != self.durable || watermark != self.durable {
            return Err(error(
                "checkpoint requires exact durable watermark and no staged writes",
            ));
        }
        let generation = self
            .generation
            .checked_add(1)
            .filter(|g| *g <= i64::MAX as u64)
            .ok_or_else(|| error("checkpoint generation exhausted"))?;
        let checkpoint = Checkpoint {
            generation,
            watermark,
            version,
            payload: payload.to_vec(),
        };
        self.poisoned = true;
        barrier("before_begin");
        let transaction = self.connection.transaction().map_err(error)?;
        transaction
            .execute(
                "INSERT OR REPLACE INTO pactum_checkpoint VALUES (1, 1, ?1, ?2, ?3, ?4, ?5)",
                params![
                    generation,
                    watermark,
                    version,
                    payload,
                    checkpoint_checksum(&checkpoint)
                ],
            )
            .map_err(error)?;
        barrier("snapshot_written");
        if retention == Retention::Archive {
            transaction
                .execute(
                    "INSERT INTO pactum_archive SELECT * FROM pactum_records WHERE sequence <= ?1",
                    [watermark],
                )
                .map_err(error)?;
        }
        barrier("archive_written");
        transaction
            .execute(
                "DELETE FROM pactum_records WHERE sequence <= ?1",
                [watermark],
            )
            .map_err(error)?;
        barrier("history_removed");
        transaction
            .execute(
                "UPDATE pactum_head SET generation=?1, checksum=?2 WHERE singleton=1",
                params![generation, head_checksum(watermark, generation)],
            )
            .map_err(error)?;
        barrier("before_commit");
        transaction.commit().map_err(error)?;
        barrier("after_commit");
        self.generation = generation;
        self.poisoned = false;
        Ok(())
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
            let generation = self.generation;
            if let Err(e) = self.connection.execute(
                "UPDATE pactum_head SET sequence=?1, checksum=?2 WHERE singleton=1",
                params![self.staged, head_checksum(self.staged, generation)],
            ) {
                self.poisoned = true;
                return Err(error(e));
            }
            if let Err(e) = self.connection.execute_batch("COMMIT") {
                self.poisoned = true;
                return Err(error(e));
            }
            self.durable = self.staged;
        }
        Ok(self.durable)
    }
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError> {
        let mut entries = Vec::new();
        self.visit(&mut |e| {
            entries.push(e);
            Ok(())
        })?;
        Ok(entries)
    }
    fn visit(
        &mut self,
        visitor: &mut dyn FnMut(Entry) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        self.healthy()?;
        let watermark = self.checkpoint()?.map_or(0, |c| c.watermark);
        let mut expected = watermark;
        let mut statement = self
            .connection
            .prepare(
                "SELECT sequence, version, payload, checksum FROM pactum_records ORDER BY sequence",
            )
            .map_err(error)?;
        let mut rows = statement.query([]).map_err(error)?;
        while let Some(row) = rows.next().map_err(error)? {
            let sequence: u64 = row.get(0).map_err(error)?;
            if self.staged != self.durable && sequence > self.durable {
                break;
            }
            let version = row.get(1).map_err(error)?;
            let payload: Vec<u8> = row.get(2).map_err(error)?;
            let hash: Vec<u8> = row.get(3).map_err(error)?;
            expected = expected
                .checked_add(1)
                .ok_or_else(|| error("sequence exhausted"))?;
            if sequence != expected || hash != checksum(sequence, version, &payload) {
                return Err(error("journal sequence gap or checksum mismatch"));
            }
            visitor(Entry {
                sequence,
                version,
                payload,
            })?;
        }
        // On open durable is not known yet; later reads must cover the known tail.
        if self.durable != 0 && expected != self.durable {
            return Err(error("journal durable tail missing"));
        }
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<Option<Checkpoint>, JournalError> {
        self.healthy()?;
        let mut statement = self.connection.prepare(
            "SELECT schema, generation, watermark, version, payload, checksum FROM pactum_checkpoint WHERE singleton=1"
        ).map_err(error)?;
        let mut rows = statement.query([]).map_err(error)?;
        let Some(row) = rows.next().map_err(error)? else {
            return Ok(None);
        };
        let schema: u32 = row.get(0).map_err(error)?;
        let checkpoint = Checkpoint {
            generation: row.get(1).map_err(error)?,
            watermark: row.get(2).map_err(error)?,
            version: row.get(3).map_err(error)?,
            payload: row.get(4).map_err(error)?,
        };
        let hash: Vec<u8> = row.get(5).map_err(error)?;
        if schema != 1
            || checkpoint.generation == 0
            || checkpoint.watermark > i64::MAX as u64
            || hash != checkpoint_checksum(&checkpoint)
        {
            return Err(error("corrupt or incompatible checkpoint"));
        }
        Ok(Some(checkpoint))
    }
    fn compact(
        &mut self,
        watermark: Sequence,
        version: u32,
        payload: &[u8],
        retention: Retention,
    ) -> Result<(), JournalError> {
        self.compact_with_barrier(watermark, version, payload, retention, &mut |_| {})
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_cutover_child() {
        let Ok(path) = std::env::var("PACTUM_COMPACT_PATH") else {
            return;
        };
        let target = std::env::var("PACTUM_COMPACT_BARRIER").unwrap();
        let retention = if std::env::var("PACTUM_COMPACT_ARCHIVE").unwrap() == "true" {
            Retention::Archive
        } else {
            Retention::Compact
        };
        let mut journal = SqliteJournal::open(path).unwrap();
        journal
            .compact_with_barrier(
                journal.durable,
                7,
                b"replacement-state",
                retention,
                &mut |barrier| {
                    if barrier == target {
                        std::process::exit(86);
                    }
                },
            )
            .unwrap();
        panic!("barrier was not reached");
    }
    #[test]
    fn process_death_at_every_cutover_barrier_preserves_one_complete_generation() {
        for existing in [false, true] {
            for retention in [Retention::Compact, Retention::Archive] {
                for barrier in [
                    "before_begin",
                    "snapshot_written",
                    "archive_written",
                    "history_removed",
                    "before_commit",
                    "after_commit",
                ] {
                    let temp = tempfile::tempdir().unwrap();
                    let path = temp.path().join("journal.db");
                    let mut j = SqliteJournal::open(&path).unwrap();
                    j.append(7, b"first").unwrap();
                    j.flush().unwrap();
                    if existing {
                        j.compact(1, 7, b"old-state", Retention::Compact).unwrap();
                    }
                    j.append(7, b"second").unwrap();
                    j.flush().unwrap();
                    drop(j);
                    let output = std::process::Command::new(std::env::current_exe().unwrap())
                        .args(["--exact", "tests::checkpoint_cutover_child", "--nocapture"])
                        .env("PACTUM_COMPACT_PATH", &path)
                        .env("PACTUM_COMPACT_BARRIER", barrier)
                        .env(
                            "PACTUM_COMPACT_ARCHIVE",
                            (retention == Retention::Archive).to_string(),
                        )
                        .output()
                        .unwrap();
                    assert_eq!(output.status.code(), Some(86), "{output:?}");
                    let mut j = SqliteJournal::open(&path).unwrap();
                    let checkpoint = j.checkpoint().unwrap();
                    let entries = j.replay().unwrap();
                    let mut archived = Vec::new();
                    j.export_archive(&mut |e| {
                        archived.push(e);
                        Ok(())
                    })
                    .unwrap();
                    if barrier == "after_commit" {
                        let c = checkpoint.unwrap();
                        assert_eq!(c.generation, if existing { 2 } else { 1 });
                        assert_eq!(c.watermark, 2);
                        assert_eq!(c.payload, b"replacement-state");
                        assert!(entries.is_empty());
                        assert_eq!(
                            archived.len(),
                            if retention == Retention::Archive {
                                if existing { 1 } else { 2 }
                            } else {
                                0
                            }
                        );
                    } else {
                        assert_eq!(
                            checkpoint.map(|c| c.payload),
                            existing.then(|| b"old-state".to_vec())
                        );
                        assert_eq!(entries.len(), if existing { 1 } else { 2 });
                        assert_eq!(entries.last().unwrap().sequence, 2);
                        assert!(archived.is_empty());
                    }
                    assert_eq!(j.append(7, b"tail").unwrap(), 3);
                    j.flush().unwrap();
                    drop(j);
                    assert_eq!(
                        SqliteJournal::open(&path)
                            .unwrap()
                            .replay()
                            .unwrap()
                            .last()
                            .unwrap()
                            .sequence,
                        3
                    );
                }
            }
        }
    }
    #[test]
    fn checkpoint_corruption_missing_state_and_tail_fail_closed() {
        for sql in [
            "UPDATE pactum_checkpoint SET schema=2",
            "UPDATE pactum_checkpoint SET version=9",
            "UPDATE pactum_checkpoint SET generation=generation+1",
            "UPDATE pactum_checkpoint SET watermark=watermark+1",
            "UPDATE pactum_checkpoint SET payload=x'00'",
            "UPDATE pactum_checkpoint SET checksum=x'00'",
            "DELETE FROM pactum_checkpoint",
            "DELETE FROM pactum_records",
            "DELETE FROM pactum_head",
            "UPDATE pactum_head SET sequence=1",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("j.db");
            let mut j = SqliteJournal::open(&path).unwrap();
            j.append(1, b"one").unwrap();
            j.flush().unwrap();
            j.compact(1, 1, b"state", Retention::Compact).unwrap();
            j.append(1, b"tail").unwrap();
            j.flush().unwrap();
            drop(j);
            Connection::open(&path).unwrap().execute_batch(sql).unwrap();
            assert!(SqliteJournal::open(&path).is_err(), "{sql}");
        }
    }
    #[test]
    fn exact_watermark_staging_and_archive_export() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("j.db");
        let mut j = SqliteJournal::open(&path).unwrap();
        j.append(1, b"one").unwrap();
        assert!(j.compact(0, 1, b"state", Retention::Compact).is_err());
        j.flush().unwrap();
        assert!(j.compact(0, 1, b"state", Retention::Compact).is_err());
        j.compact(1, 1, b"state", Retention::Archive).unwrap();
        j.compact(1, 1, b"state", Retention::Archive).unwrap();
        assert_eq!(j.checkpoint().unwrap().unwrap().generation, 2);
        let mut count = 0;
        j.export_archive(&mut |e| {
            assert_eq!(e.payload, b"one");
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
        j.append(1, b"staged").unwrap();
        assert!(j.replay().unwrap().is_empty());
        drop(j);
        assert!(
            SqliteJournal::open(&path)
                .unwrap()
                .replay()
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn failed_checkpoint_write_poisoning_requires_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("j.db");
        let mut j = SqliteJournal::open(&path).unwrap();
        j.append(1, b"decision").unwrap();
        j.flush().unwrap();
        j.connection.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE INSERT ON pactum_checkpoint BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
        assert!(j.compact(1, 1, b"state", Retention::Compact).is_err());
        assert!(j.replay().is_err());
        assert!(j.append(1, b"other").is_err());
        drop(j);
        let mut j = SqliteJournal::open(&path).unwrap();
        assert!(j.checkpoint().unwrap().is_none());
        assert_eq!(j.replay().unwrap()[0].payload, b"decision");
    }
    #[test]
    fn legacy_schema_migrates_without_changing_sequence() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("j.db");
        let mut j = SqliteJournal::open(&path).unwrap();
        j.append(1, b"legacy").unwrap();
        j.flush().unwrap();
        drop(j);
        Connection::open(&path).unwrap().execute_batch("DROP TABLE pactum_head; DROP TABLE pactum_checkpoint; DROP TABLE pactum_archive; PRAGMA user_version=1;").unwrap();
        let mut j = SqliteJournal::open(&path).unwrap();
        assert_eq!(j.replay().unwrap()[0].payload, b"legacy");
        assert_eq!(j.append(1, b"new").unwrap(), 2);
        j.flush().unwrap();
    }
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
        connection.execute_batch("PRAGMA user_version=3").unwrap();
        assert!(SqliteJournal::open(&path).is_err());
    }
}

/// Backend-native execution and staged publication reference adapter.
pub mod execution;
pub use execution::{SqliteBackend, Value, VisibleVersion};
