//! Independent ordered byte journal with an explicit durability barrier.
//!
//! Implementations preserve every flushed prefix and reject corruption. Any
//! append/flush error is ambiguous: drop and reopen before further decisions.
use std::{error::Error, fmt};

/// Journal position, starting at one.
pub type Sequence = u64;

/// Durable opaque record; payload schema belongs to the consumer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Contiguous journal position.
    pub sequence: Sequence,
    /// Consumer-defined format version.
    pub version: u32,
    /// Consumer-defined bytes.
    pub payload: Vec<u8>,
}

/// Storage failure, including uncertain durability and corrupt replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalError(pub String);
impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Error for JournalError {}

/// Single-owner append-only storage, independent of the transaction engine.
///
/// Append stages records in order. Flush atomically persists the staged batch.
/// Replay returns only the durable contiguous prefix, including writes whose
/// barrier acknowledgement was lost. Drop may discard unflushed staging.
/// After *any* write error, callers must drop and reopen the journal.
pub trait Journal: Send {
    /// Stage a record; success does not imply durability.
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError>;
    /// Persist all preceding appends, returning the last durable sequence.
    fn flush(&mut self) -> Result<Sequence, JournalError>;
    /// Read all durable entries; never silently truncate corrupt records.
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError>;
}

/// Backend conformance check for a fresh journal. Reopen/crash behavior needs
/// additional backend-specific tests. Panics if the contract is violated.
pub fn check_conformance(journal: &mut impl Journal) -> Result<(), JournalError> {
    assert!(journal.replay()?.is_empty());
    assert_eq!(journal.append(7, b"one")?, 1);
    assert_eq!(journal.append(8, b"two")?, 2);
    assert!(journal.replay()?.is_empty());
    assert_eq!(journal.flush()?, 2);
    assert_eq!(journal.flush()?, 2);
    assert_eq!(
        journal.replay()?,
        vec![
            Entry {
                sequence: 1,
                version: 7,
                payload: b"one".to_vec()
            },
            Entry {
                sequence: 2,
                version: 8,
                payload: b"two".to_vec()
            },
        ]
    );
    Ok(())
}
