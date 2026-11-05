//! Independent ordered byte journal with an explicit durability barrier.
//!
//! Implementations preserve every flushed prefix and reject corruption. Any
//! append/flush error is ambiguous: drop and reopen before further decisions.
pub use pactum_errors::{Diagnostic, ErrorCode, ErrorContext};

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
#[derive(Clone, Debug, PartialEq, Eq, snafu::Snafu)]
#[snafu(transparent)]
pub struct JournalError {
    source: pactum_errors::Diagnostic,
}
impl JournalError {
    /// Construct a safe legacy diagnostic; completion remains conservative.
    pub fn message(detail: String) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::message(
            pactum_errors::ErrorCode::DurabilityUnknown,
            detail,
        ))
    }
    /// Report a validated durable schema/history invariant failure.
    pub fn corrupt(detail: String) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::message(
            pactum_errors::ErrorCode::CorruptHistory,
            detail,
        ))
    }
    /// Preserve a typed boundary source using the conservative default code.
    pub fn from_source(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::classified(pactum_errors::ErrorCode::DurabilityUnknown, source)
    }
    /// Preserve a typed boundary source with an explicit machine-readable code.
    pub fn classified(
        code: pactum_errors::ErrorCode,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::from_diagnostic(pactum_errors::Diagnostic::caused(code, source))
    }
    /// Wrap an existing structured diagnostic without losing its classification.
    pub fn from_diagnostic(source: pactum_errors::Diagnostic) -> Self {
        Self { source }
    }
    /// Add allowlisted boundary identifiers, preserving prior metadata and sources.
    pub fn with_context(self, context: pactum_errors::ErrorContext) -> Self {
        Self {
            source: self.source.with_context(context),
        }
    }
    /// Inspect the structured diagnostic.
    pub fn diagnostic(&self) -> &pactum_errors::Diagnostic {
        &self.source
    }
}
impl std::ops::Deref for JournalError {
    type Target = pactum_errors::Diagnostic;
    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

/// Validated checkpoint envelope. The backend authenticates all fields with a checksum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// Monotonic checkpoint generation, starting at one.
    pub generation: u64,
    /// Last sequence represented by the state; tail starts at watermark + 1.
    pub watermark: Sequence,
    /// Consumer state schema, independent of journal record versions.
    pub version: u32,
    /// Consumer state including permanent retry bindings and pending work.
    pub payload: Vec<u8>,
}

/// Policy for event history covered by a checkpoint. Retry bindings must always
/// remain in checkpoint state; expiring transaction keys is not supported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Retention {
    /// Remove covered events after installing their replacement state atomically.
    Compact,
    /// Move covered events into an archive, available for streaming export.
    Archive,
}

/// Single-owner append-only storage, independent of the transaction engine.
///
/// Append stages records in order. Flush atomically persists the staged batch.
/// Replay returns only the durable contiguous tail after any checkpoint,
/// including writes whose barrier acknowledgement was lost. Restore checkpoint
/// state before applying that tail. Drop may discard unflushed staging.
/// After *any* write error, callers must drop and reopen the journal.
pub trait Journal: Send {
    /// Stage a record; success does not imply durability.
    fn append(&mut self, version: u32, payload: &[u8]) -> Result<Sequence, JournalError>;
    /// Persist all preceding appends, returning the last durable sequence.
    fn flush(&mut self) -> Result<Sequence, JournalError>;
    /// Read the durable tail after the active checkpoint. With no checkpoint,
    /// this is the full history. Never silently truncate corrupt records.
    /// Consumers must restore checkpoint state before applying these entries.
    fn replay(&mut self) -> Result<Vec<Entry>, JournalError>;
    /// Stream durable tail entries after the checkpoint, in sequence order.
    /// The default supports legacy backends; override to avoid full-history allocation.
    fn visit(
        &mut self,
        visitor: &mut dyn FnMut(Entry) -> Result<(), JournalError>,
    ) -> Result<(), JournalError> {
        for entry in self.replay()? {
            visitor(entry)?;
        }
        Ok(())
    }
    /// Read and validate the active checkpoint, or None for legacy history.
    fn checkpoint(&mut self) -> Result<Option<Checkpoint>, JournalError> {
        Ok(None)
    }
    /// Atomically replace checkpoint and covered history at an exact durable
    /// watermark. Reject staged writes. Any failure requires dropping/reopening.
    /// Consumer must supply validated complete state, including every retry key.
    fn compact(
        &mut self,
        _watermark: Sequence,
        _version: u32,
        _payload: &[u8],
        _retention: Retention,
    ) -> Result<(), JournalError> {
        Err(JournalError::message(
            "checkpoint installation is unsupported".into(),
        ))
    }
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
