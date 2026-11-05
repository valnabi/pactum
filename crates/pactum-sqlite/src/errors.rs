//! Source-preserving mapping for the SQLite reference adapters.
use pactum_errors::{Diagnostic, ErrorCode};
use snafu::Snafu;

/// Typed reference-adapter boundary error. SQL/source text is never auto-logged.
#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum AdapterError {
    /// SQLite operation failed, possibly after committing.
    #[snafu(context(false), display("SQLite operation failed"))]
    Sqlite {
        /// Original driver error.
        source: rusqlite::Error,
    },
    /// Filesystem operation failed.
    #[snafu(context(false), display("filesystem operation failed"))]
    Io {
        /// Original I/O error.
        source: std::io::Error,
    },
    /// Encoding or decoding failed.
    #[snafu(context(false), display("serialization failed"))]
    Serialization {
        /// Original codec error.
        source: serde_json::Error,
    },
    /// Wrapped journal contract failure.
    #[snafu(context(false), display("journal operation failed"))]
    Journal {
        /// Journal error and its original source.
        source: pactum_journal::JournalError,
    },
    /// Wrapped engine failure.
    #[snafu(context(false), display("engine operation failed"))]
    Engine {
        /// Engine diagnostic.
        source: pactum_core::Error,
    },
    /// Clock cannot supply a valid authority time.
    #[snafu(context(false), display("authority clock failed"))]
    Clock {
        /// Original clock error.
        source: std::time::SystemTimeError,
    },
    /// Integer range cannot represent an input or durable field.
    #[snafu(context(false), display("integer range exceeded"))]
    Range {
        /// Original conversion error.
        source: std::num::TryFromIntError,
    },
    /// Resource payload is not UTF-8.
    #[snafu(context(false), display("invalid resource text"))]
    Utf8 {
        /// Original decoding error.
        source: std::str::Utf8Error,
    },
    /// Resource payload is not a valid integer.
    #[snafu(context(false), display("invalid resource number"))]
    Parse {
        /// Original parsing error.
        source: std::num::ParseIntError,
    },
    /// Safe explicit validation/invariant message.
    #[snafu(display("{detail}"))]
    Invalid {
        /// Safe message, never arbitrary source text.
        detail: String,
    },
}
impl From<&str> for AdapterError {
    fn from(detail: &str) -> Self {
        Self::Invalid {
            detail: detail.into(),
        }
    }
}
impl From<String> for AdapterError {
    fn from(detail: String) -> Self {
        Self::Invalid { detail }
    }
}
impl AdapterError {
    /// Translate typed causes into stable public codes without string matching.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Sqlite {
                source: rusqlite::Error::SqliteFailure(error, _),
            } => match error.code {
                rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase => {
                    ErrorCode::CorruptHistory
                }
                // Busy writes remain conservatively ambiguous: an outer commit may
                // already have happened. Read-only boundaries may opt into Unavailable.
                _ => ErrorCode::Storage,
            },
            Self::Journal { source } => source.code(),
            Self::Engine { source } => source.code(),
            Self::Invalid { .. } | Self::Range { .. } | Self::Utf8 { .. } | Self::Parse { .. } => {
                ErrorCode::InvalidInput
            }
            Self::Serialization { .. } => ErrorCode::CorruptHistory,
            _ => ErrorCode::Storage,
        }
    }
    /// Retain the typed adapter and original driver/I/O/codec source chain.
    pub fn diagnostic(self) -> Diagnostic {
        if let Self::Invalid { detail } = self {
            Diagnostic::message(ErrorCode::InvalidInput, detail)
        } else {
            Diagnostic::caused(self.code(), self)
        }
    }
}
