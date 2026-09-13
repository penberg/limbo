use thiserror::Error;

use crate::storage::page_cache::CacheError;

#[derive(Debug, Clone, Error, miette::Diagnostic)]
pub enum LimboError {
    #[error("Corrupt database: {0}")]
    Corrupt(String),
    #[error("File is not a database")]
    NotADB,
    #[error("Internal error: {0}")]
    InternalError(String),
    /// An error raised by emitted bytecode (`Insn::Halt` with SQLITE_ERROR),
    /// e.g. ALTER TABLE validation or window-function argument checks.
    /// Displayed bare, like sqlite3_errmsg. Kept distinct from `Constraint`
    /// so `abort()` does not apply ON CONFLICT resolution to it.
    #[error("{0}")]
    SqlError(String),
    #[error(transparent)]
    CacheError(#[from] CacheError),
    #[error("Database is full: {0}")]
    DatabaseFull(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    /// Boxed: the parser error is ~96 bytes inline and would dominate
    /// `LimboError`'s size, which rides in every hot-path `Result`.
    #[error(transparent)]
    #[diagnostic(transparent)]
    LexerError(#[from] Box<turso_parser::error::Error>),
    #[error("Conversion error: {0}")]
    ConversionError(String),
    #[error("Env variable error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    #[error("Transaction error: {0}")]
    TxError(String),
    #[error(transparent)]
    CompletionError(#[from] CompletionError),
    #[error("Locking error: {0}")]
    LockingError(String),
    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Parse error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("Parse error: {0}")]
    InvalidDate(String),
    #[error("Parse error: {0}")]
    InvalidTime(String),
    #[error("Modifier parsing error: {0}")]
    InvalidModifier(String),
    #[error("{0}")]
    InvalidArgument(String),
    #[error("Invalid formatter supplied: {0}")]
    InvalidFormatter(String),
    #[error("{0}")]
    Constraint(String),
    #[error("{0}")]
    /// We need to specify for ROLLBACK|FAIL resolve types when to roll the tx back
    /// so instead of matching on the string, we introduce a specific ForeignKeyConstraint error
    ForeignKeyConstraint(String),
    #[error("{1}")]
    Raise(turso_parser::ast::ResolveType, String),
    #[error("RaiseIgnore")]
    RaiseIgnore,
    #[error("Extension error: {0}")]
    ExtensionError(String),
    #[error("integer overflow")]
    IntegerOverflow,
    #[error("string or blob too big")]
    TooBig,
    #[error("database table is locked")]
    TableLocked,
    #[error("attempt to write a readonly database")]
    ReadOnly,
    #[error("Database is busy")]
    Busy,
    /// A transaction-control or savepoint operation, or a second concurrent
    /// write statement, was rejected because another statement on the same
    /// connection is still in progress. This carries SQLITE_BUSY semantics at
    /// the API surface (SQLite reports these as SQLITE_BUSY, e.g. "cannot
    /// commit transaction - SQL statements in progress"), but unlike `Busy`
    /// it is not retryable in place and must never invoke the busy handler:
    /// waiting cannot help, only the application finishing or resetting its
    /// own statement can. The payload names the rejected operation.
    #[error("{0} - SQL statements in progress")]
    StatementsInProgress(&'static str),
    #[error("interrupt")]
    Interrupt,
    #[error("Database snapshot is stale. You must rollback and retry the whole transaction.")]
    BusySnapshot,
    #[error("Conflict: {0}")]
    Conflict(String),
    #[error("Database schema changed")]
    SchemaUpdated,
    #[error("Database schema conflict")]
    SchemaConflict,
    #[error(
        "Database is empty, header does not exist - page 1 should've been allocated before this"
    )]
    Page1NotAlloc,
    #[error("Transaction terminated")]
    TxTerminated,
    #[error("Write-write conflict")]
    WriteWriteConflict,
    #[error("Commit dependency aborted")]
    CommitDependencyAborted,
    #[error("No such transaction ID: {0}")]
    NoSuchTransactionID(String),
    #[error("Null value")]
    NullValue,
    #[error("invalid column type")]
    InvalidColumnType,
    #[error("Invalid blob size, expected {0}")]
    InvalidBlobSize(usize),
    /// An incremental-blob handle was invalidated because the row it points to
    /// (or its table) was modified after the handle was opened. Mirrors SQLite's
    /// expired blob handles: every subsequent read/write must fail with
    /// SQLITE_ABORT until the handle is closed.
    #[error("blob handle expired")]
    BlobHandleExpired,
    #[error("Planning error: {0}")]
    PlanningError(String),
    #[error("Checkpoint failed: {0}")]
    CheckpointFailed(String),
    #[error("Unsupported text encoding: {0}. Only UTF-8 is supported.")]
    UnsupportedEncoding(String),
    #[error("Out of memory")]
    OutOfMemory,
}

/// `?` in functions returning boxed errors (see `InsnResult`) composes
/// `From<X> for LimboError` with the boxing. One impl per source error type
/// that crosses that boundary; a blanket impl would overlap std's
/// `From<T> for Box<T>`.
macro_rules! boxed_from {
    ($ty:ty) => {
        impl From<$ty> for Box<LimboError> {
            fn from(err: $ty) -> Self {
                Box::new(LimboError::from(err))
            }
        }
    };
}
boxed_from!(crate::alloc::TryReserveError);
boxed_from!(std::collections::TryReserveError);
boxed_from!(turso_parser::error::Error);
boxed_from!(CompletionError);
boxed_from!(CacheError);
boxed_from!(bumpalo::AllocErr);
boxed_from!(crate::alloc::AllocError);

impl From<Box<LimboError>> for LimboError {
    fn from(err: Box<LimboError>) -> Self {
        *err
    }
}

impl From<turso_parser::error::Error> for LimboError {
    fn from(err: turso_parser::error::Error) -> Self {
        LimboError::LexerError(Box::new(err))
    }
}

impl LimboError {
    /// The primary SQLite result code for this error, e.g. what the sqlite3
    /// shell appends after a runtime error message ("... (19)").
    pub fn sqlite_result_code(&self) -> i32 {
        match self {
            Self::Constraint(_) | Self::ForeignKeyConstraint(_) | Self::Raise(..) => 19,
            Self::Busy | Self::BusySnapshot | Self::StatementsInProgress(_) => 5,
            Self::TableLocked => 6,
            Self::ReadOnly => 8,
            Self::Interrupt => 9,
            Self::Corrupt(_) => 11,
            Self::DatabaseFull(_) => 13,
            Self::SchemaUpdated | Self::SchemaConflict => 17,
            Self::TooBig => 18,
            Self::NotADB => 26,
            Self::BlobHandleExpired => 4,
            _ => 1,
        }
    }
}

impl From<crate::alloc::AllocError> for LimboError {
    fn from(_: crate::alloc::AllocError) -> Self {
        Self::OutOfMemory
    }
}

impl From<crate::alloc::TryReserveError> for LimboError {
    fn from(_: crate::alloc::TryReserveError) -> Self {
        Self::OutOfMemory
    }
}

#[cfg(not(nightly))]
impl From<allocator_api2::collections::TryReserveError> for LimboError {
    fn from(_: allocator_api2::collections::TryReserveError) -> Self {
        Self::OutOfMemory
    }
}

impl From<std::collections::TryReserveError> for LimboError {
    fn from(_: std::collections::TryReserveError) -> Self {
        Self::OutOfMemory
    }
}

impl From<bumpalo::AllocErr> for LimboError {
    fn from(_: bumpalo::AllocErr) -> Self {
        Self::OutOfMemory
    }
}

impl From<bumpalo::collections::CollectionAllocErr> for LimboError {
    fn from(_: bumpalo::collections::CollectionAllocErr) -> Self {
        Self::OutOfMemory
    }
}

#[cfg(target_family = "unix")]
impl From<rustix::io::Errno> for LimboError {
    fn from(value: rustix::io::Errno) -> Self {
        CompletionError::from(value).into()
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
impl From<&'static str> for LimboError {
    fn from(value: &'static str) -> Self {
        CompletionError::UringIOError(value).into()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Error)]
pub enum CompletionError {
    #[error("I/O error ({1}): {0}")]
    IOError(std::io::ErrorKind, &'static str),
    #[cfg(target_family = "unix")]
    #[error("I/O error: {0}")]
    RustixIOError(#[from] rustix::io::Errno),
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    #[error("I/O error: {0}")]
    // TODO: if needed create an enum for IO Uring errors so that we don't have to pass strings around
    UringIOError(&'static str),
    #[error("Completion was aborted")]
    Aborted,
    #[error("Decryption failed for page={page_idx}")]
    DecryptionError { page_idx: usize },
    #[error("Page codec failed for page={page_idx}")]
    PageCodecError { page_idx: usize },
    #[error("I/O error: partial write")]
    ShortWrite,
    #[error("I/O error: short read on page {page_idx}: expected {expected} bytes, got {actual}")]
    ShortRead {
        page_idx: usize,
        expected: usize,
        actual: usize,
    },
    #[error("I/O error: short read on WAL frame at offset {offset}: expected {expected} bytes, got {actual}")]
    ShortReadWalFrame {
        offset: u64,
        expected: usize,
        actual: usize,
    },
    #[error("WAL frame page mismatch at frame {frame_id}: expected page {expected}, got {actual}")]
    WalFramePageMismatch {
        frame_id: u64,
        expected: usize,
        actual: u32,
    },
    #[error("Checksum mismatch on page {page_id}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        page_id: usize,
        expected: u64,
        actual: u64,
    },
    #[error("tursodb not compiled with checksum feature")]
    ChecksumNotEnabled,
}

/// Convert a `std::io::Error` into a `LimboError` with an operation label.
pub fn io_error(e: std::io::Error, op: &'static str) -> LimboError {
    LimboError::CompletionError(CompletionError::IOError(e.kind(), op))
}

#[cold]
// makes all branches that return errors marked as unlikely
pub(crate) const fn cold_return<T>(v: T) -> T {
    v
}

#[macro_export]
macro_rules! bail_parse_error {
    ($($arg:tt)*) => {
        return $crate::error::cold_return(Err($crate::error::LimboError::ParseError(format!($($arg)*)).into()))
    };
}

#[macro_export]
macro_rules! bail_corrupt_error {
    ($($arg:tt)*) => {
        return $crate::error::cold_return(Err($crate::error::LimboError::Corrupt(format!($($arg)*)).into()))
    };
}

/// Bounds-checked buffer slicing that returns `LimboError::Corrupt` on out-of-bounds.
///
/// Accepts any range expression: `buf, pos..`, `buf, start..end`, etc.
#[macro_export]
macro_rules! slice_in_bounds_or_corrupt {
    ($buf:expr, $range:expr) => {
        $buf.get($range).ok_or_else(|| {
            $crate::error::cold_return($crate::error::LimboError::Corrupt(format!(
                "range {:?} out of bounds for buffer size {}",
                $range,
                $buf.len()
            )))
        })?
    };
}

/// Asserts a condition or bails with `LimboError::Corrupt`.
///
/// Usage:
///   `assert_or_bail_corrupt!(condition, "message {}", arg)`
#[macro_export]
macro_rules! assert_or_bail_corrupt {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            $crate::bail_corrupt_error!($($arg)*);
        }
    };
}

#[macro_export]
macro_rules! bail_constraint_error {
    ($($arg:tt)*) => {
        return $crate::error::cold_return(Err($crate::error::LimboError::Constraint(format!($($arg)*)).into()))
    };
}

impl From<turso_ext::ResultCode> for LimboError {
    fn from(err: turso_ext::ResultCode) -> Self {
        cold_return(LimboError::ExtensionError(err.to_string()))
    }
}

pub const SQLITE_ERROR: usize = 1;
pub const SQLITE_CONSTRAINT: usize = 19;
pub const SQLITE_CONSTRAINT_CHECK: usize = SQLITE_CONSTRAINT | (1 << 8);
pub const SQLITE_CONSTRAINT_PRIMARYKEY: usize = SQLITE_CONSTRAINT | (6 << 8);
#[allow(dead_code)]
pub const SQLITE_CONSTRAINT_FOREIGNKEY: usize = SQLITE_CONSTRAINT | (3 << 8);
pub const SQLITE_CONSTRAINT_NOTNULL: usize = SQLITE_CONSTRAINT | (5 << 8);
pub const SQLITE_CONSTRAINT_TRIGGER: usize = SQLITE_CONSTRAINT | (7 << 8);
pub const SQLITE_FULL: usize = 13; // we want this in autoincrement - incase if user inserts max allowed int
pub const SQLITE_CONSTRAINT_UNIQUE: usize = 2067;
// Standard SQLite error code; kept for documentation and potential
// reuse. The sequence inner-tx wrap used to emit Insn::Halt with this
// code, but halt()'s constraint catch-all mis-wrapped it; Busy is now
// returned directly via Err(LimboError::Busy) from
// op_sequence_commit_inner_tx.
#[allow(dead_code)]
pub const SQLITE_BUSY: usize = 5;
