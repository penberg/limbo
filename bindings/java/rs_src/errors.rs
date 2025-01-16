use jni::errors::{Error, JniError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LimboError {
    #[error("Custom error: `{0}`")]
    CustomError(String),

    #[error("Invalid database pointer")]
    InvalidDatabasePointer,

    #[error("Invalid connection pointer")]
    InvalidConnectionPointer,

    #[error("JNI Errors: `{0}`")]
    JNIErrors(Error)
}

impl From<limbo_core::LimboError> for LimboError {
    fn from(value: limbo_core::LimboError) -> Self {
        todo!()
    }
}

impl From<LimboError> for JniError {
    fn from(value: LimboError) -> Self {
        match value {
            LimboError::CustomError(_)
            | LimboError::InvalidDatabasePointer
            | LimboError::InvalidConnectionPointer
            | LimboError::JNIErrors(_) => {
                eprintln!("Error occurred: {:?}", value);
                JniError::Other(-1)
            }
        }
    }
}

impl From<jni::errors::Error> for LimboError {
    fn from(value: jni::errors::Error) -> Self {
        LimboError::JNIErrors(value)
    }
}

pub type Result<T> = std::result::Result<T, LimboError>;

/// This struct defines error codes that correspond to the constants defined in the
/// Java package `org.github.tursodatabase.LimboErrorCode`.
///
/// These error codes are used to handle and represent specific error conditions
/// that may occur within the Rust code and need to be communicated to the Java side.
#[derive(Clone)]
pub struct ErrorCode;

impl ErrorCode {
    // TODO: change CONNECTION_FAILURE_STATEMENT_IS_DML to appropriate error code number
    pub const STATEMENT_IS_DML: i32 = -1;
}

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ERROR: i32 = 1;
pub const SQLITE_INTERNAL: i32 = 2;
pub const SQLITE_PERM: i32 = 3;
pub const SQLITE_ABORT: i32 = 4;
pub const SQLITE_BUSY: i32 = 5;
pub const SQLITE_LOCKED: i32 = 6;
pub const SQLITE_NOMEM: i32 = 7;
pub const SQLITE_READONLY: i32 = 8;
pub const SQLITE_INTERRUPT: i32 = 9;
pub const SQLITE_IOERR: i32 = 10;
pub const SQLITE_CORRUPT: i32 = 11;
pub const SQLITE_NOTFOUND: i32 = 12;
pub const SQLITE_FULL: i32 = 13;
pub const SQLITE_CANTOPEN: i32 = 14;
pub const SQLITE_PROTOCOL: i32 = 15;
pub const SQLITE_EMPTY: i32 = 16;
pub const SQLITE_SCHEMA: i32 = 17;
pub const SQLITE_TOOBIG: i32 = 18;
pub const SQLITE_CONSTRAINT: i32 = 19;
pub const SQLITE_MISMATCH: i32 = 20;
pub const SQLITE_MISUSE: i32 = 21;
pub const SQLITE_NOLFS: i32 = 22;
pub const SQLITE_AUTH: i32 = 23;
pub const SQLITE_ROW: i32 = 100;
pub const SQLITE_DONE: i32 = 101;

// types returned by sqlite3_column_type()
pub const SQLITE_INTEGER: i32 = 1;
pub const SQLITE_FLOAT: i32 = 2;
pub const SQLITE_TEXT: i32 = 3;
pub const SQLITE_BLOB: i32 = 4;
pub const SQLITE_NULL: i32 = 5;

// Limbo custom error codes
pub const LIMBO_DATABASE_ALREADY_CLOSED: i32 = 1000;
pub const LIMBO_ETC: i32 = 9999;
