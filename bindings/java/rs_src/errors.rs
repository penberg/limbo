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
    JNIErrors(Error),
}

impl From<limbo_core::LimboError> for LimboError {
    fn from(_value: limbo_core::LimboError) -> Self {
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

#[allow(dead_code)]
pub const SQLITE_OK: i32 = 0;
#[allow(dead_code)]
pub const SQLITE_ERROR: i32 = 1;
#[allow(dead_code)]
pub const SQLITE_INTERNAL: i32 = 2;
#[allow(dead_code)]
pub const SQLITE_PERM: i32 = 3;
#[allow(dead_code)]
pub const SQLITE_ABORT: i32 = 4;
#[allow(dead_code)]
pub const SQLITE_BUSY: i32 = 5;
#[allow(dead_code)]
pub const SQLITE_LOCKED: i32 = 6;
#[allow(dead_code)]
pub const SQLITE_NOMEM: i32 = 7;
#[allow(dead_code)]
pub const SQLITE_READONLY: i32 = 8;
#[allow(dead_code)]
pub const SQLITE_INTERRUPT: i32 = 9;
#[allow(dead_code)]
pub const SQLITE_IOERR: i32 = 10;
#[allow(dead_code)]
pub const SQLITE_CORRUPT: i32 = 11;
#[allow(dead_code)]
pub const SQLITE_NOTFOUND: i32 = 12;
#[allow(dead_code)]
pub const SQLITE_FULL: i32 = 13;
#[allow(dead_code)]
pub const SQLITE_CANTOPEN: i32 = 14;
#[allow(dead_code)]
pub const SQLITE_PROTOCOL: i32 = 15;
#[allow(dead_code)]
pub const SQLITE_EMPTY: i32 = 16;
#[allow(dead_code)]
pub const SQLITE_SCHEMA: i32 = 17;
#[allow(dead_code)]
pub const SQLITE_TOOBIG: i32 = 18;
#[allow(dead_code)]
pub const SQLITE_CONSTRAINT: i32 = 19;
#[allow(dead_code)]
pub const SQLITE_MISMATCH: i32 = 20;
#[allow(dead_code)]
pub const SQLITE_MISUSE: i32 = 21;
#[allow(dead_code)]
pub const SQLITE_NOLFS: i32 = 22;
#[allow(dead_code)]
pub const SQLITE_AUTH: i32 = 23;
#[allow(dead_code)]
pub const SQLITE_ROW: i32 = 100;
#[allow(dead_code)]
pub const SQLITE_DONE: i32 = 101;

#[allow(dead_code)]
pub const SQLITE_INTEGER: i32 = 1;
#[allow(dead_code)]
pub const SQLITE_FLOAT: i32 = 2;
#[allow(dead_code)]
pub const SQLITE_TEXT: i32 = 3;
#[allow(dead_code)]
pub const SQLITE_BLOB: i32 = 4;
#[allow(dead_code)]
pub const SQLITE_NULL: i32 = 5;

pub const LIMBO_FAILED_TO_PARSE_BYTE_ARRAY: i32 = 1100;
pub const LIMBO_FAILED_TO_PREPARE_STATEMENT: i32 = 1200;
pub const LIMBO_ETC: i32 = 9999;
