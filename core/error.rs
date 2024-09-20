use thiserror::Error;

#[derive(Debug, Error)]
pub enum LimboError {
    #[error("Corrupt database: {0}")]
    Corrupt(String),
    #[error("File is not a database")]
    NotADB,
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Parse error: {0}")]
    LexerError(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("Conversion error: {0}")]
    ConversionError(String),
    #[error("Env variable error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[cfg(target_os = "linux")]
    #[error("I/O error: {0}")]
    LinuxIOError(String),
    #[error("Locking error: {0}")]
    LockingError(String),
    #[cfg(target_os = "macos")]
    #[error("I/O error: {0}")]
    RustixIOError(#[from] rustix::io::Errno),
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
    #[error("Runtime error: {0}")]
    Constraint(String),
}

#[macro_export]
macro_rules! bail_parse_error {
    ($($arg:tt)*) => {
        return Err($crate::error::LimboError::ParseError(format!($($arg)*)))
    };
}

#[macro_export]
macro_rules! bail_corrupt_error {
    ($($arg:tt)*) => {
        return Err($crate::error::LimboError::Corrupt(format!($($arg)*)))
    };
}

pub const SQLITE_CONSTRAINT: usize = 19;
pub const SQLITE_CONSTRAINT_PRIMARYKEY: usize = SQLITE_CONSTRAINT | (6 << 8);
