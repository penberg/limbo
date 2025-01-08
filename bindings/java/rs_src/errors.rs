use jni::errors::{Error, JniError};

#[derive(Debug, Clone)]
pub struct CustomError {
    pub message: String,
}

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

impl From<jni::errors::Error> for CustomError {
    fn from(value: Error) -> Self {
        CustomError {
            message: value.to_string(),
        }
    }
}

impl From<CustomError> for JniError {
    fn from(value: CustomError) -> Self {
        eprintln!("Error occurred: {:?}", value.message);
        JniError::Other(-1)
    }
}
