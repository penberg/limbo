use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum DatabaseError {
    #[error("no such transaction ID: `{0}`")]
    NoSuchTransactionID(u64),
    #[error("transaction aborted because of a write-write conflict")]
    WriteWriteConflict,
    #[error("transaction is terminated")]
    TxTerminated,
    #[error("I/O error: {0}")]
    Io(String),
}
