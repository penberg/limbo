use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("no such transaction ID: `{0}`")]
    NoSuchTransactionID(u64),
}
