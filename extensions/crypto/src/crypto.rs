use crate::Error;
use blake3::Hasher;
use limbo_ext::{Value, ValueType};
use ring::digest::{self, digest};

pub fn sha256(data: &Value) -> Result<Vec<u8>, Error> {
    match data.value_type() {
        ValueType::Error | ValueType::Null => Err(Error::InvalidType),
        _ => {
            let hash = digest(&digest::SHA256, &data.as_bytes());
            Ok(hash.as_ref().to_vec())
        }
    }
}

pub fn sha512(data: &Value) -> Result<Vec<u8>, Error> {
    match data.value_type() {
        ValueType::Error | ValueType::Null => Err(Error::InvalidType),
        _ => {
            let hash = digest(&digest::SHA512, &data.as_bytes());
            Ok(hash.as_ref().to_vec())
        }
    }
}

pub fn sha384(data: &Value) -> Result<Vec<u8>, Error> {
    match data.value_type() {
        ValueType::Error | ValueType::Null => Err(Error::InvalidType),
        _ => {
            let hash = digest(&digest::SHA384, &data.as_bytes());
            Ok(hash.as_ref().to_vec())
        }
    }
}

pub fn blake3(data: &Value) -> Result<Vec<u8>, Error> {
    match data.value_type() {
        ValueType::Error | ValueType::Null => Err(Error::InvalidType),
        _ => {
            let mut hasher = Hasher::new();
            hasher.update(data.as_bytes().as_ref());
            Ok(hasher.finalize().as_bytes().to_vec())
        }
    }
}

pub fn sha1(data: &Value) -> Result<Vec<u8>, Error> {
    match data.value_type() {
        ValueType::Error | ValueType::Null => Err(Error::InvalidType),
        _ => {
            let hash = digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, &data.as_bytes());
            Ok(hash.as_ref().to_vec())
        }
    }
}
