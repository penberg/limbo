use crate::Error;
use blake3::Hasher;
use data_encoding::{BASE32, BASE64, HEXLOWER};
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

pub fn md5(data: &Value) -> Result<Vec<u8>, Error> {
    match data.value_type() {
        ValueType::Error | ValueType::Null => Err(Error::InvalidType),
        _ => {
            let digest = md5::compute::<&Vec<u8>>(data.as_bytes().as_ref());

            Ok(digest.as_ref().to_vec())
        }
    }
}

pub fn encode(data: &Value, format: &Value) -> Result<Value, Error> {
    match (data.value_type(), format.value_type()) {
        (ValueType::Error, _) | (ValueType::Null, _) => Err(Error::InvalidType),
        (_, ValueType::Text) => match format.to_text().unwrap().to_lowercase().as_str() {
            "base32" => Ok(Value::from_text(BASE32.encode(data.as_bytes().as_ref()))),
            "base64" => Ok(Value::from_text(BASE64.encode(data.as_bytes().as_ref()))),
            "hex" => Ok(Value::from_text(HEXLOWER.encode(data.as_bytes().as_ref()))),
            "base85" => {
                let result = ascii85::encode(data.as_bytes().as_ref())
                    .replace("<~", "")
                    .replace("~>", "");
                Ok(Value::from_text(result))
            }
            "url" => {
                let data = data.as_bytes();
                let url = urlencoding::encode_binary(&data);
                Ok(Value::from_text(url.to_string()))
            }
            _ => Err(Error::UnknownOperation),
        },
        _ => Err(Error::InvalidType),
    }
}
