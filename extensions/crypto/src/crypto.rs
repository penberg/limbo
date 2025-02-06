use crate::Error;
use blake3::Hasher;
use data_encoding::{BASE32, BASE64, HEXLOWER};
use limbo_ext::{Value, ValueType};
use ring::digest::{self, digest};
use std::{borrow::Cow, error::Error as StdError};

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
            "base85" => Ok(Value::from_text(encode_ascii85(data.as_bytes().as_ref()))),
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

pub fn decode(data: &Value, format: &Value) -> Result<Value, Error> {
    match (data.value_type(), format.value_type()) {
        (ValueType::Error, _) | (ValueType::Null, _) => Err(Error::InvalidType),
        (ValueType::Text, ValueType::Text) => {
            let format_str = format.to_text().ok_or(Error::InvalidType)?.to_lowercase();
            let input_text = data.to_text().ok_or(Error::InvalidType)?;

            match format_str.as_str() {
                "base32" => {
                    let payload = BASE32
                        .decode(input_text.as_bytes())
                        .map_err(|_| Error::DecodeFailed)?;
                    Ok(Value::from_text(
                        String::from_utf8(payload).map_err(|_| Error::InvalidUtf8)?,
                    ))
                }
                "base64" => {
                    let payload = BASE64
                        .decode(input_text.as_bytes())
                        .map_err(|_| Error::DecodeFailed)?;
                    Ok(Value::from_text(
                        String::from_utf8(payload).map_err(|_| Error::InvalidUtf8)?,
                    ))
                }
                "hex" => {
                    let payload = HEXLOWER
                        .decode(input_text.to_lowercase().as_bytes())
                        .map_err(|_| Error::DecodeFailed)?;
                    Ok(Value::from_text(
                        String::from_utf8(payload).map_err(|_| Error::InvalidUtf8)?,
                    ))
                }
                "base85" => {
                    let decoded = decode_ascii85(&input_text).map_err(|_| Error::DecodeFailed)?;

                    Ok(Value::from_text(
                        String::from_utf8(decoded).map_err(|_| Error::InvalidUtf8)?,
                    ))
                }
                "url" => {
                    let decoded = urlencoding::decode_binary(input_text.as_bytes());
                    Ok(Value::from_text(
                        String::from_utf8(decoded.to_vec()).map_err(|_| Error::InvalidUtf8)?,
                    ))
                }
                _ => Err(Error::UnknownOperation),
            }
        }
        _ => Err(Error::InvalidType),
    }
}

// Ascii85 functions to avoid +1 dependency and to remove '~>' '<~'

const TABLE: [u32; 5] = [85 * 85 * 85 * 85, 85 * 85 * 85, 85 * 85, 85, 1];

fn decode_ascii85(input: &str) -> Result<Vec<u8>, Box<dyn StdError>> {
    let mut result = Vec::with_capacity(4 * (input.len() / 5 + 16));

    let mut counter = 0;
    let mut chunk = 0;

    for digit in input.trim().bytes().filter(|c| !c.is_ascii_whitespace()) {
        if digit == b'z' {
            if counter == 0 {
                result.extend_from_slice(&[0, 0, 0, 0]);
            } else {
                return Err("Missaligned z in input".into());
            }
        }

        if digit < 33 || digit > 117 {
            return Err("Input char is out of range for Ascii85".into());
        }

        decode_digit(digit, &mut counter, &mut chunk, &mut result);
    }

    let mut to_remove = 0;

    while counter != 0 {
        decode_digit(b'u', &mut counter, &mut chunk, &mut result);
        to_remove += 1;
    }

    result.drain((result.len() - to_remove)..result.len());

    Ok(result)
}

fn decode_digit(digit: u8, counter: &mut usize, chunk: &mut u32, result: &mut Vec<u8>) {
    let byte = digit - 33;

    *chunk += byte as u32 * TABLE[*counter];

    if *counter == 4 {
        result.extend_from_slice(&chunk.to_be_bytes());
        *chunk = 0;
        *counter = 0;
    } else {
        *counter += 1;
    }
}

fn encode_ascii85(input: &[u8]) -> String {
    let mut result = String::with_capacity(5 * (input.len() / 4 + 16));

    for chunk in input.chunks(4) {
        let (chunk, count) = if chunk.len() == 4 {
            (Cow::from(chunk), 5)
        } else {
            let mut new_chunk = Vec::new();
            new_chunk.resize_with(4, || 0);
            new_chunk[..chunk.len()].copy_from_slice(chunk);
            (Cow::from(new_chunk), 5 - (4 - chunk.len()))
        };

        let number = u32::from_be_bytes(chunk.as_ref().try_into().expect("Internal Error"));

        for i in 0..count {
            let digit = (((number / TABLE[i]) % 85) + 33) as u8;
            result.push(digit as char);
        }
    }

    result
}
