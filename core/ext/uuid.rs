use super::ExtFunc;
use crate::{
    types::{LimboText, OwnedValue},
    Database, LimboError,
};
use std::rc::Rc;
use uuid::{ContextV7, Timestamp, Uuid};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UuidFunc {
    Uuid4Str,
    Uuid7,
    Uuid7TS,
    UuidStr,
    UuidBlob,
}

impl UuidFunc {
    pub fn resolve_function(name: &str, num_args: usize) -> Option<ExtFunc> {
        match name {
            "uuid4_str" => Some(ExtFunc::Uuid(Self::Uuid4Str)),
            "uuid7" if num_args < 2 => Some(ExtFunc::Uuid(Self::Uuid7)),
            "uuid_str" if num_args == 1 => Some(ExtFunc::Uuid(Self::UuidStr)),
            "uuid_blob" if num_args == 1 => Some(ExtFunc::Uuid(Self::UuidBlob)),
            "uuid7_timestamp_ms" if num_args == 1 => Some(ExtFunc::Uuid(Self::Uuid7TS)),
            // postgres_compatability
            "gen_random_uuid" => Some(ExtFunc::Uuid(Self::Uuid4Str)),
            _ => None,
        }
    }
}

impl std::fmt::Display for UuidFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uuid4Str => write!(f, "uuid4_str"),
            Self::Uuid7 => write!(f, "uuid7"),
            Self::Uuid7TS => write!(f, "uuid7_timestamp_ms"),
            Self::UuidStr => write!(f, "uuid_str"),
            Self::UuidBlob => write!(f, "uuid_blob"),
        }
    }
}

pub fn exec_uuid(var: &UuidFunc, sec: Option<&OwnedValue>) -> crate::Result<OwnedValue> {
    match var {
        UuidFunc::Uuid4Str => Ok(OwnedValue::Text(LimboText::new(Rc::new(
            Uuid::new_v4().to_string(),
        )))),
        UuidFunc::Uuid7 => {
            let uuid = match sec {
                Some(OwnedValue::Integer(ref seconds)) => {
                    let ctx = ContextV7::new();
                    if *seconds < 0 {
                        // not valid unix timestamp, error or null?
                        return Ok(OwnedValue::Null);
                    }
                    Uuid::new_v7(Timestamp::from_unix(ctx, *seconds as u64, 0))
                }
                _ => Uuid::now_v7(),
            };
            Ok(OwnedValue::Blob(Rc::new(uuid.into_bytes().to_vec())))
        }
        _ => unreachable!(),
    }
}

pub fn exec_uuid4() -> crate::Result<OwnedValue> {
    Ok(OwnedValue::Blob(Rc::new(
        Uuid::new_v4().into_bytes().to_vec(),
    )))
}

pub fn exec_uuidstr(reg: &OwnedValue) -> crate::Result<OwnedValue> {
    match reg {
        OwnedValue::Blob(blob) => {
            let uuid = Uuid::from_slice(blob).map_err(|e| LimboError::ParseError(e.to_string()))?;
            Ok(OwnedValue::Text(LimboText::new(Rc::new(uuid.to_string()))))
        }
        OwnedValue::Text(ref val) => {
            let uuid =
                Uuid::parse_str(&val.value).map_err(|e| LimboError::ParseError(e.to_string()))?;
            Ok(OwnedValue::Text(LimboText::new(Rc::new(uuid.to_string()))))
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Err(LimboError::ParseError(
            "Invalid argument type for UUID function".to_string(),
        )),
    }
}

pub fn exec_uuidblob(reg: &OwnedValue) -> crate::Result<OwnedValue> {
    match reg {
        OwnedValue::Text(val) => {
            let uuid =
                Uuid::parse_str(&val.value).map_err(|e| LimboError::ParseError(e.to_string()))?;
            Ok(OwnedValue::Blob(Rc::new(uuid.as_bytes().to_vec())))
        }
        OwnedValue::Blob(blob) => {
            let uuid = Uuid::from_slice(blob).map_err(|e| LimboError::ParseError(e.to_string()))?;
            Ok(OwnedValue::Blob(Rc::new(uuid.as_bytes().to_vec())))
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Err(LimboError::ParseError(
            "Invalid argument type for UUID function".to_string(),
        )),
    }
}

pub fn exec_ts_from_uuid7(reg: &OwnedValue) -> OwnedValue {
    let uuid = match reg {
        OwnedValue::Blob(blob) => {
            Uuid::from_slice(blob).map_err(|e| LimboError::ParseError(e.to_string()))
        }
        OwnedValue::Text(val) => {
            Uuid::parse_str(&val.value).map_err(|e| LimboError::ParseError(e.to_string()))
        }
        _ => Err(LimboError::ParseError(
            "Invalid argument type for UUID function".to_string(),
        )),
    };
    match uuid {
        Ok(uuid) => OwnedValue::Integer(uuid_to_unix(uuid.as_bytes()) as i64),
        // display error? sqlean seems to set value to null
        Err(_) => OwnedValue::Null,
    }
}

#[inline(always)]
fn uuid_to_unix(uuid: &[u8; 16]) -> u64 {
    ((uuid[0] as u64) << 40)
        | ((uuid[1] as u64) << 32)
        | ((uuid[2] as u64) << 24)
        | ((uuid[3] as u64) << 16)
        | ((uuid[4] as u64) << 8)
        | (uuid[5] as u64)
}

pub fn init(db: &mut Database) {
    db.define_scalar_function("uuid4", |_args| exec_uuid4());
}

#[cfg(test)]
#[cfg(feature = "uuid")]
pub mod test {
    use super::UuidFunc;
    use crate::types::OwnedValue;
    #[test]
    fn test_exec_uuid_v4blob() {
        use super::exec_uuid4;
        use uuid::Uuid;
        let owned_val = exec_uuid4();
        match owned_val {
            Ok(OwnedValue::Blob(blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(&blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 4);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v4str() {
        use super::{exec_uuid, UuidFunc};
        use uuid::Uuid;
        let func = UuidFunc::Uuid4Str;
        let owned_val = exec_uuid(&func, None);
        match owned_val {
            Ok(OwnedValue::Text(v4str)) => {
                assert_eq!(v4str.value.len(), 36);
                let uuid = Uuid::parse_str(&v4str.value);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 4);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v7_now() {
        use super::{exec_uuid, UuidFunc};
        use uuid::Uuid;
        let func = UuidFunc::Uuid7;
        let owned_val = exec_uuid(&func, None);
        match owned_val {
            Ok(OwnedValue::Blob(blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(&blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 7);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v7_with_input() {
        use super::{exec_uuid, UuidFunc};
        use uuid::Uuid;
        let func = UuidFunc::Uuid7;
        let owned_val = exec_uuid(&func, Some(&OwnedValue::Integer(946702800)));
        match owned_val {
            Ok(OwnedValue::Blob(blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(&blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 7);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v7_now_to_timestamp() {
        use super::{exec_ts_from_uuid7, exec_uuid, UuidFunc};
        use uuid::Uuid;
        let func = UuidFunc::Uuid7;
        let owned_val = exec_uuid(&func, None);
        match owned_val {
            Ok(OwnedValue::Blob(ref blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 7);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
        let result = exec_ts_from_uuid7(&owned_val.expect("uuid7"));
        if let OwnedValue::Integer(ref ts) = result {
            let unixnow = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                * 1000) as i64;
            assert!(*ts >= unixnow - 1000);
        }
    }

    #[test]
    fn test_exec_uuid_v7_to_timestamp() {
        use super::{exec_ts_from_uuid7, exec_uuid, UuidFunc};
        use uuid::Uuid;
        let func = UuidFunc::Uuid7;
        let owned_val = exec_uuid(&func, Some(&OwnedValue::Integer(946702800)));
        match owned_val {
            Ok(OwnedValue::Blob(ref blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 7);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
        let result = exec_ts_from_uuid7(&owned_val.expect("uuid7"));
        assert_eq!(result, OwnedValue::Integer(946702800 * 1000));
        if let OwnedValue::Integer(ts) = result {
            let time = chrono::DateTime::from_timestamp(ts / 1000, 0);
            assert_eq!(
                time.unwrap(),
                "2000-01-01T05:00:00Z"
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_exec_uuid_v4_str_to_blob() {
        use super::{exec_uuid, exec_uuidblob, UuidFunc};
        use uuid::Uuid;
        let owned_val = exec_uuidblob(
            &exec_uuid(&UuidFunc::Uuid4Str, None).expect("uuid v4 string to generate"),
        );
        match owned_val {
            Ok(OwnedValue::Blob(blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(&blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 4);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v7_str_to_blob() {
        use super::{exec_uuid, exec_uuidblob, exec_uuidstr, UuidFunc};
        use uuid::Uuid;
        // convert a v7 blob to a string then back to a blob
        let owned_val = exec_uuidblob(
            &exec_uuidstr(&exec_uuid(&UuidFunc::Uuid7, None).expect("uuid v7 blob to generate"))
                .expect("uuid v7 string to generate"),
        );
        match owned_val {
            Ok(OwnedValue::Blob(blob)) => {
                assert_eq!(blob.len(), 16);
                let uuid = Uuid::from_slice(&blob);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 7);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v4_blob_to_str() {
        use super::{exec_uuid4, exec_uuidstr};
        use uuid::Uuid;
        // convert a v4 blob to a string
        let owned_val = exec_uuidstr(&exec_uuid4().expect("uuid v7 blob to generate"));
        match owned_val {
            Ok(OwnedValue::Text(v4str)) => {
                assert_eq!(v4str.value.len(), 36);
                let uuid = Uuid::parse_str(&v4str.value);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 4);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }

    #[test]
    fn test_exec_uuid_v7_blob_to_str() {
        use super::{exec_uuid, exec_uuidstr};
        use uuid::Uuid;
        // convert a v7 blob to a string
        let owned_val = exec_uuidstr(
            &exec_uuid(&UuidFunc::Uuid7, Some(&OwnedValue::Integer(123456789)))
                .expect("uuid v7 blob to generate"),
        );
        match owned_val {
            Ok(OwnedValue::Text(v7str)) => {
                assert_eq!(v7str.value.len(), 36);
                let uuid = Uuid::parse_str(&v7str.value);
                assert!(uuid.is_ok());
                assert_eq!(uuid.unwrap().get_version_num(), 7);
            }
            _ => panic!("exec_uuid did not return a Blob variant"),
        }
    }
}
