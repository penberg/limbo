use limbo_ext::{register_extension, scalar, Value, ValueType};

register_extension! {
    scalars: {uuid4_str, uuid4_blob, uuid7_str, uuid7, uuid7_ts, uuid_str, uuid_blob },
}

#[scalar(name = "uuid4_str", alias = "gen_random_uuid")]
fn uuid4_str(_args: &[Value]) -> Value {
    let uuid = uuid::Uuid::new_v4().to_string();
    Value::from_text(uuid)
}

#[scalar(name = "uuid4")]
fn uuid4_blob(_args: &[Value]) -> Value {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    Value::from_blob(bytes.to_vec())
}

#[scalar(name = "uuid7_str")]
fn uuid7_str(args: &[Value]) -> Value {
    let timestamp = if args.is_empty() {
        let ctx = uuid::ContextV7::new();
        uuid::Timestamp::now(ctx)
    } else {
        match args[0].value_type() {
            ValueType::Integer => {
                let ctx = uuid::ContextV7::new();
                let Some(int) = args[0].to_integer() else {
                    return Value::null();
                };
                uuid::Timestamp::from_unix(ctx, int as u64, 0)
            }
            ValueType::Text => {
                let Some(text) = args[0].to_text() else {
                    return Value::null();
                };
                match text.parse::<i64>() {
                    Ok(unix) => {
                        if unix <= 0 {
                            return Value::null();
                        }
                        uuid::Timestamp::from_unix(uuid::ContextV7::new(), unix as u64, 0)
                    }
                    Err(_) => return Value::null(),
                }
            }
            _ => return Value::null(),
        }
    };
    let uuid = uuid::Uuid::new_v7(timestamp);
    Value::from_text(uuid.to_string())
}

#[scalar(name = "uuid7")]
fn uuid7(&self, args: &[Value]) -> Value {
    let timestamp = if args.is_empty() {
        let ctx = uuid::ContextV7::new();
        uuid::Timestamp::now(ctx)
    } else {
        match args[0].value_type() {
            ValueType::Integer => {
                let ctx = uuid::ContextV7::new();
                let Some(int) = args[0].to_integer() else {
                    return Value::null();
                };
                uuid::Timestamp::from_unix(ctx, int as u64, 0)
            }
            _ => return Value::null(),
        }
    };
    let uuid = uuid::Uuid::new_v7(timestamp);
    let bytes = uuid.as_bytes();
    Value::from_blob(bytes.to_vec())
}

#[scalar(name = "uuid7_timestamp_ms")]
fn uuid7_ts(args: &[Value]) -> Value {
    match args[0].value_type() {
        ValueType::Blob => {
            let Some(blob) = &args[0].to_blob() else {
                return Value::null();
            };
            let uuid = uuid::Uuid::from_slice(blob.as_slice()).unwrap();
            let unix = uuid_to_unix(uuid.as_bytes());
            Value::from_integer(unix as i64)
        }
        ValueType::Text => {
            let Some(text) = args[0].to_text() else {
                return Value::null();
            };
            let Ok(uuid) = uuid::Uuid::parse_str(&text) else {
                return Value::null();
            };
            let unix = uuid_to_unix(uuid.as_bytes());
            Value::from_integer(unix as i64)
        }
        _ => Value::null(),
    }
}

#[scalar(name = "uuid_str")]
fn uuid_str(args: &[Value]) -> Value {
    let Some(blob) = args[0].to_blob() else {
        return Value::null();
    };
    let parsed = uuid::Uuid::from_slice(blob.as_slice())
        .ok()
        .map(|u| u.to_string());
    match parsed {
        Some(s) => Value::from_text(s),
        None => Value::null(),
    }
}

#[scalar(name = "uuid_blob")]
fn uuid_blob(&self, args: &[Value]) -> Value {
    let Some(text) = args[0].to_text() else {
        return Value::null();
    };
    match uuid::Uuid::parse_str(&text) {
        Ok(uuid) => Value::from_blob(uuid.as_bytes().to_vec()),
        Err(_) => Value::null(),
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
