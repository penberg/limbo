use limbo_extension::{
    declare_scalar_functions, register_extension, register_scalar_functions, Blob, TextValue,
    Value, ValueType,
};

register_extension! {
    scalars: {
        "uuid4_str" => uuid4_str,
        "uuid4" => uuid4_blob,
        "uuid7_str" => uuid7_str,
        "uuid7" => uuid7_blob,
        "uuid_str" => uuid_str,
        "uuid_blob" => uuid_blob,
        "exec_ts_from_uuid7" => exec_ts_from_uuid7,
    },
}

declare_scalar_functions! {
    #[args(0)]
    fn uuid4_str(_args: &[Value]) -> Value {
        let uuid = uuid::Uuid::new_v4().to_string();
        Value::from_text(uuid)
    }

     #[args(0)]
    fn uuid4_blob(_args: &[Value]) -> Value {
        let uuid = uuid::Uuid::new_v4();
        let bytes = uuid.as_bytes();
        Value::from_blob(bytes.to_vec())
    }

    #[args(0..=1)]
    fn uuid7_str(args: &[Value]) -> Value {
        let timestamp = if args.is_empty() {
            let ctx = uuid::ContextV7::new();
             uuid::Timestamp::now(ctx)
        } else if args[0].value_type == limbo_extension::ValueType::Integer {
                let ctx = uuid::ContextV7::new();
                let int = args[0].value as i64;
                uuid::Timestamp::from_unix(ctx, int as u64, 0)
            } else {
                return Value::null();
        };
        let uuid = uuid::Uuid::new_v7(timestamp);
        Value::from_text(uuid.to_string())
    }

    #[args(0..=1)]
    fn uuid7_blob(args: &[Value]) -> Value {
        let timestamp = if args.is_empty() {
            let ctx = uuid::ContextV7::new();
             uuid::Timestamp::now(ctx)
        } else if args[0].value_type == limbo_extension::ValueType::Integer {
                let ctx = uuid::ContextV7::new();
                let int = args[0].value as i64;
                uuid::Timestamp::from_unix(ctx, int as u64, 0)
            } else {
                return Value::null();
        };
        let uuid = uuid::Uuid::new_v7(timestamp);
        let bytes = uuid.as_bytes();
        Value::from_blob(bytes.to_vec())
    }

    #[args(1)]
    fn exec_ts_from_uuid7(args: &[Value]) -> Value {
        match args[0].value_type {
             ValueType::Blob => {
                let blob = Blob::from_value(&args[0]).unwrap();
                let slice = unsafe{ std::slice::from_raw_parts(blob.data, blob.size as usize)};
                let uuid = uuid::Uuid::from_slice(slice).unwrap();
                let unix = uuid_to_unix(uuid.as_bytes());
                Value::from_integer(unix as i64)
            }
            ValueType::Text => {
                let Some(text) = (unsafe {TextValue::from_value(&args[0])}) else {
                    return Value::null();
                };
                let uuid = uuid::Uuid::parse_str(unsafe {text.as_str()}).unwrap();
                let unix = uuid_to_unix(uuid.as_bytes());
                Value::from_integer(unix as i64)
            }
            _ => Value::null(),
        }
    }

     #[args(1)]
    fn uuid_str(args: &[Value]) -> Value {
        if args[0].value_type != limbo_extension::ValueType::Blob {
            log::debug!("uuid_str was passed a non-blob arg");
            return Value::null();
        }
        if let Some(blob) = Blob::from_value(&args[0]) {
        let slice = unsafe{ std::slice::from_raw_parts(blob.data, blob.size as usize)};
        let parsed = uuid::Uuid::from_slice(slice).ok().map(|u| u.to_string());
        match parsed {
            Some(s) => Value::from_text(s),
            None => Value::null()
        }
        } else {
            Value::null()
        }
    }

     #[args(1)]
    fn uuid_blob(args: &[Value]) -> Value {
        if args[0].value_type != limbo_extension::ValueType::Text {
            log::debug!("uuid_blob was passed a non-text arg");
            return Value::null();
        }
        let Some(text) = (unsafe { TextValue::from_value(&args[0])}) else {
            return Value::null();
        };
        match uuid::Uuid::parse_str(unsafe {text.as_str()}) {
            Ok(uuid) => {
                    Value::from_blob(uuid.as_bytes().to_vec())
                }
            Err(_) => Value::null()
        }
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
