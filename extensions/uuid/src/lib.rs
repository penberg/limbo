use limbo_extension::{
    declare_scalar_functions, register_extension, register_scalar_functions, Blob, TextValue, Value,
};

register_extension! {
    scalars: {
        "uuid4_str" => uuid4_str,
        "uuid4" => uuid4_blob,
        "uuid_str" => uuid_str,
        "uuid_blob" => uuid_blob,
    },
}

declare_scalar_functions! {
    #[args(min  = 0, max = 0)]
    fn uuid4_str(_args: &[Value]) -> Value {
        let uuid = uuid::Uuid::new_v4().to_string();
        Value::from_text(uuid)
    }

     #[args(min = 0, max = 0)]
    fn uuid4_blob(_args: &[Value]) -> Value {
        let uuid = uuid::Uuid::new_v4();
        let bytes = uuid.as_bytes();
        Value::from_blob(bytes.to_vec())
    }

     #[args(min = 1, max = 1)]
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

     #[args(min = 1, max = 1)]
    fn uuid_blob(args: &[Value]) -> Value {
        if args[0].value_type != limbo_extension::ValueType::Text {
            log::debug!("uuid_blob was passed a non-text arg");
            return Value::null();
        }
        if let Some(text) = TextValue::from_value(&args[0]) {
        match uuid::Uuid::parse_str(unsafe {text.as_str()}) {
            Ok(uuid) => {
                    Value::from_blob(uuid.as_bytes().to_vec())
                }
            Err(_) => Value::null()
        }
        } else {
            Value::null()
        }
    }
}
