use limbo_extension::{
    declare_scalar_functions, register_extension, register_scalar_functions, Value,
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
        Value::from_blob(bytes)
    }

     #[args(min = 1, max = 1)]
    fn uuid_str(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::null();
        }
        if args[0].value_type != limbo_extension::ValueType::Blob {
            return Value::null();
        }
        let data_ptr = args[0].blob.data;
        let size = args[0].blob.size;
        if data_ptr.is_null() || size != 16 {
            return Value::null();
        }
        let slice = unsafe{ std::slice::from_raw_parts(data_ptr, size)};
        let parsed = uuid::Uuid::from_slice(slice).ok().map(|u| u.to_string());
        match parsed {
            Some(s) => Value::from_text(s),
            None => Value::null()
        }
    }

     #[args(min = 1, max = 1)]
    fn uuid_blob(args: &[Value]) -> Value {
        if args.len() != 1 {
            return Value::null();
        }
        if args[0].value_type != limbo_extension::ValueType::Text {
            return Value::null();
        }
        let text = args[0].text.to_string();
        match uuid::Uuid::parse_str(&text) {
            Ok(uuid) => Value::from_blob(uuid.as_bytes()),
            Err(_) => Value::null()
        }
    }
}
