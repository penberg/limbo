use limbo_ext::{register_extension, scalar, ResultCode, Value};

mod vector;

use vector::*;

#[derive(Debug)]
enum Error {
    InvalidType,
    InvalidFormat,
    InvalidDimensions,
}

type Result<T> = std::result::Result<T, Error>;

#[scalar(name = "vector32", alias = "vector")]
fn vector32(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }
    let Ok(x) = parse_vector(&args[0], Some(VectorType::Float32)) else {
        return Value::error(ResultCode::Error);
    };
    vector_serialize_f32(x)
}

#[scalar(name = "vector64")]
fn vector64(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }
    let Ok(x) = parse_vector(&args[0], Some(VectorType::Float64)) else {
        return Value::error(ResultCode::Error);
    };
    vector_serialize_f64(x)
}

#[scalar(name = "vector_extract")]
fn vector_extract(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }
    let Some(blob) = args[0].to_blob() else {
        return Value::error(ResultCode::Error);
    };
    if blob.is_empty() {
        return Value::from_text("[]".to_string());
    }
    let Ok(vector_type) = vector_type(&blob) else {
        return Value::error(ResultCode::Error);
    };
    let Ok(vector) = vector_deserialize(vector_type, &blob) else {
        return Value::error(ResultCode::Error);
    };
    Value::from_text(vector_to_text(&vector))
}

#[scalar(name = "vector_distance_cos")]
fn vector_distance_cos(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::Error);
    }
    let Ok(x) = parse_vector(&args[0], None) else {
        return Value::error(ResultCode::Error);
    };
    let Ok(y) = parse_vector(&args[1], None) else {
        return Value::error(ResultCode::Error);
    };
    let Ok(dist) = do_vector_distance_cos(&x, &y) else {
        return Value::error(ResultCode::Error);
    };
    Value::from_float(dist)
}

register_extension! {
    scalars: { vector32, vector64, vector_extract, vector_distance_cos },
}
