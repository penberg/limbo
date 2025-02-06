use crypto::{blake3, decode, encode, md5, sha1, sha256, sha384, sha512};
use limbo_ext::{register_extension, scalar, ResultCode, Value};

mod crypto;

#[derive(Debug)]
enum Error {
    InvalidType,
    UnknownOperation,
    DecodeFailed,
    InvalidUtf8,
}

#[scalar(name = "crypto_sha256", alias = "crypto_sha256")]
fn crypto_sha256(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }

    let Ok(hash) = sha256(&args[0]) else {
        return Value::error(ResultCode::Error);
    };

    Value::from_blob(hash)
}

#[scalar(name = "crypto_sha512", alias = "crypto_sha512")]
fn crypto_sha512(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }

    let Ok(hash) = sha512(&args[0]) else {
        return Value::error(ResultCode::Error);
    };

    Value::from_blob(hash)
}

#[scalar(name = "crypto_sha384", alias = "crypto_sha384")]
fn crypto_sha384(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }

    let Ok(hash) = sha384(&args[0]) else {
        return Value::error(ResultCode::Error);
    };

    Value::from_blob(hash)
}

#[scalar(name = "crypto_blake3", alias = "crypto_blake3")]
fn crypto_blake3(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }

    let Ok(hash) = blake3(&args[0]) else {
        return Value::error(ResultCode::Error);
    };

    Value::from_blob(hash)
}

#[scalar(name = "crypto_sha1", alias = "crypto_sha1")]
fn crypto_sha1(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }

    let Ok(hash) = sha1(&args[0]) else {
        return Value::error(ResultCode::Error);
    };

    Value::from_blob(hash)
}

#[scalar(name = "crypto_md5", alias = "crypto_md5")]
fn crypto_md5(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::error(ResultCode::Error);
    }

    let Ok(hash) = md5(&args[0]) else {
        return Value::error(ResultCode::Error);
    };

    Value::from_blob(hash)
}

#[scalar(name = "crypto_encode", alias = "crypto_encode")]
fn crypto_encode(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::Error);
    }

    let Ok(payload) = encode(&args[0], &args[1]) else {
        return Value::error(ResultCode::Error);
    };

    payload
}

#[scalar(name = "crypto_decode", alias = "crypto_decode")]
fn crypto_decode(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::error(ResultCode::Error);
    }

    let Ok(payload) = decode(&args[0], &args[1]) else {
        return Value::error(ResultCode::Error);
    };

    payload
}

register_extension! {
    scalars: { crypto_sha256, crypto_sha512, crypto_sha384, crypto_blake3, crypto_sha1, crypto_md5, crypto_encode, crypto_decode },
}
