// bindings/java/src/macros.rs
#[macro_export]
macro_rules! eprint_return {
    ($log:expr, $error:expr) => {{
        eprintln!("{}", $log);
        Err($error)
    }};
}

#[macro_export]
macro_rules! eprint_return_null {
    ($log:expr, $error:expr) => {{
        eprintln!("{}", $log);
        JObject::null()
    }};
}
