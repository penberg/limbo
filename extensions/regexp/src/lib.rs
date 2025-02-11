use limbo_ext::{register_extension, scalar, Value, ValueType};
use regex::Regex;

register_extension! {
    scalars: { regexp, regexp_like, regexp_substr, regexp_replace }
}

#[scalar(name = "regexp")]
fn regexp(args: &[Value]) -> Value {
    regex(&args[0], &args[1])
}

fn regex(pattern: &Value, haystack: &Value) -> Value {
    match (pattern.value_type(), haystack.value_type()) {
        (ValueType::Text, ValueType::Text) => {
            let Some(pattern) = pattern.to_text() else {
                return Value::null();
            };
            let Some(haystack) = haystack.to_text() else {
                return Value::null();
            };
            let re = match Regex::new(pattern) {
                Ok(re) => re,
                Err(_) => return Value::null(),
            };
            Value::from_integer(re.is_match(haystack) as i64)
        }
        _ => Value::null(),
    }
}

#[scalar(name = "regexp_like")]
fn regexp_like(args: &[Value]) -> Value {
    regex(&args[1], &args[0])
}

#[scalar(name = "regexp_substr")]
fn regexp_substr(&self, args: &[Value]) -> Value {
    match (args[0].value_type(), args[1].value_type()) {
        (ValueType::Text, ValueType::Text) => {
            let Some(haystack) = &args[0].to_text() else {
                return Value::null();
            };
            let Some(pattern) = &args[1].to_text() else {
                return Value::null();
            };
            let re = match Regex::new(pattern) {
                Ok(re) => re,
                Err(_) => return Value::null(),
            };
            match re.find(haystack) {
                Some(mat) => Value::from_text(mat.as_str().to_string()),
                None => Value::null(),
            }
        }
        _ => Value::null(),
    }
}

#[scalar(name = "regexp_replace")]
fn regexp_replace(&self, args: &[Value]) -> Value {
    let replacement = match args.get(2) {
        Some(repl) => repl.to_text().unwrap_or_default(),
        None => "", // If args[2] does not exist, use an empty string
    };

    match (args.get(0), args.get(1)) {
        (Some(haystack), Some(pattern)) => {
            let Some(haystack_text) = haystack.to_text() else {
                return Value::from_text("".to_string()); // Return an empty string if haystack is not valid
            };
            let Some(pattern_text) = pattern.to_text() else {
                return Value::from_text("".to_string()); // Return an empty string if pattern is not valid
            };

            let re = match Regex::new(&pattern_text) {
                Ok(re) => re,
                Err(_) => return Value::from_text("".to_string()), // Return an empty string if regex compilation fails
            };
            Value::from_text(re.replace(&haystack_text, replacement).to_string())
        }
        _ => Value::from_text("".to_string()), // Return an empty string for invalid value types
    }
}
