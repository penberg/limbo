use limbo_ext::{register_extension, ArgSpec, Scalar, ScalarDerive, Value, ValueType};
use regex::Regex;

register_extension! {
    scalars: { Regexp, RegexpLike, RegexpSubstr }
}

#[derive(ScalarDerive)]
struct Regexp;

impl Scalar for Regexp {
    fn name(&self) -> &'static str {
        "regexp"
    }
    fn args(&self) -> ArgSpec {
        ArgSpec::Exact(2)
    }
    fn call(&self, args: &[Value]) -> Value {
        regex(&args[0], &args[1])
    }
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
            let re = match Regex::new(&pattern) {
                Ok(re) => re,
                Err(_) => return Value::null(),
            };
            Value::from_integer(re.is_match(&haystack) as i64)
        }
        _ => Value::null(),
    }
}

#[derive(ScalarDerive)]
struct RegexpLike;

impl Scalar for RegexpLike {
    fn name(&self) -> &'static str {
        "regexp_like"
    }
    fn args(&self) -> ArgSpec {
        ArgSpec::Exact(2)
    }
    fn call(&self, args: &[Value]) -> Value {
        regex(&args[1], &args[0])
    }
}

#[derive(ScalarDerive)]
struct RegexpSubstr;

impl Scalar for RegexpSubstr {
    fn args(&self) -> ArgSpec {
        ArgSpec::Exact(2)
    }
    fn call(&self, args: &[Value]) -> Value {
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
    fn name(&self) -> &'static str {
        "regexp_substr"
    }
}
