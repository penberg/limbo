mod de;
mod error;
mod json_operations;
mod json_path;
mod ser;

use std::rc::Rc;

pub use crate::json::de::from_str;
use crate::json::de::ordered_object;
use crate::json::error::Error as JsonError;
pub use crate::json::json_operations::json_patch;
use crate::json::json_path::{json_path, JsonPath, PathElement};
pub use crate::json::ser::to_string;
use crate::types::{LimboText, OwnedValue, TextSubtype};
use indexmap::IndexMap;
use jsonb::Error as JsonbError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum Val {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<Val>),
    Removed,
    #[serde(with = "ordered_object")]
    Object(Vec<(String, Val)>),
}

pub fn get_json(json_value: &OwnedValue) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => {
            // optimization: once we know the subtype is a valid JSON, we do not have
            // to go through parsing JSON and serializing it back to string
            if t.subtype == TextSubtype::Json {
                return Ok(json_value.to_owned());
            }

            let json_val = get_json_value(json_value)?;
            let json = to_string(&json_val).unwrap();

            Ok(OwnedValue::Text(LimboText::json(Rc::new(json))))
        }
        OwnedValue::Blob(b) => {
            // TODO: use get_json_value after we implement a single Struct
            //   to represent both JSON and JSONB
            if let Ok(json) = jsonb::from_slice(b) {
                Ok(OwnedValue::Text(LimboText::json(Rc::new(json.to_string()))))
            } else {
                crate::bail_parse_error!("malformed JSON");
            }
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => {
            let json_val = get_json_value(json_value)?;
            let json = to_string(&json_val).unwrap();

            Ok(OwnedValue::Text(LimboText::json(Rc::new(json))))
        }
    }
}

fn get_json_value(json_value: &OwnedValue) -> crate::Result<Val> {
    match json_value {
        OwnedValue::Text(ref t) => match from_str::<Val>(&t.value) {
            Ok(json) => Ok(json),
            Err(_) => {
                crate::bail_parse_error!("malformed JSON")
            }
        },
        OwnedValue::Blob(b) => {
            if let Ok(_json) = jsonb::from_slice(b) {
                todo!("jsonb to json conversion");
            } else {
                crate::bail_parse_error!("malformed JSON");
            }
        }
        OwnedValue::Null => Ok(Val::Null),
        OwnedValue::Float(f) => Ok(Val::Float(*f)),
        OwnedValue::Integer(i) => Ok(Val::Integer(*i)),
        _ => Ok(Val::String(json_value.to_string())),
    }
}

pub fn json_array(values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    let mut s = String::new();
    s.push('[');

    // TODO: use `convert_db_type_to_json` and map each value with that function,
    // so we can construct a `Val::Array` with each value and then serialize it directly.
    for (idx, value) in values.iter().enumerate() {
        match value {
            OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
            OwnedValue::Text(t) => {
                if t.subtype == TextSubtype::Json {
                    s.push_str(&t.value);
                } else {
                    match to_string(&*t.value) {
                        Ok(json) => s.push_str(&json),
                        Err(_) => crate::bail_parse_error!("malformed JSON"),
                    }
                }
            }
            OwnedValue::Integer(i) => match to_string(&i) {
                Ok(json) => s.push_str(&json),
                Err(_) => crate::bail_parse_error!("malformed JSON"),
            },
            OwnedValue::Float(f) => match to_string(&f) {
                Ok(json) => s.push_str(&json),
                Err(_) => crate::bail_parse_error!("malformed JSON"),
            },
            OwnedValue::Null => s.push_str("null"),
            _ => unreachable!(),
        }

        if idx < values.len() - 1 {
            s.push(',');
        }
    }

    s.push(']');
    Ok(OwnedValue::Text(LimboText::json(Rc::new(s))))
}

pub fn json_array_length(
    json_value: &OwnedValue,
    json_path: Option<&OwnedValue>,
) -> crate::Result<OwnedValue> {
    let json = get_json_value(json_value)?;

    let arr_val = if let Some(path) = json_path {
        match json_extract_single(&json, path, true)? {
            Some(val) => val,
            None => return Ok(OwnedValue::Null),
        }
    } else {
        &json
    };

    match arr_val {
        Val::Array(val) => Ok(OwnedValue::Integer(val.len() as i64)),
        Val::Null => Ok(OwnedValue::Null),
        _ => Ok(OwnedValue::Integer(0)),
    }
}

/// Implements the -> operator. Always returns a proper JSON value.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_extract(value: &OwnedValue, path: &OwnedValue) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    let json = get_json_value(value)?;
    let extracted = json_extract_single(&json, path, false)?;

    if let Some(val) = extracted {
        let json = to_string(val).unwrap();

        Ok(OwnedValue::Text(LimboText::json(Rc::new(json))))
    } else {
        Ok(OwnedValue::Null)
    }
}

/// Implements the ->> operator. Always returns a SQL representation of the JSON subcomponent.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_shift_extract(
    value: &OwnedValue,
    path: &OwnedValue,
) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    let json = get_json_value(value)?;
    let extracted = json_extract_single(&json, path, false)?.unwrap_or_else(|| &Val::Null);

    convert_json_to_db_type(extracted, true)
}

/// Extracts a JSON value from a JSON object or array.
/// If there's only a single path, the return value might be either a TEXT or a database type.
/// https://sqlite.org/json1.html#the_json_extract_function
pub fn json_extract(value: &OwnedValue, paths: &[OwnedValue]) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if paths.is_empty() {
        return Ok(OwnedValue::Null);
    } else if paths.len() == 1 {
        let json = get_json_value(value)?;
        let extracted = json_extract_single(&json, &paths[0], true)?.unwrap_or_else(|| &Val::Null);

        return convert_json_to_db_type(extracted, false);
    }

    let json = get_json_value(value)?;
    let mut result = "[".to_string();

    for path in paths {
        match path {
            OwnedValue::Null => {
                return Ok(OwnedValue::Null);
            }
            _ => {
                let extracted =
                    json_extract_single(&json, path, true)?.unwrap_or_else(|| &Val::Null);

                if paths.len() == 1 && extracted == &Val::Null {
                    return Ok(OwnedValue::Null);
                }

                result.push_str(&to_string(&extracted).unwrap());
                result.push(',');
            }
        }
    }

    result.pop(); // remove the final comma
    result.push(']');

    Ok(OwnedValue::Text(LimboText::json(Rc::new(result))))
}

/// Returns a value with type defined by SQLite documentation:
///   > the SQL datatype of the result is NULL for a JSON null,
///   > INTEGER or REAL for a JSON numeric value,
///   > an INTEGER zero for a JSON false value,
///   > an INTEGER one for a JSON true value,
///   > the dequoted text for a JSON string value,
///   > and a text representation for JSON object and array values.
///
/// https://sqlite.org/json1.html#the_json_extract_function
///
/// *all_as_db* - if true, objects and arrays will be returned as pure TEXT without the JSON subtype
fn convert_json_to_db_type(extracted: &Val, all_as_db: bool) -> crate::Result<OwnedValue> {
    match extracted {
        Val::Null => Ok(OwnedValue::Null),
        Val::Float(f) => Ok(OwnedValue::Float(*f)),
        Val::Integer(i) => Ok(OwnedValue::Integer(*i)),
        Val::Bool(b) => {
            if *b {
                Ok(OwnedValue::Integer(1))
            } else {
                Ok(OwnedValue::Integer(0))
            }
        }
        Val::String(s) => Ok(OwnedValue::Text(LimboText::new(Rc::new(s.clone())))),
        _ => {
            let json = to_string(&extracted).unwrap();
            if all_as_db {
                Ok(OwnedValue::Text(LimboText::new(Rc::new(json))))
            } else {
                Ok(OwnedValue::Text(LimboText::json(Rc::new(json))))
            }
        }
    }
}

/// Converts a DB value (`OwnedValue`) to a JSON representation (`Val`).
/// Note that when the internal text value is a json,
/// the returned `Val` will be an object. If the internal text value is a regular text,
/// then a string will be returned. This is useful to track if the value came from a json
/// function and therefore we must interpret it as json instead of raw text when working with it.
fn convert_db_type_to_json(value: &OwnedValue) -> crate::Result<Val> {
    let val = match value {
        OwnedValue::Null => Val::Null,
        OwnedValue::Float(f) => Val::Float(*f),
        OwnedValue::Integer(i) => Val::Integer(*i),
        OwnedValue::Text(t) => match t.subtype {
            // Convert only to json if the subtype is json (if we got it from another json function)
            TextSubtype::Json => get_json_value(value)?,
            TextSubtype::Text => Val::String(t.value.to_string()),
        },
        OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
        unsupported_value => crate::bail_constraint_error!(
            "JSON cannot hold this type of value: {unsupported_value:?}"
        ),
    };
    Ok(val)
}

pub fn json_type(value: &OwnedValue, path: Option<&OwnedValue>) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    let json = get_json_value(value)?;

    let json = if let Some(path) = path {
        match json_extract_single(&json, path, true)? {
            Some(val) => val,
            None => return Ok(OwnedValue::Null),
        }
    } else {
        &json
    };

    let val = match json {
        Val::Null => "null",
        Val::Bool(v) => {
            if *v {
                "true"
            } else {
                "false"
            }
        }
        Val::Integer(_) => "integer",
        Val::Float(_) => "real",
        Val::String(_) => "text",
        Val::Array(_) => "array",
        Val::Object(_) => "object",
        Val::Removed => unreachable!(),
    };

    Ok(OwnedValue::Text(LimboText::json(Rc::new(val.to_string()))))
}

/// Returns the value at the given JSON path. If the path does not exist, it returns None.
/// If the path is an invalid path, returns an error.
///
/// *strict* - if false, we will try to resolve the path even if it does not start with "$"
///   in a way that's compatible with the `->` and `->>` operators. See examples in the docs:
///   https://sqlite.org/json1.html#the_and_operators
fn json_extract_single<'a>(
    json: &'a Val,
    path: &OwnedValue,
    strict: bool,
) -> crate::Result<Option<&'a Val>> {
    let json_path = if strict {
        match path {
            OwnedValue::Text(t) => json_path(t.value.as_str())?,
            OwnedValue::Null => return Ok(None),
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    } else {
        match path {
            OwnedValue::Text(t) => {
                if t.value.starts_with("$") {
                    json_path(t.value.as_str())?
                } else {
                    JsonPath {
                        elements: vec![PathElement::Root(), PathElement::Key(t.value.to_string())],
                    }
                }
            }
            OwnedValue::Null => return Ok(None),
            OwnedValue::Integer(i) => JsonPath {
                elements: vec![PathElement::Root(), PathElement::ArrayLocator(*i as i32)],
            },
            OwnedValue::Float(f) => JsonPath {
                elements: vec![PathElement::Root(), PathElement::Key(f.to_string())],
            },
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    };

    let mut current_element = &Val::Null;

    for element in json_path.elements.iter() {
        match element {
            PathElement::Root() => {
                current_element = json;
            }
            PathElement::Key(key) => {
                let key = key.as_str();

                match current_element {
                    Val::Object(map) => {
                        if let Some((_, value)) = map.iter().find(|(k, _)| k == key) {
                            current_element = value;
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                }
            }
            PathElement::ArrayLocator(idx) => match current_element {
                Val::Array(array) => {
                    let mut idx = *idx;

                    if idx < 0 {
                        idx += array.len() as i32;
                    }

                    if idx < array.len() as i32 {
                        current_element = &array[idx as usize];
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            },
        }
    }

    Ok(Some(current_element))
}

pub fn json_error_position(json: &OwnedValue) -> crate::Result<OwnedValue> {
    match json {
        OwnedValue::Text(t) => match from_str::<Val>(&t.value) {
            Ok(_) => Ok(OwnedValue::Integer(0)),
            Err(JsonError::Message { location, .. }) => {
                if let Some(loc) = location {
                    Ok(OwnedValue::Integer(loc.column as i64))
                } else {
                    Err(crate::error::LimboError::InternalError(
                        "failed to determine json error position".into(),
                    ))
                }
            }
        },
        OwnedValue::Blob(b) => match jsonb::from_slice(b) {
            Ok(_) => Ok(OwnedValue::Integer(0)),
            Err(JsonbError::Syntax(_, pos)) => Ok(OwnedValue::Integer(pos as i64)),
            _ => Err(crate::error::LimboError::InternalError(
                "failed to determine json error position".into(),
            )),
        },
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Ok(OwnedValue::Integer(0)),
    }
}

/// Constructs a JSON object from a list of values that represent key-value pairs.
/// The number of values must be even, and the first value of each pair (which represents the map key)
/// must be a TEXT value. The second value of each pair can be any JSON value (which represents the map value)
pub fn json_object(values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    let value_map = values
        .chunks(2)
        .map(|chunk| match chunk {
            [key, value] => {
                let key = match key {
                    OwnedValue::Text(t) => t.value.to_string(),
                    _ => crate::bail_constraint_error!("labels must be TEXT"),
                };
                let json_val = convert_db_type_to_json(value)?;

                Ok((key, json_val))
            }
            _ => crate::bail_constraint_error!("json_object requires an even number of values"),
        })
        .collect::<Result<IndexMap<String, Val>, _>>()?;

    let result = crate::json::to_string(&value_map).unwrap();
    Ok(OwnedValue::Text(LimboText::json(Rc::new(result))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OwnedValue;

    #[test]
    fn test_get_json_valid_json5() {
        let input = OwnedValue::build_text(Rc::new("{ key: 'value' }".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_double_single_quotes() {
        let input = OwnedValue::build_text(Rc::new("{ key: ''value'' }".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_infinity() {
        let input = OwnedValue::build_text(Rc::new("{ \"key\": Infinity }".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("{\"key\":9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_negative_infinity() {
        let input = OwnedValue::build_text(Rc::new("{ \"key\": -Infinity }".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("{\"key\":-9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_nan() {
        let input = OwnedValue::build_text(Rc::new("{ \"key\": NaN }".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("{\"key\":null}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = OwnedValue::build_text(Rc::new("{ key: value }".to_string()));
        let result = get_json(&input);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = OwnedValue::build_text(Rc::new("{\"key\":\"value\"}".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = OwnedValue::build_text(Rc::new("{key:\"value\"".to_string()));
        let result = get_json(&input);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_blob_valid_jsonb() {
        let binary_json = b"\x40\0\0\x01\x10\0\0\x03\x10\0\0\x03\x61\x73\x64\x61\x64\x66".to_vec();
        let input = OwnedValue::Blob(Rc::new(binary_json));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.value.contains("\"asd\":\"adf\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_blob_invalid_jsonb() {
        let binary_json: Vec<u8> = vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
        let input = OwnedValue::Blob(Rc::new(binary_json));
        let result = get_json(&input);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_non_text() {
        let input = OwnedValue::Null;
        let result = get_json(&input).unwrap();
        if let OwnedValue::Null = result {
            // Test passed
        } else {
            panic!("Expected OwnedValue::Null");
        }
    }

    #[test]
    fn test_json_array_simple() {
        let text = OwnedValue::build_text(Rc::new("value1".to_string()));
        let json = OwnedValue::Text(LimboText::json(Rc::new("\"value2\"".to_string())));
        let input = vec![text, json, OwnedValue::Integer(1), OwnedValue::Float(1.1)];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.value.as_str(), "[\"value1\",\"value2\",1,1.1]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_json_array_empty() {
        let input = vec![];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.value.as_str(), "[]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_json_array_blob_invalid() {
        let blob = OwnedValue::Blob(Rc::new("1".as_bytes().to_vec()));

        let input = vec![blob];

        let result = json_array(&input);

        match result {
            Ok(_) => panic!("Expected error for blob input"),
            Err(e) => assert!(e.to_string().contains("JSON cannot hold BLOB values")),
        }
    }

    #[test]
    fn test_json_array_length() {
        let input = OwnedValue::build_text(Rc::new("[1,2,3,4]".to_string()));
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_empty() {
        let input = OwnedValue::build_text(Rc::new("[]".to_string()));
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_root() {
        let input = OwnedValue::build_text(Rc::new("[1,2,3,4]".to_string()));
        let result = json_array_length(
            &input,
            Some(&OwnedValue::build_text(Rc::new("$".to_string()))),
        )
        .unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_not_array() {
        let input = OwnedValue::build_text(Rc::new("{one: [1,2,3,4]}".to_string()));
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_prop() {
        let input = OwnedValue::build_text(Rc::new("{one: [1,2,3,4]}".to_string()));
        let result = json_array_length(
            &input,
            Some(&OwnedValue::build_text(Rc::new("$.one".to_string()))),
        )
        .unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index() {
        let input = OwnedValue::build_text(Rc::new("[[1,2,3,4]]".to_string()));
        let result = json_array_length(
            &input,
            Some(&OwnedValue::build_text(Rc::new("$[0]".to_string()))),
        )
        .unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_not_array() {
        let input = OwnedValue::build_text(Rc::new("[1,2,3,4]".to_string()));
        let result = json_array_length(
            &input,
            Some(&OwnedValue::build_text(Rc::new("$[2]".to_string()))),
        )
        .unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_bad_prop() {
        let input = OwnedValue::build_text(Rc::new("{one: [1,2,3,4]}".to_string()));
        let result = json_array_length(
            &input,
            Some(&OwnedValue::build_text(Rc::new("$.two".to_string()))),
        )
        .unwrap();
        assert_eq!(OwnedValue::Null, result);
    }

    #[test]
    fn test_json_array_length_simple_json_subtype() {
        let input = OwnedValue::build_text(Rc::new("[1,2,3]".to_string()));
        let wrapped = get_json(&input).unwrap();
        let result = json_array_length(&wrapped, None).unwrap();

        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 3);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_extract_missing_path() {
        let result = json_extract(
            &OwnedValue::build_text(Rc::new("{\"a\":2}".to_string())),
            &[OwnedValue::build_text(Rc::new("$.x".to_string()))],
        );

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }
    #[test]
    fn test_json_extract_null_path() {
        let result = json_extract(
            &OwnedValue::build_text(Rc::new("{\"a\":2}".to_string())),
            &[OwnedValue::Null],
        );

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }

    #[test]
    fn test_json_path_invalid() {
        let result = json_extract(
            &OwnedValue::build_text(Rc::new("{\"a\":2}".to_string())),
            &[OwnedValue::Float(1.1)],
        );

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.to_string().contains("JSON path error")),
        }
    }

    #[test]
    fn test_json_error_position_no_error() {
        let input = OwnedValue::build_text(Rc::new("[1,2,3]".to_string()));
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_no_error_more() {
        let input = OwnedValue::build_text(Rc::new(r#"{"a":55,"b":72 , }"#.to_string()));
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_object() {
        let input = OwnedValue::build_text(Rc::new(r#"{"a":55,"b":72,,}"#.to_string()));
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_array() {
        let input = OwnedValue::build_text(Rc::new(r#"["a",55,"b",72,,]"#.to_string()));
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_null() {
        let input = OwnedValue::Null;
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Null);
    }

    #[test]
    fn test_json_error_position_integer() {
        let input = OwnedValue::Integer(5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_float() {
        let input = OwnedValue::Float(-5.5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_blob() {
        let input = OwnedValue::Blob(Rc::new(r#"["a",55,"b",72,,]"#.as_bytes().to_owned()));
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_object_simple() {
        let key = OwnedValue::build_text(Rc::new("key".to_string()));
        let value = OwnedValue::build_text(Rc::new("value".to_string()));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.value.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_multiple_values() {
        let text_key = OwnedValue::build_text(Rc::new("text_key".to_string()));
        let text_value = OwnedValue::build_text(Rc::new("text_value".to_string()));
        let json_key = OwnedValue::build_text(Rc::new("json_key".to_string()));
        let json_value = OwnedValue::Text(LimboText::json(Rc::new(
            r#"{"json":"value","number":1}"#.to_string(),
        )));
        let integer_key = OwnedValue::build_text(Rc::new("integer_key".to_string()));
        let integer_value = OwnedValue::Integer(1);
        let float_key = OwnedValue::build_text(Rc::new("float_key".to_string()));
        let float_value = OwnedValue::Float(1.1);
        let null_key = OwnedValue::build_text(Rc::new("null_key".to_string()));
        let null_value = OwnedValue::Null;

        let input = vec![
            text_key,
            text_value,
            json_key,
            json_value,
            integer_key,
            integer_value,
            float_key,
            float_value,
            null_key,
            null_value,
        ];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(
            json_text.value.as_str(),
            r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
        );
    }

    #[test]
    fn test_json_object_json_value_is_rendered_as_json() {
        let key = OwnedValue::build_text(Rc::new("key".to_string()));
        let value = OwnedValue::Text(LimboText::json(Rc::new(r#"{"json":"value"}"#.to_string())));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.value.as_str(), r#"{"key":{"json":"value"}}"#);
    }

    #[test]
    fn test_json_object_json_text_value_is_rendered_as_regular_text() {
        let key = OwnedValue::build_text(Rc::new("key".to_string()));
        let value = OwnedValue::Text(LimboText::new(Rc::new(r#"{"json":"value"}"#.to_string())));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(
            json_text.value.as_str(),
            r#"{"key":"{\"json\":\"value\"}"}"#
        );
    }

    #[test]
    fn test_json_object_nested() {
        let key = OwnedValue::build_text(Rc::new("key".to_string()));
        let value = OwnedValue::build_text(Rc::new("value".to_string()));
        let input = vec![key, value];

        let parent_key = OwnedValue::build_text(Rc::new("parent_key".to_string()));
        let parent_value = json_object(&input).unwrap();
        let parent_input = vec![parent_key, parent_value];

        let result = json_object(&parent_input).unwrap();

        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(
            json_text.value.as_str(),
            r#"{"parent_key":{"key":"value"}}"#
        );
    }

    #[test]
    fn test_json_object_duplicated_keys() {
        let key = OwnedValue::build_text(Rc::new("key".to_string()));
        let value = OwnedValue::build_text(Rc::new("value".to_string()));
        let input = vec![key.clone(), value.clone(), key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.value.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_empty() {
        let input = vec![];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.value.as_str(), r#"{}"#);
    }

    #[test]
    fn test_json_object_non_text_key() {
        let key = OwnedValue::Integer(1);
        let value = OwnedValue::build_text(Rc::new("value".to_string()));
        let input = vec![key, value];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for non-TEXT key"),
            Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
        }
    }

    #[test]
    fn test_json_odd_number_of_values() {
        let key = OwnedValue::build_text(Rc::new("key".to_string()));
        let value = OwnedValue::build_text(Rc::new("value".to_string()));
        let input = vec![key.clone(), value, key];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for odd number of values"),
            Err(e) => assert!(e
                .to_string()
                .contains("json_object requires an even number of values")),
        }
    }
}
pub fn is_json_valid(json_value: &OwnedValue) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => match from_str::<Val>(&t.value) {
            Ok(_) => Ok(OwnedValue::Integer(1)),
            Err(_) => Ok(OwnedValue::Integer(0)),
        },
        OwnedValue::Blob(b) => match jsonb::from_slice(b) {
            Ok(_) => Ok(OwnedValue::Integer(1)),
            Err(_) => Ok(OwnedValue::Integer(0)),
        },
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Ok(OwnedValue::Integer(1)),
    }
}
