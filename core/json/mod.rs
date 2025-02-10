mod de;
mod error;
mod json_operations;
mod json_path;
mod ser;

pub use crate::json::de::from_str;
use crate::json::de::ordered_object;
use crate::json::error::Error as JsonError;
pub use crate::json::json_operations::{json_patch, json_remove};
use crate::json::json_path::{json_path, JsonPath, PathElement};
pub use crate::json::ser::to_string;
use crate::types::{OwnedValue, Text, TextSubtype};
use indexmap::IndexMap;
use jsonb::Error as JsonbError;
use ser::to_string_pretty;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

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

pub fn get_json(json_value: &OwnedValue, indent: Option<&str>) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => {
            // optimization: once we know the subtype is a valid JSON, we do not have
            // to go through parsing JSON and serializing it back to string
            if t.subtype == TextSubtype::Json {
                return Ok(json_value.to_owned());
            }

            let json_val = get_json_value(json_value)?;
            let json = match indent {
                Some(indent) => to_string_pretty(&json_val, indent)?,
                None => to_string(&json_val)?,
            };

            Ok(OwnedValue::Text(Text::json(&json)))
        }
        OwnedValue::Blob(b) => {
            // TODO: use get_json_value after we implement a single Struct
            //   to represent both JSON and JSONB
            if let Ok(json) = jsonb::from_slice(b) {
                Ok(OwnedValue::Text(Text::json(&json.to_string())))
            } else {
                crate::bail_parse_error!("malformed JSON");
            }
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => {
            let json_val = get_json_value(json_value)?;
            let json = match indent {
                Some(indent) => to_string_pretty(&json_val, indent)?,
                None => to_string(&json_val)?,
            };

            Ok(OwnedValue::Text(Text::json(&json)))
        }
    }
}

fn get_json_value(json_value: &OwnedValue) -> crate::Result<Val> {
    match json_value {
        OwnedValue::Text(ref t) => match from_str::<Val>(t.as_str()) {
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
                    s.push_str(t.as_str());
                } else {
                    match to_string(&t.as_str().to_string()) {
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
    Ok(OwnedValue::Text(Text::json(&s)))
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

pub fn json_set(json: &OwnedValue, values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    let mut json_value = get_json_value(json)?;

    values
        .chunks(2)
        .map(|chunk| match chunk {
            [path, value] => {
                let path = json_path_from_owned_value(path, true)?;

                if let Some(path) = path {
                    let new_value = match value {
                        OwnedValue::Text(
                            t @ Text {
                                subtype: TextSubtype::Text,
                                ..
                            },
                        ) => Val::String(t.as_str().to_string()),
                        _ => get_json_value(value)?,
                    };

                    let mut new_json_value = json_value.clone();

                    match create_and_mutate_json_by_path(&mut new_json_value, path, |val| match val
                    {
                        Target::Array(arr, index) => arr[index] = new_value.clone(),
                        Target::Value(val) => *val = new_value.clone(),
                    }) {
                        Some(_) => json_value = new_json_value,
                        _ => {}
                    }
                }

                Ok(())
            }
            _ => crate::bail_constraint_error!("json_set needs an odd number of arguments"),
        })
        .collect::<crate::Result<()>>()?;

    convert_json_to_db_type(&json_value, true)
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
        let json = to_string(val)?;

        Ok(OwnedValue::Text(Text::json(&json)))
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
    let extracted = json_extract_single(&json, path, false)?.unwrap_or(&Val::Null);

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
        let extracted = json_extract_single(&json, &paths[0], true)?.unwrap_or(&Val::Null);

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
                let extracted = json_extract_single(&json, path, true)?.unwrap_or(&Val::Null);

                if paths.len() == 1 && extracted == &Val::Null {
                    return Ok(OwnedValue::Null);
                }

                result.push_str(&to_string(&extracted)?);
                result.push(',');
            }
        }
    }

    result.pop(); // remove the final comma
    result.push(']');

    Ok(OwnedValue::Text(Text::json(&result)))
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
        Val::Removed => Ok(OwnedValue::Null),
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
        Val::String(s) => Ok(OwnedValue::Text(Text::from_str(s))),
        _ => {
            let json = to_string(&extracted)?;
            if all_as_db {
                Ok(OwnedValue::build_text(&json))
            } else {
                Ok(OwnedValue::Text(Text::json(&json)))
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
            TextSubtype::Text => Val::String(t.as_str().to_string()),
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

    Ok(OwnedValue::Text(Text::json(val)))
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
    let json_path = match json_path_from_owned_value(path, strict)? {
        Some(path) => path,
        None => return Ok(None),
    };

    let mut current_element = &Val::Null;

    for element in json_path.elements.iter() {
        match element {
            PathElement::Root() => {
                current_element = json;
            }
            PathElement::Key(key, _) => match current_element {
                Val::Object(map) => {
                    if let Some((_, value)) = map.iter().find(|(k, _)| k == key) {
                        current_element = value;
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            },
            PathElement::ArrayLocator(idx) => match current_element {
                Val::Array(array) => {
                    if let Some(mut idx) = *idx {
                        if idx < 0 {
                            idx += array.len() as i32;
                        }

                        if idx < array.len() as i32 {
                            current_element = &array[idx as usize];
                        } else {
                            return Ok(None);
                        }
                    }
                }
                _ => return Ok(None),
            },
        }
    }

    Ok(Some(current_element))
}

fn json_path_from_owned_value(path: &OwnedValue, strict: bool) -> crate::Result<Option<JsonPath>> {
    let json_path = if strict {
        match path {
            OwnedValue::Text(t) => json_path(t.as_str())?,
            OwnedValue::Null => return Ok(None),
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    } else {
        match path {
            OwnedValue::Text(t) => {
                if t.as_str().starts_with("$") {
                    json_path(t.as_str())?
                } else {
                    JsonPath {
                        elements: vec![
                            PathElement::Root(),
                            PathElement::Key(Cow::Borrowed(t.as_str()), false),
                        ],
                    }
                }
            }
            OwnedValue::Null => return Ok(None),
            OwnedValue::Integer(i) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::ArrayLocator(Some(*i as i32)),
                ],
            },
            OwnedValue::Float(f) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::Key(Cow::Owned(f.to_string()), false),
                ],
            },
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    };

    Ok(Some(json_path))
}

enum Target<'a> {
    Array(&'a mut Vec<Val>, usize),
    Value(&'a mut Val),
}

fn mutate_json_by_path<F, R>(json: &mut Val, path: JsonPath, closure: F) -> Option<R>
where
    F: FnMut(Target) -> R,
{
    find_target(json, &path).map(closure)
}

fn find_target<'a>(json: &'a mut Val, path: &JsonPath) -> Option<Target<'a>> {
    let mut current = json;
    for (i, key) in path.elements.iter().enumerate() {
        let is_last = i == path.elements.len() - 1;
        match key {
            PathElement::Root() => continue,
            PathElement::ArrayLocator(index) => match current {
                Val::Array(arr) => {
                    if let Some(index) = match index {
                        Some(i) if *i < 0 => arr.len().checked_sub(i.unsigned_abs() as usize),
                        Some(i) => ((*i as usize) < arr.len()).then_some(*i as usize),
                        None => Some(arr.len()),
                    } {
                        if is_last {
                            return Some(Target::Array(arr, index));
                        } else {
                            current = &mut arr[index];
                        }
                    } else {
                        return None;
                    }
                }
                _ => {
                    return None;
                }
            },
            PathElement::Key(key, _) => match current {
                Val::Object(obj) => {
                    if let Some(pos) = &obj
                        .iter()
                        .position(|(k, v)| k == key && !matches!(v, Val::Removed))
                    {
                        let val = &mut obj[*pos].1;
                        current = val;
                    } else {
                        return None;
                    }
                }
                _ => {
                    return None;
                }
            },
        }
    }
    Some(Target::Value(current))
}

fn create_and_mutate_json_by_path<F, R>(json: &mut Val, path: JsonPath, closure: F) -> Option<R>
where
    F: FnOnce(Target) -> R,
{
    find_or_create_target(json, &path).map(closure)
}

fn find_or_create_target<'a>(json: &'a mut Val, path: &JsonPath) -> Option<Target<'a>> {
    let mut current = json;
    for (i, key) in path.elements.iter().enumerate() {
        let is_last = i == path.elements.len() - 1;
        match key {
            PathElement::Root() => continue,
            PathElement::ArrayLocator(index) => match current {
                Val::Array(arr) => {
                    if let Some(index) = match index {
                        Some(i) if *i < 0 => arr.len().checked_sub(i.unsigned_abs() as usize),
                        Some(i) => Some(*i as usize),
                        None => Some(arr.len()),
                    } {
                        if is_last {
                            if index == arr.len() {
                                arr.push(Val::Null);
                            }

                            if index >= arr.len() {
                                return None;
                            }

                            return Some(Target::Array(arr, index));
                        } else {
                            if index == arr.len() {
                                arr.push(
                                    if matches!(path.elements[i + 1], PathElement::ArrayLocator(_))
                                    {
                                        Val::Array(vec![])
                                    } else {
                                        Val::Object(vec![])
                                    },
                                );
                            }

                            if index >= arr.len() {
                                return None;
                            }

                            current = &mut arr[index];
                        }
                    } else {
                        return None;
                    }
                }
                _ => {
                    *current = Val::Array(vec![]);
                }
            },
            PathElement::Key(key, _) => match current {
                Val::Object(obj) => {
                    if let Some(pos) = &obj
                        .iter()
                        .position(|(k, v)| k == key && !matches!(v, Val::Removed))
                    {
                        let val = &mut obj[*pos].1;
                        current = val;
                    } else {
                        let element = if !is_last
                            && matches!(path.elements[i + 1], PathElement::ArrayLocator(_))
                        {
                            Val::Array(vec![])
                        } else {
                            Val::Object(vec![])
                        };

                        obj.push((key.to_string(), element));
                        let index = obj.len() - 1;
                        current = &mut obj[index].1;
                    }
                }
                _ => {
                    return None;
                }
            },
        }
    }
    Some(Target::Value(current))
}

pub fn json_error_position(json: &OwnedValue) -> crate::Result<OwnedValue> {
    match json {
        OwnedValue::Text(t) => match from_str::<Val>(t.as_str()) {
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
                    OwnedValue::Text(t) => t.as_str().to_string(),
                    _ => crate::bail_constraint_error!("labels must be TEXT"),
                };
                let json_val = convert_db_type_to_json(value)?;

                Ok((key, json_val))
            }
            _ => crate::bail_constraint_error!("json_object requires an even number of values"),
        })
        .collect::<Result<IndexMap<String, Val>, _>>()?;

    let result = crate::json::to_string(&value_map)?;
    Ok(OwnedValue::Text(Text::json(&result)))
}

pub fn is_json_valid(json_value: &OwnedValue) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => match from_str::<Val>(t.as_str()) {
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

pub fn json_quote(value: &OwnedValue) -> crate::Result<OwnedValue> {
    match value {
        OwnedValue::Text(ref t) => {
            // If X is a JSON value returned by another JSON function,
            // then this function is a no-op
            if t.subtype == TextSubtype::Json {
                // Should just return the json value with no quotes
                return Ok(value.to_owned());
            }

            let mut escaped_value = String::with_capacity(t.value.len() + 4);
            escaped_value.push('"');

            for c in t.as_str().chars() {
                match c {
                    '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => {
                        escaped_value.push('\\');
                        escaped_value.push(c);
                    }
                    c => escaped_value.push(c),
                }
            }
            escaped_value.push('"');

            Ok(OwnedValue::build_text(&escaped_value))
        }
        // Numbers are unquoted in json
        OwnedValue::Integer(ref int) => Ok(OwnedValue::Integer(int.to_owned())),
        OwnedValue::Float(ref float) => Ok(OwnedValue::Float(float.to_owned())),
        OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
        OwnedValue::Null => Ok(OwnedValue::build_text("null")),
        _ => {
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::types::OwnedValue;

    #[test]
    fn test_get_json_valid_json5() {
        let input = OwnedValue::build_text("{ key: 'value' }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_double_single_quotes() {
        let input = OwnedValue::build_text("{ key: ''value'' }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_infinity() {
        let input = OwnedValue::build_text("{ \"key\": Infinity }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_negative_infinity() {
        let input = OwnedValue::build_text("{ \"key\": -Infinity }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":-9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_nan() {
        let input = OwnedValue::build_text("{ \"key\": NaN }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":null}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = OwnedValue::build_text("{ key: value }");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = OwnedValue::build_text("{\"key\":\"value\"}");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = OwnedValue::build_text("{key:\"value\"");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_blob_valid_jsonb() {
        let binary_json = b"\x40\0\0\x01\x10\0\0\x03\x10\0\0\x03\x61\x73\x64\x61\x64\x66".to_vec();
        let input = OwnedValue::Blob(Rc::new(binary_json));
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"asd\":\"adf\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_blob_invalid_jsonb() {
        let binary_json: Vec<u8> = vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
        let input = OwnedValue::Blob(Rc::new(binary_json));
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_non_text() {
        let input = OwnedValue::Null;
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Null = result {
            // Test passed
        } else {
            panic!("Expected OwnedValue::Null");
        }
    }

    #[test]
    fn test_json_array_simple() {
        let text = OwnedValue::build_text("value1");
        let json = OwnedValue::Text(Text::json("\"value2\""));
        let input = vec![text, json, OwnedValue::Integer(1), OwnedValue::Float(1.1)];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.as_str(), "[\"value1\",\"value2\",1,1.1]");
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
            assert_eq!(res.as_str(), "[]");
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
        let input = OwnedValue::build_text("[1,2,3,4]");
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_empty() {
        let input = OwnedValue::build_text("[]");
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_root() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_not_array() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_prop() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$.one"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index() {
        let input = OwnedValue::build_text("[[1,2,3,4]]");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$[0]"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_not_array() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$[2]"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_bad_prop() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$.two"))).unwrap();
        assert_eq!(OwnedValue::Null, result);
    }

    #[test]
    fn test_json_array_length_simple_json_subtype() {
        let input = OwnedValue::build_text("[1,2,3]");
        let wrapped = get_json(&input, None).unwrap();
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
            &OwnedValue::build_text("{\"a\":2}"),
            &[OwnedValue::build_text("$.x")],
        );

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }
    #[test]
    fn test_json_extract_null_path() {
        let result = json_extract(&OwnedValue::build_text("{\"a\":2}"), &[OwnedValue::Null]);

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }

    #[test]
    fn test_json_path_invalid() {
        let result = json_extract(
            &OwnedValue::build_text("{\"a\":2}"),
            &[OwnedValue::Float(1.1)],
        );

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.to_string().contains("JSON path error")),
        }
    }

    #[test]
    fn test_json_error_position_no_error() {
        let input = OwnedValue::build_text("[1,2,3]");
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_no_error_more() {
        let input = OwnedValue::build_text(r#"{"a":55,"b":72 , }"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_object() {
        let input = OwnedValue::build_text(r#"{"a":55,"b":72,,}"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_array() {
        let input = OwnedValue::build_text(r#"["a",55,"b",72,,]"#);
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
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_multiple_values() {
        let text_key = OwnedValue::build_text("text_key");
        let text_value = OwnedValue::build_text("text_value");
        let json_key = OwnedValue::build_text("json_key");
        let json_value = OwnedValue::Text(Text::json(r#"{"json":"value","number":1}"#));
        let integer_key = OwnedValue::build_text("integer_key");
        let integer_value = OwnedValue::Integer(1);
        let float_key = OwnedValue::build_text("float_key");
        let float_value = OwnedValue::Float(1.1);
        let null_key = OwnedValue::build_text("null_key");
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
            json_text.as_str(),
            r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
        );
    }

    #[test]
    fn test_json_object_json_value_is_rendered_as_json() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::Text(Text::json(r#"{"json":"value"}"#));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":{"json":"value"}}"#);
    }

    #[test]
    fn test_json_object_json_text_value_is_rendered_as_regular_text() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::Text(Text::new(r#"{"json":"value"}"#));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"{\"json\":\"value\"}"}"#);
    }

    #[test]
    fn test_json_object_nested() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key, value];

        let parent_key = OwnedValue::build_text("parent_key");
        let parent_value = json_object(&input).unwrap();
        let parent_input = vec![parent_key, parent_value];

        let result = json_object(&parent_input).unwrap();

        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"parent_key":{"key":"value"}}"#);
    }

    #[test]
    fn test_json_object_duplicated_keys() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key.clone(), value.clone(), key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_empty() {
        let input = vec![];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{}"#);
    }

    #[test]
    fn test_json_object_non_text_key() {
        let key = OwnedValue::Integer(1);
        let value = OwnedValue::build_text("value");
        let input = vec![key, value];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for non-TEXT key"),
            Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
        }
    }

    #[test]
    fn test_json_odd_number_of_values() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key.clone(), value, key];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for odd number of values"),
            Err(e) => assert!(e
                .to_string()
                .contains("json_object requires an even number of values")),
        }
    }

    #[test]
    fn test_find_target_array() {
        let mut val = Val::Array(vec![
            Val::String("first".to_string()),
            Val::String("second".to_string()),
        ]);
        let path = JsonPath {
            elements: vec![PathElement::ArrayLocator(Some(0))],
        };

        match find_target(&mut val, &path) {
            Some(Target::Array(_, idx)) => assert_eq!(idx, 0),
            _ => panic!("Expected Array target"),
        }
    }

    #[test]
    fn test_find_target_negative_index() {
        let mut val = Val::Array(vec![
            Val::String("first".to_string()),
            Val::String("second".to_string()),
        ]);
        let path = JsonPath {
            elements: vec![PathElement::ArrayLocator(Some(-1))],
        };

        match find_target(&mut val, &path) {
            Some(Target::Array(_, idx)) => assert_eq!(idx, 1),
            _ => panic!("Expected Array target"),
        }
    }

    #[test]
    fn test_find_target_object() {
        let mut val = Val::Object(vec![("key".to_string(), Val::String("value".to_string()))]);
        let path = JsonPath {
            elements: vec![PathElement::Key(Cow::Borrowed("key"), false)],
        };

        match find_target(&mut val, &path) {
            Some(Target::Value(_)) => {}
            _ => panic!("Expected Value target"),
        }
    }

    #[test]
    fn test_find_target_removed() {
        let mut val = Val::Object(vec![
            ("key".to_string(), Val::Removed),
            ("key".to_string(), Val::String("value".to_string())),
        ]);
        let path = JsonPath {
            elements: vec![PathElement::Key(Cow::Borrowed("key"), false)],
        };

        match find_target(&mut val, &path) {
            Some(Target::Value(val)) => assert!(matches!(val, Val::String(_))),
            _ => panic!("Expected second value, not removed"),
        }
    }

    #[test]
    fn test_mutate_json() {
        let mut val = Val::Array(vec![Val::String("test".to_string())]);
        let path = JsonPath {
            elements: vec![PathElement::ArrayLocator(Some(0))],
        };

        let result = mutate_json_by_path(&mut val, path, |target| match target {
            Target::Array(arr, idx) => {
                arr.remove(idx);
                "removed"
            }
            _ => panic!("Expected Array target"),
        });

        assert_eq!(result, Some("removed"));
        assert!(matches!(val, Val::Array(arr) if arr.is_empty()));
    }

    #[test]
    fn test_mutate_json_none() {
        let mut val = Val::Array(vec![]);
        let path = JsonPath {
            elements: vec![PathElement::ArrayLocator(Some(0))],
        };

        let result: Option<()> = mutate_json_by_path(&mut val, path, |_| {
            panic!("Should not be called");
        });

        assert_eq!(result, None);
    }

    #[test]
    fn test_json_path_from_owned_value_root_strict() {
        let path = OwnedValue::Text(Text::new("$"));

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match result.elements[..] {
            [PathElement::Root()] => {}
            _ => panic!("Expected root"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_root_non_strict() {
        let path = OwnedValue::Text(Text::new("$"));

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match result.elements[..] {
            [PathElement::Root()] => {}
            _ => panic!("Expected root"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_named_strict() {
        let path = OwnedValue::Text(Text::new("field"));

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_named_non_strict() {
        let path = OwnedValue::Text(Text::new("field"));

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, false)] if *field == "field" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_integer_strict() {
        let path = OwnedValue::Integer(3);
        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_integer_non_strict() {
        let path = OwnedValue::Integer(3);

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::ArrayLocator(index)] if *index == Some(3) => {}
            _ => panic!("Expected root and array locator"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_null_strict() {
        let path = OwnedValue::Null;

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_null_non_strict() {
        let path = OwnedValue::Null;

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_float_strict() {
        let path = OwnedValue::Float(1.23);

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_float_non_strict() {
        let path = OwnedValue::Float(1.23);

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, false)] if *field == "1.23" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_set_field_empty_object() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.field"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"field":"value"}"#)
        );
    }

    #[test]
    fn test_json_set_replace_field() {
        let result = json_set(
            &OwnedValue::build_text(r#"{"field":"old_value"}"#),
            &[
                OwnedValue::build_text("$.field"),
                OwnedValue::build_text("new_value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"field":"new_value"}"#)
        );
    }

    #[test]
    fn test_json_set_set_deeply_nested_key() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object.doesnt.exist"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"object":{"doesnt":{"exist":"value"}}}"#)
        );
    }

    #[test]
    fn test_json_set_add_value_to_empty_array() {
        let result = json_set(
            &OwnedValue::build_text("[]"),
            &[
                OwnedValue::build_text("$[0]"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text(r#"["value"]"#));
    }

    #[test]
    fn test_json_set_add_value_to_nonexistent_array() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.some_array[0]"),
                OwnedValue::Integer(123),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"some_array":[123]}"#)
        );
    }

    #[test]
    fn test_json_set_add_value_to_array() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[1]"), OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[123,456]"));
    }

    #[test]
    fn test_json_set_add_value_to_array_out_of_bounds() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[200]"), OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[123]"));
    }

    #[test]
    fn test_json_set_replace_value_in_array() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[0]"), OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[456]"));
    }

    #[test]
    fn test_json_set_null_path() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[OwnedValue::Null, OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("{}"));
    }

    #[test]
    fn test_json_set_multiple_keys() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[
                OwnedValue::build_text("$[0]"),
                OwnedValue::Integer(456),
                OwnedValue::build_text("$[1]"),
                OwnedValue::Integer(789),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[456,789]"));
    }

    #[test]
    fn test_json_set_missing_value() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[0]")],
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_json_set_add_array_in_nested_object() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object[0].field"),
                OwnedValue::Integer(123),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"object":[{"field":123}]}"#)
        );
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object[0][0]"),
                OwnedValue::Integer(123),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"object":[[123]]}"#)
        );
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object_out_of_bounds() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object[123].another"),
                OwnedValue::build_text("value"),
                OwnedValue::build_text("$.field"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"field":"value"}"#)
        );
    }
}
