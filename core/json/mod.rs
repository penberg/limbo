mod cache;
mod error;
pub(crate) mod jsonb;
mod ops;
pub(crate) mod path;
pub(crate) mod vtab;

use crate::json::error::Error as JsonError;
pub use crate::json::ops::{
    json_insert, json_patch, json_remove, json_replace, jsonb_insert, jsonb_patch, jsonb_remove,
    jsonb_replace,
};
use crate::json::path::{json_path, JsonPath, PathElement};
use crate::numeric::{str_to_i64, Numeric};
use crate::types::{AsValueRef, Text, TextSubtype, Value, ValueType};
use crate::{bail_constraint_error, LimboError, ValueRef};
pub use cache::JsonCacheCell;
use jsonb::{
    jsonb_error_position, unescape_string, validate_jsonb, ElementType, Jsonb, JsonbHeader,
    ParseInfo, PathOperationMode, SearchOperation, SetOperation,
};
use std::borrow::Cow;
use std::fmt::Write as _;
use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub enum Conv {
    Strict,
    NotStrict,
    ToString,
}

#[cfg(feature = "json")]
pub enum OutputVariant {
    ElementType,
    ElementTypePlain,
    Binary,
    String,
}

pub fn get_json(json_value: &Value, indent: Option<&str>) -> crate::Result<Value> {
    match json_value {
        Value::Text(ref t) if t.subtype == TextSubtype::Json && indent.is_none() => {
            // optimization: once we know the subtype is a valid JSON, we do not have
            // to go through parsing JSON and serializing it back to string
            Ok(json_value.to_owned())
        }
        Value::Null => Ok(Value::Null),
        _ => {
            let json_val = convert_dbtype_to_jsonb(json_value, Conv::Strict)?;
            let mut json = match indent {
                Some(indent) => json_val.to_string_pretty(Some(indent))?,
                None => json_val.to_string()?,
            };

            // An infinite REAL argument converts to the payload
            // 9.0e+999, but SQLite's json() renders it as 9e999 (while
            // json_array and json_quote keep the long form) (#4196).
            json = json.replace("9.0e+999", "9e999");

            if indent.is_some() {
                // json_pretty() output carries no subtype in SQLite.
                Ok(Value::Text(Text::new(json)))
            } else {
                Ok(Value::Text(Text::json(json)))
            }
        }
    }
}

/// Converts a value to `Jsonb`, using the provided cache, and returns a `Value::Blob` containing
/// the jsonb.
pub fn jsonb(json_value: &Value, cache: &JsonCacheCell) -> crate::Result<Value> {
    if matches!(json_value, Value::Null) {
        return Ok(Value::Null);
    }
    let json_conv_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);

    let jsonbin =
        cache
            .get_or_insert_with(json_value, json_conv_fn)
            .map_err(|error| match error {
                LimboError::OutOfMemory => LimboError::OutOfMemory,
                _ => LimboError::ParseError("malformed JSON".to_string()),
            })?;
    Ok(Value::Blob(jsonbin.data()))
}

pub fn convert_dbtype_to_raw_jsonb(data: &Value, strict: Conv) -> crate::Result<crate::ValueBlob> {
    let json = convert_dbtype_to_jsonb(data, strict)?;
    Ok(json.data())
}

/// Return the encoded length of the raw JSONB element beginning at `cursor`.
///
/// Aggregate JSON payloads keep an unfinalized one-byte array/object header
/// followed by complete JSONB elements. Window xInverse uses this boundary to
/// remove the oldest array value or object key/value pair without reparsing
/// textual JSON.
pub(crate) fn raw_jsonb_element_len(data: &[u8], cursor: usize) -> crate::Result<usize> {
    let (header, header_len) = JsonbHeader::from_slice(cursor, data)?;
    let element_len = header_len
        .checked_add(header.payload_size())
        .ok_or_else(|| LimboError::ParseError("malformed JSON".to_string()))?;
    let end = cursor
        .checked_add(element_len)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| LimboError::ParseError("malformed JSON".to_string()))?;
    debug_assert_eq!(end - cursor, element_len);
    Ok(element_len)
}

pub fn json_from_raw_bytes_agg(data: &[u8], raw: bool) -> crate::Result<Value> {
    let mut json = Jsonb::from_raw_data(data)?;
    let el_type = json.element_type()?;
    json.finalize_unsafe(el_type)?;
    if raw {
        json_string_to_db_type(json, el_type, OutputVariant::Binary)
    } else {
        json_string_to_db_type(json, el_type, OutputVariant::ElementType)
    }
}

pub fn convert_dbtype_to_jsonb(val: impl AsValueRef, strict: Conv) -> crate::Result<Jsonb> {
    let val = val.as_value_ref();
    convert_ref_dbtype_to_jsonb(val, strict)
}

fn parse_as_json_text(slice: &[u8], mode: Conv) -> crate::Result<Jsonb> {
    let zero_pos = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
    let truncated = &slice[..zero_pos];
    let str = std::str::from_utf8(truncated)
        .map_err(|_| LimboError::ParseError("malformed JSON".to_string()))?;
    Jsonb::from_str_with_mode(str, mode).map_err(Into::into)
}

/// Parses like [parse_as_json_text] but also reports whether the text
/// used any JSON5-only syntax, which json_valid needs to tell strict
/// RFC 8259 documents apart from merely parseable ones.
fn parse_as_json_text_tracking(slice: &[u8]) -> crate::Result<(Jsonb, ParseInfo)> {
    let zero_pos = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
    let truncated = &slice[..zero_pos];
    let str = std::str::from_utf8(truncated)
        .map_err(|_| LimboError::ParseError("malformed JSON".to_string()))?;
    Jsonb::from_str_tracking(str).map_err(Into::into)
}

fn malformed_json_error(error: JsonError) -> LimboError {
    match error {
        JsonError::OutOfMemory => LimboError::OutOfMemory,
        JsonError::Message { .. } => LimboError::ParseError("malformed JSON".to_string()),
    }
}

/// Whether a blob is a complete, fully valid JSONB document. Unlike
/// [`looks_like_jsonb_blob`] this examines the whole payload, because
/// later readers trust its interior offsets and decode its text
/// payloads as `&str`: any blob's first byte parses as a plausible
/// header, so the header alone cannot identify JSONB.
fn is_jsonb_blob(slice: &[u8]) -> bool {
    validate_jsonb(slice)
}

/// SQLite's shallow "superficially looks like JSONB" test
/// (jsonFuncArgMightBeBinary): the outer header must parse, claim
/// exactly the whole blob, and a NULL/TRUE/FALSE element must have no
/// payload. The payload bytes themselves are never examined, so a blob
/// can pass this test and still fail full validation.
fn looks_like_jsonb_blob(slice: &[u8]) -> bool {
    if slice.is_empty() {
        return false;
    }
    // SQLite reads the 8-byte size encoding (header nibble 15) with a
    // 32-bit size, so the header only parses when the first four size
    // bytes are zero.
    if slice[0] >> 4 == 15 && (slice.len() < 9 || slice[1..5] != [0, 0, 0, 0]) {
        return false;
    }
    let Ok((header, header_offset)) = JsonbHeader::from_slice(0, slice) else {
        return false;
    };
    let payload_size = header.payload_size();
    if header_offset.checked_add(payload_size) != Some(slice.len()) {
        return false;
    }
    if payload_size > 0
        && matches!(
            header.element_type(),
            ElementType::NULL | ElementType::TRUE | ElementType::FALSE
        )
    {
        return false;
    }
    // RFC 8259 text can only masquerade as JSONB when it starts with
    // '{', '[' or a digit, and in every such coincidence the claimed
    // payload is at most 7 bytes. Like SQLite, resolve those blobs by
    // validating strictly and falling back to text when that fails.
    if payload_size <= 7 && matches!(slice[0], b'{' | b'[' | b'0'..=b'9') {
        return jsonb_error_position(slice) == 0;
    }
    true
}

pub fn convert_ref_dbtype_to_jsonb(val: ValueRef<'_>, strict: Conv) -> crate::Result<Jsonb> {
    match val {
        ValueRef::Text(text) => {
            let res = if text.subtype == TextSubtype::Json || matches!(strict, Conv::Strict) {
                // Like SQLite, text parsed as a JSON document stops at
                // the first NUL; a text value converted to a string
                // literal keeps it (Conv::ToString stringifies below).
                let str = text.as_str();
                let str = if matches!(strict, Conv::ToString) {
                    str
                } else {
                    &str[..str.find('\0').unwrap_or(str.len())]
                };
                Jsonb::from_str_with_mode(str, strict)
            } else {
                // Handle as a string literal otherwise
                // Escape backslashes first, then double quotes
                let mut str = text.replace('\\', "\\\\").replace('"', "\\\"");
                // Quote the string to make it a JSON string
                str.insert(0, '"');
                str.push('"');
                Jsonb::from_str(&str)
            };
            res.map_err(malformed_json_error)
        }
        ValueRef::Blob(blob) => {
            let bytes = blob;
            // Valid JSON can start with these whitespace characters
            let index = bytes
                .iter()
                .position(|&b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
                .unwrap_or(bytes.len());
            let slice = &bytes[index..];
            let json = match slice {
                // branch with no overlapping initial byte
                [b'"', ..] | [b'-', ..] | [b'0'..=b'2', ..] => parse_as_json_text(slice, strict)?,
                _ => match JsonbHeader::from_slice(0, slice) {
                    Ok((header, header_offset)) => {
                        let payload_size = header.payload_size();
                        let total_expected = match header_offset.checked_add(payload_size) {
                            Some(t) => t,
                            None => {
                                return Err(LimboError::ParseError("malformed JSON".to_string()))
                            }
                        };

                        if total_expected != slice.len() {
                            parse_as_json_text(slice, strict)?
                        } else {
                            // Validate the whole document, not just the outer
                            // header: later readers trust interior offsets and
                            // the UTF-8 well-formedness of text payloads.
                            let jsonb = Jsonb::from_raw_data(slice)?;
                            if jsonb.is_valid() {
                                jsonb
                            } else {
                                parse_as_json_text(slice, strict)?
                            }
                        }
                    }
                    Err(_) => parse_as_json_text(slice, strict)?,
                },
            };
            json.element_type()?;
            Ok(json)
        }
        ValueRef::Null => Ok(Jsonb::from_raw_data(
            JsonbHeader::make_null().into_bytes().as_bytes(),
        )?),
        ValueRef::Numeric(numeric) if matches!(strict, Conv::ToString) => {
            let text = Value::from(numeric).to_string();
            Jsonb::from_str_with_mode(&text, strict).map_err(malformed_json_error)
        }
        ValueRef::Numeric(Numeric::Float(float)) => {
            let float: f64 = float.into();
            // Handle infinity for JSON compatibility with SQLite (#4196)
            if float.is_infinite() {
                let json_str = if float.is_sign_negative() {
                    "-9.0e+999"
                } else {
                    "9.0e+999"
                };
                Jsonb::from_str(json_str).map_err(malformed_json_error)
            } else {
                let mut buff = ryu::Buffer::new();
                let s_ryu = buff.format(float);
                let mut s = Cow::Borrowed(s_ryu);

                if let Some(e_idx) = s_ryu.find('e') {
                    // Scientific notation case
                    s = Cow::Owned(String::with_capacity(s_ryu.len() + 4));
                    let inner = s.to_mut();
                    let mantissa = &s_ryu[..e_idx];
                    let exponent = &s_ryu[e_idx + 1..];

                    inner.push_str(mantissa);
                    if !mantissa.contains('.') {
                        inner.push_str(".0");
                    }
                    inner.push('e');
                    if !exponent.starts_with('-') && !exponent.starts_with('+') {
                        inner.push('+');
                    }
                    inner.push_str(exponent);
                }

                Jsonb::from_str(&s).map_err(malformed_json_error)
            }
        }
        ValueRef::Numeric(Numeric::Integer(int)) => {
            Jsonb::from_str(&int.to_string()).map_err(malformed_json_error)
        }
    }
}

pub(crate) fn ensure_blob_arg_is_jsonb(value: ValueRef<'_>) -> crate::Result<()> {
    if let ValueRef::Blob(blob) = value {
        if !is_jsonb_blob(blob) {
            crate::bail_constraint_error!("JSON cannot hold BLOB values")
        }
    }
    Ok(())
}

pub fn curry_convert_dbtype_to_jsonb(
    strict: Conv,
) -> impl FnOnce(ValueRef) -> crate::Result<Jsonb> {
    move |val| convert_dbtype_to_jsonb(val, strict)
}

pub fn json_array<I, E, V>(values: I) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let values = values.into_iter();
    let mut json = Jsonb::make_empty_array(values.len())?;

    for value in values {
        let value = value.as_value_ref();
        ensure_blob_arg_is_jsonb(value)?;
        let value = convert_dbtype_to_jsonb(value, Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }
    json.finalize_unsafe(ElementType::ARRAY)?;

    json_string_to_db_type(json, ElementType::ARRAY, OutputVariant::ElementType)
}

pub fn jsonb_array<I, E, V>(values: I) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let values = values.into_iter();
    let mut json = Jsonb::make_empty_array(values.len())?;

    for value in values {
        let value = value.as_value_ref();
        ensure_blob_arg_is_jsonb(value)?;
        let value = convert_dbtype_to_jsonb(value, Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }
    json.finalize_unsafe(ElementType::ARRAY)?;

    json_string_to_db_type(json, ElementType::ARRAY, OutputVariant::Binary)
}

pub fn json_array_length(
    value: &Value,
    path: Option<&Value>,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    if let Value::Null = value {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;

    if path.is_none() {
        let len = json.array_len()?;
        return Ok(Value::from_i64(len as i64));
    }

    let path = json_path_from_db_value(path.expect("We already checked none"), true)?;

    if let Some(path) = path {
        let mut op = SearchOperation::new(json.len() / 2)?;
        let _ = json.operate_on_path(&path, &mut op);
        if let Ok(len) = op.result().array_len() {
            return Ok(Value::from_i64(len as i64));
        }
    }
    Ok(Value::Null)
}

pub fn json_set<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }
    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;

        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;

        ensure_blob_arg_is_jsonb(second.as_value_ref())?;

        let path = json_path_from_db_value(&first, true)?;

        let value = convert_dbtype_to_jsonb(second, Conv::NotStrict)?;
        let mut op = SetOperation::new(value);
        if let Some(path) = path {
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, OutputVariant::String)
}

pub fn jsonb_set<I, E, V>(args: I, json_cache: &JsonCacheCell) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut args = args.into_iter();
    if args.len() == 0 {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let first_arg = args.next().ok_or_else(|| {
        crate::LimboError::InternalError("args should not be empty after length check".to_string())
    })?;
    let mut json = json_cache.get_or_insert_with(first_arg, make_jsonb_fn)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while args.len() > 1 {
        let first = args.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "args should have at least 2 elements in loop".to_string(),
            )
        })?;
        let path = json_path_from_db_value(&first, true)?;

        let second = args.next().ok_or_else(|| {
            crate::LimboError::InternalError("args should have second element in loop".to_string())
        })?;
        let value = convert_dbtype_to_jsonb(second, Conv::NotStrict)?;
        let mut op = SetOperation::new(value);
        if let Some(path) = path {
            let _ = json.operate_on_path(&path, &mut op);
        }
    }

    let el_type = json.element_type()?;

    json_string_to_db_type(json, el_type, OutputVariant::Binary)
}

/// Implements the -> operator. Always returns a proper JSON value.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_extract(
    value: impl AsValueRef,
    path: impl AsValueRef,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let value = value.as_value_ref();
    if let ValueRef::Null = value {
        return Ok(Value::Null);
    }

    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;
    if let Some(path) = json_path_from_db_value(&path, false)? {
        let mut op = SearchOperation::new(json.len())?;
        let res = json.operate_on_path(&path, &mut op);
        let extracted = op.result();
        if res.is_ok() {
            Ok(Value::Text(Text::json(extracted.to_string()?)))
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

/// Implements the ->> operator. Always returns a SQL representation of the JSON subcomponent.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_shift_extract(
    value: impl AsValueRef,
    path: impl AsValueRef,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value> {
    let value = value.as_value_ref();
    if let ValueRef::Null = value {
        return Ok(Value::Null);
    }
    let make_jsonb_fn = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let mut json = json_cache.get_or_insert_with(value, make_jsonb_fn)?;
    if let Some(path) = json_path_from_db_value(&path, false)? {
        let mut op = SearchOperation::new(json.len())?;
        let res = json.operate_on_path(&path, &mut op);
        let extracted = op.result();
        let element_type = match extracted.element_type() {
            Err(_) => return Ok(Value::Null),
            Ok(el) => el,
        };

        if res.is_ok() {
            Ok(json_string_to_db_type(
                extracted,
                element_type,
                OutputVariant::ElementTypePlain,
            )?)
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

/// Extracts a JSON value from a JSON object or array.
/// If there's only a single path, the return value might be either a TEXT or a database type.
/// https://sqlite.org/json1.html#the_json_extract_function
pub fn json_extract<I, E, V>(
    value: impl AsValueRef,
    paths: I,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let value = value.as_value_ref();
    if let ValueRef::Null = value {
        return Ok(Value::Null);
    }

    let paths = paths.into_iter();
    if paths.len() == 0 {
        return Ok(Value::Null);
    }
    let convert_to_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let jsonb = json_cache.get_or_insert_with(value, convert_to_jsonb)?;
    let (json, element_type) = jsonb_extract_internal(jsonb, paths)?;

    let result = json_string_to_db_type(json, element_type, OutputVariant::ElementType)?;

    Ok(result)
}

pub fn jsonb_extract<I, E, V>(
    value: &Value,
    paths: I,
    json_cache: &JsonCacheCell,
) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    if let Value::Null = value {
        return Ok(Value::Null);
    }

    let paths = paths.into_iter();
    if paths.len() == 0 {
        return Ok(Value::Null);
    }
    let convert_to_jsonb = curry_convert_dbtype_to_jsonb(Conv::Strict);
    let jsonb = json_cache.get_or_insert_with(value, convert_to_jsonb)?;

    let (json, element_type) = jsonb_extract_internal(jsonb, paths)?;
    let result = json_string_to_db_type(json, element_type, OutputVariant::ElementType)?;

    Ok(result)
}

fn jsonb_extract_internal<E, V>(value: Jsonb, mut paths: E) -> crate::Result<(Jsonb, ElementType)>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
{
    let null = Jsonb::from_raw_data(JsonbHeader::make_null().into_bytes().as_bytes())?;
    if paths.len() == 1 {
        let first_path = paths.next().ok_or_else(|| {
            crate::LimboError::InternalError("paths should have one element".to_string())
        })?;
        if let Some(path) = json_path_from_db_value(&first_path, true)? {
            let mut json = value;

            let mut op = SearchOperation::new(json.len())?;
            let res = json.operate_on_path(&path, &mut op);
            let extracted = op.result();
            let element_type = match extracted.element_type() {
                Err(_) => return Ok((null, ElementType::NULL)),
                Ok(el) => el,
            };
            if res.is_ok() {
                return Ok((extracted, element_type));
            } else {
                return Ok((null, ElementType::NULL));
            }
        } else {
            return Ok((null, ElementType::NULL));
        }
    }

    let mut json = value;
    let mut result = Jsonb::make_empty_array(json.len())?;

    // TODO: make an op to avoid creating new json for every path element
    for path in paths {
        let path = json_path_from_db_value(&path, true);
        if let Some(path) = path? {
            let mut op = SearchOperation::new(json.len())?;
            let res = json.operate_on_path(&path, &mut op);
            let extracted = op.result();
            if res.is_ok() {
                result.append_to_array_unsafe(&extracted.data());
            } else {
                result.append_to_array_unsafe(JsonbHeader::make_null().into_bytes().as_bytes());
            }
        } else {
            return Ok((null, ElementType::NULL));
        }
    }
    result.finalize_unsafe(ElementType::ARRAY)?;
    Ok((result, ElementType::ARRAY))
}

/// converts a `Jsonb` value to a db Value
///
/// # Arguments
///
/// - `jsonb` – the value to convert
/// - `element_type` – the element type of the jsonb
/// - `flag` – how the result should be formatted (null values will stay null).
///   - If the flag is `OutputVariant::Binary`, the result is a `Value::Blob`.
///   - If it is `OutputVariant::ElementType` and the `element_type` is text, the result has a subtype of `TestSubtype::Text`, with the outer quotes removed.
///   - If it is `OutputVariant::String` and the `element_type` is text, the result has a subtype of `TextSubtype::Text`.
///   - If the `element_type` is not text, the flag is ignored.
pub fn json_string_to_db_type(
    json: Jsonb,
    element_type: ElementType,
    flag: OutputVariant,
) -> crate::Result<Value> {
    if element_type == ElementType::NULL {
        return Ok(Value::Null);
    }
    if matches!(flag, OutputVariant::Binary) {
        return Ok(Value::Blob(json.data()));
    }
    let mut json_string = json.to_string()?;
    if matches!(flag, OutputVariant::String) {
        return Ok(Value::Text(Text::json(json_string)));
    }
    match element_type {
        ElementType::ARRAY | ElementType::OBJECT => {
            if matches!(flag, OutputVariant::ElementTypePlain) {
                Ok(Value::Text(Text::new(json_string)))
            } else {
                Ok(Value::Text(Text::json(json_string)))
            }
        }
        ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
            if matches!(
                flag,
                OutputVariant::ElementType | OutputVariant::ElementTypePlain
            ) {
                if element_type == ElementType::TEXT5 {
                    Ok(Value::Text(Text::new(json.scalar_string_value()?)))
                } else {
                    json_string.remove(json_string.len() - 1);
                    json_string.remove(0);
                    Ok(Value::Text(Text::new(unescape_string(&json_string))))
                }
            } else {
                Ok(Value::Text(Text::new(json_string)))
            }
        }
        ElementType::FLOAT5 | ElementType::FLOAT => {
            // Infinity parses from its 9e999 rendering to an infinite
            // f64, which is exactly what ->> and json_extract return.
            match json_string.parse::<f64>() {
                Ok(float_val) => Ok(Value::from_f64(float_val)),
                Err(_) => Err(LimboError::Constraint("malformed JSON".to_string())),
            }
        }
        ElementType::INT | ElementType::INT5 => {
            let result = i64::from_str(&json_string);
            if let Ok(int) = result {
                Ok(Value::from_i64(int))
            } else {
                let res = f64::from_str(&json_string);
                match res {
                    Ok(num) => Ok(Value::from_f64(num)),
                    Err(_) => Err(LimboError::Constraint("malformed JSON".to_string())),
                }
            }
        }
        ElementType::TRUE => Ok(Value::from_i64(1)),
        ElementType::FALSE => Ok(Value::from_i64(0)),
        _ => unreachable!(),
    }
}

pub fn json_type(value: impl AsValueRef, path: Option<impl AsValueRef>) -> crate::Result<Value> {
    let value = value.as_value_ref();
    if let ValueRef::Null = value {
        return Ok(Value::Null);
    }
    if path.is_none() {
        let json = convert_dbtype_to_jsonb(value, Conv::Strict)?;
        let element_type = json.element_type()?;

        // The type name is plain metadata text, not JSON: SQLite gives
        // it no subtype.
        return Ok(Value::Text(Text::new(String::from(element_type))));
    }
    let path_value = path.ok_or_else(|| {
        crate::LimboError::InternalError("path should be Some after is_none check".to_string())
    })?;
    if let Some(path) = json_path_from_db_value(&path_value, true)? {
        let mut json = convert_dbtype_to_jsonb(value, Conv::Strict)?;

        if let Ok(mut path) = json.navigate_path(&path, PathOperationMode::ReplaceExisting) {
            let target = path.pop().expect("Should exist");
            let element_type = if let Some(el_index) = target.get_array_index() {
                json.element_type_at(el_index)
            } else {
                json.element_type_at(target.field_value_index)
            }?;
            Ok(Value::Text(Text::new(String::from(element_type))))
        } else {
            Ok(Value::Null)
        }
    } else {
        Ok(Value::Null)
    }
}

fn json_path_from_db_value<'a>(
    path: &'a (impl AsValueRef + 'a),
    strict: bool,
) -> crate::Result<Option<JsonPath<'a>>> {
    let path = path.as_value_ref();
    let json_path = if strict {
        match path {
            ValueRef::Text(t) => json_path(t.as_str())?,
            ValueRef::Null => return Ok(None),
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    } else {
        match path {
            ValueRef::Text(t) => {
                if t.as_str().starts_with("$") {
                    json_path(t.as_str())?
                } else {
                    JsonPath {
                        elements: vec![
                            PathElement::Root(),
                            PathElement::Key(Cow::Borrowed(t.as_str()), true),
                        ],
                    }
                }
            }
            ValueRef::Null => return Ok(None),
            ValueRef::Numeric(Numeric::Integer(i)) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::ArrayLocator(Some(i as i32)),
                ],
            },
            ValueRef::Numeric(Numeric::Float(f)) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::Key(Cow::Owned(f64::from(f).to_string()), false),
                ],
            },
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    };

    Ok(Some(json_path))
}

pub fn json_error_position(json: impl AsValueRef) -> crate::Result<Value> {
    match json.as_value_ref() {
        ValueRef::Text(t) => {
            // Like SQLite, text parsed as a JSON document stops at the
            // first NUL.
            let text = t.as_str();
            let text = &text[..text.find('\0').unwrap_or(text.len())];
            match Jsonb::from_str(text) {
                Ok(_) => Ok(Value::from_i64(0)),
                Err(JsonError::Message { location, .. }) => {
                    if let Some(loc) = location {
                        // The parser reports a byte offset, but SQLite
                        // reports the position in characters (jsonErrorFunc
                        // counts the non-continuation bytes before the
                        // error), which differs for multibyte UTF-8 input.
                        let byte_offset = loc.min(text.len());
                        let char_offset = text.as_bytes()[..byte_offset]
                            .iter()
                            .filter(|&&b| !(0x80..0xC0).contains(&b))
                            .count();
                        Ok(Value::from_i64(char_offset as i64 + 1))
                    } else {
                        Err(crate::error::LimboError::InternalError(
                            "failed to determine json error position".into(),
                        ))
                    }
                }
                Err(JsonError::OutOfMemory) => Err(LimboError::OutOfMemory),
            }
        }
        ValueRef::Blob(blob) => {
            // SQLite classifies the raw blob: one that looks like
            // JSONB reports the byte offset of its first malformed
            // element (0 when fully valid). Anything else is read as
            // text, which for a blob stops at the first NUL. The lossy
            // UTF-8 conversion stands in for SQLite's byte-wise
            // parser: each bad byte becomes one replacement character,
            // so error positions still line up.
            if looks_like_jsonb_blob(blob) {
                return Ok(Value::from_i64(jsonb_error_position(blob) as i64));
            }
            let zero_pos = blob.iter().position(|&b| b == 0).unwrap_or(blob.len());
            match Jsonb::from_str(&String::from_utf8_lossy(&blob[..zero_pos])) {
                Ok(_) => Ok(Value::from_i64(0)),
                Err(JsonError::Message { location, .. }) => {
                    Ok(Value::from_i64(location.map_or(1, |loc| loc as i64 + 1)))
                }
                Err(JsonError::OutOfMemory) => Err(LimboError::OutOfMemory),
            }
        }
        ValueRef::Null => Ok(Value::Null),
        _ => Ok(Value::from_i64(0)),
    }
}

/// Constructs a JSON object from a list of values that represent key-value pairs.
/// The number of values must be even, and the first value of each pair (which represents the map key)
/// must be a TEXT value. The second value of each pair can be any JSON value (which represents the map value)
pub fn json_object<I, E, V>(values: I) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() % 2 != 0 {
        bail_constraint_error!("json_object() requires an even number of arguments")
    }
    let mut json = Jsonb::make_empty_obj(values.len() * 50)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while values.len() > 1 {
        let first = values.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "values should have at least 2 elements in loop".to_string(),
            )
        })?;
        let first = first.as_value_ref();
        if first.value_type() != ValueType::Text {
            bail_constraint_error!("json_object() labels must be TEXT")
        }
        let key = convert_dbtype_to_jsonb(first, Conv::ToString)?;
        json.append_jsonb_to_end(key.data());

        let second = values.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "values should have second element in loop".to_string(),
            )
        })?;
        ensure_blob_arg_is_jsonb(second.as_value_ref())?;
        let value = convert_dbtype_to_jsonb(second, Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }

    json.finalize_unsafe(ElementType::OBJECT)?;

    json_string_to_db_type(json, ElementType::OBJECT, OutputVariant::String)
}

pub fn jsonb_object<I, E, V>(values: I) -> crate::Result<Value>
where
    V: AsValueRef,
    E: ExactSizeIterator<Item = V>,
    I: IntoIterator<IntoIter = E, Item = V>,
{
    let mut values = values.into_iter();
    if values.len() % 2 != 0 {
        bail_constraint_error!("json_object() requires an even number of arguments")
    }
    let mut json = Jsonb::make_empty_obj(values.len() * 50)?;

    // TODO: when `array_chunks` is stabilized we can chunk by 2 here
    while values.len() > 1 {
        let first = values.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "values should have at least 2 elements in loop".to_string(),
            )
        })?;
        let first = first.as_value_ref();
        if first.value_type() != ValueType::Text {
            bail_constraint_error!("json_object() labels must be TEXT")
        }
        let key = convert_dbtype_to_jsonb(first, Conv::ToString)?;
        json.append_jsonb_to_end(key.data());

        let second = values.next().ok_or_else(|| {
            crate::LimboError::InternalError(
                "values should have second element in loop".to_string(),
            )
        })?;
        ensure_blob_arg_is_jsonb(second.as_value_ref())?;
        let value = convert_dbtype_to_jsonb(second, Conv::NotStrict)?;
        json.append_jsonb_to_end(value.data());
    }

    json.finalize_unsafe(ElementType::OBJECT)?;

    json_string_to_db_type(json, ElementType::OBJECT, OutputVariant::Binary)
}

/// json_valid() flag: X is text that is strict RFC 8259 JSON.
pub const JSON_VALID_FLAG_TEXT_STRICT: i64 = 0x01;
/// json_valid() flag: X is text that is JSON5.
pub const JSON_VALID_FLAG_TEXT_JSON5: i64 = 0x02;
/// json_valid() flag: X is a blob that superficially looks like JSONB.
pub const JSON_VALID_FLAG_BLOB_PROBABLE: i64 = 0x04;
/// json_valid() flag: X is a blob that is valid JSONB.
pub const JSON_VALID_FLAG_BLOB_STRICT: i64 = 0x08;

/// Implements json_valid(X, Y). Y is a bitmask of the JSON_VALID_FLAG_*
/// constants picking which representations count as valid, and X is
/// valid if any selected check passes. The one-argument json_valid(X)
/// is defined by SQLite as json_valid(X, 1), so callers pass
/// JSON_VALID_FLAG_TEXT_STRICT when Y is absent.
pub fn is_json_valid(
    json_value: impl AsValueRef,
    flags_value: impl AsValueRef,
) -> crate::Result<Value> {
    let flags = match flags_value.as_value_ref() {
        ValueRef::Numeric(Numeric::Integer(int)) => int,
        ValueRef::Numeric(Numeric::Float(float)) => f64::from(float) as i64,
        ValueRef::Text(text) => str_to_i64(text.as_str()).unwrap_or(0),
        ValueRef::Blob(blob) => str_to_i64(String::from_utf8_lossy(blob)).unwrap_or(0),
        ValueRef::Null => 0,
    };
    if !(1..=15).contains(&flags) {
        // SQLite raises this through sqlite3_result_error, which is
        // error class SQLITE_ERROR; a Constraint error would surface
        // as SQLITE_CONSTRAINT through the C API.
        return Err(LimboError::SqlError(
            "FLAGS parameter to json_valid() must be between 1 and 15".to_string(),
        ));
    }

    let text_checks = |slice: &[u8]| -> crate::Result<bool> {
        // With neither text flag selected the answer is already 0.
        // SQLite does not parse at all in that case, so a huge input
        // must not turn into an out-of-memory error here either.
        if flags & (JSON_VALID_FLAG_TEXT_STRICT | JSON_VALID_FLAG_TEXT_JSON5) == 0 {
            return Ok(false);
        }
        match parse_as_json_text_tracking(slice) {
            Ok((_, info)) => Ok(if info.has_json5 {
                flags & JSON_VALID_FLAG_TEXT_JSON5 != 0
            } else {
                flags & (JSON_VALID_FLAG_TEXT_STRICT | JSON_VALID_FLAG_TEXT_JSON5) != 0
            }),
            Err(LimboError::OutOfMemory) => Err(LimboError::OutOfMemory),
            Err(_) => Ok(false),
        }
    };

    let json_value = json_value.as_value_ref();
    let valid = match json_value {
        ValueRef::Null => return Ok(Value::Null),
        ValueRef::Blob(blob) => {
            // SQLite classifies the raw blob. The probable check is the
            // shallow one: a valid outer wrapper with malformed contents
            // passes flag 0x04 but fails flag 0x08.
            if looks_like_jsonb_blob(blob) {
                flags & JSON_VALID_FLAG_BLOB_PROBABLE != 0
                    || (flags & JSON_VALID_FLAG_BLOB_STRICT != 0 && jsonb_error_position(blob) == 0)
            } else {
                text_checks(blob)?
            }
        }
        ValueRef::Text(text) => text_checks(text.as_str().as_bytes())?,
        ValueRef::Numeric(Numeric::Float(float)) => {
            let float: f64 = float.into();
            if float.is_infinite() {
                flags & JSON_VALID_FLAG_TEXT_JSON5 != 0
            } else {
                flags & (JSON_VALID_FLAG_TEXT_STRICT | JSON_VALID_FLAG_TEXT_JSON5) != 0
            }
        }
        ValueRef::Numeric(_) => {
            flags & (JSON_VALID_FLAG_TEXT_STRICT | JSON_VALID_FLAG_TEXT_JSON5) != 0
        }
    };
    Ok(Value::from_i64(i64::from(valid)))
}

pub fn json_quote(value: impl AsValueRef) -> crate::Result<Value> {
    let value = value.as_value_ref();
    match value {
        ValueRef::Text(ref t) => {
            // If X is a JSON value returned by another JSON function,
            // then this function is a no-op
            if t.subtype == TextSubtype::Json {
                // Should just return the json value with no quotes
                return Ok(value.to_owned()?);
            }

            let mut escaped_value = String::with_capacity(t.value.len() + 4);
            escaped_value.push('"');

            for c in t.as_str().chars() {
                match c {
                    '"' | '\\' => {
                        escaped_value.push('\\');
                        escaped_value.push(c);
                    }
                    '\u{0008}' => escaped_value.push_str("\\b"),
                    '\u{000c}' => escaped_value.push_str("\\f"),
                    '\n' => escaped_value.push_str("\\n"),
                    '\r' => escaped_value.push_str("\\r"),
                    '\t' => escaped_value.push_str("\\t"),
                    c if (c as u32) < 0x20 => {
                        let _ = write!(escaped_value, "\\u{:04x}", c as u32);
                    }
                    c => escaped_value.push(c),
                }
            }
            escaped_value.push('"');

            Ok(Value::Text(Text::json(escaped_value)))
        }
        // Numbers are unquoted in json, but must be returned as TEXT
        ValueRef::Numeric(n) => match n {
            crate::numeric::Numeric::Integer(i) => Ok(Value::Text(Text::json(i.to_string()))),
            crate::numeric::Numeric::Float(_) => {
                let json = convert_ref_dbtype_to_jsonb(ValueRef::Numeric(n), Conv::Strict)?;
                Ok(Value::Text(Text::json(json.to_string()?)))
            }
        },
        ValueRef::Blob(blob) => {
            if is_jsonb_blob(blob) {
                let json = Jsonb::from_raw_data(blob)?;
                Ok(Value::Text(Text::json(json.to_string()?)))
            } else {
                crate::bail_constraint_error!("JSON cannot hold BLOB values")
            }
        }
        ValueRef::Null => Ok(Value::Text(Text::json("null".to_string()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numeric::Numeric;
    use crate::types::Value;

    #[test]
    fn json_valid_bad_flags_are_a_plain_sql_error_not_a_constraint() {
        // SQLite raises the FLAGS error through sqlite3_result_error,
        // which is error class SQLITE_ERROR. The C bindings map
        // LimboError::Constraint to SQLITE_CONSTRAINT, so the variant
        // matters to C API users, not just the message.
        for flags in [Value::from_i64(0), Value::from_i64(16), Value::Null] {
            let err = is_json_valid(Value::build_text("{}"), &flags).unwrap_err();
            assert!(matches!(err, LimboError::SqlError(_)), "{err:?}");
        }
    }

    #[test]
    fn test_jsonb_preserves_malformed_json_error_and_cache_reusability() {
        let cache = JsonCacheCell::new();
        let invalid = Value::build_text("{");

        assert!(matches!(
            jsonb(&invalid, &cache),
            Err(LimboError::ParseError(message)) if message == "malformed JSON"
        ));

        let valid = Value::build_text(r#"{"key":"value"}"#);
        assert!(jsonb(&valid, &cache).is_ok());
    }

    #[test]
    fn test_get_json_valid_json5() {
        let input = Value::build_text("{ key: 'value' }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_infinity() {
        let input = Value::build_text("{ \"key\": Infinity }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_negative_infinity() {
        let input = Value::build_text("{ \"key\": -Infinity }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":-9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_nan() {
        let input = Value::build_text("{ \"key\": NaN }");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":null}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = Value::build_text("{ key: value }");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = Value::build_text("{\"key\":\"value\"}");
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = Value::build_text("{key:\"value\"");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_blob_valid_jsonb() {
        let binary_json = crate::alloc::vec![124, 55, 104, 101, 121, 39, 121, 111];
        let input = Value::Blob(binary_json);
        let result = get_json(&input, None).unwrap();
        if let Value::Text(result_str) = result {
            assert!(result_str.as_str().contains(r#"{"hey":"yo"}"#));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_get_json_blob_invalid_jsonb() {
        let binary_json: crate::ValueBlob = crate::alloc::vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
        let input = Value::Blob(binary_json);
        let result = get_json(&input, None);
        println!("{result:?}");
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_non_text() {
        let input = Value::Null;
        let result = get_json(&input, None).unwrap();
        if let Value::Null = result {
            // Test passed
        } else {
            panic!("Expected Value::Null");
        }
    }

    #[test]
    fn test_json_array_simple() {
        let text = Value::build_text("value1");
        let json = Value::Text(Text::json("\"value2\"".to_string()));
        let input = [text, json, Value::from_i64(1), Value::from_f64(1.1)];

        let result = json_array(&input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "[\"value1\",\"value2\",1,1.1]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_array_with_infinity() {
        let infinity = Value::from_f64(f64::INFINITY);
        let neg_infinity = Value::from_f64(f64::NEG_INFINITY);
        let input = [Value::from_i64(1), infinity, neg_infinity];

        let result = json_array(&input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "[1,9.0e+999,-9.0e+999]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_object_with_infinity() {
        let infinity = Value::from_f64(f64::INFINITY);
        let key = Value::build_text("k");
        let input = [key, infinity];

        let result = json_object(&input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), r#"{"k":9.0e+999}"#);
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_object_with_negative_infinity() {
        let neg_infinity = Value::from_f64(f64::NEG_INFINITY);
        let key = Value::build_text("k");
        let input = [key, neg_infinity];

        let result = json_object(&input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), r#"{"k":-9.0e+999}"#);
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_with_infinity() {
        let infinity = Value::from_f64(f64::INFINITY);
        let result = get_json(&infinity, None).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "9e999");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text, got {result:?}");
        }
    }

    #[test]
    fn test_json_with_negative_infinity() {
        let neg_infinity = Value::from_f64(f64::NEG_INFINITY);
        let result = get_json(&neg_infinity, None).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "-9e999");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text, got {result:?}");
        }
    }

    #[test]
    fn test_json_array_empty() {
        let input: [Value; 0] = [];

        let result = json_array(input).unwrap();
        if let Value::Text(res) = result {
            assert_eq!(res.as_str(), "[]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected Value::Text");
        }
    }

    #[test]
    fn test_json_array_blob_invalid() {
        let blob = Value::from_slice(b"1").expect(crate::alloc::ALLOC_ERR_MSG);

        let input = [blob];

        let result = json_array(&input);

        match result {
            Ok(_) => panic!("Expected error for blob input"),
            Err(e) => assert!(e.to_string().contains("JSON cannot hold BLOB values")),
        }
    }

    #[test]
    fn test_json_array_length() {
        let input = Value::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_null() {
        let input = Value::Null;
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_json_array_length_empty() {
        let input = Value::build_text("[]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_root() {
        let input = Value::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, Some(&Value::build_text("$")), &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_not_array() {
        let input = Value::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result = json_array_length(&input, None, &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_via_prop() {
        let input = Value::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$.one")), &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_via_index() {
        let input = Value::build_text("[[1,2,3,4]]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$[0]")), &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_via_index_not_array() {
        let input = Value::build_text("[1,2,3,4]");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$[2]")), &json_cache).unwrap();
        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_array_length_via_index_bad_prop() {
        let input = Value::build_text("{one: [1,2,3,4]}");
        let json_cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::build_text("$.two")), &json_cache).unwrap();
        assert_eq!(Value::Null, result);
    }

    #[test]
    fn test_json_array_length_simple_json_subtype() {
        let input = Value::build_text("[1,2,3]");
        let json_cache = JsonCacheCell::new();
        let wrapped = get_json(&input, None).unwrap();
        let result = json_array_length(&wrapped, None, &json_cache).unwrap();

        if let Value::Numeric(Numeric::Integer(res)) = result {
            assert_eq!(res, 3);
        } else {
            panic!("Expected Value::Numeric(Numeric::Integer)");
        }
    }

    #[test]
    fn test_json_extract_missing_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            Value::build_text("{\"a\":2}"),
            &[Value::build_text("$.x")],
            &json_cache,
        );

        match result {
            Ok(Value::Null) => (),
            _ => panic!("Expected null result, got: {result:?}"),
        }
    }
    #[test]
    fn test_json_extract_null_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(Value::build_text("{\"a\":2}"), &[Value::Null], &json_cache);

        match result {
            Ok(Value::Null) => (),
            _ => panic!("Expected null result, got: {result:?}"),
        }
    }

    #[test]
    fn test_json_path_invalid() {
        let json_cache = JsonCacheCell::new();
        let result = json_extract(
            Value::build_text("{\"a\":2}"),
            &[Value::from_f64(1.1)],
            &json_cache,
        );

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.to_string().contains("JSON path error")),
        }
    }

    #[test]
    fn test_json_error_position_no_error() {
        let input = Value::build_text("[1,2,3]");
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::from_i64(0));
    }

    #[test]
    fn test_json_error_position_no_error_more() {
        let input = Value::build_text(r#"{"a":55,"b":72 , }"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::from_i64(0));
    }

    #[test]
    fn test_json_error_position_object() {
        let input = Value::build_text(r#"{"a":55,"b":72,,}"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::from_i64(16));
    }

    #[test]
    fn test_json_error_position_array() {
        let input = Value::build_text(r#"["a",55,"b",72,,]"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::from_i64(16));
    }

    #[test]
    fn test_json_error_position_null() {
        let input = Value::Null;
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_json_error_position_integer() {
        let input = Value::from_i64(5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::from_i64(0));
    }

    #[test]
    fn test_json_error_position_float() {
        let input = Value::from_f64(-5.5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, Value::from_i64(0));
    }

    #[test]
    fn test_json_object_simple() {
        let key = Value::build_text("key");
        let value = Value::build_text("value");
        let input = [key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_multiple_values() {
        let text_key = Value::build_text("text_key");
        let text_value = Value::build_text("text_value");
        let json_key = Value::build_text("json_key");
        let json_value = Value::Text(Text::json(r#"{"json":"value","number":1}"#.to_string()));
        let integer_key = Value::build_text("integer_key");
        let integer_value = Value::from_i64(1);
        let float_key = Value::build_text("float_key");
        let float_value = Value::from_f64(1.1);
        let null_key = Value::build_text("null_key");
        let null_value = Value::Null;

        let input = [
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
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(
            json_text.as_str(),
            r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
        );
    }

    #[test]
    fn test_json_object_json_value_is_rendered_as_json() {
        let key = Value::build_text("key");
        let value = Value::Text(Text::json(r#"{"json":"value"}"#.to_string()));
        let input = [key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":{"json":"value"}}"#);
    }

    #[test]
    fn test_json_object_json_text_value_is_rendered_as_regular_text() {
        let key = Value::build_text("key");
        let value = Value::Text(Text::new(r#"{"json":"value"}"#));
        let input = [key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"{\"json\":\"value\"}"}"#);
    }

    #[test]
    fn test_json_object_nested() {
        let key = Value::build_text("key");
        let value = Value::build_text("value");
        let input = [key, value];

        let parent_key = Value::build_text("parent_key");
        let parent_value = json_object(&input).unwrap();
        let parent_input = [parent_key, parent_value];

        let result = json_object(&parent_input).unwrap();

        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"parent_key":{"key":"value"}}"#);
    }

    #[test]
    fn test_json_object_duplicated_keys() {
        let key = Value::build_text("key");
        let value = Value::build_text("value");
        let input = [key.clone(), value.clone(), key, value];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value","key":"value"}"#);
    }

    #[test]
    fn test_json_object_empty() {
        let input: [Value; 0] = [];

        let result = json_object(&input).unwrap();
        let Value::Text(json_text) = result else {
            panic!("Expected Value::Text");
        };
        assert_eq!(json_text.as_str(), r#"{}"#);
    }

    #[test]
    fn test_json_object_non_text_key() {
        let key = Value::from_i64(1);
        let value = Value::build_text("value");
        let input = [key, value];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for non-TEXT key"),
            Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
        }
    }

    #[test]
    fn test_json_odd_number_of_values() {
        let key = Value::build_text("key");
        let value = Value::build_text("value");
        let input = [key.clone(), value, key];

        assert!(json_object(&input).is_err());
    }

    #[test]
    fn test_json_object_escapes_special_characters() {
        let cases = [
            // (key, value, expected_json)
            ("key", r"Hello\World", r#"{"key":"Hello\\World"}"#),
            (
                r"key\with\backslash",
                "value",
                r#"{"key\\with\\backslash":"value"}"#,
            ),
            ("key", "Hello\nWorld", r#"{"key":"Hello\nWorld"}"#),
            ("key", "Hello\tWorld", r#"{"key":"Hello\tWorld"}"#),
            ("key", "Hello\rWorld", r#"{"key":"Hello\rWorld"}"#),
            ("key", "Hello\x01World", r#"{"key":"Hello\u0001World"}"#),
            ("key", "Hello\x08\x0cWorld", r#"{"key":"Hello\b\fWorld"}"#),
            ("key", "ä\n", "{\"key\":\"ä\\n\"}"),
            ("key", "日本語\t", "{\"key\":\"日本語\\t\"}"),
        ];

        for (key, value, expected) in cases {
            let input = [Value::build_text(key), Value::build_text(value)];
            let result = json_object(&input).unwrap();
            let Value::Text(json_text) = result else {
                panic!("Expected Value::Text");
            };
            assert_eq!(
                json_text.as_str(),
                expected,
                "Failed for key={key:?}, value={value:?}"
            );
        }
    }

    #[test]
    fn test_json_path_from_db_value_root_strict() {
        let path = Value::Text(Text::new("$"));

        let result = json_path_from_db_value(&path, true);
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
    fn test_json_path_from_db_value_root_non_strict() {
        let path = Value::Text(Text::new("$"));

        let result = json_path_from_db_value(&path, false);
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
    fn test_json_path_from_db_value_named_strict() {
        let path = Value::Text(Text::new("field"));

        assert!(json_path_from_db_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_db_value_named_non_strict() {
        let path = Value::Text(Text::new("field"));

        let result = json_path_from_db_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, true)] if *field == "field" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_path_from_db_value_integer_strict() {
        let path = Value::from_i64(3);
        assert!(json_path_from_db_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_db_value_integer_non_strict() {
        let path = Value::from_i64(3);

        let result = json_path_from_db_value(&path, false);
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
    fn test_json_path_from_db_value_null_strict() {
        let path = Value::Null;

        let result = json_path_from_db_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_db_value_null_non_strict() {
        let path = Value::Null;

        let result = json_path_from_db_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_db_value_float_strict() {
        let path = Value::from_f64(1.23);

        assert!(json_path_from_db_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_db_value_float_non_strict() {
        let path = Value::from_f64(1.23);

        let result = json_path_from_db_value(&path, false);
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
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("{}"),
                Value::build_text("$.field"),
                Value::build_text("value"),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#);
    }

    #[test]
    fn test_json_set_replace_field() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text(r#"{"field":"old_value"}"#),
                Value::build_text("$.field"),
                Value::build_text("new_value"),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"field":"new_value"}"#
        );
    }

    #[test]
    fn test_json_set_set_deeply_nested_key() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("{}"),
                Value::build_text("$.object.doesnt.exist"),
                Value::build_text("value"),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"object":{"doesnt":{"exist":"value"}}}"#
        );
    }

    #[test]
    fn test_json_set_add_value_to_empty_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("[]"),
                Value::build_text("$[0]"),
                Value::build_text("value"),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"["value"]"#);
    }

    #[test]
    fn test_json_set_add_value_to_nonexistent_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("{}"),
                Value::build_text("$.some_array[0]"),
                Value::from_i64(123),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"some_array":[123]}"#
        );
    }

    #[test]
    fn test_json_set_add_value_to_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("[123]"),
                Value::build_text("$[1]"),
                Value::from_i64(456),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[123,456]");
    }

    #[test]
    fn test_json_set_add_value_to_array_out_of_bounds() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("[123]"),
                Value::build_text("$[200]"),
                Value::from_i64(456),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[123]");
    }

    #[test]
    fn test_json_set_replace_value_in_array() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("[123]"),
                Value::build_text("$[0]"),
                Value::from_i64(456),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[456]");
    }

    #[test]
    fn test_json_set_null_path() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[Value::build_text("{}"), Value::Null, Value::from_i64(456)],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "{}");
    }

    #[test]
    fn test_json_set_multiple_keys() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("[123]"),
                Value::build_text("$[0]"),
                Value::from_i64(456),
                Value::build_text("$[1]"),
                Value::from_i64(789),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), "[456,789]");
    }

    #[test]
    fn test_json_set_add_array_in_nested_object() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("{}"),
                Value::build_text("$.object[0].field"),
                Value::from_i64(123),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap().to_text().unwrap(),
            r#"{"object":[{"field":123}]}"#
        );
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("{}"),
                Value::build_text("$.object[0][0]"),
                Value::from_i64(123),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"object":[[123]]}"#);
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object_out_of_bounds() {
        let json_cache = JsonCacheCell::new();
        let result = json_set(
            &[
                Value::build_text("{}"),
                Value::build_text("$.object[123].another"),
                Value::build_text("value"),
                Value::build_text("$.field"),
                Value::build_text("value"),
            ],
            &json_cache,
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap().to_text().unwrap(), r#"{"field":"value"}"#,);
    }

    #[test]
    fn test_is_jsonb_blob_rejects_scalar_like_overlap_header() {
        // `|` is 0x7C: OBJECT with a 7-byte inline payload, so this is exactly at
        // the length where a scalar blob and a JSONB object are indistinguishable
        // by header alone.
        let overlapping_scalar = b"|1234567";
        assert_eq!(overlapping_scalar.len(), 8);
        assert!(!is_jsonb_blob(overlapping_scalar));
    }

    /// Object with a payload larger than a scalar blob, so header inspection
    /// alone accepts it, but the TEXT5 key holds bytes that are not UTF-8.
    /// Reading such a key used to reach `str::from_utf8_unchecked`.
    #[test]
    fn test_is_jsonb_blob_rejects_invalid_utf8_key() {
        let invalid_utf8_key = b"\x9C\x79aaaaaa\xF0\x00";
        assert!(!is_jsonb_blob(invalid_utf8_key));
    }
}
