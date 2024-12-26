mod de;
mod error;
mod path;
mod ser;

use std::rc::Rc;

pub use crate::json::de::from_str;
pub use crate::json::ser::to_string;
use crate::types::{LimboText, OwnedValue, TextSubtype};
use indexmap::IndexMap;
use path::get_json_val_by_path;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(untagged)]
pub enum Val {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<Val>),
    Object(IndexMap<String, Val>),
}

pub fn get_json(json_value: &OwnedValue) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => {
            if t.subtype == TextSubtype::Json {
                return Ok(json_value.to_owned());
            }

            match crate::json::from_str::<Val>(&t.value) {
                Ok(json) => {
                    let json = crate::json::to_string(&json).unwrap();
                    Ok(OwnedValue::Text(LimboText::json(Rc::new(json))))
                }
                Err(_) => {
                    crate::bail_parse_error!("malformed JSON")
                }
            }
        }
        OwnedValue::Blob(b) => {
            if let Ok(json) = jsonb::from_slice(b) {
                Ok(OwnedValue::Text(LimboText::json(Rc::new(json.to_string()))))
            } else {
                crate::bail_parse_error!("malformed JSON");
            }
        }
        _ => Ok(json_value.to_owned()),
    }
}

pub fn json_array(values: Vec<&OwnedValue>) -> crate::Result<OwnedValue> {
    let mut s = String::new();
    s.push('[');

    for (idx, value) in values.iter().enumerate() {
        match value {
            OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
            OwnedValue::Text(t) => {
                if t.subtype == TextSubtype::Json {
                    s.push_str(&t.value);
                } else {
                    match crate::json::to_string(&*t.value) {
                        Ok(json) => s.push_str(&json),
                        Err(_) => crate::bail_parse_error!("malformed JSON"),
                    }
                }
            }
            OwnedValue::Integer(i) => match crate::json::to_string(&i) {
                Ok(json) => s.push_str(&json),
                Err(_) => crate::bail_parse_error!("malformed JSON"),
            },
            OwnedValue::Float(f) => match crate::json::to_string(&f) {
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
    let path = match json_path {
        Some(OwnedValue::Text(t)) => Some(t.value.to_string()),
        Some(OwnedValue::Integer(i)) => Some(i.to_string()),
        Some(OwnedValue::Float(f)) => Some(f.to_string()),
        _ => None::<String>,
    };

    let top_val = match json_value {
        OwnedValue::Text(ref t) => crate::json::from_str::<Val>(&t.value),
        OwnedValue::Blob(b) => match jsonb::from_slice(b) {
            Ok(j) => {
                let json = j.to_string();
                crate::json::from_str(&json)
            }
            Err(_) => crate::bail_parse_error!("malformed JSON"),
        },
        _ => return Ok(OwnedValue::Integer(0)),
    };

    let Ok(top_val) = top_val else {
        crate::bail_parse_error!("malformed JSON")
    };

    let arr_val = if let Some(path) = path {
        match get_json_val_by_path(&top_val, &path) {
            Ok(Some(val)) => val,
            Ok(None) => return Ok(OwnedValue::Null),
            Err(e) => return Err(e),
        }
    } else {
        &top_val
    };

    if let Val::Array(val) = &arr_val {
        return Ok(OwnedValue::Integer(val.len() as i64));
    }
    Ok(OwnedValue::Integer(0))
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
        let input = vec![
            &text,
            &json,
            &OwnedValue::Integer(1),
            &OwnedValue::Float(1.1),
        ];

        let result = json_array(input).unwrap();
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

        let result = json_array(input).unwrap();
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

        let input = vec![&blob];

        let result = json_array(input);

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
}
