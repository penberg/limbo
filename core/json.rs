use std::rc::Rc;

use crate::types::OwnedValue;
use serde_json::Value;

pub fn get_json(json_value: &OwnedValue) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => {
            if let Ok(json) = json5::from_str::<Value>(t.as_str()) {
                Ok(OwnedValue::Text(Rc::new(json.to_string())))
            } else {
                crate::bail_parse_error!("malformed JSON");
            }
        }
        OwnedValue::Blob(b) => {
            if let Ok(json) = jsonb::from_slice(b) {
                Ok(OwnedValue::Text(Rc::new(json.to_string())))
            } else {
                crate::bail_parse_error!("malformed JSON");
            }
        }
        _ => Ok(json_value.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OwnedValue;

    #[test]
    fn test_get_json_valid_json5() {
        let input = OwnedValue::Text(Rc::new("{ key: 'value' }".to_string())); // Valid JSON5
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.contains("\"key\":\"value\""));
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = OwnedValue::Text(Rc::new("{ key: value }".to_string()));
        let result = get_json(&input);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = OwnedValue::Text(Rc::new("{\"key\":\"value\"}".to_string()));
        let result = get_json(&input).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.contains("\"key\":\"value\""));
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = OwnedValue::Text(Rc::new("{key:\"value\"".to_string())); // Invalid JSON
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
            assert!(result_str.contains("\"asd\":\"adf\""));
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
}
