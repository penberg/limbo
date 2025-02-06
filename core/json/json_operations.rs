use std::collections::VecDeque;

use crate::{
    json::{mutate_json_by_path, Target},
    types::OwnedValue,
};

use super::{convert_json_to_db_type, get_json_value, json_path::json_path, Val};

/// Represents a single patch operation in the merge queue.
///
/// Used internally by the `merge_patch` function to track the path and value
/// for each pending merge operation.
#[derive(Debug, Clone)]
struct PatchOperation {
    path_start: usize,
    path_len: usize,
    patch: Val,
}

/// The function follows RFC 7386 JSON Merge Patch semantics:
/// * If the patch is null, the target is replaced with null
/// * If the patch contains a scalar value, the target is replaced with that value
/// * If both target and patch are objects, the patch is recursively applied
/// * null values in the patch result in property removal from the target
pub fn json_patch(target: &OwnedValue, patch: &OwnedValue) -> crate::Result<OwnedValue> {
    match (target, patch) {
        (OwnedValue::Blob(_), _) | (_, OwnedValue::Blob(_)) => {
            crate::bail_constraint_error!("blob is not supported!");
        }
        _ => (),
    }

    let mut parsed_target = get_json_value(target)?;
    let parsed_patch = get_json_value(patch)?;
    let mut patcher = JsonPatcher::new(16);
    patcher.apply_patch(&mut parsed_target, parsed_patch);

    convert_json_to_db_type(&parsed_target, false)
}

#[derive(Debug)]
struct JsonPatcher {
    queue: VecDeque<PatchOperation>,
    path_storage: Vec<usize>,
}

impl JsonPatcher {
    fn new(queue_capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(queue_capacity),
            path_storage: Vec::with_capacity(256),
        }
    }

    fn apply_patch(&mut self, target: &mut Val, patch: Val) {
        self.queue.push_back(PatchOperation {
            path_start: 0,
            path_len: 0,
            patch,
        });

        while let Some(op) = self.queue.pop_front() {
            if let Some(current) = self.navigate_to_target(target, op.path_start, op.path_len) {
                self.apply_operation(current, op);
            } else {
                continue;
            }
        }
    }

    fn navigate_to_target<'a>(
        &self,
        target: &'a mut Val,
        path_start: usize,
        path_len: usize,
    ) -> Option<&'a mut Val> {
        let mut current = target;
        for i in 0..path_len {
            let key = self.path_storage[path_start + i];
            if let Val::Object(ref mut obj) = current {
                current = &mut obj
                    .get_mut(key)
                    .unwrap_or_else(|| {
                        panic!("Invalid path at depth {}: key '{}' not found", i, key)
                    })
                    .1;
            } else {
                return None;
            }
        }
        Some(current)
    }

    fn apply_operation(&mut self, current: &mut Val, operation: PatchOperation) {
        let path_start = operation.path_start;
        let path_len = operation.path_len;
        match (current, operation.patch) {
            (current_val, Val::Null) => *current_val = Val::Removed,
            (Val::Object(target_map), Val::Object(patch_map)) => {
                self.merge_objects(target_map, patch_map, path_start, path_len);
            }
            (current_val, patch_val) => *current_val = patch_val,
        }
    }

    fn merge_objects(
        &mut self,
        target_map: &mut Vec<(String, Val)>,
        patch_map: Vec<(String, Val)>,
        path_start: usize,
        path_len: usize,
    ) {
        for (key, patch_val) in patch_map {
            self.process_key_value(target_map, key, patch_val, path_start, path_len);
        }
    }

    fn process_key_value(
        &mut self,
        target_map: &mut Vec<(String, Val)>,
        key: String,
        patch_val: Val,
        path_start: usize,
        path_len: usize,
    ) {
        if let Some(pos) = target_map
            .iter()
            .position(|(target_key, _)| target_key == &key)
        {
            self.queue_nested_patch(pos, patch_val, path_start, path_len);
        } else if !matches!(patch_val, Val::Null) {
            target_map.push((key, Val::Object(vec![])));
            self.queue_nested_patch(target_map.len() - 1, patch_val, path_start, path_len)
        }
    }

    fn queue_nested_patch(&mut self, pos: usize, val: Val, path_start: usize, path_len: usize) {
        let new_path_start = self.path_storage.len();
        let new_path_len = path_len + 1;
        for i in 0..path_len {
            self.path_storage.push(self.path_storage[path_start + i]);
        }
        self.path_storage.push(pos);
        self.queue.push_back(PatchOperation {
            path_start: new_path_start,
            path_len: new_path_len,
            patch: val,
        });
    }
}

pub fn json_remove(args: &[OwnedValue]) -> crate::Result<OwnedValue> {
    if args.is_empty() {
        return Ok(OwnedValue::Null);
    }

    let mut parsed_target = get_json_value(&args[0])?;
    if args.len() == 1 {
        return Ok(args[0].clone());
    }

    let paths: Result<Vec<_>, _> = args[1..]
        .iter()
        .map(|path| {
            if let OwnedValue::Text(path) = path {
                json_path(path.as_str())
            } else {
                crate::bail_constraint_error!("bad JSON path: {:?}", path.to_string())
            }
        })
        .collect();
    let paths = paths?;

    for path in paths {
        mutate_json_by_path(&mut parsed_target, path, |val| match val {
            Target::Array(arr, index) => {
                arr.remove(index);
            }
            Target::Value(val) => *val = Val::Removed,
        });
    }

    convert_json_to_db_type(&parsed_target, false)
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::types::Text;

    use super::*;

    fn create_text(s: &str) -> OwnedValue {
        OwnedValue::Text(Text::from_str(s))
    }

    fn create_json(s: &str) -> OwnedValue {
        OwnedValue::Text(Text::json(Rc::new(s.to_string())))
    }

    #[test]
    fn test_new_patcher() {
        let patcher = JsonPatcher::new(10);
        assert_eq!(patcher.queue.capacity(), 10);
        assert_eq!(patcher.path_storage.capacity(), 256);
        assert!(patcher.queue.is_empty());
        assert!(patcher.path_storage.is_empty());
    }

    #[test]
    fn test_simple_value_replacement() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![("key1".to_string(), Val::String("old".to_string()))]);
        let patch = Val::Object(vec![("key1".to_string(), Val::String("new".to_string()))]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            assert_eq!(map[0].1, Val::String("new".to_string()));
        } else {
            panic!("Expected object");
        }
    }

    #[test]
    fn test_nested_object_patch() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![(
            "a".to_string(),
            Val::Object(vec![("b".to_string(), Val::String("old".to_string()))]),
        )]);
        let patch = Val::Object(vec![(
            "a".to_string(),
            Val::Object(vec![("b".to_string(), Val::String("new".to_string()))]),
        )]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            if let Val::Object(nested) = &map[0].1 {
                assert_eq!(nested[0].1, Val::String("new".to_string()));
            } else {
                panic!("Expected nested object");
            }
        }
    }

    #[test]
    fn test_null_removal() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![
            ("keep".to_string(), Val::String("value".to_string())),
            ("remove".to_string(), Val::String("value".to_string())),
        ]);
        let patch = Val::Object(vec![("remove".to_string(), Val::Null)]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            assert_eq!(map[0].1, Val::String("value".to_string()));
            assert_eq!(map[1].1, Val::Removed);
        }
    }

    #[test]
    fn test_duplicate_keys_first_occurrence() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![
            ("key".to_string(), Val::String("first".to_string())),
            ("key".to_string(), Val::String("second".to_string())),
            ("key".to_string(), Val::String("third".to_string())),
        ]);
        let patch = Val::Object(vec![(
            "key".to_string(),
            Val::String("modified".to_string()),
        )]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            assert_eq!(map[0].1, Val::String("modified".to_string()));
            assert_eq!(map[1].1, Val::String("second".to_string()));
            assert_eq!(map[2].1, Val::String("third".to_string()));
        }
    }

    #[test]
    fn test_add_new_key() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![(
            "existing".to_string(),
            Val::String("value".to_string()),
        )]);
        let patch = Val::Object(vec![("new".to_string(), Val::String("value".to_string()))]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            assert_eq!(map.len(), 2);
            assert_eq!(map[1].0, "new");
            assert_eq!(map[1].1, Val::String("value".to_string()));
        }
    }

    #[test]
    fn test_deep_nested_patch() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![(
            "level1".to_string(),
            Val::Object(vec![(
                "level2".to_string(),
                Val::Object(vec![("level3".to_string(), Val::String("old".to_string()))]),
            )]),
        )]);
        let patch = Val::Object(vec![(
            "level1".to_string(),
            Val::Object(vec![(
                "level2".to_string(),
                Val::Object(vec![("level3".to_string(), Val::String("new".to_string()))]),
            )]),
        )]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(l1) = target {
            if let Val::Object(l2) = &l1[0].1 {
                if let Val::Object(l3) = &l2[0].1 {
                    assert_eq!(l3[0].1, Val::String("new".to_string()));
                }
            }
        }
    }

    #[test]
    fn test_null_patch_on_nonexistent_key() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![(
            "existing".to_string(),
            Val::String("value".to_string()),
        )]);
        let patch = Val::Object(vec![("nonexistent".to_string(), Val::Null)]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            assert_eq!(map.len(), 1); // Should not add new key for null patch
            assert_eq!(map[0].0, "existing");
        }
    }

    #[test]
    fn test_nested_duplicate_keys() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![(
            "outer".to_string(),
            Val::Object(vec![
                ("inner".to_string(), Val::String("first".to_string())),
                ("inner".to_string(), Val::String("second".to_string())),
            ]),
        )]);
        let patch = Val::Object(vec![(
            "outer".to_string(),
            Val::Object(vec![(
                "inner".to_string(),
                Val::String("modified".to_string()),
            )]),
        )]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(outer) = target {
            if let Val::Object(inner) = &outer[0].1 {
                assert_eq!(inner[0].1, Val::String("modified".to_string()));
                assert_eq!(inner[1].1, Val::String("second".to_string()));
            }
        }
    }

    #[test]
    #[should_panic(expected = "Invalid path")]
    fn test_invalid_path_navigation() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![("a".to_string(), Val::Object(vec![]))]);
        patcher.path_storage.push(0);
        patcher.path_storage.push(999); // Invalid index

        patcher.navigate_to_target(&mut target, 0, 2);
    }

    #[test]
    fn test_merge_empty_objects() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![]);
        let patch = Val::Object(vec![]);

        patcher.apply_patch(&mut target, patch);

        if let Val::Object(map) = target {
            assert!(map.is_empty());
        }
    }

    #[test]
    fn test_path_storage_growth() {
        let mut patcher = JsonPatcher::new(10);
        let mut target = Val::Object(vec![(
            "a".to_string(),
            Val::Object(vec![("b".to_string(), Val::Object(vec![]))]),
        )]);
        let patch = Val::Object(vec![(
            "a".to_string(),
            Val::Object(vec![("b".to_string(), Val::String("value".to_string()))]),
        )]);

        patcher.apply_patch(&mut target, patch);

        // Path storage should contain [0, 0] for accessing a.b
        assert_eq!(patcher.path_storage.len(), 3);
        assert_eq!(patcher.path_storage[0], 0);
        assert_eq!(patcher.path_storage[1], 0);
    }

    #[test]
    fn test_basic_text_replacement() {
        let target = create_text(r#"{"name":"John","age":"30"}"#);
        let patch = create_text(r#"{"age":"31"}"#);

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(result, create_json(r#"{"name":"John","age":"31"}"#));
    }

    #[test]
    fn test_null_field_removal() {
        let target = create_text(r#"{"name":"John","email":"john@example.com"}"#);
        let patch = create_text(r#"{"email":null}"#);

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(result, create_json(r#"{"name":"John"}"#));
    }

    #[test]
    fn test_nested_object_merge() {
        let target =
            create_text(r#"{"user":{"name":"John","details":{"age":"30","score":"95.5"}}}"#);

        let patch = create_text(r#"{"user":{"details":{"score":"97.5"}}}"#);

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(
            result,
            create_json(r#"{"user":{"name":"John","details":{"age":"30","score":"97.5"}}}"#)
        );
    }

    #[test]
    #[should_panic(expected = "blob is not supported!")]
    fn test_blob_not_supported() {
        let target = OwnedValue::Blob(Rc::new(vec![1, 2, 3]));
        let patch = create_text("{}");
        json_patch(&target, &patch).unwrap();
    }

    #[test]
    fn test_deep_null_replacement() {
        let target = create_text(r#"{"level1":{"level2":{"keep":"value","remove":"value"}}}"#);

        let patch = create_text(r#"{"level1":{"level2":{"remove":null}}}"#);

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(
            result,
            create_json(r#"{"level1":{"level2":{"keep":"value"}}}"#)
        );
    }

    #[test]
    fn test_empty_patch() {
        let target = create_json(r#"{"name":"John","age":"30"}"#);
        let patch = create_text("{}");

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(result, target);
    }

    #[test]
    fn test_add_new_field() {
        let target = create_text(r#"{"existing":"value"}"#);
        let patch = create_text(r#"{"new":"field"}"#);

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(result, create_json(r#"{"existing":"value","new":"field"}"#));
    }

    #[test]
    fn test_complete_object_replacement() {
        let target = create_text(r#"{"old":{"nested":"value"}}"#);
        let patch = create_text(r#"{"old":"new_value"}"#);

        let result = json_patch(&target, &patch).unwrap();
        assert_eq!(result, create_json(r#"{"old":"new_value"}"#));
    }

    #[test]
    fn test_json_remove_empty_args() {
        let args = vec![];
        assert_eq!(json_remove(&args).unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_json_remove_array_element() {
        let args = vec![create_json(r#"[1,2,3,4,5]"#), create_text("$[2]")];

        let result = json_remove(&args).unwrap();
        match result {
            OwnedValue::Text(t) => assert_eq!(t.as_str(), "[1,2,4,5]"),
            _ => panic!("Expected Text value"),
        }
    }

    #[test]
    fn test_json_remove_multiple_paths() {
        let args = vec![
            create_json(r#"{"a": 1, "b": 2, "c": 3}"#),
            create_text("$.a"),
            create_text("$.c"),
        ];

        let result = json_remove(&args).unwrap();
        match result {
            OwnedValue::Text(t) => assert_eq!(t.as_str(), r#"{"b":2}"#),
            _ => panic!("Expected Text value"),
        }
    }

    #[test]
    fn test_json_remove_nested_paths() {
        let args = vec![
            create_json(r#"{"a": {"b": {"c": 1, "d": 2}}}"#),
            create_text("$.a.b.c"),
        ];

        let result = json_remove(&args).unwrap();
        match result {
            OwnedValue::Text(t) => assert_eq!(t.as_str(), r#"{"a":{"b":{"d":2}}}"#),
            _ => panic!("Expected Text value"),
        }
    }

    #[test]
    fn test_json_remove_duplicate_keys() {
        let args = vec![
            create_json(r#"{"a": 1, "a": 2, "a": 3}"#),
            create_text("$.a"),
        ];

        let result = json_remove(&args).unwrap();
        match result {
            OwnedValue::Text(t) => assert_eq!(t.as_str(), r#"{"a":2,"a":3}"#),
            _ => panic!("Expected Text value"),
        }
    }

    #[test]
    fn test_json_remove_invalid_path() {
        let args = vec![
            create_json(r#"{"a": 1}"#),
            OwnedValue::Integer(42), // Invalid path type
        ];

        assert!(json_remove(&args).is_err());
    }

    #[test]
    fn test_json_remove_complex_case() {
        let args = vec![
            create_json(r#"{"a":[1,2,3],"b":{"x":1,"x":2},"c":[{"y":1},{"y":2}]}"#),
            create_text("$.a[1]"),
            create_text("$.b.x"),
            create_text("$.c[0].y"),
        ];

        let result = json_remove(&args).unwrap();
        match result {
            OwnedValue::Text(t) => {
                let value = t.as_str();
                assert!(value.contains(r#"[1,3]"#));
                assert!(value.contains(r#"{"x":2}"#));
            }
            _ => panic!("Expected Text value"),
        }
    }
}
