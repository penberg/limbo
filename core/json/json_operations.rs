use std::collections::{HashSet, VecDeque};

use crate::types::OwnedValue;

use super::{convert_json_to_db_type, get_json_value, Val};

/// Represents a single patch operation in the merge queue.
///
/// Used internally by the `merge_patch` function to track the path and value
/// for each pending merge operation.
#[derive(Debug, Clone)]
struct PatchOperation {
    path: Vec<usize>,
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
    let mut patcher = Patcher::new(16);
    patcher.apply_patch(&mut parsed_target, parsed_patch);

    convert_json_to_db_type(&parsed_target, false)
}

struct Patcher {
    queue: VecDeque<PatchOperation>,
    applied_keys: HashSet<String>,
}

impl Patcher {
    fn new(queue_capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(queue_capacity),
            applied_keys: HashSet::new(),
        }
    }

    fn apply_patch(&mut self, target: &mut Val, patch: Val) {
        self.queue.push_back(PatchOperation {
            path: Vec::new(),
            patch,
        });

        while let Some(op) = self.queue.pop_front() {
            let current = self.navigate_to_target(target, &op.path);
            self.apply_operation(current, op.patch, &op.path);
        }
    }

    fn navigate_to_target<'a>(&self, target: &'a mut Val, path: &Vec<usize>) -> &'a mut Val {
        let mut current = target;
        for (depth, &key) in path.iter().enumerate() {
            if let Val::Object(ref mut obj) = current {
                current = &mut obj
                    .get_mut(key)
                    .unwrap_or_else(|| {
                        panic!("Invalid path at depth {}: key '{}' not found", depth, key)
                    })
                    .1;
            }
        }
        current
    }

    fn apply_operation(&mut self, current: &mut Val, patch: Val, path: &Vec<usize>) {
        match (current, patch) {
            (current_val, Val::Null) => *current_val = Val::Null,
            (Val::Object(target_map), Val::Object(patch_map)) => {
                self.merge_objects(target_map, patch_map, path);
            }
            (current_val, patch_val) => *current_val = patch_val,
        }
    }

    fn merge_objects(
        &mut self,
        target_map: &mut Vec<(String, Val)>,
        patch_map: Vec<(String, Val)>,
        path: &Vec<usize>,
    ) {
        self.applied_keys.clear();
        let original_keys: Vec<String> = target_map.iter().map(|(key, _)| key.clone()).collect();

        for (key, patch_val) in patch_map {
            self.process_key_value(target_map, &original_keys, key, patch_val, path);
        }
    }

    fn process_key_value(
        &mut self,
        target_map: &mut Vec<(String, Val)>,
        original_keys: &[String],
        key: String,
        patch_val: Val,
        path: &Vec<usize>,
    ) {
        if !original_keys.contains(&key) {
            target_map.push((key, patch_val));
            return;
        }

        if let Some(pos) = target_map
            .iter()
            .position(|(target_key, _)| target_key == &key)
        {
            if !self.applied_keys.insert(key) {
                return;
            }

            match patch_val {
                Val::Null => {
                    target_map.remove(pos);
                }
                val => {
                    self.queue_nested_patch(pos, val, path);
                }
            }
        } else {
            target_map.push((key, patch_val));
        }
    }

    fn queue_nested_patch(&mut self, pos: usize, val: Val, path: &Vec<usize>) {
        let mut new_path = path.clone();
        new_path.push(pos);
        self.queue.push_back(PatchOperation {
            path: new_path,
            patch: val,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::types::LimboText;

    use super::*;

    fn create_text(s: &str) -> OwnedValue {
        OwnedValue::Text(LimboText::new(Rc::new(s.to_string())))
    }

    fn create_json(s: &str) -> OwnedValue {
        OwnedValue::Text(LimboText::json(Rc::new(s.to_string())))
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
}
