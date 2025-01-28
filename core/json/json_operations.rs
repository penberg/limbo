use std::collections::VecDeque;

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

    merge_patch(&mut parsed_target, parsed_patch);

    convert_json_to_db_type(&parsed_target, false)
}

/// # Implementation Notes
///
/// * Processes patches in breadth-first order
/// * Maintains path information for nested updates
/// * Handles object merging with proper null value semantics
/// * Preserves existing values not mentioned in the patch
///
/// Following RFC 7386 rules:
/// * null values remove properties
/// * Objects are merged recursively
/// * Non-object values replace the target completely
fn merge_patch(target: &mut Val, patch: Val) {
    let mut queue = VecDeque::with_capacity(8);

    queue.push_back(PatchOperation {
        path: Vec::new(),
        patch,
    });
    while let Some(PatchOperation { path, patch }) = queue.pop_front() {
        let mut current: &mut Val = target;

        for (depth, key) in path.iter().enumerate() {
            if let Val::Object(ref mut obj) = current {
                current = &mut obj
                    .get_mut(*key)
                    .unwrap_or_else(|| {
                        panic!("Invalid path at depth {}: key '{}' not found", depth, key)
                    })
                    .1;
            }
        }

        match (current, patch) {
            (curr, Val::Null) => {
                *curr = Val::Null;
            }
            (Val::Object(target_map), Val::Object(patch_map)) => {
                for (key, patch_val) in patch_map {
                    if let Some(pos) = target_map
                        .iter()
                        .position(|(target_key, _)| target_key == &key)
                    {
                        match patch_val {
                            Val::Null => {
                                target_map.remove(pos);
                            }
                            val => {
                                let mut new_path = path.clone();
                                new_path.push(pos);
                                queue.push_back(PatchOperation {
                                    path: new_path,
                                    patch: val,
                                });
                            }
                        };
                    } else {
                        target_map.push((key, patch_val));
                    };
                }
            }
            (curr, patch_val) => {
                *curr = patch_val;
            }
        }
    }
}
