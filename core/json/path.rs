use super::Val;

pub fn get_json_val_by_path<'v>(val: &'v Val, path: &str) -> crate::Result<Option<&'v Val>> {
    match path.strip_prefix('$') {
        Some(tail) => json_val_by_path(val, tail),
        None => crate::bail_parse_error!("malformed path"),
    }
}

fn json_val_by_path<'v>(val: &'v Val, path: &str) -> crate::Result<Option<&'v Val>> {
    if path.is_empty() {
        return Ok(Some(val));
    }

    match val {
        Val::Array(inner) => {
            if inner.is_empty() {
                return Ok(None);
            }
            let Some(tail) = path.strip_prefix('[') else {
                return Ok(None);
            };
            let (from_end, tail) = if let Some(updated_tail) = tail.strip_prefix("#-") {
                (true, updated_tail)
            } else {
                (false, tail)
            };

            let Some((idx_str, tail)) = tail.split_once("]") else {
                crate::bail_parse_error!("malformed path");
            };

            if idx_str.is_empty() {
                return Ok(None);
            }
            let Ok(idx) = idx_str.parse::<usize>() else {
                crate::bail_parse_error!("malformed path");
            };
            let result = if from_end {
                inner.get(inner.len() - 1 - idx)
            } else {
                inner.get(idx)
            };

            if let Some(result) = result {
                return json_val_by_path(result, tail);
            }
            Ok(None)
        }
        Val::Object(inner) => {
            let Some(tail) = path.strip_prefix('.') else {
                return Ok(None);
            };

            let (property, tail) = if let Some(tail) = tail.strip_prefix('"') {
                if let Some((property, tail)) = tail.split_once('"') {
                    (property, tail)
                } else {
                    crate::bail_parse_error!("malformed path");
                }
            } else if let Some(idx) = tail.find('.') {
                (&tail[..idx], &tail[idx..])
            } else {
                (tail, "")
            };

            if let Some(result) = inner.get(property) {
                return json_val_by_path(result, tail);
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_root() {
        assert_eq!(
            get_json_val_by_path(&Val::Bool(true), "$",).unwrap(),
            Some(&Val::Bool(true))
        );
    }

    #[test]
    fn test_path_index() {
        assert_eq!(
            get_json_val_by_path(
                &Val::Array(vec![Val::Integer(33), Val::Integer(55), Val::Integer(66)]),
                "$[2]",
            )
            .unwrap(),
            Some(&Val::Integer(66))
        );
    }

    #[test]
    fn test_path_negative_index() {
        assert_eq!(
            get_json_val_by_path(
                &Val::Array(vec![Val::Integer(33), Val::Integer(55), Val::Integer(66)]),
                "$[#-2]",
            )
            .unwrap(),
            Some(&Val::Integer(33))
        );
    }

    #[test]
    fn test_path_index_deep() {
        assert_eq!(
            get_json_val_by_path(
                &Val::Array(vec![Val::Array(vec![
                    Val::Integer(33),
                    Val::Integer(55),
                    Val::Integer(66)
                ])]),
                "$[0][1]",
            )
            .unwrap(),
            Some(&Val::Integer(55))
        );
    }

    #[test]
    fn test_path_prop_simple() {
        assert_eq!(
            get_json_val_by_path(
                &Val::Object(
                    [
                        ("foo".into(), Val::Integer(55)),
                        ("bar".into(), Val::Integer(66))
                    ]
                    .into()
                ),
                "$.bar",
            )
            .unwrap(),
            Some(&Val::Integer(66))
        );
    }

    #[test]
    fn test_path_prop_nested() {
        assert_eq!(
            get_json_val_by_path(
                &Val::Object(
                    [(
                        "foo".into(),
                        Val::Object([("bar".into(), Val::Integer(66))].into())
                    )]
                    .into()
                ),
                "$.foo.bar",
            )
            .unwrap(),
            Some(&Val::Integer(66))
        );
    }

    #[test]
    fn test_path_prop_quoted() {
        assert_eq!(
            get_json_val_by_path(
                &Val::Object(
                    [
                        ("foo.baz".into(), Val::Integer(55)),
                        ("bar".into(), Val::Integer(66))
                    ]
                    .into()
                ),
                r#"$."foo.baz""#,
            )
            .unwrap(),
            Some(&Val::Integer(55))
        );
    }
}
