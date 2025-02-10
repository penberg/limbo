use crate::bail_parse_error;
use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq)]
enum PPState {
    Start,
    AfterRoot,
    InKey,
    InArrayIndex,
    ExpectDotOrBracket,
}

#[derive(Clone, Debug, PartialEq)]
enum ArrayIndexState {
    Start,
    AfterHash,
    CollectingNumbers,
    IsMax,
}

/// Describes a JSON path, which is a sequence of keys and/or array locators.
#[derive(Clone, Debug)]
pub struct JsonPath<'a> {
    pub elements: Vec<PathElement<'a>>,
}

type RawString = bool;

/// PathElement describes a single element of a JSON path.
#[derive(Clone, Debug, PartialEq)]
pub enum PathElement<'a> {
    /// Root element: '$'
    Root(),
    /// JSON key
    Key(Cow<'a, str>, RawString),
    /// Array locator, eg. [2], [#-5]
    ArrayLocator(Option<i32>),
}

type IsMaxNumber = bool;

fn collect_num(current: i128, adding: i128, negative: bool) -> (i128, IsMaxNumber) {
    let ten = 10i128;
    let adding = adding as i128;

    let result = if negative {
        current.saturating_mul(ten).saturating_sub(adding)
    } else {
        current.saturating_mul(ten).saturating_add(adding)
    };

    let is_max = result == i128::MAX || result == i128::MIN;
    (result, is_max)
}

fn estimate_path_capacity(input: &str) -> usize {
    // After $ we need either . or [ for each component
    // So divide remaining length by 2 (minimum chars per component)
    // Add 1 for the root component
    1 + (input.len() - 1) / 2
}

/// Parses path into a Vec of Strings, where each string is a key or an array locator.
pub fn json_path(path: &str) -> crate::Result<JsonPath<'_>> {
    if path.is_empty() {
        bail_parse_error!("Bad json path: {}", path)
    }
    let mut parser_state = PPState::Start;
    let mut index_state = ArrayIndexState::Start;
    let mut key_start = 0;
    let mut index_buffer: i128 = 0;
    let mut path_components = Vec::with_capacity(estimate_path_capacity(path));
    let mut path_iter = path.char_indices();

    while let Some(ch) = path_iter.next() {
        match parser_state {
            PPState::Start => {
                handle_start(ch, &mut parser_state, &mut path_components, path)?;
            }
            PPState::AfterRoot => {
                handle_after_root(
                    ch,
                    &mut parser_state,
                    &mut index_state,
                    &mut key_start,
                    &mut index_buffer,
                    path,
                )?;
            }
            PPState::InKey => {
                handle_in_key(
                    ch,
                    &mut parser_state,
                    &mut index_state,
                    &mut key_start,
                    &mut index_buffer,
                    &mut path_components,
                    &mut path_iter,
                    path,
                )?;
            }
            PPState::InArrayIndex => {
                handle_array_index(
                    ch,
                    &mut parser_state,
                    &mut index_state,
                    &mut index_buffer,
                    &mut path_components,
                    &mut path_iter,
                    path,
                )?;
            }
            PPState::ExpectDotOrBracket => {
                handle_expect_dot_or_bracket(
                    ch,
                    &mut parser_state,
                    &mut index_state,
                    &mut key_start,
                    &mut index_buffer,
                    path,
                )?;
            }
        }
    }

    finalize_path(parser_state, key_start, path, &mut path_components)?;
    Ok(JsonPath {
        elements: path_components,
    })
}

fn handle_start(
    ch: (usize, char),
    parser_state: &mut PPState,
    path_components: &mut Vec<PathElement>,
    path: &str,
) -> crate::Result<()> {
    match ch {
        (_, '$') => {
            path_components.push(PathElement::Root());
            *parser_state = PPState::AfterRoot;
            Ok(())
        }
        (_, _) => bail_parse_error!("Bad json path: {}", path),
    }
}

fn handle_after_root(
    ch: (usize, char),
    parser_state: &mut PPState,
    index_state: &mut ArrayIndexState,
    key_start: &mut usize,
    index_buffer: &mut i128,
    path: &str,
) -> crate::Result<()> {
    match ch {
        (idx, '.') => {
            *parser_state = PPState::InKey;
            *key_start = idx + ch.1.len_utf8();
            Ok(())
        }
        (_, '[') => {
            *index_state = ArrayIndexState::Start;
            *parser_state = PPState::InArrayIndex;
            *index_buffer = 0;
            Ok(())
        }
        (_, _) => bail_parse_error!("Bad json path: {}", path),
    }
}

fn handle_in_key<'a>(
    ch: (usize, char),
    parser_state: &mut PPState,
    index_state: &mut ArrayIndexState,
    key_start: &mut usize,
    index_buffer: &mut i128,
    path_components: &mut Vec<PathElement<'a>>,
    path_iter: &mut std::str::CharIndices,
    path: &'a str,
) -> crate::Result<()> {
    match ch {
        (idx, '.' | '[') => {
            let key_end = idx;
            if key_end > *key_start {
                let key = &path[*key_start..key_end];
                if ch.1 == '[' {
                    *index_state = ArrayIndexState::Start;
                    *parser_state = PPState::InArrayIndex;
                    *index_buffer = 0;
                } else {
                    *key_start = idx + ch.1.len_utf8();
                }
                path_components.push(PathElement::Key(Cow::Borrowed(key), false));
            } else {
                bail_parse_error!("Bad json path: {}", path)
            }
        }
        (_, '"') => {
            handle_quoted_key(parser_state, key_start, path_components, path_iter, path)?;
        }
        (_, _) => (),
    }
    Ok(())
}

fn handle_quoted_key<'a>(
    parser_state: &mut PPState,
    key_start: &mut usize,
    path_components: &mut Vec<PathElement<'a>>,
    path_iter: &mut std::str::CharIndices,
    path: &'a str,
) -> crate::Result<()> {
    while let Some((idx, ch)) = path_iter.next() {
        match ch {
            '\\' => {
                path_iter.next();
            }
            '"' => {
                if *key_start < idx {
                    let key = &path[*key_start + 1..idx];
                    path_components.push(PathElement::Key(Cow::Borrowed(key), true));
                    *parser_state = PPState::ExpectDotOrBracket;
                    return Ok(());
                }
            }
            _ => continue,
        }
    }
    Ok(())
}

fn handle_array_index<'a>(
    ch: (usize, char),
    parser_state: &mut PPState,
    index_state: &mut ArrayIndexState,
    index_buffer: &mut i128,
    path_components: &mut Vec<PathElement<'a>>,
    path_iter: &mut std::str::CharIndices,
    path: &str,
) -> crate::Result<()> {
    match (&index_state, ch.1) {
        (ArrayIndexState::Start, '#') => {
            *index_state = ArrayIndexState::AfterHash;
        }
        (ArrayIndexState::Start, '0'..='9') => {
            *index_buffer = ch.1.to_digit(10).unwrap() as i128;
            *index_state = ArrayIndexState::CollectingNumbers;
        }
        (ArrayIndexState::AfterHash, '-') => {
            handle_negative_index(index_state, index_buffer, path_iter, path)?;
        }
        (ArrayIndexState::AfterHash, ']') => {
            *parser_state = PPState::ExpectDotOrBracket;
            path_components.push(PathElement::ArrayLocator(None));
        }
        (ArrayIndexState::CollectingNumbers, '0'..='9') => {
            let (new_num, is_max) = collect_num(
                *index_buffer,
                ch.1.to_digit(10).unwrap() as i128,
                *index_buffer < 0,
            );
            if is_max {
                *index_state = ArrayIndexState::IsMax;
            }
            *index_buffer = new_num;
        }
        (ArrayIndexState::IsMax, '0'..='9') => (),
        (ArrayIndexState::CollectingNumbers | ArrayIndexState::IsMax, ']') => {
            *parser_state = PPState::ExpectDotOrBracket;
            path_components.push(PathElement::ArrayLocator(Some(*index_buffer as i32)));
        }
        (_, _) => bail_parse_error!("Bad json path: {}", path),
    }
    Ok(())
}

fn handle_negative_index(
    index_state: &mut ArrayIndexState,
    index_buffer: &mut i128,
    path_iter: &mut std::str::CharIndices,
    path: &str,
) -> crate::Result<()> {
    if let Some((_, next_c)) = path_iter.next() {
        if next_c.is_ascii_digit() {
            *index_buffer = -(next_c.to_digit(10).unwrap() as i128);
            *index_state = ArrayIndexState::CollectingNumbers;
            Ok(())
        } else {
            bail_parse_error!("Bad json path: {}", path)
        }
    } else {
        bail_parse_error!("Bad json path: {}", path)
    }
}

fn handle_expect_dot_or_bracket(
    ch: (usize, char),
    parser_state: &mut PPState,
    index_state: &mut ArrayIndexState,
    key_start: &mut usize,
    index_buffer: &mut i128,
    path: &str,
) -> crate::Result<()> {
    match ch {
        (idx, '.') => {
            *key_start = idx + ch.1.len_utf8();
            *parser_state = PPState::InKey;
            Ok(())
        }
        (_, '[') => {
            *index_state = ArrayIndexState::Start;
            *parser_state = PPState::InArrayIndex;
            *index_buffer = 0;
            Ok(())
        }
        (_, _) => bail_parse_error!("Bad json path: {}", path),
    }
}

fn finalize_path<'a>(
    parser_state: PPState,
    key_start: usize,
    path: &'a str,
    path_components: &mut Vec<PathElement<'a>>,
) -> crate::Result<()> {
    match parser_state {
        PPState::InArrayIndex => bail_parse_error!("Bad json path: {}", path),
        PPState::InKey => {
            if key_start < path.len() {
                let key = &path[key_start..];
                if key.starts_with('"') & !key.ends_with('"') {
                    bail_parse_error!("Bad json path: {}", path)
                }
                path_components.push(PathElement::Key(Cow::Borrowed(key), false));
            } else {
                bail_parse_error!("Bad json path: {}", path)
            }
        }
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_path_root() {
        let path = json_path("$").unwrap();
        assert_eq!(path.elements.len(), 1);
        assert_eq!(path.elements[0], PathElement::Root());
    }

    #[test]
    fn test_json_path_single_locator() {
        let path = json_path("$.x").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("x"), false)
        );
    }

    #[test]
    fn test_json_path_single_array_locator() {
        let path = json_path("$[0]").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));
    }

    #[test]
    fn test_json_path_single_negative_array_locator() {
        let path = json_path("$[#-2]").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(-2)));
    }

    #[test]
    fn test_json_path_invalid() {
        let invalid_values = vec![
            "", "$$$", "$.", "$ ", "$[", "$]", "$[-1]", "x", "[]", "$[0", "$[0x]", "$\"",
        ];

        for value in invalid_values {
            let path = json_path(value);

            match path {
                Err(crate::error::LimboError::ParseError(_)) => {
                    // happy path
                }
                _ => panic!("Expected error for: {:?}, got: {:?}", value, path),
            }
        }
    }

    #[test]
    fn test_json_path() {
        let path = json_path("$.store.book[0].title").unwrap();
        assert_eq!(path.elements.len(), 5);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("store"), false)
        );
        assert_eq!(
            path.elements[2],
            PathElement::Key(Cow::Borrowed("book"), false)
        );
        assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(0)));
        assert_eq!(
            path.elements[4],
            PathElement::Key(Cow::Borrowed("title"), false)
        );
    }

    #[test]
    fn test_large_index_wrapping() {
        let path = json_path("$[4294967296]").unwrap();
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));

        let path = json_path("$[4294967297]").unwrap();
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(1)));
    }

    #[test]
    fn test_deeply_nested_path() {
        let path = json_path("$[0][1][2].key[3].other").unwrap();
        assert_eq!(path.elements.len(), 7);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));
        assert_eq!(path.elements[2], PathElement::ArrayLocator(Some(1)));
        assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(2)));
        assert_eq!(
            path.elements[4],
            PathElement::Key(Cow::Borrowed("key"), false)
        );
        assert_eq!(path.elements[5], PathElement::ArrayLocator(Some(3)));
    }

    #[test]
    fn test_edge_cases() {
        // Empty key
        assert!(json_path("$.").is_err());

        // Multiple dots
        assert!(json_path("$..key").is_err());

        // Unclosed brackets
        assert!(json_path("$[0").is_err());
        assert!(json_path("$[").is_err());

        // Invalid negative index format
        assert!(json_path("$[-1]").is_err()); // should be $[#-1]
    }

    #[test]
    fn test_path_capacity() {
        // Test that our capacity estimation is reasonable
        let short_path = "$[0]";
        assert!(estimate_path_capacity(short_path) >= 2);

        let long_path = "$.a.b.c.d.e.f.g[0][1][2]";
        assert!(estimate_path_capacity(long_path) >= 11);
    }

    #[test]
    fn test_quoted_keys() {
        let path = json_path(r#"$."key""#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("key"), true)
        );

        let path = json_path(r#"$."key.with.dots""#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("key.with.dots"), true)
        );

        let path = json_path(r#"$."key[0]""#).unwrap();
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("key[0]"), true)
        );
    }

    #[test]
    fn test_empty_quoted_key() {
        assert!(json_path(r#"$."""#).is_ok());
    }
}
