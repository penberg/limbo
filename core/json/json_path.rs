use crate::bail_parse_error;
use std::borrow::Cow;

/// You have to know your PP state
#[derive(Clone, Debug, PartialEq)]
enum PPState {
    Start,
    AfterRoot,
    InKey,
    InArrayIndex,
    ExpectDotOrBracket,
}

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

/// PathElement describes a single element of a JSON path.
#[derive(Clone, Debug, PartialEq)]
pub enum PathElement<'a> {
    /// Root element: '$'
    Root(),
    /// JSON key
    Key(Cow<'a, str>),
    /// Array locator, eg. [2], [#-5]
    ArrayLocator(i32),
}

type IsMaxNumber = bool;

fn collect_num(current: i32, adding: i32, negative: bool) -> (i32, IsMaxNumber) {
    let mut is_max = false;
    let current = if negative {
        current
            .checked_mul(10)
            .and_then(|n| n.checked_sub(adding))
            .unwrap_or_else(|| {
                is_max = true;
                i32::MIN
            })
    } else {
        current
            .checked_mul(10)
            .and_then(|n| n.checked_add(adding))
            .unwrap_or_else(|| {
                is_max = true;
                i32::MAX
            })
    };
    (current, is_max)
}

/// Parses path into a Vec of Strings, where each string is a key or an array locator.
pub fn json_path<'a>(path: &'a str) -> crate::Result<JsonPath<'a>> {
    let mut parser_state = PPState::Start;
    let mut index_state = ArrayIndexState::Start;

    let mut key_start = 0;
    let mut index_buffer = 0;

    let mut path_components = Vec::with_capacity(5);
    let mut path_iter = path.char_indices();
    while let Some(ch) = path_iter.next() {
        let ch_len = ch.1.len_utf8();
        match parser_state {
            PPState::Start => match ch {
                (_, '$') => {
                    path_components.push(PathElement::Root());
                    parser_state = PPState::AfterRoot
                }
                (_, _) => bail_parse_error!("Bad json path: {}", path),
            },
            PPState::AfterRoot => match ch {
                (idx, '.') => {
                    parser_state = PPState::InKey;
                    key_start = idx + ch_len;
                }
                (_, '[') => {
                    index_state = ArrayIndexState::Start;
                    parser_state = PPState::InArrayIndex;
                    index_buffer = 0;
                }
                (_, _) => bail_parse_error!("Bad json path: {}", path),
            },
            PPState::InKey => match ch {
                (idx, '.' | '[') => {
                    let key_end = idx;
                    if key_end > key_start {
                        let key = &path[key_start..key_end];
                        if ch.1 == '[' {
                            index_state = ArrayIndexState::Start;
                            parser_state = PPState::InArrayIndex;
                            index_buffer = 0;
                        } else {
                            key_start = idx + ch_len;
                        }
                        path_components.push(PathElement::Key(Cow::Borrowed(key)));
                    } else {
                        unreachable!()
                    }
                }
                (_, _) => continue,
            },
            PPState::InArrayIndex => {
                let (_, c) = ch;

                match (&index_state, c) {
                    (ArrayIndexState::Start, '#') => index_state = ArrayIndexState::AfterHash,
                    (ArrayIndexState::Start, '0'..='9') => {
                        index_buffer = c.to_digit(10).unwrap() as i32;
                        index_state = ArrayIndexState::CollectingNumbers;
                    }
                    (ArrayIndexState::AfterHash, '-') => {
                        if let Some((_, next_c)) = path_iter.next() {
                            if next_c.is_ascii_digit() {
                                index_buffer = -(next_c.to_digit(10).unwrap() as i32);
                                index_state = ArrayIndexState::CollectingNumbers;
                            } else {
                                bail_parse_error!("Bad json path: {}", path);
                            }
                        } else {
                            bail_parse_error!("Bad json path: {}", path);
                        }
                    }
                    (ArrayIndexState::CollectingNumbers, '0'..='9') => {
                        let (new_num, is_max) = collect_num(
                            index_buffer,
                            c.to_digit(10).unwrap() as i32,
                            index_buffer < 0,
                        );
                        if is_max {
                            index_state = ArrayIndexState::IsMax;
                            index_buffer = new_num;
                        }
                    }
                    (ArrayIndexState::IsMax, '0'..='9') => continue,
                    (ArrayIndexState::CollectingNumbers | ArrayIndexState::IsMax, ']') => {
                        parser_state = PPState::ExpectDotOrBracket;
                        path_components.push(PathElement::ArrayLocator(index_buffer))
                    }
                    (_, _) => bail_parse_error!("Bad json path: {}", path),
                }
            }
            PPState::ExpectDotOrBracket => match ch {
                (idx, '.') => {
                    key_start = idx + ch_len;
                    parser_state = PPState::InKey;
                }
                (_, '[') => {
                    index_state = ArrayIndexState::Start;
                    parser_state = PPState::InArrayIndex;
                    index_buffer = 0;
                }
                (_, _) => bail_parse_error!("Bad json path: {}", path),
            },
        }
    }
    if parser_state == PPState::InKey && key_start < path.len() {
        let key = &path[key_start..];
        path_components.push(PathElement::Key(Cow::Borrowed(key)));
    }

    Ok(JsonPath {
        elements: path_components,
    })
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
        assert_eq!(path.elements[1], PathElement::Key(Cow::Borrowed("x")));
    }

    #[test]
    fn test_json_path_single_array_locator() {
        let path = json_path("$[0]").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(0));
    }

    #[test]
    fn test_json_path_single_negative_array_locator() {
        let path = json_path("$[#-2]").unwrap();
        assert_eq!(path.elements.len(), 2);
        assert_eq!(path.elements[0], PathElement::Root());
        assert_eq!(path.elements[1], PathElement::ArrayLocator(-2));
    }

    #[test]
    fn test_json_path_invalid() {
        let invalid_values = vec![
            "", "$$$", "$.", "$ ", "$[", "$]", "$[-1]", "x", "[]", "$[0", "$[0x]", "$\"",
        ];

        for value in invalid_values {
            let path = json_path(value);

            match path {
                Err(crate::error::LimboError::Constraint(_)) => {
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
        assert_eq!(path.elements[1], PathElement::Key(Cow::Borrowed("store")));
        assert_eq!(path.elements[2], PathElement::Key(Cow::Borrowed("book")));
        assert_eq!(path.elements[3], PathElement::ArrayLocator(0));
        assert_eq!(path.elements[4], PathElement::Key(Cow::Borrowed("title")));
    }
}
