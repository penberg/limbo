use std::collections::HashMap;

use regex::{Regex, RegexBuilder};

use crate::{types::OwnedValue, LimboError};

pub fn construct_like_escape_arg(escape_value: &OwnedValue) -> Result<char, LimboError> {
    match escape_value {
        OwnedValue::Text(text) => {
            let mut escape_chars = text.value.chars();
            match (escape_chars.next(), escape_chars.next()) {
                (Some(escape), None) => Ok(escape),
                _ => Result::Err(LimboError::Constraint(
                    "ESCAPE expression must be a single character".to_string(),
                )),
            }
        }
        _ => {
            unreachable!("Like on non-text registers");
        }
    }
}

// Implements LIKE pattern matching with escape
pub fn exec_like_with_escape(pattern: &str, text: &str, escape: char) -> bool {
    construct_like_regex_with_escape(pattern, escape).is_match(text)
}

fn construct_like_regex_with_escape(pattern: &str, escape: char) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    let mut chars = pattern.chars();

    while let Some(ch) = chars.next() {
        match ch {
            esc_ch if esc_ch == escape => {
                if let Some(escaped_char) = chars.next() {
                    if regex_syntax::is_meta_character(escaped_char) {
                        regex_pattern.push('\\');
                    }
                    regex_pattern.push(escaped_char);
                }
            }
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            c => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(c);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
}

// Implements GLOB pattern matching. Caches the constructed regex if a cache is provided
pub fn exec_glob(
    regex_cache: Option<&mut HashMap<String, Regex>>,
    pattern: &str,
    text: &str,
) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => match construct_glob_regex(pattern) {
                Ok(re) => {
                    let res = re.is_match(text);
                    cache.insert(pattern.to_string(), re);
                    res
                }
                Err(_) => false,
            },
        }
    } else {
        construct_glob_regex(pattern)
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }
}

fn push_char_to_regex_pattern(c: char, regex_pattern: &mut String) {
    if regex_syntax::is_meta_character(c) {
        regex_pattern.push('\\');
    }
    regex_pattern.push(c);
}

fn construct_glob_regex(pattern: &str) -> Result<Regex, LimboError> {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    let mut chars = pattern.chars();
    let mut bracket_closed = true;

    while let Some(ch) = chars.next() {
        match ch {
            '[' => {
                bracket_closed = false;
                regex_pattern.push('[');
                if let Some(next_ch) = chars.next() {
                    match next_ch {
                        ']' => {
                            // The string  enclosed by the brackets cannot be empty;
                            // therefore ']' can be allowed between the brackets,
                            // provided that it is the first  character.
                            // so this means
                            // - `[]]` will be translated to `[\]]`
                            // - `[[]` will be translated to `[\[]`
                            regex_pattern.push_str("\\]");
                        }
                        '^' => {
                            // For the most cases we can pass `^` directly to regex
                            // but in certain cases like [^][a] , `[^]` will  make regex crate
                            // throw unenclosed character class. So this means
                            // - `[^][a]` will be translated to `[^\]a]`
                            regex_pattern.push('^');
                            if let Some(next_ch_2) = chars.next() {
                                match next_ch_2 {
                                    ']' => {
                                        regex_pattern.push('\\');
                                        regex_pattern.push(']');
                                    }
                                    c => {
                                        push_char_to_regex_pattern(c, &mut regex_pattern);
                                    }
                                }
                            }
                        }
                        c => {
                            push_char_to_regex_pattern(c, &mut regex_pattern);
                        }
                    }
                };

                while let Some(next_ch) = chars.next() {
                    match next_ch {
                        ']' => {
                            bracket_closed = true;
                            regex_pattern.push(']');
                            break;
                        }
                        '-' => {
                            regex_pattern.push('-');
                        }
                        c => {
                            push_char_to_regex_pattern(c, &mut regex_pattern);
                        }
                    }
                }
            }
            '?' => {
                regex_pattern.push('.');
            }
            '*' => {
                regex_pattern.push_str(".*");
            }
            c => {
                push_char_to_regex_pattern(c, &mut regex_pattern);
            }
        }
    }
    regex_pattern.push('$');

    if bracket_closed {
        Ok(Regex::new(&regex_pattern).unwrap())
    } else {
        Result::Err(LimboError::Constraint(
            "blob pattern is not closed".to_string(),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_exec_like_with_escape() {
        assert!(exec_like_with_escape("abcX%", "abc%", 'X'));
        assert!(!exec_like_with_escape("abcX%", "abc5", 'X'));
        assert!(!exec_like_with_escape("abcX%", "abc", 'X'));
        assert!(!exec_like_with_escape("abcX%", "abcX%", 'X'));
        assert!(!exec_like_with_escape("abcX%", "abc%%", 'X'));
        assert!(exec_like_with_escape("abcX_", "abc_", 'X'));
        assert!(!exec_like_with_escape("abcX_", "abc5", 'X'));
        assert!(!exec_like_with_escape("abcX_", "abc", 'X'));
        assert!(!exec_like_with_escape("abcX_", "abcX_", 'X'));
        assert!(!exec_like_with_escape("abcX_", "abc__", 'X'));
        assert!(exec_like_with_escape("abcXX", "abcX", 'X'));
        assert!(!exec_like_with_escape("abcXX", "abc5", 'X'));
        assert!(!exec_like_with_escape("abcXX", "abc", 'X'));
        assert!(!exec_like_with_escape("abcXX", "abcXX", 'X'));
    }

    #[test]
    fn test_glob_no_cache() {
        assert!(exec_glob(None, r#"?*/abc/?*"#, r#"x//a/ab/abc/y"#));
        assert!(exec_glob(None, r#"a[1^]"#, r#"a1"#));
        assert!(exec_glob(None, r#"a[1^]*"#, r#"a^"#));
        assert!(!exec_glob(None, r#"a[a*"#, r#"a["#));
        assert!(!exec_glob(None, r#"a[a"#, r#"a[a"#));
        assert!(exec_glob(None, r#"a[[]"#, r#"a["#));
        assert!(exec_glob(None, r#"abc[^][*?]efg"#, r#"abcdefg"#));
        assert!(!exec_glob(None, r#"abc[^][*?]efg"#, r#"abc]efg"#));
    }
}
