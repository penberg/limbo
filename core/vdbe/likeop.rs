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
}
