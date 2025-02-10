use crate::types::OwnedValue;
use crate::LimboError;

#[inline(always)]
pub fn exec_printf(values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    if values.is_empty() {
        return Ok(OwnedValue::Null);
    }
    let format_str = match &values[0] {
        OwnedValue::Text(t) => t.as_str(),
        _ => return Ok(OwnedValue::Null),
    };

    let mut result = String::new();
    let mut args_index = 1;
    let mut chars = format_str.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '%' {
            result.push(c);
            continue;
        }

        match chars.next() {
            Some('%') => {
                result.push('%');
                continue;
            }
            Some('d') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                match &values[args_index] {
                    OwnedValue::Integer(i) => result.push_str(&i.to_string()),
                    OwnedValue::Float(f) => result.push_str(&f.to_string()),
                    _ => result.push_str("0".into()),
                }
                args_index += 1;
            }
            Some('s') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                match &values[args_index] {
                    OwnedValue::Text(t) => result.push_str(t.as_str()),
                    OwnedValue::Null => result.push_str("(null)"),
                    v => result.push_str(&v.to_string()),
                }
                args_index += 1;
            }
            Some('f') => {
                if args_index >= values.len() {
                    return Err(LimboError::InvalidArgument("not enough arguments".into()));
                }
                match &values[args_index] {
                    OwnedValue::Float(f) => result.push_str(&f.to_string()),
                    OwnedValue::Integer(i) => result.push_str(&(*i as f64).to_string()),
                    _ => result.push_str("0.0".into()),
                }
                args_index += 1;
            }
            None => {
                return Err(LimboError::InvalidArgument(
                    "incomplete format specifier".into(),
                ))
            }
            _ => {
                return Err(LimboError::InvalidFormatter(
                    "this formatter is not supported".into(),
                ));
            }
        }
    }
    Ok(OwnedValue::build_text(&result))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> OwnedValue {
        OwnedValue::build_text(value)
    }

    fn integer(value: i64) -> OwnedValue {
        OwnedValue::Integer(value)
    }

    fn float(value: f64) -> OwnedValue {
        OwnedValue::Float(value)
    }

    #[test]
    fn test_printf_no_args() {
        assert_eq!(exec_printf(&[]).unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_printf_basic_string() {
        assert_eq!(
            exec_printf(&[text("Hello World")]).unwrap(),
            text("Hello World")
        );
    }

    #[test]
    fn test_printf_string_formatting() {
        let test_cases = vec![
            // Simple string substitution
            (
                vec![text("Hello, %s!"), text("World")],
                text("Hello, World!"),
            ),
            // Multiple string substitutions
            (
                vec![text("%s %s!"), text("Hello"), text("World")],
                text("Hello World!"),
            ),
            // String with null value
            (
                vec![text("Hello, %s!"), OwnedValue::Null],
                text("Hello, (null)!"),
            ),
            // String with number conversion
            (vec![text("Value: %s"), integer(42)], text("Value: 42")),
            // Escaping percent sign
            (vec![text("100%% complete")], text("100% complete")),
        ];
        for (input, output) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), output);
        }
    }

    #[test]
    fn test_printf_integer_formatting() {
        let test_cases = vec![
            // Basic integer formatting
            (vec![text("Number: %d"), integer(42)], text("Number: 42")),
            // Negative integer
            (vec![text("Number: %d"), integer(-42)], text("Number: -42")),
            // Multiple integers
            (
                vec![text("%d + %d = %d"), integer(2), integer(3), integer(5)],
                text("2 + 3 = 5"),
            ),
            // Non-numeric value defaults to 0
            (
                vec![text("Number: %d"), text("not a number")],
                text("Number: 0"),
            ),
        ];
        for (input, output) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), output)
        }
    }

    #[test]
    fn test_printf_float_formatting() {
        let test_cases = vec![
            // Basic float formatting
            (vec![text("Number: %f"), float(42.5)], text("Number: 42.5")),
            // Negative float
            (
                vec![text("Number: %f"), float(-42.5)],
                text("Number: -42.5"),
            ),
            // Integer as float
            (vec![text("Number: %f"), integer(42)], text("Number: 42")),
            // Multiple floats
            (
                vec![text("%f + %f = %f"), float(2.5), float(3.5), float(6.0)],
                text("2.5 + 3.5 = 6"),
            ),
            // Non-numeric value defaults to 0.0
            (
                vec![text("Number: %f"), text("not a number")],
                text("Number: 0.0"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), expected);
        }
    }

    #[test]
    fn test_printf_mixed_formatting() {
        let test_cases = vec![
            // Mix of string and integer
            (
                vec![text("%s: %d"), text("Count"), integer(42)],
                text("Count: 42"),
            ),
            // Mix of all types
            (
                vec![
                    text("%s: %d (%f%%)"),
                    text("Progress"),
                    integer(75),
                    float(75.5),
                ],
                text("Progress: 75 (75.5%)"),
            ),
            // Complex format
            (
                vec![
                    text("Name: %s, ID: %d, Score: %f"),
                    text("John"),
                    integer(123),
                    float(95.5),
                ],
                text("Name: John, ID: 123, Score: 95.5"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), expected);
        }
    }

    #[test]
    fn test_printf_error_cases() {
        let error_cases = vec![
            // Not enough arguments
            vec![text("%d %d"), integer(42)],
            // Invalid format string
            vec![text("%z"), integer(42)],
            // Incomplete format specifier
            vec![text("incomplete %")],
        ];

        for case in error_cases {
            assert!(exec_printf(&case).is_err());
        }
    }

    #[test]
    fn test_printf_edge_cases() {
        let test_cases = vec![
            // Empty format string
            (vec![text("")], text("")),
            // Only percent signs
            (vec![text("%%%%")], text("%%")),
            // String with no format specifiers
            (vec![text("No substitutions")], text("No substitutions")),
            // Multiple consecutive format specifiers
            (
                vec![text("%d%d%d"), integer(1), integer(2), integer(3)],
                text("123"),
            ),
            // Format string with special characters
            (
                vec![text("Special chars: %s"), text("\n\t\r")],
                text("Special chars: \n\t\r"),
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(exec_printf(&input).unwrap(), expected);
        }
    }
}
