use pest_derive::Parser;

#[derive(Parser)]
#[grammar_inline = r#"
array_locator = @{ "[" ~ array_index ~ "]" }
array_index = @{ "\\#-"? ~ ASCII_DIGIT+ }

char_literal = @{ !("\\" | line_terminator) ~ ANY }

char_escape_sequence = @{ single_escape_char | non_escape_char }

double_quote_char = _{
  "\\" ~ escape_sequence |
  line_continuation |
  !"\"" ~ char_literal
}

escape_char = _{ single_escape_char | ASCII_DIGIT | "x" | "u" }

escape_sequence = _{
  char_escape_sequence |
  nul_escape_sequence |
  "x" ~ hex_escape_sequence |
  "u" ~ unicode_escape_sequence
}

hex_escape_sequence = @{ ASCII_HEX_DIGIT{2} }

identifier = ${ identifier_start ~ identifier_part* }

identifier_part = _{
  identifier_start |
  &(
    NONSPACING_MARK |
    DIACRITIC | // not sure about this, spec says "Combining spacing mark (Mc)"
    DECIMAL_NUMBER |
    CONNECTOR_PUNCTUATION |
    "\u{200C}" |
    "\u{200D}"
  ) ~ char_literal
}

identifier_start = _{
  &(unicode_letter | "$" | "_") ~ char_literal |
  "\\u" ~ unicode_escape_sequence
}

key = _{ identifier | string }

line_continuation = _{ "\\" ~ line_terminator_sequence }

line_terminator = _{ "\u{000A}" | "\u{000D}" | "\u{2028}" | "\u{2029}" }

line_terminator_sequence = _{ "\u{000D}" ~ "\u{000A}" | line_terminator }

non_escape_char = _{ !(escape_char | line_terminator) ~ ANY }

nul_escape_sequence = @{ "0" }

single_escape_char = _{ "'" | "\"" | "\\" | "b" | "f" | "n" | "r" | "t" | "v" }

string = ${ "\"" ~ double_quote_char* ~ "\"" }

unicode_escape_sequence = @{ ASCII_HEX_DIGIT{4} }

unicode_letter = _{
  UPPERCASE_LETTER |
  LOWERCASE_LETTER |
  TITLECASE_LETTER |
  MODIFIER_LETTER |
  OTHER_LETTER |
  LETTER_NUMBER
}

path = _{ "$" ~ key ~ (array_locator | "." ~ key)* }
"#]
struct Parser;

// Extracts a single JSON value from a JSON string using a JSON path.
// Assumes that value is already a valid JSON string.
pub(crate) fn json_extract_single<'a>(value: &'a str, path: &str) -> crate::Result<&'a str> {
    Ok(value)
}
