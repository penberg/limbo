use std::ops::Range;

use winnow::ascii::Caseless;
use winnow::combinator::{alt, delimited, dispatch, fail, opt, peek, preceded, repeat, terminated};
use winnow::error::{AddContext, ParserError, StrContext};
use winnow::token::{any, literal, one_of, take_while};
use winnow::PResult;
use winnow::{prelude::*, Located};

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SqlTokenKind {
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    GroupBy,
    OrderBy,
    Limit,
    ParenL,
    ParenR,
    Comma,
    Asterisk,
    Plus,
    Minus,
    Slash,
    Eq,
    Neq,
    Ge,
    Gt,
    Le,
    Lt,
    Join,
    Left,
    Inner,
    Outer,
    On,
    In,
    NotIn,
    Asc,
    Desc,
    Semicolon,
    Period,
    Like,
    NotLike,
    Glob,
    Literal,
    Identifier,
    Between,
    Case,
    Having,
    When,
    Then,
    Else,
    End,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SqlToken {
    kind: SqlTokenKind,
    span: Range<u32>,
}

impl SqlToken {
    pub fn materialize<'a>(&self, source: &'a [u8]) -> &'a [u8] {
        &source[self.span.start as usize..self.span.end as usize]
    }

    pub fn print(&self, source: &[u8]) -> String {
        let token_slice = &source[self.span.start as usize..self.span.end as usize];
        let token_info = match self.kind {
            SqlTokenKind::Literal => {
                format!("literal: {}", std::str::from_utf8(token_slice).unwrap())
            }
            SqlTokenKind::Identifier => {
                format!("identifier: {}", std::str::from_utf8(token_slice).unwrap())
            }
            SqlTokenKind::Select => format!("'SELECT'"),
            SqlTokenKind::From => format!("'FROM'"),
            SqlTokenKind::Where => format!("'WHERE'"),
            SqlTokenKind::And => format!("'AND'"),
            SqlTokenKind::Or => format!("'OR'"),
            SqlTokenKind::Not => format!("'NOT'"),
            SqlTokenKind::As => format!("'AS'"),
            SqlTokenKind::GroupBy => format!("'GROUP BY'"),
            SqlTokenKind::OrderBy => format!("'ORDER BY'"),
            SqlTokenKind::Limit => format!("'LIMIT'"),
            SqlTokenKind::ParenL => format!("'('"),
            SqlTokenKind::ParenR => format!("')'"),
            SqlTokenKind::Comma => format!("','"),
            SqlTokenKind::Asterisk => format!("'*'"),
            SqlTokenKind::Plus => format!("'+'"),
            SqlTokenKind::Minus => format!("'-'"),
            SqlTokenKind::Slash => format!("/"),
            SqlTokenKind::Eq => format!("="),
            SqlTokenKind::Neq => format!("!="),
            SqlTokenKind::Ge => format!(">="),
            SqlTokenKind::Gt => format!(">"),
            SqlTokenKind::Le => format!("<="),
            SqlTokenKind::Lt => format!("<"),
            SqlTokenKind::Join => format!("JOIN"),
            SqlTokenKind::Left => format!("LEFT"),
            SqlTokenKind::Inner => format!("INNER"),
            SqlTokenKind::Outer => format!("OUTER"),
            SqlTokenKind::On => format!("ON"),
            SqlTokenKind::In => format!("IN"),
            SqlTokenKind::NotIn => format!("NOT IN"),
            SqlTokenKind::Asc => format!("ASC"),
            SqlTokenKind::Desc => format!("DESC"),
            SqlTokenKind::Semicolon => format!(";"),
            SqlTokenKind::Period => format!("."),
            SqlTokenKind::Like => format!("LIKE"),
            SqlTokenKind::NotLike => format!("NOT LIKE"),
            SqlTokenKind::Glob => format!("GLOB"),
            SqlTokenKind::Between => format!("BETWEEN"),
            SqlTokenKind::Case => format!("CASE"),
            SqlTokenKind::Having => format!("HAVING"),
            SqlTokenKind::When => format!("WHEN"),
            SqlTokenKind::Then => format!("THEN"),
            SqlTokenKind::Else => format!("ELSE"),
            SqlTokenKind::End => format!("END"),
        };
        // Add context before token start, but minimum is 0
        let error_span_left = std::cmp::max(0, self.span.start as i64 - 10);
        let error_span_right = std::cmp::min(source.len(), self.span.end as usize + 10);
        format!(
            "{} at position {} near '{}'",
            token_info,
            self.span.start,
            std::str::from_utf8(&source[error_span_left as usize..error_span_right as usize])
                .unwrap()
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SqlTokenStream<'a> {
    pub source: &'a [u8],
    pub tokens: Vec<SqlToken>,
    pub index: usize,
}

impl<'a> SqlTokenStream<'a> {
    pub fn next_token(&mut self) -> Option<SqlToken> {
        if self.index < self.tokens.len() {
            let token = self.tokens[self.index].clone();

            self.index += 1;
            Some(token)
        } else {
            None
        }
    }

    pub fn peek(&self, offset: usize) -> Option<SqlToken> {
        if self.index + offset < self.tokens.len() {
            Some(self.tokens[self.index + offset].clone())
        } else {
            None
        }
    }

    pub fn peek_kind(&self, offset: usize) -> Option<SqlTokenKind> {
        self.peek(offset).map(|token| token.kind)
    }
}

pub fn parse_sql_string_to_tokens<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &'i [u8],
) -> PResult<SqlTokenStream<'i>, E> {
    // We'll use a heuristic to guess the initial capacity of the vector
    // based on the length of the input.
    let initial_vec_capacity = input.len() / 2;
    let mut tokens = Vec::with_capacity(initial_vec_capacity);
    let mut stream = Located::new(input);

    while !stream.is_empty() {
        match parse_sql_token(&mut stream) {
            Ok(token) => {
                if token.kind == SqlTokenKind::Semicolon {
                    break;
                }
                tokens.push(token);
            }
            Err(e) => return Err(e),
        }
    }

    Ok(SqlTokenStream {
        source: input,
        tokens,
        index: 0,
    })
}

fn parse_sql_token<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlToken, E> {
    delimited(ws, sql_token, ws).parse_next(input)
}

fn peek_not_identifier_character<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<(), E> {
    match input.first() {
        Some(b) => {
            if utf8_compatible_identifier_char(*b) {
                fail(input)
            } else {
                Ok(())
            }
        }
        None => Ok(()),
    }
}

fn keywords_a_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("AND")).value(SqlTokenKind::And),
            literal(Caseless("ASC")).value(SqlTokenKind::Asc),
            literal(Caseless("AS")).value(SqlTokenKind::As),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_b_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("BETWEEN")).value(SqlTokenKind::Between),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_c_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("CASE")).value(SqlTokenKind::Case),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_d_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("DESC")).value(SqlTokenKind::Desc),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_e_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("ELSE")).value(SqlTokenKind::Else),
            literal(Caseless("END")).value(SqlTokenKind::End),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_f_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("FROM")).value(SqlTokenKind::From),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_g_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("GLOB")).value(SqlTokenKind::Glob),
            literal(Caseless("GROUP BY")).value(SqlTokenKind::GroupBy),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_h_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("HAVING")).value(SqlTokenKind::Having),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_i_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("INNER")).value(SqlTokenKind::Inner),
            literal(Caseless("IN")).value(SqlTokenKind::In),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_j_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("JOIN")).value(SqlTokenKind::Join),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_l_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("LEFT")).value(SqlTokenKind::Left),
            literal(Caseless("LIKE")).value(SqlTokenKind::Like),
            literal(Caseless("LIMIT")).value(SqlTokenKind::Limit),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_n_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("NOT IN")).value(SqlTokenKind::NotIn),
            literal(Caseless("NOT LIKE")).value(SqlTokenKind::NotLike),
            literal(Caseless("NOT")).value(SqlTokenKind::Not),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_o_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("ON")).value(SqlTokenKind::On),
            literal(Caseless("ORDER BY")).value(SqlTokenKind::OrderBy),
            literal(Caseless("OR")).value(SqlTokenKind::Or),
            literal(Caseless("OUTER")).value(SqlTokenKind::Outer),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_s_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("SELECT")).value(SqlTokenKind::Select),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_t_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((literal(Caseless("THEN")).value(SqlTokenKind::Then),)),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn keywords_w_or_identifier<
    'i,
    E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>,
>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    let keywords = terminated(
        alt((
            literal(Caseless("WHERE")).value(SqlTokenKind::Where),
            literal(Caseless("WHEN")).value(SqlTokenKind::When),
        )),
        peek_not_identifier_character,
    );
    let mut choices = alt((keywords, tk_identifier.value(SqlTokenKind::Identifier)));
    choices.parse_next(input)
}

fn sql_token<'i, E: ParserError<Located<&'i [u8]>> + AddContext<Located<&'i [u8]>, StrContext>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlToken, E> {
    dispatch! {peek(any);
        b'a' | b'A' => keywords_a_or_identifier,
        b'b' | b'B' => keywords_b_or_identifier,
        b'c' | b'C' => keywords_c_or_identifier,
        b'd' | b'D' => keywords_d_or_identifier,
        b'e' | b'E' => keywords_e_or_identifier,
        b'f' | b'F' => keywords_f_or_identifier,
        b'g' | b'G' => keywords_g_or_identifier,
        b'h' | b'H' => keywords_h_or_identifier,
        b'i' | b'I' => keywords_i_or_identifier,
        b'j' | b'J' => keywords_j_or_identifier,
        b'k' | b'K' => tk_identifier.value(SqlTokenKind::Identifier),
        b'l' | b'L' => keywords_l_or_identifier,
        b'm' | b'M' => tk_identifier.value(SqlTokenKind::Identifier),
        b'n' | b'N' => keywords_n_or_identifier,
        b'o' | b'O' => keywords_o_or_identifier,
        b'p' | b'P' => tk_identifier.value(SqlTokenKind::Identifier),
        b'q' | b'Q' => tk_identifier.value(SqlTokenKind::Identifier),
        b'r' | b'R' => tk_identifier.value(SqlTokenKind::Identifier),
        b's' | b'S' => keywords_s_or_identifier,
        b't' | b'T' => keywords_t_or_identifier,
        b'u' | b'U' => tk_identifier.value(SqlTokenKind::Identifier),
        b'w' | b'W' => keywords_w_or_identifier,
        b'x' | b'X' => tk_identifier.value(SqlTokenKind::Identifier),
        b'y' | b'Y' => tk_identifier.value(SqlTokenKind::Identifier),
        b'z' | b'Z' => tk_identifier.value(SqlTokenKind::Identifier),
        b'$' => tk_identifier.value(SqlTokenKind::Identifier),
        b'0'..=b'9' => alt((float, decimal)),
        b'.' => alt((float, tk_period.value(SqlTokenKind::Period))),
        b',' => tk_comma.value(SqlTokenKind::Comma),
        b';' => tk_semicolon.value(SqlTokenKind::Semicolon),
        b'(' => tk_paren_l.value(SqlTokenKind::ParenL),
        b')' => tk_paren_r.value(SqlTokenKind::ParenR),
        b'+' => tk_plus.value(SqlTokenKind::Plus),
        b'-' => tk_minus.value(SqlTokenKind::Minus),
        b'*' => tk_asterisk.value(SqlTokenKind::Asterisk),
        b'/' => tk_slash.value(SqlTokenKind::Slash),
        b'\'' => tk_string_literal.value(SqlTokenKind::Literal),
        b'>' => comparison_operators_gt_ge,
        b'<' => comparison_operators_lt_le_neq,
        b'=' => literal('=').value(SqlTokenKind::Eq),
        b'!' => literal("!=").value(SqlTokenKind::Neq),
        _ => tk_identifier.value(SqlTokenKind::Identifier),
    }
    .with_span()
    .map(|(kind, span)| SqlToken {
        kind,
        span: span.start as u32..span.end as u32,
    })
    .parse_next(input)
}

fn comparison_operators_gt_ge<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        literal(">=").value(SqlTokenKind::Ge),
        literal(">").value(SqlTokenKind::Gt),
    ))
    .parse_next(input)
}

fn comparison_operators_lt_le_neq<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        literal("<=").value(SqlTokenKind::Le),
        literal("<>").value(SqlTokenKind::Neq),
        literal("<").value(SqlTokenKind::Lt),
    ))
    .parse_next(input)
}

fn ws<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    take_while(0.., |b| WS.contains(&b)).parse_next(input)
}

const WS: &[u8] = &[b' ', b'\t', b'\r', b'\n'];

fn tk_period<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(".").parse_next(input)
}

fn tk_semicolon<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    literal(";")
        .value(SqlTokenKind::Semicolon)
        .parse_next(input)
}

fn tk_paren_l<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("(").parse_next(input)
}

fn tk_paren_r<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(")").parse_next(input)
}

fn tk_comma<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal(",").parse_next(input)
}

fn tk_asterisk<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("*").parse_next(input)
}

fn tk_plus<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("+").parse_next(input)
}

fn tk_minus<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("-").parse_next(input)
}

fn tk_slash<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("/").parse_next(input)
}

const HIGHEST_ASCII_CHARACTER: u8 = 0x7F;

fn utf8_compatible_identifier_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c > HIGHEST_ASCII_CHARACTER
}

fn tk_identifier<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    take_while(1.., utf8_compatible_identifier_char).parse_next(input)
}

fn decimal<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    take_while(1.., |c: u8| c.is_ascii_digit())
        .value(SqlTokenKind::Literal)
        .parse_next(input)
}

fn float<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<SqlTokenKind, E> {
    alt((
        // Case one: .42
        (
            '.',
            decimal,
            opt((one_of(['e', 'E']), opt(one_of(['+', '-'])), decimal)),
        )
            .take(),
        // Case two: 42e42 and 42.42e42
        (
            decimal,
            opt(preceded('.', decimal)),
            one_of(['e', 'E']),
            opt(one_of(['+', '-'])),
            decimal,
        )
            .take(),
        // Case three: 42. and 42.42
        (decimal, '.', opt(decimal)).take(),
    ))
    .value(SqlTokenKind::Literal)
    .parse_next(input)
}

fn double_singlequote_escape_sequence<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<&'i [u8], E> {
    literal("''").parse_next(input)
}

fn tk_string_literal<'i, E: ParserError<Located<&'i [u8]>>>(
    input: &mut Located<&'i [u8]>,
) -> PResult<(), E> {
    delimited(
        literal("'"),
        repeat(
            0..,
            alt((
                take_while(1.., |c| c != b'\''),
                double_singlequote_escape_sequence,
            )),
        )
        .fold(|| (), |_, _| ()),
        literal("'"),
    )
    .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = winnow::error::ContextError;

    #[test]
    fn test_token_parsing() {
        let mut test_cases = vec![
            (
                "SELECT",
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
            ),
            (
                "FROM",
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 0..4,
                },
            ),
            (
                "WHERE",
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 0..5,
                },
            ),
            (
                "AND",
                SqlToken {
                    kind: SqlTokenKind::And,
                    span: 0..3,
                },
            ),
            (
                "OR",
                SqlToken {
                    kind: SqlTokenKind::Or,
                    span: 0..2,
                },
            ),
            (
                "NOT",
                SqlToken {
                    kind: SqlTokenKind::Not,
                    span: 0..3,
                },
            ),
            (
                "GROUP BY",
                SqlToken {
                    kind: SqlTokenKind::GroupBy,
                    span: 0..8,
                },
            ),
            (
                "ORDER BY",
                SqlToken {
                    kind: SqlTokenKind::OrderBy,
                    span: 0..8,
                },
            ),
            (
                "LIMIT",
                SqlToken {
                    kind: SqlTokenKind::Limit,
                    span: 0..5,
                },
            ),
            (
                "=",
                SqlToken {
                    kind: SqlTokenKind::Eq,
                    span: 0..1,
                },
            ),
            (
                "!=",
                SqlToken {
                    kind: SqlTokenKind::Neq,
                    span: 0..2,
                },
            ),
            (
                "<>",
                SqlToken {
                    kind: SqlTokenKind::Neq,
                    span: 0..2,
                },
            ),
            (
                ">=",
                SqlToken {
                    kind: SqlTokenKind::Ge,
                    span: 0..2,
                },
            ),
            (
                ">",
                SqlToken {
                    kind: SqlTokenKind::Gt,
                    span: 0..1,
                },
            ),
            (
                "<=",
                SqlToken {
                    kind: SqlTokenKind::Le,
                    span: 0..2,
                },
            ),
            (
                "<",
                SqlToken {
                    kind: SqlTokenKind::Lt,
                    span: 0..1,
                },
            ),
            (
                "(",
                SqlToken {
                    kind: SqlTokenKind::ParenL,
                    span: 0..1,
                },
            ),
            (
                ")",
                SqlToken {
                    kind: SqlTokenKind::ParenR,
                    span: 0..1,
                },
            ),
            (
                ",",
                SqlToken {
                    kind: SqlTokenKind::Comma,
                    span: 0..1,
                },
            ),
            (
                "*",
                SqlToken {
                    kind: SqlTokenKind::Asterisk,
                    span: 0..1,
                },
            ),
            (
                "+",
                SqlToken {
                    kind: SqlTokenKind::Plus,
                    span: 0..1,
                },
            ),
            (
                "-",
                SqlToken {
                    kind: SqlTokenKind::Minus,
                    span: 0..1,
                },
            ),
            (
                "/",
                SqlToken {
                    kind: SqlTokenKind::Slash,
                    span: 0..1,
                },
            ),
            (
                "column_name",
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 0..11,
                },
            ),
            (
                "🤡",
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 0..4,
                },
            ),
            (
                "'string literal'",
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 0..16,
                },
            ),
            (
                "123",
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 0..3,
                },
            ),
            (
                "123.45",
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 0..6,
                },
            ),
            (
                "BETWEEN",
                SqlToken {
                    kind: SqlTokenKind::Between,
                    span: 0..7,
                },
            ),
            (
                "CASE",
                SqlToken {
                    kind: SqlTokenKind::Case,
                    span: 0..4,
                },
            ),
            (
                "HAVING",
                SqlToken {
                    kind: SqlTokenKind::Having,
                    span: 0..6,
                },
            ),
            (
                "ELSE",
                SqlToken {
                    kind: SqlTokenKind::Else,
                    span: 0..4,
                },
            ),
            (
                "END",
                SqlToken {
                    kind: SqlTokenKind::End,
                    span: 0..3,
                },
            ),
            (
                "WHEN",
                SqlToken {
                    kind: SqlTokenKind::When,
                    span: 0..4,
                },
            ),
            (
                "THEN",
                SqlToken {
                    kind: SqlTokenKind::Then,
                    span: 0..4,
                },
            ),
        ];

        for (input, expected) in test_cases.iter_mut() {
            assert_eq!(
                parse_sql_token::<Error>(&mut Located::new(input.as_bytes())),
                Ok(expected.clone())
            );
        }

        // Test case-insensitivity
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("select".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Select,
                span: 0..6
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("FROM".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::From,
                span: 0..4
            })
        );

        // Test with trailing content
        let input = "SELECT ".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Select,
                span: 0..6
            })
        );

        // Test with trailing content for other tokens
        let input = "WHERE condition".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Where,
                span: 0..5
            })
        );

        let input = "AND another_condition".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::And,
                span: 0..3
            })
        );

        let input = "OR alternative".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Or,
                span: 0..2
            })
        );

        let input = "NOT excluded".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Not,
                span: 0..3
            })
        );

        let input = "GROUP BY column".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::GroupBy,
                span: 0..8
            })
        );

        let input = "ORDER BY column DESC".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::OrderBy,
                span: 0..8
            })
        );

        let input = "LIMIT 10".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Limit,
                span: 0..5
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("FROM table".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::From,
                span: 0..4
            })
        );

        // Test identifier
        let input = "column_name".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Identifier,
                span: 0..11
            })
        );

        // Test string literal
        let input = "'string value'".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Literal,
                span: 0..14
            })
        );

        // Test numeric literal
        let input = "123.45".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(input)),
            Ok(SqlToken {
                kind: SqlTokenKind::Literal,
                span: 0..6
            })
        );

        // Test comparison operators
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Eq,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("!=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Neq,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("<>".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Neq,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(">=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Ge,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(">".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Gt,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("<=".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Le,
                span: 0..2
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("<".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Lt,
                span: 0..1
            })
        );

        // Test new tokens
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("(".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::ParenL,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(")".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::ParenR,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new(",".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Comma,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("*".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Asterisk,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("+".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Plus,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("-".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Minus,
                span: 0..1
            })
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut Located::new("/".as_bytes())),
            Ok(SqlToken {
                kind: SqlTokenKind::Slash,
                span: 0..1
            })
        );
    }

    #[test]
    fn test_parse_sql_string_into_tokens() {
        let input = b"SELECT column1 FROM table WHERE condition ORDER BY column1 LIMIT 10";
        let expected = SqlTokenStream {
            source: input,
            tokens: vec![
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 7..14,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 15..19,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 20..25,
                },
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 26..31,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 32..41,
                },
                SqlToken {
                    kind: SqlTokenKind::OrderBy,
                    span: 42..50,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 51..58,
                },
                SqlToken {
                    kind: SqlTokenKind::Limit,
                    span: 59..64,
                },
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 65..67,
                },
            ],
            index: 0,
        };

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));

        // Test with mixed case and literals
        let input = b"select col1 from TABLE where COL1 = 'value' GROUP BY col1";
        let expected = SqlTokenStream {
            source: input,
            tokens: vec![
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 7..11,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 12..16,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 17..22,
                },
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 23..28,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 29..33,
                },
                SqlToken {
                    kind: SqlTokenKind::Eq,
                    span: 34..35,
                },
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 36..43,
                },
                SqlToken {
                    kind: SqlTokenKind::GroupBy,
                    span: 44..52,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 53..57,
                },
            ],
            index: 0,
        };

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));

        // Test with new tokens
        let input = b"SELECT * FROM (SELECT id, name FROM users) AS subquery WHERE id > 5";
        let expected = SqlTokenStream {
            source: input,
            tokens: vec![
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 0..6,
                },
                SqlToken {
                    kind: SqlTokenKind::Asterisk,
                    span: 7..8,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 9..13,
                },
                SqlToken {
                    kind: SqlTokenKind::ParenL,
                    span: 14..15,
                },
                SqlToken {
                    kind: SqlTokenKind::Select,
                    span: 15..21,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 22..24,
                },
                SqlToken {
                    kind: SqlTokenKind::Comma,
                    span: 24..25,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 26..30,
                },
                SqlToken {
                    kind: SqlTokenKind::From,
                    span: 31..35,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 36..41,
                },
                SqlToken {
                    kind: SqlTokenKind::ParenR,
                    span: 41..42,
                },
                SqlToken {
                    kind: SqlTokenKind::As,
                    span: 43..45,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 46..54,
                },
                SqlToken {
                    kind: SqlTokenKind::Where,
                    span: 55..60,
                },
                SqlToken {
                    kind: SqlTokenKind::Identifier,
                    span: 61..63,
                },
                SqlToken {
                    kind: SqlTokenKind::Gt,
                    span: 64..65,
                },
                SqlToken {
                    kind: SqlTokenKind::Literal,
                    span: 66..67,
                },
            ],
            index: 0,
        };

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));
    }
}
