use winnow::ascii::Caseless;
use winnow::combinator::{alt, delimited};
use winnow::error::{AddContext, ParserError, StrContext};
use winnow::prelude::*;
use winnow::token::{literal, take_while};
use winnow::PResult;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SqlToken<'a> {
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
    Asc,
    Desc,
    Semicolon,
    Like,
    Literal(&'a [u8]),
    Identifier(&'a [u8]),
}

type Stream<'i> = &'i [u8];

pub fn parse_sql_string_to_tokens<
    'i,
    E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>,
>(
    input: &'i [u8],
) -> PResult<Vec<SqlToken<'i>>, E> {
    let mut tokens = Vec::new();
    let mut stream = input;

    while !stream.is_empty() {
        match parse_sql_token(&mut stream) {
            Ok(token) => {
                if token == SqlToken::Semicolon {
                    break;
                }
                tokens.push(token);
            }
            Err(e) => return Err(e),
        }
    }

    Ok(tokens)
}

fn parse_sql_token<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    delimited(ws, sql_token, ws).parse_next(input)
}

fn comparison_operator<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    alt((
        tk_neq.value(SqlToken::Neq),
        tk_eq.value(SqlToken::Eq),
        tk_ge.value(SqlToken::Ge),
        tk_gt.value(SqlToken::Gt),
        tk_le.value(SqlToken::Le),
        tk_lt.value(SqlToken::Lt),
    ))
    .parse_next(input)
}

fn join_related_stuff<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    alt((
        tk_join.value(SqlToken::Join),
        tk_inner.value(SqlToken::Inner),
        tk_outer.value(SqlToken::Outer),
        tk_left.value(SqlToken::Left),
        tk_on.value(SqlToken::On),
    ))
    .parse_next(input)
}

fn asc_desc<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    alt((tk_asc.value(SqlToken::Asc), tk_desc.value(SqlToken::Desc))).parse_next(input)
}

fn mathy_operator<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    alt((
        tk_plus.value(SqlToken::Plus),
        tk_minus.value(SqlToken::Minus),
        tk_slash.value(SqlToken::Slash),
        tk_asterisk.value(SqlToken::Asterisk),
        tk_paren_l.value(SqlToken::ParenL),
        tk_paren_r.value(SqlToken::ParenR),
        tk_comma.value(SqlToken::Comma),
    ))
    .parse_next(input)
}

fn keyword<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    alt((
        tk_select.value(SqlToken::Select),
        tk_from.value(SqlToken::From),
        tk_where.value(SqlToken::Where),
        tk_and.value(SqlToken::And),
        tk_order_by.value(SqlToken::OrderBy),
        tk_or.value(SqlToken::Or),
        tk_not.value(SqlToken::Not),
        tk_group_by.value(SqlToken::GroupBy),
        tk_limit.value(SqlToken::Limit),
        join_related_stuff,
        asc_desc,
        tk_as.value(SqlToken::As),
        tk_like.value(SqlToken::Like),
    ))
    .parse_next(input)
}

fn sql_token<'i, E: ParserError<Stream<'i>> + AddContext<Stream<'i>, StrContext>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    alt((
        keyword,
        comparison_operator,
        mathy_operator,
        tk_literal.map(SqlToken::Literal),
        tk_identifier.map(SqlToken::Identifier),
        tk_semicolon,
    ))
    .parse_next(input)
}

fn ws<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    take_while(0.., |b| WS.contains(&b)).parse_next(input)
}

const WS: &[u8] = &[b' ', b'\t', b'\r', b'\n'];

fn tk_like<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("LIKE")).parse_next(input)
}

fn tk_semicolon<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<SqlToken<'i>, E> {
    literal(";").value(SqlToken::Semicolon).parse_next(input)
}

fn tk_join<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("JOIN")).parse_next(input)
}

fn tk_inner<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("INNER")).parse_next(input)
}

fn tk_outer<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("OUTER")).parse_next(input)
}

fn tk_left<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("LEFT")).parse_next(input)
}

fn tk_on<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("ON")).parse_next(input)
}

fn tk_as<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("AS")).parse_next(input)
}

fn tk_select<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("SELECT")).parse_next(input)
}

fn tk_from<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("FROM")).parse_next(input)
}

fn tk_where<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("WHERE")).parse_next(input)
}

fn tk_and<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("AND")).parse_next(input)
}

fn tk_or<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("OR")).parse_next(input)
}

fn tk_not<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("NOT")).parse_next(input)
}

fn tk_group_by<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("GROUP BY")).parse_next(input)
}

fn tk_order_by<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("ORDER BY")).parse_next(input)
}

fn tk_limit<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("LIMIT")).parse_next(input)
}

fn tk_eq<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("=").parse_next(input)
}

fn tk_neq<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    alt((literal("!="), literal("<>"))).parse_next(input)
}

fn tk_ge<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(">=").parse_next(input)
}

fn tk_gt<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(">").parse_next(input)
}

fn tk_le<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("<=").parse_next(input)
}

fn tk_lt<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("<").parse_next(input)
}

fn tk_paren_l<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("(").parse_next(input)
}

fn tk_paren_r<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(")").parse_next(input)
}

fn tk_comma<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(",").parse_next(input)
}

fn tk_asterisk<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("*").parse_next(input)
}

fn tk_plus<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("+").parse_next(input)
}

fn tk_minus<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("-").parse_next(input)
}

fn tk_slash<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal("/").parse_next(input)
}

fn tk_identifier<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    take_while(1.., |b: u8| b.is_ascii_alphanumeric() || b == b'_').parse_next(input)
}

fn tk_literal<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    alt((
        delimited(
            literal("'"),
            take_while(0.., |b: u8| b != b'\''),
            literal("'"),
        ),
        take_while(1.., |b: u8| b.is_ascii_digit() || b == b'.'),
    ))
    .parse_next(input)
}

fn tk_asc<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("ASC")).parse_next(input)
}

fn tk_desc<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<&'i [u8], E> {
    literal(Caseless("DESC")).parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = winnow::error::ContextError;

    #[test]
    fn test_token_parsing() {
        let mut test_cases = vec![
            ("SELECT", SqlToken::Select),
            ("FROM", SqlToken::From),
            ("WHERE", SqlToken::Where),
            ("AND", SqlToken::And),
            ("OR", SqlToken::Or),
            ("NOT", SqlToken::Not),
            ("GROUP BY", SqlToken::GroupBy),
            ("ORDER BY", SqlToken::OrderBy),
            ("LIMIT", SqlToken::Limit),
            ("=", SqlToken::Eq),
            ("!=", SqlToken::Neq),
            ("<>", SqlToken::Neq),
            (">=", SqlToken::Ge),
            (">", SqlToken::Gt),
            ("<=", SqlToken::Le),
            ("<", SqlToken::Lt),
            ("(", SqlToken::ParenL),
            (")", SqlToken::ParenR),
            (",", SqlToken::Comma),
            ("*", SqlToken::Asterisk),
            ("+", SqlToken::Plus),
            ("-", SqlToken::Minus),
            ("/", SqlToken::Slash),
            ("column_name", SqlToken::Identifier(b"column_name")),
            ("'string literal'", SqlToken::Literal(b"string literal")),
            ("123", SqlToken::Literal(b"123")),
            ("123.45", SqlToken::Literal(b"123.45")),
        ];

        for (input, expected) in test_cases.iter_mut() {
            assert_eq!(
                parse_sql_token::<Error>(&mut input.as_bytes()),
                Ok(expected.clone())
            );
        }

        // Test case-insensitivity
        assert_eq!(
            parse_sql_token::<Error>(&mut "select".as_bytes()),
            Ok(SqlToken::Select)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "FROM".as_bytes()),
            Ok(SqlToken::From)
        );

        // Test with trailing content
        let mut input = "SELECT ".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::Select));
        assert_eq!(input, b"");

        // Test with trailing content for other tokens
        let mut input = "WHERE condition".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::Where));
        assert_eq!(input, b"condition");

        let mut input = "AND another_condition".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::And));
        assert_eq!(input, b"another_condition");

        let mut input = "OR alternative".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::Or));
        assert_eq!(input, b"alternative");

        let mut input = "NOT excluded".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::Not));
        assert_eq!(input, b"excluded");

        let mut input = "GROUP BY column".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::GroupBy));
        assert_eq!(input, b"column");

        let mut input = "ORDER BY column DESC".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::OrderBy));
        assert_eq!(input, b"column DESC");

        let mut input = "LIMIT 10".as_bytes();
        assert_eq!(parse_sql_token::<Error>(&mut input), Ok(SqlToken::Limit));
        assert_eq!(input, b"10");
        assert_eq!(
            parse_sql_token::<Error>(&mut "FROM table".as_bytes()),
            Ok(SqlToken::From)
        );

        // Test identifier
        let mut input = "column_name".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut input),
            Ok(SqlToken::Identifier(b"column_name"))
        );
        assert_eq!(input, b"");

        // Test string literal
        let mut input = "'string value'".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut input),
            Ok(SqlToken::Literal(b"string value"))
        );
        assert_eq!(input, b"");

        // Test numeric literal
        let mut input = "123.45".as_bytes();
        assert_eq!(
            parse_sql_token::<Error>(&mut input),
            Ok(SqlToken::Literal(b"123.45"))
        );
        assert_eq!(input, b"");

        // Test comparison operators
        assert_eq!(
            parse_sql_token::<Error>(&mut "=".as_bytes()),
            Ok(SqlToken::Eq)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "!=".as_bytes()),
            Ok(SqlToken::Neq)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "<>".as_bytes()),
            Ok(SqlToken::Neq)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut ">=".as_bytes()),
            Ok(SqlToken::Ge)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut ">".as_bytes()),
            Ok(SqlToken::Gt)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "<=".as_bytes()),
            Ok(SqlToken::Le)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "<".as_bytes()),
            Ok(SqlToken::Lt)
        );

        // Test new tokens
        assert_eq!(
            parse_sql_token::<Error>(&mut "(".as_bytes()),
            Ok(SqlToken::ParenL)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut ")".as_bytes()),
            Ok(SqlToken::ParenR)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut ",".as_bytes()),
            Ok(SqlToken::Comma)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "*".as_bytes()),
            Ok(SqlToken::Asterisk)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "+".as_bytes()),
            Ok(SqlToken::Plus)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "-".as_bytes()),
            Ok(SqlToken::Minus)
        );
        assert_eq!(
            parse_sql_token::<Error>(&mut "/".as_bytes()),
            Ok(SqlToken::Slash)
        );
    }

    #[test]
    fn test_parse_sql_string_into_tokens() {
        let input = b"SELECT column1 FROM table WHERE condition ORDER BY column1 LIMIT 10";
        let expected = vec![
            SqlToken::Select,
            SqlToken::Identifier(b"column1"),
            SqlToken::From,
            SqlToken::Identifier(b"table"),
            SqlToken::Where,
            SqlToken::Identifier(b"condition"),
            SqlToken::OrderBy,
            SqlToken::Identifier(b"column1"),
            SqlToken::Limit,
            SqlToken::Literal(b"10"),
        ];

        let result = parse_sql_string_to_tokens::<Error>(input);
        assert_eq!(result, Ok(expected));

        // Test with mixed case and literals
        let input = b"select col1 from TABLE where COL1 = 'value' GROUP BY col1";
        let expected = vec![
            SqlToken::Select,
            SqlToken::Identifier(b"col1"),
            SqlToken::From,
            SqlToken::Identifier(b"TABLE"),
            SqlToken::Where,
            SqlToken::Identifier(b"COL1"),
            SqlToken::Eq,
            SqlToken::Literal(b"value"),
            SqlToken::GroupBy,
            SqlToken::Identifier(b"col1"),
        ];

        let result = parse_sql_string_to_tokens::<Error>(input).unwrap();
        assert_eq!(result, expected);

        // Test with new tokens
        let input = b"SELECT * FROM (SELECT id, name FROM users) AS subquery WHERE id > 5";
        let expected = vec![
            SqlToken::Select,
            SqlToken::Asterisk,
            SqlToken::From,
            SqlToken::ParenL,
            SqlToken::Select,
            SqlToken::Identifier(b"id"),
            SqlToken::Comma,
            SqlToken::Identifier(b"name"),
            SqlToken::From,
            SqlToken::Identifier(b"users"),
            SqlToken::ParenR,
            SqlToken::As,
            SqlToken::Identifier(b"subquery"),
            SqlToken::Where,
            SqlToken::Identifier(b"id"),
            SqlToken::Gt,
            SqlToken::Literal(b"5"),
        ];

        let result = parse_sql_string_to_tokens::<Error>(input).unwrap();
        assert_eq!(result, expected);
    }
}
