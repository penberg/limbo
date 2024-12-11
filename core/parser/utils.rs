use super::ast::ResultColumn;
use super::expression::parse_expr;
use super::tokenizer::{SqlTokenKind, SqlTokenStream};
use super::Table;

use std::error::Error;
use std::fmt;

#[derive(Debug, PartialEq)]
pub struct SqlParseError {
    pub message: String,
}

impl SqlParseError {
    pub fn new(message: impl Into<String>) -> Self {
        SqlParseError {
            message: message.into(),
        }
    }
}

impl fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQL parse error: {}", self.message)
    }
}

impl Error for SqlParseError {}

pub(crate) fn next_token_is(input: &mut SqlTokenStream, expected: SqlTokenKind) -> bool {
    if input.peek_kind(0) == Some(expected) {
        input.next_token();
        return true;
    }

    false
}

pub(crate) fn expect_token(
    input: &mut SqlTokenStream,
    expected: SqlTokenKind,
) -> Result<(), SqlParseError> {
    if input.peek_kind(0) == Some(expected) {
        input.next_token();
        Ok(())
    } else if let Some(wrong_token) = input.peek(0) {
        Err(SqlParseError::new(format!(
            "Expected token {:?}, got {}",
            expected,
            wrong_token.print(&input.source)
        )))
    } else {
        Err(SqlParseError::new(format!(
            "Expected {:?}, got EOF",
            expected
        )))
    }
}

pub(crate) fn parse_result_columns(
    input: &mut SqlTokenStream,
) -> Result<Vec<ResultColumn>, SqlParseError> {
    let mut columns = vec![parse_result_column(input)?];

    while next_token_is(input, SqlTokenKind::Comma) {
        columns.push(parse_result_column(input)?);
    }

    Ok(columns)
}

fn parse_result_column(input: &mut SqlTokenStream) -> Result<ResultColumn, SqlParseError> {
    if next_token_is(input, SqlTokenKind::Asterisk) {
        Ok(ResultColumn::Star)
    } else {
        match (input.peek_kind(0), input.peek_kind(1), input.peek_kind(2)) {
            (
                Some(SqlTokenKind::Identifier),
                Some(SqlTokenKind::Period),
                Some(SqlTokenKind::Asterisk),
            ) => {
                let name_token = input.next_token().unwrap();
                let name = std::str::from_utf8(name_token.materialize(&input.source)).unwrap();
                input.next_token();
                input.next_token();
                Ok(ResultColumn::TableStar {
                    table: Table {
                        name: name.to_string(),
                        alias: None,
                        table_no: None,
                    },
                })
            }
            _ => {
                let expr = parse_expr(input, 0)?;
                let alias = parse_optional_alias(input)?;
                Ok(ResultColumn::Expr { expr, alias })
            }
        }
    }
}

pub(crate) fn parse_optional_alias(
    input: &mut SqlTokenStream,
) -> Result<Option<String>, SqlParseError> {
    if next_token_is(input, SqlTokenKind::As) {
        if let Some(SqlTokenKind::Identifier) = input.peek_kind(0) {
            let alias_token = input.next_token().unwrap();
            let alias = std::str::from_utf8(alias_token.materialize(&input.source)).unwrap();
            Ok(Some(alias.to_string()))
        } else {
            Err(SqlParseError::new("Expected identifier after AS"))
        }
    } else if let Some(SqlTokenKind::Identifier) = input.peek_kind(0) {
        let alias_token = input.next_token().unwrap();
        let alias = std::str::from_utf8(alias_token.materialize(&input.source)).unwrap();
        Ok(Some(alias.to_string()))
    } else {
        Ok(None)
    }
}
