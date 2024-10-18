use super::ast::ResultColumn;
use super::expression::parse_expr;
use super::tokenizer::{SqlToken, SqlTokenStream};
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

pub(crate) fn expect_token(
    input: &mut SqlTokenStream,
    expected: SqlToken,
) -> Result<(), SqlParseError> {
    if input.peek(0) == Some(expected) {
        input.next_token();
        Ok(())
    } else if let Some(wrong_token) = input.peek(0) {
        Err(SqlParseError::new(format!(
            "Expected token {}, got {}",
            expected, wrong_token
        )))
    } else {
        Err(SqlParseError::new("Expected token, got EOF"))
    }
}

pub(crate) fn parse_result_columns(
    input: &mut SqlTokenStream,
) -> Result<Vec<ResultColumn>, SqlParseError> {
    let mut columns = vec![parse_result_column(input)?];

    while let Ok(()) = expect_token(input, SqlToken::Comma) {
        columns.push(parse_result_column(input)?);
    }

    Ok(columns)
}

fn parse_result_column(input: &mut SqlTokenStream) -> Result<ResultColumn, SqlParseError> {
    if let Ok(()) = expect_token(input, SqlToken::Asterisk) {
        Ok(ResultColumn::Star)
    } else {
        match (input.peek(0), input.peek(1), input.peek(2)) {
            (
                Some(SqlToken::Identifier(name)),
                Some(SqlToken::Period),
                Some(SqlToken::Asterisk),
            ) => {
                input.next_token();
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
    if let Ok(()) = expect_token(input, SqlToken::As) {
        if let Some(SqlToken::Identifier(alias)) = input.peek(0) {
            input.next_token();
            Ok(Some(alias.to_string()))
        } else {
            Err(SqlParseError::new("Expected identifier after AS"))
        }
    } else if let Some(SqlToken::Identifier(alias)) = input.peek(0) {
        input.next_token();
        Ok(Some(alias.to_string()))
    } else {
        Ok(None)
    }
}
