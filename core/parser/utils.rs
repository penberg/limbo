use winnow::error::{ContextError, ErrMode};
use winnow::stream::Stream;
use winnow::PResult;

use super::ast::ResultColumn;
use super::expression::parse_expr;
use super::{SqlToken, SqlTokenStream};

pub(crate) fn expect_token(input: &mut SqlTokenStream, expected: SqlToken) -> PResult<()> {
    if input.get(0) == Some(&expected) {
        input.next_token();
        Ok(())
    } else {
        Err(ErrMode::Backtrack(ContextError::new()))
    }
}

pub(crate) fn parse_result_columns(input: &mut SqlTokenStream) -> PResult<Vec<ResultColumn>> {
    let mut columns = vec![parse_result_column(input)?];

    while let Ok(()) = expect_token(input, SqlToken::Comma) {
        columns.push(parse_result_column(input)?);
    }

    Ok(columns)
}

fn parse_result_column(input: &mut SqlTokenStream) -> PResult<ResultColumn> {
    if let Ok(()) = expect_token(input, SqlToken::Asterisk) {
        Ok(ResultColumn::Star)
    } else {
        let expr = parse_expr(input, 0)?;
        let alias = parse_optional_alias(input)?;
        Ok(ResultColumn::Expr { expr, alias })
    }
}

pub(crate) fn parse_optional_alias(input: &mut SqlTokenStream) -> PResult<Option<String>> {
    if let Ok(()) = expect_token(input, SqlToken::As) {
        if let Some(SqlToken::Identifier(alias)) = input.get(0) {
            input.next_token();
            Ok(Some(std::str::from_utf8(alias).unwrap().to_string()))
        } else {
            Err(ErrMode::Backtrack(ContextError::new()))
        }
    } else if let Some(SqlToken::Identifier(alias)) = input.get(0) {
        input.next_token();
        Ok(Some(std::str::from_utf8(alias).unwrap().to_string()))
    } else {
        Ok(None)
    }
}
