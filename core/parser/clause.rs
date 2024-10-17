use winnow::error::{ContextError, ErrMode};
use winnow::stream::Stream;
use winnow::PResult;

use super::ast::{Expression, FromClause, Join, JoinType, JoinVariant, Table};
use super::expression::parse_expr;
use super::utils::{expect_token, parse_optional_alias};
use super::{Direction, SqlToken, SqlTokenStream};

pub(crate) fn parse_from_clause(input: &mut SqlTokenStream) -> PResult<Option<FromClause>> {
    if let Err(_) = expect_token(input, SqlToken::From) {
        return Ok(None);
    }

    let table = parse_table(input)?;
    let joins = parse_joins(input)?;

    Ok(Some(FromClause { table, joins }))
}

fn parse_table(input: &mut SqlTokenStream) -> PResult<Table> {
    match input.get(0) {
        Some(SqlToken::Identifier(name)) => {
            input.next_token();
            let alias = parse_optional_alias(input)?;
            Ok(Table {
                name: std::str::from_utf8(name).unwrap().to_string(),
                alias,
                table_no: None,
            })
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

fn parse_joins(input: &mut SqlTokenStream) -> PResult<Vec<Join>> {
    let mut joins = Vec::new();

    while let Ok(join) = parse_join(input) {
        joins.push(join);
    }

    Ok(joins)
}

fn parse_join(input: &mut SqlTokenStream) -> PResult<Join> {
    let join_type = parse_join_type(input)?;
    expect_token(input, SqlToken::Join)?;
    let table = parse_table(input)?;
    let on = parse_on_clause(input)?;

    Ok(Join {
        join_type,
        table,
        on,
    })
}

fn parse_join_type(input: &mut SqlTokenStream) -> PResult<JoinType> {
    let mut join_type = JoinType::new();

    while let Some(token) = input.get(0) {
        match token {
            SqlToken::Inner => {
                input.next_token();
                join_type = join_type.with(JoinVariant::Inner);
            }
            SqlToken::Outer => {
                input.next_token();
                join_type = join_type.with(JoinVariant::Outer);
            }
            SqlToken::Left => {
                input.next_token();
                join_type = join_type.with(JoinVariant::Left);
            }
            _ => break,
        }
    }

    Ok(join_type)
}

fn parse_on_clause(input: &mut SqlTokenStream) -> PResult<Option<Expression>> {
    if let Ok(()) = expect_token(input, SqlToken::On) {
        parse_expr(input, 0).map(Some)
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_where_clause(input: &mut SqlTokenStream) -> PResult<Option<Expression>> {
    if let Ok(()) = expect_token(input, SqlToken::Where) {
        parse_expr(input, 0).map(Some)
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_group_by_clause(
    input: &mut SqlTokenStream,
) -> PResult<Option<Vec<Expression>>> {
    if let Ok(()) = expect_token(input, SqlToken::GroupBy) {
        let mut expressions = vec![parse_expr(input, 0)?];
        while let Some(token) = input.get(0) {
            if let SqlToken::Comma = token {
                input.next_token();
                expressions.push(parse_expr(input, 0)?);
            } else {
                break;
            }
        }
        Ok(Some(expressions))
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_order_by_clause(
    input: &mut SqlTokenStream,
) -> PResult<Option<Vec<(Expression, Direction)>>> {
    if let Ok(()) = expect_token(input, SqlToken::OrderBy) {
        let mut expressions = vec![parse_order_by_expr(input)?];
        while let Some(token) = input.get(0) {
            if let SqlToken::Comma = token {
                input.next_token();
                expressions.push(parse_order_by_expr(input)?);
            } else {
                break;
            }
        }
        Ok(Some(expressions))
    } else {
        Ok(None)
    }
}

fn parse_order_by_expr(input: &mut SqlTokenStream) -> PResult<(Expression, Direction)> {
    let expr = parse_expr(input, 0)?;
    if let Ok(()) = expect_token(input, SqlToken::Asc) {
        Ok((expr, Direction::Ascending))
    } else if let Ok(()) = expect_token(input, SqlToken::Desc) {
        Ok((expr, Direction::Descending))
    } else {
        Ok((expr, Direction::Ascending))
    }
}

pub(crate) fn parse_limit_clause(input: &mut SqlTokenStream) -> PResult<Option<u64>> {
    if let Ok(()) = expect_token(input, SqlToken::Limit) {
        if let Some(SqlToken::Literal(limit)) = input.get(0) {
            input.next_token();
            std::str::from_utf8(limit)
                .unwrap()
                .parse::<u64>()
                .map(Some)
                .map_err(|_| ErrMode::Backtrack(ContextError::new()))
        } else {
            Err(ErrMode::Backtrack(ContextError::new()))
        }
    } else {
        Ok(None)
    }
}
