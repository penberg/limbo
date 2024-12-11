use super::ast::{Expression, FromClause, Join, JoinType, JoinVariant, Table};
use super::expression::parse_expr;
use super::tokenizer::{SqlTokenKind, SqlTokenStream};
use super::utils::{expect_token, next_token_is, parse_optional_alias, SqlParseError};
use super::Direction;

pub(crate) fn parse_from_clause(
    input: &mut SqlTokenStream,
) -> Result<Option<FromClause>, SqlParseError> {
    if !next_token_is(input, SqlTokenKind::From) {
        return Ok(None);
    }

    let table = parse_table(input)?;
    let joins = parse_joins(input)?;

    Ok(Some(FromClause { table, joins }))
}

fn parse_table(input: &mut SqlTokenStream) -> Result<Table, SqlParseError> {
    match input.peek_kind(0) {
        Some(SqlTokenKind::Identifier) => {
            let id_token = input.next_token().unwrap();
            let name = std::str::from_utf8(id_token.materialize(&input.source)).unwrap();
            let alias = parse_optional_alias(input)?;
            Ok(Table {
                name: name.to_string(),
                alias,
                table_no: None,
            })
        }
        Some(_) => {
            let wrong_token = input.peek(0).unwrap();
            Err(SqlParseError::new(&format!(
                "Expected identifier, got: {}",
                wrong_token.print(&input.source)
            )))
        }
        None => Err(SqlParseError::new("Expected identifier, got EOF")),
    }
}

fn parse_joins(input: &mut SqlTokenStream) -> Result<Option<Vec<Join>>, SqlParseError> {
    let mut joins = None;

    while let Some(join) = parse_join(input)? {
        if joins.is_none() {
            joins = Some(vec![]);
        }
        joins.as_mut().unwrap().push(join);
    }

    Ok(joins)
}

fn parse_join(input: &mut SqlTokenStream) -> Result<Option<Join>, SqlParseError> {
    if let Some(join_type) = parse_join_type(input)? {
        let table = parse_table(input)?;
        let on = parse_on_clause(input)?;
        return Ok(Some(Join {
            join_type,
            table,
            on,
        }));
    }

    Ok(None)
}

fn parse_join_type(input: &mut SqlTokenStream) -> Result<Option<JoinType>, SqlParseError> {
    let mut join_type = JoinType::new();

    if let Some(SqlTokenKind::Comma) = input.peek_kind(0) {
        input.next_token().unwrap();
        return Ok(Some(join_type.with(JoinVariant::Inner)));
    }

    let mut has_qualifiers = false;
    while let Some(token_kind) = input.peek_kind(0) {
        match token_kind {
            SqlTokenKind::Inner => {
                input.next_token();
                join_type = join_type.with(JoinVariant::Inner);
                has_qualifiers = true;
            }
            SqlTokenKind::Outer => {
                input.next_token().unwrap();
                join_type = join_type.with(JoinVariant::Outer);
                has_qualifiers = true;
            }
            SqlTokenKind::Left => {
                input.next_token().unwrap();
                join_type = join_type.with(JoinVariant::Left);
                has_qualifiers = true;
            }
            _ => break,
        }
    }

    if has_qualifiers {
        expect_token(input, SqlTokenKind::Join)?;
        return Ok(Some(join_type));
    } else if let Some(SqlTokenKind::Join) = input.peek_kind(0) {
        input.next_token().unwrap();
        return Ok(Some(join_type));
    } else {
        return Ok(None);
    }
}

fn parse_on_clause(input: &mut SqlTokenStream) -> Result<Option<Expression>, SqlParseError> {
    if next_token_is(input, SqlTokenKind::On) {
        parse_expr(input, 0).map(Some)
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_where_clause(
    input: &mut SqlTokenStream,
) -> Result<Option<Expression>, SqlParseError> {
    if next_token_is(input, SqlTokenKind::Where) {
        parse_expr(input, 0).map(Some)
    } else {
        Ok(None)
    }
}

pub(crate) fn parse_group_by_clause(
    input: &mut SqlTokenStream,
) -> Result<Option<Vec<Expression>>, SqlParseError> {
    if next_token_is(input, SqlTokenKind::GroupBy) {
        let mut expressions = vec![parse_expr(input, 0)?];
        while let Some(token) = input.peek_kind(0) {
            if let SqlTokenKind::Comma = token {
                input.next_token().unwrap();
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
) -> Result<Option<Vec<(Expression, Direction)>>, SqlParseError> {
    if next_token_is(input, SqlTokenKind::OrderBy) {
        let mut expressions = vec![parse_order_by_expr(input)?];
        while let Some(token) = input.peek_kind(0) {
            if let SqlTokenKind::Comma = token {
                input.next_token().unwrap();
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

fn parse_order_by_expr(
    input: &mut SqlTokenStream,
) -> Result<(Expression, Direction), SqlParseError> {
    let expr = parse_expr(input, 0)?;
    if next_token_is(input, SqlTokenKind::Asc) {
        Ok((expr, Direction::Ascending))
    } else if next_token_is(input, SqlTokenKind::Desc) {
        Ok((expr, Direction::Descending))
    } else {
        Ok((expr, Direction::Ascending))
    }
}

pub(crate) fn parse_limit_clause(input: &mut SqlTokenStream) -> Result<Option<u64>, SqlParseError> {
    if next_token_is(input, SqlTokenKind::Limit) {
        if let Some(SqlTokenKind::Literal) = input.peek_kind(0) {
            let limit_token = input.next_token().unwrap();
            std::str::from_utf8(limit_token.materialize(&input.source))
                .unwrap()
                .parse::<u64>()
                .map(Some)
                .map_err(|_| SqlParseError::new("Expected integer literal after LIMIT"))
        } else {
            let got = input.peek(0).unwrap().print(&input.source);
            Err(SqlParseError::new(&format!(
                "Expected integer literal after LIMIT, got: {}",
                got
            )))
        }
    } else {
        Ok(None)
    }
}
