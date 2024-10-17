use winnow::error::{ContextError, ErrMode};
use winnow::stream::Stream;
use winnow::PResult;

use super::ast::{Column, Expression};
use super::operators::peek_operator;
use super::utils::expect_token;
use super::{SqlToken, SqlTokenStream};

pub(crate) fn parse_expr(input: &mut SqlTokenStream, min_precedence: u8) -> PResult<Expression> {
    let mut lhs = parse_atom(input)?;

    loop {
        let (op, precedence) = match peek_operator(input) {
            Some((op, prec)) if prec >= min_precedence => (op, prec),
            _ => break,
        };

        input.next_token(); // Consume the operator

        let rhs = parse_expr(input, precedence + 1)?;

        lhs = Expression::Binary {
            lhs: Box::new(lhs),
            op,
            rhs: Box::new(rhs),
        };
    }

    Ok(lhs)
}

fn parse_function_call(name: String, input: &mut SqlTokenStream) -> PResult<Expression> {
    // parse until we find a closing parenthesis. we expect to find Expr + comma, so we need to peek after
    // each Expr to see if we should continue parsing
    // at this point we have already consumed the opening parenthesis, so we can just parse the Expr
    if let Some(SqlToken::ParenR) = input.get(0) {
        input.next_token();
        return Ok(Expression::FunctionCall { name, args: None });
    }
    let mut args = vec![parse_expr(input, 0)?];
    while let Some(SqlToken::Comma) = input.get(0) {
        input.next_token();
        args.push(parse_expr(input, 0)?);
    }
    expect_token(input, SqlToken::ParenR)?;
    Ok(Expression::FunctionCall {
        name,
        args: Some(args),
    })
}

fn parse_atom(input: &mut SqlTokenStream) -> PResult<Expression> {
    // if first token is an identifier, we need to check if it's a function call or a column
    if let Some(SqlToken::Identifier(name)) = input.get(0) {
        if let Some(SqlToken::ParenL) = input.get(1) {
            // it's a function call
            input.next_token().unwrap();
            input.next_token().unwrap();
            let name = std::str::from_utf8(name).unwrap().to_string();
            return parse_function_call(name, input);
        } else {
            input.next_token();
            return Ok(Expression::Column(Column {
                name: std::str::from_utf8(name).unwrap().to_string(),
                alias: None,
                table_no: None,
                column_no: None,
            }));
        }
    }
    match input.get(0) {
        Some(SqlToken::Identifier(name)) => {
            input.next_token();
            Ok(Expression::Column(Column {
                name: std::str::from_utf8(name).unwrap().to_string(),
                alias: None,
                table_no: None,
                column_no: None,
            }))
        }
        Some(SqlToken::Literal(value)) => {
            input.next_token();
            Ok(Expression::Literal(
                std::str::from_utf8(value).unwrap().to_string(),
            ))
        }
        Some(SqlToken::ParenL) => {
            input.next_token();
            let expr = parse_expr(input, 0)?;
            expect_token(input, SqlToken::ParenR)?;
            Ok(Expression::Parenthesized(Box::new(expr)))
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}
