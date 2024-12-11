use super::ast::{Column, Expression};
use super::operators::peek_operator;
use super::tokenizer::{SqlTokenKind, SqlTokenStream};
use super::utils::{expect_token, next_token_is, SqlParseError};
use super::Operator;

pub(crate) fn parse_expr(
    input: &mut SqlTokenStream,
    min_precedence: u8,
) -> Result<Expression, SqlParseError> {
    let mut lhs = parse_atom(input)?;

    loop {
        let (op, precedence) = match peek_operator(input) {
            Some((op, prec)) if prec >= min_precedence => (op, prec),
            _ => break,
        };

        // if operator is 'IN' or 'NOT IN', we need to parse the list of expressions
        if op == Operator::In || op == Operator::NotIn {
            input.next_token(); // Consume the operator
            let list = parse_in_list_expr(input)?;
            lhs = Expression::InList {
                expr: Box::new(lhs),
                list,
                not: op == Operator::NotIn,
            };
            continue;
        }

        // if operator is 'BETWEEN', we need to parse the start and end expressions,
        // separated by 'AND'
        if op == Operator::Between {
            input.next_token();
            let start = parse_atom(input)?;
            expect_token(input, SqlTokenKind::And)?;
            let end = parse_atom(input)?;
            lhs = Expression::Between {
                lhs: Box::new(lhs),
                start: Box::new(start),
                end: Box::new(end),
            };
            continue;
        }

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

fn parse_in_list_expr(
    input: &mut SqlTokenStream,
) -> Result<Option<Vec<Expression>>, SqlParseError> {
    expect_token(input, SqlTokenKind::ParenL)?;
    if next_token_is(input, SqlTokenKind::ParenR) {
        return Ok(None);
    }
    let mut list = vec![parse_expr(input, 0)?];
    while next_token_is(input, SqlTokenKind::Comma) {
        list.push(parse_expr(input, 0)?);
    }
    expect_token(input, SqlTokenKind::ParenR)?;
    Ok(Some(list))
}

fn parse_function_call(
    name: String,
    input: &mut SqlTokenStream,
) -> Result<Expression, SqlParseError> {
    // parse until we find a closing parenthesis. we expect to find Expr + comma, so we need to peek after
    // each Expr to see if we should continue parsing
    // at this point we have already consumed the opening parenthesis, so we can just parse the Expr
    if let Some(SqlTokenKind::ParenR) = input.peek_kind(0) {
        input.next_token();
        return Ok(Expression::FunctionCall { name, args: None });
    }
    let mut args = vec![parse_expr(input, 0)?];
    while let Some(SqlTokenKind::Comma) = input.peek_kind(0) {
        input.next_token();
        args.push(parse_expr(input, 0)?);
    }
    expect_token(input, SqlTokenKind::ParenR)?;
    Ok(Expression::FunctionCall {
        name,
        args: Some(args),
    })
}

fn parse_atom(input: &mut SqlTokenStream) -> Result<Expression, SqlParseError> {
    // if first token is an identifier, we need to check if it's a function call or a column
    if let Some(SqlTokenKind::Identifier) = input.peek_kind(0) {
        let id_token = input.next_token().unwrap();
        let name = std::str::from_utf8(id_token.materialize(&input.source)).unwrap();
        if let Some(SqlTokenKind::ParenL) = input.peek_kind(0) {
            // it's a function call
            input.next_token().unwrap();
            return parse_function_call(name.to_string(), input);
        } else {
            // it's a column. handle qualified column case
            let mut table_name = None;
            let column_name = name.to_string();

            // Check if the next token is a period, indicating a qualified column
            if let Some(SqlTokenKind::Period) = input.peek_kind(0) {
                input.next_token().unwrap();
                table_name = Some(column_name);

                // The column name should be the identifier after the dot
                if let Some(SqlTokenKind::Identifier) = input.peek_kind(0) {
                    let id_token = input.next_token().unwrap();
                    let col_name =
                        std::str::from_utf8(id_token.materialize(&input.source)).unwrap();
                    return Ok(Expression::Column(Column {
                        name: col_name.to_string(),
                        alias: None,
                        table_name,
                        table_no: None,
                        column_no: None,
                    }));
                } else if let Some(wrong_token) = input.peek(0) {
                    return Err(SqlParseError::new(format!(
                        "Expected column name after '.', got {}",
                        wrong_token.print(&input.source)
                    )));
                } else {
                    return Err(SqlParseError::new("Expected column name after '.'"));
                }
            }

            return Ok(Expression::Column(Column {
                name: column_name,
                alias: None,
                table_name,
                table_no: None,
                column_no: None,
            }));
        }
    }
    match input.peek_kind(0) {
        Some(SqlTokenKind::Case) => Ok(parse_case_expr(input)?),
        Some(SqlTokenKind::Literal) => {
            let literal_token = input.next_token().unwrap();
            let value = literal_token.materialize(&input.source);
            // try parsing bytes as string, then number, then blob
            let maybe_string = std::str::from_utf8(value);
            if let Ok(string) = maybe_string {
                let maybe_number = string.parse::<f64>();
                match maybe_number {
                    Ok(_) => return Ok(Expression::LiteralNumber(string.to_string())),
                    Err(_) => {
                        let leading_and_trailing_singlequote_byte_removed =
                            &string[1..string.len() - 1];
                        return Ok(Expression::LiteralString(
                            leading_and_trailing_singlequote_byte_removed.to_string(),
                        ));
                    }
                }
            }
            return Ok(Expression::LiteralBlob(value.to_vec()));
        }
        Some(SqlTokenKind::Asterisk) => {
            input.next_token().unwrap();
            Ok(Expression::LiteralString("*".to_string()))
        }
        Some(SqlTokenKind::Minus) => {
            input.next_token().unwrap();
            let expr = parse_expr(input, 0)?;
            Ok(Expression::Unary {
                op: Operator::Minus,
                expr: Box::new(expr),
            })
        }
        Some(SqlTokenKind::Like) => {
            // like is also a function call, so we need to parse it as such
            if let Some(SqlTokenKind::ParenL) = input.peek_kind(1) {
                input.next_token();
                input.next_token();
                return parse_function_call("like".to_string(), input);
            }

            Err(SqlParseError::new(
                "Expected '(' after 'like' when not used as a binary operator",
            ))
        }
        Some(SqlTokenKind::Glob) => {
            input.next_token().unwrap();
            // if next token is a paren, it's a function call
            // otherwise, it's a unary glob
            if let Some(SqlTokenKind::ParenL) = input.peek_kind(0) {
                input.next_token();
                return parse_function_call("glob".to_string(), input);
            }
            let expr = parse_expr(input, 0)?;
            Ok(Expression::Unary {
                op: Operator::Glob,
                expr: Box::new(expr),
            })
        }
        Some(SqlTokenKind::ParenL) => {
            input.next_token().unwrap();
            let expr = parse_expr(input, 0)?;
            expect_token(input, SqlTokenKind::ParenR)?;
            Ok(Expression::Parenthesized(Box::new(expr)))
        }
        Some(_) => {
            let wrong_token = input.peek(0).unwrap();
            Err(
                SqlParseError::new(format!(
                    "Expected one of: literal, identifier, function call, or parenthesized expression, got {}",
                    wrong_token.print(&input.source)
                ))
            )
        }
        None => Err(SqlParseError::new("Unexpected end of input")),
    }
}

fn parse_case_expr(input: &mut SqlTokenStream) -> Result<Expression, SqlParseError> {
    expect_token(input, SqlTokenKind::Case)?;
    // There are two types of case expressions: simple and searched.
    // Simple case expressions have a WHEN and THEN for each condition.
    // Searched case expressions have a single WHEN and THEN for all conditions.
    // We can check if it's a searched case expression by checking if the next token is 'WHEN'
    if let Some(SqlTokenKind::When) = input.peek_kind(0) {
        return parse_case_expr_searched(input);
    }
    parse_case_expr_simple(input)
}

fn parse_case_expr_simple(input: &mut SqlTokenStream) -> Result<Expression, SqlParseError> {
    let input_expression = parse_expr(input, 0)?;
    let mut when_then_pairs = vec![];
    if input.peek_kind(0) != Some(SqlTokenKind::When) {
        return Err(SqlParseError::new(
            "Expected 'WHEN' after base expression in simple case expression",
        ));
    }
    while let Some(SqlTokenKind::When) = input.peek_kind(0) {
        input.next_token();
        let when_expr = parse_expr(input, 0)?;
        expect_token(input, SqlTokenKind::Then)?;
        let then_expr = parse_expr(input, 0)?;
        when_then_pairs.push((when_expr, then_expr));
    }
    let mut else_expr = None;
    if let Some(SqlTokenKind::Else) = input.peek_kind(0) {
        input.next_token();
        else_expr = Some(Box::new(parse_expr(input, 0)?));
    }
    expect_token(input, SqlTokenKind::End)?;
    Ok(Expression::Case {
        base: Some(Box::new(input_expression)),
        when_then_pairs,
        else_expr,
    })
}

fn parse_case_expr_searched(input: &mut SqlTokenStream) -> Result<Expression, SqlParseError> {
    if input.peek_kind(0) != Some(SqlTokenKind::When) {
        return Err(SqlParseError::new(
            "Expected 'WHEN' after base expression in searched case expression",
        ));
    }
    let mut when_then_pairs = vec![];
    while let Some(SqlTokenKind::When) = input.peek_kind(0) {
        input.next_token();
        let when_expr = parse_expr(input, 0)?;
        expect_token(input, SqlTokenKind::Then)?;
        let then_expr = parse_expr(input, 0)?;
        when_then_pairs.push((when_expr, then_expr));
    }
    let mut else_expr = None;
    if let Some(SqlTokenKind::Else) = input.peek_kind(0) {
        input.next_token();
        else_expr = Some(Box::new(parse_expr(input, 0)?));
    }
    expect_token(input, SqlTokenKind::End)?;
    Ok(Expression::Case {
        base: None,
        when_then_pairs,
        else_expr,
    })
}
