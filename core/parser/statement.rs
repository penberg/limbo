use super::ast::{SelectStatement, SqlStatement};
use super::clause::{
    parse_from_clause, parse_group_by_clause, parse_limit_clause, parse_order_by_clause,
    parse_where_clause,
};
use super::tokenizer::{self, SqlTokenKind, SqlTokenStream};
use super::utils::{expect_token, parse_result_columns, SqlParseError};

pub fn parse_sql_statement(input: &str) -> Result<SqlStatement, SqlParseError> {
    let token_stream = tokenizer::parse_sql_string_to_tokens::<()>(input.as_bytes());
    match token_stream {
        Ok(mut token_stream) => parse_select_statement(&mut token_stream).map(SqlStatement::Select),
        Err(e) => Err(SqlParseError::new(format!(
            "FIXME fix this garbage error - Tokenizer error: {}",
            e
        ))),
    }
}

fn parse_select_statement(input: &mut SqlTokenStream) -> Result<SelectStatement, SqlParseError> {
    expect_token(input, SqlTokenKind::Select)?;

    let columns = parse_result_columns(input)?;
    let from = parse_from_clause(input)?;
    let where_clause = parse_where_clause(input)?;
    let group_by = parse_group_by_clause(input)?;
    let order_by = parse_order_by_clause(input)?;
    let limit = parse_limit_clause(input)?;

    assert_eof(input)?;

    Ok(SelectStatement {
        columns,
        from,
        where_clause,
        group_by,
        order_by,
        limit,
    })
}

fn assert_eof(input: &mut SqlTokenStream) -> Result<(), SqlParseError> {
    if let Some(token) = input.peek(0) {
        return Err(SqlParseError::new(format!(
            "Unexpected token at end of input: {}",
            token.print(&input.source)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use crate::parser::{
        Column, Direction, Expression, FromClause, Join, JoinType, JoinVariant, Operator,
        ResultColumn, Table,
    };

    use super::*;

    #[test]
    fn select_star_from_mytable_limit_10() {
        let mut input = "SELECT * FROM mytable LIMIT 10".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                group_by: None,
                order_by: None,
                where_clause: None,
                limit: Some(10)
            }))
        );
    }

    #[test]
    fn select_mycolumn_mycolumn2_from_mytable() {
        let mut input = "SELECT mycolumn, mycolumn2 FROM mytable".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![
                    ResultColumn::Expr {
                        expr: Expression::Column(Column {
                            name: "mycolumn".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        }),
                        alias: None
                    },
                    ResultColumn::Expr {
                        expr: Expression::Column(Column {
                            name: "mycolumn2".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        }),
                        alias: None
                    }
                ],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                group_by: None,
                order_by: None,
                where_clause: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_join_mytable2() {
        let mut input = "SELECT * FROM mytable JOIN mytable2".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: Some(vec![Join {
                        join_type: JoinType::new().with(JoinVariant::Inner),
                        table: Table {
                            name: "mytable2".into(),
                            alias: None,
                            table_no: None
                        },
                        on: None
                    }]),
                }),
                group_by: None,
                order_by: None,
                where_clause: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_left_outer_join_mytable2() {
        let mut input = "SELECT * FROM mytable LEFT OUTER JOIN mytable2".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: Some(vec![Join {
                        join_type: JoinType::new()
                            .with(JoinVariant::Left)
                            .with(JoinVariant::Outer),
                        table: Table {
                            name: "mytable2".into(),
                            alias: None,
                            table_no: None
                        },
                        on: None
                    }]),
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_where_column1_eq_value() {
        let mut input = "SELECT * FROM mytable WHERE column1 = 'value'".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Column(Column {
                        name: "column1".into(),
                        table_name: None,
                        alias: None,
                        table_no: None,
                        column_no: None
                    })),
                    op: Operator::Eq,
                    rhs: Box::new(Expression::LiteralString("value".into()))
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_where_column1_eq_value_and_column2_gt_10() {
        let mut input =
            "SELECT * FROM mytable WHERE column1 = 'value' AND column2 > 10".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Column(Column {
                            name: "column1".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        })),
                        op: Operator::Eq,
                        rhs: Box::new(Expression::LiteralString("value".into()))
                    }),
                    op: Operator::And,
                    rhs: Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Column(Column {
                            name: "column2".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        })),
                        op: Operator::Gt,
                        rhs: Box::new(Expression::LiteralNumber("10".into()))
                    })
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_order_by_column1_nodir_column2_asc_column3_desc() {
        let mut input =
            "SELECT * FROM mytable ORDER BY column1, column2 ASC, column3 DESC".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: Some(vec![
                    (
                        Expression::Column(Column {
                            name: "column1".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        }),
                        Direction::Ascending
                    ),
                    (
                        Expression::Column(Column {
                            name: "column2".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        }),
                        Direction::Ascending
                    ),
                    (
                        Expression::Column(Column {
                            name: "column3".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        }),
                        Direction::Descending
                    )
                ]),
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_group_by_column1_column2() {
        let mut input = "SELECT * FROM mytable GROUP BY column1, column2".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: Some(vec![
                    Expression::Column(Column {
                        name: "column1".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    }),
                    Expression::Column(Column {
                        name: "column2".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    }),
                ]),
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_where_cond_and_parenthesized_cond() {
        let mut input = "SELECT * FROM mytable WHERE column1 = 'value' AND (column2 = 'value2' OR column3 = 'value3')".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Column(Column {
                            name: "column1".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        })),
                        op: Operator::Eq,
                        rhs: Box::new(Expression::LiteralString("value".into()))
                    }),
                    op: Operator::And,
                    rhs: Box::new(Expression::Parenthesized(Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "column2".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::Eq,
                            rhs: Box::new(Expression::LiteralString("value2".into()))
                        }),
                        op: Operator::Or,
                        rhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "column3".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::Eq,
                            rhs: Box::new(Expression::LiteralString("value3".into()))
                        })
                    })))
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_count_foo_from_mytable() {
        let mut input = "SELECT COUNT(foo) FROM mytable".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Expr {
                    expr: Expression::FunctionCall {
                        name: "COUNT".into(),
                        args: Some(vec![Expression::Column(Column {
                            name: "foo".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        })])
                    },
                    alias: None,
                }],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_length_count_1_from_mytable() {
        let mut input = "SELECT LENGTH(COUNT(1)) FROM mytable".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Expr {
                    expr: Expression::FunctionCall {
                        name: "LENGTH".into(),
                        args: Some(vec![Expression::FunctionCall {
                            name: "COUNT".into(),
                            args: Some(vec![Expression::LiteralNumber("1".into())]),
                        }]),
                    },
                    alias: None,
                }],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_star_from_mytable_where_column1_like_value() {
        let mut input = "SELECT * FROM mytable WHERE column1 LIKE 'value'".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Column(Column {
                        name: "column1".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    })),
                    op: Operator::Like,
                    rhs: Box::new(Expression::LiteralString("value".into())),
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_qualified_col_from_mytable_as_mtbl() {
        let mut input = "SELECT mtbl.column1 FROM mytable AS mtbl".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Expr {
                    expr: Expression::Column(Column {
                        name: "column1".into(),
                        table_name: Some("mtbl".into()),
                        alias: None,
                        table_no: None,
                        column_no: None
                    }),
                    alias: None,
                }],
                from: Some(FromClause {
                    table: Table {
                        name: "mytable".into(),
                        alias: Some("mtbl".into()),
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_with_function_calls() {
        let mut input = "SELECT COUNT(id), LENGTH(first_name) FROM users LIMIT 5".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![
                    ResultColumn::Expr {
                        expr: Expression::FunctionCall {
                            name: "COUNT".into(),
                            args: Some(vec![Expression::Column(Column {
                                name: "id".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })])
                        },
                        alias: None,
                    },
                    ResultColumn::Expr {
                        expr: Expression::FunctionCall {
                            name: "LENGTH".into(),
                            args: Some(vec![Expression::Column(Column {
                                name: "first_name".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })])
                        },
                        alias: None,
                    }
                ],
                from: Some(FromClause {
                    table: Table {
                        name: "users".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: Some(5)
            }))
        );
    }

    #[test]
    fn select_with_case_insensitive_keywords() {
        let mut input = "SeLeCt * FrOm UsErS wHeRe AgE > 50 LiMiT 10".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "UsErS".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Column(Column {
                        name: "AgE".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    })),
                    op: Operator::Gt,
                    rhs: Box::new(Expression::LiteralNumber("50".into()))
                }),
                group_by: None,
                order_by: None,
                limit: Some(10)
            }))
        );
    }

    #[test]
    fn select_with_in_operator() {
        let mut input =
            "SELECT * FROM products WHERE name IN ('hat', 'shirt', 'shoes')".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "products".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::InList {
                    expr: Box::new(Expression::Column(Column {
                        name: "name".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    })),
                    list: Some(vec![
                        Expression::LiteralString("hat".into()),
                        Expression::LiteralString("shirt".into()),
                        Expression::LiteralString("shoes".into())
                    ]),
                    not: false
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_with_not_in_operator() {
        let mut input =
            "SELECT * FROM products WHERE name NOT IN ('hat', 'shirt', 'shoes')".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "products".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::InList {
                    expr: Box::new(Expression::Column(Column {
                        name: "name".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    })),
                    list: Some(vec![
                        Expression::LiteralString("hat".into()),
                        Expression::LiteralString("shirt".into()),
                        Expression::LiteralString("shoes".into())
                    ]),
                    not: true
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_with_like_operator() {
        let mut input = "SELECT * FROM products WHERE name LIKE 'sweat%'".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "products".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Column(Column {
                        name: "name".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    })),
                    op: Operator::Like,
                    rhs: Box::new(Expression::LiteralString("sweat%".into()))
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_with_complex_where_clause() {
        let mut input = "SELECT * FROM products WHERE (price > 50 AND name != 'hat') OR (name IN ('shirt', 'shoes') AND price < 30)".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "products".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Parenthesized(Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "price".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::Gt,
                            rhs: Box::new(Expression::LiteralNumber("50".into()))
                        }),
                        op: Operator::And,
                        rhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "name".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::NotEq,
                            rhs: Box::new(Expression::LiteralString("hat".into()))
                        })
                    }))),
                    op: Operator::Or,
                    rhs: Box::new(Expression::Parenthesized(Box::new(Expression::Binary {
                        lhs: Box::new(Expression::InList {
                            expr: Box::new(Expression::Column(Column {
                                name: "name".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })),
                            list: Some(vec![
                                Expression::LiteralString("shirt".into()),
                                Expression::LiteralString("shoes".into())
                            ]),
                            not: false
                        }),
                        op: Operator::And,
                        rhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "price".into(),
                                alias: None,
                                table_name: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::Lt,
                            rhs: Box::new(Expression::LiteralNumber("30".into()))
                        })
                    })))
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_with_multiple_joins() {
        let mut input = "SELECT u.first_name, p.name, o.order_date FROM users u JOIN products p ON u.id = p.user_id JOIN orders o ON u.id = o.user_id LIMIT 5".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![
                    ResultColumn::Expr {
                        expr: Expression::Column(Column {
                            name: "first_name".into(),
                            alias: None,
                            table_name: Some("u".into()),
                            table_no: None,
                            column_no: None
                        }),
                        alias: None,
                    },
                    ResultColumn::Expr {
                        expr: Expression::Column(Column {
                            name: "name".into(),
                            alias: None,
                            table_name: Some("p".into()),
                            table_no: None,
                            column_no: None
                        }),
                        alias: None,
                    },
                    ResultColumn::Expr {
                        expr: Expression::Column(Column {
                            name: "order_date".into(),
                            alias: None,
                            table_name: Some("o".into()),
                            table_no: None,
                            column_no: None
                        }),
                        alias: None,
                    }
                ],
                from: Some(FromClause {
                    table: Table {
                        name: "users".into(),
                        alias: Some("u".into()),
                        table_no: None
                    },
                    joins: Some(vec![
                        Join {
                            join_type: JoinType::new().with(JoinVariant::Inner),
                            table: Table {
                                name: "products".into(),
                                alias: Some("p".into()),
                                table_no: None
                            },
                            on: Some(Expression::Binary {
                                lhs: Box::new(Expression::Column(Column {
                                    name: "id".into(),
                                    alias: None,
                                    table_name: Some("u".into()),
                                    table_no: None,
                                    column_no: None
                                })),
                                op: Operator::Eq,
                                rhs: Box::new(Expression::Column(Column {
                                    name: "user_id".into(),
                                    alias: None,
                                    table_name: Some("p".into()),
                                    table_no: None,
                                    column_no: None
                                }))
                            })
                        },
                        Join {
                            join_type: JoinType::new().with(JoinVariant::Inner),
                            table: Table {
                                name: "orders".into(),
                                alias: Some("o".into()),
                                table_no: None
                            },
                            on: Some(Expression::Binary {
                                lhs: Box::new(Expression::Column(Column {
                                    name: "id".into(),
                                    alias: None,
                                    table_name: Some("u".into()),
                                    table_no: None,
                                    column_no: None
                                })),
                                op: Operator::Eq,
                                rhs: Box::new(Expression::Column(Column {
                                    name: "user_id".into(),
                                    alias: None,
                                    table_name: Some("o".into()),
                                    table_no: None,
                                    column_no: None
                                }))
                            })
                        }
                    ]),
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: Some(5)
            }))
        );
    }

    #[test]
    fn select_with_between_operator() {
        let mut input = "SELECT * FROM products WHERE price BETWEEN 10 AND 20".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Star],
                from: Some(FromClause {
                    table: Table {
                        name: "products".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: Some(Expression::Between {
                    lhs: Box::new(Expression::Column(Column {
                        name: "price".into(),
                        alias: None,
                        table_name: None,
                        table_no: None,
                        column_no: None
                    })),
                    start: Box::new(Expression::LiteralNumber("10".into())),
                    end: Box::new(Expression::LiteralNumber("20".into())),
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }

    #[test]
    fn select_with_case_expr_searched() {
        let mut input =
            "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END as age_group FROM users"
                .to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Expr {
                    expr: Expression::Case {
                        base: None,
                        when_then_pairs: vec![(
                            Expression::Binary {
                                lhs: Box::new(Expression::Column(Column {
                                    name: "age".into(),
                                    alias: None,
                                    table_name: None,
                                    table_no: None,
                                    column_no: None
                                })),
                                op: Operator::Lt,
                                rhs: Box::new(Expression::LiteralNumber("18".into())),
                            },
                            Expression::LiteralString("minor".into())
                        )],
                        else_expr: Some(Box::new(Expression::LiteralString("adult".into()))),
                    },
                    alias: Some("age_group".into()),
                }],
                from: Some(FromClause {
                    table: Table {
                        name: "users".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: None,
            }))
        );
    }

    #[test]
    fn select_with_case_expr_base() {
        let mut input = "SELECT CASE age WHEN 18 THEN 'eighteen' WHEN 19 THEN 'nineteen' ELSE 'other' END as age_word FROM users".to_string();
        let result = parse_sql_statement(&mut input);
        assert_eq!(
            result,
            Ok(SqlStatement::Select(SelectStatement {
                columns: vec![ResultColumn::Expr {
                    expr: Expression::Case {
                        base: Some(Box::new(Expression::Column(Column {
                            name: "age".into(),
                            alias: None,
                            table_name: None,
                            table_no: None,
                            column_no: None
                        }))),
                        when_then_pairs: vec![
                            (
                                Expression::LiteralNumber("18".into()),
                                Expression::LiteralString("eighteen".into())
                            ),
                            (
                                Expression::LiteralNumber("19".into()),
                                Expression::LiteralString("nineteen".into())
                            ),
                        ],
                        else_expr: Some(Box::new(Expression::LiteralString("other".into()))),
                    },
                    alias: Some("age_word".into()),
                }],
                from: Some(FromClause {
                    table: Table {
                        name: "users".into(),
                        alias: None,
                        table_no: None
                    },
                    joins: None,
                }),
                where_clause: None,
                group_by: None,
                order_by: None,
                limit: None,
            }))
        );
    }

    #[test]
    fn test_error_message() {
        let test_cases = vec![
            ("SELECT * FROM users WHERE column1 = 'value' ORDER BY", "Unexpected end of input"),
            ("SELECT foo, bar FROM WHERE column1 = 'value'", "Expected identifier, got: 'WHERE' at position 21 near ' bar FROM WHERE column1 ='"),
            ("SELECT * FROM users ORDER BY name ASCII", "Unexpected token at end of input: identifier: ASCII at position 34 near 'R BY name ASCII'"),
        ];
        for (input, expected_message) in test_cases {
            let result = parse_sql_statement(&input);
            assert_eq!(
                result,
                Err(SqlParseError {
                    message: expected_message.into(),
                })
            );
        }
    }

    #[test]
    fn shitty_fuzzer() {
        let current_dir = std::env::current_dir().unwrap();
        // scraped all the SELECT tests from testing/*.test
        let mut file = File::open(
            current_dir
                .join("parser")
                .join("parser-shitty-fuzzer-input.txt"),
        )
        .expect("Failed to open parser-shitty-fuzzer-input.txt");
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        let queries = contents.split('\n').collect::<Vec<&str>>();

        let ignore_queries_starting_with_fail = true;
        for query in queries {
            let mut query = query.to_string();
            if ignore_queries_starting_with_fail && query.starts_with("FAIL:") {
                // skip these queries until i have the energy to fix them
                println!("parser fuzzer skipping query: {}", query);
                continue;
            }
            let result = parse_sql_statement(&mut query);
            assert!(
                result.is_ok(),
                "Failed to parse query {}, error: {}",
                query,
                result.unwrap_err()
            );
        }
    }
}
