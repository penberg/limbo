use winnow::PResult;

use super::ast::{SelectStatement, SqlStatement};
use super::clause::{
    parse_from_clause, parse_group_by_clause, parse_limit_clause, parse_order_by_clause,
    parse_where_clause,
};
use super::tokenizer::SqlToken;
use super::utils::{expect_token, parse_result_columns};
use super::{tokenizer, SqlTokenStream};

pub fn parse_sql_statement(input: &mut str) -> PResult<SqlStatement> {
    let tokens = tokenizer::parse_sql_string_to_tokens(input.as_bytes())?;
    parse_select_statement(&mut tokens.as_slice()).map(SqlStatement::Select)
}

fn parse_select_statement(input: &mut SqlTokenStream) -> PResult<SelectStatement> {
    expect_token(input, SqlToken::Select)?;

    let columns = parse_result_columns(input)?;
    let from = parse_from_clause(input)?;
    let where_clause = parse_where_clause(input)?;
    let group_by = parse_group_by_clause(input)?;
    let order_by = parse_order_by_clause(input)?;
    let limit = parse_limit_clause(input)?;

    Ok(SelectStatement {
        columns,
        from,
        where_clause,
        group_by,
        order_by,
        limit,
    })
}

#[cfg(test)]
mod tests {
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
                    joins: vec![]
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
                            table_no: None,
                            column_no: None
                        }),
                        alias: None
                    },
                    ResultColumn::Expr {
                        expr: Expression::Column(Column {
                            name: "mycolumn2".into(),
                            alias: None,
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
                    joins: vec![]
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
                    joins: vec![Join {
                        join_type: JoinType::new().with(JoinVariant::Inner),
                        table: Table {
                            name: "mytable2".into(),
                            alias: None,
                            table_no: None
                        },
                        on: None
                    }]
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
                    joins: vec![Join {
                        join_type: JoinType::new()
                            .with(JoinVariant::Left)
                            .with(JoinVariant::Outer),
                        table: Table {
                            name: "mytable2".into(),
                            alias: None,
                            table_no: None
                        },
                        on: None
                    }]
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
                    joins: vec![]
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Column(Column {
                        name: "column1".into(),
                        alias: None,
                        table_no: None,
                        column_no: None
                    })),
                    op: Operator::Eq,
                    rhs: Box::new(Expression::Literal("value".into()))
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
                    joins: vec![]
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Column(Column {
                            name: "column1".into(),
                            alias: None,
                            table_no: None,
                            column_no: None
                        })),
                        op: Operator::Eq,
                        rhs: Box::new(Expression::Literal("value".into()))
                    }),
                    op: Operator::And,
                    rhs: Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Column(Column {
                            name: "column2".into(),
                            alias: None,
                            table_no: None,
                            column_no: None
                        })),
                        op: Operator::Gt,
                        rhs: Box::new(Expression::Literal("10".into()))
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
                    joins: vec![]
                }),
                where_clause: None,
                group_by: None,
                order_by: Some(vec![
                    (
                        Expression::Column(Column {
                            name: "column1".into(),
                            alias: None,
                            table_no: None,
                            column_no: None
                        }),
                        Direction::Ascending
                    ),
                    (
                        Expression::Column(Column {
                            name: "column2".into(),
                            alias: None,
                            table_no: None,
                            column_no: None
                        }),
                        Direction::Ascending
                    ),
                    (
                        Expression::Column(Column {
                            name: "column3".into(),
                            alias: None,
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
                    joins: vec![]
                }),
                where_clause: None,
                group_by: Some(vec![
                    Expression::Column(Column {
                        name: "column1".into(),
                        alias: None,
                        table_no: None,
                        column_no: None
                    }),
                    Expression::Column(Column {
                        name: "column2".into(),
                        alias: None,
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
                    joins: vec![]
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Column(Column {
                            name: "column1".into(),
                            alias: None,
                            table_no: None,
                            column_no: None
                        })),
                        op: Operator::Eq,
                        rhs: Box::new(Expression::Literal("value".into()))
                    }),
                    op: Operator::And,
                    rhs: Box::new(Expression::Parenthesized(Box::new(Expression::Binary {
                        lhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "column2".into(),
                                alias: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::Eq,
                            rhs: Box::new(Expression::Literal("value2".into()))
                        }),
                        op: Operator::Or,
                        rhs: Box::new(Expression::Binary {
                            lhs: Box::new(Expression::Column(Column {
                                name: "column3".into(),
                                alias: None,
                                table_no: None,
                                column_no: None
                            })),
                            op: Operator::Eq,
                            rhs: Box::new(Expression::Literal("value3".into()))
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
                    joins: vec![]
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
                            args: Some(vec![Expression::Literal("1".into())]),
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
                    joins: vec![]
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
                    joins: vec![]
                }),
                where_clause: Some(Expression::Binary {
                    lhs: Box::new(Expression::Column(Column {
                        name: "column1".into(),
                        alias: None,
                        table_no: None,
                        column_no: None
                    })),
                    op: Operator::Like,
                    rhs: Box::new(Expression::Literal("value".into())),
                }),
                group_by: None,
                order_by: None,
                limit: None
            }))
        );
    }
}
