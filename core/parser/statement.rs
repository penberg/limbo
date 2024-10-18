use super::ast::{SelectStatement, SqlStatement};
use super::clause::{
    parse_from_clause, parse_group_by_clause, parse_limit_clause, parse_order_by_clause,
    parse_where_clause,
};
use super::tokenizer::{self, SqlToken, SqlTokenStream};
use super::utils::{expect_token, parse_result_columns, SqlParseError};

pub fn parse_sql_statement(input: &mut str) -> Result<SqlStatement, SqlParseError> {
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
                    joins: vec![]
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
                    joins: vec![]
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
                    joins: vec![]
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
                    joins: vec![]
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
