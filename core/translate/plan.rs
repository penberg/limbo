use core::fmt;
use std::{
    fmt::{Display, Formatter},
    rc::Rc,
};

use sqlite3_parser::ast;

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Index},
    util::normalize_ident,
    Result,
};

#[derive(Debug)]
pub struct Plan {
    pub root_operator: Operator,
    pub referenced_tables: Vec<BTreeTableReference>,
    pub available_indexes: Vec<Rc<Index>>,
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.root_operator)
    }
}

/**
  An Operator is a Node in the query plan.
  Operators form a tree structure, with each having zero or more children.
  For example, a query like `SELECT t1.foo FROM t1 ORDER BY t1.foo LIMIT 1` would have the following structure:
    Limit
      Order
        Project
          Scan

  Operators also have a unique ID, which is used to identify them in the query plan and attach metadata.
  They also have a step counter, which is used to track the current step in the operator's execution.
  TODO: perhaps 'step' shouldn't be in this struct, since it's an execution time concept, not a plan time concept.
*/
#[derive(Clone, Debug)]
pub enum Operator {
    // Aggregate operator
    // This operator is used to compute aggregate functions like SUM, AVG, COUNT, etc.
    // It takes a source operator and a list of aggregate functions to compute.
    // GROUP BY is not supported yet.
    Aggregate {
        id: usize,
        source: Box<Operator>,
        aggregates: Vec<Aggregate>,
        group_by: Option<Vec<ast::Expr>>,
        step: usize,
    },
    // Filter operator
    // This operator is used to filter rows from the source operator.
    // It takes a source operator and a list of predicates to evaluate.
    // Only rows for which all predicates evaluate to true are passed to the next operator.
    // Generally filter operators will only exist in unoptimized plans,
    // as the optimizer will try to push filters down to the lowest possible level,
    // e.g. a table scan.
    Filter {
        id: usize,
        source: Box<Operator>,
        predicates: Vec<ast::Expr>,
    },
    // Limit operator
    // This operator is used to limit the number of rows returned by the source operator.
    Limit {
        id: usize,
        source: Box<Operator>,
        limit: usize,
        step: usize,
    },
    // Join operator
    // This operator is used to join two source operators.
    // It takes a left and right source operator, a list of predicates to evaluate,
    // and a boolean indicating whether it is an outer join.
    Join {
        id: usize,
        left: Box<Operator>,
        right: Box<Operator>,
        predicates: Option<Vec<ast::Expr>>,
        outer: bool,
        step: usize,
    },
    // Order operator
    // This operator is used to sort the rows returned by the source operator.
    Order {
        id: usize,
        source: Box<Operator>,
        key: Vec<(ast::Expr, Direction)>,
        step: usize,
    },
    // Projection operator
    // This operator is used to project columns from the source operator.
    // It takes a source operator and a list of expressions to evaluate.
    // e.g. SELECT foo, bar FROM t1
    // In this example, the expressions would be [foo, bar]
    // and the source operator would be a Scan operator for table t1.
    Projection {
        id: usize,
        source: Box<Operator>,
        expressions: Vec<ProjectionColumn>,
        step: usize,
    },
    // Scan operator
    // This operator is used to scan a table.
    // It takes a table to scan and an optional list of predicates to evaluate.
    // The predicates are used to filter rows from the table.
    // e.g. SELECT * FROM t1 WHERE t1.foo = 5
    Scan {
        id: usize,
        table_reference: BTreeTableReference,
        predicates: Option<Vec<ast::Expr>>,
        step: usize,
    },
    // Search operator
    // This operator is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search {
        id: usize,
        table_reference: BTreeTableReference,
        search: Search,
        predicates: Option<Vec<ast::Expr>>,
        step: usize,
    },
    // Nothing operator
    // This operator is used to represent an empty query.
    // e.g. SELECT * from foo WHERE 0 will eventually be optimized to Nothing.
    Nothing,
}

#[derive(Clone, Debug)]
pub struct BTreeTableReference {
    pub table: Rc<BTreeTable>,
    pub table_identifier: String,
}

/// An enum that represents a search operation that can be used to search for a row in a table using an index
/// (i.e. a primary key or a secondary index)
#[derive(Clone, Debug)]
pub enum Search {
    /// A primary key equality search. This is a special case of the primary key search
    /// that uses the SeekRowid bytecode instruction.
    PrimaryKeyEq { cmp_expr: ast::Expr },
    /// A primary key search. Uses bytecode instructions like SeekGT, SeekGE etc.
    PrimaryKeySearch {
        cmp_op: ast::Operator,
        cmp_expr: ast::Expr,
    },
    /// A secondary index search. Uses bytecode instructions like SeekGE, SeekGT etc.
    IndexSearch {
        index: Rc<Index>,
        cmp_op: ast::Operator,
        cmp_expr: ast::Expr,
    },
}

#[derive(Clone, Debug)]
pub enum ProjectionColumn {
    Column(ast::Expr),
    Star,
    TableStar(BTreeTableReference),
}

impl ProjectionColumn {
    pub fn column_count(&self, referenced_tables: &[BTreeTableReference]) -> usize {
        match self {
            ProjectionColumn::Column(_) => 1,
            ProjectionColumn::Star => {
                let mut count = 0;
                for table_reference in referenced_tables {
                    count += table_reference.table.columns.len();
                }
                count
            }
            ProjectionColumn::TableStar(table_reference) => table_reference.table.columns.len(),
        }
    }
}

impl Operator {
    pub fn column_count(&self, referenced_tables: &[BTreeTableReference]) -> usize {
        match self {
            Operator::Aggregate {
                group_by,
                aggregates,
                ..
            } => aggregates.len() + group_by.as_ref().map_or(0, |g| g.len()),
            Operator::Filter { source, .. } => source.column_count(referenced_tables),
            Operator::Limit { source, .. } => source.column_count(referenced_tables),
            Operator::Join { left, right, .. } => {
                left.column_count(referenced_tables) + right.column_count(referenced_tables)
            }
            Operator::Order { source, .. } => source.column_count(referenced_tables),
            Operator::Projection { expressions, .. } => expressions
                .iter()
                .map(|e| e.column_count(referenced_tables))
                .sum(),
            Operator::Scan {
                table_reference, ..
            } => table_reference.table.columns.len(),
            Operator::Search {
                table_reference, ..
            } => table_reference.table.columns.len(),
            Operator::Nothing => 0,
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        match self {
            Operator::Aggregate {
                aggregates,
                group_by,
                ..
            } => {
                let mut names = vec![];
                for agg in aggregates.iter() {
                    names.push(agg.func.to_string().to_string());
                }

                if let Some(group_by) = group_by {
                    for expr in group_by.iter() {
                        match expr {
                            ast::Expr::Id(ident) => names.push(ident.0.clone()),
                            ast::Expr::Qualified(tbl, ident) => {
                                names.push(format!("{}.{}", tbl.0, ident.0))
                            }
                            e => names.push(e.to_string()),
                        }
                    }
                }

                names
            }
            Operator::Filter { source, .. } => source.column_names(),
            Operator::Limit { source, .. } => source.column_names(),
            Operator::Join { left, right, .. } => {
                let mut names = left.column_names();
                names.extend(right.column_names());
                names
            }
            Operator::Order { source, .. } => source.column_names(),
            Operator::Projection { expressions, .. } => expressions
                .iter()
                .map(|e| match e {
                    ProjectionColumn::Column(expr) => match expr {
                        ast::Expr::Id(ident) => ident.0.clone(),
                        ast::Expr::Qualified(tbl, ident) => format!("{}.{}", tbl.0, ident.0),
                        _ => "expr".to_string(),
                    },
                    ProjectionColumn::Star => "*".to_string(),
                    ProjectionColumn::TableStar(table_reference) => {
                        format!("{}.{}", table_reference.table_identifier, "*")
                    }
                })
                .collect(),
            Operator::Scan {
                table_reference, ..
            } => table_reference
                .table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
            Operator::Search {
                table_reference, ..
            } => table_reference
                .table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect(),
            Operator::Nothing => vec![],
        }
    }

    pub fn id(&self) -> usize {
        match self {
            Operator::Aggregate { id, .. } => *id,
            Operator::Filter { id, .. } => *id,
            Operator::Limit { id, .. } => *id,
            Operator::Join { id, .. } => *id,
            Operator::Order { id, .. } => *id,
            Operator::Projection { id, .. } => *id,
            Operator::Scan { id, .. } => *id,
            Operator::Search { id, .. } => *id,
            Operator::Nothing => unreachable!(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Display for Direction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Direction::Ascending => write!(f, "ASC"),
            Direction::Descending => write!(f, "DESC"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Aggregate {
    pub func: AggFunc,
    pub args: Vec<ast::Expr>,
    pub original_expr: ast::Expr,
}

impl Display for Aggregate {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(|arg| arg.to_string())
            .collect::<Vec<String>>()
            .join(", ");
        write!(f, "{:?}({})", self.func, args_str)
    }
}

// For EXPLAIN QUERY PLAN
impl Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        fn fmt_operator(
            operator: &Operator,
            f: &mut Formatter,
            level: usize,
            last: bool,
        ) -> fmt::Result {
            let indent = if level == 0 {
                if last { "`--" } else { "|--" }.to_string()
            } else {
                format!(
                    "   {}{}",
                    "|  ".repeat(level - 1),
                    if last { "`--" } else { "|--" }
                )
            };

            match operator {
                Operator::Aggregate {
                    source, aggregates, ..
                } => {
                    // e.g. Aggregate count(*), sum(x)
                    let aggregates_display_string = aggregates
                        .iter()
                        .map(|agg| agg.to_string())
                        .collect::<Vec<String>>()
                        .join(", ");
                    writeln!(f, "{}AGGREGATE {}", indent, aggregates_display_string)?;
                    fmt_operator(source, f, level + 1, true)
                }
                Operator::Filter {
                    source, predicates, ..
                } => {
                    let predicates_string = predicates
                        .iter()
                        .map(|p| p.to_string())
                        .collect::<Vec<String>>()
                        .join(" AND ");
                    writeln!(f, "{}FILTER {}", indent, predicates_string)?;
                    fmt_operator(source, f, level + 1, true)
                }
                Operator::Limit { source, limit, .. } => {
                    writeln!(f, "{}TAKE {}", indent, limit)?;
                    fmt_operator(source, f, level + 1, true)
                }
                Operator::Join {
                    left,
                    right,
                    predicates,
                    outer,
                    ..
                } => {
                    let join_name = if *outer { "OUTER JOIN" } else { "JOIN" };
                    match predicates
                        .as_ref()
                        .and_then(|ps| if ps.is_empty() { None } else { Some(ps) })
                    {
                        Some(ps) => {
                            let predicates_string = ps
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<String>>()
                                .join(" AND ");
                            writeln!(f, "{}{} ON {}", indent, join_name, predicates_string)?;
                        }
                        None => writeln!(f, "{}{}", indent, join_name)?,
                    }
                    fmt_operator(left, f, level + 1, false)?;
                    fmt_operator(right, f, level + 1, true)
                }
                Operator::Order { source, key, .. } => {
                    let sort_keys_string = key
                        .iter()
                        .map(|(expr, dir)| format!("{} {}", expr, dir))
                        .collect::<Vec<String>>()
                        .join(", ");
                    writeln!(f, "{}SORT {}", indent, sort_keys_string)?;
                    fmt_operator(source, f, level + 1, true)
                }
                Operator::Projection {
                    source,
                    expressions,
                    ..
                } => {
                    let expressions = expressions
                        .iter()
                        .map(|expr| match expr {
                            ProjectionColumn::Column(c) => c.to_string(),
                            ProjectionColumn::Star => "*".to_string(),
                            ProjectionColumn::TableStar(table_reference) => {
                                format!("{}.{}", table_reference.table_identifier, "*")
                            }
                        })
                        .collect::<Vec<String>>()
                        .join(", ");
                    writeln!(f, "{}PROJECT {}", indent, expressions)?;
                    fmt_operator(source, f, level + 1, true)
                }
                Operator::Scan {
                    table_reference,
                    predicates: filter,
                    ..
                } => {
                    let table_name =
                        if table_reference.table.name == table_reference.table_identifier {
                            table_reference.table_identifier.clone()
                        } else {
                            format!(
                                "{} AS {}",
                                &table_reference.table.name, &table_reference.table_identifier
                            )
                        };
                    let filter_string = filter.as_ref().map(|f| {
                        let filters_string = f
                            .iter()
                            .map(|p| p.to_string())
                            .collect::<Vec<String>>()
                            .join(" AND ");
                        format!("FILTER {}", filters_string)
                    });
                    match filter_string {
                        Some(fs) => writeln!(f, "{}SCAN {} {}", indent, table_name, fs),
                        None => writeln!(f, "{}SCAN {}", indent, table_name),
                    }?;
                    Ok(())
                }
                Operator::Search {
                    table_reference,
                    search,
                    ..
                } => {
                    match search {
                        Search::PrimaryKeyEq { .. } | Search::PrimaryKeySearch { .. } => {
                            writeln!(
                                f,
                                "{}SEARCH {} USING INTEGER PRIMARY KEY (rowid=?)",
                                indent, table_reference.table_identifier
                            )?;
                        }
                        Search::IndexSearch { index, .. } => {
                            writeln!(
                                f,
                                "{}SEARCH {} USING INDEX {}",
                                indent, table_reference.table_identifier, index.name
                            )?;
                        }
                    }
                    Ok(())
                }
                Operator::Nothing => Ok(()),
            }
        }
        writeln!(f, "QUERY PLAN")?;
        fmt_operator(self, f, 0, true)
    }
}

/**
  Returns a bitmask where each bit corresponds to a table in the `tables` vector.
  If a table is referenced in the given Operator, the corresponding bit is set to 1.
  Example:
    if tables = [(table1, "t1"), (table2, "t2"), (table3, "t3")],
    and the Operator is a join between table2 and table3,
    then the return value will be (in bits): 110
*/
pub fn get_table_ref_bitmask_for_operator<'a>(
    tables: &'a Vec<BTreeTableReference>,
    operator: &'a Operator,
) -> Result<usize> {
    let mut table_refs_mask = 0;
    match operator {
        Operator::Aggregate { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, source)?;
        }
        Operator::Filter {
            source, predicates, ..
        } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, source)?;
            for predicate in predicates {
                table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, predicate)?;
            }
        }
        Operator::Limit { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, source)?;
        }
        Operator::Join { left, right, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, left)?;
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, right)?;
        }
        Operator::Order { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, source)?;
        }
        Operator::Projection { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, source)?;
        }
        Operator::Scan {
            table_reference, ..
        } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|t| Rc::ptr_eq(&t.table, &table_reference.table))
                    .unwrap();
        }
        Operator::Search {
            table_reference, ..
        } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|t| Rc::ptr_eq(&t.table, &table_reference.table))
                    .unwrap();
        }
        Operator::Nothing => {}
    }
    Ok(table_refs_mask)
}

/**
  Returns a bitmask where each bit corresponds to a table in the `tables` vector.
  If a table is referenced in the given AST expression, the corresponding bit is set to 1.
  Example:
    if tables = [(table1, "t1"), (table2, "t2"), (table3, "t3")],
    and predicate = "t1.a = t2.b"
    then the return value will be (in bits): 011
*/
pub fn get_table_ref_bitmask_for_ast_expr<'a>(
    tables: &'a Vec<BTreeTableReference>,
    predicate: &'a ast::Expr,
) -> Result<usize> {
    let mut table_refs_mask = 0;
    match predicate {
        ast::Expr::Binary(e1, _, e2) => {
            table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, e1)?;
            table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, e2)?;
        }
        ast::Expr::Id(ident) => {
            let ident = normalize_ident(&ident.0);
            let matching_tables = tables
                .iter()
                .enumerate()
                .filter(|(_, table_reference)| table_reference.table.get_column(&ident).is_some());

            let mut matches = 0;
            let mut matching_tbl = None;
            for table in matching_tables {
                matching_tbl = Some(table);
                matches += 1;
                if matches > 1 {
                    crate::bail_parse_error!("ambiguous column name {}", &ident)
                }
            }

            if let Some((tbl_index, _)) = matching_tbl {
                table_refs_mask |= 1 << tbl_index;
            } else {
                crate::bail_parse_error!("column not found: {}", &ident)
            }
        }
        ast::Expr::Qualified(tbl, ident) => {
            let tbl = normalize_ident(&tbl.0);
            let ident = normalize_ident(&ident.0);
            let matching_table = tables
                .iter()
                .enumerate()
                .find(|(_, t)| t.table_identifier == tbl);

            if matching_table.is_none() {
                crate::bail_parse_error!("introspect: table not found: {}", &tbl)
            }
            let (table_index, table_reference) = matching_table.unwrap();
            if table_reference.table.get_column(&ident).is_none() {
                crate::bail_parse_error!("column with qualified name {}.{} not found", &tbl, &ident)
            }

            table_refs_mask |= 1 << table_index;
        }
        ast::Expr::Literal(_) => {}
        ast::Expr::Like { lhs, rhs, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, lhs)?;
            table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, rhs)?;
        }
        ast::Expr::FunctionCall {
            args: Some(args), ..
        } => {
            for arg in args {
                table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, arg)?;
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, lhs)?;
            if let Some(rhs_list) = rhs {
                for rhs_expr in rhs_list {
                    table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, rhs_expr)?;
                }
            }
        }
        _ => {}
    }

    Ok(table_refs_mask)
}
