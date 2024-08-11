use core::fmt;
use std::{
    fmt::{Display, Formatter},
    rc::Rc,
};

use sqlite3_parser::ast;

use crate::{function::AggFunc, schema::BTreeTable, util::normalize_ident, Result};

pub struct Plan {
    pub root_node: Operator,
    pub referenced_tables: Vec<(Rc<BTreeTable>, String)>,
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.root_node)
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
    // SeekRowid operator
    // This operator is used to retrieve a single row from a table by its rowid.
    // rowid_predicate is an expression that produces the comparison value for the rowid.
    // e.g. rowid = 5, or rowid = other_table.foo
    // predicates is an optional list of additional predicates to evaluate.
    SeekRowid {
        id: usize,
        table: Rc<BTreeTable>,
        table_identifier: String,
        rowid_predicate: ast::Expr,
        predicates: Option<Vec<ast::Expr>>,
    },
    // Limit operator
    // This operator is used to limit the number of rows returned by the source operator.
    Limit {
        id: usize,
        source: Box<Operator>,
        limit: usize,
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
    },
    // Order operator
    // This operator is used to sort the rows returned by the source operator.
    Order {
        id: usize,
        source: Box<Operator>,
        key: Vec<(ast::Expr, Direction)>,
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
    },
    // Scan operator
    // This operator is used to scan a table.
    // It takes a table to scan and an optional list of predicates to evaluate.
    // The predicates are used to filter rows from the table.
    // e.g. SELECT * FROM t1 WHERE t1.foo = 5
    Scan {
        id: usize,
        table: Rc<BTreeTable>,
        table_identifier: String,
        predicates: Option<Vec<ast::Expr>>,
    },
    // Nothing operator
    // This operator is used to represent an empty query.
    // e.g. SELECT * from foo WHERE 0 will eventually be optimized to Nothing.
    Nothing,
}

#[derive(Clone, Debug)]
pub enum ProjectionColumn {
    Column(ast::Expr),
    Star,
    TableStar(Rc<BTreeTable>, String),
}

impl ProjectionColumn {
    pub fn column_count(&self, referenced_tables: &[(Rc<BTreeTable>, String)]) -> usize {
        match self {
            ProjectionColumn::Column(_) => 1,
            ProjectionColumn::Star => {
                let mut count = 0;
                for (table, _) in referenced_tables {
                    count += table.columns.len();
                }
                count
            }
            ProjectionColumn::TableStar(table, _) => table.columns.len(),
        }
    }
}

impl Operator {
    pub fn column_count(&self, referenced_tables: &[(Rc<BTreeTable>, String)]) -> usize {
        match self {
            Operator::Aggregate { aggregates, .. } => aggregates.len(),
            Operator::Filter { source, .. } => source.column_count(referenced_tables),
            Operator::SeekRowid { table, .. } => table.columns.len(),
            Operator::Limit { source, .. } => source.column_count(referenced_tables),
            Operator::Join { left, right, .. } => {
                left.column_count(referenced_tables) + right.column_count(referenced_tables)
            }
            Operator::Order { source, .. } => source.column_count(referenced_tables),
            Operator::Projection { expressions, .. } => expressions
                .iter()
                .map(|e| e.column_count(referenced_tables))
                .sum(),
            Operator::Scan { table, .. } => table.columns.len(),
            Operator::Nothing => 0,
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        match self {
            Operator::Aggregate { .. } => {
                todo!();
            }
            Operator::Filter { source, .. } => source.column_names(),
            Operator::SeekRowid { table, .. } => {
                table.columns.iter().map(|c| c.name.clone()).collect()
            }
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
                    ProjectionColumn::TableStar(_, tbl) => format!("{}.{}", tbl, "*"),
                })
                .collect(),
            Operator::Scan { table, .. } => table.columns.iter().map(|c| c.name.clone()).collect(),
            Operator::Nothing => vec![],
        }
    }

    pub fn id(&self) -> usize {
        match self {
            Operator::Aggregate { id, .. } => *id,
            Operator::Filter { id, .. } => *id,
            Operator::SeekRowid { id, .. } => *id,
            Operator::Limit { id, .. } => *id,
            Operator::Join { id, .. } => *id,
            Operator::Order { id, .. } => *id,
            Operator::Projection { id, .. } => *id,
            Operator::Scan { id, .. } => *id,
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
        fn fmt_node(node: &Operator, f: &mut Formatter, level: usize) -> fmt::Result {
            let indent = "    ".repeat(level);
            match node {
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
                    fmt_node(source, f, level + 1)
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
                    fmt_node(source, f, level + 1)
                }
                Operator::SeekRowid {
                    table,
                    rowid_predicate,
                    predicates,
                    ..
                } => {
                    match predicates {
                        Some(ps) => {
                            let predicates_string = ps
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<String>>()
                                .join(" AND ");
                            writeln!(
                                f,
                                "{}SEEK {}.rowid ON rowid={} FILTER {}",
                                indent, &table.name, rowid_predicate, predicates_string
                            )?;
                        }
                        None => writeln!(
                            f,
                            "{}SEEK {}.rowid ON rowid={}",
                            indent, &table.name, rowid_predicate
                        )?,
                    }

                    Ok(())
                }
                Operator::Limit { source, limit, .. } => {
                    writeln!(f, "{}TAKE {}", indent, limit)?;
                    fmt_node(source, f, level + 1)
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
                    fmt_node(left, f, level + 1)?;
                    fmt_node(right, f, level + 1)
                }
                Operator::Order { source, key, .. } => {
                    let sort_keys_string = key
                        .iter()
                        .map(|(expr, dir)| format!("{} {}", expr, dir))
                        .collect::<Vec<String>>()
                        .join(", ");
                    writeln!(f, "{}SORT {}", indent, sort_keys_string)?;
                    fmt_node(source, f, level + 1)
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
                            ProjectionColumn::TableStar(_, a) => format!("{}.{}", a, "*"),
                        })
                        .collect::<Vec<String>>()
                        .join(", ");
                    writeln!(f, "{}PROJECT {}", indent, expressions)?;
                    fmt_node(source, f, level + 1)
                }
                Operator::Scan {
                    table,
                    predicates: filter,
                    table_identifier,
                    ..
                } => {
                    let table_name = format!("{} AS {}", &table.name, &table_identifier);
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
                Operator::Nothing => Ok(()),
            }
        }
        fmt_node(self, f, 0)
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
pub fn get_table_ref_bitmask_for_query_plan_node<'a>(
    tables: &'a Vec<(Rc<BTreeTable>, String)>,
    node: &'a Operator,
) -> Result<usize> {
    let mut table_refs_mask = 0;
    match node {
        Operator::Aggregate { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, source)?;
        }
        Operator::Filter {
            source, predicates, ..
        } => {
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, source)?;
            for predicate in predicates {
                table_refs_mask |= get_table_ref_bitmask_for_ast_expr(tables, predicate)?;
            }
        }
        Operator::SeekRowid { table, .. } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|(t, _)| Rc::ptr_eq(t, table))
                    .unwrap();
        }
        Operator::Limit { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, source)?;
        }
        Operator::Join { left, right, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, left)?;
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, right)?;
        }
        Operator::Order { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, source)?;
        }
        Operator::Projection { source, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_query_plan_node(tables, source)?;
        }
        Operator::Scan { table, .. } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|(t, _)| Rc::ptr_eq(t, table))
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
    tables: &'a Vec<(Rc<BTreeTable>, String)>,
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
                .filter(|(_, (table, _))| table.get_column(&ident).is_some());

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
                .find(|(_, (table, t_id))| *t_id == tbl);

            if matching_table.is_none() {
                crate::bail_parse_error!("introspect: table not found: {}", &tbl)
            }
            let matching_table = matching_table.unwrap();
            if matching_table.1 .0.get_column(&ident).is_none() {
                crate::bail_parse_error!("column with qualified name {}.{} not found", &tbl, &ident)
            }

            table_refs_mask |= 1 << matching_table.0;
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
