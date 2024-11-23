use core::fmt;
use std::{
    fmt::{Display, Formatter},
    rc::Rc,
};

use sqlite3_parser::ast;

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Index},
    Result,
};

#[derive(Debug)]
pub enum ResultSetColumn {
    Expr {
        expr: ast::Expr,
        contains_aggregates: bool,
    },
    Agg(Aggregate),
}

#[derive(Debug)]
pub struct Plan {
    pub source: SourceOperator,
    pub result_columns: Vec<ResultSetColumn>,
    pub where_clause: Option<Vec<ast::Expr>>,
    pub group_by: Option<Vec<ast::Expr>>,
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    pub aggregates: Option<Vec<Aggregate>>,
    pub limit: Option<usize>,
    pub referenced_tables: Vec<BTreeTableReference>,
    pub available_indexes: Vec<Rc<Index>>,
}

impl Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
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
pub enum SourceOperator {
    // Join operator
    // This operator is used to join two source operators.
    // It takes a left and right source operator, a list of predicates to evaluate,
    // and a boolean indicating whether it is an outer join.
    Join {
        id: usize,
        left: Box<SourceOperator>,
        right: Box<SourceOperator>,
        predicates: Option<Vec<ast::Expr>>,
        outer: bool,
    },
    // Scan operator
    // This operator is used to scan a table.
    // It takes a table to scan and an optional list of predicates to evaluate.
    // The predicates are used to filter rows from the table.
    // e.g. SELECT * FROM t1 WHERE t1.foo = 5
    // The iter_dir are uset to indicate the direction of the iterator.
    // The use of Option for iter_dir is aimed at implementing a conservative optimization strategy: it only pushes
    // iter_dir down to Scan when iter_dir is None, to prevent potential result set errors caused by multiple
    // assignments. for more detailed discussions, please refer to https://github.com/penberg/limbo/pull/376
    Scan {
        id: usize,
        table_reference: BTreeTableReference,
        predicates: Option<Vec<ast::Expr>>,
        iter_dir: Option<IterationDirection>,
    },
    // Search operator
    // This operator is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search {
        id: usize,
        table_reference: BTreeTableReference,
        search: Search,
        predicates: Option<Vec<ast::Expr>>,
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
    pub table_index: usize,
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

impl SourceOperator {
    pub fn id(&self) -> usize {
        match self {
            SourceOperator::Join { id, .. } => *id,
            SourceOperator::Scan { id, .. } => *id,
            SourceOperator::Search { id, .. } => *id,
            SourceOperator::Nothing => unreachable!(),
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
impl Display for SourceOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        fn fmt_operator(
            operator: &SourceOperator,
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
                SourceOperator::Join {
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
                SourceOperator::Scan {
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
                SourceOperator::Search {
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
                SourceOperator::Nothing => Ok(()),
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
    operator: &'a SourceOperator,
) -> Result<usize> {
    let mut table_refs_mask = 0;
    match operator {
        SourceOperator::Join { left, right, .. } => {
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, left)?;
            table_refs_mask |= get_table_ref_bitmask_for_operator(tables, right)?;
        }
        SourceOperator::Scan {
            table_reference, ..
        } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|t| Rc::ptr_eq(&t.table, &table_reference.table))
                    .unwrap();
        }
        SourceOperator::Search {
            table_reference, ..
        } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|t| Rc::ptr_eq(&t.table, &table_reference.table))
                    .unwrap();
        }
        SourceOperator::Nothing => {}
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
        ast::Expr::Column { table, .. } => {
            table_refs_mask |= 1 << table;
        }
        ast::Expr::Id(_) => unreachable!("Id should be resolved to a Column before optimizer"),
        ast::Expr::Qualified(_, _) => {
            unreachable!("Qualified should be resolved to a Column before optimizer")
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
