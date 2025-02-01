use core::fmt;
use sqlite3_parser::ast;
use std::{
    fmt::{Display, Formatter},
    rc::Rc,
};

use crate::{
    function::AggFunc,
    schema::{BTreeTable, Column, Index, Table},
    vdbe::BranchOffset,
};
use crate::{
    schema::{PseudoTable, Type},
    translate::plan::Plan::{Delete, Select},
};

#[derive(Debug, Clone)]
pub struct ResultSetColumn {
    pub expr: ast::Expr,
    pub name: String,
    // TODO: encode which aggregates (e.g. index bitmask of plan.aggregates) are present in this column
    pub contains_aggregates: bool,
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    pub exprs: Vec<ast::Expr>,
    /// having clause split into a vec at 'AND' boundaries.
    pub having: Option<Vec<ast::Expr>>,
}

/// In a query plan, WHERE clause conditions and JOIN conditions are all folded into a vector of JoinAwareConditionExpr.
/// This is done so that we can evaluate the conditions at the correct loop depth.
/// We also need to keep track of whether the condition came from an OUTER JOIN. Take this example:
/// SELECT * FROM users u LEFT JOIN products p ON u.id = 5.
/// Even though the condition only refers to 'u', we CANNOT evaluate it at the users loop, because we need to emit NULL
/// values for the columns of 'p', for EVERY row in 'u', instead of completely skipping any rows in 'u' where the condition is false.
#[derive(Debug, Clone)]
pub struct JoinAwareConditionExpr {
    /// The original condition expression.
    pub expr: ast::Expr,
    /// Is this condition originally from an OUTER JOIN?
    /// If so, we need to evaluate it at the loop of the right table in that JOIN,
    /// regardless of which tables it references.
    /// We also cannot e.g. short circuit the entire query in the optimizer if the condition is statically false.
    pub from_outer_join: bool,
    /// The loop index where to evaluate the condition.
    /// For example, in `SELECT * FROM u JOIN p WHERE u.id = 5`, the condition can already be evaluated at the first loop (idx 0),
    /// because that is the rightmost table that it references.
    pub eval_at_loop: usize,
}

/// A query plan is either a SELECT or a DELETE (for now)
#[derive(Debug, Clone)]
pub enum Plan {
    Select(SelectPlan),
    Delete(DeletePlan),
}

/// The type of the query, either top level or subquery
#[derive(Debug, Clone)]
pub enum SelectQueryType {
    TopLevel,
    Subquery {
        /// The register that holds the program offset that handles jumping to/from the subquery.
        yield_reg: usize,
        /// The index of the first instruction in the bytecode that implements the subquery.
        coroutine_implementation_start: BranchOffset,
    },
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    /// List of table references in loop order, outermost first.
    pub table_references: Vec<TableReference>,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries. all join conditions also get shoved in here,
    /// and we keep track of which join they came from (mainly for OUTER JOIN processing)
    pub where_clause: Vec<JoinAwareConditionExpr>,
    /// group by clause
    pub group_by: Option<GroupBy>,
    /// order by clause
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    /// all the aggregates collected from the result columns, order by, and (TODO) having clauses
    pub aggregates: Vec<Aggregate>,
    /// limit clause
    pub limit: Option<isize>,
    /// offset clause
    pub offset: Option<isize>,
    /// all the indexes available
    pub available_indexes: Vec<Rc<Index>>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
    /// query type (top level or subquery)
    pub query_type: SelectQueryType,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DeletePlan {
    /// List of table references. Delete is always a single table.
    pub table_references: Vec<TableReference>,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries.
    pub where_clause: Vec<JoinAwareConditionExpr>,
    /// order by clause
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    /// limit clause
    pub limit: Option<isize>,
    /// offset clause
    pub offset: Option<isize>,
    /// all the indexes available
    pub available_indexes: Vec<Rc<Index>>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

pub fn select_star(tables: &[TableReference], out_columns: &mut Vec<ResultSetColumn>) {
    for (current_table_index, table) in tables.iter().enumerate() {
        let maybe_using_cols = table
            .join_info
            .as_ref()
            .and_then(|join_info| join_info.using.as_ref());
        out_columns.extend(
            table
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, col)| {
                    // If we are joining with USING, we need to deduplicate the columns from the right table
                    // that are also present in the USING clause.
                    if let Some(using_cols) = maybe_using_cols {
                        !using_cols
                            .iter()
                            .any(|using_col| col.name.eq_ignore_ascii_case(&using_col.0))
                    } else {
                        true
                    }
                })
                .map(|(i, col)| ResultSetColumn {
                    name: col.name.clone(),
                    expr: ast::Expr::Column {
                        database: None,
                        table: current_table_index,
                        column: i,
                        is_rowid_alias: col.is_rowid_alias,
                    },
                    contains_aggregates: false,
                }),
        );
    }
}

/// Join information for a table reference.
#[derive(Debug, Clone)]
pub struct JoinInfo {
    /// Whether this is an OUTER JOIN.
    pub outer: bool,
    /// The USING clause for the join, if any. NATURAL JOIN is transformed into USING (col1, col2, ...).
    pub using: Option<ast::DistinctNames>,
}

/// A table reference in the query plan.
/// For example, SELECT * FROM users u JOIN products p JOIN (SELECT * FROM users) sub
/// has three table references:
/// 1. operation=Scan, table=users, table_identifier=u, reference_type=BTreeTable, join_info=None
/// 2. operation=Scan, table=products, table_identifier=p, reference_type=BTreeTable, join_info=Some(JoinInfo { outer: false, using: None }),
/// 3. operation=Subquery, table=users, table_identifier=sub, reference_type=Subquery, join_info=None
#[derive(Debug, Clone)]
pub struct TableReference {
    /// The operation that this table reference performs.
    pub op: Operation,
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub identifier: String,
    /// The join info for this table reference, if it is the right side of a join (which all except the first table reference have)
    pub join_info: Option<JoinInfo>,
}

/**
  A SourceOperator is a reference in the query plan that reads data from a table.
*/
#[derive(Clone, Debug)]
pub enum Operation {
    // Scan operation
    // This operation is used to scan a table.
    // The iter_dir are uset to indicate the direction of the iterator.
    // The use of Option for iter_dir is aimed at implementing a conservative optimization strategy: it only pushes
    // iter_dir down to Scan when iter_dir is None, to prevent potential result set errors caused by multiple
    // assignments. for more detailed discussions, please refer to https://github.com/tursodatabase/limbo/pull/376
    Scan {
        iter_dir: Option<IterationDirection>,
    },
    // Search operation
    // This operation is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search(Search),
    /// Subquery operation
    /// This operation is used to represent a subquery in the query plan.
    /// The subquery itself (recursively) contains an arbitrary SelectPlan.
    Subquery {
        plan: Box<SelectPlan>,
        result_columns_start_reg: usize,
    },
}

impl TableReference {
    /// Returns the btree table for this table reference, if it is a BTreeTable.
    pub fn btree(&self) -> Option<Rc<BTreeTable>> {
        self.table.btree()
    }

    /// Creates a new TableReference for a subquery.
    pub fn new_subquery(identifier: String, plan: SelectPlan, join_info: Option<JoinInfo>) -> Self {
        let table = Table::Pseudo(Rc::new(PseudoTable::new_with_columns(
            plan.result_columns
                .iter()
                .map(|rc| Column {
                    name: rc.name.clone(),
                    ty: Type::Text, // FIXME: infer proper type
                    ty_str: "TEXT".to_string(),
                    is_rowid_alias: false,
                    primary_key: false,
                    notnull: false,
                    default: None,
                })
                .collect(),
        )));
        Self {
            op: Operation::Subquery {
                plan: Box::new(plan),
                result_columns_start_reg: 0, // Will be set in the bytecode emission phase
            },
            table,
            identifier: identifier.clone(),
            join_info,
        }
    }

    pub fn columns(&self) -> &[Column] {
        self.table.columns()
    }
}

/// An enum that represents a search operation that can be used to search for a row in a table using an index
/// (i.e. a primary key or a secondary index)
#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug)]
pub enum Search {
    /// A rowid equality point lookup. This is a special case that uses the SeekRowid bytecode instruction and does not loop.
    RowidEq { cmp_expr: JoinAwareConditionExpr },
    /// A rowid search. Uses bytecode instructions like SeekGT, SeekGE etc.
    RowidSearch {
        cmp_op: ast::Operator,
        cmp_expr: JoinAwareConditionExpr,
    },
    /// A secondary index search. Uses bytecode instructions like SeekGE, SeekGT etc.
    IndexSearch {
        index: Rc<Index>,
        cmp_op: ast::Operator,
        cmp_expr: JoinAwareConditionExpr,
    },
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
                        if table_reference.table.get_name() == table_reference.table_identifier {
                            table_reference.table_identifier.clone()
                        } else {
                            format!(
                                "{} AS {}",
                                &table_reference.table.get_name(),
                                &table_reference.table_identifier
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
                        Search::RowidEq { .. } | Search::RowidSearch { .. } => {
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
                SourceOperator::Subquery { plan, .. } => {
                    fmt_operator(&plan.source, f, level + 1, last)
                }
                SourceOperator::Nothing { .. } => Ok(()),
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
    tables: &'a Vec<TableReference>,
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
                    .position(|t| t.table_identifier == table_reference.table_identifier)
                    .unwrap();
        }
        SourceOperator::Search {
            table_reference, ..
        } => {
            table_refs_mask |= 1
                << tables
                    .iter()
                    .position(|t| t.table_identifier == table_reference.table_identifier)
                    .unwrap();
        }
        SourceOperator::Subquery { .. } => {}
        SourceOperator::Nothing { .. } => {}
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
#[allow(clippy::only_used_in_recursion)]
pub fn get_table_ref_bitmask_for_ast_expr<'a>(
    tables: &'a Vec<TableReference>,
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
