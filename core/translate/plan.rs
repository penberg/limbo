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
    Result,
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
    /// A tree of sources (tables).
    pub source: SourceOperator,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries.
    pub where_clause: Option<Vec<ast::Expr>>,
    /// group by clause
    pub group_by: Option<GroupBy>,
    /// order by clause
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    /// all the aggregates collected from the result columns, order by, and (TODO) having clauses
    pub aggregates: Vec<Aggregate>,
    /// limit clause
    pub limit: Option<usize>,
    /// all the tables referenced in the query
    pub referenced_tables: Vec<TableReference>,
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
    /// A tree of sources (tables).
    pub source: SourceOperator,
    /// the columns inside SELECT ... FROM
    pub result_columns: Vec<ResultSetColumn>,
    /// where clause split into a vec at 'AND' boundaries.
    pub where_clause: Option<Vec<ast::Expr>>,
    /// order by clause
    pub order_by: Option<Vec<(ast::Expr, Direction)>>,
    /// limit clause
    pub limit: Option<usize>,
    /// all the tables referenced in the query
    pub referenced_tables: Vec<TableReference>,
    /// all the indexes available
    pub available_indexes: Vec<Rc<Index>>,
    /// query contains a constant condition that is always false
    pub contains_constant_false_condition: bool,
}

impl Display for Plan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Select(select_plan) => write!(f, "{}", select_plan.source),
            Delete(delete_plan) => write!(f, "{}", delete_plan.source),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IterationDirection {
    Forwards,
    Backwards,
}

impl SourceOperator {
    pub fn select_star(&self, out_columns: &mut Vec<ResultSetColumn>) {
        for (table_index, col, idx) in self.select_star_helper() {
            out_columns.push(ResultSetColumn {
                name: col.name.clone(),
                expr: ast::Expr::Column {
                    database: None,
                    table: table_index,
                    column: idx,
                    is_rowid_alias: col.is_rowid_alias,
                },
                contains_aggregates: false,
            });
        }
    }

    /// All this ceremony is required to deduplicate columns when joining with USING
    fn select_star_helper(&self) -> Vec<(usize, &Column, usize)> {
        match self {
            SourceOperator::Join {
                left, right, using, ..
            } => {
                let mut columns = left.select_star_helper();

                // Join columns are filtered out from the right side
                // in the case of a USING join.
                if let Some(using_cols) = using {
                    let right_columns = right.select_star_helper();

                    for (table_index, col, idx) in right_columns {
                        if !using_cols
                            .iter()
                            .any(|using_col| col.name.eq_ignore_ascii_case(&using_col.0))
                        {
                            columns.push((table_index, col, idx));
                        }
                    }
                } else {
                    columns.extend(right.select_star_helper());
                }
                columns
            }
            SourceOperator::Scan {
                table_reference, ..
            }
            | SourceOperator::Search {
                table_reference, ..
            }
            | SourceOperator::Subquery {
                table_reference, ..
            } => table_reference
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| (table_reference.table_index, col, i))
                .collect(),
            SourceOperator::Nothing { .. } => Vec::new(),
        }
    }
}

/**
  A SourceOperator is a Node in the query plan that reads data from a table.
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
        using: Option<ast::DistinctNames>,
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
        table_reference: TableReference,
        predicates: Option<Vec<ast::Expr>>,
        iter_dir: Option<IterationDirection>,
    },
    // Search operator
    // This operator is used to search for a row in a table using an index
    // (i.e. a primary key or a secondary index)
    Search {
        id: usize,
        table_reference: TableReference,
        search: Search,
        predicates: Option<Vec<ast::Expr>>,
    },
    Subquery {
        id: usize,
        table_reference: TableReference,
        plan: Box<SelectPlan>,
        predicates: Option<Vec<ast::Expr>>,
    },
    // Nothing operator
    // This operator is used to represent an empty query.
    // e.g. SELECT * from foo WHERE 0 will eventually be optimized to Nothing.
    Nothing {
        id: usize,
    },
}

/// The type of the table reference, either BTreeTable or Subquery
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableReferenceType {
    /// A BTreeTable is a table that is stored on disk in a B-tree index.
    BTreeTable,
    /// A subquery.
    Subquery {
        /// The index of the first register in the query plan that contains the result columns of the subquery.
        result_columns_start_reg: usize,
    },
}

/// A query plan has a list of TableReference objects, each of which represents a table or subquery.
#[derive(Clone, Debug)]
pub struct TableReference {
    /// Table object, which contains metadata about the table, e.g. columns.
    pub table: Table,
    /// The name of the table as referred to in the query, either the literal name or an alias e.g. "users" or "u"
    pub table_identifier: String,
    /// The index of this reference in the list of TableReference objects in the query plan
    /// The reference at index 0 is the first table in the FROM clause, the reference at index 1 is the second table in the FROM clause, etc.
    /// So, the index is relevant for determining when predicates (WHERE, ON filters etc.) should be evaluated.
    pub table_index: usize,
    /// The type of the table reference, either BTreeTable or Subquery
    pub reference_type: TableReferenceType,
}

impl TableReference {
    pub fn btree(&self) -> Option<Rc<BTreeTable>> {
        match self.reference_type {
            TableReferenceType::BTreeTable => self.table.btree(),
            TableReferenceType::Subquery { .. } => None,
        }
    }
    pub fn new_subquery(identifier: String, table_index: usize, plan: &SelectPlan) -> Self {
        Self {
            table: Table::Pseudo(Rc::new(PseudoTable::new_with_columns(
                plan.result_columns
                    .iter()
                    .map(|rc| Column {
                        name: rc.name.clone(),
                        ty: Type::Text, // FIXME: infer proper type
                        is_rowid_alias: false,
                        primary_key: false,
                    })
                    .collect(),
            ))),
            table_identifier: identifier.clone(),
            table_index,
            reference_type: TableReferenceType::Subquery {
                result_columns_start_reg: 0, // Will be set in the bytecode emission phase
            },
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
    RowidEq { cmp_expr: ast::Expr },
    /// A rowid search. Uses bytecode instructions like SeekGT, SeekGE etc.
    RowidSearch {
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
            SourceOperator::Subquery { id, .. } => *id,
            SourceOperator::Nothing { id } => *id,
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
