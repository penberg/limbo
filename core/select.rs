use sqlite3_parser::ast;

use crate::{function::Func, schema::Table, vdbe::BranchOffset};

pub struct SrcTable<'a> {
    pub table: Table,
    pub alias: Option<&'a String>,
    pub join_info: Option<&'a ast::JoinedSelectTable>, // FIXME: preferably this should be a reference with lifetime == Select ast expr
}

#[derive(Debug)]
pub struct ColumnInfo<'a> {
    pub func: Option<Func>,
    pub args: &'a Option<Vec<ast::Expr>>,
    pub columns_to_allocate: usize, /* number of result columns this col will result on */
}

impl<'a> ColumnInfo<'a> {
    pub fn new() -> Self {
        Self {
            func: None,
            args: &None,
            columns_to_allocate: 1,
        }
    }

    pub fn is_aggregation_function(&self) -> bool {
        matches!(self.func, Some(Func::Agg(_)))
    }
}

pub struct LoopInfo {
    pub rewind_offset: BranchOffset,
    pub rewind_label: BranchOffset,
    pub open_cursor: usize,
}

pub struct Select<'a> {
    pub columns: &'a Vec<ast::ResultColumn>,
    pub column_info: Vec<ColumnInfo<'a>>,
    pub src_tables: Vec<SrcTable<'a>>, // Tables we use to get data from. This includes "from" and "joins"
    pub limit: &'a Option<ast::Limit>,
    pub order_by: &'a Option<Vec<ast::SortedColumn>>,
    pub exist_aggregation: bool,
    pub where_clause: &'a Option<ast::Expr>,
    /// Ordered list of opened read table loops
    /// Used for generating a loop that looks like this:
    /// cursor 0 = open table 0
    /// for each row in cursor 0
    ///     cursor 1 = open table 1
    ///     for each row in cursor 1
    ///         ...
    ///     end cursor 1
    /// end cursor 0
    pub loops: Vec<LoopInfo>,
}
