use sqlite3_parser::ast::{self, JoinOperator, JoinType};

use crate::{function::Func, schema::Table, vdbe::BranchOffset};

#[derive(Debug)]
pub struct SrcTable<'a> {
    pub table: Table,
    pub identifier: String,
    pub join_info: Option<&'a ast::JoinedSelectTable>,
}

impl SrcTable<'_> {
    pub fn is_outer_join(&self) -> bool {
        matches!(
            self.join_info,
            Some(ast::JoinedSelectTable {
                operator: JoinOperator::TypedJoin {
                    natural: false,
                    join_type: Some(
                        JoinType::Left
                            | JoinType::LeftOuter
                            | JoinType::Right
                            | JoinType::RightOuter
                    )
                },
                ..
            })
        )
    }
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

pub struct LeftJoinBookkeeping {
    // integer register that holds a flag that is set to true if the current row has a match for the left join
    pub match_flag_register: usize,
    // label for the instruction that sets the match flag to true
    pub set_match_flag_true_label: BranchOffset,
    // label for the instruction where the program jumps to if the current row has a match for the left join
    pub on_match_jump_to_label: BranchOffset,
}

pub struct LoopInfo {
    // The table or table alias that we are looping over
    pub identifier: String,
    // Metadata about a left join, if any
    pub left_join_bookkeeping: Option<LeftJoinBookkeeping>,
    // The label for the instruction that reads the next row for this table
    pub next_row_label: BranchOffset,
    // The label for the instruction that rewinds the cursor for this table
    pub rewind_label: BranchOffset,
    // The label for the instruction that is jumped to in the Rewind instruction if the table is empty
    pub rewind_on_empty_label: BranchOffset,
    // The ID of the cursor that is opened for this table
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
