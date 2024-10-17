#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    LtEq = 3,
    Gt = 4,
    GtEq = 5,
    And = 6,
    Or = 7,
    Plus = 8,
    Minus = 9,
    Multiply = 10,
    Divide = 11,
    Like = 12,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub alias: Option<String>,
    pub table_no: Option<u64>,
    pub column_no: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    Column(Column),
    Literal(String),
    Binary {
        lhs: Box<Expression>,
        op: Operator,
        rhs: Box<Expression>,
    },
    Parenthesized(Box<Expression>),
    FunctionCall {
        name: String,
        args: Option<Vec<Expression>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultColumn {
    Expr {
        expr: Expression,
        alias: Option<String>,
    },
    Star,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub alias: Option<String>,
    pub table_no: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinVariant {
    Inner = 0,
    Outer = 1,
    Left = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinType {
    pub(crate) bitmask: u8,
}

impl JoinType {
    pub fn new() -> Self {
        Self {
            bitmask: JoinVariant::Inner as u8,
        }
    }

    pub fn with(mut self, variant: JoinVariant) -> Self {
        // outer needs to clear inner and vice versa
        match variant {
            JoinVariant::Inner => {
                self.bitmask &= !(JoinVariant::Outer as u8);
                self.bitmask |= JoinVariant::Inner as u8;
            }
            JoinVariant::Outer => {
                self.bitmask |= JoinVariant::Outer as u8;
            }
            JoinVariant::Left => {
                self.bitmask |= JoinVariant::Left as u8;
                self.bitmask -= JoinVariant::Outer as u8;
            }
        }
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Join {
    pub join_type: JoinType,
    pub table: Table,
    pub on: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromClause {
    pub table: Table,
    pub joins: Vec<Join>,
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Direction {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub columns: Vec<ResultColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<Vec<Expression>>,
    pub order_by: Option<Vec<(Expression, Direction)>>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStatement {
    Select(SelectStatement),
}
