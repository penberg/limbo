pub mod check;
pub mod fmt;

use std::{num::NonZeroU32, sync::Arc};

use crate::lexer::is_quotable_keyword;
use strum_macros::{EnumIter, EnumString};

/// `?` or `$` Prepared statement arg placeholder(s)
#[derive(Default)]
pub struct ParameterInfo {
    /// Number of SQL parameters in a prepared statement, like `sqlite3_bind_parameter_count`
    pub count: u32,
    /// Parameter name(s) if any
    pub names: Vec<String>,
}

/// Output format of an `EXPLAIN QUERY PLAN` statement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EqpFormat {
    /// `FORMAT=TEXT`
    #[default]
    Text,
    /// `FORMAT=JSON`
    Json,
}

/// Statement or Explain statement
// https://sqlite.org/syntax/sql-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    /// `EXPLAIN` statement
    Explain(Stmt),
    /// `EXPLAIN QUERY PLAN` statement
    ExplainQueryPlan { stmt: Stmt, format: EqpFormat },
    /// statement
    Stmt(Stmt),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CreateVirtualTable {
    /// `IF NOT EXISTS`
    pub if_not_exists: bool,
    /// table name
    pub tbl_name: QualifiedName,
    /// module name
    pub module_name: Name,
    /// args
    pub args: Vec<String>, // TODO smol str
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Update {
    /// CTE
    pub with: Option<With>,
    /// `OR`
    pub or_conflict: Option<ResolveType>,
    /// table name
    pub tbl_name: QualifiedName,
    /// `INDEXED`
    pub indexed: Option<Indexed>,
    /// `SET` assignments
    pub sets: Vec<Set>,
    /// `FROM`
    pub from: Option<FromClause>,
    /// `WHERE` clause
    pub where_clause: Option<Box<Expr>>,
    /// `RETURNING`
    pub returning: Vec<ResultColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AlterTable {
    // table name
    pub name: QualifiedName,
    // `ALTER TABLE` body
    pub body: AlterTableBody,
}
/// SQL statement
// https://sqlite.org/syntax/sql-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Stmt {
    /// `ALTER TABLE`: table name, body
    AlterTable(AlterTable),
    /// `ANALYSE`: object name
    Analyze {
        // object name
        name: Option<QualifiedName>,
    },
    /// `ATTACH DATABASE`
    Attach {
        /// filename
        // TODO distinction between ATTACH and ATTACH DATABASE
        expr: Box<Expr>,
        /// schema name
        db_name: Box<Expr>,
        /// password
        key: Option<Box<Expr>>,
    },
    /// `BEGIN`: tx type, tx name
    Begin {
        // transaction type
        typ: Option<TransactionType>,
        // transaction name
        name: Option<Name>,
    },
    /// `COMMIT`/`END`: tx name
    Commit {
        // tx name
        name: Option<Name>,
    }, // TODO distinction between COMMIT and END
    /// `CREATE INDEX`
    CreateIndex {
        /// `UNIQUE`
        unique: bool,
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// index name
        idx_name: QualifiedName,
        /// table name
        tbl_name: Name,
        /// USING module
        using: Option<Name>,
        /// indexed columns or expressions
        columns: Vec<SortedColumn>,
        /// WITH parameters
        with_clause: Vec<(Name, Box<Expr>)>,
        /// partial index
        where_clause: Option<Box<Expr>>,
    },
    /// `CREATE TABLE`
    CreateTable {
        /// `TEMPORARY`
        temporary: bool, // TODO distinction between TEMP and TEMPORARY
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// table name
        tbl_name: QualifiedName,
        /// table body
        body: CreateTableBody,
    },
    /// `CREATE TRIGGER`
    CreateTrigger {
        /// `TEMPORARY`
        temporary: bool,
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// trigger name
        trigger_name: QualifiedName,
        /// `BEFORE`/`AFTER`/`INSTEAD OF`
        time: Option<TriggerTime>,
        /// `DELETE`/`INSERT`/`UPDATE`
        event: TriggerEvent,
        /// table name
        tbl_name: QualifiedName,
        /// `FOR EACH ROW`
        for_each_row: bool,
        /// `WHEN`
        when_clause: Option<Box<Expr>>,
        /// statements
        commands: Vec<TriggerCmd>,
    },
    /// `CREATE VIEW`
    CreateView {
        /// `TEMPORARY`
        temporary: bool,
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// view name
        view_name: QualifiedName,
        /// columns
        columns: Vec<IndexedColumn>,
        /// query
        select: Select,
    },
    /// `CREATE MATERIALIZED VIEW`
    CreateMaterializedView {
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// view name
        view_name: QualifiedName,
        /// columns
        columns: Vec<IndexedColumn>,
        /// query
        select: Select,
    },

    /// `CREATE VIRTUAL TABLE`
    CreateVirtualTable(CreateVirtualTable),
    /// `CREATE TYPE`
    CreateType {
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// type name
        type_name: String,
        /// type body
        body: CreateTypeBody,
    },
    /// `CREATE DOMAIN`
    CreateDomain {
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// domain name
        domain_name: String,
        /// base type (primitive or another domain/custom type)
        base_type: String,
        /// default expression
        default: Option<Box<Expr>>,
        /// NOT NULL constraint
        not_null: bool,
        /// CHECK constraints
        constraints: Vec<DomainConstraint>,
    },
    /// `DELETE`
    Delete {
        /// CTE
        with: Option<With>,
        /// `FROM` table name
        tbl_name: QualifiedName,
        /// `INDEXED`
        indexed: Option<Indexed>,
        /// `WHERE` clause
        where_clause: Option<Box<Expr>>,
        /// `RETURNING`
        returning: Vec<ResultColumn>,
    },
    /// `DETACH DATABASE`: db name
    Detach {
        // db name
        name: Box<Expr>,
    }, // TODO distinction between DETACH and DETACH DATABASE
    /// `DROP INDEX`
    DropIndex {
        /// `IF EXISTS`
        if_exists: bool,
        /// index name
        idx_name: QualifiedName,
    },
    /// `DROP TABLE`
    DropTable {
        /// `IF EXISTS`
        if_exists: bool,
        /// table name
        tbl_name: QualifiedName,
    },
    /// `DROP TRIGGER`
    DropTrigger {
        /// `IF EXISTS`
        if_exists: bool,
        /// trigger name
        trigger_name: QualifiedName,
    },
    /// `DROP VIEW`
    DropView {
        /// `IF EXISTS`
        if_exists: bool,
        /// view name
        view_name: QualifiedName,
    },
    /// `DROP TYPE`
    DropType {
        /// `IF EXISTS`
        if_exists: bool,
        /// type name
        type_name: String,
    },
    /// `DROP DOMAIN`
    DropDomain {
        /// `IF EXISTS`
        if_exists: bool,
        /// domain name
        domain_name: String,
    },
    /// `INSERT`
    Insert {
        /// CTE
        with: Option<With>,
        /// `OR`
        or_conflict: Option<ResolveType>, // TODO distinction between REPLACE and INSERT OR REPLACE
        /// table name
        tbl_name: QualifiedName,
        /// `COLUMNS`
        columns: Vec<Name>,
        /// `VALUES` or `SELECT`
        body: InsertBody,
        /// `RETURNING`
        returning: Vec<ResultColumn>,
    },
    /// `PRAGMA`: pragma name, body
    Pragma {
        // pragma name
        name: QualifiedName,
        // pragma body
        body: Option<PragmaBody>,
    },
    /// `REINDEX`
    Reindex {
        /// collation or index or table name
        name: Option<QualifiedName>,
    },
    /// `RELEASE`: savepoint name
    Release {
        // savepoint name
        name: Name,
    }, // TODO distinction between RELEASE and RELEASE SAVEPOINT
    /// `ROLLBACK`
    Rollback {
        /// transaction name
        tx_name: Option<Name>,
        /// savepoint name
        savepoint_name: Option<Name>, // TODO distinction between TO and TO SAVEPOINT
    },
    /// `SAVEPOINT`: savepoint name
    Savepoint {
        // savepoint name
        name: Name,
    },
    /// `SELECT`
    Select(Select),
    /// `UPDATE`
    Update(Update),
    /// `VACUUM`: database name, into expr
    Vacuum {
        // database name
        name: Option<Name>,
        // into expression
        into: Option<Box<Expr>>,
    },
    /// `OPTIMIZE INDEX`: index name
    Optimize {
        /// index name (None means optimize all indexes)
        idx_name: Option<QualifiedName>,
    },
    /// `CREATE SEQUENCE`
    CreateSequence {
        /// `IF NOT EXISTS`
        if_not_exists: bool,
        /// sequence name
        seq_name: QualifiedName,
        /// start value
        start: Option<i64>,
        /// increment by
        increment: Option<i64>,
        /// minimum value
        min_value: Option<i64>,
        /// maximum value
        max_value: Option<i64>,
        /// cycle on overflow
        cycle: bool,
    },
    /// `DROP SEQUENCE`
    DropSequence {
        /// `IF EXISTS`
        if_exists: bool,
        /// sequence name
        seq_name: QualifiedName,
    },
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Internal ID of a table reference.
///
/// Used by [Expr::Column] and [Expr::RowId] to refer to a table.
/// E.g. in 'SELECT * FROM t UNION ALL SELECT * FROM t', there are two table references,
/// so there are two TableInternalIds.
///
/// FIXME: rename this to TableReferenceId.
pub struct TableInternalId(usize);

impl TableInternalId {
    /// used in generated columns to signify "the table that the column belongs to"
    pub const SELF_TABLE: Self = Self(0);

    pub const fn is_self_table(&self) -> bool {
        self.0 == 0
    }
}

impl Default for TableInternalId {
    fn default() -> Self {
        Self(1)
    }
}

impl From<usize> for TableInternalId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl std::ops::AddAssign<usize> for TableInternalId {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs;
    }
}

impl From<TableInternalId> for usize {
    fn from(value: TableInternalId) -> Self {
        value.0
    }
}

impl std::fmt::Display for TableInternalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "t{}", self.0)
    }
}

/// SQL expression
/// Pre-resolved field/variant index for FieldAccess expressions.
/// Populated during binding so that translation can emit instructions directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FieldAccessResolution {
    StructField { field_index: usize },
    UnionVariant { tag_index: u8 },
}

// https://sqlite.org/syntax/expr.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Expr {
    /// `BETWEEN`
    Between {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// start
        start: Box<Expr>,
        /// end
        end: Box<Expr>,
    },
    /// binary expression
    Binary(Box<Expr>, Operator, Box<Expr>),
    /// Register reference for DBSP expression compilation
    /// This is not part of SQL syntax but used internally for incremental computation
    Register(usize),
    /// `CASE` expression
    Case {
        /// operand
        base: Option<Box<Expr>>,
        /// `WHEN` condition `THEN` result
        when_then_pairs: Vec<(Box<Expr>, Box<Expr>)>,
        /// `ELSE` result
        else_expr: Option<Box<Expr>>,
    },
    /// CAST expression
    Cast {
        /// expression
        expr: Box<Expr>,
        /// `AS` type name
        type_name: Option<Type>,
    },
    /// `COLLATE`: expression
    Collate(Box<Expr>, Name),
    /// schema-name.table-name.column-name
    DoublyQualified(Name, Name, Name),
    /// `EXISTS` subquery
    Exists(Select),
    /// Struct/union field access (produced by translator, not parser directly)
    FieldAccess {
        /// base expression (e.g., column reference)
        base: Box<Expr>,
        /// field or variant name
        field: Name,
        /// pre-resolved field/variant index (populated during binding)
        resolved: Option<FieldAccessResolution>,
    },
    /// call to a built-in function
    FunctionCall {
        /// function name
        name: Name,
        /// `DISTINCT`
        distinctness: Option<Distinctness>,
        /// arguments
        args: Vec<Box<Expr>>,
        /// `ORDER BY` (aggregate argument ordering inside the parentheses)
        order_by: Vec<SortedColumn>,
        /// `WITHIN GROUP (ORDER BY ...)` for ordered-set aggregates
        within_group: Vec<SortedColumn>,
        /// `FILTER`
        filter_over: FunctionTail,
    },
    /// Function call expression with '*' as arg
    FunctionCallStar {
        /// function name
        name: Name,
        /// `FILTER`
        filter_over: FunctionTail,
    },
    /// Identifier
    Id(Name),
    /// Column
    Column {
        /// the x in `x.y.z`. index of the db in catalog.
        database: Option<usize>,
        /// the y in `x.y.z`. index of the table in catalog.
        table: TableInternalId,
        /// the z in `x.y.z`. index of the column in the table.
        column: usize,
        /// is the column a rowid alias
        is_rowid_alias: bool,
    },
    /// `ROWID`
    RowId {
        /// the x in `x.y.z`. index of the db in catalog.
        database: Option<usize>,
        /// the y in `x.y.z`. index of the table in catalog.
        table: TableInternalId,
    },
    /// `IN`
    InList {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// values
        rhs: Vec<Box<Expr>>,
    },
    /// `IN` subselect
    InSelect {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// subquery
        rhs: Select,
    },
    /// `IN` table name / function
    InTable {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// table name
        rhs: QualifiedName,
        /// table function arguments
        args: Vec<Box<Expr>>,
    },
    /// `IS NULL`
    IsNull(Box<Expr>),
    /// `LIKE`
    Like {
        /// expression
        lhs: Box<Expr>,
        /// `NOT`
        not: bool,
        /// operator
        op: LikeOperator,
        /// pattern
        rhs: Box<Expr>,
        /// `ESCAPE` char
        escape: Option<Box<Expr>>,
    },
    /// Literal expression
    Literal(Literal),
    /// Name
    Name(Name),
    /// `NOT NULL` or `NOTNULL`
    NotNull(Box<Expr>),
    /// Parenthesized subexpression
    Parenthesized(Vec<Box<Expr>>),
    /// Qualified name
    Qualified(Name, Name),
    /// `RAISE` function call
    Raise(ResolveType, Option<Box<Expr>>),
    /// Subquery expression
    Subquery(Select),
    /// Unary expression
    Unary(UnaryOperator, Box<Expr>),
    /// Parameters
    Variable(Variable),
    /// Subqueries from e.g. the WHERE clause are planned separately
    /// and their results will be placed in registers or in an ephemeral index
    /// pointed to by this type.
    SubqueryResult {
        /// Internal "opaque" identifier for the subquery. When the translator encounters
        /// a [Expr::SubqueryResult], it needs to know which subquery in the corresponding
        /// query plan it references.
        subquery_id: TableInternalId,
        /// Left-hand side expression for IN subqueries.
        /// This property plus 'not_in' are only relevant for IN subqueries,
        /// and the reason they are not included in the [SubqueryType] enum is so that
        /// we don't have to clone this Box.
        lhs: Option<Box<Expr>>,
        /// Whether the IN subquery is a NOT IN subquery.
        not_in: bool,
        /// The type of subquery.
        query_type: SubqueryType,
    },
    /// `DEFAULT` keyword in INSERT VALUES
    Default,
    /// `ARRAY[expr, ...]` array literal
    Array {
        /// elements of the array
        elements: Vec<Box<Expr>>,
    },
    /// `expr[index]` subscript/element access
    Subscript {
        /// base expression (the array)
        base: Box<Expr>,
        /// index expression
        index: Box<Expr>,
    },
}

impl Default for Expr {
    fn default() -> Self {
        Self::Literal(Literal::Null)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Variable {
    pub index: NonZeroU32,
    pub name: Option<Box<str>>,
    /// Type of the source column, if known (e.g. from trigger NEW/OLD rewrite).
    pub col_type: Option<Box<str>>,
    /// True for an explicit `?N` marker. Numbered markers carry no allocated
    /// name; their "?N" spelling is derived from the index on demand.
    #[cfg_attr(feature = "serde", serde(default))]
    pub numbered: bool,
}

impl Variable {
    pub fn indexed(index: NonZeroU32) -> Self {
        Self {
            index,
            name: None,
            col_type: None,
            numbered: false,
        }
    }

    /// An explicit `?N` marker.
    pub fn numbered(index: NonZeroU32) -> Self {
        Self {
            index,
            name: None,
            col_type: None,
            numbered: true,
        }
    }

    pub fn indexed_typed(index: NonZeroU32, col_type: &str) -> Self {
        Self {
            index,
            name: None,
            col_type: if col_type.is_empty() {
                None
            } else {
                Some(col_type.into())
            },
            numbered: false,
        }
    }

    pub fn named(name: impl Into<Box<str>>, index: NonZeroU32) -> Self {
        Self {
            index,
            name: Some(name.into()),
            col_type: None,
            numbered: false,
        }
    }

    /// True for positional parameters — a bare `?` or an explicit `?N` —
    /// and false for named ones (`:x`, `@x`, `$x`). Positional markers
    /// carry no allocated name.
    pub fn is_positional(&self) -> bool {
        self.name.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SubqueryType {
    /// EXISTS subquery; result is stored in a single register.
    Exists { result_reg: usize },
    /// Row value subquery; result is stored in a range of registers.
    /// Example: x = (SELECT ...) or (x, y) = (SELECT ...)
    RowValue {
        result_reg_start: usize,
        num_regs: usize,
    },
    /// IN subquery; result is stored in an ephemeral index.
    /// Example: x <NOT> IN (SELECT ...)
    In {
        cursor_id: usize,
        /// Affinity string used by the IN operator probe and ephemeral materialization.
        /// Mirrors SQLite's exprINAffinity behavior.
        affinity_str: Arc<String>,
    },
}

impl Expr {
    pub fn into_boxed(self) -> Box<Expr> {
        Box::new(self)
    }

    pub fn unary(operator: UnaryOperator, expr: Expr) -> Expr {
        Expr::Unary(operator, Box::new(expr))
    }

    pub fn binary(lhs: Expr, operator: Operator, rhs: Expr) -> Expr {
        Expr::Binary(Box::new(lhs), operator, Box::new(rhs))
    }

    pub fn not_null(expr: Expr) -> Expr {
        Expr::NotNull(Box::new(expr))
    }

    pub fn between(lhs: Expr, not: bool, start: Expr, end: Expr) -> Expr {
        Expr::Between {
            lhs: Box::new(lhs),
            not,
            start: Box::new(start),
            end: Box::new(end),
        }
    }

    pub fn in_select(lhs: Expr, not: bool, select: Select) -> Expr {
        Expr::InSelect {
            lhs: Box::new(lhs),
            not,
            rhs: select,
        }
    }

    pub fn like(
        lhs: Expr,
        not: bool,
        operator: LikeOperator,
        rhs: Expr,
        escape: Option<Expr>,
    ) -> Expr {
        Expr::Like {
            lhs: Box::new(lhs),
            not,
            op: operator,
            rhs: Box::new(rhs),
            escape: escape.map(Box::new),
        }
    }

    pub fn is_null(expr: Expr) -> Expr {
        Expr::IsNull(Box::new(expr))
    }

    pub fn collate(expr: Expr, name: Name) -> Expr {
        Expr::Collate(Box::new(expr), name)
    }

    pub fn cast(expr: Expr, type_name: Option<Type>) -> Expr {
        Expr::Cast {
            expr: Box::new(expr),
            type_name,
        }
    }

    pub fn raise(resolve_type: ResolveType, expr: Option<Expr>) -> Expr {
        Expr::Raise(resolve_type, expr.map(Box::new))
    }

    pub const fn can_be_null(&self) -> bool {
        // todo: better handling columns. Check sqlite3ExprCanBeNull
        match self {
            Expr::Literal(literal) => !matches!(
                literal,
                Literal::Numeric(_) | Literal::String(_) | Literal::Blob(_)
            ),
            _ => true,
        }
    }
}

/// SQL literal
#[derive(Clone, Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Literal {
    /// Number
    Numeric(String),
    /// String
    // TODO Check that string is already quoted and correctly escaped
    String(String),
    /// BLOB
    // TODO Check that string is valid (only hexa)
    Blob(String),
    /// Keyword
    Keyword(String),
    #[default]
    /// `NULL`
    Null,
    /// `TRUE` - SQLite boolean literal (equivalent to 1 but semantically distinct for IS TRUE)
    True,
    /// `FALSE` - SQLite boolean literal (equivalent to 0 but semantically distinct for IS FALSE)
    False,
    /// `CURRENT_DATE`
    CurrentDate,
    /// `CURRENT_TIME`
    CurrentTime,
    /// `CURRENT_TIMESTAMP`
    CurrentTimestamp,
}

pub fn blob_literal_hex(blob: &str) -> &str {
    debug_assert!(blob.len() >= 3);
    debug_assert!(matches!(blob.as_bytes()[0], b'x' | b'X'));
    debug_assert_eq!(blob.as_bytes()[1], b'\'');
    debug_assert_eq!(blob.as_bytes()[blob.len() - 1], b'\'');
    &blob[2..blob.len() - 1]
}

/// Textual comparison operator in an expression
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LikeOperator {
    /// `GLOB`
    Glob,
    /// `LIKE`
    Like,
    /// `MATCH`
    Match,
    /// `REGEXP`
    Regexp,
}

/// SQL operators
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Operator {
    /// `+`
    Add,
    /// `AND`
    And,
    /// `->`
    ArrowRight,
    /// `->>`
    ArrowRightShift,
    /// `&`
    BitwiseAnd,
    /// `|`
    BitwiseOr,
    /// `~`
    BitwiseNot,
    /// String concatenation (`||`)
    Concat,
    /// `=` or `==`
    Equals,
    /// `/`
    Divide,
    /// `>`
    Greater,
    /// `>=`
    GreaterEquals,
    /// `IS`
    Is,
    /// `IS NOT`
    IsNot,
    /// `<<`
    LeftShift,
    /// `<`
    Less,
    /// `<=`
    LessEquals,
    /// `%`
    Modulus,
    /// `*`
    Multiply,
    /// `!=` or `<>`
    NotEquals,
    /// `OR`
    Or,
    /// `>>`
    RightShift,
    /// `-`
    Subtract,
    /// `@>` array contains
    ArrayContains,
    /// `&&` array overlap
    ArrayOverlap,
}

impl Operator {
    /// returns whether order of operations can be ignored
    pub const fn is_commutative(&self) -> bool {
        matches!(
            self,
            Operator::Add
                | Operator::Multiply
                | Operator::BitwiseAnd
                | Operator::BitwiseOr
                | Operator::Equals
                | Operator::NotEquals
        )
    }

    /// Returns true if this operator is a comparison operator that may need affinity conversion
    pub const fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Equals
                | Self::NotEquals
                | Self::Less
                | Self::LessEquals
                | Self::Greater
                | Self::GreaterEquals
                | Self::Is
                | Self::IsNot
        )
    }
}

/// Unary operators
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UnaryOperator {
    /// bitwise negation (`~`)
    BitwiseNot,
    /// negative-sign
    Negative,
    /// `NOT`
    Not,
    /// positive-sign
    Positive,
}

/// `SELECT` statement
// https://sqlite.org/lang_select.html
// https://sqlite.org/syntax/factored-select-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Select {
    /// CTE
    pub with: Option<With>,
    /// body
    pub body: SelectBody,
    /// `ORDER BY`
    pub order_by: Vec<SortedColumn>, // ORDER BY term does not match any column in the result set
    /// `LIMIT`
    pub limit: Option<Limit>,
}

/// `SELECT` body
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SelectBody {
    /// first select
    pub select: OneSelect,
    /// compounds
    pub compounds: Vec<CompoundSelect>,
}

/// Compound select
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CompoundSelect {
    /// operator
    pub operator: CompoundOperator,
    /// select
    pub select: OneSelect,
}

/// Compound operators
// https://sqlite.org/syntax/compound-operator.html
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompoundOperator {
    /// `UNION`
    Union,
    /// `UNION ALL`
    UnionAll,
    /// `EXCEPT`
    Except,
    /// `INTERSECT`
    Intersect,
}

/// `SELECT` core
// https://sqlite.org/syntax/select-core.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OneSelect {
    /// `SELECT`
    Select {
        /// `DISTINCT`
        distinctness: Option<Distinctness>,
        /// columns
        columns: Vec<ResultColumn>,
        /// `FROM` clause
        from: Option<FromClause>,
        /// `WHERE` clause
        where_clause: Option<Box<Expr>>,
        /// `GROUP BY`
        group_by: Option<GroupBy>,
        /// `WINDOW` definition
        window_clause: Vec<WindowDef>,
    },
    /// `VALUES`
    Values(Vec<Vec<Box<Expr>>>),
}

/// `SELECT` ... `FROM` clause
// https://sqlite.org/syntax/join-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FromClause {
    /// table
    pub select: Box<SelectTable>, // FIXME mandatory
    /// `JOIN`ed tabled
    pub joins: Vec<JoinedSelectTable>,
}

/// `SELECT` distinctness
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Distinctness {
    /// `DISTINCT`
    Distinct,
    /// `ALL`
    All,
}

/// `SELECT` or `RETURNING` result column
// https://sqlite.org/syntax/result-column.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResultColumn {
    /// expression
    Expr(Box<Expr>, Option<As>),
    /// `*`
    Star,
    /// table name.`*`
    TableStar(Name),
}

/// Alias
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum As {
    /// `AS`
    As(Name),
    /// no `AS`
    Elided(Name), // FIXME Ids
    /// Implicit column name from original SQL text (not serialized to SQL).
    /// Used to preserve the original expression text as the column name
    /// for unaliased expressions, matching SQLite behavior.
    ImplicitColumnName(Name),
}

impl As {
    /// Returns the inner `Name` regardless of variant.
    pub fn name(&self) -> &Name {
        match self {
            As::As(name) | As::Elided(name) | As::ImplicitColumnName(name) => name,
        }
    }

    /// Returns `true` if this is a user-provided alias (`AS foo` or elided `foo`),
    /// not a system-generated implicit column name.
    pub fn is_explicit(&self) -> bool {
        matches!(self, As::As(_) | As::Elided(_))
    }
}

/// `JOIN` clause
// https://sqlite.org/syntax/join-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JoinedSelectTable {
    /// operator
    pub operator: JoinOperator,
    /// table
    pub table: Box<SelectTable>,
    /// constraint
    pub constraint: Option<JoinConstraint>,
}

/// Table or subquery
// https://sqlite.org/syntax/table-or-subquery.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SelectTable {
    /// table
    Table(QualifiedName, Option<As>, Option<Indexed>),
    /// table function call
    TableCall(QualifiedName, Vec<Box<Expr>>, Option<As>),
    /// `SELECT` subquery
    Select(Select, Option<As>),
    /// subquery
    Sub(FromClause, Option<As>),
}

/// Join operators
// https://sqlite.org/syntax/join-operator.html
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JoinOperator {
    /// `,`
    Comma,
    /// `JOIN`
    TypedJoin(Option<JoinType>),
}

// https://github.com/sqlite/sqlite/blob/80511f32f7e71062026edd471913ef0455563964/src/select.c#L197-L257
bitflags::bitflags! {
    /// `JOIN` types
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    pub struct JoinType: u8 {
        /// `INNER`
        const INNER   = 0x01;
        /// `CROSS` => INNER|CROSS
        const CROSS   = 0x02;
        /// `NATURAL`
        const NATURAL = 0x04;
        /// `LEFT` => LEFT|OUTER
        const LEFT    = 0x08;
        /// `RIGHT` => RIGHT|OUTER
        const RIGHT   = 0x10;
        /// `OUTER`
        const OUTER   = 0x20;
    }
}

/// `JOIN` constraint
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum JoinConstraint {
    /// `ON`
    On(Box<Expr>),
    /// `USING`: col names
    Using(Vec<Name>),
}

/// `GROUP BY`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct GroupBy {
    /// expressions
    pub exprs: Vec<Box<Expr>>,
    /// `HAVING`
    pub having: Option<Box<Expr>>, // HAVING clause on a non-aggregate query
}

/// identifier or string or `CROSS` or `FULL` or `INNER` or `LEFT` or `NATURAL` or `OUTER` or `RIGHT`.
///
/// Two Names are equal if they refer to the same identifier, regardless of
/// quoting style (e.g. `ABORT` and `"ABORT"` are the same identifier).
#[derive(Clone, Debug, Eq)]
pub struct Name {
    quote: Option<char>,
    value: String,
}

impl PartialEq for Name {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl std::hash::Hash for Name {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Name {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Name {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NameVisitor;
        impl<'de> serde::de::Visitor<'de> for NameVisitor {
            type Value = Name;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Name::from_bytes(v.as_bytes()))
            }
        }
        deserializer.deserialize_str(NameVisitor)
    }
}

impl Name {
    /// Create name which will have exactly the value of given string
    /// (e.g. if s = "\"str\"" - the name value will contain quotes and translation to SQL will give us """str""")
    pub fn exact(s: String) -> Self {
        Self {
            value: s,
            quote: None,
        }
    }
    /// Parse name from the bytes (e.g. handle quoting and handle escaped quotes)
    pub fn from_bytes(s: &[u8]) -> Self {
        Self::from_string(unsafe { std::str::from_utf8_unchecked(s) })
    }
    pub const fn empty() -> Self {
        Self {
            value: String::new(),
            quote: None,
        }
    }
    /// Create a name parsed from a bracket-quoted identifier (`[name]`),
    /// remembering the bracket quoting so the name renders back as written.
    pub const fn bracketed(s: String) -> Self {
        Self {
            value: s,
            quote: Some('['),
        }
    }
    /// Parse name from the string (e.g. handle quoting and handle escaped quotes)
    pub fn from_string(s: impl AsRef<str>) -> Self {
        let s = s.as_ref();
        let bytes = s.as_bytes();

        if s.is_empty() {
            return Name::exact(s.to_string());
        }

        if matches!(bytes[0], b'"' | b'\'' | b'`') {
            assert!(s.len() >= 2);
            assert!(bytes[bytes.len() - 1] == bytes[0]);
            let s = match bytes[0] {
                b'"' => s[1..s.len() - 1].replace("\"\"", "\""),
                b'\'' => s[1..s.len() - 1].replace("''", "'"),
                b'`' => s[1..s.len() - 1].replace("``", "`"),
                _ => unreachable!(),
            };
            Name {
                value: s,
                quote: Some(bytes[0] as char),
            }
        } else if bytes[0] == b'[' {
            assert!(s.len() >= 2);
            assert!(bytes[bytes.len() - 1] == b']');
            Name::bracketed(s[1..s.len() - 1].to_string())
        } else {
            Name::exact(s.to_string())
        }
    }

    /// Return string value of the name
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Convert value to the string literal (e.g. single-quoted string with escaped single quotes)
    pub fn as_literal(&self) -> String {
        format!("'{}'", self.value.replace("'", "''"))
    }

    /// Convert value to the name string (e.g. double-quoted string with escaped double quotes)
    pub fn as_ident(&self) -> String {
        // let's keep original quotes if they were set
        // (parser.rs tests validates that behaviour)
        if self.quote == Some('[') {
            // A `]` cannot be escaped inside a bracket-quoted identifier, so
            // fall back to double quotes when the name contains one.
            if !self.value.contains(']') {
                return format!("[{}]", self.value);
            }
            return format!("\"{}\"", self.value.replace('"', "\"\""));
        }
        if let Some(quote) = self.quote {
            let single = quote.to_string();
            let double = single.clone() + &single;
            return quote.to_string()
                + self.value.replace(&single, &double).as_str()
                + quote.to_string().as_str();
        }
        let value = self.value.as_bytes();
        let safe_char = |&c: &u8| c.is_ascii_alphanumeric() || c == b'_';
        if !value.is_empty() && value.iter().all(safe_char) && !is_quotable_keyword(value) {
            self.value.clone()
        } else {
            format!("\"{}\"", self.value.replace("\"", "\"\""))
        }
    }

    /// Checks if a name represents a quoted string that should get fallback behavior
    /// Need to detect legacy conversion of double quoted keywords to string literals
    /// (see https://sqlite.org/lang_keywords.html)
    ///
    /// Also, used to detect string literals in PRAGMA cases
    pub fn quoted_with(&self, quote: char) -> bool {
        self.quote == Some(quote)
    }

    pub const fn quoted(&self) -> bool {
        self.quote.is_some()
    }
}

/// Qualified name
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QualifiedName {
    /// schema
    pub db_name: Option<Name>,
    /// object name
    pub name: Name,
    /// alias
    pub alias: Option<Name>, // FIXME restrict alias usage (fullname vs xfullname)
}

impl QualifiedName {
    /// Constructor
    pub const fn single(name: Name) -> Self {
        Self {
            db_name: None,
            name,
            alias: None,
        }
    }
    /// Constructor
    pub const fn fullname(db_name: Name, name: Name) -> Self {
        Self {
            db_name: Some(db_name),
            name,
            alias: None,
        }
    }
    /// Constructor
    pub const fn xfullname(db_name: Name, name: Name, alias: Name) -> Self {
        Self {
            db_name: Some(db_name),
            name,
            alias: Some(alias),
        }
    }
    /// Constructor
    pub const fn alias(name: Name, alias: Name) -> Self {
        Self {
            db_name: None,
            name,
            alias: Some(alias),
        }
    }

    /// Return the resolved identifier as a String
    pub fn identifier(&self) -> String {
        self.alias.as_ref().map_or_else(
            || self.name.as_str().to_string(),
            |alias| alias.as_str().to_string(),
        )
    }
}

/// `ALTER TABLE` body
// https://sqlite.org/lang_altertable.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AlterTableBody {
    /// `RENAME TO`: new table name
    RenameTo(Name),
    /// `ADD COLUMN`
    AddColumn(ColumnDefinition), // TODO distinction between ADD and ADD COLUMN
    /// `ALTER COLUMN`
    AlterColumn { old: Name, new: ColumnDefinition },
    /// `RENAME COLUMN`
    RenameColumn {
        /// old name
        old: Name,
        /// new name
        new: Name,
    },
    /// `DROP COLUMN`
    DropColumn(Name), // TODO distinction between DROP and DROP COLUMN
}

/// Operator mapping in a `CREATE TYPE` body
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TypeOperator {
    /// operator symbol: "+", "-", "*", "/", "<", "=", etc.
    pub op: String,
    /// function name to call, or None for naked operators (use base type comparison)
    pub func_name: Option<String>,
}

/// A parameter in a `CREATE TYPE` definition, with an optional type annotation.
/// e.g. `value text` or `maxlen integer` or just `maxlen` (untyped, backward compat).
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TypeParam {
    pub name: String,
    /// Type annotation. None means untyped (backward compat).
    pub ty: Option<String>,
}

/// A single named CHECK constraint on a domain
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DomainConstraint {
    /// CONSTRAINT name (optional)
    pub name: Option<String>,
    /// CHECK expression using `value` placeholder
    pub check: Box<Expr>,
}

/// Body of a `CREATE TYPE` statement
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CreateTypeBody {
    /// Custom type with encode/decode (e.g., varchar, email)
    CustomType {
        /// type parameters, e.g. `(value text, maxlen integer)` for varchar
        params: Vec<TypeParam>,
        /// base storage type: "text", "integer", "real", "blob"
        base: String,
        /// encode expression (called on write), uses `value` placeholder for input
        encode: Option<Box<Expr>>,
        /// decode expression (called on read), uses `value` placeholder for input
        decode: Option<Box<Expr>>,
        /// operator-to-function mappings
        operators: Vec<TypeOperator>,
        /// default expression for columns of this type
        default: Option<Box<Expr>>,
    },
    /// `CREATE TYPE name AS STRUCT(field1 type1, field2 type2, ...)`
    Struct(Vec<TypeField>),
    /// `CREATE TYPE name AS UNION(variant1 type1, variant2 type2, ...)`
    Union(Vec<TypeField>),
}

/// `CREATE TABLE` body
// https://sqlite.org/lang_createtable.html
// https://sqlite.org/syntax/create-table-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CreateTableBody {
    /// columns and constraints
    ColumnsAndConstraints {
        /// table column definitions
        columns: Vec<ColumnDefinition>,
        /// table constraints
        constraints: Vec<NamedTableConstraint>,
        /// table options
        options: TableOptions,
    },
    /// `AS` select
    AsSelect(Select),
}

/// Table column definition
// https://sqlite.org/syntax/column-def.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnDefinition {
    /// column name
    pub col_name: Name,
    /// column type
    pub col_type: Option<Type>,
    /// column constraints
    pub constraints: Vec<NamedColumnConstraint>,
}

/// Named column constraint
// https://sqlite.org/syntax/column-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NamedColumnConstraint {
    /// constraint name
    pub name: Option<Name>,
    /// constraint
    pub constraint: ColumnConstraint,
}

/// Column constraint
// https://sqlite.org/syntax/column-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "simulator", derive(strum::EnumDiscriminants))]
#[cfg_attr(
    feature = "simulator",
    strum_discriminants(derive(strum::VariantArray))
)]
pub enum ColumnConstraint {
    /// `PRIMARY KEY`
    PrimaryKey {
        /// `ASC` / `DESC`
        order: Option<SortOrder>,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
        /// `AUTOINCREMENT`
        auto_increment: bool,
    },
    /// `NULL`
    NotNull {
        /// `NOT`
        nullable: bool,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
    },
    /// `UNIQUE`
    Unique(Option<ResolveType>),
    /// `CHECK`
    Check {
        /// constraint expression
        expr: Box<Expr>,
        /// The text between the CHECK parens exactly as the user wrote it,
        /// whitespace-trimmed. SQLite reports an unnamed failed constraint
        /// with this text. `None` when the constraint did not come from this
        /// parser — the PostgreSQL frontend's translator builds these nodes
        /// from its own AST — or after an ALTER TABLE rewrite changed the
        /// expression out from under the captured text.
        source: Option<String>,
    },
    /// `DEFAULT`
    Default(Box<Expr>),
    /// `COLLATE`
    Collate {
        /// collation name
        collation_name: Name, // FIXME Ids
    },
    /// `REFERENCES` foreign-key clause
    ForeignKey {
        /// clause
        clause: ForeignKeyClause,
        /// `DEFERRABLE`
        defer_clause: Option<DeferSubclause>,
    },
    /// `GENERATED`
    Generated {
        /// expression
        expr: Box<Expr>,
        /// `STORED` / `VIRTUAL`
        typ: Option<GeneratedColumnType>,
    },
}

/// Generated column type
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum GeneratedColumnType {
    /// `STORED`
    Stored,
    /// `VIRTUAL`
    Virtual,
}

/// Named table constraint
// https://sqlite.org/syntax/table-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NamedTableConstraint {
    /// constraint name
    pub name: Option<Name>,
    /// constraint
    pub constraint: TableConstraint,
}

/// Table constraint
// https://sqlite.org/syntax/table-constraint.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TableConstraint {
    /// `PRIMARY KEY`
    PrimaryKey {
        /// columns
        columns: Vec<SortedColumn>,
        /// `AUTOINCREMENT`
        auto_increment: bool,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
    },
    /// `UNIQUE`
    Unique {
        /// columns
        columns: Vec<SortedColumn>,
        /// `ON CONFLICT` clause
        conflict_clause: Option<ResolveType>,
    },
    /// `CHECK`
    Check {
        /// constraint expression
        expr: Box<Expr>,
        /// The text between the CHECK parens exactly as the user wrote it,
        /// whitespace-trimmed. SQLite reports an unnamed failed constraint
        /// with this text. `None` when the constraint did not come from this
        /// parser — the PostgreSQL frontend's translator builds these nodes
        /// from its own AST — or after an ALTER TABLE rewrite changed the
        /// expression out from under the captured text.
        source: Option<String>,
    },
    /// `FOREIGN KEY`
    ForeignKey {
        /// columns
        columns: Vec<IndexedColumn>,
        /// `REFERENCES`
        clause: ForeignKeyClause,
        /// `DEFERRABLE`
        defer_clause: Option<DeferSubclause>,
    },
}

/// `CREATE TABLE` options with preserved original text
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TableOptions {
    /// Original text for WITHOUT ROWID option (e.g., "WITHOUT ROWID", "without rowid", "WiThOuT rOwId")
    pub without_rowid_text: Option<String>,
    /// Original text for STRICT option (e.g., "STRICT", "strict", "StRiCt")
    pub strict_text: Option<String>,
}

impl TableOptions {
    /// Create empty table options
    pub fn empty() -> Self {
        Self {
            without_rowid_text: None,
            strict_text: None,
        }
    }

    /// Check if table has WITHOUT ROWID option
    pub fn contains_without_rowid(&self) -> bool {
        self.without_rowid_text.is_some()
    }

    /// Check if table has STRICT option
    pub fn contains_strict(&self) -> bool {
        self.strict_text.is_some()
    }

    /// For backward compatibility with bitflags interface
    pub fn contains(&self, flag: TableOptionsFlag) -> bool {
        match flag {
            TableOptionsFlag::WithoutRowid => self.contains_without_rowid(),
            TableOptionsFlag::Strict => self.contains_strict(),
        }
    }
}

/// Flags for table options (used for backward compatibility)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableOptionsFlag {
    WithoutRowid,
    Strict,
}

/// Sort orders
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SortOrder {
    /// `ASC`
    Asc,
    /// `DESC`
    Desc,
}

/// `NULLS FIRST` or `NULLS LAST`
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NullsOrder {
    /// `NULLS FIRST`
    First,
    /// `NULLS LAST`
    Last,
}

impl NullsOrder {
    pub fn reverse(&self) -> Self {
        match self {
            NullsOrder::First => NullsOrder::Last,
            NullsOrder::Last => NullsOrder::First,
        }
    }

    pub fn default_for(order: SortOrder) -> Self {
        match order {
            SortOrder::Asc => NullsOrder::First,
            SortOrder::Desc => NullsOrder::Last,
        }
    }
}

/// `REFERENCES` clause
// https://sqlite.org/syntax/foreign-key-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ForeignKeyClause {
    /// foreign table name
    pub tbl_name: Name,
    /// foreign table columns
    pub columns: Vec<IndexedColumn>,
    /// referential action(s) / deferrable option(s)
    pub args: Vec<RefArg>,
}

/// foreign-key reference args
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RefArg {
    /// `ON DELETE`
    OnDelete(RefAct),
    /// `ON INSERT`
    OnInsert(RefAct),
    /// `ON UPDATE`
    OnUpdate(RefAct),
    /// `MATCH`
    Match(Name),
}

/// foreign-key reference actions
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RefAct {
    /// `SET NULL`
    SetNull,
    /// `SET DEFAULT`
    SetDefault,
    /// `CASCADE`
    Cascade,
    /// `RESTRICT`
    Restrict,
    /// `NO ACTION`
    NoAction,
}

/// foreign-key defer clause
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DeferSubclause {
    /// `DEFERRABLE`
    pub deferrable: bool,
    /// `INITIALLY` `DEFERRED` / `IMMEDIATE`
    pub init_deferred: Option<InitDeferredPred>,
}

/// `INITIALLY` `DEFERRED` / `IMMEDIATE`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InitDeferredPred {
    /// `INITIALLY DEFERRED`
    InitiallyDeferred,
    /// `INITIALLY IMMEDIATE`
    InitiallyImmediate, // default
}

/// Indexed column
// https://sqlite.org/syntax/indexed-column.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IndexedColumn {
    /// column name
    pub col_name: Name,
    /// `COLLATE`
    pub collation_name: Option<Name>, // FIXME Ids
    /// `ORDER BY`
    pub order: Option<SortOrder>,
}

/// `INDEXED BY` / `NOT INDEXED`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Indexed {
    /// `INDEXED BY`: idx name
    IndexedBy(Name),
    /// `NOT INDEXED`
    NotIndexed,
}

/// Sorted column
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SortedColumn {
    /// expression
    pub expr: Box<Expr>,
    /// `ASC` / `DESC`
    pub order: Option<SortOrder>,
    /// `NULLS FIRST` / `NULLS LAST`
    pub nulls: Option<NullsOrder>,
}

/// `LIMIT`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Limit {
    /// count
    pub expr: Box<Expr>,
    /// `OFFSET`
    pub offset: Option<Box<Expr>>, // TODO distinction between LIMIT offset, count and LIMIT count OFFSET offset
}

/// `INSERT` body
// https://sqlite.org/lang_insert.html
// https://sqlite.org/syntax/insert-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[allow(clippy::large_enum_variant)]
pub enum InsertBody {
    /// `SELECT` or `VALUES`
    Select(Select, Option<Box<Upsert>>),
    /// `DEFAULT VALUES`
    DefaultValues,
}

/// `UPDATE ... SET`
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Set {
    /// column name(s)
    pub col_names: Vec<Name>,
    /// expression
    pub expr: Box<Expr>,
}

/// `PRAGMA` body
// https://sqlite.org/syntax/pragma-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PragmaBody {
    /// `=`
    Equals(PragmaValue),
    /// function call
    Call(PragmaValue),
}

/// `PRAGMA` value
// https://sqlite.org/syntax/pragma-value.html
pub type PragmaValue = Box<Expr>; // TODO

/// `PRAGMA` value
// https://sqlite.org/pragma.html
#[derive(Clone, Debug, PartialEq, Eq, EnumIter, EnumString, strum::Display)]
#[strum(serialize_all = "snake_case")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PragmaName {
    /// Returns the application ID of the database file.
    ApplicationId,
    /// set the autovacuum mode
    AutoVacuum,
    /// set the busy_timeout (see https://www.sqlite.org/pragma.html#pragma_busy_timeout)
    BusyTimeout,
    /// `cache_size` pragma
    CacheSize,
    /// set the cache spill behavior
    CacheSpill,
    /// When ON, each INSERT, UPDATE and DELETE returns one row with the
    /// number of rows it changed.
    CountChanges,
    /// encryption cipher algorithm name for encrypted databases
    #[strum(serialize = "cipher")]
    #[cfg_attr(feature = "serde", serde(rename = "cipher"))]
    EncryptionCipher,
    /// Control fsync error retry behavior (0 = off/panic, 1 = on/retry)
    DataSyncRetry,
    /// List databases
    DatabaseList,
    /// Encoding - only support utf8
    Encoding,
    /// Current free page count.
    FreelistCount,
    /// Enable or disable foreign key constraint enforcement
    ForeignKeys,
    /// Returns information about foreign keys declared by a table
    ForeignKeyList,
    /// Deprecated: control whether column names include table name prefix
    FullColumnNames,
    /// List all SQL functions known to the database connection
    FunctionList,
    /// Use F_FULLFSYNC instead of fsync on macOS (only supported on macOS)
    #[cfg(target_vendor = "apple")]
    Fullfsync,
    /// Enable or disable CHECK constraint enforcement
    IgnoreCheckConstraints,
    /// Run integrity check on the database file
    IntegrityCheck,
    /// `journal_mode` pragma
    JournalMode,
    /// `locking_mode` pragma
    LockingMode,
    /// Run a quick integrity check (skips expensive index consistency validation)
    QuickCheck,
    /// encryption key for encrypted databases, specified as hexadecimal string.
    #[strum(serialize = "hexkey")]
    #[cfg_attr(feature = "serde", serde(rename = "hexkey"))]
    EncryptionKey,
    /// Noop as per SQLite docs
    LegacyFileFormat,
    /// Set or get the maximum number of pages in the database file.
    MaxPageCount,
    /// `module_list` pragma
    /// `module_list` lists modules used by virtual tables.
    ModuleList,
    /// Return the total number of pages in the database file.
    PageCount,
    /// Return the page size of the database in bytes.
    PageSize,
    /// make connection query only
    QueryOnly,
    /// Returns schema version of the database file.
    SchemaVersion,
    /// Deprecated: control whether unaliased column names omit the table name prefix
    ShortColumnNames,
    /// Alias for `require_where` pragma, as an homage to MySQL (https://dev.mysql.com/doc/refman/9.6/en/mysql-tips.html#safe-updates)
    IAmADummy,
    /// Reject DELETE/UPDATE without WHERE clause
    RequireWhere,
    /// Control database synchronization mode (OFF | FULL | NORMAL | EXTRA)
    Synchronous,
    /// Control where temporary tables and indices are stored (DEFAULT=0, FILE=1, MEMORY=2)
    TempStore,
    /// returns information about the columns of an index
    IndexInfo,
    /// returns extended information about the columns of an index
    IndexXinfo,
    /// returns the list of indexes for a table
    IndexList,
    /// returns information about all tables and views
    TableList,
    /// returns information about the columns of a table
    TableInfo,
    /// returns extended information about the columns of a table
    ///
    /// The only differece from TableInfo is additional "hidden" column whose value signifies
    /// - a normal column (0)
    /// - a dynamic or stored generated column (2 or 3)
    /// - or a hidden column in a virtual table (1)
    TableXinfo,
    /// enable capture-changes logic for the connection (stable name)
    CaptureDataChangesConn,
    /// enable capture-changes logic for the connection (deprecated alias)
    UnstableCaptureDataChangesConn,
    /// Returns the user version of the database file.
    UserVersion,
    /// trigger a checkpoint to run on database(s) if WAL is enabled
    WalCheckpoint,
    /// Sets or queries the threshold (in bytes) at which MVCC triggers an automatic checkpoint.
    MvccCheckpointThreshold,
    /// Sets or queries the threshold (in newly inserted row versions since the
    /// last GC pass) at which MVCC runs an inline, non-blocking garbage
    /// collection pass on the commit path. -1 disables inline GC.
    MvccGcThreshold,
    /// Sets or queries whether concurrent MVCC commits batch their logical-log
    /// appends behind a single fsync.
    MvccGroupCommit,
    /// Sets or queries the number of visible FTS index segments a statement
    /// flush may leave behind before the write path merges them. 0 disables
    /// write-path merging.
    FtsMergeThreshold,
    /// List all available types (built-in and custom)
    ListTypes,
    /// Deprecated no-op: control whether callback is invoked for empty result sets
    EmptyResultCallbacks,
    /// VDBE opcode trace output
    VdbeTrace,
}

/// `CREATE TRIGGER` time
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TriggerTime {
    /// `BEFORE`
    Before, // default
    /// `AFTER`
    After,
    /// `INSTEAD OF`
    InsteadOf,
}

/// `CREATE TRIGGER` event
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TriggerEvent {
    /// `DELETE`
    Delete,
    /// `INSERT`
    Insert,
    /// `UPDATE`
    Update,
    /// `UPDATE OF`: col names
    UpdateOf(Vec<Name>),
}

/// `CREATE TRIGGER` command
// https://sqlite.org/lang_createtrigger.html
// https://sqlite.org/syntax/create-trigger-stmt.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TriggerCmd {
    /// `UPDATE`
    Update {
        /// `OR`
        or_conflict: Option<ResolveType>,
        /// table name
        tbl_name: Name,
        /// `SET` assignments
        sets: Vec<Set>,
        /// `FROM`
        from: Option<FromClause>,
        /// `WHERE` clause
        where_clause: Option<Box<Expr>>,
    },
    /// `INSERT`
    Insert {
        /// `OR`
        or_conflict: Option<ResolveType>,
        /// table name
        tbl_name: Name,
        /// `COLUMNS`
        col_names: Vec<Name>,
        /// `SELECT` or `VALUES`
        select: Select,
        /// `ON CONFLICT` clause
        upsert: Option<Box<Upsert>>,
        /// `RETURNING`
        returning: Vec<ResultColumn>,
    },
    /// `DELETE`
    Delete {
        /// table name
        tbl_name: Name,
        /// `WHERE` clause
        where_clause: Option<Box<Expr>>,
    },
    /// `SELECT`
    Select(Select),
}

/// Conflict resolution types
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResolveType {
    /// `ROLLBACK`
    Rollback,
    /// `ABORT`
    Abort, // default
    /// `FAIL`
    Fail,
    /// `IGNORE`
    Ignore,
    /// `REPLACE`
    Replace,
}

impl ResolveType {
    /// Get the OE_XXX bit value
    pub fn bit_value(&self) -> usize {
        match self {
            ResolveType::Rollback => 1,
            ResolveType::Abort => 2,
            ResolveType::Fail => 3,
            ResolveType::Ignore => 4,
            ResolveType::Replace => 5,
        }
    }
}

/// `WITH` clause
// https://sqlite.org/lang_with.html
// https://sqlite.org/syntax/with-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct With {
    /// `RECURSIVE`
    pub recursive: bool,
    /// CTEs
    pub ctes: Vec<CommonTableExpr>,
}

/// CTE materialization
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Materialized {
    /// No hint
    Any,
    /// `MATERIALIZED`
    Yes,
    /// `NOT MATERIALIZED`
    No,
}

/// CTE
// https://sqlite.org/syntax/common-table-expression.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommonTableExpr {
    /// table name
    pub tbl_name: Name,
    /// table columns
    pub columns: Vec<IndexedColumn>, // check no duplicate
    /// `MATERIALIZED`
    pub materialized: Materialized,
    /// query
    pub select: Select,
}

/// A field in a STRUCT or UNION type declaration, e.g. `x INT` in `STRUCT(x INT, y TEXT)`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TypeField {
    /// field/variant name
    pub name: Name,
    /// field/variant type (recursive — allows nested STRUCT/UNION)
    pub field_type: Type,
}

/// Column type
// https://sqlite.org/syntax/type-name.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Type {
    /// type name
    pub name: String, // TODO Validate: Ids+
    /// type size
    pub size: Option<TypeSize>,
    /// Number of array dimensions: 0 = scalar, 1 = `type[]`, 2 = `type[][]`, etc.
    pub array_dimensions: u32,
}

impl Type {
    /// Returns true when this type has at least one array dimension.
    pub fn is_array(&self) -> bool {
        self.array_dimensions > 0
    }
}

/// Column type size limit(s)
// https://sqlite.org/syntax/type-name.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TypeSize {
    /// maximum size
    MaxSize(Box<Expr>),
    /// precision
    TypeSize(Box<Expr>, Box<Expr>),
}

/// Transaction types
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TransactionType {
    /// `DEFERRED`
    Deferred, // default
    /// `IMMEDIATE`
    Immediate,
    /// `EXCLUSIVE`
    Exclusive,
    /// `CONCURRENT`,
    Concurrent,
}

/// Upsert clause
// https://sqlite.org/lang_upsert.html
// https://sqlite.org/syntax/upsert-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Upsert {
    /// conflict targets
    pub index: Option<UpsertIndex>,
    /// `DO` clause
    pub do_clause: UpsertDo,
    /// next upsert
    pub next: Option<Box<Upsert>>,
}

/// Upsert conflict targets
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UpsertIndex {
    /// columns
    pub targets: Vec<SortedColumn>,
    /// `WHERE` clause
    pub where_clause: Option<Box<Expr>>,
}

/// Upsert `DO` action
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum UpsertDo {
    /// `SET`
    Set {
        /// assignments
        sets: Vec<Set>,
        /// `WHERE` clause
        where_clause: Option<Box<Expr>>,
    },
    /// `NOTHING`
    Nothing,
}

/// Function call tail
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FunctionTail {
    /// `FILTER` clause
    pub filter_clause: Option<Box<Expr>>,
    /// `OVER` clause
    pub over_clause: Option<Over>,
}

/// Function call `OVER` clause
// https://sqlite.org/syntax/over-clause.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Over {
    /// Window definition
    Window(Window),
    /// Window name
    Name(Name),
}

/// `OVER` window definition
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WindowDef {
    /// window name
    pub name: Name,
    /// window definition
    pub window: Window,
}

/// Window definition
// https://sqlite.org/syntax/window-defn.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Window {
    /// base window name
    pub base: Option<Name>,
    /// `PARTITION BY`
    pub partition_by: Vec<Box<Expr>>,
    /// `ORDER BY`
    pub order_by: Vec<SortedColumn>,
    /// frame spec
    pub frame_clause: Option<FrameClause>,
}

/// Frame specification
// https://sqlite.org/syntax/frame-spec.html
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FrameClause {
    /// unit
    pub mode: FrameMode,
    /// start bound
    pub start: FrameBound,
    /// end bound
    pub end: Option<FrameBound>,
    /// `EXCLUDE`
    pub exclude: Option<FrameExclude>,
}

/// Frame modes
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameMode {
    /// `GROUPS`
    Groups,
    /// `RANGE`
    Range,
    /// `ROWS`
    Rows,
}

/// Frame bounds
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `FOLLOWING`
    Following(Box<Expr>),
    /// `PRECEDING`
    Preceding(Box<Expr>),
    /// `UNBOUNDED FOLLOWING`
    UnboundedFollowing,
    /// `UNBOUNDED PRECEDING`
    UnboundedPreceding,
}

/// Frame exclusions
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FrameExclude {
    /// `NO OTHERS`
    NoOthers,
    /// `CURRENT ROW`
    CurrentRow,
    /// `GROUP`
    Group,
    /// `TIES`
    Ties,
}
