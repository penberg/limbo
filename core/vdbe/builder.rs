use crate::{alloc, turso_assert, turso_assert_eq, Result, Value, ValueRef};

use rustc_hash::FxHashMap as HashMap;
use std::ops::Range;
use tracing::{instrument, Level};
use turso_parser::ast::{self, ResolveType, SortOrder, TableInternalId};

use crate::{
    index_method::IndexMethodAttachment,
    parameters::Parameters,
    schema::{BTreeTable, Column, ColumnLayout, Index, PseudoCursorType, Schema, Table, Trigger},
    translate::{
        collate::CollationSeq,
        emitter::{MaterializedColumnRef, TransactionMode},
        plan::{ResultSetColumn, TableReferences},
    },
    Arc, CaptureDataChangesInfo, Connection, VirtualTable,
};

// Keep distinct hash-table ids far from table internal ids to avoid collisions.
const HASH_TABLE_ID_BASE: usize = 1 << 30;

#[derive(Default)]
pub struct TableRefIdCounter {
    next_free: ast::TableInternalId,
}

impl TableRefIdCounter {
    pub fn new() -> Self {
        Self {
            next_free: TableInternalId::default(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> ast::TableInternalId {
        let id = self.next_free;
        self.next_free += 1;
        id
    }
}

use super::{
    affinity::Affinity, explain::ExplainInfo, BranchOffset, CursorID, Insn, InsnReference,
    PrepareContext, PreparedProgram, Program,
};
use crate::translate::eqp::{EqpCteMaterialization, EqpDetail};
use crate::translate::plan::BitSet;
use std::num::NonZeroUsize;

/// A key that uniquely identifies a cursor.
/// The key is a pair of table reference id and index.
/// The index is only provided when the cursor is an index cursor.
#[derive(Debug, Clone)]
pub struct CursorKey {
    /// The table reference that the cursor is associated with.
    /// We cannot use e.g. the table query identifier (e.g. 'users' or 'u')
    /// because it might be ambiguous, e.g. this silly example:
    /// `SELECT * FROM t WHERE EXISTS (SELECT * from t)` <-- two different cursors, which 't' should we use as key?
    ///  TableInternalIds are unique within a program, since there is one id per table reference.
    pub table_reference_id: TableInternalId,
    /// The index, in case of an index cursor.
    /// The combination of table internal id and index is enough to disambiguate.
    pub index: Option<Arc<Index>>,
    /// Whether this cursor is an special case build cursor.
    pub is_build: bool,
}

impl CursorKey {
    pub fn table(table_reference_id: TableInternalId) -> Self {
        Self {
            table_reference_id,
            index: None,
            is_build: false,
        }
    }

    pub fn index(table_reference_id: TableInternalId, index: Arc<Index>) -> Self {
        Self {
            table_reference_id,
            index: Some(index),
            is_build: false,
        }
    }

    /// Create a cursor key for hash join build operations.
    /// This creates a separate cursor from the regular table cursor.
    pub fn hash_build(table_reference_id: TableInternalId) -> Self {
        Self {
            table_reference_id,
            index: None,
            is_build: true,
        }
    }

    pub fn equals(&self, other: &CursorKey) -> bool {
        if self.table_reference_id != other.table_reference_id {
            return false;
        }
        if self.is_build != other.is_build {
            return false;
        }
        match (self.index.as_ref(), other.index.as_ref()) {
            (Some(self_index), Some(other_index)) => self_index.name == other_index.name,
            (None, None) => true,
            _ => false,
        }
    }
}

/// Context for resolving `Expr::Column` that has a `TableInternalId::SELF_TABLE` placeholder.
#[derive(Clone)]
pub enum SelfTableContext {
    ForSelect {
        table_ref_id: TableInternalId,
        referenced_tables: TableReferences,
    },
    ForDML {
        dml_ctx: DmlColumnContext,
        table: Arc<BTreeTable>,
    },
}

#[derive(Clone)]
enum DmlColumnRegisters {
    // Used to compute column registers lazily
    Layout {
        base_reg: usize,
        rowid_reg: usize,
        layout: ColumnLayout,
    },
    Indexed {
        column_regs: Vec<usize>,
    },
}

#[derive(Clone)]
pub struct DmlColumnContext {
    registers: DmlColumnRegisters,
    rowid_alias_col: Option<usize>,
}

impl DmlColumnContext {
    pub fn layout(
        columns: &[Column],
        base_reg: usize,
        rowid_reg: usize,
        layout: ColumnLayout,
    ) -> Self {
        let rowid_alias_col = columns.iter().position(|c| c.is_rowid_alias());

        Self {
            registers: DmlColumnRegisters::Layout {
                base_reg,
                rowid_reg,
                layout,
            },
            rowid_alias_col,
        }
    }

    pub fn from_column_reg_mapping<'a>(pairs: impl Iterator<Item = (&'a Column, usize)>) -> Self {
        let mut rowid_alias_col = None;
        let mut column_regs = Vec::new();
        for (idx, (col, reg)) in pairs.enumerate() {
            column_regs.push(reg);
            if col.is_rowid_alias() {
                rowid_alias_col = Some(idx);
            }
        }
        Self {
            registers: DmlColumnRegisters::Indexed { column_regs },
            rowid_alias_col,
        }
    }

    pub fn to_column_reg(&self, col_idx: usize) -> usize {
        match &self.registers {
            DmlColumnRegisters::Layout {
                base_reg,
                rowid_reg,
                layout,
            } => {
                if self.rowid_alias_col == Some(col_idx) {
                    *rowid_reg
                } else {
                    layout.to_register(*base_reg, col_idx)
                }
            }
            DmlColumnRegisters::Indexed { column_regs } => column_regs[col_idx],
        }
    }
}

enum BuilderQueryMode {
    Normal,
    Explain,
    ExplainQueryPlan {
        format: ast::EqpFormat,
        current_parent_idx: Option<usize>,
    },
}

impl BuilderQueryMode {
    const fn new(query_mode: QueryMode) -> Self {
        match query_mode {
            QueryMode::Normal => Self::Normal,
            QueryMode::Explain => Self::Explain,
            QueryMode::ExplainQueryPlan { format } => Self::ExplainQueryPlan {
                format,
                current_parent_idx: None,
            },
        }
    }

    const fn query_mode(&self) -> QueryMode {
        match self {
            Self::Normal => QueryMode::Normal,
            Self::Explain => QueryMode::Explain,
            Self::ExplainQueryPlan { format, .. } => {
                QueryMode::ExplainQueryPlan { format: *format }
            }
        }
    }

    const fn is_explain_query_plan(&self) -> bool {
        matches!(self, Self::ExplainQueryPlan { .. })
    }
}

pub struct ProgramBuilder {
    /// A span of instructions from (offset_start_inclusive, offset_end_exclusive),
    /// that are deemed to be compile-time constant and can be hoisted out of loops
    /// so that they get evaluated only once at the start of the program.
    pub constant_spans: Vec<(usize, usize)>,
    /// Cursors that are referenced by the program. Indexed by [CursorKey].
    /// Certain types of cursors do not need a [CursorKey] (e.g. temp tables, sorter),
    /// because they never need to use [ProgramBuilder::resolve_cursor_id] to find it
    /// again. Hence, the key is optional.
    pub cursor_ref: Vec<(Option<CursorKey>, CursorType)>,
    /// A vector where index=label number, value=resolved offset. Resolved in build().
    /// For each allocated label, the offset of the instruction emitted *just
    /// before* the label's logical "next-insn" anchor. The label resolves to
    /// `anchor_offset + 1` so it tracks whichever instruction ends up at that
    /// position, even after `emit_constant_insns` reorders the program.
    label_to_resolved_offset: Vec<Option<InsnReference>>,
    explain: ExplainInfo,
    pub parameters: Parameters,
    pub result_columns: Vec<ResultSetColumn>,
    /// Instruction, the function to execute it with, and its original index in the vector.
    pub insns: Vec<(Insn, usize)>,
    /// Registry of materialized CTEs, keyed by cte_id.
    /// Used to share materialized data across multiple CTE references via OpenDup.
    materialized_ctes: HashMap<usize, MaterializedCteInfo>,
    /// Stack of CTE names currently being planned. Used to detect circular
    /// references in non-recursive CTEs and to prevent fallthrough to schema
    /// resolution for same-named tables/views.
    ctes_being_defined: Vec<String>,
    /// If this ProgramBuilder is building trigger subprogram, a ref to the trigger is stored here.
    pub trigger: Option<Arc<Trigger>>,
    pub table_reference_counter: TableRefIdCounter,
    /// Curr collation sequence. Bool indicates whether it was set by a COLLATE expr
    collation: Option<(CollationSeq, bool)>,
    capture_data_changes_info: Option<CaptureDataChangesInfo>,
    /// Whether the main database uses MVCC journal mode, set once at translation time from the connection.
    mvcc_enabled: bool,
    // TODO: when we support multiple dbs, this should be a write mask to track which DBs need to be written
    txn_mode: TransactionMode,
    /// Set of database IDs that need write transactions (for attached databases).
    write_databases: BitSet,
    /// Set of attached database IDs that need read transactions.
    read_databases: BitSet,
    /// Schema cookies for attached databases at prepare time.
    write_database_cookies: HashMap<usize, u32>,
    /// Schema cookies for attached databases opened for reading.
    read_database_cookies: HashMap<usize, u32>,
    /// Temporary cursor overrides maps table internal IDs to cursor IDs that should be used instead of the normal resolution.
    /// This allows for things like hash build to use a separate cursor for iterating the same table.
    cursor_overrides: HashMap<usize, CursorID>,
    /// Maps identifier names to registers for custom type encode/decode expressions.
    /// When set, `Expr::Id("value")` resolves to the register holding the input value,
    /// and type parameter names resolve to registers holding their concrete values.
    pub id_register_overrides: HashMap<String, usize>,
    /// Hash join build signatures keyed by hash table id.
    hash_build_signatures: HashMap<usize, HashBuildSignature>,
    /// Hash tables to keep open across subplans (e.g. materialization).
    hash_tables_to_keep_open: BitSet,
    /// Maps table internal_id to result_columns_start_reg for FROM clause subqueries.
    /// Used when nested subqueries need to reference columns from outer query subqueries.
    subquery_result_regs: HashMap<TableInternalId, usize>,
    /// The mode in which the query is being executed.
    mode: BuilderQueryMode,
    pub flags: ProgramBuilderFlags,
    /// True once any `Insn::Function` has been emitted. See [`Self::may_abort`].
    emitted_function_call: bool,
    next_free_register: usize,
    free_register_ranges: Vec<Range<usize>>,
    next_free_cursor_id: usize,
    free_cursor_ids: Vec<usize>,
    next_hash_table_id: usize,
    pub table_references: TableReferences,
    /// Current parsing nesting level
    nested_level: usize,
    init_label: BranchOffset,
    start_offset: BranchOffset,
    pub(crate) reg_result_cols_start: Option<usize>,
    pub resolve_type: ResolveType,
    /// When set, all triggers fired from this program should use this conflict resolution.
    /// This is used in UPSERT DO UPDATE context to ensure nested trigger's OR IGNORE/REPLACE
    /// clauses don't suppress errors.
    pub trigger_conflict_override: Option<ResolveType>,
    /// Counter for CTE identity tracking. Each CTE definition gets a unique ID
    /// so that multiple references to the same CTE can share materialized data.
    next_cte_id: usize,
    /// Counter for subquery numbering in EXPLAIN QUERY PLAN output.
    next_subquery_eqp_id: usize,
    /// Write-context for union-typed columns: tells `union_value('tag', val)`
    /// which union TypeDef to resolve the tag against.
    ///
    /// Unlike read-path functions (`union_tag(col)`, `union_extract(col, 'tag')`)
    /// which resolve the union type from the column expression they operate on,
    /// `union_value()` constructs a *new* value — the SQL syntax doesn't reference
    /// the target column, so the type must come from the INSERT/UPDATE/UPSERT context.
    ///
    /// This follows the same save/restore pattern as `id_register_overrides`
    /// (ENCODE/DECODE context).
    /// Callers must save with `.take()`, set the new value, translate the expression,
    /// then restore the saved value. For nested unions (union-in-union), the
    /// `UnionValueFunc` handler in expr.rs saves/restores this to the inner union
    /// type before translating the value argument.
    pub(crate) target_union_type: Option<Arc<crate::schema::TypeDef>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct ProgramBuilderFlags(u8);

impl ProgramBuilderFlags {
    const ROLLBACK: u8 = 1 << 0;
    const IS_MULTI_WRITE: u8 = 1 << 1;
    const MAY_ABORT: u8 = 1 << 2;
    const READONLY: u8 = 1 << 3;
    const IS_SUBPROGRAM: u8 = 1 << 4;
    const HAS_STATEMENT_CONFLICT: u8 = 1 << 5;
    const SUPPRESS_CUSTOM_TYPE_DECODE: u8 = 1 << 6;
    const SUPPRESS_COLUMN_DEFAULT: u8 = 1 << 7;

    const fn new(is_subprogram: bool) -> Self {
        let mut new = Self(0);
        new.set_is_multi_write(true);
        new.set_may_abort(true);
        new.set_readonly(true);
        new.set_is_subprogram(is_subprogram);
        new.set_is_multi_write(true);
        new.set_may_abort(true);
        new
    }

    #[inline]
    const fn get(self, bit: u8) -> bool {
        (self.0 & bit) != 0
    }

    #[inline]
    const fn set(&mut self, bit: u8, value: bool) {
        if value {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }

    #[inline]
    pub const fn rollback(self) -> bool {
        self.get(Self::ROLLBACK)
    }
    #[inline]
    pub const fn set_rollback(&mut self, v: bool) {
        self.set(Self::ROLLBACK, v)
    }

    #[inline]
    /// Mirrors SQLite's isMultiWrite: true if the statement may modify/insert multiple rows.
    /// If a non-autocommit transaction can modify multiple rows, statement subjournaling is always
    /// required for proper cleanup on abort. If only one row can be modified, then journaling is not
    /// necessary because on abort there is nothing to clean up.
    /// Defaults to true for safety; specific translate paths (e.g., single-row INSERT) set false.
    pub const fn is_multi_write(self) -> bool {
        self.get(Self::IS_MULTI_WRITE)
    }
    #[inline]
    pub const fn set_is_multi_write(&mut self, v: bool) {
        self.set(Self::IS_MULTI_WRITE, v)
    }

    #[inline]
    /// Mirrors SQLite's mayAbort: true if the statement may throw an ABORT exception.
    /// This flag is used in combination with is_multi_write to determine if statement subjournaling is required.
    /// Defaults to true for safety; specific translate paths (e.g., INSERT with no constraints) set false.
    pub const fn may_abort(self) -> bool {
        self.get(Self::MAY_ABORT)
    }
    #[inline]
    pub const fn set_may_abort(&mut self, v: bool) {
        self.set(Self::MAY_ABORT, v)
    }

    #[inline]
    /// True until the builder emits an opcode that may directly modify persistent
    /// database contents, mirroring sqlite3_stmt_readonly() classification over
    /// compiled bytecode.
    pub const fn readonly(self) -> bool {
        self.get(Self::READONLY)
    }
    #[inline]
    pub const fn set_readonly(&mut self, v: bool) {
        self.set(Self::READONLY, v)
    }

    #[inline]
    /// Whether this is a subprogram (trigger or FK action). Subprograms skip Transaction instructions.
    pub const fn is_subprogram(self) -> bool {
        self.get(Self::IS_SUBPROGRAM)
    }
    #[inline]
    pub const fn set_is_subprogram(&mut self, v: bool) {
        self.set(Self::IS_SUBPROGRAM, v)
    }

    #[inline]
    /// Whether the resolve_type was explicitly set from a statement-level OR clause.
    /// When false, per-constraint ON CONFLICT clauses from CREATE TABLE should be used.
    pub fn has_statement_conflict(self) -> bool {
        self.get(Self::HAS_STATEMENT_CONFLICT)
    }
    #[inline]
    pub fn set_has_statement_conflict(&mut self, v: bool) {
        self.set(Self::HAS_STATEMENT_CONFLICT, v)
    }

    #[inline]
    /// When set, translate_expr will skip custom type decode for Expr::Column.
    /// This is used when building ORDER BY sort keys so the sorter compares
    /// encoded (on-disk) values. Decode is presentation-only.
    pub const fn suppress_custom_type_decode(self) -> bool {
        self.get(Self::SUPPRESS_CUSTOM_TYPE_DECODE)
    }
    #[inline]
    pub const fn set_suppress_custom_type_decode(&mut self, v: bool) {
        self.set(Self::SUPPRESS_CUSTOM_TYPE_DECODE, v)
    }

    #[inline]
    /// When true, the next `emit_column` call will not bake the default value
    /// into the Column instruction. Used for custom type columns where the default
    /// needs to be encoded before use.
    pub const fn suppress_column_default(self) -> bool {
        self.get(Self::SUPPRESS_COLUMN_DEFAULT)
    }
    #[inline]
    pub const fn set_suppress_column_default(&mut self, v: bool) {
        self.set(Self::SUPPRESS_COLUMN_DEFAULT, v)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MaterializedBuildInputModeTag {
    RowidOnly,
    Payload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Signature of a hash build to allow reuse when inputs are unchanged.
/// TODO: this is very heavy... we might consider hashing instead of storing full data.
pub struct HashBuildSignature {
    /// WHERE term indices used as hash join keys.
    pub join_key_indices: Vec<usize>,
    /// Build-table columns stored as payload.
    pub payload_refs: Vec<MaterializedColumnRef>,
    /// Affinity string applied to join keys.
    pub key_affinities: String,
    /// Whether a bloom filter is enabled for this build.
    pub use_bloom_filter: bool,
    /// Rowid input cursor when the build side is materialized.
    pub materialized_input_cursor: Option<CursorID>,
    /// RowidOnly vs KeyPayload
    pub materialized_mode: Option<MaterializedBuildInputModeTag>,
}

/// Information about a materialized CTE, used for sharing data across multiple references.
#[derive(Debug, Clone)]
pub struct MaterializedCteInfo {
    /// The ephemeral table cursor holding materialized CTE data.
    pub cursor_id: CursorID,
    /// The table definition, needed for allocating dup cursors with the same CursorType.
    pub table: Arc<BTreeTable>,
    /// Number of result columns.
    pub num_columns: usize,
}

#[derive(Debug, Clone)]
pub enum CursorType {
    BTreeTable(Arc<BTreeTable>),
    BTreeIndex(Arc<Index>),
    IndexMethod(Arc<dyn IndexMethodAttachment>),
    Pseudo(PseudoCursorType),
    Sorter,
    VirtualTable(Arc<VirtualTable>),
    MaterializedView(
        Arc<BTreeTable>,
        Arc<crate::sync::Mutex<crate::incremental::view::IncrementalView>>,
    ),
}

impl CursorType {
    pub const fn is_index(&self) -> bool {
        matches!(self, CursorType::BTreeIndex(_))
    }

    pub fn get_explain_description(&self) -> String {
        let out = match self {
            CursorType::BTreeTable(btree_table) => {
                let mut col_count = btree_table.columns().len();
                if btree_table.get_rowid_alias_column().is_none() {
                    col_count += 1;
                }
                Some((
                    col_count,
                    btree_table
                        .columns()
                        .iter()
                        .map(|col| {
                            if let Some(coll) = col.collation_opt() {
                                format!("{coll}")
                            } else {
                                "B".to_string()
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                ))
            }
            CursorType::BTreeIndex(index) => {
                let mut col_count = index.columns.len();
                if index.has_rowid {
                    col_count += 1;
                }
                Some((
                    col_count,
                    index
                        .columns
                        .iter()
                        .map(|col| {
                            let sign = match col.order {
                                SortOrder::Asc => "",
                                SortOrder::Desc => "-",
                            };
                            if let Some(coll) = col.collation {
                                format!("{sign}{coll}")
                            } else {
                                format!("{sign}B")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(","),
                ))
            }
            _ => None,
        };

        out.map_or(String::new(), |(col_count, collations)| {
            format!("k({col_count},{collations})")
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum QueryMode {
    Normal,
    Explain,
    ExplainQueryPlan { format: ast::EqpFormat },
}

impl QueryMode {
    pub const fn new(cmd: &ast::Cmd) -> Self {
        match cmd {
            ast::Cmd::ExplainQueryPlan { format, .. } => {
                QueryMode::ExplainQueryPlan { format: *format }
            }
            ast::Cmd::Explain(_) => QueryMode::Explain,
            ast::Cmd::Stmt(_) => QueryMode::Normal,
        }
    }
}

pub struct ProgramBuilderOpts {
    pub num_cursors: usize,
    pub approx_num_insns: usize,
    pub approx_num_labels: usize,
}

impl ProgramBuilderOpts {
    pub const fn new(
        num_cursors: usize,
        approx_num_insns: usize,
        approx_num_labels: usize,
    ) -> Self {
        Self {
            num_cursors,
            approx_num_insns,
            approx_num_labels,
        }
    }
}

/// Use this macro to emit an OP_Explain instruction.
/// Please use this macro instead of calling emit_explain() directly,
/// because we want to avoid building the [EqpDetail] if we are not in explain mode.
#[macro_export]
macro_rules! emit_explain {
    ($builder:expr, $push:expr, $detail:expr) => {
        if let $crate::QueryMode::ExplainQueryPlan { .. } = $builder.get_query_mode() {
            $builder.emit_explain_should_not_be_called_directly($push, $detail);
        }
    };
}

impl ProgramBuilder {
    /// Register an `ast::Variable` in the parameter list. Returns the
    /// `NonZeroUsize` index for use in `Insn::Variable`.
    pub fn register_variable(&mut self, variable: &ast::Variable) -> NonZeroUsize {
        let index = usize::try_from(variable.index.get())
            .expect("u32 variable index must fit into usize")
            .try_into()
            .expect("variable index must be non-zero");
        if let Some(name) = variable.name.as_deref() {
            self.parameters.push_named_at(name, index);
        } else if variable.numbered {
            self.parameters.push_numbered(index);
        } else {
            self.parameters.push_index(index);
        }
        index
    }

    /// Run a nested emission scope without leaking its result-column register base
    /// into the surrounding builder state.
    pub fn with_scoped_result_cols_start<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> crate::Result<T>,
    ) -> crate::Result<T> {
        let saved = self.reg_result_cols_start;
        let result = f(self);
        self.reg_result_cols_start = saved;
        result
    }

    pub fn new(
        query_mode: QueryMode,
        capture_data_changes_info: Option<CaptureDataChangesInfo>,
        opts: ProgramBuilderOpts,
    ) -> Self {
        ProgramBuilder::_new(query_mode, capture_data_changes_info, opts, None, false)
    }
    pub fn new_for_trigger(
        query_mode: QueryMode,
        capture_data_changes_info: Option<CaptureDataChangesInfo>,
        opts: ProgramBuilderOpts,
        trigger: Arc<Trigger>,
    ) -> Self {
        ProgramBuilder::_new(
            query_mode,
            capture_data_changes_info,
            opts,
            Some(trigger),
            true,
        )
    }
    /// Create a ProgramBuilder for a subprogram (FK actions, etc.) that runs within
    /// an existing transaction and doesn't emit Transaction instructions.
    pub fn new_for_subprogram(
        query_mode: QueryMode,
        capture_data_changes_info: Option<CaptureDataChangesInfo>,
        opts: ProgramBuilderOpts,
    ) -> Self {
        ProgramBuilder::_new(query_mode, capture_data_changes_info, opts, None, true)
    }

    #[turso_macros::trace_stack]
    fn _new(
        query_mode: QueryMode,
        capture_data_changes_info: Option<CaptureDataChangesInfo>,
        opts: ProgramBuilderOpts,
        trigger: Option<Arc<Trigger>>,
        is_subprogram: bool,
    ) -> Self {
        Self {
            table_reference_counter: TableRefIdCounter::new(),
            next_free_register: 1,
            free_register_ranges: Vec::new(),
            next_free_cursor_id: 0,
            free_cursor_ids: Vec::new(),
            next_hash_table_id: HASH_TABLE_ID_BASE,
            insns: Vec::with_capacity(opts.approx_num_insns),
            cursor_ref: Vec::with_capacity(opts.num_cursors),
            constant_spans: Vec::new(),
            label_to_resolved_offset: Vec::with_capacity(opts.approx_num_labels),
            explain: ExplainInfo::default(),
            parameters: Parameters::new(),
            result_columns: Vec::new(),
            table_references: TableReferences::new(vec![], vec![]),
            collation: None,
            nested_level: 0,
            // These labels will be filled when `prologue()` is called
            init_label: BranchOffset::Placeholder,
            start_offset: BranchOffset::Placeholder,
            capture_data_changes_info,
            mvcc_enabled: false,
            txn_mode: TransactionMode::None,
            write_databases: BitSet::default(),
            read_databases: BitSet::default(),
            write_database_cookies: HashMap::default(),
            read_database_cookies: HashMap::default(),
            mode: BuilderQueryMode::new(query_mode),
            reg_result_cols_start: None,
            flags: ProgramBuilderFlags::new(is_subprogram),
            emitted_function_call: false,
            trigger,
            resolve_type: ResolveType::Abort,
            trigger_conflict_override: None,
            cursor_overrides: HashMap::default(),
            id_register_overrides: HashMap::default(),
            hash_build_signatures: HashMap::default(),
            hash_tables_to_keep_open: BitSet::default(),
            subquery_result_regs: HashMap::default(),
            next_cte_id: 0,
            materialized_ctes: HashMap::default(),
            ctes_being_defined: Vec::new(),
            next_subquery_eqp_id: 1,
            target_union_type: None,
        }
    }

    pub const fn next_subquery_eqp_id(&mut self) -> usize {
        let id = self.next_subquery_eqp_id;
        self.next_subquery_eqp_id += 1;
        id
    }

    pub const fn alloc_hash_table_id(&mut self) -> usize {
        let id = self.next_hash_table_id;
        self.next_hash_table_id = self
            .next_hash_table_id
            .checked_add(1)
            .expect("hash table id overflow");
        id
    }

    /// Allocate a unique CTE identity. Each CTE definition in a query gets a unique ID
    /// so that multiple references to the same CTE can share materialized data via OpenDup.
    pub const fn alloc_cte_id(&mut self) -> usize {
        let id = self.next_cte_id;
        self.next_cte_id += 1;
        id
    }

    /// Check if a CTE has already been materialized.
    /// Returns the materialization info if the CTE cursor can be shared via OpenDup.
    pub fn get_materialized_cte(&self, cte_id: usize) -> Option<&MaterializedCteInfo> {
        self.materialized_ctes.get(&cte_id)
    }

    /// Register a materialized CTE so that subsequent references can share it via OpenDup.
    pub fn register_materialized_cte(&mut self, cte_id: usize, info: MaterializedCteInfo) {
        self.materialized_ctes.insert(cte_id, info);
    }

    /// Mark a CTE name as currently being planned. While on the stack,
    /// `parse_table` will reject references to this name with "circular
    /// reference" instead of falling through to schema resolution.
    pub fn push_cte_being_defined(&mut self, name: String) {
        self.ctes_being_defined.push(name);
    }

    /// Remove the most recently pushed CTE name after planning completes.
    pub fn pop_cte_being_defined(&mut self) {
        self.ctes_being_defined.pop();
    }

    /// Check whether a name refers to a CTE currently being planned.
    pub fn is_cte_being_defined(&self, name: &str) -> bool {
        self.ctes_being_defined.iter().any(|n| n == name)
    }

    /// Hide CTEs being defined whose names an inner WITH clause redefines:
    /// the inner definitions shadow the outer names for that lexical scope,
    /// so references to them are not circular. Returns the hidden names for
    /// [Self::unmask_shadowed_ctes_being_defined].
    pub fn mask_shadowed_ctes_being_defined(&mut self, shadowing_names: &[String]) -> Vec<String> {
        let mut masked = Vec::new();
        self.ctes_being_defined.retain(|name| {
            if shadowing_names.contains(name) {
                masked.push(name.clone());
                false
            } else {
                true
            }
        });
        masked
    }

    /// Restore names hidden by [Self::mask_shadowed_ctes_being_defined] when
    /// their shadowing scope ends. Membership is all that matters for the
    /// circular-reference check, so restore order is irrelevant.
    pub fn unmask_shadowed_ctes_being_defined(&mut self, masked: Vec<String>) {
        self.ctes_being_defined.extend(masked);
    }

    /// Temporarily take the CTE-being-defined stack (e.g. during view
    /// expansion, which should not see CTE context from the caller).
    pub fn take_ctes_being_defined(&mut self) -> Vec<String> {
        std::mem::take(&mut self.ctes_being_defined)
    }

    /// Restore the CTE-being-defined stack after a context-isolated expansion.
    pub fn restore_ctes_being_defined(&mut self, saved: Vec<String>) {
        self.ctes_being_defined = saved;
    }

    pub const fn set_resolve_type(&mut self, resolve_type: ResolveType) {
        self.resolve_type = resolve_type;
    }

    /// Set the trigger conflict override. When set, all triggers fired from this program
    /// should use this conflict resolution instead of their own OR clauses.
    pub const fn set_trigger_conflict_override(&mut self, resolve_type: ResolveType) {
        self.trigger_conflict_override = Some(resolve_type);
    }

    /// Returns true if the given hash table id should be kept open across subplans.
    pub fn should_keep_hash_table_open(&self, hash_table_id: usize) -> bool {
        self.hash_tables_to_keep_open.get(hash_table_id)
    }

    /// Set the set of hash tables to keep open across subplans.
    pub fn set_hash_tables_to_keep_open(&mut self, tables: &BitSet) {
        self.hash_tables_to_keep_open.clone_from(tables);
    }

    /// Reset the set of hash tables to keep open.
    pub fn clear_hash_tables_to_keep_open(&mut self) {
        self.hash_tables_to_keep_open = BitSet::default();
    }

    /// Returns true if the given hash build signature matches the recorded one for the given hash table id.
    pub fn hash_build_signature_matches(
        &self,
        hash_table_id: usize,
        signature: &HashBuildSignature,
    ) -> bool {
        self.hash_build_signatures
            .get(&hash_table_id)
            .is_some_and(|existing| existing == signature)
    }

    /// Returns true if there is a recorded hash build signature for the given hash table id.
    pub fn has_hash_build_signature(&self, hash_table_id: usize) -> bool {
        self.hash_build_signatures.contains_key(&hash_table_id)
    }

    /// Insert or update the hash build signature for the given hash table id.
    pub fn record_hash_build_signature(
        &mut self,
        hash_table_id: usize,
        signature: HashBuildSignature,
    ) {
        self.hash_build_signatures.insert(hash_table_id, signature);
    }

    /// Clear the hash build signature for the given hash table id.
    pub fn clear_hash_build_signature(&mut self, hash_table_id: usize) {
        self.hash_build_signatures.remove(&hash_table_id);
    }

    /// Store the result_columns_start_reg for a FROM clause subquery by its internal_id.
    /// Used so nested subqueries can access columns from outer query subqueries.
    pub fn set_subquery_result_reg(&mut self, internal_id: TableInternalId, result_reg: usize) {
        self.subquery_result_regs.insert(internal_id, result_reg);
    }

    /// Look up the result_columns_start_reg for a FROM clause subquery by its internal_id.
    /// Returns None if the subquery hasn't been emitted yet.
    pub fn get_subquery_result_reg(&self, internal_id: TableInternalId) -> Option<usize> {
        self.subquery_result_regs.get(&internal_id).copied()
    }

    /// Mark that this statement may modify/insert multiple rows (mirrors SQLite's sqlite3MultiWrite).
    /// When false, statement journals are skipped since single-write statements are atomic.
    pub const fn set_multi_write(&mut self, is_multi_write: bool) {
        self.flags.set_is_multi_write(is_multi_write);
    }

    /// Mark that this statement may throw an ABORT exception (mirrors SQLite's sqlite3MayAbort).
    pub const fn set_may_abort(&mut self, may_abort: bool) {
        self.flags.set_may_abort(may_abort);
    }

    /// True if this statement may throw an ABORT exception. Combines the
    /// translate paths' constraint analysis with emission taint: any emitted
    /// function call can raise at runtime, like SQLite's
    /// sqlite3VdbeAddFunctionCall() → sqlite3MayAbort(). The taint is a
    /// separate monotonic bit (not folded into the flag) because the analysis
    /// assigns the flag mid-translation and would clobber it.
    pub const fn may_abort(&self) -> bool {
        self.flags.may_abort() || self.emitted_function_call
    }

    pub const fn capture_data_changes_info(&self) -> &Option<CaptureDataChangesInfo> {
        &self.capture_data_changes_info
    }

    /// Whether the main database uses MVCC journal mode. See [`Self::mvcc_enabled`].
    pub const fn is_mvcc_enabled(&self) -> bool {
        self.mvcc_enabled
    }

    pub fn set_mvcc_enabled(&mut self, enabled: bool) {
        self.mvcc_enabled = enabled;
    }

    pub fn extend(&mut self, opts: &ProgramBuilderOpts) {
        self.insns.reserve(opts.approx_num_insns);
        self.cursor_ref.reserve(opts.num_cursors);
        self.label_to_resolved_offset
            .reserve(opts.approx_num_labels);
    }

    /// Start a new constant span. The next instruction to be emitted will be the first
    /// instruction in the span.
    pub fn constant_span_start(&mut self) -> usize {
        let span = self.constant_spans.len();
        let start = self.insns.len();
        self.constant_spans.push((start, usize::MAX));
        span
    }

    /// End the current constant span. The last instruction that was emitted is the last
    /// instruction in the span.
    pub fn constant_span_end(&mut self, span_idx: usize) {
        let span = &mut self.constant_spans[span_idx];
        if span.1 == usize::MAX {
            span.1 = self.insns.len().saturating_sub(1);
        }
    }

    /// End all constant spans that are currently open. This is used to handle edge cases
    /// where we think a parent expression is constant, but we decide during the evaluation
    /// of one of its children that it is not.
    pub fn constant_span_end_all(&mut self) {
        for span in self.constant_spans.iter_mut() {
            if span.1 == usize::MAX {
                span.1 = self.insns.len().saturating_sub(1);
            }
        }
    }

    /// Check if there is a constant span that is currently open.
    pub fn constant_span_is_open(&self) -> bool {
        self.constant_spans
            .last()
            .is_some_and(|(_, end)| *end == usize::MAX)
    }

    /// Get the index of the next constant span.
    /// Used in [crate::translate::expr::translate_expr_no_constant_opt()] to invalidate
    /// all constant spans after the given index.
    pub const fn constant_spans_next_idx(&self) -> usize {
        self.constant_spans.len()
    }

    /// Invalidate all constant spans after the given index. This is used when we want to
    /// be sure that constant optimization is never used for translating a given expression.
    /// See [crate::translate::expr::translate_expr_no_constant_opt()] for more details.
    pub fn constant_spans_invalidate_after(&mut self, idx: usize) {
        self.constant_spans.truncate(idx);
    }

    pub fn alloc_register(&mut self) -> usize {
        self.alloc_registers(1)
    }

    pub fn alloc_registers(&mut self, amount: usize) -> usize {
        if amount == 0 {
            return self.next_free_register;
        }
        if let Some(index) = self
            .free_register_ranges
            .iter()
            .position(|range| range.len() >= amount)
        {
            let reg = self.free_register_ranges[index].start;
            self.free_register_ranges[index].start += amount;
            if self.free_register_ranges[index].is_empty() {
                self.free_register_ranges.remove(index);
            }
            return reg;
        }
        let reg = self.next_free_register;
        self.next_free_register += amount;
        reg
    }

    /// Allow later code to use registers that a removed plan had reserved.
    pub(crate) fn release_registers(&mut self, start: usize, amount: usize) {
        if amount == 0 {
            return;
        }
        let end = start.checked_add(amount).expect("register range overflow");
        turso_assert!(start > 0 && end <= self.next_free_register);
        self.free_register_ranges.push(start..end);
        self.free_register_ranges
            .sort_unstable_by_key(|range| range.start);

        let mut merged: Vec<Range<usize>> = Vec::with_capacity(self.free_register_ranges.len());
        for range in self.free_register_ranges.drain(..) {
            if let Some(last) = merged.last_mut() {
                turso_assert!(last.end <= range.start, "register range released twice");
                if last.end == range.start {
                    last.end = range.end;
                    continue;
                }
            }
            merged.push(range);
        }
        self.free_register_ranges = merged;

        while self
            .free_register_ranges
            .last()
            .is_some_and(|range| range.end == self.next_free_register)
        {
            let range = self
                .free_register_ranges
                .pop()
                .expect("last register range was present");
            self.next_free_register = range.start;
        }
    }

    /// Returns the next register that will be allocated by alloc_register/alloc_registers.
    pub const fn peek_next_register(&self) -> usize {
        self.next_free_register
    }

    pub fn alloc_registers_and_init_w_null(&mut self, amount: usize) -> usize {
        let reg = self.alloc_registers(amount);
        self.emit_insn(Insn::Null {
            dest: reg,
            dest_end: if amount == 1 {
                None
            } else {
                Some(reg + amount - 1)
            },
        });
        reg
    }

    pub fn alloc_cursor_id_keyed(&mut self, key: CursorKey, cursor_type: CursorType) -> usize {
        turso_assert!(
            !self
                .cursor_ref
                .iter()
                .any(|(k, _)| k.as_ref().is_some_and(|k| k.equals(&key))),
            "duplicate cursor key"
        );
        self._alloc_cursor_id(Some(key), cursor_type)
    }

    pub fn alloc_cursor_id_keyed_if_not_exists(
        &mut self,
        key: CursorKey,
        cursor_type: CursorType,
    ) -> usize {
        if let Some(cursor_id) = self.resolve_cursor_id_safe(&key) {
            cursor_id
        } else {
            self._alloc_cursor_id(Some(key), cursor_type)
        }
    }

    /// allocate proper cursor for the given index (either [CursorType::BTreeIndex] or [CursorType::IndexMethod])
    pub fn alloc_cursor_index(
        &mut self,
        key: Option<CursorKey>,
        index: &Arc<Index>,
    ) -> crate::Result<usize> {
        tracing::debug!("alloc cursor: {:?} {:?}", key, index.index_method.is_some());
        let module = index.index_method.as_ref();
        if let Some(m) = module {
            if !m.definition().backing_btree {
                return Ok(self._alloc_cursor_id(key, CursorType::IndexMethod(m.clone())));
            }
        }
        Ok(self._alloc_cursor_id(key, CursorType::BTreeIndex(index.clone())))
    }

    pub fn alloc_cursor_index_if_not_exists(
        &mut self,
        key: CursorKey,
        index: &Arc<Index>,
    ) -> crate::Result<usize> {
        if let Some(cursor_id) = self.resolve_cursor_id_safe(&key) {
            Ok(cursor_id)
        } else {
            self.alloc_cursor_index(Some(key), index)
        }
    }

    pub fn alloc_cursor_id(&mut self, cursor_type: CursorType) -> usize {
        self._alloc_cursor_id(None, cursor_type)
    }

    fn _alloc_cursor_id(&mut self, key: Option<CursorKey>, cursor_type: CursorType) -> usize {
        if let Some(cursor) = self.free_cursor_ids.pop() {
            self.cursor_ref[cursor] = (key, cursor_type);
            return cursor;
        }
        let cursor = self.next_free_cursor_id;
        self.next_free_cursor_id += 1;
        self.cursor_ref.push((key, cursor_type));
        turso_assert_eq!(self.cursor_ref.len(), self.next_free_cursor_id);
        cursor
    }

    /// Allow later code to use a cursor that a removed plan had reserved.
    pub(crate) fn release_cursor_id(&mut self, cursor_id: usize) {
        turso_assert!(cursor_id < self.next_free_cursor_id);
        turso_assert!(!self.free_cursor_ids.contains(&cursor_id));

        if cursor_id + 1 == self.next_free_cursor_id {
            self.cursor_ref.pop();
            self.next_free_cursor_id -= 1;
            while let Some(index) = self
                .free_cursor_ids
                .iter()
                .position(|id| *id + 1 == self.next_free_cursor_id)
            {
                self.free_cursor_ids.remove(index);
                self.cursor_ref.pop();
                self.next_free_cursor_id -= 1;
            }
            return;
        }

        self.free_cursor_ids.push(cursor_id);
        self.free_cursor_ids
            .sort_unstable_by(|left, right| right.cmp(left));
    }

    pub fn add_pragma_result_column(&mut self, col_name: String) {
        // TODO figure out a better type definition for ResultSetColumn
        // or invent another way to set pragma result columns
        let expr = ast::Expr::Id(ast::Name::empty());
        self.result_columns.push(ResultSetColumn {
            expr,
            alias: Some(col_name),
            implicit_column_name: None,
            contains_aggregates: false,
        });
    }

    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn emit_insn(&mut self, insn: Insn) {
        // This seemingly empty trace here is needed so that a function span is emmited with it
        tracing::trace!("");
        self.flags
            .set_readonly(self.flags.readonly() & insn.is_readonly());
        // Any function can raise at runtime; see Self::may_abort.
        if matches!(insn, Insn::Function { .. }) {
            self.emitted_function_call = true;
        }
        if let Insn::Column {
            cursor_id,
            column,
            dest,
            default,
        } = insn
        {
            self.emit_column_maybe_fused(cursor_id, column, dest, default);
            return;
        }
        self.insns.push((insn, self.insns.len()));
    }

    /// Emits a `Column` or `ColumnRange` opcode, fusing it into an immediately preceding `Column`
    /// or `ColumnRange` on the same cursor when both the column index and the destination register
    /// are exactly consecutive and no label is set to target the current opcode.
    fn emit_column_maybe_fused(
        &mut self,
        cursor_id: CursorID,
        column: usize,
        dest: usize,
        default: Option<Value>,
    ) {
        if self.column_run_fusable(cursor_id) && !self.label_targets_next_insn() {
            if let Some((prev_insn, _)) = self.insns.last_mut() {
                match prev_insn {
                    Insn::Column {
                        cursor_id: prev_cursor,
                        column: prev_column,
                        dest: prev_dest,
                        default: prev_default,
                    } if *prev_cursor == cursor_id
                        && column == *prev_column + 1
                        && dest == *prev_dest + 1 =>
                    {
                        let defaults = vec![prev_default.take(), default];
                        *prev_insn = Insn::ColumnRange {
                            cursor_id,
                            start_column: *prev_column,
                            dest: *prev_dest,
                            defaults,
                        };
                        return;
                    }
                    Insn::ColumnRange {
                        cursor_id: prev_cursor,
                        start_column,
                        dest: prev_dest,
                        defaults,
                    } if *prev_cursor == cursor_id
                        && column == *start_column + defaults.len()
                        && dest == *prev_dest + defaults.len() =>
                    {
                        defaults.push(default);
                        return;
                    }
                    _ => {}
                }
            }
        }

        self.insns.push((
            Insn::Column {
                cursor_id,
                column,
                dest,
                default,
            },
            self.insns.len(),
        ));
    }

    fn column_run_fusable(&self, cursor_id: CursorID) -> bool {
        self.cursor_ref
            .get(cursor_id)
            .map(|(_, cursor_type)| cursor_type.accepts_column_range_fusing())
            .unwrap_or(false)
    }

    fn label_targets_next_insn(&self) -> bool {
        let Some(last) = self.insns.len().checked_sub(1) else {
            return false;
        };
        let last = last as u32;
        self.label_to_resolved_offset.contains(&Some(last))
    }

    /// Emit an instruction that should not start or extend a constant span on its own.
    /// If a parent constant span is already open, the instruction is emitted normally
    /// within that span (the parent's `is_constant` classification takes precedence).
    #[instrument(skip(self), level = Level::DEBUG)]
    pub fn emit_no_constant_insn(&mut self, insn: Insn) {
        if !self.constant_span_is_open() {
            self.constant_span_end_all();
        }
        self.emit_insn(insn);
    }

    pub fn close_cursors(&mut self, cursors: &[CursorID]) {
        for cursor in cursors {
            self.emit_insn(Insn::Close { cursor_id: *cursor });
        }
    }

    pub fn emit_string8(&mut self, value: String, dest: usize) {
        self.emit_insn(Insn::String8 { value, dest });
    }

    pub fn emit_string8_new_reg(&mut self, value: String) -> usize {
        let dest = self.alloc_register();
        self.emit_insn(Insn::String8 { value, dest });
        dest
    }

    pub fn emit_int(&mut self, value: i64, dest: usize) {
        self.emit_insn(Insn::Integer { value, dest });
    }

    pub fn emit_bool(&mut self, value: bool, dest: usize) {
        self.emit_insn(Insn::Integer {
            value: if value { 1 } else { 0 },
            dest,
        });
    }

    pub fn emit_null(&mut self, dest: usize, dest_end: Option<usize>) {
        self.emit_insn(Insn::Null { dest, dest_end });
    }

    pub fn emit_result_row(&mut self, start_reg: usize, count: usize) {
        self.emit_insn(Insn::ResultRow { start_reg, count });
    }

    fn emit_halt(&mut self, rollback: bool) {
        self.emit_insn(Insn::Halt {
            err_code: 0,
            description: if rollback {
                "rollback".to_string()
            } else {
                String::new()
            },
            on_error: None,
            description_reg: None,
        });
    }

    // no users yet, but I want to avoid someone else in the future
    // just adding parameters to emit_halt! If you use this, remove the
    // clippy warning please.
    #[allow(dead_code)]
    pub fn emit_halt_err(&mut self, err_code: usize, description: String) {
        self.emit_insn(Insn::Halt {
            err_code,
            description,
            on_error: None,
            description_reg: None,
        });
    }

    pub fn add_comment(&mut self, insn_index: BranchOffset, comment: &'static str) {
        if let BuilderQueryMode::Explain | BuilderQueryMode::ExplainQueryPlan { .. } = self.mode {
            self.explain
                .comments
                .push((insn_index.as_offset_int(), comment));
        }
    }

    pub const fn get_query_mode(&self) -> QueryMode {
        self.mode.query_mode()
    }

    /// Prefer calling the emit_explain! macro instead.
    pub fn emit_explain_should_not_be_called_directly(&mut self, push: bool, detail: EqpDetail) {
        let BuilderQueryMode::ExplainQueryPlan {
            current_parent_idx, ..
        } = self.mode
        else {
            return;
        };
        self.emit_insn(Insn::Explain {
            p1: self.insns.len(),
            p2: current_parent_idx,
            detail: Box::new(detail),
        });
        if push {
            let emitted = self.insns.len() - 1;
            *self.current_parent_idx_mut() = Some(emitted);
        }
    }

    pub fn with_cte_materialization_eqp<T>(
        &mut self,
        cte_id: usize,
        name: &str,
        emit: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let insns_start = self.insns.len();
        let emitted = emit(self)?;
        if self.mode.is_explain_query_plan() {
            let node_ids: Vec<usize> = (insns_start..self.insns.len())
                .filter(|&i| matches!(self.insns[i].0, Insn::Explain { .. }))
                .collect();
            if !node_ids.is_empty() {
                self.explain
                    .cte_materializations
                    .push(EqpCteMaterialization {
                        cte_id,
                        name: name.to_string(),
                        node_ids,
                    });
            }
        }
        Ok(emitted)
    }

    pub fn pop_current_parent_explain(&mut self) {
        let BuilderQueryMode::ExplainQueryPlan {
            current_parent_idx: Some(current),
            ..
        } = self.mode
        else {
            return;
        };
        let (Insn::Explain { p2, .. }, _) = &self.insns[current] else {
            unreachable!("current_parent_idx must point to an Explain insn");
        };
        let grandparent = *p2;
        *self.current_parent_idx_mut() = grandparent;
    }

    /// Panics if `self.mode` is not `ExplainQueryPlan`.
    fn current_parent_idx_mut(&mut self) -> &mut Option<usize> {
        match &mut self.mode {
            BuilderQueryMode::ExplainQueryPlan {
                current_parent_idx, ..
            } => current_parent_idx,
            BuilderQueryMode::Normal | BuilderQueryMode::Explain => {
                unreachable!("caller must check EXPLAIN QUERY PLAN mode first")
            }
        }
    }

    pub fn mark_last_insn_constant(&mut self) {
        if self.constant_span_is_open() {
            // no need to mark this insn as constant as the surrounding parent expression is already constant
            return;
        }

        let prev = self.insns.len().saturating_sub(1);
        self.constant_spans.push((prev, prev));
    }

    fn emit_constant_insns(&mut self) {
        // Move compile-time constant instructions to the end of the program,
        // where they are executed once after Init jumps to it.

        // Stable partition: non-constant instructions first, then constant.
        // Since spans are sorted and non-overlapping, we track our position
        // in the span list and never look back - O(n + m) total, where
        // n = number of instructions, m = number of constant spans.
        let mut non_constant = Vec::with_capacity(self.insns.len());
        let mut constant = Vec::new();
        let mut span_idx = 0;

        for item in self.insns.drain(..) {
            let idx = item.1;

            // Advance past spans we've completely passed
            while span_idx < self.constant_spans.len() && self.constant_spans[span_idx].1 < idx {
                span_idx += 1;
            }

            // Check if current span contains this index
            let is_constant =
                span_idx < self.constant_spans.len() && self.constant_spans[span_idx].0 <= idx;

            if is_constant {
                constant.push(item);
            } else {
                non_constant.push(item);
            }
        }

        self.insns = non_constant;
        self.insns.extend(constant);

        // Build old index -> new position mapping
        let mut old_to_new = vec![0usize; self.insns.len()];
        for (new_pos, (_, old_idx)) in self.insns.iter().enumerate() {
            old_to_new[*old_idx] = new_pos;
        }

        for resolved_offset in self.label_to_resolved_offset.iter_mut() {
            if let Some(old_offset) = resolved_offset {
                *resolved_offset = Some(old_to_new[*old_offset as usize] as u32);
            }
        }

        self.explain.remap_insn_indices(&old_to_new);

        if self.mode.is_explain_query_plan() {
            for i in 0..self.insns.len() {
                let (Insn::Explain { p2, .. }, _) = &self.insns[i] else {
                    continue;
                };

                let new_p2 = p2.map(|old| old_to_new[old]);

                let (Insn::Explain { p1, p2, .. }, _) = &mut self.insns[i] else {
                    unreachable!();
                };

                *p1 = i;
                *p2 = new_p2;
            }

            let current_parent_idx = self.current_parent_idx_mut();
            *current_parent_idx = current_parent_idx.map(|old| old_to_new[old]);
        }
    }

    pub const fn offset(&self) -> BranchOffset {
        BranchOffset::Offset(self.insns.len() as InsnReference)
    }

    pub fn allocate_label(&mut self) -> BranchOffset {
        let label_n = self.label_to_resolved_offset.len();
        self.label_to_resolved_offset.push(None);
        BranchOffset::Label(label_n as u32)
    }

    /// Resolve a label to whatever instruction follows the one that was
    /// last emitted.
    ///
    /// Use this when your use case is: "the program should jump to whatever instruction
    /// follows the one that was previously emitted", and you don't care exactly
    /// which instruction that is. Examples include "the start of a loop", or
    /// "after the loop ends".
    ///
    /// It is important to handle those cases this way, because the precise
    /// instruction that follows any given instruction might change due to
    /// reordering the emitted instructions.
    #[inline]
    pub fn preassign_label_to_next_insn(&mut self, label: BranchOffset) {
        let BranchOffset::Label(label_number) = label else {
            unreachable!("preassign_label_to_next_insn requires a Label, got {label:?}");
        };
        let anchor = self.offset().as_offset_int().saturating_sub(1);
        self.label_to_resolved_offset[label_number as usize] = Some(anchor);
    }

    /// Resolve `dest` so that it ends up pointing at the same final offset as
    /// `anchor`. `anchor` must already be preassigned. Use when several labels
    /// have to target the same logical program point but the point was
    /// anchored earlier (or in a different function) and
    /// `preassign_label_to_next_insn` cannot be called again at that moment.
    ///
    /// Using this helper (instead of capturing a raw `BranchOffset::Offset`
    /// from `program.offset()` and passing it to multiple resolutions) keeps
    /// all the linked labels correctly remapped when `emit_constant_insns`
    /// hoists compile-time constants — raw offsets don't get remapped, but
    /// label resolutions do.
    #[inline]
    pub fn link_label_to_other_label(&mut self, dest: BranchOffset, anchor: BranchOffset) {
        let BranchOffset::Label(dest_n) = dest else {
            unreachable!("link_label_to_other_label dest must be a Label, got {dest:?}");
        };
        let BranchOffset::Label(anchor_n) = anchor else {
            unreachable!("link_label_to_other_label anchor must be a Label, got {anchor:?}");
        };
        let resolution = self.label_to_resolved_offset[anchor_n as usize]
            .expect("anchor label must already be preassigned/resolved");
        self.label_to_resolved_offset[dest_n as usize] = Some(resolution);
    }

    /// Resolve unresolved labels to a specific offset in the instruction list.
    ///
    /// This function scans all instructions and resolves any labels to their corresponding offsets.
    /// It ensures that all labels are resolved correctly and updates the target program counter (PC)
    /// of each instruction that references a label.
    pub fn resolve_labels(&mut self) -> crate::Result<()> {
        let resolve = |pc: &mut BranchOffset, insn_name: &str| -> crate::Result<()> {
            if let BranchOffset::Label(label) = pc {
                let Some(Some(anchor)) = self.label_to_resolved_offset.get(*label as usize) else {
                    crate::bail_corrupt_error!(
                        "Reference to undefined or unresolved label in {insn_name}: {label}"
                    );
                };
                *pc = BranchOffset::Offset(anchor + 1);
            }
            Ok(())
        };
        for (insn, _) in self.insns.iter_mut() {
            match insn {
                Insn::Init { target_pc } => {
                    resolve(target_pc, "Init")?;
                }
                Insn::Eq {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Eq")?;
                }
                Insn::Ne {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ne")?;
                }
                Insn::Lt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Lt")?;
                }
                Insn::Le {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Le")?;
                }
                Insn::Gt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Gt")?;
                }
                Insn::Ge {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ge")?;
                }
                Insn::If {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "If")?;
                }
                Insn::IfNot {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "IfNot")?;
                }
                Insn::Rewind { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Rewind")?;
                }
                Insn::Last { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "Last")?;
                }
                Insn::Goto { target_pc } => {
                    resolve(target_pc, "Goto")?;
                }
                Insn::DecrJumpZero {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "DecrJumpZero")?;
                }
                Insn::SorterNext {
                    cursor_id: _cursor_id,
                    pc_if_next,
                } => {
                    resolve(pc_if_next, "SorterNext")?;
                }
                Insn::SorterSort { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "SorterSort")?;
                }
                Insn::SorterCompare {
                    pc_when_nonequal: target_pc,
                    ..
                } => {
                    resolve(target_pc, "SorterCompare")?;
                }
                Insn::NotNull {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "NotNull")?;
                }
                Insn::ColumnHasField { target_pc, .. } => {
                    resolve(target_pc, "ColumnHasField")?;
                }
                Insn::IfPos { target_pc, .. } => {
                    resolve(target_pc, "IfPos")?;
                }
                Insn::Next { pc_if_next, .. } => {
                    resolve(pc_if_next, "Next")?;
                }
                Insn::Once {
                    target_pc_when_reentered,
                    ..
                } => {
                    resolve(target_pc_when_reentered, "Once")?;
                }
                Insn::ResetOnce { region_end, .. } => {
                    resolve(region_end, "ResetOnce")?;
                }
                Insn::Prev { pc_if_prev, .. } => {
                    resolve(pc_if_prev, "Prev")?;
                }
                Insn::InitCoroutine {
                    yield_reg: _,
                    jump_on_definition,
                    start_offset,
                } => {
                    resolve(jump_on_definition, "InitCoroutine")?;
                    resolve(start_offset, "InitCoroutine")?;
                }
                Insn::NotExists {
                    cursor: _,
                    rowid_reg: _,
                    target_pc,
                } => {
                    resolve(target_pc, "NotExists")?;
                }
                Insn::MustBeInt {
                    target_pc: Some(target_pc),
                    ..
                } => {
                    resolve(target_pc, "MustBeInt")?;
                }
                Insn::Yield {
                    yield_reg: _,
                    end_offset,
                    subtype_clear_start_reg: _,
                    subtype_clear_count: _,
                } => {
                    resolve(end_offset, "Yield")?;
                }
                Insn::SeekRowid { target_pc, .. } => {
                    resolve(target_pc, "SeekRowid")?;
                }
                Insn::Gosub { target_pc, .. } => {
                    resolve(target_pc, "Gosub")?;
                }
                Insn::Jump {
                    target_pc_eq,
                    target_pc_lt,
                    target_pc_gt,
                } => {
                    resolve(target_pc_eq, "Jump")?;
                    resolve(target_pc_lt, "Jump")?;
                    resolve(target_pc_gt, "Jump")?;
                }
                Insn::SeekGE { target_pc, .. } => resolve(target_pc, "SeekGE")?,
                Insn::SeekGT { target_pc, .. } => resolve(target_pc, "SeekGT")?,
                Insn::SeekLE { target_pc, .. } => resolve(target_pc, "SeekLE")?,
                Insn::SeekLT { target_pc, .. } => resolve(target_pc, "SeekLT")?,
                Insn::IdxGE { target_pc, .. } => resolve(target_pc, "IdxGE")?,
                Insn::IdxLE { target_pc, .. } => resolve(target_pc, "IdxLE")?,
                Insn::IdxGT { target_pc, .. } => resolve(target_pc, "IdxGT")?,
                Insn::IdxLT { target_pc, .. } => resolve(target_pc, "IdxLT")?,
                Insn::IndexMethodQuery { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "IndexMethodQuery")?;
                }
                Insn::IsNull { reg: _, target_pc } => resolve(target_pc, "IsNull")?,
                Insn::VNext { pc_if_next, .. } => resolve(pc_if_next, "VNext")?,
                Insn::VFilter { pc_if_empty, .. } => resolve(pc_if_empty, "VFilter")?,
                Insn::RowSetRead { pc_if_empty, .. } => resolve(pc_if_empty, "RowSetRead")?,
                Insn::RowSetTest { pc_if_found, .. } => resolve(pc_if_found, "RowSetTest")?,
                Insn::NoConflict { target_pc, .. } => resolve(target_pc, "NoConflict")?,
                Insn::Found { target_pc, .. } => resolve(target_pc, "Found")?,
                Insn::NotFound { target_pc, .. } => resolve(target_pc, "NotFound")?,
                Insn::FkIfZero { target_pc, .. } => resolve(target_pc, "FkIfZero")?,
                Insn::Filter { target_pc, .. } => resolve(target_pc, "Filter")?,
                Insn::HashProbe { target_pc, .. } => resolve(target_pc, "HashProbe")?,
                Insn::HashNext { target_pc, .. } => resolve(target_pc, "HashNext")?,
                Insn::HashDistinct { data } => resolve(&mut data.target_pc, "HashDistinct")?,
                Insn::HashScanUnmatched { target_pc, .. } => {
                    resolve(target_pc, "HashScanUnmatched")?
                }
                Insn::HashNextUnmatched { target_pc, .. } => {
                    resolve(target_pc, "HashNextUnmatched")?
                }
                Insn::HashGraceInit { target_pc, .. } => resolve(target_pc, "HashGraceInit")?,
                Insn::HashGraceLoadPartition { target_pc, .. } => {
                    resolve(target_pc, "HashGraceLoadPartition")?
                }
                Insn::HashGraceNextProbe { target_pc, .. } => {
                    resolve(target_pc, "HashGraceNextProbe")?
                }
                Insn::HashGraceAdvancePartition { target_pc, .. } => {
                    resolve(target_pc, "HashGraceAdvancePartition")?
                }
                Insn::Program {
                    ignore_jump_target, ..
                } => resolve(ignore_jump_target, "Program")?,
                _ => {}
            }
        }
        self.label_to_resolved_offset.clear();
        Ok(())
    }

    /// Set a cursor override for a table. When resolving a table cursor for this table,
    /// the override cursor will be used instead of the normal resolution.
    pub fn set_cursor_override(&mut self, table_ref_id: TableInternalId, cursor_id: CursorID) {
        self.cursor_overrides.insert(table_ref_id.into(), cursor_id);
    }

    /// Clear the cursor override for a table.
    pub fn clear_cursor_override(&mut self, table_ref_id: TableInternalId) {
        self.cursor_overrides.remove(&table_ref_id.into());
    }

    /// Clear all cursor overrides.
    pub fn clear_all_cursor_overrides(&mut self) {
        self.cursor_overrides.clear();
    }

    /// Check if a cursor override is active for a given table.
    pub fn has_cursor_override(&self, table_ref_id: TableInternalId) -> bool {
        self.cursor_overrides.contains_key(&table_ref_id.into())
    }

    // translate [CursorKey] to cursor id
    pub fn resolve_cursor_id_safe(&self, key: &CursorKey) -> Option<CursorID> {
        // Check cursor overrides first, only apply override for table cursors.
        // Index cursor lookups are not overridden because when a cursor override is active,
        // the calling code (translate_expr) should skip index logic entirely.
        if key.index.is_none() && !key.is_build {
            let table_id: usize = key.table_reference_id.into();
            if let Some(&cursor_id) = self.cursor_overrides.get(&table_id) {
                return Some(cursor_id);
            }
        }
        self.cursor_ref
            .iter()
            .position(|(k, _)| k.as_ref().is_some_and(|k| k.equals(key)))
    }

    pub fn resolve_cursor_id(&self, key: &CursorKey) -> CursorID {
        self.resolve_cursor_id_safe(key)
            .unwrap_or_else(|| panic!("Cursor not found: {key:?}"))
    }

    /// Resolve the first allocated index cursor for a given table reference.
    /// This method exists due to a limitation of our translation system where
    /// a subquery that references an outer query table cannot know whether a
    /// table cursor, index cursor, or both were opened for that table reference.
    /// Hence: currently we first try to resolve a table cursor, and if that fails,
    /// we resolve an index cursor via this method.
    pub fn resolve_any_index_cursor_id_for_table(&self, table_ref_id: TableInternalId) -> CursorID {
        self.resolve_any_index_cursor_id_for_table_safe(table_ref_id)
            .unwrap_or_else(|| panic!("No index cursor found for table {table_ref_id}"))
    }

    pub fn resolve_any_index_cursor_id_for_table_safe(
        &self,
        table_ref_id: TableInternalId,
    ) -> Option<CursorID> {
        self.cursor_ref.iter().position(|(k, _)| {
            k.as_ref()
                .is_some_and(|k| k.table_reference_id == table_ref_id && k.index.is_some())
        })
    }

    /// Resolve the [Index] that a given cursor is associated with.
    pub fn resolve_index_for_cursor_id(&self, cursor_id: CursorID) -> Arc<Index> {
        let cursor_ref = &self
            .cursor_ref
            .get(cursor_id)
            .unwrap_or_else(|| panic!("Cursor not found: {cursor_id}"))
            .1;
        let CursorType::BTreeIndex(index) = cursor_ref else {
            panic!("Cursor is not an index: {cursor_id}");
        };
        index.clone()
    }

    /// Get the [CursorType] of a given cursor.
    pub fn get_cursor_type(&self, cursor_id: CursorID) -> Option<&CursorType> {
        self.cursor_ref
            .get(cursor_id)
            .map(|(_, cursor_type)| cursor_type)
    }

    pub const fn set_collation(&mut self, c: Option<(CollationSeq, bool)>) {
        self.collation = c
    }

    pub const fn curr_collation_ctx(&self) -> Option<(CollationSeq, bool)> {
        self.collation
    }

    pub fn curr_collation(&self) -> Option<CollationSeq> {
        self.collation.map(|c| c.0)
    }

    pub const fn reset_collation(&mut self) {
        self.collation = None;
    }

    #[inline]
    pub fn nested<T>(&mut self, body: impl FnOnce(&mut Self) -> T) -> T {
        self.incr_nesting();
        let res = body(self);
        self.decr_nesting();
        res
    }

    #[inline]
    const fn incr_nesting(&mut self) {
        self.nested_level += 1;
    }

    #[inline]
    const fn decr_nesting(&mut self) {
        self.nested_level -= 1;
    }

    /// Returns true if we are inside a nested subquery context.
    #[inline]
    pub const fn is_nested(&self) -> bool {
        self.nested_level > 0
    }

    /// Initialize the program with basic setup and return initial metadata and labels
    pub fn prologue(&mut self) {
        if self.flags.is_subprogram() {
            // Subprograms (triggers, FK actions) don't need Transaction - they run within parent's tx
            self.init_label = self.allocate_label();
            self.emit_insn(Insn::Init {
                target_pc: self.init_label,
            });
            self.preassign_label_to_next_insn(self.init_label);
            self.start_offset = self.offset();
            return;
        }
        if self.nested_level == 0 {
            self.init_label = self.allocate_label();

            self.emit_insn(Insn::Init {
                target_pc: self.init_label,
            });

            self.start_offset = self.offset();
        }
    }

    /// Tries to mirror: https://github.com/sqlite/sqlite/blob/e77e589a35862f6ac9c4141cfd1beb2844b84c61/src/build.c#L5379
    pub fn begin_write_operation(&mut self) -> Result<(), alloc::TryReserveError> {
        self.txn_mode = TransactionMode::Write;
        self.write_databases.set(crate::MAIN_DB_ID)
    }

    /// Begin a write operation on a specific database (for attached databases).
    pub fn begin_write_on_database(
        &mut self,
        database_id: usize,
        schema_cookie: u32,
    ) -> Result<(), alloc::TryReserveError> {
        self.txn_mode = TransactionMode::Write;
        self.write_databases.set(database_id)?;
        self.write_database_cookies
            .insert(database_id, schema_cookie);
        Ok(())
    }

    pub fn begin_read_operation(&mut self) -> Result<(), alloc::TryReserveError> {
        // Just override the transaction mode when it is None
        if matches!(self.txn_mode, TransactionMode::None) {
            self.txn_mode = TransactionMode::Read;
        }
        self.read_databases.set(crate::MAIN_DB_ID)
    }

    /// Begin a read operation on a specific attached database.
    /// This ensures a Transaction instruction is emitted for the attached pager
    /// so that a WAL read lock is acquired.
    pub fn begin_read_on_database(
        &mut self,
        database_id: usize,
        schema_cookie: u32,
    ) -> Result<(), alloc::TryReserveError> {
        if matches!(self.txn_mode, TransactionMode::None) {
            self.txn_mode = TransactionMode::Read;
        }
        self.read_databases.set(database_id)?;
        self.read_database_cookies
            .insert(database_id, schema_cookie);
        Ok(())
    }

    pub const fn begin_concurrent_operation(&mut self) {
        self.txn_mode = TransactionMode::Concurrent;
    }

    /// Indicates the rollback behvaiour for the halt instruction in epilogue
    pub const fn rollback(&mut self) {
        self.flags.set_rollback(true);
    }

    /// Clean up and finalize the program, resolving any remaining labels
    /// Note that although these are the final instructions, typically an SQLite
    /// query will jump to the Transaction instruction via init_label.
    pub fn epilogue(&mut self, schema: &Schema) {
        if self.flags.is_subprogram() {
            // Subprograms (triggers, FK actions) just emit Halt without Transaction
            let description = if self.trigger.is_some() {
                "trigger"
            } else {
                "fk action"
            };
            self.emit_insn(Insn::Halt {
                err_code: 0,
                description: description.to_string(),
                on_error: None,
                description_reg: None,
            });
            return;
        }
        if self.nested_level == 0 {
            // "rollback" flag is used to determine if halt should rollback the transaction.
            self.emit_halt(self.flags.rollback());
            self.preassign_label_to_next_insn(self.init_label);

            if !matches!(self.txn_mode, TransactionMode::None) {
                let write_dbs = self.write_databases.clone();
                for db_id in &write_dbs {
                    let schema_cookie = if db_id == crate::MAIN_DB_ID {
                        schema.schema_version
                    } else {
                        self.write_database_cookies
                            .get(&db_id)
                            .copied()
                            .unwrap_or(0)
                    };
                    self.emit_insn(Insn::Transaction {
                        db: db_id,
                        tx_mode: self.txn_mode,
                        schema_cookie,
                    });
                }
                // Emit Transaction for each non-main database that only needs a read
                // (skip databases already covered by write_databases)
                let read_dbs = self.read_databases.clone();
                for db_id in &read_dbs {
                    if !write_dbs.get(db_id) {
                        let schema_cookie = if db_id == crate::MAIN_DB_ID {
                            schema.schema_version
                        } else {
                            self.read_database_cookies.get(&db_id).copied().unwrap_or(0)
                        };
                        self.emit_insn(Insn::Transaction {
                            db: db_id,
                            tx_mode: TransactionMode::Read,
                            schema_cookie,
                        });
                    }
                }
            }

            if !self.constant_spans.is_empty() {
                self.emit_constant_insns();
            }
            self.emit_insn(Insn::Goto {
                target_pc: self.start_offset,
            });
        }
    }

    /// Checks whether `table` or any of its indices has been opened in the program
    pub fn is_table_open(&self, table: &Table) -> bool {
        self.table_references.contains_table(table)
    }

    /// Returns true if the cursor is backed by a table or index B-tree.
    pub fn cursor_is_btree(&self, cursor_id: CursorID) -> bool {
        matches!(
            self.cursor_ref[cursor_id].1,
            CursorType::BTreeTable(_) | CursorType::BTreeIndex(_)
        )
    }

    /// Returns the BTreeTable for the given cursor, if it is a BTreeTable cursor.
    pub fn btree_table_from_cursor(&self, cursor_id: CursorID) -> Option<&Arc<BTreeTable>> {
        match &self.cursor_ref[cursor_id].1 {
            CursorType::BTreeTable(t) => Some(t),
            _ => None,
        }
    }

    #[inline]
    pub fn cursor_loop(&mut self, cursor_id: CursorID, f: impl Fn(&mut ProgramBuilder, usize)) {
        let loop_start = self.allocate_label();
        let loop_end = self.allocate_label();

        self.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: loop_end,
        });
        self.preassign_label_to_next_insn(loop_start);

        let rowid = self.alloc_register();

        self.emit_insn(Insn::RowId {
            cursor_id,
            dest: rowid,
        });

        self.emit_insn(Insn::IsNull {
            reg: rowid,
            target_pc: loop_end,
        });

        f(self, rowid);

        self.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loop_start,
            fullscan: false,
        });
        self.preassign_label_to_next_insn(loop_end);
    }

    pub fn emit_column_or_rowid(&mut self, cursor_id: CursorID, column: usize, out: usize) {
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).expect("cursor_id is valid");
        if let CursorType::BTreeTable(btree) = cursor_type {
            let column_def = btree
                .columns()
                .get(column)
                .expect("column index out of bounds");
            if column_def.is_rowid_alias() {
                // Consume the suppress_column_default flag so it doesn't
                // leak to the next column (emit_column normally consumes it).
                self.flags.set_suppress_column_default(false);
                self.emit_insn(Insn::RowId {
                    cursor_id,
                    dest: out,
                });
            } else {
                self.emit_column(cursor_id, column, out);
            }
        } else {
            self.emit_column(cursor_id, column, out);
        }
    }

    /// Emit a ColumnHasField instruction that jumps to `target_pc` if the
    /// cursor's record has a field at the given logical column index.
    /// Falls through if the record is short (ALTER TABLE ADD COLUMN).
    pub fn emit_column_has_field(
        &mut self,
        cursor_id: CursorID,
        column: usize,
        target_pc: BranchOffset,
    ) {
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).expect("cursor_id is valid");
        let physical_column = match cursor_type {
            CursorType::BTreeTable(btree) => btree.logical_to_physical_column(column),
            _ => column,
        };
        self.emit_insn(Insn::ColumnHasField {
            cursor_id,
            column: physical_column,
            target_pc,
        });
    }

    /// Emit an Affinity instruction for a single register with the given column affinity.
    pub fn emit_column_affinity(&mut self, register: usize, affinity: Affinity) {
        self.emit_insn(Insn::Affinity {
            start_reg: register,
            count: NonZeroUsize::MIN,
            affinities: affinity.aff_mask().to_string(),
        });
    }

    fn emit_column(&mut self, cursor_id: CursorID, column: usize, out: usize) {
        let (_, cursor_type) = self.cursor_ref.get(cursor_id).expect("cursor_id is valid");

        if let CursorType::BTreeTable(btree) = cursor_type {
            let column_def = btree
                .columns()
                .get(column)
                .expect("column index out of bounds");
            turso_assert!(
                !column_def.is_virtual_generated(),
                "emit_column called with virtual generated column index",
                {"column_index": column}
            );
        }

        let physical_column = match cursor_type {
            CursorType::BTreeTable(btree) => btree.logical_to_physical_column(column),
            _ => column,
        };

        let default = 'value: {
            let default = match cursor_type {
                CursorType::BTreeTable(btree) => &btree.columns()[column].default,
                CursorType::BTreeIndex(index) => &index.columns[column].default,
                CursorType::MaterializedView(btree, _) => &btree.columns()[column].default,
                _ => break 'value None,
            };

            let Some(ref default_expr) = default else {
                break 'value None;
            };

            // Try to constant-fold the default expression into a Value for the
            // Column instruction. Non-constant defaults (e.g. DEFAULT (ABS(-5)))
            // can't be folded and yield None here — that's correct: they are
            // evaluated at INSERT time via translate_expr. The Column default
            // only matters for pre-existing rows after ALTER TABLE ADD COLUMN,
            // and ALTER TABLE already validates that the default is constant.
            let mut value = match crate::translate::alter::eval_constant_default_value(default_expr)
            {
                Ok(v) => v,
                Err(_) => break 'value None,
            };

            // Apply column affinity to the default value, matching SQLite's
            // sqlite3ColumnDefault which calls sqlite3ValueFromExpr with
            // pCol->affinity. This ensures e.g. ALTER TABLE ADD COLUMN c TEXT
            // DEFAULT 0 returns text "0" rather than integer 0 for pre-existing rows.
            let affinity = match cursor_type {
                CursorType::BTreeTable(btree) => btree.columns()[column].affinity(),
                CursorType::MaterializedView(btree, _) => btree.columns()[column].affinity(),
                _ => Affinity::Blob,
            };
            if let Some(converted) = affinity.convert(&value) {
                value = match converted {
                    either::Either::Left(ValueRef::Numeric(numeric)) => Value::from(numeric),
                    either::Either::Left(_) => {
                        unreachable!("affinity conversion returned an unexpected borrowed value")
                    }
                    either::Either::Right(val) => val,
                };
            }

            Some(value)
        };

        let default = if self.flags.suppress_column_default() {
            self.flags.set_suppress_column_default(false);
            None
        } else {
            default
        };

        self.emit_insn(Insn::Column {
            cursor_id,
            column: physical_column,
            dest: out,
            default,
        });
    }

    pub fn build_prepared_program(
        mut self,
        prepare_context: PrepareContext,
        change_cnt_on: bool,
        sql: &str,
    ) -> crate::Result<PreparedProgram> {
        self.resolve_labels()?;

        self.parameters.list.dedup();

        // Mirrors SQLite's: usesStmtJournal = isMultiWrite && mayAbort
        // Statement journals are only needed when a statement writes multiple rows AND could
        // abort midway (e.g. constraint violation). Single-row writes are atomic and don't
        // need statement-level rollback. Both flags default to true; specific translate paths
        // (e.g., single-row INSERT) set is_multi_write=false to opt out.
        let needs_stmt_subtransactions = matches!(self.txn_mode, TransactionMode::Write)
            && self.flags.is_multi_write()
            && self.may_abort();

        let prepared = PreparedProgram {
            max_registers: self.next_free_register,
            insns: self.insns,
            cursor_ref: self.cursor_ref,
            explain: self.explain,
            parameters: self.parameters,
            change_cnt_on,
            readonly: self.flags.readonly(),
            result_columns: self.result_columns,
            table_references: self.table_references,
            sql: sql.to_string(),
            refreshes_analyze_stats: sql
                .trim_start()
                .as_bytes()
                .get(..7)
                .is_some_and(|head| head.eq_ignore_ascii_case(b"ANALYZE")),
            needs_stmt_subtransactions: crate::Arc::new(crate::AtomicBool::new(
                needs_stmt_subtransactions,
            )),
            trigger: self.trigger.take(),
            is_subprogram: self.flags.is_subprogram(),
            resolve_type: self.resolve_type,
            prepare_context,
            write_databases: self.write_databases,
            read_databases: self.read_databases,
        };
        Ok(prepared)
    }

    #[turso_macros::trace_stack]
    pub fn build(
        self,
        connection: Arc<Connection>,
        change_cnt_on: bool,
        sql: &str,
    ) -> crate::Result<Program> {
        let prepare_context = PrepareContext::from_connection(&connection);
        let prepared = self.build_prepared_program(prepare_context, change_cnt_on, sql)?;
        Ok(Program::from_prepared(Arc::new(prepared), connection))
    }
}

pub(crate) trait CursorTypeExt {
    fn accepts_column_range_fusing(&self) -> bool;
}

impl CursorTypeExt for CursorType {
    fn accepts_column_range_fusing(&self) -> bool {
        matches!(
            self,
            CursorType::BTreeTable(_)
                | CursorType::BTreeIndex(_)
                | CursorType::Pseudo(_)
                | CursorType::Sorter
        )
    }
}
