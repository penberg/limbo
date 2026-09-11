use crate::alloc::vec;
use crate::alloc::TursoFromIterator;
use crate::alloc::*;
use crate::function::{Deterministic, Func, ScalarFunc};
use crate::incremental::view::IncrementalView;
use crate::incremental::{compiler::DBSP_CIRCUIT_VERSION, operator::create_dbsp_state_index};
use crate::index_method::{IndexMethodAttachment, IndexMethodConfiguration};
use crate::return_if_io;
use crate::stats::AnalyzeStats;
use crate::sync::RwLock;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{
    bind_and_rewrite_expr, walk_expr, walk_expr_mut, BindingBehavior, WalkControl,
};
use crate::translate::index::{resolve_index_method_parameters, resolve_sorted_columns};
use crate::translate::planner::ROWID_STRS;
use crate::types::IOResultOr;
use crate::types::{IOResult, ImmutableRecord};
use crate::util::{exprs_are_equivalent, normalize_ident};
use crate::vdbe::affinity::Affinity;
use crate::vdbe::CursorID;
use crate::{turso_assert, turso_debug_assert};
use smallvec::SmallVec;
use turso_macros::AtomicEnum;

#[derive(Debug, Clone, AtomicEnum)]
pub enum ViewState {
    Ready,
    InProgress,
}

/// Simple view structure for non-materialized views
#[derive(Debug)]
pub struct View {
    pub name: String,
    pub sql: String,
    pub select_stmt: ast::Select,
    pub columns: Vec<Column>,
    pub state: AtomicViewState,
}

impl View {
    fn new(name: String, sql: String, select_stmt: ast::Select, columns: Vec<Column>) -> Self {
        Self {
            name,
            sql,
            select_stmt,
            columns,
            state: AtomicViewState::new(ViewState::Ready),
        }
    }

    pub fn process(&self) -> Result<()> {
        let state = self.state.get();
        match state {
            ViewState::InProgress => {
                bail_parse_error!("view {} is circularly defined", self.name)
            }
            ViewState::Ready => {
                self.state.set(ViewState::InProgress);
                Ok(())
            }
        }
    }

    pub fn done(&self) {
        let state = self.state.get();
        match state {
            ViewState::InProgress => {
                self.state.set(ViewState::Ready);
            }
            ViewState::Ready => {}
        }
    }
}

impl Clone for View {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            sql: self.sql.clone(),
            select_stmt: self.select_stmt.clone(),
            columns: self.columns.clone(),
            state: AtomicViewState::new(ViewState::Ready),
        }
    }
}

/// Type alias for regular views collection
pub type ViewsMap = HashMap<String, Arc<View>>;

/// Trigger structure
#[derive(Debug, Clone)]
pub struct Trigger {
    pub name: String,
    pub sql: String,
    pub table_name: String,
    pub time: turso_parser::ast::TriggerTime,
    pub event: turso_parser::ast::TriggerEvent,
    pub for_each_row: bool,
    pub when_clause: Option<turso_parser::ast::Expr>,
    pub commands: std::vec::Vec<turso_parser::ast::TriggerCmd>,
    pub temporary: bool,
    /// For temp triggers that target a table in a specific database.
    /// - `None` — the trigger was created without a db qualifier and
    ///   targets a table in its own schema (or, if it's a temp trigger
    ///   and no temp shadow exists, the parent schema's table).
    /// - `Some(MAIN_DB_ID | TEMP_DB_ID | <attached_id>)` — resolved
    ///   qualifier.
    /// - `Some(crate::INVALID_DB_ID)` — the qualifier referenced an
    ///   attached db name that could not be resolved at parse time
    ///   (e.g. reloading `CREATE TEMP TRIGGER ... ON aux.x` when
    ///   `aux` is not attached). The trigger never fires against a
    ///   real db, which is the correct fail-safe behaviour.
    pub target_database_id: Option<usize>,
}

impl Trigger {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        sql: String,
        table_name: String,
        time: Option<turso_parser::ast::TriggerTime>,
        event: turso_parser::ast::TriggerEvent,
        for_each_row: bool,
        when_clause: Option<turso_parser::ast::Expr>,
        commands: std::vec::Vec<turso_parser::ast::TriggerCmd>,
        temporary: bool,
        target_database_id: Option<usize>,
    ) -> Self {
        Self {
            name,
            sql,
            table_name,
            time: time.unwrap_or(turso_parser::ast::TriggerTime::Before),
            event,
            for_each_row,
            when_clause,
            commands,
            temporary,
            target_database_id,
        }
    }
}

use crate::storage::btree::{BTreeCursor, CursorTrait};
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::translate::collate::CollationSeq;
use crate::translate::plan::{BitSet, ColumnMask, Plan, TableReferences};
use crate::util::{
    module_args_from_sql, module_name_from_sql, type_from_name, UnparsedFromSqlIndex,
};
use crate::Result;
use crate::{bail_parse_error, LimboError, MvCursor, Pager, SymbolTable, ValueRef, VirtualTable};
use bitflags::bitflags;
use column_info::{ColumnInfo, NewColumnInfoParams};
use core::fmt;
use rustc_hash::{FxBuildHasher, FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::VecDeque;
use std::sync::OnceLock;
use tracing::trace;
use turso_parser::ast::{
    self, ColumnDefinition, Expr, InitDeferredPred, Literal, Name, NullsOrder, RefAct, ResolveType,
    SortOrder, TableInternalId, TypeOperator,
};
use turso_parser::{
    ast::{Cmd, CreateTableBody, ResultColumn, Stmt},
    parser::Parser,
};

pub const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
pub const SCHEMA_TABLE_NAME_ALT: &str = "sqlite_master";
pub const TEMP_SCHEMA_TABLE_NAME: &str = "sqlite_temp_schema";
pub const TEMP_SCHEMA_TABLE_NAME_ALT: &str = "sqlite_temp_master";
pub const SQLITE_SEQUENCE_TABLE_NAME: &str = "sqlite_sequence";
pub const TURSO_TYPES_TABLE_NAME: &str = "__turso_internal_types";
pub const DBSP_TABLE_PREFIX: &str = "__turso_internal_dbsp_state_v";
pub const TURSO_INTERNAL_PREFIX: &str = "__turso_internal_";
pub const SEQ_BACKING_TABLE_PREFIX: &str = "__turso_internal_seq_";
// Prefix for the hidden sequence *name* owned by an AUTOINCREMENT table.
// This is not itself a table name. Its physical backing table is still named
// by applying SEQ_BACKING_TABLE_PREFIX to the full sequence name.
pub const AUTOINCREMENT_SEQ_PREFIX: &str = "__turso_internal_autoincrement_";

/// Name of the hidden sequence owned by an AUTOINCREMENT table.
pub fn autoincrement_sequence_name(table_name: &str) -> String {
    String::from(AUTOINCREMENT_SEQ_PREFIX) + table_name
}

struct SequenceBackingTableSource {
    sequence_name: String,
    root_page: i64,
    num_columns: usize,
}

struct SequenceMetadata {
    // is_called intentionally omitted from descriptor reconstruction —
    // the runtime watermark (including is_called) is always read from
    // the backing-table row at nextval time, not seeded from schema.
    start: i64,
    increment: i64,
    min: i64,
    max: i64,
    cycle: bool,
}

use crate::util::quote_identifier as quote_ident;

/// Recursively rewrite `Expr::Id("value")` (case-insensitive) to `Expr::Id(col_name)`.
pub fn rewrite_value_to_column(expr: &ast::Expr, col_name: &str) -> Box<ast::Expr> {
    let mut cloned = expr.clone();
    let _ = walk_expr_mut(&mut cloned, &mut |e| {
        if let ast::Expr::Id(name) = e {
            if name.as_str().eq_ignore_ascii_case("value") {
                *e = ast::Expr::Id(ast::Name::exact(col_name.to_string()));
            }
        }
        Ok(WalkControl::Continue)
    });
    Box::new(cloned)
}

/// Field definition within a StructDef.
#[derive(Debug, Clone)]
pub struct StructFieldDef {
    pub name: String,
    pub base_affinity: Affinity,
    pub type_name: String,
}

/// Definition for a STRUCT composite type.
#[derive(Debug, Clone)]
pub struct StructDef {
    pub fields: Vec<StructFieldDef>,
}

/// Variant definition within a UnionDef.
#[derive(Debug, Clone)]
pub struct UnionVariantDef {
    pub tag_name: String,
    pub tag_index: u8,
    pub base_affinity: Affinity,
    pub type_name: String,
}

/// Definition for a UNION discriminated union type.
#[derive(Debug, Clone)]
pub struct UnionDef {
    pub variants: Vec<UnionVariantDef>,
    /// Cached variant tag names for `UnionTag` instructions.
    /// Built once at type registration time so we don't rebuild per-instruction.
    pub tag_names: Arc<[String]>,
}

/// The kind-specific payload of a custom type.
#[derive(Debug, Clone)]
pub enum TypeDefKind {
    Custom {
        params: std::vec::Vec<ast::TypeParam>,
        base: String,
        encode: Option<Box<ast::Expr>>,
        decode: Option<Box<ast::Expr>>,
        operators: std::vec::Vec<TypeOperator>,
        default: Option<Box<ast::Expr>>,
    },
    Struct(StructDef),
    Union(UnionDef),
}

/// Custom type definition, loaded from sqlite_turso_types
#[derive(Debug, Clone)]
/// A fully-resolved custom type: the chain of TypeDefs from the named type
/// up to the ultimate primitive, plus the primitive name itself.
pub struct ResolvedType {
    /// The ultimate primitive type name (e.g., "integer", "text", "blob").
    pub primitive: String,
    /// TypeDefs from child (the named type) to ancestor (closest to primitive).
    pub chain: Vec<Arc<TypeDef>>,
}

impl ResolvedType {
    /// The leaf (directly named) type definition.
    pub fn leaf(&self) -> &TypeDef {
        &self.chain[0]
    }

    /// Whether the leaf type is a domain.
    pub fn is_domain(&self) -> bool {
        self.chain[0].is_domain
    }

    /// Find the first DEFAULT expression in the type chain (child first, then ancestors).
    /// Matches PostgreSQL: a child domain inherits the parent's DEFAULT when it
    /// doesn't declare its own.
    pub fn default_expr(&self) -> Option<&ast::Expr> {
        self.chain.iter().find_map(|td| td.default_expr())
    }
}

#[derive(Debug, Clone)]
pub struct TypeDef {
    pub name: String,
    pub is_builtin: bool,
    pub not_null: bool,
    /// Whether this is a domain (CREATE DOMAIN) vs a custom type (CREATE TYPE).
    pub is_domain: bool,
    /// Original SQL for round-trip persistence. Stored verbatim from creation.
    pub sql: String,
    /// CHECK constraints from CREATE DOMAIN, stored as first-class data.
    /// Empty for regular CREATE TYPE definitions.
    pub domain_checks: std::vec::Vec<ast::DomainConstraint>,
    pub kind: TypeDefKind,
}

impl TypeDef {
    /// Returns true if this is a STRUCT type.
    pub fn is_struct(&self) -> bool {
        matches!(self.kind, TypeDefKind::Struct(_))
    }

    /// Returns true if this is a UNION type.
    pub fn is_union(&self) -> bool {
        matches!(self.kind, TypeDefKind::Union(_))
    }

    /// Returns the StructDef if this is a STRUCT type.
    pub fn struct_def(&self) -> Option<&StructDef> {
        match &self.kind {
            TypeDefKind::Struct(sd) => Some(sd),
            _ => None,
        }
    }

    /// Returns the UnionDef if this is a UNION type.
    pub fn union_def(&self) -> Option<&UnionDef> {
        match &self.kind {
            TypeDefKind::Union(ud) => Some(ud),
            _ => None,
        }
    }

    /// Returns the encode expression (Custom types only).
    pub fn encode(&self) -> Option<&ast::Expr> {
        match &self.kind {
            TypeDefKind::Custom { encode, .. } => encode.as_deref(),
            _ => None,
        }
    }

    /// Returns the decode expression (Custom types only).
    pub fn decode(&self) -> Option<&ast::Expr> {
        match &self.kind {
            TypeDefKind::Custom { decode, .. } => decode.as_deref(),
            _ => None,
        }
    }

    /// Returns the base type name.
    pub fn base(&self) -> &str {
        match &self.kind {
            TypeDefKind::Custom { base, .. } => base,
            TypeDefKind::Struct(_) | TypeDefKind::Union(_) => "blob",
        }
    }

    /// Returns the params (Custom types only, empty for Struct/Union).
    pub fn params(&self) -> &[ast::TypeParam] {
        match &self.kind {
            TypeDefKind::Custom { params, .. } => params,
            _ => &[],
        }
    }

    /// Returns the operators (Custom types only, empty for Struct/Union).
    pub fn operators(&self) -> &[TypeOperator] {
        match &self.kind {
            TypeDefKind::Custom { operators, .. } => operators,
            _ => &[],
        }
    }

    /// Returns the default expression (Custom types only).
    pub fn default_expr(&self) -> Option<&ast::Expr> {
        match &self.kind {
            TypeDefKind::Custom { default, .. } => default.as_deref(),
            _ => None,
        }
    }

    /// Find a struct field by name. Returns (field_index, &StructFieldDef).
    pub fn find_struct_field(&self, name: &str) -> Option<(usize, &StructFieldDef)> {
        self.struct_def().and_then(|sd| {
            sd.fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.name.eq_ignore_ascii_case(name))
        })
    }

    /// Resolve a tag name to its numeric index within this union type.
    /// Returns None if this is not a union or the variant doesn't exist.
    pub fn resolve_union_tag_index(&self, tag_name: &str) -> Option<u8> {
        self.find_union_variant(tag_name).map(|(idx, _)| idx)
    }

    /// Find a union variant by tag name. Returns (tag_index, &UnionVariantDef).
    pub fn find_union_variant(&self, name: &str) -> Option<(u8, &UnionVariantDef)> {
        self.union_def().and_then(|ud| {
            ud.variants
                .iter()
                .find(|v| v.tag_name.eq_ignore_ascii_case(name))
                .map(|v| (v.tag_index, v))
        })
    }

    /// Construct a TypeDef from a parsed CREATE TYPE statement.
    pub fn from_create_type(
        type_name: &str,
        body: &ast::CreateTypeBody,
        is_builtin: bool,
        sql: String,
    ) -> crate::Result<Self> {
        Ok(match body {
            ast::CreateTypeBody::CustomType {
                params,
                base,
                encode,
                decode,
                operators,
                default,
            } => Self {
                name: type_name.to_string(),
                is_builtin,
                not_null: false,
                is_domain: false,
                sql,
                domain_checks: std::vec::Vec::new(),
                kind: TypeDefKind::Custom {
                    params: params.clone(),
                    base: base.clone(),
                    encode: encode.clone(),
                    decode: decode.clone(),
                    operators: operators.clone(),
                    default: default.clone(),
                },
            },
            ast::CreateTypeBody::Struct(fields) => {
                let struct_fields: Vec<StructFieldDef> = fields
                    .iter()
                    .map(|f| StructFieldDef {
                        name: f.name.to_string(),
                        base_affinity: Affinity::affinity(&f.field_type.name),
                        type_name: f.field_type.name.clone(),
                    })
                    .try_collect()?;
                Self {
                    name: type_name.to_string(),
                    is_builtin,
                    not_null: false,
                    is_domain: false,
                    sql,
                    domain_checks: std::vec::Vec::new(),
                    kind: TypeDefKind::Struct(StructDef {
                        fields: struct_fields,
                    }),
                }
            }
            ast::CreateTypeBody::Union(fields) => {
                if fields.len() > 256 {
                    return Err(crate::LimboError::ParseError(format!(
                        "UNION type cannot have more than 256 variants (got {})",
                        fields.len()
                    )));
                }
                let variants: Vec<UnionVariantDef> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| UnionVariantDef {
                        tag_name: f.name.to_string(),
                        tag_index: i as u8,
                        base_affinity: Affinity::affinity(&f.field_type.name),
                        type_name: f.field_type.name.clone(),
                    })
                    .try_collect()?;
                Self {
                    name: type_name.to_string(),
                    is_builtin,
                    not_null: false,
                    is_domain: false,
                    sql,
                    domain_checks: std::vec::Vec::new(),
                    kind: TypeDefKind::Union(UnionDef {
                        // Arc<[T]> is a shared-pointer boundary: collect directly,
                        // skipping the intermediate allocator Vec.
                        tag_names: variants.iter().map(|v| v.tag_name.clone()).collect(),
                        variants,
                    }),
                }
            }
        })
    }

    /// Construct a TypeDef from a parsed CREATE DOMAIN statement.
    /// Stores constraints as first-class data for propagation to table CHECK constraints.
    pub fn from_domain(
        domain_name: &str,
        base_type: &str,
        not_null: bool,
        constraints: &[ast::DomainConstraint],
        default: Option<Box<ast::Expr>>,
        sql: String,
    ) -> Self {
        Self {
            name: domain_name.to_string(),
            is_builtin: false,
            not_null,
            is_domain: true,
            sql,
            domain_checks: constraints.to_vec(),
            kind: TypeDefKind::Custom {
                params: std::vec::Vec::new(),
                base: base_type.to_string(),
                encode: None,
                decode: None,
                operators: std::vec::Vec::new(),
                default,
            },
        }
    }

    /// The expected input type for `value` in this custom type.
    /// Looks for a `value` parameter with a type annotation.
    /// Falls back to base type if `value` is not declared.
    pub fn value_input_type(&self) -> &str {
        for p in self.params() {
            if p.name.eq_ignore_ascii_case("value") {
                return p.ty.as_deref().unwrap_or_else(|| self.base());
            }
        }
        self.base()
    }

    /// The non-value params (user-provided at column declaration time).
    pub fn user_params(&self) -> impl Iterator<Item = &turso_parser::ast::TypeParam> {
        self.params()
            .iter()
            .filter(|p| !p.name.eq_ignore_ascii_case("value"))
    }

    /// Returns the original SQL used to create this type or domain.
    pub fn to_sql(&self) -> &str {
        &self.sql
    }
}

/// Accumulators for schema loading - kept separate to avoid moving through state variants
struct MakeFromBtreeAccumulators {
    from_sql_indexes: Vec<UnparsedFromSqlIndex>,
    automatic_indices: HashMap<String, Vec<(String, i64)>>,
    /// Store DBSP state table root pages: view_name -> dbsp_state_root_page
    dbsp_state_roots: HashMap<String, i64>,
    /// Store DBSP state table index root pages: view_name -> dbsp_state_index_root_page
    dbsp_state_index_roots: HashMap<String, i64>,
    /// Store materialized view info (SQL and root page) for later creation
    materialized_view_info: HashMap<String, (String, i64)>,
}

/// Phase tracking for async schema loading
#[derive(Default, Debug)]
pub enum MakeFromBtreePhase {
    #[default]
    Init,
    Rewinding,
    FetchingRecord,
    Advancing,
    /// After the sqlite_schema scan completes we walk each sequence's
    /// backing table to reconstruct its descriptor (start / inc / min /
    /// max / cycle). These two phases drive that scan via the
    /// `sequence_cursor` field on the state, yielding `IOResult::IO` on
    /// each cursor I/O — the previous implementation called the
    /// synchronous `populate_sequences(pager)` helper at the EOF of
    /// `FetchingRecord`, which blocked the pager inside an async state
    /// machine (and inside whatever vdbe-level state machine was
    /// driving the schema reparse).
    PopulatingSequencesRewind,
    PopulatingSequencesFetch,
    Done,
}

/// State machine for async schema loading - passed by caller, not stored on Schema
pub struct MakeFromBtreeState {
    phase: MakeFromBtreePhase,
    cursor: Option<BTreeCursor>,
    accumulators: Option<MakeFromBtreeAccumulators>,
    read_tx_active: bool,
    /// Backing tables left to walk during the
    /// `PopulatingSequencesRewind`/`PopulatingSequencesFetch` phases.
    sequence_sources: Vec<SequenceBackingTableSource>,
    /// Cursor for the source currently being scanned (the back of
    /// `sequence_sources` is popped onto this when entering Rewind).
    sequence_cursor: Option<BTreeCursor>,
}

impl Default for MakeFromBtreeState {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeFromBtreeState {
    pub fn new() -> Self {
        Self {
            phase: MakeFromBtreePhase::Init,
            cursor: None,
            accumulators: None,
            read_tx_active: false,
            sequence_sources: vec![],
            sequence_cursor: None,
        }
    }

    /// Cleanup on error - ensures end_read_tx is called
    pub fn cleanup(&mut self, pager: &Pager) {
        if self.read_tx_active {
            pager.end_read_tx();
            self.read_tx_active = false;
        }
        self.cursor = None;
        self.accumulators = None;
    }
}

/// Used to refer to the implicit rowid column in tables without an alias during UPDATE
pub const ROWID_SENTINEL: usize = usize::MAX;

/// The Position in Table for indexes which are arbitrary expressions (index.expr.is_some())
pub const EXPR_INDEX_SENTINEL: usize = usize::MAX;

/// Internal table prefixes that should be protected from CREATE/DROP
pub const RESERVED_TABLE_PREFIXES: [&str; 2] = ["sqlite_", "__turso_internal_"];

/// Check if a table name refers to a system table that should be protected from direct writes
pub fn is_system_table(table_name: &str) -> bool {
    RESERVED_TABLE_PREFIXES
        .iter()
        .any(|prefix| table_name.to_lowercase().starts_with(prefix))
}

pub fn allow_user_dml(table_name: &str) -> bool {
    const NAMES: [&str; 2] = [SCHEMA_TABLE_NAME, SCHEMA_TABLE_NAME_ALT];
    !(NAMES.iter().any(|n| n.eq_ignore_ascii_case(table_name))
        || table_name
            .get(..TURSO_INTERNAL_PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(TURSO_INTERNAL_PREFIX)))
}

// Sequence persistence design
// ===========================
//
// Every sequence — user-created (CREATE SEQUENCE) and implicit
// (AUTOINCREMENT) — is backed by a B-tree table
// `__turso_internal_seq_<name>` with schema (value INTEGER PRIMARY KEY,
// is_called, start, inc, min, max, cycle). The runtime watermark IS the
// disk state: there is no in-memory counter. Every nextval/setval reads
// the current watermark row inside the executing transaction, computes
// the new value, and writes it back — nextval INSERTs a new row;
// setval DELETEs every row then INSERTs one at the requested value.
//
// At commit time the backing table is compacted to one row at MAX(value)
// for ascending sequences or MIN(value) for descending. AUTOINCREMENT
// sequences additionally mirror their watermark into `sqlite_sequence` so
// the high-water mark is readable by SQLite-compatible tools.
//
// Rollback semantics fall out of bundling the backing-table writes with
// the user's transaction:
//   * Commit → the sequence advance is on disk.
//   * Rollback → the sequence advance is not on disk.
// A value emitted only by rolled-back transactions may be re-emitted by a
// later nextval — there is no allocator state retained outside the
// committed row. This matches SQLite AUTOINCREMENT's behavior and does
// not match PostgreSQL's "permanently burned" semantics; consumers
// needing globally unique ids should pair nextval with an INSERT in the
// same transaction.
//
// Cross-process correctness comes for free from the disk-only model:
// under WAL the write lock serializes processes so the next holder
// observes the latest committed watermark.
/// Schema descriptor for a sequence. Pure data — the runtime state lives
/// in the backing table `__turso_internal_seq_<name>` and is read from
/// disk by `Insn::SequenceComputeNext` + surrounding cursor bytecode on
/// every nextval/setval call. See `core/translate/sequence.rs` and the
/// disk-only design notes above.
///
/// `Clone` is implemented so `Arc::make_mut` can in-place edit the `name`
/// field during `ALTER TABLE … RENAME TO …` on an AUTOINCREMENT table —
/// keeping the sequence's identity in sync with the parent table's new
/// name without forcing a schema reparse.
#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,
    pub start_value: i64,
    pub increment_by: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
}

impl Sequence {
    pub fn new(
        name: String,
        start: Option<i64>,
        increment: Option<i64>,
        min_value: Option<i64>,
        max_value: Option<i64>,
        cycle: bool,
    ) -> crate::Result<Self> {
        let increment_by = increment.unwrap_or(1);
        if increment_by == 0 {
            return Err(crate::LimboError::ParseError(
                "INCREMENT must not be zero".to_string(),
            ));
        }
        let min_val = min_value.unwrap_or(if increment_by > 0 { 1 } else { i64::MIN });
        let max_val = max_value.unwrap_or(if increment_by > 0 { i64::MAX } else { -1 });
        if min_val >= max_val {
            return Err(crate::LimboError::ParseError(format!(
                "MINVALUE ({min_val}) must be less than MAXVALUE ({max_val})"
            )));
        }
        let start_val = start.unwrap_or(if increment_by > 0 { min_val } else { max_val });
        if start_val < min_val {
            return Err(crate::LimboError::ParseError(format!(
                "START value ({start_val}) cannot be less than MINVALUE ({min_val})"
            )));
        }
        if start_val > max_val {
            return Err(crate::LimboError::ParseError(format!(
                "START value ({start_val}) cannot be greater than MAXVALUE ({max_val})"
            )));
        }
        Ok(Self {
            name,
            start_value: start_val,
            increment_by,
            min_value: min_val,
            max_value: max_val,
            cycle,
        })
    }
}

/// Type of schema object for conflict checking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectType {
    Table,
    View,
    Index,
}

#[derive(Debug)]
pub struct Schema {
    pub tables: HashMap<String, Arc<Table>>,
    #[cfg(feature = "conn_raw_api")]
    pub(crate) table_names_by_root_page: HashMap<i64, String>,

    /// Track which tables are actually materialized views
    pub materialized_view_names: HashSet<String>,
    /// Store original SQL for materialized views (for .schema command)
    pub materialized_view_sql: HashMap<String, String>,
    /// The incremental view objects (DBSP circuits)
    pub incremental_views: HashMap<String, Arc<Mutex<IncrementalView>>>,

    pub views: ViewsMap,

    /// table_name to list of triggers
    pub triggers: HashMap<String, VecDeque<Arc<Trigger>>>,

    /// table_name to list of indexes for the table
    pub indexes: HashMap<String, VecDeque<Arc<Index>>>,
    pub has_indexes: HashSet<String>,
    pub schema_version: u32,
    /// Statistics collected via ANALYZE for regular B-tree tables and indexes.
    pub analyze_stats: AnalyzeStats,

    /// Mapping from table names to the materialized views that depend on them
    pub table_to_materialized_views: HashMap<String, Vec<String>>,

    /// Track views that exist but have incompatible versions
    pub incompatible_views: HashSet<String>,

    /// View rows in sqlite_schema whose stored SQL failed to parse (e.g.
    /// older versions wrote view column lists without identifier quoting).
    /// The rows are tolerated at load time so the database stays usable;
    /// tracking the names lets DROP VIEW remove them.
    pub broken_views: HashSet<String>,

    /// Root pages of tables/indexes that have been dropped but not yet checkpointed.
    /// In MVCC mode, when a table is dropped, the btree pages are not freed until checkpoint.
    /// integrity_check needs to know about these pages to avoid false positives about "page never used".
    pub dropped_root_pages: HashSet<i64>,

    /// Custom type registry, loaded from sqlite_turso_types
    pub type_registry: HashMap<String, Arc<TypeDef>>,

    pub generated_columns_enabled: bool,
    /// Named sequences (CREATE SEQUENCE)
    pub sequences: HashMap<String, Arc<Sequence>>,
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

fn bootstrap_builtin_types(registry: &mut HashMap<String, Arc<TypeDef>>) -> crate::Result<()> {
    use turso_parser::ast::{Cmd, Stmt};
    use turso_parser::parser::Parser;

    let type_sqls: &[&str] = &[
        #[cfg(feature = "uuid")]
        "CREATE TYPE uuid(value text) BASE blob ENCODE uuid_blob(value) DECODE uuid_str(value) DEFAULT uuid4_str() OPERATOR '<'",
        "CREATE TYPE boolean(value any) BASE integer ENCODE boolean_to_int(value) DECODE CASE WHEN value THEN 1 ELSE 0 END OPERATOR '<'",
        #[cfg(feature = "json")]
        "CREATE TYPE json(value text) BASE text ENCODE json(value) DECODE value",
        #[cfg(feature = "json")]
        "CREATE TYPE jsonb(value text) BASE blob ENCODE jsonb(value) DECODE json(value)",
        "CREATE TYPE varchar(value text, maxlen integer) BASE text ENCODE CASE WHEN length(value) <= maxlen THEN value ELSE RAISE(ABORT, 'value too long for varchar') END DECODE value OPERATOR '<'",
        "CREATE TYPE date(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN date(value) IS NULL THEN RAISE(ABORT, 'invalid date value') ELSE date(value) END DECODE value OPERATOR '<'",
        // ENCODE preserves sub-second precision through strftime + a rtrim pair
        // that strips trailing zeros and the dangling dot, matching PostgreSQL's
        // text format: whole seconds render as `HH:MM:SS` (no .000), trailing
        // zeros are dropped (`.500` -> `.5`), and the dot is removed when no
        // fractional digits remain. `time(...)` / `datetime(...)` would truncate
        // the fraction outright, silently dropping precision on insert.
        // Caveat: Turso's `%f` directive is millisecond resolution, so PG's
        // microsecond inputs are clamped to 3 digits (`.123456` -> `.123`).
        "CREATE TYPE time(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN time(value) IS NULL THEN RAISE(ABORT, 'invalid time value') ELSE rtrim(rtrim(strftime('%H:%M:%f', value), '0'), '.') END DECODE value OPERATOR '<'",
        "CREATE TYPE timestamp(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN datetime(value) IS NULL THEN RAISE(ABORT, 'invalid timestamp value') ELSE rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f', value), '0'), '.') END DECODE value OPERATOR '<'",
        "CREATE TYPE timestamptz(value text) BASE text ENCODE CASE WHEN value IS NULL THEN NULL WHEN datetime(value) IS NULL THEN RAISE(ABORT, 'invalid timestamp value') ELSE rtrim(rtrim(strftime('%Y-%m-%d %H:%M:%f', value), '0'), '.') END DECODE value OPERATOR '<'",
        "CREATE TYPE smallint(value integer) BASE integer ENCODE CASE WHEN value BETWEEN -32768 AND 32767 THEN value ELSE RAISE(ABORT, 'integer out of range for smallint') END DECODE value OPERATOR '<'",
        "CREATE TYPE bigint(value integer) BASE integer",
        "CREATE TYPE inet(value text) BASE text ENCODE validate_ipaddr(value) DECODE value",
        "CREATE TYPE cidr(value text) BASE text ENCODE value DECODE value",
        "CREATE TYPE macaddr(value text) BASE text ENCODE value DECODE value",
        "CREATE TYPE macaddr8(value text) BASE text ENCODE value DECODE value",
        "CREATE TYPE bytea(value blob) BASE blob OPERATOR '<'",
        "CREATE TYPE numeric(value any, precision integer, scale integer) BASE blob ENCODE numeric_encode(value, precision, scale) DECODE numeric_decode(value) OPERATOR '+' numeric_add OPERATOR '-' numeric_sub OPERATOR '*' numeric_mul OPERATOR '/' numeric_div OPERATOR '<' numeric_lt OPERATOR '=' numeric_eq",
    ];

    for sql in type_sqls {
        let mut parser = Parser::new(sql.as_bytes());
        let Ok(Some(Cmd::Stmt(Stmt::CreateType {
            type_name, body, ..
        }))) = parser.next_cmd()
        else {
            return Err(crate::LimboError::InternalError(format!(
                "failed to parse built-in type SQL: {sql}"
            )));
        };

        let type_def = TypeDef::from_create_type(&type_name, &body, true, sql.to_string())?;
        registry.insert(type_name.to_lowercase(), Arc::new(type_def));
    }

    // Register aliases
    let aliases: &[(&str, &str)] = &[
        ("bool", "boolean"),
        ("int2", "smallint"),
        ("int8", "bigint"),
    ];
    for (alias, target) in aliases {
        if let Some(type_def) = registry.get(*target).cloned() {
            registry.insert(alias.to_string(), type_def);
        }
    }
    Ok(())
}

impl Schema {
    fn normalize_table_lookup_name(&self, name: &str) -> String {
        let name = normalize_ident(name);
        if name.eq(SCHEMA_TABLE_NAME_ALT)
            || name.eq(TEMP_SCHEMA_TABLE_NAME)
            || name.eq(TEMP_SCHEMA_TABLE_NAME_ALT)
        {
            SCHEMA_TABLE_NAME.to_string()
        } else {
            name
        }
    }

    /// Create a schema with custom types enabled.
    ///
    /// Panics if a hardcoded built-in type definition is malformed (programmer
    /// bug). Production code that opens user databases should prefer
    /// [`Schema::with_options`] which returns `Result`.
    pub fn new() -> Self {
        Self::with_options(true, &crate::dialect::SqliteDialect)
            .expect("built-in type definitions are malformed")
    }

    pub fn with_options(
        enable_custom_types: bool,
        dialect: &dyn crate::dialect::Dialect,
    ) -> crate::Result<Self> {
        let mut tables: HashMap<String, Arc<Table>> = HashMap::default();
        #[cfg(feature = "conn_raw_api")]
        let mut table_names_by_root_page = HashMap::default();
        let has_indexes = HashSet::default();
        let indexes: HashMap<String, VecDeque<Arc<Index>>> = HashMap::default();
        #[allow(clippy::arc_with_non_send_sync)]
        tables.insert(
            SCHEMA_TABLE_NAME.to_string(),
            Arc::new(Table::BTree(sqlite_schema_table()?.into())),
        );
        #[cfg(feature = "conn_raw_api")]
        table_names_by_root_page.insert(1, SCHEMA_TABLE_NAME.to_string());
        let materialized_view_names = HashSet::default();
        let materialized_view_sql = HashMap::default();
        let incremental_views = HashMap::default();
        let views: ViewsMap = HashMap::default();
        let triggers = HashMap::default();
        let table_to_materialized_views: HashMap<String, Vec<String>> = HashMap::default();
        let incompatible_views = HashSet::default();
        let mut type_registry = HashMap::default();
        if enable_custom_types {
            bootstrap_builtin_types(&mut type_registry)?;
        }
        let mut schema = Self {
            tables,
            #[cfg(feature = "conn_raw_api")]
            table_names_by_root_page,
            materialized_view_names,
            materialized_view_sql,
            incremental_views,
            views,
            triggers,
            indexes,
            has_indexes,
            schema_version: 0,
            analyze_stats: AnalyzeStats::default(),
            table_to_materialized_views,
            incompatible_views,
            broken_views: HashSet::default(),
            dropped_root_pages: HashSet::default(),
            type_registry,
            generated_columns_enabled: false,
            sequences: HashMap::default(),
        };
        dialect.register_catalog(&mut schema, enable_custom_types)?;
        Ok(schema)
    }

    /// Add an `InternalVirtualTable` to the schema's catalog. The wrapped
    /// table appears under the name returned by its `name()` method and is
    /// queryable like any other table. Returns the name actually inserted.
    ///
    /// Intended for callers that want to surface state as a queryable table
    /// without going through `CREATE VIRTUAL TABLE` — for example, extensions
    /// that contribute metadata tables or alternative-dialect catalogs.
    pub fn register_internal_vtab<T>(&mut self, table: T) -> crate::Result<String>
    where
        T: crate::vtab::InternalVirtualTable + 'static,
    {
        let vtab = crate::vtab::VirtualTable::wrap_internal_table(table)?;
        let name = vtab.name.clone();
        let lookup_name = normalize_ident(&name);
        self.tables.insert(
            lookup_name,
            Arc::new(Table::Virtual(Arc::new((*vtab).clone()))),
        );
        Ok(name)
    }

    /// Look up a custom type definition by name.
    /// Custom types are only valid on STRICT tables; pass `is_strict` from the
    /// owning table so that non-STRICT tables never resolve a custom type.
    pub fn get_type_def(&self, type_name: &str, is_strict: bool) -> Option<&Arc<TypeDef>> {
        if !is_strict {
            return None;
        }
        self.type_registry.get(&type_name.to_lowercase())
    }

    /// Look up a custom type definition by name without a strictness check.
    /// Only use this for operations that aren't column-scoped (e.g. DROP TYPE,
    /// CREATE TABLE validation, CAST).
    pub fn get_type_def_unchecked(&self, type_name: &str) -> Option<&Arc<TypeDef>> {
        self.type_registry.get(&type_name.to_lowercase())
    }

    /// Resolve a custom type fully: look it up (with strictness gate) and chase
    /// the base-type chain to the ultimate primitive.
    /// Returns `Ok(None)` if the type is not registered (or the table isn't strict).
    pub fn resolve_type(
        &self,
        type_name: &str,
        is_strict: bool,
    ) -> crate::Result<Option<ResolvedType>> {
        if !is_strict {
            return Ok(None);
        }
        self.resolve_type_unchecked(type_name)
    }

    /// Resolve a custom type fully without a strictness check.
    /// Returns `Ok(None)` if the type is not in the registry.
    pub fn resolve_type_unchecked(&self, type_name: &str) -> crate::Result<Option<ResolvedType>> {
        let key = type_name.to_lowercase();
        if !self.type_registry.contains_key(&key) {
            return Ok(None);
        }
        let (primitive, chain) = self.resolve_base_type_chain(type_name)?;
        Ok(Some(ResolvedType { primitive, chain }))
    }

    pub fn remove_type(&mut self, type_name: &str) {
        self.type_registry.remove(&type_name.to_lowercase());
    }

    /// Chase the base type chain: domain_a → domain_b → integer
    /// Returns (ultimate_primitive, ordered_chain_of_TypeDefs)
    /// The chain is ordered from child to ancestor.
    /// Errors on cycles or missing intermediate types.
    pub fn resolve_base_type_chain(
        &self,
        type_name: &str,
    ) -> crate::Result<(String, Vec<Arc<TypeDef>>)> {
        let mut chain = vec![];
        let mut visited = std::collections::HashSet::new();
        let mut current = type_name.to_lowercase();

        loop {
            if !visited.insert(current.clone()) {
                return Err(crate::LimboError::ParseError(format!(
                    "circular type dependency detected: {current}"
                )));
            }
            match self.type_registry.get(&current) {
                Some(td) => {
                    chain.try_push(Arc::clone(td))?;
                    current = td.base().to_lowercase();
                }
                None => {
                    // current is not in the registry — it's a primitive
                    return Ok((current, chain));
                }
            }
        }
    }

    /// Parse a CREATE TYPE SQL string and add the type to the in-memory registry.
    pub fn add_type_from_sql(&mut self, sql: &str) -> crate::Result<()> {
        use turso_parser::ast::{Cmd, Stmt};
        use turso_parser::parser::Parser;

        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd();
        match cmd {
            Ok(Some(Cmd::Stmt(Stmt::CreateType {
                type_name, body, ..
            }))) => {
                let type_def =
                    TypeDef::from_create_type(&type_name, &body, false, sql.to_string())?;
                self.type_registry
                    .insert(type_name.to_lowercase(), Arc::new(type_def));
            }
            Ok(Some(Cmd::Stmt(Stmt::CreateDomain {
                domain_name,
                base_type,
                default,
                not_null,
                constraints,
                ..
            }))) => {
                let type_def = TypeDef::from_domain(
                    &domain_name,
                    &base_type,
                    not_null,
                    &constraints,
                    default,
                    sql.to_string(),
                );
                self.type_registry
                    .insert(domain_name.to_lowercase(), Arc::new(type_def));
            }
            _ => {
                return Err(crate::LimboError::ParseError(format!(
                    "invalid type sql: {sql}"
                )));
            }
        }
        Ok(())
    }

    /// Load type definitions from CREATE TYPE SQL strings and resolve custom
    /// type affinities on all STRICT tables. This is the shared entry point
    /// used by both initial database open and schema reparse.
    pub fn load_type_definitions(&mut self, type_sqls: &[String]) -> crate::Result<()> {
        for sql in type_sqls {
            self.add_type_from_sql(sql)?;
        }
        self.resolve_all_custom_type_affinities()?;
        Ok(())
    }

    /// Resolve custom type affinities for all STRICT tables in the schema.
    /// Call this after loading user-defined types from __turso_internal_types
    /// so that columns declared with custom types use the BASE type's affinity.
    pub fn resolve_all_custom_type_affinities(&mut self) -> Result<()> {
        let mut tables: SmallVec<[(String, Arc<Table>); 8]> = SmallVec::with_capacity(8);
        for (name, table) in self.tables.iter().filter(|(_, t)| {
            t.is_strict()
                && t.btree().is_some_and(|bt| {
                    bt.columns
                        .iter()
                        .any(|c| self.get_type_def_unchecked(&c.ty_str).is_some())
                })
        }) {
            let bt = table.btree().expect("checked btree table");
            let mut modified = (*bt).clone();
            modified.resolve_custom_type_affinities(self);
            modified.propagate_domain_constraints(self)?;
            tables.push((name.clone(), Arc::new(Table::BTree(Arc::new(modified)))));
        }
        for (name, table) in tables {
            self.tables.insert(name, table);
        }
        Ok(())
    }

    pub fn is_unique_idx_name(&self, name: &str) -> bool {
        !self
            .indexes
            .iter()
            .any(|idx| idx.1.iter().any(|i| i.name == name))
    }

    pub fn add_materialized_view(&mut self, view: IncrementalView, table: Arc<Table>, sql: String) {
        let name = normalize_ident(view.name());

        // Add to tables (so it appears as a regular table)
        #[cfg(feature = "conn_raw_api")]
        self.register_table_root_page(&name, table.as_ref());
        self.tables.insert(name.clone(), table);

        // Track that this is a materialized view
        self.materialized_view_names.insert(name.clone());
        self.materialized_view_sql.insert(name.clone(), sql);

        // Store the incremental view (DBSP circuit)
        self.incremental_views
            .insert(name, Arc::new(Mutex::new(view)));
    }

    pub fn get_materialized_view(&self, name: &str) -> Option<Arc<Mutex<IncrementalView>>> {
        let name = normalize_ident(name);
        self.incremental_views.get(&name).cloned()
    }

    /// Check if DBSP state table exists with the current version
    pub fn has_compatible_dbsp_state_table(&self, view_name: &str) -> bool {
        let view_name = normalize_ident(view_name);
        let expected_table_name = format!("{DBSP_TABLE_PREFIX}{DBSP_CIRCUIT_VERSION}_{view_name}");

        // Check if a table with the expected versioned name exists
        self.tables.contains_key(&expected_table_name)
    }

    pub fn is_materialized_view(&self, name: &str) -> bool {
        let name = normalize_ident(name);
        self.materialized_view_names.contains(&name)
    }

    /// Apply a function to a table's incompatible dependent materialized views
    pub fn with_incompatible_dependent_views<F, T>(&self, table_name: &str, f: F) -> T
    where
        F: FnOnce(&[&String]) -> T,
    {
        let table_name = normalize_ident(table_name);
        let mut views: SmallVec<[&String; 8]> = SmallVec::with_capacity(8);

        // Get all materialized views that depend on this table
        if let Some(v) = self.table_to_materialized_views.get(&table_name) {
            v.iter()
                .filter(|name| self.incompatible_views.contains(&**name))
                .for_each(|n| views.push(n));
        }
        f(&views)
    }

    pub fn remove_view(&mut self, name: &str) -> Result<()> {
        let name = normalize_ident(name);

        if self.views.contains_key(&name) {
            self.views.remove(&name);
            Ok(())
        } else if self.materialized_view_names.contains(&name) {
            // Remove from tables
            self.remove_table(&name);

            // Remove DBSP state table and its indexes from in-memory schema
            let dbsp_table_name = format!("{DBSP_TABLE_PREFIX}{DBSP_CIRCUIT_VERSION}_{name}");
            self.remove_table(&dbsp_table_name);
            self.remove_indices_for_table(&dbsp_table_name);

            // Remove from materialized view tracking
            self.materialized_view_names.remove(&name);
            self.materialized_view_sql.remove(&name);
            self.incremental_views.remove(&name);

            // Remove from table_to_materialized_views dependencies
            for views in self.table_to_materialized_views.values_mut() {
                views.retain(|v| v != &name);
            }

            Ok(())
        } else {
            Err(crate::LimboError::ParseError(format!(
                "no such view: {name}"
            )))
        }
    }

    /// Register that a materialized view depends on a table
    pub fn add_materialized_view_dependency(&mut self, table_name: &str, view_name: &str) {
        let table_name = normalize_ident(table_name);
        let view_name = normalize_ident(view_name);

        self.table_to_materialized_views
            .entry(table_name)
            .or_insert_with(|| vec![])
            .push(view_name);
    }

    /// Get all materialized views that depend on a given table
    pub fn get_dependent_materialized_views(&self, table_name: &str) -> Vec<String> {
        if self.table_to_materialized_views.is_empty() {
            return vec![];
        }
        let table_name = normalize_ident(table_name);
        self.table_to_materialized_views
            .get(&table_name)
            .cloned()
            .unwrap_or_else(|| vec![])
    }

    /// Add a regular (non-materialized) view
    pub fn add_view(&mut self, view: View) -> Result<()> {
        self.check_object_name_conflict(&view.name, SchemaObjectType::View)?;
        let name = normalize_ident(&view.name);
        self.views.insert(name, Arc::new(view));
        Ok(())
    }

    /// Get a regular view by name
    pub fn get_view(&self, name: &str) -> Option<Arc<View>> {
        let name = normalize_ident(name);
        self.views.get(&name).cloned()
    }

    pub fn add_trigger(&mut self, trigger: Trigger, table_name: &str) -> Result<()> {
        // Triggers have their own namespace and duplicate trigger names
        // are checked in `translate_create_trigger`
        let table_name = normalize_ident(table_name);

        // See [Schema::add_index] for why we push to the front of the deque.
        self.triggers
            .entry(table_name)
            .or_default()
            .push_front(Arc::new(trigger));

        Ok(())
    }

    pub fn remove_trigger(&mut self, name: &str) -> Result<()> {
        let name = normalize_ident(name);

        let mut removed = false;
        for triggers_list in self.triggers.values_mut() {
            for i in 0..triggers_list.len() {
                let trigger = &triggers_list[i];
                if normalize_ident(&trigger.name) == name {
                    removed = true;
                    triggers_list.remove(i);
                    break;
                }
            }
            if removed {
                break;
            }
        }
        if !removed {
            return Err(crate::LimboError::ParseError(format!(
                "no such trigger: {name}"
            )));
        }
        Ok(())
    }
    pub fn remove_triggers_for_table(&mut self, table_name: &str) {
        let table_name = normalize_ident(table_name);
        self.triggers.remove(&table_name);
    }

    pub fn set_trigger_target_database_id(
        &mut self,
        trigger_name: &str,
        target_database_id: usize,
    ) -> Result<()> {
        let trigger_name = normalize_ident(trigger_name);
        let trigger = self
            .triggers
            .values_mut()
            .flatten()
            .find(|trigger| normalize_ident(&trigger.name) == trigger_name)
            .ok_or_else(|| {
                crate::LimboError::InternalError(format!(
                    "new trigger {trigger_name} was not loaded into the schema"
                ))
            })?;
        Arc::make_mut(trigger).target_database_id = Some(target_database_id);
        Ok(())
    }

    /// Like [`remove_triggers_for_table`] but only removes triggers whose
    /// `target_database_id` matches `target_db` (or is `None`, meaning
    /// "targets the parent schema's table of this name", which also
    /// applies). Used from `DROP TABLE main.t` to clean up temp triggers
    /// without accidentally removing ones that target `temp.t` or
    /// `aux.t` (the plain `remove_triggers_for_table` keys only on
    /// table name).
    pub fn remove_triggers_for_table_with_db(&mut self, table_name: &str, target_db: usize) {
        let table_name = normalize_ident(table_name);
        let Some(bucket) = self.triggers.get_mut(&table_name) else {
            return;
        };
        // Check once whether this schema has a table with the same name.
        // If it does, unqualified triggers resolve to that local table,
        // not to the one being dropped in `target_db`.
        let has_shadow_table = self.tables.contains_key(&table_name);
        bucket.retain(|trigger| {
            match trigger.target_database_id {
                Some(db) => db != target_db,
                // Unqualified triggers resolve to the local schema's table
                // first. Only remove when no local table shadows the name.
                None => has_shadow_table,
            }
        });
        if bucket.is_empty() {
            self.triggers.remove(&table_name);
        }
    }

    pub fn get_trigger_for_table(&self, table_name: &str, name: &str) -> Option<Arc<Trigger>> {
        let table_name = normalize_ident(table_name);
        let name = normalize_ident(name);
        self.triggers
            .get(&table_name)
            .and_then(|triggers| triggers.iter().find(|t| t.name == name).cloned())
    }

    pub fn get_triggers_for_table(
        &self,
        table_name: &str,
    ) -> impl Iterator<Item = &Arc<Trigger>> + Clone {
        let table_name = normalize_ident(table_name);
        self.triggers
            .get(&table_name)
            .map(|triggers| triggers.iter())
            .unwrap_or_default()
    }

    pub fn get_trigger(&self, name: &str) -> Option<Arc<Trigger>> {
        let name = normalize_ident(name);
        self.triggers
            .values()
            .flatten()
            .find(|t| t.name == name)
            .cloned()
    }

    pub fn add_btree_table(&mut self, table: Arc<BTreeTable>) -> Result<()> {
        self.check_object_name_conflict(&table.name, SchemaObjectType::Table)?;
        let name = normalize_ident(&table.name);
        #[cfg(feature = "conn_raw_api")]
        self.table_names_by_root_page
            .insert(table.root_page, name.clone());
        self.tables.insert(name, Table::BTree(table).into());
        Ok(())
    }

    pub fn add_virtual_table(&mut self, table: Arc<VirtualTable>) -> Result<()> {
        self.check_object_name_conflict(&table.name, SchemaObjectType::Table)?;
        let name = normalize_ident(&table.name);
        self.tables.insert(name, Table::Virtual(table).into());
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        let name = self.normalize_table_lookup_name(name);
        self.tables.get(&name).cloned()
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn table_name_for_root_page(&self, root_page: i64) -> Option<&str> {
        self.table_names_by_root_page
            .get(&root_page)
            .map(String::as_str)
    }

    pub fn remove_table(&mut self, table_name: &str) {
        let name = normalize_ident(table_name);
        #[cfg(feature = "conn_raw_api")]
        {
            if let Some(table) = self.tables.remove(&name) {
                self.unregister_table_root_page(&table);
            }
        }
        #[cfg(not(feature = "conn_raw_api"))]
        {
            self.tables.remove(&name);
        }
        self.analyze_stats.remove_table(&name);

        // If this was a materialized view, also clean up the metadata
        if self.materialized_view_names.remove(&name) {
            self.incremental_views.remove(&name);
            self.materialized_view_sql.remove(&name);
        }
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn register_table_root_page(&mut self, name: &str, table: &Table) {
        if let Table::BTree(table) = table {
            self.table_names_by_root_page
                .insert(table.root_page, normalize_ident(name));
        }
    }

    #[cfg(feature = "conn_raw_api")]
    pub fn unregister_table_root_page(&mut self, table: &Table) {
        if let Table::BTree(table) = table {
            self.table_names_by_root_page.remove(&table.root_page);
        }
    }

    pub fn get_btree_table(&self, name: &str) -> Option<Arc<BTreeTable>> {
        let name = self.normalize_table_lookup_name(name);
        if let Some(table) = self.tables.get(&name) {
            table.btree()
        } else {
            None
        }
    }

    pub fn add_index(&mut self, index: Arc<Index>) -> Result<()> {
        self.check_object_name_conflict(&index.name, SchemaObjectType::Index)?;
        let table_name = normalize_ident(&index.table_name);
        // We must add the new index to the front of the deque, because SQLite stores index definitions as a linked list
        // where the newest parsed index entry is at the head of list. If we would add it to the back of a regular Vec for example,
        // then we would evaluate ON CONFLICT DO UPDATE clauses in the wrong index iteration order and UPDATE the wrong row.
        // Additionally, REPLACE indexes must go after all the non-REPLACE indexes so that
        // non-mutating conflict resolutions all happen before mutating ones, ensuring that
        // no half-committed state is left behind.
        let is_replace = index.on_conflict == Some(ResolveType::Replace);
        let indexes_for_table = self.indexes.entry(table_name).or_default();
        if is_replace {
            // REPLACE indexes sort newest-first among themselves.
            let first_replace = indexes_for_table
                .iter()
                .position(|idx| idx.on_conflict == Some(ResolveType::Replace));
            let pos = first_replace.unwrap_or(indexes_for_table.len());
            indexes_for_table.insert(pos, index);
        } else {
            // Non-REPLACE indexes go at the front, newest first.
            indexes_for_table.push_front(index);
        }
        turso_debug_assert!(
            indexes_for_table
                .iter()
                .position(|idx| idx.on_conflict == Some(ResolveType::Replace))
                .is_none_or(|first_replace| {
                    indexes_for_table
                        .iter()
                        .skip(first_replace)
                        .all(|idx| idx.on_conflict == Some(ResolveType::Replace))
                }),
            "REPLACE indexes must form a contiguous suffix"
        );
        Ok(())
    }

    pub fn get_indices(&self, table_name: &str) -> impl Iterator<Item = &Arc<Index>> {
        let name = normalize_ident(table_name);
        self.indexes
            .get(&name)
            .map(|v| v.iter())
            .unwrap_or_default()
            .filter(|i| !i.is_backing_btree_index())
    }

    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    pub fn has_fts_index(&self, table_name: &str) -> bool {
        self.get_indices(table_name).any(|idx| {
            idx.index_method.as_ref().is_some_and(|m| {
                m.definition().method_name == crate::index_method::fts::FTS_INDEX_METHOD_NAME
            })
        })
    }

    pub fn get_index(&self, table_name: &str, index_name: &str) -> Option<&Arc<Index>> {
        let name = normalize_ident(table_name);
        self.indexes
            .get(&name)?
            .iter()
            .find(|index| index.name == index_name)
    }

    pub fn remove_indices_for_table(&mut self, table_name: &str) {
        let name = normalize_ident(table_name);
        self.indexes.remove(&name);
        self.analyze_stats.remove_table(&name);
    }

    pub fn remove_index(&mut self, idx: &Index) {
        let name = normalize_ident(&idx.table_name);
        self.indexes
            .get_mut(&name)
            .expect("Must have the index")
            .retain_mut(|other_idx| other_idx.name != idx.name);
        self.analyze_stats.remove_index(&name, &idx.name);
    }

    pub fn table_has_indexes(&self, table_name: &str) -> bool {
        let name = normalize_ident(table_name);
        self.has_indexes.contains(&name)
    }

    pub fn table_set_has_index(&mut self, table_name: &str) {
        self.has_indexes.insert(table_name.to_string());
    }

    /// Update [Schema] by scanning the first root page (sqlite_schema)
    /// Returns IOResultOr<()> to allow async operation with external IO loop
    pub fn make_from_btree(
        &mut self,
        state: &mut MakeFromBtreeState,
        mv_cursor: Option<Arc<RwLock<MvCursor>>>,
        pager: &Arc<Pager>,
        syms: &SymbolTable,
        dialect: &dyn crate::dialect::Dialect,
    ) -> IOResultOr<()> {
        let result = self.make_from_btree_internal(state, mv_cursor, pager, syms, dialect);
        if result.is_err() {
            state.cleanup(pager);
        } else if let Ok(IOResult::Done(..)) = result {
            turso_assert!(
                !state.read_tx_active,
                "make_from_btree must properly cleanup internal state in case of success"
            );
        }
        result
    }

    fn make_from_btree_internal(
        &mut self,
        state: &mut MakeFromBtreeState,
        mv_cursor: Option<Arc<RwLock<MvCursor>>>,
        pager: &Arc<Pager>,
        syms: &SymbolTable,
        dialect: &dyn crate::dialect::Dialect,
    ) -> IOResultOr<()> {
        loop {
            tracing::debug!("make_from_btree: state.phase={:?}", state.phase);
            match &state.phase {
                MakeFromBtreePhase::Init => {
                    if mv_cursor.is_some() {
                        return Err(crate::LimboError::ParseError(
                            "MVCC is not supported for make_from_btree schema recovery".to_string(),
                        )
                        .into());
                    }

                    state.cursor = Some(BTreeCursor::new_table(Arc::clone(pager), 1, 10));
                    pager.begin_read_tx()?;
                    state.read_tx_active = true;

                    state.accumulators = Some(MakeFromBtreeAccumulators {
                        from_sql_indexes: Vec::try_with_capacity_ext(10)?,
                        automatic_indices: HashMap::with_capacity_and_hasher(10, FxBuildHasher),
                        dbsp_state_roots: HashMap::default(),
                        dbsp_state_index_roots: HashMap::default(),
                        materialized_view_info: HashMap::default(),
                    });

                    state.phase = MakeFromBtreePhase::Rewinding;
                }

                MakeFromBtreePhase::Rewinding => {
                    let cursor = state
                        .cursor
                        .as_mut()
                        .expect("cursor must be initialized in Init phase");
                    return_if_io!(cursor.rewind());
                    state.phase = MakeFromBtreePhase::FetchingRecord;
                }

                MakeFromBtreePhase::FetchingRecord => {
                    let cursor = state
                        .cursor
                        .as_mut()
                        .expect("cursor must be initialized in Init phase");
                    let row = return_if_io!(cursor.record());

                    let Some(row) = row else {
                        // EOF on the sqlite_schema scan. Hand off to the
                        // async sequence-descriptor walk — pulled out of
                        // the prior synchronous `populate_sequences(pager)`
                        // call so the schema state machine can yield
                        // `IOResult::IO` on each cursor read instead of
                        // blocking the pager. The Rewind/Fetch phases
                        // below pop the back of `state.sequence_sources`,
                        // install the descriptor, drop the cursor, and
                        // repeat until empty.
                        state.sequence_sources = self.sequence_backing_tables();
                        state.cursor = None;
                        state.phase = MakeFromBtreePhase::PopulatingSequencesRewind;
                        continue;
                    };

                    // Process the row (no IO - CPU only)
                    // sqlite schema table has 5 columns: type, name, tbl_name, rootpage, sql
                    let ty_value = row.get_value(0)?;
                    let ValueRef::Text(ty) = ty_value else {
                        return Err(
                            LimboError::ConversionError("Expected text value".into()).into()
                        );
                    };
                    let ValueRef::Text(name) = row.get_value(1)? else {
                        return Err(
                            LimboError::ConversionError("Expected text value".into()).into()
                        );
                    };
                    let table_name_value = row.get_value(2)?;
                    let ValueRef::Text(table_name) = table_name_value else {
                        return Err(
                            LimboError::ConversionError("Expected text value".into()).into()
                        );
                    };
                    let root_page_value = row.get_value(3)?;
                    let ValueRef::Numeric(crate::numeric::Numeric::Integer(root_page)) =
                        root_page_value
                    else {
                        return Err(
                            LimboError::ConversionError("Expected integer value".into()).into()
                        );
                    };
                    let sql_value = row.get_value(4)?;
                    let sql_textref = match sql_value {
                        ValueRef::Text(sql) => Some(sql),
                        _ => None,
                    };
                    let sql = sql_textref.map(|s| s.as_str());

                    let acc = state
                        .accumulators
                        .as_mut()
                        .expect("accumulators must be initialized in Init phase");
                    // `make_from_btree` is called during database open before
                    // any connection exists, so there is no attached catalog
                    // to consult. Any `CREATE TEMP TRIGGER ... ON aux.x` row
                    // maps to `Some(INVALID_DB_ID)` until a connection-scoped
                    // reparse runs with a real resolver.
                    self.handle_schema_row(
                        &ty,
                        &name,
                        &table_name,
                        root_page,
                        sql,
                        syms,
                        &mut acc.from_sql_indexes,
                        &mut acc.automatic_indices,
                        &mut acc.dbsp_state_roots,
                        &mut acc.dbsp_state_index_roots,
                        &mut acc.materialized_view_info,
                        &|_| None,
                        dialect,
                    )?;

                    state.phase = MakeFromBtreePhase::Advancing;
                }

                MakeFromBtreePhase::Advancing => {
                    let cursor = state
                        .cursor
                        .as_mut()
                        .expect("cursor must be initialized in Init phase");
                    return_if_io!(cursor.next());
                    state.phase = MakeFromBtreePhase::FetchingRecord;
                }

                MakeFromBtreePhase::PopulatingSequencesRewind => {
                    // Either no sources left → finalize, or pop the next
                    // source and rewind its cursor.
                    if state.sequence_sources.is_empty() {
                        pager.end_read_tx();
                        state.read_tx_active = false;

                        let acc = state
                            .accumulators
                            .take()
                            .expect("accumulators must be initialized in Init phase");
                        self.populate_indices(
                            syms,
                            acc.from_sql_indexes,
                            acc.automatic_indices,
                            mv_cursor.is_some(),
                        )?;
                        self.populate_materialized_views(
                            acc.materialized_view_info,
                            acc.dbsp_state_roots,
                            acc.dbsp_state_index_roots,
                        )?;

                        state.phase = MakeFromBtreePhase::Done;
                        return Ok(IOResult::Done(()));
                    }
                    // Drop any cursor from a previous source before opening
                    // the new one (Drop logic on the BTreeCursor releases
                    // its page pins).
                    state.sequence_cursor = None;
                    let source = state
                        .sequence_sources
                        .last()
                        .expect("non-empty checked above");
                    // MVCC backing tables that haven't been checkpointed
                    // yet carry the negative-root sentinel; the pager
                    // can't read them directly. Skip — the SQL fallback
                    // (`Connection::populate_sequences_via_sql`) will
                    // load them via the MVCC row layer.
                    if source.root_page <= 0 {
                        state.sequence_sources.pop();
                        continue;
                    }
                    let cursor =
                        BTreeCursor::new_table(pager.clone(), source.root_page, source.num_columns);
                    state.sequence_cursor = Some(cursor);
                    let cursor = state.sequence_cursor.as_mut().expect("just set");
                    return_if_io!(cursor.rewind());
                    state.phase = MakeFromBtreePhase::PopulatingSequencesFetch;
                }

                MakeFromBtreePhase::PopulatingSequencesFetch => {
                    let cursor = state
                        .sequence_cursor
                        .as_mut()
                        .expect("cursor must be initialized in PopulatingSequencesRewind");
                    let record = return_if_io!(cursor.record());
                    let source = state.sequence_sources.pop().expect("at least one source");
                    let record = record.ok_or_else(|| {
                        LimboError::Corrupt(format!(
                            "internal sequence backing table for \"{}\" is empty; \
                             the descriptor metadata row must always be present",
                            source.sequence_name
                        ))
                    })?;
                    let metadata = Self::read_sequence_metadata(record).ok_or_else(|| {
                        LimboError::Corrupt(format!(
                            "internal sequence backing table for \"{}\" descriptor \
                             row is malformed (expected integers for \
                             start/inc/min/max/cycle)",
                            source.sequence_name
                        ))
                    })?;
                    self.install_sequence_descriptor(&source.sequence_name, metadata)?;
                    // Drop the cursor before transitioning back so we
                    // release its page pins before the next source's
                    // rewind starts.
                    state.sequence_cursor = None;
                    state.phase = MakeFromBtreePhase::PopulatingSequencesRewind;
                }

                MakeFromBtreePhase::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// Populate indices parsed from the schema.
    /// from_sql_indexes: indices explicitly created with CREATE INDEX
    /// automatic_indices: indices created automatically for primary key and unique constraints
    pub fn populate_indices(
        &mut self,
        syms: &SymbolTable,
        from_sql_indexes: Vec<UnparsedFromSqlIndex>,
        automatic_indices: HashMap<String, Vec<(String, i64)>>,
        mvcc_enabled: bool,
    ) -> Result<()> {
        for unparsed_sql_from_index in from_sql_indexes {
            let table = self
                .get_btree_table(&unparsed_sql_from_index.table_name)
                .ok_or_else(|| {
                    LimboError::Corrupt(format!(
                        "sqlite_schema contains index for missing table '{}': rootpage={} sql={}",
                        unparsed_sql_from_index.table_name,
                        unparsed_sql_from_index.root_page,
                        unparsed_sql_from_index.sql
                    ))
                })?;
            let index = Index::from_sql(
                syms,
                &unparsed_sql_from_index.sql,
                unparsed_sql_from_index.root_page,
                table.as_ref(),
            )?;
            self.add_index(Arc::new(index))?;
        }

        for automatic_index in automatic_indices {
            // Autoindexes must be parsed in definition order.
            // The SQL statement parser enforces that the column definitions come first, and compounds are defined after that,
            // e.g. CREATE TABLE t (a, b, UNIQUE(a, b)), and you can't do something like CREATE TABLE t (a, b, UNIQUE(a, b), c);
            // Hence, we can process the singles first (unique_set.columns.len() == 1), and then the compounds (unique_set.columns.len() > 1).
            let table = self.get_btree_table(&automatic_index.0).ok_or_else(|| {
                LimboError::Corrupt(format!(
                    "sqlite_schema contains automatic index for missing table '{}': indexes={:?}",
                    automatic_index.0, automatic_index.1
                ))
            })?;
            let mut automatic_indexes = automatic_index.1;
            automatic_indexes.reverse(); // reverse so we can pop() without shifting array elements, while still processing in left-to-right order

            // we must process unique_sets in this exact order in order to emit automatic indices schema entries in the same order
            let mut pk_index_added = false;
            for unique_set in &table.unique_sets {
                if unique_set.is_primary_key {
                    assert!(
                        table.primary_key_columns.len() == unique_set.columns.len(),
                        "trying to add a {}-column primary key index for table {}, but the table has {} primary key columns",
                        unique_set.columns.len(),
                        table.name,
                        table.primary_key_columns.len()
                    );
                    // Add composite primary key index
                    assert!(
                        !pk_index_added,
                        "trying to add a second primary key index for table {}",
                        table.name
                    );
                    pk_index_added = true;

                    if unique_set.columns.len() == 1 {
                        let col_name = &unique_set.columns.first().unwrap().name;
                        let Some((_, column)) = table.get_column(col_name) else {
                            return Err(LimboError::ParseError(format!(
                                "Column {col_name} not found in table {}",
                                table.name
                            )));
                        };
                        if column.is_rowid_alias() {
                            // rowid alias, no index needed
                            continue;
                        }
                    }

                    if let Some(index_entry) = automatic_indexes.pop() {
                        self.add_index(Arc::new(Index::automatic_from_primary_key(
                            table.as_ref(),
                            index_entry,
                            unique_set.columns.len(),
                            unique_set.conflict_clause,
                            &unique_set.columns,
                        )?))?;
                    } else if mvcc_enabled {
                        // In MVCC mode, automatic indices might not be fully populated yet during recovery
                        // Skip creating this index - it will be added later when its schema row is processed
                        continue;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Missing automatic index entry for primary key on table {}",
                            table.name
                        )));
                    }
                } else {
                    // Add composite unique index
                    let mut column_indices_and_sort_orders =
                        Vec::try_with_capacity_ext(unique_set.columns.len())?;
                    for unique_column in unique_set.columns.iter() {
                        let Some((pos_in_table, _)) = table.get_column(&unique_column.name) else {
                            return Err(crate::LimboError::ParseError(format!(
                                "Column {} not found in table {}",
                                unique_column.name, table.name
                            )));
                        };
                        column_indices_and_sort_orders
                            .push_within_capacity((pos_in_table, unique_column.sort_order))
                            .expect("unique columns vector was preallocated to its input length");
                    }
                    if let Some(index_entry) = automatic_indexes.pop() {
                        self.add_index(Arc::new(Index::automatic_from_unique(
                            table.as_ref(),
                            index_entry,
                            column_indices_and_sort_orders,
                            unique_set.conflict_clause,
                            &unique_set.columns,
                        )?))?;
                    } else if mvcc_enabled {
                        // In MVCC mode, automatic indices might not be fully populated yet during recovery
                        // Skip creating this index - it will be added later when its schema row is processed
                        continue;
                    } else {
                        return Err(LimboError::InternalError(format!(
                            "Missing automatic index entry for UNIQUE constraint on table {}",
                            table.name
                        )));
                    }
                }
            }

            // In MVCC mode during recovery, not all automatic index schema rows might be visible yet
            // during incremental schema reparsing, so we may have extra entries
            if !mvcc_enabled {
                assert!(
                    automatic_indexes.is_empty(),
                    "all automatic indexes parsed from sqlite_schema should have been consumed, but {} remain",
                    automatic_indexes.len()
                );
            }
        }
        Ok(())
    }

    /// Populate materialized views parsed from the schema.
    pub fn populate_materialized_views(
        &mut self,
        materialized_view_info: HashMap<String, (String, i64)>,
        dbsp_state_roots: HashMap<String, i64>,
        dbsp_state_index_roots: HashMap<String, i64>,
    ) -> Result<()> {
        for (view_name, (sql, main_root)) in materialized_view_info {
            // Look up the DBSP state root for this view
            // If missing, it means version mismatch - skip this view
            // Check if we have a compatible DBSP state root
            let dbsp_state_root = if let Some(&root) = dbsp_state_roots.get(&view_name) {
                root
            } else {
                tracing::warn!(
                    "Materialized view '{}' has incompatible version or missing DBSP state table",
                    view_name
                );
                // Track this as an incompatible view
                self.incompatible_views.insert(view_name.clone());
                // Use a dummy root page - the view won't be usable anyway
                0
            };

            // Look up the DBSP state index root (may not exist for older schemas)
            let dbsp_state_index_root =
                dbsp_state_index_roots.get(&view_name).copied().unwrap_or(0);

            // Register the DBSP state index so integrity check can account for its pages.
            if dbsp_state_index_root > 0 && dbsp_state_root > 0 {
                let mut index = create_dbsp_state_index(dbsp_state_index_root);
                let dbsp_table_name =
                    format!("{DBSP_TABLE_PREFIX}{DBSP_CIRCUIT_VERSION}_{view_name}");
                index.name = format!("sqlite_autoindex_{dbsp_table_name}_1");
                index.table_name = dbsp_table_name;
                if let Err(e) = self.add_index(std::sync::Arc::new(index)) {
                    if !e.to_string().contains("already exists") {
                        return Err(e);
                    }
                }
            }

            // Create the IncrementalView with all root pages
            let incremental_view = IncrementalView::from_sql(
                &sql,
                self,
                main_root,
                dbsp_state_root,
                dbsp_state_index_root,
            )?;
            let referenced_tables = incremental_view.get_referenced_table_names();

            // Create a BTreeTable for the materialized view
            let cols = incremental_view.column_schema.flat_columns();
            let logical_to_physical_map =
                BTreeTable::build_logical_to_physical_map(&cols, &[], true);
            let table = Arc::new(Table::BTree(Arc::new(BTreeTable {
                name: view_name.clone(),
                root_page: main_root,
                columns: cols,
                primary_key_columns: vec![],
                has_rowid: true,
                is_strict: false,
                has_autoincrement: false,
                foreign_keys: vec![],
                check_constraints: vec![],
                rowid_alias_conflict_clause: None,
                unique_sets: vec![],
                has_virtual_columns: false,
                logical_to_physical_map,
                column_dependencies: Default::default(),
            })));

            // Only add to schema if compatible
            if !self.incompatible_views.contains(&view_name) {
                self.add_materialized_view(incremental_view, table, sql);
            }

            // Register dependencies regardless of compatibility
            for table_name in referenced_tables {
                self.add_materialized_view_dependency(&table_name, &view_name);
            }
        }
        Ok(())
    }

    /// Yield (backing_table_name, sequence_name) for every backing table
    /// currently in the schema. Shared shape for the SQL-based descriptor
    /// loader in `Connection` so the prefix-strip lives in one place.
    pub fn sequence_backing_table_names(&self) -> Vec<(String, String)> {
        self.tables
            .keys()
            .filter_map(|name| {
                let seq_name = name.strip_prefix(SEQ_BACKING_TABLE_PREFIX)?;
                Some((name.clone(), seq_name.to_string()))
            })
            .try_collect()
            .expect(crate::alloc::ALLOC_ERR_MSG)
    }

    fn sequence_backing_tables(&self) -> Vec<SequenceBackingTableSource> {
        self.tables
            .iter()
            .filter_map(|(name, table)| {
                let bt = table.btree()?;
                let sequence_name = name.strip_prefix(SEQ_BACKING_TABLE_PREFIX)?.to_string();
                Some(SequenceBackingTableSource {
                    sequence_name,
                    root_page: bt.root_page,
                    num_columns: bt.columns().len(),
                })
            })
            .try_collect()
            .expect(crate::alloc::ALLOC_ERR_MSG)
    }

    fn read_sequence_metadata(record: &ImmutableRecord) -> Option<SequenceMetadata> {
        let mut values = [0i64; 6];
        for (i, value) in values.iter_mut().enumerate() {
            match record.get_value(i + 1) {
                Ok(ValueRef::Numeric(crate::numeric::Numeric::Integer(v))) => {
                    *value = v;
                }
                _ => return None,
            }
        }
        let [_is_called, start, increment, min, max, cycle] = values;
        Some(SequenceMetadata {
            start,
            increment,
            min,
            max,
            cycle: cycle != 0,
        })
    }

    fn install_sequence_descriptor(
        &mut self,
        sequence_name: &str,
        metadata: SequenceMetadata,
    ) -> crate::Result<()> {
        let seq = Sequence::new(
            sequence_name.to_string(),
            Some(metadata.start),
            Some(metadata.increment),
            Some(metadata.min),
            Some(metadata.max),
            metadata.cycle,
        )
        .map_err(|err| {
            LimboError::Corrupt(format!(
                "internal sequence backing table for \"{sequence_name}\" \
                 has invalid persisted metadata \
                 (start={}, increment={}, min={}, max={}, cycle={}): {err}",
                metadata.start, metadata.increment, metadata.min, metadata.max, metadata.cycle,
            ))
        })?;
        self.sequences
            .insert(normalize_ident(sequence_name), std::sync::Arc::new(seq));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn handle_schema_row(
        &mut self,
        ty: &str,
        name: &str,
        table_name: &str,
        root_page: i64,
        maybe_sql: Option<&str>,
        syms: &SymbolTable,
        from_sql_indexes: &mut Vec<UnparsedFromSqlIndex>,
        automatic_indices: &mut HashMap<String, Vec<(String, i64)>>,
        dbsp_state_roots: &mut HashMap<String, i64>,
        dbsp_state_index_roots: &mut HashMap<String, i64>,
        materialized_view_info: &mut HashMap<String, (String, i64)>,
        // Resolves an attached database name (case-insensitive) to its
        // connection-local database id. Used when reparsing temp trigger
        // SQL that qualifies its target with an attached db name like
        // `CREATE TEMP TRIGGER tr ON aux.x ...`. Callers without a
        // connection (tests, offline schema loading) can pass
        // `&|_| None`; unresolvable names become `Some(INVALID_DB_ID)`
        // so the trigger never fires against a real db.
        resolve_attached_db: &dyn Fn(&str) -> Option<usize>,
        dialect: &dyn crate::dialect::Dialect,
    ) -> Result<()> {
        match ty {
            "table" => {
                let sql = maybe_sql.expect("sql should be present for table");
                // In the SQLite file format a `type='table'` row describes a
                // virtual table iff its rootpage is 0: virtual tables have no
                // B-tree, while every real table stores a nonzero root (MVCC
                // uses negative logical ids, which are still B-tree tables).
                // Classify on the root page before touching the SQL text so
                // only the B-tree arm needs to parse the row's stored SQL.
                if root_page == 0 {
                    match Parser::new(sql.as_bytes()).next_cmd()? {
                        Some(Cmd::Stmt(Stmt::CreateVirtualTable(_))) => {}
                        other => {
                            return Err(LimboError::Corrupt(format!(
                                "sqlite_schema table row {name} with root page 0 has unexpected SQL {sql:?}: parsed as {other:?}"
                            )));
                        }
                    }

                    // a virtual table is found in the sqlite_schema, but it's no
                    // longer in the in-memory schema. We need to recreate it if
                    // the module is loaded in the symbol table.
                    let vtab = if let Some(vtab) = syms.vtabs.get(name) {
                        vtab.clone()
                    } else {
                        let mod_name = module_name_from_sql(sql)?;
                        crate::VirtualTable::table(
                            Some(name),
                            mod_name,
                            module_args_from_sql(sql)?,
                            syms,
                        )?
                    };
                    self.add_virtual_table(vtab)?;
                } else {
                    let table = dialect.parse_table_sql(sql, root_page)?;

                    if table.has_virtual_columns && !self.generated_columns_enabled {
                        return Err(LimboError::ParseError(format!(
                            "table '{}' uses generated columns but the generated_columns feature is not enabled",
                            table.name
                        )));
                    }

                    // Detect sequence-backing tables by name prefix.
                    // Just add the table (for B-tree access); sequences are created by
                    // AddSequence at CREATE time or initialize_sequences at open time.
                    if table.name.starts_with(SEQ_BACKING_TABLE_PREFIX) {
                        self.add_btree_table(Arc::new(table))?;
                        return Ok(());
                    }

                    // Check if this is a DBSP state table
                    if table.name.starts_with(DBSP_TABLE_PREFIX) {
                        // Extract version and view name from __turso_internal_dbsp_state_v<version>_<viewname>
                        let suffix = table.name.strip_prefix(DBSP_TABLE_PREFIX).unwrap();

                        // Parse version and view name (format: "<version>_<viewname>")
                        if let Some(underscore_pos) = suffix.find('_') {
                            let version_str = &suffix[..underscore_pos];
                            let view_name = &suffix[underscore_pos + 1..];

                            // Check version compatibility
                            if let Ok(stored_version) = version_str.parse::<u32>() {
                                if stored_version == DBSP_CIRCUIT_VERSION {
                                    // Version matches, store the root page
                                    dbsp_state_roots.insert(view_name.to_string(), root_page);
                                } else {
                                    // Version mismatch - DO NOT insert into dbsp_state_roots
                                    // This will cause populate_materialized_views to skip this view
                                    tracing::warn!(
                                        "Skipping materialized view '{}' - has version {} but current version is {}. DROP and recreate the view to use it.",
                                        view_name,
                                        stored_version,
                                        DBSP_CIRCUIT_VERSION
                                    );
                                    // We can't track incompatible views here since we're in handle_schema_row
                                    // which doesn't have mutable access to self
                                }
                            }
                        }
                    }

                    let mut table = table;
                    table.resolve_custom_type_affinities(self);
                    table.propagate_domain_constraints(self)?;
                    let has_autoinc = table.has_autoincrement;
                    let tbl_name = table.name.clone();
                    self.add_btree_table(Arc::new(table))?;

                    // Create the hidden sequence object owned by this
                    // AUTOINCREMENT table. The `__turso_internal_autoincrement_`
                    // prefix is a sequence namespace marker, not a table name;
                    // the physical table is the corresponding
                    // `__turso_internal_seq_<sequence-name>` backing table.
                    if has_autoinc {
                        let seq_name = autoincrement_sequence_name(&tbl_name);
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            self.sequences.entry(normalize_ident(&seq_name))
                        {
                            let seq = Sequence::new(
                                seq_name.clone(),
                                Some(1),
                                Some(1),
                                None,
                                None,
                                false,
                            )?;
                            e.insert(Arc::new(seq));
                        }
                    }
                }
            }
            "index" => {
                match maybe_sql {
                    Some(sql) => {
                        from_sql_indexes.push(UnparsedFromSqlIndex {
                            table_name: table_name.to_string(),
                            root_page,
                            sql: sql.to_string(),
                        });
                    }
                    None => {
                        // Automatic index on primary key and/or unique constraint, e.g.
                        // table|foo|foo|2|CREATE TABLE foo (a text PRIMARY KEY, b)
                        // index|sqlite_autoindex_foo_1|foo|3|
                        let index_name = name.to_string();
                        let table_name = table_name.to_string();

                        // Check if this is an index for a DBSP state table
                        if table_name.starts_with(DBSP_TABLE_PREFIX) {
                            // Extract version and view name from __turso_internal_dbsp_state_v<version>_<viewname>
                            let suffix = table_name.strip_prefix(DBSP_TABLE_PREFIX).unwrap();

                            // Parse version and view name (format: "<version>_<viewname>")
                            if let Some(underscore_pos) = suffix.find('_') {
                                let version_str = &suffix[..underscore_pos];
                                let view_name = &suffix[underscore_pos + 1..];

                                // Only store index root if version matches
                                if let Ok(stored_version) = version_str.parse::<u32>() {
                                    if stored_version == DBSP_CIRCUIT_VERSION {
                                        dbsp_state_index_roots
                                            .insert(view_name.to_string(), root_page);
                                    }
                                }
                            }
                        } else {
                            match automatic_indices.entry(table_name) {
                                std::collections::hash_map::Entry::Vacant(e) => {
                                    e.insert(vec![(index_name, root_page)]);
                                }
                                std::collections::hash_map::Entry::Occupied(mut e) => {
                                    e.get_mut().push((index_name, root_page));
                                }
                            }
                        }
                    }
                }
            }
            "view" => {
                use crate::schema::View;
                use turso_parser::ast::{Cmd, Stmt};
                use turso_parser::parser::Parser;

                let sql = maybe_sql.expect("sql should be present for view");
                let view_name = name.to_string();

                // Parse the SQL to determine if it's a regular or materialized view
                let mut parser = Parser::new(sql.as_bytes());
                let parsed = parser.next_cmd();
                if !matches!(&parsed, Ok(Some(Cmd::Stmt(_)))) {
                    // Tolerate view rows whose stored SQL no longer parses
                    // (e.g. older versions wrote view column lists without
                    // identifier quoting). The database stays usable; the
                    // name is tracked so DROP VIEW can remove the row.
                    tracing::warn!(
                        "view '{view_name}' has unparseable SQL in sqlite_schema; \
                         it is unavailable but can be removed with DROP VIEW: {sql}"
                    );
                    self.broken_views.insert(view_name);
                } else if let Ok(Some(Cmd::Stmt(stmt))) = parsed {
                    match stmt {
                        Stmt::CreateMaterializedView { .. } => {
                            // Store materialized view info for later creation
                            // We'll handle reuse logic and create the actual IncrementalView
                            // in a later pass when we have both the main root page and DBSP state root
                            materialized_view_info
                                .insert(view_name.clone(), (sql.to_string(), root_page));

                            // Mark the existing view for potential reuse
                            if self.incremental_views.contains_key(&view_name) {
                                // We'll check for reuse in the third pass
                            }
                        }
                        Stmt::CreateView {
                            view_name: _,
                            columns: column_names,
                            select,
                            ..
                        } => {
                            crate::util::validate_select_for_unsupported_features(&select)?;

                            // Extract actual columns from the SELECT statement
                            let view_column_schema =
                                crate::util::extract_view_columns(&select, self)?;

                            // If column names were provided in CREATE VIEW (col1, col2, ...),
                            // use them to rename the columns
                            let mut final_columns = view_column_schema.flat_columns();
                            for (i, indexed_col) in column_names.iter().enumerate() {
                                if let Some(col) = final_columns.get_mut(i) {
                                    // as_str: Display would render the quoted form,
                                    // embedding literal quote characters in the name
                                    col.name = Some(indexed_col.col_name.as_str().to_string());
                                }
                            }

                            // Create regular view
                            let view =
                                View::new(name.to_string(), sql.to_string(), select, final_columns);
                            self.add_view(view)?;
                        }
                        _ => {}
                    }
                }
            }
            "trigger" => {
                use turso_parser::ast::{Cmd, Stmt};
                use turso_parser::parser::Parser;

                let sql = maybe_sql.expect("sql should be present for trigger");
                let trigger_name = name.to_string();

                let mut parser = Parser::new(sql.as_bytes());
                let Ok(Some(Cmd::Stmt(Stmt::CreateTrigger {
                    temporary,
                    if_not_exists: _,
                    trigger_name: _,
                    time,
                    event,
                    tbl_name,
                    for_each_row,
                    when_clause,
                    commands,
                }))) = parser.next_cmd()
                else {
                    return Err(crate::LimboError::ParseError(format!(
                        "invalid trigger sql: {sql}"
                    )));
                };
                // Resolve the target database from the SQL qualifier:
                // CREATE TEMP TRIGGER ... ON main.tbl → target is MAIN_DB_ID
                // CREATE TEMP TRIGGER ... ON tbl     → target is None (unqualified)
                // CREATE TEMP TRIGGER ... ON aux.tbl → resolve `aux` via the
                //     attached catalog; if the name is unknown to this
                //     connection use `INVALID_DB_ID` so the trigger never
                //     fires on a mismatched db. Using `None` (the old
                //     behaviour) would treat an unresolved attached name
                //     the same as an unqualified reference, causing the
                //     trigger to fire on every table with a matching name.
                let target_database_id = tbl_name.db_name.as_ref().map(|db_name| {
                    let db = db_name.as_str();
                    if db.eq_ignore_ascii_case("main") {
                        crate::MAIN_DB_ID
                    } else if db.eq_ignore_ascii_case("temp") {
                        crate::TEMP_DB_ID
                    } else {
                        resolve_attached_db(db).unwrap_or(crate::INVALID_DB_ID)
                    }
                });
                self.add_trigger(
                    Trigger::new(
                        trigger_name,
                        sql.to_string(),
                        // Store the bare (unquoted) table name. `Name::to_string()`
                        // renders the quoted form (`"t1"`), which then fails every
                        // schema lookup since `normalize_ident` does not strip quotes.
                        // This must match the bucket key used in `add_trigger` below.
                        tbl_name.name.as_str().to_string(),
                        time,
                        event,
                        for_each_row,
                        when_clause.map(|e| *e),
                        commands,
                        temporary,
                        target_database_id,
                    ),
                    tbl_name.name.as_str(),
                )?;
            }
            // Types are stored in sqlite_turso_types, not sqlite_schema
            _ => {}
        };

        Ok(())
    }

    /// Compute all resolved FKs *referencing* `table_name` (arg: `table_name` is the parent).
    /// Each item contains the child table, normalized columns/positions, and the parent lookup
    /// strategy (rowid vs. UNIQUE index or PK).
    pub fn resolved_fks_referencing(&self, table_name: &str) -> Result<Vec<ResolvedFkRef>> {
        let target = normalize_ident(table_name);
        let parent_tbl = self
            .get_btree_table(&target)
            .ok_or_else(|| fk_mismatch_err("<unknown>", &target))?;

        let mut out = Vec::try_with_capacity_ext(4)?; // arbitrary estimate
        for t in self.tables.values() {
            let Some(child) = t.btree() else {
                continue;
            };
            for fk in &child.foreign_keys {
                if !fk.parent_table.eq_ignore_ascii_case(&target) {
                    continue;
                }
                out.try_push(self.resolve_fk(
                    fk,
                    &child,
                    &parent_tbl,
                    /*require_unique=*/ false,
                )?)?;
            }
        }
        Ok(out)
    }

    /// Compute all resolved FKs *declared by* `child_table`.
    /// Unlike `resolved_fks_referencing`, this requires every non-rowid parent key
    /// to be backed by a non-partial UNIQUE index on exactly those columns.
    pub fn resolved_fks_for_child(&self, child_table: &str) -> crate::Result<Vec<ResolvedFkRef>> {
        let child_name = normalize_ident(child_table);
        let child = self
            .get_btree_table(&child_name)
            .ok_or_else(|| fk_mismatch_err(&child_name, "<unknown>"))?;

        let mut out = Vec::try_with_capacity_ext(child.foreign_keys.len())?;
        for fk in &child.foreign_keys {
            let parent_name = normalize_ident(&fk.parent_table);
            let parent_tbl = self
                .get_btree_table(&parent_name)
                .ok_or_else(|| fk_mismatch_err(&child.name, &parent_name))?;
            out.push_within_capacity(self.resolve_fk(
                fk,
                &child,
                &parent_tbl,
                /*require_unique=*/ true,
            )?)
            .expect("resolved FK vector was preallocated to child.foreign_keys.len()");
        }
        Ok(out)
    }

    /// Resolve a single FK declared on `child` referencing `parent_tbl`.
    /// When `require_unique` is set, a non-rowid parent key must be backed by
    /// a non-partial UNIQUE index on exactly those columns.
    fn resolve_fk(
        &self,
        fk: &Arc<ForeignKey>,
        child: &Arc<BTreeTable>,
        parent_tbl: &Arc<BTreeTable>,
        require_unique: bool,
    ) -> Result<ResolvedFkRef> {
        // child_columns is validated non-empty at parse time, but keep a defensive check
        // because schema can be loaded from user-provided sqlite files.
        if fk.child_columns.is_empty() {
            return Err(fk_mismatch_err(&child.name, &parent_tbl.name));
        }

        let mut child_pos: Vec<usize> = Vec::try_with_capacity_ext(fk.child_columns.len())?;
        for cname in fk.child_columns.iter() {
            let (i, _) = child
                .get_column(cname)
                .ok_or_else(|| fk_mismatch_err(&child.name, &parent_tbl.name))?;
            child_pos
                .push_within_capacity(i)
                .expect("child FK position vector was preallocated to fk.child_columns.len()");
        }

        // Resolve parent columns: explicit list, or default to parent's PK columns.
        let parent_cols: Box<[String]> = if fk.parent_columns.is_empty() {
            if parent_tbl.primary_key_columns.is_empty() {
                return Err(fk_mismatch_err(&child.name, &parent_tbl.name));
            }
            parent_tbl
                .primary_key_columns
                .iter()
                .map(|(col, _)| col.clone())
                .try_collect()?
        } else {
            fk.parent_columns.clone()
        };

        if parent_cols.len() != fk.child_columns.len() {
            return Err(fk_mismatch_err(&child.name, &parent_tbl.name));
        }

        let mut parent_pos: Vec<usize> = Vec::try_with_capacity_ext(parent_cols.len())?;
        for pc in parent_cols.iter() {
            let pos = parent_tbl.get_column(pc).map(|(i, _)| i).or_else(|| {
                ROWID_STRS
                    .iter()
                    .any(|r| pc.eq_ignore_ascii_case(r))
                    .then_some(0)
            });
            let Some(p) = pos else {
                return Err(fk_mismatch_err(&child.name, &parent_tbl.name));
            };
            parent_pos
                .push_within_capacity(p)
                .expect("parent FK position vector was preallocated to parent_cols.len()");
        }

        // A single-column parent key is the rowid when it names rowid/_rowid_/oid
        // or points at an INTEGER PRIMARY KEY rowid alias.
        let parent_uses_rowid = parent_cols.len() == 1 && {
            let pc = parent_cols[0].as_str();
            ROWID_STRS.iter().any(|r| pc.eq_ignore_ascii_case(r))
                || parent_tbl.columns.iter().any(|col| {
                    col.is_rowid_alias()
                        && col
                            .name
                            .as_deref()
                            .is_some_and(|n| n.eq_ignore_ascii_case(pc))
                })
        };

        let parent_unique_index = if parent_uses_rowid {
            None
        } else {
            let found = self
                .get_indices(&parent_tbl.name)
                .find(|idx| {
                    idx.unique
                        && idx.where_clause.is_none()
                        && idx.columns.len() == parent_cols.len()
                        && idx
                            .columns
                            .iter()
                            .zip(parent_cols.iter())
                            .all(|(ic, pc)| ic.name.eq_ignore_ascii_case(pc))
                })
                .cloned();
            if require_unique && found.is_none() {
                return Err(fk_mismatch_err(&child.name, &parent_tbl.name));
            }
            found
        };

        fk.validate()?;
        Ok(ResolvedFkRef {
            child_table: Arc::clone(child),
            fk: Arc::clone(fk),
            parent_cols,
            child_pos: child_pos.into_boxed_slice(),
            parent_pos: parent_pos.into_boxed_slice(),
            parent_uses_rowid,
            parent_unique_index,
        })
    }

    /// Returns if any table declares a FOREIGN KEY whose parent is `table_name`.
    pub fn any_resolved_fks_referencing(&self, table_name: &str) -> bool {
        self.tables.values().any(|t| {
            let Some(bt) = t.btree() else {
                return false;
            };
            bt.foreign_keys
                .iter()
                .any(|fk| fk.parent_table == table_name)
        })
    }

    /// Returns true if `table_name` declares any FOREIGN KEYs
    pub fn has_child_fks(&self, table_name: &str) -> bool {
        self.get_table(table_name)
            .and_then(|t| t.btree())
            .is_some_and(|t| !t.foreign_keys.is_empty())
    }

    fn check_object_name_conflict(&self, name: &str, creating: SchemaObjectType) -> Result<()> {
        if let Some(existing) = self.get_object_type(name) {
            // Match SQLite's message shapes: indexes and tables/views live in
            // different namespaces, so a cross-namespace clash names the other
            // side ("there is already a ...") instead of "already exists".
            let msg = match (creating, existing) {
                (SchemaObjectType::Index, SchemaObjectType::Index) => {
                    format!("index {name} already exists")
                }
                (SchemaObjectType::Index, _) => format!("there is already a table named {name}"),
                (_, SchemaObjectType::Index) => {
                    format!("there is already an index named {name}")
                }
                (_, SchemaObjectType::Table) => format!("table {name} already exists"),
                (_, SchemaObjectType::View) => format!("view {name} already exists"),
            };
            return Err(crate::LimboError::ParseError(msg));
        }
        Ok(())
    }

    pub fn get_sequence(&self, name: &str) -> Option<&Arc<Sequence>> {
        self.sequences.get(&normalize_ident(name))
    }

    /// Remove a sequence and its backing table from the in-memory schema.
    pub fn remove_sequence(&mut self, name: &str) {
        let normalized = normalize_ident(name);
        self.sequences.remove(&normalized);
        let backing_table = crate::translate::sequence::sequence_backing_table_name(&normalized);
        self.tables.remove(&backing_table);
    }

    /// Returns the type of schema object with the given name, if one exists.
    /// Checks tables, views, and indexes.
    pub fn get_object_type(&self, name: &str) -> Option<SchemaObjectType> {
        let normalized_name = self.normalize_table_lookup_name(name);

        if self.tables.contains_key(&normalized_name) {
            return Some(SchemaObjectType::Table);
        }

        if self.views.contains_key(&normalized_name) {
            return Some(SchemaObjectType::View);
        }

        for index_list in self.indexes.values() {
            if index_list.iter().any(|i| i.name.eq_ignore_ascii_case(name)) {
                return Some(SchemaObjectType::Index);
            }
        }

        None
    }
}

impl TryClone for UniqueSet {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: self.columns.try_clone()?,
            is_primary_key: self.is_primary_key,
            conflict_clause: self.conflict_clause,
        })
    }
}

// Copy enums stored as `Vec` elements: their clone cannot allocate.
crate::alloc::impl_try_clone_via_clone!(
    turso_parser::ast::SortOrder,
    crate::translate::collate::CollationSeq,
);

// Std-pinned schema element types: every owned field allocates through the
// std global allocator (Strings, `std::vec::Vec`, boxed parser AST), so
// forwarding to `Clone` is correct today. TODO(alloc): give these real
// fallible impls when their fields become allocator-aware.
crate::alloc::impl_try_clone_via_clone!(Column, IndexColumn, CheckConstraint, UniqueSetColumn);

impl Schema {
    #[turso_macros::allocation_site(crate::alloc::SchemaAllocationSite::MakeMut)]
    pub(crate) fn try_make_mut(schema: &mut Arc<Self>) -> Result<&mut Self, TryReserveError> {
        if Arc::get_mut(schema).is_none() {
            *schema = Arc::new(schema.as_ref().try_clone()?);
        }
        Ok(Arc::get_mut(schema).expect("schema was made unique above"))
    }
}

impl TryClone for View {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            name: self.name.clone(),
            sql: self.sql.clone(),
            select_stmt: self.select_stmt.clone(),
            columns: self.columns.try_clone()?,
            state: AtomicViewState::new(ViewState::Ready),
        })
    }
}

impl TryClone for VirtualTable {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            name: self.name.clone(),
            columns: self.columns.try_clone()?,
            kind: self.kind,
            vtab_type: self.vtab_type.clone(),
            vtab_id: self.vtab_id,
            is_droppable: self.is_droppable,
            innocuous: self.innocuous,
        })
    }
}

impl TryClone for BTreeTable {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            root_page: self.root_page,
            name: self.name.clone(),
            primary_key_columns: self.primary_key_columns.try_clone()?,
            columns: self.columns.try_clone()?,
            has_rowid: self.has_rowid,
            is_strict: self.is_strict,
            has_autoincrement: self.has_autoincrement,
            unique_sets: self.unique_sets.try_clone()?,
            foreign_keys: self.foreign_keys.try_clone()?,
            check_constraints: self.check_constraints.try_clone()?,
            rowid_alias_conflict_clause: self.rowid_alias_conflict_clause,
            has_virtual_columns: self.has_virtual_columns,
            logical_to_physical_map: self.logical_to_physical_map.try_clone()?,
            column_dependencies: Default::default(),
        })
    }
}

impl TryClone for FromClauseSubquery {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            name: self.name.clone(),
            plan: self.plan.clone(),
            columns: self.columns.try_clone()?,
            result_columns_start_reg: self.result_columns_start_reg,
            materialized_cursor_id: self.materialized_cursor_id,
            cte: self.cte,
        })
    }
}

impl TryClone for RecursiveCteInput {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            name: self.name.clone(),
            columns: self.columns.try_clone()?,
        })
    }
}

impl TryClone for Table {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(match self {
            Table::BTree(table) => Table::BTree(Arc::new(table.as_ref().try_clone()?)),
            Table::Virtual(table) => Table::Virtual(Arc::new(table.as_ref().try_clone()?)),
            Table::FromClauseSubquery(from_clause_subquery) => {
                Table::FromClauseSubquery(Arc::new(from_clause_subquery.as_ref().try_clone()?))
            }
            Table::RecursiveCteInput(input) => {
                Table::RecursiveCteInput(Arc::new(input.as_ref().try_clone()?))
            }
        })
    }
}

impl TryClone for Index {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        Ok(Self {
            name: self.name.clone(),
            table_name: self.table_name.clone(),
            root_page: self.root_page,
            columns: self.columns.try_clone()?,
            unique: self.unique,
            ephemeral: self.ephemeral,
            has_rowid: self.has_rowid,
            where_clause: self.where_clause.clone(),
            index_method: self.index_method.clone(),
            on_conflict: self.on_conflict,
        })
    }
}

impl TryClone for Schema {
    /// Copying a `Schema` requires deep cloning of all internal tables and indexes, even though they are wrapped in `Arc`.
    /// Simply copying the `Arc` pointers would result in multiple `Schema` instances sharing the same underlying tables and indexes,
    /// which could lead to panics or data races if any instance attempts to modify them.
    /// To ensure each `Schema` is independent and safe to modify, we clone the underlying data for all tables and indexes.
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        let tables = self
            .tables
            .iter()
            .map(|(name, table)| {
                Ok::<_, TryReserveError>((name.clone(), Arc::new(table.as_ref().try_clone()?)))
            })
            .try_collect::<Result<_, TryReserveError>>()??;
        let indexes = self
            .indexes
            .iter()
            .map(|(name, indexes)| {
                let indexes = indexes
                    .iter()
                    .map(|index| index.as_ref().try_clone().map(Arc::new))
                    .try_collect::<Result<VecDeque<_>, TryReserveError>>()??;
                Ok::<_, TryReserveError>((name.clone(), indexes))
            })
            .try_collect::<Result<_, TryReserveError>>()??;
        let materialized_view_names = self.materialized_view_names.try_clone()?;
        let materialized_view_sql = self.materialized_view_sql.try_clone()?;
        let incremental_views = self
            .incremental_views
            .iter()
            .map(|(name, view)| (name.clone(), view.clone()))
            .try_collect()?;
        let views = self
            .views
            .iter()
            .map(|(name, view)| {
                Ok::<_, TryReserveError>((name.clone(), Arc::new(view.as_ref().try_clone()?)))
            })
            .try_collect::<Result<_, TryReserveError>>()??;
        let triggers = self
            .triggers
            .iter()
            .map(|(table_name, triggers)| {
                Ok::<_, TryReserveError>((
                    table_name.clone(),
                    triggers
                        .iter()
                        .map(|t| Arc::new((**t).clone()))
                        .try_collect()?,
                ))
            })
            .try_collect::<Result<_, TryReserveError>>()??;
        let incompatible_views = self.incompatible_views.try_clone()?;
        Ok(Self {
            tables,
            #[cfg(feature = "conn_raw_api")]
            table_names_by_root_page: self.table_names_by_root_page.try_clone()?,
            materialized_view_names,
            materialized_view_sql,
            incremental_views,
            views,
            triggers,
            indexes,
            has_indexes: self.has_indexes.try_clone()?,
            schema_version: self.schema_version,
            analyze_stats: self.analyze_stats.clone(),
            table_to_materialized_views: self.table_to_materialized_views.try_clone()?,
            incompatible_views,
            broken_views: self.broken_views.try_clone()?,
            dropped_root_pages: self.dropped_root_pages.try_clone()?,
            type_registry: self.type_registry.try_clone()?,
            generated_columns_enabled: self.generated_columns_enabled,
            sequences: self.sequences.try_clone()?,
        })
    }
}

/// Maps schema column indices to register offsets for DML operations.
//TODO this should be integrated into a Columns domain type
// This type should also replace BTreeTable::has_virtual_columns
#[derive(Debug, Clone)]
pub enum ColumnLayout {
    Identity {
        column_count: usize,
    },
    Mapped {
        // col_index -> offset
        offsets: Vec<usize>,
        non_virtual_col_count: usize,
    },
}

impl ColumnLayout {
    pub fn from_table(table: &Table) -> Result<Self, TryReserveError> {
        match table {
            Table::BTree(btree) => Self::from_btree(btree),
            Table::Virtual(vtable) => Ok(Self::Identity {
                column_count: vtable.as_ref().columns.len(),
            }),
            Table::FromClauseSubquery(subquery) => Ok(Self::Identity {
                column_count: subquery.columns.len(),
            }),
            Table::RecursiveCteInput(input) => Ok(Self::Identity {
                column_count: input.columns.len(),
            }),
        }
    }

    pub fn from_btree(btree: &BTreeTable) -> Result<Self, TryReserveError> {
        let total = btree.columns.len();
        let non_virtual_col_count = btree
            .columns
            .iter()
            .filter(|c| !c.is_virtual_generated())
            .count();
        let offsets = btree.logical_to_physical_map.try_clone()?;
        let is_identity = non_virtual_col_count == total && offsets.iter().copied().eq(0..total);
        if is_identity {
            Ok(Self::Identity {
                column_count: total,
            })
        } else {
            Ok(Self::Mapped {
                offsets,
                non_virtual_col_count,
            })
        }
    }

    pub fn from_columns(columns: &[Column]) -> Result<Self, TryReserveError> {
        let total = columns.len();
        let non_virtual_col_count = columns.iter().filter(|c| !c.is_virtual_generated()).count();
        if non_virtual_col_count == total {
            return Ok(Self::Identity {
                column_count: total,
            });
        }
        let mut offsets = try_vec![0usize; total]?;
        let mut nv_idx = 0;
        let mut v_idx = non_virtual_col_count;
        for (i, col) in columns.iter().enumerate() {
            if col.is_virtual_generated() {
                offsets[i] = v_idx;
                v_idx += 1;
            } else {
                offsets[i] = nv_idx;
                nv_idx += 1;
            }
        }
        Ok(Self::Mapped {
            offsets,
            non_virtual_col_count,
        })
    }

    /// Map a schema column index to its register offset.
    #[inline(always)]
    pub fn to_reg_offset(&self, col_idx: usize) -> usize {
        match self {
            Self::Identity { .. } => col_idx,
            Self::Mapped { offsets, .. } => offsets[col_idx],
        }
    }

    /// Resolve schema column index to an absolute register.
    #[inline(always)]
    pub fn to_register(&self, base: usize, schema_idx: usize) -> usize {
        base + self.to_reg_offset(schema_idx)
    }

    #[inline(always)]
    pub fn num_non_virtual_cols(&self) -> usize {
        match self {
            Self::Identity {
                column_count: total,
            } => *total,
            Self::Mapped {
                non_virtual_col_count,
                ..
            } => *non_virtual_col_count,
        }
    }

    #[inline(always)]
    pub fn column_count(&self) -> usize {
        match self {
            Self::Identity {
                column_count: total,
            } => *total,
            Self::Mapped { offsets, .. } => offsets.len(),
        }
    }

    pub fn column_idx_for_offset(&self, offset: usize) -> Option<usize> {
        match self {
            Self::Identity { column_count } => {
                if offset < *column_count {
                    Some(offset)
                } else {
                    None
                }
            }
            Self::Mapped { offsets, .. } => offsets.iter().position(|&s| s == offset),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Table {
    BTree(Arc<BTreeTable>),
    Virtual(Arc<VirtualTable>),
    FromClauseSubquery(Arc<FromClauseSubquery>),
    RecursiveCteInput(Arc<RecursiveCteInput>),
}

impl Table {
    pub fn get_root_page(&self) -> crate::Result<i64> {
        match self {
            Table::BTree(table) => Ok(table.root_page),
            Table::Virtual(_) => Err(crate::LimboError::InternalError(
                "Virtual tables do not have a root page".to_string(),
            )),
            Table::FromClauseSubquery(_) => Err(crate::LimboError::InternalError(
                "FROM clause subqueries do not have a root page".to_string(),
            )),
            Table::RecursiveCteInput(_) => Err(crate::LimboError::InternalError(
                "recursive CTE inputs do not have a root page".to_string(),
            )),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Self::BTree(table) => &table.name,
            Self::Virtual(table) => &table.name,
            Self::FromClauseSubquery(from_clause_subquery) => &from_clause_subquery.name,
            Self::RecursiveCteInput(input) => &input.name,
        }
    }

    pub fn get_column_at(&self, index: usize) -> Option<&Column> {
        match self {
            Self::BTree(table) => table.columns.get(index),
            Self::Virtual(table) => table.columns.get(index),
            Self::FromClauseSubquery(from_clause_subquery) => {
                from_clause_subquery.columns.get(index)
            }
            Self::RecursiveCteInput(input) => input.columns.get(index),
        }
    }

    /// Returns the column position and column for a given column name.
    pub fn get_column_by_name(&self, name: &str) -> Option<(usize, &Column)> {
        match self {
            Self::BTree(table) => table.get_column(name),
            Self::Virtual(table) => table.columns.iter().enumerate().find(|(_, col)| {
                col.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(name))
            }),
            Self::FromClauseSubquery(from_clause_subquery) => from_clause_subquery
                .columns
                .iter()
                .enumerate()
                .find(|(_, col)| {
                    col.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(name))
                }),
            Self::RecursiveCteInput(input) => input.columns.iter().enumerate().find(|(_, col)| {
                col.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(name))
            }),
        }
    }

    pub fn columns(&self) -> &[Column] {
        match self {
            Self::BTree(table) => &table.columns,
            Self::Virtual(table) => &table.columns,
            Self::FromClauseSubquery(from_clause_subquery) => &from_clause_subquery.columns,
            Self::RecursiveCteInput(input) => &input.columns,
        }
    }

    pub fn is_strict(&self) -> bool {
        match self {
            Self::BTree(table) => table.is_strict,
            Self::Virtual(_) => false,
            Self::FromClauseSubquery(_) => false,
            Self::RecursiveCteInput(_) => false,
        }
    }

    pub fn btree(&self) -> Option<Arc<BTreeTable>> {
        match self {
            Self::BTree(table) => Some(table.clone()),
            Self::Virtual(_) => None,
            Self::FromClauseSubquery(_) => None,
            Self::RecursiveCteInput(_) => None,
        }
    }

    /// Like `btree()` but returns an error instead of None.
    pub fn require_btree(&self) -> crate::Result<Arc<BTreeTable>> {
        self.btree().ok_or_else(|| {
            crate::LimboError::InternalError(
                "operation requires a btree table, not a virtual table".into(),
            )
        })
    }

    pub fn btree_mut(&mut self) -> Option<&mut Arc<BTreeTable>> {
        match self {
            Self::BTree(table) => Some(table),
            Self::Virtual(_) => None,
            Self::FromClauseSubquery(_) => None,
            Self::RecursiveCteInput(_) => None,
        }
    }

    pub fn virtual_table(&self) -> Option<Arc<VirtualTable>> {
        match self {
            Self::Virtual(table) => Some(table.clone()),
            _ => None,
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BTree(a), Self::BTree(b)) => Arc::ptr_eq(a, b),
            (Self::Virtual(a), Self::Virtual(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

/// UniqueSet describes a column or set of columns for which rows are unique (PRIMARY KEY, UNIQUE)
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniqueSet {
    pub columns: Vec<UniqueSetColumn>,
    pub is_primary_key: bool,
    pub conflict_clause: Option<ResolveType>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UniqueSetColumn {
    pub name: String,
    pub sort_order: SortOrder,
    pub collation: Option<CollationSeq>,
    pub nulls_order: Option<NullsOrder>,
}

#[derive(Clone, Debug)]
pub struct CheckConstraint {
    /// Optional constraint name
    pub name: Option<String>,
    /// CHECK expression
    pub expr: ast::Expr,
    /// The expression's source text exactly as the user wrote it between the
    /// CHECK parens (whitespace-trimmed). SQLite reports an unnamed failed
    /// constraint with this text, not a re-rendering of the expression.
    pub source: Option<String>,
    /// Column name if this is a column-level CHECK constraint (defined inline with the column).
    /// None if this is a table-level CHECK constraint.
    pub column: Option<String>,
}

impl CheckConstraint {
    pub fn new(
        name: Option<&ast::Name>,
        expr: &ast::Expr,
        source: Option<&str>,
        column: Option<&str>,
    ) -> Self {
        Self {
            name: name.map(|n| n.as_str().to_string()),
            expr: expr.clone(),
            source: source.map(|s| s.to_string()),
            column: column.map(|s| s.to_string()),
        }
    }

    /// Returns the SQL representation of this CHECK constraint (e.g. `CHECK(x > 0)`).
    pub fn sql(&self) -> String {
        match &self.source {
            // Keep the user's spelling when rebuilding SQL. Put the closing
            // paren on a new line when a line comment might otherwise swallow
            // it. Closed block comments are safe as-is.
            Some(source) if !source.is_empty() => {
                let newline = if source.contains("--") { "\n" } else { "" };
                format!("CHECK({source}{newline})")
            }
            _ => format!("CHECK({})", self.expr),
        }
    }
}

/// RAII wrapper that resets its inner value when cloned.
#[derive(Debug, Default)]
pub struct ResetOnClone<T: Default>(T);

impl<T: Default> Clone for ResetOnClone<T> {
    fn clone(&self) -> Self {
        Self(T::default())
    }
}

bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct BTreeCharacteristics: u8 {
        /// Table has a rowid column (i.e. not `WITHOUT ROWID`).
        const HAS_ROWID         = 0b0000_0001;
        /// Table is declared `STRICT`.
        const STRICT            = 0b0000_0010;
        /// Table has an `AUTOINCREMENT` column.
        const HAS_AUTOINCREMENT = 0b0000_0100;
    }
}

#[derive(Debug)]
pub(crate) struct GeneratedColGraph {
    /// `dependencies[j]` = columns `j` transitively reads from (excludes `j`).
    dependencies: Vec<ColumnMask>,
    /// `dependents[i]` = columns that transitively read from `i` (excludes `i`).
    dependents: Vec<ColumnMask>,
    /// Column indices in topological (dependency) order. Contains all columns.
    topological_sort: Vec<usize>,
}

impl GeneratedColGraph {
    fn build(columns: &[Column]) -> Result<Self> {
        let n = columns.len();

        let mut direct_deps = try_vec![ColumnMask::default(); n]?;
        let mut direct_dependents = try_vec![ColumnMask::default(); n]?;
        let mut in_degree: Vec<u32> = try_vec![0; n]?;

        // walk each virtual column's expression once to extract edges
        for (j, col) in columns.iter().enumerate() {
            let GeneratedType::Virtual { ref expr, .. } = col.generated_type() else {
                continue;
            };
            let mut direct = BitSet::default();
            collect_column_dependencies_of_gencol(expr, columns, &mut direct);
            if direct.get(j) {
                bail_parse_error!(
                    "generated column \"{}\" cannot reference itself",
                    col.name.as_deref().unwrap_or("?")
                );
            }
            let direct_mask: ColumnMask = ColumnMask::try_from_iter(direct.iter())?;
            direct_deps[j].union_with(&direct_mask)?;
            for i in direct.iter() {
                direct_dependents[i].set(j)?;
                in_degree[j] += 1;
            }
        }

        // Kahn's algorithm (topological sort) over direct_deps.
        let mut topological_sort: Vec<usize> = Vec::try_with_capacity_ext(n)?;
        let mut ready: Vec<usize> = (0..n).filter(|&i| in_degree[i] == 0).try_collect()?;
        while let Some(i) = ready.pop() {
            topological_sort.try_push(i)?;
            for j in direct_dependents[i].iter() {
                in_degree[j] -= 1;
                if in_degree[j] == 0 {
                    ready.try_push(j)?;
                }
            }
        }

        // see if there's cycles in the graph
        if topological_sort.len() != n {
            let cycle_names: Vec<&str> = (0..n)
                .filter(|i| in_degree[*i] > 0)
                .filter_map(|i| columns[i].name.as_deref())
                .try_collect()?;
            bail_parse_error!(
                "circular dependency in generated columns: {}",
                cycle_names.join(", ")
            );
        }

        // compute transitive closures.
        let mut dependencies = try_vec![ColumnMask::default(); n]?;
        for &j in &topological_sort {
            dependencies[j] = direct_deps[j].try_clone()?;
            for i in direct_deps[j].iter() {
                let snapshot = dependencies[i].try_clone()?;
                dependencies[j].union_with(&snapshot)?;
            }
        }

        // compute transitive closures of the transpose graph (dependents)
        let mut dependents = try_vec![ColumnMask::default(); n]?;
        for &i in topological_sort.iter().rev() {
            dependents[i] = direct_dependents[i].try_clone()?;
            for j in direct_dependents[i].iter() {
                let snapshot = dependents[j].try_clone()?;
                dependents[i].union_with(&snapshot)?;
            }
        }

        Ok(Self {
            dependencies,
            dependents,
            topological_sort,
        })
    }
}

#[derive(Clone, Debug)]
pub struct BTreeTable {
    pub root_page: i64,
    pub name: String,
    pub primary_key_columns: Vec<(String, SortOrder)>,
    columns: Vec<Column>,
    pub has_rowid: bool,
    pub is_strict: bool,
    pub has_autoincrement: bool,
    pub unique_sets: Vec<UniqueSet>,
    pub foreign_keys: Vec<Arc<ForeignKey>>,
    pub check_constraints: Vec<CheckConstraint>,
    /// ON CONFLICT clause for the INTEGER PRIMARY KEY constraint.
    /// Stored here because rowid-alias PKs have their UniqueSet removed.
    pub rowid_alias_conflict_clause: Option<ResolveType>,
    pub has_virtual_columns: bool,
    pub logical_to_physical_map: Vec<usize>,
    column_dependencies: ResetOnClone<OnceLock<GeneratedColGraph>>,
}

pub struct ColumnsMut<'a> {
    table: &'a mut BTreeTable,
}

impl std::ops::Deref for ColumnsMut<'_> {
    type Target = Vec<Column>;
    fn deref(&self) -> &Vec<Column> {
        &self.table.columns
    }
}

impl std::ops::DerefMut for ColumnsMut<'_> {
    fn deref_mut(&mut self) -> &mut Vec<Column> {
        &mut self.table.columns
    }
}

impl Drop for ColumnsMut<'_> {
    fn drop(&mut self) {
        self.table.column_dependencies.0 = OnceLock::new();
        self.table.has_virtual_columns =
            self.table.columns.iter().any(|c| c.is_virtual_generated());
        self.table.logical_to_physical_map = BTreeTable::build_logical_to_physical_map(
            &self.table.columns,
            &self.table.primary_key_columns,
            self.table.has_rowid,
        );
    }
}

impl BTreeTable {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        root_page: i64,
        name: String,
        primary_key_columns: Vec<(String, SortOrder)>,
        columns: Vec<Column>,
        characteristics: BTreeCharacteristics,
        unique_sets: Vec<UniqueSet>,
        foreign_keys: Vec<Arc<ForeignKey>>,
        check_constraints: Vec<CheckConstraint>,
        rowid_alias_conflict_clause: Option<ResolveType>,
    ) -> Self {
        let has_virtual_columns = columns.iter().any(|c| c.is_virtual_generated());
        let has_rowid = characteristics.contains(BTreeCharacteristics::HAS_ROWID);
        let logical_to_physical_map =
            Self::build_logical_to_physical_map(&columns, &primary_key_columns, has_rowid);
        Self {
            root_page,
            name,
            primary_key_columns,
            columns,
            has_rowid,
            is_strict: characteristics.contains(BTreeCharacteristics::STRICT),
            has_autoincrement: characteristics.contains(BTreeCharacteristics::HAS_AUTOINCREMENT),
            unique_sets,
            foreign_keys,
            check_constraints,
            rowid_alias_conflict_clause,
            has_virtual_columns,
            logical_to_physical_map,
            column_dependencies: Default::default(),
        }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> ColumnsMut<'_> {
        ColumnsMut { table: self }
    }

    /// Create a table reference for TypeCheck where custom type columns have
    /// their `ty_str` replaced with the base type name, and where virtual columns
    /// are skipped. This ensures TypeCheck validates the encoded value against the
    /// correct base type (e.g., BLOB) rather than accepting any STRICT type via the wildcard arm.
    pub fn type_check_table_ref(table: &Arc<BTreeTable>, schema: &Schema) -> Arc<BTreeTable> {
        let has_virtual = table.has_virtual_columns();
        let has_custom = table
            .columns
            .iter()
            .any(|c| c.is_array() || schema.get_type_def(&c.ty_str, table.is_strict).is_some());
        if !has_custom && !has_virtual {
            return Arc::clone(table);
        }
        let mut modified = (**table).clone();
        if has_virtual {
            modified.columns.retain(|c| !c.is_virtual_generated());
            modified.has_virtual_columns = false;
        }
        for col in &mut modified.columns {
            if col.is_array() {
                // Arrays are stored as record-format blobs.
                col.ty_str = "BLOB".to_string();
                col.override_affinity(Affinity::Blob);
            } else if let Ok(Some(resolved)) = schema.resolve_type(&col.ty_str, table.is_strict) {
                col.ty_str = resolved.primitive.to_uppercase();
                col.override_affinity(Affinity::None);
            }
        }
        Arc::new(modified)
    }

    /// Create a table ref for pre-encode TypeCheck that validates user input
    /// against the type's declared `value` input type (or base if not declared).
    /// For UPDATE, `only_columns` limits which columns are checked — non-SET
    /// columns hold encoded values and must be skipped (set to ANY).
    pub fn input_type_check_table_ref(
        table: &Arc<BTreeTable>,
        schema: &Schema,
        only_columns: Option<&ColumnMask>,
    ) -> Result<Arc<BTreeTable>> {
        let has_virtual = table.has_virtual_columns();
        let has_custom = table
            .columns
            .iter()
            .any(|c| c.is_array() || schema.get_type_def(&c.ty_str, table.is_strict).is_some());
        if !has_custom && !has_virtual {
            return Ok(Arc::clone(table));
        }
        let mut modified = (**table).clone();
        let remapped_only_columns = if has_virtual {
            let remapped = only_columns
                .map(|only| {
                    let mut new_set = ColumnMask::default();
                    let mut physical = 0usize;
                    for (orig, col) in modified.columns.iter().enumerate() {
                        if col.is_virtual_generated() {
                            continue;
                        }
                        if only.get(orig) {
                            new_set.set(physical)?;
                        }
                        physical += 1;
                    }
                    Ok::<_, LimboError>(new_set)
                })
                .transpose()?;
            modified.columns.retain(|c| !c.is_virtual_generated());
            modified.has_virtual_columns = false;
            remapped
        } else {
            None
        };
        let effective_only = remapped_only_columns.as_ref().or(only_columns);
        for (i, col) in modified.columns.iter_mut().enumerate() {
            if let Some(only) = effective_only {
                if !only.get(i) {
                    col.ty_str = "ANY".to_string();
                    col.override_affinity(Affinity::Blob);
                    continue;
                }
            }
            if col.is_array() {
                // Pre-encode: user input can be text ('[1,2]') or blob (ARRAY[]),
                // so accept ANY here; the encoder handles conversion.
                col.ty_str = "ANY".to_string();
                col.override_affinity(Affinity::Blob);
            } else if let Some(type_def) = schema.get_type_def(&col.ty_str, table.is_strict) {
                col.ty_str = type_def.value_input_type().to_uppercase();
            }
        }
        Ok(Arc::new(modified))
    }

    /// Override column type metadata for custom type columns so that
    /// SQLite's name-based type/affinity rules use the BASE type
    /// instead of the custom type name (e.g. "doubled" contains "DOUB"
    /// which would incorrectly map to REAL instead of INTEGER).
    pub fn resolve_custom_type_affinities(&mut self, schema: &Schema) {
        if !self.is_strict {
            return;
        }
        for col in &mut self.columns {
            if col.is_array() {
                // Arrays are stored as record-format blobs regardless of element type.
                col.set_ty(Type::Blob);
                col.override_affinity(Affinity::Blob);
                continue;
            }
            if let Ok(Some(resolved)) = schema.resolve_type_unchecked(&col.ty_str) {
                let (base_ty, _) = type_from_name(&resolved.primitive);
                col.set_ty(base_ty);
                col.override_affinity(Affinity::affinity(&resolved.primitive));
            }
        }
    }

    /// Propagate domain NOT NULL and CHECK constraints to table columns.
    /// For each column whose type resolves to a domain, this:
    /// - Sets the column's NOT NULL flag if any domain in the chain has NOT NULL
    /// - Adds domain CHECK constraints (with `value` rewritten to the column name)
    ///   to the table's check_constraints list
    pub fn propagate_domain_constraints(&mut self, schema: &Schema) -> Result<()> {
        if !self.is_strict {
            return Ok(());
        }
        // Collect new constraints and notnull flags to avoid borrowing issues
        let mut new_checks = vec![];
        let mut notnull_cols = vec![];

        for (col_idx, col) in self.columns.iter().enumerate() {
            let Ok(Some(resolved)) = schema.resolve_type_unchecked(&col.ty_str) else {
                continue;
            };
            if !resolved.is_domain() {
                continue;
            }
            let col_name = col.name.as_deref().unwrap_or("").to_string();
            for td in &resolved.chain {
                if td.not_null {
                    notnull_cols.try_push(col_idx)?;
                }
                for (i, dc) in td.domain_checks.iter().enumerate() {
                    let rewritten = rewrite_value_to_column(&dc.check, &col_name);
                    let name = dc
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}", td.name, i));
                    new_checks.try_push(CheckConstraint {
                        name: Some(name),
                        expr: *rewritten,
                        source: None,
                        column: Some(col_name.clone()),
                    })?;
                }
            }
        }

        for col_idx in notnull_cols {
            self.columns[col_idx].set_notnull(true);
        }
        self.check_constraints.try_extend(new_checks)?;
        Ok(())
    }

    pub fn get_rowid_alias_column(&self) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.is_rowid_alias())
    }

    pub fn has_virtual_columns(&self) -> bool {
        self.has_virtual_columns
    }

    /// Build a `ColumnLayout` for this table's register mapping.
    pub fn column_layout(&self) -> Result<ColumnLayout, TryReserveError> {
        ColumnLayout::from_btree(self)
    }

    /// Returns the column position and column for a given column name.
    /// Returns None if the column name is not found.
    /// E.g. if table is CREATE TABLE t (a, b, c)
    /// then get_column("b") returns (1, &Column { .. })
    pub fn get_column(&self, name: &str) -> Option<(usize, &Column)> {
        self.columns.iter().enumerate().find(|(_, column)| {
            column
                .name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
    }

    pub fn from_sql(sql: &str, root_page: i64) -> Result<BTreeTable> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateTable { tbl_name, body, .. })) => {
                Self::from_create_table_ast(&tbl_name, &body, root_page)
            }
            Some(Cmd::Stmt(Stmt::CreateVirtualTable(vtab))) => Err(LimboError::Corrupt(format!(
                "sqlite_schema root_page must be 0 for virtual table {}, got {root_page}",
                vtab.tbl_name.name.as_str()
            ))),
            other => Err(LimboError::Corrupt(format!(
                "sqlite_schema table row has unexpected SQL {sql:?}: parsed as {other:?}"
            ))),
        }
    }

    /// Build a table definition from an already-parsed `CREATE TABLE`
    /// statement. This is the single AST-to-table lowering path shared by
    /// [`BTreeTable::from_sql`] and callers that hold a translated AST.
    pub fn from_create_table_ast(
        tbl_name: &ast::QualifiedName,
        body: &CreateTableBody,
        root_page: i64,
    ) -> Result<BTreeTable> {
        create_table(tbl_name.name.as_str(), body, root_page)
    }

    /// Reconstruct the SQL for the table.
    /// FIXME: this makes us incompatible with SQLite since sqlite stores the user-provided SQL as is in
    /// `sqlite_schema.sql`
    /// For example, if a user creates a table like: `CREATE TABLE t              (x)`, we store it as
    /// `CREATE TABLE t (x)`, whereas sqlite stores it with the original extra whitespace.
    pub fn to_sql(&self) -> String {
        let mut sql = format!("CREATE TABLE {} (", quote_ident(&self.name));
        let needs_pk_inline = self.primary_key_columns.len() == 1;
        // Add columns
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }

            let column_name = column.name.as_ref().expect("column name is None");
            sql.push_str(&quote_ident(column_name));

            if !column.ty_str.is_empty() {
                sql.push(' ');
                sql.push_str(&column.ty_str);
                if column.is_array() {
                    sql.push_str("[]");
                }
            }

            if column.notnull()
                && (column.explicit_notnull() || !self.is_without_rowid_inline_pk(column))
            {
                sql.push_str(" NOT NULL");
            }

            if column.unique() {
                sql.push_str(" UNIQUE");
            }
            if needs_pk_inline && column.primary_key() {
                sql.push_str(" PRIMARY KEY");
                if self.has_autoincrement && column.is_rowid_alias() {
                    sql.push_str(" AUTOINCREMENT");
                }
            }

            if let Some(default) = &column.default {
                sql.push_str(" DEFAULT ");
                sql.push_str(&default.to_string());
            }

            if let Some(collation) = column.collation_opt() {
                match collation {
                    CollationSeq::Binary => sql.push_str(" COLLATE BINARY"),
                    CollationSeq::NoCase => sql.push_str(" COLLATE NOCASE"),
                    CollationSeq::Rtrim => sql.push_str(" COLLATE RTRIM"),
                    CollationSeq::Locale(_) => {
                        sql.push_str(" COLLATE ");
                        sql.push_str(&quote_ident(&collation.name()));
                    }
                    CollationSeq::Unset | CollationSeq::Custom(_) => {
                        // Unset should not be reachable -- ignore it
                        // Custom collation is not allowed in schema definitions
                    }
                };
            }

            if let GeneratedType::Virtual { original_sql, .. } = &column.generated_type() {
                sql.push_str(" AS (");
                sql.push_str(original_sql);
                sql.push(')');
            }

            // Add column-level CHECK constraints inline
            for check_constraint in &self.check_constraints {
                if check_constraint.column.as_deref() == Some(column_name) {
                    sql.push(' ');
                    if let Some(name) = &check_constraint.name {
                        sql.push_str("CONSTRAINT ");
                        sql.push_str(&Name::exact(name.clone()).as_ident());
                        sql.push(' ');
                    }
                    sql.push_str(&check_constraint.sql());
                }
            }
        }

        let has_table_pk = !self.primary_key_columns.is_empty();
        // Add table-level PRIMARY KEY constraint if exists
        if !needs_pk_inline && has_table_pk {
            sql.push_str(", PRIMARY KEY (");
            for (i, col) in self.primary_key_columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&col.0);
            }
            sql.push(')');
        }

        for fk in &self.foreign_keys {
            sql.push_str(", FOREIGN KEY (");
            for (i, col) in fk.child_columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(col);
            }
            sql.push_str(") REFERENCES ");
            sql.push_str(&fk.parent_table);
            sql.push('(');
            for (i, col) in fk.parent_columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(col);
            }
            sql.push(')');

            // Add ON DELETE/UPDATE actions, NoAction is default so just make empty in that case
            if fk.on_delete != RefAct::NoAction {
                sql.push_str(" ON DELETE ");
                sql.push_str(match fk.on_delete {
                    RefAct::SetNull => "SET NULL",
                    RefAct::SetDefault => "SET DEFAULT",
                    RefAct::Cascade => "CASCADE",
                    RefAct::Restrict => "RESTRICT",
                    _ => "",
                });
            }
            if fk.on_update != RefAct::NoAction {
                sql.push_str(" ON UPDATE ");
                sql.push_str(match fk.on_update {
                    RefAct::SetNull => "SET NULL",
                    RefAct::SetDefault => "SET DEFAULT",
                    RefAct::Cascade => "CASCADE",
                    RefAct::Restrict => "RESTRICT",
                    _ => "",
                });
            }
            if fk.deferred {
                sql.push_str(" DEFERRABLE INITIALLY DEFERRED");
            }
        }

        // Add table-level CHECK constraints (column-level ones were emitted inline above)
        for check_constraint in &self.check_constraints {
            if check_constraint.column.is_some() {
                continue;
            }
            sql.push_str(", ");
            if let Some(name) = &check_constraint.name {
                sql.push_str("CONSTRAINT ");
                sql.push_str(&Name::exact(name.clone()).as_ident());
                sql.push(' ');
            }
            sql.push_str(&check_constraint.sql());
        }

        // Add table-level UNIQUE constraints
        for unique_set in &self.unique_sets {
            // Skip primary key (handled above)
            if unique_set.is_primary_key {
                continue;
            }
            // Skip single-column unique constraints that were already emitted inline
            if unique_set.columns.len() == 1 {
                let col_name = &unique_set.columns[0].name;
                if let Some((_, col)) = self.get_column(col_name) {
                    if col.unique() {
                        continue;
                    }
                }
            }
            sql.push_str(", UNIQUE (");
            for (i, unique_column) in unique_set.columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&quote_ident(&unique_column.name));
            }
            sql.push(')');
        }

        sql.push(')');

        // Add STRICT keyword if this is a STRICT table
        if self.is_strict {
            sql.push_str(" STRICT");
        }
        if !self.has_rowid {
            if self.is_strict {
                sql.push_str(", WITHOUT ROWID");
            } else {
                sql.push_str(" WITHOUT ROWID");
            }
        }

        sql
    }

    fn is_without_rowid_inline_pk(&self, column: &Column) -> bool {
        !self.has_rowid && self.primary_key_columns.len() == 1 && column.primary_key()
    }

    pub fn column_collations(&self) -> Result<Vec<CollationSeq>> {
        Ok(self
            .columns
            .iter()
            .map(|column| column.collation())
            .try_collect()?)
    }

    #[inline]
    pub fn logical_to_physical_column(&self, logical: usize) -> usize {
        self.logical_to_physical_map[logical]
    }

    pub fn build_logical_to_physical_map(
        columns: &[Column],
        primary_key_columns: &[(String, SortOrder)],
        has_rowid: bool,
    ) -> Vec<usize> {
        Self::try_build_logical_to_physical_map(columns, primary_key_columns, has_rowid)
            .expect(crate::alloc::ALLOC_ERR_MSG)
    }

    pub fn try_build_logical_to_physical_map(
        columns: &[Column],
        primary_key_columns: &[(String, SortOrder)],
        has_rowid: bool,
    ) -> Result<Vec<usize>, crate::alloc::TryReserveError> {
        let mut map = try_vec![usize::MAX; columns.len()]?;
        let mut physical = 0;

        if !has_rowid {
            for (pk_name, _) in primary_key_columns {
                let Some((pk_idx, col)) = columns.iter().enumerate().find(|(_, col)| {
                    col.name
                        .as_ref()
                        .is_some_and(|name| name.eq_ignore_ascii_case(pk_name))
                }) else {
                    continue;
                };
                if col.is_virtual_generated() || map[pk_idx] != usize::MAX {
                    continue;
                }
                map[pk_idx] = physical;
                physical += 1;
            }
        }

        for (idx, col) in columns.iter().enumerate() {
            if col.is_virtual_generated() || map[idx] != usize::MAX {
                continue;
            }
            map[idx] = physical;
            physical += 1;
        }

        for offset in &mut map {
            if *offset == usize::MAX {
                *offset = physical;
                physical += 1;
            }
        }
        Ok(map)
    }

    pub fn prepare_generated_columns(&mut self) -> Result<()> {
        {
            let mut guard = self.columns_mut();
            for i in 0..guard.len() {
                if guard[i].is_virtual_generated() {
                    let mut expr = guard[i].generated_expr().cloned().unwrap();
                    resolve_gencol_expr_columns(&mut expr, &guard)?;
                    *guard[i].generated_expr_mut().unwrap() = expr;
                }
            }
        }
        self.column_graph()?;
        Ok(())
    }

    pub fn shift_generated_column_indices_after_drop(
        &mut self,
        dropped_index: usize,
    ) -> Result<()> {
        if !self.has_virtual_columns {
            return Ok(());
        }

        for column in &mut self.columns {
            let Some(expr) = column.generated_expr_mut() else {
                continue;
            };

            walk_expr_mut(expr, &mut |e| match e {
                Expr::Column {
                    table,
                    column,
                    is_rowid_alias: _,
                    ..
                } if table.is_self_table() => {
                    if *column == dropped_index {
                        return Err(LimboError::InternalError(
                            "dropped column remained referenced by generated column".to_string(),
                        ));
                    }
                    if *column > dropped_index {
                        *column -= 1;
                    }
                    Ok(WalkControl::Continue)
                }
                _ => Ok(WalkControl::Continue),
            })?;
        }

        Ok(())
    }

    fn column_graph(&self) -> Result<&GeneratedColGraph> {
        if let Some(graph) = self.column_dependencies.0.get() {
            return Ok(graph);
        }
        let graph = GeneratedColGraph::build(&self.columns)?;
        // we ignore a concurrent initialization, because OnceLock::get_or_try_init is still nightly-only
        let _ = self.column_dependencies.0.set(graph);
        Ok(self
            .column_dependencies
            .0
            .get()
            .expect("column_dependencies was just initialized"))
    }

    /// Returns an iterator over columns in topological (dependency) order. Processing
    /// columns in this order guarantees that all dependencies of generated columns are computed
    /// before the columns that reference them.
    pub(crate) fn columns_topo_sort(&self) -> Result<ColumnsTopologicalSort<'_>> {
        let topo = self.column_graph()?.topological_sort.try_to_vec()?;
        Ok(ColumnsTopologicalSort {
            columns: &self.columns,
            topological_sort: topo,
        })
    }

    #[cfg(test)]
    pub(crate) fn peek_column_dependencies(&self) -> Option<&GeneratedColGraph> {
        self.column_dependencies.0.get()
    }

    pub(crate) fn columns_affected_by_update(
        &self,
        updated_cols: impl IntoIterator<Item = usize>,
    ) -> Result<ColumnMask> {
        let graph = self.column_graph()?;
        let mut affected = ColumnMask::default();
        for i in updated_cols {
            affected.set(i)?;
            if i < graph.dependents.len() {
                let snapshot = graph.dependents[i].try_clone()?;
                affected.union_with(&snapshot)?;
            }
        }
        Ok(affected)
    }

    pub(crate) fn dependencies_of_columns(
        &self,
        targets: impl IntoIterator<Item = usize>,
    ) -> Result<ColumnMask> {
        let graph = self.column_graph()?;
        let mut deps = ColumnMask::default();
        for j in targets {
            if !self.columns[j].is_virtual_generated() {
                deps.set(j)?;
                continue;
            }
            for i in graph.dependencies[j].iter() {
                if !self.columns[i].is_virtual_generated() {
                    deps.set(i)?;
                }
            }
        }
        Ok(deps)
    }
}

/// Topologically sorted generated columns, yielding `(column_index, &Column)`.
pub(crate) struct ColumnsTopologicalSort<'a> {
    columns: &'a [Column],
    /// indices of `columns`
    topological_sort: Vec<usize>,
}

impl<'a> ColumnsTopologicalSort<'a> {
    pub fn iter(&self) -> impl Iterator<Item = (usize, &'a Column)> + '_ {
        self.topological_sort
            .iter()
            .map(|&idx| (idx, &self.columns[idx]))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PseudoCursorType {
    pub column_count: usize,
}

impl PseudoCursorType {
    pub fn new() -> Self {
        Self { column_count: 0 }
    }

    pub fn new_with_columns(columns: impl AsRef<[Column]>) -> Self {
        Self {
            column_count: columns.as_ref().len(),
        }
    }
}

/// A derived table from a FROM clause subquery.
#[derive(Debug, Clone)]
pub struct FromClauseSubquery {
    /// The name of the derived table; uses the alias if available.
    pub name: String,
    /// The query plan for the derived table. Can be either a simple SelectPlan
    /// or a compound select (UNION/INTERSECT/EXCEPT).
    pub plan: Box<Plan>,
    /// The columns of the derived table.
    pub columns: Vec<Column>,
    /// The start register for the result columns of the derived table;
    /// must be set before data is read from it.
    pub result_columns_start_reg: Option<usize>,
    /// The table cursor backing a materialized EphemeralTable representation of
    /// this subquery, if one was emitted.
    pub materialized_cursor_id: Option<CursorID>,
    /// CTE-specific materialization metadata, when this FROM-subquery is a CTE
    /// reference rather than an inline derived table.
    pub cte: Option<FromClauseSubqueryCteMetadata>,
}

/// The one-row table read by the recursive part of a recursive CTE.
#[derive(Debug, Clone)]
pub struct RecursiveCteInput {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Copy)]
pub struct FromClauseSubqueryCteMetadata {
    /// Identity shared by all references to the same CTE definition.
    pub id: usize,
    /// True when more than one read in the same query tree can reuse one
    /// materialized result for this CTE.
    pub shared_materialization: bool,
    /// True for explicit WITH ... AS MATERIALIZED.
    pub materialize_hint: bool,
}

impl FromClauseSubquery {
    pub fn cte_id(&self) -> Option<usize> {
        self.cte.map(|cte| cte.id)
    }

    pub fn materialize_hint(&self) -> bool {
        self.cte.is_some_and(|cte| cte.materialize_hint)
    }

    pub fn shared_materialization(&self) -> bool {
        self.cte.is_some_and(|cte| cte.shared_materialization)
    }

    pub fn set_shared_materialization(&mut self, shared: bool) {
        if let Some(cte) = &mut self.cte {
            cte.shared_materialization = shared;
        }
    }

    /// Shared CTE references and explicit MATERIALIZED hints both force a
    /// table-backed materialization that can be scanned or probed later.
    pub fn requires_table_materialization(&self) -> bool {
        self.shared_materialization() || self.materialize_hint()
    }

    /// Only simple single-reference SELECT subqueries can safely use their
    /// synthesized seek index as the storage target directly. Compound
    /// subqueries still need table-backed storage so their set-operation
    /// semantics are preserved before any later SEARCH shape is chosen.
    pub fn supports_direct_index_materialization(&self) -> bool {
        matches!(self.plan.as_ref(), Plan::Select(_)) && !self.requires_table_materialization()
    }
}

fn collect_column_refs(expr: &Expr) -> HashSet<String> {
    collect_column_dependencies_of_expr(expr, &[])
}

/// Extract all column name references from an expression as a set.
/// `columns` is used to resolve pre-resolved `Expr::Column { SELF_TABLE }` back to names.
//TODO all this usage of [normalize_ident] should be replaced with a proper [Identifier] domain type.
pub fn collect_column_dependencies_of_expr(expr: &Expr, columns: &[Column]) -> HashSet<String> {
    let mut refs = HashSet::default();

    let _ = walk_expr(expr, &mut |e| match e {
        Expr::Id(name) | Expr::Name(name) => {
            refs.insert(normalize_ident(name.as_str()));
            Ok(WalkControl::Continue)
        }
        Expr::Qualified(_, col) | Expr::DoublyQualified(_, _, col) => {
            refs.insert(normalize_ident(col.as_str()));
            Ok(WalkControl::Continue)
        }
        Expr::Column { table, column, .. } if table.is_self_table() => {
            if let Some(col) = columns.get(*column) {
                if let Some(name) = &col.name {
                    refs.insert(normalize_ident(name));
                }
            }
            Ok(WalkControl::Continue)
        }
        Expr::Subquery(_)
        | Expr::Exists(_)
        | Expr::InTable { .. }
        | Expr::SubqueryResult { .. } => Ok(WalkControl::SkipChildren),
        _ => Ok(WalkControl::Continue),
    });

    refs
}

fn collect_column_dependencies_of_gencol(expr: &Expr, columns: &[Column], out: &mut BitSet) {
    let _ = walk_expr(expr, &mut |e| {
        match e {
            Expr::Column { table, column, .. } if table.is_self_table() => {
                out.set(*column)?;
            }
            Expr::Id(name) | Expr::Name(name) => {
                if let Some(idx) = find_column_index_by_name(columns, name.as_str()) {
                    out.set(idx)?;
                }
            }
            Expr::Qualified(_, col) | Expr::DoublyQualified(_, _, col) => {
                if let Some(idx) = find_column_index_by_name(columns, col.as_str()) {
                    out.set(idx)?;
                }
            }
            Expr::Subquery(_)
            | Expr::Exists(_)
            | Expr::InTable { .. }
            | Expr::SubqueryResult { .. } => {
                unreachable!("generated columns cannot contain subqueries")
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    });
}

fn find_column_index_by_name(columns: &[Column], col_name: &str) -> Option<usize> {
    columns.iter().enumerate().find_map(|(i, col)| {
        col.name
            .as_ref()
            .filter(|name| name.eq_ignore_ascii_case(col_name))
            .map(|_| i)
    })
}

/// Resolve [Expr::Id] / [Expr::Qualified] / [Expr::DoublyQualified] in a generated column
/// or partial-index expression to `Expr::Column { table: SELF_TABLE, column: idx }`.
pub fn resolve_gencol_expr_columns(gencol_expr: &mut Expr, columns: &[Column]) -> Result<()> {
    walk_expr_mut(gencol_expr, &mut |e| match e {
        Expr::Id(name) | Expr::Qualified(_, name) | Expr::DoublyQualified(_, _, name) => {
            let col_name = normalize_ident(name.as_str());
            let (idx, col) = columns
                .iter()
                .enumerate()
                .find(|(_, c)| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&col_name))
                })
                .ok_or_else(|| LimboError::ParseError(format!("no such column: {col_name}")))?;
            *e = Expr::Column {
                database: None,
                table: TableInternalId::SELF_TABLE,
                column: idx,
                is_rowid_alias: col.is_rowid_alias(),
            };
            Ok(WalkControl::Continue)
        }
        _ => Ok(WalkControl::Continue),
    })?;
    Ok(())
}

/// Re-render the SQL text of a generated-column expression using current column names. The input
/// AST may have been previously resolved into `Expr::Column { table: SELF_TABLE, column: idx, .. }`
/// nodes; we replace each such self-table reference with a fresh `Expr::Id(<col-name>)` before
/// stringifying so the result round-trips through the parser, even if a referenced column was
/// renamed since the original `original_sql` was captured.
pub fn render_gencol_expr_sql_with_new_names(expr: &Expr, columns: &[Column]) -> Result<String> {
    let mut clone = expr.clone();
    walk_expr_mut(&mut clone, &mut |e| -> Result<WalkControl> {
        if let Expr::Column { table, column, .. } = e {
            if table.is_self_table() {
                if let Some(col) = columns.get(*column) {
                    if let Some(name) = col.name.as_ref() {
                        *e = Expr::Id(Name::exact(name.clone()));
                    }
                }
            }
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(clone.to_string())
}

pub(crate) fn is_deterministic_schema_function_call(func: &Func, args: &[Box<Expr>]) -> bool {
    match func {
        Func::Scalar(
            ScalarFunc::Date
            | ScalarFunc::Time
            | ScalarFunc::DateTime
            | ScalarFunc::UnixEpoch
            | ScalarFunc::JulianDay
            | ScalarFunc::StrfTime
            | ScalarFunc::TimeDiff,
        ) => is_deterministic_datetime_call(func, args),
        _ => func.is_deterministic(),
    }
}

// SQLite allows date/time functions in schema-persistent expressions only when
// they do not depend on the current clock or local timezone. This applies to
// expression indexes, partial index WHERE clauses, and generated columns.
fn is_deterministic_datetime_call(func: &Func, args: &[Box<Expr>]) -> bool {
    match func {
        Func::Scalar(ScalarFunc::Date)
        | Func::Scalar(ScalarFunc::Time)
        | Func::Scalar(ScalarFunc::DateTime)
        | Func::Scalar(ScalarFunc::UnixEpoch)
        | Func::Scalar(ScalarFunc::JulianDay) => {
            !args.is_empty()
                && !is_current_time_expr(args[0].as_ref())
                && !args[1..]
                    .iter()
                    .any(|arg| is_unsafe_datetime_modifier(arg.as_ref()))
        }
        Func::Scalar(ScalarFunc::StrfTime) => {
            args.len() >= 2
                && !is_current_time_expr(args[1].as_ref())
                && !args[2..]
                    .iter()
                    .any(|arg| is_unsafe_datetime_modifier(arg.as_ref()))
        }
        Func::Scalar(ScalarFunc::TimeDiff) => {
            !args.iter().any(|arg| is_current_time_expr(arg.as_ref()))
        }
        _ => unreachable!("non-datetime function passed to datetime index validator"),
    }
}

fn is_current_time_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ast::Literal::String(value)) if string_literal_eq(value, "now")
    ) || matches!(
        expr,
        Expr::Literal(
            ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp
        )
    )
}

fn is_unsafe_datetime_modifier(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ast::Literal::String(value))
            if string_literal_eq(value, "localtime") || string_literal_eq(value, "utc")
    ) || is_current_time_expr(expr)
}

fn string_literal_eq(value: &str, expected: &str) -> bool {
    value.trim_matches('\'').eq_ignore_ascii_case(expected)
}

pub(crate) fn validate_generated_expr(expr: &Expr) -> Result<()> {
    use ast::Expr;
    match expr {
        Expr::Qualified(_, _) => {
            bail_parse_error!("the \".\" operator prohibited in generated columns");
        }
        Expr::DoublyQualified(_, _, _) => {
            bail_parse_error!("the \".\" operator prohibited in generated columns");
        }

        Expr::Variable(_) => {
            bail_parse_error!("bind parameters prohibited in generated columns");
        }

        Expr::Subquery(_) | Expr::InSelect { .. } | Expr::Exists(_) | Expr::InTable { .. } => {
            bail_parse_error!("subqueries prohibited in generated columns");
        }

        Expr::FunctionCall {
            name,
            args,
            filter_over,
            ..
        } => {
            if filter_over.over_clause.is_some() {
                bail_parse_error!("window functions prohibited in generated columns");
            }
            let arg_count = args.len();
            let Some(func) = Func::resolve_function(name.as_str(), arg_count)? else {
                return Err(LimboError::ParseError(format!(
                    "could not resolve function {}",
                    name.as_str()
                )));
            };
            if matches!(func, Func::Agg(_)) {
                bail_parse_error!("aggregate functions prohibited in generated columns");
            }
            if !is_deterministic_schema_function_call(&func, args) {
                bail_parse_error!("non-deterministic functions prohibited in generated columns");
            }
            for arg in args {
                validate_generated_expr(arg)?;
            }
        }

        Expr::FunctionCallStar { name, filter_over } => {
            if filter_over.over_clause.is_some() {
                bail_parse_error!("window functions prohibited in generated columns");
            }
            let Some(func) = Func::resolve_function(name.as_str(), 0)? else {
                return Err(LimboError::ParseError(format!(
                    "could not resolve function {}",
                    name.as_str()
                )));
            };

            if matches!(func, Func::Agg(_)) {
                bail_parse_error!("aggregate functions prohibited in generated columns");
            }
            if !func.is_deterministic() {
                bail_parse_error!("non-deterministic functions prohibited in generated columns");
            }
        }

        Expr::Binary(lhs, _, rhs) => {
            validate_generated_expr(lhs)?;
            validate_generated_expr(rhs)?;
        }
        Expr::Unary(_, inner) => {
            validate_generated_expr(inner)?;
        }
        Expr::Parenthesized(exprs) => {
            for e in exprs {
                validate_generated_expr(e)?;
            }
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
            ..
        } => {
            if let Some(b) = base {
                validate_generated_expr(b)?;
            }
            for (w, t) in when_then_pairs {
                validate_generated_expr(w)?;
                validate_generated_expr(t)?;
            }
            if let Some(e) = else_expr {
                validate_generated_expr(e)?;
            }
        }
        Expr::Cast { expr, .. } => {
            validate_generated_expr(expr)?;
        }
        Expr::InList { lhs, rhs, .. } => {
            validate_generated_expr(lhs)?;
            for e in rhs {
                validate_generated_expr(e)?;
            }
        }
        Expr::Between {
            lhs, start, end, ..
        } => {
            validate_generated_expr(lhs)?;
            validate_generated_expr(start)?;
            validate_generated_expr(end)?;
        }
        Expr::Like {
            lhs, rhs, escape, ..
        } => {
            validate_generated_expr(lhs)?;
            validate_generated_expr(rhs)?;
            if let Some(e) = escape {
                validate_generated_expr(e)?;
            }
        }
        Expr::Collate(inner, _) => {
            validate_generated_expr(inner)?;
        }
        Expr::IsNull(inner) | Expr::NotNull(inner) => {
            validate_generated_expr(inner)?;
        }
        // CURRENT_TIME/DATE/TIMESTAMP parse as literals but evaluate to a
        // different value on every read; SQLite rejects them like any other
        // non-deterministic function (an index on such a column goes stale
        // as soon as the value is recomputed).
        Expr::Literal(
            ast::Literal::CurrentDate | ast::Literal::CurrentTime | ast::Literal::CurrentTimestamp,
        ) => {
            bail_parse_error!("non-deterministic functions prohibited in generated columns");
        }
        _ => {}
    }
    Ok(())
}

/// Peel an optional `COLLATE` wrapper off a PRIMARY KEY / UNIQUE table
/// constraint column, e.g. `PRIMARY KEY(a COLLATE NOCASE)`, returning the
/// inner expression and the resolved collation.
fn constraint_column_collation(expr: &Expr) -> Result<(&Expr, Option<CollationSeq>)> {
    match expr {
        Expr::Collate(inner, collation_name) => {
            let collation_seq = CollationSeq::new(collation_name.as_str())?;
            if collation_seq.is_custom() {
                crate::bail_parse_error!(
                    "custom collations are not supported in schema definitions"
                );
            }
            Ok((inner.as_ref(), Some(collation_seq)))
        }
        _ => Ok((expr, None)),
    }
}

pub fn create_table(tbl_name: &str, body: &CreateTableBody, root_page: i64) -> Result<BTreeTable> {
    let table_name = normalize_ident(tbl_name);
    trace!("Creating table {}", table_name);
    let has_rowid;
    let mut has_autoincrement = false;
    let mut primary_key_columns = vec![];
    let mut foreign_keys = vec![];
    let mut check_constraints = vec![];
    let mut cols: Vec<Column> = vec![];
    let is_strict: bool;
    let mut unique_sets_columns: Vec<UniqueSet> = vec![];
    let mut unique_sets_constraints: Vec<UniqueSet> = vec![];
    match body {
        CreateTableBody::ColumnsAndConstraints {
            columns,
            constraints,
            options,
        } => {
            has_rowid = !options.contains_without_rowid();
            is_strict = options.contains_strict();
            let column_fk_count = columns
                .iter()
                .flat_map(|col| col.constraints.iter())
                .filter(|constraint| {
                    matches!(
                        &constraint.constraint,
                        ast::ColumnConstraint::ForeignKey { .. }
                    )
                })
                .count();

            // we need to preserve order of unique sets definition
            // but also, we analyze constraints first in order to check PRIMARY KEY constraint and recognize rowid alias properly
            // that's why we maintain 2 unique_set sequences and merge them together in the end

            let mut table_fk_order = column_fk_count;
            for c in constraints {
                if let ast::TableConstraint::PrimaryKey {
                    columns,
                    auto_increment,
                    conflict_clause,
                } = &c.constraint
                {
                    if !primary_key_columns.is_empty() {
                        crate::bail_parse_error!(
                            "table \"{}\" has more than one primary key",
                            tbl_name
                        );
                    }
                    if *auto_increment {
                        has_autoincrement = true;
                    }
                    let mut pk_unique_set_columns = Vec::try_with_capacity_ext(columns.len())?;
                    for column in columns {
                        let (expr, collation) = constraint_column_collation(column.expr.as_ref())?;
                        let col_name = match expr {
                            Expr::Id(id) => normalize_ident(id.as_str()),
                            Expr::Literal(Literal::String(value)) => {
                                value.trim_matches('\'').to_owned()
                            }
                            expr => {
                                bail_parse_error!("unsupported primary key expression: {}", expr)
                            }
                        };
                        let sort_order = column.order.unwrap_or(SortOrder::Asc);
                        pk_unique_set_columns.try_push(UniqueSetColumn {
                            name: col_name.clone(),
                            sort_order,
                            collation,
                            nulls_order: column.nulls,
                        })?;
                        primary_key_columns.try_push((col_name, sort_order))?;
                    }
                    unique_sets_constraints.try_push(UniqueSet {
                        columns: pk_unique_set_columns,
                        is_primary_key: true,
                        conflict_clause: *conflict_clause,
                    })?;
                } else if let ast::TableConstraint::Unique {
                    columns,
                    conflict_clause,
                } = &c.constraint
                {
                    let mut unique_columns = Vec::try_with_capacity_ext(columns.len())?;
                    for column in columns {
                        let (expr, collation) = constraint_column_collation(column.expr.as_ref())?;
                        let col_name = match expr {
                            Expr::Id(id) => id.as_str().to_string(),
                            Expr::Literal(Literal::String(value)) => {
                                value.trim_matches('\'').to_owned()
                            }
                            expr => {
                                bail_parse_error!("unsupported unique key expression: {}", expr)
                            }
                        };
                        unique_columns.try_push(UniqueSetColumn {
                            name: col_name,
                            sort_order: column.order.unwrap_or(SortOrder::Asc),
                            collation,
                            nulls_order: column.nulls,
                        })?;
                    }
                    let unique_set = UniqueSet {
                        columns: unique_columns,
                        is_primary_key: false,
                        conflict_clause: *conflict_clause,
                    };
                    unique_sets_constraints.try_push(unique_set)?;
                } else if let ast::TableConstraint::ForeignKey {
                    columns,
                    clause,
                    defer_clause,
                } = &c.constraint
                {
                    let child_columns: Box<[String]> = columns
                        .iter()
                        .map(|ic| normalize_ident(ic.col_name.as_str()))
                        .try_collect()?;
                    // derive parent columns: explicit or default to parent PK
                    let parent_table = normalize_ident(clause.tbl_name.as_str());
                    let parent_columns: Box<[String]> = clause
                        .columns
                        .iter()
                        .map(|ic| normalize_ident(ic.col_name.as_str()))
                        .try_collect()?;

                    // Only check arity if parent columns were explicitly listed
                    if !parent_columns.is_empty() && child_columns.len() != parent_columns.len() {
                        crate::bail_parse_error!(
                            "foreign key on \"{}\" has {} child column(s) but {} parent column(s)",
                            tbl_name,
                            child_columns.len(),
                            parent_columns.len()
                        );
                    }
                    // deferrable semantics
                    let deferred = match defer_clause {
                        Some(d) => {
                            d.deferrable
                                && matches!(
                                    d.init_deferred,
                                    Some(InitDeferredPred::InitiallyDeferred)
                                )
                        }
                        None => false, // NOT DEFERRABLE INITIALLY IMMEDIATE by default
                    };
                    let fk = ForeignKey {
                        parent_table,
                        parent_columns,
                        child_columns,
                        on_delete: clause
                            .args
                            .iter()
                            .find_map(|a| {
                                if let ast::RefArg::OnDelete(x) = a {
                                    Some(*x)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(RefAct::NoAction),
                        on_update: clause
                            .args
                            .iter()
                            .find_map(|a| {
                                if let ast::RefArg::OnUpdate(x) = a {
                                    Some(*x)
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(RefAct::NoAction),
                        deferred,
                        decl_order: table_fk_order,
                    };
                    foreign_keys.try_push(Arc::new(fk))?;
                    table_fk_order += 1;
                } else if let ast::TableConstraint::Check { expr, source } = &c.constraint {
                    check_constraints.try_push(CheckConstraint::new(
                        c.name.as_ref(),
                        expr,
                        source.as_deref(),
                        None,
                    ))?;
                }
            }

            // Due to a bug in SQLite, this check is needed to maintain backwards compatibility with rowid alias
            // SQLite docs: https://sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
            // Issue: https://github.com/tursodatabase/turso/issues/3665
            let mut primary_key_desc_columns_constraint = false;

            let mut column_fk_order = 0;
            for ast::ColumnDefinition {
                col_name,
                col_type,
                constraints,
            } in columns
            {
                let name = col_name.as_str().to_string();
                // Regular sqlite tables have an integer rowid that uniquely identifies a row.
                // Even if you create a table with a column e.g. 'id INT PRIMARY KEY', there will still
                // be a separate hidden rowid, and the 'id' column will have a separate index built for it.
                //
                // However:
                // A column defined as exactly INTEGER PRIMARY KEY is a rowid alias, meaning that the rowid
                // and the value of this column are the same.
                // https://www.sqlite.org/lang_createtable.html#rowids_and_the_integer_primary_key
                let ty_str = col_type
                    .as_ref()
                    .cloned()
                    .map(|ast::Type { name, .. }| name)
                    .unwrap_or_default();

                let ty_params: std::vec::Vec<Box<Expr>> = match col_type {
                    Some(ast::Type {
                        size: Some(ast::TypeSize::MaxSize(ref expr)),
                        ..
                    }) => std::vec![expr.clone()],
                    Some(ast::Type {
                        size: Some(ast::TypeSize::TypeSize(ref e1, ref e2)),
                        ..
                    }) => std::vec![e1.clone(), e2.clone()],
                    _ => std::vec::Vec::new(),
                };

                let mut typename_exactly_integer = false;
                let ty = match col_type {
                    Some(data_type) => {
                        let (ty, ei) = type_from_name(&data_type.name);
                        typename_exactly_integer = ei;
                        ty
                    }
                    None => Type::Null,
                };

                let mut default = None;
                let mut generated: Option<Box<Expr>> = None;
                let mut primary_key = false;
                let mut notnull = false;
                let mut explicit_notnull = false;
                let mut notnull_conflict_clause = None;
                let mut order = SortOrder::Asc;
                let mut unique = false;
                let mut collation = None;
                for c_def in constraints {
                    match &c_def.constraint {
                        ast::ColumnConstraint::Check { expr, source } => {
                            check_constraints.try_push(CheckConstraint::new(
                                c_def.name.as_ref(),
                                expr,
                                source.as_deref(),
                                Some(&name),
                            ))?;
                        }
                        ast::ColumnConstraint::Generated { expr, typ } => {
                            if typ
                                .as_ref()
                                .is_some_and(|t| matches!(t, ast::GeneratedColumnType::Stored))
                            {
                                bail_parse_error!("Stored generated columns are not supported");
                            }
                            validate_generated_expr(expr)?;
                            generated = Some(expr.clone());
                        }
                        ast::ColumnConstraint::PrimaryKey {
                            order: o,
                            auto_increment,
                            conflict_clause,
                            ..
                        } => {
                            if !primary_key_columns.is_empty() {
                                crate::bail_parse_error!(
                                    "table \"{}\" has more than one primary key",
                                    tbl_name
                                );
                            }
                            primary_key = true;
                            if *auto_increment {
                                has_autoincrement = true;
                            }
                            if let Some(o) = o {
                                order = *o;
                            }
                            unique_sets_columns.try_push(UniqueSet {
                                columns: try_vec![UniqueSetColumn {
                                    name: name.clone(),
                                    sort_order: order,
                                    collation: None,
                                    nulls_order: None,
                                }]?,
                                is_primary_key: true,
                                conflict_clause: *conflict_clause,
                            })?;
                        }
                        ast::ColumnConstraint::NotNull {
                            nullable,
                            conflict_clause,
                            ..
                        } => {
                            notnull = !nullable;
                            explicit_notnull = !nullable;
                            notnull_conflict_clause = *conflict_clause;
                        }
                        ast::ColumnConstraint::Default(ref expr) => {
                            default = Some(
                                translate_ident_to_string_literal(expr)
                                    .unwrap_or_else(|| expr.clone()),
                            );
                        }
                        ast::ColumnConstraint::Unique(conflict) => {
                            unique = true;
                            unique_sets_columns.try_push(UniqueSet {
                                columns: try_vec![UniqueSetColumn {
                                    name: name.clone(),
                                    sort_order: order,
                                    collation: None,
                                    nulls_order: None,
                                }]?,
                                is_primary_key: false,
                                conflict_clause: *conflict,
                            })?;
                        }
                        ast::ColumnConstraint::Collate { ref collation_name } => {
                            let collation_seq = CollationSeq::new(collation_name.as_str())?;
                            if collation_seq.is_custom() {
                                crate::bail_parse_error!(
                                    "custom collations are not supported in schema definitions"
                                );
                            }
                            collation = Some(collation_seq);
                        }
                        ast::ColumnConstraint::ForeignKey {
                            clause,
                            defer_clause,
                        } => {
                            if clause.columns.len() > 1 {
                                crate::bail_parse_error!(
                                    "foreign key on {} should reference only one column of table {}",
                                    name,
                                    clause.tbl_name.as_str()
                                );
                            }
                            let fk = ForeignKey {
                                parent_table: normalize_ident(clause.tbl_name.as_str()),
                                parent_columns: clause
                                    .columns
                                    .iter()
                                    .map(|c| normalize_ident(c.col_name.as_str()))
                                    .try_collect()?,
                                on_delete: clause
                                    .args
                                    .iter()
                                    .find_map(|arg| {
                                        if let ast::RefArg::OnDelete(act) = arg {
                                            Some(*act)
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or(RefAct::NoAction),
                                on_update: clause
                                    .args
                                    .iter()
                                    .find_map(|arg| {
                                        if let ast::RefArg::OnUpdate(act) = arg {
                                            Some(*act)
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or(RefAct::NoAction),
                                child_columns: Box::from([name.clone()]),
                                deferred: match defer_clause {
                                    Some(d) => {
                                        d.deferrable
                                            && matches!(
                                                d.init_deferred,
                                                Some(InitDeferredPred::InitiallyDeferred)
                                            )
                                    }
                                    None => false,
                                },
                                decl_order: column_fk_order,
                            };
                            foreign_keys.try_push(Arc::new(fk))?;
                            column_fk_order += 1;
                        }
                    }
                }

                if let Some(ref gen_expr) = generated {
                    if primary_key {
                        bail_parse_error!(
                            "generated column \"{}\" cannot be part of the PRIMARY KEY",
                            name
                        );
                    }
                    if default.is_some() {
                        bail_parse_error!(
                            "generated column \"{}\" cannot have a DEFAULT value",
                            name
                        );
                    }

                    let referenced_cols = collect_column_refs(gen_expr);
                    let current_col_name = normalize_ident(&name);

                    if referenced_cols.iter().any(|c| c == &current_col_name) {
                        bail_parse_error!("generated column \"{}\" cannot reference itself", name);
                    }
                }

                if primary_key {
                    primary_key_columns.try_push((name.clone(), order))?;
                    if order == SortOrder::Desc {
                        primary_key_desc_columns_constraint = true;
                    }
                } else if primary_key_columns
                    .iter()
                    .any(|(col_name, _)| col_name.eq_ignore_ascii_case(&name))
                {
                    if generated.is_some() {
                        crate::bail_parse_error!(
                            "generated column \"{}\" cannot be part of the PRIMARY KEY",
                            name
                        );
                    }
                    primary_key = true;
                }

                let mut col = Column::new(
                    Some(name),
                    ty_str,
                    default,
                    generated,
                    ty,
                    collation,
                    ColDef {
                        primary_key,
                        rowid_alias: typename_exactly_integer
                            && primary_key
                            && !primary_key_desc_columns_constraint,
                        notnull,
                        explicit_notnull,
                        unique,
                        hidden: false,
                        notnull_conflict_clause,
                    },
                );
                col.ty_params = ty_params;
                if let Some(t) = col_type.as_ref() {
                    if t.is_array() {
                        col.set_array_dimensions(t.array_dimensions);
                    }
                }
                cols.try_push(col)?;
            }
        }
        CreateTableBody::AsSelect(_) => {
            crate::bail_parse_error!("CREATE TABLE AS SELECT is not supported")
        }
    };

    // flip is_rowid_alias back to false if the table has multiple primary key columns
    // or if the table has no rowid
    if !has_rowid || primary_key_columns.len() > 1 {
        for col in cols.iter_mut() {
            col.set_rowid_alias(false);
        }
    }

    if has_autoincrement {
        // only allow integers
        if primary_key_columns.len() != 1 {
            crate::bail_parse_error!("AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY");
        }

        let pk_col_name = &primary_key_columns[0].0;
        let pk_col = cols.iter().find(|c| {
            c.name
                .as_deref()
                .is_some_and(|n| n.eq_ignore_ascii_case(pk_col_name))
        });

        if let Some(col) = pk_col {
            if col.ty() != Type::Integer {
                crate::bail_parse_error!("AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY");
            }
        }
    }

    // concat unqiue_sets collected from column definitions and constraints in correct order
    let mut unique_sets = unique_sets_columns
        .into_iter()
        .chain(unique_sets_constraints)
        .try_collect::<Vec<_>>()?;
    // Capture PK conflict clause before the rowid-alias UniqueSet is removed.
    let rowid_alias_conflict_clause = unique_sets
        .iter()
        .find(|us| us.is_primary_key)
        .and_then(|us| us.conflict_clause);
    for col in cols.iter() {
        if col.is_rowid_alias() {
            // Unique sets are used for creating automatic indexes. An index is not created for a rowid alias PRIMARY KEY.
            // However, an index IS created for a rowid alias UNIQUE, e.g. CREATE TABLE t(x INTEGER PRIMARY KEY, UNIQUE(x))
            let unique_set_w_only_rowid_alias = unique_sets.iter().position(|us| {
                us.is_primary_key
                    && us.columns.len() == 1
                    && us
                        .columns
                        .first()
                        .unwrap()
                        .name
                        .eq_ignore_ascii_case(col.name.as_ref().unwrap())
            });
            if let Some(u) = unique_set_w_only_rowid_alias {
                unique_sets.remove(u);
            }
        }
    }

    let mut table = BTreeTable {
        root_page,
        name: table_name,
        has_rowid,
        primary_key_columns,
        has_autoincrement,
        columns: cols,
        is_strict,
        foreign_keys,
        unique_sets: {
            // If there are any unique sets that have identical column names in the same order (even if they are PRIMARY KEY and UNIQUE and have different sort orders), remove the duplicates.
            // Examples:
            // PRIMARY KEY (a, b) and UNIQUE (a desc, b) are the same
            // PRIMARY KEY (a, b) and UNIQUE (b, a) are not the same
            // Using a n^2 monkey algorithm here because n is small, CPUs are fast, life is short, and most importantly:
            // we want to preserve the order of the sets -- automatic index names in sqlite_schema must be in definition order.
            let mut i = 0;
            while i < unique_sets.len() {
                let mut j = i + 1;
                while j < unique_sets.len() {
                    let lengths_equal =
                        unique_sets[i].columns.len() == unique_sets[j].columns.len();
                    if lengths_equal
                        && unique_sets[i]
                            .columns
                            .iter()
                            .zip(unique_sets[j].columns.iter())
                            .all(|(a, b)| a.name.eq_ignore_ascii_case(&b.name))
                    {
                        // SQLite rejects duplicate constraints on the same columns when both
                        // specify ON CONFLICT with different resolve types.
                        if let (Some(a), Some(b)) = (
                            unique_sets[i].conflict_clause,
                            unique_sets[j].conflict_clause,
                        ) {
                            if a != b {
                                crate::bail_parse_error!(
                                    "conflicting ON CONFLICT clauses specified"
                                );
                            }
                        }
                        unique_sets.remove(j);
                    } else {
                        j += 1;
                    }
                }
                i += 1;
            }
            unique_sets
        },
        check_constraints,
        rowid_alias_conflict_clause,
        has_virtual_columns: false,
        logical_to_physical_map: vec![],
        column_dependencies: Default::default(),
    };
    table.prepare_generated_columns()?;
    if !table.has_rowid {
        if table.primary_key_columns.is_empty() {
            crate::bail_parse_error!("PRIMARY KEY missing on table {}", table.name);
        }
        for (pk_name, _) in &table.primary_key_columns {
            let Some((_, col)) = table.get_column(pk_name) else {
                crate::bail_parse_error!(
                    "PRIMARY KEY column {pk_name} not found in table {}",
                    table.name
                );
            };
            if !col.notnull() {
                let Some(idx) = table.get_column(pk_name).map(|(idx, _)| idx) else {
                    unreachable!("PRIMARY KEY column should exist");
                };
                table.columns[idx].set_notnull(true);
            }
        }
    }
    table.logical_to_physical_map = BTreeTable::build_logical_to_physical_map(
        &table.columns,
        &table.primary_key_columns,
        table.has_rowid,
    );
    Ok(table)
}

/// SQLite treats bare identifiers in DEFAULT clauses as string literals.
/// E.g., `DEFAULT hello` becomes the string "hello", not a column reference.
pub fn translate_ident_to_string_literal(expr: &Expr) -> Option<Box<Expr>> {
    match expr {
        Expr::Name(name) | Expr::Id(name) => {
            Some(Box::new(Expr::Literal(Literal::String(name.as_literal()))))
        }
        _ => None,
    }
}

pub fn _build_pseudo_table(columns: &[ResultColumn]) -> PseudoCursorType {
    let table = PseudoCursorType::new();
    for column in columns {
        match column {
            ResultColumn::Expr(expr, _as_name) => {
                todo!("unsupported expression {:?}", expr);
            }
            ResultColumn::Star => {
                todo!();
            }
            ResultColumn::TableStar(_) => {
                todo!();
            }
        }
    }
    table
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Columns in this table (child side). Never empty (validated at parse time).
    pub child_columns: Box<[String]>,
    /// Referenced (parent) table
    pub parent_table: String,
    /// Parent-side referenced columns. Empty means "use parent's PRIMARY KEY".
    pub parent_columns: Box<[String]>,
    pub on_delete: RefAct,
    pub on_update: RefAct,
    /// DEFERRABLE INITIALLY DEFERRED
    pub deferred: bool,
    /// Declaration order among this table's foreign key constraints.
    ///
    /// SQLite reports PRAGMA foreign_key_list rows in reverse declaration order.
    pub decl_order: usize,
}
#[inline]
fn fk_mismatch_err(child: &str, parent: &str) -> crate::LimboError {
    crate::LimboError::ForeignKeyConstraint(format!(
        "foreign key mismatch - \"{child}\" referencing \"{parent}\""
    ))
}

impl ForeignKey {
    fn validate(&self) -> Result<()> {
        if self
            .parent_columns
            .iter()
            .any(|c| ROWID_STRS.iter().any(|&r| r.eq_ignore_ascii_case(c)))
        {
            return Err(crate::LimboError::ForeignKeyConstraint(format!(
                "foreign key mismatch referencing \"{}\"",
                self.parent_table
            )));
        }
        Ok(())
    }
}

/// A single resolved foreign key where `parent_table == target`.
///
/// Child column names live in `fk.child_columns` — not duplicated here.
#[derive(Clone, Debug)]
pub struct ResolvedFkRef {
    /// Child table that owns the FK.
    pub child_table: Arc<BTreeTable>,
    /// The FK as declared on the child table.
    pub fk: Arc<ForeignKey>,

    /// Resolved parent columns: either `fk.parent_columns` or, when that is
    /// empty, the parent table's PRIMARY KEY columns. Always non-empty.
    pub parent_cols: Box<[String]>,
    /// Column positions in the child/parent tables (pos_in_table)
    pub child_pos: BoxedSlice<usize>,
    pub parent_pos: BoxedSlice<usize>,

    /// If the parent key is rowid or a rowid-alias (single-column only)
    pub parent_uses_rowid: bool,
    /// For non-rowid parents: the UNIQUE index that enforces the parent key.
    /// (None when `parent_uses_rowid == true`.)
    pub parent_unique_index: Option<Arc<Index>>,
}

impl ResolvedFkRef {
    /// Returns if any referenced parent column can change when these column positions are updated.
    pub fn parent_key_may_change(
        &self,
        updated_parent_positions: &ColumnMask,
        parent_tbl: &BTreeTable,
    ) -> Result<bool> {
        if self.parent_uses_rowid {
            return Ok(self.parent_key_may_change_with_affected_columns(
                updated_parent_positions,
                updated_parent_positions,
                parent_tbl,
            ));
        }
        let affected_parent_positions =
            parent_tbl.columns_affected_by_update(updated_parent_positions)?;
        Ok(self.parent_key_may_change_with_affected_columns(
            updated_parent_positions,
            &affected_parent_positions,
            parent_tbl,
        ))
    }

    pub(crate) fn parent_key_may_change_with_affected_columns(
        &self,
        updated_parent_positions: &ColumnMask,
        affected_parent_positions: &ColumnMask,
        parent_tbl: &BTreeTable,
    ) -> bool {
        if self.parent_uses_rowid {
            // parent rowid changes if the parent's rowid or alias is updated
            if let Some((idx, _)) = parent_tbl
                .columns
                .iter()
                .enumerate()
                .find(|(_, c)| c.is_rowid_alias())
            {
                return updated_parent_positions.get(idx);
            }
            // Without a rowid alias, a direct rowid update is represented separately with ROWID_SENTINEL
            return true;
        }
        self.parent_pos
            .iter()
            .any(|p| affected_parent_positions.get(*p))
    }

    /// Returns if any child column of this FK is in `updated_child_positions`
    pub fn child_key_changed(
        &self,
        updated_child_positions: &ColumnMask,
        child_tbl: &BTreeTable,
    ) -> bool {
        if self
            .child_pos
            .iter()
            .any(|p| updated_child_positions.get(*p))
        {
            return true;
        }
        // special case: if FK uses a rowid alias on child, and rowid changed
        if self.fk.child_columns.len() == 1 {
            let (i, col) = child_tbl.get_column(&self.fk.child_columns[0]).unwrap();
            if col.is_rowid_alias() && updated_child_positions.get(i) {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: Option<String>,
    pub ty_str: String,
    pub ty_params: std::vec::Vec<Box<Expr>>,
    pub default: Option<Box<Expr>>,
    generated_type: GeneratedType,
    info: ColumnInfo,
    explicit_notnull: bool,
    /// ON CONFLICT clause for NOT NULL constraint on this column.
    pub notnull_conflict_clause: Option<ResolveType>,
}

#[derive(Default)]
pub struct ColDef {
    pub primary_key: bool,
    pub rowid_alias: bool,
    pub notnull: bool,
    pub explicit_notnull: bool,
    pub unique: bool,
    pub hidden: bool,
    pub notnull_conflict_clause: Option<ResolveType>,
}

#[derive(Debug, Clone)]
pub enum GeneratedType {
    /// `resolved` holds the expression with column references resolved to
    /// `Expr::Column { table: SELF_TABLE }` for use at compile time.
    /// `original_sql` preserves the original SQL text for `to_sql()` round-tripping.
    Virtual {
        expr: Box<Expr>,
        original_sql: String,
    },
    // Stored { resolved: Box<Expr>, original_sql: String },
    NotGenerated,
}

impl Column {
    pub fn affinity(&self) -> Affinity {
        self.info
            .affinity()
            .unwrap_or_else(|| Affinity::affinity(&self.ty_str))
    }

    pub fn affinity_with_strict(&self, is_strict: bool) -> Affinity {
        if is_strict && self.ty_str.eq_ignore_ascii_case("ANY") {
            Affinity::Blob
        } else {
            self.affinity()
        }
    }
    pub fn new_default_text(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
    ) -> Self {
        Self::new(
            name,
            ty_str,
            default,
            None,
            Type::Text,
            None,
            ColDef::default(),
        )
    }
    pub fn new_default_integer(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
    ) -> Self {
        Self::new(
            name,
            ty_str,
            default,
            None,
            Type::Integer,
            None,
            ColDef::default(),
        )
    }
    #[inline]
    pub fn new(
        name: Option<String>,
        ty_str: String,
        default: Option<Box<Expr>>,
        generated: Option<Box<Expr>>,
        ty: Type,
        col: Option<CollationSeq>,
        coldef: ColDef,
    ) -> Self {
        let generated_type = match generated {
            Some(expr) => {
                let original_sql = expr.to_string();
                GeneratedType::Virtual { expr, original_sql }
            }
            None => GeneratedType::NotGenerated,
        };
        let mut info = ColumnInfo::new(NewColumnInfoParams {
            ty,
            collation: col,
            coldef: &coldef,
        });
        info.override_affinity(Affinity::affinity(&ty_str));

        Self {
            name,
            ty_str,
            ty_params: std::vec::Vec::new(),
            default,
            generated_type,
            info,
            explicit_notnull: coldef.explicit_notnull,
            notnull_conflict_clause: coldef.notnull_conflict_clause,
        }
    }

    #[inline]
    pub fn collation_opt(&self) -> Option<CollationSeq> {
        if self.has_explicit_collation() {
            Some(self.collation())
        } else {
            None
        }
    }

    #[inline]
    pub fn explicit_notnull(&self) -> bool {
        self.explicit_notnull
    }

    /// Returns an error if this column is a generated column.
    /// `verb_phrase` should describe the operation, e.g. "INSERT into" or "UPDATE".
    pub fn ensure_not_generated(&self, verb_phrase: &str, col_name: &str) -> Result<()> {
        if !matches!(self.generated_type, GeneratedType::NotGenerated) {
            bail_parse_error!("cannot {} generated column \"{}\"", verb_phrase, col_name);
        }
        Ok(())
    }

    #[inline]
    pub fn generated_type(&self) -> &GeneratedType {
        &self.generated_type
    }

    #[inline]
    pub fn is_generated(&self) -> bool {
        !matches!(self.generated_type, GeneratedType::NotGenerated)
    }

    #[inline]
    pub fn is_virtual_generated(&self) -> bool {
        matches!(self.generated_type, GeneratedType::Virtual { .. })
    }

    #[inline]
    pub fn generated_expr(&self) -> Option<&Expr> {
        match &self.generated_type {
            GeneratedType::Virtual { expr, .. } => Some(expr.as_ref()),
            GeneratedType::NotGenerated => None,
        }
    }

    #[inline]
    pub fn generated_expr_mut(&mut self) -> Option<&mut Expr> {
        match &mut self.generated_type {
            GeneratedType::Virtual { expr, .. } => Some(expr.as_mut()),
            GeneratedType::NotGenerated => None,
        }
    }

    #[inline]
    pub fn set_generated_original_sql(&mut self, new_sql: String) {
        if let GeneratedType::Virtual {
            ref mut original_sql,
            ..
        } = self.generated_type
        {
            *original_sql = new_sql;
        }
    }

    #[inline]
    pub fn override_affinity(&mut self, affinity: Affinity) {
        self.info.override_affinity(affinity)
    }
    #[inline]
    pub fn ty(&self) -> Type {
        self.info.ty()
    }
    #[inline]
    pub fn set_ty(&mut self, ty: Type) {
        self.info.set_ty(ty)
    }
    #[inline]
    pub fn collation(&self) -> CollationSeq {
        self.info.collation()
    }
    #[inline]
    pub fn has_explicit_collation(&self) -> bool {
        self.info.has_explicit_collation()
    }
    #[inline]
    pub fn set_collation(&mut self, c: Option<CollationSeq>) {
        self.info.set_collation(c)
    }
    #[inline]
    pub fn primary_key(&self) -> bool {
        self.info.primary_key()
    }
    #[inline]
    pub fn is_rowid_alias(&self) -> bool {
        self.info.is_rowid_alias()
    }
    #[inline]
    pub fn notnull(&self) -> bool {
        self.info.notnull()
    }
    #[inline]
    pub fn set_notnull(&mut self, v: bool) {
        self.info.set_notnull(v)
    }
    #[inline]
    pub fn unique(&self) -> bool {
        self.info.unique()
    }
    #[inline]
    pub fn hidden(&self) -> bool {
        self.info.hidden()
    }
    #[inline]
    pub fn set_rowid_alias(&mut self, v: bool) {
        self.info.set_rowid_alias(v)
    }
    #[inline]
    pub fn set_unique(&mut self, v: bool) {
        self.info.set_unique(v)
    }
    #[inline]
    pub fn set_hidden(&mut self, v: bool) {
        self.info.set_hidden(v)
    }
    #[inline]
    pub fn is_array(&self) -> bool {
        self.info.is_array()
    }
    /// Number of array dimensions (0 = scalar, 1 = `[]`, 2 = `[][]`, etc.)
    #[inline]
    pub fn array_dimensions(&self) -> u32 {
        self.info.array_dimensions()
    }
    #[inline]
    pub fn set_array_dimensions(&mut self, dims: u32) {
        self.info.set_array_dimensions(dims)
    }
}

// TODO: This might replace some of util::columns_from_create_table_body
impl TryFrom<&ColumnDefinition> for Column {
    type Error = crate::LimboError;

    fn try_from(value: &ColumnDefinition) -> crate::Result<Self> {
        let name = value.col_name.as_str();

        let mut default = None;
        let mut generated = None;
        let mut notnull = false;
        let mut notnull_conflict_clause = None;
        let mut primary_key = false;
        let mut unique = false;
        let mut collation = None;

        for ast::NamedColumnConstraint { constraint, .. } in &value.constraints {
            match constraint {
                ast::ColumnConstraint::PrimaryKey { .. } => primary_key = true,
                ast::ColumnConstraint::NotNull {
                    nullable,
                    conflict_clause,
                    ..
                } => {
                    notnull = !nullable;
                    notnull_conflict_clause = *conflict_clause;
                }
                ast::ColumnConstraint::Unique(..) => unique = true,
                ast::ColumnConstraint::Default(expr) => {
                    default.replace(
                        translate_ident_to_string_literal(expr).unwrap_or_else(|| expr.clone()),
                    );
                }
                ast::ColumnConstraint::Collate { collation_name } => {
                    let collation_seq = CollationSeq::new(collation_name.as_str())?;
                    if collation_seq.is_custom() {
                        crate::bail_parse_error!(
                            "custom collations are not supported in schema definitions"
                        );
                    }
                    collation.replace(collation_seq);
                }
                ast::ColumnConstraint::Generated { expr, .. } => {
                    generated = Some(expr.clone());
                }
                _ => {}
            };
        }

        let ty = match value.col_type {
            Some(ref data_type) => type_from_name(&data_type.name).0,
            None => Type::Null,
        };

        let ty_str = value
            .col_type
            .as_ref()
            .map(|t| t.name.to_string())
            .unwrap_or_default();

        let ty_params: std::vec::Vec<Box<turso_parser::ast::Expr>> = match &value.col_type {
            Some(ast::Type {
                size: Some(ast::TypeSize::MaxSize(ref expr)),
                ..
            }) => std::vec![expr.clone()],
            Some(ast::Type {
                size: Some(ast::TypeSize::TypeSize(ref e1, ref e2)),
                ..
            }) => std::vec![e1.clone(), e2.clone()],
            _ => std::vec::Vec::new(),
        };

        let hidden = ty_str.contains("HIDDEN");

        let mut col = Column::new(
            Some(name.to_string()),
            ty_str,
            default,
            generated,
            ty,
            collation,
            ColDef {
                primary_key,
                rowid_alias: primary_key && matches!(ty, Type::Integer),
                notnull,
                explicit_notnull: notnull,
                unique,
                hidden,
                notnull_conflict_clause,
            },
        );
        col.ty_params = ty_params;
        if let Some(t) = value.col_type.as_ref() {
            if t.is_array() {
                col.set_array_dimensions(t.array_dimensions);
            }
        }
        Ok(col)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    Null = 0,
    Text = 1,
    Numeric = 2,
    Integer = 3,
    Real = 4,
    Blob = 5,
}

impl Type {
    #[inline]
    const fn from_bits(bits: u8) -> Self {
        match bits {
            0 => Type::Null,
            1 => Type::Text,
            2 => Type::Numeric,
            3 => Type::Integer,
            4 => Type::Real,
            5 => Type::Blob,
            _ => Type::Null,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Null => "",
            Self::Text => "TEXT",
            Self::Numeric => "NUMERIC",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
        };
        write!(f, "{s}")
    }
}

pub fn sqlite_schema_table() -> Result<BTreeTable> {
    let columns = try_vec![
        Column::new_default_text(Some("type".to_string()), "TEXT".to_string(), None),
        Column::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        Column::new_default_text(Some("tbl_name".to_string()), "TEXT".to_string(), None),
        Column::new_default_integer(Some("rootpage".to_string()), "INT".to_string(), None),
        Column::new_default_text(Some("sql".to_string()), "TEXT".to_string(), None),
    ]?;
    let logical_to_physical_map =
        BTreeTable::try_build_logical_to_physical_map(&columns, &[], true)?;
    Ok(BTreeTable {
        root_page: 1,
        name: "sqlite_schema".to_string(),
        has_rowid: true,
        is_strict: false,
        has_autoincrement: false,
        primary_key_columns: try_vec![]?,
        columns,
        foreign_keys: try_vec![]?,
        check_constraints: try_vec![]?,
        rowid_alias_conflict_clause: None,
        unique_sets: try_vec![]?,
        has_virtual_columns: false,
        logical_to_physical_map,
        column_dependencies: Default::default(),
    })
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub root_page: i64,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub ephemeral: bool,
    /// Does the index have a rowid as the last column?
    /// This is the case for btree indexes (persistent or ephemeral) that
    /// have been created based on a table with a rowid.
    /// For example, WITHOUT ROWID tables and SELECT DISTINCT ephemeral indexes
    /// will not have a rowid.
    pub has_rowid: bool,
    pub where_clause: Option<Box<Expr>>,
    pub index_method: Option<Arc<dyn IndexMethodAttachment>>,
    /// ON CONFLICT clause from the constraint definition (PRIMARY KEY or UNIQUE).
    pub on_conflict: Option<ResolveType>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub name: String,
    pub order: SortOrder,
    pub nulls_order: Option<NullsOrder>,
    /// the position of the column in the source table.
    /// for example:
    /// CREATE TABLE t (a,b,c)
    /// CREATE INDEX idx ON t(b)
    /// b.pos_in_table == 1
    pub pos_in_table: usize,
    pub collation: Option<CollationSeq>,
    pub default: Option<Box<Expr>>,
    /// Expression for expression indexes. None for simple column indexes.
    pub expr: Option<Box<Expr>>,
}

macro_rules! impl_effective_nulls_order {
    ($ty:ty) => {
        impl $ty {
            pub fn effective_nulls_order_when_iterated(
                &self,
                iter_dir: $crate::translate::plan::IterationDirection,
            ) -> turso_parser::ast::NullsOrder {
                match iter_dir {
                    $crate::translate::plan::IterationDirection::Forwards => {
                        self.effective_nulls_order()
                    }
                    $crate::translate::plan::IterationDirection::Backwards => {
                        self.effective_nulls_order().reverse()
                    }
                }
            }

            pub fn effective_nulls_order(&self) -> turso_parser::ast::NullsOrder {
                self.nulls_order
                    .unwrap_or_else(|| turso_parser::ast::NullsOrder::default_for(self.order))
            }
        }
    };
}

pub(crate) use impl_effective_nulls_order;

impl_effective_nulls_order!(IndexColumn);

impl IndexColumn {
    /// Returns a default column with the given name and position.
    pub fn new(name: impl ToString, pos_in_table: usize) -> Self {
        Self {
            name: name.to_string(),
            order: SortOrder::Asc,
            nulls_order: None,
            pos_in_table,
            collation: None,
            default: None,
            expr: None,
        }
    }

    pub fn new_many<I>(names: I) -> Vec<Self>
    where
        I: IntoIterator,
        I::Item: ToString,
        I::IntoIter: ExactSizeIterator,
    {
        let iter = names.into_iter();
        let mut cols = <Vec<_> as TursoVecExt<_>>::with_capacity(iter.len());

        iter.enumerate()
            .map(|(i, name)| Self {
                name: name.to_string(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: i,
                collation: None,
                default: None,
                expr: None,
            })
            .for_each(|col| cols.push(col));

        cols
    }
}

impl Index {
    pub fn from_sql(
        syms: &SymbolTable,
        sql: &str,
        root_page: i64,
        table: &BTreeTable,
    ) -> Result<Index> {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next_cmd()?;
        match cmd {
            Some(Cmd::Stmt(Stmt::CreateIndex {
                idx_name,
                tbl_name,
                columns,
                unique,
                where_clause,
                using,
                with_clause,
                ..
            })) => {
                let index_name = normalize_ident(idx_name.name.as_str());
                let index_columns = resolve_sorted_columns(table, &columns)?;
                if let Some(using) = using {
                    if where_clause.is_some() {
                        bail_parse_error!("custom index module do not support partial indices");
                    }
                    if unique {
                        bail_parse_error!("custom index module do not support UNIQUE indices");
                    }
                    let parameters = resolve_index_method_parameters(with_clause)?;
                    let Some(module) = syms.index_methods.get(using.as_str()) else {
                        bail_parse_error!("unknown module name: '{}'", using);
                    };
                    let configuration = IndexMethodConfiguration {
                        table_name: table.name.clone(),
                        index_name: index_name.clone(),
                        columns: index_columns.try_clone()?,
                        parameters,
                    };
                    let descriptor = module.attach(&configuration)?;
                    Ok(Index {
                        name: index_name,
                        table_name: normalize_ident(tbl_name.as_str()),
                        root_page,
                        columns: index_columns,
                        unique: false,
                        ephemeral: false,
                        has_rowid: table.has_rowid,
                        where_clause: None,
                        index_method: Some(descriptor),
                        on_conflict: None,
                    })
                } else {
                    Ok(Index {
                        name: index_name,
                        table_name: normalize_ident(tbl_name.as_str()),
                        root_page,
                        columns: index_columns,
                        unique,
                        ephemeral: false,
                        has_rowid: table.has_rowid,
                        where_clause,
                        index_method: None,
                        on_conflict: None,
                    })
                }
            }
            _ => todo!("Expected create index statement"),
        }
    }

    /// Check if this is an expression index.
    pub fn is_expression_index(&self) -> bool {
        self.columns.iter().any(|c| c.expr.is_some())
    }

    /// check if this is special backing_btree index created and managed by custom index_method
    pub fn is_backing_btree_index(&self) -> bool {
        self.index_method
            .as_ref()
            .is_some_and(|x| x.definition().backing_btree)
    }

    /// Whether this schema index owns a B-tree root page.
    pub fn is_btree_backed(&self) -> bool {
        self.index_method.is_none() || self.is_backing_btree_index()
    }

    pub fn automatic_from_primary_key(
        table: &BTreeTable,
        auto_index: (String, i64), // name, root_page
        column_count: usize,
        conflict_clause: Option<ResolveType>,
        constraint_columns: &[UniqueSetColumn],
    ) -> Result<Index> {
        let has_primary_key_index =
            table.get_rowid_alias_column().is_none() && !table.primary_key_columns.is_empty();
        assert!(has_primary_key_index);
        let (index_name, root_page) = auto_index;

        let mut primary_keys = Vec::try_with_capacity_ext(column_count)?;
        for (i, (col_name, order)) in table.primary_key_columns.iter().enumerate() {
            let Some((pos_in_table, _)) = table.get_column(col_name) else {
                return Err(crate::LimboError::ParseError(format!(
                    "Column {} not found in table {}",
                    col_name, table.name
                )));
            };
            let (_, column) = table.get_column(col_name).unwrap();
            primary_keys
                .push_within_capacity(IndexColumn {
                    name: normalize_ident(col_name),
                    order: *order,
                    nulls_order: constraint_columns.get(i).and_then(|c| c.nulls_order),
                    pos_in_table,
                    collation: constraint_columns
                        .get(i)
                        .and_then(|c| c.collation)
                        .or_else(|| column.collation_opt()),
                    default: column.default.clone(),
                    expr: None,
                })
                .expect("primary key index columns vector was preallocated");
        }

        assert!(primary_keys.len() == column_count);

        Ok(Index {
            name: normalize_ident(index_name.as_str()),
            table_name: table.name.clone(),
            root_page,
            columns: primary_keys,
            unique: true,
            ephemeral: false,
            has_rowid: table.has_rowid,
            where_clause: None,
            index_method: None,
            on_conflict: conflict_clause,
        })
    }

    pub fn automatic_from_unique(
        table: &BTreeTable,
        auto_index: (String, i64), // name, root_page
        column_indices_and_sort_orders: Vec<(usize, SortOrder)>,
        conflict_clause: Option<ResolveType>,
        constraint_columns: &[UniqueSetColumn],
    ) -> Result<Index> {
        let (index_name, root_page) = auto_index;

        let mut unique_cols = Vec::try_with_capacity_ext(column_indices_and_sort_orders.len())?;
        for (i, (pos, sort_order)) in column_indices_and_sort_orders.iter().enumerate() {
            let Some((pos_in_table, col)) = table
                .columns
                .iter()
                .enumerate()
                .find(|(pos_in_table, _)| pos == pos_in_table)
            else {
                return Err(crate::LimboError::ParseError(format!(
                    "Unique constraint column not found in table {}",
                    table.name
                )));
            };
            unique_cols
                .push_within_capacity(IndexColumn {
                    name: normalize_ident(col.name.as_ref().unwrap()),
                    order: *sort_order,
                    nulls_order: constraint_columns.get(i).and_then(|c| c.nulls_order),
                    pos_in_table,
                    collation: constraint_columns
                        .get(i)
                        .and_then(|c| c.collation)
                        .or_else(|| col.collation_opt()),
                    default: col.default.clone(),
                    expr: None,
                })
                .expect("unique index columns vector was preallocated");
        }

        Ok(Index {
            name: normalize_ident(index_name.as_str()),
            table_name: table.name.clone(),
            root_page,
            columns: unique_cols,
            unique: true,
            ephemeral: false,
            has_rowid: table.has_rowid,
            where_clause: None,
            index_method: None,
            on_conflict: conflict_clause,
        })
    }

    /// Given a column position in the table, return the position in the index.
    /// Returns None if the column is not found in the index.
    /// For example, given:
    /// CREATE TABLE t (a, b, c)
    /// CREATE INDEX idx ON t(b)
    /// then column_table_pos_to_index_pos(1) returns Some(0)
    pub fn column_table_pos_to_index_pos(&self, table_pos: usize) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.pos_in_table == table_pos)
    }

    /// Given an expression, return the position in the index if it matches an expression index column.
    /// Expression index matching is textual (after binding), so the caller should normalize the query
    /// expression to resemble the stored index expression (e.g. unqualified column names).
    pub fn expression_to_index_pos(&self, expr: &Expr) -> Option<usize> {
        self.columns.iter().position(|c| {
            c.expr
                .as_ref()
                .is_some_and(|e| exprs_are_equivalent(e, expr))
        })
    }

    /// Walk the where_clause Expr of a partial index and validate that it doesn't reference any other
    /// tables or use any disallowed constructs.
    pub fn validate_where_expr(&self, table: &Table, _resolver: &Resolver) -> bool {
        let Some(where_clause) = &self.where_clause else {
            return true;
        };

        let tbl_norm = self.table_name.as_str();
        let has_col = |name: &str| {
            table.columns().iter().any(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|cn| cn.eq_ignore_ascii_case(name))
            })
        };
        let is_tbl = |ns: &str| normalize_ident(ns) == tbl_norm;
        let is_deterministic_fn = |name: &str, argc: usize| {
            let n = normalize_ident(name);
            Func::resolve_function(&n, argc).is_ok_and(|f| f.is_some_and(|f| f.is_deterministic()))
        };

        let mut ok = true;
        let _ = walk_expr(where_clause.as_ref(), &mut |e: &Expr| -> crate::Result<
            WalkControl,
        > {
            if !ok {
                return Ok(WalkControl::SkipChildren);
            }
            match e {
                Expr::Literal(_) | Expr::RowId { .. } => {}
                // Unqualified identifier: must be a column of the target table or ROWID
                Expr::Id(n) => {
                    let n = n.as_str();
                    if !ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(n)) && !has_col(n) {
                        ok = false;
                    }
                }
                // Qualified: qualifier must match this index's table; column must exist
                Expr::Qualified(ns, col) | Expr::DoublyQualified(_, ns, col) => {
                    if !is_tbl(ns.as_str()) || !has_col(col.as_str()) {
                        ok = false;
                    }
                }
                Expr::FunctionCall {
                    name, filter_over, ..
                }
                | Expr::FunctionCallStar {
                    name, filter_over, ..
                } => {
                    // reject windowed
                    if filter_over.over_clause.is_some() {
                        ok = false;
                    } else {
                        let argc = match e {
                            Expr::FunctionCall { args, .. } => args.len(),
                            Expr::FunctionCallStar { .. } => 0,
                            _ => unreachable!(),
                        };
                        // Reject non-deterministic functions. Function arguments can reference
                        // columns of the indexed table (e.g., LENGTH(t0.c0)), which will be
                        // validated by the Expr::Id and Expr::Qualified cases during the walk.
                        if !is_deterministic_fn(name.as_str(), argc) {
                            ok = false;
                        }
                    }
                }
                // Explicitly disallowed constructs
                Expr::Exists(_)
                | Expr::InSelect { .. }
                | Expr::Subquery(_)
                | Expr::Raise { .. }
                | Expr::Variable(_) => {
                    ok = false;
                }
                _ => {}
            }
            Ok(if ok {
                WalkControl::Continue
            } else {
                WalkControl::SkipChildren
            })
        });
        ok
    }

    /// Bind a copy of this index's WHERE clause against the given table references.
    ///
    /// Returns `Ok(None)` when the index has no WHERE clause. Binding errors are
    /// propagated, never swallowed: callers must not treat `None` as "binding failed".
    ///
    /// The predicate may qualify columns with the table's real name
    /// (e.g. `CREATE INDEX i ON t(a) WHERE t.b > 0`), while the DML statement
    /// may refer to that table only under an alias (e.g. `UPDATE t AS z ...`).
    /// Rewrite such qualifiers to the identifier the statement uses, so the
    /// predicate always resolves against the index's own table, like in SQLite.
    pub fn bind_where_expr(
        &self,
        table_refs: Option<&mut TableReferences>,
        resolver: &Resolver,
    ) -> crate::Result<Option<ast::Expr>> {
        let Some(where_clause) = &self.where_clause else {
            return Ok(None);
        };
        let mut expr = where_clause.clone();
        let target_identifier = table_refs.as_deref().and_then(|refs| {
            // Only a real b-tree table can be the DML target that owns this index.
            // A CTE or subquery sharing the table's name (e.g. `WITH t AS ...
            // UPDATE t ...`) must not be picked, so match on b-tree identity.
            let mut matches = refs
                .joined_tables()
                .iter()
                .map(|jt| (&jt.identifier, &jt.table))
                .chain(
                    refs.outer_query_refs()
                        .iter()
                        .map(|r| (&r.identifier, &r.table)),
                )
                .filter(|(_, table)| {
                    table
                        .btree()
                        .is_some_and(|bt| normalize_ident(&bt.name) == self.table_name)
                })
                .map(|(identifier, _)| identifier);
            let target = matches.next().cloned();
            assert!(
                matches.next().is_none(),
                "multiple table references match the table of partial index {}",
                self.name
            );
            target
        });
        if let Some(identifier) = target_identifier {
            walk_expr_mut(&mut expr, &mut |e: &mut Expr| {
                if let Expr::Qualified(ns, _) | Expr::DoublyQualified(_, ns, _) = e {
                    if normalize_ident(ns.as_str()) == self.table_name {
                        *ns = Name::exact(identifier.clone());
                    }
                }
                Ok(WalkControl::Continue)
            })?;
        }
        bind_and_rewrite_expr(
            &mut expr,
            table_refs,
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
        Ok(Some(*expr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::vec;

    #[test]
    pub fn test_has_rowid_true() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(table.has_rowid, "has_rowid should be set to true");
        Ok(())
    }

    #[test]
    pub fn test_has_rowid_false() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(!table.has_rowid, "has_rowid should be set to false");
        Ok(())
    }

    #[test]
    pub fn test_column_default_collation_is_effective_binary() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.collation(), CollationSeq::Binary);
        assert_eq!(column.collation_opt(), None);
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_text() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a TEXT PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !column.is_rowid_alias(),
            "column 'a´ has type different than INTEGER so can't be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            column.is_rowid_alias(),
            "column 'a´ should be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition() -> Result<()>
    {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            column.is_rowid_alias(),
            "column 'a´ should be a rowid alias"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_separate_primary_key_definition_without_rowid(
    ) -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a)) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !column.is_rowid_alias(),
            "column 'a´ shouldn't be a rowid alias because table has no rowid"
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_single_integer_without_rowid() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID;"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !column.is_rowid_alias(),
            "column 'a´ shouldn't be a rowid alias because table has no rowid"
        );
        Ok(())
    }

    #[test]
    pub fn test_multiple_pk_forbidden() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY);"#;
        let table = BTreeTable::from_sql(sql, 0);
        let error = table.unwrap_err();
        assert!(
            matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
        );
        Ok(())
    }

    #[test]
    pub fn test_column_is_rowid_alias_separate_composite_primary_key_definition() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(
            !column.is_rowid_alias(),
            "column 'a´ shouldn't be a rowid alias because table has composite primary key"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_inline_single() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT, c REAL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key(), "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'b' shouldn't be a primary key"
        );
        let column = table.get_column("c").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'c' shouldn't be a primary key"
        );
        assert_eq!(
            vec![("a".to_string(), SortOrder::Asc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_inline_multiple_forbidden() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT PRIMARY KEY, c REAL);"#;
        let table = BTreeTable::from_sql(sql, 0);
        let error = table.unwrap_err();
        assert!(
            matches!(error, LimboError::ParseError(e) if e.contains("table \"t1\" has more than one primary key"))
        );
        Ok(())
    }

    #[test]
    pub fn test_conflicting_on_conflict_unique_rejected() -> Result<()> {
        let sql =
            r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT IGNORE);"#;
        let table = BTreeTable::from_sql(sql, 0);
        let error = table.unwrap_err();
        assert!(
            matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
        );
        Ok(())
    }

    #[test]
    pub fn test_conflicting_on_conflict_composite_unique_rejected() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b) ON CONFLICT FAIL, UNIQUE(a, b) ON CONFLICT IGNORE);"#;
        let table = BTreeTable::from_sql(sql, 0);
        let error = table.unwrap_err();
        assert!(
            matches!(error, LimboError::ParseError(e) if e.contains("conflicting ON CONFLICT clauses"))
        );
        Ok(())
    }

    #[test]
    pub fn test_same_on_conflict_unique_allowed() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a) ON CONFLICT FAIL);"#;
        assert!(BTreeTable::from_sql(sql, 0).is_ok());
        Ok(())
    }

    #[test]
    pub fn test_one_on_conflict_unique_allowed() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a UNIQUE ON CONFLICT FAIL, b, UNIQUE(a));"#;
        assert!(BTreeTable::from_sql(sql, 0).is_ok());
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_single() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a desc));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key(), "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'b' shouldn't be a primary key"
        );
        let column = table.get_column("c").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'c' shouldn't be a primary key"
        );
        assert_eq!(
            vec![("a".to_string(), SortOrder::Desc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_multiple() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY(a, b desc));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key(), "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(column.primary_key(), "column 'b' shouldn be a primary key");
        let column = table.get_column("c").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'c' shouldn't be a primary key"
        );
        assert_eq!(
            vec![
                ("a".to_string(), SortOrder::Asc),
                ("b".to_string(), SortOrder::Desc)
            ],
            table.primary_key_columns,
            "primary key column names should be ['a', 'b']"
        );
        Ok(())
    }

    #[test]
    pub fn test_primary_key_separate_single_quoted() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY('a'));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key(), "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'b' shouldn't be a primary key"
        );
        let column = table.get_column("c").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'c' shouldn't be a primary key"
        );
        assert_eq!(
            vec![("a".to_string(), SortOrder::Asc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }
    #[test]
    pub fn test_primary_key_separate_single_doubly_quoted() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, c REAL, PRIMARY KEY("a"));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.primary_key(), "column 'a' should be a primary key");
        let column = table.get_column("b").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'b' shouldn't be a primary key"
        );
        let column = table.get_column("c").unwrap().1;
        assert!(
            !column.primary_key(),
            "column 'c' shouldn't be a primary key"
        );
        assert_eq!(
            vec![("a".to_string(), SortOrder::Asc)],
            table.primary_key_columns,
            "primary key column names should be ['a']"
        );
        Ok(())
    }

    #[test]
    pub fn test_default_value() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER DEFAULT 23);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        let default = column.default.clone().unwrap();
        assert_eq!(default.to_string(), "23");
        Ok(())
    }

    #[test]
    pub fn test_col_notnull() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER NOT NULL);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(column.notnull());
        Ok(())
    }

    #[test]
    pub fn test_col_notnull_negative() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert!(!column.notnull());
        Ok(())
    }

    #[test]
    pub fn test_col_type_string_integer() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a InTeGeR);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let column = table.get_column("a").unwrap().1;
        assert_eq!(column.ty_str, "InTeGeR");
        Ok(())
    }

    #[test]
    pub fn test_sqlite_schema() -> Result<()> {
        let expected = r#"CREATE TABLE sqlite_schema (type TEXT, name TEXT, tbl_name TEXT, rootpage INT, sql TEXT)"#;
        let actual = sqlite_schema_table()?.to_sql();
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn check_constraint_comments_survive_table_reconstruction() -> Result<()> {
        for (sql, comment) in [
            (
                "CREATE TABLE t (x CHECK(x /* block comment */ > 0))",
                "/* block comment */",
            ),
            (
                "CREATE TABLE t (x CHECK(x > 0 -- line comment\n))",
                "-- line comment",
            ),
        ] {
            let reconstructed = BTreeTable::from_sql(sql, 0)?.to_sql();

            assert!(
                reconstructed.contains(comment),
                "reconstructed SQL: {reconstructed}"
            );
            BTreeTable::from_sql(&reconstructed, 0)?;
        }

        Ok(())
    }

    #[test]
    pub fn test_special_column_names() -> Result<()> {
        let tests = [
            ("foobar", "CREATE TABLE t (foobar TEXT)"),
            ("_table_name3", r#"CREATE TABLE t (_table_name3 TEXT)"#),
            ("special name", r#"CREATE TABLE t ("special name" TEXT)"#),
            ("foo&bar", r#"CREATE TABLE t ("foo&bar" TEXT)"#),
            (" name", r#"CREATE TABLE t (" name" TEXT)"#),
        ];

        for (input_column_name, expected_sql) in tests {
            let sql = format!(r#"CREATE TABLE t ("{input_column_name}" TEXT)"#);
            let actual = BTreeTable::from_sql(&sql, 0)?.to_sql();
            assert_eq!(expected_sql, actual);
        }

        Ok(())
    }

    #[test]
    fn test_special_table_names_are_quoted_in_to_sql() -> Result<()> {
        let tests = [
            (
                r#"CREATE TABLE "t t" (x TEXT)"#,
                r#"CREATE TABLE "t t" (x TEXT)"#,
            ),
            (
                r#"CREATE TABLE "123table" (x TEXT)"#,
                r#"CREATE TABLE "123table" (x TEXT)"#,
            ),
            (
                r#"CREATE TABLE "t""t" (x TEXT)"#,
                r#"CREATE TABLE "t""t" (x TEXT)"#,
            ),
        ];

        for (input_sql, expected_sql) in tests {
            let actual = BTreeTable::from_sql(input_sql, 0)?.to_sql();
            assert_eq!(actual, expected_sql);
        }

        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_automatic_index_single_column() {
        // Without composite primary keys, we should not have an automatic index on a primary key that is a rowid alias
        let sql = r#"CREATE TABLE t1 (a INTEGER PRIMARY KEY, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0).unwrap();
        let _index = Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
        )
        .unwrap();
    }

    #[test]
    fn test_automatic_index_composite_key() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT, PRIMARY KEY(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            2,
            None,
            &[],
        )?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));
        assert!(matches!(index.columns[1].order, SortOrder::Asc));
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_automatic_index_no_primary_key() {
        let sql = r#"CREATE TABLE t1 (a INTEGER, b TEXT);"#;
        let table = BTreeTable::from_sql(sql, 0).unwrap();
        Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
        )
        .unwrap();
    }

    #[test]
    fn test_automatic_index_nonexistent_column() {
        // Create a table with a primary key column that doesn't exist in the table
        let columns = vec![Column::new_default_integer(
            Some("a".to_string()),
            "INT".to_string(),
            None,
        )];
        let logical_to_physical_map =
            BTreeTable::build_logical_to_physical_map(&columns, &[], true);
        let table = BTreeTable {
            root_page: 0,
            name: "t1".to_string(),
            has_rowid: true,
            is_strict: false,
            has_autoincrement: false,
            primary_key_columns: vec![("nonexistent".to_string(), SortOrder::Asc)],
            columns,
            unique_sets: vec![],
            foreign_keys: vec![],
            check_constraints: vec![],
            rowid_alias_conflict_clause: None,
            has_virtual_columns: false,
            logical_to_physical_map,
            column_dependencies: Default::default(),
        };

        let result = Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_automatic_index_unique_column() -> Result<()> {
        let sql = r#"CREATE table t1 (x INTEGER, y INTEGER UNIQUE);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            vec![(1, SortOrder::Asc)],
            None,
            &[],
        )?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "y");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));
        Ok(())
    }

    #[test]
    fn test_automatic_index_pkey_unique_column() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (x PRIMARY KEY, y UNIQUE);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let indices = [
            Index::automatic_from_primary_key(
                &table,
                ("sqlite_autoindex_t1_1".to_string(), 2),
                1,
                None,
                &[],
            )?,
            Index::automatic_from_unique(
                &table,
                ("sqlite_autoindex_t1_2".to_string(), 3),
                vec![(1, SortOrder::Asc)],
                None,
                &[],
            )?,
        ];

        assert_eq!(indices[0].name, "sqlite_autoindex_t1_1");
        assert_eq!(indices[0].table_name, "t1");
        assert_eq!(indices[0].root_page, 2);
        assert!(indices[0].unique);
        assert_eq!(indices[0].columns.len(), 1);
        assert_eq!(indices[0].columns[0].name, "x");
        assert!(matches!(indices[0].columns[0].order, SortOrder::Asc));

        assert_eq!(indices[1].name, "sqlite_autoindex_t1_2");
        assert_eq!(indices[1].table_name, "t1");
        assert_eq!(indices[1].root_page, 3);
        assert!(indices[1].unique);
        assert_eq!(indices[1].columns.len(), 1);
        assert_eq!(indices[1].columns[0].name, "y");
        assert!(matches!(indices[1].columns[0].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_automatic_index_pkey_many_unique_columns() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a PRIMARY KEY, b UNIQUE, c, d, UNIQUE(c, d));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let auto_indices = [
            ("sqlite_autoindex_t1_1".to_string(), 2),
            ("sqlite_autoindex_t1_2".to_string(), 3),
            ("sqlite_autoindex_t1_3".to_string(), 4),
        ];
        let indices = vec![
            Index::automatic_from_primary_key(
                &table,
                ("sqlite_autoindex_t1_1".to_string(), 2),
                1,
                None,
                &[],
            )?,
            Index::automatic_from_unique(
                &table,
                ("sqlite_autoindex_t1_2".to_string(), 3),
                vec![(1, SortOrder::Asc)],
                None,
                &[],
            )?,
            Index::automatic_from_unique(
                &table,
                ("sqlite_autoindex_t1_3".to_string(), 4),
                vec![(2, SortOrder::Asc), (3, SortOrder::Asc)],
                None,
                &[],
            )?,
        ];

        assert!(indices.len() == auto_indices.len());

        for (pos, index) in indices.iter().enumerate() {
            let (index_name, root_page) = &auto_indices[pos];
            assert_eq!(index.name, *index_name);
            assert_eq!(index.table_name, "t1");
            assert_eq!(index.root_page, *root_page);
            assert!(index.unique);

            if pos == 0 {
                assert_eq!(index.columns.len(), 1);
                assert_eq!(index.columns[0].name, "a");
            } else if pos == 1 {
                assert_eq!(index.columns.len(), 1);
                assert_eq!(index.columns[0].name, "b");
            } else if pos == 2 {
                assert_eq!(index.columns.len(), 2);
                assert_eq!(index.columns[0].name, "c");
                assert_eq!(index.columns[1].name, "d");
            }

            assert!(matches!(index.columns[0].order, SortOrder::Asc));
        }

        Ok(())
    }

    #[test]
    fn test_automatic_index_unique_set_dedup() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a, b, UNIQUE(a, b), UNIQUE(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_unique(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            vec![(0, SortOrder::Asc), (1, SortOrder::Asc)],
            None,
            &[],
        )?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[1].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_automatic_index_primary_key_is_unique() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a primary key unique);"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            1,
            None,
            &[],
        )?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_automatic_index_primary_key_is_unique_and_composite() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a, b, PRIMARY KEY(a, b), UNIQUE(a, b));"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let index = Index::automatic_from_primary_key(
            &table,
            ("sqlite_autoindex_t1_1".to_string(), 2),
            2,
            None,
            &[],
        )?;

        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns[0].name, "a");
        assert_eq!(index.columns[1].name, "b");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_strict_table_to_sql() -> Result<()> {
        let sql = r#"CREATE TABLE test_strict (id INTEGER, name TEXT) STRICT"#;
        let table = BTreeTable::from_sql(sql, 0)?;

        // Verify the table is marked as strict
        assert!(table.is_strict);

        // Verify that to_sql() includes the STRICT keyword
        let reconstructed_sql = table.to_sql();
        assert!(
            reconstructed_sql.contains("STRICT"),
            "Reconstructed SQL should contain STRICT keyword: {reconstructed_sql}"
        );
        assert_eq!(
            reconstructed_sql,
            "CREATE TABLE test_strict (id INTEGER, name TEXT) STRICT"
        );

        Ok(())
    }

    #[test]
    fn test_non_strict_table_to_sql() -> Result<()> {
        let sql = r#"CREATE TABLE test_normal (id INTEGER, name TEXT)"#;
        let table = BTreeTable::from_sql(sql, 0)?;

        // Verify the table is NOT marked as strict
        assert!(!table.is_strict);

        // Verify that to_sql() does NOT include the STRICT keyword
        let reconstructed_sql = table.to_sql();
        assert!(
            !reconstructed_sql.contains("STRICT"),
            "Non-strict table SQL should not contain STRICT keyword: {reconstructed_sql}"
        );
        assert_eq!(
            reconstructed_sql,
            "CREATE TABLE test_normal (id INTEGER, name TEXT)"
        );

        Ok(())
    }

    #[test]
    fn test_autoincrement_preserved_in_to_sql() -> Result<()> {
        let sql = r#"CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, doomed INT, v TEXT)"#;
        let table = BTreeTable::from_sql(sql, 0)?;

        assert!(table.has_autoincrement);
        assert_eq!(
            table.to_sql(),
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, doomed INT, v TEXT)"
        );

        Ok(())
    }

    #[test]
    fn test_without_rowid_preserved_in_sql() -> Result<()> {
        let sql = r#"CREATE TABLE t(code TEXT PRIMARY KEY, val TEXT) WITHOUT ROWID"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(table.get_column("code").unwrap().1.notnull());
        assert_eq!(
            table.to_sql(),
            "CREATE TABLE t (code TEXT PRIMARY KEY, val TEXT) WITHOUT ROWID"
        );
        Ok(())
    }

    #[test]
    fn test_strict_without_rowid_preserved_in_sql() -> Result<()> {
        let sql = r#"CREATE TABLE t(code TEXT PRIMARY KEY, val TEXT) STRICT, WITHOUT ROWID"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        assert!(table.get_column("code").unwrap().1.notnull());
        assert_eq!(
            table.to_sql(),
            "CREATE TABLE t (code TEXT PRIMARY KEY, val TEXT) STRICT, WITHOUT ROWID"
        );
        Ok(())
    }

    #[test]
    fn test_automatic_index_unique_and_a_pk() -> Result<()> {
        let sql = r#"CREATE TABLE t1 (a NUMERIC UNIQUE UNIQUE,  b TEXT PRIMARY KEY)"#;
        let table = BTreeTable::from_sql(sql, 0)?;
        let mut indexes = vec![
            Index::automatic_from_unique(
                &table,
                ("sqlite_autoindex_t1_1".to_string(), 2),
                vec![(0, SortOrder::Asc)],
                None,
                &[],
            )?,
            Index::automatic_from_primary_key(
                &table,
                ("sqlite_autoindex_t1_2".to_string(), 3),
                1,
                None,
                &[],
            )?,
        ];

        assert!(indexes.len() == 2);
        let index = indexes.pop().unwrap();
        assert_eq!(index.name, "sqlite_autoindex_t1_2");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 3);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "b");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        let index = indexes.pop().unwrap();
        assert_eq!(index.name, "sqlite_autoindex_t1_1");
        assert_eq!(index.table_name, "t1");
        assert_eq!(index.root_page, 2);
        assert!(index.unique);
        assert_eq!(index.columns.len(), 1);
        assert_eq!(index.columns[0].name, "a");
        assert!(matches!(index.columns[0].order, SortOrder::Asc));

        Ok(())
    }

    #[test]
    fn test_schema_loading_rejects_gencol_without_flag() {
        let mut schema = Schema::new();
        schema.generated_columns_enabled = false;

        let result = schema.handle_schema_row(
            "table",
            "t1",
            "t1",
            2,
            Some("CREATE TABLE t1(a INTEGER, b AS (a*2))"),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &crate::dialect::SqliteDialect,
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("generated columns"));
    }

    #[test]
    fn test_schema_row_vtab_with_nonzero_root_page_is_corrupt() {
        let mut schema = Schema::new();

        let result = schema.handle_schema_row(
            "table",
            "v1",
            "v1",
            2,
            Some("CREATE VIRTUAL TABLE v1 USING somemodule"),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &crate::dialect::SqliteDialect,
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("root_page must be 0 for virtual table v1"));
    }

    #[test]
    fn test_schema_row_with_zero_root_page_takes_virtual_table_path() {
        let mut schema = Schema::new();

        // Root page 0 must route to virtual-table handling; with no such
        // module registered, that path fails module resolution instead of
        // creating a B-tree table with an invalid root page.
        let result = schema.handle_schema_row(
            "table",
            "v1",
            "v1",
            0,
            Some("CREATE VIRTUAL TABLE v1 USING nosuchmodule"),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &crate::dialect::SqliteDialect,
        );
        assert!(result.is_err());
        assert!(schema.get_table("v1").is_none());
    }

    #[test]
    fn test_schema_row_with_zero_root_page_rejects_malformed_sql() {
        let mut schema = Schema::new();

        let result = schema.handle_schema_row(
            "table",
            "v1",
            "v1",
            0,
            Some("CREATE VIRTUAL TABLE v1 USING"),
            &SymbolTable::default(),
            &mut vec![],
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &mut HashMap::default(),
            &|_| None,
            &crate::dialect::SqliteDialect,
        );
        assert!(result.is_err());
        assert!(schema.get_table("v1").is_none());
    }

    #[test]
    fn test_schema_row_table_with_virtual_table_substring_is_btree() {
        let mut schema = Schema::new();

        // A regular table whose SQL merely contains virtual-table syntax
        // (e.g. in a DEFAULT literal) must classify as a B-tree table.
        schema
            .handle_schema_row(
                "table",
                "t1",
                "t1",
                2,
                Some("CREATE TABLE t1(x TEXT DEFAULT 'create virtual table')"),
                &SymbolTable::default(),
                &mut vec![],
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
                &mut HashMap::default(),
                &|_| None,
                &crate::dialect::SqliteDialect,
            )
            .unwrap();
        let table = schema.get_btree_table("t1").unwrap();
        assert_eq!(table.root_page, 2);
    }

    fn indices(mask: &ColumnMask) -> Vec<usize> {
        let mut v: Vec<usize> = mask.iter().try_collect().unwrap();
        v.sort_unstable();
        v
    }

    fn stored(bits: &ColumnMask) -> Vec<usize> {
        let mut v: Vec<usize> = bits.iter().try_collect().unwrap();
        v.sort_unstable();
        v
    }

    #[test]
    fn gencol_graph_no_virtual_columns() -> Result<()> {
        let t = BTreeTable::from_sql("CREATE TABLE t(a, b)", 0)?;
        assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0]);
        assert_eq!(indices(&t.columns_affected_by_update([0, 1])?), vec![0, 1]);
        assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
        assert_eq!(stored(&t.dependencies_of_columns([])?), Vec::<usize>::new());
        Ok(())
    }

    #[test]
    fn gencol_graph_linear_chain() -> Result<()> {
        let t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL)", 0)?;
        // affected-by({a}) = {a, b, c}
        assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 1, 2]);
        // affected-by({b}) = {b, c} (b is virtual, but updating it still propagates through dependents)
        assert_eq!(indices(&t.columns_affected_by_update([1])?), vec![1, 2]);
        // deps-of({c}) = {a} (transitive stored deps of virtual c)
        assert_eq!(stored(&t.dependencies_of_columns([2])?), vec![0]);
        // deps-of({b}) = {a}
        assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
        // deps-of({a}) = {a} (stored target included)
        assert_eq!(stored(&t.dependencies_of_columns([0])?), vec![0]);
        Ok(())
    }

    #[test]
    fn gencol_graph_diamond() -> Result<()> {
        let t = BTreeTable::from_sql(
            "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (a) VIRTUAL, d AS (b + c) VIRTUAL)",
            0,
        )?;
        assert_eq!(
            indices(&t.columns_affected_by_update([0])?),
            vec![0, 1, 2, 3]
        );
        assert_eq!(stored(&t.dependencies_of_columns([3])?), vec![0]);
        assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
        Ok(())
    }

    #[test]
    fn gencol_graph_multiple_stored_roots() -> Result<()> {
        let t = BTreeTable::from_sql("CREATE TABLE t(a, b, c AS (a + b) VIRTUAL)", 0)?;
        assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 2]);
        assert_eq!(indices(&t.columns_affected_by_update([1])?), vec![1, 2]);
        assert_eq!(
            indices(&t.columns_affected_by_update([0, 1])?),
            vec![0, 1, 2]
        );
        assert_eq!(stored(&t.dependencies_of_columns([2])?), vec![0, 1]);
        Ok(())
    }

    #[test]
    fn gencol_graph_empty_input() -> Result<()> {
        let t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
        assert!(t.columns_affected_by_update(std::iter::empty())?.is_empty());
        assert!(t.dependencies_of_columns(std::iter::empty())?.is_empty());
        Ok(())
    }

    #[test]
    fn gencol_graph_disjoint_components() -> Result<()> {
        let t = BTreeTable::from_sql(
            "CREATE TABLE t(a, b AS (a) VIRTUAL, c, d AS (c) VIRTUAL)",
            0,
        )?;
        assert_eq!(indices(&t.columns_affected_by_update([0])?), vec![0, 1]);
        assert_eq!(indices(&t.columns_affected_by_update([2])?), vec![2, 3]);
        assert_eq!(stored(&t.dependencies_of_columns([1])?), vec![0]);
        assert_eq!(stored(&t.dependencies_of_columns([3])?), vec![2]);
        Ok(())
    }

    #[test]
    fn gencol_graph_deep_chain() -> Result<()> {
        // Build 50-long chain: c0 (stored), c1 := c0, c2 := c1, ... c49 := c48.
        let mut sql = String::from("CREATE TABLE t(c0");
        for i in 1..50 {
            sql.push_str(&format!(", c{i} AS (c{prev}) VIRTUAL", prev = i - 1));
        }
        sql.push(')');
        let t = BTreeTable::from_sql(&sql, 0)?;
        // affected-by({c0}) = {c0..c49}
        let affected = t.columns_affected_by_update([0])?;
        assert_eq!(affected.count(), 50);
        // deps-of({c49}) = {c0}
        assert_eq!(stored(&t.dependencies_of_columns([49])?), vec![0]);
        Ok(())
    }

    #[test]
    fn gencol_graph_very_deep_chain_no_stack_overflow() -> Result<()> {
        // Validates that the iterative Kahn's + DP don't blow the stack on
        // realistic worst-case generated-column depth.
        let mut sql = String::from("CREATE TABLE t(c0");
        for i in 1..500 {
            sql.push_str(&format!(", c{i} AS (c{prev}) VIRTUAL", prev = i - 1));
        }
        sql.push(')');
        let t = BTreeTable::from_sql(&sql, 0)?;
        assert_eq!(t.columns_affected_by_update([0])?.count(), 500);
        assert_eq!(stored(&t.dependencies_of_columns([499])?), vec![0]);
        Ok(())
    }

    #[test]
    fn gencol_graph_rowid_sentinel_passthrough() -> Result<()> {
        let t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
        let affected = t.columns_affected_by_update([ROWID_SENTINEL])?;
        // ROWID_SENTINEL is preserved in the mask flag but does not propagate through the graph
        // (no generated column can depend on ROWID_SENTINEL directly).
        assert!(affected.get(ROWID_SENTINEL));
        assert_eq!(affected.count(), 1);
        Ok(())
    }

    #[test]
    fn gencol_graph_transpose_duality() -> Result<()> {
        let t = BTreeTable::from_sql(
            "CREATE TABLE t(a, b AS (a) VIRTUAL, c AS (b) VIRTUAL, d AS (a + c) VIRTUAL)",
            0,
        )?;
        let graph = t.column_graph()?;
        // j ∈ dependencies[i] iff i ∈ dependents[j]
        for i in 0..graph.dependencies.len() {
            for j in graph.dependencies[i].iter() {
                assert!(
                    graph.dependents[j].get(i),
                    "transpose violated: {j} is in dependencies[{i}] but {i} is not in dependents[{j}]"
                );
            }
            for j in graph.dependents[i].iter() {
                assert!(
                    graph.dependencies[j].get(i),
                    "transpose violated: {j} is in dependents[{i}] but {i} is not in dependencies[{j}]"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn gencol_graph_idempotence() -> Result<()> {
        // affected_by(affected_by(xs)) == affected_by(xs).
        let t = BTreeTable::from_sql(
            "CREATE TABLE t(a, b, c AS (a) VIRTUAL, d AS (b + c) VIRTUAL)",
            0,
        )?;
        let once = t.columns_affected_by_update([0, 1])?;
        let twice = t.columns_affected_by_update(once.iter())?;
        assert_eq!(indices(&twice), indices(&once));
        Ok(())
    }

    #[test]
    fn gencol_graph_union_monotonicity() -> Result<()> {
        // affected_by(A ∪ B) == affected_by(A) ∪ affected_by(B).
        let t = BTreeTable::from_sql(
            "CREATE TABLE t(a, b, c AS (a) VIRTUAL, d AS (b) VIRTUAL, e AS (c + d) VIRTUAL)",
            0,
        )?;
        let mut expected = t.columns_affected_by_update([0])?;
        let b_mask = t.columns_affected_by_update([1])?;
        expected.union_with(&b_mask).unwrap();
        let union_mask = t.columns_affected_by_update([0, 1])?;
        assert_eq!(indices(&union_mask), indices(&expected));
        Ok(())
    }

    #[test]
    fn gencol_graph_cycle_rejected() {
        // Two-cycle: a := b, b := a. Must be rejected at CREATE TABLE time by Kahn's.
        let err = BTreeTable::from_sql(
            "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (a) VIRTUAL)",
            0,
        )
        .expect_err("cycle must be rejected");
        assert!(
            err.to_string().contains("circular dependency")
                || err.to_string().contains("cannot reference itself"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn gencol_graph_three_cycle_rejected() {
        // Three-cycle: a := b, b := c, c := a.
        let err = BTreeTable::from_sql(
            "CREATE TABLE t(stored, a AS (b) VIRTUAL, b AS (c) VIRTUAL, c AS (a) VIRTUAL)",
            0,
        )
        .expect_err("cycle must be rejected");
        assert!(err.to_string().contains("circular dependency"));
    }

    #[test]
    fn gencol_graph_self_reference_rejected() {
        let err = BTreeTable::from_sql("CREATE TABLE t(a, b AS (b) VIRTUAL)", 0)
            .expect_err("self-reference must be rejected");
        assert!(err.to_string().contains("cannot reference itself"));
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn gencol_graph_clone_invalidates_cache() -> Result<()> {
        // After cloning a BTreeTable, the cache is fresh. Mutating columns on
        // the clone via `columns_mut()` keeps it fresh; `prepare_generated_columns`
        // rebuilds correctly.
        let original = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
        // Force the cache to be populated on the original.
        let _ = original.columns_affected_by_update([0])?;
        assert!(original.peek_column_dependencies().is_some());

        // Clone: ResetOnClone makes the cloned cache empty. We keep a real clone
        // (not a move) because the point of the test is that Clone produces a
        // fresh cache independently from the original.
        let cloned = original.clone();
        assert!(cloned.peek_column_dependencies().is_none());
        // Original's cache is still populated — clone didn't touch it.
        assert!(original.peek_column_dependencies().is_some());

        // The clone still returns correct results — cache rebuilds lazily.
        assert_eq!(
            indices(&cloned.columns_affected_by_update([0])?),
            vec![0, 1]
        );
        assert!(cloned.peek_column_dependencies().is_some());
        Ok(())
    }

    #[test]
    fn gencol_graph_columns_mut_invalidates_cache() -> Result<()> {
        let mut t = BTreeTable::from_sql("CREATE TABLE t(a, b AS (a) VIRTUAL)", 0)?;
        // Force the cache to be populated.
        let _ = t.columns_affected_by_update([0])?;
        assert!(t.peek_column_dependencies().is_some());

        // Any access through columns_mut() wipes the cache, even if we don't mutate.
        let _ = t.columns_mut();
        assert!(t.peek_column_dependencies().is_none());
        Ok(())
    }

    /// `install_sequence_descriptor` must surface an error when the
    /// persisted metadata is invalid (e.g. min > max) rather than
    /// silently dropping the sequence. The internal backing table is
    /// the only persistent record of the sequence; a silent drop would
    /// manifest later as a misleading "sequence does not exist" on the
    /// next nextval that masks real on-disk corruption.
    #[test]
    fn install_sequence_descriptor_rejects_invalid_metadata_with_corruption_error() {
        let mut schema = Schema::new();
        let bogus = SequenceMetadata {
            // increment of zero is universally invalid; Sequence::new
            // rejects it with a clear error.
            start: 0,
            increment: 0,
            min: 0,
            max: 100,
            cycle: false,
        };
        let result = schema.install_sequence_descriptor("broken_seq", bogus);
        let err = result.expect_err(
            "invalid persisted descriptor must surface as an error, not be silently dropped",
        );
        assert!(
            matches!(err, LimboError::Corrupt(_)),
            "expected Corrupt error for unreadable internal backing table, got: {err:?}",
        );
        assert!(
            !schema.sequences.contains_key("broken_seq"),
            "rejected descriptor must not land in the sequences map",
        );
    }

    #[test]
    fn set_base_affinity_is_consistent_with_accessor() {
        for affinity in [
            Affinity::Blob,
            Affinity::Text,
            Affinity::Numeric,
            Affinity::Integer,
            Affinity::Real,
            Affinity::None,
        ] {
            let mut col = Column::new(
                Some("x".to_string()),
                "BLOB".to_string(),
                None,
                None,
                Type::Blob,
                None,
                ColDef::default(),
            );
            col.override_affinity(affinity);
            assert_eq!(
                col.affinity(),
                affinity,
                "stored {affinity:?} but read back {:?}",
                col.affinity(),
            );
        }
    }
}

mod column_info {
    use crate::schema::{ColDef, Type};
    use crate::vdbe::affinity::Affinity;
    use crate::vdbe::CollationSeq;

    // flags
    const F_PRIMARY_KEY: u32 = 1;
    const F_ROWID_ALIAS: u32 = 2;
    const F_NOTNULL: u32 = 4;
    const F_UNIQUE: u32 = 8;
    const F_HIDDEN: u32 = 16;

    // pack Type and Collation in the remaining bits
    const TYPE_SHIFT: u32 = 5;
    const TYPE_MASK: u32 = 0b111 << TYPE_SHIFT;
    const COLL_SHIFT: u32 = TYPE_SHIFT + 3;
    const COLL_MASK: u32 = 0b1111_1111_1111 << COLL_SHIFT;

    // Bits 20-22: base type affinity. Column affinity will resolve to this
    // value only if it is > 0, else it uses ty_str.
    const BASE_AFF_SHIFT: u32 = COLL_SHIFT + 12;
    const BASE_AFF_MASK: u32 = 0b111 << BASE_AFF_SHIFT;

    // Bits 23-25: array dimensions (0 = scalar, 1-7 = number of [] dimensions)
    const ARRAY_DIM_SHIFT: u32 = BASE_AFF_SHIFT + 3;
    const ARRAY_DIM_MASK: u32 = 0b111 << ARRAY_DIM_SHIFT;

    /// ColumnInfo packs information on a [Column] into a single `u32`.
    #[derive(Clone, Debug)]
    pub struct ColumnInfo(u32);

    pub struct NewColumnInfoParams<'a> {
        pub ty: Type,
        pub collation: Option<CollationSeq>,
        pub coldef: &'a ColDef,
    }

    impl ColumnInfo {
        #[inline]
        pub fn new(params: NewColumnInfoParams) -> Self {
            let mut raw: u32 = 0;

            raw |= (params.ty as u32) << TYPE_SHIFT;
            if let Some(c) = params.collation {
                raw |= (u32::from(c.to_bits()) << COLL_SHIFT) & COLL_MASK;
            }
            if params.coldef.primary_key {
                raw |= F_PRIMARY_KEY
            }
            if params.coldef.rowid_alias {
                raw |= F_ROWID_ALIAS
            }
            if params.coldef.notnull {
                raw |= F_NOTNULL
            }
            if params.coldef.unique {
                raw |= F_UNIQUE
            }
            if params.coldef.hidden {
                raw |= F_HIDDEN
            }

            Self(raw)
        }

        #[inline]
        pub fn primary_key(&self) -> bool {
            self.0 & F_PRIMARY_KEY != 0
        }

        #[inline]
        pub fn is_rowid_alias(&self) -> bool {
            self.0 & F_ROWID_ALIAS != 0
        }

        #[inline]
        pub fn set_rowid_alias(&mut self, v: bool) {
            self.set_flag(F_ROWID_ALIAS, v);
        }

        #[inline]
        pub fn notnull(&self) -> bool {
            self.0 & F_NOTNULL != 0
        }

        #[inline]
        pub fn set_notnull(&mut self, v: bool) {
            self.set_flag(F_NOTNULL, v);
        }

        #[inline]
        pub fn unique(&self) -> bool {
            self.0 & F_UNIQUE != 0
        }

        #[inline]
        pub fn set_unique(&mut self, v: bool) {
            self.set_flag(F_UNIQUE, v);
        }

        #[inline]
        fn set_flag(&mut self, mask: u32, val: bool) {
            if val {
                self.0 |= mask
            } else {
                self.0 &= !mask
            }
        }

        #[inline]
        pub fn hidden(&self) -> bool {
            self.0 & F_HIDDEN != 0
        }

        #[inline]
        pub fn set_hidden(&mut self, v: bool) {
            self.set_flag(F_HIDDEN, v);
        }

        #[inline]
        pub fn is_array(&self) -> bool {
            (self.0 & ARRAY_DIM_MASK) != 0
        }

        #[inline]
        pub fn array_dimensions(&self) -> u32 {
            (self.0 & ARRAY_DIM_MASK) >> ARRAY_DIM_SHIFT
        }

        #[inline]
        pub fn set_array_dimensions(&mut self, dims: u32) {
            assert!(dims <= 7, "array dimensions must be <= 7");
            self.0 = (self.0 & !ARRAY_DIM_MASK) | (dims << ARRAY_DIM_SHIFT);
        }

        #[inline]
        pub fn ty(&self) -> Type {
            let v = ((self.0 & TYPE_MASK) >> TYPE_SHIFT) as u8;
            Type::from_bits(v)
        }

        #[inline]
        pub fn set_ty(&mut self, ty: Type) {
            self.0 = (self.0 & !TYPE_MASK) | (((ty as u32) << TYPE_SHIFT) & TYPE_MASK);
        }

        #[inline]
        pub fn collation(&self) -> CollationSeq {
            let v = ((self.0 & COLL_MASK) >> COLL_SHIFT) as u16;
            if v == CollationSeq::Unset.to_bits() {
                CollationSeq::Binary
            } else {
                CollationSeq::from_storage_bits(v)
            }
        }

        #[inline]
        pub fn has_explicit_collation(&self) -> bool {
            let v = ((self.0 & COLL_MASK) >> COLL_SHIFT) as u16;
            v != CollationSeq::Unset.to_bits()
        }

        #[inline]
        pub fn set_collation(&mut self, c: Option<CollationSeq>) {
            self.0 &= !COLL_MASK;
            if let Some(c) = c {
                self.0 |= ((c.to_bits() as u32) << COLL_SHIFT) & COLL_MASK;
            }
        }

        #[inline]
        pub fn affinity(&self) -> Option<Affinity> {
            let v = (self.0 & BASE_AFF_MASK) >> BASE_AFF_SHIFT;
            Affinity::from_repr(v)
        }

        #[inline]
        pub fn override_affinity(&mut self, affinity: Affinity) {
            let v: u32 = affinity as u32;
            self.0 = (self.0 & !BASE_AFF_MASK) | ((v << BASE_AFF_SHIFT) & BASE_AFF_MASK);
        }
    }
}
