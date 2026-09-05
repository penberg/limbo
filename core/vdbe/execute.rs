use crate::alloc::{
    DynAllocator, TryClone, TursoAllocExt, TursoIteratorExt, TursoSliceExt,
    TursoTryWithCapacityExt, TursoVecExt,
};
use crate::cdc::TURSO_CDC_VERSION_TABLE_NAME;
use crate::error::SQLITE_CONSTRAINT_UNIQUE;
use crate::function::{AccumulatorFunc, AlterTableFunc, WindowFunc};
use crate::io::TempFile;
use crate::mvcc::cursor::{MvccCursorType, NextRowidResult};
use crate::mvcc::database::{
    BootstrapState, CheckpointReadLockState, CheckpointStateMachine, TxID,
};
use crate::mvcc::MvccClock;
use crate::numeric::Numeric;
use crate::schema::{
    render_gencol_expr_sql_with_new_names, Schema, Table, EXPR_INDEX_SENTINEL, SCHEMA_TABLE_NAME,
    SQLITE_SEQUENCE_TABLE_NAME,
};
use crate::state_machine::StateMachine;
use crate::storage::btree::{
    integrity_check, CursorTrait, IntegrityCheckError, IntegrityCheckState, PageCategory,
};
use crate::storage::database::DatabaseFile;
use crate::storage::journal_mode;
use crate::storage::page_cache::PageCache;
use crate::storage::pager::{default_page1, CreateBTreeFlags, PageRef, SavepointResult};
use crate::storage::sqlite3_ondisk::{DatabaseHeader, PageSize, RawVersion};
use crate::translate::collate::CollationSeq;
use crate::types::IOResultOr;
use crate::types::{
    compare_immutable, compare_immutable_single, compare_records_generic, AsValueRef, Extendable,
    IOCompletions, IOResult, ImmutableRecord, IndexInfo, SeekResult, Text, ValueIterator,
};
use crate::util::{
    escape_sql_string_literal, normalize_ident, rename_identifiers,
    rename_identifiers_scoped_when_clause, rewrite_check_expr_table_refs,
    rewrite_column_level_fk_parent_columns_if_needed, rewrite_column_references_if_needed,
    rewrite_fk_parent_cols_if_self_ref, rewrite_fk_parent_table_if_needed,
    rewrite_inline_col_fk_target_if_needed, rewrite_trigger_cmd_column_refs,
    rewrite_trigger_cmd_table_refs, rewrite_view_sql_for_column_rename,
    trigger_still_references_renamed_column, trim_ascii_whitespace, RewrittenView,
};
use crate::vdbe::affinity::{
    apply_numeric_affinity, real_to_i64, try_for_float, Affinity, NumericParseResult, ParsedNumber,
};
use crate::vdbe::hash_table::{
    HashEntry, HashInsertResult, HashTable, HashTableConfig, PendingHashInsert, DEFAULT_MEM_BUDGET,
};
use crate::vdbe::insn::InsertFlags;
use crate::vdbe::metrics::HashJoinMetrics;
use crate::vdbe::vacuum::VacuumInPlaceOpContext;
use crate::vdbe::value::ComparisonOp;
use crate::vdbe::ValueIteratorExt;
use crate::vdbe::{
    registers_to_ref_values, DeferredSeekState, EndStatement, OpHashBuildState, OpHashProbeState,
    StepResult, TxnCleanup, VacuumOpState,
};
use crate::vector::{
    vector1bit, vector32, vector32_sparse, vector64, vector8, vector_concat, vector_distance_cos,
    vector_distance_dot, vector_distance_jaccard, vector_distance_l2, vector_extract, vector_slice,
};
use crate::{
    connection::Row,
    get_cursor, info, is_attached_db,
    storage::wal::CheckpointResult,
    turso_assert,
    types::{AggContext, Cursor, ExternalAggState, SeekKey, SeekOp, SumAggState, Value, ValueType},
    util::{cast_real_to_integer, checked_cast_text_to_numeric},
    vdbe::{
        builder::CursorType,
        insn::{IdxInsertFlags, Insn, SavepointOp},
    },
    CaptureDataChangesInfo, CdcVersion, CheckpointMode, Completion, Connection, Database,
    DatabaseStorage, IOExt, MvCursor, NonNan, OpenFlags, QueryMode, Statement, TransactionState,
    ValueRef, WalAutoActions, MAIN_DB_ID, TEMP_DB_ID,
};
use crate::{
    error::{
        LimboError, SQLITE_CONSTRAINT, SQLITE_CONSTRAINT_CHECK, SQLITE_CONSTRAINT_FOREIGNKEY,
        SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_TRIGGER,
        SQLITE_ERROR, SQLITE_FULL,
    },
    function::{AggFunc, ExtFunc, MathFunc, MathFuncArity, ScalarFunc, VectorFunc},
    functions::{
        datetime::{
            exec_date, exec_datetime_full, exec_julianday, exec_strftime, exec_time, exec_unixepoch,
        },
        printf::exec_printf,
    },
    stats::StatAccum,
    translate::emitter::TransactionMode,
};
use branches::{mark_unlikely, unlikely};
use either::Either;
use smallvec::SmallVec;
use std::any::Any;
use std::str::FromStr;
use std::{
    borrow::BorrowMut,
    num::NonZero,
    sync::{atomic::Ordering, Arc},
};
use turso_macros::{match_ignore_ascii_case, turso_debug_assert};

use crate::pseudo::PseudoCursor;

use crate::storage::btree::{BTreeCursor, BTreeKey};

#[inline]
fn btree_cursor_with_yield_context(
    cursor: Box<BTreeCursor>,
    connection: &Arc<Connection>,
) -> Box<BTreeCursor> {
    #[cfg(any(test, injected_yields))]
    {
        let mut cursor = cursor;
        cursor.install_yield_context(connection);
        cursor
    }
    #[cfg(not(any(test, injected_yields)))]
    {
        let _ = connection;
        cursor
    }
}

use super::{
    array::{
        array_values_from_blob, compare_arrays, compute_array_length, compute_array_length_at_dim,
        exec_array_append, exec_array_cat, exec_array_contains, exec_array_contains_all,
        exec_array_overlap, exec_array_position, exec_array_prepend, exec_array_remove,
        exec_array_slice, exec_array_to_string, exec_string_to_array, make_array_from_registers,
        parse_text_array, serialize_array_from_blob, values_to_record_blob,
    },
    insn::{
        AddSequenceData, AggStepData, ArrayEncodeData, Cookie, IntegrityCkData, RegisterOrLiteral,
        SortComparatorType, SorterOpenData,
    },
    CommitState,
};
use crate::sync::{Mutex, RwLock};
use turso_parser::ast::{self, ForeignKeyClause, Name, QualifiedName, ResolveType};
use turso_parser::parser::Parser;

use super::sorter::Sorter;
use crate::vdbe::vacuum::{
    capture_custom_types, mirror_symbols, reject_unsupported_vacuum_auto_vacuum_mode,
    vacuum_target_build_step, vacuum_target_opts_from_source, VacuumDbHeaderMeta,
    VacuumTargetBuildConfig, VacuumTargetBuildContext,
};

#[cfg(feature = "json")]
use crate::{
    function::JsonFunc, json, json::convert_dbtype_to_raw_jsonb, json::ensure_blob_arg_is_jsonb,
    json::get_json, json::is_json_valid, json::json_array, json::json_array_length,
    json::json_arrow_extract, json::json_arrow_shift_extract, json::json_error_position,
    json::json_extract, json::json_from_raw_bytes_agg, json::json_insert, json::json_object,
    json::json_patch, json::json_quote, json::json_remove, json::json_replace, json::json_set,
    json::json_type, json::jsonb, json::jsonb_array, json::jsonb_extract, json::jsonb_insert,
    json::jsonb_object, json::jsonb_patch, json::jsonb_remove, json::jsonb_replace,
    json::jsonb_set, json::raw_jsonb_element_len, json::Conv,
};

use super::{Program, ProgramState, Register};

#[cfg(feature = "fs")]
use crate::connection::resolve_ext_path;
use crate::vdbe::builder::CursorTypeExt;
use crate::{bail_constraint_error, must_be_btree_cursor, MvStore, Pager, Result};

type MvccCheckpointStateMachine = CheckpointStateMachine<MvccClock, DynAllocator>;

/// Macro to destructure an Insn enum variant, only to be used when it
/// is *impossible* to be another variant.
macro_rules! load_insn {
    ($variant:ident { $($field:tt $(: $binding:pat)?),* $(,)? }, $insn:expr) => {
        #[cfg(debug_assertions)]
        let Insn::$variant { $($field $(: $binding)?),* } = $insn else {
            panic!("Expected Insn::{}, got {:?}", stringify!($variant), $insn);
        };
        #[cfg(not(debug_assertions))]
        let Insn::$variant { $($field $(: $binding)?),*} = $insn else {
             // this will optimize away the branch
            unsafe { std::hint::unreachable_unchecked() };
        };
    };
}

macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr {
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
            Err(err) => {
                mark_unlikely();
                return Err(err.into());
            }
        }
    };
}

macro_rules! check_arg_count {
    ($actual:expr, $expected:expr) => {
        if unlikely($actual != $expected) {
            return Err(LimboError::InternalError(format!(
                "expected {} argument(s), got {}",
                $expected, $actual
            ))
            .into());
        }
    };
}

/// Errors are boxed so an op's whole return value stays small: a LimboError
/// is 40 bytes and would otherwise be copied through memory on every executed
/// instruction. `?` boxes automatically via `From`; explicit `Err` sites box
/// at the point the error is created.
pub type InsnResult = Result<InsnFunctionStepResult, Box<LimboError>>;
pub type InsnFunction = fn(&Program, &mut ProgramState, &Insn, &Arc<Pager>) -> InsnResult;

/// Parse a Value (text, int, float, or blob) into a BigDecimal.
fn value_to_bigdecimal(val: &Value) -> Result<bigdecimal::BigDecimal> {
    use bigdecimal::BigDecimal;
    use std::str::FromStr;
    match val {
        Value::Numeric(Numeric::Integer(i)) => Ok(BigDecimal::from(*i)),
        Value::Numeric(Numeric::Float(f)) => BigDecimal::from_str(&f.to_string())
            .map_err(|_| LimboError::Constraint(format!("invalid numeric value: {f}"))),
        Value::Text(t) => BigDecimal::from_str(&t.value)
            .map_err(|_| LimboError::Constraint(format!("invalid numeric value: \"{}\"", t.value))),
        Value::Blob(b) => crate::numeric::decimal::blob_to_bigdecimal(b),
        _ => Err(LimboError::Constraint(format!(
            "cannot convert to numeric: \"{val}\""
        ))),
    }
}

/// Create a sort comparator closure from a SortComparatorType enum.
fn make_sort_comparator(
    cmp_type: &SortComparatorType,
) -> Result<crate::vdbe::sorter::SortComparator> {
    use std::cmp::Ordering;
    let cmp: crate::vdbe::sorter::SortComparator = match cmp_type {
        SortComparatorType::NumericLt => {
            std::sync::Arc::new(|a: &ValueRef, b: &ValueRef| -> Result<Ordering> {
                let res = match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    _ => {
                        // Decode from ValueRef to Value for value_to_bigdecimal
                        let a_val = a.to_owned()?;
                        let b_val = b.to_owned()?;
                        match (value_to_bigdecimal(&a_val), value_to_bigdecimal(&b_val)) {
                            (Ok(a_dec), Ok(b_dec)) => a_dec.cmp(&b_dec),
                            _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                        }
                    }
                };
                Ok(res)
            })
        }
        SortComparatorType::StringReverse => {
            std::sync::Arc::new(|a: &ValueRef, b: &ValueRef| -> Result<Ordering> {
                fn reverse_str(v: &ValueRef) -> String {
                    match v {
                        ValueRef::Text(t) => t.to_string().chars().rev().collect(),
                        _ => String::new(),
                    }
                }
                let res = match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    _ => reverse_str(a).cmp(&reverse_str(b)),
                };
                Ok(res)
            })
        }
        SortComparatorType::TestUintLt => {
            std::sync::Arc::new(|a: &ValueRef, b: &ValueRef| -> Result<Ordering> {
                fn to_u64(v: &ValueRef) -> Option<u64> {
                    match v {
                        ValueRef::Null => None,
                        ValueRef::Numeric(Numeric::Integer(i)) => {
                            if *i >= 0 {
                                Some(*i as u64)
                            } else {
                                None
                            }
                        }
                        ValueRef::Text(t) => t.to_string().parse::<u64>().ok(),
                        _ => None,
                    }
                }
                let res = match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    _ => match (to_u64(a), to_u64(b)) {
                        (Some(a), Some(b)) => a.cmp(&b),
                        _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                    },
                };
                Ok(res)
            })
        }
        SortComparatorType::ArrayLt => {
            std::sync::Arc::new(|a: &ValueRef, b: &ValueRef| -> Result<Ordering> {
                let res = match (a, b) {
                    (ValueRef::Null, ValueRef::Null) => Ordering::Equal,
                    (ValueRef::Null, _) => Ordering::Less,
                    (_, ValueRef::Null) => Ordering::Greater,
                    (ValueRef::Blob(a_blob), ValueRef::Blob(b_blob)) => {
                        crate::vdbe::array::compare_arrays(a_blob, b_blob)
                            .unwrap_or(Ordering::Equal)
                    }
                    (ValueRef::Text(a_text), ValueRef::Text(b_text)) => {
                        let a_vals = crate::vdbe::array::parse_text_array(a_text);
                        let b_vals = crate::vdbe::array::parse_text_array(b_text);
                        match (a_vals, b_vals) {
                            (Some(av), Some(bv)) => {
                                let a_blob = crate::vdbe::array::values_to_record_blob(&av)?;
                                let b_blob = crate::vdbe::array::values_to_record_blob(&bv)?;
                                if let (Value::Blob(ab), Value::Blob(bb)) = (&a_blob, &b_blob) {
                                    crate::vdbe::array::compare_arrays(ab, bb)
                                        .unwrap_or(Ordering::Equal)
                                } else {
                                    Ordering::Equal
                                }
                            }
                            _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                        }
                    }
                    _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
                };
                Ok(res)
            })
        }
    };
    Ok(cmp)
}

/// Compare two values using the specified collation for text values.
/// Non-text values are compared using their natural ordering.
fn compare_with_collation(
    lhs: &Value,
    rhs: &Value,
    collation: Option<CollationSeq>,
    collation_comparator: &Option<crate::vdbe::sorter::SortComparator>,
) -> Result<std::cmp::Ordering> {
    Ok(match (lhs, rhs) {
        (Value::Text(lhs_text), Value::Text(rhs_text)) => {
            if let Some(comparator) = collation_comparator {
                comparator(&lhs.as_ref(), &rhs.as_ref())?
            } else if let Some(coll) = collation {
                coll.compare_strings(lhs_text.as_str(), rhs_text.as_str())
            } else {
                lhs.cmp(rhs)
            }
        }
        _ => lhs.cmp(rhs),
    })
}

fn compare_with_program_collation<V1, V2>(
    program: &Program,
    lhs: V1,
    rhs: V2,
    collation: CollationSeq,
) -> Result<std::cmp::Ordering>
where
    V1: AsValueRef,
    V2: AsValueRef,
{
    let lhs = lhs.as_value_ref();
    let rhs = rhs.as_value_ref();
    if collation.is_custom() {
        if let (ValueRef::Text(lhs_text), ValueRef::Text(rhs_text)) = (lhs, rhs) {
            return program.connection.compare_external_collation(
                collation,
                lhs_text.as_str(),
                rhs_text.as_str(),
            );
        }
    }
    Ok(compare_immutable_single(lhs, rhs, collation))
}

fn comparison_matches_order(op: ComparisonOp, order: std::cmp::Ordering) -> bool {
    match op {
        ComparisonOp::Eq => order.is_eq(),
        ComparisonOp::Ne => !order.is_eq(),
        ComparisonOp::Lt => order.is_lt(),
        ComparisonOp::Le => order.is_le(),
        ComparisonOp::Gt => order.is_gt(),
        ComparisonOp::Ge => order.is_ge(),
    }
}

pub enum InsnFunctionStepResult {
    Done,
    IO(IOCompletions),
    Row,
    Step,
}

impl<T> From<IOResult<T>> for InsnFunctionStepResult {
    fn from(value: IOResult<T>) -> Self {
        match value {
            IOResult::Done(_) => InsnFunctionStepResult::Done,
            IOResult::IO(io) => InsnFunctionStepResult::IO(io),
        }
    }
}

pub fn op_init(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Init { target_pc }, insn);
    if unlikely(!target_pc.is_offset()) {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Add { lhs, rhs, dest }, insn);
    if let Some(result) = integer_operands(state, *lhs, *rhs).and_then(|(l, r)| l.checked_add(r)) {
        state.registers[*dest].set_int(result);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    op_arithmetic_slow(state, *lhs, *rhs, *dest, Value::exec_add)
}

pub fn op_subtract(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Subtract { lhs, rhs, dest }, insn);
    if let Some(result) = integer_operands(state, *lhs, *rhs).and_then(|(l, r)| l.checked_sub(r)) {
        state.registers[*dest].set_int(result);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    op_arithmetic_slow(state, *lhs, *rhs, *dest, Value::exec_subtract)
}

pub fn op_multiply(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Multiply { lhs, rhs, dest }, insn);
    if let Some(result) = integer_operands(state, *lhs, *rhs).and_then(|(l, r)| l.checked_mul(r)) {
        state.registers[*dest].set_int(result);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    op_arithmetic_slow(state, *lhs, *rhs, *dest, Value::exec_multiply)
}

/// Add, Subtract and Multiply for every operand pair that is not two
/// integers without overflow: affinity conversions, floats and NULLs.
#[inline(never)]
fn op_arithmetic_slow(
    state: &mut ProgramState,
    lhs: usize,
    rhs: usize,
    dest: usize,
    exec: fn(&Value, &Value) -> Value,
) -> InsnResult {
    state.registers[dest].set_value(exec(
        state.registers[lhs].get_value(),
        state.registers[rhs].get_value(),
    ));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// The two operands of an arithmetic opcode when both are integers, the
/// case that needs no affinity conversion.
#[inline(always)]
fn integer_operands(state: &ProgramState, lhs: usize, rhs: usize) -> Option<(i64, i64)> {
    match (&state.registers[lhs], &state.registers[rhs]) {
        (
            Register::Value(Value::Numeric(Numeric::Integer(l))),
            Register::Value(Value::Numeric(Numeric::Integer(r))),
        ) => Some((*l, *r)),
        _ => None,
    }
}

pub fn op_divide(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Divide { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_divide(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_index(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DropIndex { index, db }, insn);
    let conn = program.connection.clone();
    let is_mvcc = conn.mv_store_for_db(*db).is_some();
    conn.with_database_schema_mut(*db, |schema| {
        // In MVCC mode, track dropped index root pages so integrity_check knows about them.
        // The btree pages won't be freed until checkpoint, so integrity_check needs to
        // include them to avoid "page never used" false positives.
        if is_mvcc && index.root_page > 0 {
            schema.dropped_root_pages.insert(index.root_page);
        }
        schema.remove_index(index);
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_remainder(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Remainder { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_remainder(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_and(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(BitAnd { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_bit_and(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_or(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(BitOr { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_bit_or(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_bit_not(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(BitNot { reg, dest }, insn);
    state.registers[*dest].set_value(state.registers[*reg].get_value().exec_bit_not());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_checkpoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    fn set_not_in_wal_result(state: &mut ProgramState, dest: usize) {
        // Match SQLite for databases that are not in WAL mode.
        state.registers[dest].set_int(0);
        state.registers[dest + 1].set_int(-1);
        state.registers[dest + 2].set_int(-1);
    }

    load_insn!(
        Checkpoint {
            database,
            checkpoint_mode,
            dest,
        },
        insn
    );
    if !program.connection.auto_commit.load(Ordering::SeqCst) {
        // TODO: sqlite returns "Runtime error: database table is locked (6)" when a table is in use
        // when a checkpoint is attempted. We don't have table locks, so return TableLocked for any
        // attempt to checkpoint in an interactive transaction. This does not end the transaction,
        // however.
        return Err(LimboError::TableLocked.into());
    }
    if *database == crate::TEMP_DB_ID && program.connection.temp.database.read().is_none() {
        set_not_in_wal_result(state, *dest);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let pager = program.get_pager_from_database_index(database)?;
    if !pager.has_wal() {
        set_not_in_wal_result(state, *dest);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    if state.explicit_checkpoint_guard.is_none() {
        // No exemption for nested statements: nothing in the engine prepares
        // wal_checkpoint as a nested helper (VACUUM checkpoints through
        // Connection::checkpoint), and nested statements are not counted in
        // n_active_root_statements, so the guard's expected count still holds
        // if one ever runs this opcode. Skipping the guard whenever *any*
        // helper statement is alive on the connection would let a checkpoint
        // clobber a suspended statement that holds one (e.g. a pragma vtab
        // cursor's stored helper).
        state.explicit_checkpoint_guard = Some(
            program
                .connection
                .begin_explicit_checkpoint(pager.clone(), true)?,
        );
    }

    // In autocommit mode, this statement can still hold an implicit read tx.
    // RESTART/TRUNCATE checkpoint needs to restart WAL and may fail with Busy
    // if we keep our own statement read slot while checkpointing.
    if matches!(
        checkpoint_mode,
        CheckpointMode::Restart | CheckpointMode::Truncate { .. }
    ) && pager.holds_read_lock()
    {
        pager.end_read_tx();
    }
    // Re-fetch mv_store from connection to get the latest value.
    // This is necessary because the mv_store may have been set by a preceding JournalMode instruction
    // (e.g., when switching from WAL to MVCC mode via `PRAGMA journal_mode = "mvcc"`).
    let sync_mode = program.connection.get_sync_mode_for_database(*database)?;
    let mv_store = program.connection.mv_store_for_db(*database);
    if let Some(mv_store) = mv_store.as_ref() {
        use crate::state_machine::{StateTransition, TransitionResult};
        // MVCC honors the requested checkpoint mode: Truncate/Restart reset the WAL
        // file, Passive/Full backfill and leave it (relying on restart-on-write).
        let mut ckpt_sm = CheckpointStateMachine::new(
            pager.clone(),
            mv_store.clone(),
            program.connection.clone(),
            true,
            sync_mode,
            *database,
            *checkpoint_mode,
        );
        let CheckpointResult {
            wal_max_frame,
            wal_total_backfilled,
            ..
        } = loop {
            match ckpt_sm.step(&()) {
                Ok(TransitionResult::Continue) => {}
                Ok(TransitionResult::Done(result)) => break result,
                Ok(TransitionResult::Io(iocompletions)) => {
                    if let Err(err) = iocompletions.wait(pager.io.as_ref()) {
                        ckpt_sm.cleanup_after_external_io_error(err.clone())?;
                        return Err(err.into());
                    }
                }
                Err(err) => return Err(err.into()),
            }
        };
        // https://sqlite.org/pragma.html#pragma_wal_checkpoint
        // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
        state.registers[*dest].set_int(0);
        // 2nd col: # modified pages written to wal file
        state.registers[*dest + 1].set_int(wal_max_frame as i64);
        // 3rd col: # pages moved to db after checkpoint
        state.registers[*dest + 2].set_int(wal_total_backfilled as i64);

        state.explicit_checkpoint_guard = None;
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    let step_result = pager.checkpoint(*checkpoint_mode, sync_mode, true);
    match step_result {
        Ok(IOResult::Done(CheckpointResult {
            wal_max_frame,
            wal_total_backfilled,
            ..
        })) => {
            // https://sqlite.org/pragma.html#pragma_wal_checkpoint
            // 1st col: 1 (checkpoint SQLITE_BUSY) or 0 (not busy).
            state.registers[*dest].set_int(0);
            // 2nd col: # modified pages written to wal file
            state.registers[*dest + 1].set_int(wal_max_frame as i64);
            // 3rd col: # pages moved to db after checkpoint
            state.registers[*dest + 2].set_int(wal_total_backfilled as i64);

            state.explicit_checkpoint_guard = None;
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(IOResult::IO(io)) => Ok(InsnFunctionStepResult::IO(io)),
        Err(err) => {
            tracing::debug!("PRAGMA wal_checkpoint failed: {err:?}");
            pager.clear_checkpoint_state();
            state.explicit_checkpoint_guard = None;
            state.registers[*dest].set_int(1);
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

pub fn op_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    match insn {
        Insn::Null { dest, dest_end } | Insn::BeginSubrtn { dest, dest_end } => {
            if let Some(dest_end) = dest_end {
                for i in *dest..=*dest_end {
                    state.registers[i].set_null();
                    // Clear any associated RowSet so it can be reused in a fresh
                    // state.  In SQLite the RowSet lives inside the register and
                    // is destroyed by OP_Null; we keep RowSets in a side map, so
                    // we must remove them explicitly.
                    state.rowsets.remove(&i);
                }
            } else {
                state.registers[*dest].set_null();
                state.rowsets.remove(dest);
            }
        }
        _ => {
            mark_unlikely();
            unreachable!("unexpected Insn {:?}", insn)
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_null_row(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(NullRow { cursor_id }, insn);
    // NullRow can target a never-opened cursor slot (e.g. a Once-guarded
    // OpenAutoindex in a loop body that never ran); install a permanently
    // null placeholder, mirroring SQLite's OP_NullRow.
    if state.cursors[*cursor_id].is_none() {
        let (_, cursor_type) = program
            .cursor_ref
            .get(*cursor_id)
            .expect("cursor_id should exist in cursor_ref");
        turso_assert!(
            matches!(
                cursor_type,
                CursorType::BTreeTable(_)
                    | CursorType::BTreeIndex(_)
                    | CursorType::IndexMethod(_)
            ),
            "NullRow on unopened non-btree cursor",
            { "cursor_id": cursor_id }
        );
        state.cursors[*cursor_id] = Some(Cursor::NullRow);
    }
    state.get_cursor(*cursor_id).set_null_flag(true);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_compare(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Compare {
            start_reg_a,
            start_reg_b,
            count,
            key_info,
        },
        insn
    );
    let start_reg_a = *start_reg_a;
    let start_reg_b = *start_reg_b;
    let count = *count;

    if unlikely(start_reg_a + count > start_reg_b) {
        return Err(LimboError::InternalError("Compare registers overlap".to_string()).into());
    }

    // (https://github.com/tursodatabase/turso/issues/2304): reusing logic from compare_immutable().
    // TODO: There are tons of cases like this where we could reuse this in a similar vein
    let a_range =
        (start_reg_a..start_reg_a + count + 1).map(|idx| state.registers[idx].get_value());
    let b_range =
        (start_reg_b..start_reg_b + count + 1).map(|idx| state.registers[idx].get_value());
    let cmp = compare_immutable(a_range, b_range, key_info);

    state.last_compare = Some(cmp);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_jump(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Jump {
            target_pc_lt,
            target_pc_eq,
            target_pc_gt,
        },
        insn
    );
    assert!(target_pc_lt.is_offset());
    assert!(target_pc_eq.is_offset());
    assert!(target_pc_gt.is_offset());
    let cmp = state.last_compare.take();
    if unlikely(cmp.is_none()) {
        return Err(LimboError::InternalError("Jump without compare".to_string()).into());
    }
    let target_pc = match cmp.expect("comparison should succeed for valid operands") {
        std::cmp::Ordering::Less => *target_pc_lt,
        std::cmp::Ordering::Equal => *target_pc_eq,
        std::cmp::Ordering::Greater => *target_pc_gt,
    };
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_move(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Move {
            source_reg,
            dest_reg,
            count,
        },
        insn
    );
    let source_reg = *source_reg;
    let dest_reg = *dest_reg;
    let count = *count;
    for i in 0..count {
        state.registers[dest_reg + i] = std::mem::replace(
            &mut state.registers[source_reg + i],
            Register::Value(Value::Null),
        );
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_pos(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IfPos {
            reg,
            target_pc,
            decrement_by,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    let reg = *reg;
    let target_pc = *target_pc;
    match state.registers[reg].get_value() {
        Value::Numeric(Numeric::Integer(n)) if *n > 0 => {
            state.pc = target_pc.as_offset_int();
            state.registers[reg].set_int(*n - *decrement_by as i64);
        }
        Value::Numeric(Numeric::Integer(_)) => {
            state.pc += 1;
        }
        _ => {
            mark_unlikely();
            return Err(LimboError::InternalError(
                "IfPos: the value in the register is not an integer".into(),
            )
            .into());
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(NotNull { reg, target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    let reg = *reg;
    let target_pc = *target_pc;
    match &state.registers[reg].get_value() {
        Value::Null => {
            state.pc += 1;
        }
        _ => {
            state.pc = target_pc.as_offset_int();
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_comparison(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    let (lhs, rhs, target_pc, op) = match insn {
        Insn::Eq {
            lhs,
            rhs,
            target_pc,
            ..
        } => (*lhs, *rhs, *target_pc, ComparisonOp::Eq),
        Insn::Ne {
            lhs,
            rhs,
            target_pc,
            ..
        } => (*lhs, *rhs, *target_pc, ComparisonOp::Ne),
        Insn::Lt {
            lhs,
            rhs,
            target_pc,
            ..
        } => (*lhs, *rhs, *target_pc, ComparisonOp::Lt),
        Insn::Le {
            lhs,
            rhs,
            target_pc,
            ..
        } => (*lhs, *rhs, *target_pc, ComparisonOp::Le),
        Insn::Gt {
            lhs,
            rhs,
            target_pc,
            ..
        } => (*lhs, *rhs, *target_pc, ComparisonOp::Gt),
        Insn::Ge {
            lhs,
            rhs,
            target_pc,
            ..
        } => (*lhs, *rhs, *target_pc, ComparisonOp::Ge),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    let crate::vdbe::BranchOffset::Offset(target_pc) = target_pc else {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    };

    // Two integers compare directly: no NULL handling, affinity or collation.
    if let (
        Register::Value(Value::Numeric(Numeric::Integer(l))),
        Register::Value(Value::Numeric(Numeric::Integer(r))),
    ) = (&state.registers[lhs], &state.registers[rhs])
    {
        let should_jump = match op {
            ComparisonOp::Eq => l == r,
            ComparisonOp::Ne => l != r,
            ComparisonOp::Lt => l < r,
            ComparisonOp::Le => l <= r,
            ComparisonOp::Gt => l > r,
            ComparisonOp::Ge => l >= r,
        };
        state.pc = if should_jump { target_pc } else { state.pc + 1 };
        return Ok(InsnFunctionStepResult::Step);
    }
    let (flags, collation) = match insn {
        Insn::Eq {
            flags, collation, ..
        }
        | Insn::Ne {
            flags, collation, ..
        }
        | Insn::Lt {
            flags, collation, ..
        }
        | Insn::Le {
            flags, collation, ..
        }
        | Insn::Gt {
            flags, collation, ..
        }
        | Insn::Ge {
            flags, collation, ..
        } => (*flags, collation.unwrap_or_default()),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    op_comparison_slow(program, state, lhs, rhs, target_pc, flags, collation, op)
}

/// The comparison opcodes for every operand pair that is not two integers:
/// NULLs, affinity conversions, text collations and array comparison.
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn op_comparison_slow(
    program: &Program,
    state: &mut ProgramState,
    lhs: usize,
    rhs: usize,
    target_pc: crate::vdbe::InsnReference,
    flags: crate::vdbe::insn::CmpInsFlags,
    collation: CollationSeq,
    op: ComparisonOp,
) -> InsnResult {
    let null_eq = flags.has_nulleq();
    let jump_if_null = flags.has_jump_if_null();
    let affinity = flags.get_affinity();

    let lhs_value = state.registers[lhs].get_value();
    let rhs_value = state.registers[rhs].get_value();

    macro_rules! take_jump_if {
        ($should_take_jump:expr) => {
            if $should_take_jump {
                state.pc = target_pc;
            } else {
                state.pc += 1;
            }
            return Ok(InsnFunctionStepResult::Step);
        };
    }

    match (lhs_value, rhs_value) {
        (Value::Null, _) | (_, Value::Null) => {
            let should_jump = if null_eq {
                // SQLITE_NULLEQ makes comparison two-valued and orders a
                // single NULL below every non-NULL value. Although normal
                // expression translation only uses it with Eq/Ne, RANGE
                // boundary tests use it with relational operators so that
                // NULL peer groups compare consistently.
                let order = compare_immutable_single(lhs_value, rhs_value, collation);
                comparison_matches_order(op, order)
            } else {
                jump_if_null
            };
            take_jump_if!(should_jump);
        }
        (Value::Blob(lb), Value::Blob(rb)) if flags.has_array_cmp() => {
            // Element-wise array comparison
            if let Ok(ord) = compare_arrays(lb, rb) {
                let should_jump = match op {
                    ComparisonOp::Eq => ord.is_eq(),
                    ComparisonOp::Ne => !ord.is_eq(),
                    ComparisonOp::Lt => ord.is_lt(),
                    ComparisonOp::Le => ord.is_le(),
                    ComparisonOp::Gt => ord.is_gt(),
                    ComparisonOp::Ge => ord.is_ge(),
                };
                take_jump_if!(should_jump);
            }
        }
        (_, _) => {}
    }

    // If all else failed, do an affinity-aware comparison
    let (new_lhs, new_rhs) = (
        affinity.convert_for_compare(lhs_value),
        affinity.convert_for_compare(rhs_value),
    );

    let lhs_for_compare = new_lhs
        .as_ref()
        .map_or(Either::Left(lhs_value), Either::Right);
    let rhs_for_compare = new_rhs
        .as_ref()
        .map_or(Either::Left(rhs_value), Either::Right);
    let should_jump = if collation.is_custom() {
        let order =
            compare_with_program_collation(program, lhs_for_compare, rhs_for_compare, collation)?;
        comparison_matches_order(op, order)
    } else {
        op.compare(lhs_for_compare, rhs_for_compare, collation)
    };

    match (new_lhs, new_rhs) {
        (Some(new_lhs), None) => {
            state.registers[lhs].set_value(new_lhs.as_value_ref().to_owned()?);
        }
        (None, Some(new_rhs)) => {
            state.registers[rhs].set_value(new_rhs.as_value_ref().to_owned()?);
        }
        (Some(new_lhs), Some(new_rhs)) => {
            let (new_lhs, new_rhs) = (
                new_lhs.as_value_ref().to_owned()?,
                new_rhs.as_value_ref().to_owned()?,
            );
            state.registers[lhs].set_value(new_lhs);
            state.registers[rhs].set_value(new_rhs);
        }
        (None, None) => {}
    }

    take_jump_if!(should_jump);
}

pub fn op_if(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        If {
            reg,
            target_pc,
            jump_if_null,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    if state.registers[*reg]
        .get_value()
        .exec_if(*jump_if_null, false)
    {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_not(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IfNot {
            reg,
            target_pc,
            jump_if_null,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    if state.registers[*reg]
        .get_value()
        .exec_if(*jump_if_null, true)
    {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

fn ensure_index_method_context(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    database_id: usize,
    attachment: &dyn crate::index_method::IndexMethodAttachment,
) -> Result<Arc<crate::index_method::IndexMethodContext>> {
    // Arc, not a deep clone: an opcode that yields on IO re-enters and calls
    // this again — a cold FTS open does that hundreds of times per megabyte,
    // and cloning the context's four heap strings each time adds up.
    if let Some(context) = state.index_method_contexts[cursor_id].as_ref() {
        return Ok(Arc::clone(context));
    }
    let context = Arc::new(crate::index_method::IndexMethodContext::new(
        &program.connection,
        database_id,
        &attachment.definition(),
    )?);
    state.index_method_contexts[cursor_id] = Some(Arc::clone(&context));
    Ok(context)
}

pub fn op_open_read(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        OpenRead {
            cursor_id,
            root_page,
            db,
        },
        insn
    );

    invalidate_deferred_seeks_for_cursor(state, *cursor_id);

    let pager = program.get_pager_from_database_index(db)?;
    let mv_store = program.connection.mv_store_for_db(*db);

    if let (_, CursorType::IndexMethod(module)) = &program.cursor_ref[*cursor_id] {
        if mv_store.is_some() {
            crate::index_method::ensure_mvcc_support(&module.definition(), false)?;
        }
        let context =
            ensure_index_method_context(program, state, *cursor_id, *db, module.as_ref())?;
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }

        let cursor = state.cursors[*cursor_id]
            .as_mut()
            .expect("cursor should exist after initialization");
        let cursor = cursor.as_index_method_mut();
        return_if_io!(cursor.open_read(&context));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    if program.connection.get_mv_tx_id_for_db(*db).is_none() {
        assert!(
            *root_page >= 0,
            "root page should be non negative when we are not in a MVCC transaction"
        );
    }
    let cursors = &mut state.cursors;
    let num_columns = match cursor_type {
        CursorType::BTreeTable(table_rc) => table_rc.columns().len(),
        CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
        CursorType::MaterializedView(table_rc, _) => table_rc.columns().len(),
        _ => unreachable!("This should not have happened"),
    };

    let maybe_promote_to_mvcc_cursor = |btree_cursor: Box<dyn CursorTrait>,
                                        mv_cursor_type: MvccCursorType|
     -> Result<Box<dyn CursorTrait>> {
        if let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) {
            let mv_store = mv_store
                .as_ref()
                .expect("mv_store should be Some when MVCC transaction is active")
                .clone();
            Ok(Box::new(MvCursor::new(
                mv_store,
                &program.connection,
                tx_id,
                *root_page,
                mv_cursor_type,
                btree_cursor,
            )?))
        } else {
            Ok(btree_cursor)
        }
    };

    match cursor_type {
        CursorType::MaterializedView(_, view_mutex) => {
            // This is a materialized view with storage
            // Create btree cursor for reading the persistent data

            let btree_cursor = Box::new(BTreeCursor::new_table(
                pager.clone(),
                maybe_transform_root_page_to_positive(mv_store.as_ref(), *root_page),
                num_columns,
            ));
            let cursor = maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Table)?;

            // Get the view name and look up or create its transaction state
            let view_name = view_mutex.lock().name().to_string();
            let tx_state = program
                .connection
                .view_transaction_states
                .get_or_create(&view_name);

            // Create materialized view cursor with this view's transaction state
            let mv_cursor = crate::incremental::cursor::MaterializedViewCursor::new(
                cursor,
                view_mutex.clone(),
                pager,
                tx_state,
            )?;

            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_materialized_view(mv_cursor));
        }
        CursorType::BTreeTable(table) => {
            // Regular table
            if !table.has_rowid && program.connection.get_mv_tx_id_for_db(*db).is_some() {
                return Err(LimboError::ParseError(
                    "WITHOUT ROWID tables are not supported in MVCC mode".to_string(),
                )
                .into());
            }
            let btree_cursor: Box<dyn CursorTrait> = if table.has_rowid {
                Box::new(BTreeCursor::new_table(
                    pager,
                    maybe_transform_root_page_to_positive(mv_store.as_ref(), *root_page),
                    num_columns,
                ))
            } else {
                Box::new(BTreeCursor::new_without_rowid_table(
                    pager,
                    maybe_transform_root_page_to_positive(mv_store.as_ref(), *root_page),
                    table.as_ref(),
                    num_columns,
                ))
            };
            let cursor = maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Table)?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(index) => {
            let btree_cursor = Box::new(BTreeCursor::new_index(
                pager,
                maybe_transform_root_page_to_positive(mv_store.as_ref(), *root_page),
                index.as_ref(),
                num_columns,
            )?);
            let index_info = Arc::new(if let Some(mv_store) = mv_store.as_ref() {
                IndexInfo::new_from_index_in(index, mv_store.allocator())?
            } else {
                IndexInfo::new_from_index(index)?
            });
            let cursor =
                maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Index(index_info))?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::Pseudo(_) => {
            panic!("OpenRead on pseudo cursor");
        }
        CursorType::Sorter => {
            panic!("OpenRead on sorter cursor");
        }
        CursorType::IndexMethod(..) => {
            unreachable!("IndexMethod handled above")
        }
        CursorType::VirtualTable(_) => {
            panic!("OpenRead on virtual table cursor, use Insn:VOpen instead");
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vopen(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(VOpen { cursor_id }, insn);
    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VOpen on non-virtual table cursor");
    };
    let cursor = virtual_table.open(program.connection.clone())?;
    state
        .cursors
        .get_mut(*cursor_id)
        .unwrap_or_else(|| panic!("cursor id {} out of bounds", *cursor_id))
        .replace(Cursor::Virtual(cursor));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcreate(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        VCreate {
            module_name,
            table_name,
            args_reg,
        },
        insn
    );
    let module_name = state.registers[*module_name].get_value().to_string();
    let table_name = state.registers[*table_name].get_value().to_string();
    let args = if let Some(args_reg) = args_reg {
        if let Register::Record(rec) = &state.registers[*args_reg] {
            rec.iter()?
                .map(|v| v.map(|v| v.to_ffi()))
                .collect::<Result<_, _>>()?
        } else {
            mark_unlikely();
            return Err(
                LimboError::InternalError("VCreate: args_reg is not a record".to_string()).into(),
            );
        }
    } else {
        vec![]
    };
    let conn = program.connection.clone();
    let table =
        crate::VirtualTable::table(Some(&table_name), &module_name, args, &conn.syms.read())?;
    {
        conn.syms.write().vtabs.insert(table_name, table);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vfilter(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        VFilter {
            cursor_id,
            pc_if_empty,
            arg_count,
            args_reg,
            idx_str,
            idx_num,
        },
        insn
    );
    let has_rows = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_virtual_mut();

        let args = (0..*arg_count)
            .map(|i| state.registers[args_reg + i].get_value().try_clone())
            .try_collect::<Result<crate::alloc::Vec<_>>>()??;
        let idx_str = if let Some(idx_str) = idx_str {
            Some(state.registers[*idx_str].get_value().to_string())
        } else {
            None
        };
        cursor.filter(*idx_num as i32, idx_str, *arg_count, args)?
    };
    // Increment filter_operations metric for virtual table filter
    state.metrics.filter_operations = state.metrics.filter_operations.wrapping_add(1);
    if !has_rows {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        // VFilter positions to the first row if any exist, which counts as a read
        state.record_rows_read(1);
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vcolumn(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        VColumn {
            cursor_id,
            column,
            dest,
        },
        insn
    );
    let value = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        cursor.column(*column)?
    };
    state.registers[*dest].set_value(value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vupdate(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    #[cfg(not(feature = "cli_only"))]
    let _ = pager;
    load_insn!(
        VUpdate {
            cursor_id,
            arg_count,
            start_reg,
            conflict_action,
            ..
        },
        insn
    );
    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let CursorType::VirtualTable(virtual_table) = cursor_type else {
        panic!("VUpdate on non-virtual table cursor");
    };
    let allow_dbpage_write = {
        #[cfg(feature = "cli_only")]
        {
            virtual_table.name == crate::dbpage::DBPAGE_TABLE_NAME
                && program.connection.db.opts.unsafe_testing
        }
        #[cfg(not(feature = "cli_only"))]
        {
            false
        }
    };
    if virtual_table.readonly() && !allow_dbpage_write {
        return Err(LimboError::ReadOnly.into());
    }

    if unlikely(*arg_count < 2) {
        return Err(LimboError::InternalError(
            "VUpdate: arg_count must be at least 2 (rowid and insert_rowid)".to_string(),
        )
        .into());
    }
    let mut argv = crate::alloc::Vec::try_with_capacity_ext(*arg_count)?;
    for i in 0..*arg_count {
        if let Some(value) = state.registers.get(*start_reg + i) {
            argv.push_within_capacity(value.get_value().try_clone()?)
                .unwrap();
        } else {
            mark_unlikely();
            return Err(LimboError::InternalError(format!(
                "VUpdate: register out of bounds at {}",
                *start_reg + i
            ))
            .into());
        }
    }
    let result = if allow_dbpage_write {
        #[cfg(feature = "cli_only")]
        {
            crate::dbpage::update_dbpage(pager, &argv)
        }
        #[cfg(not(feature = "cli_only"))]
        {
            unreachable!("sqlite_dbpage writes require cli_only feature");
        }
    } else {
        virtual_table.update(&argv)
    };
    match result {
        Ok(Some(new_rowid)) => {
            state.record_rows_written(1);
            if *conflict_action == 5 {
                // ResolveType::Replace
                program.connection.update_last_rowid(new_rowid);
            }
            state.pc += 1;
        }
        Ok(None) => {
            // no-op or successful update without rowid return
            state.record_rows_written(1);
            state.pc += 1;
        }
        Err(e) => {
            // virtual table update failed
            mark_unlikely();
            return Err(
                LimboError::ExtensionError(format!("Virtual table update failed: {e}")).into(),
            );
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vnext(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        VNext {
            cursor_id,
            pc_if_next,
        },
        insn
    );
    let has_more = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_virtual_mut();
        cursor.next()?
    };
    if has_more {
        // Increment metrics for row read from virtual table (including materialized views)
        state.record_rows_read(1);
        state.pc = pc_if_next.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vdestroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(VDestroy { db: _, table_name }, insn);
    let conn = program.connection.clone();
    {
        let Some(vtab) = conn.syms.write().vtabs.remove(table_name) else {
            mark_unlikely();
            return Err(crate::LimboError::InternalError(
                "Could not find Virtual Table to Destroy".to_string(),
            )
            .into());
        };
        vtab.destroy()?;
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vbegin(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(VBegin { cursor_id }, insn);
    let cursor = state.get_cursor(*cursor_id);
    let cursor = cursor.as_virtual_mut();
    let vtab_id = cursor
        .vtab_id()
        .expect("VBegin on non ext-virtual table cursor");
    let mut states = program.connection.vtab_txn_states.write();
    if states.insert(vtab_id) {
        // Only begin a new transaction if one is not already active for this virtual table module
        let vtabs = &program.connection.syms.read().vtabs;
        let vtab = vtabs
            .iter()
            .find(|p| p.1.id().eq(&vtab_id))
            .expect("Could not find virtual table for VBegin");
        vtab.1.begin()?;
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_vrename(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        VRename {
            cursor_id,
            new_name_reg
        },
        insn
    );
    let name = state.registers[*new_name_reg].get_value().to_string();
    let cursor = state.get_cursor(*cursor_id);
    let cursor = cursor.as_virtual_mut();
    let vtabs = &program.connection.syms.read().vtabs;
    let vtab = vtabs
        .iter()
        .find(|p| {
            p.1.id().eq(&cursor
                .vtab_id()
                .expect("non ext-virtual table used in VRollback"))
        })
        .expect("Could not find virtual table for VRollback");
    vtab.1.rename(&name)?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_pseudo(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        OpenPseudo {
            cursor_id,
            content_reg,
            num_fields: _,
        },
        insn
    );
    {
        let cursors = &mut state.cursors;
        let cursor = PseudoCursor::new(*content_reg);
        cursors
            .get_mut(*cursor_id)
            .expect("cursor_id should be valid")
            .replace(Cursor::new_pseudo(cursor));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_rewind(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Rewind {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());
    // Clear any bloom filter associated with this cursor so stale filter data
    // does not incorrectly reject valid matches in subsequent iterations.
    if let Some(filter) = state.get_bloom_filter_mut(*cursor_id) {
        filter.clear();
    }
    let is_empty = {
        let cursor = state.get_cursor(*cursor_id);
        match cursor {
            Cursor::BTree(btree_cursor) => {
                return_if_io!(btree_cursor.rewind());
                btree_cursor.is_empty()
            }
            Cursor::MaterializedView(mv_cursor) => {
                return_if_io!(mv_cursor.rewind());
                !mv_cursor.is_valid()?
            }
            _ => panic!("Rewind on non-btree/materialized-view cursor"),
        }
    };
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        // Rewind positions to the first row, which is effectively a read
        state.record_rows_read(1);
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_last(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Last {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());
    let is_empty = {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Last");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.last());
        cursor.is_empty()
    };
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        // Last positions to the last row, which is effectively a read
        state.record_rows_read(1);
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug, Clone, Copy)]
pub enum OpColumnState {
    Start,
    Rowid {
        index_cursor_id: usize,
        table_cursor_id: usize,
    },
    Seek {
        rowid: i64,
        table_cursor_id: usize,
    },
    GetColumn,
}

pub fn op_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Column {
            cursor_id,
            column,
            dest,
            default,
        },
        insn
    );
    op_column_impl(
        program,
        state,
        *cursor_id,
        ColumnFetch::Single {
            column: *column,
            dest: *dest,
            default,
        },
    )
}

// Not in test builds: inline(always) makes fn-item coercions produce
// per-site copies in debug, breaking test_make_sure_correct_insn_table's
// pointer-identity check.
#[cfg_attr(not(test), inline(always))]
pub fn op_column_range(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        ColumnRange {
            cursor_id,
            start_column,
            dest,
            defaults,
        },
        insn
    );
    op_column_impl(
        program,
        state,
        *cursor_id,
        ColumnFetch::Range {
            start_column: *start_column,
            dest: *dest,
            defaults,
        },
    )
}

/// What a Column-family instruction fetches once the cursor is positioned.
#[derive(Clone, Copy)]
enum ColumnFetch<'a> {
    /// A single column.
    Single {
        column: usize,
        dest: usize,
        default: &'a Option<Value>,
    },
    /// A range of consecutive columns to be copied to consecutive destination registers.
    Range {
        start_column: usize,
        dest: usize,
        defaults: &'a [Option<Value>],
    },
}

impl ColumnFetch<'_> {
    /// Writes NULL to every destination register of this fetch.
    fn write_null_regs(&self, state: &mut ProgramState) {
        match *self {
            ColumnFetch::Single { dest, .. } => state.registers[dest].set_null(),
            ColumnFetch::Range { dest, defaults, .. } => {
                state.registers[dest..dest + defaults.len()]
                    .iter_mut()
                    .for_each(|reg| reg.set_null());
            }
        }
    }

    #[inline(always)]
    fn fetch(&self, program: &Program, state: &mut ProgramState, cursor_id: usize) -> InsnResult {
        match *self {
            ColumnFetch::Single {
                column,
                dest,
                default,
            } => op_column_fetch(program, state, cursor_id, column, dest, default),
            ColumnFetch::Range {
                start_column,
                dest,
                defaults,
            } => op_column_range_fetch(program, state, cursor_id, start_column, dest, defaults),
        }
    }
}

#[inline(always)]
fn op_column_impl(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    fetch: ColumnFetch<'_>,
) -> InsnResult {
    // Fast path: no deferred seek pending and no suspended state machine. The
    // column fetch either completes or yields IO with nothing persisted, so the
    // op-state slot (enum write + drop on clear) is bypassed entirely. On IO
    // resume the slot is still idle and this path re-executes.
    if state.active_op_state.is_idle() && state.deferred_seeks[cursor_id].is_none() {
        let result = fetch.fetch(program, state, cursor_id)?;
        if matches!(result, InsnFunctionStepResult::Step) {
            state.pc += 1;
        }
        return Ok(result);
    }
    'outer: loop {
        match *state.active_op_state.column() {
            OpColumnState::Start => {
                if let Some(deferred) = state.deferred_seeks[cursor_id].take() {
                    *state.active_op_state.column() = OpColumnState::Rowid {
                        index_cursor_id: deferred.index_cursor_id,
                        table_cursor_id: deferred.table_cursor_id,
                    };
                } else {
                    *state.active_op_state.column() = OpColumnState::GetColumn;
                }
            }
            OpColumnState::Rowid {
                index_cursor_id,
                table_cursor_id,
            } => {
                let Some(rowid) = ({
                    let index_cursor = state.get_cursor(index_cursor_id);
                    match index_cursor {
                        Cursor::BTree(cursor) => return_if_io!(cursor.rowid()),
                        Cursor::IndexMethod(cursor) => return_if_io!(cursor.query_rowid()),
                        _ => panic!("unexpected cursor type"),
                    }
                }) else {
                    fetch.write_null_regs(state);
                    break 'outer;
                };
                *state.active_op_state.column() = OpColumnState::Seek {
                    rowid,
                    table_cursor_id,
                };
            }
            OpColumnState::Seek {
                rowid,
                table_cursor_id,
            } => {
                {
                    let table_cursor = state.get_cursor(table_cursor_id);
                    // MaterializedView cursors shouldn't go through deferred seek logic
                    // but if we somehow get here, handle it appropriately
                    match table_cursor {
                        Cursor::MaterializedView(mv_cursor) => {
                            // Seek to the rowid in the materialized view
                            return_if_io!(mv_cursor
                                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        }
                        _ => {
                            // Regular btree cursor
                            let table_cursor = table_cursor.as_btree_mut();
                            return_if_io!(table_cursor
                                .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        }
                    }
                }
                state.metrics.btree_seeks = state.metrics.btree_seeks.wrapping_add(1);
                state.metrics.search_count = state.metrics.search_count.wrapping_add(1);
                *state.active_op_state.column() = OpColumnState::GetColumn;
            }
            OpColumnState::GetColumn => {
                let result = fetch.fetch(program, state, cursor_id)?;
                if !matches!(result, InsnFunctionStepResult::Step) {
                    // IO yield: the slot stays at GetColumn so the resume
                    // re-enters this arm.
                    return Ok(result);
                }
                break 'outer;
            }
        }
    }

    state.active_op_state.clear();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Fetches one column of the cursor's current row into a register.
fn op_column_fetch(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    column: usize,
    dest: usize,
    default: &Option<Value>,
) -> InsnResult {
    let Some(Cursor::BTree(cursor)) = state
        .cursors
        .get_mut(cursor_id)
        .unwrap_or_else(|| panic!("cursor id {cursor_id} out of bounds"))
    else {
        return op_column_fetch_other(program, state, cursor_id, column, dest, default);
    };
    if cursor.get_null_flag() {
        tracing::trace!("op_column(null_flag)");
        state.registers[dest].set_null();
        return Ok(InsnFunctionStepResult::Step);
    }
    let Some(record) = return_if_io!(cursor.record()) else {
        // Cursor is not positioned on a valid row (e.g., empty table).
        // Return NULL, not the column's default value.
        state.registers[dest].set_null();
        return Ok(InsnFunctionStepResult::Step);
    };
    match record
        .iter()?
        .nth_into_register(column, &mut state.registers[dest])
    {
        Some(result) => result?,
        None => {
            branches::mark_unlikely();
            // The record has fewer columns than expected.
            apply_column_default(default, &mut state.registers[dest])?;
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

/// Column for every cursor that is not a b-tree cursor object: materialized
/// views, the NullRow placeholder, sorters, pseudo cursors and index methods.
#[inline(never)]
fn op_column_fetch_other(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    column: usize,
    dest: usize,
    default: &Option<Value>,
) -> InsnResult {
    // First check if this is a MaterializedViewCursor
    {
        let cursor = state.get_cursor(cursor_id);
        if let Cursor::MaterializedView(mv_cursor) = cursor {
            // Handle materialized view column access
            let value = return_if_io!(mv_cursor.column(column));
            state.registers[dest].set_value(value);
            return Ok(InsnFunctionStepResult::Step);
        }
        if matches!(cursor, Cursor::NullRow) {
            tracing::trace!("op_column(null_row placeholder)");
            state.registers[dest].set_null();
            return Ok(InsnFunctionStepResult::Step);
        }
        // Fall back to normal handling
    }

    let (_, cursor_type) = program
        .cursor_ref
        .get(cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    match cursor_type {
        CursorType::BTreeTable(_)
        | CursorType::BTreeIndex(_)
        | CursorType::MaterializedView(_, _) => {
            {
                let cursor_ref =
                    must_be_btree_cursor!(cursor_id, program.cursor_ref, state, "Column");
                let cursor = cursor_ref.as_btree_mut();

                if cursor.get_null_flag() {
                    tracing::trace!("op_column(null_flag)");
                    state.registers[dest].set_null();
                    return Ok(InsnFunctionStepResult::Step);
                }

                let record_result = return_if_io!(cursor.record());
                let Some(record) = record_result else {
                    // Cursor is not positioned on a valid row (e.g., empty table).
                    // Return NULL, not the column's default value.
                    // DEFAULT handling below is for when record exists
                    // but has fewer columns than expected.
                    state.registers[dest].set_null();
                    return Ok(InsnFunctionStepResult::Step);
                };

                let mut payload_iterator = record.iter()?;

                // Parse the header for serial types incrementally until we have the target column
                // Use nth_into_register to write directly to the register without
                // creating intermediate ValueRef allocations

                match payload_iterator.nth_into_register(column, &mut state.registers[dest]) {
                    Some(result) => {
                        result?;
                        return Ok(InsnFunctionStepResult::Step);
                    }
                    None => {
                        branches::mark_unlikely();
                        // record has fewer columns than expected
                    }
                };
            };

            apply_column_default(default, &mut state.registers[dest])?;
        }
        CursorType::Sorter => {
            let record = {
                let cursor = state.get_cursor(cursor_id);
                let cursor = cursor.as_sorter_mut();
                cursor.record().cloned()
            };
            if let Some(record) = record {
                state.registers[dest].set_value(match record.get_value_opt(column) {
                    Some(val) => val.to_owned()?,
                    None => default.clone().unwrap_or(Value::Null),
                });
            } else {
                state.registers[dest].set_null();
            }
        }
        CursorType::Pseudo(_) => {
            let content_reg = crate::get_cursor!(state, cursor_id)
                .as_pseudo_mut()
                .content_reg();
            // The record is read while decoding into the destination register,
            // so the two registers must be distinct.
            let [content, dest_reg] = state
                .registers
                .get_disjoint_mut([content_reg, dest])
                .map_err(|_| {
                    LimboError::InternalError(format!(
                        "Column: pseudo-cursor content register {content_reg} and destination register {dest} must be distinct"
                    ))
                })?;
            match content {
                Register::Record(record) => {
                    // Decode straight into the register; going through an owned
                    // Value would allocate for every TEXT/BLOB column on every row.
                    let mut payload_iterator = record.iter()?;
                    match payload_iterator.nth_into_register(column, dest_reg) {
                        Some(result) => result?,
                        // A pseudo cursor is opened with num_fields matching the
                        // record built for it, so every emitted Column index is in
                        // range. NULL on a missing column matches the b-tree arm.
                        None => {
                            turso_debug_assert!(
                                false,
                                "pseudo-cursor column out of range for record",
                                { "column": column }
                            );
                            dest_reg.set_null();
                        }
                    }
                }
                _ => dest_reg.set_null(),
            }
        }
        CursorType::IndexMethod(..) => {
            let cursor = state.cursors[cursor_id]
                .as_mut()
                .expect("cursor should exist");
            let cursor = cursor.as_index_method_mut();
            let value = return_if_io!(cursor.query_column(column));
            state.registers[dest].set_value(value);
        }
        CursorType::VirtualTable(_) => {
            panic!("Insn:Column on virtual table cursor, use Insn:VColumn instead");
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

fn apply_column_default(default: &Option<Value>, reg: &mut Register) -> Result<()> {
    let Some(default) = default else {
        reg.set_null();
        return Ok(());
    };
    match (default, reg) {
        (Value::Text(new_text), Register::Value(Value::Text(existing_text))) => {
            existing_text.do_extend(new_text)?;
        }
        (Value::Blob(new_blob), Register::Value(Value::Blob(existing_blob))) => {
            existing_blob.do_extend(new_blob)?;
        }
        (default, reg) => {
            reg.set_value(default.clone());
        }
    }
    Ok(())
}

fn op_column_range_fetch(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    start_column: usize,
    dest: usize,
    defaults: &[Option<Value>],
) -> InsnResult {
    let count = defaults.len();
    let Some(Cursor::BTree(cursor)) = state
        .cursors
        .get_mut(cursor_id)
        .unwrap_or_else(|| panic!("cursor id {cursor_id} out of bounds"))
    else {
        return op_column_range_fetch_other(
            program,
            state,
            cursor_id,
            start_column,
            dest,
            defaults,
        );
    };
    if cursor.get_null_flag() {
        tracing::trace!("op_column_range(null_flag)");
        for reg in &mut state.registers[dest..dest + count] {
            reg.set_null();
        }
        return Ok(InsnFunctionStepResult::Step);
    }
    let Some(record) = return_if_io!(cursor.record()) else {
        for reg in &mut state.registers[dest..dest + count] {
            reg.set_null();
        }
        return Ok(InsnFunctionStepResult::Step);
    };
    let filled = record
        .iter()?
        .decode_into_registers_after(start_column, &mut state.registers[dest..dest + count])?;
    if filled < count {
        branches::mark_unlikely();
        // The record has fewer columns than expected.
        for (default, reg) in defaults[filled..]
            .iter()
            .zip(&mut state.registers[dest + filled..dest + count])
        {
            apply_column_default(default, reg)?;
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

/// ColumnRange for every cursor that is not a b-tree cursor object: sorters,
/// pseudo cursors, and the cursors that reject range fusing.
#[inline(never)]
fn op_column_range_fetch_other(
    program: &Program,
    state: &mut ProgramState,
    cursor_id: usize,
    start_column: usize,
    dest: usize,
    defaults: &[Option<Value>],
) -> InsnResult {
    let (_, cursor_type) = program
        .cursor_ref
        .get(cursor_id)
        .expect("cursor_id should exist in cursor_ref");

    if !cursor_type.accepts_column_range_fusing() {
        crate::bail_parse_error!(
            "ColumnRange called with unsupported cursor {:?}",
            state.get_cursor(cursor_id)
        )
    }

    let count = defaults.len();
    match cursor_type {
        CursorType::BTreeTable(_)
        | CursorType::BTreeIndex(_)
        | CursorType::MaterializedView(_, _) => {
            let filled = {
                let cursor_ref =
                    must_be_btree_cursor!(cursor_id, program.cursor_ref, state, "ColumnRange");
                if matches!(cursor_ref, Cursor::NullRow) {
                    tracing::trace!("op_column_range(null_row)");
                    for reg in &mut state.registers[dest..dest + count] {
                        reg.set_null();
                    }
                    return Ok(InsnFunctionStepResult::Step);
                }
                let cursor = cursor_ref.as_btree_mut();

                if cursor.get_null_flag() {
                    tracing::trace!("op_column_range(null_flag)");
                    for reg in &mut state.registers[dest..dest + count] {
                        reg.set_null();
                    }
                    return Ok(InsnFunctionStepResult::Step);
                }

                let record_result = return_if_io!(cursor.record());
                let Some(record) = record_result else {
                    for reg in &mut state.registers[dest..dest + count] {
                        reg.set_null();
                    }
                    return Ok(InsnFunctionStepResult::Step);
                };

                record.iter()?.decode_into_registers_after(
                    start_column,
                    &mut state.registers[dest..dest + count],
                )?
            };
            if filled < count {
                branches::mark_unlikely();
                // The record has fewer columns than expected.
                for (default, reg) in defaults[filled..]
                    .iter()
                    .zip(&mut state.registers[dest + filled..dest + count])
                {
                    apply_column_default(default, reg)?;
                }
            }
        }
        CursorType::Sorter => {
            if let Some(record) = get_cursor!(state, cursor_id).as_sorter_mut().record() {
                let mut payload_iterator = record.iter()?;
                let filled = payload_iterator.decode_into_registers_after(
                    start_column,
                    &mut state.registers[dest..dest + count],
                )?;
                for (default, reg) in defaults[filled..]
                    .iter()
                    .zip(&mut state.registers[dest + filled..dest + count])
                {
                    apply_column_default(default, reg)?;
                }
            } else {
                for reg in &mut state.registers[dest..dest + count] {
                    reg.set_null();
                }
            }
        }
        CursorType::Pseudo(_) => {
            let content_reg = get_cursor!(state, cursor_id).as_pseudo_mut().content_reg();
            if content_reg >= dest && content_reg < dest + count {
                return Err(LimboError::InternalError(format!(
                    "ColumnRange: pseudo-cursor content register {content_reg} must not overlap destination registers {dest}..{}",
                    dest + count
                )).into());
            }
            let (content, dest_regs) = if content_reg < dest {
                let (left, right) = state.registers.split_at_mut(dest);
                (&mut left[content_reg], &mut right[..count])
            } else {
                let (left, right) = state.registers.split_at_mut(dest + count);
                (&mut right[content_reg - (dest + count)], &mut left[dest..])
            };
            match content {
                Register::Record(record) => {
                    let filled = record
                        .iter()?
                        .decode_into_registers_after(start_column, dest_regs)?;
                    if filled < count {
                        crate::bail_parse_error!("pseudo-cursor column out of range for record");
                    }
                }
                other => {
                    crate::bail_parse_error!("non-register register in pseudo-record ({other:?})")
                }
            }
        }
        other => {
            panic!("unexpected cursor type in op_column_range_fetch body ({other:?})")
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_column_has_field(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        ColumnHasField {
            cursor_id,
            column,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");

    let has_field = match cursor_type {
        CursorType::BTreeTable(_)
        | CursorType::BTreeIndex(_)
        | CursorType::MaterializedView(_, _) => {
            let cursor_ref =
                must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "ColumnHasField");
            if matches!(cursor_ref, Cursor::NullRow) {
                false
            } else {
                let cursor = cursor_ref.as_btree_mut();
                if cursor.get_null_flag() {
                    false
                } else {
                    match return_if_io!(cursor.record()) {
                        Some(record) => record.column_count() > *column,
                        None => false,
                    }
                }
            }
        }
        // Non-btree cursors always "have" all fields
        _ => true,
    };

    if has_field {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_type_check(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        TypeCheck {
            start_reg,
            count,
            check_generated: _,
            table_reference,
        },
        insn
    );
    assert!(table_reference.is_strict);
    state.registers[*start_reg..*start_reg + *count]
        .iter_mut()
        .zip(table_reference.columns().iter())
        .try_for_each(|(reg, col)| {
            // INT PRIMARY KEY is not row_id_alias so we throw error if this col is NULL
            if !col.is_rowid_alias() && col.primary_key() && matches!(reg.get_value(), Value::Null)
            {
                bail_constraint_error!(
                    "NOT NULL constraint failed: {}.{} ({})",
                    &table_reference.name,
                    col.name.as_deref().unwrap_or(""),
                    SQLITE_CONSTRAINT
                )
            } else if col.is_rowid_alias() && matches!(reg.get_value(), Value::Null) {
                // Handle INTEGER PRIMARY KEY for null as usual (Rowid will be auto-assigned)
                // (Turbofish pins the closure's error type; the bail macros are
                // polymorphic over boxed and unboxed LimboError.)
                return Ok::<(), LimboError>(());
            } else if matches!(reg.get_value(), Value::Null) {
                // STRICT only enforces type affinity on non-NULL values.
                // NULL is valid in any column without NOT NULL constraint.
                return Ok(());
            }
            let ty_str = &col.ty_str;
            let ty_bytes = ty_str.as_bytes();
            let is_builtin_type = turso_macros::match_ignore_ascii_case!(match ty_bytes {
                b"ANY" | b"INTEGER" | b"INT" | b"REAL" | b"BLOB" | b"TEXT" => true,
                _ => false,
            });
            if is_builtin_type {
                match_ignore_ascii_case!(match ty_bytes {
                    b"ANY" => {}
                    _ => {
                        let col_affinity = col.affinity();
                        let _applied = apply_affinity_char(reg, col_affinity);
                        let value_type = reg.get_value().value_type();
                        match_ignore_ascii_case!(match ty_bytes {
                            b"INTEGER" | b"INT" if value_type == ValueType::Integer => {}
                            b"REAL" if value_type == ValueType::Float => {}
                            b"BLOB" if value_type == ValueType::Blob => {}
                            b"TEXT" if value_type == ValueType::Text => {}
                            _ => bail_constraint_error!(
                                "cannot store {} value in {} column {}.{} ({})",
                                value_type,
                                ty_str,
                                &table_reference.name,
                                col.name.as_deref().unwrap_or(""),
                                SQLITE_CONSTRAINT
                            ),
                        });
                    }
                });
            }
            // Custom types: skip type check — encode function validates
            Ok(())
        })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Parse an array input (JSON text or record blob), validate/coerce each element
/// against the declared element type using STRICT type-checking logic, then
/// serialize to a native record-format BLOB for storage.
pub fn op_array_encode(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ArrayEncode { data }, insn);
    let ArrayEncodeData {
        reg,
        element_affinity,
        element_type,
        table_name,
        col_name,
    } = data.as_ref();

    let val = state.registers[*reg].get_value();
    if matches!(val, Value::Null) {
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    // Fast path: blob input with ANY type — validate it is a well-formed record
    // before accepting. This avoids the redundant extract→reserialize when MakeArray
    // output feeds directly into ArrayEncode with ANY affinity, but rejects raw blobs
    // (e.g. zeroblob, X'DEADBEEF') that would crash on read.
    if let Value::Blob(b) = val {
        if element_type.eq_ignore_ascii_case("ANY") {
            // Validate blob is a well-formed record using streaming iterator
            // (no Vec<Value> allocation). Rejects empty blobs and invalid records.
            let valid = !b.is_empty()
                && ValueIterator::new(b)
                    .map(|iter| iter.into_iter().all(|r| r.is_ok()))
                    .unwrap_or(false);
            if !valid {
                bail_constraint_error!(
                    "cannot store non-array value in {} column {}.{} ({})",
                    element_type,
                    table_name,
                    col_name,
                    SQLITE_CONSTRAINT
                );
            }
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    }

    // Extract elements from either blob (MakeArray) or text (JSON literal)
    let raw_elements = match val {
        Value::Blob(b) => array_values_from_blob(b).ok(),
        Value::Text(text) => parse_text_array(text.as_str()),
        _ => None,
    };
    let Some(raw_elements) = raw_elements else {
        bail_constraint_error!(
            "cannot store non-array value in {} column {}.{} ({})",
            element_type,
            table_name,
            col_name,
            SQLITE_CONSTRAINT
        );
    };

    const MAX_ARRAY_ELEMENTS: usize = 100_000;
    if raw_elements.len() > MAX_ARRAY_ELEMENTS {
        bail_constraint_error!(
            "array exceeds maximum element count ({MAX_ARRAY_ELEMENTS}) for column {table_name}.{col_name} ({SQLITE_CONSTRAINT})"
        );
    }

    let mut coerced_elements: Vec<Value> = Vec::with_capacity(raw_elements.len());
    for elem in raw_elements {
        // NULL elements are allowed — same as STRICT allows NULL in columns
        if matches!(elem, Value::Null) {
            coerced_elements.push(elem);
            continue;
        }

        // Apply affinity coercion — same as STRICT's TypeCheck
        let mut reg_tmp = Register::Value(elem);
        apply_affinity_char(&mut reg_tmp, *element_affinity);
        let coerced = reg_tmp.get_value().clone();

        // Check value type matches the declared element type — same as STRICT's TypeCheck
        let value_type = coerced.value_type();
        let ty_bytes = element_type.as_bytes();
        let type_ok = turso_macros::match_ignore_ascii_case!(match ty_bytes {
            b"ANY" => true,
            b"INTEGER" | b"INT" => value_type == ValueType::Integer,
            b"REAL" => value_type == ValueType::Float,
            b"TEXT" => value_type == ValueType::Text,
            b"BLOB" => value_type == ValueType::Blob,
            _ => true, // custom types validated by their own encode
        });

        if !type_ok {
            bail_constraint_error!(
                "cannot store {} value in {} ({})",
                value_type,
                element_type,
                SQLITE_CONSTRAINT
            );
        }

        coerced_elements.push(coerced);
    }

    // Serialize coerced elements as a native record-format BLOB
    let record = ImmutableRecord::from_values(&coerced_elements, coerced_elements.len())?;
    state.registers[*reg].set_blob(record.into_payload())?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Convert a native record-format array BLOB to PG text representation for display.
pub fn op_array_decode(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ArrayDecode { reg }, insn);

    let val = state.registers[*reg].get_value();
    if matches!(val, Value::Null) {
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let text = match val {
        Value::Blob(b) if b.is_empty() => "{}".to_string(),
        Value::Blob(b) => serialize_array_from_blob(b)?,
        _ => {
            // Not a blob — leave as-is (might be text from a function result)
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    };
    state.registers[*reg].set_text(Text::new(text))?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Access element at index from a record-format array BLOB.
pub fn op_array_element(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        ArrayElement {
            array_reg,
            index_reg,
            dest,
        },
        insn
    );

    let arr_val = state.registers[*array_reg].get_value();
    if matches!(arr_val, Value::Null) {
        state.registers[*dest].set_null();
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let idx = match state.registers[*index_reg].get_value() {
        Value::Numeric(Numeric::Integer(i)) if *i >= 1 => (*i - 1) as usize,
        _ => {
            // Non-positive, non-integer, or NULL index → NULL result (PG convention: 1-based)
            state.registers[*dest].set_null();
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    };

    let result = match arr_val {
        Value::Blob(blob) => match ValueIterator::new(blob) {
            Ok(mut iter) => iter
                .nth(idx)
                .transpose()?
                .map(|vref| vref.to_owned())
                .transpose()?
                .unwrap_or(Value::Null),
            Err(_) => Value::Null,
        },
        _ => Value::Null,
    };

    state.registers[*dest].set_value(result);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Get the number of elements in a record-format array BLOB.
pub fn op_array_length(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ArrayLength { reg, dest }, insn);

    let val = state.registers[*reg].get_value();
    match compute_array_length(val) {
        Some(count) => state.registers[*dest].set_int(count),
        None => state.registers[*dest].set_null(),
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Create an array from contiguous registers as a record-format BLOB.
pub fn op_make_array(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        MakeArray {
            start_reg,
            count,
            dest
        },
        insn
    );

    let end = start_reg
        .checked_add(*count)
        .ok_or_else(|| LimboError::InternalError("MakeArray: register range overflow".into()))?;
    if end > state.registers.len() {
        return Err(LimboError::InternalError(format!(
            "MakeArray: register range {}..{} exceeds register file size {}",
            start_reg,
            end,
            state.registers.len()
        ))
        .into());
    }
    state.registers[*dest].set_value(make_array_from_registers(
        &state.registers,
        *start_reg,
        *count,
    )?);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Create an array from contiguous registers with dynamic count.
pub fn op_make_array_dynamic(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        MakeArrayDynamic {
            start_reg,
            count_reg,
            dest
        },
        insn
    );

    let count = match state.registers[*count_reg].get_value() {
        Value::Numeric(Numeric::Integer(n)) if *n >= 0 => *n as usize,
        _ => 0,
    };

    let end = start_reg.checked_add(count).ok_or_else(|| {
        LimboError::InternalError("MakeArrayDynamic: register range overflow".into())
    })?;
    if end > state.registers.len() {
        return Err(LimboError::InternalError(format!(
            "MakeArrayDynamic: register range {}..{} exceeds register file size {}",
            start_reg,
            end,
            state.registers.len()
        ))
        .into());
    }

    state.registers[*dest].set_value(make_array_from_registers(
        &state.registers,
        *start_reg,
        count,
    )?);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Split the register file to get a shared reference to `src` and a mutable
/// reference to `dst`. Panics if `src == dst`.
#[inline]
/// Extract a field from a struct blob by field index.
pub fn op_struct_field(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        StructField {
            src_reg,
            field_index,
            dest,
        },
        insn
    );

    let (src, dst) = super::split_registers(&mut state.registers, *src_reg, *dest);

    match src.get_value() {
        Value::Blob(blob) => {
            let mut iter = ValueIterator::new(blob)?;
            match iter.nth_into_register(*field_index, dst) {
                Some(result) => result?,
                None => dst.set_null(),
            }
        }
        _ => dst.set_null(),
    };

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Pack a tag name and a value into a union blob.
/// Format: [tag_name_len: 1 byte][tag_name: N bytes][record-format value]
/// The tag name is embedded directly in the instruction as a String.
pub fn op_union_pack(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        UnionPack {
            tag_index,
            value_reg,
            dest,
        },
        insn
    );

    let record =
        ImmutableRecord::from_registers(std::slice::from_ref(&state.registers[*value_reg]), 1)?;
    let record_bytes = record.into_payload();

    // Format: [tag_index: 1 byte][record bytes]
    let mut blob = <crate::ValueBlob as TursoVecExt<u8>>::with_capacity(1 + record_bytes.len());
    blob.push(*tag_index);
    blob.extend_from_slice(&record_bytes);
    state.registers[*dest].set_value(Value::Blob(blob));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Extract the tag name from a union blob as text.
/// Format: [tag_index: 1 byte][record-format value]
/// Looks up tag_index in tag_names to return the variant name.
pub fn op_union_tag(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        UnionTag {
            src_reg,
            dest,
            tag_names
        },
        insn
    );

    let val = state.registers[*src_reg].get_value();
    let result = match val {
        Value::Blob(blob) if !blob.is_empty() => {
            let tag_index = blob[0] as usize;
            debug_assert!(
                tag_index < tag_names.len(),
                "union tag index {tag_index} out of range (len={})",
                tag_names.len()
            );
            match tag_names.get(tag_index) {
                Some(name) => Value::build_text(name.clone()),
                None => Value::Null,
            }
        }
        _ => Value::Null,
    };

    state.registers[*dest].set_value(result);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Extract the value from a union blob if the tag name matches.
pub fn op_union_extract(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        UnionExtract {
            src_reg,
            expected_tag,
            dest,
        },
        insn
    );

    // First pass: check tag index and compute record slice bounds (immutable borrow only)
    let record_range = match state.registers[*src_reg].get_value() {
        Value::Null => {
            state.registers[*dest].set_null();
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
        Value::Blob(blob) if !blob.is_empty() => {
            if blob[0] == *expected_tag && blob.len() > 1 {
                Some(1..blob.len())
            } else {
                None
            }
        }
        _ => None,
    };

    // Second pass: extract value using nth_into_register (needs split mutable borrow)
    match record_range {
        Some(range) => {
            let (src, dst) = super::split_registers(&mut state.registers, *src_reg, *dest);

            if let Value::Blob(blob) = src.get_value() {
                let mut iter = ValueIterator::new(&blob[range])?;
                match iter.nth_into_register(0, dst) {
                    Some(result) => result?,
                    None => dst.set_null(),
                }
            } else {
                dst.set_null();
            }
        }
        None => state.registers[*dest].set_null(),
    };

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Copy a register value to a dynamically-computed destination register.
pub fn op_reg_copy_offset(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        RegCopyOffset {
            src,
            base,
            offset_reg
        },
        insn
    );

    let offset = match state.registers[*offset_reg].get_value() {
        Value::Numeric(Numeric::Integer(n)) if *n >= 0 => *n as usize,
        _ => 0,
    };
    let dest = *base + offset;
    if dest >= state.registers.len() {
        return Err(LimboError::InternalError(format!(
            "RegCopyOffset: destination register {} out of bounds (max {})",
            dest,
            state.registers.len()
        ))
        .into());
    }
    if dest != *src {
        // try_clone_from reuses the destination register's allocation.
        let [src, dst] = state
            .registers
            .get_disjoint_mut([*src, dest])
            .expect("RegCopyOffset source and destination registers are distinct");
        dst.try_clone_from(src)?;
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Concatenate/append/prepend arrays. Runtime dispatch based on operand types.
pub fn op_array_concat(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ArrayConcat { lhs, rhs, dest }, insn);

    // Check NULL before cloning to avoid unnecessary allocation
    let lhs_ref = state.registers[*lhs].get_value();
    let rhs_ref = state.registers[*rhs].get_value();

    // PG-compatible NULL handling for arrays:
    // array || NULL = array, NULL || array = array, NULL || NULL = NULL
    if matches!(lhs_ref, Value::Null) && matches!(rhs_ref, Value::Null) {
        state.registers[*dest].set_null();
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    if matches!(lhs_ref, Value::Null) {
        state.registers[*dest].set_value(rhs_ref.clone());
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    if matches!(rhs_ref, Value::Null) {
        state.registers[*dest].set_value(lhs_ref.clone());
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let result = match (lhs_ref, rhs_ref) {
        (Value::Blob(lb), Value::Blob(rb)) => {
            let mut elems_a = array_values_from_blob(lb)?;
            let elems_b = array_values_from_blob(rb)?;
            elems_a.extend(elems_b);
            values_to_record_blob(&elems_a)?
        }
        (Value::Blob(lb), _) => {
            let mut elems = array_values_from_blob(lb)?;
            elems.push(rhs_ref.clone());
            values_to_record_blob(&elems)?
        }
        (_, Value::Blob(rb)) => {
            let mut elems = array_values_from_blob(rb)?;
            elems.insert(0, lhs_ref.clone());
            values_to_record_blob(&elems)?
        }
        _ => {
            // Neither is an array blob — fall back to string concat
            Value::build_text(format!("{lhs_ref}{rhs_ref}"))
        }
    };

    state.registers[*dest].set_value(result);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Set element at index in a record-format array BLOB.
pub fn op_array_set_element(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        ArraySetElement {
            array_reg,
            index_reg,
            value_reg,
            dest,
        },
        insn
    );

    let arr_val = state.registers[*array_reg].get_value();
    if matches!(arr_val, Value::Null) {
        state.registers[*dest].set_null();
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let idx = match state.registers[*index_reg].get_value() {
        Value::Numeric(Numeric::Integer(i)) if *i >= 1 => (*i - 1) as usize,
        _ => {
            // Invalid index (non-positive, non-integer): preserve original array (PG: 1-based)
            state.registers[*dest].set_value(arr_val.clone());
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    };

    let new_val = state.registers[*value_reg].get_value().clone();

    let Value::Blob(blob) = arr_val else {
        return Err(
            LimboError::InternalError("ArraySetElement: expected blob array".into()).into(),
        );
    };
    let mut elements = array_values_from_blob(blob)?;
    if idx >= elements.len() {
        // Out-of-bounds: preserve original array unchanged
        state.registers[*dest].set_blob(blob.clone())?;
    } else {
        elements[idx] = new_val;
        state.registers[*dest].set_value(values_to_record_blob(&elements)?);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Extract a subslice of elements from a record-format array BLOB.
pub fn op_array_slice(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        ArraySlice {
            array_reg,
            start_reg,
            end_reg,
            dest,
        },
        insn
    );

    let arr_val = state.registers[*array_reg].get_value().clone();
    let start_val = state.registers[*start_reg].get_value().clone();
    let end_val = state.registers[*end_reg].get_value().clone();

    let result = exec_array_slice(&arr_val, &start_val, &end_val)?;
    state.registers[*dest].set_value(result);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_make_record(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        MakeRecord {
            start_reg,
            count,
            dest_reg,
            affinity_str,
            ..
        },
        insn
    );

    let start_reg = *start_reg as usize;
    let count = *count as usize;
    let dest_reg = *dest_reg as usize;

    if let Some(affinity_str) = affinity_str {
        if unlikely(affinity_str.len() != count) {
            return Err(LimboError::InternalError(format!(
                "MakeRecord: the length of affinity string ({}) does not match the count ({})",
                affinity_str.len(),
                count
            ))
            .into());
        }
        for (i, affinity_ch) in affinity_str.chars().enumerate().take(count) {
            let reg_index = start_reg + i;
            let affinity = Affinity::from_char(affinity_ch);
            apply_affinity_char(&mut state.registers[reg_index], affinity);
        }
    }

    if dest_reg >= start_reg && dest_reg - start_reg < count {
        return Err(LimboError::InternalError(format!(
            "MakeRecord: destination register {dest_reg} overlaps its source range {start_reg}..{}",
            start_reg + count
        ))
        .into());
    }

    // Serialize into the destination register's spent record buffer: per-row
    // paths become allocation-free once the buffer has grown to the row size.
    let buf = state.registers[dest_reg].take_buf();
    let regs = &state.registers[start_reg..start_reg + count];
    let record = ImmutableRecord::build_from_registers(regs, buf)?;
    state.registers[dest_reg] = Register::Record(record);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_mem_max(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(MemMax { dest_reg, src_reg }, insn);

    let dest_val = state.registers[*dest_reg].get_value();
    let src_val = state.registers[*src_reg].get_value();

    let dest_int = extract_int_value(dest_val);
    let src_int = extract_int_value(src_val);

    if dest_int < src_int {
        state.registers[*dest_reg].set_int(src_int);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Read a non-negative integer offset/length from a register for the blob opcodes.
fn blob_reg_usize(state: &ProgramState, reg: usize, opcode: &str, what: &str) -> Result<usize> {
    match state.registers[reg].get_value() {
        Value::Numeric(Numeric::Integer(n)) if *n >= 0 => Ok(*n as usize),
        other => Err(LimboError::InternalError(format!(
            "{opcode}: {what} register must hold a non-negative integer, got {other:?}"
        ))),
    }
}

/// Resolve `cursor` to the b-tree cursor the blob opcodes require, or error. Only
/// table-row cursors expose byte-level column access, so this is the single gate all
/// three opcodes (BlobRead/BlobWrite/BlobLen) share.
fn blob_btree_cursor<'a>(
    state: &'a mut ProgramState,
    cursor: usize,
    opcode: &str,
) -> Result<&'a mut dyn crate::storage::btree::CursorTrait> {
    match state.get_cursor(cursor) {
        Cursor::BTree(btree) => Ok(btree.as_mut()),
        _ => Err(LimboError::InternalError(format!(
            "{opcode} requires a b-tree cursor"
        ))),
    }
}

/// Signal incremental-blob expiry to `Blob::read`/`write`: store NULL in `dest` and
/// advance. Expiry must fail the operation without aborting the program — the paused
/// blob program owns the handle's transaction, and writes made before the expiry
/// must still commit at close (sqlite3_blob semantics). Shared by BlobRead/BlobWrite.
fn blob_expired_ack(state: &mut ProgramState, dest: usize) -> InsnFunctionStepResult {
    state.registers[dest].set_value(Value::Null);
    state.pc += 1;
    InsnFunctionStepResult::Step
}

pub fn op_blob_read(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        BlobRead {
            cursor,
            column,
            offset,
            amount,
            dest
        },
        insn
    );
    let off = blob_reg_usize(state, *offset, "BlobRead", "offset")?;
    let amt = blob_reg_usize(state, *amount, "BlobRead", "amount")?;
    // The callee validates (off, amt) against the value's length before sizing `out`,
    // so a hostile amount register cannot force an allocation here.
    let mut out: crate::ValueBlob = TursoAllocExt::new();
    match blob_btree_cursor(state, *cursor, "BlobRead")?
        .blob_read_column(*column, off, amt, &mut out)
    {
        Ok(IOResult::Done(())) => {}
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
        Err(err) if matches!(*err, LimboError::BlobHandleExpired) => {
            return Ok(blob_expired_ack(state, *dest))
        }
        Err(err) => return Err(err),
    }
    state.registers[*dest].set_value(Value::Blob(out));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Store the byte length of a column's value on the cursor's current row, validating
/// that the value is byte-addressable (TEXT or BLOB) — the VDBE backing for
/// sqlite3_blob_open's length and type check.
pub fn op_blob_len(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        BlobLen {
            cursor,
            column,
            dest
        },
        insn
    );
    let len = return_if_io!(blob_btree_cursor(state, *cursor, "BlobLen")?.blob_column_len(*column));
    let len = i64::try_from(len)
        .map_err(|_| LimboError::Corrupt("column byte length does not fit in i64".to_string()))?;
    state.registers[*dest].set_value(Value::Numeric(Numeric::Integer(len)));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Write a blob's bytes into a column's value at an offset, in place — the VDBE
/// backing for sqlite3_blob_write.
pub fn op_blob_write(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        BlobWrite {
            cursor,
            column,
            offset,
            src,
            dest
        },
        insn
    );
    let off = blob_reg_usize(state, *offset, "BlobWrite", "offset")?;
    let data = match state.registers[*src].get_value() {
        Value::Blob(b) => b.clone(),
        other => {
            return Err(LimboError::InternalError(format!(
                "BlobWrite: source register must hold a blob, got {other:?}"
            ))
            .into());
        }
    };
    match blob_btree_cursor(state, *cursor, "BlobWrite")?.blob_write_column(*column, off, &data) {
        Ok(IOResult::Done(())) => {}
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
        Err(err) if matches!(*err, LimboError::BlobHandleExpired) => {
            return Ok(blob_expired_ack(state, *dest))
        }
        Err(err) => return Err(err),
    }
    state.registers[*dest].set_value(Value::Numeric(Numeric::Integer(1)));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

// Not in test builds: inline(always) makes fn-item coercions produce
// per-site copies in debug, breaking test_make_sure_correct_insn_table's
// pointer-identity check.
#[cfg_attr(not(test), inline(always))]
pub fn op_result_row(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ResultRow { start_reg, count }, insn);
    let row = Row {
        values: &state.registers[*start_reg] as *const Register,
        count: *count,
    };
    state.result_row = Some(row);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Row)
}

// Not in test builds: inline(always) makes fn-item coercions produce
// per-site copies in debug, breaking test_make_sure_correct_insn_table's
// pointer-identity check.
#[cfg_attr(not(test), inline(always))]
pub fn op_next(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Next {
            cursor_id,
            pc_if_next,
            fullscan,
        },
        insn
    );
    let is_empty = {
        let cursor = state.get_cursor(*cursor_id);
        match cursor {
            Cursor::BTree(btree_cursor) => !return_if_io!(btree_cursor.next_row()),
            Cursor::MaterializedView(mv_cursor) => {
                let has_more = return_if_io!(mv_cursor.next());
                !has_more
            }
            Cursor::IndexMethod(_) => {
                let cursor = cursor.as_index_method_mut();
                let has_more = return_if_io!(cursor.query_next());
                !has_more
            }
            _ => panic!("Next on non-btree/materialized-view cursor"),
        }
    };
    if !is_empty {
        // Increment metrics for row read
        state.record_rows_read(1);
        state.metrics.btree_next = state.metrics.btree_next.wrapping_add(1);
        state.metrics.search_count = state.metrics.search_count.wrapping_add(1);
        // Only steps codegen marked as part of a full table scan count as
        // fullscan steps, matching SQLITE_STMTSTATUS_FULLSCAN_STEP.
        if *fullscan {
            state.metrics.fullscan_steps = state.metrics.fullscan_steps.wrapping_add(1);
        }
        if let Some((_, cursor_type)) = program.cursor_ref.get(*cursor_id) {
            if cursor_type.is_index() {
                state.metrics.index_steps = state.metrics.index_steps.wrapping_add(1);
            }
        }
        state.pc = pc_if_next.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_prev(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Prev {
            cursor_id,
            pc_if_prev,
            fullscan,
        },
        insn
    );
    let is_empty = {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Prev");
        !return_if_io!(cursor.as_btree_mut().prev_row())
    };
    if !is_empty {
        // Increment metrics for row read
        state.record_rows_read(1);
        state.metrics.btree_prev = state.metrics.btree_prev.wrapping_add(1);
        state.metrics.search_count = state.metrics.search_count.wrapping_add(1);
        // Only steps codegen marked as part of a full table scan count as
        // fullscan steps, matching SQLITE_STMTSTATUS_FULLSCAN_STEP.
        if *fullscan {
            state.metrics.fullscan_steps = state.metrics.fullscan_steps.wrapping_add(1);
        }
        if let Some((_, cursor_type)) = program.cursor_ref.get(*cursor_id) {
            if cursor_type.is_index() {
                state.metrics.index_steps = state.metrics.index_steps.wrapping_add(1);
            }
        }
        state.pc = pc_if_prev.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn halt(
    program: &Program,
    state: &mut ProgramState,
    pager: &Arc<Pager>,
    err_code: usize,
    description: &str,
    on_error: Option<ResolveType>,
) -> InsnResult {
    state.halt_in_progress = true;
    let mv_store = program.connection.mv_store();
    let auto_commit = program.connection.auto_commit.load(Ordering::SeqCst);
    // halt() runs while the statement is still stepping, so it is always
    // counted in n_active_root_statements here.
    let can_autocommit_now = state.can_autocommit_now(&program.connection, true);

    // Check if we're resuming from a FAIL commit I/O wait.
    // If pending_fail_error is set, we were in the middle of committing partial changes
    // for FAIL mode and need to continue the commit, then return the stored error.
    if let Some(pending_error) = state.pending_fail_error.take() {
        match program.commit_txn(pager.clone(), state, mv_store.as_ref(), false)? {
            IOResult::Done(_) => {
                index_method_on_transaction_committed_all(state, &program.connection);
                return Err(pending_error.into());
            }
            IOResult::IO(io) => {
                state.pending_fail_error = Some(pending_error); // put it back and wait
                return Ok(InsnFunctionStepResult::IO(io));
            }
        }
    }

    if err_code > 0 {
        vtab_rollback_all(&program.connection)?;
    }

    // Handle RAISE errors - these carry their own resolve type
    if let Some(resolve_type) = on_error {
        // RAISE(IGNORE) signals the parent to skip the current row
        if resolve_type == ResolveType::Ignore {
            // RAISE(IGNORE) is not an error: the frame unwinds, but everything
            // the trigger wrote before the RAISE is kept. Stage index-method
            // writes now, exactly like the normal trigger-subprogram halt
            // below, so the kept base rows keep their index entries too.
            if program.is_trigger_subprogram() {
                return_if_io!(index_method_stage_statement_all(state));
            }
            return Err(LimboError::RaiseIgnore.into());
        }
        if err_code > 0 {
            let error = match resolve_type {
                ResolveType::Abort | ResolveType::Rollback | ResolveType::Fail => {
                    LimboError::Raise(resolve_type, description.to_string())
                }
                ResolveType::Ignore => unreachable!("handled above"),
                ResolveType::Replace => unreachable!("Replace not valid for RAISE"),
            };

            // Trigger subprograms must not commit — just propagate the error.
            // The parent program's abort() handles transaction state.
            if program.is_trigger_subprogram() {
                return Err(error.into());
            }

            return Err(error.into());
        }
    }

    // Determine the constraint error (if any) based on error code
    let constraint_error = match err_code {
        0 => None,
        SQLITE_CONSTRAINT_PRIMARYKEY => Some(LimboError::Constraint(format!(
            "UNIQUE constraint failed: {description}"
        ))),
        SQLITE_CONSTRAINT_CHECK => Some(LimboError::Constraint(format!(
            "CHECK constraint failed: {description}"
        ))),
        SQLITE_CONSTRAINT_NOTNULL => Some(LimboError::Constraint(format!(
            "NOT NULL constraint failed: {description}"
        ))),
        SQLITE_CONSTRAINT_UNIQUE => Some(LimboError::Constraint(format!(
            "UNIQUE constraint failed: {description}"
        ))),
        SQLITE_CONSTRAINT_FOREIGNKEY => {
            Some(LimboError::ForeignKeyConstraint(description.to_string()))
        }
        SQLITE_CONSTRAINT_TRIGGER => Some(LimboError::Constraint(description.to_string())),
        SQLITE_FULL => Some(LimboError::DatabaseFull(description.to_string())),
        // SQLITE_ERROR is a generic error (e.g. ALTER TABLE validation), not a constraint.
        // SqlError displays bare like sqlite3_errmsg and abort() doesn't apply
        // ON CONFLICT resolution to it.
        SQLITE_ERROR => Some(LimboError::SqlError(description.to_string())),
        _ => Some(LimboError::Constraint(format!(
            "undocumented halt error code {description}"
        ))),
    };

    // Handle constraint errors
    if let Some(error) = constraint_error {
        // For FAIL mode with autocommit, commit partial changes before returning error.
        // This matches SQLite behavior where FAIL keeps changes made before the error.
        // Note: ON CONFLICT FAIL does NOT apply to FK violations, so we check for those first.
        if program.resolve_type == ResolveType::Fail && can_autocommit_now {
            // Check for immediate FK violations - FK errors don't respect ON CONFLICT
            if program.connection.foreign_keys_enabled()
                && state.get_fk_immediate_violations_during_stmt() > 0
            {
                return Err(LimboError::ForeignKeyConstraint(
                    "immediate foreign key constraint failed".to_string(),
                )
                .into());
            }

            // Stage every custom index before releasing the statement
            // savepoint, then commit the partial FAIL changes. Yielding for
            // I/O here is safe because Halt is re-entrant: the whole opcode
            // runs again on resume, recomputes the same outcome from the same
            // registers, and `pending_fail_error` carries the decided error
            // across resumes further down. `halt_in_progress` keeps
            // interrupt, deadline, and progress-handler requests from
            // replacing the outcome while the staged work is in flight.
            vtab_commit_all(&program.connection)?;
            return_if_io!(index_method_stage_statement_all(state));
            state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;

            // Commit the transaction with partial changes
            match program.commit_txn(pager.clone(), state, mv_store.as_ref(), false)? {
                IOResult::Done(_) => {
                    index_method_on_transaction_committed_all(state, &program.connection);
                    return Err(error.into());
                }
                IOResult::IO(io) => {
                    // store the error for reentrancy
                    state.pending_fail_error = Some(error);
                    return Ok(InsnFunctionStepResult::IO(io));
                }
            }
        }

        // A FAIL resolution that cannot commit here because an outer statement
        // is still running (this is a trigger subprogram fired mid-statement)
        // must still tell the enclosing statement to keep the changes made
        // before the error. Re-tag the constraint as a FAIL raise so the outer
        // program's abort() preserves and commits them instead of rolling the
        // statement back. FK errors do not respect ON CONFLICT, so leave them
        // as-is.
        if program.resolve_type == ResolveType::Fail {
            if let LimboError::Constraint(msg) = error {
                return Err(LimboError::Raise(ResolveType::Fail, msg).into());
            }
        }

        // For non-FAIL modes (or non-autocommit), just return the error.
        // abort() will handle rollback based on resolve_type.
        return Err(error.into());
    }

    tracing::trace!(
        "halt(auto_commit={}, can_autocommit_now={})",
        auto_commit,
        can_autocommit_now
    );

    // Check for immediate foreign key violations.
    // Any immediate violation causes the statement subtransaction to roll back.
    if program.connection.foreign_keys_enabled()
        && state.get_fk_immediate_violations_during_stmt() > 0
    {
        return Err(LimboError::ForeignKeyConstraint(
            "immediate foreign key constraint failed".to_string(),
        )
        .into());
    }

    if program.is_trigger_subprogram() {
        // Trigger statements are cached and reset before the next firing.
        // Stage their index-method writes now so reset cannot drop pending
        // work from an earlier row in the parent statement. The parent still
        // owns the statement savepoint and the final transaction outcome.
        return_if_io!(index_method_stage_statement_all(state));
        return Ok(InsnFunctionStepResult::Done);
    }

    if auto_commit {
        // In autocommit mode, a statement that leaves deferred violations must fail here,
        // and it also ends the transaction.
        if can_autocommit_now && program.connection.foreign_keys_enabled() {
            let deferred_violations = program
                .connection
                .fk_deferred_violations
                .swap(0, Ordering::AcqRel);
            if deferred_violations > 0 {
                vtab_rollback_all(&program.connection)?;
                if let Some(mv_store) = mv_store.as_ref() {
                    if let Some(tx_id) = program.connection.get_mv_tx_id() {
                        mv_store.rollback_tx(tx_id, pager.clone(), &program.connection, MAIN_DB_ID);
                    }
                    pager.end_read_tx();
                } else {
                    pager.rollback_tx(&program.connection);
                }
                program.connection.set_tx_state(TransactionState::None);
                return Err(LimboError::ForeignKeyConstraint(
                    "deferred foreign key constraint failed".to_string(),
                )
                .into());
            }
        }
        if can_autocommit_now {
            vtab_commit_all(&program.connection)?;
            return_if_io!(index_method_stage_statement_all(state));
            state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;
            // Sequence backing-table compaction and sqlite_sequence sync
            // are emitted as straight-line bytecode inside the
            // nextval / advance_past / setval translators
            // (`core/translate/sequence.rs`,
            // `core/translate/expr/functions.rs`). By the time we reach
            // the commit the backing tables are already single-row and
            // `sqlite_sequence` is already in sync, so the commit does
            // not invoke a sequence flush — keeping op_halt within the
            // vdbe async contract.
            let result = program
                .commit_txn(pager.clone(), state, mv_store.as_ref(), false)
                .map(Into::into);
            // Apply deferred CDC state and reset CDC txn ID after successful commit
            if matches!(result, Ok(InsnFunctionStepResult::Done)) {
                index_method_on_transaction_committed_all(state, &program.connection);
                if let Some(cdc_info) = state.pending_cdc_info.take() {
                    program.connection.set_capture_data_changes_info(cdc_info);
                }
                program.connection.set_cdc_transaction_id(-1);
            }
            result
        } else {
            // Another root statement may own the implicit autocommit
            // transaction. This statement can finish, but must not commit or
            // roll back connection-level state it did not start. Its
            // index-method writes must still be staged before the statement
            // savepoint is released, and its cursors handed to the connection
            // so the sibling that eventually commits (or rolls back) the
            // shared transaction delivers their outcome.
            return_if_io!(index_method_stage_statement_all(state));
            state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;
            index_method_register_transaction_all(state, &program.connection)?;
            //
            // FIXME: when this statement is a writer in MVCC mode, deferring
            // its commit to the last sibling statement means the writer's
            // caller observes success before the changes are durable. If the
            // shared transaction then ends in a rollback — the last sibling
            // reader is reset or dropped mid-scan, or a later joining writer
            // errors or is abandoned after changing rows — the finished
            // writer's changes are silently discarded
            // (see test_mvcc_completed_writer_changes_lost_when_last_reader_abandoned).
            // SQLite never has this window: it commits at the writer's own
            // halt and downgrades the transaction to read-only for the
            // remaining statements (btreeEndTransaction). The MVCC equivalent
            // is to commit here, allocating a successor read-only
            // transaction's begin timestamp inside the same
            // `get_commit_timestamp` critical section as this commit's end
            // timestamp, swapping `mv_tx` to that successor so remaining
            // readers keep an identical snapshot. That also attributes commit
            // errors (e.g. WriteWriteConflict) to the writer's own step
            // instead of whichever sibling happens to finish last.
            if let Some(cdc_info) = state.pending_cdc_info.take() {
                program.connection.set_capture_data_changes_info(cdc_info);
            }
            if program.change_cnt_on {
                program
                    .connection
                    .set_changes(state.n_change.load(Ordering::SeqCst));
                program
                    .connection
                    .add_total_changes(state.n_total_change.load(Ordering::SeqCst));
            }
            Ok(InsnFunctionStepResult::Done)
        }
    } else {
        // Even if deferred violations are present, the statement subtransaction completes successfully when
        // it is part of an interactive transaction.
        //
        // Drain index-method state before releasing the statement savepoint.
        // Otherwise the cursor's Drop fallback performs these writes after
        // statement success, where an I/O error can no longer be returned to
        // the caller or rolled back at statement scope.
        return_if_io!(index_method_stage_statement_all(state));
        state.end_statement(&program.connection, pager, EndStatement::ReleaseSavepoint)?;
        index_method_register_transaction_all(state, &program.connection)?;
        // Apply deferred CDC state after successful statement completion
        if let Some(cdc_info) = state.pending_cdc_info.take() {
            program.connection.set_capture_data_changes_info(cdc_info);
        }
        if program.change_cnt_on {
            program
                .connection
                .set_changes(state.n_change.load(Ordering::SeqCst));
            program
                .connection
                .add_total_changes(state.n_total_change.load(Ordering::SeqCst));
        }
        Ok(InsnFunctionStepResult::Done)
    }
}

/// Call xCommit on all virtual tables that participated in the current transaction.
pub(crate) fn vtab_commit_all(conn: &Connection) -> crate::Result<()> {
    let mut set = conn.vtab_txn_states.write();
    if set.is_empty() {
        return Ok(());
    }
    let reg = &conn.syms.read().vtabs;
    for id in set.drain() {
        let vtab = reg
            .iter()
            .find(|(_, vtab)| vtab.id() == id)
            .expect("vtab must exist");
        vtab.1.commit()?;
    }
    Ok(())
}

/// Stage pending writes on every index-method cursor before releasing the
/// statement savepoint. Cursor order is bytecode cursor order, which is stable
/// across resumptions and avoids attachment-order ambiguity.
pub(crate) fn index_method_stage_statement_all(state: &mut ProgramState) -> IOResultOr<()> {
    if state.index_methods_finalized {
        return Ok(IOResult::Done(()));
    }

    while state.index_method_finalize_cursor < state.cursors.len() {
        let cursor_id = state.index_method_finalize_cursor;
        if matches!(
            state.cursors[cursor_id].as_ref(),
            Some(Cursor::IndexMethod(_))
        ) {
            let Some(context) = state.index_method_contexts[cursor_id].clone() else {
                return Err(LimboError::InternalError(format!(
                    "index-method cursor {cursor_id} has no execution context"
                ))
                .into());
            };
            crate::mvcc::yield_points::inject_io_yield!(
                context,
                crate::index_method::IndexMethodYieldPoint::BeforePrepareStatement
            );
            let Some(Cursor::IndexMethod(cursor)) = state.cursors[cursor_id].as_mut() else {
                unreachable!("cursor kind checked above")
            };
            match cursor.stage_statement_commit(&context)? {
                IOResult::Done(()) => {}
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
            state.index_method_finalize_cursor += 1;
            crate::mvcc::yield_points::inject_io_yield!(
                context,
                crate::index_method::IndexMethodYieldPoint::AfterPrepareStatement
            );
            continue;
        }
        state.index_method_finalize_cursor += 1;
    }

    let subprogram_keys = state
        .index_method_finalize_subprogram_keys
        .get_or_insert_with(|| {
            let mut keys = state
                .subprogram_stmt_cache
                .keys()
                .copied()
                .collect::<Vec<_>>();
            keys.sort_unstable();
            keys
        });
    while state.index_method_finalize_subprogram < subprogram_keys.len() {
        let key = subprogram_keys[state.index_method_finalize_subprogram];
        let statement = state
            .subprogram_stmt_cache
            .get_mut(&key)
            .expect("finalization key must reference a cached subprogram");
        match statement.prepare_index_methods()? {
            IOResult::Done(()) => state.index_method_finalize_subprogram += 1,
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
        }
    }

    state.index_methods_finalized = true;
    Ok(IOResult::Done(()))
}

/// Discard statement-owned index-method work before a statement savepoint or
/// transaction is rolled back. Hooks are deliberately infallible and may not
/// perform I/O.
pub(crate) fn index_method_abort_statement_all(state: &mut ProgramState) {
    for (cursor_id, cursor_opt) in state.cursors.iter_mut().enumerate() {
        let Some(Cursor::IndexMethod(cursor)) = cursor_opt else {
            continue;
        };
        let Some(context) = state.index_method_contexts[cursor_id].as_ref() else {
            // Every opcode that installs an index-method cursor builds its
            // context first, so this cursor cannot have pending work.
            debug_assert!(
                false,
                "index-method cursor {cursor_id} installed without a context"
            );
            continue;
        };
        cursor.abort_statement(context);
    }
    for (cursor, context) in &mut state.closed_index_method_cursors {
        cursor.abort_statement(context);
    }
    for statement in state.subprogram_stmt_cache.values_mut() {
        statement.abort_index_methods();
    }
    state.index_method_finalize_cursor = 0;
    state.index_method_finalize_subprogram_keys = None;
    state.index_method_finalize_subprogram = 0;
    state.index_methods_finalized = false;
}

/// Publish safe in-memory state after a durable autocommit outcome.
pub(crate) fn index_method_on_transaction_committed_all(
    state: &mut ProgramState,
    connection: &Connection,
) {
    tracing::trace!(
        open_cursors = state
            .cursors
            .iter()
            .filter(|cursor| matches!(cursor, Some(Cursor::IndexMethod(_))))
            .count(),
        closed_cursors = state.closed_index_method_cursors.len(),
        subprograms = state.subprogram_stmt_cache.len(),
        "publishing committed index-method state"
    );
    for (cursor_id, cursor_opt) in state.cursors.iter_mut().enumerate() {
        let Some(Cursor::IndexMethod(cursor)) = cursor_opt else {
            continue;
        };
        let Some(context) = state.index_method_contexts[cursor_id].as_ref() else {
            // Every opcode that installs an index-method cursor builds its
            // context first, so this cursor cannot have pending work.
            debug_assert!(
                false,
                "index-method cursor {cursor_id} installed without a context"
            );
            continue;
        };
        cursor.on_transaction_committed(context);
    }
    for (cursor, context) in &mut state.closed_index_method_cursors {
        cursor.on_transaction_committed(context);
    }
    for statement in state.subprogram_stmt_cache.values_mut() {
        statement.commit_index_methods();
    }
    connection.index_methods_on_transaction_committed();
}

/// Transfer the final prepared cursor for every attachment touched by this
/// statement into connection-level transaction ownership. Replacing an older
/// cursor for the same attachment keeps outcome delivery exactly once while
/// retaining the newest in-transaction view.
pub(crate) fn index_method_register_transaction_all(
    state: &mut ProgramState,
    connection: &Connection,
) -> crate::Result<()> {
    let mut registered = 0usize;
    for cursor_id in 0..state.cursors.len() {
        if !matches!(state.cursors[cursor_id], Some(Cursor::IndexMethod(_))) {
            continue;
        }
        let Some(Cursor::IndexMethod(cursor)) = state.cursors[cursor_id].take() else {
            unreachable!("cursor kind checked above")
        };
        let context = state.index_method_contexts[cursor_id]
            .take()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "index-method cursor {cursor_id} has no execution context"
                ))
            })?;
        connection.register_index_method_transaction_cursor(
            crate::index_method::TransactionIndexMethodCursor { cursor, context },
        );
        registered += 1;
    }
    for (cursor, context) in state.closed_index_method_cursors.drain(..) {
        connection.register_index_method_transaction_cursor(
            crate::index_method::TransactionIndexMethodCursor { cursor, context },
        );
        registered += 1;
    }
    for statement in state.subprogram_stmt_cache.values_mut() {
        statement.register_index_methods(connection)?;
    }
    tracing::trace!(
        registered,
        "retained statement index-method cursors for transaction outcome"
    );
    Ok(())
}

/// Rollback all virtual tables that are part of the current transaction.
fn vtab_rollback_all(conn: &Connection) -> crate::Result<()> {
    let mut set = conn.vtab_txn_states.write();
    if set.is_empty() {
        return Ok(());
    }
    let reg = &conn.syms.read().vtabs;
    for id in set.drain() {
        let vtab = reg
            .iter()
            .find(|(_, vtab)| vtab.id() == id)
            .expect("vtab must exist");
        vtab.1.rollback()?;
    }
    Ok(())
}

pub fn op_halt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Halt {
            err_code,
            description,
            on_error,
            description_reg,
        },
        insn
    );
    // If description_reg is set, read the error message from that register at runtime
    // (used by RAISE with expression-based error messages).
    let desc = if let Some(reg) = description_reg {
        state.registers[*reg].get_value().to_string()
    } else {
        description.to_string()
    };
    halt(program, state, pager, *err_code, &desc, *on_error)
}

pub fn op_halt_if_null(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HaltIfNull {
            target_reg,
            err_code,
            description,
        },
        insn
    );
    if state.registers[*target_reg].get_value() == &Value::Null {
        halt(program, state, pager, *err_code, description, None)
    } else {
        state.pc += 1;
        Ok(InsnFunctionStepResult::Step)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpTransactionState {
    Start,
    AttachedBeginWriteTx,
    BeginNamedSavepoints,
    CheckSchemaCookie,
    BeginStatement,
}

#[cfg(any(test, injected_yields))]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TransactionYieldPoint {
    BeforeStart,
    BeforeMvccBegin,
}

#[cfg(any(test, injected_yields))]
impl crate::mvcc::yield_hooks::YieldPointMarker for TransactionYieldPoint {
    const POINT_COUNT: u8 = 2;

    fn ordinal(self) -> u8 {
        match self {
            TransactionYieldPoint::BeforeStart => 0,
            TransactionYieldPoint::BeforeMvccBegin => 1,
        }
    }
}

pub fn op_transaction(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    let result = op_transaction_inner(program, state, insn, pager);
    tracing::debug!(
        "op_transaction: end: state={:?}, tx_state={:?}",
        state.active_op_state,
        program.connection.get_tx_state()
    );
    match result {
        Ok(result) => Ok(result),
        Err(err) => {
            state.active_op_state.clear();
            Err(err)
        }
    }
}

/// Begin an MVCC transaction after the caller has already put pager and
/// checkpoint state in the right shape.
///
/// Use `begin_fresh_mvcc_tx` when no MVCC transaction exists yet; it owns the
/// ordering between the checkpoint gate and pager read mark. Use this helper
/// directly for read-to-write upgrades, passing the existing transaction id and
/// `CheckpointReadLockState::NotHeld`.
fn begin_mvcc_tx(
    mv_store: &MvStore,
    pager: &Arc<Pager>,
    mode: &TransactionMode,
    existing_tx_id: Option<u64>,
    connection: &Connection,
    expected_schema_generation: Option<u64>,
    checkpoint_read_guard: CheckpointReadLockState,
) -> Result<u64> {
    match mode {
        TransactionMode::None | TransactionMode::Read | TransactionMode::Concurrent => mv_store
            .begin_tx_with_schema_generation_and_checkpoint_read_guard(
                pager.clone(),
                expected_schema_generation,
                checkpoint_read_guard,
            ),
        TransactionMode::Write => mv_store.begin_exclusive_tx_with_checkpoint_read_guard(
            pager.clone(),
            existing_tx_id,
            connection,
            expected_schema_generation,
            checkpoint_read_guard,
        ),
    }
}

/// Start a brand-new MVCC transaction for one pager.
///
/// This is the VDBE helper to use when the connection does not already have an
/// MVCC transaction for the database. It enforces the only safe fresh-begin
/// order: checkpoint gate, pager read mark, MVCC transaction. That prevents a
/// reader from pinning a WAL snapshot while a blocking MVCC checkpoint is
/// already in progress.
///
/// Some public maintenance paths, such as schema reparse and raw WAL handling,
/// enter VDBE with a caller-owned pager read mark already open. In that case
/// this helper adopts that read mark for the MVCC begin but must leave cleanup
/// to the caller.
///
/// Do not use this for read-to-write upgrades. Those keep the existing MVCC
/// transaction and should call `begin_mvcc_tx` with `existing_tx_id = Some`.
fn begin_fresh_mvcc_tx(
    mv_store: &MvStore,
    pager: &Arc<Pager>,
    mode: &TransactionMode,
    connection: &Connection,
    expected_schema_generation: Option<u64>,
) -> Result<u64> {
    let checkpoint_read_guard =
        mv_store.try_acquire_checkpoint_read_guard_for_fresh_mvcc_begin()?;
    let opened_pager_read_tx = !pager.holds_read_lock();
    if opened_pager_read_tx {
        if let Err(err) = pager.begin_read_tx() {
            mv_store.release_unadopted_checkpoint_read_guard(checkpoint_read_guard);
            return Err(err);
        }
    }
    // MVCC reads must refresh WAL change counters to avoid stale page-cache reads.
    pager.mvcc_refresh_if_db_changed();
    match begin_mvcc_tx(
        mv_store,
        pager,
        mode,
        None,
        connection,
        expected_schema_generation,
        checkpoint_read_guard,
    ) {
        Ok(tx_id) => Ok(tx_id),
        Err(err) => {
            if opened_pager_read_tx {
                pager.end_read_tx();
            }
            Err(err)
        }
    }
}

fn pager_db_size_for_named_savepoint(pager: &Arc<Pager>) -> IOResultOr<u32> {
    match pager.with_header(|header| header.database_size.get()) {
        Ok(result) => Ok(result),
        Err(err) if matches!(*err, LimboError::Page1NotAlloc) => Ok(IOResult::Done(0)),
        Err(err) => Err(err),
    }
}

fn open_named_savepoint_frames_on_wal_pager(
    pager: &Arc<Pager>,
    frames: &[crate::connection::NamedSavepointFrame],
) -> IOResultOr<()> {
    if frames.is_empty() {
        return Ok(IOResult::Done(()));
    }
    pager.open_subjournal()?;
    let db_size = match pager_db_size_for_named_savepoint(pager)? {
        IOResult::Done(db_size) => db_size,
        IOResult::IO(io) => return Ok(IOResult::IO(io)),
    };
    for frame in frames {
        pager.open_named_savepoint(
            frame.name.clone(),
            db_size,
            frame.starts_transaction,
            frame.deferred_fk_violations,
        )?;
    }
    Ok(IOResult::Done(()))
}

fn open_connection_named_savepoints_for_db(
    conn: &Connection,
    db: usize,
    pager: &Arc<Pager>,
) -> IOResultOr<()> {
    conn.with_named_savepoints(|frames| {
        if frames.is_empty() {
            return Ok(IOResult::Done(()));
        }

        if let Some(mv_store) = conn.mv_store_for_db(db) {
            let Some(tx_id) = conn.get_mv_tx_id_for_db(db) else {
                return Ok(IOResult::Done(()));
            };
            for frame in frames {
                mv_store.begin_named_savepoint(
                    tx_id,
                    frame.name.clone(),
                    frame.starts_transaction,
                    frame.deferred_fk_violations,
                );
            }
            Ok(IOResult::Done(()))
        } else {
            open_named_savepoint_frames_on_wal_pager(pager, frames)
        }
    })
}

fn load_active_non_main_wal_headers_for_named_savepoint(conn: &Connection) -> IOResultOr<()> {
    conn.with_all_attached_pagers_with_index(|pagers| {
        for (db_id, pager) in pagers {
            if conn.mv_store_for_db(*db_id).is_some() || !pager.holds_read_lock() {
                continue;
            }
            match pager_db_size_for_named_savepoint(pager)? {
                IOResult::Done(_) => {}
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }
        Ok(IOResult::Done(()))
    })
}

enum SavepointMirror<'a> {
    Begin(&'a crate::connection::NamedSavepointFrame),
    Release(&'a str),
    Rollback(&'a str),
}

fn mirror_named_savepoint_to_active_non_main_databases(
    conn: &Connection,
    op: SavepointMirror<'_>,
) -> Result<()> {
    conn.with_all_attached_pagers_with_index(|pagers| {
        for (db_id, pager) in pagers {
            let db_id = *db_id;
            if let Some(mv_store) = conn.mv_store_for_db(db_id) {
                let Some(tx_id) = conn.get_mv_tx_id_for_db(db_id) else {
                    continue;
                };
                match op {
                    SavepointMirror::Begin(frame) => {
                        mv_store.begin_named_savepoint(
                            tx_id,
                            frame.name.clone(),
                            frame.starts_transaction,
                            frame.deferred_fk_violations,
                        );
                    }
                    SavepointMirror::Release(name) => {
                        let _ = mv_store.release_named_savepoint(tx_id, name);
                    }
                    SavepointMirror::Rollback(name) => {
                        let _ = mv_store.rollback_to_named_savepoint(tx_id, name);
                    }
                }
                continue;
            }
            if !pager.holds_read_lock() {
                continue;
            }
            match op {
                SavepointMirror::Begin(frame) => {
                    // A pager holding a read lock does not pin page 1
                    // in the cache, so the header read inside
                    // `open_named_savepoint_frames_on_wal_pager` can
                    // yield IO (e.g. after cache eviction). We can't
                    // propagate IO out of this closure, so surface it
                    // as a loud `InternalError` rather than panicking.
                    // In practice page 1 is almost always resident at
                    // this point and this branch is never taken, but
                    // converting panic → error makes the fallback
                    // observable.
                    match open_named_savepoint_frames_on_wal_pager(
                        pager,
                        std::slice::from_ref(frame),
                    )? {
                        IOResult::Done(()) => {}
                        IOResult::IO(_) => {
                            return Err(LimboError::InternalError(
                                "open_named_savepoint on non-main pager returned IO \
                                 (page 1 not cached) — this path is not yet \
                                 IO-reentrant"
                                    .to_string(),
                            ));
                        }
                    }
                }
                SavepointMirror::Release(name) => {
                    let _ = pager.release_named_savepoint(name)?;
                }
                SavepointMirror::Rollback(name) => {
                    let _ = pager.rollback_to_named_savepoint(name)?;
                }
            }
        }
        Ok(())
    })
}

pub fn op_transaction_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Transaction {
            db,
            tx_mode,
            schema_cookie,
        },
        insn
    );
    if program.is_trigger_subprogram() {
        crate::bail_parse_error!(
            "Transaction instruction should not be used in trigger subprograms"
        );
    }
    if *db == crate::TEMP_DB_ID {
        program.connection.ensure_temp_database()?;
    }
    let pager = program.get_pager_from_database_index(db)?;
    // Get the MvStore for the specific database (main or attached).
    let mv_store = program.connection.mv_store_for_db(*db);
    let is_main_db = *db == crate::MAIN_DB_ID;
    let is_secondary_db = !is_main_db;
    let write = matches!(
        tx_mode,
        TransactionMode::Write | TransactionMode::Concurrent
    );
    // BEGIN IMMEDIATE / EXCLUSIVE / CONCURRENT need write-capable transaction
    // access, but they are not themselves write statements. Only statements in
    // `write_databases` count for same-connection writer blocking and
    // active-writer cleanup.
    let statement_writes_db = program.write_databases.get(*db);
    loop {
        match *state.active_op_state.transaction() {
            OpTransactionState::Start => {
                let conn = program.connection.clone();
                let mut started_secondary_tx = false;
                if write && conn.is_readonly(*db) {
                    return Err(LimboError::ReadOnly.into());
                }
                let active_writers = conn.n_active_writes.load(Ordering::SeqCst);
                turso_assert!(
                    active_writers <= 1,
                    "n_active_writes must be 0 or 1, got {active_writers}"
                );
                // One connection may have many active readers, but only one
                // top-level writer. A second writer on the same connection is
                // rejected before it opens transaction or savepoint state.
                //
                // This is stricter than SQLite. SQLite can run overlapping
                // write statements on one connection because sqlite3_step()
                // does not return to the caller in the middle of built-in
                // write opcodes. Turso can suspend there for async I/O, so a
                // second writer would make reset/drop cleanup hard to get right.
                if statement_writes_db
                    && !conn.is_nested_stmt()
                    && !state.is_active_write
                    && active_writers > 0
                {
                    return Err(
                        LimboError::StatementsInProgress("cannot start a write statement").into(),
                    );
                }

                // Fast path: if checkpoint root publication already replaced the
                // shared schema, force reprepare before opening any transaction state.
                if is_main_db
                    && mv_store.is_some()
                    && conn.mvcc_schema_requires_reprepare_before_tx()
                {
                    tracing::debug!(
                        "MVCC shared schema changed without a schema-cookie change; force reprepare"
                    );
                    return Err(LimboError::SchemaUpdated.into());
                }
                #[cfg(any(test, injected_yields))]
                {
                    if let Some(IOResult::IO(io)) =
                        crate::mvcc::yield_hooks::maybe_inject_io_yield::<(), _>(
                            conn.yield_injector().as_ref(),
                            0,
                            *db as u64,
                            TransactionYieldPoint::BeforeStart,
                        )
                    {
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                }

                // 1. We try to upgrade current version
                let current_state = conn.get_tx_state();
                let (new_transaction_state, updated) = if conn.is_nested_stmt() {
                    (current_state, false)
                } else if is_secondary_db {
                    // For attached databases, don't modify the connection-level
                    // transaction state — it tracks the main database's state.
                    // Attached pager locks are managed independently below.
                    (current_state, false)
                } else {
                    match (current_state, write) {
                        // pending state means that we tried beginning a tx and the method returned IO.
                        // instead of ending the read tx, just update the state to pending.
                        (TransactionState::PendingUpgrade { .. }, write) => {
                            turso_assert!(
                                write,
                                "pending upgrade should only be set for write transactions"
                            );
                            (
                                TransactionState::Write {
                                    schema_did_change: false,
                                },
                                true,
                            )
                        }
                        (TransactionState::Write { schema_did_change }, true) => {
                            (TransactionState::Write { schema_did_change }, false)
                        }
                        (TransactionState::Write { schema_did_change }, false) => {
                            (TransactionState::Write { schema_did_change }, false)
                        }
                        (TransactionState::Read, true) => (
                            TransactionState::Write {
                                schema_did_change: false,
                            },
                            true,
                        ),
                        (TransactionState::Read, false) => (TransactionState::Read, false),
                        (TransactionState::None, true) => (
                            TransactionState::Write {
                                schema_did_change: false,
                            },
                            true,
                        ),
                        (TransactionState::None, false) => (TransactionState::Read, true),
                    }
                };

                // 2. Start transaction if needed
                if let Some(mv_store) = mv_store.as_ref() {
                    if is_secondary_db {
                        // Attached databases don't participate in the connection-level
                        // transaction state machine above, so they must open
                        // their own pager snapshot after passing the MVCC
                        // checkpoint gate.

                        let current_mv_tx = conn.get_mv_tx_for_db(*db);
                        if current_mv_tx.is_none() {
                            // Reject CONCURRENT on an attached DB if the main
                            // DB already started with BEGIN DEFERRED.
                            let conn_has_executed_begin_deferred =
                                !conn.auto_commit.load(Ordering::SeqCst)
                                    && conn.get_mv_tx().is_none();
                            if conn_has_executed_begin_deferred
                                && *tx_mode == TransactionMode::Concurrent
                            {
                                mark_unlikely();
                                return Err(LimboError::TxError(
                                    "Cannot start CONCURRENT transaction after BEGIN DEFERRED"
                                        .to_string(),
                                )
                                .into());
                            }
                            // Use the same tx_mode as the main DB's active
                            // transaction when available, so BEGIN CONCURRENT
                            // applies to all databases uniformly.
                            let effective_mode =
                                conn.get_mv_tx().map(|(_, mode)| mode).unwrap_or(*tx_mode);
                            #[cfg(any(test, injected_yields))]
                            {
                                if let Some(IOResult::IO(io)) =
                                    crate::mvcc::yield_hooks::maybe_inject_io_yield::<(), _>(
                                        conn.yield_injector().as_ref(),
                                        0,
                                        *db as u64,
                                        TransactionYieldPoint::BeforeMvccBegin,
                                    )
                                {
                                    return Ok(InsnFunctionStepResult::IO(io));
                                }
                            }
                            match begin_fresh_mvcc_tx(
                                mv_store,
                                &pager,
                                &effective_mode,
                                &conn,
                                None,
                            ) {
                                Ok(tx_id) => {
                                    conn.set_mv_tx_for_db(*db, Some((tx_id, effective_mode)));
                                    started_secondary_tx = true;
                                    if conn.get_auto_commit() && !conn.is_nested_stmt() {
                                        state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                                    }
                                }
                                Err(err) => return Err(err.into()),
                            }
                        } else if write {
                            // Upgrade: attached DB has a Read/Concurrent tx but the
                            // statement needs write access. Mirror the main DB's
                            // upgrade logic so that exclusive locks are acquired.
                            let (tx_id, current_mode) = current_mv_tx.unwrap();
                            if matches!(current_mode, TransactionMode::None | TransactionMode::Read)
                                && matches!(tx_mode, TransactionMode::Write)
                            {
                                begin_mvcc_tx(
                                    mv_store,
                                    &pager,
                                    tx_mode,
                                    Some(tx_id),
                                    &conn,
                                    None,
                                    CheckpointReadLockState::NotHeld,
                                )?;
                                conn.set_mv_tx_for_db(*db, Some((tx_id, *tx_mode)));
                            }
                        }
                    } else {
                        // Main database MVCC path (unchanged logic)
                        let started_read_tx =
                            updated && matches!(current_state, TransactionState::None);
                        if started_read_tx {
                            turso_assert!(
                                !conn.is_nested_stmt(),
                                "nested stmt should not begin a new read transaction"
                            );
                        }
                        // In MVCC we don't have write exclusivity, therefore we just need to start a transaction if needed.
                        // Programs can run Transaction twice, first with read flag and then with write flag. So a single txid is enough
                        // for both.
                        let current_mv_tx = program.connection.get_mv_tx_for_db(*db);
                        let has_existing_mv_tx = current_mv_tx.is_some();
                        if has_existing_mv_tx {
                            // MVCC reads must refresh WAL change counters to avoid stale page-cache reads.
                            pager.mvcc_refresh_if_db_changed();
                        }

                        let conn_has_executed_begin_deferred = !has_existing_mv_tx
                            && !program.connection.auto_commit.load(Ordering::SeqCst);
                        if conn_has_executed_begin_deferred
                            && *tx_mode == TransactionMode::Concurrent
                        {
                            mark_unlikely();
                            if started_read_tx {
                                conn.set_tx_state(TransactionState::None);
                                state.auto_txn_cleanup = TxnCleanup::None;
                            }
                            return Err(LimboError::TxError(
                                "Cannot start CONCURRENT transaction after BEGIN DEFERRED"
                                    .to_string(),
                            )
                            .into());
                        }

                        if !has_existing_mv_tx {
                            // Gate the begin on the connection's prepared schema generation,
                            // captured here (atomically with the connection.schema == db.schema
                            // check) and re-checked inside begin_tx's clock callback. A passive
                            // checkpoint publish runs under the same clock and bumps the
                            // generation, so one that orders into the begin window is detected
                            // there and forces a reprepare against the published roots — without
                            // the old non-atomic post-begin recheck (which also tripped on
                            // publishes strictly after our snapshot).
                            let expected_schema_generation =
                                match conn.mvcc_begin_schema_generation() {
                                    Ok(generation) => generation,
                                    Err(err) => {
                                        if started_read_tx {
                                            conn.set_tx_state(TransactionState::None);
                                            state.auto_txn_cleanup = TxnCleanup::None;
                                        }
                                        return Err(err.into());
                                    }
                                };
                            #[cfg(any(test, injected_yields))]
                            {
                                if let Some(IOResult::IO(io)) =
                                    crate::mvcc::yield_hooks::maybe_inject_io_yield::<(), _>(
                                        conn.yield_injector().as_ref(),
                                        0,
                                        *db as u64,
                                        TransactionYieldPoint::BeforeMvccBegin,
                                    )
                                {
                                    return Ok(InsnFunctionStepResult::IO(io));
                                }
                            }
                            match begin_fresh_mvcc_tx(
                                mv_store,
                                &pager,
                                tx_mode,
                                &conn,
                                expected_schema_generation,
                            ) {
                                Ok(tx_id) => {
                                    program
                                        .connection
                                        .set_mv_tx_for_db(*db, Some((tx_id, *tx_mode)));
                                    if started_read_tx && conn.get_auto_commit() {
                                        state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                                    }
                                }
                                Err(err) => {
                                    if started_read_tx {
                                        conn.set_tx_state(TransactionState::None);
                                        state.auto_txn_cleanup = TxnCleanup::None;
                                    }
                                    return Err(err.into());
                                }
                            }
                        } else if updated {
                            // TODO: fix tx_mode in Insn::Transaction, now each statement overrides it even if there's already a CONCURRENT Tx in progress, for example
                            let (tx_id, mv_tx_mode) = current_mv_tx
                                .expect("current_mv_tx should be Some when updated is true");
                            let actual_tx_mode = if mv_tx_mode == TransactionMode::Concurrent {
                                TransactionMode::Concurrent
                            } else {
                                *tx_mode
                            };
                            if matches!(new_transaction_state, TransactionState::Write { .. })
                                && matches!(actual_tx_mode, TransactionMode::Write)
                            {
                                if let Err(err) = begin_mvcc_tx(
                                    mv_store,
                                    &pager,
                                    &actual_tx_mode,
                                    Some(tx_id),
                                    &conn,
                                    None,
                                    CheckpointReadLockState::NotHeld,
                                ) {
                                    if started_read_tx {
                                        pager.end_read_tx();
                                        conn.set_tx_state(TransactionState::None);
                                        state.auto_txn_cleanup = TxnCleanup::None;
                                    }
                                    return Err(err.into());
                                }
                            }
                        }
                    }
                } else {
                    if matches!(tx_mode, TransactionMode::Concurrent) {
                        mark_unlikely();
                        return Err(LimboError::TxError(
                            "Concurrent transaction mode is only supported when MVCC is enabled"
                                .to_string(),
                        )
                        .into());
                    }
                    // For attached databases without MVCC, always start read/write
                    // transactions on the attached pager, since the connection-level
                    // transaction state may already be Read/Write from the main database.
                    if is_secondary_db {
                        if conn.get_auto_commit() && !conn.is_nested_stmt() {
                            state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                        }
                        // If the pager already holds a read lock (e.g., after
                        // SchemaUpdated reprepare or prior write tx), skip
                        // locks that are already held.
                        if pager.holds_read_lock() {
                            if matches!(tx_mode, TransactionMode::Write)
                                && !pager.holds_write_lock()
                            {
                                *state.active_op_state.transaction() =
                                    OpTransactionState::AttachedBeginWriteTx;
                                continue;
                            }
                            *state.active_op_state.transaction() =
                                OpTransactionState::CheckSchemaCookie;
                            continue;
                        }
                        pager.begin_read_tx()?;
                        started_secondary_tx = true;
                        if matches!(tx_mode, TransactionMode::Write) {
                            // Transition to AttachedBeginWriteTx to handle begin_write_tx
                            // separately, so if it returns IO we don't re-call begin_read_tx
                            // on re-entry.
                            if conn.with_named_savepoints(|savepoints| savepoints.is_empty()) {
                                *state.active_op_state.transaction() =
                                    OpTransactionState::AttachedBeginWriteTx;
                            } else {
                                *state.active_op_state.transaction() =
                                    OpTransactionState::BeginNamedSavepoints;
                            }
                            continue;
                        }
                    } else if updated && matches!(current_state, TransactionState::None) {
                        turso_assert!(
                            !conn.is_nested_stmt(),
                            "nested stmt should not begin a new read transaction"
                        );
                        pager.begin_read_tx()?;
                        // https://github.com/tursodatabase/turso/issues/7106 : If auto-commit is turned off,
                        // then we shouldn't be setting the auto_txn_cleanup, this will cause errors in case the client explicitly calls commit post a drop of the statement struct
                        if conn.get_auto_commit() {
                            state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                        }
                    }

                    if !is_secondary_db
                        && updated
                        && matches!(new_transaction_state, TransactionState::Write { .. })
                    {
                        turso_assert!(
                            !conn.is_nested_stmt(),
                            "nested stmt should not begin a new write transaction"
                        );
                        let begin_w_tx_res = pager.begin_write_tx(conn.wal_auto_actions());
                        if matches!(
                            &begin_w_tx_res,
                            Err(err) if matches!(**err, LimboError::Busy | LimboError::BusySnapshot)
                        ) {
                            // We failed to upgrade to write transaction so put the transaction into its original state.
                            // That is, if the transaction had not started, end the read transaction so that next time we
                            // start a new one.
                            match current_state {
                                TransactionState::None
                                | TransactionState::PendingUpgrade {
                                    has_read_txn: false,
                                } => {
                                    pager.end_read_tx();
                                    conn.set_tx_state(TransactionState::None);
                                    state.auto_txn_cleanup = TxnCleanup::None;
                                }
                                TransactionState::Read
                                | TransactionState::PendingUpgrade { has_read_txn: true } => {
                                    conn.set_tx_state(TransactionState::Read);
                                }
                                TransactionState::Write { .. } => {
                                    panic!("impossible state: {current_state:?}")
                                }
                            }
                            return Err(begin_w_tx_res.unwrap_err());
                        }
                        if let IOResult::IO(io) = begin_w_tx_res? {
                            // set the transaction state to pending so we don't have to
                            // end the read transaction.
                            conn.set_tx_state(TransactionState::PendingUpgrade {
                                has_read_txn: matches!(current_state, TransactionState::Read),
                            });
                            return Ok(InsnFunctionStepResult::IO(io));
                        }
                    }
                }

                // 3. Transaction state should be updated before checking for Schema cookie so that the tx is ended properly on error
                if updated {
                    conn.set_tx_state(new_transaction_state);
                }
                if is_secondary_db
                    && started_secondary_tx
                    && !conn.with_named_savepoints(|s| s.is_empty())
                {
                    *state.active_op_state.transaction() = OpTransactionState::BeginNamedSavepoints;
                    continue;
                }
                *state.active_op_state.transaction() = OpTransactionState::CheckSchemaCookie;
                continue;
            }
            // 3b. For attached databases, begin the write transaction after
            // begin_read_tx has already completed in the Start state.
            OpTransactionState::AttachedBeginWriteTx => {
                let conn = program.connection.clone();
                let res = pager.begin_write_tx(conn.wal_auto_actions())?;
                if let IOResult::IO(io) = res {
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                *state.active_op_state.transaction() = OpTransactionState::CheckSchemaCookie;
                continue;
            }
            OpTransactionState::BeginNamedSavepoints => {
                match open_connection_named_savepoints_for_db(&program.connection, *db, &pager)? {
                    IOResult::Done(()) => {
                        if is_secondary_db
                            && mv_store.is_none()
                            && matches!(tx_mode, TransactionMode::Write)
                            && !pager.holds_write_lock()
                        {
                            *state.active_op_state.transaction() =
                                OpTransactionState::AttachedBeginWriteTx;
                        } else {
                            *state.active_op_state.transaction() =
                                OpTransactionState::CheckSchemaCookie;
                        }
                        continue;
                    }
                    IOResult::IO(io) => return Ok(InsnFunctionStepResult::IO(io)),
                }
            }
            // 4. Check whether schema has changed if we are actually going to access the database.
            // Can only read header if page 1 has been allocated already
            // begin_write_tx that happens, but not begin_read_tx
            OpTransactionState::CheckSchemaCookie => {
                let res = get_schema_cookie(&pager, mv_store.as_ref(), program, *db);
                match res {
                    Ok(IOResult::Done(header_schema_cookie)) => {
                        if header_schema_cookie != *schema_cookie {
                            tracing::debug!(
                                "schema changed, force reprepare: {} != {}",
                                header_schema_cookie,
                                *schema_cookie
                            );
                            return Err(LimboError::SchemaUpdated.into());
                        }
                    }
                    Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
                    // This means we are starting a read_tx and we do not have a page 1 yet, so we just continue execution
                    Err(err) if matches!(*err, LimboError::Page1NotAlloc) => {}
                    Err(err) => {
                        return Err(err);
                    }
                }

                *state.active_op_state.transaction() = OpTransactionState::BeginStatement;
            }
            OpTransactionState::BeginStatement => {
                let needs_stmt_journal = program.needs_stmt_subtransactions.load(Ordering::Relaxed);
                let auto_commit = program.connection.auto_commit.load(Ordering::SeqCst);
                let in_explicit_txn = !auto_commit;
                if needs_stmt_journal {
                    if is_main_db {
                        let res = state.begin_statement(
                            &program.connection,
                            &pager,
                            statement_writes_db,
                        )?;
                        if let IOResult::IO(io) = res {
                            return Ok(InsnFunctionStepResult::IO(io));
                        }
                    } else if statement_writes_db && in_explicit_txn {
                        if !state.has_stmt_transaction {
                            state.has_stmt_transaction = true;
                            state.fk_deferred_violations_when_stmt_started.store(
                                program
                                    .connection
                                    .fk_deferred_violations
                                    .load(Ordering::Acquire),
                                Ordering::SeqCst,
                            );
                            state
                                .fk_immediate_violations_during_stmt
                                .store(0, Ordering::Release);
                        }
                        if let Some(mv_store) = mv_store.as_ref() {
                            // Attached MVCC DB: open an MvStore savepoint.
                            if let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) {
                                mv_store.begin_savepoint(tx_id);
                            }
                        } else {
                            // Attached WAL DB: open a pager savepoint for statement rollback.
                            let db_size = return_if_io!(
                                pager.with_header(|header| header.database_size.get())
                            );
                            pager.open_subjournal()?;
                            pager.try_use_subjournal()?;
                            let result = pager.open_savepoint(db_size);
                            if result.is_err() {
                                pager.stop_use_subjournal();
                            }
                            result?;
                            state.attached_savepoint_pagers.push(pager.clone());
                        }
                    }
                }

                let is_top_level_statement = !program.connection.is_nested_stmt();
                if statement_writes_db && is_top_level_statement {
                    if !state.is_active_write {
                        let previous = program
                            .connection
                            .n_active_writes
                            .fetch_add(1, Ordering::SeqCst);
                        turso_assert!(
                            previous == 0,
                            "starting a writer while {previous} writer(s) are already active"
                        );
                        state.is_active_write = true;
                    }
                    if is_main_db && in_explicit_txn && state.has_stmt_transaction {
                        state.auto_txn_cleanup = TxnCleanup::RollbackSavepoint;
                    }
                }
                if is_top_level_statement
                    && is_main_db
                    && auto_commit
                    && state.auto_txn_cleanup == TxnCleanup::None
                {
                    let active_root_statements = program
                        .connection
                        .n_active_root_statements
                        .load(Ordering::SeqCst);
                    if active_root_statements > 1 {
                        // A sibling statement opened the implicit transaction
                        // before this statement joined it. Mark this statement
                        // so the last remaining sibling can close the
                        // transaction.
                        state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                    }
                }
                state.pc += 1;
                state.active_op_state.clear();
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_auto_commit(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        AutoCommit {
            auto_commit,
            rollback
        },
        insn
    );

    // Main DB's MvStore drives the commit/rollback routing.  The attach-time
    // journal-mode compatibility check ensures all attached DBs match, so
    // checking the main DB is sufficient to choose the MVCC vs WAL path.
    let mv_store = program.connection.mv_store();
    let conn = program.connection.clone();
    let fk_on = conn.foreign_keys_enabled();
    let had_autocommit = conn.auto_commit.load(Ordering::SeqCst); // true, not in tx

    // Drive any multi-step commit/rollback that's already in progress.
    // This handles main DB commits (Committing), attached DB commits
    // (CommittingAttached), MVCC commits (CommittingMvcc), and attached
    // MVCC commits (CommittingAttachedMvcc) that yielded on IO and need re-entry.
    if !matches!(state.commit_state, CommitState::Ready) {
        let res = program
            .commit_txn(pager.clone(), state, mv_store.as_ref(), *rollback)
            .map(Into::into);
        // Only clear after a final, successful non-rollback COMMIT.
        if fk_on
            && !*rollback
            && matches!(
                res,
                Ok(InsnFunctionStepResult::Step | InsnFunctionStepResult::Done)
            )
        {
            conn.clear_deferred_foreign_key_violations();
        }
        if matches!(
            res,
            Ok(InsnFunctionStepResult::Step | InsnFunctionStepResult::Done)
        ) {
            if !*rollback {
                conn.index_methods_on_transaction_committed();
            }
            conn.clear_tx_poison();
            conn.clear_named_savepoints();
        }
        return res;
    }

    if program.is_trigger_subprogram() {
        // Trigger subprograms never commit or rollback.
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    #[derive(Debug)]
    enum TxOp {
        Begin,
        Commit,
        Rollback,
    }
    let tx_op = match (*auto_commit, *rollback) {
        (false, false) => TxOp::Begin,
        (true, false) => TxOp::Commit,
        (true, true) => TxOp::Rollback,
        (false, true) => {
            return Err(LimboError::InternalError(
                "Insn::AutoCommit {{ auto_commit: false, rollback: true }} is not valid".into(),
            )
            .into());
        }
    };

    // BEGIN disables autocommit; COMMIT/ROLLBACK enables it. Anything else (BEGIN within a txn,
    // or COMMIT/ROLLBACK without one) is invalid.
    let valid_transition = matches!(
        (&tx_op, had_autocommit),
        (TxOp::Begin, true) | (TxOp::Commit | TxOp::Rollback, false)
    );

    if valid_transition {
        if matches!(tx_op, TxOp::Commit | TxOp::Rollback)
            && conn.n_active_writes.load(Ordering::SeqCst) > 0
        {
            return Err(LimboError::StatementsInProgress(match tx_op {
                TxOp::Commit => "cannot commit transaction",
                TxOp::Rollback => "cannot rollback transaction",
                TxOp::Begin => unreachable!("guard only matches Commit | Rollback"),
            })
            .into());
        }

        match tx_op {
            TxOp::Rollback => {
                if let Some(mv_store) = mv_store.as_ref() {
                    if let Some(tx_id) = conn.get_mv_tx_id() {
                        mv_store.rollback_tx(tx_id, pager.clone(), &conn, MAIN_DB_ID);
                    }
                    pager.end_read_tx();
                    conn.rollback_attached_mvcc_txs(true);
                } else {
                    pager.rollback_tx(&conn);
                }
                conn.rollback_attached_wal_txns();
                conn.rollback_temp_schema();
                conn.index_methods_on_transaction_rolled_back();
                conn.set_tx_state(TransactionState::None);
                conn.auto_commit.store(true, Ordering::SeqCst);
                conn.set_cdc_transaction_id(-1);
            }
            TxOp::Commit => {
                if conn.tx_is_poisoned() {
                    // A write statement inside BEGIN was reset/dropped before
                    // it reached Done, and there was no statement savepoint to
                    // undo only that statement. Letting COMMIT proceed would
                    // persist a partial statement, so COMMIT rolls back the
                    // whole transaction and reports the abandoned write.
                    conn.rollback_manual_txn_cleanup(pager, true);
                    return Err(LimboError::TxError(
                        "cannot commit - an unfinished write statement was abandoned".to_string(),
                    )
                    .into());
                }
                // Pre-check deferred FKs; leave tx open and do NOT clear violations
                check_deferred_fk_on_commit(&conn)?;
                conn.auto_commit.store(true, Ordering::SeqCst);
                state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
            }
            TxOp::Begin => {
                turso_assert!(
                    !conn.tx_is_poisoned(),
                    "rollback-only marker leaked outside an explicit transaction"
                );
                conn.auto_commit.store(false, Ordering::SeqCst);
                return Ok(InsnFunctionStepResult::Done);
            }
        }
    } else {
        return match &tx_op {
            TxOp::Begin => Err(LimboError::TxError(
                "cannot start a transaction within a transaction".to_string(),
            )
            .into()),
            TxOp::Commit => Err(LimboError::TxError(
                "cannot commit - no transaction is active".to_string(),
            )
            .into()),
            TxOp::Rollback => Err(LimboError::TxError(
                "cannot rollback - no transaction is active".to_string(),
            )
            .into()),
        };
    }

    turso_debug_assert!(
        matches!(tx_op, TxOp::Commit | TxOp::Rollback),
        "tx_op should be commit or rollback by now",
        { "tx_op": tx_op }
    );

    // Index-method staging is a per-statement Halt responsibility (each
    // statement stages its writes at its own halt and hands its cursors to
    // the connection), so the COMMIT program has nothing to stage here. A
    // yield point at this spot would also be unsafe: `conn.auto_commit` was
    // already flipped above, and re-entering this opcode from the top after
    // an IO yield would then fail `valid_transition` with a torn-down
    // transaction.

    let res = match program
        .commit_txn(pager.clone(), state, mv_store.as_ref(), *rollback)
        .map(Into::<InsnFunctionStepResult>::into)?
    {
        res @ (InsnFunctionStepResult::Done | InsnFunctionStepResult::Step) => res,
        res @ (InsnFunctionStepResult::IO(_) | InsnFunctionStepResult::Row) => return Ok(res),
    };

    if mv_store.is_none() {
        pager.clear_savepoints()?;
        // Non-main pagers (temp + attached) accumulate savepoints from the
        // mirror path; they're not cleared by the main pager's commit/
        // rollback. Without this, a stale savepoint with the same name from
        // a previous transaction can be matched by a future ROLLBACK TO and
        // undo unrelated pages. See fuzz regression in
        // `named_savepoint_differential_fuzz`.
        conn.with_all_attached_pagers_with_index(|pagers| -> Result<()> {
            for (_, attached_pager) in pagers {
                attached_pager.clear_savepoints()?;
            }
            Ok(())
        })?;
    }

    // Clear deferred FK counters only after FINAL success of COMMIT/ROLLBACK.
    if fk_on {
        conn.clear_deferred_foreign_key_violations();
    }

    // Reset CDC transaction ID after successful COMMIT or ROLLBACK.
    conn.set_cdc_transaction_id(-1);
    if matches!(tx_op, TxOp::Commit) {
        conn.index_methods_on_transaction_committed();
    }
    conn.clear_tx_poison();
    conn.clear_named_savepoints();

    Ok(res)
}

pub fn op_savepoint(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Savepoint { op, name }, insn);
    let conn = program.connection.clone();
    let mv_store = conn.mv_store();

    // No savepoint operation may run while a write statement on this
    // connection is suspended mid-execution. SQLite rejects SAVEPOINT and
    // RELEASE with SQLITE_BUSY while write statements are in progress
    // (vdbe.c, OP_Savepoint: "cannot open/release savepoint - SQL statements
    // in progress"). SQLite does allow ROLLBACK TO there because it trips all
    // open cursors so the affected statements abort instead of resuming;
    // Turso has no cursor-tripping mechanism, and letting a suspended writer
    // resume on top of pages restored by ROLLBACK TO would interleave two
    // inconsistent page states. Rejecting all three keeps the rule simple:
    // finish or reset the active writer first.
    if !conn.is_nested_stmt() && conn.n_active_writes.load(Ordering::SeqCst) > 0 {
        return Err(LimboError::StatementsInProgress(match *op {
            SavepointOp::Begin => "cannot open savepoint",
            SavepointOp::Release => "cannot release savepoint",
            SavepointOp::RollbackTo => "cannot rollback savepoint",
        })
        .into());
    }

    match *op {
        SavepointOp::Begin => {
            // Mirroring mutates several pager savepoint stacks and therefore
            // cannot yield halfway through. Load every active WAL header
            // before opening the first savepoint so an I/O yield is restartable.
            if let IOResult::IO(io) = load_active_non_main_wal_headers_for_named_savepoint(&conn)? {
                return Ok(InsnFunctionStepResult::IO(io));
            }
            #[cfg(any(test, injected_yields))]
            {
                if mv_store.is_some() && conn.get_mv_tx_id().is_none() {
                    if let Some(IOResult::IO(io)) =
                        crate::mvcc::yield_hooks::maybe_inject_io_yield::<(), _>(
                            conn.yield_injector().as_ref(),
                            0,
                            crate::MAIN_DB_ID as u64,
                            TransactionYieldPoint::BeforeMvccBegin,
                        )
                    {
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                }
            }
            conn.with_savepoint_schema_snapshot(
                |main_schema_snapshot, temp_schema_snapshot, staged_schema_snapshot| {
                    let starts_transaction = conn.auto_commit.load(Ordering::SeqCst);
                    let deferred_fk_violations = conn.get_deferred_foreign_key_violations();

                    if let Some(mv_store) = mv_store.as_ref() {
                        let tx_id = if let Some(tx_id) = conn.get_mv_tx_id() {
                            tx_id
                        } else {
                            let tx_id = begin_fresh_mvcc_tx(
                                mv_store,
                                pager,
                                &TransactionMode::Read,
                                &conn,
                                None,
                            )?;
                            conn.set_mv_tx(Some((tx_id, TransactionMode::Read)));
                            if matches!(conn.get_tx_state(), TransactionState::None) {
                                conn.set_tx_state(TransactionState::Read);
                            }
                            tx_id
                        };
                        mv_store.begin_named_savepoint(
                            tx_id,
                            name.clone(),
                            starts_transaction,
                            deferred_fk_violations,
                        );
                    } else {
                        // TODO: SQLite's SAVEPOINT takes no locks and no
                        // snapshot; its pager savepoint materializes at
                        // write-tx begin (sqlite3PagerOpenSavepoint). The
                        // eager read tx here pins the snapshot too early and
                        // holds a read lock while idle — not needed for
                        // correctness, since the WAL position already
                        // materializes at write upgrade and db_size could
                        // move there the same way.
                        if !pager.holds_read_lock() {
                            pager.begin_read_tx()?;
                        }
                        if matches!(conn.get_tx_state(), TransactionState::None) {
                            conn.set_tx_state(TransactionState::Read);
                        }
                        pager.open_subjournal()?;
                        let db_size =
                            return_if_io!(pager.with_header(|header| header.database_size.get()));
                        pager.open_named_savepoint(
                            name.clone(),
                            db_size,
                            starts_transaction,
                            deferred_fk_violations,
                        )?;
                    }
                    let frame = crate::connection::NamedSavepointFrame {
                        name: name.clone(),
                        starts_transaction,
                        deferred_fk_violations,
                        main_schema_snapshot,
                        temp_schema_snapshot,
                        staged_schema_snapshot,
                    };
                    // Mirror onto attached/temp pagers. If any pager fails
                    // mid-flight, earlier ones already opened a savepoint
                    // with this name — and main's savepoint is already
                    // open too. Undo the partial state atomically so the
                    // connection's and pagers' savepoint stacks stay in
                    // sync. `release_named_savepoint` is idempotent on a
                    // missing name, so we can blind-release on all non-
                    // main pagers.
                    if let Err(mirror_err) = mirror_named_savepoint_to_active_non_main_databases(
                        &conn,
                        SavepointMirror::Begin(&frame),
                    ) {
                        // Release the partially-opened mirror savepoints.
                        let _ = mirror_named_savepoint_to_active_non_main_databases(
                            &conn,
                            SavepointMirror::Release(name),
                        );
                        // Release the main savepoint we just opened so it
                        // does not linger without a connection-level frame
                        // recording it (which would leak past commit /
                        // rollback).
                        if let Some(mv_store) = mv_store.as_ref() {
                            if let Some(tx_id) = conn.get_mv_tx_id() {
                                let _ = mv_store.release_named_savepoint(tx_id, name);
                            }
                        } else {
                            let _ = pager.release_named_savepoint(name);
                        }
                        return Err(mirror_err);
                    }
                    conn.push_named_savepoint(frame);
                    if starts_transaction {
                        turso_assert!(
                            !conn.tx_is_poisoned(),
                            "rollback-only marker leaked outside an explicit transaction"
                        );
                        conn.auto_commit.store(false, Ordering::SeqCst);
                    }

                    state.pc += 1;
                    Ok(InsnFunctionStepResult::Step)
                },
            )
            .map_err(Into::into)
        }
        SavepointOp::Release => {
            let release_result = if let Some(mv_store) = mv_store.as_ref() {
                match conn.get_mv_tx_id() {
                    Some(tx_id) => mv_store.release_named_savepoint(tx_id, name)?,
                    None => SavepointResult::NotFound,
                }
            } else {
                pager.release_named_savepoint(name)?
            };
            match release_result {
                SavepointResult::NotFound => {
                    mark_unlikely();
                    return Err(LimboError::TxError(format!("no such savepoint: {name}")).into());
                }
                SavepointResult::Release => {
                    let _ = conn.release_named_savepoint_frame(name);
                    mirror_named_savepoint_to_active_non_main_databases(
                        &conn,
                        SavepointMirror::Release(name),
                    )?;
                }
                SavepointResult::Commit => {
                    let _ = conn.release_named_savepoint_frame(name);
                    mirror_named_savepoint_to_active_non_main_databases(
                        &conn,
                        SavepointMirror::Release(name),
                    )?;
                    // This means that releasing the savepoint caused the transaction to commit, so we need to auto-commit here.
                    let auto_commit = Insn::AutoCommit {
                        auto_commit: true,
                        rollback: false,
                    };
                    return op_auto_commit(program, state, &auto_commit, pager);
                }
            }

            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        SavepointOp::RollbackTo => {
            let deferred_fk_snapshot = if let Some(mv_store) = mv_store.as_ref() {
                match conn.get_mv_tx_id() {
                    Some(tx_id) => mv_store.rollback_to_named_savepoint(tx_id, name)?,
                    None => None,
                }
            } else {
                pager.rollback_to_named_savepoint(name)?
            };

            let Some(deferred_fk_snapshot) = deferred_fk_snapshot else {
                mark_unlikely();
                return Err(LimboError::TxError(format!("no such savepoint: {name}")).into());
            };
            let frame_info = conn.rollback_named_savepoint_frame(name);
            mirror_named_savepoint_to_active_non_main_databases(
                &conn,
                SavepointMirror::Rollback(name),
            )?;
            conn.fk_deferred_violations
                .store(deferred_fk_snapshot, Ordering::Release);

            // Restore all in-memory schemas (main, temp, attached) from the
            // frame's snapshot. The mirror call above rolled back on-disk
            // pages for temp and attached pagers; main DB pages are rolled
            // back by `pager.rollback_to_named_savepoint`. Without this
            // restore, the in-memory schemas would keep DDL that the disk-
            // level rollback just undid.
            //
            // The main-DB snapshot replaces an older code path that
            // reparsed `sqlite_schema` from disk here. That path blocked
            // on cursor I/O (and, with sequences, on `prepare_internal +
            // run_with_row_callback` driving `pager.io.step` synchronously)
            // — both vdbe async-contract violations. The snapshot is cheap
            // (`Arc<Schema>` clone at SAVEPOINT begin) and carries the full
            // schema including the sequences map, so the restore is
            // consistent across tables / indexes / sequences without any
            // I/O.
            if let Some(info) = frame_info {
                *conn.schema.write() = info.main_schema_snapshot;
                if let Some(temp_db) = conn.temp.database.read().as_ref() {
                    match info.temp_schema_snapshot {
                        Some(snap) => *temp_db.db.schema.lock() = snap,
                        None => *temp_db.db.schema.lock() = conn.empty_temp_schema(),
                    }
                }
                *conn.database_schemas().write() = info.staged_schema_snapshot;
                conn.bump_prepare_context_generation();
            }

            // Invalidate cached schema cookies on ALL pagers whose pages may
            // have been rolled back. Next header read repopulates them from
            // the restored on-disk pages — which now match the in-memory
            // snapshot we just installed.
            conn.with_all_attached_pagers_with_index(|pagers| {
                for (_db_id, attached_pager) in pagers {
                    attached_pager.set_schema_cookie(None);
                }
            });
            pager.set_schema_cookie(None);
            conn.index_methods_on_savepoint_rolled_back();

            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

fn check_deferred_fk_on_commit(conn: &Connection) -> Result<()> {
    if !conn.foreign_keys_enabled() {
        return Ok(());
    }
    if conn.get_deferred_foreign_key_violations() > 0 {
        return Err(LimboError::ForeignKeyConstraint(
            "deferred foreign key constraint failed on commit".into(),
        ));
    }
    Ok(())
}

pub fn op_goto(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Goto { target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_gosub(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Gosub {
            target_pc,
            return_reg,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    state.registers[*return_reg].set_int((state.pc + 1) as i64);
    state.pc = target_pc.as_offset_int();
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_return(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Return {
            return_reg,
            can_fallthrough,
        },
        insn
    );
    if let Value::Numeric(Numeric::Integer(pc)) = state.registers[*return_reg].get_value() {
        let pc: u32 = (*pc)
            .try_into()
            .unwrap_or_else(|_| panic!("Return register is negative: {pc}"));
        state.pc = pc;
    } else {
        if unlikely(!*can_fallthrough) {
            return Err(
                LimboError::InternalError("Return register is not an integer".to_string()).into(),
            );
        }
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_integer(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Integer { value, dest }, insn);
    state.registers[*dest].set_int(*value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub enum OpProgramState {
    Start,
    /// Step state tracks whether we're executing a trigger subprogram (vs FK action subprogram)
    Step {
        is_trigger: bool,
        statement: Box<Statement>,
        /// Saved last_insert_rowid to restore after trigger subprogram completes.
        /// Per SQLite docs, trigger-body INSERTs must not overwrite the top-level rowid.
        saved_last_insert_rowid: Option<i64>,
        /// Saved connection-level `changes()` value to restore after a trigger subprogram.
        /// Trigger-body statements temporarily replace it via ResetCount, but the caller's
        /// value becomes visible again once the trigger returns.
        saved_changes_value: Option<i64>,
    },
}

impl Default for OpProgramState {
    fn default() -> Self {
        Self::Start
    }
}

fn finish_subprogram(
    program: &Program,
    statement: &Statement,
    is_trigger: bool,
    subprogram_aborted: bool,
    saved_last_insert_rowid: Option<i64>,
    saved_changes_value: Option<i64>,
) {
    let pending_changes = statement.n_total_change();
    if pending_changes != 0 {
        program.connection.add_total_changes(pending_changes);
    }

    // Only end trigger execution for normal completion. Error paths
    // already called end_trigger_execution() via abort() in the subprogram.
    if is_trigger && !subprogram_aborted {
        program.connection.end_trigger_execution();
    }

    // Restore last_insert_rowid after trigger execution, per SQLite semantics:
    // trigger-body INSERTs must not overwrite the top-level rowid.
    if let Some(rowid) = saved_last_insert_rowid {
        program.connection.update_last_rowid(rowid);
    }

    // Restore `changes()`, but not `total_changes()`
    if let Some(changes) = saved_changes_value {
        program.connection.set_changes(changes);
    }
}

pub fn op_reset_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    if !matches!(insn, Insn::ResetCount) {
        panic!("Expected Insn::ResetCount, got {insn:?}");
    }

    let nchange = state.n_change.swap(0, Ordering::SeqCst);
    let ntotal_change = state.n_total_change.swap(0, Ordering::SeqCst);
    program.connection.set_changes(nchange);
    program.connection.add_total_changes(ntotal_change);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_change_count(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ChangeCount { dest }, insn);
    let nchange = state.n_change.load(Ordering::SeqCst);
    state.registers[*dest].set_int(nchange);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Execute a subprogram (Program opcode).
/// Used for both triggers and FK actions (CASCADE, SET NULL, etc.)
pub fn op_program(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Program {
            param_registers,
            program: subprogram,
            ignore_jump_target,
        },
        insn
    );
    let subprogram = subprogram.prepared_program()?;
    loop {
        match std::mem::take(state.active_op_state.program()) {
            OpProgramState::Start => {
                // Try to reuse a cached statement for this PC, otherwise create a new one.
                // When we have triggers or fk-actions with multi-row inserts, we can re-use
                // cached statements by storing them key'd by the state.pc if we are in a loop
                let pc_key = state.pc as usize;
                let mut statement =
                    if let Some(mut cached) = state.subprogram_stmt_cache.remove(&pc_key) {
                        cached.reset_for_subprogram_reuse();
                        cached
                    } else {
                        Box::new(Statement::new_with_origin(
                            Program::from_prepared(subprogram.clone(), program.connection.clone()),
                            pager.clone(),
                            QueryMode::Normal,
                            0,
                            crate::statement::StatementOrigin::Subprogram,
                            false,
                        ))
                    };

                // Check if this is a trigger subprogram - if so, track execution
                // and save last_insert_rowid so it can be restored after the trigger finishes.
                let (is_trigger, saved_last_insert_rowid, saved_last_changes_value) =
                    if let Some(ref trigger) = statement.get_trigger() {
                        program.connection.start_trigger_execution(trigger.clone());
                        (
                            true,
                            Some(program.connection.last_insert_rowid()),
                            Some(program.connection.changes()),
                        )
                    } else {
                        (false, None, None)
                    };

                // Copy parameter values from parent registers into the subprogram's parameters.
                for (param_idx, &parent_reg) in param_registers.iter().enumerate() {
                    let value = state.registers[parent_reg].get_value().clone();
                    let param_index = NonZero::<usize>::new(param_idx + 1)
                        .expect("param_idx + 1 should be non-zero");
                    statement.bind_at(param_index, value)?;
                }

                *state.active_op_state.program() = OpProgramState::Step {
                    is_trigger,
                    statement,
                    saved_last_insert_rowid,
                    saved_changes_value: saved_last_changes_value,
                };
            }
            OpProgramState::Step {
                is_trigger,
                mut statement,
                saved_last_insert_rowid,
                saved_changes_value: saved_last_changes_value,
            } => {
                let mut raise_ignore = false;
                // Track whether the subprogram aborted with an error. When abort()
                // runs inside the subprogram, it already calls end_trigger_execution(),
                // so we must not call it again after the loop.
                let mut subprogram_aborted = false;
                loop {
                    let res = statement.step_subprogram();
                    match res {
                        Ok(step_result) => match step_result {
                            StepResult::Done => break,
                            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                                let io = statement
                                    .take_io_completions()
                                    .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                                *state.active_op_state.program() = OpProgramState::Step {
                                    is_trigger,
                                    statement,
                                    saved_last_insert_rowid,
                                    saved_changes_value: saved_last_changes_value,
                                };
                                return Ok(InsnFunctionStepResult::IO(io));
                            }
                            StepResult::Row => continue,
                            StepResult::Interrupt | StepResult::Busy => {
                                *state.active_op_state.program() = OpProgramState::Step {
                                    is_trigger,
                                    statement,
                                    saved_last_insert_rowid,
                                    saved_changes_value: saved_last_changes_value,
                                };
                                return Err(LimboError::Busy.into());
                            }
                        },
                        Err(LimboError::Constraint(constraint_err)) => {
                            if program.resolve_type != ResolveType::Ignore {
                                subprogram_aborted = true;
                                finish_subprogram(
                                    program,
                                    &statement,
                                    is_trigger,
                                    subprogram_aborted,
                                    saved_last_insert_rowid,
                                    saved_last_changes_value,
                                );
                                return Err(LimboError::Constraint(constraint_err).into());
                            }
                            subprogram_aborted = true;
                            break;
                        }
                        Err(LimboError::RaiseIgnore) => {
                            raise_ignore = true;
                            subprogram_aborted = true;
                            break;
                        }
                        Err(err) => {
                            subprogram_aborted = true;
                            finish_subprogram(
                                program,
                                &statement,
                                is_trigger,
                                subprogram_aborted,
                                saved_last_insert_rowid,
                                saved_last_changes_value,
                            );
                            return Err(err.into());
                        }
                    }
                }
                finish_subprogram(
                    program,
                    &statement,
                    is_trigger,
                    subprogram_aborted,
                    saved_last_insert_rowid,
                    saved_last_changes_value,
                );

                // Cache the statement for reuse on subsequent fires of this
                // same Program instruction (e.g. next row in an INSERT loop).
                // Only cache on clean completion - aborted statements have dirty
                // internal state and cannot be safely reused.
                if !subprogram_aborted {
                    let pc_key = state.pc as usize;
                    state.subprogram_stmt_cache.insert(pc_key, statement);
                }
                if raise_ignore {
                    // RAISE(IGNORE) — skip the current row by jumping to ignore_jump_target
                    state.pc = ignore_jump_target.as_offset_int();
                } else {
                    state.pc += 1;
                }
                state.active_op_state.clear();
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_real(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Real { value, dest }, insn);
    state.registers[*dest]
        .set_float(NonNan::new(*value).expect("f64 passed to op_real should be a valid NonNan"));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_real_affinity(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(RealAffinity { register }, insn);
    if let Value::Numeric(Numeric::Integer(i)) = &state.registers[*register].get_value() {
        state.registers[*register].set_float(
            NonNan::new(*i as f64)
                .expect("i64 passed to op_real_affinity should be a valid NonNan"),
        );
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_string8(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(String8 { value, dest }, insn);
    state.registers[*dest].set_text(Text::new(value.clone()))?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_blob(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Blob { value, dest }, insn);
    state.registers[*dest].set_blob(value.clone())?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_row_data(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(RowData { cursor_id, dest }, insn);

    // Copy the row into the destination register's spent record buffer: one
    // payload copy, no allocation once the buffer has grown to the row size.
    let buf = state.registers[*dest].take_buf();
    let record = {
        let cursor_ref = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "RowData");
        let cursor = cursor_ref.as_btree_mut();
        let record_option = return_if_io!(cursor.record());

        let record = record_option.ok_or_else(|| {
            mark_unlikely();
            LimboError::InternalError("RowData: cursor has no record".to_string())
        })?;

        ImmutableRecord::copy_payload(record.get_payload(), buf)?
    };

    let reg = &mut state.registers[*dest];
    *reg = Register::Record(record);

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug, Clone, Copy)]
pub enum OpRowIdState {
    Start,
    Record {
        index_cursor_id: usize,
        table_cursor_id: usize,
    },
    Seek {
        rowid: i64,
        table_cursor_id: usize,
    },
    GetRowid,
}

// Not in test builds: inline(always) makes fn-item coercions produce
// per-site copies in debug, breaking test_make_sure_correct_insn_table's
// pointer-identity check.
#[cfg_attr(not(test), inline(always))]
pub fn op_row_id(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(RowId { cursor_id, dest }, insn);
    // Fast path: no deferred seek pending and no suspended state machine, so
    // the op-state slot (enum write + drop on clear) is bypassed. On an IO
    // yield nothing is persisted and this path simply re-executes.
    if state.active_op_state.is_idle() && state.deferred_seeks[*cursor_id].is_none() {
        let result = op_row_id_read(state, *cursor_id, *dest)?;
        if matches!(result, InsnFunctionStepResult::Step) {
            state.pc += 1;
        }
        return Ok(result);
    }
    loop {
        match *state.active_op_state.row_id() {
            OpRowIdState::Start => {
                if let Some(deferred) = state.deferred_seeks[*cursor_id].take() {
                    *state.active_op_state.row_id() = OpRowIdState::Record {
                        index_cursor_id: deferred.index_cursor_id,
                        table_cursor_id: deferred.table_cursor_id,
                    };
                } else {
                    *state.active_op_state.row_id() = OpRowIdState::GetRowid;
                }
            }
            OpRowIdState::Record {
                index_cursor_id,
                table_cursor_id,
            } => {
                let rowid = {
                    let index_cursor = state.get_cursor(index_cursor_id);
                    match index_cursor {
                        Cursor::BTree(index_cursor) => {
                            let record = return_if_io!(index_cursor.record());
                            let record =
                                record.as_ref().expect("index cursor should have a record");
                            let rowid = record
                                .last_value()
                                .expect("record should have a last value");
                            match rowid {
                                Ok(ValueRef::Numeric(Numeric::Integer(rowid))) => rowid,
                                _ => unreachable!(),
                            }
                        }
                        Cursor::IndexMethod(index_cursor) => {
                            return_if_io!(index_cursor.query_rowid())
                                .expect("index cursor should have a rowid")
                        }
                        _ => panic!("unexpected cursor type"),
                    }
                };
                *state.active_op_state.row_id() = OpRowIdState::Seek {
                    rowid,
                    table_cursor_id,
                }
            }
            OpRowIdState::Seek {
                rowid,
                table_cursor_id,
            } => {
                {
                    let table_cursor = state.get_cursor(table_cursor_id);
                    let table_cursor = table_cursor.as_btree_mut();
                    return_if_io!(
                        table_cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true })
                    );
                }
                *state.active_op_state.row_id() = OpRowIdState::GetRowid;
            }
            OpRowIdState::GetRowid => {
                let result = op_row_id_read(state, *cursor_id, *dest)?;
                if !matches!(result, InsnFunctionStepResult::Step) {
                    // IO yield: the slot stays at GetRowid so the resume
                    // re-enters this arm.
                    return Ok(result);
                }
                break;
            }
        }
    }

    state.active_op_state.clear();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Reads the rowid of the cursor's current position into a register.
fn op_row_id_read(state: &mut ProgramState, cursor_id: usize, dest: usize) -> InsnResult {
    let cursor = state
        .cursors
        .get_mut(cursor_id)
        .expect("cursor_id should be valid");
    match cursor {
        Some(Cursor::NullRow) => {
            state.registers[dest].set_null();
        }
        Some(Cursor::BTree(btree_cursor)) => {
            if btree_cursor.get_null_flag() {
                state.registers[dest].set_null();
                return Ok(InsnFunctionStepResult::Step);
            }
            if let Some(rowid) = return_if_io!(btree_cursor.rowid()) {
                state.registers[dest].set_int(rowid);
            } else {
                state.registers[dest].set_null();
            }
        }
        Some(Cursor::Virtual(virtual_cursor)) => {
            let rowid = virtual_cursor.rowid();
            if rowid != 0 {
                state.registers[dest].set_int(rowid);
            } else {
                state.registers[dest].set_null();
            }
        }
        Some(Cursor::MaterializedView(mv_cursor)) => {
            if let Some(rowid) = return_if_io!(mv_cursor.rowid()) {
                state.registers[dest].set_int(rowid);
            } else {
                state.registers[dest].set_null();
            }
        }
        Some(Cursor::IndexMethod(cursor)) => {
            if let Some(rowid) = return_if_io!(cursor.query_rowid()) {
                state.registers[dest].set_int(rowid);
            } else {
                state.registers[dest].set_null();
            }
        }
        _ => {
            mark_unlikely();
            return Err(LimboError::InternalError(
                "RowId: cursor is not a table, virtual, or materialized view cursor".to_string(),
            )
            .into());
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_idx_row_id(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IdxRowId { cursor_id, dest }, insn);
    let cursors = &mut state.cursors;
    let cursor = cursors
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid")
        .as_mut()
        .expect("cursor should exist");

    let rowid = match cursor {
        Cursor::BTree(cursor) => return_if_io!(cursor.rowid()),
        Cursor::IndexMethod(cursor) => return_if_io!(cursor.query_rowid()),
        Cursor::NullRow => None,
        _ => panic!("unexpected cursor type"),
    };
    match rowid {
        Some(rowid) => state.registers[*dest].set_int(rowid),
        None => state.registers[*dest].set_null(),
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_rowid(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SeekRowid {
            cursor_id,
            src_reg,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    invalidate_deferred_seeks_for_cursor(state, *cursor_id);
    let (pc, did_seek) = {
        let cursor = get_cursor!(state, *cursor_id);

        // Handle MaterializedView cursor
        let (pc, did_seek) = match cursor {
            Cursor::MaterializedView(mv_cursor) => {
                let rowid = match state.registers[*src_reg].get_value() {
                    Value::Numeric(Numeric::Integer(rowid)) => Some(*rowid),
                    Value::Null => None,
                    _ => None,
                };

                match rowid {
                    Some(rowid) => {
                        let seek_result = return_if_io!(mv_cursor
                            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        let pc = if !matches!(seek_result, SeekResult::Found) {
                            target_pc.as_offset_int()
                        } else {
                            state.pc + 1
                        };
                        (pc, true)
                    }
                    None => (target_pc.as_offset_int(), false),
                }
            }
            Cursor::BTree(btree_cursor) => {
                let rowid = match state.registers[*src_reg].get_value() {
                    Value::Numeric(Numeric::Integer(rowid)) => Some(*rowid),
                    Value::Null => None,
                    // For non-integer values try to apply affinity and convert them to integer.
                    other => {
                        let mut temp_reg = Register::Value(other.clone());
                        let converted = apply_affinity_char(&mut temp_reg, Affinity::Numeric);
                        if converted {
                            match temp_reg.get_value() {
                                Value::Numeric(Numeric::Integer(i)) => Some(*i),
                                Value::Numeric(Numeric::Float(f)) => Some(f64::from(*f) as i64),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    }
                };

                match rowid {
                    Some(rowid) => {
                        let seek_result = return_if_io!(btree_cursor
                            .seek(SeekKey::TableRowId(rowid), SeekOp::GE { eq_only: true }));
                        let pc = if !matches!(seek_result, SeekResult::Found) {
                            target_pc.as_offset_int()
                        } else {
                            state.pc + 1
                        };
                        (pc, true)
                    }
                    None => (target_pc.as_offset_int(), false),
                }
            }
            _ => panic!("SeekRowid on non-btree/materialized-view cursor"),
        };
        (pc, did_seek)
    };
    // Increment btree_seeks metric for SeekRowid operation after cursor is dropped
    if did_seek {
        state.metrics.btree_seeks = state.metrics.btree_seeks.wrapping_add(1);
    }
    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_deferred_seek(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        DeferredSeek {
            index_cursor_id,
            table_cursor_id,
        },
        insn
    );
    state.deferred_seeks[*table_cursor_id] = Some(DeferredSeekState {
        index_cursor_id: *index_cursor_id,
        table_cursor_id: *table_cursor_id,
    });
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Separate enum for seek key to avoid lifetime issues
/// with using [SeekKey] - OpSeekState always owns the key,
/// unless it's [OpSeekKey::IndexKeyFromRegister] in which case the record
/// is owned by the program state's registers and we store the register number.
#[derive(Debug)]
pub enum OpSeekKey {
    TableRowId(i64),
    IndexKeyFromRegister(usize),
    IndexKeyUnpacked { start_reg: usize, num_regs: usize },
}

#[derive(Debug)]
pub enum OpSeekState {
    /// Initial state
    Start,
    /// Position cursor with seek operation with (rowid, op) search parameters
    Seek { key: OpSeekKey, op: SeekOp },
    /// Advance cursor (with [BTreeCursor::next]/[BTreeCursor::prev] methods) which was
    /// positioned after [OpSeekState::Seek] state if [BTreeCursor::seek] returned [SeekResult::TryAdvance]
    Advance { op: SeekOp },
    /// Move cursor to the last BTree row if DB knows that comparison result will be fixed (due to type ordering, e.g. NUMBER always <= TEXT)
    MoveLast,
}

pub fn op_seek(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    let (cursor_id, is_index, start_reg, num_regs, target_pc) = match insn {
        Insn::SeekGE {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        }
        | Insn::SeekLE {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        }
        | Insn::SeekGT {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        }
        | Insn::SeekLT {
            cursor_id,
            is_index,
            start_reg,
            num_regs,
            target_pc,
            ..
        } => (cursor_id, *is_index, *start_reg, *num_regs, target_pc),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    assert!(
        target_pc.is_offset(),
        "op_seek: target_pc should be an offset, is: {target_pc:?}"
    );
    let has_null_that_cannot_match = match insn {
        Insn::SeekGE {
            eq_only: true,
            null_matching_mask,
            ..
        }
        | Insn::SeekLE {
            eq_only: true,
            null_matching_mask,
            ..
        } => state.registers[start_reg..start_reg + num_regs]
            .iter()
            .enumerate()
            .any(|(i, value)| {
                // `IS` can match NULL. `=` cannot.
                value.is_null() && !null_matching_mask.get(i)
            }),
        _ => false,
    };

    if has_null_that_cannot_match {
        // Exact-match seeks use "=" semantics across the full unpacked key.
        // If any key column is NULL, the comparison is unknown, so no row can match.
        // Translation often emits IsNull guards earlier, but outer joins can still
        // null-extend these registers at runtime after non-null analysis has run.
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }

    let record_source = RecordSource::Unpacked {
        start_reg,
        num_regs,
    };

    let op = match insn {
        Insn::SeekGE { eq_only, .. } => SeekOp::GE { eq_only: *eq_only },
        Insn::SeekGT { .. } => SeekOp::GT,
        Insn::SeekLE { eq_only, .. } => SeekOp::LE { eq_only: *eq_only },
        Insn::SeekLT { .. } => SeekOp::LT,
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    match seek_internal(
        program,
        state,
        pager,
        record_source,
        *cursor_id,
        is_index,
        op,
    ) {
        Ok(SeekInternalResult::Found) => {
            state.metrics.search_count = state.metrics.search_count.wrapping_add(1);
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(SeekInternalResult::NotFound) => {
            state.metrics.search_count = state.metrics.search_count.wrapping_add(1);
            state.pc = target_pc.as_offset_int();
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(SeekInternalResult::IO(io)) => Ok(InsnFunctionStepResult::IO(io)),
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug)]
pub enum SeekInternalResult {
    Found,
    NotFound,
    IO(IOCompletions),
}
#[derive(Clone, Copy)]
pub enum RecordSource {
    Unpacked { start_reg: usize, num_regs: usize },
    Packed { record_reg: usize },
}

/// Internal function used by many VDBE instructions that need to perform a seek operation.
///
/// Explanation for some of the arguments:
/// - `record_source`: whether the seek key record is already a record (packed) or it will be constructed from registers (unpacked)
/// - `cursor_id`: the cursor id
/// - `is_index`: true if the cursor is an index, false if it is a table
/// - `op`: the [SeekOp] to perform
#[allow(clippy::too_many_arguments)]
pub fn seek_internal(
    program: &Program,
    state: &mut ProgramState,
    pager: &Arc<Pager>,
    record_source: RecordSource,
    cursor_id: usize,
    is_index: bool,
    op: SeekOp,
) -> Result<SeekInternalResult> {
    let mv_store = program.connection.mv_store();
    /// wrapper so we can use the ? operator and handle errors correctly in this outer function
    fn inner(
        _program: &Program,
        state: &mut ProgramState,
        _pager: &Arc<Pager>,
        _mv_store: Option<&Arc<MvStore>>,
        record_source: RecordSource,
        cursor_id: usize,
        is_index: bool,
        op: SeekOp,
    ) -> Result<SeekInternalResult> {
        loop {
            match &state.seek_state {
                OpSeekState::Start => {
                    if is_index {
                        // FIXME: text-to-numeric conversion should also happen here when applicable (when index column is numeric)
                        // See below for the table-btree implementation of this
                        match record_source {
                            RecordSource::Unpacked {
                                start_reg,
                                num_regs,
                            } => {
                                state.seek_state = OpSeekState::Seek {
                                    key: OpSeekKey::IndexKeyUnpacked {
                                        start_reg,
                                        num_regs,
                                    },
                                    op,
                                };
                            }
                            RecordSource::Packed { record_reg } => {
                                state.seek_state = OpSeekState::Seek {
                                    key: OpSeekKey::IndexKeyFromRegister(record_reg),
                                    op,
                                };
                            }
                        };
                        continue;
                    }
                    let RecordSource::Unpacked {
                        start_reg,
                        num_regs,
                    } = record_source
                    else {
                        unreachable!("op_seek: record_source should be Unpacked for table-btree");
                    };
                    assert_eq!(num_regs, 1, "op_seek: num_regs should be 1 for table-btree");
                    let original_value = state.registers[start_reg].get_value();
                    let mut temp_value = original_value.clone();

                    let conversion_successful = if matches!(temp_value, Value::Text(_)) {
                        let new_val = apply_numeric_affinity(temp_value.as_value_ref(), false);
                        let converted = new_val.is_some();
                        if let Some(new_val) = new_val {
                            temp_value = new_val.to_owned()?;
                        }
                        converted
                    } else {
                        true // Non-text values don't need conversion
                    };
                    let int_key = extract_int_value(&temp_value);
                    let lost_precision = !conversion_successful
                        || !matches!(temp_value, Value::Numeric(Numeric::Integer(_)));
                    let actual_op = if lost_precision {
                        match &temp_value {
                            Value::Numeric(Numeric::Float(f)) => {
                                let f_val = f64::from(*f);
                                // When extract_int_value clamped to i64::MAX/MIN,
                                // the cast `int_key as f64` loses the fact that the
                                // float is outside the i64 range. Detect this and
                                // set the comparison result directly.
                                //
                                // For i64::MAX: any float > 9223372036854774784.0
                                // is >= 9223372036854775808.0 (the next f64), which
                                // exceeds i64::MAX (9223372036854775807). i64::MAX
                                // is not exactly representable as f64, so the float
                                // is always strictly greater.
                                //
                                // For i64::MIN: -2^63 IS exactly representable as
                                // f64, so we must distinguish the exact match from
                                // floats that are strictly less.
                                let c = if int_key == i64::MAX && f_val > 9223372036854774784.0 {
                                    // Float exceeds i64::MAX, so int_key < float
                                    -1
                                } else if int_key == i64::MIN && f_val < -9223372036854774784.0 {
                                    if f_val == (i64::MIN as f64) {
                                        // Float is exactly i64::MIN
                                        0
                                    } else {
                                        // Float is below i64::MIN, so int_key > float
                                        1
                                    }
                                } else {
                                    let int_key_as_float = int_key as f64;
                                    if int_key_as_float > f_val {
                                        1
                                    } else if int_key_as_float < f_val {
                                        -1
                                    } else {
                                        0
                                    }
                                };

                                match c.cmp(&0) {
                                    std::cmp::Ordering::Less => match op {
                                        SeekOp::LT => SeekOp::LE { eq_only: false }, // (x < 5.1) -> (x <= 5)
                                        SeekOp::GE { .. } => SeekOp::GT, // (x >= 5.1) -> (x > 5)
                                        other => other,
                                    },
                                    std::cmp::Ordering::Greater => match op {
                                        SeekOp::GT => SeekOp::GE { eq_only: false }, // (x > 4.9) -> (x >= 5)
                                        SeekOp::LE { .. } => SeekOp::LT, // (x <= 4.9) -> (x < 5)
                                        other => other,
                                    },
                                    std::cmp::Ordering::Equal => op,
                                }
                            }
                            Value::Text(_) | Value::Blob(_) => {
                                match op {
                                    SeekOp::GT | SeekOp::GE { .. } => {
                                        // No integers are > or >= non-numeric text
                                        return Ok(SeekInternalResult::NotFound);
                                    }
                                    SeekOp::LT | SeekOp::LE { .. } => {
                                        // All integers are < or <= non-numeric text
                                        // Move to last position and then use the normal seek logic
                                        state.seek_state = OpSeekState::MoveLast;
                                        continue;
                                    }
                                }
                            }
                            _ => op,
                        }
                    } else {
                        op
                    };
                    let rowid = if matches!(original_value, Value::Null) {
                        match actual_op {
                            SeekOp::GE { .. } | SeekOp::GT => {
                                return Ok(SeekInternalResult::NotFound);
                            }
                            SeekOp::LE { .. } | SeekOp::LT => {
                                // No integers are < NULL
                                return Ok(SeekInternalResult::NotFound);
                            }
                        }
                    } else {
                        int_key
                    };
                    state.seek_state = OpSeekState::Seek {
                        key: OpSeekKey::TableRowId(rowid),
                        op: actual_op,
                    };
                    continue;
                }
                OpSeekState::Seek { key, op } => {
                    let seek_result = match key {
                        OpSeekKey::TableRowId(rowid) => {
                            let cursor = get_cursor!(state, cursor_id).as_btree_mut();
                            match cursor.seek(SeekKey::TableRowId(*rowid), *op)? {
                                IOResult::Done(seek_result) => seek_result,
                                IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                            }
                        }
                        OpSeekKey::IndexKeyFromRegister(record_reg) => {
                            let (cursor, record) = {
                                let (cursors, registers) = (&mut state.cursors, &state.registers);
                                let cursor = cursors
                                    .get_mut(cursor_id)
                                    .and_then(|c| c.as_mut())
                                    .expect("op_seek: cursor should be allocated")
                                    .as_btree_mut();
                                let record = match &registers[*record_reg] {
                                    Register::Record(ref record) => record,
                                    _ => unreachable!(
                                        "op_seek: record_reg should be a Record register when OpSeekKey::IndexKeyFromRegister is used"
                                    ),
                                };
                                (cursor, record)
                            };
                            match cursor.seek(SeekKey::IndexKey(record.as_record_ref()), *op)? {
                                IOResult::Done(seek_result) => seek_result,
                                IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                            }
                        }
                        OpSeekKey::IndexKeyUnpacked {
                            start_reg,
                            num_regs,
                        } => {
                            let start_reg = *start_reg;
                            let num_regs = *num_regs;
                            let cursor = get_cursor!(state, cursor_id).as_btree_mut();
                            let registers = &state.registers[start_reg..start_reg + num_regs];
                            match cursor.seek_unpacked(registers, *op)? {
                                IOResult::Done(seek_result) => seek_result,
                                IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                            }
                        }
                    };
                    // Increment btree_seeks metric after seek operation and cursor is dropped
                    state.metrics.btree_seeks = state.metrics.btree_seeks.wrapping_add(1);
                    let found = match seek_result {
                        SeekResult::Found => true,
                        SeekResult::NotFound => false,
                        SeekResult::TryAdvance => {
                            state.seek_state = OpSeekState::Advance { op: *op };
                            continue;
                        }
                    };
                    return Ok(if found {
                        SeekInternalResult::Found
                    } else {
                        SeekInternalResult::NotFound
                    });
                }
                OpSeekState::Advance { op } => {
                    let found = {
                        let cursor = get_cursor!(state, cursor_id);
                        let cursor = cursor.as_btree_mut();
                        // Seek operation has anchor number which equals to the closed boundary of the range
                        // (e.g. for >= x - anchor is x, for > x - anchor is x + 1)
                        //
                        // Before Advance state, cursor was positioned to the leaf page which should hold the anchor.
                        // Sometimes this leaf page can have no matching rows, and in this case
                        // we need to move cursor in the direction of Seek to find record which matches the seek filter
                        //
                        // Consider following scenario: Seek [> 666]
                        // interior page dividers:       I1: [ .. 667 .. ]
                        //                                       /   \
                        //             leaf pages:    P1[661,665]   P2[anything here is GT 666]
                        // After the initial Seek, cursor will be positioned after the end of leaf page P1 [661, 665]
                        // because this is potential position for insertion of value 666.
                        // But as P1 has no row matching Seek criteria - we need to move it to the right
                        // (and as we at the page boundary, we will move cursor to the next neighbor leaf, which guaranteed to have
                        // row keys greater than divider, which is greater or equal than anchor)

                        // this same logic applies for indexes, but the next/prev record is expected to be found in the parent page's
                        // divider cell.
                        turso_assert!(
                            !cursor.get_skip_advance(),
                            "skip_advance should not be true in the middle of a seek operation"
                        );
                        let result = match op {
                            // deliberately call get_next_record() instead of next() to avoid skip_advance triggering unwantedly
                            SeekOp::GT | SeekOp::GE { .. } => cursor.next()?,
                            SeekOp::LT | SeekOp::LE { .. } => cursor.prev()?,
                        };
                        match result {
                            IOResult::Done(()) => cursor.has_record(),
                            IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                        }
                    };
                    return Ok(if found {
                        SeekInternalResult::Found
                    } else {
                        SeekInternalResult::NotFound
                    });
                }
                OpSeekState::MoveLast => {
                    let cursor = state.get_cursor(cursor_id);
                    let cursor = cursor.as_btree_mut();
                    match cursor.last()? {
                        IOResult::Done(()) => {}
                        IOResult::IO(io) => return Ok(SeekInternalResult::IO(io)),
                    }
                    // The MoveLast variant is only used for SeekOp::LT and SeekOp::LE when
                    // the seek condition is true for every row, so the last row satisfies
                    // it — but an empty table has no row to sit on, and reporting Found
                    // there would make the scan loop emit one garbage row.
                    return Ok(if cursor.is_empty() {
                        SeekInternalResult::NotFound
                    } else {
                        SeekInternalResult::Found
                    });
                }
            }
        }
    }

    let result = inner(
        program,
        state,
        pager,
        mv_store.as_ref(),
        record_source,
        cursor_id,
        is_index,
        op,
    );
    if !matches!(result, Ok(SeekInternalResult::IO(..))) {
        state.seek_state = OpSeekState::Start;
    }
    result
}

/// Returns the tie-breaker ordering for SQLite index comparison opcodes.
///
/// When comparing index keys that omit the PRIMARY KEY/ROWID, SQLite uses a
/// tie-breaker value (`default_rc` in the C code) to determine the result when
/// the non-primary-key portions of the keys are equal.
///
/// This function extracts the appropriate tie-breaker based on the comparison opcode:
///
/// ## Tie-breaker Logic
///
/// - **`IdxLE` and `IdxGT`**: Return `Ordering::Less` (equivalent to `default_rc = -1`)
///   - When keys are equal, these operations should favor the "less than" result
///   - `IdxLE`: "less than or equal" - equality should be treated as "less"
///   - `IdxGT`: "greater than" - equality should be treated as "less" (so condition fails)
///
/// - **`IdxGE` and `IdxLT`**: Return `Ordering::Equal` (equivalent to `default_rc = 0`)
///   - When keys are equal, these operations should treat it as true equality
///   - `IdxGE`: "greater than or equal" - equality should be treated as "equal"
///   - `IdxLT`: "less than" - equality should be treated as "equal" (so condition fails)
///
/// ## SQLite Implementation Details
///
/// In SQLite's C implementation, this corresponds to:
/// ```c
/// if( pOp->opcode<OP_IdxLT ){
///     assert( pOp->opcode==OP_IdxLE || pOp->opcode==OP_IdxGT );
///     r.default_rc = -1;  // Ordering::Less
/// }else{
///     assert( pOp->opcode==OP_IdxGE || pOp->opcode==OP_IdxLT );
///     r.default_rc = 0;   // Ordering::Equal
/// }
/// ```
#[inline(always)]
fn get_tie_breaker_from_idx_comp_op(insn: &Insn) -> std::cmp::Ordering {
    match insn {
        Insn::IdxLE { .. } | Insn::IdxGT { .. } => std::cmp::Ordering::Less,
        Insn::IdxGE { .. } | Insn::IdxLT { .. } => std::cmp::Ordering::Equal,
        _ => panic!("Invalid instruction for index comparison"),
    }
}

#[allow(clippy::let_and_return)]
pub fn op_idx_ge(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IdxGE {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            // Create the comparison record from registers
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(
                idx_record,  // The serialized record from the index
                values,      // The record built from registers
                &index_info, // Sort order flags
                0,
                tie_breaker,
            )?;

            if ord.is_ge() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_seek_end(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(SeekEnd { cursor_id }, *insn);
    {
        let cursor = state.get_cursor(cursor_id);
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.seek_end());
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_le(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IdxLE {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(idx_record, values, &index_info, 0, tie_breaker)?;

            if ord.is_le() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_gt(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IdxGT {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);
            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(idx_record, values, &index_info, 0, tie_breaker)?;

            if ord.is_gt() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

#[allow(clippy::let_and_return)]
pub fn op_idx_lt(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IdxLT {
            cursor_id,
            start_reg,
            num_regs,
            target_pc,
        },
        insn
    );
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }

    let pc = {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_btree_mut();
        let index_info = cursor.get_index_info().clone();

        let pc = if let Some(idx_record) = return_if_io!(cursor.record()) {
            let values =
                registers_to_ref_values(&state.registers[*start_reg..*start_reg + *num_regs]);

            let tie_breaker = get_tie_breaker_from_idx_comp_op(insn);
            let ord = compare_records_generic(idx_record, values, &index_info, 0, tie_breaker)?;

            if ord.is_lt() {
                target_pc.as_offset_int()
            } else {
                state.pc + 1
            }
        } else {
            // No record at cursor position, jump to target
            target_pc.as_offset_int()
        };
        pc
    };

    state.pc = pc;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_decr_jump_zero(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DecrJumpZero { reg, target_pc }, insn);
    if !target_pc.is_offset() {
        crate::bail_corrupt_error!("Unresolved label: {target_pc:?}");
    }
    match &mut state.registers[*reg] {
        Register::Value(Value::Numeric(Numeric::Integer(n))) => {
            *n = n.saturating_sub(1);
            if *n == 0 {
                state.pc = target_pc.as_offset_int();
            } else {
                state.pc += 1;
            }
        }
        Register::Value(_) | Register::Record(_) => {
            bail_constraint_error!("datatype mismatch");
        }
        Register::Aggregate(_) => {
            mark_unlikely();
            return Err(LimboError::InternalError(
                "DecrJumpZero: unexpected aggregate register".into(),
            )
            .into());
        }
    }
    Ok(InsnFunctionStepResult::Step)
}

/// Add one floating-point value to a running sum using Kahan–Babuška–
/// Neumaier summation (KBN, referred to throughout the sum/avg code).
/// Adding many floats the naive way loses low-order bits; KBN keeps a
/// separate small "error" term (`state.r_err`) that captures the rounding
/// lost on each addition, so the final total stays far more accurate. This
/// matches how SQLite sums floating-point values.
fn apply_kbn_step(acc: &mut Value, r: f64, state: &mut SumAggState) {
    // NaN from Inf + (-Inf) is sticky: once acc is Null, it stays Null.
    // See https://sqlite.org/lang_aggfunc.html ("result is NULL").
    if matches!(acc, Value::Null) {
        return;
    }
    let s = acc.to_float_or_zero();
    let t = s + r;
    if t.is_nan() {
        *acc = Value::Null;
        return;
    }
    // When t is infinite, the KBN correction computes inf - inf = NaN,
    // which is meaningless. Skip compensation in that case.
    if t.is_finite() {
        let correction = if s.abs() > r.abs() {
            (s - t) + r
        } else {
            (r - t) + s
        };
        state.r_err += correction;
    }
    *acc = Value::from_f64(t);
}

// Add a (possibly large) integer to the running float sum. An integer big
// enough to lose precision as a single f64 is split into a high and a low
// part, each added separately, so no bits are dropped.
fn apply_kbn_step_int(acc: &mut Value, i: i64, state: &mut SumAggState) {
    const THRESHOLD: i64 = 4503599627370496; // 2^52

    if i <= -THRESHOLD || i >= THRESHOLD {
        let i_sm = i % 16384;
        let i_big = i - i_sm;

        apply_kbn_step(acc, i_big as f64, state);
        apply_kbn_step(acc, i_sm as f64, state);
    } else {
        apply_kbn_step(acc, i as f64, state);
    }
}

/// Switch sum() from its exact integer total to the floating-point
/// total, seeding it with the integer sum so far. A large integer can't
/// fit in a double exactly, so split it into a high part (the value) and
/// a low part (the running error term) and lose no bits. Mirrors
/// SQLite's `kahanBabuskaNeumaierInit` (func.c).
fn kbn_init_from_int(acc: &mut Value, i: i64, state: &mut SumAggState) {
    const THRESHOLD: i64 = 4503599627370496; // 2^52

    if i <= -THRESHOLD || i >= THRESHOLD {
        let i_sm = i % 16384;
        *acc = Value::from_f64((i - i_sm) as f64);
        state.r_err = i_sm as f64;
    } else {
        *acc = Value::from_f64(i as f64);
        state.r_err = 0.0;
    }
}

/// Initialize aggregate payload with default values.
/// Payload layout by aggregate type:
/// - Count/Count0: [Integer(0)]
/// - Sum: [Null, Float(0.0), Integer(0), Integer(0), Integer(0)]
///   // acc, r_err, approx, ovrfl, count
/// - Total: [Float(0.0), Float(0.0), Integer(0), Integer(0), Integer(0)]
///   // same but starts at 0.0
/// - Avg: [Float(0.0), Float(0.0), Integer(0)]  // sum, r_err, count - uses KBN like SUM
/// - Min/Max: [Null]
/// - GroupConcat/StringAgg:
///   [Null|Text, Integer(count), Integer(first separator length),
///   Blob(varying inter-string separator lengths)]
/// - JsonGroupObject/JsonbGroupObject: [Blob([])]
/// - JsonGroupArray/JsonbGroupArray: [Blob([])]
fn init_agg_payload(func: &AggFunc, payload: &mut crate::alloc::Vec<Value>) -> Result<()> {
    match func {
        AggFunc::Count | AggFunc::Count0 => payload.push(Value::from_i64(0)),
        AggFunc::Sum | AggFunc::Total => {
            let acc = if matches!(func, AggFunc::Total) {
                Value::from_f64(0.0)
            } else {
                Value::Null
            };
            payload.push(acc);
            payload.push(Value::from_f64(0.0));
            payload.push(Value::from_i64(0));
            payload.push(Value::from_i64(0));
            payload.push(Value::from_i64(0));
        }
        AggFunc::Avg => {
            payload.push(Value::from_f64(0.0));
            payload.push(Value::from_f64(0.0));
            payload.push(Value::from_i64(0));
        }
        AggFunc::Min | AggFunc::Max => payload.push(Value::Null),
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            // Use Null as sentinel to distinguish "no values yet" from an
            // accumulated empty string. SQLite normally remembers one
            // separator length and only materializes the per-separator queue
            // if the length changes.
            payload.push(Value::Null);
            payload.push(Value::from_i64(0));
            payload.push(Value::from_i64(0));
            payload.push(Value::Blob(crate::alloc::vec![]));
        }
        AggFunc::External(_) => {
            mark_unlikely();
            // External aggregates use ExternalAggState, not flat payload
            return Err(LimboError::InternalError(
                "External aggregate not supported in init_agg_payload".to_string(),
            ));
        }
        AggFunc::ArrayAgg => {
            // payload[0] = element count (Integer), remaining slots = accumulated values.
            // We serialize to a record blob only in finalize, avoiding O(n²) re-serialization.
            payload.push(Value::from_i64(0));
        }
        AggFunc::Mode => {
            // [0] = collation bits, [1] = count, [2..] = buffered values.
            payload.push(Value::from_i64(0)); // collation (recorded at step time)
            payload.push(Value::from_i64(0)); // count
        }
        AggFunc::PercentileCont | AggFunc::PercentileDisc => {
            // [0] = collation bits, [1] = count, [2] = fraction, [3..] = buffered values.
            payload.push(Value::from_i64(0)); // collation (recorded at step time)
            payload.push(Value::from_i64(0)); // count
            payload.push(Value::Null); // fraction (set on first step)
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            payload.push(Value::Blob(crate::alloc::vec![]));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            payload.push(Value::Blob(crate::alloc::vec![]));
        }
    };
    Ok(())
}

/// Process a single input row and update the aggregate state in the payload.
///
/// This is the core aggregation logic shared between both aggregation strategies:
/// - **Register-based (sort-stream)**: Called from `op_agg_step` (AggStep instruction).
///   The payload lives in `AggContext::Builtin` stored in a register.
/// - **Hash-based (future enhancement)**: Called from `step_aggregate` during HashAggStep. The payload lives
///   in hash table entries keyed by GROUP BY values.
///
/// The payload slice contains the intermediate aggregate state (initialized by
/// `init_agg_payload`), and this function incorporates the new row's values.
///
/// # Payload layouts (see `init_agg_payload` for initial values):
/// - **Count**: `[count: Integer]` - increments if arg is not NULL
/// - **Count0**: `[count: Integer]` - always increments (COUNT(*))
/// - **Avg**: `[sum: Float, r_err: Float, count: Integer]` - uses KBN compensation like SUM
/// - **Sum/Total**:
///   `[acc, r_err: Float, approx: Integer, ovrfl: Integer, count: Integer]`
///   - `acc`: running sum (Null/Integer/Float depending on inputs)
///   - `r_err`: Kahan-Babuška-Neumaier compensation term for floating-point precision
///   - `approx`: 1 if result is approximate (float arithmetic used)
///   - `ovrfl`: 1 if integer overflow occurred (Total promotes to float, Sum errors)
/// - **Min/Max**: `[current_extreme: Value]` - tracks min/max seen so far
/// - **GroupConcat/StringAgg**:
///   `[accumulated: Null|Text, count: Integer, first_separator_len: Integer,
///     separator_lengths: Blob]`
/// - **JsonGroup***: `[raw_jsonb: Blob]` - accumulated raw JSONB bytes
/// - **ArrayAgg/Mode/PercentileCont/PercentileDisc**: buffer every input value by growing the
///   payload `Vec` (one push per row); the leading slots hold a running count and, for the
///   ordered-set aggregates, the collation / percentile fraction to use at finalize time.
#[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::AggAccumulate)]
fn update_agg_payload(
    func: &AggFunc,
    arg: &Value,                // first argument
    maybe_arg2: Option<&Value>, // for GroupConcat/StringAgg, JsonGroupObject/JsonbGroupObject,
    payload: &mut crate::alloc::Vec<Value>,
    collation: CollationSeq,
    comparator: impl FnOnce() -> Result<Option<crate::vdbe::sorter::SortComparator>>,
) -> Result<()> {
    match func {
        AggFunc::Count => {
            // COUNT(column) increments only when arg is not NULL. Empty args treated as non-NULL
            // (would indicate a bug in query translation, but matches SQLite behavior of counting).
            if !matches!(arg, Value::Null) {
                // invariant as per init_agg_payload: payload[0] is always an integer
                let Value::Numeric(Numeric::Integer(i)) = &mut payload[0] else {
                    mark_unlikely();
                    return Err(LimboError::InternalError(
                        "Count: payload is not an integer".to_string(),
                    ));
                };
                *i = i.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
            }
        }
        AggFunc::Count0 => {
            // invariant as per init_agg_payload: payload[0] is always an integer
            let Value::Numeric(Numeric::Integer(i)) = &mut payload[0] else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "Count0: payload is not an integer".to_string(),
                ));
            };
            *i = i.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
        }
        AggFunc::Avg => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            // invariant as per init_agg_payload: payload[0] is Float (sum), payload[1] is Float (r_err), payload[2] is Integer (count)
            let [sum_val, r_err_val, count_val, ..] = payload.as_mut_slice() else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "Avg: payload too short".to_string(),
                ));
            };
            if matches!(*sum_val, Value::Null) {
                return Ok(());
            }
            let r_err = r_err_val.to_float_or_zero();
            let Value::Numeric(Numeric::Integer(count)) = count_val else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "Avg: payload[2] is not an integer".to_string(),
                ));
            };
            let mut sum_state = SumAggState {
                r_err,
                ..Default::default()
            };
            match classify_numeric_arg(arg) {
                NumericArg::Integer(i) => apply_kbn_step_int(sum_val, i, &mut sum_state),
                NumericArg::Float(f) => apply_kbn_step(sum_val, f, &mut sum_state),
                NumericArg::Null => unreachable!("NULL early-returned above"),
            }
            *r_err_val = Value::from_f64(sum_state.r_err);
            *count = count.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
        }
        AggFunc::Sum | AggFunc::Total => {
            // invariant as per init_agg_payload: payload[0] is acc (Null/Integer/Float),
            // payload[1] is Float (r_err), payload[2] is Integer (approx),
            // payload[3] is Integer (ovrfl), payload[4] is Integer (count)
            let [acc, r_err_val, approx_val, ovrfl_val, count_val, ..] = payload.as_mut_slice()
            else {
                return Err(LimboError::InternalError(
                    "Sum/Total: payload too short".to_string(),
                ));
            };
            let r_err_f = r_err_val.to_float_or_zero();
            let Value::Numeric(Numeric::Integer(approx_i)) = approx_val else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "Sum/Total: payload[2] is not an integer".to_string(),
                ));
            };
            let Value::Numeric(Numeric::Integer(ovrfl_i)) = ovrfl_val else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "Sum/Total: payload[3] is not an integer".to_string(),
                ));
            };
            let Value::Numeric(Numeric::Integer(count)) = count_val else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "Sum/Total: payload[4] is not an integer".to_string(),
                ));
            };
            let mut sum_state = SumAggState {
                r_err: r_err_f,
                approx: *approx_i != 0,
                ovrfl: *ovrfl_i != 0,
            };
            if !matches!(arg, Value::Null) {
                *count = count.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
            }
            if matches!(*acc, Value::Null) && sum_state.approx {
                return Ok(());
            }
            match arg {
                Value::Null => {}
                Value::Numeric(Numeric::Integer(i)) => match acc {
                    Value::Null => {
                        *acc = Value::from_i64(*i);
                    }
                    Value::Numeric(Numeric::Integer(acc_i)) => match acc_i.checked_add(*i) {
                        Some(sum) => *acc_i = sum,
                        None => {
                            // The integer total overflowed i64. Switch to
                            // the floating-point total and remember the
                            // overflow: sum() reports it at the end unless
                            // a later float clears it, and total() never
                            // reports it. Mirrors sumStep (func.c:1838-1846).
                            let prev = *acc_i;
                            sum_state.approx = true;
                            sum_state.ovrfl = true;
                            kbn_init_from_int(acc, prev, &mut sum_state);
                            apply_kbn_step_int(acc, *i, &mut sum_state);
                        }
                    },
                    Value::Numeric(Numeric::Float(_)) => {
                        apply_kbn_step_int(acc, *i, &mut sum_state);
                    }
                    _ => unreachable!("Sum/Total accumulator initialized to Null/Integer/Float"),
                },
                Value::Numeric(Numeric::Float(f)) => match acc {
                    Value::Null => {
                        *acc = Value::Numeric(Numeric::Float(*f));
                        sum_state.approx = true;
                    }
                    Value::Numeric(Numeric::Integer(i)) => {
                        // First float, after an all-integer run. Switch to
                        // the floating-point total, seeding it from the
                        // exact integer sum so far (func.c:1834-1836).
                        let prev = *i;
                        sum_state.approx = true;
                        kbn_init_from_int(acc, prev, &mut sum_state);
                        apply_kbn_step(acc, f64::from(*f), &mut sum_state);
                    }
                    Value::Numeric(Numeric::Float(_)) => {
                        // Already a floating-point total. A float input
                        // clears any earlier integer-overflow flag: the
                        // result is a float approximation regardless, so
                        // there's nothing to report (func.c:1852).
                        sum_state.approx = true;
                        sum_state.ovrfl = false;
                        apply_kbn_step(acc, f64::from(*f), &mut sum_state);
                    }
                    _ => unreachable!("Sum/Total accumulator initialized to Null/Integer/Float"),
                },
                Value::Text(t) => {
                    let (parse_result, parsed_number) = try_for_float(t.as_str().as_bytes());
                    handle_text_sum(acc, &mut sum_state, parsed_number, parse_result, false);
                }
                Value::Blob(b) => {
                    let (parse_result, parsed_number) = try_for_float(b);
                    handle_text_sum(acc, &mut sum_state, parsed_number, parse_result, true);
                }
            }
            *r_err_val = Value::from_f64(sum_state.r_err);
            *approx_i = sum_state.approx as i64;
            *ovrfl_i = sum_state.ovrfl as i64;
        }
        AggFunc::Min | AggFunc::Max => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            if matches!(payload[0], Value::Null) {
                payload[0].try_clone_from(arg)?;
                return Ok(());
            }
            use std::cmp::Ordering;
            let comparator = comparator()?;
            // Use custom type comparator if available, otherwise fall back to collation
            let cmp = if let Some(ref cmp_fn) = comparator {
                let arg_ref = arg.as_ref();
                let payload_ref = payload[0].as_ref();
                cmp_fn(&arg_ref, &payload_ref)?
            } else {
                compare_with_collation(arg, &payload[0], Some(collation), &comparator)?
            };
            let should_update = match func {
                AggFunc::Max => cmp == Ordering::Greater,
                AggFunc::Min => cmp == Ordering::Less,
                _ => false,
            };
            if should_update {
                payload[0].try_clone_from(arg)?;
            }
        }
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            let [acc, count_slot, first_separator_len_slot, separator_lengths_slot, ..] =
                payload.as_mut_slice()
            else {
                unreachable!("GroupConcat payload is initialized with four slots");
            };
            let Value::Numeric(Numeric::Integer(count)) = count_slot else {
                unreachable!("GroupConcat count slot is Integer");
            };
            let Value::Numeric(Numeric::Integer(first_separator_len)) = first_separator_len_slot
            else {
                unreachable!("GroupConcat first separator length is Integer");
            };
            let Value::Blob(separator_lengths) = separator_lengths_slot else {
                unreachable!("GroupConcat separator queue is Blob");
            };
            let first_term = matches!(acc, Value::Null);
            if first_term {
                // Start an empty Text accumulator; append below without an
                // intermediate String for Text inputs.
                *acc = Value::build_text(String::new());
            }

            let delimiter = match maybe_arg2 {
                Some(delimiter) => delimiter,
                None => &Value::build_text(","),
            };
            if first_term {
                let mut rendered = Value::build_text(String::new());
                rendered.exec_group_concat(delimiter)?;
                let Value::Text(rendered) = rendered else {
                    unreachable!("GroupConcat rendering target is Text");
                };
                *first_separator_len = i64::try_from(rendered.as_str().len())
                    .map_err(|_| LimboError::IntegerOverflow)?;
            } else {
                let before_separator = match acc {
                    Value::Text(text) => text.as_str().len(),
                    _ => unreachable!("GroupConcat accumulator is Text after initialization"),
                };
                acc.exec_group_concat(delimiter)?;
                let after_separator = match acc {
                    Value::Text(text) => text.as_str().len(),
                    _ => unreachable!("GroupConcat accumulator is Text after its first value"),
                };
                let separator_len = after_separator
                    .checked_sub(before_separator)
                    .expect("appending a separator cannot shrink GroupConcat");
                let separator_len =
                    i64::try_from(separator_len).map_err(|_| LimboError::IntegerOverflow)?;
                if separator_len != *first_separator_len || !separator_lengths.is_empty() {
                    let count = usize::try_from(*count).map_err(|_| LimboError::IntegerOverflow)?;
                    if separator_lengths.is_empty() {
                        let prior_separator_count = count.saturating_sub(1);
                        separator_lengths.try_reserve(
                            count
                                .checked_mul(std::mem::size_of::<i64>())
                                .ok_or(LimboError::IntegerOverflow)?,
                        )?;
                        let bytes = first_separator_len.to_be_bytes();
                        for _ in 0..prior_separator_count {
                            separator_lengths.extend_from_slice(&bytes);
                        }
                    } else {
                        separator_lengths.try_reserve(std::mem::size_of::<i64>())?;
                    }
                    separator_lengths.extend_from_slice(&separator_len.to_be_bytes());
                }
            }
            acc.exec_group_concat(arg)?;
            *count = count.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
        }
        AggFunc::External(_) => {
            mark_unlikely();
            return Err(LimboError::InternalError(
                "External aggregate not supported in update_agg_payload".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
            // arg = key, maybe_arg2 = value
            let Some(value) = maybe_arg2 else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "JsonGroupObject/JsonbGroupObject: no value provided".to_string(),
                ));
            };
            ensure_blob_arg_is_jsonb(value.as_value_ref())?;
            let mut key_vec = convert_dbtype_to_raw_jsonb(arg, Conv::ToString)?;
            let mut val_vec = convert_dbtype_to_raw_jsonb(value, Conv::NotStrict)?;
            let Value::Blob(vec) = &mut payload[0] else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "JsonGroupObject: payload[0] is not a blob".to_string(),
                ));
            };
            vec.try_reserve(usize::from(vec.is_empty()) + key_vec.len() + val_vec.len())?;
            if vec.is_empty() {
                // bits for obj header
                vec.push(12);
            }
            vec.append(&mut key_vec);
            vec.append(&mut val_vec);
        }
        AggFunc::ArrayAgg => {
            // Buffer every value (including NULLs) by growing the payload Vec; payload[0] is a
            // running count. O(1) per row.
            let count = payload[0].as_int().ok_or_else(|| {
                LimboError::InternalError("array_agg count slot must be an integer".into())
            })? as usize;
            payload[0] = Value::from_i64((count + 1) as i64);
            payload.try_push(arg.try_clone()?)?;
        }
        AggFunc::Mode => {
            // Record the value's collation (constant per group) for finalize-time sorting, then
            // buffer the value. Ordered-set aggregates ignore NULL inputs.
            payload[0] = Value::from_i64(collation.to_bits() as i64);
            if !matches!(arg, Value::Null) {
                let count = payload[1].as_int().unwrap_or(0) as usize;
                payload[1] = Value::from_i64((count + 1) as i64);
                payload.try_push(arg.try_clone()?)?;
            }
        }
        AggFunc::PercentileCont | AggFunc::PercentileDisc => {
            payload[0] = Value::from_i64(collation.to_bits() as i64);
            // The fraction is a per-group constant; record it on every step.
            if let Some(fraction) = maybe_arg2 {
                payload[2].try_clone_from(fraction)?;
            }
            if !matches!(arg, Value::Null) {
                let count = payload[1].as_int().unwrap_or(0) as usize;
                payload[1] = Value::from_i64((count + 1) as i64);
                payload.try_push(arg.try_clone()?)?;
            }
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray | AggFunc::JsonbGroupArray => {
            // arg = value
            ensure_blob_arg_is_jsonb(arg.as_value_ref())?;
            let mut data = convert_dbtype_to_raw_jsonb(arg, Conv::NotStrict)?;
            let Value::Blob(vec) = &mut payload[0] else {
                mark_unlikely();
                return Err(LimboError::InternalError(
                    "JsonGroupArray: payload[0] is not a blob".to_string(),
                ));
            };
            vec.try_reserve(usize::from(vec.is_empty()) + data.len())?;
            if vec.is_empty() {
                vec.push(11); // bits for array header
            }
            vec.append(&mut data);
        }
    }
    Ok(())
}

/// Convert the intermediate aggregate state in `payload` into the final result value.
///
/// This finalization logic is shared between both aggregation strategies:
/// - **Register-based (sort-stream)**: Called from `op_agg_final` (AggFinal/AggValue
///   instructions) when a group boundary is crossed.
/// - **Hash-based (future enhancement)**: Called during the emit phase of HashAggNext after all rows have
///   been processed. The payload may have been merged via `merge_agg_payload` if
///   spilling occurred.
///
/// # Finalization logic by aggregate type:
/// - **Count/Count0**: Returns the count directly
/// - **Avg**: Computes `sum / count`, returns NULL if count is 0
/// - **Sum**: Returns the accumulated value, applying Kahan compensation if approximate.
///   Returns NULL if no non-NULL values were seen (unless float arithmetic was used).
/// - **Total**: Like Sum but always returns Float, defaulting to 0.0 for empty groups
/// - **Min/Max**: Returns the tracked extreme value directly
/// - **GroupConcat/StringAgg**: Returns the accumulated string
/// - **JsonGroup***: Parses accumulated raw JSONB bytes into proper JSON output
fn finalize_agg_payload(func: &AggFunc, payload: &[Value]) -> Result<Value> {
    let val = match func {
        AggFunc::Count | AggFunc::Count0 => payload[0].clone(),
        AggFunc::Avg => {
            // Payload: [sum, r_err, count]
            let count = payload[2].as_int().unwrap_or(0);
            if count == 0 || matches!(&payload[0], Value::Null) {
                Value::Null
            } else {
                let sum = payload[0].to_float_or_zero();
                let r_err = payload[1].to_float_or_zero();
                // Apply KBN compensation before dividing
                Value::from_f64((sum + r_err) / count as f64)
            }
        }
        AggFunc::Sum => {
            let Value::Numeric(Numeric::Integer(count)) = &payload[4] else {
                unreachable!("Sum count slot is Integer per init_agg_payload");
            };
            if *count == 0 {
                return Ok(Value::Null);
            }
            let acc = &payload[0];
            let approx = payload[2].as_int().unwrap_or(0) != 0;
            let ovrfl = payload[3].as_int().unwrap_or(0) != 0;
            let r_err = payload[1].to_float_or_zero();
            // An all-integer sum that overflowed and was never cleared by
            // a float is reported here, at the end — not when it happened.
            // The frame may have shrunk back into range since, but SQLite
            // still reports it (sumFinalize). This check comes before the
            // NULL-accumulator (NaN) case below, matching SQLite's order.
            if approx && ovrfl {
                mark_unlikely();
                return Err(LimboError::IntegerOverflow);
            }
            match acc {
                Value::Null => Value::Null,
                Value::Numeric(Numeric::Integer(i)) if !approx => Value::from_i64(*i),
                // An infinite error term can't be added back meaningfully,
                // so return the total alone (sumFinalize's overflow guard).
                Value::Numeric(Numeric::Float(f)) if r_err.is_finite() => {
                    Value::from_f64(f64::from(*f) + r_err)
                }
                Value::Numeric(Numeric::Float(f)) => Value::from_f64(f64::from(*f)),
                _ => Value::from_f64(acc.to_float_or_zero() + r_err),
            }
        }
        AggFunc::Total => {
            // Payload: [acc, r_err, approx, ovrfl, count]
            let acc = &payload[0];
            let approx = payload[2].as_int().unwrap_or(0) != 0;
            let r_err = payload[1].to_float_or_zero();
            match acc {
                Value::Null if approx => Value::Null,
                Value::Null => Value::from_f64(0.0),
                Value::Numeric(Numeric::Integer(i)) => Value::from_f64(*i as f64 + r_err),
                Value::Numeric(Numeric::Float(f)) => Value::from_f64(f64::from(*f) + r_err),
                _ => unreachable!("Total accumulator initialized to Null/Integer/Float"),
            }
        }
        AggFunc::Min | AggFunc::Max => payload[0].clone(),
        AggFunc::GroupConcat | AggFunc::StringAgg => payload[0].clone(),
        AggFunc::ArrayAgg => {
            // payload[0] = count, payload[1..] = accumulated values.
            // Serialize to a record blob only once at finalization.
            let count = payload[0].as_int().unwrap_or(0) as usize;
            if count == 0 {
                Value::Null
            } else if 1 + count > payload.len() {
                return Err(LimboError::InternalError(format!(
                    "ArrayAgg: count ({count}) exceeds payload length ({})",
                    payload.len() - 1
                )));
            } else {
                let elements = &payload[1..1 + count];
                Value::Blob(ImmutableRecord::from_values(elements, count)?.into_payload())
            }
        }
        AggFunc::Mode => {
            // payload: [0]=collation bits, [1]=count, [2..]=buffered values.
            let collation =
                CollationSeq::from_storage_bits(payload[0].as_int().unwrap_or(0) as u16);
            let count = payload[1].as_int().unwrap_or(0) as usize;
            if count == 0 || 2 + count > payload.len() {
                Value::Null
            } else {
                ordered_set_mode(&payload[2..2 + count], collation)
            }
        }
        AggFunc::PercentileCont | AggFunc::PercentileDisc => {
            // payload: [0]=collation bits, [1]=count, [2]=fraction, [3..]=buffered values.
            // The fraction was range-checked in translate (pre-loop), so it is
            // here either NULL (→ NULL output, matching PG) or a valid number.
            if matches!(payload[2], Value::Null) {
                return Ok(Value::Null);
            }
            let collation =
                CollationSeq::from_storage_bits(payload[0].as_int().unwrap_or(0) as u16);
            let count = payload[1].as_int().unwrap_or(0) as usize;
            let fraction = payload[2].as_float();
            if count == 0 {
                Value::Null
            } else if 3 + count > payload.len() {
                return Err(LimboError::InternalError(format!(
                    "percentile: count ({count}) exceeds payload length ({})",
                    payload.len()
                )));
            } else {
                let values = &payload[3..3 + count];
                if matches!(func, AggFunc::PercentileDisc) {
                    ordered_set_percentile_disc(values, fraction, collation)
                } else {
                    ordered_set_percentile_cont(values, fraction)
                }
            }
        }
        AggFunc::External(_) => {
            mark_unlikely();
            // External aggregates are finalized via AggContext::compute_external()
            return Err(LimboError::InternalError(
                "finalize_agg_payload called for External aggregate".to_string(),
            ));
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, false)?
        }
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupObject => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, true)?
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupArray => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, false)?
        }
        #[cfg(feature = "json")]
        AggFunc::JsonbGroupArray => {
            let data = payload[0].to_blob().expect("Should be blob");
            json_from_raw_bytes_agg(data, true)?
        }
    };

    Ok(val)
}

/// Most frequent value of an ordered set (`mode() WITHIN GROUP (ORDER BY x)`).
/// Ties are broken by the smallest value, matching PostgreSQL (the first value the
/// ascending ordering would return).
/// Compares two ordered-set values, honoring `collation` for text (built-in and locale
/// collations are resolved without a connection; extension `Custom` collations fall back
/// to BINARY, matching other connection-less comparison paths).
fn ordered_set_compare(a: &Value, b: &Value, collation: CollationSeq) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Text(lhs), Value::Text(rhs)) => {
            collation.compare_strings(lhs.as_str(), rhs.as_str())
        }
        _ => a.cmp(b),
    }
}

fn ordered_set_mode(values: &[Value], collation: CollationSeq) -> Value {
    // NULLs were already dropped at step time.
    let mut sorted: Vec<Value> = values.to_vec();
    sorted.sort_by(|a, b| ordered_set_compare(a, b, collation));
    let mut best_idx = 0usize;
    let mut best_count = 0usize;
    let mut i = 0;
    while i < sorted.len() {
        let mut j = i + 1;
        while j < sorted.len()
            && ordered_set_compare(&sorted[j], &sorted[i], collation) == std::cmp::Ordering::Equal
        {
            j += 1;
        }
        // Strict `>` keeps the earliest (smallest, since sorted ascending) run on ties.
        if j - i > best_count {
            best_count = j - i;
            best_idx = i;
        }
        i = j;
    }
    sorted[best_idx].clone()
}

/// Continuous (interpolated) percentile of an ordered set.
///
/// Non-numeric values are ignored (PostgreSQL instead requires a numeric/interval input
/// type and errors otherwise, but columns are dynamically typed here). `percentile_disc`
/// differs: it returns the actual element of any type, so it does not filter.
fn ordered_set_percentile_cont(values: &[Value], fraction: f64) -> Value {
    let mut floats: Vec<f64> = values
        .iter()
        .filter_map(|v| match v {
            Value::Numeric(Numeric::Integer(i)) => Some(*i as f64),
            Value::Numeric(Numeric::Float(f)) => Some(f64::from(*f)),
            _ => None,
        })
        .collect();
    if floats.is_empty() {
        return Value::Null;
    }
    floats.sort_by(|a, b| a.total_cmp(b));
    let n = floats.len() as f64;
    let index = fraction * (n - 1.0);
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    if lower == upper {
        Value::from_f64(floats[lower])
    } else {
        let weight = index - lower as f64;
        Value::from_f64(floats[lower] * (1.0 - weight) + floats[upper] * weight)
    }
}

/// Discrete percentile of an ordered set: returns the actual element (in its original
/// type) at the position PostgreSQL would select — the smallest value whose cumulative
/// distribution (`row / n`) is at least `fraction`.
fn ordered_set_percentile_disc(values: &[Value], fraction: f64, collation: CollationSeq) -> Value {
    let mut sorted: Vec<Value> = values.to_vec();
    sorted.sort_by(|a, b| ordered_set_compare(a, b, collation));
    let n = sorted.len();
    let index = if fraction <= 0.0 {
        0
    } else {
        ((fraction * n as f64).ceil() as usize).saturating_sub(1)
    };
    sorted[index.min(n - 1)].clone()
}

/// Per-row step for pure window functions (those carried in
/// `AccumulatorFunc::Window`). Mirrors `op_agg_step` but with no FILTER and
/// per-function state semantics. State lives in the same
/// `Register::Aggregate(AggContext::Builtin(_))` slot used by built-in
/// aggregates so AggValue can read it back via the standard path. The `col`
/// argument is the register holding the function's single argument (when it
/// has one — 0-ary functions like row_number ignore it).
fn op_window_step(
    state: &mut ProgramState,
    acc_reg: usize,
    arg_reg: usize,
    func: &WindowFunc,
) -> InsnResult {
    match func {
        WindowFunc::RowNumber => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::from_i64(0)
                    ]?));
            }
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                unreachable!("row_number accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(counter)) = &mut payload[0] else {
                unreachable!("row_number counter must be Integer");
            };
            *counter += 1;
        }
        // rank() — mirrors SQLite's rankStepFunc.
        //
        // State (payload):
        //   [0] = current rank value (what AggValue returns; cleared to 0 by
        //         AggValue so the next step can detect that AggValue just ran).
        //   [1] = rows-seen counter for the partition (always increments).
        //
        // The "latch on zero" trick: AggValue clears payload[0] every time it
        // runs, and a flush only happens at peer-group / partition boundaries.
        // So when this step observes payload[0] == 0, it knows AggValue just
        // fired on the prior peer group — meaning this row begins a fresh peer
        // group, and the current row position (rows_seen) is the new rank.
        // Subsequent rows in the same peer group see payload[0] != 0 and leave
        // the rank value untouched, so every peer reads the same rank.
        WindowFunc::Rank => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::from_i64(0),
                        Value::from_i64(0),
                    ]?));
            }
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                unreachable!("rank accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(rows_seen)) = &mut payload[1] else {
                unreachable!("rank rows_seen counter must be Integer");
            };
            *rows_seen += 1;
            let rows_seen = *rows_seen;
            let Value::Numeric(Numeric::Integer(rank)) = &mut payload[0] else {
                unreachable!("rank current value must be Integer");
            };
            if *rank == 0 {
                *rank = rows_seen;
            }
        }
        // dense_rank() — mirrors SQLite's dense_rankStepFunc.
        //
        // State (payload):
        //   [0] = current dense_rank value (never cleared; persists across
        //         peer groups in a partition and resets at partition boundary
        //         via the Null on acc_reg).
        //   [1] = "pending bump" flag.
        //
        // Step sets the flag every source row (no-op past the first row of a
        // peer group since the flag is already 1). AggValue, called once per
        // peer-group flush, sees the flag set, bumps payload[0] by one, then
        // clears the flag. Subsequent source rows within the same peer group
        // re-set the flag, but no AggValue runs between them, so the bump
        // happens exactly once per peer group — yielding no gaps.
        WindowFunc::DenseRank => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::from_i64(0),
                        Value::from_i64(0),
                    ]?));
            }
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                unreachable!("dense_rank accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(pending)) = &mut payload[1] else {
                unreachable!("dense_rank pending flag must be Integer");
            };
            *pending = 1;
        }
        // first_value / nth_value are normally computed by a positional
        // seek at output time and never stepped. They reach this xStep
        // only in the EXCLUDE path, which rescans the frame per output row
        // and feeds each included row through here. Payload: [0] the
        // captured value, [1] a "captured yet?" flag, so a captured NULL
        // stays distinct from an empty frame.
        WindowFunc::FirstValue => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::Null,
                        Value::from_i64(0),
                    ]?));
            }
            let [arg_slot, acc_slot] = state
                .registers
                .get_disjoint_mut([arg_reg, acc_reg])
                .map_err(|_| {
                    LimboError::InternalError(format!(
                        "first_value: argument register {arg_reg} and accumulator register {acc_reg} must be distinct"
                    ))
                })?;
            let Register::Aggregate(AggContext::Builtin(payload)) = acc_slot else {
                unreachable!("first_value accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(seen)) = &payload[1] else {
                unreachable!("first_value seen flag must be Integer");
            };
            if *seen == 0 {
                payload[0].try_clone_from(arg_slot.get_value())?;
                payload[1] = Value::from_i64(1);
            }
        }
        // Only reached in the EXCLUDE rescan, like first_value above. Each
        // included row bumps the 1-based position in [1]; the argument is
        // captured into [0] when the position reaches N. N is re-validated
        // per row, matching SQLite.
        WindowFunc::NthValue => {
            if !coerce_register_to_integer(state, arg_reg + 1) {
                return Err(LimboError::InvalidArgument(
                    "second argument to nth_value must be a positive integer".into(),
                )
                .into());
            }
            let Value::Numeric(Numeric::Integer(n)) = state.registers[arg_reg + 1].get_value()
            else {
                unreachable!("coerce_register_to_integer must store an Integer");
            };
            let n = *n;
            if n <= 0 {
                return Err(LimboError::InvalidArgument(
                    "second argument to nth_value must be a positive integer".into(),
                )
                .into());
            }
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::Null,
                        Value::from_i64(0),
                    ]?));
            }
            let [arg_slot, acc_slot] = state
                .registers
                .get_disjoint_mut([arg_reg, acc_reg])
                .map_err(|_| {
                    LimboError::InternalError(format!(
                        "nth_value: argument register {arg_reg} and accumulator register {acc_reg} must be distinct"
                    ))
                })?;
            let Register::Aggregate(AggContext::Builtin(payload)) = acc_slot else {
                unreachable!("nth_value accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(step)) = &payload[1] else {
                unreachable!("nth_value step count must be Integer");
            };
            let step = step.checked_add(1).ok_or(LimboError::IntegerOverflow)?;
            payload[1] = Value::from_i64(step);
            if step == n {
                payload[0].try_clone_from(arg_slot.get_value())?;
            }
        }
        // last_value(expr) — mirrors SQLite's LastValueCtx {pVal, nVal}
        // (window.c:478-497): payload[0] holds the value of the most
        // recently stepped row, payload[1] counts the rows currently in
        // the frame. xInverse decrements the count as rows leave and
        // clears the value when the frame empties, so a moving frame that
        // empties out yields NULL.
        WindowFunc::LastValue => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::Null,
                        Value::from_i64(0),
                    ]?));
            }
            let [arg_slot, acc_slot] = state
                .registers
                .get_disjoint_mut([arg_reg, acc_reg])
                .map_err(|_| {
                    LimboError::InternalError(format!(
                        "last_value: argument register {arg_reg} and accumulator register {acc_reg} must be distinct"
                    ))
                })?;
            let Register::Aggregate(AggContext::Builtin(payload)) = acc_slot else {
                unreachable!("last_value accumulator must be a Builtin payload");
            };
            payload[0].try_clone_from(arg_slot.get_value())?;
            let Value::Numeric(Numeric::Integer(count)) = &mut payload[1] else {
                unreachable!("last_value frame-row count must be Integer");
            };
            *count += 1;
        }
        // percent_rank() / cume_dist() — mirror SQLite's CallCount-based
        // percent_rankStepFunc / cume_distStepFunc (window.c:328, :373).
        // Both share the same xStep semantics: count every row entering
        // the frame end. With end=UNBOUNDED FOLLOWING, that's every row
        // in the partition. xInverse and xValue do the rest.
        //
        // State (payload):
        //   [0] = nTotal (partition row count).
        //   [1] = nStep  (rows that have left the frame start).
        WindowFunc::PercentRank | WindowFunc::CumeDist => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::from_i64(0),
                        Value::from_i64(0),
                    ]?));
            }
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                unreachable!("percent_rank/cume_dist accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(ntotal)) = &mut payload[0] else {
                unreachable!("percent_rank/cume_dist nTotal must be Integer");
            };
            *ntotal += 1;
        }
        // ntile(N) — mirrors SQLite's NtileCtx (window.c:410). Frame is
        // ROWS CURRENT ROW TO UNBOUNDED FOLLOWING. The step fires for
        // every row entering the frame end (i.e. every row in the
        // partition); the inverse fires for every row leaving the
        // frame start, advancing the iRow counter that xValue uses to
        // pick a bucket.
        //
        // State (payload):
        //   [0] = nTotal — partition row count, incremented per step.
        //   [1] = nParam — the bucket count, captured from arg[0] on
        //         first step.
        //   [2] = iRow   — current output-row index within the
        //         partition, incremented per inverse.
        WindowFunc::Ntile => {
            if let Register::Value(Value::Null) = state.registers[acc_reg] {
                // ntile(N): N is coerced with SQLite's `sqlite3_value_int64`
                // rules — the leading integer prefix of text, blobs read as
                // text, floats truncated toward zero — then must be positive.
                let nparam = extract_int_value(state.registers[arg_reg].get_value());
                if nparam <= 0 {
                    return Err(LimboError::InvalidArgument(
                        "argument of ntile must be a positive integer".into(),
                    )
                    .into());
                }
                state.registers[acc_reg] =
                    Register::Aggregate(AggContext::Builtin(crate::alloc::try_vec![
                        Value::from_i64(0),
                        Value::from_i64(nparam),
                        Value::from_i64(0),
                    ]?));
            }
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                unreachable!("ntile accumulator must be a Builtin payload");
            };
            let Value::Numeric(Numeric::Integer(ntotal)) = &mut payload[0] else {
                unreachable!("ntile nTotal counter must be Integer");
            };
            *ntotal += 1;
        }
        other => {
            return Err(LimboError::InternalError(format!(
                "window function {other} reached runtime dispatch but has no handler"
            ))
            .into());
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Per-row value read for pure window functions.
fn op_window_value(
    state: &mut ProgramState,
    acc_reg: usize,
    dest_reg: usize,
    func: &WindowFunc,
) -> InsnResult {
    let value = match func {
        WindowFunc::RowNumber => match &state.registers[acc_reg] {
            Register::Aggregate(AggContext::Builtin(payload)) => payload[0].clone(),
            other => {
                return Err(LimboError::InternalError(format!(
                    "row_number accumulator in unexpected register state: {other:?}"
                ))
                .into());
            }
        },
        WindowFunc::Rank => {
            // Read current rank, then clear it so the next peer group's first
            // AggStep latches a fresh value (matches SQLite's rankValueFunc).
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                return Err(LimboError::InternalError(format!(
                    "rank accumulator in unexpected register state: {:?}",
                    state.registers[acc_reg]
                ))
                .into());
            };
            std::mem::replace(&mut payload[0], Value::from_i64(0))
        }
        WindowFunc::DenseRank => {
            // If the pending-bump flag is set, increment the dense_rank value
            // and clear the flag, then return the (possibly bumped) value.
            // Mirrors SQLite's dense_rankValueFunc.
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                return Err(LimboError::InternalError(format!(
                    "dense_rank accumulator in unexpected register state: {:?}",
                    state.registers[acc_reg]
                ))
                .into());
            };
            let Value::Numeric(Numeric::Integer(pending)) = &mut payload[1] else {
                unreachable!("dense_rank pending flag must be Integer");
            };
            let should_bump = *pending != 0;
            if should_bump {
                *pending = 0;
            }
            let Value::Numeric(Numeric::Integer(value_slot)) = &mut payload[0] else {
                unreachable!("dense_rank current value must be Integer");
            };
            if should_bump {
                *value_slot += 1;
            }
            Value::from_i64(*value_slot)
        }
        WindowFunc::FirstValue | WindowFunc::NthValue => {
            // Also EXCLUDE-only: streaming reads these by positional seek.
            // The rescan rebuilds the accumulator for each output row; here
            // we just read the captured value without consuming it. An
            // empty included set leaves the register NULL.
            match &state.registers[acc_reg] {
                Register::Aggregate(AggContext::Builtin(payload)) => payload[0].clone(),
                Register::Value(Value::Null) => Value::Null,
                other => {
                    return Err(LimboError::InternalError(format!(
                        "{func} accumulator in unexpected register state: {other:?}"
                    ))
                    .into());
                }
            }
        }
        WindowFunc::LastValue => {
            // xValue must not consume the accumulator: when the frame end
            // stops short of the last row of the partition, the same value
            // is read once for every row output, and a moving frame reads
            // it between AggStep calls. Mirrors SQLite's last_valueValueFunc
            // (window.c:524-529), which copies without clearing. If no row
            // ever entered the frame (an empty frame, or every row skipped
            // before the first step), the register was never written and is
            // still NULL, so the result is NULL — the same outcome SQLite
            // gives for an aggregate context that was never stepped.
            match &state.registers[acc_reg] {
                Register::Aggregate(AggContext::Builtin(payload)) => payload[0].clone(),
                Register::Value(Value::Null) => Value::Null,
                other => {
                    return Err(LimboError::InternalError(format!(
                        "last_value accumulator in unexpected register state: {other:?}"
                    ))
                    .into());
                }
            }
        }
        // percent_rank() — mirrors SQLite's percent_rankValueFunc
        // (window.c:352). The value is (rows in earlier peer groups) /
        // (nTotal - 1); 0.0 when there's only one row in the partition.
        WindowFunc::PercentRank => {
            let Register::Aggregate(AggContext::Builtin(payload)) = &state.registers[acc_reg]
            else {
                return Err(LimboError::InternalError(format!(
                    "percent_rank accumulator in unexpected register state: {:?}",
                    state.registers[acc_reg]
                ))
                .into());
            };
            let Value::Numeric(Numeric::Integer(ntotal)) = &payload[0] else {
                unreachable!("percent_rank nTotal must be Integer");
            };
            let Value::Numeric(Numeric::Integer(nstep)) = &payload[1] else {
                unreachable!("percent_rank nStep must be Integer");
            };
            let ntotal = *ntotal;
            let nstep = *nstep;
            let r = if ntotal > 1 {
                nstep as f64 / (ntotal - 1) as f64
            } else {
                0.0
            };
            Value::from_f64(r)
        }
        // cume_dist() — mirrors SQLite's cume_distValueFunc
        // (window.c:397). value = nStep / nTotal.
        WindowFunc::CumeDist => {
            let Register::Aggregate(AggContext::Builtin(payload)) = &state.registers[acc_reg]
            else {
                return Err(LimboError::InternalError(format!(
                    "cume_dist accumulator in unexpected register state: {:?}",
                    state.registers[acc_reg]
                ))
                .into());
            };
            let Value::Numeric(Numeric::Integer(ntotal)) = &payload[0] else {
                unreachable!("cume_dist nTotal must be Integer");
            };
            let Value::Numeric(Numeric::Integer(nstep)) = &payload[1] else {
                unreachable!("cume_dist nStep must be Integer");
            };
            let ntotal = *ntotal;
            let nstep = *nstep;
            // nTotal is incremented per xStep, so it's always >= 1 once
            // we're emitting rows from this partition. A divide-by-zero
            // here would mean we're computing cume_dist on an empty
            // partition, which can't happen.
            let r = nstep as f64 / ntotal as f64;
            Value::from_f64(r)
        }
        // ntile bucket computation — mirrors SQLite's ntileValueFunc
        // (window.c:453). The partition is split into nParam buckets;
        // when nTotal isn't a clean multiple, the first nLarge buckets
        // get one extra row each (size nSize+1) and the rest get nSize.
        // iRow is the 0-based output position within the partition.
        WindowFunc::Ntile => {
            let Register::Aggregate(AggContext::Builtin(payload)) = &state.registers[acc_reg]
            else {
                unreachable!("ntile accumulator must be a Builtin payload (xStep runs first)");
            };
            let Value::Numeric(Numeric::Integer(ntotal)) = &payload[0] else {
                unreachable!("ntile nTotal must be Integer");
            };
            let Value::Numeric(Numeric::Integer(nparam)) = &payload[1] else {
                unreachable!("ntile nParam must be Integer");
            };
            let Value::Numeric(Numeric::Integer(irow)) = &payload[2] else {
                unreachable!("ntile iRow must be Integer");
            };
            let ntotal = *ntotal;
            let nparam = *nparam;
            let irow = *irow;
            let nsize = ntotal / nparam;
            let bucket = if nsize == 0 {
                irow + 1
            } else {
                let nlarge = ntotal - nparam * nsize;
                let ismall = nlarge * (nsize + 1);
                if irow < ismall {
                    1 + irow / (nsize + 1)
                } else {
                    1 + nlarge + (irow - ismall) / nsize
                }
            };
            Value::from_i64(bucket)
        }
        other => {
            return Err(LimboError::InternalError(format!(
                "window function {other} reached runtime dispatch but has no handler"
            ))
            .into());
        }
    };
    state.registers[dest_reg].set_value(value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Per-row inverse-step for pure window functions. Companion to
/// `op_window_step`: fires when a row crosses the frame-start cursor on its
/// way out of the function's frame.
fn op_window_inverse(
    state: &mut ProgramState,
    acc_reg: usize,
    _arg_reg: usize,
    func: &WindowFunc,
) -> InsnResult {
    match func {
        // percent_rank / cume_dist xInverse — increment nStep, the
        // count of rows that have dropped off the start of the frame.
        // Under GROUPS mode AGGINVERSE fires xInverse once for every row
        // of the group that is leaving, so a group of size G adds G.
        WindowFunc::PercentRank | WindowFunc::CumeDist => {
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                return Err(LimboError::InternalError(format!(
                    "percent_rank/cume_dist accumulator in unexpected register state at inverse: {:?}",
                    state.registers[acc_reg]
                )).into());
            };
            let Value::Numeric(Numeric::Integer(nstep)) = &mut payload[1] else {
                unreachable!("percent_rank/cume_dist nStep must be Integer");
            };
            *nstep += 1;
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        // ntile's xInverse advances iRow — the output-row position
        // within the partition that xValue reads. xStep has already
        // populated nTotal and nParam by the time the flush loop fires
        // inverse, so the payload is always Builtin here.
        WindowFunc::Ntile => {
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                unreachable!(
                    "ntile accumulator must be a Builtin payload at inverse (xStep runs first)"
                );
            };
            let Value::Numeric(Numeric::Integer(irow)) = &mut payload[2] else {
                unreachable!("ntile iRow must be Integer");
            };
            *irow += 1;
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        // last_value's xInverse — a row left the frame from the left;
        // when none remain the captured value is cleared so xValue
        // yields NULL. Mirrors SQLite's last_valueInvFunc
        // (window.c:505-522).
        WindowFunc::LastValue => {
            let Register::Aggregate(AggContext::Builtin(payload)) = &mut state.registers[acc_reg]
            else {
                return Err(LimboError::InternalError(format!(
                    "last_value accumulator in unexpected register state at inverse: {:?}",
                    state.registers[acc_reg]
                ))
                .into());
            };
            let Value::Numeric(Numeric::Integer(count)) = &mut payload[1] else {
                unreachable!("last_value frame-row count must be Integer");
            };
            debug_assert!(*count > 0, "last_value xInverse without matching xStep");
            *count -= 1;
            if *count == 0 {
                payload[0] = Value::Null;
            }
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        // first_value / nth_value / lag / lead don't keep a running total
        // — at output time they just look up the row they need. So when a
        // row leaves the frame there is nothing to undo: this inverse step
        // does nothing. Mirrors SQLite wiring these functions' xInverse to
        // a no-op (window.c:591).
        WindowFunc::FirstValue | WindowFunc::NthValue | WindowFunc::Lag | WindowFunc::Lead => {
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        _ => unreachable!("AggInverse fired for {func} but no inverse arm is wired"),
    }
}

pub fn op_agg_inverse(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        AggInverse {
            acc_reg,
            col,
            delimiter: _,
            func,
            comparator: _,
        },
        insn
    );

    if let AccumulatorFunc::Window(win_func) = func {
        return op_window_inverse(state, *acc_reg, *col, win_func);
    }
    let func = func.expect_agg();

    // Run xInverse to undo a prior xStep when a row leaves the frame
    // from the left. min/max are handled separately using SQLite's
    // sorted-index strategy.
    let arg = state.registers[*col].get_value().clone();
    let Register::Aggregate(agg) = &mut state.registers[*acc_reg] else {
        return Err(LimboError::InternalError(format!(
            "AggInverse: acc_reg {} not initialized — xStep should have run first",
            *acc_reg
        ))
        .into());
    };
    let payload = agg.payload_mut();
    inverse_agg_payload(func, arg, payload)?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Classify an arg for SQLite-style sum/avg dispatch — mirrors
/// `sqlite3_value_numeric_type` (`vdbeapi.c`), applying numeric
/// affinity to TEXT/BLOB. Returns the parsed pieces so callers don't
/// re-parse, and so step and inverse agree on which path (integer or
/// float) handles each row.
enum NumericArg {
    Integer(i64),
    Float(f64),
    Null,
}

fn classify_numeric_arg(v: &Value) -> NumericArg {
    match v {
        Value::Null => NumericArg::Null,
        Value::Numeric(Numeric::Integer(i)) => NumericArg::Integer(*i),
        Value::Numeric(Numeric::Float(f)) => NumericArg::Float(f64::from(*f)),
        // A string that is entirely a valid integer (e.g. "42") is summed
        // on the exact-integer path, so a large integer written as text
        // doesn't lose precision. A string that is only a valid number up
        // to a point (e.g. "5abc") goes through the float path instead —
        // such partial parses shouldn't get exact integer accumulation.
        // Matches SQLite's numeric-affinity handling in `sumStep`.
        Value::Text(t) => match try_for_float(t.as_str().as_bytes()) {
            (NumericParseResult::ValidPrefixOnly, ParsedNumber::Integer(i)) => {
                NumericArg::Float(i as f64)
            }
            (_, ParsedNumber::Integer(i)) => NumericArg::Integer(i),
            (_, ParsedNumber::Float(f)) => NumericArg::Float(f),
            (_, ParsedNumber::None) => NumericArg::Float(0.0),
        },
        // A blob is always summed on the float path, even when its bytes
        // parse as an integer — matches SQLite's blob handling in `sumStep`.
        Value::Blob(b) => match try_for_float(b).1 {
            ParsedNumber::Integer(i) => NumericArg::Float(i as f64),
            ParsedNumber::Float(f) => NumericArg::Float(f),
            ParsedNumber::None => NumericArg::Float(0.0),
        },
    }
}

/// Add `i` to the exact-integer running sum. `i64::MIN` is handled in two
/// steps because it is the one value whose magnitude doesn't fit back into
/// an `i64`; splitting it keeps the sum on the exact-integer path (no loss
/// of precision past 2^53) instead of falling back to floating point.
/// Matches SQLite's `func.c:1875-1880`.
fn kbn_step_int_neg(acc: &mut Value, i: i64, state: &mut SumAggState) {
    if i == i64::MIN {
        apply_kbn_step_int(acc, i64::MAX, state);
        apply_kbn_step_int(acc, 1, state);
    } else {
        apply_kbn_step_int(acc, -i, state);
    }
}

/// Subtract the contribution of a single row from the aggregate state,
/// undoing a previous `update_agg_payload` call. Mirrors SQLite's
/// xInverse implementations at `func.c:1859-1986`.
fn inverse_agg_payload(func: &AggFunc, arg: Value, payload: &mut [Value]) -> Result<()> {
    match func {
        AggFunc::Count => {
            if !matches!(arg, Value::Null) {
                let Value::Numeric(Numeric::Integer(i)) = &mut payload[0] else {
                    unreachable!("Count payload is Integer per init_agg_payload");
                };
                // Matches SQLite's `assert(p->n>0)` at `func.c:1976` —
                // every xInverse is paired with a prior xStep for the
                // same arg, so the counter can never fall below zero.
                debug_assert!(*i > 0, "Count xInverse without matching xStep");
                *i -= 1;
            }
        }
        AggFunc::Count0 => {
            let Value::Numeric(Numeric::Integer(i)) = &mut payload[0] else {
                unreachable!("Count(*) payload is Integer per init_agg_payload");
            };
            debug_assert!(*i > 0, "Count(*) xInverse without matching xStep");
            *i -= 1;
        }
        AggFunc::Sum | AggFunc::Total => {
            let parsed = classify_numeric_arg(&arg);
            if matches!(parsed, NumericArg::Null) {
                return Ok(());
            }
            let [acc, r_err_val, approx_val, ovrfl_val, count_val, ..] = payload else {
                unreachable!(
                    "Sum/Total payload has acc/r_err/approx/ovrfl/count per init_agg_payload"
                );
            };
            let Value::Numeric(Numeric::Integer(approx_i)) = approx_val else {
                unreachable!("Sum/Total approx slot is Integer per init_agg_payload");
            };
            let Value::Numeric(Numeric::Integer(count)) = count_val else {
                unreachable!("Sum/Total count slot is Integer per init_agg_payload");
            };
            debug_assert!(*count > 0, "Sum/Total xInverse without matching xStep");
            *count -= 1;
            let r_err = r_err_val.to_float_or_zero();
            let ovrfl = matches!(ovrfl_val, Value::Numeric(Numeric::Integer(n)) if *n != 0);
            let mut sum_state = SumAggState {
                r_err,
                approx: true,
                ovrfl,
            };
            // Undo a row's contribution with an exact integer subtract,
            // but only while the total is still an exact integer (sum()
            // with no float yet; total() never qualifies, it starts as a
            // float). If that subtract overflows i64, switch to the
            // floating-point total and remember the overflow, just like
            // the step path — so sum() still reports it at the end.
            // Mirrors sumInverse (func.c). We only take this branch for
            // integer args; a float here is impossible, since a float
            // step would already have switched away from the exact total.
            if *approx_i == 0 {
                if let Value::Numeric(Numeric::Integer(acc_i)) = acc {
                    if let NumericArg::Integer(sub) = parsed {
                        match acc_i.checked_sub(sub) {
                            Some(x) => {
                                *acc_i = x;
                                return Ok(());
                            }
                            None => {
                                let prev = *acc_i;
                                sum_state.ovrfl = true;
                                kbn_init_from_int(acc, prev, &mut sum_state);
                                kbn_step_int_neg(acc, sub, &mut sum_state);
                                *approx_val = Value::from_i64(1);
                                *r_err_val = Value::from_f64(sum_state.r_err);
                                *ovrfl_val = Value::from_i64(sum_state.ovrfl as i64);
                                return Ok(());
                            }
                        }
                    }
                }
            }
            match parsed {
                NumericArg::Integer(i) => kbn_step_int_neg(acc, i, &mut sum_state),
                NumericArg::Float(f) => apply_kbn_step(acc, -f, &mut sum_state),
                NumericArg::Null => unreachable!("Null was early-returned above"),
            }
            *r_err_val = Value::from_f64(sum_state.r_err);
            *ovrfl_val = Value::from_i64(sum_state.ovrfl as i64);
        }
        AggFunc::Avg => {
            let parsed = classify_numeric_arg(&arg);
            if matches!(parsed, NumericArg::Null) {
                return Ok(());
            }
            let [sum_val, r_err_val, count_val, ..] = payload else {
                unreachable!("Avg payload has sum/r_err/count per init_agg_payload");
            };
            let r_err = r_err_val.to_float_or_zero();
            let Value::Numeric(Numeric::Integer(count)) = count_val else {
                unreachable!("Avg count slot is Integer per init_agg_payload");
            };
            debug_assert!(*count > 0, "Avg xInverse without matching xStep");
            let mut sum_state = SumAggState {
                r_err,
                ..Default::default()
            };
            match parsed {
                NumericArg::Integer(i) => kbn_step_int_neg(sum_val, i, &mut sum_state),
                NumericArg::Float(f) => apply_kbn_step(sum_val, -f, &mut sum_state),
                NumericArg::Null => unreachable!("Null was early-returned above"),
            }
            *r_err_val = Value::from_f64(sum_state.r_err);
            *count -= 1;
        }
        AggFunc::GroupConcat | AggFunc::StringAgg => {
            if matches!(arg, Value::Null) {
                return Ok(());
            }
            let [acc, count_slot, first_separator_len_slot, separator_lengths_slot, ..] = payload
            else {
                unreachable!("GroupConcat payload is initialized with four slots");
            };
            let Value::Numeric(Numeric::Integer(count)) = count_slot else {
                unreachable!("GroupConcat count slot is Integer");
            };
            let Value::Numeric(Numeric::Integer(first_separator_len)) = first_separator_len_slot
            else {
                unreachable!("GroupConcat first separator length is Integer");
            };
            let Value::Blob(separator_lengths) = separator_lengths_slot else {
                unreachable!("GroupConcat separator queue is Blob");
            };
            debug_assert!(*count > 0, "GroupConcat xInverse without matching xStep");
            let old_count = usize::try_from(*count)
                .map_err(|_| LimboError::InternalError("negative GroupConcat count".into()))?;
            let expected_separator_bytes = old_count
                .saturating_sub(1)
                .checked_mul(std::mem::size_of::<i64>())
                .ok_or(LimboError::IntegerOverflow)?;
            if !separator_lengths.is_empty() && separator_lengths.len() != expected_separator_bytes
            {
                return Err(LimboError::InternalError(format!(
                    "GroupConcat separator queue has {} bytes for {old_count} values",
                    separator_lengths.len()
                )));
            }

            let mut rendered = Value::build_text(String::new());
            rendered.exec_group_concat(&arg)?;
            let Value::Text(rendered) = rendered else {
                unreachable!("GroupConcat rendering target is Text");
            };
            let value_len = rendered.as_str().len();

            *count -= 1;
            let separator_len = if !separator_lengths.is_empty() && *count > 0 {
                let bytes: [u8; std::mem::size_of::<i64>()] = separator_lengths
                    [..std::mem::size_of::<i64>()]
                    .try_into()
                    .expect("separator queue length checked above");
                separator_lengths.drain(..std::mem::size_of::<i64>());
                usize::try_from(i64::from_be_bytes(bytes))
                    .map_err(|_| LimboError::IntegerOverflow)?
            } else if separator_lengths.is_empty() {
                usize::try_from(*first_separator_len).map_err(|_| {
                    LimboError::InternalError("negative GroupConcat separator length".into())
                })?
            } else {
                0
            };
            let remove_len = value_len
                .checked_add(separator_len)
                .ok_or(LimboError::IntegerOverflow)?;
            let Value::Text(text) = acc else {
                unreachable!("GroupConcat accumulator is Text after a non-NULL xStep");
            };
            let accumulated_len = text.as_str().len();
            if remove_len < accumulated_len && !text.as_str().is_char_boundary(remove_len) {
                return Err(LimboError::InternalError(format!(
                    "GroupConcat xInverse removes {remove_len} bytes from {accumulated_len}"
                )));
            }
            if remove_len >= accumulated_len {
                *acc = Value::Null;
                separator_lengths.clear();
            } else {
                text.value.to_mut().drain(..remove_len);
            }
        }
        AggFunc::Min
        | AggFunc::Max
        | AggFunc::ArrayAgg
        | AggFunc::Mode
        | AggFunc::PercentileCont
        | AggFunc::PercentileDisc => {
            // Aggregates without an xInverse are rejected for moving-start
            // frames, so reaching this arm is a planner regression.
            unreachable!("planner should reject moving-start frame over non-invertible {func}");
        }
        AggFunc::External(_) => {
            unreachable!("planner rejects moving-start frames over external aggregates");
        }
        #[cfg(feature = "json")]
        AggFunc::JsonGroupObject
        | AggFunc::JsonbGroupObject
        | AggFunc::JsonGroupArray
        | AggFunc::JsonbGroupArray => {
            let Value::Blob(data) = &mut payload[0] else {
                unreachable!("JSON group accumulator payload is Blob");
            };
            let expected_container =
                if matches!(func, AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject) {
                    12
                } else {
                    11
                };
            if data.first().map(|header| header & 0x0f) != Some(expected_container) {
                return Err(LimboError::InternalError(
                    "JSON group xInverse found an invalid accumulator header".into(),
                ));
            }
            let element_count = if expected_container == 12 { 2 } else { 1 };
            let mut end = 1usize;
            for _ in 0..element_count {
                end = end
                    .checked_add(raw_jsonb_element_len(data, end)?)
                    .ok_or(LimboError::IntegerOverflow)?;
            }
            data.drain(1..end);
        }
    }
    Ok(())
}

pub fn op_agg_step(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(AggStep { data }, insn);
    let AggStepData {
        acc_reg,
        col,
        delimiter,
        func,
        comparator,
        collation,
    } = data.as_ref();

    if let AccumulatorFunc::Window(win_func) = func {
        return op_window_step(state, *acc_reg, *col, win_func);
    }
    let func = func.expect_agg();

    // Initialize aggregate state if not already done
    if let Register::Value(Value::Null) = state.registers[*acc_reg] {
        state.registers[*acc_reg] = match func {
            AggFunc::External(ext_func) => match ext_func.as_ref() {
                ExtFunc::Aggregate {
                    context,
                    init,
                    step,
                    finalize,
                    argc,
                    aggregate_destructor,
                    value_destructor,
                    ..
                } => Register::Aggregate(AggContext::External(ExternalAggState {
                    context: *context,
                    state: unsafe { (init)(*context) },
                    argc: (*argc).max(0) as usize,
                    step_fn: *step,
                    finalize_fn: *finalize,
                    aggregate_destructor: *aggregate_destructor,
                    value_destructor: *value_destructor,
                })),
                _ => unreachable!("scalar function called in aggregate context"),
            },
            _ => {
                // Built-in aggregates use flat payload
                let mut payload = crate::alloc::vec![];
                init_agg_payload(func, &mut payload)?;
                Register::Aggregate(AggContext::Builtin(payload))
            }
        };
    }

    let current_collation = collation.unwrap_or(CollationSeq::Binary);
    let comparator_factory = move || -> Result<Option<crate::vdbe::sorter::SortComparator>> {
        Ok(match comparator.as_ref() {
            Some(comparator) => Some(make_sort_comparator(comparator)?),
            None if current_collation.is_custom() => Some(
                program
                    .connection
                    .make_collation_comparator(current_collation)?,
            ),
            None => None,
        })
    };

    // Step the aggregate
    match func {
        AggFunc::External(_) => {
            // External aggregates use FFI and need special handling
            let (context, step_fn, state_ptr, argc, aggregate_destructor, value_destructor) = {
                let Register::Aggregate(agg) = &state.registers[*acc_reg] else {
                    unreachable!();
                };
                let AggContext::External(agg_state) = agg else {
                    unreachable!();
                };
                (
                    agg_state.context,
                    agg_state.step_fn,
                    agg_state.state,
                    agg_state.argc,
                    agg_state.aggregate_destructor,
                    agg_state.value_destructor,
                )
            };
            let mut ext_values = Vec::with_capacity(argc);
            if argc != 0 {
                let register_slice = &state.registers[*col..*col + argc];
                for ov in register_slice.iter() {
                    ext_values.push(ov.get_value().to_ffi());
                }
            }
            let argv_ptr = if ext_values.is_empty() {
                std::ptr::null()
            } else {
                ext_values.as_ptr()
            };
            let mut result = unsafe { step_fn(context, state_ptr, argc as i32, argv_ptr) };
            let value = Value::from_ffi_ref(&result);
            if let Some(value_destructor) = value_destructor {
                unsafe { value_destructor(&mut result) };
            } else {
                unsafe { result.__free_internal_type() };
            }
            for ext_value in ext_values {
                unsafe { ext_value.__free_internal_type() };
            }
            if let Err(err) = value {
                if let Some(aggregate_destructor) = aggregate_destructor {
                    unsafe { aggregate_destructor(state_ptr as usize) };
                }
                state.registers[*acc_reg].set_value(Value::Null);
                return Err(err.into());
            }
        }
        _ => {
            let maybe_arg2 = match func {
                AggFunc::GroupConcat | AggFunc::StringAgg => {
                    Some(state.registers[*delimiter].get_value().try_clone()?)
                }
                #[cfg(feature = "json")]
                AggFunc::JsonGroupObject | AggFunc::JsonbGroupObject => {
                    Some(state.registers[*delimiter].get_value().try_clone()?)
                }
                AggFunc::PercentileCont | AggFunc::PercentileDisc => {
                    Some(state.registers[*delimiter].get_value().try_clone()?)
                }
                _ => None,
            };

            let [arg_reg, acc_slot] =
                state
                    .registers
                    .get_disjoint_mut([*col, *acc_reg])
                    .map_err(|_| {
                        LimboError::InternalError(format!(
                        "AggStep: input register {} and accumulator register {} must be distinct",
                        *col, *acc_reg
                    ))
                    })?;
            let arg = arg_reg.get_value();
            let Register::Aggregate(agg) = acc_slot else {
                return Err(LimboError::InternalError(format!(
                    "AggStep: register {} does not hold an aggregate accumulator",
                    *acc_reg
                ))
                .into());
            };
            let payload = agg.payload_vec_mut();
            update_agg_payload(
                func,
                arg,
                maybe_arg2.as_ref(),
                payload,
                current_collation,
                comparator_factory,
            )?;
        }
    };

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_agg_final(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    let (acc_reg, dest_reg, func) = match insn {
        Insn::AggFinal { register, func } => (*register, *register, func),
        Insn::AggValue {
            acc_reg,
            dest_reg,
            func,
        } => (*acc_reg, *dest_reg, func),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };

    if let AccumulatorFunc::Window(win_func) = func {
        return op_window_value(state, acc_reg, dest_reg, win_func);
    }
    let func = func.expect_agg();

    match &state.registers[acc_reg] {
        Register::Aggregate(agg) => {
            let value = match agg {
                AggContext::External(_) => {
                    // External aggregates use FFI finalization
                    agg.compute_external()?
                }
                AggContext::Builtin(payload) => {
                    // Built-in aggregates use shared finalization
                    finalize_agg_payload(func, payload)?
                }
            };
            state.registers[dest_reg].set_value(value);
        }
        Register::Value(Value::Null) => {
            // No row was stepped: write the empty-set default explicitly.
            // For window aggregates `dest_reg` differs from `acc_reg` and may
            // still hold an earlier partition's result.
            match func {
                AggFunc::Total => {
                    state.registers[dest_reg]
                        .set_float(NonNan::new(0.0).expect("0.0 is a valid NonNan"));
                }
                AggFunc::Count | AggFunc::Count0 => {
                    state.registers[dest_reg].set_int(0);
                }
                #[cfg(feature = "json")]
                AggFunc::JsonGroupArray => {
                    state.registers[dest_reg].set_text(Text::json("[]".to_string()))?;
                }
                #[cfg(feature = "json")]
                AggFunc::JsonbGroupArray => {
                    state.registers[dest_reg]
                        .set_blob(json::jsonb::Jsonb::make_empty_array(1)?.data())?;
                }
                #[cfg(feature = "json")]
                AggFunc::JsonGroupObject => {
                    state.registers[dest_reg].set_text(Text::json("{}".to_string()))?;
                }
                #[cfg(feature = "json")]
                AggFunc::JsonbGroupObject => {
                    state.registers[dest_reg]
                        .set_blob(json::jsonb::Jsonb::make_empty_obj(1)?.data())?;
                }
                AggFunc::External(ext_func) => {
                    let value = match ext_func.as_ref() {
                        ExtFunc::Aggregate {
                            context,
                            init,
                            finalize,
                            aggregate_destructor,
                            value_destructor,
                            ..
                        } => {
                            let aggregate_context = unsafe { init(*context) };
                            let mut result = unsafe { finalize(*context, aggregate_context) };
                            let value = Value::from_ffi_ref(&result);
                            if let Some(value_destructor) = value_destructor {
                                unsafe { value_destructor(&mut result) };
                            } else {
                                unsafe { result.__free_internal_type() };
                            }
                            if let Some(aggregate_destructor) = aggregate_destructor {
                                unsafe { aggregate_destructor(aggregate_context as usize) };
                            }
                            value?
                        }
                        _ => unreachable!("scalar function called in aggregate context"),
                    };
                    state.registers[dest_reg].set_value(value);
                }
                _ => {
                    state.registers[dest_reg].set_value(Value::Null);
                }
            }
        }
        other => {
            panic!("Unexpected value {other:?} in AggFinal");
        }
    };

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_open(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(SorterOpen { data }, insn);
    let SorterOpenData {
        cursor_id,
        columns: _,
        order_collations_nulls,
        comparators,
    } = data.as_ref();
    // be careful here - we must not use any async operations after pager.with_header because this op-code has no proper state-machine
    let page_size = match pager.with_header(|header| header.page_size) {
        Ok(IOResult::Done(page_size)) => page_size,
        Err(_) => PageSize::default(),
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
    };
    let page_size = page_size.get() as usize;

    let cache_size = program.connection.get_cache_size();

    // Set the buffer size threshold to be roughly the same as the limit configured for the page-cache.
    let max_buffer_size_bytes = if cache_size < 0 {
        (cache_size.unsigned_abs() as usize).saturating_mul(1024)
    } else {
        (cache_size as usize).saturating_mul(page_size)
    };
    let mut order = Vec::try_with_capacity_ext(order_collations_nulls.len())?;
    let mut collations = crate::alloc::Vec::try_with_capacity_ext(order_collations_nulls.len())?;
    let mut nulls_orders = crate::alloc::Vec::try_with_capacity_ext(order_collations_nulls.len())?;
    for (ord, coll, nulls) in order_collations_nulls.iter() {
        // Preallocated for every ORDER BY term above, so these pushes cannot grow the vectors.
        order.push(*ord);
        collations.push(coll.unwrap_or_default());
        nulls_orders.push(*nulls);
    }
    let mut sort_comparators =
        crate::alloc::Vec::try_with_capacity_ext(order_collations_nulls.len())?;
    for (idx, (_, coll, _)) in order_collations_nulls.iter().enumerate() {
        let comparator = match comparators.get(idx).and_then(|c| c.as_ref()) {
            Some(comparator) => Some(make_sort_comparator(comparator)?),
            None => match coll {
                Some(collation) if collation.is_custom() => {
                    Some(program.connection.make_collation_comparator(*collation)?)
                }
                _ => None,
            },
        };
        // Preallocated for every ORDER BY term above, so this push cannot grow the vector.
        sort_comparators.push(comparator);
    }
    let temp_store = program.connection.get_temp_store();
    let cursor = Sorter::new(
        &order,
        collations,
        nulls_orders,
        sort_comparators,
        max_buffer_size_bytes,
        page_size,
        pager.io.clone(),
        temp_store,
    )?;
    let cursors = &mut state.cursors;
    cursors
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid")
        .replace(Cursor::new_sorter(cursor));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_data(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SorterData {
            cursor_id,
            dest_reg,
            pseudo_cursor,
        },
        insn
    );
    // Column ops read the record through the pseudo cursor's content register,
    // so SorterData's destination must be that same register.
    {
        let content_reg = state
            .get_cursor(*pseudo_cursor)
            .as_pseudo_mut()
            .content_reg();
        if content_reg != *dest_reg {
            return Err(LimboError::InternalError(format!(
                "SorterData: destination register {dest_reg} must be pseudo-cursor {pseudo_cursor}'s content register {content_reg}"
            )).into());
        }
    }
    {
        let cursor = state.get_cursor(*cursor_id);
        if cursor.as_sorter_mut().record().is_none() {
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    }
    // Move the record into the content register with no allocation or copy;
    // the register's spent buffer seeds the sorter's next row.
    let buf = state.registers[*dest_reg].take_buf();
    let record = {
        let cursor = state.get_cursor(*cursor_id);
        cursor
            .as_sorter_mut()
            .take_current(buf)
            .expect("sorter record checked above")
    };
    state.registers[*dest_reg] = Register::Record(record);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_insert(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SorterInsert {
            cursor_id,
            record_reg,
        },
        insn
    );
    {
        let cursor = get_cursor!(state, *cursor_id);
        let cursor = cursor.as_sorter_mut();
        let record = match &state.registers[*record_reg] {
            Register::Record(record) => record,
            _ => unreachable!("SorterInsert on non-record register"),
        };
        return_if_io!(cursor.insert(record));
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_sort(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SorterSort {
            cursor_id,
            pc_if_empty,
        },
        insn
    );
    let (is_empty, did_sort) = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        let is_empty = cursor.is_empty();
        if !is_empty {
            return_if_io!(cursor.sort());
        }
        (is_empty, !is_empty)
    };
    // Increment metrics for sort operation after cursor is dropped
    if did_sort {
        state.metrics.sort_operations = state.metrics.sort_operations.wrapping_add(1);
    }
    state.metrics.search_count = state.metrics.search_count.saturating_sub(1);
    if is_empty {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_next(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SorterNext {
            cursor_id,
            pc_if_next,
        },
        insn
    );
    assert!(pc_if_next.is_offset());
    let has_more = {
        let cursor = state.get_cursor(*cursor_id);
        let cursor = cursor.as_sorter_mut();
        return_if_io!(cursor.next());
        cursor.has_more()
    };
    if has_more {
        state.metrics.search_count = state.metrics.search_count.wrapping_add(1);
        state.pc = pc_if_next.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sorter_compare(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SorterCompare {
            cursor_id,
            sorted_record_reg,
            num_regs,
            pc_when_nonequal,
        },
        insn
    );

    let previous_sorter_values = {
        let Register::Record(record) = &state.registers[*sorted_record_reg] else {
            mark_unlikely();
            return Err(
                LimboError::InternalError("Sorted record must be a record".to_string()).into(),
            );
        };
        &record.get_values_range(0..*num_regs)?
    };

    // Inlined `state.get_cursor` to prevent borrowing conflit with `state.registers`
    let cursor = state
        .cursors
        .get_mut(*cursor_id)
        .unwrap_or_else(|| panic!("cursor id {cursor_id} out of bounds"))
        .as_mut()
        .unwrap_or_else(|| panic!("cursor id {cursor_id} is None"));

    let cursor = cursor.as_sorter_mut();
    let Some(current_sorter_record) = cursor.record() else {
        mark_unlikely();
        return Err(LimboError::InternalError("Sorter must have a record".to_string()).into());
    };

    let current_sorter_values = &current_sorter_record.get_values_range(0..*num_regs)?;
    // If the current sorter record has a NULL in any of the significant fields, the comparison is not equal.
    let is_equal = current_sorter_values
        .iter()
        .all(|v| !matches!(v, ValueRef::Null))
        && compare_immutable(
            previous_sorter_values,
            current_sorter_values,
            &cursor.index_key_info,
        )
        .is_eq();
    if is_equal {
        state.pc += 1;
    } else {
        state.pc = pc_when_nonequal.as_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

/// Insert the integer value held by register P2 into a RowSet object held in register P1.
///
/// An assertion fails if P2 is not an integer.
pub fn op_rowset_add(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        RowSetAdd {
            rowset_reg,
            value_reg,
        },
        insn
    );

    let value = state.registers[*value_reg].get_value();
    let rowid = match value {
        Value::Numeric(Numeric::Integer(i)) => *i,
        _ => {
            mark_unlikely();
            return Err(
                LimboError::InternalError("RowSetAdd: P2 must be an integer".to_string()).into(),
            );
        }
    };

    let rowset = state.rowsets.entry(*rowset_reg).or_default();

    rowset.insert(rowid)?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Extract the smallest value from the RowSet object in P1 and put that value into register P3.
/// Or, if RowSet object P1 is initially empty, leave P3 unchanged and jump to instruction P2.
pub fn op_rowset_read(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        RowSetRead {
            rowset_reg,
            pc_if_empty,
            dest_reg,
        },
        insn
    );
    assert!(pc_if_empty.is_offset());

    let rowset = state.rowsets.get_mut(rowset_reg);

    match rowset {
        Some(rowset) => {
            if rowset.is_empty() {
                state.pc = pc_if_empty.as_offset_int();
            } else if let Some(smallest) = rowset.smallest() {
                state.registers[*dest_reg].set_int(smallest);
                state.pc += 1;
            } else {
                state.pc = pc_if_empty.as_offset_int();
            }
        }
        None => {
            state.pc = pc_if_empty.as_offset_int();
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

/// Register P3 is assumed to hold a 64-bit integer value. If register P1 contains a RowSet object
/// and that RowSet object contains the value held in P3, jump to register P2. Otherwise, insert
/// the integer in P3 into the RowSet and continue on to the next opcode.
///
/// The RowSet object is optimized for the case where sets of integers are inserted in distinct
/// phases, which each set contains no duplicates. Each set is identified by a unique P4 value.
/// The first set must have P4==0, the final set must have P4==-1, and for all other sets must
/// have P4>0.
///
/// This allows optimizations: (a) when P4==0 there is no need to test the RowSet object for P3,
/// as it is guaranteed not to contain it, (b) when P4==-1 there is no need to insert the value,
/// as it will never be tested for, and (c) when a value that is part of set X is inserted, there
/// is no need to search to see if the same value was previously inserted as part of set X (only
/// if it was previously inserted as part of some other set).
pub fn op_rowset_test(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        RowSetTest {
            rowset_reg,
            pc_if_found,
            value_reg,
            batch,
        },
        insn
    );
    assert!(pc_if_found.is_offset());

    let value = state.registers[*value_reg].get_value();
    let rowid = match value {
        Value::Numeric(Numeric::Integer(i)) => *i,
        _ => {
            mark_unlikely();
            return Err(
                LimboError::InternalError("RowSetTest: P3 must be an integer".to_string()).into(),
            );
        }
    };

    let rowset = state.rowsets.entry(*rowset_reg).or_default();

    let found = if *batch == 0 {
        // SQLite rowsets assume that in each batch, the caller makes sure no
        // duplicates are inserted. Hence if batch==0, we can return false without
        // checking.
        false
    } else {
        rowset.test(rowid, *batch)
    };

    if found {
        state.pc = pc_if_found.as_offset_int();
    } else {
        if *batch != -1 {
            rowset.insert(rowid)?;
        }
        state.pc += 1;
    }

    Ok(InsnFunctionStepResult::Step)
}

fn parse_schema_sql_for_alter(
    dialect: &dyn crate::Dialect,
    entry_type: &str,
    root_page: i64,
    sql: &str,
) -> Result<Option<ast::Cmd>> {
    if entry_type == "table" && root_page != 0 {
        let stmt = dialect.parse_table_sql_ast(sql)?;
        if !matches!(stmt, ast::Stmt::CreateTable { .. }) {
            return Err(LimboError::Corrupt(
                "storage-backed table schema is not CREATE TABLE".to_string(),
            ));
        }
        return Ok(Some(ast::Cmd::Stmt(stmt)));
    }
    dialect.parse(sql).map(|(cmd, _)| cmd)
}

pub fn op_function(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Function {
            constant_mask: _,
            func,
            start_reg,
            dest,
        },
        insn
    );
    let arg_count = func.arg_count;

    match &func.func {
        #[cfg(feature = "json")]
        crate::function::Func::Json(json_func) => match json_func {
            JsonFunc::Json => {
                let json_value = &state.registers[*start_reg];
                let json_str = get_json(json_value.get_value(), None);
                match json_str {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }

            JsonFunc::Jsonb => {
                let json_value = &state.registers[*start_reg];
                let json_blob = jsonb(json_value.get_value(), &state.json_cache);
                match json_blob {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }

            JsonFunc::JsonArray
            | JsonFunc::JsonObject
            | JsonFunc::JsonbArray
            | JsonFunc::JsonbObject => {
                let reg_values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);

                let json_func = match json_func {
                    JsonFunc::JsonArray => json_array,
                    JsonFunc::JsonObject => json_object,
                    JsonFunc::JsonbArray => jsonb_array,
                    JsonFunc::JsonbObject => jsonb_object,
                    _ => unreachable!(),
                };
                let json_result = json_func(reg_values);

                match json_result {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonExtract => {
                let result = match arg_count {
                    0 => Ok(Value::Null),
                    _ => {
                        let val = &state.registers[*start_reg];
                        let reg_values = registers_to_ref_values(
                            &state.registers[*start_reg + 1..*start_reg + arg_count],
                        );

                        json_extract(val.get_value(), reg_values, &state.json_cache)
                    }
                };

                match result {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonbExtract => {
                let result = match arg_count {
                    0 => Ok(Value::Null),
                    _ => {
                        let val = &state.registers[*start_reg];
                        let reg_values = registers_to_ref_values(
                            &state.registers[*start_reg + 1..*start_reg + arg_count],
                        );

                        jsonb_extract(val.get_value(), reg_values, &state.json_cache)
                    }
                };

                match result {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }

            JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                assert_eq!(arg_count, 2);
                let json = &state.registers[*start_reg];
                let path = &state.registers[*start_reg + 1];
                let json_func = match json_func {
                    JsonFunc::JsonArrowExtract => json_arrow_extract,
                    JsonFunc::JsonArrowShiftExtract => json_arrow_shift_extract,
                    _ => unreachable!(),
                };
                let json_str = json_func(json.get_value(), path.get_value(), &state.json_cache);
                match json_str {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                let json_value = &state.registers[*start_reg];
                let path_value = if arg_count > 1 {
                    Some(&state.registers[*start_reg + 1])
                } else {
                    None
                };
                let func_result = match json_func {
                    JsonFunc::JsonArrayLength => json_array_length(
                        json_value.get_value(),
                        path_value.map(|x| x.get_value()),
                        &state.json_cache,
                    ),
                    JsonFunc::JsonType => {
                        json_type(json_value.get_value(), path_value.map(|x| x.get_value()))
                    }
                    _ => unreachable!(),
                };

                match func_result {
                    Ok(result) => state.registers[*dest].set_value(result),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonErrorPosition => {
                let json_value = &state.registers[*start_reg];
                match json_error_position(json_value.get_value()) {
                    Ok(pos) => state.registers[*dest].set_value(pos),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonValid => {
                let json_value = &state.registers[*start_reg];
                // json_valid(X) is defined as json_valid(X, 1).
                let default_flags = Value::from_i64(json::JSON_VALID_FLAG_TEXT_STRICT);
                let flags_value = if arg_count > 1 {
                    state.registers[*start_reg + 1].get_value()
                } else {
                    &default_flags
                };
                state.registers[*dest]
                    .set_value(is_json_valid(json_value.get_value(), flags_value)?);
            }
            JsonFunc::JsonPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest].set_value(json_patch(
                    target.get_value(),
                    patch.get_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonbPatch => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let target = &state.registers[*start_reg];
                let patch = &state.registers[*start_reg + 1];
                state.registers[*dest].set_value(jsonb_patch(
                    target.get_value(),
                    patch.get_value(),
                    &state.json_cache,
                )?);
            }
            JsonFunc::JsonRemove => {
                if let Ok(json) = json_remove(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest].set_value(json);
                } else {
                    state.registers[*dest].set_null();
                }
            }
            JsonFunc::JsonbRemove => {
                if let Ok(json) = jsonb_remove(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest].set_value(json);
                } else {
                    state.registers[*dest].set_null();
                }
            }
            JsonFunc::JsonReplace => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_replace() needs an odd number of arguments")
                }
                if let Ok(json) = json_replace(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest].set_value(json);
                } else {
                    state.registers[*dest].set_null();
                }
            }
            JsonFunc::JsonbReplace => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_replace() needs an odd number of arguments")
                }
                if let Ok(json) = jsonb_replace(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest].set_value(json);
                } else {
                    state.registers[*dest].set_null();
                }
            }
            JsonFunc::JsonInsert => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_insert() needs an odd number of arguments")
                }
                if let Ok(json) = json_insert(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest].set_value(json);
                } else {
                    state.registers[*dest].set_null();
                }
            }
            JsonFunc::JsonbInsert => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_insert() needs an odd number of arguments")
                }
                if let Ok(json) = jsonb_insert(
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]),
                    &state.json_cache,
                ) {
                    state.registers[*dest].set_value(json);
                } else {
                    state.registers[*dest].set_null();
                }
            }
            JsonFunc::JsonPretty => {
                let json_value = &state.registers[*start_reg];
                let indent = if arg_count > 1 {
                    Some(&state.registers[*start_reg + 1])
                } else {
                    None
                };

                // Blob should be converted to Ascii in a lossy way
                // However, Rust strings uses utf-8
                // so the behavior at the moment is slightly different
                // To the way blobs are parsed here in SQLite.
                let indent = match indent {
                    Some(value) => match value.get_value() {
                        Value::Text(text) => text.as_str(),
                        Value::Numeric(Numeric::Integer(val)) => &val.to_string(),
                        Value::Numeric(Numeric::Float(val)) => &f64::from(*val).to_string(),
                        Value::Blob(val) => &String::from_utf8_lossy(val),
                        _ => "    ",
                    },
                    // If the second argument is omitted or is NULL, then indentation is four spaces per level
                    None => "    ",
                };

                let json_str = get_json(json_value.get_value(), Some(indent))?;
                state.registers[*dest].set_value(json_str);
            }
            JsonFunc::JsonSet => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_set() needs an odd number of arguments")
                }
                let reg_values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);

                let json_result = json_set(reg_values, &state.json_cache);

                match json_result {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonbSet => {
                if arg_count % 2 == 0 {
                    bail_constraint_error!("json_set() needs an odd number of arguments")
                }
                let reg_values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);

                let json_result = jsonb_set(reg_values, &state.json_cache);

                match json_result {
                    Ok(json) => state.registers[*dest].set_value(json),
                    Err(e) => return Err(e.into()),
                }
            }
            JsonFunc::JsonQuote => {
                let json_value = &state.registers[*start_reg];

                match json_quote(json_value.get_value()) {
                    Ok(result) => state.registers[*dest].set_value(result),
                    Err(e) => return Err(e.into()),
                }
            }
        },
        crate::function::Func::Scalar(scalar_func) => match scalar_func {
            ScalarFunc::Array | ScalarFunc::ArrayElement | ScalarFunc::ArraySetElement => {
                unreachable!("desugared to dedicated instructions, not Function")
            }
            ScalarFunc::Cast => {
                assert_eq!(arg_count, 2);
                assert!(*start_reg + 1 < state.registers.len());
                let result = {
                    let Value::Text(cast_type) = state.registers[*start_reg + 1].get_value() else {
                        unreachable!("Cast with non-text type");
                    };
                    state.registers[*start_reg]
                        .get_value()
                        .exec_cast(cast_type.as_str())?
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Changes => {
                let res = &program.connection.changes;
                let changes = res.load(Ordering::SeqCst);
                state.registers[*dest].set_int(changes);
            }
            ScalarFunc::Char => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest].set_value(Value::exec_char(
                    reg_values.iter().map(|reg| reg.get_value()),
                ));
            }
            ScalarFunc::Coalesce => {}
            ScalarFunc::Concat => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result =
                    Value::exec_concat_strings(reg_values.iter().map(|reg| reg.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::ConcatWs => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                let result = Value::exec_concat_ws(reg_values.iter().map(|reg| reg.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Glob => {
                if arg_count != 2 {
                    mark_unlikely();
                    return Err(LimboError::ParseError(
                        "wrong number of arguments to function GLOB()".to_string(),
                    )
                    .into());
                }
                let pattern_reg = &state.registers[*start_reg];
                let match_reg = &state.registers[*start_reg + 1];

                let pattern_value = pattern_reg.get_value();
                let match_value = match_reg.get_value();

                if pattern_value == &Value::Null || match_value == &Value::Null {
                    state.registers[*dest].set_null();
                } else {
                    let pattern_cow = match pattern_value {
                        Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                        v => match v.exec_cast("TEXT")? {
                            Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                            _ => unreachable!("Cast to TEXT should yield Text"),
                        },
                    };

                    let match_cow = match match_value {
                        Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                        v => match v.exec_cast("TEXT")? {
                            Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                            _ => unreachable!("Cast to TEXT should yield Text"),
                        },
                    };

                    let matches = Value::exec_glob(&pattern_cow, &match_cow)?;
                    state.registers[*dest].set_int(matches as i64);
                }
            }
            ScalarFunc::IfNull => {}
            ScalarFunc::Iif => {}
            ScalarFunc::Instr => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = &state.registers[*start_reg + 1];
                match reg_value.get_value().exec_instr(pattern_value.get_value()) {
                    Value::Numeric(Numeric::Integer(i)) => state.registers[*dest].set_int(i),
                    _ => state.registers[*dest].set_null(),
                };
            }
            ScalarFunc::LastInsertRowid => {
                state.registers[*dest].set_int(program.connection.last_insert_rowid());
            }
            ScalarFunc::Like => {
                let pattern_reg = &state.registers[*start_reg];
                let match_reg = &state.registers[*start_reg + 1];

                let pattern_value = pattern_reg.get_value();
                let match_value = match_reg.get_value();

                // 1. Check for NULL inputs
                if pattern_value == &Value::Null || match_value == &Value::Null {
                    state.registers[*dest].set_null();
                } else {
                    // 2. Resolve Escape Character (if 3rd arg exists)
                    let mut escape_char = None;
                    let mut is_null_result = false;

                    if arg_count == 3 {
                        let escape_value = state.registers[*start_reg + 2].get_value();
                        match escape_value {
                            Value::Null => {
                                is_null_result = true;
                            }
                            _ => {
                                let escape_cow = match escape_value {
                                    Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                                    v => match v.exec_cast("TEXT")? {
                                        Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                                        _ => unreachable!("Cast to TEXT should yield Text"),
                                    },
                                };

                                let mut chars = escape_cow.chars();
                                let c = chars.next();
                                if c.is_none() || chars.next().is_some() {
                                    return Err(LimboError::Constraint(
                                        "ESCAPE expression must be a single character".to_string(),
                                    )
                                    .into());
                                }
                                escape_char = c;
                            }
                        }
                    }

                    if is_null_result {
                        state.registers[*dest].set_null();
                    } else {
                        // 3. Prepare Pattern and Text
                        let pattern_cow = match pattern_value {
                            Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                            v => match v.exec_cast("TEXT")? {
                                Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                                _ => unreachable!("Cast to TEXT should yield Text"),
                            },
                        };

                        let match_cow = match match_value {
                            Value::Text(s) => std::borrow::Cow::Borrowed(s.as_str()),
                            v => match v.exec_cast("TEXT")? {
                                Value::Text(s) => std::borrow::Cow::Owned(s.to_string()),
                                _ => unreachable!("Cast to TEXT should yield Text"),
                            },
                        };

                        // 4. Execute Like
                        let matches = Value::exec_like(&pattern_cow, &match_cow, escape_char)?;
                        state.registers[*dest].set_int(matches as i64);
                    }
                }
            }
            ScalarFunc::NextVal => {
                // The translator no longer emits `Function NextVal` — nextval
                // is now handled entirely by the SequenceComputeNext +
                // SetSequenceCurrval opcode pair plus surrounding cursor ops.
                // Reaching this handler means stale bytecode somewhere.
                return Err(crate::LimboError::InternalError(
                    "ScalarFunc::NextVal handler should be unreachable under \
                     the disk-only sequence design"
                        .to_string(),
                )
                .into());
            }
            ScalarFunc::CurrVal => {
                let seq_name = match state.registers[*start_reg].get_value() {
                    Value::Text(t) => t.as_str().to_string(),
                    _ => {
                        return Err(crate::LimboError::ParseError(
                            "currval() requires a text argument".to_string(),
                        )
                        .into());
                    }
                };
                // The connection's currval session map persists across
                // DROP SEQUENCE (the entry is keyed by name with no link
                // to the schema). Resolve the sequence first so currval
                // on a dropped sequence reports "does not exist" rather
                // than silently returning the stale per-session value.
                program.connection.find_sequence(&seq_name)?;
                match program.connection.get_sequence_currval(&seq_name) {
                    Some(val) => {
                        state.registers[*dest].set_value(Value::from_i64(val));
                    }
                    None => {
                        return Err(crate::LimboError::ParseError(format!(
                            "currval of sequence \"{seq_name}\" is not yet defined in this session",
                        ))
                        .into());
                    }
                }
            }
            ScalarFunc::SetVal => {
                // Pre-write validation: argument type + range check. The
                // translator emits this Function call before the inline
                // DELETE-all + INSERT bytecode, so any error returned here
                // aborts the statement BEFORE any disk write — which is what
                // keeps the op_insert "expected integer key" assertion safe
                // and the user-facing error message intact. After validation
                // the result value is placed in `dest`; currval bookkeeping
                // and dirty-marking are emitted by the translator via the
                // separate SetSequenceCurrval opcode.
                //
                // Note: in the disk-only design there is no pending-setval
                // queue, no in-memory atomic to mutate, and no exclusive-tx
                // requirement. The autonomous-tx mechanism (planned for MVCC
                // concurrency in a follow-up) will subsume serialization.
                let seq_name = match state.registers[*start_reg].get_value() {
                    Value::Text(t) => t.as_str().to_string(),
                    _ => {
                        return Err(crate::LimboError::ParseError(
                            "setval() requires a text argument".to_string(),
                        )
                        .into());
                    }
                };
                let value = match state.registers[*start_reg + 1].get_value().as_int() {
                    Some(v) => v,
                    None => {
                        return Err(crate::LimboError::ParseError(
                            "setval() requires an integer value".to_string(),
                        )
                        .into());
                    }
                };
                if arg_count > 2
                    && state.registers[*start_reg + 2]
                        .get_value()
                        .as_int()
                        .is_none()
                {
                    return Err(crate::LimboError::ParseError(
                        "setval() requires an integer is_called argument".to_string(),
                    )
                    .into());
                }
                let seq = program.connection.find_sequence(&seq_name)?;
                if value < seq.min_value || value > seq.max_value {
                    return Err(crate::LimboError::ParseError(format!(
                        "setval: value {} is out of bounds for sequence \"{}\" ({}..{})",
                        value, seq.name, seq.min_value, seq.max_value
                    ))
                    .into());
                }
                state.registers[*dest].set_value(Value::from_i64(value));
            }
            ScalarFunc::Abs
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Subtype
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Unistr
            | ScalarFunc::UnistrQuote
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob => {
                let reg_value = state.registers[*start_reg].borrow_mut().get_value();
                let result = match scalar_func {
                    ScalarFunc::Sign => reg_value.exec_sign(),
                    ScalarFunc::Abs => Some(reg_value.exec_abs()?),
                    ScalarFunc::Lower => reg_value.exec_lower(),
                    ScalarFunc::Upper => reg_value.exec_upper(),
                    ScalarFunc::Length => Some(reg_value.exec_length()),
                    ScalarFunc::OctetLength => Some(reg_value.exec_octet_length()),
                    ScalarFunc::Subtype => Some(reg_value.exec_subtype()),
                    ScalarFunc::Typeof => Some(reg_value.exec_typeof()),
                    ScalarFunc::Unicode => Some(reg_value.exec_unicode()),
                    ScalarFunc::Unistr => Some(reg_value.exec_unistr()?),
                    ScalarFunc::Quote => Some(reg_value.exec_quote()),
                    ScalarFunc::UnistrQuote => Some(reg_value.exec_unistr_quote()),
                    ScalarFunc::RandomBlob => {
                        Some(reg_value.exec_randomblob(|dest| pager.io.fill_bytes(dest))?)
                    }
                    ScalarFunc::ZeroBlob => Some(reg_value.exec_zeroblob()?),
                    ScalarFunc::Soundex => Some(reg_value.exec_soundex()),
                    _ => unreachable!(),
                };
                state.registers[*dest].set_value(result.unwrap_or(Value::Null));
            }
            ScalarFunc::Hex => {
                let reg_value = state.registers[*start_reg].borrow_mut();
                let result = reg_value.get_value().exec_hex();
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Unhex => {
                let reg_value = &state.registers[*start_reg];
                let ignored_chars = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_unhex(ignored_chars.map(|x| x.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::GetByte => {
                let value = state.registers[*start_reg].get_value();
                let offset = state.registers[*start_reg + 1].get_value();
                let result = value.exec_get_byte(offset)?;
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::SetByte => {
                let value = state.registers[*start_reg].get_value();
                let offset = state.registers[*start_reg + 1].get_value();
                let new_value = state.registers[*start_reg + 2].get_value();
                let result = value.exec_set_byte(offset, new_value)?;
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Random => {
                state.registers[*dest].set_int(pager.io.generate_random_number());
            }
            #[cfg(feature = "test_helper")]
            ScalarFunc::TestNondetCounter => {
                // Test-only: process-global atomic counter that increments on
                // every evaluation. Used in sqltests to verify that the
                // planner does not deduplicate equivalent SQL calls that
                // contain nondeterministic functions.
                static TEST_NONDET_COUNTER: std::sync::atomic::AtomicI64 =
                    std::sync::atomic::AtomicI64::new(0);
                let value = TEST_NONDET_COUNTER.fetch_add(1, Ordering::Relaxed);
                state.registers[*dest].set_int(value);
            }
            ScalarFunc::Trim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_trim(pattern_value.map(|x| x.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::LTrim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_ltrim(pattern_value.map(|x| x.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::RTrim => {
                let reg_value = &state.registers[*start_reg];
                let pattern_value = if func.arg_count == 2 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_rtrim(pattern_value.map(|x| x.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Round => {
                let reg_value = &state.registers[*start_reg];
                assert!(arg_count == 1 || arg_count == 2);
                let precision_value = if arg_count > 1 {
                    state.registers.get(*start_reg + 1)
                } else {
                    None
                };
                let result = reg_value
                    .get_value()
                    .exec_round(precision_value.map(|x| x.get_value()));
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Min => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest]
                    .set_value(Value::exec_min(reg_values.iter().map(|v| v.get_value())));
            }
            ScalarFunc::Max => {
                let reg_values = &state.registers[*start_reg..*start_reg + arg_count];
                state.registers[*dest]
                    .set_value(Value::exec_max(reg_values.iter().map(|v| v.get_value())));
            }
            ScalarFunc::Nullif => {
                let first_value = &state.registers[*start_reg];
                let second_value = &state.registers[*start_reg + 1];
                state.registers[*dest].set_value(Value::exec_nullif(
                    first_value.get_value(),
                    second_value.get_value(),
                ));
            }
            ScalarFunc::Substr | ScalarFunc::Substring => {
                let str_value = &state.registers[*start_reg];
                let start_value = &state.registers[*start_reg + 1];
                let length_value = if func.arg_count == 3 {
                    Some(&state.registers[*start_reg + 2])
                } else {
                    None
                };
                let result = Value::exec_substring(
                    str_value.get_value(),
                    start_value.get_value(),
                    length_value.map(|x| x.get_value()),
                )?;
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Date => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_date(values);
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Time => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_time(values);
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::TimeDiff => {
                if arg_count != 2 {
                    state.registers[*dest].set_null();
                } else {
                    let start = state.registers[*start_reg].get_value();
                    let end = state.registers[*start_reg + 1].get_value();

                    let result = crate::functions::datetime::exec_timediff([start, end]);

                    state.registers[*dest].set_value(result);
                }
            }
            ScalarFunc::TotalChanges => {
                let res = &program.connection.total_changes;
                let total_changes = res.load(Ordering::SeqCst);
                state.registers[*dest].set_int(total_changes);
            }
            ScalarFunc::DateTime => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_datetime_full(values);
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::JulianDay => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_julianday(values);
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::UnixEpoch => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_unixepoch(values);
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::TursoVersion => {
                if !program.connection.is_db_initialized() {
                    state.registers[*dest].set_text(Text::new(info::build::PKG_VERSION))?;
                } else {
                    let version_integer =
                        return_if_io!(pager.with_header(|header| header.version_number)).get()
                            as i64;
                    let version = execute_turso_version(version_integer);
                    state.registers[*dest].set_text(Text::new(version))?;
                }
            }
            ScalarFunc::SqliteVersion => {
                let version = execute_sqlite_version();
                state.registers[*dest].set_text(Text::new(version))?;
            }
            ScalarFunc::SqliteSourceId => {
                let src_id = format!(
                    "{} {}",
                    info::build::BUILT_TIME_SQLITE,
                    info::build::GIT_COMMIT_HASH.unwrap_or("unknown")
                );
                state.registers[*dest].set_text(Text::new(src_id))?;
            }
            ScalarFunc::Replace => {
                assert_eq!(arg_count, 3);
                let source = &state.registers[*start_reg];
                let pattern = &state.registers[*start_reg + 1];
                let replacement = &state.registers[*start_reg + 2];
                state.registers[*dest].set_value(Value::exec_replace(
                    source.get_value(),
                    pattern.get_value(),
                    replacement.get_value(),
                )?);
            }
            #[cfg(feature = "fs")]
            #[cfg(not(target_family = "wasm"))]
            ScalarFunc::LoadExtension => {
                if !program.connection.can_load_extensions() {
                    crate::bail_parse_error!("runtime extension loading is disabled");
                }
                let extension = &state.registers[*start_reg];
                let ext = resolve_ext_path(&extension.get_value().to_string())?;
                program.connection.load_extension(ext)?;
            }
            ScalarFunc::StrfTime => {
                let values =
                    registers_to_ref_values(&state.registers[*start_reg..*start_reg + arg_count]);
                let result = exec_strftime(values);
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Printf => {
                let result = exec_printf(&state.registers[*start_reg..*start_reg + arg_count])?;
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::TableColumnsJsonArray => {
                assert_eq!(arg_count, 1);
                #[cfg(not(feature = "json"))]
                {
                    return Err(LimboError::InvalidArgument(
                        "table_columns_json_array: turso must be compiled with JSON support"
                            .to_string(),
                    ));
                }
                #[cfg(feature = "json")]
                {
                    use crate::types::TextSubtype;

                    let table = state.registers[*start_reg].get_value();
                    let Value::Text(table) = table else {
                        return Err(LimboError::InvalidArgument(
                            "table_columns_json_array: function argument must be of type TEXT"
                                .to_string(),
                        )
                        .into());
                    };
                    let table = {
                        let schema = program.connection.schema.read();
                        match schema.get_table(table.as_str()) {
                            Some(table) => table,
                            None => {
                                return Err(LimboError::InvalidArgument(format!(
                                    "table_columns_json_array: table {table} doesn't exists"
                                ))
                                .into());
                            }
                        }
                    };

                    let mut json =
                        json::jsonb::Jsonb::make_empty_array(table.columns().len() * 10)?;
                    for column in table.columns() {
                        use crate::types::TextRef;

                        let name = column.name.as_ref().expect("column should have a name");
                        let name_json = json::convert_ref_dbtype_to_jsonb(
                            ValueRef::Text(TextRef::new(name, TextSubtype::Text)),
                            json::Conv::ToString,
                        )?;
                        json.append_jsonb_to_end(name_json.data());
                    }
                    json.finalize_unsafe(json::jsonb::ElementType::ARRAY)?;
                    state.registers[*dest].set_value(json::json_string_to_db_type(
                        json,
                        json::jsonb::ElementType::ARRAY,
                        json::OutputVariant::String,
                    )?);
                }
            }
            ScalarFunc::BinRecordJsonObject => {
                assert_eq!(arg_count, 2);
                #[cfg(not(feature = "json"))]
                {
                    return Err(LimboError::InvalidArgument(
                        "bin_record_json_object: turso must be compiled with JSON support"
                            .to_string(),
                    ));
                }
                #[cfg(feature = "json")]
                'outer: {
                    use crate::types::ValueIterator;
                    use std::str::FromStr;

                    let columns_str = state.registers[*start_reg].get_value();
                    let bin_record = state.registers[*start_reg + 1].get_value();
                    let Value::Text(columns_str) = columns_str else {
                        return Err(LimboError::InvalidArgument(
                            "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                        ).into());
                    };

                    if let Value::Null = bin_record {
                        state.registers[*dest].set_null();
                        break 'outer;
                    }

                    let Value::Blob(bin_record) = bin_record else {
                        return Err(LimboError::InvalidArgument(
                            "bin_record_json_object: function arguments must be of type TEXT and BLOB correspondingly".to_string()
                        ).into());
                    };
                    let mut columns_json_array =
                        json::jsonb::Jsonb::from_str(columns_str.as_str())?;
                    let columns_len = columns_json_array.array_len()?;

                    let mut payload_iterator = ValueIterator::new(bin_record.as_slice())?;

                    let mut json = json::jsonb::Jsonb::make_empty_obj(columns_len)?;
                    for i in 0..columns_len {
                        let mut op = json::jsonb::SearchOperation::new(0)?;
                        let path = json::path::JsonPath {
                            elements: vec![
                                json::path::PathElement::Root(),
                                json::path::PathElement::ArrayLocator(Some(i as i32)),
                            ],
                        };

                        columns_json_array.operate_on_path(&path, &mut op)?;
                        let column_name = op.result();
                        json.append_jsonb_to_end(column_name.data());

                        let val = match payload_iterator.next() {
                            Some(Ok(v)) => v,
                            Some(Err(e)) => return Err(e.into()),
                            None => {
                                return Err(LimboError::InvalidArgument(
                                    "bin_record_json_object: binary record has fewer columns than specified in the columns argument".to_string()
                                ).into());
                            }
                        };

                        if let ValueRef::Blob(..) = val {
                            return Err(LimboError::InvalidArgument(
                                "bin_record_json_object: formatting of BLOB values stored in binary record is not supported".to_string()
                            ).into());
                        }
                        let val_json =
                            json::convert_ref_dbtype_to_jsonb(val, json::Conv::NotStrict)?;
                        json.append_jsonb_to_end(val_json.data());
                    }
                    json.finalize_unsafe(json::jsonb::ElementType::OBJECT)?;

                    state.registers[*dest].set_value(json::json_string_to_db_type(
                        json,
                        json::jsonb::ElementType::OBJECT,
                        json::OutputVariant::String,
                    )?);
                }
            }
            ScalarFunc::Attach => {
                assert_eq!(arg_count, 3);
                let filename = state.registers[*start_reg].get_value();
                let dbname = state.registers[*start_reg + 1].get_value();
                let _key = state.registers[*start_reg + 2].get_value(); // Not used in read-only implementation

                let Value::Text(filename_str) = filename else {
                    return Err(LimboError::InvalidArgument(
                        "attach: filename argument must be text".to_string(),
                    )
                    .into());
                };

                let Value::Text(dbname_str) = dbname else {
                    return Err(LimboError::InvalidArgument(
                        "attach: database name argument must be text".to_string(),
                    )
                    .into());
                };

                return_if_io!(program.connection.attach_database(
                    filename_str.as_str(),
                    dbname_str.as_str(),
                    state.active_op_state.attach(),
                ));
                // Sequence descriptors for the attached database are
                // loaded lazily by `maybe_reparse_schema` on the next
                // statement (ATTACH bumps the schema cookie, so the
                // very next prepare reparses). Issuing a SQL load via
                // `prepare_internal` from inside this opcode handler
                // would drive `pager.io.step()` synchronously and break
                // the vdbe async contract.

                state.active_op_state.clear();
                state.registers[*dest].set_null();
            }
            ScalarFunc::Detach => {
                assert_eq!(arg_count, 1);
                let dbname = state.registers[*start_reg].get_value();

                let Value::Text(dbname_str) = dbname else {
                    return Err(LimboError::InvalidArgument(
                        "detach: database name argument must be text".to_string(),
                    )
                    .into());
                };

                // Call the detach_database method on the connection
                program.connection.detach_database(dbname_str.as_str())?;

                // Set result to NULL (detach doesn't return a value)
                state.registers[*dest].set_null();
            }
            ScalarFunc::Unlikely | ScalarFunc::Likely | ScalarFunc::Likelihood => {
                panic!(
                    "{scalar_func:?} should be stripped during expression translation and never reach VDBE",
                );
            }
            ScalarFunc::StatInit => {
                // stat_init(n_col): Initialize a statistics accumulator
                // Returns a blob containing the serialized StatAccum
                assert!(arg_count >= 1);
                let n_col = match state.registers[*start_reg].get_value() {
                    Value::Numeric(Numeric::Integer(n)) => *n as usize,
                    _ => 0,
                };
                let accum = StatAccum::new(n_col);
                state.registers[*dest].set_blob(accum.to_bytes())?;
            }
            ScalarFunc::StatPush => {
                // stat_push(accum_blob, i_chng): Push a row into the accumulator
                // i_chng is the index of the leftmost column that changed from the previous row
                // Returns the updated accumulator blob
                assert!(arg_count >= 2);
                let accum_blob = state.registers[*start_reg].get_value();
                let i_chng = match state.registers[*start_reg + 1].get_value() {
                    Value::Numeric(Numeric::Integer(n)) => *n as usize,
                    _ => 0,
                };
                let result = match accum_blob {
                    Value::Blob(bytes) => {
                        if let Some(mut accum) = StatAccum::from_bytes(bytes) {
                            accum.push(i_chng);
                            Value::Blob(accum.to_bytes())
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::StatGet => {
                // stat_get(accum_blob): Get the stat1 string from the accumulator
                // Returns the stat string "total avg1 avg2 ..."
                assert!(arg_count >= 1);
                let accum_blob = state.registers[*start_reg].get_value();
                let result = match accum_blob {
                    Value::Blob(bytes) => {
                        if let Some(accum) = StatAccum::from_bytes(bytes) {
                            let stat_str = accum.get_stat1();
                            if stat_str.is_empty() {
                                Value::Null
                            } else {
                                Value::build_text(stat_str)
                            }
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::ConnTxnId => {
                // conn_txn_id(candidate): get-or-set semantics for CDC transaction ID.
                // If unset (-1), store the candidate and return it.
                // If already set, return the existing value, ignoring the candidate.
                assert_eq!(arg_count, 1);
                let candidate = match state.registers[*start_reg].get_value() {
                    Value::Numeric(Numeric::Integer(n)) => *n,
                    _ => -1,
                };
                let current = program.connection.get_cdc_transaction_id();
                if current == -1 {
                    program.connection.set_cdc_transaction_id(candidate);
                    state.registers[*dest].set_int(candidate);
                } else {
                    state.registers[*dest].set_int(current);
                }
            }
            ScalarFunc::IsAutocommit => {
                // is_autocommit(): returns 1 if autocommit, 0 otherwise.
                let auto_commit = program.connection.auto_commit.load(Ordering::SeqCst);
                state.registers[*dest].set_int(if auto_commit { 1 } else { 0 });
            }
            ScalarFunc::SequenceWatermark => {
                assert_eq!(arg_count, 1);
                let sequence_name = match state.registers[*start_reg].get_value() {
                    Value::Text(text) => text.as_str(),
                    Value::Null => {
                        state.registers[*dest].set_value(Value::Null);
                        return Ok(InsnFunctionStepResult::Step);
                    }
                    _ => {
                        return Err(LimboError::InternalError(
                            "sequence_watermark_experimental() argument must be TEXT".to_string(),
                        )
                        .into());
                    }
                };
                let (db_id, sequence_key) =
                    if let Some((schema_name, sequence_name)) = sequence_name.split_once('.') {
                        (
                            program.connection.get_database_id_by_name(schema_name)?,
                            crate::util::normalize_ident(sequence_name),
                        )
                    } else {
                        (MAIN_DB_ID, crate::util::normalize_ident(sequence_name))
                    };
                program.connection.find_sequence(sequence_name)?;
                let watermark = program
                    .connection
                    .mv_store_for_db(db_id)
                    .and_then(|mv_store| mv_store.sequence_watermark(&sequence_key));
                match watermark {
                    Some(watermark) => state.registers[*dest].set_int(watermark),
                    None => state.registers[*dest].set_value(Value::Null),
                }
            }
            ScalarFunc::TestUintEncode => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Numeric(Numeric::Integer(i)) => {
                        if *i < 0 {
                            return Err(LimboError::InternalError(
                                "test_uint_encode: negative value".to_string(),
                            )
                            .into());
                        }
                        Value::build_text(i.to_string())
                    }
                    Value::Numeric(Numeric::Float(f)) => {
                        if *f < 0.0 || f.fract() != 0.0 {
                            return Err(LimboError::InternalError(
                                "test_uint_encode: not a non-negative integer".to_string(),
                            )
                            .into());
                        }
                        Value::build_text((f64::from(*f) as u64).to_string())
                    }
                    Value::Text(t) => {
                        let s = t.to_string();
                        s.parse::<u64>().map_err(|_| {
                            LimboError::InternalError(format!(
                                "test_uint_encode: invalid uint: {s}"
                            ))
                        })?;
                        Value::build_text(s)
                    }
                    _ => {
                        return Err(LimboError::InternalError(
                            "test_uint_encode: unsupported type".to_string(),
                        )
                        .into());
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::TestUintDecode => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    other => other.clone(),
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv => {
                check_arg_count!(arg_count, 2);
                let a = parse_test_uint(&state.registers[*start_reg])?;
                let b = parse_test_uint(&state.registers[*start_reg + 1])?;
                let result = match (a, b) {
                    (Some(a), Some(b)) => {
                        let r = match scalar_func {
                            ScalarFunc::TestUintAdd => a.checked_add(b),
                            ScalarFunc::TestUintSub => a.checked_sub(b),
                            ScalarFunc::TestUintMul => a.checked_mul(b),
                            ScalarFunc::TestUintDiv => {
                                if b == 0 {
                                    None
                                } else {
                                    Some(a / b)
                                }
                            }
                            _ => unreachable!(),
                        };
                        match r {
                            Some(v) => Value::build_text(v.to_string()),
                            None => {
                                return Err(LimboError::InternalError(
                                    "test_uint arithmetic overflow/underflow".to_string(),
                                )
                                .into());
                            }
                        }
                    }
                    _ => Value::Null,
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::TestUintLt | ScalarFunc::TestUintEq => {
                check_arg_count!(arg_count, 2);
                let a = parse_test_uint(&state.registers[*start_reg])?;
                let b = parse_test_uint(&state.registers[*start_reg + 1])?;
                let result = match (a, b) {
                    (Some(a), Some(b)) => {
                        let cmp = match scalar_func {
                            ScalarFunc::TestUintLt => a < b,
                            ScalarFunc::TestUintEq => a == b,
                            _ => unreachable!(),
                        };
                        Value::from_i64(if cmp { 1 } else { 0 })
                    }
                    _ => Value::Null,
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::StringReverse => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Text(t) => {
                        let reversed: String = t.to_string().chars().rev().collect();
                        Value::build_text(reversed)
                    }
                    other => {
                        let s = other.to_string();
                        let reversed: String = s.chars().rev().collect();
                        Value::build_text(reversed)
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::Gcd => {
                check_arg_count!(arg_count, 2);
                let a = state.registers[*start_reg].get_value();
                let b = state.registers[*start_reg + 1].get_value();
                state.registers[*dest].set_value(crate::functions::math::exec_gcd(a, b)?);
            }
            ScalarFunc::Lcm => {
                check_arg_count!(arg_count, 2);
                let a = state.registers[*start_reg].get_value();
                let b = state.registers[*start_reg + 1].get_value();
                state.registers[*dest].set_value(crate::functions::math::exec_lcm(a, b)?);
            }
            ScalarFunc::Repeat => {
                check_arg_count!(arg_count, 2);
                let input = state.registers[*start_reg].get_value();
                let count = state.registers[*start_reg + 1].get_value();
                state.registers[*dest]
                    .set_value(crate::functions::string::exec_repeat(input, count));
            }
            ScalarFunc::Lpad => {
                let input = state.registers[*start_reg].get_value();
                let length = state.registers[*start_reg + 1].get_value();
                let fill = if arg_count >= 3 {
                    Some(state.registers[*start_reg + 2].get_value())
                } else {
                    None
                };
                state.registers[*dest]
                    .set_value(crate::functions::string::exec_lpad(input, length, fill));
            }
            ScalarFunc::Rpad => {
                let input = state.registers[*start_reg].get_value();
                let length = state.registers[*start_reg + 1].get_value();
                let fill = if arg_count >= 3 {
                    Some(state.registers[*start_reg + 2].get_value())
                } else {
                    None
                };
                state.registers[*dest]
                    .set_value(crate::functions::string::exec_rpad(input, length, fill));
            }
            ScalarFunc::BooleanToInt => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Numeric(Numeric::Integer(i)) => match *i {
                        0 => Value::from_i64(0),
                        1 => Value::from_i64(1),
                        _ => {
                            return Err(LimboError::Constraint(format!(
                                "invalid input for type boolean: \"{i}\""
                            ))
                            .into());
                        }
                    },
                    Value::Text(t) => {
                        let v = &t.value;
                        match v.to_ascii_lowercase().as_str() {
                            "true" | "t" | "yes" | "on" | "1" => Value::from_i64(1),
                            "false" | "f" | "no" | "off" | "0" => Value::from_i64(0),
                            _ => {
                                return Err(LimboError::Constraint(format!(
                                    "invalid input for type boolean: \"{v}\""
                                ))
                                .into());
                            }
                        }
                    }
                    other => {
                        return Err(LimboError::Constraint(format!(
                            "invalid input for type boolean: \"{other}\""
                        ))
                        .into());
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::IntToBoolean => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Numeric(Numeric::Integer(0)) => Value::build_text("false".to_string()),
                    _ => Value::build_text("true".to_string()),
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::ValidateIpAddr => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Text(t) => {
                        let v = &t.value;
                        v.parse::<std::net::IpAddr>().map_err(|_| {
                            LimboError::Constraint(format!("invalid input for type inet: \"{v}\""))
                        })?;
                        val.get_value().clone()
                    }
                    other => {
                        return Err(LimboError::Constraint(format!(
                            "invalid input for type inet: \"{other}\""
                        ))
                        .into());
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::NumericEncode => {
                check_arg_count!(arg_count, 3);
                let val = &state.registers[*start_reg];
                let precision_reg = &state.registers[*start_reg + 1];
                let scale_reg = &state.registers[*start_reg + 2];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    other => {
                        use crate::numeric::decimal::{
                            bigdecimal_to_blob, validate_precision_scale,
                        };
                        use bigdecimal::BigDecimal;
                        use std::str::FromStr;

                        let precision = match precision_reg.get_value() {
                            Value::Numeric(Numeric::Integer(i)) => *i,
                            _ => {
                                return Err(LimboError::Constraint(
                                    "numeric_encode: precision must be an integer".to_string(),
                                )
                                .into());
                            }
                        };
                        let scale = match scale_reg.get_value() {
                            Value::Numeric(Numeric::Integer(i)) => *i,
                            _ => {
                                return Err(LimboError::Constraint(
                                    "numeric_encode: scale must be an integer".to_string(),
                                )
                                .into());
                            }
                        };
                        let text = match other {
                            Value::Numeric(Numeric::Integer(i)) => i.to_string(),
                            Value::Numeric(Numeric::Float(f)) => f.to_string(),
                            Value::Text(t) => t.value.to_string(),
                            _ => {
                                return Err(LimboError::Constraint(format!(
                                    "invalid input for type numeric: \"{other}\""
                                ))
                                .into());
                            }
                        };
                        let bd = BigDecimal::from_str(&text).map_err(|_| {
                            LimboError::Constraint(format!(
                                "invalid input for type numeric: \"{text}\""
                            ))
                        })?;
                        let validated = validate_precision_scale(&bd, precision, scale)?;
                        Value::from_blob(bigdecimal_to_blob(&validated))
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::NumericDecode => {
                check_arg_count!(arg_count, 1);
                let val = &state.registers[*start_reg];
                let result = match val.get_value() {
                    Value::Null => Value::Null,
                    Value::Blob(b) => {
                        let bd = crate::numeric::decimal::blob_to_bigdecimal(b)?;
                        Value::build_text(crate::numeric::decimal::format_numeric(&bd))
                    }
                    other => {
                        return Err(LimboError::Constraint(format!(
                            "numeric_decode: expected blob, got \"{other}\""
                        ))
                        .into());
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv => {
                check_arg_count!(arg_count, 2);
                let lhs_val = state.registers[*start_reg].get_value().clone();
                let rhs_val = state.registers[*start_reg + 1].get_value().clone();
                let result = match (&lhs_val, &rhs_val) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    _ => {
                        let a = value_to_bigdecimal(&lhs_val)?;
                        let b = value_to_bigdecimal(&rhs_val)?;
                        let res = match scalar_func {
                            ScalarFunc::NumericAdd => a + b,
                            ScalarFunc::NumericSub => a - b,
                            ScalarFunc::NumericMul => a * b,
                            ScalarFunc::NumericDiv => {
                                use bigdecimal::Zero;
                                if b.is_zero() {
                                    return Err(LimboError::Constraint(
                                        "division by zero".to_string(),
                                    )
                                    .into());
                                }
                                a / b
                            }
                            _ => unreachable!(),
                        };
                        Value::build_text(crate::numeric::decimal::format_numeric(&res))
                    }
                };
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::NumericLt | ScalarFunc::NumericEq => {
                check_arg_count!(arg_count, 2);
                let lhs_val = state.registers[*start_reg].get_value().clone();
                let rhs_val = state.registers[*start_reg + 1].get_value().clone();
                match (&lhs_val, &rhs_val) {
                    (Value::Null, _) | (_, Value::Null) => state.registers[*dest].set_null(),
                    _ => {
                        let a = value_to_bigdecimal(&lhs_val)?;
                        let b = value_to_bigdecimal(&rhs_val)?;
                        let cmp_result = match scalar_func {
                            ScalarFunc::NumericLt => a < b,
                            ScalarFunc::NumericEq => a == b,
                            _ => unreachable!(),
                        };
                        state.registers[*dest].set_int(cmp_result as i64)
                    }
                };
            }
            ScalarFunc::ArrayAppend => {
                check_arg_count!(arg_count, 2);
                let arr_val = state.registers[*start_reg].get_value().clone();
                let elem_val = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_append(&arr_val, &elem_val)?);
            }
            ScalarFunc::ArrayPrepend => {
                check_arg_count!(arg_count, 2);
                let elem_val = state.registers[*start_reg].get_value().clone();
                let arr_val = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_prepend(&arr_val, &elem_val)?);
            }
            ScalarFunc::ArrayCat => {
                check_arg_count!(arg_count, 2);
                let a_val = state.registers[*start_reg].get_value().clone();
                let b_val = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_cat(&a_val, &b_val)?);
            }
            ScalarFunc::ArrayRemove => {
                check_arg_count!(arg_count, 2);
                let arr_val = state.registers[*start_reg].get_value().clone();
                let target = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_remove(&arr_val, &target)?);
            }
            ScalarFunc::ArrayContains => {
                check_arg_count!(arg_count, 2);
                let arr_val = state.registers[*start_reg].get_value().clone();
                let target = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_contains(&arr_val, &target));
            }
            ScalarFunc::ArrayPosition => {
                check_arg_count!(arg_count, 2);
                let arr_val = state.registers[*start_reg].get_value().clone();
                let target = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_position(&arr_val, &target));
            }
            ScalarFunc::ArrayLength => {
                // 1-arg form: equivalent to `array_length(arr, 1)`. 2-arg form
                // honors the dimension and walks into nested arrays for dim > 1
                // (Turso arrays are 1-indexed, so `array_upper(arr, dim)` shares
                // this code path via its alias and produces the same result).
                let arr_val = state.registers[*start_reg].get_value();
                let dim = if arg_count >= 2 {
                    state.registers[*start_reg + 1]
                        .get_value()
                        .as_int()
                        .unwrap_or(0)
                } else {
                    1
                };
                match compute_array_length_at_dim(arr_val, dim) {
                    Some(count) => state.registers[*dest].set_int(count),
                    None => state.registers[*dest].set_null(),
                };
            }
            ScalarFunc::ArraySlice => {
                check_arg_count!(arg_count, 3);
                let arr_val = state.registers[*start_reg].get_value().clone();
                let start_idx = state.registers[*start_reg + 1].get_value().clone();
                let end_idx = state.registers[*start_reg + 2].get_value().clone();
                let result = exec_array_slice(&arr_val, &start_idx, &end_idx)?;
                state.registers[*dest].set_value(result);
            }
            ScalarFunc::StringToArray => {
                let text = state.registers[*start_reg].get_value().clone();
                let delimiter = state.registers[*start_reg + 1].get_value().clone();
                let null_str = if arg_count >= 3 {
                    Some(state.registers[*start_reg + 2].get_value().clone())
                } else {
                    None
                };
                state.registers[*dest].set_value(exec_string_to_array(
                    &text,
                    &delimiter,
                    null_str.as_ref(),
                )?);
            }
            ScalarFunc::ArrayToString => {
                let arr_val = state.registers[*start_reg].get_value().clone();
                let delimiter = state.registers[*start_reg + 1].get_value().clone();
                let null_str = if arg_count >= 3 {
                    Some(state.registers[*start_reg + 2].get_value().clone())
                } else {
                    None
                };
                state.registers[*dest].set_value(exec_array_to_string(
                    &arr_val,
                    &delimiter,
                    null_str.as_ref(),
                ));
            }
            ScalarFunc::ArrayOverlap => {
                check_arg_count!(arg_count, 2);
                let a_val = state.registers[*start_reg].get_value().clone();
                let b_val = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_overlap(&a_val, &b_val));
            }
            ScalarFunc::ArrayContainsAll => {
                check_arg_count!(arg_count, 2);
                let a_val = state.registers[*start_reg].get_value().clone();
                let b_val = state.registers[*start_reg + 1].get_value().clone();
                state.registers[*dest].set_value(exec_array_contains_all(&a_val, &b_val));
            }
            ScalarFunc::StructPack
            | ScalarFunc::StructExtractFunc
            | ScalarFunc::UnionValueFunc
            | ScalarFunc::UnionTagFunc
            | ScalarFunc::UnionExtractFunc => {
                return Err(LimboError::InternalError(format!(
                    "{scalar_func} should be desugared to a dedicated instruction, not Function"
                ))
                .into());
            }
        },
        crate::function::Func::Vector(vector_func) => {
            let args = &state.registers[*start_reg..*start_reg + arg_count];
            match vector_func {
                VectorFunc::Vector => {
                    let result = vector32(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::Vector32 => {
                    let result = vector32(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::Vector32Sparse => {
                    let result = vector32_sparse(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::Vector64 => {
                    let result = vector64(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::Vector8 => {
                    let result = vector8(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::Vector1Bit => {
                    let result = vector1bit(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorExtract => {
                    let result = vector_extract(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorDistanceCos => {
                    let result = vector_distance_cos(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorDistanceDot => {
                    let result = vector_distance_dot(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorDistanceL2 => {
                    let result = vector_distance_l2(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorDistanceJaccard => {
                    let result = vector_distance_jaccard(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorConcat => {
                    let result = vector_concat(args)?;
                    state.registers[*dest].set_value(result);
                }
                VectorFunc::VectorSlice => {
                    let result = vector_slice(args)?;
                    state.registers[*dest].set_value(result)
                }
            }
        }
        crate::function::Func::External(f) => match f.func {
            ExtFunc::Scalar {
                context,
                callback,
                context_destructor,
                value_destructor,
                ..
            } => {
                let mut ext_values = Vec::with_capacity(arg_count);
                if arg_count != 0 {
                    let register_slice = &state.registers[*start_reg..*start_reg + arg_count];
                    for ov in register_slice.iter() {
                        ext_values.push(ov.get_value().to_ffi());
                    }
                }
                let argv_ptr = if ext_values.is_empty() {
                    std::ptr::null()
                } else {
                    ext_values.as_ptr()
                };
                let mut result = unsafe {
                    callback(
                        context,
                        arg_count as i32,
                        argv_ptr,
                        context_destructor,
                        value_destructor,
                    )
                };
                let value = Value::from_ffi_ref(&result);
                if let Some(value_destructor) = value_destructor {
                    unsafe { value_destructor(&mut result) };
                } else {
                    unsafe { result.__free_internal_type() };
                }
                for ext_value in ext_values {
                    unsafe { ext_value.__free_internal_type() };
                }
                state.registers[*dest].set_value(value?);
            }
            _ => unreachable!("aggregate called in scalar context"),
        },
        crate::function::Func::Math(math_func) => match math_func.arity() {
            MathFuncArity::Nullary => match math_func {
                MathFunc::Pi => {
                    state.registers[*dest].set_float(
                        NonNan::new(std::f64::consts::PI).expect("PI is a valid NonNan"),
                    );
                }
                _ => {
                    unreachable!("Unexpected mathematical Nullary function {:?}", math_func);
                }
            },

            MathFuncArity::Unary => {
                let reg_value = &state.registers[*start_reg];
                let result = reg_value.get_value().exec_math_unary(math_func);
                state.registers[*dest].set_value(result);
            }

            MathFuncArity::Binary => {
                let lhs = &state.registers[*start_reg];
                let rhs = &state.registers[*start_reg + 1];
                let result = lhs.get_value().exec_math_binary(rhs.get_value(), math_func);
                state.registers[*dest].set_value(result);
            }

            MathFuncArity::UnaryOrBinary => match math_func {
                MathFunc::Log => {
                    let result = match arg_count {
                        1 => {
                            let arg = &state.registers[*start_reg];
                            arg.get_value().exec_math_log(None)
                        }
                        2 => {
                            let base = &state.registers[*start_reg];
                            let arg = &state.registers[*start_reg + 1];
                            arg.get_value().exec_math_log(Some(base.get_value()))
                        }
                        _ => unreachable!(
                            "{:?} function with unexpected number of arguments",
                            math_func
                        ),
                    };
                    state.registers[*dest].set_value(result);
                }
                _ => unreachable!(
                    "Unexpected mathematical UnaryOrBinary function {:?}",
                    math_func
                ),
            },
        },
        crate::function::Func::Dialect(name) => {
            let args: Vec<Value> = state.registers[*start_reg..*start_reg + arg_count]
                .iter()
                .map(|r| r.get_value().clone())
                .collect();
            let result = program.connection.dialect().exec_scalar_function(
                &program.connection,
                name,
                &args,
            )?;
            state.registers[*dest].set_value(result);
        }
        crate::function::Func::AlterTable(alter_func) => {
            let r#type = &state.registers[*start_reg].get_value().clone();
            let Value::Text(entry_type) = r#type else {
                panic!("sqlite_schema.type should be TEXT")
            };

            let Value::Text(name) = &state.registers[*start_reg + 1].get_value() else {
                panic!("sqlite_schema.name should be TEXT")
            };
            let name = name.to_string();

            let Value::Text(tbl_name) = &state.registers[*start_reg + 2].get_value() else {
                panic!("sqlite_schema.tbl_name should be TEXT")
            };
            let tbl_name = tbl_name.to_string();

            let Value::Numeric(Numeric::Integer(root_page)) =
                &state.registers[*start_reg + 3].get_value().clone()
            else {
                panic!("sqlite_schema.root_page should be INTEGER")
            };

            let sql = &state.registers[*start_reg + 4].get_value().clone();

            let (new_name, new_tbl_name, new_sql) = match alter_func {
                AlterTableFunc::RenameTable => {
                    let rename_from = {
                        match &state.registers[*start_reg + 5].get_value() {
                            Value::Text(rename_from) => normalize_ident(rename_from.as_str()),
                            _ => panic!("rename_from parameter should be TEXT"),
                        }
                    };

                    let original_rename_to = {
                        match &state.registers[*start_reg + 6].get_value() {
                            Value::Text(rename_to) => rename_to,
                            _ => panic!("rename_to parameter should be TEXT"),
                        }
                    };
                    let rename_to = normalize_ident(original_rename_to.as_str());

                    let new_name = if let Some(column) =
                        &name.strip_prefix(&format!("sqlite_autoindex_{rename_from}_"))
                    {
                        format!("sqlite_autoindex_{rename_to}_{column}")
                    } else if name == rename_from {
                        rename_to.clone()
                    } else {
                        name
                    };

                    let new_tbl_name = if tbl_name == rename_from {
                        rename_to.clone()
                    } else {
                        tbl_name
                    };

                    let new_sql = 'sql: {
                        let Value::Text(sql) = sql else {
                            break 'sql None;
                        };

                        let cmd = parse_schema_sql_for_alter(
                            program.connection.dialect().as_ref(),
                            entry_type.as_str(),
                            *root_page,
                            sql.as_str(),
                        )?;
                        let Some(ast::Cmd::Stmt(stmt)) = cmd else {
                            return Err(LimboError::InternalError(
                                "Unexpected command during ALTER TABLE RENAME processing"
                                    .to_string(),
                            )
                            .into());
                        };

                        match stmt {
                            ast::Stmt::CreateIndex {
                                tbl_name,
                                unique,
                                if_not_exists,
                                idx_name,
                                columns,
                                where_clause,
                                using,
                                with_clause,
                            } => {
                                let table_name = normalize_ident(tbl_name.as_str());

                                if rename_from != table_name {
                                    break 'sql None;
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        tbl_name: ast::Name::exact(original_rename_to.to_string()),
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        columns,
                                        where_clause,
                                        using,
                                        with_clause,
                                    }
                                    .to_string(),
                                )
                            }
                            ast::Stmt::CreateTable {
                                tbl_name,
                                temporary,
                                if_not_exists,
                                body,
                            } => {
                                let this_table = normalize_ident(tbl_name.name.as_str());

                                let ast::CreateTableBody::ColumnsAndConstraints {
                                    mut columns,
                                    mut constraints,
                                    options,
                                } = body
                                else {
                                    return Err(LimboError::InternalError(
                                        "CREATE TABLE AS SELECT schemas cannot be altered"
                                            .to_string(),
                                    )
                                    .into());
                                };

                                let mut any_change = false;

                                // Rewrite FK targets in both paths
                                for c in &mut constraints {
                                    if let ast::TableConstraint::ForeignKey { clause, .. } =
                                        &mut c.constraint
                                    {
                                        any_change |= rewrite_fk_parent_table_if_needed(
                                            clause,
                                            &rename_from,
                                            original_rename_to.as_str(),
                                        );
                                    }
                                }
                                for col in &mut columns {
                                    any_change |= rewrite_inline_col_fk_target_if_needed(
                                        col,
                                        &rename_from,
                                        original_rename_to.as_str(),
                                    );
                                }

                                // Rewrite table-qualified refs in CHECK constraints
                                // (e.g. t1.a > 0 → t2.a > 0)
                                if this_table == rename_from {
                                    for c in &mut constraints {
                                        if let ast::TableConstraint::Check {
                                            ref mut expr,
                                            ref mut source,
                                        } = c.constraint
                                        {
                                            rewrite_check_expr_table_refs(
                                                expr,
                                                &rename_from,
                                                &rename_to,
                                            );
                                            // The captured source text no longer
                                            // matches the rewritten expression.
                                            *source = None;
                                        }
                                    }
                                    for col in &mut columns {
                                        for cc in &mut col.constraints {
                                            if let ast::ColumnConstraint::Check {
                                                ref mut expr,
                                                ref mut source,
                                            } = cc.constraint
                                            {
                                                rewrite_check_expr_table_refs(
                                                    expr,
                                                    &rename_from,
                                                    &rename_to,
                                                );
                                                *source = None;
                                            }
                                        }
                                    }
                                }

                                if this_table == rename_from {
                                    // Rebuild with new table identifier so SQL persists the new name.
                                    let new_stmt = ast::Stmt::CreateTable {
                                        tbl_name: ast::QualifiedName {
                                            db_name: None,
                                            name: ast::Name::exact(original_rename_to.to_string()),
                                            alias: None,
                                        },
                                        temporary,
                                        if_not_exists,
                                        body: ast::CreateTableBody::ColumnsAndConstraints {
                                            columns,
                                            constraints,
                                            options,
                                        },
                                    };
                                    Some(
                                        program
                                            .connection
                                            .dialect()
                                            .format_rewritten_table_sql(&new_stmt)?,
                                    )
                                } else {
                                    // Other tables: only emit if we actually changed their FK targets.
                                    if !any_change {
                                        break 'sql None;
                                    }
                                    let new_stmt = ast::Stmt::CreateTable {
                                        tbl_name,
                                        temporary,
                                        if_not_exists,
                                        body: ast::CreateTableBody::ColumnsAndConstraints {
                                            columns,
                                            constraints,
                                            options,
                                        },
                                    };
                                    Some(
                                        program
                                            .connection
                                            .dialect()
                                            .format_rewritten_table_sql(&new_stmt)?,
                                    )
                                }
                            }
                            ast::Stmt::CreateVirtualTable(ast::CreateVirtualTable {
                                tbl_name,
                                if_not_exists,
                                module_name,
                                args,
                            }) => {
                                let this_table = normalize_ident(tbl_name.name.as_str());
                                if this_table != rename_from {
                                    None
                                } else {
                                    let new_stmt =
                                        ast::Stmt::CreateVirtualTable(ast::CreateVirtualTable {
                                            tbl_name: ast::QualifiedName {
                                                db_name: tbl_name.db_name,
                                                name: ast::Name::exact(
                                                    original_rename_to.to_string(),
                                                ),
                                                alias: None,
                                            },
                                            if_not_exists,
                                            module_name,
                                            args,
                                        });
                                    Some(new_stmt.to_string())
                                }
                            }
                            ast::Stmt::CreateTrigger {
                                temporary,
                                if_not_exists,
                                trigger_name,
                                time,
                                event,
                                tbl_name: trigger_tbl_name,
                                for_each_row,
                                mut when_clause,
                                mut commands,
                            } => {
                                let trigger_tbl = normalize_ident(trigger_tbl_name.name.as_str());

                                // Rewrite ON table name if it matches the renamed table
                                let new_trigger_tbl_name = if trigger_tbl == rename_from {
                                    ast::QualifiedName {
                                        db_name: trigger_tbl_name.db_name,
                                        name: ast::Name::exact(original_rename_to.to_string()),
                                        alias: None,
                                    }
                                } else {
                                    trigger_tbl_name
                                };

                                // Rewrite WHEN clause qualified refs
                                if let Some(ref mut when) = when_clause {
                                    rewrite_check_expr_table_refs(
                                        when,
                                        &rename_from,
                                        original_rename_to.as_str(),
                                    );
                                }

                                // Rewrite table references in trigger body commands
                                for cmd in &mut commands {
                                    rewrite_trigger_cmd_table_refs(
                                        cmd,
                                        &rename_from,
                                        original_rename_to.as_str(),
                                    );
                                }

                                Some(
                                    ast::Stmt::CreateTrigger {
                                        temporary,
                                        if_not_exists,
                                        trigger_name,
                                        time,
                                        event,
                                        tbl_name: new_trigger_tbl_name,
                                        for_each_row,
                                        when_clause,
                                        commands,
                                    }
                                    .to_string(),
                                )
                            }
                            _ => None,
                        }
                    };

                    (new_name, new_tbl_name, new_sql)
                }
                AlterTableFunc::AlterColumn | AlterTableFunc::RenameColumn => {
                    let table = {
                        match &state.registers[*start_reg + 5].get_value() {
                            Value::Text(rename_to) => normalize_ident(rename_to.as_str()),
                            _ => panic!("table parameter should be TEXT"),
                        }
                    };

                    let original_rename_from = {
                        match &state.registers[*start_reg + 6].get_value() {
                            Value::Text(rename_from) => rename_from,
                            _ => panic!("rename_from parameter should be TEXT"),
                        }
                    };
                    let rename_from = normalize_ident(original_rename_from.as_str());

                    let column_def = {
                        match &state.registers[*start_reg + 7].get_value() {
                            Value::Text(column_def) => column_def.as_str(),
                            _ => panic!("rename_to parameter should be TEXT"),
                        }
                    };

                    let column_def =
                        Parser::new(column_def.as_bytes()).parse_column_definition(true)?;

                    let _rename_to = normalize_ident(column_def.col_name.as_str());

                    let new_sql = 'sql: {
                        let Value::Text(sql) = sql else {
                            break 'sql None;
                        };

                        let cmd = parse_schema_sql_for_alter(
                            program.connection.dialect().as_ref(),
                            entry_type.as_str(),
                            *root_page,
                            sql.as_str(),
                        )?;
                        let Some(ast::Cmd::Stmt(stmt)) = cmd else {
                            return Err(LimboError::InternalError(
                                "Unexpected command during ALTER TABLE RENAME COLUMN processing"
                                    .to_string(),
                            )
                            .into());
                        };

                        match stmt {
                            ast::Stmt::CreateIndex {
                                tbl_name,
                                mut columns,
                                unique,
                                if_not_exists,
                                idx_name,
                                mut where_clause,
                                using,
                                with_clause,
                            } => {
                                if table != normalize_ident(tbl_name.as_str()) {
                                    break 'sql None;
                                }

                                for column in &mut columns {
                                    rename_identifiers(
                                        column.expr.as_mut(),
                                        &rename_from,
                                        column_def.col_name.as_str(),
                                    );
                                }

                                if let Some(ref mut wc) = where_clause {
                                    rename_identifiers(
                                        wc,
                                        &rename_from,
                                        column_def.col_name.as_str(),
                                    );
                                }

                                Some(
                                    ast::Stmt::CreateIndex {
                                        tbl_name,
                                        columns,
                                        unique,
                                        if_not_exists,
                                        idx_name,
                                        where_clause,
                                        using,
                                        with_clause,
                                    }
                                    .to_string(),
                                )
                            }
                            ast::Stmt::CreateTable {
                                tbl_name,
                                body,
                                temporary,
                                if_not_exists,
                            } => {
                                let ast::CreateTableBody::ColumnsAndConstraints {
                                    mut columns,
                                    mut constraints,
                                    options,
                                } = body
                                else {
                                    return Err(LimboError::InternalError(
                                        "CREATE TABLE AS SELECT schemas cannot be altered"
                                            .to_string(),
                                    )
                                    .into());
                                };

                                let normalized_tbl_name = normalize_ident(tbl_name.name.as_str());

                                if normalized_tbl_name == table {
                                    // This is the table being altered - update its column
                                    let Some(column) = columns.iter_mut().find(|column| {
                                        normalize_ident(column.col_name.as_str()) == rename_from
                                    }) else {
                                        // MVCC/temp-schema rewrite can reach an already-updated
                                        // CREATE TABLE SQL image for the target table. Treat that
                                        // as idempotent and keep the existing SQL text.
                                        break 'sql None;
                                    };

                                    match alter_func {
                                        AlterTableFunc::AlterColumn => *column = column_def.clone(),
                                        AlterTableFunc::RenameColumn => {
                                            column.col_name = column_def.col_name.clone()
                                        }
                                        _ => unreachable!(),
                                    }

                                    // Update table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY)
                                    for constraint in &mut constraints {
                                        match &mut constraint.constraint {
                                            ast::TableConstraint::PrimaryKey {
                                                columns: pk_cols,
                                                ..
                                            } => {
                                                for col in pk_cols {
                                                    let (ast::Expr::Name(ref name)
                                                    | ast::Expr::Id(ref name)) = *col.expr
                                                    else {
                                                        return Err(LimboError::ParseError("Unexpected expression in PRIMARY KEY constraint".to_string()).into());
                                                    };
                                                    if normalize_ident(name.as_str()) == rename_from
                                                    {
                                                        *col.expr = ast::Expr::Name(Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        ));
                                                    }
                                                }
                                            }
                                            ast::TableConstraint::Unique {
                                                columns: uniq_cols,
                                                ..
                                            } => {
                                                for col in uniq_cols {
                                                    let (ast::Expr::Name(ref name)
                                                    | ast::Expr::Id(ref name)) = *col.expr
                                                    else {
                                                        return Err(LimboError::ParseError("Unexpected expression in UNIQUE constraint".to_string()).into());
                                                    };
                                                    if normalize_ident(name.as_str()) == rename_from
                                                    {
                                                        *col.expr = ast::Expr::Name(Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        ));
                                                    }
                                                }
                                            }
                                            ast::TableConstraint::ForeignKey {
                                                columns: child_cols,
                                                clause,
                                                ..
                                            } => {
                                                // Update child columns in this table's FK definitions
                                                for child_col in child_cols {
                                                    if normalize_ident(child_col.col_name.as_str())
                                                        == rename_from
                                                    {
                                                        child_col.col_name = Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        );
                                                    }
                                                }
                                                rewrite_fk_parent_cols_if_self_ref(
                                                    clause,
                                                    &normalized_tbl_name,
                                                    &rename_from,
                                                    column_def.col_name.as_str(),
                                                );
                                            }
                                            ast::TableConstraint::Check {
                                                ref mut expr,
                                                ref mut source,
                                            } => {
                                                rename_identifiers(
                                                    expr,
                                                    &rename_from,
                                                    column_def.col_name.as_str(),
                                                );
                                                *source = None;
                                            }
                                        }
                                    }

                                    for col in &mut columns {
                                        rewrite_column_references_if_needed(
                                            col,
                                            &normalized_tbl_name,
                                            &rename_from,
                                            column_def.col_name.as_str(),
                                        )?;
                                    }
                                } else {
                                    // This is a different table, check if it has FKs referencing the renamed column
                                    let mut fk_updated = false;

                                    for constraint in &mut constraints {
                                        if let ast::TableConstraint::ForeignKey {
                                            columns: _,
                                            clause:
                                                ForeignKeyClause {
                                                    tbl_name,
                                                    columns: parent_cols,
                                                    ..
                                                },
                                            ..
                                        } = &mut constraint.constraint
                                        {
                                            // Check if this FK references the table being altered
                                            if normalize_ident(tbl_name.as_str()) == table {
                                                // Update parent column references if they match the renamed column
                                                for parent_col in parent_cols {
                                                    if normalize_ident(parent_col.col_name.as_str())
                                                        == rename_from
                                                    {
                                                        parent_col.col_name = Name::exact(
                                                            column_def.col_name.as_str().to_owned(),
                                                        );
                                                        fk_updated = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    for col in &mut columns {
                                        let _before = fk_updated;
                                        let mut local_col = col.clone();
                                        rewrite_column_level_fk_parent_columns_if_needed(
                                            &mut local_col,
                                            &table,
                                            &rename_from,
                                            column_def.col_name.as_str(),
                                        );
                                        if local_col != *col {
                                            *col = local_col;
                                            fk_updated = true;
                                        }
                                    }

                                    // Only return updated SQL if we actually changed something
                                    if !fk_updated {
                                        break 'sql None;
                                    }
                                }
                                let new_stmt = ast::Stmt::CreateTable {
                                    tbl_name,
                                    body: ast::CreateTableBody::ColumnsAndConstraints {
                                        columns,
                                        constraints,
                                        options,
                                    },
                                    temporary,
                                    if_not_exists,
                                };
                                Some(
                                    program
                                        .connection
                                        .dialect()
                                        .format_rewritten_table_sql(&new_stmt)?,
                                )
                            }
                            // Trigger SQL is rewritten by separate UPDATE statements
                            // generated by alter.rs (via rewrite_trigger_sql_for_column_rename),
                            // so we skip triggers here to avoid redundant work.
                            _ => None,
                        }
                    };

                    (name, tbl_name, new_sql)
                }
            };

            state.registers[*dest].set_value(r#type.clone());
            state.registers[*dest + 1].set_text(Text::from(new_name))?;
            state.registers[*dest + 2].set_text(Text::from(new_tbl_name))?;
            state.registers[*dest + 3].set_int(*root_page);

            if let Some(new_sql) = new_sql {
                state.registers[*dest + 4].set_text(Text::from(new_sql))?;
            } else {
                state.registers[*dest + 4].set_value(sql.clone());
            }
        }
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        crate::function::Func::Fts(fts_func) => {
            // FTS functions are typically handled via index method pattern matching.
            // If we reach here, just return a fallback since no FTS index matched.
            use crate::function::FtsFunc;
            match fts_func {
                FtsFunc::Score => {
                    // Without an FTS index match, return 0.0 as a default score
                    state.registers[*dest]
                        .set_float(NonNan::new(0.0).expect("0.0 is a valid NonNan"));
                }
                FtsFunc::Match => {
                    // fts_match(col1, col2, ..., query): returns 1 if any column matches query
                    // Minimum: fts_match(text, query) = 2 args
                    if arg_count < 2 {
                        return Err(LimboError::InvalidArgument(
                            "fts_match requires at least 2 arguments: text, query".to_string(),
                        )
                        .into());
                    }

                    // Last arg is the query, first N-1 args are text columns
                    let num_text_cols = arg_count - 1;
                    let query = state.registers[*start_reg + num_text_cols].get_value();

                    if matches!(query, Value::Null) {
                        state.registers[*dest].set_int(0);
                    } else {
                        let query_str = query.to_string();

                        // Concatenate all text columns with space separator
                        let est_len = 16;
                        let mut combined_text = String::with_capacity(num_text_cols * est_len);
                        for i in 0..num_text_cols {
                            let text = state.registers[*start_reg + i].get_value();
                            if !matches!(text, Value::Null) {
                                if !combined_text.is_empty() {
                                    combined_text.push(' ');
                                }
                                combined_text.push_str(&text.to_string());
                            }
                        }

                        let matches =
                            crate::index_method::fts::fts_match(&combined_text, &query_str);
                        state.registers[*dest].set_int(matches.into());
                    }
                }
                FtsFunc::Highlight => {
                    // fts_highlight(col1, col2, ..., before_tag, after_tag, query)
                    // Variable number of text columns, followed by before_tag, after_tag, query
                    // Minimum: fts_highlight(text, before_tag, after_tag, query) = 4 args
                    if arg_count < 4 {
                        return Err(LimboError::InvalidArgument(
                            "fts_highlight requires at least 4 arguments: text, before_tag, after_tag, query"
                                .to_string(),
                        ).into());
                    }

                    // Last 3 args are: before_tag, after_tag, query
                    // First N-3 args are text columns
                    let num_text_cols = arg_count - 3;
                    let before_tag = state.registers[*start_reg + num_text_cols].get_value();
                    let after_tag = state.registers[*start_reg + num_text_cols + 1].get_value();
                    let query = state.registers[*start_reg + num_text_cols + 2].get_value();

                    // Handle NULL values in tags or query
                    if matches!(query, Value::Null)
                        || matches!(before_tag, Value::Null)
                        || matches!(after_tag, Value::Null)
                    {
                        state.registers[*dest].set_null();
                    } else {
                        let query_str = query.to_string();
                        let before_str = before_tag.to_string();
                        let after_str = after_tag.to_string();

                        // Concatenate all text columns with space separator
                        let mut combined_text = String::new();
                        for i in 0..num_text_cols {
                            let text = state.registers[*start_reg + i].get_value();
                            if !matches!(text, Value::Null) {
                                if !combined_text.is_empty() {
                                    combined_text.push(' ');
                                }
                                combined_text.push_str(&text.to_string());
                            }
                        }

                        let highlighted = crate::index_method::fts::fts_highlight(
                            &combined_text,
                            &query_str,
                            &before_str,
                            &after_str,
                        );
                        state.registers[*dest].set_text(Text::new(highlighted))?;
                    }
                }
            }
        }
        crate::function::Func::Agg(_) => {
            unreachable!("Aggregate functions should not be handled here")
        }
        crate::function::Func::Window(_) => {
            unreachable!("Window functions should not be handled here")
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub(crate) type OpAttachState = crate::connection::AttachDatabaseState;

pub fn op_sequence(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Sequence {
            cursor_id,
            target_reg
        },
        insn
    );
    let cursor_seq = state
        .cursor_seqs
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid");
    let seq_num = *cursor_seq;
    *cursor_seq += 1;
    state.registers[*target_reg].set_int(seq_num);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_sequence_test(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SequenceTest {
            cursor_id,
            target_pc,
            value_reg: _
        },
        insn
    );
    let cursor_seq = state
        .cursor_seqs
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid");
    let was_zero = *cursor_seq == 0;
    *cursor_seq += 1;
    state.pc = if was_zero {
        target_pc.as_offset_int()
    } else {
        state.pc + 1
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_init_coroutine(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        InitCoroutine {
            yield_reg,
            jump_on_definition,
            start_offset,
        },
        insn
    );
    assert!(jump_on_definition.is_offset());
    let start_offset = start_offset.as_offset_int();
    state.registers[*yield_reg].set_int(start_offset as i64);
    state.ended_coroutine.retain(|n| *n != *yield_reg as u32);
    let jump_on_definition = jump_on_definition.as_offset_int();
    state.pc = if jump_on_definition == 0 {
        state.pc + 1
    } else {
        jump_on_definition
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_end_coroutine(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(EndCoroutine { yield_reg }, insn);

    if let Value::Numeric(Numeric::Integer(pc)) = state.registers[*yield_reg].get_value() {
        state.ended_coroutine.push(*yield_reg as u32);
        let pc: u32 = (*pc)
            .try_into()
            .unwrap_or_else(|_| panic!("EndCoroutine: pc overflow: {pc}"));
        state.pc = pc - 1; // yield jump is always next to yield. Here we subtract 1 to go back to yield instruction
    } else {
        unreachable!();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_yield(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Yield {
            yield_reg,
            end_offset,
            subtype_clear_start_reg,
            subtype_clear_count,
        },
        insn
    );
    if let Value::Numeric(Numeric::Integer(pc)) = state.registers[*yield_reg].get_value() {
        if state.ended_coroutine.contains(&(*yield_reg as u32)) {
            state.pc = end_offset.as_offset_int();
        } else {
            let pc: u32 = (*pc)
                .try_into()
                .unwrap_or_else(|_| panic!("Yield: pc overflow: {pc}"));
            // swap the program counter with the value in the yield register
            // this is the mechanism that allows jumping back and forth between the coroutine and the caller
            state.registers[*yield_reg].set_int((state.pc + 1) as i64);
            state.pc = pc;

            // Strip JSON subtypes from co-routine output columns so they do not
            // survive the subquery boundary, matching SQLite's OP_Copy P5=0x0002.
            // subtype_clear_count > 0 only for coroutine body yields.
            #[cfg(feature = "json")]
            if *subtype_clear_count > 0 {
                use crate::types::TextSubtype;
                for reg in &mut state.registers
                    [*subtype_clear_start_reg..*subtype_clear_start_reg + *subtype_clear_count]
                {
                    if let Register::Value(Value::Text(text)) = reg {
                        if text.subtype == TextSubtype::Json {
                            text.subtype = TextSubtype::Text;
                        }
                    }
                }
            }
        }
    } else {
        unreachable!(
            "yield_reg {} contains non-integer value: {:?}",
            *yield_reg, state.registers[*yield_reg]
        );
    }
    Ok(InsnFunctionStepResult::Step)
}

pub struct OpInsertState {
    pub sub_state: OpInsertSubState,
    pub old_record: Option<(i64, crate::alloc::Vec<Value>)>,
    /// Set by the NoopCheck sub-state to indicate the row already has the exact
    /// same payload, so the physical write can be skipped.
    pub is_noop_update: bool,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OpInsertSubState {
    /// If this insert overwrites a record, capture the old record for incremental view maintenance.
    /// If cursor is already positioned (no REQUIRE_SEEK), capture directly.
    /// If REQUIRE_SEEK is set, transition to Seek first.
    MaybeCaptureRecord,
    /// Seek to the correct position if needed.
    /// In a table insert, if the caller does not pass InsertFlags::REQUIRE_SEEK, they must ensure that a seek has already happened to the correct location.
    /// This typically happens by invoking either Insn::NewRowid or Insn::NotExists, because:
    /// 1. op_new_rowid() seeks to the end of the table, which is the correct insertion position.
    /// 2. op_not_exists() seeks to the position in the table where the target rowid would be inserted.
    Seek,
    /// Capture the old record at the current cursor position for IVM.
    /// The cursor must already be positioned (by a prior seek or by NotExists/NewRowid).
    CaptureRecord,
    /// Check whether the update is a no-op (existing record matches new record).
    /// Must complete before Insert so that cursor.rowid()/record() are never
    /// interleaved with a partially-completed cursor.insert().
    NoopCheck,
    /// Insert the row into the table.
    Insert,
    /// Updating last_insert_rowid may return IO, so we need a separate state for it so that we don't
    /// start inserting the same row multiple times.
    UpdateLastRowid,
    /// If there are dependent incremental views, apply the change.
    ApplyViewChange,
}

pub fn op_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Insert {
            cursor: cursor_id,
            key_reg,
            record_reg,
            flag,
            table_name,
        },
        insn
    );

    loop {
        match state.active_op_state.insert().sub_state {
            OpInsertSubState::MaybeCaptureRecord => {
                let has_dependent_views = {
                    let schema = program.connection.schema.read();
                    !schema
                        .get_dependent_materialized_views(table_name)
                        .is_empty()
                };
                // If there are no dependent views, we don't need to capture the old record.
                // We also don't need to do it if the rowid of the UPDATEd row was changed, because
                // op_delete already captured the deletion for IVM, and this insert only needs to
                // record the new row (which ApplyViewChange handles without old_record).
                let needs_capture =
                    has_dependent_views && !flag.has(InsertFlags::UPDATE_ROWID_CHANGE);

                if flag.has(InsertFlags::REQUIRE_SEEK) {
                    state.active_op_state.insert().sub_state = OpInsertSubState::Seek;
                } else if needs_capture {
                    state.active_op_state.insert().sub_state = OpInsertSubState::CaptureRecord;
                } else {
                    state.active_op_state.insert().sub_state = OpInsertSubState::NoopCheck;
                }
                continue;
            }
            OpInsertSubState::Seek => {
                let is_without_rowid = {
                    let cursor = get_cursor!(state, *cursor_id);
                    !cursor.as_btree_mut().has_rowid()
                };
                if let SeekInternalResult::IO(io) = seek_internal(
                    program,
                    state,
                    pager,
                    if is_without_rowid {
                        RecordSource::Packed {
                            record_reg: *record_reg,
                        }
                    } else {
                        RecordSource::Unpacked {
                            start_reg: *key_reg,
                            num_regs: 1,
                        }
                    },
                    *cursor_id,
                    is_without_rowid,
                    SeekOp::GE { eq_only: true },
                )? {
                    return Ok(InsnFunctionStepResult::IO(io));
                }
                let has_dependent_views = {
                    let schema = program.connection.schema.read();
                    !schema
                        .get_dependent_materialized_views(table_name)
                        .is_empty()
                };
                let needs_capture =
                    has_dependent_views && !flag.has(InsertFlags::UPDATE_ROWID_CHANGE);
                if needs_capture {
                    state.active_op_state.insert().sub_state = OpInsertSubState::CaptureRecord;
                } else {
                    state.active_op_state.insert().sub_state = OpInsertSubState::NoopCheck;
                }
                continue;
            }
            OpInsertSubState::CaptureRecord => {
                {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    if !cursor.has_rowid() {
                        state.active_op_state.insert().old_record = None;
                        state.active_op_state.insert().sub_state = OpInsertSubState::NoopCheck;
                        continue;
                    }
                }
                let insert_key = match &state.registers[*key_reg].get_value() {
                    Value::Numeric(Numeric::Integer(i)) => *i,
                    _ => unreachable!("expected integer key in insert"),
                };

                let cursor = state.get_cursor(*cursor_id);
                let cursor = cursor.as_btree_mut();
                let maybe_key = return_if_io!(cursor.rowid());
                let old_record = if let Some(key) = maybe_key {
                    if key == insert_key {
                        let maybe_record = return_if_io!(cursor.record());
                        if let Some(record) = maybe_record {
                            let mut values = record.get_values_owned()?;
                            let schema = program.connection.schema.read();
                            if let Some(table) = schema.get_table(table_name) {
                                for (i, col) in table.columns().iter().enumerate() {
                                    if col.is_rowid_alias() && i < values.len() {
                                        values[i] = Value::from_i64(key);
                                    }
                                }
                            }
                            Some((key, values))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                state.active_op_state.insert().old_record = old_record;
                state.active_op_state.insert().sub_state = OpInsertSubState::NoopCheck;
                continue;
            }
            // TODO: add some InsertFlags that allows us to skip this check when we know for
            // certain that the update is not a no-op to avoid the branch.
            OpInsertSubState::NoopCheck => {
                // UPDATE fast path: skip the physical write if the target row already
                // has the exact same record payload. This check is isolated in its own
                // sub-state so that cursor.rowid()/record() IO yields never interleave
                // with a partially-completed cursor.insert().
                //
                // In MVCC mode this check must be skipped: after Delete, cursor.record()
                // cannot reliably return the pre-delete payload (btree-resident rows
                // still appear physically intact, MVCC-store rows become invisible).
                // The noop check is fundamentally incompatible with the MVCC
                // Delete+Insert update pattern.
                state.active_op_state.insert().is_noop_update = false;
                let is_mvcc = {
                    let cursor_ref = get_cursor!(state, *cursor_id);
                    cursor_ref.as_btree_mut().is_mvcc()
                };
                let has_rowid = {
                    let cursor = get_cursor!(state, *cursor_id);
                    cursor.as_btree_mut().has_rowid()
                };
                if !is_mvcc
                    && has_rowid
                    && flag.has(InsertFlags::SKIP_LAST_ROWID)
                    && !flag.has(InsertFlags::UPDATE_ROWID_CHANGE)
                {
                    let key = match &state.registers[*key_reg].get_value() {
                        Value::Numeric(Numeric::Integer(i)) => *i,
                        _ => unreachable!("expected integer key"),
                    };
                    let cursor = get_cursor!(state, *cursor_id);
                    let cursor = cursor.as_btree_mut();
                    let existing_key = return_if_io!(cursor.rowid());
                    if existing_key == Some(key) {
                        let record = match &state.registers[*record_reg] {
                            Register::Record(r) => std::borrow::Cow::Borrowed(r),
                            Register::Value(value) => {
                                let values = [value];
                                let record = ImmutableRecord::from_values(values, values.len())?;
                                std::borrow::Cow::Owned(record)
                            }
                            Register::Aggregate(..) => {
                                unreachable!("Cannot insert an aggregate value.")
                            }
                        };
                        let existing_record = return_if_io!(cursor.record());
                        if existing_record.is_some_and(|r| r == record.as_ref()) {
                            state.active_op_state.insert().is_noop_update = true;
                        }
                    }
                }
                state.active_op_state.insert().sub_state = OpInsertSubState::Insert;
                continue;
            }
            OpInsertSubState::Insert => {
                if !state.active_op_state.insert().is_noop_update {
                    let record = match &state.registers[*record_reg] {
                        Register::Record(r) => std::borrow::Cow::Borrowed(r),
                        Register::Value(value) => {
                            let values = [value];
                            let record = ImmutableRecord::from_values(values, values.len())?;
                            std::borrow::Cow::Owned(record)
                        }
                        Register::Aggregate(..) => {
                            unreachable!("Cannot insert an aggregate value.")
                        }
                    };
                    let cursor = get_cursor!(state, *cursor_id);
                    let cursor = cursor.as_btree_mut();
                    if cursor.has_rowid() {
                        let key = match &state.registers[*key_reg].get_value() {
                            Value::Numeric(Numeric::Integer(i)) => *i,
                            _ => unreachable!("expected integer key"),
                        };
                        return_if_io!(cursor.insert(&BTreeKey::new_table_rowid(key, Some(&record))));
                    } else {
                        return_if_io!(
                            cursor.insert(&BTreeKey::new_index_key(record.as_record_ref()))
                        );
                    }
                    state.record_rows_written(1);
                }
                // Only update last_insert_rowid for regular table inserts, not schema modifications
                let root_page = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    cursor.root_page()
                };
                if root_page != 1
                    && table_name != SQLITE_SEQUENCE_TABLE_NAME
                    && !flag.has(InsertFlags::EPHEMERAL_TABLE_INSERT)
                {
                    state.active_op_state.insert().sub_state = OpInsertSubState::UpdateLastRowid;
                } else {
                    // Schema table writes (sqlite_master, sqlite_sequence, ephemeral)
                    // must not produce view deltas. The p4 table_name on these inserts
                    // refers to the *target* table (for the update hook), not the table
                    // actually being written to. Tracking deltas here would feed the
                    // sqlite_master record into the DBSP circuit as if it were data
                    // from the named table, corrupting the materialized view.
                    state.active_op_state.insert().old_record = None;
                    break;
                }
            }
            OpInsertSubState::UpdateLastRowid => {
                let has_rowid = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    cursor.has_rowid()
                };
                if has_rowid {
                    let maybe_rowid = {
                        let cursor = state.get_cursor(*cursor_id);
                        let cursor = cursor.as_btree_mut();
                        return_if_io!(cursor.rowid())
                    };
                    if let Some(rowid) = maybe_rowid {
                        if !flag.has(InsertFlags::SKIP_LAST_ROWID) {
                            program.connection.update_last_rowid(rowid);
                        }
                        if !flag.has(InsertFlags::SKIP_ALL_CHANGE_COUNTS) {
                            if flag.has(InsertFlags::SKIP_STATEMENT_CHANGE_COUNT) {
                                state.record_total_change();
                            } else {
                                state.record_statement_change();
                            }
                        }
                    }
                } else if !flag.has(InsertFlags::SKIP_ALL_CHANGE_COUNTS) {
                    if flag.has(InsertFlags::SKIP_STATEMENT_CHANGE_COUNT) {
                        state.record_total_change();
                    } else {
                        state.record_statement_change();
                    }
                }
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                if !dependent_views.is_empty() {
                    if !has_rowid {
                        return Err(LimboError::ParseError(
                            "WITHOUT ROWID tables with dependent materialized views are not supported"
                                .to_string(),
                        ).into());
                    }
                    state.active_op_state.insert().sub_state = OpInsertSubState::ApplyViewChange;
                    continue;
                }
                break;
            }
            OpInsertSubState::ApplyViewChange => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                assert!(!dependent_views.is_empty());

                let (key, values) = {
                    let key = match &state.registers[*key_reg].get_value() {
                        Value::Numeric(Numeric::Integer(i)) => *i,
                        _ => unreachable!("expected integer key"),
                    };

                    let record = match &state.registers[*record_reg] {
                        Register::Record(r) => std::borrow::Cow::Borrowed(r),
                        Register::Value(value) => {
                            let values = [value];
                            let record = ImmutableRecord::from_values(values, values.len())?;
                            std::borrow::Cow::Owned(record)
                        }
                        Register::Aggregate(..) => {
                            unreachable!("Cannot insert an aggregate value.")
                        }
                    };

                    // Add insertion of new row to view deltas
                    let mut new_values = record.get_values_owned()?;

                    // Fix rowid alias columns: replace Null with actual rowid value
                    let schema = program.connection.schema.read();
                    if let Some(table) = schema.get_table(table_name) {
                        for (i, col) in table.columns().iter().enumerate() {
                            if col.is_rowid_alias() && i < new_values.len() {
                                new_values[i] = Value::from_i64(key);
                            }
                        }
                    }

                    (key, new_values)
                };

                if let Some((key, values)) = state.active_op_state.insert().old_record.take() {
                    for view_name in dependent_views.iter() {
                        let tx_state = program
                            .connection
                            .view_transaction_states
                            .get_or_create(view_name);
                        tx_state.delete(table_name, key, values.to_vec());
                    }
                }
                for view_name in dependent_views.iter() {
                    let tx_state = program
                        .connection
                        .view_transaction_states
                        .get_or_create(view_name);

                    tx_state.insert(table_name, key, values.to_vec());
                }

                break;
            }
        }
    }

    state.active_op_state.clear();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_int_64(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Int64 {
            _p1,
            out_reg,
            _p3,
            value,
        },
        insn
    );
    state.registers[*out_reg].set_int(*value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub struct OpDeleteState {
    pub sub_state: OpDeleteSubState,
    pub deleted_record: Option<(i64, crate::alloc::Vec<Value>)>,
}

#[derive(Clone, Copy)]
pub enum OpDeleteSubState {
    /// Capture the record before deletion, if the are dependent views.
    MaybeCaptureRecord,
    /// Delete the record.
    Delete,
    /// Apply the change to the dependent views.
    ApplyViewChange,
}

pub fn op_delete(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Delete {
            cursor_id,
            table_name,
            is_part_of_update,
        },
        insn
    );

    loop {
        match state.active_op_state.delete().sub_state {
            OpDeleteSubState::MaybeCaptureRecord => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                if dependent_views.is_empty() {
                    state.active_op_state.delete().sub_state = OpDeleteSubState::Delete;
                    continue;
                }

                let deleted_record = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    // Get the current key
                    let maybe_key = return_if_io!(cursor.rowid());
                    let key = maybe_key.ok_or_else(|| {
                        LimboError::InternalError("Cannot delete: no current row".to_string())
                    })?;
                    // Get the current record before deletion and extract values
                    let maybe_record = return_if_io!(cursor.record());
                    if let Some(record) = maybe_record {
                        let mut values = record.get_values_owned()?;

                        // Fix rowid alias columns: replace Null with actual rowid value
                        if let Some(table) = schema.get_table(table_name) {
                            for (i, col) in table.columns().iter().enumerate() {
                                if col.is_rowid_alias() && i < values.len() {
                                    values[i] = Value::from_i64(key);
                                }
                            }
                        }
                        Some((key, values))
                    } else {
                        None
                    }
                };
                state.active_op_state.delete().deleted_record = deleted_record;
                state.active_op_state.delete().sub_state = OpDeleteSubState::Delete;
                continue;
            }
            OpDeleteSubState::Delete => {
                {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.delete());
                }
                // Increment metrics for row write (DELETE is a write operation)
                state.record_rows_written(1);
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                if dependent_views.is_empty() {
                    break;
                }
                state.active_op_state.delete().sub_state = OpDeleteSubState::ApplyViewChange;
                continue;
            }
            OpDeleteSubState::ApplyViewChange => {
                let schema = program.connection.schema.read();
                let dependent_views = schema.get_dependent_materialized_views(table_name);
                assert!(!dependent_views.is_empty());
                let maybe_deleted_record = state.active_op_state.delete().deleted_record.take();
                if let Some((key, values)) = maybe_deleted_record {
                    for view_name in dependent_views {
                        let tx_state = program
                            .connection
                            .view_transaction_states
                            .get_or_create(&view_name);
                        tx_state.delete(table_name, key, values.to_vec());
                    }
                }
                break;
            }
        }
    }

    state.active_op_state.clear();
    if !is_part_of_update {
        // DELETEs do not count towards the total changes if they are part of an UPDATE statement,
        // i.e. the DELETE and subsequent INSERT of a row are the same "change".
        state.record_statement_change();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Debug)]
pub enum OpIdxDeleteState {
    Seeking,
    Verifying,
    Deleting,
}
pub fn op_idx_delete(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IdxDelete {
            cursor_id,
            start_reg,
            num_regs,
            raise_error_if_no_matching_entry,
        },
        insn
    );

    if let Some(Cursor::IndexMethod(cursor)) = &mut state.cursors[*cursor_id] {
        return_if_io!(cursor.delete(&state.registers[*start_reg..*start_reg + *num_regs]));
        state.record_rows_written(1);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    loop {
        #[cfg(debug_assertions)]
        tracing::debug!(
            "op_idx_delete(cursor_id={}, start_reg={}, num_regs={}, rootpage={}, state={:?})",
            cursor_id,
            start_reg,
            num_regs,
            state.get_cursor(*cursor_id).as_btree_mut().root_page(),
            state.active_op_state.idx_delete()
        );
        match state.active_op_state.idx_delete() {
            OpIdxDeleteState::Seeking => {
                let found = match seek_internal(
                    program,
                    state,
                    pager,
                    RecordSource::Unpacked {
                        start_reg: *start_reg,
                        num_regs: *num_regs,
                    },
                    *cursor_id,
                    true,
                    SeekOp::GE { eq_only: true },
                ) {
                    Ok(SeekInternalResult::Found) => true,
                    Ok(SeekInternalResult::NotFound) => false,
                    Ok(SeekInternalResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
                    Err(e) => return Err(e.into()),
                };

                if !found {
                    // If we didn't find it because a txn we depended on was aborted, then it means it isn't really corrupt, we simply
                    // might have found some garbage data because other tx trashed all row versions we depended on (basically it sets begin: None, end: None).
                    if program.connection.mvcc_tx_should_abort() {
                        return Err(LimboError::CommitDependencyAborted.into());
                    }
                    // If P5 is not zero, then raise an SQLITE_CORRUPT_INDEX error if no matching index entry is found
                    // Also, do not raise this (self-correcting and non-critical) error if in writable_schema mode.

                    if *raise_error_if_no_matching_entry {
                        let reg_values = (*start_reg..*start_reg + *num_regs)
                            .map(|i| &state.registers[i])
                            .collect::<Vec<_>>();
                        return Err(LimboError::Corrupt(format!(
                            "IdxDelete: no matching index entry found for key {reg_values:?} while seeking"
                        )).into());
                    }
                    state.pc += 1;
                    state.active_op_state.clear();
                    return Ok(InsnFunctionStepResult::Step);
                }
                *state.active_op_state.idx_delete() = OpIdxDeleteState::Verifying;
            }
            OpIdxDeleteState::Verifying => {
                let rowid = {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };

                if rowid.is_none() && *raise_error_if_no_matching_entry {
                    let reg_values = (*start_reg..*start_reg + *num_regs)
                        .map(|i| &state.registers[i])
                        .collect::<Vec<_>>();
                    return Err(LimboError::Corrupt(format!(
                        "IdxDelete: no matching index entry found for key while verifying: {reg_values:?}"
                    )).into());
                }
                *state.active_op_state.idx_delete() = OpIdxDeleteState::Deleting;
            }
            OpIdxDeleteState::Deleting => {
                {
                    let cursor = state.get_cursor(*cursor_id);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.delete());
                }
                // Increment metrics for index write (delete is a write operation)
                state.record_rows_written(1);
                state.pc += 1;
                state.active_op_state.clear();
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpIdxInsertState {
    /// Optional seek step done before an unique constraint check or if the caller indicates a seek is required.
    MaybeSeek,
    /// Optional unique constraint check done before an insert.
    UniqueConstraintCheck,
    /// Main insert step. This is always performed.
    Insert,
}

pub fn op_idx_insert(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IdxInsert {
            cursor_id,
            record_reg,
            flags,
            unpacked_start,
            unpacked_count,
            ..
        },
        *insn
    );

    if let Some(Cursor::IndexMethod(cursor)) = &mut state.cursors[cursor_id] {
        let Some(start) = unpacked_start else {
            return Err(LimboError::InternalError(
                "IndexMethod must receive unpacked values".to_string(),
            )
            .into());
        };
        let Some(count) = unpacked_count else {
            return Err(LimboError::InternalError(
                "IndexMethod must receive unpacked values".to_string(),
            )
            .into());
        };
        return_if_io!(cursor.insert(&state.registers[start..start + count as usize]));
        state.record_rows_written(1);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let record_to_insert = match &state.registers[record_reg] {
        Register::Record(ref r) => r,
        o => {
            return Err(LimboError::InternalError(format!("expected record, got {o:?}")).into());
        }
    };

    match *state.active_op_state.idx_insert() {
        OpIdxInsertState::MaybeSeek => {
            let (_, cursor_type) = program
                .cursor_ref
                .get(cursor_id)
                .expect("cursor_id should exist in cursor_ref");
            let CursorType::BTreeIndex(index_meta) = cursor_type else {
                panic!("IdxInsert: not a BTreeIndex cursor");
            };

            // USE_SEEK: cursor was already positioned by a preceding NoConflict operation.
            // Skip the redundant seek and go directly to insert.
            // For unique indexes, this also skips UniqueConstraintCheck since NoConflict already verified uniqueness.
            //
            // HOWEVER: If the record contains NULLs, NoConflict skips the seek entirely
            // (since NULLs can't conflict), so we must fall back to seeking here.
            if flags.has(IdxInsertFlags::USE_SEEK) && !record_to_insert.contains_null()? {
                *state.active_op_state.idx_insert() = OpIdxInsertState::Insert;
                return Ok(InsnFunctionStepResult::Step);
                // Fall through to do the seek since NoConflict skipped it due to NULLs
            }

            match seek_internal(
                program,
                state,
                pager,
                RecordSource::Packed { record_reg },
                cursor_id,
                true,
                SeekOp::GE { eq_only: true },
            )? {
                SeekInternalResult::Found => {
                    *state.active_op_state.idx_insert() = if index_meta.unique {
                        OpIdxInsertState::UniqueConstraintCheck
                    } else {
                        OpIdxInsertState::Insert
                    };
                    Ok(InsnFunctionStepResult::Step)
                }
                SeekInternalResult::NotFound => {
                    *state.active_op_state.idx_insert() = OpIdxInsertState::Insert;
                    Ok(InsnFunctionStepResult::Step)
                }
                SeekInternalResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
            }
        }
        OpIdxInsertState::UniqueConstraintCheck => {
            let ignore_conflict = 'i: {
                let cursor = get_cursor!(state, cursor_id);
                let cursor = cursor.as_btree_mut();
                let has_rowid = cursor.has_rowid();
                let index_info = cursor.get_index_info().clone();
                let record_opt = return_if_io!(cursor.record());
                let Some(record) = record_opt.as_ref() else {
                    // Cursor not pointing at a record — table is empty or past last
                    break 'i false;
                };
                // Cursor is pointing at a record; if the index has a rowid, exclude it from the comparison since it's a pointer to the table row;
                // UNIQUE indexes disallow duplicates like (a=1,b=2,rowid=1) and (a=1,b=2,rowid=2).
                let existing_key = if has_rowid {
                    let count = record.column_count();
                    &record.get_values_range(0..count.saturating_sub(1))?
                } else {
                    &record.get_values()?[..]
                };
                let inserted_key_vals = &record_to_insert.get_values()?;
                if existing_key.len() != inserted_key_vals.len() {
                    break 'i false;
                }

                let conflict =
                    compare_immutable(existing_key, inserted_key_vals, &index_info.key_info)
                        == std::cmp::Ordering::Equal;
                if conflict {
                    if flags.has(IdxInsertFlags::NO_OP_DUPLICATE) {
                        break 'i true;
                    }
                    return Err(LimboError::Constraint(
                        "UNIQUE constraint failed: duplicate key".into(),
                    )
                    .into());
                }

                false
            };
            if ignore_conflict {
                state.pc += 1;
                state.active_op_state.clear();
                Ok(InsnFunctionStepResult::Step)
            } else {
                *state.active_op_state.idx_insert() = OpIdxInsertState::Insert;
                Ok(InsnFunctionStepResult::Step)
            }
        }
        OpIdxInsertState::Insert => {
            {
                let cursor = get_cursor!(state, cursor_id);
                let cursor = cursor.as_btree_mut();
                return_if_io!(
                    cursor.insert(&BTreeKey::new_index_key(record_to_insert.as_record_ref()))
                );
            }
            if flags.has(IdxInsertFlags::NCHANGE) {
                state.record_rows_written(1);
            }
            state.active_op_state.clear();
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpNewRowidState {
    Start,
    SeekingToLast {
        mvcc_already_initialized: bool,
    },
    ReadingMaxRowid,
    GeneratingRandom {
        attempts: u32,
    },
    VerifyingCandidate {
        attempts: u32,
        candidate: i64,
    },
    /// In case a rowid was generated and not provided by the user, we need to call next() on the cursor
    /// after generating the rowid. This is because the rowid was generated by seeking to the last row in the
    /// table, and we need to insert _after_ that row.
    GoNext,
}

pub fn op_new_rowid(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    new_rowid_inner(program, state, insn, pager).inspect_err(|_| {
        // In case of error we need to unlock rowid lock from mvcc cursor
        load_insn!(
            NewRowid {
                cursor,
                rowid_reg: _,
                prev_largest_reg: _,
            },
            insn
        );
        let mv_store = program.connection.mv_store();
        if mv_store.is_some() {
            let cursor = state.get_cursor(*cursor);
            let cursor = cursor.as_btree_mut() as &mut dyn Any;
            if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                mvcc_cursor.end_new_rowid();
            }
        }
    })
}

fn new_rowid_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        NewRowid {
            cursor,
            rowid_reg,
            prev_largest_reg,
        },
        insn
    );

    const MAX_ROWID: i64 = i64::MAX;
    const MAX_ATTEMPTS: u32 = 100;
    let mv_store = program.connection.mv_store();
    loop {
        match *state.active_op_state.new_rowid() {
            OpNewRowidState::Start => {
                if mv_store.is_some() {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut() as &mut dyn Any;
                    if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                        match return_if_io!(mvcc_cursor.start_new_rowid()) {
                            NextRowidResult::Uninitialized => {
                                *state.active_op_state.new_rowid() =
                                    OpNewRowidState::SeekingToLast {
                                        mvcc_already_initialized: false,
                                    };
                            }
                            NextRowidResult::Next {
                                new_rowid,
                                prev_rowid,
                            } => {
                                // Allocator already initialized — release lock immediately
                                mvcc_cursor.end_new_rowid();
                                state.registers[*rowid_reg].set_int(new_rowid);
                                if *prev_largest_reg > 0 {
                                    state.registers[*prev_largest_reg]
                                        .set_int(prev_rowid.unwrap_or(0));
                                }
                                state.active_op_state.clear();
                                state.pc += 1;
                                return Ok(InsnFunctionStepResult::Step);
                            }
                            NextRowidResult::FindRandom => {
                                mvcc_cursor.end_new_rowid();
                                *state.active_op_state.new_rowid() =
                                    OpNewRowidState::GeneratingRandom { attempts: 0 };
                            }
                        }
                    } else {
                        // Not an MvCursor — must be an ephemeral cursor or an attached
                        // DB cursor without MVCC (e.g., :memory: attached DBs skip MVCC).
                        // Keep the downcast check as a safety net against unexpected cursor types.
                        assert!(
                            cursor.downcast_ref::<BTreeCursor>().is_some(),
                            "Expected MvCursor or BTreeCursor in op_new_rowid"
                        );
                        *state.active_op_state.new_rowid() = OpNewRowidState::SeekingToLast {
                            mvcc_already_initialized: false,
                        };
                    }
                } else {
                    *state.active_op_state.new_rowid() = OpNewRowidState::SeekingToLast {
                        mvcc_already_initialized: false,
                    };
                }
            }

            OpNewRowidState::SeekingToLast {
                mvcc_already_initialized,
            } => {
                {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.seek_to_last());
                }
                if mvcc_already_initialized {
                    *state.active_op_state.new_rowid() = OpNewRowidState::GoNext;
                } else {
                    *state.active_op_state.new_rowid() = OpNewRowidState::ReadingMaxRowid;
                }
            }

            OpNewRowidState::ReadingMaxRowid => {
                let current_max = {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.rowid())
                };

                if mv_store.is_some() {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut() as &mut dyn Any;
                    if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                        // Initialize the monotonic counter from the btree max.
                        // The allocator lock is held, so no other thread can
                        // race between this read and initialize.
                        mvcc_cursor.initialize_max_rowid(current_max)?;
                        // Allocate the first rowid from the freshly initialized counter.
                        match mvcc_cursor.allocate_next_rowid() {
                            Some((new_rowid, prev_rowid)) => {
                                state.registers[*rowid_reg].set_int(new_rowid);
                                if *prev_largest_reg > 0 {
                                    state.registers[*prev_largest_reg]
                                        .set_int(prev_rowid.unwrap_or(0));
                                }
                                tracing::trace!("new_rowid={}", new_rowid);
                                *state.active_op_state.new_rowid() = OpNewRowidState::GoNext;
                                continue;
                            }
                            None => {
                                // At i64::MAX — fall back to random
                                *state.active_op_state.new_rowid() =
                                    OpNewRowidState::GeneratingRandom { attempts: 0 };
                                continue;
                            }
                        }
                    }
                }

                // Non-MVCC path (or ephemeral cursor in MVCC mode)
                if *prev_largest_reg > 0 {
                    state.registers[*prev_largest_reg].set_int(current_max.unwrap_or(0));
                }
                match current_max {
                    Some(rowid) if rowid < MAX_ROWID => {
                        state.registers[*rowid_reg].set_int(rowid + 1);
                        tracing::trace!("new_rowid={}", rowid + 1);
                        *state.active_op_state.new_rowid() = OpNewRowidState::GoNext;
                        continue;
                    }
                    Some(_) => {
                        *state.active_op_state.new_rowid() =
                            OpNewRowidState::GeneratingRandom { attempts: 0 };
                    }
                    None => {
                        tracing::trace!("new_rowid=1");
                        state.registers[*rowid_reg].set_int(1);
                        *state.active_op_state.new_rowid() = OpNewRowidState::GoNext;
                        continue;
                    }
                }
            }

            OpNewRowidState::GeneratingRandom { attempts } => {
                if attempts >= MAX_ATTEMPTS {
                    return Err(LimboError::DatabaseFull("Unable to find an unused rowid after 100 attempts - database is probably full".to_string()).into());
                }

                // Generate a random i64 and constrain it to the lower half of the rowid range.
                // We use the lower half (1 to MAX_ROWID/2) because we're in random mode only
                // when sequential allocation reached MAX_ROWID, meaning the upper range is full.
                let mut random_rowid: i64 = pager.io.generate_random_number();
                random_rowid &= MAX_ROWID >> 1; // Mask to keep value in range [0, MAX_ROWID/2]
                random_rowid += 1; // Ensure positive

                *state.active_op_state.new_rowid() = OpNewRowidState::VerifyingCandidate {
                    attempts,
                    candidate: random_rowid,
                };
            }

            OpNewRowidState::VerifyingCandidate {
                attempts,
                candidate,
            } => {
                let exists = {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    let seek_result =
                        return_if_io!(cursor
                            .seek(SeekKey::TableRowId(candidate), SeekOp::GE { eq_only: true }));
                    matches!(seek_result, SeekResult::Found)
                };

                if !exists {
                    // Found unused rowid!
                    state.registers[*rowid_reg].set_int(candidate);
                    state.active_op_state.clear();
                    state.pc += 1;

                    if mv_store.is_some() {
                        let cursor = state.get_cursor(*cursor);
                        let cursor = cursor.as_btree_mut() as &mut dyn Any;
                        if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                            mvcc_cursor.end_new_rowid();
                        }
                    }

                    return Ok(InsnFunctionStepResult::Step);
                } else {
                    // Collision, try again
                    *state.active_op_state.new_rowid() = OpNewRowidState::GeneratingRandom {
                        attempts: attempts + 1,
                    };
                }
            }
            OpNewRowidState::GoNext => {
                {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut();
                    return_if_io!(cursor.next());
                }
                state.active_op_state.clear();
                state.pc += 1;

                if mv_store.is_some() {
                    let cursor = state.get_cursor(*cursor);
                    let cursor = cursor.as_btree_mut() as &mut dyn Any;
                    if let Some(mvcc_cursor) = cursor.downcast_mut::<MvCursor>() {
                        mvcc_cursor.end_new_rowid();
                    }
                }

                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

fn coerce_register_to_integer(state: &mut ProgramState, reg: usize) -> bool {
    let converted = match state.registers[reg].get_value() {
        Value::Numeric(Numeric::Integer(_)) => return true,
        Value::Numeric(Numeric::Float(f)) => cast_real_to_integer(f64::from(*f)).ok(),
        Value::Text(text) => match checked_cast_text_to_numeric(text.as_str(), true) {
            Ok(Value::Numeric(Numeric::Integer(i))) => Some(i),
            Ok(Value::Numeric(Numeric::Float(f))) => cast_real_to_integer(f64::from(f)).ok(),
            _ => None,
        },
        _ => None,
    };
    let Some(i) = converted else {
        return false;
    };

    state.registers[reg].set_int(i);
    true
}

pub fn op_must_be_int(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(MustBeInt { reg, target_pc }, insn);
    if !coerce_register_to_integer(state, *reg) {
        if let Some(target_pc) = target_pc {
            state.pc = target_pc.as_offset_int();
            return Ok(InsnFunctionStepResult::Step);
        }
        bail_constraint_error!("datatype mismatch");
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_soft_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(SoftNull { reg }, insn);
    state.registers[*reg].set_null();
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Clone, Copy)]
pub enum OpNoConflictState {
    Start,
    Seeking(RecordSource),
}

/// If a matching record is not found in the btree ("no conflict"), jump to the target PC.
/// Otherwise, continue execution.
pub fn op_no_conflict(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        NoConflict {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        },
        insn
    );

    loop {
        match *state.active_op_state.no_conflict() {
            OpNoConflictState::Start => {
                let record_source = if *num_regs == 0 {
                    RecordSource::Packed {
                        record_reg: *record_reg,
                    }
                } else {
                    RecordSource::Unpacked {
                        start_reg: *record_reg,
                        num_regs: *num_regs,
                    }
                };

                // If there is at least one NULL in the index record, there cannot be a conflict so we can immediately jump.
                let contains_nulls = match &record_source {
                    RecordSource::Packed { record_reg } => {
                        let Register::Record(record) = &state.registers[*record_reg] else {
                            return Err(LimboError::InternalError(
                                "NoConflict: expected a record in the register".into(),
                            )
                            .into());
                        };
                        record.iter()?.any(|val| matches!(val, Ok(ValueRef::Null)))
                    }
                    RecordSource::Unpacked {
                        start_reg,
                        num_regs,
                    } => (0..*num_regs).any(|i| {
                        matches!(
                            &state.registers[start_reg + i],
                            Register::Value(Value::Null)
                        )
                    }),
                };

                if contains_nulls {
                    state.pc = target_pc.as_offset_int();
                    state.active_op_state.clear();
                    return Ok(InsnFunctionStepResult::Step);
                } else {
                    *state.active_op_state.no_conflict() =
                        OpNoConflictState::Seeking(record_source);
                }
            }
            OpNoConflictState::Seeking(record_source) => {
                return match seek_internal(
                    program,
                    state,
                    pager,
                    record_source,
                    *cursor_id,
                    true,
                    SeekOp::GE { eq_only: true },
                )? {
                    SeekInternalResult::Found => {
                        state.pc += 1;
                        state.active_op_state.clear();
                        Ok(InsnFunctionStepResult::Step)
                    }
                    SeekInternalResult::NotFound => {
                        state.pc = target_pc.as_offset_int();
                        state.active_op_state.clear();
                        Ok(InsnFunctionStepResult::Step)
                    }
                    SeekInternalResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
                };
            }
        }
    }
}

pub fn op_not_exists(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        NotExists {
            cursor,
            rowid_reg,
            target_pc,
        },
        insn
    );
    let cursor = must_be_btree_cursor!(*cursor, program.cursor_ref, state, "NotExists");
    let cursor = cursor.as_btree_mut();
    let exists = return_if_io!(cursor.exists(state.registers[*rowid_reg].get_value()));

    if exists {
        state.pc += 1;
    } else {
        state.pc = target_pc.as_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_offset_limit(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        OffsetLimit {
            limit_reg,
            combined_reg,
            offset_reg,
        },
        insn
    );
    let limit_val = match state.registers[*limit_reg].get_value() {
        Value::Numeric(Numeric::Integer(val)) => val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in limit_reg is not an integer".into(),
            )
            .into());
        }
    };
    let offset_val = match state.registers[*offset_reg].get_value() {
        Value::Numeric(Numeric::Integer(val)) if *val < 0 => 0,
        Value::Numeric(Numeric::Integer(val)) if *val >= 0 => *val,
        _ => {
            return Err(LimboError::InternalError(
                "OffsetLimit: the value in offset_reg is not an integer".into(),
            )
            .into());
        }
    };

    let offset_limit_sum = limit_val.overflowing_add(offset_val);
    if *limit_val <= 0 || offset_limit_sum.1 {
        state.registers[*combined_reg].set_int(-1);
    } else {
        state.registers[*combined_reg].set_int(offset_limit_sum.0);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}
// this cursor may be reused for next insert
// Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
// If not there might be some optimizations obviously.
pub fn op_open_write(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        OpenWrite {
            cursor_id,
            root_page,
            db,
        },
        insn
    );
    invalidate_deferred_seeks_for_cursor(state, *cursor_id);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly.into());
    }
    let pager = program.get_pager_from_database_index(db)?;
    let mv_store = program.connection.mv_store_for_db(*db);

    if let (_, CursorType::IndexMethod(module)) = &program.cursor_ref[*cursor_id] {
        if mv_store.is_some() {
            crate::index_method::ensure_mvcc_support(&module.definition(), true)?;
        }
        let context =
            ensure_index_method_context(program, state, *cursor_id, *db, module.as_ref())?;
        if state.cursors[*cursor_id].is_none() {
            let cursor = module.init()?;
            let cursor_ref = &mut state.cursors[*cursor_id];
            *cursor_ref = Some(Cursor::IndexMethod(cursor));
        }

        let cursor = state.cursors[*cursor_id]
            .as_mut()
            .expect("cursor should exist");
        let cursor = cursor.as_index_method_mut();
        return_if_io!(cursor.open_write(&context));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let root_page = match root_page {
        RegisterOrLiteral::Literal(lit) => *lit,
        RegisterOrLiteral::Register(reg) => match &state.registers[*reg].get_value() {
            Value::Numeric(Numeric::Integer(val)) => *val,
            _ => {
                return Err(LimboError::InternalError(
                    "OpenWrite: the value in root_page is not an integer".into(),
                )
                .into());
            }
        },
    };

    const SQLITE_SCHEMA_ROOT_PAGE: i64 = 1;

    if root_page == SQLITE_SCHEMA_ROOT_PAGE {
        if let Some(mv_store) = mv_store.as_ref() {
            let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) else {
                return Err(LimboError::InternalError(
                    "Schema changes in MVCC mode require an exclusive MVCC transaction".to_string(),
                )
                .into());
            };
            if !mv_store.is_exclusive_tx(&tx_id) {
                return Err(LimboError::TxError(
                    "DDL statements require an exclusive transaction (use BEGIN instead of BEGIN CONCURRENT)".to_string(),
                ).into());
            }
        }
    }

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let cursors = &mut state.cursors;
    let maybe_index = match cursor_type {
        CursorType::BTreeIndex(index) => Some(index),
        _ => None,
    };

    // Check if we can reuse the existing cursor
    let can_reuse_cursor = if let Some(Some(Cursor::BTree(btree_cursor))) = cursors.get(*cursor_id)
    {
        // Reuse if the root_page matches (same table/index)
        btree_cursor.root_page() == root_page
    } else {
        false
    };

    if !can_reuse_cursor {
        let maybe_promote_to_mvcc_cursor = |btree_cursor: Box<dyn CursorTrait>,
                                            mv_cursor_type: MvccCursorType|
         -> Result<Box<dyn CursorTrait>> {
            if let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) {
                let mv_store = mv_store
                    .as_ref()
                    .expect("mv_store should be Some when MVCC transaction is active")
                    .clone();
                Ok(Box::new(MvCursor::new(
                    mv_store,
                    &program.connection,
                    tx_id,
                    root_page,
                    mv_cursor_type,
                    btree_cursor,
                )?))
            } else if mv_store.is_some() {
                Err(LimboError::InternalError(
                    "OpenWrite requires an active MVCC transaction".to_string(),
                ))
            } else {
                Ok(btree_cursor)
            }
        };
        if let Some(index) = maybe_index {
            let num_columns = index.columns.len();
            let btree_cursor = btree_cursor_with_yield_context(
                Box::new(BTreeCursor::new_index(
                    pager,
                    maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                    index.as_ref(),
                    num_columns,
                )?),
                &program.connection,
            );
            let index_info = Arc::new(if let Some(mv_store) = mv_store.as_ref() {
                IndexInfo::new_from_index_in(index, mv_store.allocator())?
            } else {
                IndexInfo::new_from_index(index)?
            });
            let cursor =
                maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Index(index_info))?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        } else {
            if matches!(cursor_type, CursorType::BTreeTable(table_rc) if !table_rc.has_rowid)
                && program.connection.get_mv_tx_id_for_db(*db).is_some()
            {
                return Err(LimboError::ParseError(
                    "WITHOUT ROWID tables are not supported in MVCC mode".to_string(),
                )
                .into());
            }
            let num_columns = match cursor_type {
                CursorType::BTreeTable(table_rc) => table_rc.columns().len(),
                CursorType::MaterializedView(table_rc, _) => table_rc.columns().len(),
                _ => unreachable!(
                    "Expected BTreeTable or MaterializedView. This should not have happened."
                ),
            };

            let btree_cursor: Box<dyn CursorTrait> = match cursor_type {
                CursorType::BTreeTable(table_rc) if !table_rc.has_rowid => {
                    btree_cursor_with_yield_context(
                        Box::new(BTreeCursor::new_without_rowid_table(
                            pager,
                            maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                            table_rc.as_ref(),
                            num_columns,
                        )),
                        &program.connection,
                    )
                }
                _ => btree_cursor_with_yield_context(
                    Box::new(BTreeCursor::new_table(
                        pager,
                        maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                        num_columns,
                    )),
                    &program.connection,
                ),
            };
            let cursor = maybe_promote_to_mvcc_cursor(btree_cursor, MvccCursorType::Table)?;
            cursors
                .get_mut(*cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_copy(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Copy {
            src_reg,
            dst_reg,
            extra_amount,
        },
        insn
    );
    for i in 0..=*extra_amount {
        let (src, dst) = (*src_reg + i, *dst_reg + i);
        if src == dst {
            continue;
        }
        // try_clone_from reuses the destination register's allocation.
        let [src, dst] = state
            .registers
            .get_disjoint_mut([src, dst])
            .expect("Copy source and destination registers are distinct");
        dst.try_clone_from(src)?;
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Reopening a cursor slot invalidates any deferred seek that points to it or
/// uses it as the driving index cursor. For example, UPDATE ... FROM may use a
/// target-table cursor in the collection phase behind a DeferredSeek, then
/// reopen that same slot for the write phase. If the stale deferred seek
/// survives, the first Column/RowId read in the write loop can jump back to the
/// collection-phase index cursor and read the wrong row.
fn invalidate_deferred_seeks_for_cursor(state: &mut ProgramState, cursor_id: usize) {
    for deferred_seek in &mut state.deferred_seeks {
        if let Some(ds) = deferred_seek {
            if ds.index_cursor_id == cursor_id || ds.table_cursor_id == cursor_id {
                *deferred_seek = None;
            }
        }
    }
}

pub fn op_create_btree(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(CreateBtree { db, root, flags }, insn);

    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly.into());
    }
    let mv_store = program.connection.mv_store_for_db(*db);

    if let Some(mv_store) = mv_store.as_ref() {
        let root_page = mv_store.get_next_table_id();
        state.registers[*root].set_int(root_page);
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    let pager = program.get_pager_from_database_index(db)?;
    // FIXME: handle page cache is full
    let root_page = return_if_io!(pager.btree_create(flags));
    state.registers[*root].set_int(root_page as i64);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_create(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IndexMethodCreate { db, cursor_id }, insn);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly.into());
    }
    let (_, CursorType::IndexMethod(module)) = &program.cursor_ref[*cursor_id] else {
        unreachable!("IndexMethodCreate emitted against a non-index-method cursor {cursor_id}");
    };
    if program.connection.mv_store_for_db(*db).is_some() {
        crate::index_method::ensure_mvcc_support(&module.definition(), true)?;
    }
    // Build the context before installing the cursor: a context error must
    // not leave a cursor in the slot with no context, a state the statement
    // outcome helpers cannot deliver hooks to.
    let context = ensure_index_method_context(program, state, *cursor_id, *db, module.as_ref())?;
    if state.cursors[*cursor_id].is_none() {
        let cursor = module.init()?;
        state.cursors[*cursor_id] = Some(Cursor::IndexMethod(cursor));
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist")
        .as_index_method_mut();
    return_if_io!(cursor.create(&context));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_destroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IndexMethodDestroy { db, cursor_id }, insn);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly.into());
    }
    let Some((_, CursorType::IndexMethod(module))) = program.cursor_ref.get(*cursor_id) else {
        unreachable!("IndexMethodDestroy emitted against a non-index-method cursor {cursor_id}");
    };
    if program.connection.mv_store_for_db(*db).is_some() {
        crate::index_method::ensure_mvcc_support(&module.definition(), true)?;
    }
    // Context before cursor install; see op_index_method_create.
    let context = ensure_index_method_context(program, state, *cursor_id, *db, module.as_ref())?;
    if state.cursors[*cursor_id].is_none() {
        let cursor = module.init()?;
        state.cursors[*cursor_id] = Some(Cursor::IndexMethod(cursor));
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist")
        .as_index_method_mut();
    return_if_io!(cursor.destroy(&context));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_optimize(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IndexMethodOptimize { db, cursor_id }, insn);
    if program.connection.is_readonly(*db) {
        return Err(LimboError::ReadOnly.into());
    }
    let Some((_, CursorType::IndexMethod(module))) = program.cursor_ref.get(*cursor_id) else {
        unreachable!("IndexMethodOptimize emitted against a non-index-method cursor {cursor_id}");
    };
    if program.connection.mv_store_for_db(*db).is_some() {
        crate::index_method::ensure_mvcc_support(&module.definition(), true)?;
    }
    // Context before cursor install; see op_index_method_create.
    let context = ensure_index_method_context(program, state, *cursor_id, *db, module.as_ref())?;
    if state.cursors[*cursor_id].is_none() {
        let cursor = module.init()?;
        state.cursors[*cursor_id] = Some(Cursor::IndexMethod(cursor));
    }
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist")
        .as_index_method_mut();
    return_if_io!(cursor.optimize(&context));

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_index_method_query(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IndexMethodQuery {
            db: _,
            cursor_id,
            start_reg,
            count_reg,
            pc_if_empty,
        },
        insn
    );
    let cursor = state.cursors[*cursor_id]
        .as_mut()
        .expect("cursor should exist");
    let cursor = cursor.as_index_method_mut();
    let has_rows =
        return_if_io!(cursor.query_start(&state.registers[*start_reg..*start_reg + *count_reg]));
    if !has_rows {
        state.pc = pc_if_empty.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub enum OpDestroyState {
    CreateCursor,
    DestroyBtree(Arc<RwLock<BTreeCursor>>),
}

/// State carried across asynchronous steps while clearing an existing b-tree.
pub enum OpClearBtreeState {
    CreateCursor,
    ClearBtree {
        pager: Arc<Pager>,
        cursor: Arc<RwLock<BTreeCursor>>,
    },
}

pub fn op_destroy(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Destroy {
            db,
            root,
            former_root_reg,
            is_temp: _,
        },
        insn
    );
    let mv_store = program.connection.mv_store_for_db(*db);
    if mv_store.is_some() {
        // MVCC only does pager operations in checkpoint
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let destroy_pager = if *db != MAIN_DB_ID {
        program.get_pager_from_database_index(db)?
    } else {
        pager.clone()
    };

    loop {
        match state.active_op_state.destroy() {
            OpDestroyState::CreateCursor => {
                // Destroy doesn't do anything meaningful with the table/index distinction so we can just use a
                // table btree cursor for both.
                let cursor = BTreeCursor::new(destroy_pager.clone(), *root, 0);
                *state.active_op_state.destroy() =
                    OpDestroyState::DestroyBtree(Arc::new(RwLock::new(cursor)));
            }
            OpDestroyState::DestroyBtree(ref mut cursor) => {
                let maybe_former_root_page = return_if_io!(cursor.write().btree_destroy());
                state.registers[*former_root_reg]
                    .set_int(maybe_former_root_page.unwrap_or(0) as i64);
                state.active_op_state.clear();
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

/// Executes `ClearBtree` by deleting all cells from a persistent b-tree root.
///
/// REINDEX uses this after sorting all replacement index records and before
/// refilling the existing root page. The operation invalidates other b-tree
/// cursors on the same pager because page contents and cursor positions may no
/// longer be valid after the clear.
pub fn op_clear_btree(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    match op_clear_btree_inner(program, state, insn, pager) {
        Ok(result) => Ok(result),
        Err(err) => {
            if !matches!(*err, LimboError::Busy | LimboError::BusySnapshot) {
                state.active_op_state.clear();
            }
            Err(err)
        }
    }
}

fn op_clear_btree_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ClearBtree { db, root }, insn);

    let mv_store = program.connection.mv_store_for_db(*db);
    if mv_store.is_some() {
        return Err(LimboError::InternalError(
            "ClearBtree is not supported in MVCC mode".to_string(),
        )
        .into());
    }

    let clear_pager = if *db != MAIN_DB_ID {
        program.get_pager_from_database_index(db)?
    } else {
        pager.clone()
    };

    loop {
        match state.active_op_state.clear_btree() {
            OpClearBtreeState::CreateCursor => {
                let cursor = BTreeCursor::new(clear_pager.clone(), *root, 0);
                *state.active_op_state.clear_btree() = OpClearBtreeState::ClearBtree {
                    pager: clear_pager.clone(),
                    cursor: Arc::new(RwLock::new(cursor)),
                };
            }
            OpClearBtreeState::ClearBtree { pager, cursor } => {
                return_if_io!(cursor.write().clear_btree());
                for other_cursor_opt in state.cursors.iter_mut().flatten() {
                    if let Cursor::BTree(ref mut btree_cursor) = other_cursor_opt {
                        if Arc::ptr_eq(&btree_cursor.get_pager(), pager) {
                            btree_cursor.invalidate_btree_cache();
                        }
                    }
                }
                state.active_op_state.clear();
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_reset_sorter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ResetSorter { cursor_id }, insn);

    let (_, cursor_type) = program
        .cursor_ref
        .get(*cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    let cursor = state.get_cursor(*cursor_id);

    match cursor_type {
        CursorType::BTreeTable(_) | CursorType::BTreeIndex(_) => {
            let cursor = cursor.as_btree_mut();
            return_if_io!(cursor.clear_btree());
        }
        CursorType::Sorter => {
            return Err(LimboError::InternalError(
                "ResetSorter is not supported for sorter cursors".to_string(),
            )
            .into());
        }
        _ => {
            return Err(LimboError::InternalError(format!(
                "ResetSorter is not supported for {cursor_type:?}"
            ))
            .into());
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DropTable { db, table_name, .. }, insn);
    let conn = program.connection.clone();
    let is_mvcc = conn.mv_store_for_db(*db).is_some();
    {
        conn.with_database_schema_mut(*db, |schema| {
            // In MVCC mode, track dropped root pages so integrity_check knows about them.
            // The btree pages won't be freed until checkpoint, so integrity_check needs
            // to include them to avoid "page never used" false positives.
            if is_mvcc {
                let table = schema
                    .get_table(table_name)
                    .expect("DROP TABLE: table must exist in schema");
                if let Some(btree) = table.btree() {
                    // Only track positive root pages (checkpointed tables).
                    // Negative root pages are non-checkpointed and don't exist in btree file.
                    if btree.root_page > 0 {
                        schema.dropped_root_pages.insert(btree.root_page);
                    }
                }
                // Capture index root pages (table may not have indexes)
                if let Some(indexes) = schema.indexes.get(table_name) {
                    for index in indexes.iter() {
                        if index.root_page > 0 {
                            schema.dropped_root_pages.insert(index.root_page);
                        }
                    }
                }
            }
            schema.remove_indices_for_table(table_name);
            schema.remove_triggers_for_table(table_name);
            schema.remove_table(table_name);
        })?;
        // SQLite also removes temp triggers that target the dropped table.
        // Only needed when dropping from a non-temp database. We must
        // scope the removal to triggers whose `target_database_id`
        // matches the dropped db — otherwise `DROP TABLE main.t` would
        // also nuke temp triggers that target `temp.t` or `aux.t`.
        if *db != crate::TEMP_DB_ID && conn.temp.database.read().is_some() {
            let dropped_db = *db;
            conn.with_database_schema_mut(crate::TEMP_DB_ID, |temp_schema| {
                temp_schema.remove_triggers_for_table_with_db(table_name, dropped_db);
            })?;
        }
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_view(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DropView { db, view_name }, insn);
    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema.remove_view(view_name).ok();
        schema.broken_views.remove(view_name);
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_type(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DropType { db, type_name }, insn);
    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema.remove_type(type_name);
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_sequence(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(AddSequence { data }, insn);
    let AddSequenceData {
        db,
        name,
        start,
        increment,
        min_value,
        max_value,
        cycle,
    } = data.as_ref();
    let seq = crate::schema::Sequence::new(
        name.clone(),
        Some(*start),
        Some(*increment),
        Some(*min_value),
        Some(*max_value),
        *cycle,
    )?;
    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema
            .sequences
            .insert(crate::util::normalize_ident(name), std::sync::Arc::new(seq));
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_sequence(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DropSequence { db, seq_name }, insn);
    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema.remove_sequence(seq_name);
    })?;
    // Drop this connection's stale currval. Otherwise a same-session
    // DROP SEQUENCE + CREATE SEQUENCE <same name> would let currval()
    // return the prior sequence's last value instead of erroring with
    // "not yet defined in this session" — covered by the regression
    // test `currval-after-drop-then-recreate-uses-new-sequence-currval`
    // in `sequence_adversarial.sqltest`.
    conn.clear_sequence_currval(seq_name);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_type(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(AddType { db, sql }, insn);
    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| schema.add_type_from_sql(sql))??;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Compute the next value of a sequence from a watermark row that has
/// already been loaded into registers by the surrounding bytecode. Pure
/// arithmetic — no I/O. The translator emits a cursor seek + Column reads
/// to populate `in_value_reg` and `in_is_called_reg`; this opcode applies
/// the start/increment/cycle/exhaustion logic to produce the next value.
///
/// When the backing table is empty (`was_empty_reg` holds 1), the next
/// value is `start_value`. When the row exists and `is_called` is false,
/// the next value is the existing value (the start has not yet been
/// emitted). Otherwise the next value is `value + increment`, wrapping
/// to the opposite bound when `cycle` is set, or returning
/// `LimboError::DatabaseFull` on exhaustion.
pub fn op_sequence_compute_next(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SequenceComputeNext {
            db,
            seq_name_reg,
            in_value_reg,
            in_is_called_reg,
            was_empty_reg,
            out_value_reg,
        },
        insn
    );

    let seq_name = match state.registers[*seq_name_reg].get_value() {
        Value::Text(t) => t.as_str().to_string(),
        _ => {
            return Err(crate::LimboError::ParseError(
                "SequenceComputeNext: seq_name_reg must be text".to_string(),
            )
            .into());
        }
    };

    // Strip the schema-qualifier from the user's spelling when the
    // register contains a qualified name like "aux.my_seq" — the
    // schema's sequences map is keyed by the bare name. The opcode's
    // `db` field already encodes the target database.
    let bare_seq_name = seq_name
        .rsplit_once('.')
        .map(|(_, n)| n)
        .unwrap_or(&seq_name);
    let seq = program
        .connection
        .with_schema(*db, |s| s.get_sequence(bare_seq_name).cloned())
        .ok_or_else(|| {
            crate::LimboError::ParseError(format!("sequence \"{seq_name}\" does not exist"))
        })?;

    let was_empty = state.registers[*was_empty_reg]
        .get_value()
        .as_int()
        .unwrap_or(0)
        != 0;

    let next = if was_empty {
        seq.start_value
    } else {
        let current = state.registers[*in_value_reg]
            .get_value()
            .as_int()
            .ok_or_else(|| {
                crate::LimboError::InternalError(
                    "SequenceComputeNext: in_value_reg must be integer".to_string(),
                )
            })?;
        let is_called = state.registers[*in_is_called_reg]
            .get_value()
            .as_int()
            .unwrap_or(0)
            != 0;
        if !is_called {
            current
        } else {
            let exhausted_max = current.checked_add(seq.increment_by).is_none()
                || (seq.increment_by > 0
                    && current.saturating_add(seq.increment_by) > seq.max_value)
                || (seq.increment_by < 0
                    && current.saturating_add(seq.increment_by) < seq.min_value);

            if exhausted_max {
                if seq.cycle {
                    if seq.increment_by > 0 {
                        seq.min_value
                    } else {
                        seq.max_value
                    }
                } else if seq
                    .name
                    .starts_with(crate::schema::AUTOINCREMENT_SEQ_PREFIX)
                {
                    // AUTOINCREMENT exhaustion uses SQLite's canonical
                    // SQLITE_FULL "database or disk is full" message so
                    // callers and tests don't have to know whether the
                    // rowid came from a SERIAL-style implicit sequence
                    // or any other autoinc path.
                    return Err(crate::LimboError::DatabaseFull(
                        "database or disk is full".to_string(),
                    )
                    .into());
                } else {
                    return Err(crate::LimboError::DatabaseFull(format!(
                        "nextval: reached {} value of sequence \"{}\"",
                        if seq.increment_by > 0 {
                            "maximum"
                        } else {
                            "minimum"
                        },
                        seq.name
                    ))
                    .into());
                }
            } else {
                current + seq.increment_by
            }
        }
    };

    state.registers[*out_value_reg].set_value(Value::from_i64(next));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Record the value produced by a nextval/setval call as this connection's
/// currval for the named sequence. Pure synchronous register operation —
/// backing-table compaction and `sqlite_sequence` synchronization for
/// AUTOINCREMENT sequences are emitted as inline bytecode by the translator
/// (see `core/translate/sequence.rs::emit_backing_table_compaction` and
/// `emit_autoincrement_sqlite_sequence_sync`).
pub fn op_set_sequence_currval(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SetSequenceCurrval {
            seq_name_reg,
            value_reg,
        },
        insn
    );

    let seq_name = match state.registers[*seq_name_reg].get_value() {
        Value::Text(t) => t.as_str().to_string(),
        _ => {
            return Err(crate::LimboError::ParseError(
                "SetSequenceCurrval: seq_name_reg must be text".to_string(),
            )
            .into());
        }
    };
    let value = state.registers[*value_reg]
        .get_value()
        .as_int()
        .ok_or_else(|| {
            crate::LimboError::InternalError(
                "SetSequenceCurrval: value_reg must be integer".to_string(),
            )
        })?;

    program.connection.set_sequence_currval(&seq_name, value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Publish sequence allocation metadata used by `sequence_watermark_experimental()`.
///
/// The translator emits this only after the sequence backing-table RMW has
/// committed successfully. At that point `SequenceCommitInnerTx` has restored
/// the connection's MVCC transaction to the outer tx, so any active allocation
/// is attributed to the transaction whose row changes may become visible later.
pub fn op_sequence_track_allocation(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SequenceTrackAllocation {
            db,
            seq_name_reg,
            value_reg,
        },
        insn
    );

    let seq_name = match state.registers[*seq_name_reg].get_value() {
        Value::Text(t) => t.as_str().to_string(),
        _ => {
            return Err(crate::LimboError::ParseError(
                "SequenceTrackAllocation: seq_name_reg must be text".to_string(),
            )
            .into());
        }
    };
    let value = state.registers[*value_reg]
        .get_value()
        .as_int()
        .ok_or_else(|| {
            crate::LimboError::InternalError(
                "SequenceTrackAllocation: value_reg must be integer".to_string(),
            )
        })?;

    let bare_seq_name = seq_name
        .rsplit_once('.')
        .map(|(_, n)| n)
        .unwrap_or(&seq_name);
    let normalized_seq_name = crate::util::normalize_ident(bare_seq_name);

    if let Some(mv_store) = program.connection.mv_store_for_db(*db) {
        let seq = program
            .connection
            .with_schema(*db, |s| s.get_sequence(&normalized_seq_name).cloned())
            .ok_or_else(|| {
                crate::LimboError::ParseError(format!("sequence \"{seq_name}\" does not exist"))
            })?;
        let current_watermark =
            crate::mvcc::database::first_unsafe_sequence_watermark(&seq, value, true);
        mv_store.set_sequence_watermark(&normalized_seq_name, current_watermark);

        if let Some((tx_id, _)) = program.connection.get_mv_tx_for_db(*db) {
            if mv_store.is_tx_rollbackable(tx_id) {
                mv_store.register_sequence_allocation(tx_id, &normalized_seq_name, value)?;
            }
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Register an in-flight sequence allocation against the *outer* transaction
/// *before* the paired `SequenceCommitInnerTx` publishes the new boundary.
///
/// Without this, there is a window where one connection has committed the
/// inner-tx RMW (so its allocated value is now readable by other connections
/// and the next allocator can advance the stored watermark past it) but has not
/// yet run `SequenceTrackAllocation` to register its allocation. A reader that
/// computes `sequence_watermark_experimental()` in that window sees the
/// advanced boundary without the lower active allocation, claims the value
/// safe, and a forward-only cursor advances past a row the outer tx still holds
/// uncommitted — skipping it once the outer tx commits.
///
/// Registering here, ahead of the commit that makes the value observable,
/// guarantees the active allocation is in place before any other connection can
/// advance the watermark past it. The post-commit `SequenceTrackAllocation`
/// re-registers the same `(tx, value)` (an idempotent `min`) and additionally
/// covers the skipped-inner-tx (exclusive outer / WAL) and autocommit paths,
/// where the outer tx is not encoded in `saved_outer_reg`.
pub fn op_sequence_register_allocation(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SequenceRegisterAllocation {
            db,
            seq_name_reg,
            value_reg,
            saved_outer_reg,
        },
        insn
    );

    // Only the wrapped inner-tx path encodes a saved outer mv_tx. An empty
    // blob means there is no outer tx whose uncommitted row needs protecting
    // (autocommit) or the inner tx was skipped (exclusive / WAL) — both are
    // handled by the post-commit `SequenceTrackAllocation`.
    let saved_outer = match state.registers[*saved_outer_reg].get_value() {
        Value::Blob(bytes) => decode_saved_outer_mv_tx(bytes.as_slice()),
        _ => None,
    };
    let Some((outer_tx_id, _)) = saved_outer else {
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    };

    let seq_name = match state.registers[*seq_name_reg].get_value() {
        Value::Text(t) => t.as_str().to_string(),
        _ => {
            return Err(crate::LimboError::ParseError(
                "SequenceRegisterAllocation: seq_name_reg must be text".to_string(),
            )
            .into());
        }
    };
    let value = state.registers[*value_reg]
        .get_value()
        .as_int()
        .ok_or_else(|| {
            crate::LimboError::InternalError(
                "SequenceRegisterAllocation: value_reg must be integer".to_string(),
            )
        })?;

    let bare_seq_name = seq_name
        .rsplit_once('.')
        .map(|(_, n)| n)
        .unwrap_or(&seq_name);
    let normalized_seq_name = crate::util::normalize_ident(bare_seq_name);

    if let Some(mv_store) = program.connection.mv_store_for_db(*db) {
        if mv_store.is_tx_rollbackable(outer_tx_id) {
            mv_store.register_sequence_allocation(outer_tx_id, &normalized_seq_name, value)?;
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

// Path-kind sentinel values for the SequenceBeginInnerTx /
// SequenceCommitInnerTx pair. The translator emits a literal compare
// against `PATH_KIND_CONFLICT_RETRY` to decide whether to jump back to
// the retry-top label.
const SEQ_PATH_SKIPPED: i64 = 0;
const SEQ_PATH_WRAPPED: i64 = 1;
const SEQ_COMMIT_STATUS_OK: i64 = 0;
const SEQ_COMMIT_STATUS_CONFLICT_RETRY: i64 = 1;

/// Saved outer mv_tx state, encoded in a register payload via a Blob.
/// Format: 9 bytes — 8-byte little-endian u64 tx_id followed by 1-byte
/// mode tag (0=Read, 1=Write/exclusive, 2=Concurrent). Empty Blob means
/// "no prior mv_tx for this db" (must be restored to `None`).
fn encode_saved_outer_mv_tx(
    outer: Option<(TxID, crate::translate::emitter::TransactionMode)>,
) -> crate::ValueBlob {
    use crate::translate::emitter::TransactionMode;
    let Some((tx_id, mode)) = outer else {
        return crate::alloc::vec![];
    };
    let mut buf = <crate::ValueBlob as TursoVecExt<u8>>::with_capacity(9);
    buf.extend_from_slice(&tx_id.to_le_bytes());
    let mode_tag: u8 = match mode {
        TransactionMode::None => 0,
        TransactionMode::Read => 0,
        TransactionMode::Write => 1,
        TransactionMode::Concurrent => 2,
    };
    buf.push(mode_tag);
    buf
}

fn decode_saved_outer_mv_tx(
    bytes: &[u8],
) -> Option<(TxID, crate::translate::emitter::TransactionMode)> {
    use crate::translate::emitter::TransactionMode;
    if bytes.is_empty() {
        return None;
    }
    let tx_id =
        u64::from_le_bytes(bytes[0..8].try_into().expect("blob has at least 9 bytes")) as TxID;
    let mode = match bytes[8] {
        0 => TransactionMode::Read,
        1 => TransactionMode::Write,
        2 => TransactionMode::Concurrent,
        _ => panic!("invalid mode tag in saved outer mv_tx"),
    };
    Some((tx_id, mode))
}

/// Maximum number of consecutive `WriteWriteConflict` / `BusySnapshot`
/// retries `op_sequence_commit_inner_tx` will absorb before bailing out
/// with `LimboError::Busy`. Tracks per-`ProgramState` via
/// `sequence_inner_retry_count`; the count resets on the first
/// successful commit. Higher than typical contention windows but
/// bounded so a structurally-degenerate conflict pattern surfaces as
/// Busy rather than spinning forever.
const SEQUENCE_INNER_TX_RETRY_BUDGET: u32 = 100;

/// Begin the autonomous inner tx for a sequence RMW. Single-step. See
/// `Insn::SequenceBeginInnerTx` doc for full semantics.
///
/// Skipped path: when MVCC is not in use for this db, OR the connection
/// already holds an exclusive outer tx. Reason: an exclusive outer
/// holds `pager_commit_lock` for its entire lifetime; an inner
/// Concurrent tx on the SAME connection would yield forever waiting
/// for the lock (which only releases at outer commit/rollback), and
/// since the outer is suspended inside this opcode, the lock can
/// never release. Same-thread deadlock. We avoid the inner-tx wrap
/// entirely in that case — the cursor RMW below runs in the outer tx,
/// and the matching `SequenceCommitInnerTx` is a no-op. Per the
/// contract, exclusive blocks all other writers (in-process and
/// cross-process), so any value emitted-then-rolled-back was never
/// observed by a live reader; re-emission after rollback is acceptable.
pub fn op_sequence_begin_inner_tx(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SequenceBeginInnerTx {
            db,
            path_kind_reg,
            saved_outer_reg,
        },
        insn
    );

    let conn = program.connection.clone();
    let Some(mv_store) = conn.mv_store_for_db(*db) else {
        // WAL mode: no inner tx needed. The WAL single-writer lock
        // already serializes writes across processes.
        state.registers[*path_kind_reg].set_value(Value::from_i64(SEQ_PATH_SKIPPED));
        state.registers[*saved_outer_reg].set_value(Value::Blob(crate::alloc::vec![]));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    };

    let outer_tx = conn.get_mv_tx_for_db(*db);
    if let Some((outer_id, _)) = outer_tx {
        if mv_store.is_exclusive_tx(&outer_id) {
            state.registers[*path_kind_reg].set_value(Value::from_i64(SEQ_PATH_SKIPPED));
            state.registers[*saved_outer_reg].set_value(Value::Blob(crate::alloc::vec![]));
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }
    }

    let inner_tx_id = mv_store.begin_tx(pager.clone())?;
    conn.set_mv_tx_for_db(
        *db,
        Some((
            inner_tx_id,
            crate::translate::emitter::TransactionMode::Concurrent,
        )),
    );

    let saved = encode_saved_outer_mv_tx(outer_tx);
    state.registers[*path_kind_reg].set_value(Value::from_i64(SEQ_PATH_WRAPPED));
    state.registers[*saved_outer_reg].set_value(Value::Blob(saved));
    // Record the in-flight inner-tx state so a statement reset / abort
    // before the matching SequenceCommitInnerTx can roll it back. See
    // `Statement::cleanup_orphaned_seq_inner_tx`.
    state.sequence_inner_tx_pending = Some(crate::vdbe::SequenceInnerTxState {
        db: *db,
        inner_tx_id,
        saved_outer: outer_tx,
    });
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Commit the autonomous inner tx started by a paired
/// `SequenceBeginInnerTx`. Multi-step. Drives the inner's
/// `CommitStateMachine` one step per opcode call, yielding
/// `InsnFunctionStepResult::IO(io)` to the VDBE driver when commit
/// state machine wants IO. Mirrors `op_auto_commit`'s drive of the
/// outer commit's state machine.
///
/// On Done: restores the connection's mv_tx for `db` to the saved
/// outer, sets `status_reg = SEQ_COMMIT_STATUS_OK`, advances pc.
///
/// On WriteWriteConflict / BusySnapshot / Conflict: rolls back the
/// inner tx, restores outer mv_tx, sets `status_reg = SEQ_COMMIT_
/// STATUS_CONFLICT_RETRY`, advances pc. The translator emits a
/// conditional jump back to the retry-top label so the RMW retries
/// under a fresh inner tx.
pub fn op_sequence_commit_inner_tx(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SequenceCommitInnerTx {
            db,
            path_kind_reg,
            saved_outer_reg,
            status_reg,
        },
        insn
    );

    let path_kind = state.registers[*path_kind_reg]
        .get_value()
        .as_int()
        .ok_or_else(|| {
            LimboError::InternalError(
                "SequenceCommitInnerTx: path_kind_reg must be integer".to_string(),
            )
        })?;
    if path_kind == SEQ_PATH_SKIPPED {
        // No inner tx to commit. The cursor RMW ran inline in the
        // outer tx.
        state.registers[*status_reg].set_value(Value::from_i64(SEQ_COMMIT_STATUS_OK));
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }

    let conn = program.connection.clone();
    let mv_store = conn.mv_store_for_db(*db).ok_or_else(|| {
        LimboError::InternalError(
            "SequenceCommitInnerTx: no MV store for db but path was Wrapped".to_string(),
        )
    })?;

    let saved_outer = match state.registers[*saved_outer_reg].get_value() {
        Value::Blob(bytes) => decode_saved_outer_mv_tx(bytes.as_slice()),
        _ => {
            return Err(LimboError::InternalError(
                "SequenceCommitInnerTx: saved_outer_reg must be blob".to_string(),
            )
            .into());
        }
    };

    // First entry for this opcode invocation: build the
    // CommitStateMachine. On subsequent re-entries (after a yielded
    // IO completes), we pick up the already-constructed state machine
    // from ProgramState.
    if state.sequence_inner_commit.is_none() {
        let inner_tx_id = match conn.get_mv_tx_for_db(*db) {
            Some((tx_id, _)) => tx_id,
            None => {
                return Err(LimboError::InternalError(
                    "SequenceCommitInnerTx: connection has no mv_tx but path was Wrapped"
                        .to_string(),
                )
                .into());
            }
        };
        state.sequence_inner_commit = Some(mv_store.commit_tx(inner_tx_id, &conn, *db)?);
    }

    let commit_sm = state.sequence_inner_commit.as_mut().expect("just set");

    match commit_sm.step(&mv_store) {
        Ok(IOResult::IO(io)) => Ok(InsnFunctionStepResult::IO(io)),
        Ok(IOResult::Done(())) => {
            state.sequence_inner_commit = None;
            state.sequence_inner_tx_pending = None;
            state.sequence_inner_retry_count = 0;
            conn.set_mv_tx_for_db(*db, saved_outer);
            state.registers[*status_reg].set_value(Value::from_i64(SEQ_COMMIT_STATUS_OK));
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Err(err)
            if matches!(
                *err,
                LimboError::WriteWriteConflict | LimboError::BusySnapshot | LimboError::Conflict(_)
            ) =>
        {
            state.sequence_inner_commit = None;
            // Inner tx may already be in a state where rollback is a
            // no-op (e.g. self-aborted); `is_tx_rollbackable` guards.
            if let Some((inner_tx_id, _)) = conn.get_mv_tx_for_db(*db) {
                if mv_store.is_tx_rollbackable(inner_tx_id) {
                    mv_store.rollback_tx(inner_tx_id, pager.clone(), &conn, *db);
                }
            }
            state.sequence_inner_tx_pending = None;
            conn.set_mv_tx_for_db(*db, saved_outer);
            // Bail out with Busy when the retry budget is exhausted.
            // Routing this through `Insn::Halt { err_code: SQLITE_BUSY }`
            // would land in `op_halt`'s constraint_error catch-all and be
            // mis-wrapped as `LimboError::Constraint("undocumented halt
            // error code ...")` — `halt()` is for permanent statement
            // errors, not transient retryable ones. Returning Err here
            // surfaces as a proper `LimboError::Busy` to the caller.
            state.sequence_inner_retry_count = state.sequence_inner_retry_count.saturating_add(1);
            // Mirror onto the connection so cross-statement observers (tests
            // asserting that the non-CYCLE hot path is conflict-free) can
            // witness retries that the per-statement counter would discard.
            program
                .connection
                .sequence_inner_retries
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if state.sequence_inner_retry_count > SEQUENCE_INNER_TX_RETRY_BUDGET {
                state.sequence_inner_retry_count = 0;
                let _ = err;
                return Err(LimboError::Busy.into());
            }
            // Re-export the specific class via the status register —
            // the translator-emitted retry uses it to decide whether
            // to jump back.
            let _ = err;
            state.registers[*status_reg]
                .set_value(Value::from_i64(SEQ_COMMIT_STATUS_CONFLICT_RETRY));
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Err(e) => {
            state.sequence_inner_commit = None;
            if let Some((inner_tx_id, _)) = conn.get_mv_tx_for_db(*db) {
                if mv_store.is_tx_rollbackable(inner_tx_id) {
                    mv_store.rollback_tx(inner_tx_id, pager.clone(), &conn, *db);
                }
            }
            state.sequence_inner_tx_pending = None;
            conn.set_mv_tx_for_db(*db, saved_outer);
            Err(e)
        }
    }
}

pub fn op_drop_trigger(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(DropTrigger { db, trigger_name }, insn);

    let conn = program.connection.clone();
    conn.with_database_schema_mut(*db, |schema| {
        schema.remove_trigger(trigger_name).ok();
    })?;
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_close(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Close { cursor_id }, insn);
    if let Some(Cursor::IndexMethod(cursor)) = state.cursors[*cursor_id].as_mut() {
        let context = state.index_method_contexts[*cursor_id]
            .as_ref()
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "index-method cursor {cursor_id} closed without an execution context"
                ))
            })?
            .clone();
        return_if_io!(cursor.stage_statement_commit(&context));
        let Some(Cursor::IndexMethod(cursor)) = state.cursors[*cursor_id].take() else {
            unreachable!("cursor variant checked above");
        };
        let context = state.index_method_contexts[*cursor_id]
            .take()
            .expect("index-method context checked above");
        state.closed_index_method_cursors.push((cursor, context));
    }
    let cursors = &mut state.cursors;
    cursors
        .get_mut(*cursor_id)
        .expect("cursor_id should be valid")
        .take();
    if let Some(deferred_seek) = state.deferred_seeks.get_mut(*cursor_id) {
        deferred_seek.take();
    }
    state.ephemeral_temp_files.remove(cursor_id);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_is_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IsNull { reg, target_pc }, insn);
    if matches!(state.registers[*reg], Register::Value(Value::Null)) {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_page_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(PageCount { db, dest }, insn);
    let pager = program.get_pager_from_database_index(db)?;
    let mv_store = program.connection.mv_store_for_db(*db);
    let count = match with_header(&pager, mv_store.as_ref(), program, *db, |header| {
        header.database_size.get()
    }) {
        Err(_) => 0.into(),
        Ok(IOResult::Done(v)) => v.into(),
        Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
    };
    state.registers[*dest].set_int(count);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// State for the async ParseSchema instruction state machine.
/// Stored in the active opcode state slot so that when the inner
/// schema query yields IO, we can return control to the outer caller and
/// resume later without losing intermediate parsing state.
pub struct OpParseSchemaInner {
    stmt: crate::Statement,
    schema_arc: Arc<Schema>,
    from_sql_indexes: crate::alloc::Vec<crate::util::UnparsedFromSqlIndex>,
    automatic_indices: crate::HashMap<String, crate::alloc::Vec<(String, i64)>>,
    dbsp_state_roots: crate::HashMap<String, i64>,
    dbsp_state_index_roots: crate::HashMap<String, i64>,
    materialized_view_info: crate::HashMap<String, (String, i64)>,
    trigger_target_database_id: Option<usize>,
    db: usize,
    previous_auto_commit: bool,
}

pub type OpParseSchemaState = Option<Box<OpParseSchemaInner>>;

impl OpParseSchemaInner {
    /// The connection's `auto_commit` value saved at ParseSchema entry, so
    /// abort paths can restore it when the op never reached its Done arm.
    pub(crate) fn previous_auto_commit(&self) -> bool {
        self.previous_auto_commit
    }
}

pub fn op_parse_schema(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        ParseSchema {
            db,
            where_clause,
            trigger_target_database_id
        },
        insn
    );

    let conn = program.connection.clone();

    // If we have in-progress state, resume stepping through schema rows.
    if state.active_op_state.parse_schema().is_some() {
        return op_parse_schema_step(state, &conn);
    }

    // set auto commit to false in order for parse schema to not commit changes as transaction state is stored in connection,
    // and we use the same connection for nested query.
    let previous_auto_commit = conn.auto_commit.load(Ordering::SeqCst);
    conn.auto_commit.store(false, Ordering::SeqCst);

    // For attached databases, qualify the sqlite_schema table with the database name
    let schema_table = if *db != crate::MAIN_DB_ID {
        let db_name = conn
            .get_database_name_by_index(*db)
            .unwrap_or_else(|| "main".to_string());
        format!("{db_name}.sqlite_schema")
    } else {
        SCHEMA_TABLE_NAME.to_string()
    };
    let sql = if let Some(where_clause) = where_clause {
        format!("SELECT * FROM {schema_table} WHERE {where_clause}")
    } else {
        format!("SELECT * FROM {schema_table}")
    };
    let mut stmt = conn.prepare_internal(sql)?;
    // ParseSchema runs as a nested helper statement inside the parent schema
    // mutation. It only reads sqlite_schema, so a statement subtransaction is
    // unnecessary and can contend with the parent's subjournal on temp/attached
    // pagers, surfacing as SQLITE_BUSY during temp DDL.
    stmt.program
        .prepared
        .needs_stmt_subtransactions
        .store(false, Ordering::Relaxed);

    // Get a mutable schema clone *without* holding the schema lock during
    // nested statement execution.  The nested Statement may call reprepare()
    // which also acquires the schema / database_schemas write lock, so holding
    // it here would deadlock on the same thread (parking_lot RwLock is not
    // re-entrant).
    let schema_arc = if *db == crate::TEMP_DB_ID {
        // TEMP: single source of truth is `temp_db.db.schema`. Skip
        // `database_schemas` staging entirely — it would only go stale.
        conn.temp
            .database
            .read()
            .as_ref()
            .map(|temp_db| temp_db.db.schema.lock().clone())
            .unwrap_or_else(|| conn.empty_temp_schema())
    } else if *db != crate::MAIN_DB_ID {
        let fallback_schema = {
            let attached_dbs = conn.attached_databases.read();
            let Some(entry) = attached_dbs.index_to_data.get(db) else {
                conn.auto_commit
                    .store(previous_auto_commit, Ordering::SeqCst);
                return Err(LimboError::InternalError(format!(
                    "stale reference to detached database (index {db})"
                ))
                .into());
            };
            let schema = entry.db.schema.lock().clone();
            schema
        };
        let mut schemas = conn.database_schemas().write();
        schemas.entry(*db).or_insert(fallback_schema).clone() // cheap Arc clone; write lock released at end of block
    } else {
        conn.schema.read().clone()
    };

    // Set up MVCC transaction for the nested statement
    let mv_tx = program.connection.get_mv_tx();
    stmt.set_mv_tx(mv_tx);

    // Store state for resumption across IO boundaries
    *state.active_op_state.parse_schema() = Some(Box::new(OpParseSchemaInner {
        stmt,
        schema_arc,
        from_sql_indexes: crate::alloc::Vec::try_with_capacity_ext(10)?,
        automatic_indices: Default::default(),
        dbsp_state_roots: Default::default(),
        dbsp_state_index_roots: Default::default(),
        materialized_view_info: Default::default(),
        trigger_target_database_id: *trigger_target_database_id,
        db: *db,
        previous_auto_commit,
    }));

    // Now begin stepping through the schema rows
    op_parse_schema_step(state, &conn)
}

/// Drive the inner schema statement one step at a time.
/// Returns IO to the outer VDBE loop when the inner statement needs it,
/// preserving all intermediate parsing state for resumption.
fn op_parse_schema_step(state: &mut ProgramState, conn: &Arc<Connection>) -> InsnResult {
    loop {
        let inner = state.active_op_state.parse_schema().as_mut().unwrap();
        match inner.stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                let io = inner
                    .stmt
                    .take_io_completions()
                    .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                return Ok(InsnFunctionStepResult::IO(io));
            }
            StepResult::Row => {
                let inner = state
                    .active_op_state
                    .parse_schema()
                    .as_mut()
                    .expect("parse schema state should exist");
                let row = inner.stmt.row().expect("row should be present");
                let ty = row.get::<&str>(0)?;
                let name = row.get::<&str>(1)?;
                let table_name = row.get::<&str>(2)?;
                let root_page = row.get::<i64>(3)?;
                let sql = row.get::<&str>(4).ok();
                let schema = Schema::try_make_mut(&mut inner.schema_arc)?;
                let syms = conn.syms.read();
                let attached_resolver = |alias: &str| -> Option<usize> {
                    conn.attached_databases()
                        .read()
                        .get_database_by_name(&crate::util::normalize_ident(alias))
                        .map(|(idx, _)| idx)
                };
                schema.handle_schema_row(
                    ty,
                    name,
                    table_name,
                    root_page,
                    sql,
                    &syms,
                    &mut inner.from_sql_indexes,
                    &mut inner.automatic_indices,
                    &mut inner.dbsp_state_roots,
                    &mut inner.dbsp_state_index_roots,
                    &mut inner.materialized_view_info,
                    &attached_resolver,
                    conn.dialect().as_ref(),
                )?;
                if ty == "trigger" {
                    if let Some(target_database_id) = inner.trigger_target_database_id {
                        schema.set_trigger_target_database_id(name, target_database_id)?;
                    }
                }
                continue;
            }
            StepResult::Done => {
                // Take the state to finalize
                let OpParseSchemaInner {
                    stmt,
                    mut schema_arc,
                    from_sql_indexes,
                    automatic_indices,
                    dbsp_state_roots,
                    dbsp_state_index_roots,
                    materialized_view_info,
                    trigger_target_database_id: _,
                    db,
                    previous_auto_commit,
                } = *state
                    .active_op_state
                    .parse_schema()
                    .take()
                    .expect("parse schema state should exist");
                let schema = Schema::try_make_mut(&mut schema_arc)?;
                let mv_store = stmt.mv_store();
                let syms = conn.syms.read();

                let res1 = schema.populate_indices(
                    &syms,
                    from_sql_indexes,
                    automatic_indices,
                    mv_store.is_some(),
                );
                let res2 = schema.populate_materialized_views(
                    materialized_view_info,
                    dbsp_state_roots,
                    dbsp_state_index_roots,
                );

                // Store the modified schema back
                if db == crate::TEMP_DB_ID {
                    // TEMP: write directly to `temp_db.db.schema` — the single
                    // source of truth; never stage in `database_schemas`.
                    //
                    // Refresh schema_version from the pager header first. At
                    // this point SetCookie has already run for the current
                    // DDL, so the header cookie is authoritative; without this
                    // the cached version can drift on subsequent temp DDLs in
                    // the same transaction and trigger a spurious
                    // SchemaUpdated -> reprepare -> rollback_attached() loop
                    // that wipes in-flight dirty pages.
                    if let Some(temp_db) = conn.temp.database.read().as_ref() {
                        if let Some(cookie) = temp_db.pager.get_schema_cookie_cached() {
                            schema.schema_version = cookie;
                        }
                        *temp_db.db.schema.lock() = schema_arc;
                    }
                } else if db != crate::MAIN_DB_ID {
                    conn.database_schemas().write().insert(db, schema_arc);
                } else {
                    *conn.schema.write() = schema_arc;
                }
                drop(stmt);
                conn.auto_commit
                    .store(previous_auto_commit, Ordering::SeqCst);
                let _ = (res1?, res2?);

                state.pc += 1;
                state.active_op_state.clear();
                return Ok(InsnFunctionStepResult::Step);
            }
            StepResult::Interrupt => {
                let OpParseSchemaInner {
                    stmt,
                    previous_auto_commit,
                    ..
                } = *state
                    .active_op_state
                    .parse_schema()
                    .take()
                    .expect("parse schema state should exist");
                drop(stmt);
                conn.auto_commit
                    .store(previous_auto_commit, Ordering::SeqCst);
                return Err(LimboError::Interrupt.into());
            }
            StepResult::Busy => {
                let OpParseSchemaInner {
                    stmt,
                    previous_auto_commit,
                    ..
                } = *state
                    .active_op_state
                    .parse_schema()
                    .take()
                    .expect("parse schema state should exist");
                drop(stmt);
                conn.auto_commit
                    .store(previous_auto_commit, Ordering::SeqCst);
                return Err(LimboError::Busy.into());
            }
        }
    }
}

/// Phases of the multi-statement state machine driven by [`op_init_cdc_version`].
/// Each phase owns a single sub-statement that is stepped to completion before
/// transitioning to the next phase, yielding `IO` to the outer VDBE loop instead
/// of blocking the executor thread on `pager.io.step()`.
#[derive(Debug)]
pub enum OpInitCdcVersionPhase {
    /// `SELECT 1 FROM sqlite_schema WHERE ... AND name=cdc_table` — used to detect
    /// a legacy v1 CDC table (one that pre-dates version tracking).
    CheckTable,
    /// `CREATE TABLE IF NOT EXISTS <cdc_table> (...)`.
    CreateCdcTable,
    /// `CREATE TABLE IF NOT EXISTS <version_table> (...)`.
    CreateVersionTable,
    /// `INSERT OR IGNORE INTO <version_table> VALUES (...)`.
    InsertVersion,
    /// `SELECT version FROM <version_table> WHERE table_name=...`.
    ReadVersion,
}

pub struct OpInitCdcVersionInner {
    phase: OpInitCdcVersionPhase,
    stmt: crate::Statement,
    cdc_table_exists: bool,
    actual_version: Option<CdcVersion>,
}

pub type OpInitCdcVersionState = Option<Box<OpInitCdcVersionInner>>;

fn prepare_cdc_internal(conn: &Arc<Connection>, sql: String) -> Result<crate::Statement> {
    let stmt = conn.prepare_internal(sql)?;
    stmt.program
        .prepared
        .needs_stmt_subtransactions
        .store(false, Ordering::Relaxed);
    Ok(stmt)
}

pub fn op_init_cdc_version(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        InitCdcVersion {
            cdc_table_name,
            version,
            cdc_mode,
        },
        insn
    );

    let conn = program.connection.clone();
    let escaped_cdc_table_name = escape_sql_string_literal(cdc_table_name);

    // First entry — handle no-op cases without spinning up the state machine,
    // and otherwise prime the first sub-statement (Phase 1: existence check).
    if state.active_op_state.init_cdc_version().is_none() {
        // "off" — disable CDC (table and version entry are preserved).
        if CaptureDataChangesInfo::parse(cdc_mode, None)?.is_none() {
            state.pending_cdc_info = Some(None);
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }

        // If CDC is already enabled, re-parse with current version and exit
        // early. Idempotent; avoids CDC capturing its own table creation when
        // the pragma is called multiple times.
        if let Some(info) = conn.get_capture_data_changes_info().as_ref() {
            let opts = CaptureDataChangesInfo::parse(cdc_mode, info.version)?;
            state.pending_cdc_info = Some(opts);
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }

        let stmt = prepare_cdc_internal(
            &conn,
            format!(
                "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='{escaped_cdc_table_name}'",
            ),
        )?;
        *state.active_op_state.init_cdc_version() = Some(Box::new(OpInitCdcVersionInner {
            phase: OpInitCdcVersionPhase::CheckTable,
            stmt,
            cdc_table_exists: false,
            actual_version: None,
        }));
    }

    let res = drive_init_cdc_version(
        state,
        &conn,
        version,
        cdc_mode,
        cdc_table_name,
        &escaped_cdc_table_name,
    );
    // Any error tears down the parked state machine so a subsequent step on the
    // same ProgramState (without an explicit reset) starts fresh instead of
    // resuming from a dangling sub-statement.
    if res.is_err() {
        state.active_op_state.clear();
    }
    res
}

fn drive_init_cdc_version(
    state: &mut ProgramState,
    conn: &Arc<Connection>,
    version: &CdcVersion,
    cdc_mode: &str,
    cdc_table_name: &str,
    escaped_cdc_table_name: &str,
) -> InsnResult {
    loop {
        let inner = state.active_op_state.init_cdc_version().as_mut().unwrap();
        match inner.stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                let io = inner
                    .stmt
                    .take_io_completions()
                    .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                return Ok(InsnFunctionStepResult::IO(io));
            }
            StepResult::Row => match &inner.phase {
                OpInitCdcVersionPhase::CheckTable => {
                    // Any row means the table exists; keep stepping until Done.
                    inner.cdc_table_exists = true;
                }
                OpInitCdcVersionPhase::ReadVersion => {
                    let row = inner.stmt.row().expect("row should be present");
                    if let crate::Value::Text(text) = row.get::<&crate::Value>(0)? {
                        inner.actual_version = Some(text.to_string().parse::<CdcVersion>()?);
                    }
                }
                phase => unreachable!(
                    "op_init_cdc_version: unexpected Row from non-SELECT phase {:?}",
                    phase
                ),
            },
            StepResult::Done => match inner.phase {
                OpInitCdcVersionPhase::CheckTable => {
                    let create_sql = match version {
                        CdcVersion::V1 => format!(
                            "CREATE TABLE IF NOT EXISTS {cdc_table_name} (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
                        ),
                        CdcVersion::V2 => format!(
                            "CREATE TABLE IF NOT EXISTS {cdc_table_name} (change_id INTEGER PRIMARY KEY AUTOINCREMENT, change_time INTEGER, change_txn_id INTEGER, change_type INTEGER, table_name TEXT, id, before BLOB, after BLOB, updates BLOB)",
                        ),
                    };
                    inner.stmt = prepare_cdc_internal(conn, create_sql)?;
                    inner.phase = OpInitCdcVersionPhase::CreateCdcTable;
                }
                OpInitCdcVersionPhase::CreateCdcTable => {
                    inner.stmt = prepare_cdc_internal(
                        conn,
                        format!(
                            "CREATE TABLE IF NOT EXISTS {TURSO_CDC_VERSION_TABLE_NAME} (table_name TEXT PRIMARY KEY, version TEXT NOT NULL)",
                        ),
                    )?;
                    inner.phase = OpInitCdcVersionPhase::CreateVersionTable;
                }
                OpInitCdcVersionPhase::CreateVersionTable => {
                    // If the CDC table pre-existed without a version row, it's
                    // a legacy v1 table — pin to V1 regardless of the requested
                    // version.
                    let version_to_insert = if inner.cdc_table_exists {
                        CdcVersion::V1
                    } else {
                        *version
                    };
                    inner.stmt = prepare_cdc_internal(
                        conn,
                        format!(
                            "INSERT OR IGNORE INTO {TURSO_CDC_VERSION_TABLE_NAME} (table_name, version) VALUES ('{escaped_cdc_table_name}', '{version_to_insert}')",
                        ),
                    )?;
                    inner.phase = OpInitCdcVersionPhase::InsertVersion;
                }
                OpInitCdcVersionPhase::InsertVersion => {
                    inner.stmt = prepare_cdc_internal(
                        conn,
                        format!(
                            "SELECT version FROM {TURSO_CDC_VERSION_TABLE_NAME} WHERE table_name = '{escaped_cdc_table_name}'",
                        ),
                    )?;
                    inner.phase = OpInitCdcVersionPhase::ReadVersion;
                }
                OpInitCdcVersionPhase::ReadVersion => {
                    // Read back the actual version (may differ from `version`
                    // if the row already existed with an older version).
                    let actual_version = inner.actual_version.unwrap_or(*version);
                    let opts = CaptureDataChangesInfo::parse(cdc_mode, Some(actual_version))?;
                    // Defer enabling CDC until the program completes
                    // successfully (Halt). Ensures rollback leaves the
                    // connection's CDC state unchanged.
                    state.pending_cdc_info = Some(opts);
                    state.active_op_state.clear();
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                }
            },
            // Interrupt/Busy fall through to the caller, which clears the
            // parked state machine on error.
            StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
            StepResult::Busy => return Err(LimboError::Busy.into()),
        }
    }
}

pub fn op_populate_materialized_views(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(PopulateMaterializedViews { cursors }, insn);

    let conn = program.connection.clone();

    // For each view, get its cursor and root page
    let mut view_info = Vec::new();
    {
        let cursors_ref = &state.cursors;
        for (view_name, cursor_id) in cursors {
            // Get the cursor to find the root page
            let cursor = cursors_ref
                .get(*cursor_id)
                .and_then(|c| c.as_ref())
                .ok_or_else(|| {
                    LimboError::InternalError(format!("Cursor {cursor_id} not found"))
                })?;

            let root_page = match cursor {
                crate::types::Cursor::BTree(btree_cursor) => btree_cursor.root_page(),
                _ => {
                    return Err(LimboError::InternalError(
                        "Expected BTree cursor for materialized view".into(),
                    )
                    .into());
                }
            };

            view_info.push((view_name.clone(), root_page, *cursor_id));
        }
    }

    // Now populate the views (after releasing the schema borrow)
    for (view_name, _root_page, cursor_id) in view_info {
        let schema = conn.schema.read();
        if let Some(view) = schema.get_materialized_view(&view_name) {
            let mut view = view.lock();
            // Drop the schema borrow before calling populate_from_table
            drop(schema);

            // Get the cursor for writing
            // Get a mutable reference to the cursor
            let cursors_ref = &mut state.cursors;
            let cursor = cursors_ref
                .get_mut(cursor_id)
                .and_then(|c| c.as_mut())
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "Cursor {cursor_id} not found for population"
                    ))
                })?;

            // Extract the BTreeCursor
            let btree_cursor = match cursor {
                crate::types::Cursor::BTree(btree_cursor) => btree_cursor,
                _ => {
                    return Err(LimboError::InternalError(
                        "Expected BTree cursor for materialized view population".into(),
                    )
                    .into());
                }
            };

            // Now populate it with the cursor for writing
            return_if_io!(view.populate_from_table(&conn, pager, btree_cursor.as_mut()));
        }
    }

    // All views populated, advance to next instruction
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_read_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ReadCookie { db, dest, cookie }, insn);
    let pager = program.get_pager_from_database_index(db)?;
    let mv_store = program.connection.mv_store_for_db(*db);

    let cookie_value =
        match with_header(
            &pager,
            mv_store.as_ref(),
            program,
            *db,
            |header| match cookie {
                Cookie::ApplicationId => header.application_id.get().into(),
                Cookie::DatabaseFormat => header.schema_format.get().into(),
                Cookie::DatabaseTextEncoding => match header.text_encoding {
                    crate::storage::sqlite3_ondisk::TextEncoding::Unset
                    | crate::storage::sqlite3_ondisk::TextEncoding::Utf8 => 1,
                    crate::storage::sqlite3_ondisk::TextEncoding::Utf16Le => 2,
                    crate::storage::sqlite3_ondisk::TextEncoding::Utf16Be => 3,
                    _ => 0,
                },
                Cookie::DefaultPageCacheSize => header.default_page_cache_size.get() as i64,
                Cookie::UserVersion => header.user_version.get().into(),
                Cookie::SchemaVersion => header.schema_cookie.get().into(),
                Cookie::LargestRootPageNumber => header.vacuum_mode_largest_root_page.get().into(),
                cookie => todo!("{cookie:?} is not yet implement for ReadCookie"),
            },
        ) {
            Err(_) => 0.into(),
            Ok(IOResult::Done(v)) => v,
            Ok(IOResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
        };

    state.registers[*dest].set_int(cookie_value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_set_cookie(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        SetCookie {
            db,
            cookie,
            value,
            p5: _,
        },
        insn
    );
    let pager = program.get_pager_from_database_index(db)?;
    let mv_store = program.connection.mv_store_for_db(*db);
    if let Some(mv_store) = mv_store.as_ref() {
        let Some(tx_id) = program.connection.get_mv_tx_id_for_db(*db) else {
            return Err(LimboError::InternalError(
                "Header updates in MVCC mode require an active transaction".to_string(),
            )
            .into());
        };
        if !mv_store.is_exclusive_tx(&tx_id) {
            // Header cookies are global metadata with no row-level conflict keys; require
            // SQLite-style single-writer semantics (same policy as DDL in MVCC).
            return Err(LimboError::TxError(
                "Header updates require an exclusive transaction (use BEGIN instead of BEGIN CONCURRENT)".to_string(),
            ).into());
        }
    }

    match with_header_mut(&pager, mv_store.as_ref(), program, *db, |header| {
        match cookie {
            Cookie::ApplicationId => header.application_id = (*value).into(),
            Cookie::DatabaseFormat => header.schema_format = (*value as u32).into(),
            Cookie::DatabaseTextEncoding => {
                header.text_encoding = match *value {
                    1 => crate::storage::sqlite3_ondisk::TextEncoding::Utf8,
                    2 => crate::storage::sqlite3_ondisk::TextEncoding::Utf16Le,
                    3 => crate::storage::sqlite3_ondisk::TextEncoding::Utf16Be,
                    _ => {
                        return Err(LimboError::InternalError(format!(
                            "unsupported text encoding cookie value: {value}",
                        )));
                    }
                };
            }
            Cookie::DefaultPageCacheSize => {
                header.default_page_cache_size =
                    crate::storage::sqlite3_ondisk::CacheSize::new(*value);
            }
            Cookie::UserVersion => header.user_version = (*value).into(),
            Cookie::LargestRootPageNumber => {
                header.vacuum_mode_largest_root_page = (*value as u32).into();
            }
            Cookie::IncrementalVacuum => header.incremental_vacuum_enabled = (*value as u32).into(),
            Cookie::SchemaVersion => {
                // Only mark schema_did_change on connection for main database (db 0).
                // Attached databases track their schema independently.
                if *db == crate::MAIN_DB_ID {
                    match program.connection.get_tx_state() {
                        TransactionState::Write { .. } => {
                            program.connection.set_tx_state(TransactionState::Write {
                                schema_did_change: true,
                            });
                        }
                        TransactionState::Read => unreachable!(
                            "invalid transaction state for SetCookie: TransactionState::Read, should be write"
                        ),
                        TransactionState::None => unreachable!(
                            "invalid transaction state for SetCookie: TransactionState::None, should be write"
                        ),
                        TransactionState::PendingUpgrade { .. } => unreachable!(
                            "invalid transaction state for SetCookie: TransactionState::PendingUpgrade, should be write"
                        ),
                    }
                } else if *db == crate::TEMP_DB_ID {
                    // TEMP has no shared `Database::schema` to publish to, so
                    // commit/rollback consult a separate `committed_temp_schema`
                    // snapshot on the connection. Flag that it needs updating.
                    program.connection.mark_temp_schema_did_change();
                }
                program.connection.with_database_schema_mut(*db, |schema| {
                    schema.schema_version = *value as u32
                })?;
                header.schema_cookie = (*value as u32).into();
            }
        };
        Ok(())
    })? {
        IOResult::Done(result) => result?,
        IOResult::IO(io) => return Ok(InsnFunctionStepResult::IO(io)),
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_right(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ShiftRight { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_shift_right(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_shift_left(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ShiftLeft { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_shift_left(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_imm(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(AddImm { register, value }, insn);

    let current = &state.registers[*register];
    let current_value = match current {
        Register::Value(val) => val,
        Register::Aggregate(_) => &Value::Null,
        Register::Record(_) => &Value::Null,
    };

    // SQLite coerces the register to an integer before adding.
    let lhs = match current_value {
        Value::Numeric(Numeric::Integer(i)) => *i,
        Value::Numeric(Numeric::Float(f)) => real_to_i64(f64::from(*f)),
        Value::Text(s) => crate::numeric::str_to_i64(s.as_str()).unwrap_or(0),
        Value::Blob(b) => {
            crate::numeric::str_to_i64(String::from_utf8_lossy(b).as_ref()).unwrap_or(0)
        }
        Value::Null => 0,
    };
    let int_val = lhs.wrapping_add(*value);

    state.registers[*register].set_int(int_val);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_variable(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Variable { index, dest }, insn);
    state.registers[*dest].set_value(state.get_parameter(*index));
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_zero_or_null(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ZeroOrNull { rg1, rg2, dest }, insn);
    if state.registers[*rg1].is_null() || state.registers[*rg2].is_null() {
        state.registers[*dest].set_null()
    } else {
        state.registers[*dest].set_int(0);
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_not(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Not { reg, dest }, insn);
    match state.registers[*reg].get_value().exec_boolean_not() {
        Value::Numeric(Numeric::Integer(i)) => state.registers[*dest].set_int(i),
        _ => state.registers[*dest].set_null(),
    };
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Implements IS TRUE, IS FALSE, IS NOT TRUE, IS NOT FALSE.
/// A value is "true" only if it's a non-zero number.
/// Text and blobs are parsed as numbers; if not parseable, treated as 0 (falsy).
/// NULL is handled specially with the null_value parameter.
pub fn op_is_true(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        IsTrue {
            reg,
            dest,
            null_value,
            invert
        },
        insn
    );
    let value = state.registers[*reg].get_value();
    // Use Numeric::try_into_bool which handles the conversion of text/blob to numbers
    let final_result = match Numeric::from_value(value).map(|val| val.to_bool()) {
        // For NULL, store null_value directly (no inversion)
        None => {
            if *null_value {
                1
            } else {
                0
            }
        }
        // For non-NULL, optionally invert the boolean result
        Some(is_truthy) => {
            let result = if is_truthy { 1 } else { 0 };
            if *invert {
                1 - result
            } else {
                result
            }
        }
    };
    state.registers[*dest].set_int(final_result);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_concat(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Concat { lhs, rhs, dest }, insn);
    let value = state.registers[*lhs]
        .get_value()
        .exec_concat(state.registers[*rhs].get_value())?;
    state.registers[*dest].set_value(value);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_and(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(And { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_and(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_or(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Or { lhs, rhs, dest }, insn);
    state.registers[*dest].set_value(
        state.registers[*lhs]
            .get_value()
            .exec_or(state.registers[*rhs].get_value()),
    );
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_noop(
    _program: &Program,
    state: &mut ProgramState,
    _insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    // Do nothing
    // Advance the program counter for the next opcode
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

#[derive(Default)]
pub enum OpOpenEphemeralState {
    #[default]
    Start,
    // Fast path states for reusing existing ephemeral cursor
    ClearExisting,
    RewindExisting,
    // Slow path states for creating new ephemeral cursor
    StartingTxn {
        pager: Arc<Pager>,
        temp_file: Option<TempFile>,
    },
    CreateBtree {
        pager: Arc<Pager>,
        temp_file: Option<TempFile>,
    },
    // clippy complains this variant is too big when compared to the rest of the variants
    // so it says we need to box it here
    Rewind {
        cursor: Box<dyn CursorTrait>,
        temp_file: Option<TempFile>,
    },
}
pub fn op_open_ephemeral(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    let (cursor_id, is_table) = match insn {
        Insn::OpenEphemeral {
            cursor_id,
            is_table,
        } => (*cursor_id, *is_table),
        Insn::OpenAutoindex { cursor_id } => (*cursor_id, false),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };
    let mv_store = program.connection.mv_store();
    match state.active_op_state.open_ephemeral() {
        OpOpenEphemeralState::Start => {
            tracing::trace!("Start");
            // Fast path: if cursor already has an ephemeral btree, just clear it instead of
            // recreating the entire pager/file/btree. This is important for performance when
            // OpenEphemeral is called repeatedly during statement execution.
            // A NullRow placeholder is not a real ephemeral btree; drop it.
            if matches!(state.cursors[cursor_id], Some(Cursor::NullRow)) {
                state.cursors[cursor_id] = None;
            }
            if state.cursors[cursor_id].is_some() {
                *state.active_op_state.open_ephemeral() = OpOpenEphemeralState::ClearExisting;
                return Ok(InsnFunctionStepResult::Step);
            }
            // Ephemeral tables always use the main DB's page size (db index 0)
            // regardless of which database triggered the ephemeral allocation.
            let page_size = return_if_io!(with_header(
                pager,
                mv_store.as_ref(),
                program,
                0,
                |header| { header.page_size }
            ));
            let conn = program.connection.clone();
            let io = conn.pager.load().io.clone();
            let temp_store = conn.get_temp_store();
            let temp_file = TempFile::with_temp_store(&io, temp_store)?;
            let db_file: Arc<dyn DatabaseStorage> =
                Arc::new(DatabaseFile::new(temp_file.file.clone()));
            let db_file_io: Arc<dyn crate::IO> = io;

            let buffer_pool = program.connection.db.buffer_pool.clone();

            // Ephemeral databases always start empty, so create their own init_page_1
            let ephemeral_init_page_1 =
                Arc::new(arc_swap::ArcSwapOption::new(Some(default_page1(None))));

            let pager = Arc::new(Pager::new(
                db_file,
                None,
                db_file_io,
                PageCache::default(),
                buffer_pool,
                Arc::new(Mutex::new(())),
                ephemeral_init_page_1,
            )?);

            pager.set_page_size(page_size);

            *state.active_op_state.open_ephemeral() = OpOpenEphemeralState::StartingTxn {
                pager,
                temp_file: Some(temp_file),
            };
        }
        OpOpenEphemeralState::ClearExisting => {
            tracing::trace!("ClearExisting");
            let cursor = state.cursors[cursor_id]
                .as_mut()
                .expect("cursor should exist in ClearExisting state");
            let btree_cursor = cursor.as_btree_mut();
            btree_cursor.set_null_flag(false);
            return_if_io!(btree_cursor.clear_btree());
            invalidate_deferred_seeks_for_cursor(state, cursor_id);
            *state.active_op_state.open_ephemeral() = OpOpenEphemeralState::RewindExisting;
        }
        OpOpenEphemeralState::RewindExisting => {
            tracing::trace!("RewindExisting");
            let cursor = state.cursors[cursor_id]
                .as_mut()
                .expect("cursor should exist in RewindExisting state");
            let btree_cursor = cursor.as_btree_mut();
            return_if_io!(btree_cursor.rewind());
            state.pc += 1;
            state.active_op_state.clear();
        }
        OpOpenEphemeralState::StartingTxn { pager, temp_file } => {
            tracing::trace!("StartingTxn");
            pager
                .begin_read_tx() // we have to begin a read tx before beginning a write
                .expect("Failed to start read transaction");
            return_if_io!(pager.begin_write_tx(WalAutoActions::all_enabled()));
            *state.active_op_state.open_ephemeral() = OpOpenEphemeralState::CreateBtree {
                pager: pager.clone(),
                temp_file: temp_file.take(),
            };
        }
        OpOpenEphemeralState::CreateBtree { pager, temp_file } => {
            tracing::trace!("CreateBtree");
            // FIXME: handle page cache is full
            let flag = if is_table {
                &CreateBTreeFlags::new_table()
            } else {
                &CreateBTreeFlags::new_index()
            };
            let root_page = return_if_io!(pager.btree_create(flag)) as i64;

            let (_, cursor_type) = program
                .cursor_ref
                .get(cursor_id)
                .expect("cursor_id should exist in cursor_ref");

            let num_columns = match cursor_type {
                CursorType::BTreeTable(table_rc) => table_rc.columns().len(),
                CursorType::BTreeIndex(index_arc) => index_arc.columns.len(),
                _ => unreachable!("This should not have happened"),
            };

            let cursor = if let CursorType::BTreeIndex(index) = cursor_type {
                BTreeCursor::new_index(pager.clone(), root_page, index, num_columns)
            } else {
                Ok(BTreeCursor::new_table(
                    pager.clone(),
                    root_page,
                    num_columns,
                ))
            }?;
            *state.active_op_state.open_ephemeral() = OpOpenEphemeralState::Rewind {
                cursor: Box::new(cursor),
                temp_file: temp_file.take(),
            };
        }
        OpOpenEphemeralState::Rewind {
            cursor,
            temp_file: _,
        } => {
            return_if_io!(cursor.rewind());

            let cursors = &mut state.cursors;

            let (_, cursor_type) = program
                .cursor_ref
                .get(cursor_id)
                .expect("cursor_id should exist in cursor_ref");

            let OpOpenEphemeralState::Rewind { cursor, temp_file } =
                std::mem::take(state.active_op_state.open_ephemeral())
            else {
                unreachable!()
            };

            let tf = temp_file.expect("temp_file must be present in Rewind state");
            state.ephemeral_temp_files.insert(cursor_id, tf);

            // Table content is erased if the cursor already exists
            match cursor_type {
                CursorType::BTreeTable(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .expect("cursor_id should be valid")
                        .replace(Cursor::new_btree(cursor));
                }
                CursorType::BTreeIndex(_) => {
                    cursors
                        .get_mut(cursor_id)
                        .expect("cursor_id should be valid")
                        .replace(Cursor::new_btree(cursor));
                }
                CursorType::Pseudo(_) => {
                    panic!("OpenEphemeral on pseudo cursor");
                }
                CursorType::Sorter => {
                    panic!("OpenEphemeral on sorter cursor");
                }
                CursorType::VirtualTable(_) => {
                    panic!("OpenEphemeral on virtual table cursor, use Insn::VOpen instead");
                }
                CursorType::IndexMethod(..) => {
                    panic!("OpenEphemeral on index method cursor")
                }
                CursorType::MaterializedView(_, _) => {
                    panic!("OpenEphemeral on materialized view cursor");
                }
            }

            state.pc += 1;
            state.active_op_state.clear();
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_open_dup(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        OpenDup {
            new_cursor_id,
            original_cursor_id,
        },
        insn
    );
    let mv_store = program.connection.mv_store();

    let original_cursor = state.get_cursor(*original_cursor_id);
    let original_cursor = original_cursor.as_btree_mut();

    let root_page = original_cursor.root_page();
    // We use the pager from the original cursor instead of the one attached to
    // the connection because each ephemeral table creates its own pager (and
    // a separate database file).
    let pager = original_cursor.get_pager();

    // Ephemeral tables have their own pager, so we need to check if this is an
    // ephemeral cursor by comparing pagers. Ephemeral cursors should NOT be wrapped
    // in MvCursor because the mv_store doesn't have mappings for ephemeral table root pages.
    let is_ephemeral = pager.wal.is_none();

    let (_, cursor_type) = program
        .cursor_ref
        .get(*original_cursor_id)
        .expect("cursor_id should exist in cursor_ref");
    match cursor_type {
        CursorType::BTreeTable(table) => {
            if !table.has_rowid && program.connection.get_mv_tx_id().is_some() {
                return Err(LimboError::ParseError(
                    "WITHOUT ROWID tables are not supported in MVCC mode".to_string(),
                )
                .into());
            }
            let cursor: Box<dyn CursorTrait> = if table.has_rowid {
                Box::new(BTreeCursor::new_table(
                    pager,
                    maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                    table.columns().len(),
                ))
            } else {
                Box::new(BTreeCursor::new_without_rowid_table(
                    pager,
                    maybe_transform_root_page_to_positive(mv_store.as_ref(), root_page),
                    table.as_ref(),
                    table.columns().len(),
                ))
            };
            let cursor: Box<dyn CursorTrait> = if !is_ephemeral {
                if let Some(tx_id) = program.connection.get_mv_tx_id() {
                    let mv_store = mv_store
                        .as_ref()
                        .expect("mv_store should be Some when MVCC transaction is active")
                        .clone();
                    Box::new(MvCursor::new(
                        mv_store,
                        &program.connection,
                        tx_id,
                        root_page,
                        MvccCursorType::Table,
                        cursor,
                    )?)
                } else {
                    cursor
                }
            } else {
                cursor
            };
            let cursors = &mut state.cursors;
            cursors
                .get_mut(*new_cursor_id)
                .expect("cursor_id should be valid")
                .replace(Cursor::new_btree(cursor));
        }
        CursorType::BTreeIndex(_) => {
            return Err(LimboError::InternalError(
                "OpenDup is not supported for BTreeIndex".to_string(),
            )
            .into());
        }
        _ => {
            return Err(LimboError::InternalError(format!(
                "OpenDup is not supported for {cursor_type:?}"
            ))
            .into());
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Execute the [Insn::Once] instruction.
///
/// This instruction is used to execute a block of code only once.
/// If the instruction is executed again, it will jump to the target program counter.
pub fn op_once(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Once {
            target_pc_when_reentered,
        },
        insn
    );
    assert!(target_pc_when_reentered.is_offset());
    let offset = state.pc;
    if state.once.contains(&offset) {
        state.pc = target_pc_when_reentered.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }
    state.once.push(offset);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Execute the [Insn::ResetOnce] instruction.
///
/// Forgets that any [Insn::Once] between this instruction and `region_end` has
/// run, so a re-executed inlined trigger body re-evaluates its run-once blocks
/// instead of reusing a value cached during an earlier firing.
pub fn op_reset_once(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(ResetOnce { region_end }, insn);
    assert!(region_end.is_offset());
    let start = state.pc;
    let end = region_end.as_offset_int();
    state.once.retain(|pc| *pc <= start || *pc >= end);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_found(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    let (cursor_id, target_pc, record_reg, num_regs) = match insn {
        Insn::NotFound {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        } => (cursor_id, target_pc, record_reg, num_regs),
        Insn::Found {
            cursor_id,
            target_pc,
            record_reg,
            num_regs,
        } => (cursor_id, target_pc, record_reg, num_regs),
        _ => unreachable!("unexpected Insn {:?}", insn),
    };

    let not = matches!(insn, Insn::NotFound { .. });

    let record_source = if *num_regs == 0 {
        RecordSource::Packed {
            record_reg: *record_reg,
        }
    } else {
        RecordSource::Unpacked {
            start_reg: *record_reg,
            num_regs: *num_regs,
        }
    };
    let seek_result = match seek_internal(
        program,
        state,
        pager,
        record_source,
        *cursor_id,
        true,
        SeekOp::GE { eq_only: true },
    ) {
        Ok(SeekInternalResult::Found) => SeekResult::Found,
        Ok(SeekInternalResult::NotFound) => SeekResult::NotFound,
        Ok(SeekInternalResult::IO(io)) => return Ok(InsnFunctionStepResult::IO(io)),
        Err(e) => return Err(e.into()),
    };

    let found = matches!(seek_result, SeekResult::Found);
    let do_jump = (!found && not) || (found && !not);
    if do_jump {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }

    Ok(InsnFunctionStepResult::Step)
}

pub fn op_affinity(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Affinity {
            start_reg,
            count,
            affinities,
        },
        insn
    );

    if affinities.len() != count.get() {
        return Err(LimboError::InternalError(
            "Affinity: the length of affinities does not match the count".into(),
        )
        .into());
    }

    for (i, affinity_char) in affinities.chars().enumerate().take(count.get()) {
        let reg_index = *start_reg + i;

        let affinity = Affinity::from_char(affinity_char);

        apply_affinity_char(&mut state.registers[reg_index], affinity);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_count(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Count {
            cursor_id,
            target_reg,
            exact,
        },
        insn
    );

    let count = {
        let cursor = must_be_btree_cursor!(*cursor_id, program.cursor_ref, state, "Count");
        let cursor = cursor.as_btree_mut();
        return_if_io!(cursor.count())
    };

    state.registers[*target_reg].set_int(count as i64);

    // For optimized COUNT(*) queries, the count represents rows that would be read
    // SQLite tracks this differently (as pages read), but for consistency we track as rows
    if *exact {
        state.record_rows_read(count as u64);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Format integrity check errors into a result string.
/// Returns NULL when no errors were found.
fn format_integrity_check_result(errors: &[IntegrityCheckError]) -> Result<Option<String>> {
    if errors.is_empty() {
        Ok(None)
    } else {
        let errors: crate::alloc::Vec<_> = crate::with_btree_allocation_site!(
            IntegrityCheck,
            errors.iter().map(|error| error.to_string()).try_collect()
        )?;
        Ok(Some(errors.join("\n")))
    }
}

fn has_freelist_error(errors: &[IntegrityCheckError]) -> bool {
    errors.iter().any(|err| match err {
        IntegrityCheckError::FreelistTrunkCorrupt { .. }
        | IntegrityCheckError::FreelistPointerOutOfRange { .. } => true,
        IntegrityCheckError::PageReferencedMultipleTimes { page_category, .. } => {
            matches!(
                page_category,
                PageCategory::FreeListTrunk | PageCategory::FreePage
            )
        }
        _ => false,
    })
}

pub enum OpIntegrityCheckState {
    Start,
    CheckingBTreeStructure {
        errors: crate::alloc::Vec<IntegrityCheckError>,
        current_root_idx: usize,
        current_dropped_idx: usize,
        state: IntegrityCheckState,
    },
}

pub fn op_integrity_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IntegrityCk { data }, insn);
    let IntegrityCkData {
        db,
        max_errors,
        roots,
        dropped_roots,
        message_register,
    } = data.as_ref();

    let mv_store = program.connection.mv_store_for_db(*db);
    // Use the correct pager for the target database (main or attached)
    let target_pager = if *db == MAIN_DB_ID {
        pager.clone()
    } else {
        program.get_pager_from_database_index(db)?
    };
    // Passive MVCC: read page-1 freelist fields from the pager; the MVCC header can lag.
    let physical_header_store = mv_store
        .as_ref()
        .filter(|mv_store| !mv_store.uses_passive_checkpoint());
    match state.active_op_state.integrity_check() {
        OpIntegrityCheckState::Start => {
            let (freelist_trunk_page, db_size) = return_if_io!(with_header(
                &target_pager,
                physical_header_store,
                program,
                *db,
                |header| (header.freelist_trunk_page.get(), header.database_size.get())
            ));
            let mut errors: crate::alloc::Vec<_> = crate::alloc::vec![];
            let mut integrity_check_state = IntegrityCheckState::new(db_size as usize);
            let mut current_root_idx = 0;

            if freelist_trunk_page > 0 {
                let expected_freelist_count = return_if_io!(with_header(
                    &target_pager,
                    physical_header_store,
                    program,
                    *db,
                    |header| { header.freelist_pages.get() }
                ));
                integrity_check_state.set_expected_freelist_count(expected_freelist_count as usize);
                integrity_check_state.start(
                    freelist_trunk_page as i64,
                    PageCategory::FreeListTrunk,
                    &mut errors,
                )?;
            } else if !roots.is_empty() {
                integrity_check_state.start(roots[0], PageCategory::Normal, &mut errors)?;
                current_root_idx += 1;
            }

            *state.active_op_state.integrity_check() =
                OpIntegrityCheckState::CheckingBTreeStructure {
                    errors,
                    state: integrity_check_state,
                    current_root_idx,
                    current_dropped_idx: 0,
                };
        }
        OpIntegrityCheckState::CheckingBTreeStructure {
            errors,
            current_root_idx,
            current_dropped_idx,
            state: integrity_check_state,
        } => {
            return_if_io!(integrity_check(
                integrity_check_state,
                errors,
                &target_pager,
                mv_store.as_ref()
            ));

            if errors.len() >= *max_errors {
                errors.truncate(*max_errors);
                match format_integrity_check_result(errors)? {
                    Some(msg) => state.registers[*message_register].set_text(Text::new(msg))?,
                    None => state.registers[*message_register].set_null(),
                }
                state.active_op_state.clear();
                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }

            if *current_root_idx < roots.len() {
                integrity_check_state.start(
                    roots[*current_root_idx],
                    PageCategory::Normal,
                    errors,
                )?;
                *current_root_idx += 1;
                return Ok(InsnFunctionStepResult::Step);
            }

            while *current_dropped_idx < dropped_roots.len() {
                let dropped_root = dropped_roots[*current_dropped_idx];
                *current_dropped_idx += 1;
                if integrity_check_state
                    .page_reference
                    .contains_key(&dropped_root)
                {
                    continue;
                }
                integrity_check_state.start(dropped_root, PageCategory::Normal, errors)?;
                return Ok(InsnFunctionStepResult::Step);
            }

            if !has_freelist_error(errors)
                && integrity_check_state.freelist_count.actual_count
                    != integrity_check_state.freelist_count.expected_count
            {
                crate::with_btree_allocation_site!(
                    IntegrityCheck,
                    errors.try_push(IntegrityCheckError::FreelistCountMismatch {
                        actual_count: integrity_check_state.freelist_count.actual_count,
                        expected_count: integrity_check_state.freelist_count.expected_count,
                    })
                )?;
            }

            #[cfg(feature = "autovacuum")]
            let skip_page_never_used = !matches!(
                target_pager.get_auto_vacuum_mode(),
                crate::storage::pager::AutoVacuumMode::None
            );
            #[cfg(not(feature = "autovacuum"))]
            let skip_page_never_used = false;

            if !skip_page_never_used {
                for page_number in 2..=integrity_check_state.db_size {
                    if !integrity_check_state
                        .page_reference
                        .contains_key(&(page_number as i64))
                    {
                        if target_pager.pending_byte_page_id() != Some(page_number as u32) {
                            crate::with_btree_allocation_site!(
                                IntegrityCheck,
                                errors.try_push(IntegrityCheckError::PageNeverUsed {
                                    page_id: page_number as i64,
                                })
                            )?;
                        }
                    } else if target_pager.pending_byte_page_id() == Some(page_number as u32) {
                        crate::with_btree_allocation_site!(
                            IntegrityCheck,
                            errors.try_push(IntegrityCheckError::PendingBytePageUsed {
                                page_id: page_number as i64,
                            })
                        )?;
                    }

                    if errors.len() >= *max_errors {
                        break;
                    }
                }
            }

            errors.truncate(*max_errors);
            match format_integrity_check_result(errors)? {
                Some(msg) => state.registers[*message_register].set_text(Text::new(msg))?,
                None => state.registers[*message_register].set_null(),
            }
            state.active_op_state.clear();
            state.pc += 1;
        }
    }

    Ok(InsnFunctionStepResult::Step)
}
pub fn op_cast(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Cast { reg, affinity }, insn);

    let value = state.registers[*reg].get_value().clone();
    let result = match affinity {
        Affinity::Blob | Affinity::None => value.exec_cast("BLOB"),
        Affinity::Text => value.exec_cast("TEXT"),
        Affinity::Numeric => value.exec_cast("NUMERIC"),
        Affinity::Integer => value.exec_cast("INTEGER"),
        Affinity::Real => value.exec_cast("REAL"),
    }?;

    state.registers[*reg].set_value(result);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Regenerate trigger SQL from its in-memory fields to keep trigger.sql
/// in sync after table/column renames.
fn regenerate_trigger_sql(trigger: &crate::schema::Trigger) -> String {
    use crate::translate::trigger::create_trigger_to_sql;

    let trigger_name = QualifiedName {
        db_name: None,
        name: Name::from_string(&trigger.name),
        alias: None,
    };
    let tbl_name = QualifiedName {
        db_name: None,
        name: Name::from_string(&trigger.table_name),
        alias: None,
    };
    create_trigger_to_sql(
        trigger.temporary,
        false, // IF NOT EXISTS is stripped from stored schema SQL
        &trigger_name,
        Some(trigger.time),
        &trigger.event,
        &tbl_name,
        trigger.for_each_row,
        trigger.when_clause.as_ref(),
        &trigger.commands,
    )
}

fn sql_might_reference_identifier(sql: &str, ident: &str) -> bool {
    if ident.is_empty() {
        return true;
    }

    let ident = ident.as_bytes();
    sql.as_bytes()
        .windows(ident.len())
        .any(|window| window.eq_ignore_ascii_case(ident))
}

fn with_relevant_trigger_schemas_mut(
    conn: &Connection,
    table_db_id: usize,
    mut f: impl FnMut(&mut Schema) -> crate::Result<()>,
) -> crate::Result<()> {
    conn.with_database_schema_mut(table_db_id, |schema| f(schema))??;
    if table_db_id != crate::TEMP_DB_ID && conn.temp.database.read().is_some() {
        conn.with_database_schema_mut(crate::TEMP_DB_ID, |schema| f(schema))??;
    }
    Ok(())
}

fn rewrite_trigger_for_table_rename(
    trigger: &mut crate::schema::Trigger,
    normalized_from: &str,
    normalized_to: &str,
) {
    let old_sql = trigger.sql.clone();
    for cmd in &mut trigger.commands {
        rewrite_trigger_cmd_table_refs(cmd, normalized_from, normalized_to);
    }
    if let Some(ref mut when) = trigger.when_clause {
        rewrite_check_expr_table_refs(when, normalized_from, normalized_to);
    }
    let new_sql = regenerate_trigger_sql(trigger);
    if new_sql != old_sql {
        trigger.sql = new_sql;
    }
}

fn rewrite_trigger_for_column_rename(
    trigger: &mut crate::schema::Trigger,
    table_name: &str,
    old_col: &str,
    new_col: &str,
) -> crate::Result<()> {
    let trigger_tbl = normalize_ident(&trigger.table_name);
    let old_sql = trigger.sql.clone();
    if let Some(ref mut when) = trigger.when_clause {
        rename_identifiers_scoped_when_clause(when, table_name, &trigger_tbl, old_col, new_col);
    }
    if trigger_tbl == table_name {
        if let ast::TriggerEvent::UpdateOf(ref mut cols) = trigger.event {
            for col in cols {
                if normalize_ident(col.as_str()) == normalize_ident(old_col) {
                    *col = ast::Name::exact(new_col.to_owned());
                }
            }
        }
    }
    for cmd in &mut trigger.commands {
        rewrite_trigger_cmd_column_refs(cmd, table_name, &trigger_tbl, old_col, new_col);
    }
    if trigger_still_references_renamed_column(trigger, table_name, old_col) {
        return Err(LimboError::ParseError(format!(
            "error in trigger {} after rename: no such column: {}",
            trigger.name, old_col
        )));
    }
    // Keep trigger.sql in sync with the rewritten in-memory AST so a subsequent
    // RENAME COLUMN sees the current column name (the translate step uses the
    // in-memory trigger.sql as the source of the next rewrite).
    let new_sql = regenerate_trigger_sql(trigger);
    if new_sql != old_sql {
        trigger.sql = new_sql;
    }
    Ok(())
}

pub fn op_rename_table(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(RenameTable { db, from, to }, insn);

    let normalized_from = normalize_ident(from.as_str());
    let normalized_to = normalize_ident(to.as_str());

    let conn = program.connection.clone();

    conn.with_database_schema_mut(*db, |schema| -> crate::Result<()> {
        if let Some(mut indexes) = schema.indexes.remove(&normalized_from) {
            let autoindex_prefix = format!("sqlite_autoindex_{normalized_from}_");
            indexes.iter_mut().for_each(|index| {
                let index = Arc::make_mut(index);
                normalized_to.clone_into(&mut index.table_name);
                // Rename autoindexes to match the new table name
                if let Some(suffix) = index.name.strip_prefix(&autoindex_prefix) {
                    index.name = format!("sqlite_autoindex_{normalized_to}_{suffix}");
                }
            });

            schema.indexes.insert(normalized_to.to_owned(), indexes);
        };

        let mut table = schema
            .tables
            .remove(&normalized_from)
            .expect("table being renamed should be in schema");
        #[cfg(feature = "conn_raw_api")]
        schema.unregister_table_root_page(table.as_ref());
        match Arc::make_mut(&mut table) {
            Table::BTree(btree) => {
                let btree = Arc::make_mut(btree);
                // update this table's own foreign keys
                for fk_arc in &mut btree.foreign_keys {
                    let fk = Arc::make_mut(fk_arc);
                    if normalize_ident(&fk.parent_table) == normalized_from {
                        fk.parent_table.clone_from(&normalized_to);
                    }
                }

                // Rewrite table-qualified refs in CHECK constraints
                for check in &mut btree.check_constraints {
                    rewrite_check_expr_table_refs(
                        &mut check.expr,
                        &normalized_from,
                        &normalized_to,
                    );
                    // The captured source text no longer matches the
                    // rewritten expression.
                    check.source = None;
                }

                normalized_to.clone_into(&mut btree.name);
            }
            Table::Virtual(vtab) => {
                Arc::make_mut(vtab).name.clone_from(&normalized_to);
            }
            _ => panic!("only btree and virtual tables can be renamed"),
        }

        #[cfg(feature = "conn_raw_api")]
        schema.register_table_root_page(&normalized_to, table.as_ref());
        schema.tables.insert(normalized_to.to_owned(), table);

        for (tname, t_arc) in schema.tables.iter_mut() {
            // skip the table we just renamed
            if normalize_ident(tname) == normalized_to {
                continue;
            }
            if let Table::BTree(ref mut child_btree_arc) = Arc::make_mut(t_arc) {
                let child_btree = Arc::make_mut(child_btree_arc);
                for fk_arc in &mut child_btree.foreign_keys {
                    if normalize_ident(&fk_arc.parent_table) == normalized_from {
                        let fk = Arc::make_mut(fk_arc);
                        fk.parent_table.clone_from(&normalized_to);
                    }
                }
            }
        }

        // Update triggers: move from old table name key to new, and update
        // each trigger's table_name field and body commands.
        if let Some(mut triggers) = schema.triggers.remove(&normalized_from) {
            for trigger_arc in &mut triggers {
                let trigger = Arc::make_mut(trigger_arc);
                normalized_to.clone_into(&mut trigger.table_name);
                rewrite_trigger_for_table_rename(trigger, &normalized_from, &normalized_to);
            }
            schema.triggers.insert(normalized_to.to_owned(), triggers);
        }

        // If the renamed table owned an implicit AUTOINCREMENT sequence,
        // the sequence's name (`__turso_internal_autoincrement_<old>`) and
        // its backing table's name (`__turso_internal_seq_<…>`) are both
        // derived from the parent table. The bytecode-side rewrite in
        // `emit_rename_autoincrement_backing_table_entry` already updated
        // the persistent sqlite_schema row; here we mirror the rename in
        // the in-memory `schema.sequences` and `schema.tables` maps so a
        // subsequent INSERT under MVCC can find the sequence via
        // `autoincrement_sequence_name(<new>)`. Without this, every
        // INSERT into the renamed table would error with "missing
        // implicit sequence for AUTOINCREMENT table" until the next
        // schema reparse.
        let old_seq_name = crate::schema::autoincrement_sequence_name(&normalized_from);
        if let Some(mut seq_arc) = schema.sequences.remove(&old_seq_name) {
            let new_seq_name = crate::schema::autoincrement_sequence_name(&normalized_to);
            // Mutate the Sequence's `name` field so its internal identity
            // tracks the new table. `seq.name` is used by the exhaustion
            // error message in `op_sequence_compute_next` and by the
            // `AUTOINCREMENT_SEQ_PREFIX` strip-check in
            // `emit_autoincrement_sqlite_sequence_sync` — keeping it in
            // sync with the map key avoids a misleading error and a
            // missed sqlite_sequence mirror.
            Arc::make_mut(&mut seq_arc).name.clone_from(&new_seq_name);
            let old_backing_table =
                crate::translate::sequence::sequence_backing_table_name(&old_seq_name);
            let new_backing_table =
                crate::translate::sequence::sequence_backing_table_name(&new_seq_name);
            schema.sequences.insert(new_seq_name, seq_arc);
            if let Some(mut backing_arc) = schema.tables.remove(&old_backing_table) {
                if let Table::BTree(btree_arc) = Arc::make_mut(&mut backing_arc) {
                    Arc::make_mut(btree_arc).name.clone_from(&new_backing_table);
                }
                schema.tables.insert(new_backing_table, backing_arc);
            }
        }

        // Also update triggers on OTHER tables that reference the renamed table
        // in their body commands (e.g., INSERT INTO old_name in a trigger on another table)
        for (_, triggers) in schema.triggers.iter_mut() {
            for trigger_arc in triggers.iter_mut() {
                if !sql_might_reference_identifier(&trigger_arc.sql, &normalized_from) {
                    continue;
                }
                let trigger = Arc::make_mut(trigger_arc);
                rewrite_trigger_for_table_rename(trigger, &normalized_from, &normalized_to);
            }
        }

        Ok(())
    })??;

    if *db != crate::TEMP_DB_ID && conn.temp.database.read().is_some() {
        conn.with_database_schema_mut(crate::TEMP_DB_ID, |schema| -> crate::Result<()> {
            // A TEMP trigger may run on a table in another database. Triggers
            // are found by the name of that table. When the table is renamed,
            // move its TEMP triggers to the new name. Changing only the stored
            // SQL would leave them under the old name, so they would no longer
            // run.
            let temp_shadows_old_name = schema.tables.contains_key(&normalized_from);
            if let Some(triggers) = schema.triggers.remove(&normalized_from) {
                let mut triggers_to_keep = std::collections::VecDeque::new();
                let mut triggers_to_rename = std::collections::VecDeque::new();
                for mut trigger_arc in triggers {
                    let targets_renamed_database = match trigger_arc.target_database_id {
                        Some(target_db) => target_db == *db,
                        None => !temp_shadows_old_name,
                    };
                    if targets_renamed_database {
                        let trigger = Arc::make_mut(&mut trigger_arc);
                        normalized_to.clone_into(&mut trigger.table_name);
                        rewrite_trigger_for_table_rename(trigger, &normalized_from, &normalized_to);
                        triggers_to_rename.push_back(trigger_arc);
                    } else {
                        triggers_to_keep.push_back(trigger_arc);
                    }
                }
                if !triggers_to_keep.is_empty() {
                    schema
                        .triggers
                        .insert(normalized_from.to_owned(), triggers_to_keep);
                }
                schema
                    .triggers
                    .entry(normalized_to.to_owned())
                    .or_default()
                    .extend(triggers_to_rename);
            }

            for triggers in schema.triggers.values_mut() {
                for trigger_arc in triggers.iter_mut() {
                    if !sql_might_reference_identifier(&trigger_arc.sql, &normalized_from) {
                        continue;
                    }
                    let trigger = Arc::make_mut(trigger_arc);
                    rewrite_trigger_for_table_rename(trigger, &normalized_from, &normalized_to);
                }
            }
            Ok(())
        })??;
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_drop_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        DropColumn {
            db,
            table,
            column_index
        },
        insn
    );

    let conn = program.connection.clone();

    let normalized_table_name = normalize_ident(table.as_str());

    let column_name = conn.with_schema(*db, |schema| {
        let table = schema
            .tables
            .get(&normalized_table_name)
            .expect("table being ALTERed should be in schema");
        table
            .get_column_at(*column_index)
            .expect("column being ALTERed should be in schema")
            .name
            .as_ref()
            .expect("column being ALTERed should be named")
            .clone()
    });

    conn.with_database_schema_mut(*db, |schema| -> Result<()> {
        let table = schema
            .tables
            .get_mut(&normalized_table_name)
            .expect("table being renamed should be in schema");

        let table = Arc::get_mut(table).expect("this should be the only strong reference");

        let Table::BTree(btree) = table else {
            panic!("only btree tables can be renamed");
        };

        let btree = Arc::make_mut(btree);
        btree.columns_mut().remove(*column_index);
        // Remove column-level CHECK constraints for the dropped column
        let col_name = column_name.clone();
        btree.check_constraints.retain(|c| {
            c.column
                .as_ref()
                .is_none_or(|col| normalize_ident(col) != normalize_ident(&col_name))
        });

        btree.shift_generated_column_indices_after_drop(*column_index)?;
        Ok(())
    })??;

    conn.with_schema(*db, |schema| -> crate::Result<()> {
        if let Some(indexes) = schema.indexes.get(&normalized_table_name) {
            for index in indexes {
                if index
                    .columns
                    .iter()
                    .any(|column| column.pos_in_table == *column_index)
                {
                    return Err(LimboError::ParseError(format!(
                        "cannot drop column \"{column_name}\": indexed"
                    )));
                }
            }
        }
        Ok(())
    })?;

    // Shift left pos_in_table in all indexes, and self-table placeholders in generated column
    // expressions, to account for the dropped column. For example, if the dropped column had index
    // 2, then anything that was indexed on column 3 or higher should be decremented by 1.
    conn.with_database_schema_mut(*db, |schema| -> Result<()> {
        if let Some(indexes) = schema.indexes.get_mut(&normalized_table_name) {
            for index in indexes {
                let index = Arc::get_mut(index).expect("this should be the only strong reference");
                for index_column in index.columns.iter_mut() {
                    if index_column.pos_in_table != EXPR_INDEX_SENTINEL
                        && index_column.pos_in_table > *column_index
                    {
                        index_column.pos_in_table -= 1;
                    }
                    if let Some(ref mut expr) = index_column.expr {
                        crate::translate::expr::walk_expr_mut(expr, &mut |e| {
                            if let ast::Expr::Column {
                                table, column: c, ..
                            } = e
                            {
                                if table.is_self_table() && *c > *column_index {
                                    *c -= 1;
                                }
                            }
                            Ok(crate::translate::expr::WalkControl::Continue)
                        })?;
                    }
                }
            }
        }
        Ok(())
    })??;

    conn.with_schema(*db, |schema| -> crate::Result<()> {
        for (view_name, view) in schema.views.iter() {
            let view_select_sql = format!("SELECT * FROM {view_name}");
            let _ = conn.prepare(view_select_sql.as_str()).map_err(|e| {
                LimboError::ParseError(format!(
                    "cannot drop column \"{}\": referenced in VIEW {view_name}: {}. {e}",
                    column_name, view.sql,
                ))
            })?;
        }
        Ok(())
    })?;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_add_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(AddColumn { data }, insn);

    let conn = program.connection.clone();
    let normalized_table_name = normalize_ident(data.table.as_str());
    let new_check_constraints = data.check_constraints.try_to_vec()?;
    let new_foreign_keys = data.foreign_keys.try_to_vec()?;

    conn.with_database_schema_mut(data.db, |schema| -> Result<()> {
        let table_ref = schema
            .tables
            .get_mut(&normalized_table_name)
            .expect("table being altered should be in schema");

        let table_ref = Arc::make_mut(table_ref);

        let crate::schema::Table::BTree(btree) = table_ref else {
            panic!("only btree tables can have columns added");
        };

        let btree = Arc::make_mut(btree);
        btree.columns_mut().try_push(data.column.clone())?;
        // Update CHECK constraints to include any constraints from the new column
        btree.check_constraints = new_check_constraints;
        // Update foreign keys to include any FK constraints from the new column
        btree.foreign_keys = new_foreign_keys;

        // Resolve generated column expressions and update virtual column metadata
        btree.prepare_generated_columns()?;
        Ok(())
    })??;

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_alter_column(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        AlterColumn {
            db,
            table: table_name,
            column_index,
            definition,
            rename,
        },
        insn
    );

    let conn = program.connection.clone();

    let normalized_table_name = normalize_ident(table_name.as_str());
    let old_column_name = conn.with_schema(*db, |schema| {
        let table = schema
            .tables
            .get(&normalized_table_name)
            .expect("table being ALTERed should be in schema");
        table
            .get_column_at(*column_index)
            .expect("column being ALTERed should be in schema")
            .name
            .as_ref()
            .expect("column being ALTERed should be named")
            .clone()
    });
    let new_column = crate::schema::Column::try_from(definition.as_ref())?;
    let new_name = definition.col_name.as_str().to_owned();

    let view_rewrites: Vec<(usize, String, RewrittenView)> = if *rename {
        let target_db_name = conn.get_database_name_by_index(*db).ok_or_else(|| {
            LimboError::InternalError(format!("unknown database id {} during ALTER TABLE", *db))
        })?;
        let mut all_rewrites = conn.with_schema(
            *db,
            |schema| -> crate::Result<Vec<(usize, String, RewrittenView)>> {
                let mut rewrites = Vec::new();
                for (view_name, view) in schema.views.iter() {
                    if let Some(rewritten) = rewrite_view_sql_for_column_rename(
                        &view.sql,
                        schema,
                        table_name.as_str(),
                        &target_db_name,
                        &old_column_name,
                        &new_name,
                    )? {
                        rewrites.push((*db, view_name.clone(), rewritten));
                    }
                }
                Ok(rewrites)
            },
        )?;
        // Also search temp schema for views referencing the renamed column.
        if *db != crate::TEMP_DB_ID {
            let temp_rewrites = conn.with_schema(
                crate::TEMP_DB_ID,
                |schema| -> crate::Result<Vec<(usize, String, RewrittenView)>> {
                    let mut rewrites = Vec::new();
                    for (view_name, view) in schema.views.iter() {
                        if let Some(rewritten) = rewrite_view_sql_for_column_rename(
                            &view.sql,
                            schema,
                            table_name.as_str(),
                            &target_db_name,
                            &old_column_name,
                            &new_name,
                        )? {
                            rewrites.push((crate::TEMP_DB_ID, view_name.clone(), rewritten));
                        }
                    }
                    Ok(rewrites)
                },
            )?;
            all_rewrites.extend(temp_rewrites);
        }
        all_rewrites
    } else {
        Vec::new()
    };

    conn.with_database_schema_mut(*db, |schema| -> Result<()> {
        let table_arc = schema
            .tables
            .get_mut(&normalized_table_name)
            .expect("table being ALTERed should be in schema");
        let table = Arc::make_mut(table_arc);

        let Table::BTree(ref mut btree_arc) = table else {
            panic!("only btree tables can be altered");
        };
        let btree = Arc::make_mut(btree_arc);
        let clears_autoincrement_sequence = !*rename
            && btree.has_autoincrement
            && btree
                .columns()
                .get(*column_index)
                .is_some_and(|column| column.is_rowid_alias())
            && !new_column.is_rowid_alias();

        if *rename {
            btree.columns_mut()[*column_index].name = Some(new_name.clone());

            // Refresh the cached sql in generated columns
            let column_count = btree.columns().len();
            for i in 0..column_count {
                let cols_view = btree.columns();
                if let Some(new_sql) = cols_view[i]
                    .generated_expr()
                    .map(|expr| render_gencol_expr_sql_with_new_names(expr, cols_view))
                    .transpose()?
                {
                    btree.columns_mut()[i].set_generated_original_sql(new_sql)
                }
            }
        } else {
            btree.columns_mut()[*column_index] = new_column.clone();
        }

        btree.prepare_generated_columns()?;

        // Update this table's indexes that reference the old column.
        if let Some(idxs) = schema.indexes.get_mut(&normalized_table_name) {
            for idx in idxs {
                let idx = Arc::make_mut(idx);
                for ic in &mut idx.columns {
                    if let Some(expr) = &mut ic.expr {
                        rename_identifiers(expr.as_mut(), &old_column_name, &new_name);
                        if ic.pos_in_table == crate::schema::EXPR_INDEX_SENTINEL {
                            ic.name = expr.to_string();
                        }
                    }
                    if ic.pos_in_table != crate::schema::EXPR_INDEX_SENTINEL {
                        let table_column = btree
                            .columns()
                            .get(ic.pos_in_table)
                            .expect("indexed column position should exist");
                        if let Some(name) = &table_column.name {
                            ic.name.clone_from(name);
                        }
                        ic.default.clone_from(&table_column.default);
                        ic.expr = table_column
                            .generated_expr()
                            .map(|expr| Box::new(expr.clone()));
                    }
                }
                // Update partial index WHERE clause column references
                if let Some(ref mut wc) = idx.where_clause {
                    rename_identifiers(wc, &old_column_name, &new_name);
                }
            }
        }

        // Keep primary_key_columns consistent (names may change on rename)
        for (pk_name, _ord) in &mut btree.primary_key_columns {
            if pk_name.eq_ignore_ascii_case(&old_column_name) {
                pk_name.clone_from(&new_name);
            }
        }

        // Update unique_sets to reflect the renamed column
        for unique_set in &mut btree.unique_sets {
            for unique_column in &mut unique_set.columns {
                if unique_column.name.eq_ignore_ascii_case(&old_column_name) {
                    unique_column.name.clone_from(&new_name);
                }
            }
        }

        // Update CHECK constraint expressions to reference the new column name
        let old_col_normalized = normalize_ident(&old_column_name);
        for check in &mut btree.check_constraints {
            rename_identifiers(&mut check.expr, &old_col_normalized, &new_name);
            // The captured source text no longer matches the rewritten
            // expression.
            check.source = None;
            if let Some(ref mut col) = check.column {
                if col.eq_ignore_ascii_case(&old_column_name) {
                    col.clone_from(&new_name);
                }
            }
        }

        // Maintain rowid-alias bit after change/rename (INTEGER PRIMARY KEY)
        if !*rename {
            // recompute alias from `new_column`
            btree.columns_mut()[*column_index].set_rowid_alias(new_column.is_rowid_alias());
            if clears_autoincrement_sequence {
                btree.has_autoincrement = false;
            }
        }

        // Update this table's OWN foreign keys
        for fk_arc in &mut btree.foreign_keys {
            let fk = Arc::make_mut(fk_arc);
            // child side: rename child column if it matches
            for cc in &mut fk.child_columns {
                if cc.eq_ignore_ascii_case(&old_column_name) {
                    cc.clone_from(&new_name);
                }
            }
            // parent side: if self-referencing, rename parent column too
            if normalize_ident(&fk.parent_table) == normalized_table_name {
                for pc in &mut fk.parent_columns {
                    if pc.eq_ignore_ascii_case(&old_column_name) {
                        pc.clone_from(&new_name);
                    }
                }
            }
        }

        // fix OTHER tables that reference this table as parent
        for (tname, t_arc) in schema.tables.iter_mut() {
            if normalize_ident(tname) == normalized_table_name {
                continue;
            }
            if let Table::BTree(ref mut child_btree_arc) = Arc::make_mut(t_arc) {
                let child_btree = Arc::make_mut(child_btree_arc);
                for fk_arc in &mut child_btree.foreign_keys {
                    if normalize_ident(&fk_arc.parent_table) != normalized_table_name {
                        continue;
                    }
                    let fk = Arc::make_mut(fk_arc);
                    for pc in &mut fk.parent_columns {
                        if pc.eq_ignore_ascii_case(&old_column_name) {
                            pc.clone_from(&new_name);
                        }
                    }
                }
            }
        }
        Ok(())
    })??;

    if *rename {
        // Update in-memory trigger objects for the renamed column in both the
        // altered schema and temp, since temp triggers may reference main/attached tables.
        with_relevant_trigger_schemas_mut(&conn, *db, |schema| {
            for triggers in schema.triggers.values_mut() {
                for trigger_arc in triggers.iter_mut() {
                    let trigger = Arc::make_mut(trigger_arc);
                    rewrite_trigger_for_column_rename(
                        trigger,
                        &normalized_table_name,
                        &old_column_name,
                        &new_name,
                    )?;
                }
            }
            Ok(())
        })?;

        for (view_db, view_name, rewritten) in view_rewrites {
            conn.with_database_schema_mut(view_db, move |schema| -> crate::Result<()> {
                if let Some(view_arc) = schema.views.get_mut(&view_name) {
                    let view = Arc::make_mut(view_arc);
                    view.sql = rewritten.sql;
                    view.select_stmt = rewritten.select_stmt;
                    view.columns = rewritten.columns;
                }
                Ok(())
            })??;
        }

        if *db != crate::MAIN_DB_ID {
            conn.with_schema(*db, |schema| -> crate::Result<()> {
                let table = schema
                    .tables
                    .get(&normalized_table_name)
                    .expect("table being ALTERed should be in schema");
                let _column = table
                    .get_column_at(*column_index)
                    .expect("column being ALTERed should be in schema");
                for (view_name, view) in schema.views.iter() {
                    let view_select_sql = format!("SELECT * FROM {view_name}");
                    let _ = conn.prepare(view_select_sql.as_str()).map_err(|e| {
                        LimboError::ParseError(format!(
                            "cannot rename column \"{}\": referenced in VIEW {view_name}: {}. {e}",
                            old_column_name, view.sql,
                        ))
                    })?;
                }
                Ok(())
            })?;
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_if_neg(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(IfNeg { reg, target_pc }, insn);

    match &state.registers[*reg] {
        Register::Value(Value::Numeric(Numeric::Integer(i))) if *i < 0 => {
            state.pc = target_pc.as_offset_int();
        }
        Register::Value(Value::Numeric(Numeric::Float(f))) if f64::from(*f) < 0.0 => {
            state.pc = target_pc.as_offset_int();
        }
        Register::Value(Value::Null) => {
            state.pc += 1;
        }
        _ => {
            state.pc += 1;
        }
    }

    Ok(InsnFunctionStepResult::Step)
}

fn handle_text_sum(
    acc: &mut Value,
    sum_state: &mut SumAggState,
    parsed_number: ParsedNumber,
    parse_result: NumericParseResult,
    force_approx: bool,
) {
    // SQLite treats text that only partially parses as numeric (ValidPrefixOnly)
    // as approximate, so SUM returns real instead of integer. Non-integer inputs
    // (e.g. BLOB) should also force approximate results.
    let is_approx = force_approx || matches!(parse_result, NumericParseResult::ValidPrefixOnly);
    match parsed_number {
        ParsedNumber::Integer(i) => {
            if is_approx {
                sum_state.approx = true;
                match acc {
                    Value::Null => {
                        *acc = Value::from_f64(i as f64);
                    }
                    Value::Numeric(Numeric::Integer(acc_i)) => {
                        *acc = Value::from_f64(*acc_i as f64);
                        apply_kbn_step_int(acc, i, sum_state);
                    }
                    Value::Numeric(Numeric::Float(_)) => {
                        apply_kbn_step_int(acc, i, sum_state);
                    }
                    _ => unreachable!(),
                }
            } else {
                match acc {
                    Value::Null => {
                        *acc = Value::from_i64(i);
                    }
                    Value::Numeric(Numeric::Integer(acc_i)) => match acc_i.checked_add(i) {
                        Some(sum) => *acc = Value::from_i64(sum),
                        None => {
                            let acc_f = *acc_i as f64;
                            *acc = Value::from_f64(acc_f);
                            sum_state.approx = true;
                            sum_state.ovrfl = true;
                            apply_kbn_step_int(acc, i, sum_state);
                        }
                    },
                    Value::Numeric(Numeric::Float(_)) => {
                        apply_kbn_step_int(acc, i, sum_state);
                    }
                    _ => unreachable!(),
                }
            }
        }
        ParsedNumber::Float(f) => {
            if !sum_state.approx {
                if let Value::Numeric(Numeric::Integer(current_sum)) = *acc {
                    *acc = Value::from_f64(current_sum as f64);
                } else if matches!(*acc, Value::Null) {
                    *acc = Value::from_f64(0.0);
                }
                sum_state.approx = true;
            }
            apply_kbn_step(acc, f, sum_state);
        }
        ParsedNumber::None => {
            if !sum_state.approx {
                if let Value::Numeric(Numeric::Integer(current_sum)) = *acc {
                    *acc = Value::from_f64(current_sum as f64);
                } else if matches!(*acc, Value::Null) {
                    *acc = Value::from_f64(0.0);
                }
                sum_state.approx = true;
            }
        }
    }
}

pub fn op_fk_counter(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        FkCounter {
            increment_value,
            deferred,
        },
        insn
    );
    if !*deferred {
        state.increment_fk_immediate_violations_during_stmt(*increment_value);
    } else {
        // Transaction-level counter: add/subtract for deferred FKs.
        program
            .connection
            .increment_deferred_foreign_key_violations(*increment_value);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_fk_if_zero(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        FkIfZero {
            deferred,
            target_pc,
        },
        insn
    );
    let fk_enabled = program.connection.foreign_keys_enabled();

    // Jump if any:
    // Foreign keys are disabled globally
    // p1 is true AND deferred constraint counter is zero
    // p1 is false AND deferred constraint counter is non-zero
    if !fk_enabled {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }
    let v = if *deferred {
        program.connection.get_deferred_foreign_key_violations()
    } else {
        state.get_fk_immediate_violations_during_stmt()
    };

    state.pc = if v == 0 {
        target_pc.as_offset_int()
    } else {
        state.pc + 1
    };
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_fk_check(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(FkCheck { deferred }, insn);
    if !program.connection.foreign_keys_enabled() {
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    }
    let v = if *deferred {
        program.connection.get_deferred_foreign_key_violations()
    } else {
        state.get_fk_immediate_violations_during_stmt()
    };
    if v > 0 {
        return Err(LimboError::ForeignKeyConstraint(
            "immediate foreign key constraint failed".to_string(),
        )
        .into());
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_build(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashBuild { data }, insn);

    let mut op_state = match state.active_op_state.hash_build().take().filter(|s| {
        s.hash_table_id == data.hash_table_id
            && s.cursor_id == data.cursor_id
            && s.key_start_reg == data.key_start_reg
            && s.num_keys == data.num_keys
    }) {
        Some(op_state) => op_state,
        None => OpHashBuildState {
            key_values: crate::alloc::Vec::try_with_capacity_ext(data.num_keys)?,
            key_idx: 0,
            payload_values: crate::alloc::Vec::try_with_capacity_ext(data.num_payload)?,
            payload_idx: 0,
            rowid: None,
            cursor_id: data.cursor_id,
            hash_table_id: data.hash_table_id,
            key_start_reg: data.key_start_reg,
            num_keys: data.num_keys,
        },
    };

    // Create hash table if it doesn't exist yet
    let temp_store = program.connection.get_temp_store();
    // When temp_store=memory, disable the memory limit entirely to avoid spilling.
    // Spilling to an in-memory file has serialization overhead - simpler to never spill.
    let mem_budget = if matches!(temp_store, crate::TempStore::Memory) {
        usize::MAX
    } else {
        data.mem_budget
    };
    if let std::collections::hash_map::Entry::Vacant(e) =
        state.hash_tables.entry(data.hash_table_id)
    {
        let config = HashTableConfig {
            initial_buckets: 1024,
            mem_budget,
            num_keys: data.num_keys,
            collations: data.collations.try_to_vec()?,
            temp_store,
            track_matched: data.track_matched,
            partition_count: None,
        };
        e.insert(HashTable::new(config, pager.io.clone())?);
    }

    // Read pre-computed key values directly from registers
    while op_state.key_idx < data.num_keys {
        let i = op_state.key_idx;
        let reg = &state.registers[data.key_start_reg + i];
        let value = reg.get_value().clone();
        // The resume state preallocates space for every key register before the loop starts.
        op_state.key_values.push(value);
        op_state.key_idx += 1;
    }

    // Read payload values from registers if provided
    if let Some(payload_reg) = data.payload_start_reg {
        while op_state.payload_idx < data.num_payload {
            let i = op_state.payload_idx;
            let reg = &state.registers[payload_reg + i];
            let value = reg.get_value().clone();
            // The resume state preallocates space for every payload register before the loop starts.
            op_state.payload_values.push(value);
            op_state.payload_idx += 1;
        }
    }

    // Get the rowid from the cursor
    if op_state.rowid.is_none() {
        let cursor = state.get_cursor(data.cursor_id);
        let rowid_val = match cursor {
            Cursor::BTree(btree_cursor) => {
                let rowid_opt = match btree_cursor.rowid() {
                    Ok(IOResult::Done(v)) => v,
                    Ok(IOResult::IO(io)) => {
                        *state.active_op_state.hash_build() = Some(op_state);
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                    Err(e) => {
                        *state.active_op_state.hash_build() = Some(op_state);
                        return Err(e);
                    }
                };
                rowid_opt.ok_or_else(|| {
                    LimboError::InternalError("HashBuild: cursor has no rowid".to_string())
                })?
            }
            _ => {
                return Err(LimboError::InternalError(
                    "HashBuild: unsupported cursor type".to_string(),
                )
                .into());
            }
        };
        op_state.rowid = Some(rowid_val);
    }

    // Insert the rowid into the hash table
    if let Some(ht) = state.hash_tables.get_mut(&data.hash_table_id) {
        let rowid = op_state.rowid.expect("rowid set");
        let pending = PendingHashInsert {
            key_values: std::mem::replace(&mut op_state.key_values, crate::alloc::vec![]),
            rowid,
            payload_values: std::mem::replace(&mut op_state.payload_values, crate::alloc::vec![]),
        };
        match ht.insert_pending(pending, Some(&mut state.metrics.hash_join))? {
            HashInsertResult::Done => {}
            HashInsertResult::IO { io, pending } => {
                op_state.key_values = pending.key_values;
                op_state.payload_values = pending.payload_values;
                *state.active_op_state.hash_build() = Some(op_state);
                return Ok(InsnFunctionStepResult::IO(io));
            }
        }
    }

    state.active_op_state.clear();
    state.record_rows_read(1);
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_distinct(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashDistinct { data }, insn);

    let temp_store = program.connection.get_temp_store();
    // When temp_store=memory, disable the memory limit entirely to avoid spilling.
    let mem_budget = if matches!(temp_store, crate::TempStore::Memory) {
        usize::MAX
    } else {
        DEFAULT_MEM_BUDGET
    };
    if let std::collections::hash_map::Entry::Vacant(e) =
        state.hash_tables.entry(data.hash_table_id)
    {
        let config = HashTableConfig {
            initial_buckets: 1024,
            mem_budget,
            num_keys: data.num_keys,
            collations: data.collations.try_to_vec()?,
            temp_store,
            track_matched: false,
            partition_count: None,
        };
        e.insert(HashTable::new(config, pager.io.clone())?);
    }
    let hash_table = state
        .hash_tables
        .get_mut(&data.hash_table_id)
        .expect("hash table exists");

    // Stage the key values in a per-statement scratch Vec; try_clone_from
    // reuses each slot's allocation across rows.
    let key_values = &mut state.distinct_key_values;
    key_values.truncate(data.num_keys);
    for (i, dst) in key_values.iter_mut().enumerate() {
        dst.try_clone_from(state.registers[data.key_start_reg + i].get_value())?;
    }
    // Clone new values
    if key_values.len() < data.num_keys {
        key_values.try_reserve(data.num_keys)?;
        for i in key_values.len()..data.num_keys {
            key_values
                .push_within_capacity(
                    state.registers[data.key_start_reg + i]
                        .get_value()
                        .try_clone()?,
                )
                .unwrap();
        }
    }

    let mut key_refs: SmallVec<[ValueRef; 2]> = SmallVec::with_capacity(data.num_keys);
    key_refs.extend(key_values.iter().map(|v| v.as_ref()));
    match hash_table.insert_distinct(key_values, &key_refs, Some(&mut state.metrics.hash_join))? {
        IOResult::Done(inserted) => {
            state.pc = if inserted {
                state.pc + 1
            } else {
                data.target_pc.as_offset_int()
            };
            Ok(InsnFunctionStepResult::Step)
        }
        IOResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
    }
}

pub fn op_hash_build_finalize(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashBuildFinalize { hash_table_id }, insn);
    if let Some(ht) = state.hash_tables.get_mut(hash_table_id) {
        // Finalize the build phase, may flush remaining partitions to disk if spilled
        match ht.finalize_build(Some(&mut state.metrics.hash_join))? {
            crate::types::IOResult::Done(()) => {}
            crate::types::IOResult::IO(io) => {
                return Ok(InsnFunctionStepResult::IO(io));
            }
        }
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// Write payload values from a hash entry to registers.
fn write_hash_payload_to_registers(
    registers: &mut [Register],
    entry: &HashEntry,
    payload_dest_reg: Option<usize>,
    num_payload: usize,
) -> Result<()> {
    if let Some(dest_reg) = payload_dest_reg {
        for (i, value) in entry.payload_values.iter().take(num_payload).enumerate() {
            // try_clone_value_from reuses the destination register's allocation.
            registers[dest_reg + i].try_clone_value_from(value)?;
        }
    }
    Ok(())
}

pub fn op_hash_probe(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashProbe {
            hash_table_id,
            key_start_reg,
            num_keys,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
            probe_rowid_reg,
        },
        insn
    );
    let hash_table_id = *hash_table_id as usize;
    let key_start_reg = *key_start_reg as usize;
    let num_keys = *num_keys as usize;
    let dest_reg = *dest_reg as usize;
    let payload_dest_reg = payload_dest_reg.map(|r| r as usize);
    let num_payload = *num_payload as usize;
    let probe_rowid_reg = probe_rowid_reg.map(|r| r as usize);
    let (probe_keys, partition_idx, probe_buffered) =
        if let Some(op_state) = state.active_op_state.hash_probe().take() {
            if op_state.hash_table_id == hash_table_id {
                (
                    op_state.probe_keys,
                    Some(op_state.partition_idx),
                    op_state.probe_buffered,
                )
            } else {
                // Different hash table, read fresh keys
                let keys = (0..num_keys)
                    .map(|i| {
                        let reg = &state.registers[key_start_reg + i];
                        reg.get_value().clone()
                    })
                    .try_collect()?;
                (keys, None, false)
            }
        } else {
            // First entry, read probe keys from registers
            let keys = (0..num_keys)
                .map(|i| {
                    let reg = &state.registers[key_start_reg + i];
                    reg.get_value().clone()
                })
                .try_collect()?;
            (keys, None, false)
        };

    let Some(hash_table) = state.hash_tables.get_mut(&hash_table_id) else {
        // Empty build side: treat as no match and jump to target.
        state.active_op_state.clear();
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    };

    // For spilled hash tables, either buffer main-loop probe rows for grace
    // processing or probe a partition that grace logic already loaded.
    if hash_table.has_spilled() {
        let partition_idx = match partition_idx {
            Some(partition_idx) => partition_idx,
            None => hash_table.partition_for_keys(&probe_keys)?,
        };

        // Main probe loop: buffer probe rows targeting spilled build partitions.
        if let Some(rowid_reg) = probe_rowid_reg {
            if probe_buffered {
                state.pc = target_pc.as_offset_int();
                state.active_op_state.clear();
                return Ok(InsnFunctionStepResult::Step);
            }
            if !hash_table.is_partition_loaded(partition_idx) {
                // Partition is on disk: buffer this probe row for grace processing
                let probe_rowid = match state.registers[rowid_reg].get_value() {
                    Value::Numeric(Numeric::Integer(i)) => *i,
                    _ => 0,
                };
                match hash_table.buffer_probe_row(
                    probe_keys,
                    probe_rowid,
                    Some(&mut state.metrics.hash_join),
                )? {
                    IOResult::Done(()) => {}
                    IOResult::IO(io) => {
                        *state.active_op_state.hash_probe() = Some(OpHashProbeState {
                            probe_keys: crate::alloc::vec![], // keys consumed
                            hash_table_id,
                            partition_idx,
                            probe_buffered: true,
                        });
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                }
                // Jump to target_pc: this row is deferred to grace processing.
                state.pc = target_pc.as_offset_int();
                state.active_op_state.clear();
                return Ok(InsnFunctionStepResult::Step);
            }
            // Partition is in memory: probe immediately (fast path)
            state.metrics.hash_join.grace_probe_rows_streamed = state
                .metrics
                .hash_join
                .grace_probe_rows_streamed
                .saturating_add(1);
        } else if unlikely(!hash_table.is_partition_loaded(partition_idx)) {
            return Err(LimboError::InternalError(format!(
                "HashProbe reached spilled partition {partition_idx} without a preloaded build partition; probe_rowid_reg=None is grace-only"
            )).into());
        }

        // Probe the loaded partition
        match hash_table.probe_partition(
            partition_idx,
            &probe_keys,
            Some(&mut state.metrics.hash_join),
        )? {
            Some(entry) => {
                state.registers[dest_reg].set_int(entry.rowid);
                write_hash_payload_to_registers(
                    &mut state.registers,
                    entry,
                    payload_dest_reg,
                    num_payload,
                )?;
                state.active_op_state.clear();
                state.pc += 1;
                Ok(InsnFunctionStepResult::Step)
            }
            None => {
                state.active_op_state.clear();
                state.pc = target_pc.as_offset_int();
                Ok(InsnFunctionStepResult::Step)
            }
        }
    } else {
        // Non-spilled hash table, use normal probe
        match hash_table.probe(probe_keys, Some(&mut state.metrics.hash_join))? {
            Some(entry) => {
                state.registers[dest_reg].set_int(entry.rowid);
                write_hash_payload_to_registers(
                    &mut state.registers,
                    entry,
                    payload_dest_reg,
                    num_payload,
                )?;
                state.active_op_state.clear();
                state.pc += 1;
                Ok(InsnFunctionStepResult::Step)
            }
            None => {
                state.active_op_state.clear();
                state.pc = target_pc.as_offset_int();
                Ok(InsnFunctionStepResult::Step)
            }
        }
    }
}

pub fn op_hash_next(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashNext {
            hash_table_id,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );

    let hash_table = state.hash_tables.get_mut(hash_table_id).ok_or_else(|| {
        mark_unlikely();
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;
    match hash_table.next_match()? {
        Some(entry) => {
            state.registers[*dest_reg].set_int(entry.rowid);
            write_hash_payload_to_registers(
                &mut state.registers,
                entry,
                *payload_dest_reg,
                *num_payload,
            )?;
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        None => {
            state.pc = target_pc.as_offset_int();
            Ok(InsnFunctionStepResult::Step)
        }
    }
}

pub fn op_hash_close(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashClose { hash_table_id }, insn);
    if let Some(mut hash_table) = state.hash_tables.remove(hash_table_id) {
        hash_table.close();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_clear(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashClear { hash_table_id }, insn);
    if let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) {
        hash_table.clear()?;
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_reset_matched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashResetMatched { hash_table_id }, insn);
    if let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) {
        hash_table.reset_matched_bits();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_mark_matched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(HashMarkMatched { hash_table_id }, insn);
    if let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) {
        hash_table.mark_current_matched();
    }
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_scan_unmatched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashScanUnmatched {
            hash_table_id,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );

    let Some(hash_table) = state.hash_tables.get_mut(hash_table_id) else {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    };

    hash_table.begin_unmatched_scan();
    advance_unmatched_scan(
        hash_table,
        &mut state.registers,
        &mut state.pc,
        *dest_reg,
        target_pc.as_offset_int(),
        *payload_dest_reg,
        *num_payload,
        &mut state.metrics.hash_join,
    )
}

pub fn op_hash_next_unmatched(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashNextUnmatched {
            hash_table_id,
            dest_reg,
            target_pc,
            payload_dest_reg,
            num_payload,
        },
        insn
    );

    let hash_table = state.hash_tables.get_mut(hash_table_id).ok_or_else(|| {
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;

    advance_unmatched_scan(
        hash_table,
        &mut state.registers,
        &mut state.pc,
        *dest_reg,
        target_pc.as_offset_int(),
        *payload_dest_reg,
        *num_payload,
        &mut state.metrics.hash_join,
    )
}

/// Shared logic for HashScanUnmatched/HashNextUnmatched: find the next unmatched
/// entry, loading spilled partitions as needed.
#[allow(clippy::too_many_arguments)]
fn advance_unmatched_scan(
    hash_table: &mut HashTable,
    registers: &mut [Register],
    pc: &mut u32,
    dest_reg: usize,
    target_pc: u32,
    payload_dest_reg: Option<usize>,
    num_payload: usize,
    metrics: &mut HashJoinMetrics,
) -> InsnResult {
    if hash_table.has_spilled() {
        if let Some(partition_idx) = hash_table.unmatched_scan_current_partition() {
            if !hash_table.is_partition_loaded(partition_idx) {
                match hash_table.load_spilled_partition(partition_idx, Some(metrics))? {
                    crate::types::IOResult::Done(()) => {}
                    crate::types::IOResult::IO(io) => {
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                }
            }
        }
    }

    loop {
        match hash_table.next_unmatched() {
            Some(entry) => {
                registers[dest_reg].set_int(entry.rowid);
                write_hash_payload_to_registers(registers, entry, payload_dest_reg, num_payload)?;
                *pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
            None => {
                if hash_table.has_spilled() {
                    if let Some(partition_idx) = hash_table.unmatched_scan_current_partition() {
                        if !hash_table.is_partition_loaded(partition_idx) {
                            match hash_table.load_spilled_partition(partition_idx, Some(metrics))? {
                                crate::types::IOResult::Done(()) => continue,
                                crate::types::IOResult::IO(io) => {
                                    return Ok(InsnFunctionStepResult::IO(io));
                                }
                            }
                        }
                    }
                }
                *pc = target_pc;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_hash_grace_init(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashGraceInit {
            hash_table_id,
            target_pc,
        },
        insn
    );
    let hash_table_id = *hash_table_id as usize;

    let Some(hash_table) = state.hash_tables.get_mut(&hash_table_id) else {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    };

    // If no spilling occurred, skip grace processing
    if !hash_table.has_grace_partitions() {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }

    // Finalize probe spill
    match hash_table.finalize_probe_spill(Some(&mut state.metrics.hash_join))? {
        IOResult::Done(()) => {}
        IOResult::IO(io) => {
            return Ok(InsnFunctionStepResult::IO(io));
        }
    }

    // Initialize grace processing
    if !hash_table.grace_begin()? {
        state.pc = target_pc.as_offset_int();
        return Ok(InsnFunctionStepResult::Step);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_hash_grace_load_partition(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashGraceLoadPartition {
            hash_table_id,
            target_pc,
        },
        insn
    );
    let hash_table_id = *hash_table_id as usize;

    let hash_table = state.hash_tables.get_mut(&hash_table_id).ok_or_else(|| {
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;

    match hash_table.grace_load_current_partition(Some(&mut state.metrics.hash_join))? {
        IOResult::Done(true) => {
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        IOResult::Done(false) => {
            state.pc = target_pc.as_offset_int();
            Ok(InsnFunctionStepResult::Step)
        }
        IOResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
    }
}

pub fn op_hash_grace_next_probe(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashGraceNextProbe {
            hash_table_id,
            key_start_reg,
            num_keys,
            probe_rowid_dest,
            target_pc,
        },
        insn
    );
    let hash_table_id = *hash_table_id as usize;
    let key_start_reg = *key_start_reg as usize;
    let num_keys = *num_keys as usize;
    let probe_rowid_dest = *probe_rowid_dest as usize;

    let hash_table = state.hash_tables.get_mut(&hash_table_id).ok_or_else(|| {
        mark_unlikely();
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;

    match hash_table.grace_next_probe_entry()? {
        IOResult::Done(Some(entry)) => {
            // Write probe keys to registers
            for (i, value) in entry.key_values.into_iter().enumerate() {
                if i < num_keys {
                    state.registers[key_start_reg + i].set_value(value);
                }
            }
            // Write probe rowid
            state.registers[probe_rowid_dest].set_int(entry.probe_rowid);
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        IOResult::Done(None) => {
            state.pc = target_pc.as_offset_int();
            Ok(InsnFunctionStepResult::Step)
        }
        IOResult::IO(io) => Ok(InsnFunctionStepResult::IO(io)),
    }
}

pub fn op_hash_grace_advance_partition(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        HashGraceAdvancePartition {
            hash_table_id,
            target_pc,
        },
        insn
    );
    let hash_table_id = *hash_table_id as usize;

    let hash_table = state.hash_tables.get_mut(&hash_table_id).ok_or_else(|| {
        mark_unlikely();
        LimboError::InternalError(format!("Hash table not found with ID: {hash_table_id}"))
    })?;

    if hash_table.grace_advance_partition() {
        state.pc += 1;
    } else {
        state.pc = target_pc.as_offset_int();
    }
    Ok(InsnFunctionStepResult::Step)
}

fn apply_affinity_char(target: &mut Register, affinity: Affinity) -> bool {
    if let Register::Value(value) = target {
        if matches!(value, Value::Blob(_)) {
            return true;
        }

        match affinity {
            Affinity::Blob | Affinity::None => return true,

            Affinity::Text => {
                if matches!(value, Value::Text(_) | Value::Null) {
                    return true;
                }
                let text = value.to_string();
                *value = Value::Text(text.into());
                return true;
            }

            Affinity::Integer | Affinity::Numeric => {
                if matches!(value, Value::Numeric(Numeric::Integer(_))) {
                    return true;
                }
                if !matches!(value, Value::Text(_) | Value::Numeric(Numeric::Float(_))) {
                    return true;
                }

                if let Value::Numeric(Numeric::Float(fl)) = *value {
                    // For floats, try to convert to integer if it's exact
                    // This is similar to sqlite3VdbeIntegerAffinity
                    return try_float_to_integer_affinity(value, f64::from(fl));
                }

                if let Value::Text(t) = value {
                    let text = trim_ascii_whitespace(t.as_str());

                    // Handle hex numbers - they shouldn't be converted
                    if text.starts_with("0x") {
                        return false;
                    }

                    let (parse_result, parsed_value) = try_for_float(text.as_bytes());
                    let num = match parse_result {
                        NumericParseResult::NotNumeric | NumericParseResult::ValidPrefixOnly => {
                            return false;
                        }
                        NumericParseResult::PureInteger | NumericParseResult::HasDecimalOrExp => {
                            match parsed_value {
                                ParsedNumber::Integer(i) => Value::from_i64(i),
                                ParsedNumber::Float(f) => Value::from_f64(f),
                                ParsedNumber::None => return false,
                            }
                        }
                    };

                    match num {
                        Value::Numeric(Numeric::Integer(i)) => {
                            *value = Value::from_i64(i);
                            return true;
                        }
                        Value::Numeric(Numeric::Float(fl)) => {
                            // For both Numeric and Integer affinity, try to convert
                            // float to int if exact. SQLite treats INTEGER identically
                            // to NUMERIC here: both enter applyNumericAffinity() with
                            // bTryForInt=1 (sqlite/src/vdbe.c:403-408).
                            return try_float_to_integer_affinity(value, f64::from(fl));
                        }
                        other => {
                            *value = other;
                            return true;
                        }
                    }
                }

                return false;
            }

            Affinity::Real => match value {
                Value::Numeric(Numeric::Integer(i)) => {
                    *value = Value::from_f64(*i as f64);
                    return true;
                }
                Value::Numeric(Numeric::Float(_)) | Value::Null => {
                    return true;
                }
                Value::Text(t) => {
                    let text = trim_ascii_whitespace(t.as_str());
                    if text.starts_with("0x") {
                        return false;
                    }

                    let (parse_result, parsed_value) = try_for_float(text.as_bytes());
                    let coerced = match parse_result {
                        NumericParseResult::NotNumeric | NumericParseResult::ValidPrefixOnly => {
                            return false;
                        }
                        NumericParseResult::PureInteger | NumericParseResult::HasDecimalOrExp => {
                            match parsed_value {
                                ParsedNumber::Integer(i) => Value::from_f64(i as f64),
                                ParsedNumber::Float(f) => Value::from_f64(f),
                                ParsedNumber::None => return false,
                            }
                        }
                    };

                    *value = coerced;
                    return true;
                }
                _ => return true,
            },
        }
    }

    true
}

fn try_float_to_integer_affinity(value: &mut Value, fl: f64) -> bool {
    // Check if the float can be exactly represented as an integer
    if let Ok(int_val) = cast_real_to_integer(fl) {
        // Additional check: ensure round-trip conversion is exact
        // and value is within safe bounds (similar to SQLite's checks)
        if (int_val as f64) == fl && int_val > i64::MIN + 1 && int_val < i64::MAX - 1 {
            *value = Value::from_i64(int_val);
            return true;
        }
    }

    // If we can't convert to exact integer, keep as float for Numeric affinity
    // but return false to indicate the conversion wasn't "complete"
    *value = Value::from_f64(fl);
    false
}

fn parse_test_uint(reg: &Register) -> Result<Option<u64>> {
    match reg.get_value() {
        Value::Null => Ok(None),
        Value::Numeric(Numeric::Integer(i)) => {
            if *i < 0 {
                Err(LimboError::InternalError(
                    "test_uint: negative value".to_string(),
                ))
            } else {
                Ok(Some(*i as u64))
            }
        }
        Value::Text(t) => {
            let s = t.to_string();
            let v = s
                .parse::<u64>()
                .map_err(|_| LimboError::InternalError(format!("test_uint: invalid uint: {s}")))?;
            Ok(Some(v))
        }
        _ => Err(LimboError::InternalError(
            "test_uint: unsupported type".to_string(),
        )),
    }
}

// Compat for applications that test for SQLite.
fn execute_sqlite_version() -> String {
    crate::dialect::sqlite::SQLITE_VERSION.to_string()
}

fn execute_turso_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{major}.{minor}.{release}")
}

/// Coerce a value to i64 using the same non-NULL conversion rules as
/// SQLite's OP_AddImm.
pub fn extract_int_value<V: AsValueRef>(value: V) -> i64 {
    let value = value.as_value_ref();
    match value {
        ValueRef::Numeric(Numeric::Integer(i)) => i,
        ValueRef::Numeric(Numeric::Float(f)) => real_to_i64(f64::from(f)),
        ValueRef::Text(t) => crate::numeric::str_to_i64(t.as_str()).unwrap_or(0),
        ValueRef::Blob(b) => {
            crate::numeric::str_to_i64(String::from_utf8_lossy(b).as_ref()).unwrap_or(0)
        }
        ValueRef::Null => 0,
    }
}

pub fn op_max_pgcnt(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(MaxPgcnt { db, dest, new_max }, insn);

    let pager = program.get_pager_from_database_index(db)?;
    let result_value = if *new_max == 0 {
        // If new_max is 0, just return current maximum without changing it
        pager.get_max_page_count()
    } else {
        // Set new maximum page count (will be clamped to current database size)
        return_if_io!(pager.set_max_page_count(*new_max as u32))
    };

    state.registers[*dest].set_int(result_value.into());
    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

/// State machine for PRAGMA journal_mode changes
#[derive(Debug, Clone, Copy, Default)]
pub enum OpJournalModeSubState {
    /// Initial state - read header to get current mode
    #[default]
    Start,
    /// Checkpointing WAL/MVCC before mode change
    Checkpoint,
    /// Update the header with new version and get page reference
    UpdateHeader,
    /// Write page 1 to disk
    WritePage,
    /// Finalize - clear cache and setup new mode
    Finalize,
    /// Bootstrap the MV store after switching to MVCC mode
    BootstrapMvStore,
}

/// Holds the state for the journal mode change operation
#[derive(Default)]
pub struct OpJournalModeState {
    pub sub_state: OpJournalModeSubState,
    /// The previous journal mode (before the change)
    pub prev_mode: Option<journal_mode::JournalMode>,
    /// The new journal mode we're changing to
    pub new_mode: Option<journal_mode::JournalMode>,
    /// Checkpoint state machine for MVCC mode
    pub checkpoint_sm: Option<StateMachine<Box<MvccCheckpointStateMachine>>>,
    /// Bootstrap state machine when switching into MVCC mode
    pub bootstrap_state: BootstrapState,
    /// Page reference for writing header
    pub page_ref: Option<PageRef>,
    /// Abandonment guard for the WAL→MVCC switch. Armed in `Finalize` once the
    /// store is installed and the connection demoted; disarmed only on
    /// successful bootstrap. If the statement is reset/dropped while parked at a
    /// bootstrap yield, dropping this guard restores in-memory state.
    pub bootstrap_guard: Option<MvccBootstrapGuard>,
}

impl OpJournalModeState {
    pub(super) fn cleanup_checkpoint(&mut self) -> Result<()> {
        let Some(mut checkpoint_sm) = self.checkpoint_sm.take() else {
            return Ok(());
        };
        checkpoint_sm
            .inner_mut()
            .cleanup_after_external_io_error(LimboError::InternalError(
                "mvcc: abandoned journal-mode checkpoint".to_string(),
            ))
    }
}

/// Restores in-memory MVCC state if a `PRAGMA journal_mode=mvcc` bootstrap is
/// abandoned (statement reset/dropped) or errors after the connection has been
/// demoted and the shared `MvStore` installed.
///
/// Without this, `ProgramState::reset()` would clear `active_op_state` while
/// leaving `is_mvcc_bootstrap_connection` set forever (silently bypassing MVCC)
/// and the DB-wide `mv_store` installed but un-bootstrapped (`global_header =
/// None`), which other connections can trip an assertion on at commit. Mirrors
/// `MvccVacuumGuard` in `vacuum.rs`.
pub struct MvccBootstrapGuard {
    connection: Arc<Connection>,
    completed: bool,
}

impl MvccBootstrapGuard {
    fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for MvccBootstrapGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        // Bootstrap did not finish: re-promote the connection if it is still
        // demoted (bootstrap promotes itself only at `Recover`) and uninstall
        // the un-bootstrapped store so neither this connection nor others on
        // the same `Database` observe a demoted-but-unbootstrapped MVCC store.
        // The on-disk header already reads MVCC, so the database recovers on
        // the next fresh open via the regular MVCC bootstrap path.
        if self.connection.is_mvcc_bootstrap_connection() {
            self.connection.promote_to_regular_connection();
        }
        self.connection.db.mv_store.store(None);
    }
}

pub fn op_journal_mode(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    pager: &Arc<Pager>,
) -> InsnResult {
    match op_journal_mode_inner(program, state, insn, pager) {
        Ok(result) => {
            if !matches!(result, InsnFunctionStepResult::IO(_)) {
                // Reset state if we are done with this instruction
                state.active_op_state.clear();
            }
            Ok(result)
        }
        Err(err) => {
            // Reset state on error
            state.active_op_state.clear();
            Err(err)
        }
    }
}

fn op_journal_mode_inner(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    use crate::storage::sqlite3_ondisk::begin_write_btree_page;

    load_insn!(JournalMode { db, dest, new_mode }, insn);
    let pager = program.get_pager_from_database_index(db)?;
    let pager = &pager;

    loop {
        match state.active_op_state.journal_mode().sub_state {
            OpJournalModeSubState::Start => {
                // Read header to get current mode
                let mv_store = program.connection.mv_store_for_db(*db);
                let header_result = with_header(pager, mv_store.as_ref(), program, *db, |header| {
                    header.read_version
                });

                let prev_mode_raw = return_if_io!(header_result);

                let prev_mode_version = prev_mode_raw
                    .to_version()
                    .map_err(|val| LimboError::Corrupt(format!("Invalid read_version: {val}")))?;

                let prev_mode = journal_mode::JournalMode::from(prev_mode_version);
                state.active_op_state.journal_mode().prev_mode = Some(prev_mode);

                // If no new mode specified, just return current mode
                let Some(mode_str) = new_mode else {
                    let ret: &'static str = prev_mode.into();
                    state.registers[*dest].set_text(Text::new(ret))?;
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                };

                // Parse the new mode. If unknown or unsupported, silently return
                // current mode (matches SQLite behavior).
                let new_mode = match journal_mode::JournalMode::from_str(mode_str.as_str()) {
                    Ok(mode) if mode.supported() => mode,
                    _ => {
                        let ret: &'static str = prev_mode.into();
                        state.registers[*dest].set_text(Text::new(ret))?;
                        state.pc += 1;
                        return Ok(InsnFunctionStepResult::Step);
                    }
                };

                // If same mode, just return
                if prev_mode == new_mode {
                    let ret: &'static str = new_mode.into();
                    state.registers[*dest].set_text(Text::new(ret))?;
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                }

                if *db != crate::MAIN_DB_ID {
                    let ret: &'static str = prev_mode.into();
                    state.registers[*dest].set_text(Text::new(ret))?;
                    state.pc += 1;
                    return Ok(InsnFunctionStepResult::Step);
                }

                // Check if database is readonly - cannot change journal mode on readonly databases
                if program.connection.is_readonly(*db) {
                    return Err(LimboError::ReadOnly.into());
                }

                // CDC capture is connection-level state that feeds off the
                // current journal mode's write path. Changing the mode while
                // capture is active silently stops capture while the capture
                // pragma still reports it as on. Reject the change loudly
                // (0.6 rejected it with "cannot enable MVCC while CDC is
                // active"; that guard was lost in the logical log v3
                // redesign). A no-op re-run of the current mode is fine —
                // the same-mode early return above already handled it.
                if program.connection.get_capture_data_changes_info().is_some() {
                    let prev: &'static str = prev_mode.into();
                    let new: &'static str = new_mode.into();
                    return Err(LimboError::InvalidArgument(format!(
                        "cannot change journal_mode (from {prev} to {new}) while CDC capture is active: \
                         capture would silently stop; disable capture first with \
                         PRAGMA capture_data_changes_conn('off')"
                    )).into());
                }

                // MVCC has no cross-process coordination: commit
                // serialization, the logical-log append offset, and
                // checkpoint exclusion are all process-local, so switching a
                // multiprocess-coordinated database to MVCC would silently
                // lose committed transactions and corrupt live views.
                if matches!(new_mode, journal_mode::JournalMode::Mvcc)
                    && program
                        .connection
                        .db
                        .experimental_multiprocess_wal_enabled()
                {
                    return Err(LimboError::InvalidArgument(
                        "journal_mode=mvcc is not supported with experimental multiprocess WAL: MVCC does not support multiprocess access"
                            .to_string(),
                    ).into());
                }
                if matches!(new_mode, journal_mode::JournalMode::Mvcc)
                    && pager.has_external_page_codec()
                {
                    return Err(LimboError::InvalidArgument(
                        "external page codecs are not supported with MVCC databases".to_string(),
                    )
                    .into());
                }

                state.active_op_state.journal_mode().new_mode = Some(new_mode);
                state.active_op_state.journal_mode().sub_state = OpJournalModeSubState::Checkpoint;
            }

            OpJournalModeSubState::Checkpoint => {
                // Checkpoint WAL or MVCC before changing mode
                let mv_store = program.connection.mv_store_for_db(*db);
                if let Some(mv_store) = mv_store.as_ref() {
                    // MVCC checkpoint using state machine
                    if state.active_op_state.journal_mode().checkpoint_sm.is_none() {
                        state.active_op_state.journal_mode().checkpoint_sm =
                            Some(StateMachine::new(Box::new(CheckpointStateMachine::new(
                                pager.clone(),
                                mv_store.clone(),
                                program.connection.clone(),
                                true,
                                program.connection.get_sync_mode(),
                                *db,
                                // Changing journal mode requires the WAL fully reset.
                                CheckpointMode::Truncate {
                                    upper_bound_inclusive: None,
                                },
                            ))));
                    }

                    let ckpt_sm = state
                        .active_op_state
                        .journal_mode()
                        .checkpoint_sm
                        .as_mut()
                        .unwrap();
                    return_if_io!(ckpt_sm.step(&()));
                    state.active_op_state.journal_mode().checkpoint_sm = None;
                    state.active_op_state.journal_mode().sub_state =
                        OpJournalModeSubState::UpdateHeader;
                } else {
                    // WAL checkpoint
                    let checkpoint_result = pager.checkpoint(
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None,
                        },
                        program.connection.get_sync_mode(),
                        false, // Don't clear cache yet, we'll do it in Finalize
                    );
                    return_if_io!(checkpoint_result);
                    state.active_op_state.journal_mode().sub_state =
                        OpJournalModeSubState::UpdateHeader;
                }
            }

            OpJournalModeSubState::UpdateHeader => {
                let new_mode = state
                    .active_op_state
                    .journal_mode()
                    .new_mode
                    .expect("new_mode should be set");
                let new_version = new_mode
                    .as_version()
                    .expect("Should be a supported Journal Mode");
                let raw_version = RawVersion::from(new_version);

                // Get the header page reference (handles both initialized and uninitialized databases)
                // This uses the pager's cache and won't fail for empty database files
                let header_ref =
                    return_if_io!(crate::storage::pager::HeaderRefMut::from_pager(pager));

                // Update the header version
                {
                    let header = header_ref.borrow_mut();
                    header.read_version = raw_version;
                    header.write_version = raw_version;
                }

                // Save the page reference for writing
                state.active_op_state.journal_mode().page_ref = Some(header_ref.page().clone());
                // Skip ReadPage and go directly to WritePage
                state.active_op_state.journal_mode().sub_state = OpJournalModeSubState::WritePage;
            }

            OpJournalModeSubState::WritePage => {
                // Write page 1 to disk to flush the header
                let page = state
                    .active_op_state
                    .journal_mode()
                    .page_ref
                    .as_ref()
                    .expect("page_ref should be set");
                let completion = begin_write_btree_page(pager, page, None)?;
                state.active_op_state.journal_mode().sub_state = OpJournalModeSubState::Finalize;
                return Ok(InsnFunctionStepResult::IO(IOCompletions(completion)));
            }

            OpJournalModeSubState::Finalize => {
                let new_mode = state
                    .active_op_state
                    .journal_mode()
                    .new_mode
                    .expect("new_mode should be set");

                // Clear page cache
                pager.clear_page_cache(true);

                // Setup new mode
                if matches!(new_mode, journal_mode::JournalMode::Mvcc) {
                    let db_path = program.connection.get_database_canonical_path();
                    let enc_ctx = pager.io_ctx.read().encryption_context().cloned();
                    let mv_store = journal_mode::open_mv_store(
                        pager.io.clone(),
                        &db_path,
                        program.connection.db.open_flags,
                        program.connection.db.durable_storage.clone(),
                        enc_ctx,
                        program.connection.db.mv_store_allocator.clone(),
                        program
                            .connection
                            .db
                            .experimental_mvcc_passive_checkpoint_enabled(),
                    )?;
                    // Arm the abandonment guard *before* the irreversible
                    // store install + demote so a reset/drop at any subsequent
                    // bootstrap yield restores in-memory state.
                    let guard = MvccBootstrapGuard::new(program.connection.clone());
                    program.connection.db.mv_store.store(Some(mv_store.clone()));
                    program.connection.demote_to_mvcc_connection();
                    state.active_op_state.journal_mode().bootstrap_guard = Some(guard);
                    state.active_op_state.journal_mode().bootstrap_state =
                        BootstrapState::default();
                    state.active_op_state.journal_mode().sub_state =
                        OpJournalModeSubState::BootstrapMvStore;
                    continue;
                }

                if matches!(new_mode, journal_mode::JournalMode::Wal) {
                    program.connection.db.mv_store.store(None);
                }

                // Return result
                let ret: &'static str = new_mode.into();
                state.registers[*dest].set_text(Text::new(ret))?;
                state.pc += 1;

                return Ok(InsnFunctionStepResult::Step);
            }

            OpJournalModeSubState::BootstrapMvStore => {
                let mv_store_guard = program.connection.db.get_mv_store();
                let Some(mv_store) = mv_store_guard.as_ref() else {
                    return Err(LimboError::InternalError(
                        "MVCC journal mode bootstrap missing MV store".to_string(),
                    )
                    .into());
                };
                return_if_io!(mv_store.bootstrap_nonblock(
                    &program.connection,
                    &mut state.active_op_state.journal_mode().bootstrap_state
                ));

                // Bootstrap finished: disarm the abandonment guard so it does
                // not roll back the now-published MVCC store on drop.
                if let Some(guard) = state
                    .active_op_state
                    .journal_mode()
                    .bootstrap_guard
                    .as_mut()
                {
                    guard.complete();
                }

                let ret: &'static str = journal_mode::JournalMode::Mvcc.into();
                state.registers[*dest].set_text(Text::new(ret))?;
                state.pc += 1;

                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

pub fn op_filter(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        Filter {
            cursor_id,
            target_pc,
            key_reg,
            num_keys
        },
        insn
    );
    let Some(filter) = state.get_bloom_filter(*cursor_id) else {
        // always safe to fall though, no filter present
        state.pc += 1;
        return Ok(InsnFunctionStepResult::Step);
    };
    let contains = if *num_keys == 1 {
        // Single key optimization, avoid allocating a Vec
        let value = state.registers[*key_reg].get_value();
        if matches!(value, Value::Null) {
            // its always safe to fall through, so this *should* be `true` but
            // since it's always an equality predicate and we have a NULL value,
            // we can just short-circuit to false here.
            false
        } else {
            filter.contains_value(value)
        }
    } else {
        let values: Vec<&Value> = (0..*num_keys)
            .map(|i| state.registers[*key_reg + i].get_value())
            .collect();
        if values.iter().any(|v| matches!(*v, Value::Null)) {
            false
        } else {
            filter.contains_values(&values)
        }
    };

    if !contains {
        state.pc = target_pc.as_offset_int();
    } else {
        state.pc += 1;
    }
    Ok(InsnFunctionStepResult::Step)
}

pub fn op_filter_add(
    _program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(
        FilterAdd {
            cursor_id,
            key_reg,
            num_keys
        },
        insn
    );

    if *num_keys == 1 {
        let reg: *const Register = &state.registers[*key_reg];
        let value = unsafe { &*reg }.get_value();
        let filter = state.get_or_create_bloom_filter(*cursor_id);
        filter.insert_value(value);
    } else {
        let values: Vec<Value> = (0..*num_keys)
            .map(|i| state.registers[*key_reg + i].get_value().clone())
            .collect();
        let filter = state.get_or_create_bloom_filter(*cursor_id);
        let value_refs: Vec<&Value> = values.iter().collect();
        filter.insert_values(&value_refs);
    }

    state.pc += 1;
    Ok(InsnFunctionStepResult::Step)
}

fn extract_pragma_int<T>(rows: &[Vec<Value>], pragma_name: &str) -> Result<T>
where
    T: TryFrom<i64>,
{
    rows.first()
        .and_then(|row| row.first())
        .and_then(|v| match v {
            Value::Numeric(Numeric::Integer(i)) => T::try_from(*i).ok(),
            _ => None,
        })
        .ok_or_else(|| {
            LimboError::InternalError(format!("failed to read {pragma_name} from source"))
        })
}

/// Phases for the VACUUM INTO opcode wrapper.
#[derive(Default)]
pub(crate) enum VacuumIntoOpPhase {
    /// Initial state - validate preconditions and create output database.
    #[default]
    Init,
    /// Build compacted output database.
    Build,
    /// Force the committed output into a durable self-contained database file.
    FinalizeOutput,
    /// Operation complete.
    Done,
}

/// Holds the state for the VACUUM INTO opcode operation.
#[derive(Default)]
pub(crate) struct VacuumIntoOpContext {
    phase: VacuumIntoOpPhase,
    /// Database index for the source schema.
    source_db_id: usize,
    /// Escaped schema name for safe SQL interpolation.
    escaped_schema_name: String,
    /// Keep output database alive while vacuum is in progress.
    _output_db: Option<Arc<Database>>,
    /// Configuration for the shared vacuum target build state machine.
    target_build_config: Option<VacuumTargetBuildConfig>,
    /// Context for the shared vacuum target build state machine.
    target_build_context: Option<VacuumTargetBuildContext>,
}

/// VACUUM INTO - create a compacted copy of the database at the specified path.
///
/// This is an async state machine implementation that yields on I/O operations.
/// It:
/// 1. Creates a new output database with matching page_size and
///    source feature flags and schema-replay symbols
/// 2. Queries sqlite_schema for all schema objects including rootpage, ordered by rowid
/// 3. Creates storage-backed tables (rootpage != 0) in the output, excluding
///    sqlite_sequence (auto-created when AUTOINCREMENT tables are created)
/// 4. Copies data for all storage-backed tables, including sqlite_stat1 and other
///    internal storage-backed tables
/// 5. Creates user-defined secondary indexes after data copy for performance
///    (backing-btree indexes for custom index methods are excluded here)
/// 6. Finalizes output database header metadata
/// 7. Creates triggers, views, and rootpage = 0 objects last (after data copy).
///    Custom index methods (FTS, vector) recreate and backfill their backing
///    indexes from the copied table data in this phase.
pub fn op_vacuum_into(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    match op_vacuum_into_inner(program, state, insn) {
        Ok(InsnFunctionStepResult::Step) => {
            // Instruction complete, reset state
            state.op_vacuum_state = VacuumOpState::None;
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(InsnFunctionStepResult::IO(io)) => {
            // Waiting for I/O, keep state for resumption
            Ok(InsnFunctionStepResult::IO(io))
        }
        Ok(InsnFunctionStepResult::Done | InsnFunctionStepResult::Row) => {
            unreachable!("op_vacuum_into_inner only returns Step or IO")
        }
        Err(err) => {
            if matches!(state.op_vacuum_state, VacuumOpState::IntoFile(_)) {
                let VacuumOpState::IntoFile(vacuum_state) =
                    std::mem::take(&mut state.op_vacuum_state)
                else {
                    unreachable!("invalid state, we are inside vacuum into op");
                };
                if let Err(cleanup_err) =
                    cleanup_op_vacuum_into(&program.connection, state, vacuum_state)
                {
                    tracing::error!("VACUUM INTO cleanup failed after error: {cleanup_err}");
                }
            }
            Err(err)
        }
    }
}

/// Clean up any VACUUM or VACUUM INTO state on error or abort.
pub(crate) fn cleanup_vacuum_state(
    connection: &Arc<Connection>,
    state: &mut ProgramState,
) -> Result<()> {
    match std::mem::take(&mut state.op_vacuum_state) {
        VacuumOpState::None => Ok(()),
        VacuumOpState::IntoFile(vacuum_state) => {
            cleanup_op_vacuum_into(connection, state, vacuum_state)
        }
        VacuumOpState::InPlace(vacuum_state) => {
            cleanup_op_vacuum_in_place(connection, vacuum_state)
        }
    }
}

fn cleanup_op_vacuum_into(
    connection: &Arc<Connection>,
    state: &mut ProgramState,
    mut vacuum_state: Box<VacuumIntoOpContext>,
) -> Result<()> {
    if let Some(target_build_context) = vacuum_state.target_build_context.as_mut() {
        target_build_context.cleanup_after_error()?;
    }

    vacuum_state.target_build_context = None;
    vacuum_state._output_db = None;

    if state.auto_txn_cleanup == TxnCleanup::RollbackTxn {
        let pager = connection.pager.load();
        connection.rollback_manual_txn_cleanup(&pager, true);
        state.auto_txn_cleanup = TxnCleanup::None;
    }
    Ok(())
}

fn op_vacuum_into_inner(program: &Program, state: &mut ProgramState, insn: &Insn) -> InsnResult {
    load_insn!(
        VacuumInto {
            schema_name,
            dest_path
        },
        insn
    );

    if matches!(state.op_vacuum_state, VacuumOpState::None) {
        let source_db_id = program.connection.get_database_id_by_name(schema_name)?;

        // Matches sqlite that treats VACUUM temp INTO as a no-op (no file created)
        if source_db_id == TEMP_DB_ID {
            state.pc += 1;
            return Ok(InsnFunctionStepResult::Step);
        }

        state.op_vacuum_state = VacuumOpState::IntoFile(Box::new(VacuumIntoOpContext {
            escaped_schema_name: schema_name.replace('"', "\"\""),
            source_db_id,
            ..Default::default()
        }));
    }

    let VacuumOpState::IntoFile(vacuum_state) = &mut state.op_vacuum_state else {
        return Err(LimboError::InternalError(
            "VACUUM INTO resumed with incompatible VACUUM state".to_string(),
        )
        .into());
    };
    let escaped_schema_name = &vacuum_state.escaped_schema_name;
    let source_db_id = vacuum_state.source_db_id;

    loop {
        let current_phase = std::mem::take(&mut vacuum_state.phase);

        match current_phase {
            VacuumIntoOpPhase::Init => {
                // Check if we're in a transaction
                // as vacuum cannot be run inside a transaction
                if !program.connection.auto_commit.load(Ordering::SeqCst) {
                    return Err(LimboError::TxError(
                        "cannot VACUUM INTO from within a transaction".to_string(),
                    )
                    .into());
                }
                // This VACUUM INTO statement itself is the one active root
                // statement. Any count other than 1 means some other
                // top-level statement on the same connection is still active.
                if program
                    .connection
                    .n_active_root_statements
                    .load(Ordering::SeqCst)
                    != 1
                {
                    return Err(LimboError::TxError(
                        "cannot VACUUM - SQL statements in progress".to_string(),
                    )
                    .into());
                }

                // we always vacuum into a new file, so check if it exists
                if std::path::Path::new(dest_path).exists() {
                    return Err(LimboError::ParseError(format!(
                        "output file already exists: {dest_path}"
                    ))
                    .into());
                }

                // Pin source metadata before building the output database. The
                // BEGIN and pragma helpers here are blocking convenience wrappers;
                // async work starts with the schema scan in vacuum_target_build_step.
                let source_db = program.connection.get_source_database(source_db_id);
                program.connection.execute("BEGIN")?;
                state.auto_txn_cleanup = TxnCleanup::RollbackTxn;
                let page_size: u32 = extract_pragma_int(
                    &program
                        .connection
                        .pragma_query(&format!("\"{escaped_schema_name}\".page_size"))?,
                    "page_size",
                )?;
                let source_pager = program
                    .connection
                    .get_pager_from_database_index(&source_db_id)?;
                let source_auto_vacuum_mode = source_pager.get_auto_vacuum_mode();
                reject_unsupported_vacuum_auto_vacuum_mode(source_auto_vacuum_mode)?;
                let header_meta = if let Some(mv_store) =
                    program.connection.mv_store_for_db(source_db_id)
                {
                    let tx_id = program.connection.get_mv_tx_id_for_db(source_db_id);
                    mv_store.with_header(VacuumDbHeaderMeta::from_source_header, tx_id.as_ref())?
                } else {
                    source_pager.io.block(|| {
                        source_pager.with_header(VacuumDbHeaderMeta::from_source_header)
                    })?
                };

                let reserved_space: u8 = if !is_attached_db(source_db_id) {
                    // For main or temp db prefer cached value to avoid blocking I/O
                    match program.connection.get_reserved_bytes() {
                        Some(val) => val,
                        None => {
                            let pager = program.connection.pager.load();
                            pager
                                .io
                                .block(|| pager.with_header(|header| header.reserved_space))?
                        }
                    }
                } else {
                    // For attached db read from its own pager
                    let pager = program
                        .connection
                        .get_pager_from_database_index(&source_db_id)?;
                    pager
                        .io
                        .block(|| pager.with_header(|header| header.reserved_space))?
                };

                // Mirror source feature flags to the output so schema replay
                // can resolve custom types, generated columns, vtab modules, etc.
                let output_opts = vacuum_target_opts_from_source(&source_db);

                // Always use PlatformIO for the output file, even if source
                // is in-memory. This ensures VACUUM INTO writes to disk.
                let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
                let page_codec = source_pager.page_codec_external();
                let output_db = match &page_codec {
                    Some(codec) => crate::Database::open(
                        io,
                        dest_path,
                        crate::OpenOptions::new(source_db.dialect())
                            .flags(OpenFlags::Create)
                            .db_opts(output_opts)
                            .page_codec(codec.clone()),
                    )?,
                    None => crate::Database::open_file_with_flags(
                        io,
                        dest_path,
                        OpenFlags::Create,
                        output_opts,
                        None,
                        source_db.dialect(),
                    )?,
                };
                let output_conn = match page_codec {
                    Some(codec) => output_db.connect_with_page_codec(codec)?,
                    None => output_db.connect()?,
                };
                output_conn.reset_page_size(page_size)?;
                // set reserved_space on output to match source
                // this is important for databases using encryption or checksums
                // must be set before page 1 is allocated (before any schema operations)
                output_conn.set_reserved_bytes(reserved_space)?;

                mirror_symbols(&program.connection, &output_conn);
                let source_custom_types = capture_custom_types(&program.connection, source_db_id);

                let config = VacuumTargetBuildConfig {
                    source_conn: program.connection.clone(),
                    escaped_schema_name: escaped_schema_name.clone(),
                    source_db_id,
                    header_meta,
                    source_custom_types,
                    target_mvcc_enabled: source_db.mvcc_enabled(),
                    target_auto_vacuum_mode: source_auto_vacuum_mode,
                    copy_mvcc_metadata_table: false,
                };

                vacuum_state._output_db = Some(output_db);
                vacuum_state.target_build_config = Some(config);
                vacuum_state.target_build_context =
                    Some(VacuumTargetBuildContext::new(output_conn));
                vacuum_state.phase = VacuumIntoOpPhase::Build;
                continue;
            }

            VacuumIntoOpPhase::Build => {
                let config = vacuum_state
                    .target_build_config
                    .as_ref()
                    .expect("VacuumTargetBuildConfig must be set in Build state");
                let target_build_context = vacuum_state
                    .target_build_context
                    .as_mut()
                    .expect("VacuumTargetBuildContext must be set in Build state");

                match vacuum_target_build_step(config, target_build_context)? {
                    crate::IOResult::Done(()) => {
                        vacuum_state.phase = VacuumIntoOpPhase::FinalizeOutput;
                        continue;
                    }
                    crate::IOResult::IO(io) => {
                        vacuum_state.phase = VacuumIntoOpPhase::Build;
                        return Ok(InsnFunctionStepResult::IO(io));
                    }
                }
            }

            VacuumIntoOpPhase::FinalizeOutput => {
                let target_build_context = vacuum_state
                    .target_build_context
                    .as_ref()
                    .expect("VacuumTargetBuildContext must be set in FinalizeOutput state");
                crate::vdbe::vacuum::finalize_vacuum_into_output(target_build_context)?;
                vacuum_state.phase = VacuumIntoOpPhase::Done;
                continue;
            }

            VacuumIntoOpPhase::Done => {
                // Commit the source transaction started in Init.
                program.connection.execute("COMMIT")?;
                state.auto_txn_cleanup = TxnCleanup::None;

                state.pc += 1;
                return Ok(InsnFunctionStepResult::Step);
            }
        }
    }
}

/// In-place VACUUM - compact the database via target build + direct-WAL
/// copy-back. The opcode owns the source transaction lifecycle.
pub fn op_vacuum(
    program: &Program,
    state: &mut ProgramState,
    insn: &Insn,
    _pager: &Arc<Pager>,
) -> InsnResult {
    load_insn!(Vacuum { db }, insn);

    if matches!(state.op_vacuum_state, VacuumOpState::None) {
        state.op_vacuum_state = VacuumOpState::InPlace(Box::new(VacuumInPlaceOpContext::new(*db)));
    }

    let VacuumOpState::InPlace(vacuum_state) = &mut state.op_vacuum_state else {
        return Err(LimboError::InternalError(
            "VACUUM resumed with incompatible VACUUM state".to_string(),
        )
        .into());
    };

    match vacuum_state.step(&program.connection) {
        Ok(IOResult::Done(())) => {
            state.op_vacuum_state = VacuumOpState::None;
            state.pc += 1;
            Ok(InsnFunctionStepResult::Step)
        }
        Ok(IOResult::IO(io)) => Ok(InsnFunctionStepResult::IO(io)),
        Err(err) => {
            let VacuumOpState::InPlace(vacuum_state) = std::mem::take(&mut state.op_vacuum_state)
            else {
                unreachable!("invalid state, we are inside vacuum op");
            };
            if let Err(cleanup_err) = cleanup_op_vacuum_in_place(&program.connection, vacuum_state)
            {
                tracing::error!("VACUUM cleanup failed after error: {cleanup_err}");
            }
            Err(err)
        }
    }
}

/// Clean up in-place VACUUM state on error or abort. Rolls back the source
/// transaction if it was acquired, restores connection state, and drops
/// temp resources.
fn cleanup_op_vacuum_in_place(
    connection: &Arc<Connection>,
    vacuum_state: Box<VacuumInPlaceOpContext>,
) -> Result<()> {
    vacuum_state.cleanup(connection)
}

fn with_header<T, F>(
    pager: &Pager,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    db: usize,
    f: F,
) -> IOResultOr<T>
where
    F: Fn(&DatabaseHeader) -> T,
{
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id_for_db(db);
        mv_store
            .with_header(f, tx_id.as_ref())
            .map(IOResult::Done)
            .map_err(Into::into)
    } else {
        pager.with_header(&f)
    }
}

pub fn with_header_mut<T, F>(
    pager: &Pager,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    db: usize,
    f: F,
) -> IOResultOr<T>
where
    F: Fn(&mut DatabaseHeader) -> T,
{
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id_for_db(db);
        mv_store
            .with_header_mut(f, tx_id.as_ref())
            .map(IOResult::Done)
            .map_err(Into::into)
    } else {
        pager.with_header_mut(&f)
    }
}

fn get_schema_cookie(
    pager: &Arc<Pager>,
    mv_store: Option<&Arc<MvStore>>,
    program: &Program,
    db: usize,
) -> IOResultOr<u32> {
    if let Some(mv_store) = mv_store {
        let tx_id = program.connection.get_mv_tx_id_for_db(db);
        mv_store
            .with_header(|header| header.schema_cookie.get(), tx_id.as_ref())
            .map(IOResult::Done)
            .map_err(Into::into)
    } else {
        pager.get_schema_cookie()
    }
}

/// A root page in MVCC might still be marked as negative in schema. On restart it is automatically transformed to positive but in other cases
/// we need to map it to positive if we can in case checkpoint happened.
fn maybe_transform_root_page_to_positive(mvcc_store: Option<&Arc<MvStore>>, root_page: i64) -> i64 {
    if let Some(mvcc_store) = mvcc_store {
        if root_page < 0 {
            mvcc_store.get_real_table_id(root_page)
        } else {
            root_page
        }
    } else {
        root_page
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alloc::vec;
    use crate::translate::collate::CollationSeq;
    use crate::vdbe::BranchOffset;
    use crate::SqliteDialect;
    use crate::{Database, DatabaseOpts, MemoryIO, IO};

    fn prepare_test_statement() -> Statement {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.prepare("SELECT 1;").unwrap()
    }

    fn make_spilled_hash_table() -> (HashTable, crate::alloc::Vec<Value>, usize) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let config = HashTableConfig {
            initial_buckets: 4,
            mem_budget: 1024,
            num_keys: 1,
            collations: vec![CollationSeq::Binary],
            temp_store: crate::TempStore::Default,
            track_matched: false,
            ..Default::default()
        };
        let mut ht = HashTable::new(config, io).unwrap();

        for i in 0..1024 {
            match ht
                .insert(vec![Value::from_i64(i)], i, vec![], None)
                .unwrap()
            {
                IOResult::Done(()) => {}
                IOResult::IO(_) => panic!("memory IO should complete synchronously"),
            }
        }

        match ht.finalize_build(None).unwrap() {
            IOResult::Done(()) => {}
            IOResult::IO(_) => panic!("memory IO should complete synchronously"),
        }
        assert!(ht.has_spilled(), "test requires spilled hash table");

        let probe_key = (0..1024)
            .map(|i| vec![Value::from_i64(i)])
            .find(|key| {
                let partition_idx = ht.partition_for_keys(key).unwrap();
                !ht.is_partition_loaded(partition_idx)
            })
            .expect("expected an unloaded spilled partition");
        let partition_idx = ht.partition_for_keys(&probe_key).unwrap();

        (ht, probe_key, partition_idx)
    }

    /// test to check that vacuum into connection state is reset if it is
    /// interrupted mid way
    #[test]
    fn test_vacuum_into_busy_after_source_begin_rolls_back_source_txn() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)").unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();

        let source_txn_progress_calls = Arc::new(AtomicUsize::new(0));
        let did_interrupt = Arc::new(AtomicBool::new(false));
        let conn_for_progress = conn.clone();
        let source_txn_progress_calls_for_handler = source_txn_progress_calls.clone();
        let did_interrupt_for_handler = did_interrupt.clone();
        conn.set_progress_handler(
            1,
            Some(Box::new(move || {
                if !conn_for_progress.get_auto_commit() {
                    let calls =
                        source_txn_progress_calls_for_handler.fetch_add(1, Ordering::SeqCst);
                    calls >= 10 && !did_interrupt_for_handler.swap(true, Ordering::SeqCst)
                } else {
                    false
                }
            })),
        );

        let dest_dir = tempfile::TempDir::new().unwrap();
        let dest_path = dest_dir.path().join("busy_vacuum.db");
        let dest_path = dest_path.to_str().expect("temp path should be UTF-8");
        let mut stmt = conn.prepare(format!("VACUUM INTO '{dest_path}'")).unwrap();
        let step = stmt.step().unwrap();
        conn.set_progress_handler(0, None);

        assert!(
            matches!(step, StepResult::Busy),
            "progress interruption inside VACUUM INTO should surface as Busy, got {step:?}"
        );
        assert!(
            source_txn_progress_calls.load(Ordering::SeqCst) > 10,
            "test should interrupt after VACUUM INTO opens the source transaction"
        );
        assert!(
            did_interrupt.load(Ordering::SeqCst),
            "progress handler should have interrupted VACUUM INTO exactly once"
        );
        assert!(
            conn.get_auto_commit(),
            "Busy cleanup should roll back the source transaction before returning"
        );
    }

    /// same like `test_vacuum_into_busy_after_source_begin_rolls_back_source_txn`
    /// but for attached dbs
    #[test]
    fn test_cleanup_vacuum_into_rolls_back_attached_only_source_txn() {
        let dir = tempfile::TempDir::new().unwrap();
        let main_path = dir.path().join("vacuum-into-cleanup-main.db");
        let attached_path = dir.path().join("vacuum-into-cleanup-attached.db");

        let io: Arc<dyn IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open_file_with_flags(
            io,
            main_path.to_str().unwrap(),
            OpenFlags::Create,
            DatabaseOpts::new().with_attach(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        conn.execute(format!(
            "ATTACH DATABASE '{}' AS att",
            attached_path.display()
        ))
        .unwrap();
        conn.execute("CREATE TABLE att.t(x)").unwrap();
        conn.execute("INSERT INTO att.t VALUES (1)").unwrap();

        conn.execute("BEGIN").unwrap();
        let attached_db_id = conn.get_database_id_by_name("att").unwrap();
        let attached_pager = conn.get_pager_from_database_index(&attached_db_id).unwrap();
        attached_pager.begin_read_tx().unwrap();

        assert!(
            !conn.pager.load().holds_read_lock(),
            "attached-only cleanup regression requires the main pager to stay lock-free"
        );
        assert!(
            attached_pager.holds_read_lock(),
            "attached source pager should hold the pinned read snapshot"
        );

        let mut state = ProgramState::new(0, 0);
        state.auto_txn_cleanup = TxnCleanup::RollbackTxn;

        cleanup_op_vacuum_into(&conn, &mut state, Box::default()).unwrap();

        assert!(
            conn.get_auto_commit(),
            "cleanup should restore auto-commit without going through SQL ROLLBACK"
        );
        assert_eq!(state.auto_txn_cleanup, TxnCleanup::None);
        assert!(
            !attached_pager.holds_read_lock(),
            "cleanup should release the attached source read snapshot"
        );

        conn.execute("INSERT INTO att.t VALUES (2)").unwrap();
        let mut stmt = conn.prepare("SELECT COUNT(*) FROM att.t").unwrap();
        let mut count = 0_i64;
        stmt.run_with_row_callback(|row| {
            count = row.get(0)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_savepoint_loads_evicted_attached_header_before_mirroring() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            "savepoint-main.db",
            OpenFlags::Create,
            DatabaseOpts::new().with_attach(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("ATTACH 'savepoint-aux.db' AS aux").unwrap();
        conn.execute("CREATE TABLE aux.t(x)").unwrap();

        let attached_db_id = conn.get_database_id_by_name("aux").unwrap();
        let attached_pager = conn.get_pager_from_database_index(&attached_db_id).unwrap();
        attached_pager.begin_read_tx().unwrap();
        attached_pager.clear_page_cache(false);

        conn.execute("SAVEPOINT s").unwrap();
        conn.execute("RELEASE s").unwrap();
    }

    #[test]
    fn test_in_place_vacuum_succeeds_and_releases_source_locks() {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            "in-place-vacuum-design-b.db",
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        for i in 0..128 {
            conn.execute(format!("INSERT INTO t VALUES ({i}, 'value-{i}')"))
                .unwrap();
        }
        conn.execute("DELETE FROM t WHERE id % 2 = 0").unwrap();

        conn.execute("VACUUM").unwrap();

        let mut stmt = conn
            .prepare("SELECT count(*), coalesce(sum(id), 0) FROM t")
            .unwrap();
        let mut count = 0_i64;
        let mut sum = 0_i64;
        stmt.run_with_row_callback(|row| {
            count = row.get(0)?;
            sum = row.get(1)?;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 64);
        assert_eq!(sum, (1..128).step_by(2).sum::<i64>());
        assert!(conn.get_auto_commit());

        let pager = conn.pager.load();
        assert!(!pager.holds_read_lock());
        assert!(!pager.holds_write_lock());
    }

    #[test]
    fn test_in_place_vacuum_busy_before_copyback_restores_source_txn() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            "in-place-vacuum-busy-before-copyback.db",
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)")
            .unwrap();
        for i in 0..32 {
            conn.execute(format!("INSERT INTO t VALUES ({i}, 'value-{i}')"))
                .unwrap();
        }

        let did_interrupt = Arc::new(AtomicBool::new(false));
        let conn_for_progress = conn.clone();
        let did_interrupt_for_handler = did_interrupt.clone();
        conn.set_progress_handler(
            1,
            Some(Box::new(move || {
                !conn_for_progress.get_auto_commit()
                    && !did_interrupt_for_handler.swap(true, Ordering::SeqCst)
            })),
        );

        let mut stmt = conn.prepare("VACUUM").unwrap();
        let step = stmt.step().unwrap();
        conn.set_progress_handler(0, None);

        assert!(
            matches!(step, StepResult::Busy),
            "progress interruption inside in-place VACUUM should surface as Busy, got {step:?}"
        );
        assert!(
            did_interrupt.load(Ordering::SeqCst),
            "test should interrupt after in-place VACUUM opens the source snapshot"
        );
        assert!(
            conn.get_auto_commit(),
            "Busy cleanup should restore auto-commit before returning"
        );
        let pager = conn.pager.load();
        assert!(!pager.holds_read_lock());
        assert!(!pager.holds_write_lock());
    }

    #[test]
    fn test_hash_probe_rejects_unloaded_spilled_partition_without_probe_rowid() {
        let stmt = prepare_test_statement();
        let (ht, probe_key, _) = make_spilled_hash_table();

        let mut state = ProgramState::new(2, 0);
        state.hash_tables.insert(7, ht);
        state.set_register(0, Register::Value(probe_key[0].clone()));

        let insn = Insn::HashProbe {
            hash_table_id: 7,
            key_start_reg: 0,
            num_keys: 1,
            dest_reg: 1,
            target_pc: BranchOffset::Offset(99),
            payload_dest_reg: None,
            num_payload: 0,
            probe_rowid_reg: None,
        };

        let err = match op_hash_probe(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
            Ok(_) => {
                panic!("HashProbe should reject grace-only probing without a loaded partition")
            }
            Err(err) => err,
        };

        assert!(
            matches!(*err, LimboError::InternalError(ref message) if message.contains("probe_rowid_reg=None is grace-only")),
            "unexpected error: {err:?}"
        );
        assert_eq!(state.pc, 0, "pc should not advance on invariant violation");
        assert!(
            state.active_op_state.hash_probe().is_none(),
            "HashProbe should not stash resumable state for the removed fallback path"
        );
    }

    #[test]
    fn test_hash_probe_allows_grace_style_probe_after_partition_preload() {
        let stmt = prepare_test_statement();
        let (mut ht, probe_key, partition_idx) = make_spilled_hash_table();

        loop {
            match ht.load_spilled_partition(partition_idx, None).unwrap() {
                IOResult::Done(()) => break,
                IOResult::IO(_) => continue,
            }
        }
        assert!(
            ht.is_partition_loaded(partition_idx),
            "grace-style probe requires the build partition to be resident"
        );

        let expected_rowid = match &probe_key[0] {
            Value::Numeric(Numeric::Integer(i)) => *i,
            ref other => panic!("expected integer probe key, got {other:?}"),
        };

        let mut state = ProgramState::new(2, 0);
        state.hash_tables.insert(7, ht);
        state.set_register(0, Register::Value(probe_key[0].clone()));

        let insn = Insn::HashProbe {
            hash_table_id: 7,
            key_start_reg: 0,
            num_keys: 1,
            dest_reg: 1,
            target_pc: BranchOffset::Offset(99),
            payload_dest_reg: None,
            num_payload: 0,
            probe_rowid_reg: None,
        };

        let step = op_hash_probe(stmt.get_program(), &mut state, &insn, stmt.get_pager())
            .expect("preloaded grace probe should succeed");
        assert!(matches!(step, InsnFunctionStepResult::Step));
        assert_eq!(state.pc, 1, "matching probe should fall through");
        assert_eq!(
            state.get_register(1).get_value(),
            &Value::from_i64(expected_rowid),
            "HashProbe should return the matching build rowid"
        );
    }

    #[test]
    fn test_decr_jump_zero_non_integer_register_returns_error() {
        let stmt = prepare_test_statement();

        let mut state = ProgramState::new(1, 0);
        state.set_register(0, Register::Value(Value::Text("not-an-int".into())));

        let insn = Insn::DecrJumpZero {
            reg: 0,
            target_pc: crate::vdbe::BranchOffset::Offset(1),
        };

        let err = match op_decr_jump_zero(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
            Ok(_) => panic!("non-integer register must fail"),
            Err(err) => err,
        };
        assert!(matches!(*err, LimboError::Constraint(message) if message == "datatype mismatch"));
        assert_eq!(state.pc, 0);
    }

    #[test]
    fn test_make_record_overlapping_dest_returns_error() {
        let stmt = prepare_test_statement();

        let mut state = ProgramState::new(3, 0);
        for i in 0..3 {
            state.set_register(i, Register::Value(Value::from_i64(i as i64)));
        }
        let insn = Insn::MakeRecord {
            start_reg: 0,
            count: 3,
            dest_reg: 1,
            index_name: None,
            affinity_str: None,
        };

        let err = match op_make_record(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
            Ok(_) => panic!("overlapping destination register must fail"),
            Err(err) => err,
        };
        assert!(
            matches!(*err, LimboError::InternalError(ref message) if message.contains("overlaps its source range")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_sorter_data_unpaired_content_register_returns_error() {
        let stmt = prepare_test_statement();

        // Pseudo cursor 1 exposes register 2, but SorterData targets register 3.
        let mut state = ProgramState::new(4, 2);
        state.cursors[1] = Some(Cursor::new_pseudo(crate::pseudo::PseudoCursor::new(2)));
        let insn = Insn::SorterData {
            cursor_id: 0,
            dest_reg: 3,
            pseudo_cursor: 1,
        };

        let err = match op_sorter_data(stmt.get_program(), &mut state, &insn, stmt.get_pager()) {
            Ok(_) => panic!("unpaired content register must fail"),
            Err(err) => err,
        };
        assert!(
            matches!(*err, LimboError::InternalError(ref message) if message.contains("content register")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_execute_sqlite_version() {
        assert_eq!(
            execute_sqlite_version(),
            crate::dialect::sqlite::SQLITE_VERSION
        );
    }

    #[test]
    fn test_execute_turso_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_turso_version(version_integer), expected);
    }

    #[test]
    fn test_ascii_whitespace_is_trimmed() {
        // Regular ASCII whitespace SHOULD be trimmed
        let ascii_whitespace_cases = vec![
            (" 12", 12i64),            // space
            ("12 ", 12i64),            // trailing space
            (" 12 ", 12i64),           // both sides
            ("\t42\t", 42i64),         // tab
            ("\n99\n", 99i64),         // newline
            (" \t\n123\r\n ", 123i64), // mixed ASCII whitespace
            ("\x0b12", 12i64),         // leading vertical tab (0x0B)
            ("12\x0b", 12i64),         // trailing vertical tab (0x0B)
        ];

        for (input, expected_int) in ascii_whitespace_cases {
            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Integer);

            match register {
                Register::Value(Value::Numeric(Numeric::Integer(i))) => {
                    assert_eq!(
                        i, expected_int,
                        "String '{input}' should convert to {expected_int}, got {i}"
                    );
                }
                other => {
                    panic!(
                        "String '{input}' should be converted to integer {expected_int}, got {other:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_non_breaking_space_not_trimmed() {
        let test_strings = vec![
            ("12\u{00A0}", "text", 3),   // '12' + non-breaking space (3 chars, 4 bytes)
            ("\u{00A0}12", "text", 3),   // non-breaking space + '12' (3 chars, 4 bytes)
            ("12\u{00A0}34", "text", 5), // '12' + nbsp + '34' (5 chars, 6 bytes)
        ];

        for (input, _expected_type, expected_len) in test_strings {
            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Integer);

            match register {
                Register::Value(Value::Text(t)) => {
                    assert_eq!(
                        t.as_str().chars().count(),
                        expected_len,
                        "String '{input}' should have {expected_len} characters",
                    );
                }
                Register::Value(Value::Numeric(Numeric::Integer(_))) => {
                    panic!("String '{input}' should NOT be converted to integer");
                }
                other => panic!("Unexpected value type: {other:?}"),
            }
        }
    }

    #[test]
    fn test_affinity_keeps_nan_inf_text() {
        let cases = ["nan", "inf"];

        for input in cases {
            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Integer);
            match register {
                Register::Value(Value::Text(t)) => {
                    assert_eq!(t.as_str(), input, "Unexpected conversion for '{input}'");
                }
                other => {
                    panic!("'{input}' should remain text, got {other:?}");
                }
            }

            let mut register = Register::Value(Value::Text(input.into()));
            apply_affinity_char(&mut register, Affinity::Numeric);
            match register {
                Register::Value(Value::Text(t)) => {
                    assert_eq!(t.as_str(), input, "Unexpected conversion for '{input}'");
                }
                other => {
                    panic!("'{input}' should remain text, got {other:?}");
                }
            }
        }
    }

    #[test]
    fn test_init_agg_payload_count() {
        let mut payload = crate::alloc::vec![];
        init_agg_payload(&AggFunc::Count, &mut payload).unwrap();
        assert_eq!(payload.len(), 1);
        assert_eq!(payload[0], Value::from_i64(0));
    }

    #[test]
    fn test_init_agg_payload_sum() {
        let mut payload = crate::alloc::vec![];
        init_agg_payload(&AggFunc::Sum, &mut payload).unwrap();
        assert_eq!(payload.len(), 5);
        assert_eq!(payload[0], Value::Null); // acc
        assert_eq!(payload[1], Value::from_f64(0.0)); // r_err
        assert_eq!(payload[2], Value::from_i64(0)); // approx
        assert_eq!(payload[3], Value::from_i64(0)); // ovrfl
        assert_eq!(payload[4], Value::from_i64(0)); // count
    }

    #[test]
    fn test_init_agg_payload_avg() {
        let mut payload = crate::alloc::vec![];
        init_agg_payload(&AggFunc::Avg, &mut payload).unwrap();
        assert_eq!(payload.len(), 3);
        assert_eq!(payload[0], Value::from_f64(0.0)); // sum
        assert_eq!(payload[1], Value::from_f64(0.0)); // r_err
        assert_eq!(payload[2], Value::from_i64(0)); // count
    }

    #[test]
    fn test_update_count_skips_null() {
        let mut payload = crate::alloc::vec![Value::from_i64(5)];
        update_agg_payload(
            &AggFunc::Count,
            &Value::Null,
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(5)); // unchanged
    }

    #[test]
    fn test_update_count_increments() {
        let mut payload = crate::alloc::vec![Value::from_i64(5)];
        update_agg_payload(
            &AggFunc::Count,
            &Value::from_i64(42),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(6));
    }

    #[test]
    fn test_update_sum_integers() {
        let mut payload = crate::alloc::vec![
            Value::Null,
            Value::from_f64(0.0),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(0),
        ];
        update_agg_payload(
            &AggFunc::Sum,
            &Value::from_i64(10),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(10));

        update_agg_payload(
            &AggFunc::Sum,
            &Value::from_i64(5),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(15));
    }

    #[test]
    fn test_update_sum_null_is_skipped() {
        let mut payload = crate::alloc::vec![
            Value::from_i64(10),
            Value::from_f64(0.0),
            Value::from_i64(0),
            Value::from_i64(0),
            Value::from_i64(1),
        ];
        update_agg_payload(
            &AggFunc::Sum,
            &Value::Null,
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(10)); // unchanged
    }

    #[test]
    fn test_update_min_max() {
        let mut payload = crate::alloc::vec![Value::Null];
        // First value sets the min/max
        update_agg_payload(
            &AggFunc::Min,
            &Value::from_i64(5),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(5));

        // Smaller value updates min
        update_agg_payload(
            &AggFunc::Min,
            &Value::from_i64(3),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(3));

        // Larger value doesn't update min
        update_agg_payload(
            &AggFunc::Min,
            &Value::from_i64(10),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_i64(3));
    }

    #[test]
    fn test_update_avg() {
        // Payload: [sum, r_err, count]
        let mut payload = crate::alloc::vec![
            Value::from_f64(0.0),
            Value::from_f64(0.0),
            Value::from_i64(0),
        ];
        update_agg_payload(
            &AggFunc::Avg,
            &Value::from_i64(10),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_f64(10.0));
        assert_eq!(payload[2], Value::from_i64(1));

        update_agg_payload(
            &AggFunc::Avg,
            &Value::from_i64(20),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        assert_eq!(payload[0], Value::from_f64(30.0));
        assert_eq!(payload[2], Value::from_i64(2));
    }

    #[test]
    fn test_finalize_count() {
        let payload = vec![Value::from_i64(42)];
        let result = finalize_agg_payload(&AggFunc::Count, &payload).unwrap();
        assert_eq!(result, Value::from_i64(42));
    }

    #[test]
    fn test_finalize_avg() {
        // Payload: [sum, r_err, count]
        let payload = vec![
            Value::from_f64(30.0),
            Value::from_f64(0.0),
            Value::from_i64(3),
        ];
        let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
        assert_eq!(result, Value::from_f64(10.0));
    }

    #[test]
    fn test_finalize_avg_empty() {
        // Payload: [sum, r_err, count]
        let payload = vec![
            Value::from_f64(0.0),
            Value::from_f64(0.0),
            Value::from_i64(0),
        ];
        let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_finalize_avg_large_integers() {
        let mut payload = crate::alloc::vec![
            Value::from_f64(0.0),
            Value::from_f64(0.0),
            Value::from_i64(0),
        ];

        update_agg_payload(
            &AggFunc::Avg,
            &Value::from_i64(9007199254740994),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();
        update_agg_payload(
            &AggFunc::Avg,
            &Value::from_i64(-9007199254740993),
            None,
            &mut payload,
            CollationSeq::Binary,
            || Ok(None),
        )
        .unwrap();

        let result = finalize_agg_payload(&AggFunc::Avg, &payload).unwrap();
        assert_eq!(result, Value::from_f64(0.5));
    }

    #[test]
    fn test_array_agg_accumulates_correctly() {
        // Verify that array_agg produces correct results when accumulating
        // multiple values. Uses the direct payload approach (O(1) per row).
        let mut payload = crate::alloc::vec![];
        init_agg_payload(&AggFunc::ArrayAgg, &mut payload).unwrap();

        // Simulate how AggStep accumulates values directly into the payload Vec.
        for i in 0..100 {
            let count = payload[0].as_int().unwrap_or(0) as usize;
            payload[0] = Value::from_i64((count + 1) as i64);
            payload.push(Value::from_i64(i));
        }

        let result = finalize_agg_payload(&AggFunc::ArrayAgg, &payload).unwrap();
        let blob = match &result {
            Value::Blob(b) => b,
            _ => panic!("Expected Blob, got {result:?}"),
        };
        let elements = array_values_from_blob(blob).unwrap();
        assert_eq!(elements.len(), 100);
        for (i, elem) in elements.iter().enumerate() {
            assert_eq!(*elem, Value::from_i64(i as i64));
        }
    }

    #[test]
    fn test_array_agg_zero_rows_produces_valid_result() {
        // array_agg with zero rows should return NULL, matching PostgreSQL.
        // The result must not be an invalid empty blob that crashes on decode.
        let mut payload = crate::alloc::vec![];
        init_agg_payload(&AggFunc::ArrayAgg, &mut payload).unwrap();
        // No values accumulated — count stays 0.
        let result = finalize_agg_payload(&AggFunc::ArrayAgg, &payload).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_array_agg_finalize_bounds_check() {
        // If payload[0] count is larger than the actual payload length,
        // finalize should return an error rather than panicking.
        let payload = vec![Value::from_i64(999)]; // claims 999 elements but has none
        let result = finalize_agg_payload(&AggFunc::ArrayAgg, &payload);
        assert!(
            result.is_err(),
            "Should error on count exceeding payload length"
        );
    }

    #[test]
    fn test_negate_blob_subscript_invalid_utf8_no_panic() {
        // Reproduces fuzzer bug at seed 27035.
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file_with_flags(
            io,
            ":memory:",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            conn.execute("SELECT -X'18530E218A8D2D8D8F7456733370E68357745AFE13FC1B94751B77FCB00D0CAD971017936278BFF49BB4C8BD47F874ECA5226D3A433B7DFCD18661673598CED1FDB30A795F6F25'[2]")
        }));
        assert!(
            result.is_ok(),
            "Negating a blob subscript with invalid UTF-8 text should not panic"
        );
    }
}
