//! The virtual database engine (VDBE).
//!
//! The VDBE is a register-based virtual machine that execute bytecode
//! instructions that represent SQL statements. When an application prepares
//! an SQL statement, the statement is compiled into a sequence of bytecode
//! instructions that perform the needed operations, such as reading or
//! writing to a b-tree, sorting, or aggregating data.
//!
//! The instruction set of the VDBE is similar to SQLite's instruction set,
//! but with the exception that bytecodes that perform I/O operations are
//! return execution back to the caller instead of blocking. This is because
//! Turso is designed for applications that need high concurrency such as
//! serverless runtimes. In addition, asynchronous I/O makes storage
//! disaggregation easier.
//!
//! You can find a full list of SQLite opcodes at:
//!
//! https://www.sqlite.org/opcode.html

use crate::alloc::{TryReserveError, TursoFromIterator};
use crate::translate::plan::BitSet;
use crate::types::IOResultOr;
use crate::types::{Extendable, Text, ValueBlob};
use crate::{turso_assert, turso_assert_ne, turso_debug_assert, NonNan};
pub mod affinity;
pub mod array;
#[cfg(test)]
mod blob_io_tests;
pub mod bloom_filter;
pub mod builder;
pub mod execute;
pub mod explain;
#[allow(dead_code)]
pub mod hash_table;
pub mod insn;
pub mod metrics;
pub mod rowset;
pub mod sorter;
#[cfg(test)]
mod statement_lifecycle_tests;
pub mod vacuum;
pub mod value;
#[allow(unused_imports)] // for benchmarks
pub use crate::translate::collate::CollationSeq;
use crate::{
    alloc::{DynAllocator, TryClone},
    error::LimboError,
    function::FuncCtx,
    mvcc::{database::CommitStateMachine, MvccClock},
    numeric::Numeric,
    return_if_io,
    schema::Trigger,
    state_machine::StateMachine,
    translate::plan::TableReferences,
    types::{IOCompletions, IOResult},
    vdbe::{
        execute::{
            OpAttachState, OpClearBtreeState, OpColumnState, OpDeleteState, OpDeleteSubState,
            OpDestroyState, OpIdxInsertState, OpInitCdcVersionState, OpInsertState,
            OpInsertSubState, OpJournalModeState, OpNewRowidState, OpNoConflictState,
            OpParseSchemaState, OpProgramState, OpRowIdState, OpSeekState, OpTransactionState,
            VacuumIntoOpContext,
        },
        hash_table::HashTable,
        metrics::StatementMetrics,
        vacuum::VacuumInPlaceOpContext,
    },
    ValueRef, WalAutoActions,
};
use smallvec::SmallVec;

#[cfg(feature = "json")]
use crate::json::JsonCacheCell;
use crate::sync::RwLock;
use crate::{
    storage::pager::Pager,
    translate::plan::ResultSetColumn,
    types::{AggContext, Cursor, ImmutableRecord, RecordBuf, Value},
    vdbe::{builder::CursorType, insn::Insn},
};
use crate::{
    AtomicBool, CaptureDataChangesInfo, Connection, MvStore, Result, Statement, TransactionState,
};
use branches::{mark_unlikely, unlikely};
use builder::{CursorKey, QueryMode};
use execute::{
    InsnFunction, InsnFunctionStepResult, OpIdxDeleteState, OpIntegrityCheckState,
    OpOpenEphemeralState,
};
use turso_parser::ast::{EqpFormat, ResolveType};

use crate::io::TempFile;
use crate::vdbe::bloom_filter::BloomFilter;
use crate::vdbe::rowset::RowSet;
use explain::{
    insn_to_row_with_comment, ExplainInfo, EXPLAIN_COLUMNS, EXPLAIN_QUERY_PLAN_COLUMNS,
    EXPLAIN_QUERY_PLAN_JSON_COLUMNS,
};
use std::{
    collections::HashMap,
    num::NonZero,
    ops::Deref,
    sync::{
        atomic::{AtomicI64, AtomicIsize, Ordering},
        Arc,
    },
    task::Waker,
};
use tracing::{instrument, Level};

type MvccCommitStateMachine = CommitStateMachine<MvccClock, DynAllocator>;

/// State machine for committing view deltas with I/O handling
#[derive(Debug, Clone)]
pub enum ViewDeltaCommitState {
    NotStarted,
    Processing {
        views: Vec<String>, // view names (all materialized views have storage)
        current_index: usize,
    },
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Represents a target for a jump instruction.
/// Stores 32-bit ints to keep the enum word-sized.
pub enum BranchOffset {
    /// A label is a named location in the program.
    /// If there are references to it, it must always be resolved to an Offset
    /// via `ProgramBuilder::preassign_label_to_next_insn` or
    /// `ProgramBuilder::link_label_to_other_label`.
    Label(u32),
    /// An offset is a direct index into the instruction list.
    Offset(InsnReference),
    /// A placeholder is a temporary value to satisfy the compiler.
    /// It must be set later.
    Placeholder,
}

impl BranchOffset {
    /// Returns true if the branch offset is an offset.
    pub fn is_offset(&self) -> bool {
        matches!(self, BranchOffset::Offset(_))
    }

    /// Returns the offset value. Panics if the branch offset is a label or placeholder.
    pub fn as_offset_int(&self) -> InsnReference {
        match self {
            BranchOffset::Label(v) => unreachable!("Unresolved label: {}", v),
            BranchOffset::Offset(v) => *v,
            BranchOffset::Placeholder => unreachable!("Unresolved placeholder"),
        }
    }

    /// Returns the branch offset as a signed integer.
    /// Used in explain output, where we don't want to panic in case we have an unresolved
    /// label or placeholder.
    pub fn as_debug_int(&self) -> i32 {
        match self {
            BranchOffset::Label(v) => *v as i32,
            BranchOffset::Offset(v) => *v as i32,
            BranchOffset::Placeholder => i32::MAX,
        }
    }
}

pub type CursorID = usize;

pub type PageIdx = i64;

// Index of insn in list of insns
type InsnReference = u32;

#[derive(Debug)]
pub enum StepResult {
    Done,
    IO,
    Row,
    Interrupt,
    Busy,
    /// The statement explicitly yielded control back to the caller without any pending I/O.
    /// Stepping again immediately (even in a tight loop) is fine; blocking callers should
    /// still drive the event loop (`io.step()`) between steps so progress that depends on
    /// other threads' I/O is not starved.
    Yield,
    /// The statement asks the caller to wait for `duration` before stepping again,
    /// e.g. because a busy handler decided to retry after a delay. Callers that don't
    /// track time may treat this exactly like `IO`: drive the event loop and step again.
    Sleep {
        duration: std::time::Duration,
    },
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
/// The commit state of the program.
/// There are two states:
/// - Ready: The program is ready to run the next instruction, or has shut down after
///   the last instruction.
/// - Committing: The program is committing a write transaction. It is waiting for the pager to finish flushing the cache to disk,
///   primarily to the WAL, but also possibly checkpointing the WAL to the database file.
enum CommitState {
    Ready,
    Committing,
    /// Committing attached database pagers after main pager commit is done.
    CommittingAttached,
    CommittingMvcc {
        state_machine: StateMachine<Box<MvccCommitStateMachine>>,
    },
    /// Committing MVCC transactions on attached databases after main MVCC commit is done.
    CommittingAttachedMvcc {
        state_machine: StateMachine<Box<MvccCommitStateMachine>>,
        db_id: usize,
        mv_store: Arc<MvStore>,
    },
}

impl CommitState {
    fn cleanup_mvcc_checkpoint_state(&mut self) {
        match self {
            CommitState::CommittingMvcc { state_machine } => {
                state_machine.inner_mut().cleanup_mvcc_checkpoint_state()
            }
            CommitState::CommittingAttachedMvcc { state_machine, .. } => {
                state_machine.inner_mut().cleanup_mvcc_checkpoint_state()
            }
            CommitState::Ready | CommitState::Committing | CommitState::CommittingAttached => {}
        }
    }

    fn cleanup_abandoned_mvcc_commit(&mut self, connection: &Connection) {
        match self {
            CommitState::CommittingAttachedMvcc {
                state_machine,
                db_id: attached_db_id,
                ..
            } if !state_machine.is_finalized() => {
                if connection
                    .database_schemas()
                    .write()
                    .remove(attached_db_id)
                    .is_some()
                {
                    connection.bump_prepare_context_generation();
                }
            }
            CommitState::CommittingMvcc { state_machine } if !state_machine.is_finalized() => {}
            _ => return, // no-op for already-finalized state machines and non-MVCC commit states
        };

        // Replace the live CommitState with Ready so the abandoned state machine
        // drops here. CommitStateMachine::Drop -> cleanup_unfinished_commit ->
        // cleanup_dropped_commit ultimately calls rollback_tx_inner on a tx left
        // in Active/Preparing. Without this, the orphan tx stays Preparing
        // forever — any other transaction that took a commit dependency on it
        // (Hekaton §2.7 speculative read) deadlocks in WaitForDependencies.
        // The locks/exclusive slot the SM acquired are released by the same
        // cleanup path on drop.
        *self = CommitState::Ready;

        connection.rollback_attached_mvcc_txs(true);
        connection.rollback_attached_wal_txns();
        connection.rollback_temp_schema();
        connection.index_methods_on_transaction_rolled_back();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Register {
    Value(Value),
    Aggregate(AggContext),
    Record(ImmutableRecord),
}

impl TryClone for Register {
    type Error = TryReserveError;

    fn try_clone(&self) -> Result<Self, Self::Error> {
        match self {
            Register::Value(value) => Ok(Register::Value(value.try_clone()?)),
            Register::Aggregate(context) => Ok(Register::Aggregate(context.try_clone()?)),
            Register::Record(record) => Ok(Register::Record(ImmutableRecord::copy_payload(
                record.get_payload(),
                RecordBuf::alloc(),
            )?)),
        }
    }

    /// Fallibly copies `source` into this register, reusing the destination's
    /// Value or record allocation when the variants match; see
    /// [Value::try_clone_from].
    #[turso_macros::allocation_site(crate::alloc::ValueBlobAllocationSite::CloneFrom)]
    fn try_clone_from(&mut self, source: &Self) -> Result<(), Self::Error> {
        match (self, source) {
            (Register::Value(dst), Register::Value(src)) => dst.try_clone_from(src)?,
            (Register::Record(dst), Register::Record(src)) => {
                let buf = dst.as_blob_mut();
                buf.clear();
                buf.try_extend(src.get_payload().iter().copied())?;
            }
            (dst, Register::Value(src)) => {
                let mut value = Value::Null;
                value.try_clone_from(src)?;
                *dst = Register::Value(value);
            }
            (dst, Register::Record(src)) => {
                *dst = Register::Record(ImmutableRecord::copy_payload(
                    src.get_payload(),
                    RecordBuf::alloc(),
                )?);
            }
            (dst, Register::Aggregate(src)) => *dst = Register::Aggregate(src.try_clone()?),
        }
        Ok(())
    }
}

impl Register {
    /// Takes the register's spent record buffer for reuse, leaving NULL.
    /// Callers about to overwrite the register use this to recycle its
    /// allocation instead of dropping it.
    #[inline]
    pub fn take_buf(&mut self) -> RecordBuf {
        match std::mem::replace(self, Register::Value(Value::Null)) {
            Register::Record(record) => record.retire(),
            _ => RecordBuf::alloc(),
        }
    }

    /// Fallibly sets the register to a copy of `val`, reusing the register's
    /// existing allocation when possible; see [Value::try_clone_from].
    #[inline]
    pub fn try_clone_value_from(&mut self, val: &Value) -> crate::Result<()> {
        match self {
            Register::Value(v) => v.try_clone_from(val)?,
            _ => {
                let mut value = Value::Null;
                value.try_clone_from(val)?;
                *self = Register::Value(value);
            }
        }
        Ok(())
    }

    #[inline]
    pub const fn is_null(&self) -> bool {
        matches!(self, Register::Value(Value::Null))
    }

    #[inline(always)]
    /// Sets the value of the register to an integer,
    /// reusing the existing Register::Value(Value::Numeric(Numeric::Integer(_))) if possible,
    /// which is faster than always creating a new one.
    pub fn set_int(&mut self, val: i64) {
        match self {
            Register::Value(Value::Numeric(Numeric::Integer(existing))) => {
                *existing = val;
            }
            Register::Value(Value::Numeric(float)) => {
                *float = Numeric::Integer(val);
            }
            Register::Value(other_value_kind) => {
                *other_value_kind = Value::from_i64(val);
            }
            _ => {
                *self = Register::Value(Value::from_i64(val));
            }
        }
    }
    /// Set the value of the register to a floating point,
    /// reusing Register::Value(Value::Numeric(Numeric::Float(_))) if possible.
    #[inline(always)]
    pub fn set_float(&mut self, val: NonNan) {
        match self {
            Register::Value(Value::Numeric(Numeric::Float(existing))) => {
                *existing = val;
            }
            Register::Value(Value::Numeric(integer)) => {
                *integer = Numeric::Float(val);
            }
            Register::Value(other_value_kind) => {
                *other_value_kind = Value::Numeric(Numeric::Float(val));
            }
            _ => {
                *self = Register::Value(Value::Numeric(Numeric::Float(val)));
            }
        }
    }

    /// Set the value of the register to a Text,
    /// reusing Register::Value(Value::Text(_)) buffer if possible.
    #[inline]
    pub fn set_text(&mut self, val: Text) -> Result<()> {
        match self {
            Register::Value(Value::Text(existing)) => {
                existing.do_extend(&val)?;
            }
            Register::Value(other_value_kind) => {
                *other_value_kind = Value::Text(val);
            }
            _ => {
                *self = Register::Value(Value::Text(val));
            }
        }
        Ok(())
    }

    /// Move a blob into the register without copying its allocation.
    #[inline]
    pub fn set_blob(&mut self, val: ValueBlob) -> Result<()> {
        match self {
            Register::Value(other_value_kind) => {
                *other_value_kind = Value::Blob(val);
            }
            _ => {
                *self = Register::Value(Value::Blob(val));
            }
        }
        Ok(())
    }

    // Set the value of the register to NULL,
    // reusing the existing Register::Value(Value::Null) if possible.
    pub fn set_null(&mut self) {
        match self {
            Register::Value(Value::Null) => {}
            Register::Value(other_value_kind) => {
                *other_value_kind = Value::Null;
            }
            _ => {
                *self = Register::Value(Value::Null);
            }
        }
    }

    /// Set the register to a generic Value, attempting to reuse backing allocation if compatible.
    pub fn set_value(&mut self, val: Value) {
        match self {
            Register::Value(v) => {
                *v = val;
            }
            _ => {
                *self = Register::Value(val);
            }
        }
    }
}

/// A row is a the list of registers that hold the values for a filtered row. This row is a pointer, therefore
/// after stepping again, row will be invalidated to be sure it doesn't point to somewhere unexpected.
#[derive(Debug)]
pub struct Row {
    values: *const Register,
    count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxnCleanup {
    None,
    RollbackTxn,
    /// begin_statement was called and statement is participating in an interactive transaction.
    /// If statement is abandoned and/or dropped without an apparent error, we should rollback statement
    /// to previous savepoint.
    RollbackSavepoint,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProgramExecutionState {
    /// No steps of the program was executed
    Init,
    /// Program started execution but didn't reach any terminal state
    Running,
    /// Interrupt requested for the program
    Interrupting,
    /// Terminal state: program interrupted
    Interrupted,
    /// Terminal state: program finished successfully
    Done,
    /// Terminal state: program failed with error
    Failed,
}

impl ProgramExecutionState {
    pub const fn is_running(&self) -> bool {
        matches!(
            self,
            ProgramExecutionState::Interrupting | ProgramExecutionState::Running
        )
    }
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            ProgramExecutionState::Interrupted
                | ProgramExecutionState::Failed
                | ProgramExecutionState::Done
        )
    }
}

/// Re-entrant state for [Insn::HashBuild].
/// Allows HashBuild to resume cleanly after async I/O without re-reading the row.
#[derive(Debug)]
pub struct OpHashBuildState {
    pub key_values: crate::alloc::Vec<Value>,
    pub key_idx: usize,
    pub payload_values: crate::alloc::Vec<Value>,
    pub payload_idx: usize,
    pub rowid: Option<i64>,
    pub cursor_id: CursorID,
    pub hash_table_id: usize,
    pub key_start_reg: usize,
    pub num_keys: usize,
}

/// Re-entrant state for [Insn::HashProbe].
/// Allows HashProbe to resume cleanly after async probe-row buffering I/O.
#[derive(Debug)]
pub struct OpHashProbeState {
    /// Cached probe key values to avoid re-reading from registers
    pub probe_keys: crate::alloc::Vec<Value>,
    /// Hash table register being probed
    pub hash_table_id: usize,
    /// Partition index being loaded (if any)
    pub partition_idx: usize,
    /// Whether the probe row was already buffered for grace processing.
    pub probe_buffered: bool,
}

// repr(u8): with the tag in its own byte, the idle test that every Column
// and RowId runs is one byte compare instead of a niche computation on a
// nested payload.
#[repr(u8)]
enum ActiveOpState {
    None,
    ClearBtree(OpClearBtreeState),
    Delete(OpDeleteState),
    Destroy(OpDestroyState),
    IdxDelete(OpIdxDeleteState),
    IntegrityCheck(OpIntegrityCheckState),
    OpenEphemeral(OpOpenEphemeralState),
    Program(OpProgramState),
    NewRowid(OpNewRowidState),
    IdxInsert(OpIdxInsertState),
    Insert(OpInsertState),
    NoConflict(OpNoConflictState),
    Column(OpColumnState),
    RowId(OpRowIdState),
    Transaction(OpTransactionState),
    Attach(OpAttachState),
    JournalMode(OpJournalModeState),
    ParseSchema(OpParseSchemaState),
    HashBuild(Option<OpHashBuildState>),
    HashProbe(Option<OpHashProbeState>),
    InitCdcVersion(OpInitCdcVersionState),
}

impl std::fmt::Debug for ActiveOpState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            ActiveOpState::None => "None",
            ActiveOpState::ClearBtree(_) => "ClearBtree",
            ActiveOpState::Delete(_) => "Delete",
            ActiveOpState::Destroy(_) => "Destroy",
            ActiveOpState::IdxDelete(_) => "IdxDelete",
            ActiveOpState::IntegrityCheck(_) => "IntegrityCheck",
            ActiveOpState::OpenEphemeral(_) => "OpenEphemeral",
            ActiveOpState::Program(_) => "Program",
            ActiveOpState::NewRowid(_) => "NewRowid",
            ActiveOpState::IdxInsert(_) => "IdxInsert",
            ActiveOpState::Insert(_) => "Insert",
            ActiveOpState::NoConflict(_) => "NoConflict",
            ActiveOpState::Column(_) => "Column",
            ActiveOpState::RowId(_) => "RowId",
            ActiveOpState::Transaction(_) => "Transaction",
            ActiveOpState::Attach(_) => "Attach",
            ActiveOpState::JournalMode(_) => "JournalMode",
            ActiveOpState::ParseSchema(_) => "ParseSchema",
            ActiveOpState::HashBuild(_) => "HashBuild",
            ActiveOpState::HashProbe(_) => "HashProbe",
            ActiveOpState::InitCdcVersion(_) => "InitCdcVersion",
        };
        f.write_str(name)
    }
}

#[derive(Debug, Default)]
struct ActiveOpStateSlot {
    state: ActiveOpState,
}

macro_rules! active_state_accessor {
    ($name:ident, $variant:ident, $ty:ty, $init:expr) => {
        fn $name(&mut self) -> &mut $ty {
            if matches!(self.state, ActiveOpState::None) {
                // None owns nothing, so skip the drop glue of the enum that
                // a plain assignment would run on the old value.
                std::mem::forget(std::mem::replace(
                    &mut self.state,
                    ActiveOpState::$variant($init),
                ));
            }
            match &mut self.state {
                ActiveOpState::$variant(state) => state,
                state => unreachable!(
                    "active opcode state mismatch: expected {}, got {:?}",
                    stringify!($variant),
                    state
                ),
            }
        }
    };
}

impl Default for ActiveOpState {
    fn default() -> Self {
        Self::None
    }
}

impl ActiveOpStateSlot {
    fn clear(&mut self) {
        if !matches!(self.state, ActiveOpState::None) {
            self.state = ActiveOpState::None;
        }
    }

    /// True when no multi-step opcode is suspended. Hot opcodes use this to
    /// bypass the slot entirely on their non-yielding fast path.
    fn is_idle(&self) -> bool {
        matches!(self.state, ActiveOpState::None)
    }

    fn cleanup_journal_mode_checkpoint(&mut self) -> Result<()> {
        match &mut self.state {
            ActiveOpState::JournalMode(state) => state.cleanup_checkpoint(),
            _ => Ok(()),
        }
    }

    active_state_accessor!(
        delete,
        Delete,
        OpDeleteState,
        OpDeleteState {
            sub_state: OpDeleteSubState::MaybeCaptureRecord,
            deleted_record: None,
        }
    );
    active_state_accessor!(
        clear_btree,
        ClearBtree,
        OpClearBtreeState,
        OpClearBtreeState::CreateCursor
    );
    active_state_accessor!(
        destroy,
        Destroy,
        OpDestroyState,
        OpDestroyState::CreateCursor
    );
    active_state_accessor!(
        idx_delete,
        IdxDelete,
        OpIdxDeleteState,
        OpIdxDeleteState::Seeking
    );
    active_state_accessor!(
        integrity_check,
        IntegrityCheck,
        OpIntegrityCheckState,
        OpIntegrityCheckState::Start
    );
    active_state_accessor!(
        open_ephemeral,
        OpenEphemeral,
        OpOpenEphemeralState,
        OpOpenEphemeralState::Start
    );
    active_state_accessor!(program, Program, OpProgramState, OpProgramState::Start);
    active_state_accessor!(new_rowid, NewRowid, OpNewRowidState, OpNewRowidState::Start);
    active_state_accessor!(
        idx_insert,
        IdxInsert,
        OpIdxInsertState,
        OpIdxInsertState::MaybeSeek
    );
    active_state_accessor!(
        insert,
        Insert,
        OpInsertState,
        OpInsertState {
            sub_state: OpInsertSubState::MaybeCaptureRecord,
            old_record: None,
            is_noop_update: false,
        }
    );
    active_state_accessor!(
        no_conflict,
        NoConflict,
        OpNoConflictState,
        OpNoConflictState::Start
    );
    active_state_accessor!(column, Column, OpColumnState, OpColumnState::Start);
    active_state_accessor!(row_id, RowId, OpRowIdState, OpRowIdState::Start);
    active_state_accessor!(
        transaction,
        Transaction,
        OpTransactionState,
        OpTransactionState::Start
    );
    active_state_accessor!(attach, Attach, OpAttachState, OpAttachState::default());
    active_state_accessor!(
        journal_mode,
        JournalMode,
        OpJournalModeState,
        OpJournalModeState::default()
    );
    active_state_accessor!(parse_schema, ParseSchema, OpParseSchemaState, None);
    active_state_accessor!(hash_build, HashBuild, Option<OpHashBuildState>, None);
    active_state_accessor!(hash_probe, HashProbe, Option<OpHashProbeState>, None);
    active_state_accessor!(
        init_cdc_version,
        InitCdcVersion,
        OpInitCdcVersionState,
        None
    );

    /// Take the ParseSchema op state if it is the active one, without
    /// touching (or panicking on) any other live op state. Used by abort
    /// cleanup, which runs regardless of which opcode was executing.
    fn take_parse_schema_if_active(&mut self) -> execute::OpParseSchemaState {
        if let ActiveOpState::ParseSchema(inner) = &mut self.state {
            let taken = inner.take();
            self.state = ActiveOpState::None;
            taken
        } else {
            None
        }
    }

    fn program_ref(&self) -> Option<&OpProgramState> {
        match &self.state {
            ActiveOpState::Program(state) => Some(state),
            _ => None,
        }
    }

    fn program_mut(&mut self) -> Option<&mut OpProgramState> {
        match &mut self.state {
            ActiveOpState::Program(state) => Some(state),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DeferredSeekState {
    pub index_cursor_id: CursorID,
    pub table_cursor_id: CursorID,
}

pub(crate) enum VacuumOpState {
    None,
    IntoFile(Box<VacuumIntoOpContext>),
    InPlace(Box<VacuumInPlaceOpContext>),
}

impl Default for VacuumOpState {
    fn default() -> Self {
        Self::None
    }
}

/// The program state describes the environment in which the program executes.
/// Bookkeeping for an in-flight sequence inner-tx wrap. The
/// `SequenceBeginInnerTx` opcode populates this on the Wrapped path
/// so a subsequent reset / unwind can roll back the inner tx and
/// restore `conn.mv_tx_for_db(db)` even when the statement aborts
/// before reaching `SequenceCommitInnerTx`.
#[derive(Clone)]
pub struct SequenceInnerTxState {
    pub db: usize,
    pub inner_tx_id: crate::mvcc::database::TxID,
    pub saved_outer: Option<(
        crate::mvcc::database::TxID,
        crate::translate::emitter::TransactionMode,
    )>,
}

pub struct ProgramState {
    /// Interrupt/progress-check gate mask for normal_step; re-derived from
    /// the progress handler's interval each time the gate fires.
    check_mask: u64,
    pub io_completions: Option<IOCompletions>,
    pub pc: InsnReference,
    pub(crate) cursors: Vec<Option<Cursor>>,
    /// Immutable execution/storage context captured when each index-method
    /// cursor is first opened for this statement.
    pub(crate) index_method_contexts:
        Vec<Option<std::sync::Arc<crate::index_method::IndexMethodContext>>>,
    /// Index-method cursors explicitly closed by bytecode after their
    /// statement work was prepared (CREATE INDEX closes its backfill cursor
    /// this way). Retain them until commit/rollback decides whether prepared
    /// in-memory state may be published.
    pub(crate) closed_index_method_cursors: Vec<(
        Box<dyn crate::index_method::IndexMethodCursor>,
        std::sync::Arc<crate::index_method::IndexMethodContext>,
    )>,
    /// Resumption coordinates for statement-level index-method finalization.
    pub(crate) index_method_finalize_cursor: usize,
    pub(crate) index_method_finalize_subprogram_keys: Option<Vec<usize>>,
    pub(crate) index_method_finalize_subprogram: usize,
    pub(crate) index_methods_finalized: bool,
    cursor_seqs: Vec<i64>,
    registers: Box<[Register]>,
    /// Trace state: register snapshot for diffing.
    pre_op_registers: Option<Box<[Register]>>,
    pub(crate) result_row: Option<Row>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seeks: Vec<Option<DeferredSeekState>>,
    /// Indicate whether a coroutine has ended for a given yield register.
    /// If an element is present, it means the coroutine with the given register number has ended.
    ended_coroutine: Vec<u32>,
    /// Indicate whether an [Insn::Once] instruction at a given program counter position has already been executed, well, once.
    once: SmallVec<[u32; 4]>,
    pub execution_state: ProgramExecutionState,
    /// Per-execution statement deadline derived from the connection query timeout.
    /// `None` means no timeout.
    pub query_deadline: Option<crate::MonotonicInstant>,
    /// Excludes new root statements while an explicit checkpoint is suspended.
    pub(crate) explicit_checkpoint_guard: Option<crate::connection::ExplicitCheckpointGuard>,
    pub parameters: Vec<Value>,
    commit_state: CommitState,
    /// In-flight commit-state-machine for an autonomous sequence
    /// inner-tx. `Insn::SequenceCommitInnerTx` constructs this on first
    /// entry and drives it one step per opcode call, yielding
    /// `InsnFunctionStepResult::IO` between steps. Cleared on terminal
    /// outcome (Done / Conflict / Err) and on statement reset.
    pub sequence_inner_commit: Option<StateMachine<Box<MvccCommitStateMachine>>>,
    /// State for a pending sequence inner-tx wrap, set by
    /// `Insn::SequenceBeginInnerTx` (Wrapped path only) and cleared
    /// by `Insn::SequenceCommitInnerTx` on any terminal outcome.
    /// Tracked here (rather than in registers) so that statement
    /// reset can roll back an orphaned inner tx and restore the
    /// connection's mv_tx — registers are wiped on reset, but the
    /// orphaned inner would otherwise linger in `mv_store.txs` and
    /// pollute the connection's mv_tx slot, breaking subsequent
    /// commits with phantom dependencies.
    pub sequence_inner_tx_pending: Option<SequenceInnerTxState>,
    /// Consecutive conflict-retry count for the in-progress sequence
    /// inner-tx wrap. Incremented on each `WriteWriteConflict` /
    /// `BusySnapshot` from `SequenceCommitInnerTx`; reset on the next
    /// successful commit. When it exceeds
    /// `SEQUENCE_INNER_TX_RETRY_BUDGET` the opcode returns
    /// `LimboError::Busy` directly instead of routing a phony
    /// `SQLITE_BUSY` halt through `op_halt`, which would mis-wrap it as
    /// a constraint error.
    pub sequence_inner_retry_count: u32,
    #[cfg(feature = "json")]
    json_cache: JsonCacheCell,
    active_op_state: ActiveOpStateSlot,
    seek_state: OpSeekState,
    /// Metrics collected for the lifetime of this prepared statement.
    pub metrics: StatementMetrics,
    op_vacuum_state: VacuumOpState,
    /// State machine for committing view deltas with I/O handling
    view_delta_state: ViewDeltaCommitState,
    /// Marker which tells about auto transaction cleanup necessary for that connection in case of reset
    /// This is used when statement in auto-commit mode reseted after previous uncomplete execution - in which case we may need to rollback transaction started on previous attempt
    pub(crate) auto_txn_cleanup: TxnCleanup,
    pub explain_state: RwLock<ExplainState>,
    /// Scratch buffer for [Insn::HashDistinct] to avoid per-row allocations.
    distinct_key_values: Vec<Value>,
    hash_tables: HashMap<usize, HashTable>,
    /// TempFile handles for ephemeral cursors, keyed by cursor_id.
    /// Dropping removes the temp file from disk.
    ephemeral_temp_files: HashMap<usize, TempFile>,
    /// Attached pagers that have open savepoints for statement rollback.
    attached_savepoint_pagers: Vec<Arc<Pager>>,
    /// Pending error to return after FAIL mode commit completes.
    /// When a constraint error occurs with FAIL resolve type in autocommit mode,
    /// we need to commit partial changes before returning the error.
    pub(crate) pending_fail_error: Option<LimboError>,
    /// FAIL can escape a trigger before the parent reaches its Halt opcode.
    /// Keep the error here while index-method writes from earlier rows finish
    /// through the normal resumable I/O path.
    pending_fail_prepare_error: Option<LimboError>,
    /// True once the Halt opcode has started finishing the statement. The
    /// statement's outcome is decided at that point, so an interrupt request
    /// arriving during Halt's resumable work (staging index-method writes,
    /// committing the rows FAIL keeps) must not preempt it — it would replace
    /// the promised outcome and drop staged work. The connection-level flag
    /// stays set and clears once no root statement is active.
    pub(crate) halt_in_progress: bool,
    /// Pending CDC info to apply after the program completes successfully.
    /// Set by InitCdcVersion opcode, applied at Halt/Done so that if the
    /// transaction rolls back, the connection's CDC state remains unchanged.
    ///
    /// capture_data_changes has type Option<CaptureDataChangesInfo> (off mode is None)
    /// so, for pending_cdc_info we wrap it in one more Option<...> layer to represent if mode changed during program execution
    pub(crate) pending_cdc_info: Option<Option<CaptureDataChangesInfo>>,
    /// Cached subprogram Statements keyed by the PC of the Program instruction.
    /// Avoids re-allocating ProgramState on each trigger/FK-action fire.
    pub(crate) subprogram_stmt_cache: HashMap<usize, Box<Statement>>,
    /// RowSet objects stored by register index
    rowsets: HashMap<usize, RowSet>,
    /// Bloom filters stored by cursor ID for probabilistic set membership testing
    /// Used to avoid unnecessary seeks on ephemeral indexes and hash tables
    pub(crate) bloom_filters: HashMap<usize, BloomFilter>,
    /// Number of deferred foreign key violations when the statement started.
    /// When a statement subtransaction rolls back, the connection's deferred foreign key violations counter
    /// is reset to this value.
    fk_deferred_violations_when_stmt_started: AtomicIsize,
    /// Number of immediate foreign key violations that occurred during the active statement. If nonzero,
    /// the statement subtransactionwill roll back.
    fk_immediate_violations_during_stmt: AtomicIsize,
    uses_subjournal: bool,
    /// Whether this statement is an active write inside an explicit transaction.
    pub(crate) is_active_write: bool,
    /// Whether begin_statement was called (savepoint + FK bookkeeping active).
    has_stmt_transaction: bool,
    pub n_change: AtomicI64,
    pub n_total_change: AtomicI64,
    /// The connection's MvStore handle, revalidated with one pointer compare
    /// per use instead of a full ArcSwap load (see `ProgramState::mv_store`).
    mv_store_cache: Option<arc_swap::Cache<MvStoreHandle, Option<Arc<MvStore>>>>,
}

/// Lets an `arc_swap::Cache` follow the MvStore slot of a database.
pub(crate) struct MvStoreHandle(Arc<crate::Database>);

impl std::ops::Deref for MvStoreHandle {
    type Target = arc_swap::ArcSwapOption<MvStore>;

    fn deref(&self) -> &Self::Target {
        &self.0.mv_store
    }
}

impl std::fmt::Debug for Program {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Program").finish()
    }
}

// See: https://github.com/tursodatabase/turso/issues/1552
// SAFETY: Rust cannot derive Send + Sync automatically mainly because of `Row` struct
// as it contains a `*const Register`.
// Program + Program State upholds Rust aliasing rules with `Row` by only giving out immutable references to
// the internal `result_row` and by invalidating the result row whenever the program is stepped.
unsafe impl Send for ProgramState {}
unsafe impl Sync for ProgramState {}
crate::assert::assert_send_sync!(ProgramState);

impl ProgramState {
    pub fn new(max_registers: usize, max_cursors: usize) -> Self {
        let cursors: Vec<Option<Cursor>> = (0..max_cursors).map(|_| None).collect();
        let cursor_seqs = vec![0i64; max_cursors];
        let registers = vec![Register::Value(Value::Null); max_registers].into_boxed_slice();
        Self {
            check_mask: 63,
            io_completions: None,
            pc: 0,
            cursors,
            index_method_contexts: vec![None; max_cursors],
            closed_index_method_cursors: Vec::new(),
            index_method_finalize_cursor: 0,
            index_method_finalize_subprogram_keys: None,
            index_method_finalize_subprogram: 0,
            index_methods_finalized: false,
            cursor_seqs,
            registers,
            pre_op_registers: None,
            result_row: None,
            last_compare: None,
            deferred_seeks: vec![None; max_cursors],
            ended_coroutine: vec![],
            once: SmallVec::<[u32; 4]>::new(),
            execution_state: ProgramExecutionState::Init,
            query_deadline: None,
            explicit_checkpoint_guard: None,
            parameters: Vec::new(),
            commit_state: CommitState::Ready,
            sequence_inner_commit: None,
            sequence_inner_tx_pending: None,
            sequence_inner_retry_count: 0,
            #[cfg(feature = "json")]
            json_cache: JsonCacheCell::new(),
            active_op_state: ActiveOpStateSlot::default(),
            seek_state: OpSeekState::Start,
            metrics: StatementMetrics::new(),
            distinct_key_values: Vec::new(),
            op_vacuum_state: VacuumOpState::None,
            view_delta_state: ViewDeltaCommitState::NotStarted,
            auto_txn_cleanup: TxnCleanup::None,
            fk_deferred_violations_when_stmt_started: AtomicIsize::new(0),
            fk_immediate_violations_during_stmt: AtomicIsize::new(0),
            rowsets: HashMap::default(),
            bloom_filters: HashMap::default(),
            hash_tables: HashMap::default(),
            ephemeral_temp_files: HashMap::default(),
            uses_subjournal: false,
            is_active_write: false,
            has_stmt_transaction: false,
            attached_savepoint_pagers: Vec::new(),
            n_change: AtomicI64::new(0),
            n_total_change: AtomicI64::new(0),
            explain_state: RwLock::new(ExplainState::default()),
            pending_fail_error: None,
            pending_fail_prepare_error: None,
            halt_in_progress: false,
            pending_cdc_info: None,
            subprogram_stmt_cache: HashMap::default(),
            mv_store_cache: None,
        }
    }

    pub fn set_register(&mut self, idx: usize, value: Register) {
        self.registers[idx] = value;
    }

    pub fn get_register(&self, idx: usize) -> &Register {
        &self.registers[idx]
    }

    pub fn column_count(&self) -> usize {
        self.registers.len()
    }

    pub fn column(&self, i: usize) -> Option<String> {
        Some(format!("{:?}", self.registers[i]))
    }

    pub fn interrupt(&mut self) {
        self.execution_state = ProgramExecutionState::Interrupting;
    }

    pub fn is_interrupted(&self) -> bool {
        matches!(self.execution_state, ProgramExecutionState::Interrupting)
    }

    pub fn bind_at(&mut self, index: NonZero<usize>, value: Value) -> Result<()> {
        let i = index.get() - 1;
        if i >= self.parameters.len() {
            self.parameters.resize(i + 1, Value::Null);
        }
        let slot = &mut self.parameters[i];
        match (slot, value) {
            (Value::Null, Value::Null) => {}
            (Value::Numeric(Numeric::Integer(existing)), Value::Numeric(Numeric::Integer(new))) => {
                *existing = new
            }
            (Value::Numeric(Numeric::Float(existing)), Value::Numeric(Numeric::Float(new))) => {
                *existing = new
            }
            (Value::Text(existing), Value::Text(new)) => existing.do_extend(&new)?,
            (Value::Blob(existing), Value::Blob(new)) => existing.do_extend(&new)?,
            (slot, value) => *slot = value,
        }
        Ok(())
    }

    pub fn clear_bindings(&mut self) {
        self.parameters.clear();
    }

    pub fn get_parameter(&self, index: NonZero<usize>) -> Value {
        let i = index.get() - 1;
        self.parameters.get(i).cloned().unwrap_or(Value::Null)
    }

    pub fn reset(&mut self, max_registers: Option<usize>, max_cursors: Option<usize>) {
        self.io_completions = None;
        self.pc = 0;

        if let Some(max_cursors) = max_cursors {
            self.cursors.resize_with(max_cursors, || None);
            self.index_method_contexts.resize_with(max_cursors, || None);
            self.cursor_seqs.resize(max_cursors, 0);
            self.deferred_seeks.resize(max_cursors, None);
        }
        self.result_row = None;
        if let Some(max_registers) = max_registers {
            // into_vec and into_boxed_slice do not allocate
            let mut registers = std::mem::take(&mut self.registers).into_vec();
            // As we are dropping whatever is in the result row, we can be sure that no one is referencing values from `*const Register` inside `Row`.
            registers.resize_with(max_registers, || Register::Value(Value::Null));
            self.registers = registers.into_boxed_slice();
        }
        // reset cursors as they can have cached information which will be no longer relevant on next program execution
        for (cursor, context) in self
            .cursors
            .iter_mut()
            .zip(self.index_method_contexts.iter_mut())
        {
            if let (Some(Cursor::IndexMethod(cursor)), Some(context)) =
                (cursor.as_mut(), context.as_ref())
            {
                cursor.close(context);
            }
            if let Some(Cursor::BTree(cursor)) = cursor.take() {
                cursor.recycle();
            }
            *context = None;
        }
        for (mut cursor, context) in self.closed_index_method_cursors.drain(..) {
            cursor.close(&context);
        }
        self.index_method_finalize_cursor = 0;
        self.index_method_finalize_subprogram_keys = None;
        self.index_method_finalize_subprogram = 0;
        self.index_methods_finalized = false;
        for r in self.registers.iter_mut() {
            match r {
                Register::Value(v) => *v = Value::Null,
                _ => r.set_null(),
            }
        }
        self.last_compare = None;
        self.deferred_seeks.iter_mut().for_each(|s| *s = None);
        self.ended_coroutine.clear();
        self.once.clear();
        self.execution_state = ProgramExecutionState::Init;
        self.query_deadline = None;
        self.explicit_checkpoint_guard = None;
        #[cfg(feature = "json")]
        self.json_cache.clear();

        // A caller can reset or drop a statement after an MVCC auto-checkpoint
        // has yielded I/O. Waiting for that I/O does not step the nested
        // CheckpointStateMachine again, so release its checkpoint lock before
        // replacing commit_state with Ready.
        self.commit_state.cleanup_mvcc_checkpoint_state();
        self.active_op_state.clear();
        self.seek_state = OpSeekState::Start;
        self.commit_state = CommitState::Ready;
        // Drop any in-flight sequence inner-tx commit-state-machine. If
        // it was mid-step the inner mv_tx has already been swapped back
        // (we handle that on every code path inside
        // `op_sequence_commit_inner_tx`) so dropping the state machine
        // here only releases its references.
        self.sequence_inner_commit = None;
        self.op_vacuum_state = VacuumOpState::None;
        self.view_delta_state = ViewDeltaCommitState::NotStarted;
        self.auto_txn_cleanup = TxnCleanup::None;
        *self.fk_immediate_violations_during_stmt.get_mut() = 0;
        *self.fk_deferred_violations_when_stmt_started.get_mut() = 0;
        self.rowsets.clear();
        self.bloom_filters.clear();
        self.hash_tables.clear();
        self.ephemeral_temp_files.clear();
        self.uses_subjournal = false;
        self.is_active_write = false;
        self.has_stmt_transaction = false;
        self.distinct_key_values.clear();
        self.attached_savepoint_pagers.clear();
        *self.n_change.get_mut() = 0;
        *self.n_total_change.get_mut() = 0;
        // reset has exclusive access, so no lock or atomic store is needed.
        *self.explain_state.get_mut() = ExplainState::default();
        self.pending_fail_error = None;
        self.pending_fail_prepare_error = None;
        self.halt_in_progress = false;
        self.pending_cdc_info = None;
        self.subprogram_stmt_cache.clear();
    }

    pub(crate) fn record_statement_change(&self) {
        self.n_change.fetch_add(1, Ordering::SeqCst);
        self.n_total_change.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn record_total_change(&self) {
        self.n_total_change.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether this statement may finish the implicit autocommit transaction
    /// now, including re-entry while its commit is in progress.
    #[inline]
    /// `self_counted` is true while this statement is still included in
    /// `Connection::n_active_root_statements`. It is false when a statement
    /// that already finished (released on Done or on its step error) is being
    /// reset or dropped — then every counted statement is a *sibling*, and
    /// treating the count as "just me" would make teardown finish or roll
    /// back a transaction a suspended sibling is still using (e.g. a COMMIT
    /// parked inside its post-commit auto-checkpoint).
    pub(crate) fn can_autocommit_now(
        &mut self,
        connection: &Connection,
        self_counted: bool,
    ) -> bool {
        let is_already_committing = !matches!(self.commit_state, CommitState::Ready);
        if is_already_committing {
            return true;
        }
        let active_writers = connection.n_active_writes.load(Ordering::SeqCst);
        turso_assert!(
            active_writers <= 1,
            "n_active_writes must be 0 or 1, got {active_writers}"
        );
        if self.is_active_write {
            turso_assert!(
                active_writers == 1,
                "active writer state without an active writer count"
            );
        }
        if self.mv_store(connection).is_some() {
            // MVCC keeps one tx id on the connection. A writer waits for
            // sibling readers, and a reader waits for sibling readers/writers.
            return self.auto_txn_cleanup == TxnCleanup::RollbackTxn
                && connection.n_active_root_statements.load(Ordering::SeqCst)
                    == i32::from(self_counted)
                && (self.is_active_write || active_writers == 0);
        }
        if self.auto_txn_cleanup == TxnCleanup::RollbackTxn && self.is_active_write {
            // Pager/WAL writers can finish while sibling readers remain
            // active, like SQLite commits when the halting statement is the
            // only writer (the nVdbeWrite check in sqlite3VdbeHalt). The
            // readers keep their cursors and release them when they finish.
            return true;
        }
        // Non-main pagers keep their transaction state on the pager itself,
        // like SQLite keeps it on the Btree handle (Btree.inTrans), and the
        // transaction is shared by every statement on the connection.
        let attached_txn_open = || {
            connection.with_all_attached_pagers_with_index(|pagers| {
                pagers
                    .iter()
                    .any(|(_, pager)| pager.holds_read_lock() || pager.holds_write_lock())
            })
        };
        if connection.n_active_root_statements.load(Ordering::SeqCst) > i32::from(self_counted) {
            // Readers can finish while sibling readers remain active, but a
            // shared attached transaction may only be finished by the last
            // active statement, like SQLite's btreeEndTransaction keeps the
            // transaction open while db->nVdbeRead > 1.
            return self.auto_txn_cleanup == TxnCleanup::RollbackTxn
                && active_writers == 0
                && !attached_txn_open();
        }
        // This is the last active statement: finish its own transaction, or
        // an attached transaction a deferring sibling left behind — SQLite's
        // vdbeCommit visits every database on halt, so leftovers are closed
        // even by a statement that never started a transaction itself.
        if active_writers != 0 {
            return false;
        }
        self.auto_txn_cleanup == TxnCleanup::RollbackTxn
            || (connection.get_auto_commit() && attached_txn_open())
    }

    /// The MvStore this statement runs against: the same answer as
    /// `Connection::mv_store`, but a full ArcSwap load (thread-local debt
    /// slot, ~50 instructions) only when the slot changed since the last
    /// call. The MVCC bootstrap connection never uses the store.
    #[inline(always)]
    pub(crate) fn mv_store(&mut self, connection: &Connection) -> Option<&Arc<MvStore>> {
        if connection.is_mvcc_bootstrap_connection() {
            return None;
        }
        self.db_mv_store(connection).as_ref()
    }

    /// The MvStore of the database, whether or not this connection is the
    /// MVCC bootstrap connection: the same answer as `Database::get_mv_store`.
    #[inline(always)]
    pub(crate) fn db_mv_store(&mut self, connection: &Connection) -> &Option<Arc<MvStore>> {
        let cache = self
            .mv_store_cache
            .get_or_insert_with(|| arc_swap::Cache::new(MvStoreHandle(connection.db.clone())));
        debug_assert!(
            std::ptr::eq(cache.arc_swap(), &connection.db.mv_store),
            "statement state used with a connection on another database"
        );
        cache.load()
    }

    #[inline]
    pub fn record_rows_read(&mut self, count: u64) {
        self.metrics.rows_read = self.metrics.rows_read.wrapping_add(count);
    }

    #[inline]
    pub fn record_rows_written(&mut self, count: u64) {
        self.metrics.rows_written = self.metrics.rows_written.wrapping_add(count);
    }

    /// Runs `f` on the metrics of this statement including its active and
    /// cached subprograms, without copying them when there is no subprogram.
    pub(crate) fn with_metrics<R>(&self, f: impl FnOnce(&StatementMetrics) -> R) -> R {
        let has_subprograms = matches!(
            self.active_op_state.program_ref(),
            Some(OpProgramState::Step { .. })
        ) || !self.subprogram_stmt_cache.is_empty();
        if has_subprograms {
            f(&self.metrics())
        } else {
            f(&self.metrics)
        }
    }

    pub(crate) fn metrics(&self) -> StatementMetrics {
        let mut metrics = self.metrics.clone();
        if let Some(OpProgramState::Step { statement, .. }) = self.active_op_state.program_ref() {
            metrics.merge(&statement.metrics());
        }
        for statement in self.subprogram_stmt_cache.values() {
            metrics.merge(&statement.metrics());
        }
        metrics
    }

    pub(crate) fn reset_metrics(&mut self) {
        self.metrics.reset();
        if let Some(OpProgramState::Step { statement, .. }) = self.active_op_state.program_mut() {
            statement.reset_metrics();
        }
        for statement in self.subprogram_stmt_cache.values_mut() {
            statement.reset_metrics();
        }
    }

    pub(crate) fn reset_stmt_status(&mut self, counter: crate::statement::StatementStatusCounter) {
        match counter {
            crate::statement::StatementStatusCounter::FullscanStep => {
                self.metrics.fullscan_steps = 0
            }
            crate::statement::StatementStatusCounter::Sort => self.metrics.sort_operations = 0,
            crate::statement::StatementStatusCounter::VmStep => self.metrics.insn_executed = 0,
            crate::statement::StatementStatusCounter::Reprepare => self.metrics.reprepares = 0,
            crate::statement::StatementStatusCounter::RowsRead => self.metrics.rows_read = 0,
            crate::statement::StatementStatusCounter::RowsWritten => self.metrics.rows_written = 0,
        }
        if let Some(OpProgramState::Step { statement, .. }) = self.active_op_state.program_mut() {
            statement.reset_stmt_status(counter);
        }
        for statement in self.subprogram_stmt_cache.values_mut() {
            statement.reset_stmt_status(counter);
        }
    }

    pub fn get_cursor(&mut self, cursor_id: CursorID) -> &mut Cursor {
        self.cursors
            .get_mut(cursor_id)
            .unwrap_or_else(|| panic!("cursor id {cursor_id} out of bounds"))
            .as_mut()
            .unwrap_or_else(|| panic!("cursor id {cursor_id} is None"))
    }

    /// Close all virtual table cursors owned by this program.
    ///
    /// A virtual table cursor can own a nested helper statement on the same
    /// connection (e.g. `PragmaVirtualTableCursor` runs `PRAGMA ...` via
    /// `Connection::prepare_internal`), and that helper holds the
    /// connection's nested-statement guard until it is dropped. Both
    /// `commit_txn` and `abort` consult `Connection::is_nested_stmt()` to
    /// decide whether the current statement owns top-level transaction
    /// finalization, so the helpers must be dropped first — otherwise a root
    /// statement that scanned a pragma virtual table misclassifies itself as
    /// nested, skips ending its implicit read transaction, and subsequent
    /// writes on the connection never auto-commit (issue #7466).
    pub(crate) fn close_virtual_table_cursors(&mut self) {
        for slot in self.cursors.iter_mut() {
            if matches!(slot, Some(Cursor::Virtual(_))) {
                *slot = None;
            }
        }
    }

    /// Begin a statement subtransaction.
    ///
    /// Creates a savepoint on the main DB's MvStore (or pager for WAL mode),
    /// and snapshots FK violation counters for potential statement rollback.
    /// Attached DB savepoints are opened per-DB in `op_transaction_inner`
    /// when each DB's Transaction opcode is executed.
    ///
    /// Pager/MVCC savepoints are only opened for write statements inside an
    /// explicit transaction. In autocommit mode, a statement abort is a
    /// transaction abort, so savepoints are unnecessary.
    pub fn begin_statement(
        &mut self,
        connection: &Connection,
        pager: &Arc<Pager>,
        write: bool,
    ) -> IOResultOr<()> {
        let in_explicit_txn = !connection.auto_commit.load(Ordering::SeqCst);
        if write && in_explicit_txn {
            // Check if MVCC is active - if so, use MVCC savepoints instead of pager savepoints
            if let Some(mv_store) = connection.mv_store().as_ref() {
                if let Some(tx_id) = connection.get_mv_tx_id() {
                    mv_store.begin_savepoint(tx_id);
                }
            } else {
                // Non-MVCC mode: use pager savepoints
                let db_size = return_if_io!(pager.with_header(|header| header.database_size.get()));
                pager.open_subjournal()?;
                pager.try_use_subjournal()?;
                let result = pager.open_savepoint(db_size);
                if result.is_err() {
                    pager.stop_use_subjournal();
                }
                result?;
                self.uses_subjournal = true;
            }
        }

        self.has_stmt_transaction = true;

        // Store the deferred foreign key violations counter at the start of the statement.
        // This is used to ensure that if an interactive transaction had deferred FK violations and a statement subtransaction rolls back,
        // the deferred FK violations are not lost.
        self.fk_deferred_violations_when_stmt_started.store(
            connection.fk_deferred_violations.load(Ordering::Acquire),
            Ordering::SeqCst,
        );
        // Reset the immediate foreign key violations counter to 0. If this is nonzero when the statement completes, the statement subtransaction will roll back.
        self.fk_immediate_violations_during_stmt
            .store(0, Ordering::Release);
        Ok(IOResult::Done(()))
    }

    /// End a statement subtransaction.
    ///
    /// Mirrors SQLite's vdbeCloseStatement (vdbeaux.c:3203-3248). Pager/MVCC
    /// savepoint management and FK violation counter restoration are independent
    /// concerns: pager savepoints may be skipped (e.g. autocommit optimization)
    /// while FK bookkeeping still needs cleanup.
    pub fn end_statement(
        &mut self,
        connection: &Connection,
        pager: &Arc<Pager>,
        end_statement: EndStatement,
    ) -> Result<()> {
        if self.is_active_write {
            let previous = connection.n_active_writes.fetch_sub(1, Ordering::SeqCst);
            turso_assert!(
                previous == 1,
                "ending a writer with {previous} active writer(s)"
            );
            self.is_active_write = false;
        }
        // If begin_statement was never called, no savepoint/FK cleanup needed.
        if !self.has_stmt_transaction {
            return Ok(());
        }
        self.has_stmt_transaction = false;

        // Drain attached pagers upfront so we can clean them up regardless of path.
        let attached_pagers: Vec<Arc<Pager>> = self.attached_savepoint_pagers.drain(..).collect();
        let result = match end_statement {
            EndStatement::ReleaseSavepoint => {
                if let Some(mv_store) = connection.mv_store().as_ref() {
                    if let Some(tx_id) = connection.get_mv_tx_id() {
                        mv_store.release_savepoint(tx_id);
                    }
                    connection.for_each_attached_mv_tx(|db_id, tx_id| {
                        if let Some(attached_mv) = connection.mv_store_for_db(db_id) {
                            attached_mv.release_savepoint(tx_id);
                        }
                    });
                    Ok(())
                } else if self.uses_subjournal || !attached_pagers.is_empty() {
                    if self.uses_subjournal {
                        pager.release_savepoint()?;
                    }
                    for p in &attached_pagers {
                        p.release_savepoint()?;
                    }
                    Ok(())
                } else {
                    Ok(())
                }
            }
            EndStatement::RollbackSavepoint => {
                // Rollback pager/MVCC savepoint if one was opened.
                let pager_err = if let Some(mv_store) = connection.mv_store().as_ref() {
                    let mut err = None;
                    if let Some(tx_id) = connection.get_mv_tx_id() {
                        if let Err(e) = mv_store.rollback_first_savepoint(tx_id) {
                            err = Some(e);
                        }
                    }
                    connection.for_each_attached_mv_tx(|db_id, tx_id| {
                        if let Some(attached_mv) = connection.mv_store_for_db(db_id) {
                            if let Err(e) = attached_mv.rollback_first_savepoint(tx_id) {
                                if err.is_none() {
                                    err = Some(e);
                                }
                            }
                        }
                    });
                    err
                } else if self.uses_subjournal {
                    match pager.rollback_to_newest_savepoint() {
                        Ok(_) => {
                            let mut err = None;
                            for p in &attached_pagers {
                                if let Err(e) = p.rollback_to_newest_savepoint() {
                                    err = Some(e);
                                    break;
                                }
                            }
                            err
                        }
                        Err(e) => Some(e),
                    }
                } else if !attached_pagers.is_empty() {
                    let mut err = None;
                    for p in &attached_pagers {
                        if let Err(e) = p.rollback_to_newest_savepoint() {
                            err = Some(e);
                        }
                    }
                    err
                } else {
                    None
                };

                // Always restore FK violation counters on statement rollback,
                // regardless of whether a pager savepoint was opened.
                // Mirrors SQLite's vdbeCloseStatement (vdbeaux.c:3243-3246).
                connection.fk_deferred_violations.store(
                    self.fk_deferred_violations_when_stmt_started
                        .load(Ordering::Acquire),
                    Ordering::SeqCst,
                );

                match pager_err {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
        };
        if self.uses_subjournal {
            pager.stop_use_subjournal();
            self.uses_subjournal = false;
        }
        for p in &attached_pagers {
            p.stop_use_subjournal();
        }
        result
    }

    /// Gets or creates a bloom filter for the given cursor ID.
    pub fn get_or_create_bloom_filter(&mut self, cursor_id: usize) -> &mut BloomFilter {
        self.bloom_filters.entry(cursor_id).or_default()
    }

    /// Gets or creates a bloom filter with a specific capacity for the given cursor ID.
    pub fn get_or_create_bloom_filter_with_capacity(
        &mut self,
        cursor_id: usize,
        expected_items: u32,
        false_positive_rate: f32,
    ) -> &mut BloomFilter {
        self.bloom_filters
            .entry(cursor_id)
            .or_insert_with(|| BloomFilter::with_capacity(expected_items, false_positive_rate))
    }

    /// Gets an existing bloom filter for the given cursor ID.
    pub fn get_bloom_filter(&self, cursor_id: usize) -> Option<&BloomFilter> {
        self.bloom_filters.get(&cursor_id)
    }

    /// Gets a mutable reference to an existing bloom filter for the given cursor ID.
    pub fn get_bloom_filter_mut(&mut self, cursor_id: usize) -> Option<&mut BloomFilter> {
        self.bloom_filters.get_mut(&cursor_id)
    }

    /// Removes and drops the bloom filter for the given cursor ID.
    pub fn remove_bloom_filter(&mut self, cursor_id: usize) {
        self.bloom_filters.remove(&cursor_id);
    }

    /// Checks if a bloom filter exists for the given cursor ID.
    pub fn has_bloom_filter(&self, cursor_id: usize) -> bool {
        self.bloom_filters.contains_key(&cursor_id)
    }

    pub fn get_fk_immediate_violations_during_stmt(&self) -> isize {
        self.fk_immediate_violations_during_stmt
            .load(Ordering::Acquire)
    }

    pub fn increment_fk_immediate_violations_during_stmt(&self, v: isize) {
        self.fk_immediate_violations_during_stmt
            .fetch_add(v, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Action to take at the end of a statement subtransaction.
pub enum EndStatement {
    /// Release (commit) the savepoint -- effectively removing the savepoint as it is no longer needed for undo purposes.
    ReleaseSavepoint,
    /// Rollback (abort) to the newest savepoint: read pages from the subjournal and restore them to the page cache.
    /// This is used to undo the changes made by the statement.
    RollbackSavepoint,
}

impl Register {
    pub fn get_value(&self) -> &Value {
        match self {
            Register::Value(v) => v,
            Register::Record(r) => {
                turso_assert!(!r.is_invalidated());
                r.as_blob_value()
            }
            _ => panic!("register holds unexpected value: {self:?}"),
        }
    }
}

#[macro_export]
macro_rules! must_be_btree_cursor {
    ($cursor_id:expr, $cursor_ref:expr, $state:expr, $insn_name:expr) => {{
        let (_, cursor_type) = $cursor_ref.get($cursor_id).unwrap();
        if matches!(
            cursor_type,
            CursorType::BTreeTable(_)
                | CursorType::BTreeIndex(_)
                | CursorType::MaterializedView(_, _)
        ) {
            $crate::get_cursor!($state, $cursor_id)
        } else {
            panic!("{} on unexpected cursor", $insn_name)
        }
    }};
}

/// Macro is necessary to help the borrow checker see we are only accessing state.cursor field
/// and nothing else
#[macro_export]
macro_rules! get_cursor {
    ($state:expr, $cursor_id:expr) => {
        $state
            .cursors
            .get_mut($cursor_id)
            .unwrap_or_else(|| panic!("cursor id {} out of bounds", $cursor_id))
            .as_mut()
            .unwrap_or_else(|| panic!("cursor id {} is None", $cursor_id))
    };
}

/// Tracks the state of explain mode execution, including which subprograms need to be processed.
#[derive(Default)]
pub struct ExplainState {
    /// Subprograms queued for explain output, processed after the parent program finishes.
    pending: std::collections::VecDeque<Arc<PreparedProgram>>,
    /// Prepared subprograms that have already been queued for explain output.
    ///
    /// Recursive foreign-key action programs can contain a `Program` instruction
    /// that calls the same prepared program again. Without this set, EXPLAIN
    /// keeps printing the same subprogram forever.
    queued_subprograms: std::collections::HashSet<usize>,
    /// The subprogram currently being explained, if any.
    current: Option<Arc<PreparedProgram>>,
}

impl ExplainState {
    /// Queue a subprogram for EXPLAIN output if this statement has not queued it before.
    fn queue_subprogram_once(&mut self, subprogram: Arc<PreparedProgram>) {
        let subprogram_id = Arc::as_ptr(&subprogram) as usize;
        if self.queued_subprograms.insert(subprogram_id) {
            self.pending.push_back(subprogram);
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedProgram {
    pub max_registers: usize,
    // we store original indices because we don't want to create new vec from
    // ProgramBuilder
    pub insns: Vec<(Insn, usize)>,
    pub cursor_ref: Vec<(Option<CursorKey>, CursorType)>,
    pub explain: ExplainInfo,
    pub parameters: crate::parameters::Parameters,
    pub change_cnt_on: bool,
    /// Flag that detect if the sqlite statement will directly manipulate the database file.\
    /// mirrors: https://sqlite.org/c3ref/stmt_readonly.html.
    pub readonly: bool,
    pub result_columns: Vec<ResultSetColumn>,
    pub table_references: TableReferences,
    pub sql: String,
    /// The statement is ANALYZE, so its completion refreshes the in-memory
    /// planner statistics. Decided once here instead of by scanning the SQL
    /// text on every completion.
    pub refreshes_analyze_stats: bool,
    /// Whether the statement needs to be wrapped in a statement subtransaction
    /// when run as part of an interactive (non-autocommit) transaction.
    /// See [crate::vdbe::builder::ProgramBuilder::is_multi_write] and [crate::vdbe::builder::ProgramBuilder::may_abort] for more details.
    pub needs_stmt_subtransactions: Arc<AtomicBool>,
    /// If this Program is a trigger subprogram, a ref to the trigger is stored here.
    pub trigger: Option<Arc<Trigger>>,
    /// Whether this program is a subprogram (trigger or FK action) that runs within a parent statement.
    pub is_subprogram: bool,
    pub resolve_type: ResolveType,
    pub prepare_context: PrepareContext,
    /// Set of attached database indices that need write transactions.
    pub write_databases: BitSet,
    /// Set of attached database indices that need read transactions.
    pub read_databases: BitSet,
}

#[derive(Clone)]
pub struct Program {
    pub(crate) prepared: Arc<PreparedProgram>,
    pub connection: Arc<Connection>,
}

/// Captures connection settings at statement preparation time for cache invalidation.
///
/// This struct is used to detect when a cached prepared statement needs to be recompiled
/// because relevant connection settings have changed. When `matches_connection()` returns
/// false, the statement will be automatically reprepared before execution.
///
/// # Adding New Fields
///
/// If you add a new setting to `Connection` that affects statement compilation or execution,
/// When adding a new connection setting that affects query compilation, you MUST call
/// `bump_prepare_context_generation()` in its setter so that prepared statements know
/// they need to be reprepared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareContext {
    /// Identity check: the prepared statement must belong to the same database.
    database_ptr: usize,
    /// Generation counter snapshot taken at prepare time. Compared against the
    /// connection's current generation to detect setting changes (pragmas,
    /// attach/detach, extension registration, etc.) without rebuilding the full
    /// context on every step.
    generation: u64,
}

impl PrepareContext {
    pub fn from_connection(connection: &Connection) -> Self {
        Self {
            database_ptr: connection.database_ptr(),
            generation: connection.prepare_context_generation(),
        }
    }

    #[inline]
    pub fn matches_connection(&self, connection: &Connection) -> bool {
        self.database_ptr == connection.database_ptr()
            && self.generation == connection.prepare_context_generation()
    }
}

impl PreparedProgram {
    pub fn bind(self: Arc<Self>, connection: Arc<Connection>) -> Program {
        Program {
            prepared: self,
            connection,
        }
    }

    pub fn is_compatible_with(&self, connection: &Connection) -> bool {
        self.prepare_context.matches_connection(connection)
    }

    #[inline]
    pub const fn is_readonly(&self) -> bool {
        self.readonly
    }
}

impl Program {
    #[inline]
    pub fn prepared(&self) -> &Arc<PreparedProgram> {
        &self.prepared
    }

    pub fn from_prepared(prepared: Arc<PreparedProgram>, connection: Arc<Connection>) -> Self {
        Self {
            prepared,
            connection,
        }
    }

    #[inline]
    pub fn is_readonly(&self) -> bool {
        self.prepared().is_readonly()
    }
}

impl Program {
    fn get_pager_from_database_index(&self, idx: &usize) -> Result<Arc<Pager>> {
        self.connection.get_pager_from_database_index(idx)
    }

    // `prev_steps` is the vm_steps value at the previous consultation, so the
    // progress handler sees every crossed multiple of its interval.
    #[inline]
    fn maybe_request_interrupt<I>(&self, state: &mut ProgramState, io: &I, prev_steps: u64) -> bool
    where
        I: crate::IO + ?Sized,
    {
        // Once Halt has started finishing the statement, its outcome is
        // decided; an interrupt (or deadline, or progress handler) must not
        // preempt the remaining resumable finalization work. A request that
        // arrived before Halt began still interrupts as usual.
        if state.halt_in_progress && !state.is_interrupted() {
            return false;
        }
        let connection_interrupt = self.connection.is_interrupted();
        let hit_query_deadline = state
            .query_deadline
            .is_some_and(|deadline| io.current_time_monotonic() >= deadline);
        let progress_interrupt = self
            .connection
            .should_interrupt_for_progress(prev_steps, state.metrics.vm_steps);
        if connection_interrupt || hit_query_deadline || progress_interrupt {
            state.interrupt();
        }
        state.is_interrupted()
    }

    #[turso_macros::trace_stack]
    pub fn step(
        &self,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
        query_mode: QueryMode,
        waker: Option<&Waker>,
    ) -> Result<StepResult, Box<LimboError>> {
        state.execution_state = ProgramExecutionState::Running;
        let result = if let QueryMode::Normal = query_mode {
            self.normal_step(state, pager, waker)
        } else {
            self.explain_step_for_mode(state, pager, query_mode)
        };
        // Rows are the common result and leave the execution state untouched.
        if let Ok(StepResult::Row) = &result {
            return result;
        }
        match &result {
            Ok(StepResult::Done) => {
                state.execution_state = ProgramExecutionState::Done;
            }
            Ok(StepResult::Interrupt) => {
                state.execution_state = ProgramExecutionState::Interrupted;
            }
            Err(_) => {
                state.execution_state = ProgramExecutionState::Failed;
            }
            _ => {}
        }
        result
    }

    #[inline(never)]
    fn explain_step_for_mode(
        &self,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
        query_mode: QueryMode,
    ) -> Result<StepResult, Box<LimboError>> {
        match query_mode {
            QueryMode::Normal => unreachable!("normal queries do not step through explain"),
            QueryMode::Explain => self.explain_step(state, pager),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Text,
            } => self.explain_query_plan_step(state, pager),
            QueryMode::ExplainQueryPlan {
                format: EqpFormat::Json,
            } => self.explain_query_plan_json_step(state, pager),
        }
    }

    fn explain_step(
        &self,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
    ) -> Result<StepResult, Box<LimboError>> {
        turso_debug_assert!(state.column_count() == EXPLAIN_COLUMNS.len());
        if self.connection.is_closed() {
            let tx_state = self.connection.get_tx_state();
            if let TransactionState::Write { .. } = tx_state {
                pager.rollback_tx(&self.connection);
            }
            return Err(LimboError::InternalError("Connection closed".to_string()).into());
        }

        if self.maybe_request_interrupt(
            state,
            pager.io.as_ref(),
            state.metrics.vm_steps.saturating_sub(1),
        ) {
            return Ok(StepResult::Interrupt);
        }

        state.metrics.vm_steps = state.metrics.vm_steps.wrapping_add(1);

        let mut explain_state = state.explain_state.write();

        // Advance to the next subprogram if the current one is finished
        loop {
            if let Some(ref current) = explain_state.current {
                if (state.pc as usize) < current.insns.len() {
                    break;
                }
            } else if (state.pc as usize) < self.insns.len() {
                break;
            }
            // Current program is done, pop next subprogram from queue
            if let Some(next) = explain_state.pending.pop_front() {
                explain_state.current = Some(next);
                state.pc = 0;
            } else {
                explain_state.current = None;
                return Ok(StepResult::Done);
            }
        }

        let pc = state.pc as usize;

        // Explain the current instruction from the active program.
        // We collect subprograms separately to avoid borrow conflicts with explain_state.
        let (row, subprogram) = if let Some(ref current) = explain_state.current {
            let (insn, _) = &current.insns[pc];
            let sub = if let Insn::Program {
                program: subprogram,
                ..
            } = insn
            {
                Some(subprogram.prepared_program()?)
            } else {
                None
            };
            let comment = current.explain.comment_at(state.pc);
            (insn_to_row_with_comment(current, insn, comment), sub)
        } else {
            let (insn, _) = &self.insns[pc];
            let sub = if let Insn::Program {
                program: subprogram,
                ..
            } = insn
            {
                Some(subprogram.prepared_program()?)
            } else {
                None
            };
            let comment = self.explain.comment_at(state.pc);
            (insn_to_row_with_comment(self, insn, comment), sub)
        };
        if let Some(sub) = subprogram {
            explain_state.queue_subprogram_once(sub);
        }
        let (opcode, p1, p2, p3, p4, p5, comment) = row;

        state.registers[0].set_int(state.pc as i64);
        state.registers[1].set_value(Value::from_text(opcode));
        state.registers[2].set_int(p1);
        state.registers[3].set_int(p2);
        state.registers[4].set_int(p3);
        state.registers[5].set_value(p4);
        state.registers[6].set_int(p5);
        state.registers[7].set_value(Value::from_text(comment));
        state.result_row = Some(Row {
            values: &state.registers[0] as *const Register,
            count: EXPLAIN_COLUMNS.len(),
        });
        state.pc += 1;
        Ok(StepResult::Row)
    }

    fn explain_query_plan_step(
        &self,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
    ) -> Result<StepResult, Box<LimboError>> {
        turso_debug_assert!(state.column_count() == EXPLAIN_QUERY_PLAN_COLUMNS.len());
        loop {
            if self.connection.is_closed() {
                // Connection is closed for whatever reason, rollback the transaction.
                let state = self.connection.get_tx_state();
                if let TransactionState::Write { .. } = state {
                    pager.rollback_tx(&self.connection);
                }
                return Err(LimboError::InternalError("Connection closed".to_string()).into());
            }

            if self.maybe_request_interrupt(
                state,
                pager.io.as_ref(),
                state.metrics.vm_steps.saturating_sub(1),
            ) {
                return Ok(StepResult::Interrupt);
            }

            // FIXME: do we need this?
            state.metrics.vm_steps = state.metrics.vm_steps.wrapping_add(1);

            if state.pc as usize >= self.insns.len() {
                return Ok(StepResult::Done);
            }

            let Insn::Explain { p1, p2, detail } = &self.insns[state.pc as usize].0 else {
                state.pc += 1;
                continue;
            };

            state.registers[0].set_int(*p1 as i64);
            state.registers[1] =
                Register::Value(Value::from_i64(p2.as_ref().map(|p| *p).unwrap_or(0) as i64));
            state.registers[2].set_int(0);
            state.registers[3].set_value(Value::from_text(detail.to_string()));
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: EXPLAIN_QUERY_PLAN_COLUMNS.len(),
            });
            state.pc += 1;
            return Ok(StepResult::Row);
        }
    }

    /// Step function of `EXPLAIN QUERY PLAN FORMAT=JSON`: emit the whole plan
    /// as a single row holding one JSON document, then finish.
    fn explain_query_plan_json_step(
        &self,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
    ) -> Result<StepResult, Box<LimboError>> {
        turso_debug_assert!(state.column_count() == EXPLAIN_QUERY_PLAN_JSON_COLUMNS.len());
        if self.connection.is_closed() {
            // Connection is closed for whatever reason, rollback the transaction.
            let tx_state = self.connection.get_tx_state();
            if let TransactionState::Write { .. } = tx_state {
                pager.rollback_tx(&self.connection);
            }
            return Err(LimboError::InternalError("Connection closed".to_string()).into());
        }
        if self.maybe_request_interrupt(
            state,
            pager.io.as_ref(),
            state.metrics.vm_steps.saturating_sub(1),
        ) {
            return Ok(StepResult::Interrupt);
        }
        // The single row has already been returned when pc is non-zero.
        if state.pc != 0 {
            return Ok(StepResult::Done);
        }
        state.registers[0].set_value(Value::from_text(crate::translate::eqp::program_plan_json(
            self,
        )));
        state.result_row = Some(Row {
            values: &state.registers[0] as *const Register,
            count: EXPLAIN_QUERY_PLAN_JSON_COLUMNS.len(),
        });
        state.pc = 1;
        Ok(StepResult::Row)
    }

    /// `PRAGMA vdbe_trace`: prints the registers the previous opcode changed
    /// and the opcode about to run.
    #[inline(never)]
    fn trace_registers(&self, state: &mut ProgramState, insn: &Insn, vdbe_trace: bool) {
        if !vdbe_trace {
            return;
        }
        // Diff registers from PREVIOUS opcode
        // The last opcode (Halt) won't have its diff printed, but Halt
        // doesn't write to any registers
        if let Some(ref old) = state.pre_op_registers {
            for (i, (old_reg, new_reg)) in old.iter().zip(state.registers.iter()).enumerate() {
                if old_reg != new_reg {
                    match new_reg {
                        Register::Value(v) => eprintln!("R[{i}] = {v}"),
                        Register::Aggregate(_) => eprintln!("R[{i}] = <aggregate>"),
                        Register::Record(_) => eprintln!("R[{i}] = <record>"),
                    }
                }
            }
            state.pre_op_registers = None;
        }

        // Print CURRENT opcode
        if matches!(insn, Insn::Init { .. }) {
            eprintln!("VDBE Trace:");
        }
        eprintln!(
            "{}",
            explain::insn_to_str(
                self,
                state.pc,
                insn,
                String::new(),
                self.explain.comment_at(state.pc)
            )
        );
        // Snapshot for next iteration
        state.pre_op_registers = Some(state.registers.clone());
    }

    fn normal_step(
        &self,
        state: &mut ProgramState,
        pager: &Arc<Pager>,
        waker: Option<&Waker>,
    ) -> Result<StepResult, Box<LimboError>> {
        let enable_tracing = tracing::enabled!(tracing::Level::TRACE);
        let vdbe_trace = self.connection.get_vdbe_trace();
        // One flag for the per-instruction test; the two kinds of tracing are
        // told apart only once it is set.
        let trace_insns = enable_tracing || vdbe_trace;
        // Reborrow the instruction list once: reloading it through `self`
        // every iteration defeats LLVM's hoisting because the opcode call
        // below is opaque to it.
        let insns = self.insns.as_slice();
        // Invalidate the previous result row once per step call: rows are only
        // handed out between step calls, and ResultRow returns immediately
        // after setting a fresh one.
        let _ = state.result_row.take();
        // The outer loop runs once per step call and is re-entered only when an
        // instruction completed its IO inline; the inner loop dispatches
        // instructions without re-inspecting the completion slot every time.
        'io_check: loop {
            if let Some(io) = &state.io_completions {
                if !io.finished() {
                    io.set_waker(waker);
                    return Ok(StepResult::IO);
                }
                if let Some(err) = io.get_error() {
                    if pager.is_checkpointing() {
                        // Wrap IO errors that occurred during checkpointing in CheckpointFailed error,
                        // so that abort() knows not to try to rollback the transaction, because the transaction
                        // is already durable in the WAL and hence committed.
                        // This also lets the simulator know that it should shadow the results of the query because
                        // the write itself succeeded.
                        let checkpoint_err = LimboError::CheckpointFailed(err.to_string());
                        tracing::error!("Checkpoint failed: {checkpoint_err}");
                        if let Err(abort_err) =
                            self.abort(pager, Some(&checkpoint_err), state, true)
                        {
                            tracing::error!(
                                "Abort also failed during checkpoint error handling: {abort_err}"
                            );
                        }
                        pager.cleanup_after_checkpoint_failure();
                        return Err(checkpoint_err.into());
                    }
                    let err = err.into();
                    if let Err(abort_err) = self.abort(pager, Some(&err), state, true) {
                        tracing::error!("Abort failed during error handling: {abort_err}");
                    }
                    return Err(err.into());
                }
                state.io_completions = None;
            }
            // A trigger can return FAIL before the parent program reaches
            // Halt. FAIL keeps changes made by earlier rows, so their
            // index-method writes must finish before abort() releases the
            // statement savepoint and commits those partial changes. The
            // error is stored by the dispatch loop below, which then comes
            // back here, and by the IO arm of this block on resume.
            if state.pending_fail_prepare_error.is_some() {
                let fail_error = state
                    .pending_fail_prepare_error
                    .take()
                    .expect("checked is_some above");
                match execute::index_method_stage_statement_all(state) {
                    Ok(IOResult::Done(())) => {
                        if let Err(abort_err) = self.abort(pager, Some(&fail_error), state, true) {
                            tracing::error!(
                                "Abort failed after preparing FAIL index methods: {abort_err}"
                            );
                        }
                        return Err(fail_error.into());
                    }
                    Ok(IOResult::IO(io)) => {
                        state.pending_fail_prepare_error = Some(fail_error);
                        io.set_waker(waker);
                        if io.is_explicit_yield() {
                            return Ok(StepResult::Yield);
                        }
                        let finished = io.finished();
                        state.io_completions = Some(io);
                        if !finished {
                            return Ok(StepResult::IO);
                        }
                        continue 'io_check;
                    }
                    Err(prepare_error) => {
                        // FAIL may keep earlier base-table rows only when every
                        // matching index-method write was staged successfully.
                        // Once preparation fails, committing those rows would
                        // leave the table and index out of sync, so roll back the
                        // whole transaction while returning the real preparation
                        // error to the caller.
                        let rollback_error =
                            LimboError::Raise(ResolveType::Rollback, prepare_error.to_string());
                        if let Err(abort_err) =
                            self.abort(pager, Some(&rollback_error), state, true)
                        {
                            tracing::error!(
                                "Abort also failed after FAIL index-method preparation: \
                                 {abort_err}"
                            );
                        }
                        return Err(prepare_error);
                    }
                }
            }
            loop {
                // Closed/interrupt/deadline/progress checks run once every
                // CHECK_INTERVAL instructions instead of on each one (SQLite
                // similarly only checks at jump opcodes). vm_steps persists
                // across step calls, so the cadence spans the whole statement;
                // callers regain control at every returned row regardless.
                // The interval must stay a power of two so this gate is a
                // mask test, never a division, in the interpreter hot loop.
                // The gate mask lives in ProgramState and is re-derived from the
                // progress handler's interval only when the gate fires, so the
                // steady-state cost is one mask test with no atomics. A handler
                // with interval N < 64 narrows the mask at the next firing (a
                // one-time lag of at most CHECK_INTERVAL instructions; the
                // cadence is approximate by contract).
                const CHECK_INTERVAL: u64 = 64;
                const _: () = assert!(CHECK_INTERVAL.is_power_of_two());
                if state.metrics.vm_steps & state.check_mask == 0 {
                    let progress_ops = self.connection.progress_ops();
                    state.check_mask = if progress_ops == 0 || progress_ops >= CHECK_INTERVAL {
                        CHECK_INTERVAL - 1
                    } else {
                        progress_ops.next_power_of_two() - 1
                    };
                    if self.connection.is_closed() {
                        // Connection is closed for whatever reason, rollback the transaction.
                        let state = self.connection.get_tx_state();
                        if let TransactionState::Write { .. } = state {
                            pager.rollback_tx(&self.connection);
                        }
                        return Err(
                            LimboError::InternalError("Connection closed".to_string()).into()
                        );
                    }
                    let prev_steps = state.metrics.vm_steps.saturating_sub(state.check_mask + 1);
                    if self.maybe_request_interrupt(state, pager.io.as_ref(), prev_steps) {
                        self.abort(pager, None, state, true)?;
                        return Ok(StepResult::Interrupt);
                    }
                }

                let (insn, _) = &insns[state.pc as usize];
                if trace_insns {
                    if enable_tracing {
                        trace_insn(self, state.pc as InsnReference, insn);
                        crate::stack::trace_remaining("program_step:opcode");
                    }
                    self.trace_registers(state, insn, vdbe_trace);
                }

                // Always increment VM steps for every loop iteration
                state.metrics.vm_steps = state.metrics.vm_steps.wrapping_add(1);

                match insn::dispatch_insn(self, state, insn, pager) {
                    Ok(InsnFunctionStepResult::Step) => {
                        // Instruction completed, moving to next
                        state.metrics.insn_executed = state.metrics.insn_executed.wrapping_add(1);
                    }
                    Ok(InsnFunctionStepResult::Done) => {
                        // Instruction completed execution
                        state.metrics.insn_executed = state.metrics.insn_executed.wrapping_add(1);
                        state.auto_txn_cleanup = TxnCleanup::None;
                        return Ok(StepResult::Done);
                    }
                    Ok(InsnFunctionStepResult::IO(io)) => {
                        // Instruction not complete - waiting for I/O, will resume at same PC
                        io.set_waker(waker);
                        let is_yield = io.is_explicit_yield();
                        if is_yield {
                            // Yield: return control to the cooperative scheduler so
                            // other connections can make progress (e.g. release a
                            // contended lock). Don't store in io_completions —
                            // yields aren't pending I/O, so the instruction will
                            // simply re-execute on the next step.
                            return Ok(StepResult::Yield);
                        }
                        let finished = io.finished();
                        state.io_completions = Some(io);
                        if !finished {
                            return Ok(StepResult::IO);
                        }
                        // IO already finished: loop back to the completion check so
                        // errors are observed, then continue execution immediately.
                        continue 'io_check;
                    }
                    Ok(InsnFunctionStepResult::Row) => {
                        // Instruction completed (ResultRow already incremented PC)
                        state.metrics.insn_executed = state.metrics.insn_executed.wrapping_add(1);
                        return Ok(StepResult::Row);
                    }
                    Err(boxed_err) => match *boxed_err {
                        LimboError::Busy => {
                            // Instruction blocked - will retry at same PC
                            return Ok(StepResult::Busy);
                        }
                        LimboError::BusySnapshot
                            if self.connection.transaction_state.get()
                                == TransactionState::None =>
                        {
                            // For interactive transactions that are already in a read transaction, retrying BusySnapshot is pointless
                            // because the snapshot will continue to be stale no matter how many times we retry.
                            // However, for auto-commits or BEGIN IMMEDIATE, failing to promote to write transaction means it was rolled
                            // back, so auto-retrying can be useful.
                            return Ok(StepResult::Busy);
                        }
                        err if (matches!(err, LimboError::Constraint(_))
                            && self.resolve_type == ResolveType::Fail)
                            || matches!(err, LimboError::Raise(ResolveType::Fail, _)) =>
                        {
                            state.pending_fail_prepare_error = Some(err);
                            continue 'io_check;
                        }
                        err => {
                            if let Err(abort_err) = self.abort(pager, Some(&err), state, true) {
                                tracing::error!("Abort failed during error handling: {abort_err}");
                            }
                            return Err(err.into());
                        }
                    },
                }
            }
        }
    }

    #[instrument(skip_all, level = Level::DEBUG)]
    fn apply_view_deltas(
        &self,
        state: &mut ProgramState,
        rollback: bool,
        pager: &Arc<Pager>,
    ) -> IOResultOr<()> {
        use crate::types::IOResult;

        loop {
            match &state.view_delta_state {
                ViewDeltaCommitState::NotStarted => {
                    if self.connection.view_transaction_states.is_empty() {
                        return Ok(IOResult::Done(()));
                    }

                    if rollback {
                        // On rollback, just clear and done
                        self.connection.view_transaction_states.clear();
                        return Ok(IOResult::Done(()));
                    }

                    // Not a rollback - proceed with processing
                    let schema = self.connection.schema.read();

                    // Collect materialized views - they should all have storage
                    let mut views = Vec::new();
                    for view_name in self.connection.view_transaction_states.get_view_names() {
                        if let Some(view_mutex) = schema.get_materialized_view(&view_name) {
                            let view = view_mutex.lock();
                            let root_page = view.get_root_page();

                            // Materialized views should always have storage (root_page != 0)
                            turso_assert_ne!(
                                root_page, 0,
                                "Materialized view should have a root page",
                                { "view_name": view_name }
                            );

                            views.push(view_name);
                        }
                    }

                    state.view_delta_state = ViewDeltaCommitState::Processing {
                        views,
                        current_index: 0,
                    };
                }

                ViewDeltaCommitState::Processing {
                    views,
                    current_index,
                } => {
                    // At this point we know it's not a rollback
                    if *current_index >= views.len() {
                        // All done, clear the transaction states
                        self.connection.view_transaction_states.clear();
                        state.view_delta_state = ViewDeltaCommitState::Done;
                        return Ok(IOResult::Done(()));
                    }

                    let view_name = &views[*current_index];

                    let table_deltas = self
                        .connection
                        .view_transaction_states
                        .get(view_name)
                        .expect("view should have transaction state")
                        .get_table_deltas();

                    let schema = self.connection.schema.read();
                    if let Some(view_mutex) = schema.get_materialized_view(view_name) {
                        let mut view = view_mutex.lock();

                        // Create a DeltaSet from the per-table deltas
                        let mut delta_set = crate::incremental::compiler::DeltaSet::new();
                        for (table_name, delta) in table_deltas {
                            delta_set.insert(table_name, delta);
                        }

                        // Handle I/O from merge_delta - pass pager, circuit will create its own cursor
                        match view.merge_delta(delta_set, pager.clone())? {
                            IOResult::Done(_) => {
                                // Move to next view
                                state.view_delta_state = ViewDeltaCommitState::Processing {
                                    views: views.clone(),
                                    current_index: current_index + 1,
                                };
                            }
                            IOResult::IO(io) => {
                                // Return I/O, will resume at same index
                                return Ok(IOResult::IO(io));
                            }
                        }
                    }
                }

                ViewDeltaCommitState::Done => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    pub fn commit_txn(
        &self,
        pager: Arc<Pager>,
        program_state: &mut ProgramState,
        mv_store: Option<&Arc<MvStore>>,
        rollback: bool,
    ) -> IOResultOr<()> {
        if !rollback {
            turso_assert!(
                !matches!(
                    program_state.sequence_inner_tx_pending.as_ref(),
                    Some(pending) if pending.saved_outer.is_some()
                ),
                "cannot commit while a sequence inner tx has a saved outer user tx"
            );
        }

        // Apply view deltas with I/O handling
        match self.apply_view_deltas(program_state, rollback, &pager)? {
            IOResult::IO(io) => return Ok(IOResult::IO(io)),
            IOResult::Done(_) => {}
        }

        // Reset state for next use
        program_state.view_delta_state = ViewDeltaCommitState::NotStarted;
        // Drop virtual table cursors before the `is_nested_stmt()` check
        // below: a pragma virtual table cursor owns a nested helper statement
        // whose guard would otherwise make this top-level statement classify
        // itself as nested and skip transaction finalization entirely.
        program_state.close_virtual_table_cursors();
        let tx_state = self.connection.get_tx_state();
        if tx_state == TransactionState::None
            && matches!(program_state.commit_state, CommitState::Ready)
        {
            // No main transaction and no in-progress commit — check whether
            // any attached/temp database still has an active transaction before
            // bailing out. Defer these checks to here so the common case
            // (active main transaction) doesn't pay for the lock reads.
            let has_attached_mv_tx = self.connection.next_attached_mv_tx().is_some();
            let has_attached_wal_tx =
                self.connection
                    .with_all_attached_pagers_with_index(|pagers| {
                        pagers.iter().any(|(_, pager)| pager.holds_read_lock())
                    });
            if !has_attached_mv_tx && !has_attached_wal_tx {
                return Ok(IOResult::Done(()));
            }
        }
        if self.connection.is_nested_stmt() {
            // We don't want to commit on nested statements. Let parent handle it.
            return Ok(IOResult::Done(()));
        }
        let res = if let Some(mv_store) = mv_store {
            self.commit_txn_mvcc(pager, program_state, mv_store, rollback)
        } else {
            self.commit_txn_wal(pager, program_state, rollback)
        }?;
        if !res.is_io() {
            if self.change_cnt_on {
                self.connection
                    .set_changes(program_state.n_change.load(Ordering::SeqCst));
                self.connection
                    .add_total_changes(program_state.n_total_change.load(Ordering::SeqCst));
            }
            let transaction_finished = self.connection.auto_commit.load(Ordering::SeqCst)
                && self.connection.get_tx_state() == TransactionState::None;
            if transaction_finished {
                // Finalize the in-memory TEMP schema only when the outer
                // transaction actually finishes. Updating the committed temp
                // snapshot after every statement inside an explicit
                // transaction would make a later full ROLLBACK restore
                // uncommitted temp DDL.
                if rollback {
                    self.connection.rollback_temp_schema();
                } else {
                    self.connection.commit_temp_schema();
                }
            }
        }
        Ok(res)
    }

    fn commit_txn_wal(
        &self,
        pager: Arc<Pager>,
        program_state: &mut ProgramState,
        rollback: bool,
    ) -> IOResultOr<()> {
        let connection = self.connection.clone();
        let auto_commit = connection.auto_commit.load(Ordering::SeqCst);
        let tx_state = connection.get_tx_state();
        tracing::debug!(
            "Halt auto_commit {}, commit_state={:?}, tx_state={:?}",
            auto_commit,
            program_state.commit_state,
            tx_state,
        );
        if matches!(program_state.commit_state, CommitState::Committing) {
            // Normally a resumed commit still has an open write transaction.
            // The exception is the post-commit auto-checkpoint: commit_tx has
            // already committed the WAL, released the locks and cleared the
            // transaction state, and only the checkpoint is still in flight,
            // so the state is None.
            turso_assert!(
                matches!(
                    tx_state,
                    TransactionState::Write { .. } | TransactionState::None
                ),
                "invalid state for write commit step: {tx_state:?}"
            );
            self.step_end_write_txn(&pager, &connection, program_state, rollback)
        } else if matches!(program_state.commit_state, CommitState::CommittingAttached) {
            // Re-entry after IO yield from attached pager commit.
            match self.end_attached_write_txns(&connection, rollback)? {
                IOResult::Done(_) => {
                    program_state.commit_state = CommitState::Ready;
                    if pager.holds_read_lock() {
                        pager.end_read_tx();
                    }
                    self.end_attached_read_txns(&connection);
                    Ok(IOResult::Done(()))
                }
                IOResult::IO(io) => Ok(IOResult::IO(io)),
            }
        } else if auto_commit {
            match tx_state {
                TransactionState::Write { .. } => {
                    self.step_end_write_txn(&pager, &connection, program_state, rollback)
                }
                TransactionState::Read => {
                    connection.set_tx_state(TransactionState::None);
                    // Commit any attached write transactions that were opened
                    // independently of the main connection's transaction state.
                    // (e.g., UPDATE aux0.t SET ... only needs Read on main DB
                    // but holds a write lock on the attached pager.)
                    match self.end_attached_write_txns(&connection, rollback)? {
                        IOResult::Done(_) => {}
                        IOResult::IO(io) => {
                            program_state.commit_state = CommitState::CommittingAttached;
                            return Ok(IOResult::IO(io));
                        }
                    }
                    pager.end_read_tx();
                    self.end_attached_read_txns(&connection);
                    Ok(IOResult::Done(()))
                }
                TransactionState::None => {
                    match self.end_attached_write_txns(&connection, rollback)? {
                        IOResult::Done(_) => {}
                        IOResult::IO(io) => {
                            program_state.commit_state = CommitState::CommittingAttached;
                            return Ok(IOResult::IO(io));
                        }
                    }
                    self.end_attached_read_txns(&connection);
                    Ok(IOResult::Done(()))
                }
                TransactionState::PendingUpgrade { .. } => {
                    panic!("Unexpected transaction state: {tx_state:?} during auto-commit",)
                }
            }
        } else {
            Ok(IOResult::Done(()))
        }
    }

    /// Commit MVCC transactions across all databases in a multi-phase protocol:
    ///
    /// 1. **Main DB MVCC** — commit the main database's MvStore transaction.
    /// 2. **Attached MVCC** — commit each attached database's MvStore transaction.
    /// 3. **Attached WAL** — flush dirty pages on attached databases that use WAL
    ///    (e.g. :memory: attached while main is MVCC).
    ///
    /// **IMPORTANT**: This multi-phase commit is NOT atomic across databases.
    /// A crash between phases can leave the main and attached databases in
    /// inconsistent states (main committed, some attached DBs not committed).
    /// This matches SQLite's WAL mode behavior — cross-file atomicity only
    /// exists in legacy rollback journal mode, which we do not support.
    fn commit_txn_mvcc(
        &self,
        pager: Arc<Pager>,
        program_state: &mut ProgramState,
        mv_store: &Arc<MvStore>,
        rollback: bool,
    ) -> IOResultOr<()> {
        let conn = self.connection.clone();
        let auto_commit = conn.auto_commit.load(Ordering::SeqCst);
        if !auto_commit {
            return Ok(IOResult::Done(()));
        }

        // Phase 1: Commit main DB MVCC transaction
        if matches!(program_state.commit_state, CommitState::Ready) {
            if let Some(tx_id) = conn.get_mv_tx_id() {
                let state_machine = mv_store.commit_tx(tx_id, &conn, crate::MAIN_DB_ID)?;
                program_state.commit_state = CommitState::CommittingMvcc { state_machine };
            }
            // If no main MVCC tx, commit_state stays Ready and we fall
            // through directly to phase 2 (the CommittingMvcc and
            // CommittingAttachedMvcc checks will both miss).
        }

        if matches!(
            program_state.commit_state,
            CommitState::CommittingMvcc { .. }
        ) {
            let CommitState::CommittingMvcc { state_machine } = &mut program_state.commit_state
            else {
                unreachable!()
            };
            match self.step_end_mvcc_txn(state_machine, mv_store)? {
                IOResult::Done(_) => {
                    assert!(state_machine.is_finalized());
                    conn.set_mv_tx(None);
                    conn.set_tx_state(TransactionState::None);
                    pager.end_read_tx();
                    program_state.commit_state = CommitState::Ready;
                    // Fall through to attached phase
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }

        // Phase 2: Commit MVCC transactions on attached databases
        // Resume an in-progress attached MVCC commit
        if matches!(
            program_state.commit_state,
            CommitState::CommittingAttachedMvcc { .. }
        ) {
            let (step_result, db_id) = {
                let CommitState::CommittingAttachedMvcc {
                    state_machine,
                    db_id,
                    mv_store: ref attached_mv,
                } = &mut program_state.commit_state
                else {
                    unreachable!()
                };
                (state_machine.step(attached_mv)?, *db_id)
            };
            match step_result {
                IOResult::Done(_) => {
                    let attached_pager = conn
                        .get_pager_from_database_index(&db_id)
                        .expect("attached MVCC transaction should always have a pager");
                    conn.publish_database_schema(db_id);
                    conn.set_mv_tx_for_db(db_id, None);
                    attached_pager.end_read_tx();
                    // Fall through to look for more
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }

        // Start/continue committing remaining attached MVCC transactions
        loop {
            let Some((db_id, tx_id, _mode)) = conn.next_attached_mv_tx() else {
                break;
            };
            let Some(attached_mv_store) = conn.mv_store_for_db(db_id) else {
                conn.set_mv_tx_for_db(db_id, None);
                continue;
            };
            let mut state_machine = match attached_mv_store.commit_tx(tx_id, &conn, db_id) {
                Ok(sm) => sm,
                Err(e) => {
                    tracing::error!(
                        db_id,
                        "attached DB commit failed after main DB already committed; \
                         cross-database state is inconsistent: {e}"
                    );
                    // Rollback remaining uncommitted attached MVCC transactions
                    // so they don't block checkpointing until connection close.
                    conn.rollback_attached_mvcc_txs(true);
                    return Err(e.into());
                }
            };
            match state_machine.step(&attached_mv_store)? {
                IOResult::Done(_) => {
                    let attached_pager = conn
                        .get_pager_from_database_index(&db_id)
                        .expect("attached MVCC transaction should always have a pager");
                    conn.publish_database_schema(db_id);
                    conn.set_mv_tx_for_db(db_id, None);
                    attached_pager.end_read_tx();
                    continue;
                }
                IOResult::IO(io) => {
                    program_state.commit_state = CommitState::CommittingAttachedMvcc {
                        state_machine,
                        db_id,
                        mv_store: attached_mv_store,
                    };
                    return Ok(IOResult::IO(io));
                }
            }
        }

        // Phase 3: Commit WAL transactions on attached databases that don't use MVCC.
        // When the main DB uses MVCC, we route through commit_txn_mvcc, but attached
        // DBs may use WAL mode and need their dirty pages committed via the WAL path.
        if matches!(program_state.commit_state, CommitState::CommittingAttached) {
            // Re-entry after IO yield from attached WAL pager commit.
            match self.end_attached_write_txns(&conn, rollback)? {
                IOResult::Done(_) => {
                    program_state.commit_state = CommitState::Ready;
                    self.end_attached_read_txns(&conn);
                    return Ok(IOResult::Done(()));
                }
                IOResult::IO(io) => return Ok(IOResult::IO(io)),
            }
        }

        match self.end_attached_write_txns(&conn, rollback)? {
            IOResult::Done(_) => {}
            IOResult::IO(io) => {
                program_state.commit_state = CommitState::CommittingAttached;
                return Ok(IOResult::IO(io));
            }
        }
        self.end_attached_read_txns(&conn);

        program_state.commit_state = CommitState::Ready;
        Ok(IOResult::Done(()))
    }

    #[instrument(skip(self, pager, connection, program_state), level = Level::DEBUG)]
    fn step_end_write_txn(
        &self,
        pager: &Arc<Pager>,
        connection: &Connection,
        program_state: &mut ProgramState,
        rollback: bool,
    ) -> IOResultOr<()> {
        let commit_state = &mut program_state.commit_state;
        if matches!(commit_state, CommitState::CommittingAttached) {
            // Resume committing attached pagers after IO yield.
            match self.end_attached_write_txns(connection, rollback)? {
                IOResult::Done(_) => {
                    *commit_state = CommitState::Ready;
                }
                IOResult::IO(io) => {
                    return Ok(IOResult::IO(io));
                }
            }
            // Release read locks on attached pagers that only had read transactions
            // (end_attached_write_txns only handles pagers with write locks).
            self.end_attached_read_txns(connection);
            return Ok(IOResult::Done(()));
        }
        let txn_finish_result = if !rollback {
            pager.commit_tx(connection, connection.get_sync_mode(), true)
        } else {
            pager.rollback_tx(connection);
            Ok(IOResult::Done(()))
        };
        tracing::debug!("txn_finish_result: {:?}", txn_finish_result);
        match txn_finish_result? {
            IOResult::Done(_) => {
                // Main pager commit done, now commit attached database pagers
                match self.end_attached_write_txns(connection, rollback)? {
                    IOResult::Done(_) => {
                        *commit_state = CommitState::Ready;
                    }
                    IOResult::IO(io) => {
                        *commit_state = CommitState::CommittingAttached;
                        return Ok(IOResult::IO(io));
                    }
                }
            }
            IOResult::IO(io) => {
                tracing::trace!("Cacheflush IO");
                *commit_state = CommitState::Committing;
                return Ok(IOResult::IO(io));
            }
        }
        // Release read locks on attached pagers that only had read transactions
        // (end_attached_write_txns only handles pagers with write locks).
        self.end_attached_read_txns(connection);
        Ok(IOResult::Done(()))
    }

    /// End write transactions on all attached databases that hold write locks.
    /// Iterates ALL attached pagers (not just the current program's write_databases)
    /// because in explicit transactions, the COMMIT statement's program may differ
    /// from the statement that acquired the attached write lock.
    /// On IO yield, already-committed pagers are skipped on re-entry via holds_write_lock().
    fn end_attached_write_txns(&self, connection: &Connection, rollback: bool) -> IOResultOr<()> {
        connection.with_all_attached_pagers_with_index(|pagers| {
            for (db_id, attached_pager) in pagers {
                let db_id = *db_id;
                // MVCC-enabled attached DBs are committed in commit_txn_mvcc phase 2
                if connection.mv_store_for_db(db_id).is_some() {
                    continue;
                }
                if !attached_pager.holds_write_lock() {
                    continue;
                }
                if !rollback {
                    // Commit dirty pages to WAL, then end write+read transactions.
                    // We disable auto-checkpoint and avoid pager.commit_tx() since
                    // the checkpoint logic can leave read locks held.
                    match attached_pager.commit_wal(
                        WalAutoActions::empty(),
                        connection.get_sync_mode_for_database(db_id)?,
                        connection.get_data_sync_retry(),
                    ) {
                        Ok(IOResult::Done(_)) => {}
                        Ok(IOResult::IO(io)) => {
                            // IO pending — return so the caller can yield and re-enter.
                            // commit_wal tracks its own internal state, so calling
                            // it again on re-entry will resume correctly.
                            return Ok(IOResult::IO(io));
                        }
                        Err(e) => return Err(e),
                    }
                    // WAL commit succeeded — publish the connection-local schema
                    // changes to the shared Database so other connections can see them.
                    connection.publish_database_schema(db_id);
                    attached_pager.end_write_tx();
                    attached_pager.end_read_tx();
                    attached_pager.commit_wal_end();
                } else {
                    // Discard any local schema changes on rollback
                    connection.database_schemas().write().remove(&db_id);
                    attached_pager.rollback_attached();
                }
            }
            Ok(IOResult::Done(()))
        })
    }

    /// End read transactions on all attached databases that had transactions started.
    fn end_attached_read_txns(&self, connection: &Connection) {
        connection.with_all_attached_pagers_with_index(|pagers| {
            pagers.iter().for_each(|(db_id, attached_pager)| {
                if connection.mv_store_for_db(*db_id).is_some() {
                    // MVCC-enabled attached DBs don't use WAL read transactions, so skip.
                    return;
                }
                if attached_pager.holds_write_lock() {
                    // Attached pager has a write lock, so its read transaction was ended by end_attached_write_txns: skip.
                    return;
                }
                if attached_pager.holds_read_lock() {
                    attached_pager.end_read_tx();
                }
            });
        })
    }

    #[instrument(skip(self, commit_state, mv_store), level = Level::DEBUG)]
    fn step_end_mvcc_txn(
        &self,
        commit_state: &mut StateMachine<Box<MvccCommitStateMachine>>,
        mv_store: &Arc<MvStore>,
    ) -> IOResultOr<()> {
        commit_state.step(mv_store)
    }

    /// Aborts the program due to various conditions (explicit error, interrupt or reset of unfinished statement) by rolling back the transaction
    /// This method is no-op if program was already finished (either aborted or executed to completion)
    /// Returns an error if cleanup operations (savepoint rollback/release) fail.
    /// `self_counted` is true while this statement is still included in
    /// `Connection::n_active_root_statements` (aborts during `step`).
    /// Statement teardown passes its actual counted state: a statement that
    /// already finished was released on Done or on its step error, so the
    /// counted statements are all siblings (see
    /// [`ProgramState::can_autocommit_now`]).
    pub fn abort(
        &self,
        pager: &Arc<Pager>,
        err: Option<&LimboError>,
        state: &mut ProgramState,
        self_counted: bool,
    ) -> Result<()> {
        fn capture_abort_error(
            abort_error: &mut Option<LimboError>,
            err: LimboError,
            context: &str,
        ) {
            tracing::error!("{context}: {err}");
            if abort_error.is_none() {
                *abort_error = Some(err);
            }
        }

        let mut abort_error: Option<LimboError> = None;
        state.explicit_checkpoint_guard = None;
        // PRAGMA journal_mode owns its MVCC checkpoint in active_op_state rather
        // than commit_state. Clean it before transaction abort logic inspects
        // pager checkpoint state or reset drops the opcode state.
        if let Err(err) = state.active_op_state.cleanup_journal_mode_checkpoint() {
            capture_abort_error(
                &mut abort_error,
                err,
                "Failed to clean up journal-mode checkpoint during abort",
            );
        }
        // MVCC auto-checkpoint is owned by commit_state, not by normal_step().
        // If its yielded I/O fails, normal_step sees the error before
        // CommitStateMachine::Checkpoint gets another step, so the checkpoint
        // state machine cannot run its own error cleanup. abort() is the first
        // statement cleanup path that still owns that commit_state.
        state.commit_state.cleanup_mvcc_checkpoint_state();
        // If a CommitStateMachine was non-terminal when the program was
        // aborted — Statement dropped mid-IO yield, or `?` propagated a Busy
        // out of BeginCommitLogicalLog / SyncLogicalLog — release the locks
        // it acquired (`pager_commit_lock`, `exclusive_tx`) and roll back the
        // orphan tx. Without this the tx stays in `Preparing`, the next op on
        // this connection trips a `turso_assert_eq!(Active)`, and any other
        // writer parks forever on the leaked `pager_commit_lock`. The
        // following err-match's no-rollback arms (Busy / TxError / etc.)
        // would otherwise skip this cleanup.
        state
            .commit_state
            .cleanup_abandoned_mvcc_commit(&self.connection);

        // ParseSchema owns a nested helper statement on this connection and
        // stores `auto_commit=false` for its duration. If the program aborts
        // while that state is live (error mid-schema-row), release it here:
        // restore the saved auto_commit and drop the inner statement so its
        // nested guard is released BEFORE the `is_nested_stmt()` check below.
        // Otherwise this top-level statement misclassifies itself as nested,
        // skips transaction rollback, and leaks the DDL's exclusive MVCC tx
        // (and the cleared auto_commit) into subsequent statements — which
        // then appear to succeed without ever committing.
        if let Some(inner) = state.active_op_state.take_parse_schema_if_active() {
            self.connection
                .auto_commit
                .store(inner.previous_auto_commit(), Ordering::SeqCst);
            drop(inner);
        }

        // VACUUM (and VACUUM INTO) state can own internal helper statements whose drop path
        // releases nested guards. Clean it before checking whether this program
        // is itself nested; otherwise abort could skip top-level cleanup.
        if let Err(err) = execute::cleanup_vacuum_state(&self.connection, state) {
            capture_abort_error(
                &mut abort_error,
                err,
                "Failed to clean up VACUUM state during abort",
            );
        }

        // Virtual table cursors (pragma table-valued functions) also own
        // nested helper statements whose drop releases nested guards. Drop
        // them before the `is_nested_stmt()` check below for the same reason.
        state.close_virtual_table_cursors();
        // RAISE(IGNORE) rolls nothing back — halt() already staged the
        // trigger's index-method writes and the kept rows keep their index
        // entries — so it must not discard staged index-method work here.
        // FAIL-class errors likewise keep the changes made before the error:
        // their index-method writes were staged by the interception in
        // `program_step` before abort() was called (and, on the autocommit
        // path, already committed by halt()), so discarding cursor state here
        // would deliver a rollback outcome for work that commits below.
        let keeps_prior_changes = match err {
            Some(LimboError::RaiseIgnore) => true,
            Some(LimboError::Raise(ResolveType::Fail, _)) => true,
            Some(LimboError::Constraint(_)) => self.resolve_type == ResolveType::Fail,
            _ => false,
        };
        if (err.is_some() || state.execution_state.is_running()) && !keeps_prior_changes {
            execute::index_method_abort_statement_all(state);
        }

        // Only end trigger execution if the subprogram was actually running.
        // Cached (pooled) statements may be dropped after their trigger execution
        // was already ended by op_program; calling end again would pop the wrong
        // entry from the executing_triggers stack.
        if self.is_trigger_subprogram() && state.execution_state.is_running() {
            self.connection.end_trigger_execution();
        }
        // Roll back any in-flight autonomous sequence inner-tx and restore
        // the connection's `mv_tx` slot to the saved outer tx BEFORE the
        // statement-savepoint rollback path runs below. Without this, the
        // savepoint rollback below reads `connection.get_mv_tx_id()` and
        // gets the now-dead inner tx id — there is no savepoint on the
        // inner tx, so the outer tx's statement-level changes (rows
        // inserted before the failing nextval) never get rolled back.
        // Roll back the statement-level MVCC savepoint on the OUTER tx
        // before any downstream cleanup re-targets `connection.mv_tx`.
        // The savepoint was opened by `begin_statement` against the outer
        // tx; if a `SequenceBeginInnerTx` swap happened mid-statement the
        // connection's `mv_tx` slot now points at the (failed) inner tx,
        // and `end_statement`'s `rollback_first_savepoint` would walk the
        // wrong tx — leaving the outer tx's pre-error writes durable on
        // commit. Use `saved_outer` from the pending inner-tx record to
        // pick the right tx id, run the savepoint rollback explicitly,
        // and let the existing `Statement::cleanup_orphaned_seq_inner_tx`
        // (called from `Statement::step` after this abort returns)
        // perform the inner-tx rollback + mv_tx restoration.
        if err.is_some() && !pager.is_checkpointing() {
            if let Some(pending) = state.sequence_inner_tx_pending.as_ref() {
                if let Some((outer_tx_id, _)) = pending.saved_outer {
                    if let Some(mv_store) = self.connection.mv_store_for_db(pending.db) {
                        if let Err(e) = mv_store.rollback_first_savepoint(outer_tx_id) {
                            tracing::error!(
                                "Failed to rollback outer-tx savepoint after sequence \
                                 inner-tx aborted: {e}"
                            );
                        }
                    }
                }
            }
        }
        // Errors from nested statements are handled by the parent statement.
        if !self.connection.is_nested_stmt() && !self.is_trigger_subprogram() {
            let unfinished_statement_reset_or_drop =
                err.is_none() && state.execution_state.is_running();
            let inside_explicit_transaction = !self.connection.get_auto_commit();
            let unfinished_writer = state.is_active_write;
            let can_rollback_just_this_statement =
                state.auto_txn_cleanup == TxnCleanup::RollbackSavepoint;

            let poison_tx = unfinished_statement_reset_or_drop
                && inside_explicit_transaction
                && unfinished_writer
                && !can_rollback_just_this_statement;
            if poison_tx {
                // Example: BEGIN; UPDATE rows SET ... writes one row, then
                // returns IO before reaching Done. If the caller drops that
                // statement, we cannot pretend COMMIT is still safe: there is
                // no statement savepoint to undo only the partial UPDATE.
                self.connection.mark_tx_poisoned();
            }

            let can_autocommit_now = state.can_autocommit_now(&self.connection, self_counted);
            let is_mvcc = state.mv_store(&self.connection).is_some();
            let changed_shared_mvcc_auto_txn = !can_autocommit_now
                && state.auto_txn_cleanup == TxnCleanup::RollbackTxn
                && state.n_change.load(Ordering::SeqCst) > 0;
            if changed_shared_mvcc_auto_txn {
                turso_assert!(
                    is_mvcc,
                    "shared autocommit transaction needed full rollback outside MVCC"
                );
                // A writer changed rows in an MVCC autocommit transaction, but
                // a sibling reader is still holding that transaction open. The
                // writer had no statement savepoint, so the only safe cleanup
                // is rolling back the whole MVCC transaction.
            }
            let must_rollback_tx_if_needed = can_autocommit_now || changed_shared_mvcc_auto_txn;
            if err.is_some() && !pager.is_checkpointing() {
                // For ON CONFLICT FAIL, do NOT rollback the statement savepoint —
                // changes made before the error should persist.
                // For all other resolve types (ABORT, ROLLBACK, etc.), rollback the statement.
                let is_fail_constraint = (matches!(err, Some(LimboError::Constraint(_)))
                    && self.resolve_type == ResolveType::Fail)
                    || matches!(err, Some(LimboError::Raise(ResolveType::Fail, _)));
                if !is_fail_constraint {
                    if let Err(end_stmt_err) = state.end_statement(
                        &self.connection,
                        pager,
                        EndStatement::RollbackSavepoint,
                    ) {
                        capture_abort_error(
                            &mut abort_error,
                            end_stmt_err,
                            "Failed to rollback statement savepoint during abort",
                        );
                    }
                }
            }
            match err {
                // Transaction errors, e.g. trying to start a nested transaction, do not cause a rollback.
                Some(LimboError::TxError(_)) => {}
                // Table locked errors, e.g. trying to checkpoint in an interactive transaction, do not cause a rollback.
                Some(LimboError::TableLocked) => {}
                // Busy errors do not cause a rollback.
                Some(LimboError::Busy) => {}
                // Same-connection "SQL statements in progress" rejections do
                // not cause a rollback either: the rejected operation was
                // refused before it touched any transaction or savepoint
                // state, and the in-progress statement it collided with must
                // keep running unharmed.
                Some(LimboError::StatementsInProgress(_)) => {}
                // BusySnapshot errors do not cause a rollback either - user must rollback explicitly.
                // BusySnapshot is distinct from Busy in that a busy_timeout or handler should not be
                // used because it will not help - the snapshot is permanently stale and rollback is
                // the only way out for this poor transaction.
                Some(LimboError::BusySnapshot) => {}
                // Schema updated errors do not cause a rollback; the statement will be reprepared and retried,
                // and the caller is expected to handle transaction cleanup explicitly if needed.
                Some(LimboError::SchemaUpdated) => {}
                Some(LimboError::WriteWriteConflict | LimboError::SchemaConflict) => {
                    // These MVCC errors mean the current transaction cannot
                    // commit. Roll it back even if this statement opened a
                    // statement savepoint, as DDL does.
                    if let Err(err) = self.rollback_pending_sequence_outer_tx(state) {
                        capture_abort_error(
                            &mut abort_error,
                            err,
                            "Failed to rollback saved outer transaction after sequence conflict",
                        );
                    }
                    self.rollback_current_txn(pager);
                    self.connection.set_changes(0);
                }
                // Foreign key constraint errors: ON CONFLICT does NOT apply to FK violations.
                // FK errors always behave like ABORT: rollback statement,
                // rollback transaction in autocommit mode.
                Some(LimboError::ForeignKeyConstraint(_)) => {
                    if must_rollback_tx_if_needed {
                        self.rollback_current_txn(pager);
                    }
                    self.connection.set_changes(0);
                }
                // Constraint and RAISE errors: behavior depends on the effective resolve type.
                // For normal constraints, the resolve type comes from the statement (ON CONFLICT).
                // For RAISE errors, the resolve type is embedded in the error variant itself.
                // - ROLLBACK: rollback the entire transaction regardless of autocommit mode
                // - FAIL: don't rollback anything - changes persist, transaction stays active
                // - ABORT (default): rollback statement, rollback txn if autocommit
                Some(LimboError::Constraint(_)) | Some(LimboError::Raise(_, _)) => {
                    let effective_resolve = match err {
                        Some(LimboError::Raise(rt, _)) => *rt,
                        _ => self.resolve_type,
                    };
                    match effective_resolve {
                        ResolveType::Rollback => {
                            self.rollback_current_txn(pager);
                            // All deferred FK violations are undone by the full rollback.
                            self.connection.clear_deferred_foreign_key_violations();
                        }
                        ResolveType::Fail => {
                            // FAIL: Don't rollback the transaction.
                            // Changes made before the error persist.
                            if let Err(end_stmt_err) = state.end_statement(
                                &self.connection,
                                pager,
                                EndStatement::ReleaseSavepoint,
                            ) {
                                capture_abort_error(
                                    &mut abort_error,
                                    end_stmt_err,
                                    "Failed to release statement savepoint during abort",
                                );
                            }
                            if can_autocommit_now {
                                // Autocommit FAIL: commit partial changes.
                                // This matches halt()'s FAIL+autocommit path.
                                // Index-method writes were already staged by
                                // the FAIL interception in `program_step`
                                // (the resumable path) before abort() ran, so
                                // there is nothing left to stage here.
                                let mv_store = self.connection.mv_store();
                                if let Err(e) = execute::vtab_commit_all(&self.connection) {
                                    capture_abort_error(
                                        &mut abort_error,
                                        e,
                                        "vtab_commit_all failed during FAIL abort",
                                    );
                                }
                                let mut committed = false;
                                loop {
                                    match self.commit_txn(
                                        pager.clone(),
                                        state,
                                        mv_store.as_ref(),
                                        false,
                                    ) {
                                        Ok(IOResult::Done(_)) => {
                                            committed = true;
                                            break;
                                        }
                                        Ok(IOResult::IO(io)) => {
                                            if let Err(e) = io.wait(pager.io.as_ref()) {
                                                capture_abort_error(
                                                    &mut abort_error,
                                                    e,
                                                    "IO error during FAIL commit in abort",
                                                );
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            capture_abort_error(
                                                &mut abort_error,
                                                *e,
                                                "commit_txn failed during FAIL abort",
                                            );
                                            break;
                                        }
                                    }
                                }
                                // Deliver the committed outcome exactly once.
                                // For a plain constraint error with FAIL
                                // resolution, halt() already committed and
                                // delivered it before returning the error;
                                // only the trigger RAISE(FAIL) shape reaches
                                // the commit through this arm.
                                let halt_already_delivered =
                                    matches!(err, Some(LimboError::Constraint(_)));
                                if committed && !halt_already_delivered {
                                    execute::index_method_on_transaction_committed_all(
                                        state,
                                        &self.connection,
                                    );
                                }
                            }
                        }
                        _ => {
                            if must_rollback_tx_if_needed {
                                self.rollback_current_txn(pager);
                            }
                        }
                    }
                    let last_change = match effective_resolve {
                        ResolveType::Fail => state.n_change.load(Ordering::SeqCst),
                        _ => 0,
                    };
                    self.connection.set_changes(last_change);
                }
                Some(LimboError::RaiseIgnore) => {
                    tracing::error!(
                        "BUG: RaiseIgnore reached abort() - should be caught by op_program"
                    );
                    debug_assert!(
                        false,
                        "RaiseIgnore should be caught by op_program, not reach abort"
                    );
                }
                _ => match state.auto_txn_cleanup {
                    TxnCleanup::RollbackTxn => {
                        if must_rollback_tx_if_needed {
                            self.rollback_current_txn(pager);
                        }
                    }
                    TxnCleanup::RollbackSavepoint => {
                        if can_autocommit_now {
                            self.rollback_current_txn(pager);
                        } else if err.is_none() && !pager.is_checkpointing() {
                            if let Err(end_stmt_err) = state.end_statement(
                                &self.connection,
                                pager,
                                EndStatement::RollbackSavepoint,
                            ) {
                                capture_abort_error(
                                    &mut abort_error,
                                    end_stmt_err,
                                    "Failed to rollback statement savepoint during abort",
                                );
                            }
                        }
                    }
                    TxnCleanup::None => {
                        if can_autocommit_now
                            || (!self.connection.get_auto_commit() && err.is_some())
                        {
                            self.rollback_current_txn(pager);
                        }
                    }
                },
            }
        }
        if state.uses_subjournal {
            pager.stop_use_subjournal();
            state.uses_subjournal = false;
        }
        state.auto_txn_cleanup = TxnCleanup::None;
        if let Some(err) = abort_error {
            return Err(err);
        }
        Ok(())
    }

    fn rollback_current_txn(&self, pager: &Arc<Pager>) {
        self.connection.rollback_current_txn_state(pager, true);
        self.connection.set_cdc_transaction_id(-1);
    }

    /// MVCC sequence operations run in a separate inner tx, which temporarily
    /// replaces the connection's `mv_tx` slot. If a transaction-level conflict
    /// happens while that swap is active, the user transaction lives only in
    /// `saved_outer`, not in `connection.mv_tx`. Roll it back here and clear
    /// `saved_outer` so later statement cleanup cannot restore a transaction
    /// that has already been aborted.
    fn rollback_pending_sequence_outer_tx(&self, state: &mut ProgramState) -> Result<()> {
        let Some(pending) = state.sequence_inner_tx_pending.as_mut() else {
            return Ok(());
        };
        let db = pending.db;
        let (outer_tx_id, _) = pending.saved_outer.take().ok_or_else(|| {
            LimboError::InternalError(
                "sequence conflict rollback had pending inner transaction without saved outer \
                 transaction"
                    .to_string(),
            )
        })?;
        let mv_store = self.connection.mv_store_for_db(db).ok_or_else(|| {
            LimboError::InternalError(
                "sequence inner transaction has no MV store during conflict rollback".to_string(),
            )
        })?;
        let pager = self.connection.get_pager_from_database_index(&db)?;
        if !mv_store.is_tx_rollbackable(outer_tx_id) {
            return Err(LimboError::InternalError(format!(
                "saved sequence outer transaction {outer_tx_id} is not rollbackable during \
                 conflict rollback"
            )));
        }
        mv_store.rollback_tx(outer_tx_id, pager, &self.connection, db);
        // `rollback_tx` clears the connection's MVCC tx slot for this db. The caller's
        // generic rollback then has no current tx to inspect, so it will not flip
        // autocommit for us.
        self.connection.auto_commit.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn is_trigger_subprogram(&self) -> bool {
        self.trigger.is_some() || self.is_subprogram
    }
}

impl Deref for Program {
    type Target = PreparedProgram;

    fn deref(&self) -> &PreparedProgram {
        &self.prepared
    }
}

/// Split a register slice into an immutable ref and a mutable ref at two distinct indices.
pub(crate) fn split_registers(
    registers: &mut [Register],
    src: usize,
    dst: usize,
) -> (&Register, &mut Register) {
    debug_assert_ne!(src, dst, "split_registers: src and dst must differ");
    if src < dst {
        let (left, right) = registers.split_at_mut(dst);
        (&left[src], &mut right[0])
    } else {
        let (left, right) = registers.split_at_mut(src);
        (&right[0], &mut left[dst])
    }
}

pub fn registers_to_ref_values<'a>(
    registers: &'a [Register],
) -> impl ExactSizeIterator<Item = ValueRef<'a>> {
    registers.iter().map(|reg| reg.get_value().as_ref())
}

#[instrument(skip(program), level = Level::DEBUG)]
fn trace_insn(program: &Program, addr: InsnReference, insn: &Insn) {
    tracing::trace!(
        "\n{}",
        explain::insn_to_str(
            program,
            addr,
            insn,
            String::new(),
            program.explain.comment_at(addr)
        )
    );
}

pub trait FromValueRow<'a> {
    fn from_value(value: &'a Value) -> Result<Self>
    where
        Self: Sized + 'a;
}

impl<'a> FromValueRow<'a> for i64 {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Numeric(Numeric::Integer(i)) => Ok(*i),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for f64 {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Numeric(Numeric::Float(f)) => Ok(f64::from(*f)),
            _ => Err(LimboError::ConversionError("Expected integer value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for String {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.as_str().to_string()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for &'a str {
    fn from_value(value: &'a Value) -> Result<Self> {
        match value {
            Value::Text(s) => Ok(s.as_str()),
            _ => Err(LimboError::ConversionError("Expected text value".into())),
        }
    }
}

impl<'a> FromValueRow<'a> for &'a Value {
    fn from_value(value: &'a Value) -> Result<Self> {
        Ok(value)
    }
}

impl Row {
    pub fn get<'a, T: FromValueRow<'a> + 'a>(&'a self, idx: usize) -> Result<T> {
        let value = unsafe {
            self.values
                .add(idx)
                .as_ref()
                .expect("row value pointer should be valid")
        };
        let value = match value {
            Register::Value(value) => value,
            _ => unreachable!("a row should be formed of values only"),
        };
        T::from_value(value)
    }

    pub fn get_value(&self, idx: usize) -> &Value {
        let value = unsafe {
            self.values
                .add(idx)
                .as_ref()
                .expect("row value pointer should be valid")
        };
        match value {
            Register::Value(value) => value,
            _ => unreachable!("a row should be formed of values only"),
        }
    }

    pub fn get_values(&self) -> impl Iterator<Item = &Value> {
        let values = unsafe { std::slice::from_raw_parts(self.values, self.count) };
        // This should be ownedvalues
        // TODO: add check for this
        values.iter().map(|v| v.get_value())
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Extension trait for `ValueIterator` that allows writing directly to a `Register`
/// without allocating intermediate `ValueRef` values.
pub trait ValueIteratorExt {
    /// Skips `n` elements and writes the value directly to the register.
    /// Returns `Some(Ok(()))` on success, `Some(Err(...))` on parse error,
    /// or `None` if there are fewer than `n+1` elements.
    fn nth_into_register(&mut self, n: usize, dest: &mut Register) -> Option<Result<()>>;

    /// Skips `skip` elements, then decodes one value into each register of `dests` in order.
    /// Returns the number of registers filled, which is less than `dests.len()` when the record
    /// has fewer elements; registers past the returned count are left untouched.
    fn decode_into_registers_after(&mut self, skip: usize, dests: &mut [Register])
        -> Result<usize>;
}

impl<'a> ValueIteratorExt for crate::types::ValueIterator<'a> {
    #[inline(always)]
    fn nth_into_register(&mut self, n: usize, dest: &mut Register) -> Option<Result<()>> {
        use crate::storage::sqlite3_ondisk::read_varint;
        use crate::types::{get_serial_type_size, Extendable, Text};

        let mut header = self.header_section_ref();
        let mut data = self.data_section_ref();

        // Skip n elements
        let mut data_sum = 0;
        for _ in 0..n {
            if header.is_empty() {
                return None;
            }

            let (serial_type, bytes_read) = match read_varint(header) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            header = &header[bytes_read..];

            data_sum += match get_serial_type_size(serial_type) {
                Ok(size) => size,
                Err(e) => return Some(Err(e)),
            };
        }

        if data_sum > data.len() {
            return Some(Err(LimboError::Corrupt(
                "Data section too small for indicated serial type size".into(),
            )));
        }
        data = &data[data_sum..];

        // Read the serial type for the target element
        if header.is_empty() {
            return None;
        }

        let (serial_type, bytes_read) = match read_varint(header) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        // Update iterator state
        self.set_header_section(&header[bytes_read..]);

        // Decode directly into register based on serial type
        match serial_type {
            // NULL
            0 => {
                self.set_data_section(data);
                dest.set_null();
            }
            // I8
            1 => {
                if unlikely(data.is_empty()) {
                    return Some(Err(LimboError::Corrupt("Invalid 1-byte int".into())));
                }
                self.set_data_section(&data[1..]);
                dest.set_int(data[0] as i8 as i64);
            }
            // I16
            2 => {
                if unlikely(data.len() < 2) {
                    return Some(Err(LimboError::Corrupt("Invalid 2-byte int".into())));
                }
                self.set_data_section(&data[2..]);
                dest.set_int(i16::from_be_bytes([data[0], data[1]]) as i64);
            }
            // I24
            3 => {
                if unlikely(data.len() < 3) {
                    return Some(Err(LimboError::Corrupt("Invalid 3-byte int".into())));
                }
                self.set_data_section(&data[3..]);
                let sign_extension = if data[0] <= 0x7F { 0 } else { 0xFF };
                dest.set_int(
                    i32::from_be_bytes([sign_extension, data[0], data[1], data[2]]) as i64,
                );
            }
            // I32
            4 => {
                if unlikely(data.len() < 4) {
                    return Some(Err(LimboError::Corrupt("Invalid 4-byte int".into())));
                }
                self.set_data_section(&data[4..]);
                dest.set_int(i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as i64);
            }
            // I48
            5 => {
                if unlikely(data.len() < 6) {
                    return Some(Err(LimboError::Corrupt("Invalid 6-byte int".into())));
                }
                self.set_data_section(&data[6..]);
                let sign_extension = if data[0] <= 0x7F { 0 } else { 0xFF };
                dest.set_int(i64::from_be_bytes([
                    sign_extension,
                    sign_extension,
                    data[0],
                    data[1],
                    data[2],
                    data[3],
                    data[4],
                    data[5],
                ]));
            }
            // I64
            6 => {
                if unlikely(data.len() < 8) {
                    return Some(Err(LimboError::Corrupt("Invalid 8-byte int".into())));
                }
                self.set_data_section(&data[8..]);
                dest.set_int(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]));
            }
            // F64
            7 => {
                if unlikely(data.len() < 8) {
                    return Some(Err(LimboError::Corrupt("Invalid 8-byte float".into())));
                }
                self.set_data_section(&data[8..]);
                let val = f64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                if let Some(nn) = NonNan::new(val) {
                    dest.set_float(nn);
                } else {
                    dest.set_null();
                }
            }
            // CONST_INT0
            8 => {
                self.set_data_section(data);
                dest.set_int(0);
            }
            // CONST_INT1
            9 => {
                self.set_data_section(data);
                dest.set_int(1);
            }
            // Reserved
            10 | 11 => {
                mark_unlikely();
                return Some(Err(LimboError::Corrupt(format!(
                    "Reserved serial type: {serial_type}"
                ))));
            }
            // BLOB (n >= 12 && n & 1 == 0)
            n if n >= 12 && n & 1 == 0 => crate::with_value_blob_allocation_site!(RecordDecode, {
                let content_size = ((n - 12) / 2) as usize;
                if unlikely(data.len() < content_size) {
                    return Some(Err(LimboError::Corrupt("Invalid Blob value".into())));
                }
                self.set_data_section(&data[content_size..]);
                let blob_data = &data[..content_size];
                match dest {
                    Register::Value(Value::Blob(existing_blob)) => {
                        if let Err(err) = existing_blob.do_extend(&blob_data) {
                            return Some(Err(err));
                        }
                    }
                    _ => {
                        let blob = match crate::types::value_blob_from_slice(blob_data) {
                            Ok(blob) => blob,
                            Err(err) => return Some(Err(err.into())),
                        };
                        if let Err(err) = dest.set_blob(blob) {
                            return Some(Err(err));
                        }
                    }
                }
            }),
            // TEXT (n >= 13 && n & 1 == 1)
            n if n >= 13 && n & 1 == 1 => {
                let content_size = ((n - 13) / 2) as usize;
                if unlikely(data.len() < content_size) {
                    return Some(Err(LimboError::Corrupt("Invalid Text value".into())));
                }
                self.set_data_section(&data[content_size..]);
                let text_data = &data[..content_size];
                let Some(text_str) = validate_utf8(text_data) else {
                    mark_unlikely();
                    return Some(Err(LimboError::Corrupt(
                        "TEXT value contains invalid UTF-8".into(),
                    )));
                };
                match dest {
                    Register::Value(Value::Text(existing_text)) => {
                        if let Err(err) = existing_text.do_extend(&text_str) {
                            return Some(Err(err));
                        }
                    }
                    _ => {
                        if let Err(err) = dest.set_text(Text::new(text_str.to_string())) {
                            return Some(Err(err));
                        }
                    }
                }
            }
            _ => {
                mark_unlikely();
                return Some(Err(LimboError::Corrupt(format!(
                    "Invalid serial type: {serial_type}"
                ))));
            }
        }

        Some(Ok(()))
    }

    #[inline]
    fn decode_into_registers_after(
        &mut self,
        skip: usize,
        dests: &mut [Register],
    ) -> Result<usize> {
        for (i, dest) in dests.iter_mut().enumerate() {
            let n = if i == 0 { skip } else { 0 };
            match self.nth_into_register(n, dest) {
                Some(Ok(())) => {}
                Some(Err(e)) => return Err(e),
                None => return Ok(i),
            }
        }
        Ok(dests.len())
    }
}

/// UTF-8 validation tuned for record decoding. TEXT values are usually short
/// ASCII read at arbitrary offsets inside a b-tree page: simdutf8 only uses
/// SIMD from 64 bytes up, and core's `from_utf8` word-at-a-time path is
/// alignment-sensitive, so both are slow here. OR-ing every byte together is
/// alignment-independent and branch-light; if no byte had the high bit set
/// the value is pure ASCII and needs no further validation. Non-ASCII and
/// values longer than the cutoff fall back to full simdutf8 validation —
/// above the cutoff the scalar OR loop loses to real SIMD.
///
/// Measured by `core/benches/text_validate_benchmark.rs` (varying slice
/// alignment, ASCII content) on an Apple M2, macOS 15.7, vs
/// `simdutf8::basic::from_utf8` alone:
///
///   1-128 B:  1.4-4x faster (peak 4.1x at 16 B)
///   256-512 B: 1.1-1.2x faster
///   1-2 KB:   parity
///   4 KB:     ~25% slower without the cutoff; equal with it
///   multibyte fallback: pays the wasted OR scan (~15% at 64 B)
///   length branch: ~+0.1ns/call, visible only on 1-2 B values
#[inline]
fn validate_utf8(data: &[u8]) -> Option<&str> {
    const ASCII_SCAN_CUTOFF: usize = 512;
    if data.len() <= ASCII_SCAN_CUTOFF && is_ascii(data) {
        // SAFETY: all bytes are ASCII, which is valid UTF-8.
        return Some(unsafe { core::str::from_utf8_unchecked(data) });
    }
    simdutf8::basic::from_utf8(data).ok()
}

/// ORs the bytes together a word at a time: eight, then four, two and one
/// for the rest, so a value of any length takes at most `len / 8 + 3`
/// loads. The loads are unaligned, so the slice's position on the page
/// does not matter.
#[inline]
fn is_ascii(data: &[u8]) -> bool {
    let mut acc = 0u64;
    let mut rest = data;
    while let Some((word, tail)) = rest.split_first_chunk::<8>() {
        acc |= u64::from_ne_bytes(*word);
        rest = tail;
    }
    if let Some((word, tail)) = rest.split_first_chunk::<4>() {
        acc |= u64::from(u32::from_ne_bytes(*word));
        rest = tail;
    }
    if let Some((word, tail)) = rest.split_first_chunk::<2>() {
        acc |= u64::from(u16::from_ne_bytes(*word));
        rest = tail;
    }
    if let Some(&byte) = rest.first() {
        acc |= u64::from(byte);
    }
    acc & 0x8080_8080_8080_8080 == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn active_opcode_helpers_initialize_defaults() {
        let mut state = ProgramState::new(1, 0);

        assert!(matches!(state.active_op_state.state, ActiveOpState::None));
        assert!(matches!(
            state.active_op_state.column(),
            OpColumnState::Start
        ));
        state.active_op_state.clear();
        assert!(state.active_op_state.parse_schema().is_none());
    }

    #[test]
    fn is_ascii_checks_every_byte_of_every_length() {
        for len in 0..40 {
            let ascii: Vec<u8> = (0..len).map(|i| b'a' + (i % 26) as u8).collect();
            assert!(is_ascii(&ascii), "length {len}");
            assert_eq!(validate_utf8(&ascii), std::str::from_utf8(&ascii).ok());
            for position in 0..len {
                let mut bytes = ascii.clone();
                bytes[position] = 0xc3;
                assert!(!is_ascii(&bytes), "length {len}, byte {position}");
                assert_eq!(validate_utf8(&bytes), std::str::from_utf8(&bytes).ok());
            }
        }
        let text = "héllo wörld, ünïcödé";
        assert!(!is_ascii(text.as_bytes()));
        assert_eq!(validate_utf8(text.as_bytes()), Some(text));
    }

    #[test]
    fn nth_into_register_rejects_invalid_utf8_text() {
        let payload = [2, 15, 0xff];
        let mut iterator = crate::types::ValueIterator::new(&payload).unwrap();
        let mut destination = Register::Value(Value::Null);

        let result = iterator
            .nth_into_register(0, &mut destination)
            .expect("record contains one value");

        assert!(
            matches!(
                result,
                Err(LimboError::Corrupt(ref message))
                    if message == "TEXT value contains invalid UTF-8"
            ),
            "unexpected result: {result:?}"
        );
    }

    #[test]
    fn active_opcode_helpers_reject_mismatched_resumes() {
        let mut state = ProgramState::new(1, 0);
        *state.active_op_state.column() = OpColumnState::GetColumn;

        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _ = state.active_op_state.parse_schema();
        }));
        assert!(panic.is_err(), "mismatched opcode resume should panic");
    }

    #[test]
    fn seek_state_is_independent_from_active_opcode_slot() {
        let mut state = ProgramState::new(1, 0);

        *state.active_op_state.insert() = OpInsertState {
            sub_state: OpInsertSubState::Seek,
            old_record: None,
            is_noop_update: false,
        };
        state.seek_state = OpSeekState::MoveLast;

        assert!(matches!(
            state.active_op_state.insert().sub_state,
            OpInsertSubState::Seek
        ));
        assert!(matches!(state.seek_state, OpSeekState::MoveLast));
    }

    #[test]
    fn register_try_clone_copies_each_variant() {
        let record_values = [Value::from_i64(1), Value::build_text("record payload")];
        let aggregate_values = crate::alloc::vec![Value::build_text("aggregate payload")];
        let registers = [
            Register::Value(Value::build_text("value")),
            Register::Aggregate(AggContext::Builtin(aggregate_values)),
            Register::Record(
                ImmutableRecord::from_values(&record_values, record_values.len()).unwrap(),
            ),
        ];

        for source in registers {
            assert_eq!(source.try_clone().unwrap(), source);
        }
    }

    #[test]
    fn register_try_clone_from_reuses_matching_allocations() {
        use crate::types::Text;

        let src = Register::Value(Value::Text(Text::new(String::from("short"))));
        let mut dst = Register::Value(Value::Text(Text::new(String::from(
            "a destination string with plenty of capacity",
        ))));
        let ptr = match &dst {
            Register::Value(Value::Text(t)) => t.as_str().as_ptr(),
            _ => unreachable!(),
        };
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
        match &dst {
            Register::Value(Value::Text(t)) => assert_eq!(t.as_str().as_ptr(), ptr),
            _ => unreachable!(),
        }

        let src_values = [Value::from_i64(1), Value::build_text("record payload")];
        let src =
            Register::Record(ImmutableRecord::from_values(&src_values, src_values.len()).unwrap());
        let large_values = [Value::build_text(
            "a much longer record payload that dwarfs the source record",
        )];
        let mut dst = Register::Record(
            ImmutableRecord::from_values(&large_values, large_values.len()).unwrap(),
        );
        let ptr = match &dst {
            Register::Record(record) => record.get_payload().as_ptr(),
            _ => unreachable!(),
        };
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
        match &dst {
            Register::Record(record) => assert_eq!(record.get_payload().as_ptr(), ptr),
            _ => unreachable!(),
        }

        let src = Register::Aggregate(AggContext::Builtin(crate::alloc::vec![
            Value::build_text("agg state"),
            Value::from_i64(2),
        ]));
        let mut dst = Register::Value(Value::Null);
        dst.try_clone_from(&src).unwrap();
        assert_eq!(dst, src);
    }

    #[test]
    fn register_take_buf_recycles_record_allocations() {
        let values = [Value::build_text("some record payload")];
        let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
        let capacity = record.as_blob().capacity();
        let ptr = record.get_payload().as_ptr();

        let mut register = Register::Record(record);
        let rebuilt = ImmutableRecord::build(&values, register.take_buf()).unwrap();
        assert!(register.is_null());
        assert_eq!(rebuilt.as_blob().capacity(), capacity);
        assert_eq!(rebuilt.get_payload().as_ptr(), ptr);
    }

    #[test]
    fn register_try_clone_value_from_reuses_value_slot() {
        let value = Value::build_text(String::from("payload"));
        let mut register = Register::Value(Value::build_text(String::from(
            "existing buffer with plenty of capacity to reuse",
        )));
        let ptr = match &register {
            Register::Value(Value::Text(text)) => text.as_str().as_ptr(),
            _ => unreachable!(),
        };

        register.try_clone_value_from(&value).unwrap();
        assert_eq!(register, Register::Value(value));
        match &register {
            Register::Value(Value::Text(text)) => assert_eq!(text.as_str().as_ptr(), ptr),
            _ => unreachable!(),
        }
    }
}

/// Shuttle tests for validating the `unsafe impl Send + Sync for ProgramState` safety claims.
///
/// The safety claims are:
/// 1. `Row` contains a `*const Register` pointing into `ProgramState.registers`
/// 2. Only immutable references (`&Row`) are given out via `result_row.as_ref()`
/// 3. `result_row` is invalidated (via `.take()`) at the start of each step iteration
///
/// These tests verify that the implementation correctly upholds these invariants
/// under concurrent access patterns.

#[cfg(all(shuttle, test))]
mod shuttle_tests {
    use super::*;
    use crate::sync::Arc;
    use crate::thread;
    use crate::types::Value;

    /// Creates a minimal ProgramState for testing.
    fn create_test_state(num_registers: usize, num_cursors: usize) -> ProgramState {
        ProgramState::new(num_registers, num_cursors)
    }

    /// Test that ProgramState can be safely sent between threads.
    /// This validates the `unsafe impl Send for ProgramState` claim.
    #[test]
    fn shuttle_program_state_send() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                // Write some data to registers
                state.registers[0].set_int(42);
                state.registers[1].set_text(Text::new("test".to_string()));

                // Send state to another thread
                let handle = thread::spawn(move || {
                    // Verify data is intact after send
                    assert!(matches!(
                        &state.registers[0],
                        Register::Value(Value::Numeric(Numeric::Integer(42)))
                    ));
                    if let Register::Value(Value::Text(t)) = &state.registers[1] {
                        assert_eq!(t.as_str(), "test");
                    } else {
                        panic!("Expected text value");
                    }

                    // Modify in new thread
                    state.registers[2].set_int(100);
                    state
                });

                let state = handle.join().unwrap();
                assert!(matches!(
                    &state.registers[2],
                    Register::Value(Value::Numeric(Numeric::Integer(100)))
                ));
            },
            1000,
        );
    }

    /// Test that ProgramState with a set result_row can be safely sent.
    /// The Row contains a raw pointer that must remain valid after the send.
    #[test]
    fn shuttle_program_state_send_with_row() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                // Set up registers with test data
                state.registers[0].set_int(1);
                state.registers[1].set_int(2);
                state.registers[2].set_int(3);

                // Create a result_row pointing to registers
                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 3,
                });

                // Send to another thread - the pointer must remain valid
                // because it points to memory owned by state (the registers Vec)
                let handle = thread::spawn(move || {
                    // The row pointer should still be valid because registers moved with state
                    if let Some(row) = &state.result_row {
                        assert_eq!(row.len(), 3);
                        // Read through the pointer - this validates the pointer is still valid
                        let val = row.get::<i64>(0).unwrap();
                        assert_eq!(val, 1);
                        let val = row.get::<i64>(1).unwrap();
                        assert_eq!(val, 2);
                        let val = row.get::<i64>(2).unwrap();
                        assert_eq!(val, 3);
                    } else {
                        panic!("Expected result_row to be set");
                    }
                    state
                });

                let _ = handle.join().unwrap();
            },
            1000,
        );
    }

    /// Test concurrent reads of result_row through shared reference.
    /// This validates the `unsafe impl Sync for ProgramState` claim for read access.
    #[test]
    fn shuttle_program_state_sync_concurrent_reads() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                // Set up registers
                state.registers[0].set_int(42);
                state.registers[1].set_int(43);

                // Create result_row
                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 2,
                });

                let state = Arc::new(state);
                let state2 = Arc::clone(&state);
                let state3 = Arc::clone(&state);

                // Multiple threads reading concurrently
                let h1 = thread::spawn(move || {
                    if let Some(row) = &state.result_row {
                        let val = row.get::<i64>(0).unwrap();
                        assert_eq!(val, 42);
                    }
                });

                let h2 = thread::spawn(move || {
                    if let Some(row) = &state2.result_row {
                        let val = row.get::<i64>(1).unwrap();
                        assert_eq!(val, 43);
                    }
                });

                let h3 = thread::spawn(move || {
                    if let Some(row) = &state3.result_row {
                        assert_eq!(row.len(), 2);
                    }
                });

                h1.join().unwrap();
                h2.join().unwrap();
                h3.join().unwrap();
            },
            1000,
        );
    }

    /// Test that Row values read through the pointer are consistent.
    /// Multiple threads reading the same row values should see the same data.
    #[test]
    fn shuttle_row_pointer_consistency() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                // Set up registers with distinct values
                for i in 0..5 {
                    state.registers[i].set_int(i as i64 * 10);
                }

                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 5,
                });

                let state = Arc::new(state);
                let mut handles = vec![];

                for _ in 0..4 {
                    let state_clone = Arc::clone(&state);
                    let h = thread::spawn(move || {
                        if let Some(row) = &state_clone.result_row {
                            // All threads should see the same values
                            for i in 0..5 {
                                let val = row.get::<i64>(i).unwrap();
                                assert_eq!(val, i as i64 * 10);
                            }
                        }
                    });
                    handles.push(h);
                }

                for h in handles {
                    h.join().unwrap();
                }
            },
            1000,
        );
    }

    /// Test the result_row invalidation pattern.
    /// When result_row is taken (invalidated), concurrent reads should not see stale data.
    /// This simulates the pattern used in `normal_step()` where `result_row.take()` is called.
    #[test]
    fn shuttle_result_row_invalidation() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                state.registers[0].set_int(100);
                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 1,
                });

                // Simulate the invalidation pattern from normal_step
                // In real code, this requires &mut self, so there's no concurrent access
                let taken_row = state.result_row.take();

                // After take(), result_row should be None
                assert!(state.result_row.is_none());

                // The taken row still holds valid data (until dropped)
                if let Some(row) = taken_row {
                    let val = row.get::<i64>(0).unwrap();
                    assert_eq!(val, 100);
                }
            },
            1000,
        );
    }

    /// Test register modification after row invalidation.
    /// This validates that modifying registers after take() is safe.
    #[test]
    fn shuttle_register_modification_after_invalidation() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                state.registers[0].set_int(1);
                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 1,
                });

                // Invalidate row (simulating what normal_step does)
                let _ = state.result_row.take();

                // Now safe to modify registers
                state.registers[0].set_int(999);

                // Create new row pointing to modified registers
                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 1,
                });

                // New row should see new value
                if let Some(row) = &state.result_row {
                    let val = row.get::<i64>(0).unwrap();
                    assert_eq!(val, 999);
                }
            },
            1000,
        );
    }

    /// Test sequential send-receive pattern (simulating async task scheduling).
    /// ProgramState is moved between threads in a producer-consumer pattern.
    #[test]
    fn shuttle_sequential_thread_transfer() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);
                state.registers[0].set_int(0);

                // Thread 1: increment
                let h1 = thread::spawn(move || {
                    if let Register::Value(Value::Numeric(Numeric::Integer(v))) =
                        &state.registers[0]
                    {
                        state.registers[0].set_int(v + 1);
                    }
                    state
                });

                let mut state = h1.join().unwrap();

                // Thread 2: increment
                let h2 = thread::spawn(move || {
                    if let Register::Value(Value::Numeric(Numeric::Integer(v))) =
                        &state.registers[0]
                    {
                        state.registers[0].set_int(v + 1);
                    }
                    state
                });

                let mut state = h2.join().unwrap();

                // Thread 3: increment
                let h3 = thread::spawn(move || {
                    if let Register::Value(Value::Numeric(Numeric::Integer(v))) =
                        &state.registers[0]
                    {
                        state.registers[0].set_int(v + 1);
                    }
                    state
                });

                let state = h3.join().unwrap();

                // Final value should be 3
                assert!(matches!(
                    &state.registers[0],
                    Register::Value(Value::Numeric(Numeric::Integer(3)))
                ));
            },
            1000,
        );
    }

    /// Test that ProgramState can be wrapped in Arc for shared ownership.
    /// This is the typical pattern for concurrent database operations.
    #[test]
    fn shuttle_arc_wrapped_state() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                // Initialize with test data
                for i in 0..5 {
                    state.registers[i].set_int(i as i64);
                }

                let state = Arc::new(state);
                let mut handles = vec![];

                // Multiple threads reading registers through Arc
                for thread_id in 0u8..4 {
                    let state_clone = Arc::clone(&state);
                    let h = thread::spawn(move || {
                        // Each thread reads all registers
                        for i in 0..5 {
                            if let Register::Value(Value::Numeric(Numeric::Integer(v))) =
                                &state_clone.registers[i]
                            {
                                assert_eq!(*v, i as i64);
                            }
                        }
                        thread_id
                    });
                    handles.push(h);
                }

                for h in handles {
                    h.join().unwrap();
                }
            },
            1000,
        );
    }

    /// Test Row::get_values iterator under concurrent access.
    #[test]
    fn shuttle_row_get_values_concurrent() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(10, 2);

                state.registers[0].set_int(10);
                state.registers[1].set_int(20);
                state.registers[2].set_int(30);

                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 3,
                });

                let state = Arc::new(state);
                let state2 = Arc::clone(&state);

                let h1 = thread::spawn(move || {
                    if let Some(row) = &state.result_row {
                        let values: Vec<_> = row.get_values().collect();
                        assert_eq!(values.len(), 3);
                    }
                });

                let h2 = thread::spawn(move || {
                    if let Some(row) = &state2.result_row {
                        let mut sum = 0i64;
                        for val in row.get_values() {
                            if let Value::Numeric(Numeric::Integer(i)) = val {
                                sum += i;
                            }
                        }
                        assert_eq!(sum, 60); // 10 + 20 + 30
                    }
                });

                h1.join().unwrap();
                h2.join().unwrap();
            },
            1000,
        );
    }

    /// Stress test: Many threads reading from shared ProgramState.
    #[test]
    fn shuttle_stress_concurrent_reads() {
        shuttle::check_random(
            || {
                let mut state = create_test_state(20, 2);

                // Fill registers with identifiable data
                for i in 0..20 {
                    state.registers[i].set_int(i as i64 * 100);
                }

                state.result_row = Some(Row {
                    values: &state.registers[0] as *const Register,
                    count: 20,
                });

                let state = Arc::new(state);
                let mut handles = vec![];

                for thread_id in 0..6u8 {
                    let state_clone = Arc::clone(&state);
                    let h = thread::spawn(move || {
                        // Each thread reads different parts
                        let start = (thread_id as usize * 3) % 20;
                        if let Some(row) = &state_clone.result_row {
                            for i in 0..3 {
                                let idx = (start + i) % row.len();
                                let val = row.get::<i64>(idx).unwrap();
                                assert_eq!(val, idx as i64 * 100);
                            }
                        }
                        thread_id
                    });
                    handles.push(h);
                }

                for h in handles {
                    h.join().unwrap();
                }
            },
            1000,
        );
    }
}
