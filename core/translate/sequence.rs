// Sequence bytecode translation.
//
// See the persistence design comment on `Sequence` in core/schema.rs.
// Key invariant: every nextval() must INSERT a row into the backing table
// (`__turso_internal_seq_<name>`). This makes MVCC allocations conflict-free.
//
// Compaction and `sqlite_sequence` synchronization are emitted INLINE
// here at translate time — they are NOT performed at commit time by a
// vdbe handler issuing nested `prepare_internal()` calls, which would
// drive `pager.io.step()` synchronously and break the vdbe async
// contract (the outer `Statement::step()` must return
// `StepResult::IO` rather than blocking).

use std::sync::Arc;

use crate::bail_parse_error;
use crate::schema::{
    BTreeTable, Sequence, AUTOINCREMENT_SEQ_PREFIX, SEQ_BACKING_TABLE_PREFIX,
    SQLITE_SEQUENCE_TABLE_NAME,
};
use crate::storage::pager::CreateBTreeFlags;
use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::util::{escape_sql_string_literal, normalize_ident};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::{
    to_u32, AddSequenceData, CmpInsFlags, Cookie, InsertFlags, Insn, RegisterOrLiteral,
};
use crate::Result;
use turso_parser::ast;

pub fn sequence_backing_table_name(seq_name: &str) -> String {
    String::from(SEQ_BACKING_TABLE_PREFIX) + seq_name
}

pub fn sequence_backing_table_sql(seq_name: &str) -> String {
    let table_name = sequence_backing_table_name(seq_name);
    // INTEGER PRIMARY KEY makes `value` the rowid alias: nextval inserts are
    // keyed by the sequence value itself (no synthetic rowid) and the inline
    // compaction emitted by `emit_backing_table_compaction` collapses each
    // backing table down to a single watermark row via DELETE-all + INSERT.
    format!(
        "CREATE TABLE \"{table_name}\"(\
         value INTEGER PRIMARY KEY,\
         is_called INTEGER,\
         start INTEGER,\
         inc INTEGER,\
         min INTEGER,\
         max INTEGER,\
         cycle INTEGER)"
    )
}

/// Emit bytecode to create a sequence backing table (B-tree + sqlite_schema entry +
/// initial row + ParseSchema). Used by both CREATE SEQUENCE and CREATE TABLE with
/// AUTOINCREMENT. The sqlite_schema cursor must already be open for writing.
#[allow(clippy::too_many_arguments)]
pub fn emit_sequence_backing_table(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    sqlite_schema_cursor_id: usize,
    seq_name: &str,
    start: i64,
    increment: i64,
    min_value: i64,
    max_value: i64,
    cycle: bool,
) -> Result<()> {
    let backing_table_name = sequence_backing_table_name(seq_name);
    let sql = sequence_backing_table_sql(seq_name);

    let table_root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: database_id,
        root: table_root_reg,
        flags: CreateBTreeFlags::new_table(),
    });

    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        None,
        SchemaEntryType::Table,
        &backing_table_name,
        &backing_table_name,
        table_root_reg,
        Some(sql.clone()),
    )?;

    // Open the newly-created table by its root page register and insert the initial row.
    let seq_btree = Arc::new(BTreeTable::from_sql(&sql, 0)?);
    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_btree));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: RegisterOrLiteral::Register(table_root_reg),
        db: database_id,
    });

    let base_reg = program.alloc_registers(7);
    program.emit_insn(Insn::Integer {
        dest: base_reg,
        value: start,
    });
    program.emit_insn(Insn::Integer {
        dest: base_reg + 1,
        value: 0, // is_called = false
    });
    program.emit_insn(Insn::Integer {
        dest: base_reg + 2,
        value: start,
    });
    program.emit_insn(Insn::Integer {
        dest: base_reg + 3,
        value: increment,
    });
    program.emit_insn(Insn::Integer {
        dest: base_reg + 4,
        value: min_value,
    });
    program.emit_insn(Insn::Integer {
        dest: base_reg + 5,
        value: max_value,
    });
    program.emit_insn(Insn::Integer {
        dest: base_reg + 6,
        value: if cycle { 1 } else { 0 },
    });

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(base_reg),
        count: 7,
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });

    // value is INTEGER PRIMARY KEY (= rowid alias), so use start value as key.
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: base_reg, // base_reg holds the start value
        record_reg,
        flag: InsertFlags::new().require_seek().skip_all_change_counts(),
        table_name: seq_name.to_string(),
    });

    program.emit_insn(Insn::Close {
        cursor_id: seq_cursor_id,
    });

    // ParseSchema to add the backing table to Schema.tables
    let escaped = escape_sql_string_literal(&backing_table_name);
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(format!("name = '{escaped}'")),
        trigger_target_database_id: None,
    });

    Ok(())
}

/// Emit bytecode that reads the current watermark of a sequence's backing
/// table from disk, computes the next value, INSERTs a new watermark row,
/// and leaves the next value in `target_register`.
///
/// This is the disk-only implementation that replaces in-memory atomic CAS.
/// Used by both `translate_sequence_function` (user nextval) and the
/// AUTOINCREMENT MVCC rowid-generation path. The caller must arrange any
/// surrounding transaction setup (`begin_write_on_database`).
///
/// The seq descriptor's `start/inc/min/max/cycle` are baked into the
/// bytecode as `Insn::Integer` literals — they are immutable schema and
/// reading them back from the row would be wasteful.
pub fn emit_disk_read_nextval(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    seq_name: &str,
    seq: &Sequence,
    target_register: usize,
    // Optional caller-supplied register already holding the seq name as
    // text (e.g. populated by user-arg evaluation in the SQL translator).
    // When `None` the helper allocates its own.
    seq_name_reg: Option<usize>,
) -> Result<()> {
    let backing_table_name = sequence_backing_table_name(seq_name);
    let backing_table = resolver
        .with_schema(database_id, |s| s.get_btree_table(&backing_table_name))
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing backing table for sequence \"{seq_name}\""
            ))
        })?;
    let root_page = backing_table.root_page;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(backing_table));

    let seq_name_reg =
        seq_name_reg.unwrap_or_else(|| program.emit_string8_new_reg(seq_name.to_string()));

    // Inner-tx wrap state, shared by Begin/Commit. See
    // Insn::SequenceBeginInnerTx doc for the path-kind / saved-outer
    // semantics. Conflict-driven retries loop back to retry_top_label
    // unconditionally; the retry budget lives inside
    // `op_sequence_commit_inner_tx` (via `ProgramState::
    // sequence_inner_retry_count`) so a degenerate conflict pattern
    // surfaces as `LimboError::Busy` rather than a misclassified halt.
    let path_kind_reg = program.alloc_register();
    let saved_outer_reg = program.alloc_register();
    let status_reg = program.alloc_register();

    let retry_top_label = program.allocate_label();
    program.preassign_label_to_next_insn(retry_top_label);

    program.emit_insn(Insn::SequenceBeginInnerTx {
        db: database_id,
        path_kind_reg,
        saved_outer_reg,
    });

    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(root_page),
        db: database_id,
    });

    let col_base = program.alloc_registers(7);
    let was_empty_reg = program.alloc_register();
    let have_row_label = program.allocate_label();
    let empty_label = program.allocate_label();

    program.emit_insn(Insn::Integer {
        dest: was_empty_reg,
        value: 0,
    });
    if seq.increment_by >= 0 {
        program.emit_insn(Insn::Last {
            cursor_id,
            pc_if_empty: empty_label,
        });
    } else {
        program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: empty_label,
        });
    }
    program.emit_column_or_rowid(cursor_id, 0, col_base);
    program.emit_column_or_rowid(cursor_id, 1, col_base + 1);
    program.emit_insn(Insn::Goto {
        target_pc: have_row_label,
    });
    program.preassign_label_to_next_insn(empty_label);
    program.emit_insn(Insn::Integer {
        dest: was_empty_reg,
        value: 1,
    });
    program.preassign_label_to_next_insn(have_row_label);

    program.emit_insn(Insn::SequenceComputeNext {
        db: database_id,
        seq_name_reg,
        in_value_reg: col_base,
        in_is_called_reg: col_base + 1,
        was_empty_reg,
        out_value_reg: target_register,
    });

    program.emit_insn(Insn::Copy {
        src_reg: target_register,
        dst_reg: col_base,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Integer {
        dest: col_base + 1,
        value: 1,
    });
    emit_sequence_descriptor_literals(program, seq, col_base + 2);

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(col_base),
        count: 7,
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: target_register,
        record_reg,
        flag: InsertFlags::new().require_seek().skip_all_change_counts(),
        table_name: seq_name.to_string(),
    });

    // Inline backing-table compaction has two distinct cases:
    //
    //   * CYCLE: load-bearing for wrap-correctness. After a wrap the
    //     new value re-enters a range whose rows the backing table
    //     still holds, so Last/Rewind can pick up the *old* row
    //     rather than the wrapped one. Compaction must run on every
    //     nextval regardless of path. The contention this creates
    //     under MVCC concurrent is fundamental to CYCLE semantics
    //     (every wrap is a logical "advance the shared pointer" that
    //     PG also serializes); the inner-tx retry budget absorbs it.
    //
    //   * Non-CYCLE: bookkeeping only. Every nextval produces a *new*
    //     value, and `value INTEGER PRIMARY KEY` guarantees the row
    //     lands on a distinct B-tree key. The watermark is
    //     recoverable from Last/Rewind regardless of how many
    //     historical rows sit in the table. We therefore gate the
    //     compaction loop on `path_kind_reg`:
    //
    //       - SKIPPED path (0) — WAL single-writer OR MVCC exclusive
    //         outer tx. No concurrent allocator can race the prior-
    //         watermark row, so the original WW-on-shared-row
    //         argument doesn't apply. Compact inline so the backing
    //         table stays at one row per sequence instead of
    //         accumulating one row per nextval forever. Especially
    //         important in WAL/rollback mode, where there is no
    //         checkpoint-time `SeqCompactDriver` and a long-running
    //         workload would otherwise bloat the DB without bound.
    //
    //       - WRAPPED path (1) — MVCC autocommit or BEGIN CONCURRENT.
    //         Skip compaction; deleting the shared prior-watermark
    //         row would produce avoidable WW conflicts on the hot
    //         path. The MVCC checkpoint's `SeqCompactDriver`
    //         reclaims the rows asynchronously.
    //
    // The "MVCC concurrent stays conflict-free" half of this contract
    // is enforced by `test_nextval_no_inner_tx_retry_on_concurrent_mvcc`.
    if seq.cycle {
        emit_backing_table_compaction(program, cursor_id, seq_name, target_register);
    } else {
        let skip_compact_label = program.allocate_label();
        // If path_kind_reg is truthy (== SEQ_PATH_WRAPPED), jump past
        // the compaction loop; fall through and compact otherwise.
        program.emit_insn(Insn::If {
            reg: path_kind_reg,
            target_pc: skip_compact_label,
            jump_if_null: false,
        });
        emit_backing_table_compaction(program, cursor_id, seq_name, target_register);
        program.preassign_label_to_next_insn(skip_compact_label);
    }

    program.emit_insn(Insn::Close { cursor_id });

    // For AUTOINCREMENT-backing sequences, mirror the watermark into
    // `sqlite_sequence` in the same autonomous inner tx so SQLite-
    // compatibility readers see the current high-water mark without
    // needing a checkpoint. Under MVCC this serializes concurrent
    // AUTOINCREMENT inserts to the same table through the
    // `sqlite_sequence` row; the existing inner-tx retry budget in
    // `op_sequence_commit_inner_tx` absorbs the WW-conflicts.
    emit_autoincrement_sqlite_sequence_sync(
        program,
        resolver,
        database_id,
        seq_name,
        target_register,
    )?;

    // Register the active allocation against the outer tx *before* the inner
    // tx commits and makes this value observable to other connections. This
    // closes the race where another allocator could advance the watermark past
    // a value whose row the outer tx still holds uncommitted. See
    // `op_sequence_register_allocation`.
    program.emit_insn(Insn::SequenceRegisterAllocation {
        db: database_id,
        seq_name_reg,
        value_reg: target_register,
        saved_outer_reg,
    });

    program.emit_insn(Insn::SequenceCommitInnerTx {
        db: database_id,
        path_kind_reg,
        saved_outer_reg,
        status_reg,
    });

    // status_reg: 0 = Ok, 1 = ConflictRetry. If conflict, loop back to
    // retry_top unconditionally — `op_sequence_commit_inner_tx` returns
    // `LimboError::Busy` directly when its internal retry counter is
    // exhausted, so the translator no longer needs a per-program
    // counter register or a trailing Halt.
    let after_retry_label = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: status_reg,
        target_pc: after_retry_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: retry_top_label,
    });
    program.preassign_label_to_next_insn(after_retry_label);

    program.emit_insn(Insn::SequenceTrackAllocation {
        db: database_id,
        seq_name_reg,
        value_reg: target_register,
    });

    program.emit_insn(Insn::SetSequenceCurrval {
        seq_name_reg,
        value_reg: target_register,
    });

    Ok(())
}

/// Emit bytecode that ensures the sequence's disk watermark is at least
/// `value_reg`. If the current MAX is below the value, INSERTs a new
/// watermark row at the value; otherwise no-op. Used for AUTOINCREMENT
/// when the user supplies an explicit rowid that exceeds the running
/// watermark — mirrors `Sequence::advance_past` but on disk.
pub fn emit_disk_advance_past(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    seq_name: &str,
    seq: &Sequence,
    value_reg: usize,
) -> Result<()> {
    let backing_table_name = sequence_backing_table_name(seq_name);
    let backing_table = resolver
        .with_schema(database_id, |s| s.get_btree_table(&backing_table_name))
        .ok_or_else(|| {
            crate::LimboError::InternalError(format!(
                "missing backing table for sequence \"{seq_name}\""
            ))
        })?;
    let root_page = backing_table.root_page;
    let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(backing_table));

    // Retry budget for conflict-driven loops lives in
    // `op_sequence_commit_inner_tx` (see `ProgramState::
    // sequence_inner_retry_count`) so a Busy bail surfaces as
    // `LimboError::Busy` rather than a misclassified halt.
    let path_kind_reg = program.alloc_register();
    let saved_outer_reg = program.alloc_register();
    let status_reg = program.alloc_register();

    let retry_top_label = program.allocate_label();
    program.preassign_label_to_next_insn(retry_top_label);

    program.emit_insn(Insn::SequenceBeginInnerTx {
        db: database_id,
        path_kind_reg,
        saved_outer_reg,
    });

    program.emit_insn(Insn::OpenWrite {
        cursor_id,
        root_page: RegisterOrLiteral::Literal(root_page),
        db: database_id,
    });

    let done_seek_label = program.allocate_label();
    let do_advance_label = program.allocate_label();
    let col_value_reg = program.alloc_register();

    // Direction-aware seek to the current watermark row. If the backing
    // table is empty, we always advance.
    if seq.increment_by >= 0 {
        program.emit_insn(Insn::Last {
            cursor_id,
            pc_if_empty: do_advance_label,
        });
    } else {
        program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: do_advance_label,
        });
    }
    program.emit_column_or_rowid(cursor_id, 0, col_value_reg);

    // For ascending sequences advance only if value > current; for
    // descending advance only if value < current.
    if seq.increment_by >= 0 {
        program.emit_insn(Insn::Le {
            lhs: value_reg,
            rhs: col_value_reg,
            target_pc: done_seek_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
    } else {
        program.emit_insn(Insn::Ge {
            lhs: value_reg,
            rhs: col_value_reg,
            target_pc: done_seek_label,
            flags: CmpInsFlags::default(),
            collation: program.curr_collation(),
        });
    }

    program.preassign_label_to_next_insn(do_advance_label);

    let col_base = program.alloc_registers(7);
    program.emit_insn(Insn::Copy {
        src_reg: value_reg,
        dst_reg: col_base,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Integer {
        dest: col_base + 1,
        value: 1,
    });
    emit_sequence_descriptor_literals(program, seq, col_base + 2);

    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(col_base),
        count: 7,
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: value_reg,
        record_reg,
        flag: InsertFlags::new().require_seek().skip_all_change_counts(),
        table_name: seq_name.to_string(),
    });
    let seq_name_reg = program.emit_string8_new_reg(seq_name.to_string());
    program.emit_insn(Insn::SetSequenceCurrval {
        seq_name_reg,
        value_reg,
    });
    // Inline compaction is gated the same way as the nextval path —
    // see the long comment in `emit_disk_read_nextval` for the
    // SKIPPED-vs-WRAPPED contract. `advance_past` is only emitted for
    // AUTOINCREMENT (its call sites are gated on `mv_store_for_db`),
    // and AUTOINCREMENT sequences are never CYCLE, so the CYCLE arm
    // here is unreachable today. The non-CYCLE arm fires in the
    // narrow case of an explicit-rowid INSERT inside `BEGIN ... COMMIT`
    // (MVCC exclusive → SKIPPED path) and is otherwise skipped on the
    // WRAPPED path. Keeping symmetry with the nextval gate avoids a
    // bookkeeping divergence between the two emit paths.
    if seq.cycle {
        emit_backing_table_compaction(program, cursor_id, seq_name, value_reg);
    } else {
        let skip_compact_label = program.allocate_label();
        program.emit_insn(Insn::If {
            reg: path_kind_reg,
            target_pc: skip_compact_label,
            jump_if_null: false,
        });
        emit_backing_table_compaction(program, cursor_id, seq_name, value_reg);
        program.preassign_label_to_next_insn(skip_compact_label);
    }

    // Mirror the watermark into `sqlite_sequence` ONLY when the
    // advance branch actually fired. If `value_reg` is at-or-below the
    // current watermark we fall through to `done_seek_label` without
    // updating the backing table — emitting the sync after the label
    // would clobber `sqlite_sequence.seq` with a *lower* value than the
    // engine's actual watermark, regressing the SQLite-compatibility
    // row even though the engine's autoinc state is unchanged. Repro
    // covered by `mvcc-autoinc-explicit-low-rowid-preserves-sqlite-sequence`
    // in `sqlite/conformance/turso-sqltests/mvcc_sequence.sqltest`.
    emit_autoincrement_sqlite_sequence_sync(program, resolver, database_id, seq_name, value_reg)?;

    program.preassign_label_to_next_insn(done_seek_label);
    program.emit_insn(Insn::Close { cursor_id });

    program.emit_insn(Insn::SequenceCommitInnerTx {
        db: database_id,
        path_kind_reg,
        saved_outer_reg,
        status_reg,
    });

    let after_retry_label = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: status_reg,
        target_pc: after_retry_label,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: retry_top_label,
    });
    program.preassign_label_to_next_insn(after_retry_label);

    Ok(())
}

/// Emit a scan-and-delete loop over a sequence backing table that
/// keeps only the row whose `value` (== rowid, since `value` is the
/// table's `INTEGER PRIMARY KEY`) equals `keep_value_reg`. Runs every
/// time nextval / advance_past inserts a new watermark row, so the
/// backing table stays at exactly one row across the run.
///
/// Replaces the legacy commit-time compaction that ran via
/// `prepare_internal()` from inside a vdbe handler. Keeping the loop
/// here (in bytecode emitted at prepare time) preserves the vdbe async
/// contract — every page read is a normal `StepResult::IO` yield, not
/// a synchronous `pager.io.step()` drive.
pub(crate) fn emit_backing_table_compaction(
    program: &mut ProgramBuilder,
    cursor_id: usize,
    seq_name: &str,
    keep_value_reg: usize,
) {
    let skip_label = program.allocate_label();
    let loop_top_label = program.allocate_label();
    let skip_delete_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id,
        pc_if_empty: skip_label,
    });
    program.preassign_label_to_next_insn(loop_top_label);
    let row_value_reg = program.alloc_register();
    program.emit_column_or_rowid(cursor_id, 0, row_value_reg);
    program.emit_insn(Insn::Eq {
        lhs: row_value_reg,
        rhs: keep_value_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Delete {
        cursor_id,
        table_name: seq_name.to_string(),
        // Sequence compaction is internal bookkeeping, not a SQL row change.
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(skip_delete_label);
    program.emit_insn(Insn::Next {
        cursor_id,
        pc_if_next: loop_top_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(skip_label);
}

/// Emit bytecode that mirrors a sequence watermark into the
/// `sqlite_sequence` row keyed by `autoinc_table_name`. Used by
/// nextval / advance_past / setval for `_autoincrement_*` sequences,
/// so that SQLite-compatibility readers of `sqlite_sequence` always
/// see the current high-water mark without waiting for a checkpoint.
///
/// Under MVCC the caller's autonomous inner tx wraps this write
/// together with the backing-table insert; two concurrent
/// AUTOINCREMENT inserts to the same table will WW-conflict on the
/// `sqlite_sequence` row and serialize via the existing
/// `sequence_inner_retry_count` budget. That matches SQLite's
/// single-writer AUTOINCREMENT semantics and is the price of moving
/// the sync out of the (vdbe-violating) commit-time helper.
///
/// Returns `Ok(false)` and emits nothing if the `sqlite_sequence`
/// table does not exist in this database — the schema-bootstrap path
/// in `translate/schema.rs` creates it lazily on first need; until
/// then no AUTOINCREMENT activity is possible so there is nothing to
/// sync.
pub(crate) fn emit_sqlite_sequence_sync(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    autoinc_table_name: &str,
    value_reg: usize,
) -> Result<bool> {
    let Some(sseq_table) = resolver.with_schema(database_id, |s| {
        s.get_btree_table(SQLITE_SEQUENCE_TABLE_NAME)
    }) else {
        return Ok(false);
    };
    let sseq_root = sseq_table.root_page;
    let sseq_cursor = program.alloc_cursor_id(CursorType::BTreeTable(sseq_table));

    program.emit_insn(Insn::OpenWrite {
        cursor_id: sseq_cursor,
        root_page: RegisterOrLiteral::Literal(sseq_root),
        db: database_id,
    });

    let name_reg = program.emit_string8_new_reg(autoinc_table_name.to_string());

    // Delete every existing row matching `name = autoinc_table_name`.
    let insert_label = program.allocate_label();
    let loop_top_label = program.allocate_label();
    let skip_delete_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sseq_cursor,
        pc_if_empty: insert_label,
    });
    program.preassign_label_to_next_insn(loop_top_label);
    let col_name_reg = program.alloc_register();
    program.emit_column_or_rowid(sseq_cursor, 0, col_name_reg);
    program.emit_insn(Insn::Ne {
        lhs: col_name_reg,
        rhs: name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Delete {
        cursor_id: sseq_cursor,
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
        // sqlite_sequence maintenance is excluded from changes().
        is_part_of_update: true,
    });
    program.preassign_label_to_next_insn(skip_delete_label);
    program.emit_insn(Insn::Next {
        cursor_id: sseq_cursor,
        pc_if_next: loop_top_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(insert_label);

    // Insert the new (name, seq) row with an auto-allocated rowid.
    let col_base = program.alloc_registers(2);
    program.emit_insn(Insn::Copy {
        src_reg: name_reg,
        dst_reg: col_base,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: value_reg,
        dst_reg: col_base + 1,
        extra_amount: 0,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(col_base),
        count: to_u32(2),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sseq_cursor,
        rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: sseq_cursor,
        key_reg: rowid_reg,
        record_reg,
        flag: InsertFlags::new().require_seek().skip_all_change_counts(),
        table_name: SQLITE_SEQUENCE_TABLE_NAME.to_string(),
    });
    program.emit_insn(Insn::Close {
        cursor_id: sseq_cursor,
    });
    Ok(true)
}

/// If `seq` is an AUTOINCREMENT-backing sequence (its name begins with
/// `__turso_internal_autoincrement_`), emit `sqlite_sequence` sync
/// bytecode for the underlying table. No-op for user-defined
/// sequences. Centralizes the "is this an AUTOINCREMENT seq?" check
/// so the three call sites (nextval / advance_past / setval) stay
/// uniform.
pub(crate) fn emit_autoincrement_sqlite_sequence_sync(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    seq_name: &str,
    value_reg: usize,
) -> Result<()> {
    let Some(table_name) = seq_name.strip_prefix(AUTOINCREMENT_SEQ_PREFIX) else {
        return Ok(());
    };
    emit_sqlite_sequence_sync(program, resolver, database_id, table_name, value_reg)?;
    Ok(())
}

/// Emit five `Insn::Integer` literals — start, increment, min, max, cycle —
/// to populate the immutable suffix of a backing-table row. Shared between
/// `emit_disk_read_nextval`, `emit_disk_advance_past`, and the setval
/// emitter in `translate/expr/functions.rs`.
pub(crate) fn emit_sequence_descriptor_literals(
    program: &mut ProgramBuilder,
    seq: &Sequence,
    dest_base: usize,
) {
    program.emit_insn(Insn::Integer {
        dest: dest_base,
        value: seq.start_value,
    });
    program.emit_insn(Insn::Integer {
        dest: dest_base + 1,
        value: seq.increment_by,
    });
    program.emit_insn(Insn::Integer {
        dest: dest_base + 2,
        value: seq.min_value,
    });
    program.emit_insn(Insn::Integer {
        dest: dest_base + 3,
        value: seq.max_value,
    });
    program.emit_insn(Insn::Integer {
        dest: dest_base + 4,
        value: if seq.cycle { 1 } else { 0 },
    });
}

/// Translate CREATE SEQUENCE into bytecode that persists the definition in sqlite_schema.
#[allow(clippy::too_many_arguments)]
pub fn translate_create_sequence(
    seq_name: &ast::QualifiedName,
    if_not_exists: bool,
    start: &Option<i64>,
    increment: &Option<i64>,
    min_value: &Option<i64>,
    max_value: &Option<i64>,
    cycle: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(seq_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let normalized_name = normalize_ident(seq_name.name.as_str());

    // `__turso_internal_autoincrement_<table>` is reserved for the
    // implicit sequences CREATE TABLE ... AUTOINCREMENT installs.
    // `emit_autoincrement_sqlite_sequence_sync` classifies any
    // sequence whose name starts with that prefix as
    // AUTOINCREMENT-backing and mirrors its watermark into
    // `sqlite_sequence` keyed on the suffix. A user-created sequence
    // in that namespace would therefore be able to write arbitrary
    // rows into `sqlite_sequence` for an attacker-chosen table name —
    // persistent SQLite-compatibility metadata corruption. Reject the
    // namespace at CREATE time. Covered by
    // `create-sequence-rejects-autoincrement-internal-prefix` in
    // `sqlite/conformance/turso-sqltests/sequence.sqltest`.
    if normalized_name.starts_with(AUTOINCREMENT_SEQ_PREFIX) {
        bail_parse_error!(
            "sequence name \"{}\" is reserved for internal AUTOINCREMENT use",
            normalized_name
        );
    }

    // Check if sequence already exists
    let exists = resolver.with_schema(database_id, |s| s.get_sequence(&normalized_name).is_some());
    if exists {
        if if_not_exists {
            return Ok(());
        }
        bail_parse_error!("sequence \"{}\" already exists", normalized_name);
    }

    // Validate parameters early (gives immediate error instead of deferred)
    let seq = Sequence::new(
        normalized_name.clone(),
        *start,
        *increment,
        *min_value,
        *max_value,
        cycle,
    )?;

    // Open cursor to sqlite_schema (in the target database)
    let table = resolver.with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID).unwrap());
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    emit_sequence_backing_table(
        program,
        resolver,
        database_id,
        sqlite_schema_cursor_id,
        &normalized_name,
        seq.start_value,
        seq.increment_by,
        seq.min_value,
        seq.max_value,
        seq.cycle,
    )?;

    // Add the fully-configured Sequence to the in-memory schema
    program.emit_insn(Insn::AddSequence {
        data: Box::new(AddSequenceData {
            db: database_id,
            name: normalized_name,
            start: seq.start_value,
            increment: seq.increment_by,
            min_value: seq.min_value,
            max_value: seq.max_value,
            cycle: seq.cycle,
        }),
    });

    // Bump schema version so other connections detect the change
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_cookie as i32 + 1,
        p5: 0,
    });

    Ok(())
}

/// Emit the cursor / sqlite_schema-delete / Destroy / DropSequence bytecode
/// for removing a single sequence from `database_id`. Callers must wrap this
/// with their own `begin_write_on_database` + `SetCookie` (so that callers
/// dropping several schema objects in one statement — e.g. `DROP TABLE` on an
/// AUTOINCREMENT table cleaning up its implicit sequence — share a single
/// schema-cookie bump).
///
/// Returns `Ok(true)` when the sequence existed and cleanup bytecode was
/// emitted; `Ok(false)` when the sequence (or its backing table) is absent,
/// so the caller can decide whether that's a no-op or an error.
pub(crate) fn emit_drop_sequence_cleanup(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    database_id: usize,
    seq_name: &str,
) -> Result<bool> {
    let backing_table_name = sequence_backing_table_name(seq_name);
    let root_page = resolver.with_schema(database_id, |s| {
        s.get_sequence(seq_name)?;
        Some(s.get_btree_table(&backing_table_name)?.root_page)
    });
    let Some(root_page) = root_page else {
        return Ok(false);
    };

    // Open sqlite_schema for writing (in the target database)
    let schema_table =
        resolver.with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID).unwrap());
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(schema_table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    let seq_name_reg = program.alloc_register();
    let type_str_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        dest: seq_name_reg,
        value: backing_table_name,
    });
    program.emit_insn(Insn::String8 {
        dest: type_str_reg,
        value: "table".to_string(),
    });

    // Scan sqlite_schema for (type='table' AND name=backing_table_name) and delete the row.
    let end_loop_label = program.allocate_label();
    let loop_start_label = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: end_loop_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);
    let col0_reg = program.alloc_register();
    let col1_reg = program.alloc_register();
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 0, col0_reg);
    program.emit_column_or_rowid(sqlite_schema_cursor_id, 1, col1_reg);
    let skip_delete_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: col0_reg,
        rhs: type_str_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Ne {
        lhs: col1_reg,
        rhs: seq_name_reg,
        target_pc: skip_delete_label,
        flags: CmpInsFlags::default(),
        collation: program.curr_collation(),
    });
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: "sqlite_schema".to_string(),
        is_part_of_update: false,
    });
    program.preassign_label_to_next_insn(skip_delete_label);
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: loop_start_label,
        fullscan: false,
        is_index: false,
    });
    program.preassign_label_to_next_insn(end_loop_label);

    // Destroy the B-tree root page
    let former_root_reg = program.alloc_register();
    program.emit_insn(Insn::Destroy {
        db: database_id,
        root: root_page,
        former_root_reg,
        is_temp: 0,
    });

    // Remove from the in-memory schema (sequences + tables maps)
    program.emit_insn(Insn::DropSequence {
        db: database_id,
        seq_name: seq_name.to_string(),
    });
    Ok(true)
}

/// Translate DROP SEQUENCE into bytecode that removes the definition from sqlite_schema.
pub fn translate_drop_sequence(
    seq_name: &ast::QualifiedName,
    if_exists: bool,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_database_id(seq_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;

    let normalized_name = normalize_ident(seq_name.name.as_str());
    let dropped = emit_drop_sequence_cleanup(program, resolver, database_id, &normalized_name)?;
    if !dropped {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("sequence \"{}\" does not exist", normalized_name);
    }

    // Bump schema version so other connections detect the change
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: schema_cookie as i32 + 1,
        p5: 0,
    });

    Ok(())
}
