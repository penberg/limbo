use crate::sync::Arc;

use crate::{
    bail_parse_error,
    function::{Func, FuncCtx, ScalarFunc},
    schema::{BTreeTable, Index, RESERVED_TABLE_PREFIXES},
    storage::pager::CreateBTreeFlags,
    translate::{
        emitter::Resolver,
        schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID},
    },
    util::normalize_ident,
    vdbe::{
        affinity::Affinity,
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, CmpInsFlags, Cookie, Insn, RegisterOrLiteral},
    },
    Result,
};
use turso_parser::ast;

/// A table paired with an optional specific index to analyze.
type AnalyzeTarget = (Arc<BTreeTable>, Option<Arc<Index>>);

/// Resolve the target database_id and collect analyze targets from the QualifiedName.
///
/// ANALYZE can target:
/// - Nothing (None): analyze all tables in main (database_id = 0)
/// - A database name ("main", "aux"): analyze all tables in that database
/// - A table name: analyze all indexes on that table
/// - A qualified table name (db.table): analyze in the specified database
/// - An index name: analyze just that index
fn resolve_analyze_targets(
    target_opt: &Option<ast::QualifiedName>,
    resolver: &Resolver,
) -> Result<(usize, Vec<AnalyzeTarget>)> {
    match target_opt {
        Some(target) => {
            let normalized = normalize_ident(target.name.as_str());

            // If db_name is specified, resolve to that database
            if let Some(db_name) = &target.db_name {
                let database_id = resolver.resolve_database_id(target)?;
                let db_normalized = normalize_ident(db_name.as_str());

                // "ANALYZE db.table" — the name part is the table/index
                // But first check if the name is actually a database name too (shouldn't be with db_name set)
                let targets = resolve_targets_in_db(&normalized, database_id, resolver)?;
                if targets.is_empty() {
                    bail_parse_error!("no such table or index: {}.{}", db_normalized, normalized);
                }
                return Ok((database_id, targets));
            }

            // No db_name — check if the name is a database name first
            if normalized.eq_ignore_ascii_case("main") {
                let targets = collect_all_tables_in_db(0, resolver);
                return Ok((0, targets));
            }

            // Check if it's an attached database name
            if let Some((db_id, _)) = resolver.get_attached_database(&normalized) {
                let targets = collect_all_tables_in_db(db_id, resolver);
                return Ok((db_id, targets));
            }

            // Not a database name — search main schema for table/index
            let targets = resolve_targets_in_db(&normalized, 0, resolver)?;
            if targets.is_empty() {
                bail_parse_error!("no such table or index: {}", target.name);
            }
            Ok((0, targets))
        }
        None => {
            // ANALYZE with no target — analyze all tables in main
            let targets = collect_all_tables_in_db(0, resolver);
            Ok((0, targets))
        }
    }
}

/// Collect all user tables in the given database.
fn collect_all_tables_in_db(database_id: usize, resolver: &Resolver) -> Vec<AnalyzeTarget> {
    resolver.with_schema(database_id, |schema| {
        schema
            .tables
            .iter()
            .filter_map(|(name, table)| {
                if RESERVED_TABLE_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
                {
                    return None;
                }
                table.btree().map(|bt| (bt, None))
            })
            .collect()
    })
}

/// Resolve a name as a table or index within a specific database.
fn resolve_targets_in_db(
    name: &str,
    database_id: usize,
    resolver: &Resolver,
) -> Result<Vec<AnalyzeTarget>> {
    // Try as a table first
    let table_opt: Option<Arc<BTreeTable>> =
        resolver.with_schema(database_id, |s| s.get_btree_table(name));
    if let Some(table) = table_opt {
        return Ok(vec![(table, None)]);
    }

    // Try as an index
    let found: Option<(Arc<BTreeTable>, Arc<Index>)> =
        resolver.with_schema(database_id, |schema| {
            for (table_name, indexes) in schema.indexes.iter() {
                if let Some(index) = indexes
                    .iter()
                    .find(|idx| idx.name.eq_ignore_ascii_case(name))
                {
                    if let Some(table) = schema.get_btree_table(table_name) {
                        return Some((table, index.clone()));
                    }
                }
            }
            None
        });
    if let Some((table, index)) = found {
        return Ok(vec![(table, Some(index))]);
    }

    Ok(vec![])
}

pub fn translate_analyze(
    target_opt: Option<ast::QualifiedName>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
) -> Result<()> {
    // Resolve the target database and collect analyze targets.
    let (database_id, analyze_targets) = resolve_analyze_targets(&target_opt, resolver)?;

    if analyze_targets.is_empty() {
        return Ok(());
    }

    // Register a write transaction for the target database so that the
    // epilogue emits a Transaction instruction (which starts the MVCC
    // exclusive transaction required by OpenWrite on sqlite_schema).
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    program.begin_write_operation()?;

    // This is emitted early because SQLite does, and thus generated VDBE matches a bit closer.
    let null_reg = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: null_reg,
        dest_end: None,
    });

    // After preparing/creating sqlite_stat1, we need to OpenWrite it, and how we acquire
    // the necessary BTreeTable for cursor creation and root page for the instruction changes
    // depending on which path we take.
    let sqlite_stat1_btreetable: Arc<BTreeTable>;
    let sqlite_stat1_source: RegisterOrLiteral<_>;

    let stat1_table: Option<Arc<BTreeTable>> =
        resolver.with_schema(database_id, |s| s.get_btree_table("sqlite_stat1"));
    if let Some(sqlite_stat1) = stat1_table {
        sqlite_stat1_btreetable = sqlite_stat1.clone();
        sqlite_stat1_source = RegisterOrLiteral::Literal(sqlite_stat1.root_page);
    } else {
        // FIXME: Emit ReadCookie 0 3 2
        // FIXME: Emit If 3 +2 0
        // FIXME: Emit SetCookie 0 2 4
        // FIXME: Emit SetCookie 0 5 1

        // See the large comment in schema.rs:translate_create_table about
        // deviating from SQLite codegen, as the same deviation is being done
        // here.

        // TODO: this code half-copies translate_create_table, because there's
        // no way to get the table_root_reg back out, and it's needed for later
        // codegen to open the table we just created.  It's worth a future
        // refactoring to remove the duplication one the rest of ANALYZE is
        // implemented.
        let table_root_reg = program.alloc_register();
        program.emit_insn(Insn::CreateBtree {
            db: database_id,
            root: table_root_reg,
            flags: CreateBTreeFlags::new_table(),
        });
        let sql = "CREATE TABLE sqlite_stat1(tbl,idx,stat)";
        // The root_page==0 is false, but we don't rely on it, and there's no
        // way to initialize it with a correct value.
        sqlite_stat1_btreetable = Arc::new(BTreeTable::from_sql(sql, 0)?);
        sqlite_stat1_source = RegisterOrLiteral::Register(table_root_reg);

        let table = resolver
            .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
            .unwrap();
        let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: sqlite_schema_cursor_id,
            root_page: 1i64.into(),
            db: database_id,
        });

        // Add the table entry to sqlite_schema
        emit_schema_entry(
            program,
            resolver,
            sqlite_schema_cursor_id,
            None,
            SchemaEntryType::Table,
            "sqlite_stat1",
            "sqlite_stat1",
            table_root_reg,
            Some(sql.to_string()),
        )?;

        let parse_schema_where_clause =
            "tbl_name = 'sqlite_stat1' AND type != 'trigger'".to_string();
        program.emit_insn(Insn::ParseSchema {
            db: database_id,
            where_clause: Some(parse_schema_where_clause),
            trigger_target_database_id: None,
        });

        // Bump schema cookie so subsequent statements reparse schema.
        let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
        program.emit_insn(Insn::SetCookie {
            db: database_id,
            cookie: Cookie::SchemaVersion,
            value: schema_version as i32 + 1,
            p5: 0,
        });
    };

    // Count the number of rows in the target table(s), and insert into sqlite_stat1.
    let sqlite_stat1 = sqlite_stat1_btreetable;
    let stat_cursor = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_stat1));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: stat_cursor,
        root_page: sqlite_stat1_source,
        db: database_id,
    });

    for (target_table, target_index) in analyze_targets {
        if !target_table.has_rowid {
            bail_parse_error!("ANALYZE on tables without rowid is not supported");
        }

        // Remove existing stat rows for this target before inserting fresh ones.
        let rewind_done = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id: stat_cursor,
            pc_if_empty: rewind_done,
        });
        let loop_start = program.allocate_label();
        program.preassign_label_to_next_insn(loop_start);

        let tbl_col_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: stat_cursor,
            column: 0,
            dest: tbl_col_reg,
            default: None,
        });
        let target_tbl_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: target_table.name.to_string(),
            dest: target_tbl_reg,
        });
        program.mark_last_insn_constant();

        let skip_label = program.allocate_label();
        program.emit_insn(Insn::Ne {
            lhs: tbl_col_reg,
            rhs: target_tbl_reg,
            target_pc: skip_label,
            flags: Default::default(),
            collation: None,
        });

        if let Some(idx) = target_index.clone() {
            let idx_col_reg = program.alloc_register();
            program.emit_insn(Insn::Column {
                cursor_id: stat_cursor,
                column: 1,
                dest: idx_col_reg,
                default: None,
            });
            let target_idx_reg = program.alloc_register();
            program.emit_insn(Insn::String8 {
                value: idx.name.to_string(),
                dest: target_idx_reg,
            });
            program.mark_last_insn_constant();
            program.emit_insn(Insn::Ne {
                lhs: idx_col_reg,
                rhs: target_idx_reg,
                target_pc: skip_label,
                flags: Default::default(),
                collation: None,
            });
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: stat_cursor,
                dest: rowid_reg,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: stat_cursor,
                table_name: "sqlite_stat1".to_string(),
                is_part_of_update: false,
            });
            program.emit_insn(Insn::Next {
                cursor_id: stat_cursor,
                pc_if_next: loop_start,
                fullscan: false,
                is_index: false,
            });
        } else {
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::RowId {
                cursor_id: stat_cursor,
                dest: rowid_reg,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: stat_cursor,
                table_name: "sqlite_stat1".to_string(),
                is_part_of_update: false,
            });
            program.emit_insn(Insn::Next {
                cursor_id: stat_cursor,
                pc_if_next: loop_start,
                fullscan: false,
                is_index: false,
            });
        }

        program.preassign_label_to_next_insn(skip_label);
        program.emit_insn(Insn::Next {
            cursor_id: stat_cursor,
            pc_if_next: loop_start,
            fullscan: false,
            is_index: false,
        });
        program.preassign_label_to_next_insn(rewind_done);

        let target_cursor = program.alloc_cursor_id(CursorType::BTreeTable(target_table.clone()));
        program.emit_insn(Insn::OpenRead {
            cursor_id: target_cursor,
            root_page: target_table.root_page,
            db: database_id,
        });
        let tablename_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: target_table.name.to_string(),
            dest: tablename_reg,
        });
        program.mark_last_insn_constant();
        // Emit index stats for this table (or for a single index target).
        let is_specific_index_target = target_index.is_some();
        let indexes: Vec<Arc<Index>> = match target_index {
            Some(idx) => vec![idx],
            None => resolver.with_schema(database_id, |s| {
                s.get_indices(&target_table.name)
                    .filter(|idx| idx.index_method.is_none()) // skip custom for now
                    .cloned()
                    .collect()
            }),
        };
        // Match SQLite: emit the table-level sqlite_stat1 row only when ANALYZE
        // targets a table and there are no non-partial indexes contributing stats.
        let emit_table_stat =
            !is_specific_index_target && !indexes.iter().any(|index| index.where_clause.is_none());
        if emit_table_stat {
            let indexname_reg = program.alloc_register();
            let stat_text_reg = program.alloc_register();
            let record_reg = program.alloc_register();
            let count_reg = program.alloc_register();
            program.emit_insn(Insn::Count {
                cursor_id: target_cursor,
                target_reg: count_reg,
                exact: true,
            });
            let after_insert = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg: count_reg,
                target_pc: after_insert,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Null {
                dest: indexname_reg,
                dest_end: None,
            });
            // stat = CAST(count AS TEXT)
            program.emit_insn(Insn::Copy {
                src_reg: count_reg,
                dst_reg: stat_text_reg,
                extra_amount: 0,
            });
            program.emit_insn(Insn::Cast {
                reg: stat_text_reg,
                affinity: Affinity::Text,
            });
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(tablename_reg),
                count: to_u32(3),
                dest_reg: to_u32(record_reg),
                index_name: None,
                affinity_str: None,
            });
            let rowid_reg = program.alloc_register();
            program.emit_insn(Insn::NewRowid {
                cursor: stat_cursor,
                rowid_reg,
                prev_largest_reg: 0,
            });
            // FIXME: SQLite sets OPFLAG_APPEND on the insert, but that's not supported in turso right now.
            // SQLite doesn't emit the table name, but like... why not?
            program.emit_insn(Insn::Insert {
                cursor: stat_cursor,
                key_reg: rowid_reg,
                record_reg,
                flag: Default::default(),
                table_name: "sqlite_stat1".to_string(),
            });
            program.preassign_label_to_next_insn(after_insert);
        }
        for index in indexes {
            emit_index_stats(program, stat_cursor, &target_table, &index, database_id);
        }
    }

    // FIXME: Emit LoadAnalysis
    // FIXME: Emit Expire
    Ok(())
}

/// Emit VDBE code to gather and insert statistics for a single index.
///
/// This uses the stat_init/stat_push/stat_get functions to collect statistics.
/// The bytecode scans the index in sorted order, comparing columns to detect
/// when prefixes change, and calls stat_push with the change index.
///
/// The stat string format is: "total avg1 avg2 avg3"
/// where avgN = ceil(total / distinctN) = average rows per distinct prefix
fn emit_index_stats(
    program: &mut ProgramBuilder,
    stat_cursor: usize,
    table: &Arc<BTreeTable>,
    index: &Arc<Index>,
    database_id: usize,
) {
    let n_cols = index.columns.len();
    if n_cols == 0 {
        return;
    }

    // Open the index cursor
    let idx_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: idx_cursor,
        root_page: index.root_page,
        db: database_id,
    });

    // Allocate registers contiguously for stat_push(accum, chng):
    let reg_accum = program.alloc_register();
    let reg_chng = program.alloc_register();

    // Registers for previous row values and comparison temp
    let reg_prev_base = program.alloc_registers(n_cols);
    let reg_temp = program.alloc_register();

    // Initialize the accumulator with stat_init(n_cols)
    // Reuse reg_chng temporarily for the n_cols argument
    program.emit_insn(Insn::Integer {
        value: n_cols as i64,
        dest: reg_chng,
    });
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: reg_chng,
        dest: reg_accum,
        func: FuncCtx {
            func: Func::Scalar(ScalarFunc::StatInit),
            arg_count: 1,
        },
    });

    // Labels for control flow
    let lbl_empty = program.allocate_label();
    let lbl_loop = program.allocate_label();
    let lbl_stat_push = program.allocate_label();

    // We need one label per column for the update_prev jump targets
    let lbl_update_prev: Vec<_> = (0..n_cols).map(|_| program.allocate_label()).collect();

    // Rewind the index cursor; if empty, skip to end
    program.emit_insn(Insn::Rewind {
        cursor_id: idx_cursor,
        pc_if_empty: lbl_empty,
    });

    // First row: set chng=0 and jump to update all prev columns
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_chng,
    });
    program.emit_insn(Insn::Goto {
        target_pc: lbl_update_prev[0],
    });

    // Main loop: compare columns to find change point
    program.preassign_label_to_next_insn(lbl_loop);

    // Set reg_chng = 0, then check each column
    program.emit_insn(Insn::Integer {
        value: 0,
        dest: reg_chng,
    });

    for (i, lbl) in lbl_update_prev.iter().enumerate().take(n_cols) {
        program.emit_insn(Insn::Column {
            cursor_id: idx_cursor,
            column: i,
            dest: reg_temp,
            default: None,
        });
        program.emit_insn(Insn::Ne {
            lhs: reg_temp,
            rhs: reg_prev_base + i,
            target_pc: *lbl,
            flags: CmpInsFlags::default().null_eq(),
            collation: index.columns[i].collation,
        });
        // If columns match, increment chng and continue to next column
        if i < n_cols - 1 {
            program.emit_insn(Insn::Integer {
                value: (i + 1) as i64,
                dest: reg_chng,
            });
        }
    }

    // All columns equal - chng = n_cols (duplicate row), jump over update section to stat_push
    program.emit_insn(Insn::Integer {
        value: n_cols as i64,
        dest: reg_chng,
    });
    program.emit_insn(Insn::Goto {
        target_pc: lbl_stat_push,
    });

    // Update prev section: emit n_cols consecutive Column instructions that cascade
    // When col i differs from prev, jump here to update prev[i], prev[i+1], ..., prev[n_cols-1]
    for (i, lbl) in lbl_update_prev.iter().enumerate().take(n_cols) {
        program.preassign_label_to_next_insn(*lbl);
        program.emit_insn(Insn::Column {
            cursor_id: idx_cursor,
            column: i,
            dest: reg_prev_base + i,
            default: None,
        });
        // Fall through to next column update, then to stat_push
    }

    program.preassign_label_to_next_insn(lbl_stat_push);
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: reg_accum,
        dest: reg_accum,
        func: FuncCtx {
            func: Func::Scalar(ScalarFunc::StatPush),
            arg_count: 2,
        },
    });

    // Next iteration
    program.emit_insn(Insn::Next {
        cursor_id: idx_cursor,
        pc_if_next: lbl_loop,
        fullscan: false,
        is_index: false,
    });

    // stat_get(accum) to get the final stat string
    let reg_stat = program.alloc_register();
    program.emit_insn(Insn::Function {
        constant_mask: 0,
        start_reg: reg_accum,
        dest: reg_stat,
        func: FuncCtx {
            func: Func::Scalar(ScalarFunc::StatGet),
            arg_count: 1,
        },
    });

    // Skip insert if stat is NULL (empty index)
    program.emit_insn(Insn::IsNull {
        reg: reg_stat,
        target_pc: lbl_empty,
    });

    // Insert record into sqlite_stat1
    // Allocate contiguous registers for MakeRecord: tablename, indexname, stat
    let record_start = program.alloc_registers(3);
    program.emit_insn(Insn::String8 {
        value: table.name.to_string(),
        dest: record_start,
    });
    program.mark_last_insn_constant();
    program.emit_insn(Insn::String8 {
        value: index.name.to_string(),
        dest: record_start + 1,
    });
    program.mark_last_insn_constant();
    program.emit_insn(Insn::Copy {
        src_reg: reg_stat,
        dst_reg: record_start + 2,
        extra_amount: 0,
    });

    let idx_record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(record_start),
        count: to_u32(3),
        dest_reg: to_u32(idx_record_reg),
        index_name: None,
        affinity_str: None,
    });

    let idx_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: stat_cursor,
        rowid_reg: idx_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: stat_cursor,
        key_reg: idx_rowid_reg,
        record_reg: idx_record_reg,
        flag: Default::default(),
        table_name: "sqlite_stat1".to_string(),
    });

    // Label for empty index case, just skip the insert
    program.preassign_label_to_next_insn(lbl_empty);
}
