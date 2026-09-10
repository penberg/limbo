//! VDBE bytecode generation for pragma statements.
//! More info: https://www.sqlite.org/pragma.html.

use crate::alloc::TursoIteratorExt;
use crate::sync::Arc;
use crate::turso_soft_unreachable;
use chrono::Datelike;
use turso_macros::{match_ignore_ascii_case, turso_debug_assert};
use turso_parser::ast::PragmaName;
use turso_parser::ast::{self, Expr, Literal};

use super::integrity_check::{
    translate_integrity_check, translate_quick_check, MAX_INTEGRITY_CHECK_ERRORS,
};
use crate::function::Func;
use crate::pragma::pragma_for;
use crate::schema::{Schema, Table};
use crate::storage::encryption::{CipherMode, EncryptionKey};
use crate::storage::pager::AutoVacuumMode;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::CacheSize;
use crate::storage::wal::CheckpointMode;
use crate::translate::emitter::{Resolver, TransactionMode};
use crate::translate::plan::BitSet;
use crate::util::{normalize_ident, parse_signed_number, parse_string, IOExt as _};
use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::{Cookie, Insn};
use crate::{
    bail_parse_error, CaptureDataChangesInfo, LimboError, Numeric, Value, CDC_VERSION_CURRENT,
};
use std::str::FromStr;
use strum::IntoEnumIterator;

fn list_pragmas(program: &mut ProgramBuilder) {
    for x in PragmaName::iter() {
        let register = program.emit_string8_new_reg(x.to_string());
        program.emit_result_row(register, 1);
    }
    program.add_pragma_result_column("pragma_list".into());
}

/// Parse max_errors from an optional value expression.
/// Returns the parsed integer if value is a numeric literal, otherwise returns the default.
fn parse_max_errors_from_value(value: &Option<Expr>) -> usize {
    match value {
        Some(Expr::Literal(Literal::Numeric(n))) => {
            n.parse::<usize>().unwrap_or(MAX_INTEGRITY_CHECK_ERRORS)
        }
        _ => MAX_INTEGRITY_CHECK_ERRORS,
    }
}

fn visible_database_ids_for_table_list(connection: &crate::Connection) -> crate::Result<BitSet> {
    use crate::alloc::*;
    let mut ids = BitSet::default();
    ids.set(crate::MAIN_DB_ID)?;
    ids.set(crate::TEMP_DB_ID)?;
    ids.try_extend(
        connection
            .attached_databases()
            .read()
            .index_to_data
            .keys()
            .copied(),
    )?;
    Ok(ids)
}

fn display_table_list_name(database_id: usize, name: &str) -> String {
    if database_id == crate::TEMP_DB_ID
        && name.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME)
    {
        crate::schema::TEMP_SCHEMA_TABLE_NAME.to_string()
    } else {
        name.to_string()
    }
}

fn normalize_table_pragma_lookup_name(database_id: usize, name: &str) -> String {
    let normalized = normalize_ident(name);
    if (database_id == crate::TEMP_DB_ID
        && (normalized.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
            || normalized.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)))
        || normalized.eq_ignore_ascii_case(crate::schema::SCHEMA_TABLE_NAME_ALT)
    {
        crate::schema::SCHEMA_TABLE_NAME.to_string()
    } else {
        normalized
    }
}

fn resolve_table_pragma_database_id(
    resolver: &Resolver,
    default_database_id: usize,
    schema_was_explicit: bool,
    table_name: &str,
) -> crate::Result<usize> {
    if schema_was_explicit {
        return Ok(default_database_id);
    }

    if table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME)
        || table_name.eq_ignore_ascii_case(crate::schema::TEMP_SCHEMA_TABLE_NAME_ALT)
    {
        return Ok(crate::TEMP_DB_ID);
    }
    resolver.resolve_existing_table_database_id(table_name)
}

fn resolve_index_pragma_database_id(
    resolver: &Resolver,
    default_database_id: usize,
    schema_was_explicit: bool,
    index_name: &str,
) -> crate::Result<usize> {
    if schema_was_explicit {
        return Ok(default_database_id);
    }

    let qualified_name = ast::QualifiedName {
        db_name: None,
        name: ast::Name::exact(index_name.to_string()),
        alias: None,
    };
    resolver.resolve_existing_index_database_id(&qualified_name)
}

fn foreign_key_action_name(action: ast::RefAct) -> &'static str {
    match action {
        ast::RefAct::SetNull => "SET NULL",
        ast::RefAct::SetDefault => "SET DEFAULT",
        ast::RefAct::Cascade => "CASCADE",
        ast::RefAct::Restrict => "RESTRICT",
        ast::RefAct::NoAction => "NO ACTION",
    }
}

fn emit_table_list_rows_for_schema(
    program: &mut ProgramBuilder,
    schema: &Schema,
    database_id: usize,
    database_name: &str,
    base_reg: usize,
    filter_name: Option<&str>,
) {
    let emit_table_row = |program: &mut ProgramBuilder,
                          name: &str,
                          obj_type: &str,
                          ncol: usize,
                          wr: bool,
                          strict: bool| {
        program.emit_string8(database_name.to_string(), base_reg);
        program.emit_string8(display_table_list_name(database_id, name), base_reg + 1);
        program.emit_string8(obj_type.to_string(), base_reg + 2);
        program.emit_int(ncol as i64, base_reg + 3);
        program.emit_int(wr as i64, base_reg + 4);
        program.emit_int(strict as i64, base_reg + 5);
        program.emit_result_row(base_reg, 6);
    };

    if let Some(filter_name) = filter_name {
        let lookup_name = normalize_table_pragma_lookup_name(database_id, filter_name);
        if let Some(table) = schema.get_table(&lookup_name) {
            let (wr, strict) = match table.btree() {
                Some(bt) => (!bt.has_rowid, bt.is_strict),
                None => (false, false),
            };
            emit_table_row(
                program,
                table.get_name(),
                "table",
                table.columns().len(),
                wr,
                strict,
            );
        } else if let Some(view) = schema.get_view(&lookup_name) {
            emit_table_row(
                program,
                &view.name,
                "view",
                view.columns.len(),
                false,
                false,
            );
        }
        return;
    }

    for table in schema.tables.values() {
        let Some(bt) = table.btree() else {
            continue;
        };
        emit_table_row(
            program,
            &bt.name,
            "table",
            bt.columns().len(),
            !bt.has_rowid,
            bt.is_strict,
        );
    }
    for view in schema.views.values() {
        emit_table_row(
            program,
            &view.name,
            "view",
            view.columns.len(),
            false,
            false,
        );
    }
}

pub fn translate_pragma(
    resolver: &Resolver,
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let opts = ProgramBuilderOpts::new(0, 20, 0);
    program.extend(&opts);

    if name.name.as_str().eq_ignore_ascii_case("pragma_list") {
        list_pragmas(program);
        return Ok(());
    }

    let Ok(pragma) = PragmaName::from_str(name.name.as_str()) else {
        // SQLite silently ignores unknown PRAGMA names.
        return Ok(());
    };

    let database_id = resolver.resolve_database_id(name)?;
    let schema_was_explicit = name.db_name.is_some();

    let mode = match body {
        None => query_pragma(
            pragma,
            resolver,
            None,
            pager,
            connection,
            database_id,
            schema_was_explicit,
            program,
        )?,
        Some(ast::PragmaBody::Equals(value) | ast::PragmaBody::Call(value)) => match pragma {
            // These pragmas take a parameter but are queries, not setters
            PragmaName::IndexInfo
            | PragmaName::IndexXinfo
            | PragmaName::IndexList
            | PragmaName::ForeignKeyList
            | PragmaName::TableList
            | PragmaName::TableInfo
            | PragmaName::TableXinfo
            | PragmaName::IntegrityCheck
            | PragmaName::DatabaseList
            | PragmaName::QuickCheck => query_pragma(
                pragma,
                resolver,
                Some(*value),
                pager,
                connection,
                database_id,
                schema_was_explicit,
                program,
            )?,
            _ => update_pragma(
                pragma,
                resolver,
                *value,
                pager,
                connection,
                database_id,
                schema_was_explicit,
                program,
            )?,
        },
    };
    match mode {
        TransactionMode::None => {}
        TransactionMode::Read => {
            let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
            program.begin_read_on_database(database_id, schema_cookie)?;
            program.begin_read_operation()?;
        }
        TransactionMode::Write => {
            let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
            program.begin_write_on_database(database_id, schema_cookie)?;
            program.begin_write_operation()?;
        }
        TransactionMode::Concurrent => {
            program.begin_concurrent_operation();
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_pragma(
    pragma: PragmaName,
    resolver: &Resolver,
    value: ast::Expr,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    database_id: usize,
    schema_was_explicit: bool,
    program: &mut ProgramBuilder,
) -> crate::Result<TransactionMode> {
    let parse_pragma_enabled = |expr: &ast::Expr| -> bool {
        if let Expr::Literal(Literal::Numeric(n)) = expr {
            return !matches!(n.as_str(), "0");
        };
        let name_bytes = match expr {
            Expr::Literal(Literal::Keyword(name)) => name.as_bytes(),
            Expr::Name(name) | Expr::Id(name) => name.as_str().as_bytes(),
            _ => "".as_bytes(),
        };
        match_ignore_ascii_case!(match name_bytes {
            b"ON" | b"TRUE" | b"YES" | b"1" => true,
            _ => false,
        })
    };
    match pragma {
        PragmaName::ApplicationId => {
            let data = parse_signed_number(&value)?;
            let app_id_value = match data {
                Value::Numeric(Numeric::Integer(i)) => i as i32,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as i32,
                _ => bail_parse_error!("expected integer, got {:?}", data),
            };

            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::ApplicationId,
                value: app_id_value,
                p5: 1,
            });
            Ok(TransactionMode::Write)
        }
        PragmaName::BusyTimeout => {
            let data = parse_signed_number(&value)?;
            let busy_timeout_ms = match data {
                Value::Numeric(Numeric::Integer(i)) => i as i32,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as i32,
                _ => bail_parse_error!("expected integer, got {:?}", data),
            };
            let busy_timeout_ms = busy_timeout_ms.max(0);
            connection.set_busy_timeout(std::time::Duration::from_millis(busy_timeout_ms as u64));
            Ok(TransactionMode::None)
        }
        PragmaName::CacheSize => {
            let cache_size = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) => size,
                Value::Numeric(Numeric::Float(size)) => f64::from(size) as i64,
                _ => bail_parse_error!("Invalid value for cache size pragma"),
            };
            update_cache_size(cache_size, pager, connection)?;
            Ok(TransactionMode::None)
        }
        PragmaName::CacheSpill => {
            let enabled = parse_pragma_enabled(&value);
            connection.get_pager().set_spill_enabled(enabled);
            connection.bump_prepare_context_generation();
            Ok(TransactionMode::None)
        }
        PragmaName::Encoding => {
            let year = chrono::Local::now().year();
            bail_parse_error!("It's {year}. UTF-8 won.");
        }
        PragmaName::JournalMode => {
            // For JournalMode, when setting a value, we use the opcode
            let mode_str = match value {
                Expr::Name(name) => name.as_str().to_string(),
                Expr::Literal(Literal::Keyword(ref kw)) => kw.clone(),
                _ => parse_string(&value)?,
            };

            let result_reg = program.alloc_register();
            program.emit_insn(Insn::JournalMode {
                db: database_id,
                dest: result_reg,
                new_mode: Some(mode_str),
            });

            program.emit_result_row(result_reg, 1);
            program.add_pragma_result_column("journal_mode".into());
            Ok(TransactionMode::None)
        }
        PragmaName::LockingMode => {
            let mode = match &value {
                Expr::Name(name) => name.as_str().to_string(),
                Expr::Literal(Literal::Keyword(kw)) => kw.clone(),
                Expr::Literal(Literal::String(s)) => s.clone(),
                _ => parse_string(&value)?,
            };
            let mode_bytes = mode.as_bytes();
            match_ignore_ascii_case!(match mode_bytes {
                b"EXCLUSIVE" => {}
                _ => bail_parse_error!("locking_mode must be EXCLUSIVE"),
            });
            query_pragma(
                PragmaName::LockingMode,
                resolver,
                None,
                pager,
                connection,
                database_id,
                schema_was_explicit,
                program,
            )
        }
        PragmaName::FullColumnNames => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_full_column_names(enabled);
            Ok(TransactionMode::None)
        }
        PragmaName::ShortColumnNames => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_short_column_names(enabled);
            Ok(TransactionMode::None)
        }
        PragmaName::LegacyFileFormat | PragmaName::EmptyResultCallbacks => {
            Ok(TransactionMode::None)
        }
        PragmaName::WalCheckpoint => query_pragma(
            PragmaName::WalCheckpoint,
            resolver,
            Some(value),
            pager,
            connection,
            database_id,
            schema_was_explicit,
            program,
        ),
        PragmaName::ModuleList => Ok(TransactionMode::None),
        PragmaName::PageCount => query_pragma(
            PragmaName::PageCount,
            resolver,
            None,
            pager,
            connection,
            database_id,
            schema_was_explicit,
            program,
        ),
        PragmaName::MaxPageCount => {
            let data = parse_signed_number(&value)?;
            let max_page_count_value = match data {
                Value::Numeric(Numeric::Integer(i)) => i as usize,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as usize,
                _ => unreachable!(),
            };

            let result_reg = program.alloc_register();
            program.emit_insn(Insn::MaxPgcnt {
                db: database_id,
                dest: result_reg,
                new_max: max_page_count_value,
            });
            program.emit_result_row(result_reg, 1);
            program.add_pragma_result_column("max_page_count".into());
            Ok(TransactionMode::Write)
        }
        PragmaName::UserVersion => {
            let data = parse_signed_number(&value)?;
            let version_value = match data {
                Value::Numeric(Numeric::Integer(i)) => i as i32,
                Value::Numeric(Numeric::Float(f)) => f64::from(f) as i32,
                _ => unreachable!(),
            };

            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::UserVersion,
                value: version_value,
                p5: 1,
            });
            Ok(TransactionMode::Write)
        }
        PragmaName::SchemaVersion => {
            // SQLite allowing this to be set is an incredibly stupid idea in my view.
            // In "defensive mode", this is a silent nop. So let's emulate that always.
            program.emit_insn(Insn::Noop {});
            Ok(TransactionMode::None)
        }
        PragmaName::TableInfo => {
            // because we need control over the write parameter for the transaction,
            // this should be unreachable. We have to force-call query_pragma before
            // getting here
            unreachable!();
        }
        PragmaName::TableXinfo => {
            // because we need control over the write parameter for the transaction,
            // this should be unreachable. We have to force-call query_pragma before
            // getting here
            unreachable!();
        }
        PragmaName::PageSize => {
            let page_size = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) => size,
                Value::Numeric(Numeric::Float(size)) => f64::from(size) as i64,
                _ => bail_parse_error!("Invalid value for page size pragma"),
            };
            update_page_size(connection, page_size as u32)?;
            Ok(TransactionMode::None)
        }
        PragmaName::AutoVacuum => {
            // SQLite spells the three modes either by name or by number, and
            // treats the two spellings as the same thing.
            let requested_mode = match &value {
                Expr::Name(name) => {
                    let name = name.as_str().as_bytes();
                    match_ignore_ascii_case!(match name {
                        b"none" => Some(AutoVacuumMode::None),
                        b"full" => Some(AutoVacuumMode::Full),
                        b"incremental" => Some(AutoVacuumMode::Incremental),
                        _ => None,
                    })
                }
                _ => match parse_signed_number(&value) {
                    Ok(Value::Numeric(Numeric::Integer(n @ 0..=2))) => {
                        Some(AutoVacuumMode::from(n as u8))
                    }
                    _ => None,
                },
            };

            // Auto-vacuum is off unless the experimental flag turns it on, so
            // asking for NONE only restates the state the database is already
            // in. Requiring the flag to switch the feature off would make every
            // `PRAGMA auto_vacuum=NONE` in portable SQL fail for no reason.
            if requested_mode != Some(AutoVacuumMode::None) && !connection.db.opts.enable_autovacuum
            {
                return Err(LimboError::InvalidArgument(
                    "Autovacuum is not enabled. Use --experimental-autovacuum flag to enable it."
                        .to_string(),
                ));
            }

            // Like SQLite, the auto-vacuum mode is fixed once page 1 exists,
            // so the pragma is silently ignored after that.
            if pager.db_initialized() {
                return Ok(TransactionMode::None);
            }

            let Some(auto_vacuum_mode) = requested_mode else {
                return Err(LimboError::InvalidArgument(
                    "invalid auto vacuum mode".to_string(),
                ));
            };
            pager.persist_auto_vacuum_mode(auto_vacuum_mode)?;
            let largest_root_page_number_reg = program.alloc_register();
            program.emit_insn(Insn::ReadCookie {
                db: database_id,
                dest: largest_root_page_number_reg,
                cookie: Cookie::LargestRootPageNumber,
            });
            let set_cookie_label = program.allocate_label();
            program.emit_insn(Insn::If {
                reg: largest_root_page_number_reg,
                target_pc: set_cookie_label,
                jump_if_null: false,
            });
            program.emit_insn(Insn::Halt {
                err_code: 0,
                description: "Early halt because auto vacuum mode is not enabled".to_string(),
                on_error: None,
                description_reg: None,
            });
            program.preassign_label_to_next_insn(set_cookie_label);
            program.emit_insn(Insn::SetCookie {
                db: database_id,
                cookie: Cookie::IncrementalVacuum,
                value: i32::from(u8::from(auto_vacuum_mode)) - 1,
                p5: 0,
            });
            Ok(TransactionMode::None)
        }
        PragmaName::IntegrityCheck => unreachable!("integrity_check cannot be set"),
        PragmaName::QuickCheck => unreachable!("quick_check cannot be set"),
        PragmaName::CaptureDataChangesConn | PragmaName::UnstableCaptureDataChangesConn => {
            let value = parse_string(&value)?;
            let opts = CaptureDataChangesInfo::parse(&value, Some(CDC_VERSION_CURRENT))?;
            // InitCdcVersion handles everything at execution time:
            // - For enable: creates CDC table + version table, records version,
            //   reads back actual version, defers CDC state to Halt
            // - For disable ("off"): defers CDC=None to Halt
            let cdc_table_name = opts
                .as_ref()
                .map(|i| i.table.to_string())
                .unwrap_or_default();
            program.emit_insn(Insn::InitCdcVersion {
                cdc_table_name,
                version: CDC_VERSION_CURRENT,
                cdc_mode: value,
            });
            Ok(TransactionMode::Write)
        }
        PragmaName::DatabaseList => unreachable!("database_list cannot be set"),
        PragmaName::IndexInfo => unreachable!("index_info cannot be set"),
        PragmaName::IndexXinfo => unreachable!("index_xinfo cannot be set"),
        PragmaName::IndexList => unreachable!("index_list cannot be set"),
        PragmaName::ForeignKeyList => unreachable!("foreign_key_list cannot be set"),
        PragmaName::TableList => unreachable!("table_list cannot be set"),
        PragmaName::QueryOnly => query_pragma(
            PragmaName::QueryOnly,
            resolver,
            Some(value),
            pager,
            connection,
            database_id,
            schema_was_explicit,
            program,
        ),
        PragmaName::FreelistCount => query_pragma(
            PragmaName::FreelistCount,
            resolver,
            Some(value),
            pager,
            connection,
            database_id,
            schema_was_explicit,
            program,
        ),
        PragmaName::EncryptionKey => {
            let value = parse_string(&value)?;
            let key = EncryptionKey::from_hex_string(&value)?;
            connection.set_encryption_key(key)?;
            Ok(TransactionMode::None)
        }
        PragmaName::EncryptionCipher => {
            let value = parse_string(&value)?;
            let cipher = CipherMode::try_from(value.as_str())?;
            connection.set_encryption_cipher(cipher)?;
            Ok(TransactionMode::None)
        }
        PragmaName::Synchronous => {
            use crate::SyncMode;
            let mode = if let Expr::Literal(Literal::Numeric(n)) = &value {
                match n.as_str() {
                    "0" => SyncMode::Off,
                    "1" => SyncMode::Normal,
                    _ => SyncMode::Full, // SQLite defaults to NORMAL for invalid values, but we want to default to a higher durability level so deviating here.
                }
            } else {
                let name_bytes = match &value {
                    Expr::Literal(Literal::Keyword(name)) => name.as_bytes(),
                    Expr::Name(name) | Expr::Id(name) => name.as_str().as_bytes(),
                    _ => b"",
                };
                match_ignore_ascii_case!(match name_bytes {
                    b"OFF" | b"0" => SyncMode::Off,
                    b"NORMAL" | b"1" => SyncMode::Normal,
                    _ => SyncMode::Full,
                })
            };
            connection.set_sync_mode_for_database(database_id, mode)?;
            Ok(TransactionMode::None)
        }
        PragmaName::DataSyncRetry => {
            let retry_enabled = parse_pragma_enabled(&value);
            connection.set_data_sync_retry(retry_enabled);
            Ok(TransactionMode::None)
        }
        PragmaName::MvccCheckpointThreshold => {
            let threshold = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) if size >= -1 => size,
                _ => bail_parse_error!(
                    "mvcc_checkpoint_threshold must be -1, 0, or a positive integer"
                ),
            };

            connection.set_mvcc_checkpoint_threshold(threshold)?;
            Ok(TransactionMode::None)
        }
        PragmaName::MvccGcThreshold => {
            // 0 is rejected: it would run an inline GC pass on every commit even
            // with zero new versions (`should_gc` compares growth `>= threshold`),
            // which is pure overhead. Use -1 to disable, or a positive growth
            // threshold.
            let threshold = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) if size == -1 || size >= 1 => size,
                _ => bail_parse_error!(
                    "mvcc_gc_threshold must be -1 (disabled) or a positive integer"
                ),
            };

            connection.set_mvcc_gc_threshold(threshold)?;
            Ok(TransactionMode::None)
        }
        PragmaName::MvccGroupCommit => {
            connection.set_mvcc_group_commit(parse_pragma_enabled(&value))?;
            Ok(TransactionMode::None)
        }
        PragmaName::FtsMergeThreshold => {
            let threshold = match parse_signed_number(&value)? {
                Value::Numeric(Numeric::Integer(size)) if size >= 0 => size,
                _ => bail_parse_error!(
                    "fts_merge_threshold must be 0 (disabled) or a positive integer"
                ),
            };
            connection.set_fts_merge_threshold(threshold);
            Ok(TransactionMode::None)
        }
        PragmaName::ForeignKeys => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_foreign_keys_enabled(enabled);
            Ok(TransactionMode::None)
        }
        PragmaName::IAmADummy | PragmaName::RequireWhere => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_dml_require_where(enabled);
            Ok(TransactionMode::None)
        }
        PragmaName::CountChanges => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_count_changes(enabled);
            Ok(TransactionMode::None)
        }
        PragmaName::IgnoreCheckConstraints => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_check_constraints_ignored(enabled);
            Ok(TransactionMode::None)
        }
        #[cfg(target_vendor = "apple")]
        PragmaName::Fullfsync => {
            let enabled = parse_pragma_enabled(&value);
            let sync_type = if enabled {
                crate::io::FileSyncType::FullFsync
            } else {
                crate::io::FileSyncType::Fsync
            };
            connection.set_sync_type(sync_type);
            Ok(TransactionMode::None)
        }
        PragmaName::ListTypes => bail_parse_error!("list_types cannot be set"),
        PragmaName::TempStore => {
            use crate::TempStore;
            // Try to parse as a string first (default, file, memory)
            let temp_store = if let Expr::Literal(Literal::Numeric(n)) = &value {
                // Numeric value: 0, 1, or 2
                match n.as_str() {
                    "0" => TempStore::Default,
                    "1" => TempStore::File,
                    "2" => TempStore::Memory,
                    _ => bail_parse_error!("temp_store must be 0, 1, 2, DEFAULT, FILE, or MEMORY"),
                }
            } else {
                // Try as keyword/identifier: DEFAULT, FILE, MEMORY
                let name_bytes = match &value {
                    Expr::Literal(Literal::Keyword(name)) => name.as_bytes(),
                    Expr::Name(name) | Expr::Id(name) => name.as_str().as_bytes(),
                    Expr::Literal(Literal::String(s)) => s.as_bytes(),
                    _ => bail_parse_error!("temp_store must be 0, 1, 2, DEFAULT, FILE, or MEMORY"),
                };
                match_ignore_ascii_case!(match name_bytes {
                    b"DEFAULT" | b"0" => TempStore::Default,
                    b"FILE" | b"1" => TempStore::File,
                    b"MEMORY" | b"2" => TempStore::Memory,
                    _ => bail_parse_error!("temp_store must be 0, 1, 2, DEFAULT, FILE, or MEMORY"),
                })
            };
            // SQLite allows changing temp_store even after temp objects
            // exist: it closes the temp btree and drops everything
            // (`sqlite3BtreeClose` + `sqlite3ResetAllSchemasOfConnection`
            // in `pragma.c`). We mirror that: `set_temp_store` tears
            // down and re-initializes the temp pager.
            //
            // Changing inside an explicit transaction (BEGIN … COMMIT)
            // with active temp state is blocked because savepoint /
            // rollback bookkeeping would be inconsistent.
            if !connection.get_auto_commit() && connection.temp.database.read().is_some() {
                bail_parse_error!("temporary storage cannot be changed from within a transaction");
            }
            connection.set_temp_store(temp_store);
            Ok(TransactionMode::None)
        }
        PragmaName::VdbeTrace => {
            let enabled = parse_pragma_enabled(&value);
            connection.set_vdbe_trace(enabled);
            Ok(TransactionMode::None)
        }

        PragmaName::FunctionList => query_pragma(
            PragmaName::FunctionList,
            resolver,
            Some(value),
            pager,
            connection,
            database_id,
            schema_was_explicit,
            program,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn query_pragma(
    pragma: PragmaName,
    resolver: &Resolver,
    value: Option<ast::Expr>,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
    database_id: usize,
    schema_was_explicit: bool,
    program: &mut ProgramBuilder,
) -> crate::Result<TransactionMode> {
    let schema = resolver.schema();
    let register = program.alloc_register();
    match pragma {
        PragmaName::ApplicationId => {
            program.emit_insn(Insn::ReadCookie {
                db: database_id,
                dest: register,
                cookie: Cookie::ApplicationId,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
            Ok(TransactionMode::Read)
        }
        PragmaName::BusyTimeout => {
            program.emit_int(connection.get_busy_timeout().as_millis() as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::CacheSize => {
            program.emit_int(connection.get_cache_size() as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::CacheSpill => {
            let spill_enabled = connection.get_pager().get_spill_enabled();
            program.emit_int(spill_enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::DatabaseList => {
            let base_reg = register;
            program.alloc_registers(2);

            // Get all databases (main + attached) and emit a row for each
            let all_databases = connection.list_all_databases();
            for (seq_number, name, file_path) in all_databases {
                // seq (sequence number)
                program.emit_int(seq_number as i64, base_reg);

                // name (alias)
                program.emit_string8(name, base_reg + 1);

                // file path
                program.emit_string8(file_path, base_reg + 2);

                program.emit_result_row(base_reg, 3);
            }

            let pragma = pragma_for(&pragma);
            for col_name in pragma.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::Encoding => {
            let encoding = pager
                .io
                .block(|| pager.with_header(|header| header.text_encoding))
                .unwrap_or_default()
                .to_string();
            program.emit_string8(encoding, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::JournalMode => {
            // Use the JournalMode opcode to get the current journal mode
            program.emit_insn(Insn::JournalMode {
                db: database_id,
                dest: register,
                new_mode: None,
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::LockingMode => {
            program.emit_string8("exclusive".to_string(), register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::FullColumnNames => {
            let enabled = connection.get_full_column_names();
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::ShortColumnNames => {
            let enabled = connection.get_short_column_names();
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::LegacyFileFormat | PragmaName::EmptyResultCallbacks => {
            Ok(TransactionMode::None)
        }
        PragmaName::WalCheckpoint => {
            // Checkpoint uses 3 registers: P1, P2, P3. Ref Insn::Checkpoint for more info.
            // Allocate two more here as one was allocated at the top.
            let passive_allowed = connection
                .mv_store_for_db(database_id)
                .is_none_or(|mv_store| mv_store.uses_passive_checkpoint());
            let mode = match value {
                Some(ast::Expr::Name(name)) => {
                    let mode_name = normalize_ident(name.as_str());
                    let mode = CheckpointMode::from_str(&mode_name).map_err(|e| {
                        LimboError::ParseError(format!("Unknown Checkpoint Mode: {e}"))
                    })?;
                    if matches!(mode, CheckpointMode::Passive { .. }) && !passive_allowed {
                        return Err(LimboError::InvalidArgument(
                            "PASSIVE checkpoint requires experimental_mvcc_passive_checkpoint"
                                .into(),
                        ));
                    }
                    mode
                }
                _ if passive_allowed => CheckpointMode::Passive {
                    upper_bound_inclusive: None,
                },
                _ => CheckpointMode::Truncate {
                    upper_bound_inclusive: None,
                },
            };

            program.alloc_registers(2);
            program.emit_insn(Insn::Checkpoint {
                database: database_id,
                checkpoint_mode: mode,
                dest: register,
            });
            program.emit_result_row(register, 3);
            program.add_pragma_result_column("busy".to_string());
            program.add_pragma_result_column("log".to_string());
            program.add_pragma_result_column("checkpointed".to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::ModuleList => {
            let modules = connection.get_syms_vtab_mods();
            for module in modules {
                program.emit_string8(module.to_string(), register);
                program.emit_result_row(register, 1);
            }

            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::FunctionList => {
            // 6 columns: name, builtin, type, enc, narg, flags
            let base_reg = register;
            program.alloc_registers(5);

            const SQLITE_DETERMINISTIC: i64 = 0x800;
            const SQLITE_INNOCUOUS: i64 = 0x200000;

            // Built-in functions
            for entry in Func::builtin_function_list() {
                let mut flags: i64 = 0;
                if entry.deterministic {
                    flags |= SQLITE_DETERMINISTIC;
                }
                flags |= SQLITE_INNOCUOUS;

                program.emit_string8(entry.name, base_reg);
                program.emit_int(1, base_reg + 1); // builtin = 1
                program.emit_string8(entry.func_type.to_string(), base_reg + 2);
                program.emit_string8("utf8".to_string(), base_reg + 3);
                program.emit_int(entry.narg as i64, base_reg + 4);
                program.emit_int(flags, base_reg + 5);
                program.emit_result_row(base_reg, 6);
            }

            // External (extension) functions
            for (name, is_agg, argc, deterministic) in connection.get_syms_functions() {
                let func_type = if is_agg { "a" } else { "s" };
                let mut flags = 0;
                if deterministic {
                    flags |= SQLITE_DETERMINISTIC;
                }
                program.emit_string8(name, base_reg);
                program.emit_int(0, base_reg + 1); // builtin = 0
                program.emit_string8(func_type.to_string(), base_reg + 2);
                program.emit_string8("utf8".to_string(), base_reg + 3);
                program.emit_int(argc as i64, base_reg + 4);
                program.emit_int(flags, base_reg + 5);
                program.emit_result_row(base_reg, 6);
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::PageCount => {
            program.emit_insn(Insn::PageCount {
                db: database_id,
                dest: register,
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::Read)
        }
        PragmaName::MaxPageCount => {
            program.emit_insn(Insn::MaxPgcnt {
                db: database_id,
                dest: register,
                new_max: 0, // 0 means just return current max
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::Read)
        }
        PragmaName::IndexInfo => {
            let index_name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 3 columns: seqno, cid, name
            program.alloc_registers(2);

            if let Some(index_name) = index_name {
                let index_database_id = resolve_index_pragma_database_id(
                    resolver,
                    database_id,
                    schema_was_explicit,
                    &index_name,
                )?;
                resolver.with_schema(index_database_id, |schema| {
                    let index = schema
                        .indexes
                        .values()
                        .flatten()
                        .find(|idx| idx.name.eq_ignore_ascii_case(&index_name));

                    if let Some(index) = index {
                        for (seqno, col) in index.columns.iter().enumerate() {
                            program.emit_int(seqno as i64, base_reg);
                            program.emit_int(col.pos_in_table as i64, base_reg + 1);
                            program.emit_string8(col.name.clone(), base_reg + 2);
                            program.emit_result_row(base_reg, 3);
                        }
                    }
                });
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::IndexXinfo => {
            let index_name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 6 columns: seqno, cid, name, desc, coll, key
            program.alloc_registers(5);

            if let Some(index_name) = index_name {
                let index_database_id = resolve_index_pragma_database_id(
                    resolver,
                    database_id,
                    schema_was_explicit,
                    &index_name,
                )?;
                resolver.with_schema(index_database_id, |schema| {
                    let index = schema
                        .indexes
                        .values()
                        .flatten()
                        .find(|idx| idx.name.eq_ignore_ascii_case(&index_name));

                    if let Some(index) = index {
                        for (seqno, col) in index.columns.iter().enumerate() {
                            let desc = matches!(col.order, ast::SortOrder::Desc);
                            let coll = col
                                .collation
                                .map(|c| c.to_string().to_uppercase())
                                .unwrap_or_else(|| "BINARY".to_string());

                            program.emit_int(seqno as i64, base_reg);
                            program.emit_int(col.pos_in_table as i64, base_reg + 1);
                            program.emit_string8(col.name.clone(), base_reg + 2);
                            program.emit_int(desc as i64, base_reg + 3);
                            program.emit_string8(coll, base_reg + 4);
                            program.emit_int(1, base_reg + 5); // key column
                            program.emit_result_row(base_reg, 6);
                        }

                        // Emit trailing rowid row if the index has one
                        if index.has_rowid {
                            let seqno = index.columns.len();
                            program.emit_int(seqno as i64, base_reg);
                            program.emit_int(-1, base_reg + 1);
                            program.emit_string8(String::new(), base_reg + 2);
                            program.emit_int(0, base_reg + 3);
                            program.emit_string8("BINARY".to_string(), base_reg + 4);
                            program.emit_int(0, base_reg + 5); // not a key column
                            program.emit_result_row(base_reg, 6);
                        }
                    }
                });
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::IndexList => {
            let table_name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 5 columns: seq, name, unique, origin, partial
            program.alloc_registers(4);

            if let Some(table_name) = table_name {
                let table_database_id = resolve_table_pragma_database_id(
                    resolver,
                    database_id,
                    schema_was_explicit,
                    &table_name,
                )?;
                resolver.with_schema(table_database_id, |schema| {
                    if let Some(table) = schema.get_table(&table_name) {
                        let pk_cols: Vec<String> = table
                            .btree()
                            .map(|bt| {
                                bt.primary_key_columns
                                    .iter()
                                    .map(|(name, _)| name.clone())
                                    .collect()
                            })
                            .unwrap_or_default();

                        for (seq, index) in schema.get_indices(&table_name).enumerate() {
                            let origin = if index.name.starts_with("sqlite_autoindex_") {
                                let idx_cols: Vec<&str> =
                                    index.columns.iter().map(|c| c.name.as_str()).collect();
                                if idx_cols.len() == pk_cols.len()
                                    && idx_cols
                                        .iter()
                                        .zip(pk_cols.iter())
                                        .all(|(a, b)| a.eq_ignore_ascii_case(b))
                                {
                                    "pk"
                                } else {
                                    "u"
                                }
                            } else {
                                "c"
                            };

                            program.emit_int(seq as i64, base_reg);
                            program.emit_string8(index.name.clone(), base_reg + 1);
                            program.emit_int(index.unique as i64, base_reg + 2);
                            program.emit_string8(origin.to_string(), base_reg + 3);
                            program.emit_int(index.where_clause.is_some() as i64, base_reg + 4);
                            program.emit_result_row(base_reg, 5);
                        }
                    }
                });
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::ForeignKeyList => {
            let table_name = match value {
                Some(ast::Expr::Name(name)) => Some(name),
                _ => None,
            };

            let base_reg = register;
            // 8 columns: id, seq, table, from, to, on_update, on_delete, match
            program.alloc_registers(7);

            if let Some(table_name) = table_name {
                let table_name = table_name.as_str();
                let table_database_id = resolve_table_pragma_database_id(
                    resolver,
                    database_id,
                    schema_was_explicit,
                    table_name,
                )?;
                resolver.with_schema(table_database_id, |schema| {
                    let Some(table) = schema.get_table(table_name).and_then(|table| table.btree())
                    else {
                        return;
                    };

                    let mut foreign_keys = table.foreign_keys.iter().collect::<Vec<_>>();
                    foreign_keys.sort_by_key(|fk| std::cmp::Reverse(fk.decl_order));

                    for (id, fk) in foreign_keys.into_iter().enumerate() {
                        turso_debug_assert!(
                            fk.parent_columns.is_empty()
                                || fk.parent_columns.len() == fk.child_columns.len()
                        );
                        for (idx, child_column) in fk.child_columns.iter().enumerate() {
                            let parent_column = fk
                                .parent_columns
                                .get(idx)
                                .map(String::as_str)
                                .unwrap_or_default();

                            program.emit_int(id as i64, base_reg);
                            program.emit_int(idx as i64, base_reg + 1);
                            program.emit_string8(fk.parent_table.clone(), base_reg + 2);
                            program.emit_string8(child_column.clone(), base_reg + 3);
                            program.emit_string8(parent_column.to_string(), base_reg + 4);
                            program.emit_string8(
                                foreign_key_action_name(fk.on_update).to_string(),
                                base_reg + 5,
                            );
                            program.emit_string8(
                                foreign_key_action_name(fk.on_delete).to_string(),
                                base_reg + 6,
                            );
                            program.emit_string8("NONE".to_string(), base_reg + 7);
                            program.emit_result_row(base_reg, 8);
                        }
                    }
                });
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::TableList => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // 6 columns: schema, name, type, ncol, wr, strict
            program.alloc_registers(5);

            let database_ids = if schema_was_explicit {
                [database_id].into_iter().try_collect::<BitSet>()?
            } else {
                visible_database_ids_for_table_list(connection.as_ref())?
            };
            for current_database_id in &database_ids {
                let database_name = connection
                    .get_database_name_by_index(current_database_id)
                    .unwrap_or_else(|| "main".to_string());
                resolver.with_schema(current_database_id, |schema| {
                    emit_table_list_rows_for_schema(
                        program,
                        schema,
                        current_database_id,
                        &database_name,
                        base_reg,
                        name.as_deref(),
                    )
                });
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::TableInfo => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // we need 6 registers, but first register was allocated at the beginning  of the "query_pragma" function
            program.alloc_registers(5);
            if let Some(name) = name {
                let table_database_id = resolve_table_pragma_database_id(
                    resolver,
                    database_id,
                    schema_was_explicit,
                    &name,
                )?;
                let lookup_name = normalize_table_pragma_lookup_name(table_database_id, &name);
                resolver.with_schema(table_database_id, |db_schema| {
                    if let Some(table) = db_schema.get_table(&lookup_name) {
                        let primary_key_columns = match table.as_ref() {
                            Table::BTree(bt) => Some(bt.primary_key_columns.as_slice()),
                            _ => None,
                        };
                        emit_columns_for_table_info(
                            program,
                            table.columns(),
                            primary_key_columns,
                            base_reg,
                            false,
                        );
                    } else if let Some(view_mutex) = db_schema.get_materialized_view(&lookup_name) {
                        let view = view_mutex.lock();
                        let flat_columns = view.column_schema.flat_columns();
                        emit_columns_for_table_info(
                            program,
                            &flat_columns,
                            // Materialized views have btree storage (implicit rowid) but no
                            // declared PRIMARY KEY on their output columns; pk is always 0.
                            None,
                            base_reg,
                            false,
                        );
                    } else if let Some(view) = db_schema.get_view(&lookup_name) {
                        emit_columns_for_table_info(
                            program,
                            &view.columns,
                            // Views are query definitions, not tables; pk is always 0.
                            None,
                            base_reg,
                            false,
                        );
                    }
                });
            }
            let col_names = ["cid", "name", "type", "notnull", "dflt_value", "pk"];
            for name in col_names {
                program.add_pragma_result_column(name.into());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::TableXinfo => {
            let name = match value {
                Some(ast::Expr::Name(name)) => Some(normalize_ident(name.as_str())),
                _ => None,
            };

            let base_reg = register;
            // we need 7 registers, but first register was allocated at the beginning  of the "query_pragma" function
            program.alloc_registers(6);
            if let Some(name) = name {
                let table_database_id = resolve_table_pragma_database_id(
                    resolver,
                    database_id,
                    schema_was_explicit,
                    &name,
                )?;
                let lookup_name = normalize_table_pragma_lookup_name(table_database_id, &name);
                resolver.with_schema(table_database_id, |db_schema| {
                    if let Some(table) = db_schema.get_table(&lookup_name) {
                        let primary_key_columns = match table.as_ref() {
                            Table::BTree(bt) => Some(bt.primary_key_columns.as_slice()),
                            _ => None,
                        };
                        emit_columns_for_table_info(
                            program,
                            table.columns(),
                            primary_key_columns,
                            base_reg,
                            true,
                        );
                    } else if let Some(view_mutex) = db_schema.get_materialized_view(&lookup_name) {
                        let view = view_mutex.lock();
                        let flat_columns = view.column_schema.flat_columns();
                        emit_columns_for_table_info(
                            program,
                            &flat_columns,
                            // Materialized views have btree storage (implicit rowid) but no
                            // declared PRIMARY KEY on their output columns; pk is always 0.
                            None,
                            base_reg,
                            true,
                        );
                    } else if let Some(view) = db_schema.get_view(&lookup_name) {
                        emit_columns_for_table_info(
                            program,
                            &view.columns,
                            // Views are query definitions, not tables; pk is always 0.
                            None,
                            base_reg,
                            true,
                        );
                    }
                });
            }
            let col_names = [
                "cid",
                "name",
                "type",
                "notnull",
                "dflt_value",
                "pk",
                "hidden",
            ];
            for name in col_names {
                program.add_pragma_result_column(name.into());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::UserVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: database_id,
                dest: register,
                cookie: Cookie::UserVersion,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
            Ok(TransactionMode::Read)
        }
        PragmaName::SchemaVersion => {
            program.emit_insn(Insn::ReadCookie {
                db: database_id,
                dest: register,
                cookie: Cookie::SchemaVersion,
            });
            program.add_pragma_result_column(pragma.to_string());
            program.emit_result_row(register, 1);
            Ok(TransactionMode::Read)
        }
        PragmaName::PageSize => {
            program.emit_int(
                pager
                    .io
                    .block(|| pager.with_header(|header| header.page_size.get()))
                    .unwrap_or_else(|_| connection.get_page_size().get()) as i64,
                register,
            );
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::AutoVacuum => {
            let auto_vacuum_mode = pager.get_auto_vacuum_mode();
            let auto_vacuum_mode_i64: i64 = match auto_vacuum_mode {
                AutoVacuumMode::None => 0,
                AutoVacuumMode::Full => 1,
                AutoVacuumMode::Incremental => 2,
            };
            let register = program.alloc_register();
            program.emit_insn(Insn::Int64 {
                _p1: 0,
                out_reg: register,
                _p3: 0,
                value: auto_vacuum_mode_i64,
            });
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::IntegrityCheck => {
            let max_errors = parse_max_errors_from_value(&value);
            translate_integrity_check(
                program,
                resolver,
                database_id,
                max_errors,
                connection.as_ref(),
            )?;
            Ok(TransactionMode::Read)
        }
        PragmaName::QuickCheck => {
            let max_errors = parse_max_errors_from_value(&value);
            translate_quick_check(
                program,
                resolver,
                database_id,
                max_errors,
                connection.as_ref(),
            )?;
            Ok(TransactionMode::Read)
        }
        PragmaName::CaptureDataChangesConn | PragmaName::UnstableCaptureDataChangesConn => {
            let pragma = pragma_for(&pragma);
            let second_column = program.alloc_register();
            let third_column = program.alloc_register();
            let opts = connection.get_capture_data_changes_info();
            match opts.as_ref() {
                Some(info) => {
                    program.emit_string8(info.mode_name().to_string(), register);
                    program.emit_string8(info.table.clone(), second_column);
                    match &info.version {
                        Some(v) => program.emit_string8(v.to_string(), third_column),
                        None => program.emit_null(third_column, None),
                    }
                }
                None => {
                    program.emit_string8("off".to_string(), register);
                    program.emit_null(second_column, None);
                    program.emit_null(third_column, None);
                }
            }
            program.emit_result_row(register, 3);
            program.add_pragma_result_column(pragma.columns[0].to_string());
            program.add_pragma_result_column(pragma.columns[1].to_string());
            program.add_pragma_result_column(pragma.columns[2].to_string());
            Ok(TransactionMode::Read)
        }
        PragmaName::QueryOnly => {
            if let Some(value_expr) = value {
                let is_query_only = match value_expr {
                    ast::Expr::Literal(Literal::Numeric(i)) => i
                        .parse::<i64>()
                        .map(|v| v != 0)
                        .or_else(|_| i.parse::<f64>().map(|v| v != 0.0))
                        .map_err(|_| {
                            LimboError::ParseError(format!(
                                "Invalid numeric value for PRAGMA query_only: {i}"
                            ))
                        })?,
                    ast::Expr::Literal(Literal::String(..)) | ast::Expr::Name(..) => {
                        let s = match &value_expr {
                            ast::Expr::Literal(Literal::String(s)) => s.as_bytes(),
                            ast::Expr::Name(n) => n.as_str().as_bytes(),
                            _ => unreachable!(),
                        };
                        match_ignore_ascii_case!(match s {
                            b"1" | b"on" | b"true" => true,
                            _ => false,
                        })
                    }
                    _ => {
                        return Err(LimboError::ParseError(format!(
                            "Invalid value for PRAGMA query_only: {value_expr:?}"
                        )));
                    }
                };
                connection.set_query_only(is_query_only);
                return Ok(TransactionMode::None);
            };

            let register = program.alloc_register();
            let is_query_only = connection.get_query_only();
            program.emit_int(is_query_only as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());

            Ok(TransactionMode::None)
        }
        PragmaName::VdbeTrace => Ok(TransactionMode::None),
        PragmaName::FreelistCount => {
            let value = pager.freepage_list();
            let register = program.alloc_register();
            program.emit_int(value as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::EncryptionKey => {
            let msg = {
                if connection.encryption_key.read().is_some() {
                    "encryption key is set for this session"
                } else {
                    "encryption key is not set for this session"
                }
            };
            let register = program.alloc_register();
            program.emit_string8(msg.to_string(), register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::EncryptionCipher => {
            if let Some(cipher) = connection.get_encryption_cipher_mode() {
                let register = program.alloc_register();
                program.emit_string8(cipher.to_string(), register);
                program.emit_result_row(register, 1);
                program.add_pragma_result_column(pragma.to_string());
            }
            Ok(TransactionMode::None)
        }
        PragmaName::Synchronous => {
            let mode = connection.get_sync_mode_for_database(database_id)?;
            let register = program.alloc_register();
            program.emit_int(mode as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::DataSyncRetry => {
            let retry_enabled = connection.get_data_sync_retry();
            let register = program.alloc_register();
            program.emit_int(retry_enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::MvccCheckpointThreshold => {
            let threshold = connection.mvcc_checkpoint_threshold()?;
            let register = program.alloc_register();
            program.emit_int(threshold, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::MvccGcThreshold => {
            let threshold = connection.mvcc_gc_threshold()?;
            let register = program.alloc_register();
            program.emit_int(threshold, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::MvccGroupCommit => {
            let enabled = connection.mvcc_group_commit()?;
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::FtsMergeThreshold => {
            let threshold = connection.get_fts_merge_threshold();
            let register = program.alloc_register();
            program.emit_int(threshold, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::ForeignKeys => {
            let enabled = connection.foreign_keys_enabled();
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::IAmADummy | PragmaName::RequireWhere => {
            let register = program.alloc_register();
            let enabled = connection.get_dml_require_where();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::CountChanges => {
            let register = program.alloc_register();
            let enabled = connection.get_count_changes();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::IgnoreCheckConstraints => {
            let ignored = connection.check_constraints_ignored();
            let register = program.alloc_register();
            program.emit_int(ignored as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        #[cfg(target_vendor = "apple")]
        PragmaName::Fullfsync => {
            let enabled = connection.get_sync_type() == crate::io::FileSyncType::FullFsync;
            let register = program.alloc_register();
            program.emit_int(enabled as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::TempStore => {
            let temp_store = connection.get_temp_store();
            let register = program.alloc_register();
            program.emit_int(temp_store as i64, register);
            program.emit_result_row(register, 1);
            program.add_pragma_result_column(pragma.to_string());
            Ok(TransactionMode::None)
        }
        PragmaName::ListTypes => {
            let base_reg = register;
            program.alloc_registers(5); // 6 total (1 already allocated)

            // Built-in types: NULL parent, encode, decode, default, operators
            for builtin in &["INTEGER", "REAL", "TEXT", "BLOB", "ANY"] {
                program.emit_string8(builtin.to_string(), base_reg);
                program.emit_null(base_reg + 1, None);
                program.emit_null(base_reg + 2, None);
                program.emit_null(base_reg + 3, None);
                program.emit_null(base_reg + 4, None);
                program.emit_null(base_reg + 5, None);
                program.emit_result_row(base_reg, 6);
            }

            // Custom types from the type registry are only shown when strict mode is enabled
            // Custom types are always shown since strict mode is always enabled
            {
                // Skip aliases where key != canonical name
                let mut type_names: Vec<_> = schema
                    .type_registry
                    .iter()
                    .filter(|(key, td)| *key == &td.name.to_lowercase())
                    .map(|(key, _)| key)
                    .collect();
                type_names.sort();
                for type_name in type_names {
                    let type_def = &schema.type_registry[type_name];
                    let display_name = if type_def.params().is_empty() {
                        type_def.name.clone()
                    } else {
                        let params: Vec<String> = type_def
                            .params()
                            .iter()
                            .map(|p| match &p.ty {
                                Some(ty) => format!("{} {}", p.name, ty),
                                None => p.name.clone(),
                            })
                            .collect();
                        format!("{}({})", type_def.name, params.join(", "))
                    };
                    program.emit_string8(display_name, base_reg);
                    program.emit_string8(type_def.base().to_string(), base_reg + 1);
                    if let Some(expr) = type_def.encode() {
                        program.emit_string8(expr.to_string(), base_reg + 2);
                    } else {
                        program.emit_null(base_reg + 2, None);
                    }
                    if let Some(expr) = type_def.decode() {
                        program.emit_string8(expr.to_string(), base_reg + 3);
                    } else {
                        program.emit_null(base_reg + 3, None);
                    }
                    if let Some(expr) = type_def.default_expr() {
                        program.emit_string8(expr.to_string(), base_reg + 4);
                    } else {
                        program.emit_null(base_reg + 4, None);
                    }
                    if type_def.operators().is_empty() {
                        program.emit_null(base_reg + 5, None);
                    } else {
                        let ops: Vec<String> = type_def
                            .operators()
                            .iter()
                            .map(|op| match &op.func_name {
                                Some(f) => format!("'{}' {}", op.op, f),
                                None => format!("'{}'", op.op),
                            })
                            .collect();
                        program.emit_string8(ops.join(", "), base_reg + 5);
                    }
                    program.emit_result_row(base_reg, 6);
                }
            }

            let pragma_meta = pragma_for(&pragma);
            for col_name in pragma_meta.columns.iter() {
                program.add_pragma_result_column(col_name.to_string());
            }
            Ok(TransactionMode::None)
        }
    }
}

/// 0-based index of `column` within `primary_key_columns`, if present.
fn column_pk_index(
    column: &crate::schema::Column,
    primary_key_columns: &[(String, turso_parser::ast::SortOrder)],
) -> Option<usize> {
    let name = column.name.as_deref()?;
    primary_key_columns
        .iter()
        .position(|(pk_name, _)| name.eq_ignore_ascii_case(pk_name))
}

/// Helper function to emit column information for PRAGMA table_info
/// Used by both tables and views since they now have the same column emission logic
fn emit_columns_for_table_info(
    program: &mut ProgramBuilder,
    columns: &[crate::schema::Column],
    primary_key_columns: Option<&[(String, turso_parser::ast::SortOrder)]>,
    base_reg: usize,
    extended: bool,
) {
    // According to the SQLite documentation: "The 'cid' column should not be taken to
    // mean more than 'rank within the current result set'."
    // Therefore, we enumerate only after filtering out hidden columns (if extended set to false).
    let mut cid = 0;
    for column in columns.iter() {
        // Determine column type which will be used for filtering in table_info pragma or as "hidden" column for table_xinfo pragma.
        //
        // SQLite docs about table_xinfo:
        // > The output has the same columns as for PRAGMA table_info plus a column, "hidden",
        // > whose value signifies a normal column (0), a dynamic or stored generated column (2 or 3),
        // > or a hidden column in a virtual table (1). The rows for which this field is non-zero are those omitted for PRAGMA table_info.
        //
        // (see https://sqlite.org/pragma.html#pragma_table_xinfo)
        let column_type = if column.hidden() {
            // hidden column in virtual table
            1
        } else if column.is_virtual_generated() {
            2
        } else {
            // normal column
            0
        };

        if !extended && column_type != 0 {
            // This pragma (table_info) does not show information about generated columns or hidden columns.
            continue;
        }

        // cid
        program.emit_int(cid as i64, base_reg);
        cid += 1;

        // name
        program.emit_string8(column.name.clone().unwrap_or_default(), base_reg + 1);

        // type
        program.emit_string8(column.ty_str.clone(), base_reg + 2);

        // notnull
        program.emit_bool(column.notnull(), base_reg + 3);

        // dflt_value
        match &column.default {
            None => {
                program.emit_null(base_reg + 4, None);
            }
            Some(expr) => {
                program.emit_string8(expr.to_string(), base_reg + 4);
            }
        }

        // pk — 1-based position within the primary key, or 0 if not part of the key.
        // B-tree tables use composite key order from schema; virtual tables fall back to
        // the per-column primary key flag (always 0 or 1 for vtabs).
        let pk = match primary_key_columns.and_then(|pk_cols| column_pk_index(column, pk_cols)) {
            Some(index) => (index + 1) as i64,
            None => {
                if column.primary_key() {
                    1
                } else {
                    0
                }
            }
        };
        program.emit_int(pk, base_reg + 5);

        if extended {
            program.emit_int(column_type, base_reg + 6);
        }

        program.emit_result_row(base_reg, 6 + if extended { 1 } else { 0 });
    }
}

fn update_cache_size(
    value: i64,
    pager: Arc<Pager>,
    connection: Arc<crate::Connection>,
) -> crate::Result<()> {
    let mut cache_size_unformatted: i64 = value;

    let mut cache_size = if cache_size_unformatted < 0 {
        let kb = cache_size_unformatted
            .checked_abs()
            .unwrap_or(i64::MAX)
            .saturating_mul(1024);
        let page_size = pager
            .io
            .block(|| pager.with_header(|header| header.page_size))
            .unwrap_or_default()
            .get() as i64;
        if page_size == 0 {
            turso_soft_unreachable!("Page size cannot be zero");
            return Err(LimboError::InternalError(
                "Page size cannot be zero".to_string(),
            ));
        }
        kb / page_size
    } else {
        value
    };

    if cache_size > CacheSize::MAX_SAFE {
        cache_size = 0;
        cache_size_unformatted = 0;
    }

    if cache_size < 0 {
        cache_size = 0;
        cache_size_unformatted = 0;
    }

    let final_cache_size = if cache_size < CacheSize::MIN {
        cache_size_unformatted = CacheSize::MIN;
        CacheSize::MIN
    } else {
        cache_size
    };

    connection.set_cache_size(cache_size_unformatted as i32);

    pager
        .change_page_cache_size(final_cache_size as usize)
        .map_err(|e| LimboError::InternalError(format!("Failed to update page cache size: {e}")))?;

    Ok(())
}

fn update_page_size(connection: Arc<crate::Connection>, page_size: u32) -> crate::Result<()> {
    connection.reset_page_size(page_size)?;
    Ok(())
}
