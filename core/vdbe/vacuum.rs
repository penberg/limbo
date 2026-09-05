use crate::types::IOResultOr;
use rustc_hash::FxHashMap;
use std::sync::{atomic::Ordering, Arc};

use crate::error::LimboError;
use crate::io::{Buffer, Completion, CompletionGroup, WriteBatch as IOWriteBatch};
use crate::schema::{BTreeTable, Schema, TypeDef};
use crate::storage::pager::{AutoVacuumMode, Page, PageRef, Pager};
use crate::storage::sqlite3_ondisk::{
    CacheSize, DatabaseHeader, PageSize, RawVersion, TextEncoding, WAL_FRAME_HEADER_SIZE,
};
use crate::types::{IOCompletions, IOResult};
use crate::util::IOExt;
use crate::vdbe::Row;
use crate::Result;
use crate::{
    Connection, Database, DatabaseOpts, EncryptionKey, EncryptionOpts, MvStore, OpenFlags, SyncMode,
};
use turso_macros::{turso_assert, turso_assert_eq};

/// A representation of a row from `sqlite_schema`.
///
/// Carries `rootpage` so we can distinguish storage-backed tables/indexes
/// (rootpage != 0) from virtual tables, custom index-method indexes, views,
/// and triggers (rootpage = 0).
#[derive(Debug)]
pub(crate) struct SchemaEntry {
    pub entry_type: SchemaEntryType,
    pub name: String,
    /// `sqlite_schema.tbl_name`: for indexes and triggers, this is the table
    /// the object belongs to; for tables and views it usually matches `name`.
    pub tbl_name: String,
    pub rootpage: i64,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaEntryType {
    Table,
    Index,
    Trigger,
    View,
}

impl SchemaEntryType {
    pub fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "table" => Ok(Self::Table),
            "index" => Ok(Self::Index),
            "trigger" => Ok(Self::Trigger),
            "view" => Ok(Self::View),
            other => Err(crate::error::LimboError::Corrupt(format!(
                "unexpected sqlite_schema type: {other}"
            ))),
        }
    }
}

impl SchemaEntry {
    /// Parse from a sqlite_schema row: (type, name, tbl_name, rootpage, sql)
    pub fn from_row(row: &Row) -> crate::Result<Self> {
        let entry_type = SchemaEntryType::from_str(row.get::<&str>(0)?)?;
        Ok(Self {
            entry_type,
            name: row.get::<&str>(1)?.to_string(),
            tbl_name: row.get::<&str>(2)?.to_string(),
            rootpage: row.get::<i64>(3)?,
            sql: row.get::<&str>(4)?.to_string(),
        })
    }

    /// Whether this entry represents a storage-backed object (table or index
    /// with rootpage != 0). MVCC can use negative rootpage values before
    /// checkpointing, so this checks `!= 0` rather than `> 0`.
    pub fn is_storage_backed(&self) -> bool {
        self.rootpage != 0
    }

    pub fn is_sqlite_sequence(&self) -> bool {
        self.name == "sqlite_sequence"
    }

    /// True for backing tables of the implicit sequences created by
    /// `CREATE TABLE ... AUTOINCREMENT` — i.e. the table is named
    /// `__turso_internal_seq___turso_internal_autoincrement_<table>`.
    /// User-created sequences (`CREATE SEQUENCE foo` →
    /// `__turso_internal_seq_foo`) do NOT match.
    ///
    /// VACUUM uses this to skip the standalone `CREATE TABLE` replay
    /// for autoincrement backing tables only: the user's
    /// `CREATE TABLE ... AUTOINCREMENT` is replayed separately and
    /// auto-recreates the backing table from `CREATE TABLE` bytecode,
    /// so replaying the standalone entry too would trip
    /// "table already exists". User-sequence backing tables, on the
    /// other hand, are the ONLY persistent representation of
    /// `CREATE SEQUENCE` — they must be replayed.
    pub fn is_autoinc_sequence_backing_table(&self) -> bool {
        let Some(seq_name) = self
            .name
            .strip_prefix(crate::schema::SEQ_BACKING_TABLE_PREFIX)
        else {
            return false;
        };
        seq_name.starts_with(crate::schema::AUTOINCREMENT_SEQ_PREFIX)
    }
}

/// Split rowid-ordered schema entries into the replay phases used by VACUUM.
/// Returns indices into `entries` for `(tables_to_create, tables_to_copy,
/// indexes_to_create, post_data_entries)`.
pub(crate) fn classify_schema_entries(
    entries: &[SchemaEntry],
) -> (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>) {
    let mut tables_to_create: Vec<usize> = Vec::new();
    let mut tables_to_copy: Vec<usize> = Vec::new();
    let mut indexes_to_create: Vec<usize> = Vec::new();
    let mut post_data_entries: Vec<usize> = Vec::new();

    for (idx, entry) in entries.iter().enumerate() {
        match entry.entry_type {
            SchemaEntryType::Table if entry.is_storage_backed() => {
                // Skip sqlite_sequence and AUTOINCREMENT implicit sequence
                // backing tables in the schema creation phase: both are
                // auto-created as a side effect of replaying the user's
                // CREATE TABLE ... AUTOINCREMENT, so a standalone CREATE
                // TABLE replay would trip "table already exists".
                //
                // User-created sequence backing tables (`CREATE SEQUENCE
                // foo` → `__turso_internal_seq_foo`) DO need creation
                // replay — that backing table IS the persistent
                // representation of the sequence, and nothing else
                // creates it in the target. Excluding them used to abort
                // VACUUM with `no such table: "__turso_internal_seq_foo"`
                // during the copy phase.
                if !entry.is_sqlite_sequence() && !entry.is_autoinc_sequence_backing_table() {
                    tables_to_create.push(idx);
                }
                // All storage-backed tables get their data copied, including
                // sqlite_stat1 and other internal storage-backed tables.
                // sqlite_sequence data copy is handled specially by the caller
                // (only if the target materialized it).
                tables_to_copy.push(idx);
            }
            SchemaEntryType::Index if entry.is_storage_backed() => {
                // Storage-backed index (rootpage != 0). Includes both normal
                // user-defined indexes and backing_btree indexes for custom index
                // methods. The caller filters out backing_btree indexes since
                // those are recreated by the parent index method in the post-data
                // phase.
                indexes_to_create.push(idx);
            }
            SchemaEntryType::Trigger | SchemaEntryType::View => {
                // Triggers and views are replayed after data copy to avoid
                // triggers firing during the copy phase.
                post_data_entries.push(idx);
            }
            SchemaEntryType::Table => {
                // Virtual tables (rootpage = 0, type = table) land here.
                // Replayed after data copy alongside triggers and views.
                turso_assert!(
                    !entry.is_storage_backed(),
                    "unexpected storage-backed table (rootpage = 0): {entry.name}"
                );
                post_data_entries.push(idx);
            }
            SchemaEntryType::Index => {
                // Custom index-method indexes (FTS, vector, etc.) have rootpage = 0
                // in sqlite_schema because their storage is managed by the index
                // method, not a B-tree.
                // Replayed after data copy.
                post_data_entries.push(idx);
            }
        }
    }

    (
        tables_to_create,
        tables_to_copy,
        indexes_to_create,
        post_data_entries,
    )
}

/// Target database feature flags needed for schema replay during a vacuum build.
pub(crate) fn vacuum_target_opts_from_source(source_db: &Database) -> DatabaseOpts {
    DatabaseOpts::new()
        .with_views(source_db.experimental_views_enabled())
        .with_index_method(source_db.experimental_index_method_enabled())
        .with_custom_types(source_db.experimental_custom_types_enabled())
        .with_encryption(source_db.experimental_encryption_enabled())
        .with_autovacuum(source_db.experimental_autovacuum_enabled())
        .with_attach(source_db.experimental_attach_enabled())
        .with_generated_columns(source_db.experimental_generated_columns_enabled())
        .with_without_rowid(source_db.experimental_without_rowid_enabled())
}

pub(crate) fn reject_unsupported_vacuum_auto_vacuum_mode(mode: AutoVacuumMode) -> Result<()> {
    if matches!(mode, AutoVacuumMode::Incremental) {
        return Err(LimboError::InternalError(
            "Incremental auto-vacuum is not supported".to_string(),
        ));
    }
    Ok(())
}

/// Database header metadata that the target build must finalize before commit.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VacuumDbHeaderMeta {
    read_version: RawVersion,
    write_version: RawVersion,
    schema_cookie: u32,
    default_page_cache_size: CacheSize,
    text_encoding: TextEncoding,
    user_version: i32,
    application_id: i32,
}

impl VacuumDbHeaderMeta {
    pub(crate) fn from_source_header(source: &DatabaseHeader) -> Self {
        Self {
            read_version: source.read_version,
            write_version: source.write_version,
            schema_cookie: source.schema_cookie.get().wrapping_add(1),
            default_page_cache_size: source.default_page_cache_size,
            text_encoding: source.text_encoding,
            user_version: source.user_version.get(),
            application_id: source.application_id.get(),
        }
    }

    fn apply_to(self, header: &mut DatabaseHeader) {
        header.read_version = self.read_version;
        header.write_version = self.write_version;
        header.schema_cookie = self.schema_cookie.into();
        header.default_page_cache_size = self.default_page_cache_size;
        header.text_encoding = self.text_encoding;
        header.user_version = self.user_version.into();
        header.application_id = self.application_id.into();
    }
}

/// File-backed internal temp database used by in-place `VACUUM`.
///
/// The temp directory is dropped after the connection and database handles so
/// host files can be closed before the directory cleanup runs.
pub(crate) struct VacuumTempDb {
    pub conn: Arc<Connection>,
    _db: Arc<Database>,
    #[cfg(test)]
    path: String,
    #[cfg(not(target_family = "wasm"))]
    _temp_dir: tempfile::TempDir,
}

#[cfg(not(target_family = "wasm"))]
fn vacuum_temp_db_encryption(
    source_conn: &Arc<Connection>,
) -> Result<(Option<EncryptionOpts>, Option<EncryptionKey>)> {
    let Some(cipher_mode) = source_conn.get_encryption_cipher_mode() else {
        return Ok((None, None));
    };
    let encryption_key = source_conn.encryption_key.read().clone().ok_or_else(|| {
        LimboError::InternalError(
            "encrypted in-place VACUUM temp database requires source encryption key".to_string(),
        )
    })?;
    let encryption_opts = EncryptionOpts {
        cipher: cipher_mode.to_string(),
        hexkey: hex::encode(encryption_key.as_slice()),
    };
    Ok((Some(encryption_opts), Some(encryption_key)))
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn open_vacuum_temp_db(
    source_conn: &Arc<Connection>,
    source_db: &Arc<Database>,
    page_size: u32,
    reserved_space: u8,
) -> Result<VacuumTempDb> {
    let temp_dir = source_conn.create_tempdir()?;
    let source_db_name = std::path::Path::new(&source_db.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tursodb_vacuum_temp.db");
    // all temp files we will prefix with `etilqs_` as an homage to SQLite's lore
    let path = temp_dir.path().join(format!("etilqs_{source_db_name}"));
    let path = path
        .to_str()
        .ok_or_else(|| LimboError::InternalError("vacuum temp path is not valid UTF-8".into()))?
        .to_string();
    #[cfg(test)]
    let test_path = path.clone();

    let (encryption_opts, encryption_key) = vacuum_temp_db_encryption(source_conn)?;
    let page_codec = source_conn.pager.load().page_codec_external();
    let db = match &page_codec {
        Some(codec) => Database::open(
            source_db.io.clone(),
            &path,
            crate::OpenOptions::new(source_db.dialect())
                .flags(OpenFlags::Create)
                .db_opts(vacuum_target_opts_from_source(source_db))
                .encryption(encryption_opts)
                .page_codec(codec.clone()),
        )?,
        None => Database::open_file_with_flags(
            source_db.io.clone(),
            &path,
            OpenFlags::Create,
            vacuum_target_opts_from_source(source_db),
            encryption_opts,
            source_db.dialect(),
        )?,
    };
    let conn = match page_codec {
        Some(codec) => db.connect_with_page_codec(codec)?,
        None => db.connect_with_encryption(encryption_key)?,
    };
    conn.reset_page_size(page_size)?;
    conn.set_reserved_bytes(reserved_space)?;
    conn.wal_auto_actions_disable();

    Ok(VacuumTempDb {
        conn,
        _db: db,
        #[cfg(test)]
        path: test_path,
        _temp_dir: temp_dir,
    })
}

#[cfg(target_family = "wasm")]
pub(crate) fn open_vacuum_temp_db(
    _source_conn: &Arc<Connection>,
    _source_db: &Arc<Database>,
    _page_size: u32,
    _reserved_space: u8,
) -> Result<VacuumTempDb> {
    Err(LimboError::InternalError(
        "in-place VACUUM requires a file-backed internal temp database".to_string(),
    ))
}

fn finalize_vacuum_target_header(
    target_conn: &Arc<Connection>,
    header_meta: &VacuumDbHeaderMeta,
) -> crate::types::IOResultOr<()> {
    if let Some(mv_store) = target_conn.mv_store_for_db(crate::MAIN_DB_ID) {
        let tx_id = target_conn.get_mv_tx_id_for_db(crate::MAIN_DB_ID);
        return mv_store
            .with_header_mut(|header| header_meta.apply_to(header), tx_id.as_ref())
            .map(crate::IOResult::Done)
            .map_err(Into::into);
    }
    let pager = target_conn.pager.load();
    pager.with_header_mut(|header| header_meta.apply_to(header))
}

/// Finish a VACUUM INTO output database with durable on-disk state.
///
/// Target build runs with `synchronous=OFF` for speed, so its commit alone is
/// not the durability boundary. Before reporting success, VACUUM INTO forces a
/// final TRUNCATE checkpoint under `SyncMode::Full` so the destination's main
/// database file is self-contained and synced.
pub(crate) fn finalize_vacuum_into_output(target: &VacuumTargetBuildContext) -> Result<()> {
    target.target_conn.set_sync_mode(crate::SyncMode::Full);
    target
        .target_conn
        .checkpoint(crate::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })?;
    Ok(())
}

/// Copy functions, vtab modules, and index methods from one connection to
/// another so that schema replay on the target sees the same symbols as
/// the source.
pub(crate) fn mirror_symbols(source: &Connection, target: &Connection) {
    let source_syms = source.syms.read();
    let mut target_syms = target.syms.write();
    target_syms.functions.extend(
        source_syms
            .functions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    target_syms.vtab_modules.extend(
        source_syms
            .vtab_modules
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
    target_syms.index_methods.extend(
        source_syms
            .index_methods
            .iter()
            .map(|(k, v)| (k.clone(), v.clone())),
    );
}

/// Capture non-builtin custom type definitions from the source schema.
pub(crate) fn capture_custom_types(
    source: &Connection,
    db_id: usize,
) -> Vec<(String, Arc<TypeDef>)> {
    source.with_schema(db_id, |schema| {
        schema
            .type_registry
            .iter()
            .filter(|(_, td)| !td.is_builtin)
            .map(|(name, td)| (name.clone(), td.clone()))
            .collect()
    })
}

/// Configuration for building a compacted vacuum target database.
/// Callers must mirror source symbols (functions, vtab modules, index methods)
/// directly into the target connection before starting the state machine.
pub(crate) struct VacuumTargetBuildConfig {
    pub source_conn: Arc<Connection>,
    /// Escaped schema name for safe SQL interpolation (e.g. `"main"`).
    pub escaped_schema_name: String,
    /// Database index for schema lookups on the source connection.
    pub source_db_id: usize,
    /// Database header metadata to write before committing the target database.
    pub header_meta: VacuumDbHeaderMeta,
    /// Pre-captured source custom type definitions for STRICT table replay.
    pub source_custom_types: Vec<(String, Arc<TypeDef>)>,
    /// Whether the target database should be placed in MVCC journal mode.
    pub target_mvcc_enabled: bool,
    /// Auto-vacuum mode to use while rebuilding the target image.
    pub target_auto_vacuum_mode: AutoVacuumMode,
    /// Whether to copy the source MVCC metadata table into the target image.
    pub copy_mvcc_metadata_table: bool,
}

/// Context for the vacuum target build. Holds the target connection and all
/// intermediate state needed across async yields.
pub(crate) struct VacuumTargetBuildContext {
    target_conn: Arc<Connection>,
    phase: VacuumTargetBuildPhase,
    /// Typed schema entries collected from sqlite_schema, ordered by rowid.
    schema_entries: Vec<SchemaEntry>,
    /// Storage-backed tables to CREATE (excludes sqlite_sequence).
    tables_to_create: Vec<usize>,
    /// Storage-backed tables whose data to copy.
    tables_to_copy: Vec<usize>,
    /// User-defined secondary indexes to CREATE
    indexes_to_create: Vec<usize>,
    /// Triggers, views, and rootpage = 0 objects
    post_data_entries: Vec<usize>,
}

impl VacuumTargetBuildContext {
    pub fn new(target_conn: Arc<Connection>) -> Self {
        Self {
            target_conn,
            phase: VacuumTargetBuildPhase::Init,
            schema_entries: Vec::new(),
            tables_to_create: Vec::new(),
            tables_to_copy: Vec::new(),
            indexes_to_create: Vec::new(),
            post_data_entries: Vec::new(),
        }
    }

    pub(crate) fn cleanup_after_error(&mut self) -> Result<()> {
        self.phase = VacuumTargetBuildPhase::Done;
        if !self.target_conn.get_auto_commit() {
            // Target-build connections are disposable. On error we only need to
            // unwind connection-local transaction state before the caller drops
            // the target database handle.
            let pager = self.target_conn.pager.load();
            self.target_conn.rollback_manual_txn_cleanup(&pager, true);
        }
        Ok(())
    }
}

/// Phases for the vacuum target build state machine.
#[derive(Default)]
pub(crate) enum VacuumTargetBuildPhase {
    /// Mirror symbols/types, set perf flags, begin target tx, prepare schema query.
    #[default]
    Init,
    /// Step through schema query to collect rows
    CollectSchemaRows { schema_stmt: Box<crate::Statement> },
    /// Prepare CREATE TABLE statement on the target (idx into tables_to_create)
    PrepareCreateTable { idx: usize },
    /// Step through CREATE TABLE statement on the target (async)
    StepCreateTable {
        target_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Start copying a table's data
    StartCopyTable { table_idx: usize },
    /// Select rows from source table and insert into the target.
    CopyRows {
        select_stmt: Box<crate::Statement>,
        target_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Step through INSERT statement on the target.
    StepTargetInsert {
        select_stmt: Box<crate::Statement>,
        target_insert_stmt: Box<crate::Statement>,
        table_idx: usize,
    },
    /// Prepare CREATE INDEX statement on the target (idx into indexes_to_create)
    PrepareCreateIndex { idx: usize },
    /// Step through CREATE INDEX statement on the target.
    StepCreateIndex {
        target_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Prepare post-data schema objects (triggers, views, rootpage = 0 entries)
    PreparePostData { idx: usize },
    /// Step through post-data CREATE statement on the target.
    StepPostData {
        target_schema_stmt: Box<crate::Statement>,
        idx: usize,
    },
    /// Finalize database header metadata before committing the target database.
    FinalizeTargetHeader,
    /// Operation complete
    Done,
}

/// Build a compacted vacuum target database.
///
/// This is an async state machine implementation that yields on I/O operations.
/// The caller creates a target database with matching
/// page_size, reserved bytes, source feature flags, and schema-replay symbols.
///
/// It:
/// 1. Mirrors source symbols and custom types to the target
/// 2. Queries sqlite_schema for all schema objects including rootpage, ordered by rowid
/// 3. Creates storage-backed tables (rootpage != 0) in the target, excluding
///    sqlite_sequence (auto-created when AUTOINCREMENT tables are created)
/// 4. Copies data for all storage-backed tables, including sqlite_stat1 and other
///    internal storage-backed tables
/// 5. Creates user-defined secondary indexes after data copy for performance
///    (backing-btree indexes for custom index methods are excluded here)
/// 6. Creates triggers, views, and rootpage = 0 objects last (after data copy).
///    Custom index methods (FTS, vector) recreate and backfill their backing
///    indexes from the copied table data in this phase.
/// 7. Finalizes target database header metadata, then commits the target transaction.
pub(crate) fn vacuum_target_build_step(
    config: &VacuumTargetBuildConfig,
    state: &mut VacuumTargetBuildContext,
) -> crate::types::IOResultOr<()> {
    loop {
        let current_phase = std::mem::take(&mut state.phase);

        match current_phase {
            VacuumTargetBuildPhase::Init => {
                // Mirror source custom type definitions into the target schema
                // so that STRICT tables with custom type columns can resolve
                // those types during CREATE TABLE replay.
                if !config.source_custom_types.is_empty() {
                    state.target_conn.with_schema_mut(|target_schema| {
                        for (name, td) in &config.source_custom_types {
                            target_schema.type_registry.insert(name.clone(), td.clone());
                        }
                    })?;
                }

                // Auto-vacuum must be installed before MVCC bootstrap creates
                // internal tables, otherwise full auto-vacuum can reserve page
                // 2 as a ptrmap page after MVCC has already used it as a root.
                state
                    .target_conn
                    .pager
                    .load()
                    .persist_auto_vacuum_mode(config.target_auto_vacuum_mode)?;

                // Enable MVCC on targets that should persist as MVCC.
                if config.target_mvcc_enabled {
                    state.target_conn.execute("PRAGMA journal_mode = 'mvcc'")?;
                }

                // Performance optimizations for the target database (matches SQLite vacuum.c):
                // 1. Disable fsync - the target is a new output/temp database, if crash occurs we just delete it
                // 2. Disable foreign key checks - source data is already consistent
                // 3. Defer auto-checkpoint until the caller performs the final durable checkpoint
                // These match SQLite's vacuum.c optimizations (PAGER_SYNCHRONOUS_OFF, ~SQLITE_ForeignKeys)
                state.target_conn.set_sync_mode(crate::SyncMode::Off);
                state.target_conn.set_foreign_keys_enabled(false);
                state.target_conn.set_check_constraints_ignored(true);
                state.target_conn.wal_auto_actions_disable();

                // Wrap all operations in a single write transaction so helper
                // statements share one durable target build and cleanup can
                // roll it back reliably on error.
                state.target_conn.execute("BEGIN IMMEDIATE")?;

                // Query sqlite_schema with rootpage, ordered by rowid.
                // Decide whether schema replay should include the internal MVCC metadata table.
                // VACUUM INTO excludes it because an MVCC destination bootstraps that table
                // itself. In-place VACUUM includes it because the temp image is the future
                // physical source database and must preserve the table as-is.
                let escaped_schema_name = &config.escaped_schema_name;
                let schema_sql = if config.copy_mvcc_metadata_table {
                    format!(
                        "SELECT type, name, tbl_name, rootpage, sql \
                         FROM \"{escaped_schema_name}\".sqlite_schema \
                         WHERE sql IS NOT NULL ORDER BY rowid"
                    )
                } else {
                    format!(
                        "SELECT type, name, tbl_name, rootpage, sql \
                         FROM \"{escaped_schema_name}\".sqlite_schema \
                         WHERE sql IS NOT NULL AND name <> '{}' ORDER BY rowid",
                        crate::mvcc::database::MVCC_META_TABLE_NAME
                    )
                };
                let schema_stmt = config.source_conn.prepare_internal(schema_sql.as_str())?;

                state.phase = VacuumTargetBuildPhase::CollectSchemaRows {
                    schema_stmt: Box::new(schema_stmt),
                };
                continue;
            }

            VacuumTargetBuildPhase::CollectSchemaRows { mut schema_stmt } => {
                match schema_stmt.step()? {
                    crate::StepResult::Row => {
                        let row = schema_stmt
                            .row()
                            .expect("StepResult::Row but row() returned None");
                        state.schema_entries.push(SchemaEntry::from_row(row)?);
                        state.phase = VacuumTargetBuildPhase::CollectSchemaRows { schema_stmt };
                        continue;
                    }
                    crate::StepResult::Done => {
                        // Classify schema entries into replay phases using rootpage.
                        let (tables_create, tables_copy, indexes_create, post_data) =
                            classify_schema_entries(&state.schema_entries);
                        state.tables_to_create = tables_create;
                        state.tables_to_copy = tables_copy;
                        // Backing-btree indexes are implementation details of custom index
                        // methods. i.e. when custom indexes are created, they are created automatically
                        // The user-visible custom-index CREATE in post_data_entries
                        // recreates and backfills those backing indexes from the copied rows.
                        // for now, we will skip them
                        state.indexes_to_create = indexes_create
                            .into_iter()
                            .filter(|entry_ordinal| {
                                let entry = &state.schema_entries[*entry_ordinal];
                                !config
                                    .source_conn
                                    .with_schema(config.source_db_id, |schema| {
                                        schema
                                            .get_index(&entry.tbl_name, &entry.name)
                                            .is_some_and(|idx| idx.is_backing_btree_index())
                                    })
                            })
                            .collect();
                        state.post_data_entries = post_data;

                        state.phase = VacuumTargetBuildPhase::PrepareCreateTable { idx: 0 };
                        continue;
                    }
                    crate::StepResult::IO
                    | crate::StepResult::Yield
                    | crate::StepResult::Sleep { .. } => {
                        let io = schema_stmt
                            .take_io_completions()
                            .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                        state.phase = VacuumTargetBuildPhase::CollectSchemaRows { schema_stmt };
                        return Ok(crate::IOResult::IO(io));
                    }
                    crate::StepResult::Busy | crate::StepResult::Interrupt => {
                        return Err(LimboError::Busy.into());
                    }
                }
            }

            // Phase 1: Create storage-backed tables (rootpage != 0, type=table),
            // excluding sqlite_sequence (auto-created by AUTOINCREMENT tables).
            VacuumTargetBuildPhase::PrepareCreateTable { idx } => {
                let entries_len = state.tables_to_create.len();
                if idx >= entries_len {
                    // Done creating tables, start copying data
                    state.phase = VacuumTargetBuildPhase::StartCopyTable { table_idx: 0 };
                    continue;
                }

                let entry_ordinal = state.tables_to_create[idx];
                let entry = &state.schema_entries[entry_ordinal];
                let sql = table_sql_for_vacuum_replay(&state.target_conn, &entry.sql)?;

                // System tables (sqlite_stat1, __turso_internal_types, etc.) have
                // reserved name prefixes that translate_create_table rejects for
                // user SQL. Temporarily mark the target connection as nested during
                // prepare() so the reserved-name check is bypassed at compile
                // time. The guard is only for prepare: keeping it during step()
                // would make this CREATE TABLE look nested, so its Transaction
                // opcode would skip write setup.
                let is_system = crate::schema::is_system_table(&entry.name);
                if is_system {
                    state.target_conn.start_nested();
                }
                let target_stmt = state.target_conn.prepare(&sql);
                if is_system {
                    state.target_conn.end_nested();
                }
                let target_stmt = target_stmt?;
                state.phase = VacuumTargetBuildPhase::StepCreateTable {
                    target_schema_stmt: Box::new(target_stmt),
                    idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::StepCreateTable {
                mut target_schema_stmt,
                idx,
            } => match target_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE TABLE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.phase = VacuumTargetBuildPhase::PrepareCreateTable { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO
                | crate::StepResult::Yield
                | crate::StepResult::Sleep { .. } => {
                    let io = target_schema_stmt
                        .take_io_completions()
                        .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                    state.phase = VacuumTargetBuildPhase::StepCreateTable {
                        target_schema_stmt,
                        idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy.into());
                }
            },

            // Phase 2: Copy data for all storage-backed tables.
            // Column lists are derived from BTreeTable.columns in the schema,
            // not PRAGMA table_info, because table_info omits generated columns
            // while SELECT * includes them - causing a column count mismatch.
            VacuumTargetBuildPhase::StartCopyTable { table_idx } => {
                let tables_len = state.tables_to_copy.len();
                if table_idx >= tables_len {
                    // Done copying all tables, proceed to deferred indexes.
                    state.phase = VacuumTargetBuildPhase::PrepareCreateIndex { idx: 0 };
                    continue;
                }

                let entry_ordinal = state.tables_to_copy[table_idx];
                let entry = &state.schema_entries[entry_ordinal];
                let table_name = &entry.name;

                // sqlite_sequence: only copy data if the target has it
                // (auto-created when an AUTOINCREMENT table was created in
                // phase 1). If not present, skip — no AUTOINCREMENT tables
                // means no counters to preserve. The explicit copy is needed
                // because inserting rows with the `rowid` pseudo-column does
                // not update sqlite_sequence counters automatically.
                if entry.is_sqlite_sequence() {
                    let target_has_sequence = state
                        .target_conn
                        .schema
                        .read()
                        .get_btree_table(crate::schema::SQLITE_SEQUENCE_TABLE_NAME)
                        .is_some();
                    if !target_has_sequence {
                        state.phase = VacuumTargetBuildPhase::StartCopyTable {
                            table_idx: table_idx + 1,
                        };
                        continue;
                    }
                }

                let escaped_table_name = table_name.replace('"', "\"\"");
                // Derive copy-column list from BTreeTable.columns in schema,
                // filtering out virtual generated columns so the SELECT and
                // INSERT arities stay aligned.
                let source_btree_table = config
                    .source_conn
                    .with_schema(config.source_db_id, |schema| {
                        schema.get_btree_table(table_name)
                    });

                // sqlite_sequence and AUTOINCREMENT implicit backing tables
                // may already have rows from the AUTOINCREMENT tracking that
                // ran during the table data copy. Use INSERT OR REPLACE so
                // the source counter values overwrite any stale auto-
                // generated ones (matches SQLite vacuum.c behavior). User-
                // created sequence backing tables get a fresh CREATE TABLE
                // replay (no auto-populated rows), so plain INSERT is fine.
                // todo: sqlite disables AUTOINCREMENT during vacuum, but we don't have such a way yet
                let (select_sql, insert_sql) = build_copy_sql(
                    &config.escaped_schema_name,
                    &escaped_table_name,
                    source_btree_table.as_deref(),
                    entry.is_sqlite_sequence() || entry.is_autoinc_sequence_backing_table(),
                )?;

                // SELECT from source, INSERT into the target.
                let select_stmt = config.source_conn.prepare_internal(&select_sql)?;

                // System tables need nested mode during prepare() to bypass
                // "may not be modified" checks. Can't use prepare_internal()
                // because the nested guard must not persist into step() - the
                // Transaction opcode needs to run for page-level write setup.
                let is_system = crate::schema::is_system_table(table_name);
                if is_system {
                    state.target_conn.start_nested();
                }
                let target_insert_stmt = state.target_conn.prepare(&insert_sql);
                if is_system {
                    state.target_conn.end_nested();
                }
                let target_insert_stmt = target_insert_stmt?;

                state.phase = VacuumTargetBuildPhase::CopyRows {
                    select_stmt: Box::new(select_stmt),
                    target_insert_stmt: Box::new(target_insert_stmt),
                    table_idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::CopyRows {
                mut select_stmt,
                mut target_insert_stmt,
                table_idx,
            } => match select_stmt.step()? {
                crate::StepResult::Row => {
                    let row = select_stmt
                        .row()
                        .expect("StepResult::Row but row() returned None");

                    target_insert_stmt.reset()?;
                    target_insert_stmt.clear_bindings();
                    for (i, value) in row.get_values().cloned().enumerate() {
                        let index =
                            std::num::NonZero::new(i + 1).expect("i + 1 is always non-zero");
                        target_insert_stmt.bind_at(index, value)?;
                    }

                    state.phase = VacuumTargetBuildPhase::StepTargetInsert {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::Done => {
                    // Move to next table
                    state.phase = VacuumTargetBuildPhase::StartCopyTable {
                        table_idx: table_idx + 1,
                    };
                    continue;
                }
                crate::StepResult::IO
                | crate::StepResult::Yield
                | crate::StepResult::Sleep { .. } => {
                    let io = select_stmt
                        .take_io_completions()
                        .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                    state.phase = VacuumTargetBuildPhase::CopyRows {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy.into());
                }
            },

            VacuumTargetBuildPhase::StepTargetInsert {
                select_stmt,
                mut target_insert_stmt,
                table_idx,
            } => match target_insert_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("INSERT statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    // Go back to get next row from source
                    state.phase = VacuumTargetBuildPhase::CopyRows {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    continue;
                }
                crate::StepResult::IO
                | crate::StepResult::Yield
                | crate::StepResult::Sleep { .. } => {
                    let io = target_insert_stmt
                        .take_io_completions()
                        .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                    state.phase = VacuumTargetBuildPhase::StepTargetInsert {
                        select_stmt,
                        target_insert_stmt,
                        table_idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy.into());
                }
            },

            // Phase 3: Create user-defined secondary indexes.
            VacuumTargetBuildPhase::PrepareCreateIndex { idx } => {
                let entries_len = state.indexes_to_create.len();
                if idx >= entries_len {
                    // Done creating indexes, move to post-data objects
                    state.phase = VacuumTargetBuildPhase::PreparePostData { idx: 0 };
                    continue;
                }

                let entry_ordinal = state.indexes_to_create[idx];
                let entry = &state.schema_entries[entry_ordinal];
                // Backing-btree indexes for custom index methods were filtered
                // out when indexes_to_create was built. The remaining CREATE
                // INDEX statements are user-visible and can use ordinary prepare.
                let target_stmt = state.target_conn.prepare(&entry.sql)?;
                state.phase = VacuumTargetBuildPhase::StepCreateIndex {
                    target_schema_stmt: Box::new(target_stmt),
                    idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::StepCreateIndex {
                mut target_schema_stmt,
                idx,
            } => match target_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE INDEX statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.phase = VacuumTargetBuildPhase::PrepareCreateIndex { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO
                | crate::StepResult::Yield
                | crate::StepResult::Sleep { .. } => {
                    let io = target_schema_stmt
                        .take_io_completions()
                        .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                    state.phase = VacuumTargetBuildPhase::StepCreateIndex {
                        target_schema_stmt,
                        idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy.into());
                }
            },

            // Phase 4: Create triggers, views, and rootpage=0 schema objects.
            VacuumTargetBuildPhase::PreparePostData { idx } => {
                let entries_len = state.post_data_entries.len();
                if idx >= entries_len {
                    state.phase = VacuumTargetBuildPhase::FinalizeTargetHeader;
                    continue;
                }

                let entry_ordinal = state.post_data_entries[idx];
                let entry = &state.schema_entries[entry_ordinal];
                let target_stmt = state.target_conn.prepare(&entry.sql)?;
                state.phase = VacuumTargetBuildPhase::StepPostData {
                    target_schema_stmt: Box::new(target_stmt),
                    idx,
                };
                continue;
            }

            VacuumTargetBuildPhase::StepPostData {
                mut target_schema_stmt,
                idx,
            } => match target_schema_stmt.step()? {
                crate::StepResult::Row => {
                    unreachable!("CREATE statement unexpectedly returned a row");
                }
                crate::StepResult::Done => {
                    state.phase = VacuumTargetBuildPhase::PreparePostData { idx: idx + 1 };
                    continue;
                }
                crate::StepResult::IO
                | crate::StepResult::Yield
                | crate::StepResult::Sleep { .. } => {
                    let io = target_schema_stmt
                        .take_io_completions()
                        .unwrap_or_else(|| IOCompletions(Completion::new_yield()));
                    state.phase = VacuumTargetBuildPhase::StepPostData {
                        target_schema_stmt,
                        idx,
                    };
                    return Ok(crate::IOResult::IO(io));
                }
                crate::StepResult::Busy | crate::StepResult::Interrupt => {
                    return Err(LimboError::Busy.into());
                }
            },

            VacuumTargetBuildPhase::FinalizeTargetHeader => {
                match finalize_vacuum_target_header(&state.target_conn, &config.header_meta)? {
                    crate::IOResult::Done(()) => {}
                    crate::IOResult::IO(io) => {
                        state.phase = VacuumTargetBuildPhase::FinalizeTargetHeader;
                        return Ok(crate::IOResult::IO(io));
                    }
                }

                state.phase = VacuumTargetBuildPhase::Done;
                continue;
            }

            VacuumTargetBuildPhase::Done => {
                // Commit the target transaction started in Init phase.
                state.target_conn.execute("COMMIT")?;
                return Ok(crate::IOResult::Done(()));
            }
        }
    }
}

fn table_sql_for_vacuum_replay(target_conn: &Arc<Connection>, sql: &str) -> Result<String> {
    target_conn.dialect().table_sql_for_replay(sql)
}

// Build the SELECT and INSERT SQL strings for copying a table's data.
//
// Uses the in-memory `BTreeTable` column metadata from the schema to derive
// the copy-column list. Virtual generated columns are excluded from both
// SELECT and INSERT since they are computed, not stored. This keeps both
// lists tied to the same stored-column model.
//
// For e.g. given a schema:
// CREATE TABLE employees (
//       id INTEGER PRIMARY KEY,
//       name TEXT,
//       salary INTEGER,
//       bonus INTEGER GENERATED ALWAYS AS (salary * 0.1) VIRTUAL
//   )
//
//   Output:
//   - select_sql: SELECT rowid, "name", "salary" FROM "main"."employees"
//   - insert_sql: INSERT INTO "main"."employees" (rowid, "name", "salary") VALUES (?, ?, ?)
pub(crate) fn build_copy_sql(
    escaped_schema_name: &str,
    escaped_table_name: &str,
    source_btree_table: Option<&BTreeTable>,
    or_replace: bool,
) -> Result<(String, String)> {
    let Some(btree) = source_btree_table else {
        // Storage-backed tables must have schema metadata. If we get here,
        // the schema is inconsistent - somewhere it has gone terribly wrong
        return Err(LimboError::Corrupt(format!(
            "no schema metadata for storage-backed table \"{escaped_table_name}\""
        )));
    };

    // Collect non-virtual-generated columns with their quoted names.
    let mut data_columns: Vec<String> = Vec::new();
    let mut rowid_alias_col_idx: Option<usize> = None;
    for (i, col) in btree.columns().iter().enumerate() {
        if col.is_virtual_generated() {
            continue;
        }
        if col.is_rowid_alias() {
            rowid_alias_col_idx = Some(i);
        }
        let Some(name) = col.name.as_deref() else {
            return Err(LimboError::Corrupt(format!(
                "missing column name for table \"{escaped_table_name}\""
            )));
        };
        let escaped = name.replace('"', "\"\"");
        data_columns.push(format!("\"{escaped}\""));
    }

    if data_columns.is_empty() {
        return Err(LimboError::Corrupt(
            "found a table without any columns".to_string(),
        ));
    }

    // Determine rowid handling: for has_rowid tables, we need to preserve the
    // rowid. Find an alias name (rowid, _rowid_, or oid) that doesn't conflict
    // with an actual column name.
    let rowid_alias = if btree.has_rowid {
        ["rowid", "_rowid_", "oid"]
            .iter()
            .copied()
            .find(|alias| btree.get_column(alias).is_none())
    } else {
        None
    };

    // Build the column lists. If there's a rowid alias column (INTEGER PRIMARY KEY),
    // we exclude it from the data columns and use the rowid alias instead, since
    // the rowid alias column IS the rowid.
    //
    // Track bind_count explicitly instead of parsing the joined string - column
    // names can contain commas inside quotes which would miscount.
    let (select_cols, insert_cols, bind_count) = if let Some(alias) = rowid_alias {
        if let Some(alias_idx) = rowid_alias_col_idx {
            // Remove the rowid alias column from data_columns (it IS the rowid)
            let mut filtered: Vec<&str> = Vec::new();
            let mut col_physical_idx = 0;
            for (i, col) in btree.columns().iter().enumerate() {
                if col.is_virtual_generated() {
                    continue;
                }
                if i != alias_idx {
                    filtered.push(&data_columns[col_physical_idx]);
                }
                col_physical_idx += 1;
            }
            if filtered.is_empty() {
                // Table only has the rowid alias column
                (alias.to_string(), alias.to_string(), 1)
            } else {
                let count = filtered.len() + 1; // +1 for rowid alias
                let cols = filtered.join(", ");
                (
                    format!("{alias}, {cols}"),
                    format!("{alias}, {cols}"),
                    count,
                )
            }
        } else {
            // has_rowid but no explicit alias column - prepend the chosen rowid
            // pseudo-column to the stored column list.
            let count = data_columns.len() + 1; // +1 for rowid alias
            let cols = data_columns.join(", ");
            (
                format!("{alias}, {cols}"),
                format!("{alias}, {cols}"),
                count,
            )
        }
    } else {
        // Either WITHOUT ROWID, or a rowid table where all three pseudo-names
        // are shadowed by real columns. In the shadowed case SQL cannot name
        // the hidden rowid, and SQLite does not require rowid stability for
        // tables without an INTEGER PRIMARY KEY during VACUUM.
        let count = data_columns.len();
        let cols = data_columns.join(", ");
        (cols.clone(), cols, count)
    };

    // The first placeholder is just "?"; each later placeholder adds ", ?".
    // Reserve 3 bytes per placeholder, then subtract the 2-byte separator that
    // the first placeholder does not need.
    let mut placeholders = String::with_capacity(bind_count.saturating_mul(3).saturating_sub(2));
    for i in 0..bind_count {
        if i > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push('?');
    }

    let select_sql =
        format!("SELECT {select_cols} FROM \"{escaped_schema_name}\".\"{escaped_table_name}\"");
    let insert_prefix = if or_replace {
        "INSERT OR REPLACE INTO"
    } else {
        "INSERT INTO"
    };
    let insert_sql =
        format!("{insert_prefix} \"{escaped_table_name}\" ({insert_cols}) VALUES ({placeholders})");

    Ok((select_sql, insert_sql))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CheckpointLockCleanup {
    #[default]
    None,
    /// VACUUM owns only the raw checkpoint_lock; cleanup releases it
    /// directly with `Wal::release_vacuum_checkpoint_lock`.
    ReleaseRaw,
    /// The checkpoint state machine has consumed the raw checkpoint_lock and
    /// may now own read0/write too, so cleanup must abort checkpoint state.
    AbortCheckpoint,
}

struct MvccVacuumGuard {
    connection: Arc<Connection>,
    mv_store: Arc<MvStore>,
    connection_demoted: bool,
}

impl MvccVacuumGuard {
    fn acquire(connection: Arc<Connection>, mv_store: Arc<MvStore>) -> Result<Self> {
        mv_store.try_begin_vacuum_gate()?;
        Ok(Self {
            connection,
            mv_store,
            connection_demoted: false,
        })
    }

    fn demote_connection(&mut self) {
        turso_assert!(
            !self.connection_demoted,
            "MVCC VACUUM connection is already demoted"
        );
        self.connection.demote_to_mvcc_connection();
        self.connection_demoted = true;
    }
}

impl Drop for MvccVacuumGuard {
    fn drop(&mut self) {
        if self.connection_demoted && self.connection.is_mvcc_bootstrap_connection() {
            self.connection.promote_to_regular_connection();
        }
        self.mv_store.release_vacuum_gate();
    }
}

/// Cleanup state for in-place VACUUM. This tracks resources the opcode has
/// acquired and is **not** cleared by `std::mem::take` or drop.
///
/// Successful acquisition / publish order:
/// 1. `mvcc_guard` in `Preflight` when the source database is MVCC-enabled.
/// 2. `checkpoint_cleanup = ReleaseRaw` immediately after acquiring the raw
///    checkpoint lock in `Preflight`.
/// 3. `source_tx_open = true` and `vacuum_lock_held = true` together after
///    `begin_vacuum_blocking_tx()` succeeds in `BeginSourceTx`.
/// 4. `source_tx_open` is cleared immediately after
///    `finish_append_frames_commit()` and `end_write_tx()` in `PublishWalCommit`.
/// 5. `checkpoint_cleanup` transitions from `ReleaseRaw` to
///    `AbortCheckpoint` when the raw checkpoint lock is handed to the
///    checkpoint state machine, and finally to `None` once checkpoint cleanup
///    is no longer needed.
///
/// Since we track the resources explicitly, cleanup can decide what to roll
/// back regardless of which phase was active when the error occurred.
#[derive(Default)]
struct VacuumInPlaceCleanupState {
    /// MVCC gate/demotion guard held until VACUUM can return to MVCC reads.
    mvcc_guard: Option<MvccVacuumGuard>,
    /// Tracks which checkpoint-lock cleanup action is needed.
    checkpoint_cleanup: CheckpointLockCleanup,
    /// Source pager exclusive transaction was started.
    source_tx_open: bool,
    /// WAL VACUUM lock is held exclusively.
    vacuum_lock_held: bool,
}

/// Phases for the in-place VACUUM state machine.
enum VacuumInPlacePhase {
    /// Validate preconditions (auto_commit, active statements, readonly, memory,
    /// MVCC, WAL-backed pager). Then acquire check_point lock
    Preflight,
    /// Acquire exclusive source access on the source WAL via
    /// `begin_vacuum_blocking_tx`: checkpoint_lock is already held from Preflight;
    /// this acquires the exclusive VACUUM lock and installs the source snapshot.
    BeginSourceTx,
    /// Read source database header metadata for the target-build config.
    ReadSourceMetadata,
    /// Create temp DB and run the shared target-build engine.
    TargetBuild {
        config: Box<VacuumTargetBuildConfig>,
        context: Box<VacuumTargetBuildContext>,
    },
    /// Capture the finalized target schema/header before publishing the
    /// replacement image into the source WAL.
    CaptureTargetMetadata,
    /// Open a read transaction on the committed temp pager so that we can read the pages back
    BeginTempReadTx,
    /// Initialize the source WAL header if needed (one-time before first batch).
    /// Two IO steps: write the header, then fsync to set `initialized = true`.
    InitSourceWalHeader {
        total_pages: u32,
        /// The completion to wait on: first the header write, then the fsync.
        completion: crate::io::Completion,
        /// `false` while waiting for the header write, `true` while waiting for
        /// the fsync from `prepare_wal_finish`.
        fsync_phase: bool,
    },
    /// Read a batch of temp pages in using multi-inflight
    /// `Wal::read_frames_batch` calls, one per contiguous frame-id run.
    /// The state waits on a single `CompletionGroup` completion that
    /// covers every run in the batch.
    ReadTempBatch {
        total_pages: u32,
        /// First page id NOT included in this batch (i.e., the page id
        /// where the next batch will start, 1-based).
        next_page: u32,
        prev_prepared: Option<Box<crate::storage::wal::PreparedFrames>>,
        /// Scratch buffer for `Wal::read_frames_batch`, sized to one full copy
        /// batch and reused across copy-back batches when one frame run exactly
        /// matches that size.
        read_scratch_buf: Arc<Buffer>,
        /// Pages for this batch, pre-allocated in logical page-id order.
        batch_pages: Vec<PageRef>,
        /// Aggregate completion for every run issued for this batch.
        read_completion: Completion,
    },
    /// Write a prepared batch to the WAL file.
    WriteWalBatch {
        total_pages: u32,
        next_page: u32,
        prev_prepared: Option<Box<crate::storage::wal::PreparedFrames>>,
        read_scratch_buf: Arc<Buffer>,
        completions: Vec<Completion>,
    },
    /// Fsync the WAL if sync mode requires it.
    SyncSourceWal {
        sync_completion: crate::io::Completion,
    },
    /// Publish: commit_prepared_frames + finish_append_frames_commit, release
    /// source WAL write/source snapshot state, keep the VACUUM lock, and drop
    /// temp resources.
    PublishWalCommit,
    /// TRUNCATE checkpoint: copy WAL frames back into the DB file and truncate
    /// the WAL to zero. The pager's internal state machine handles the multi-step
    /// IO (backfill → sync DB → truncate WAL). Plain VACUUM requires this fold
    /// to complete before reporting success.
    Checkpoint,
    /// Install the pre-captured replacement metadata (from `CaptureTargetMetadata`)
    /// after the source image is committed, regardless of whether the final checkpoint succeeded.
    InstallCommittedImage,
    /// Clean up after successful commit.
    Done,
}

impl Default for VacuumInPlacePhase {
    fn default() -> Self {
        Self::Preflight
    }
}

/// Holds the context for the in-place VACUUM operation across async yields.
pub(crate) struct VacuumInPlaceOpContext {
    /// Database index being vacuumed.
    db: usize,
    /// Current phase for the in-place VACUUM operation.
    phase: VacuumInPlacePhase,
    /// File-backed compacted target image used until the replacement WAL frames
    /// are published into the source database.
    temp_db: Option<Box<VacuumTempDb>>,
    /// Normalized schema/header snapshot for the new image that will
    /// be published to the source DB. We capture it before source WAL publish so that
    /// post-commit we can install it without reading from disk.
    committed_image: Option<VacuumCommittedImageMeta>,
    /// Independent cleanup flags for resources that are not owned by `phase`.
    cleanup_state: VacuumInPlaceCleanupState,
}

impl VacuumInPlaceOpContext {
    pub(crate) fn new(db: usize) -> Self {
        Self {
            db,
            phase: VacuumInPlacePhase::Preflight,
            temp_db: None,
            committed_image: None,
            cleanup_state: VacuumInPlaceCleanupState::default(),
        }
    }

    pub(crate) fn step(&mut self, connection: &Arc<Connection>) -> IOResultOr<()> {
        vacuum_in_place_step(
            connection,
            self.db,
            &mut self.phase,
            &mut self.temp_db,
            &mut self.committed_image,
            &mut self.cleanup_state,
        )
    }

    pub(crate) fn cleanup(self, connection: &Arc<Connection>) -> Result<()> {
        let Self {
            db,
            phase,
            temp_db,
            committed_image,
            cleanup_state,
        } = self;
        vacuum_in_place_cleanup(
            connection,
            db,
            phase,
            temp_db,
            committed_image,
            cleanup_state,
        )
    }
}

/// The batch size for copy-back: how many temp pages to read per batch.
const VACUUM_COPY_BATCH_SIZE: u32 = 64;

/// Returns the first page number of the batch after the one that starts at `start_page`.
/// The current batch covers the half-open range `[start_page, next_start)`,
/// capped to at most `VACUUM_COPY_BATCH_SIZE` pages and never past `total_pages + 1`.
///
/// e.g. total_pages = 200, batch size = 64, start_page = 1
///      current batch is [1, 65), next_start == 65
///
///    if the current batch is [193, 201), so pages 193..=200
///    next_start == 201
fn next_vacuum_copy_batch_start(start_page: u32, total_pages: u32) -> u32 {
    turso_assert!(
        start_page > 0,
        "vacuum copy batch start page must be 1-based",
        { "start_page": start_page, "total_pages": total_pages }
    );
    turso_assert!(
        start_page <= total_pages,
        "vacuum copy batch start must be inside database image",
        { "start_page": start_page, "total_pages": total_pages }
    );
    turso_assert!(
        total_pages < u32::MAX,
        "vacuum copy batch requires a representable exclusive end page",
        { "total_pages": total_pages }
    );
    start_page
        .saturating_add(VACUUM_COPY_BATCH_SIZE)
        .min(total_pages + 1)
}

fn vacuum_completion_error(completion: &Completion, context: &'static str) -> LimboError {
    completion.get_error().map_or_else(
        || LimboError::InternalError(format!("VACUUM: {context} failed")),
        LimboError::CompletionError,
    )
}

/// Coalesce `(page_ref, frame_id)` pairs into contiguous-frame-id runs.
///
/// Plain `VACUUM` relies on the temp-WAL-only invariant: every temp page
/// lives in the temp WAL (auto-checkpoint is disabled on the temp
/// connection), so for each logical page id we must get a frame id via
/// `find_frame`. Sorting by `frame_id` before splitting into runs lets
/// a single `pread` over a contiguous range serve many logical pages at once.
///
/// e.g. input pairs in logical page order:
///   [(page1, 10), (page2, 11), (page3, 14), (page4, 15), (page5, 12)]
///
/// after sorting by `frame_id`:
///   [(page1, 10), (page2, 11), (page5, 12), (page3, 14), (page4, 15)]
///
/// output runs:
///   [(10, [page1, page2, page5]), (14, [page3, page4])]
fn coalesce_frame_runs(mut pairs: Vec<(PageRef, u64)>) -> Vec<(u64, Vec<PageRef>)> {
    let mut runs: Vec<(u64, Vec<PageRef>)> = Vec::new();
    if pairs.is_empty() {
        return runs;
    }
    pairs.sort_by_key(|(_, f)| *f);
    let mut prev_frame_id = None;
    for (page, frame_id) in pairs {
        turso_assert!(frame_id > 0, "WAL frame ids must be 1-based");
        if let Some(prev) = prev_frame_id {
            // the current frame id must be greater than the previous one, since it is sorted
            // however, if there are dupes, then following assertion would fail
            turso_assert!(
                frame_id > prev,
                "VACUUM temp WAL frame ids must be unique",
                { "previous_frame_id": prev, "frame_id": frame_id }
            );
        }
        prev_frame_id = Some(frame_id);
        if let Some((run_start, run_pages)) = runs.last_mut() {
            let next_expected = *run_start + run_pages.len() as u64;
            if frame_id == next_expected {
                run_pages.push(page);
                continue;
            }
        }
        runs.push((frame_id, vec![page]));
    }
    runs
}

/// Start multi-inflight reads for one copy-back batch spanning logical
/// pages `[batch_start, batch_end)`. Every page must be resident in the
/// temp WAL (temp-WAL-only invariant). Returns the pre-allocated
/// per-page `PageRef`s in logical page-id order plus the aggregate
/// completion covering every run.
fn start_temp_batch_reads(
    temp_pager: &Pager,
    batch_start: u32,
    batch_end: u32,
    scratch_buf: &Arc<Buffer>,
) -> Result<(Vec<PageRef>, Completion)> {
    turso_assert!(
        batch_start < batch_end,
        "empty vacuum read batch",
        { "batch_start": batch_start, "batch_end": batch_end }
    );
    turso_assert!(
        batch_start > 0,
        "vacuum read batch start page must be 1-based",
        { "batch_start": batch_start, "batch_end": batch_end }
    );
    turso_assert!(
        batch_end - batch_start <= VACUUM_COPY_BATCH_SIZE,
        "vacuum read batch exceeds configured copy batch size",
        { "batch_start": batch_start, "batch_end": batch_end, "batch_size": VACUUM_COPY_BATCH_SIZE }
    );
    let wal = temp_pager
        .wal
        .as_ref()
        .ok_or_else(|| LimboError::InternalError("VACUUM requires a temp WAL pager".into()))?;

    let len = (batch_end - batch_start) as usize;
    // todo: avoid doing allocation here, instead create a scratch buffer at caller and reuse it here
    let mut logical_pages: Vec<PageRef> = Vec::with_capacity(len);
    let mut pairs: Vec<(PageRef, u64)> = Vec::with_capacity(len);

    for page_id in batch_start..batch_end {
        let page: PageRef = Arc::new(Page::new(page_id as i64));
        // todo: add find_frames at WAL where you can send batch of page ids and get all the frame ids
        let frame_id = wal.find_frame(page_id as u64, None)?.ok_or_else(|| {
            LimboError::InternalError(format!(
                "VACUUM: page {page_id} not found in target WAL (WAL-only invariant violated)"
            ))
        })?;
        logical_pages.push(page.clone());
        pairs.push((page, frame_id));
    }

    // let's coalesce frame ids so that we can fetch maximum frames with a single pread
    let runs = coalesce_frame_runs(pairs);

    // Reuse the max-batch scratch buffer for one run whose read length matches
    // exactly (the common case where target-build wrote a contiguous chunk of
    // at least `VACUUM_COPY_BATCH_SIZE` pages).
    let frame_size = temp_pager.get_page_size_unchecked().get() as usize + WAL_FRAME_HEADER_SIZE;
    let scratch_total = frame_size * VACUUM_COPY_BATCH_SIZE as usize;
    turso_assert!(
        scratch_buf.len() == scratch_total,
        "VACUUM scratch buffer size must match max-batch read size",
        { "buf_len": scratch_buf.len(), "expected": scratch_total }
    );

    let mut group = CompletionGroup::new(|_| {});
    let mut scratch_claimed = false;
    for (start_frame, run_pages) in &runs {
        let run_total = frame_size * run_pages.len();
        // right now we only use the scratch buffer if it matches the run size
        // todo: we should be able to split the scratch buffer and split it across runs
        let run_scratch = if !scratch_claimed && run_total == scratch_total {
            scratch_claimed = true;
            Some(scratch_buf.clone())
        } else {
            None
        };
        let _c = wal.read_frames_batch(
            *start_frame,
            run_pages,
            temp_pager.buffer_pool.clone(),
            run_scratch,
            Some(&mut group),
        )?;
    }
    let combined = group.build();

    Ok((logical_pages, combined))
}

fn new_vacuum_read_scratch_buf(temp_pager: &Pager) -> Arc<Buffer> {
    let frame_size = temp_pager.get_page_size_unchecked().get() as usize + WAL_FRAME_HEADER_SIZE;
    let scratch_total = frame_size * VACUUM_COPY_BATCH_SIZE as usize;
    Arc::new(Buffer::new_temporary(scratch_total))
}

#[derive(Clone)]
struct VacuumCommittedImageMeta {
    header: DatabaseHeader,
    schema: Arc<Schema>,
}

fn replace_shared_schema_after_vacuum(source_db: &Database, schema: Arc<Schema>) {
    source_db.replace_schema(schema);
}

fn install_mvcc_state_after_vacuum_commit(
    source_db: &Database,
    mv_store: &MvStore,
    header: DatabaseHeader,
    schema: Arc<Schema>,
) {
    mv_store.reset_after_vacuum(header, schema.as_ref());
    replace_shared_schema_after_vacuum(source_db, schema);
}

/// Capture the finalized replacement metadata from the temp target before the
/// source WAL publish. Then once temp back pages are committed, we can just install this.
///
/// `source_sequences` is the source connection's sequence-descriptor map
/// at the moment VACUUM started. VACUUM rewrites every page but preserves
/// every sequence's definition (start/inc/min/max/cycle), so the map is
/// still valid for the post-VACUUM image — and grafting it onto the
/// reparsed schema avoids the blocking sequence-descriptor reload path,
/// which would violate the vdbe async contract from inside VACUUM's
/// state machine.
fn capture_target_metadata(
    temp_db: &VacuumTempDb,
    source_sequences: FxHashMap<String, Arc<crate::schema::Sequence>>,
) -> Result<VacuumCommittedImageMeta> {
    let temp_conn = &temp_db.conn;
    let temp_pager = temp_conn.get_pager();
    temp_pager.begin_read_tx()?;
    temp_conn.set_tx_state(crate::connection::TransactionState::Read);

    let capture_result = (|| {
        let header = temp_pager
            .io
            .block(|| temp_pager.with_header(|header| *header))?;
        temp_conn.reparse_schema_with_cookie_keeping_sequences(
            header.schema_cookie.get(),
            source_sequences,
        )?;
        Ok(VacuumCommittedImageMeta {
            header,
            schema: temp_conn.schema.read().clone(),
        })
    })();

    temp_pager.end_read_tx();
    temp_conn.set_tx_state(crate::connection::TransactionState::None);
    capture_result
}

/// Install the already-captured replacement schema/header after the source WAL
/// publish. This must be infallible: the physical replacement image is already
/// committed, so we only publish the precomputed metadata snapshot.
fn install_committed_vacuum_image(
    connection: &Arc<Connection>,
    db: usize,
    committed_image: &VacuumCommittedImageMeta,
) {
    let source_db = connection.get_source_database(db);
    let schema = committed_image.schema.clone();
    *connection.schema.write() = schema.clone();

    if source_db.mvcc_enabled() {
        let mv_store = source_db
            .get_mv_store()
            .as_ref()
            .cloned()
            .expect("MVCC database missing MV store during VACUUM metadata install");
        install_mvcc_state_after_vacuum_commit(
            source_db.as_ref(),
            mv_store.as_ref(),
            committed_image.header,
            schema,
        );
    } else {
        replace_shared_schema_after_vacuum(source_db.as_ref(), schema);
    }
}

fn reload_physical_schema_for_mvcc_vacuum(
    connection: &Arc<Connection>,
    source_pager: &Pager,
) -> Result<()> {
    source_pager.begin_read_tx()?;
    connection.set_tx_state(crate::connection::TransactionState::Read);

    // Preserve the current sequences map and graft it onto the reparsed
    // schema. The MVCC VACUUM rebuilds physical page locations but does
    // not touch sequence definitions, so the in-memory descriptors stay
    // valid. The standard `reparse_schema` would otherwise call the
    // blocking `Schema::populate_sequences` and `populate_sequences_via_sql`
    // helpers, which violate the vdbe async contract from inside the
    // VACUUM state machine.
    let preserved_sequences = connection.schema.read().sequences.clone();
    let reparse_result = connection.read_current_schema_cookie().and_then(|cookie| {
        connection.reparse_schema_with_cookie_keeping_sequences(cookie, preserved_sequences)
    });

    source_pager.end_read_tx();
    connection.set_tx_state(crate::connection::TransactionState::None);

    reparse_result
}

/// Step the in-place VACUUM state machine once. Returns `IO` to yield or
/// `Done(())` when the entire operation is complete.
///
/// `cleanup_state` independently tracks which resources the opcode has acquired so
/// that cleanup can roll back correctly
fn vacuum_in_place_step(
    connection: &Arc<Connection>,
    db: usize,
    phase: &mut VacuumInPlacePhase,
    temp_db: &mut Option<Box<VacuumTempDb>>,
    committed_image: &mut Option<VacuumCommittedImageMeta>,
    cleanup_state: &mut VacuumInPlaceCleanupState,
) -> IOResultOr<()> {
    let source_pager = connection.get_pager_from_database_index(&db)?;

    loop {
        match phase {
            VacuumInPlacePhase::Preflight => {
                // 1. Must be in auto-commit mode (no explicit transaction).
                if !connection.auto_commit.load(Ordering::SeqCst) {
                    return Err(LimboError::TxError(
                        "cannot VACUUM from within a transaction".to_string(),
                    )
                    .into());
                }
                // 2. No other active root statements on this connection.
                if connection.n_active_root_statements.load(Ordering::SeqCst) != 1 {
                    return Err(LimboError::TxError(
                        "cannot VACUUM - SQL statements in progress".to_string(),
                    )
                    .into());
                }
                // 3. Reject readonly database, well you cannot edit a readonly db ^_^
                if connection.is_readonly(db) {
                    return Err(LimboError::ReadOnly.into());
                }
                // 4. Resolve source database mode.
                let source_db = connection.get_source_database(db);
                let source_mvcc_enabled = source_db.mvcc_enabled();
                // 5. Reject in-memory databases.
                if source_db.is_in_memory_db() {
                    return Err(LimboError::InternalError(
                        "cannot VACUUM an in-memory database".to_string(),
                    )
                    .into());
                }
                reject_unsupported_vacuum_auto_vacuum_mode(source_pager.get_auto_vacuum_mode())?;
                // 6. Reject non-WAL pagers.
                let wal = source_pager.wal.as_ref().ok_or_else(|| {
                    LimboError::InternalError("VACUUM requires a WAL-mode database".to_string())
                })?;
                // 7. In MVCC mode, hold the existing MVCC stop-the-world
                // gate for the full VACUUM. Callers are expected to run MVCC
                // checkpoint before VACUUM; this gate only enforces that no
                // active or new MVCC transactions overlap the physical copy.
                let mvcc_guard = if source_mvcc_enabled {
                    let mv_store = source_db.get_mv_store().as_ref().cloned().ok_or_else(|| {
                        LimboError::InternalError(
                            "MVCC database missing MV store during VACUUM".to_string(),
                        )
                    })?;
                    // acquire the exclusive lock on the mvcc
                    let mut guard = MvccVacuumGuard::acquire(connection.clone(), mv_store.clone())?;
                    if mv_store.has_uncheckpointed_log()? {
                        return Err(LimboError::TxError(
                            "cannot VACUUM an MVCC database with uncheckpointed changes; run PRAGMA wal_checkpoint(TRUNCATE) first".to_string(),
                        ).into());
                    }
                    if connection.get_mv_tx_id_for_db(db).is_some() {
                        turso_assert!(
                            false,
                            "VACUUM must not hold an MVCC transaction after acquiring gate"
                        );
                        return Err(LimboError::InternalError(
                            "VACUUM acquired MVCC gate while this connection has an MVCC transaction"
                                .to_string(),
                        ).into());
                    }
                    guard.demote_connection();
                    // After demotion this connection stops reading schema-cookie state from the
                    // MV store and starts reading directly from the pager-backed DB image.
                    // Let's be conservative, and reparse schema
                    reload_physical_schema_for_mvcc_vacuum(connection, &source_pager)?;
                    Some(guard)
                } else {
                    None
                };
                if let Some(mvcc_guard) = mvcc_guard {
                    turso_assert!(
                        cleanup_state.mvcc_guard.is_none(),
                        "MVCC VACUUM cleanup state already owns a guard"
                    );
                    cleanup_state.mvcc_guard = Some(mvcc_guard);
                }
                // 9. Acquire the checkpoint serialization lock early. This
                // ensures a post-commit TRUNCATE checkpoint won't fail due
                // to a concurrent checkpointer. If the lock is unavailable
                // we fail fast before doing any expensive work.
                wal.try_begin_vacuum_checkpoint_lock()?;
                cleanup_state.checkpoint_cleanup = CheckpointLockCleanup::ReleaseRaw;
                *phase = VacuumInPlacePhase::BeginSourceTx;
                continue;
            }

            VacuumInPlacePhase::BeginSourceTx => {
                // Acquire exclusive WAL access in one shot:
                // vacuum lock + WAL write lock + connection snapshot.
                // Checkpoint lock was already acquired in Preflight.
                match source_pager.begin_vacuum_blocking_tx()? {
                    crate::IOResult::Done(()) => {
                        connection.auto_commit.store(false, Ordering::SeqCst);
                        connection.set_tx_state(crate::connection::TransactionState::Write {
                            schema_did_change: false,
                        });
                        cleanup_state.source_tx_open = true;
                        cleanup_state.vacuum_lock_held = true;
                        *phase = VacuumInPlacePhase::ReadSourceMetadata;
                        continue;
                    }
                    crate::IOResult::IO(io) => {
                        *phase = VacuumInPlacePhase::BeginSourceTx;
                        return Ok(IOResult::IO(io));
                    }
                }
            }

            VacuumInPlacePhase::ReadSourceMetadata => {
                turso_assert!(
                    cleanup_state.checkpoint_cleanup == CheckpointLockCleanup::ReleaseRaw,
                    "VACUUM source metadata phase requires held checkpoint lock"
                );
                turso_assert!(
                    cleanup_state.source_tx_open,
                    "VACUUM source metadata phase requires source exclusive snapshot"
                );
                let source_db = connection.get_source_database(db);

                // Read page size from pager (cached after begin_read_tx).
                let page_size = source_pager
                    .get_page_size()
                    .map(|ps| ps.get())
                    .unwrap_or(PageSize::DEFAULT as u32);

                // Read reserved bytes and header metadata in a single
                // with_header call
                let io = &*source_pager.io;
                let (reserved_space, header_meta) = io.block(|| {
                    source_pager.with_header(|h| {
                        (h.reserved_space, VacuumDbHeaderMeta::from_source_header(h))
                    })
                })?;
                // Create temp database.
                let new_temp_db =
                    open_vacuum_temp_db(connection, &source_db, page_size, reserved_space)?;

                mirror_symbols(connection, &new_temp_db.conn);
                let source_custom_types = capture_custom_types(connection, db);

                let config = VacuumTargetBuildConfig {
                    source_conn: connection.clone(),
                    escaped_schema_name: "main".to_string(),
                    source_db_id: db,
                    header_meta,
                    source_custom_types,
                    target_mvcc_enabled: false,
                    target_auto_vacuum_mode: source_pager.get_auto_vacuum_mode(),
                    copy_mvcc_metadata_table: source_db.mvcc_enabled(),
                };

                let target_build_context = VacuumTargetBuildContext::new(new_temp_db.conn.clone());
                turso_assert!(
                    temp_db.is_none(),
                    "VACUUM temp database must not already be installed"
                );
                *temp_db = Some(Box::new(new_temp_db));

                *phase = VacuumInPlacePhase::TargetBuild {
                    config: Box::new(config),
                    context: Box::new(target_build_context),
                };
                continue;
            }

            VacuumInPlacePhase::TargetBuild { config, context } => {
                match vacuum_target_build_step(config.as_ref(), context.as_mut())? {
                    crate::IOResult::Done(()) => {
                        // Temp build complete. Capture the finalized metadata
                        // snapshot now, before we copy back the pages to main db
                        *phase = VacuumInPlacePhase::CaptureTargetMetadata;
                        continue;
                    }
                    crate::IOResult::IO(io) => {
                        return Ok(IOResult::IO(io));
                    }
                }
            }

            VacuumInPlacePhase::CaptureTargetMetadata => {
                let temp_db_ref = temp_db
                    .as_ref()
                    .expect("VACUUM CaptureTargetMetadata phase requires temp db");
                let source_sequences = connection.schema.read().sequences.clone();
                *committed_image = Some(capture_target_metadata(temp_db_ref, source_sequences)?);
                *phase = VacuumInPlacePhase::BeginTempReadTx;
                continue;
            }

            VacuumInPlacePhase::BeginTempReadTx => {
                let temp_db_ref = temp_db
                    .as_ref()
                    .expect("VACUUM BeginTempReadTx phase requires temp db");
                // The temp db build is now completed and it must have released its WAL read
                // mark. So we start a fresh read snapshot on the committed temp image.
                // so `Wal::find_frame` uses the committed temp-WAL frame set for copy-back.
                let temp_pager = temp_db_ref.conn.get_pager();
                turso_assert!(
                    !temp_pager.holds_read_lock(),
                    "VACUUM temp COMMIT should release the temp WAL read lock"
                );
                temp_pager.begin_read_tx()?;

                // one invariant is that temp db should have disabled checkpoints and all raeds
                // must happen over WAL only. In that case, the db file should be same as the page
                // size.
                let temp_page_size = temp_pager.get_page_size_unchecked().get() as u64;
                let temp_db_file_size = temp_pager.db_file.size()?;
                turso_assert_eq!(
                    temp_db_file_size,
                    temp_page_size,
                    "VACUUM temp DB file must contain only page 1; compacted pages must remain in temp WAL",
                    {
                        "temp_db_file_size": temp_db_file_size,
                        "temp_page_size": temp_page_size
                    }
                );

                // Determine the compacted image size from temp page 1 header.
                let io = &*temp_pager.io;
                let total_pages: u32 =
                    io.block(|| temp_pager.with_header(|h| h.database_size.get()))?;
                let wal = source_pager
                    .wal
                    .as_ref()
                    .ok_or_else(|| LimboError::InternalError("VACUUM requires WAL".into()))?;

                turso_assert!(
                    total_pages > 0,
                    "VACUUM target build must produce at least page 1 in the committed temp image"
                );

                // Initialize the source WAL header before the first batch.
                let page_sz = source_pager.get_page_size_unchecked();

                if let Some(header_write_c) = wal.prepare_wal_start(page_sz)? {
                    // WAL not yet initialized — yield on the header write.
                    *phase = VacuumInPlacePhase::InitSourceWalHeader {
                        total_pages,
                        completion: header_write_c,
                        fsync_phase: false,
                    };
                    continue;
                }

                // WAL already initialized — kick off the first read batch.
                let batch_end = next_vacuum_copy_batch_start(1, total_pages);
                let read_scratch_buf = new_vacuum_read_scratch_buf(&temp_pager);
                let (batch_pages, read_completion) =
                    start_temp_batch_reads(&temp_pager, 1, batch_end, &read_scratch_buf)?;

                *phase = VacuumInPlacePhase::ReadTempBatch {
                    total_pages,
                    next_page: batch_end,
                    prev_prepared: None,
                    read_scratch_buf,
                    batch_pages,
                    read_completion,
                };
                continue;
            }

            VacuumInPlacePhase::InitSourceWalHeader {
                total_pages,
                completion,
                fsync_phase,
            } => {
                if !completion.finished() {
                    return Ok(IOResult::IO(IOCompletions(completion.clone())));
                }
                if !completion.succeeded() {
                    return Err(vacuum_completion_error(completion, "WAL header init").into());
                }

                if !*fsync_phase {
                    // Header write done — issue the fsync via prepare_wal_finish
                    // to set WAL `initialized = true`.
                    let wal = source_pager.wal.as_ref().unwrap();
                    let sync_c = wal.prepare_wal_finish(source_pager.get_sync_type())?;
                    *completion = sync_c;
                    *fsync_phase = true;
                    continue;
                }

                // WAL header fully initialized. Kick off the first read batch.
                let temp_db_ref = temp_db
                    .as_ref()
                    .expect("VACUUM InitSourceWalHeader phase requires temp db");
                let temp_pager = temp_db_ref.conn.get_pager();
                let batch_end = next_vacuum_copy_batch_start(1, *total_pages);
                let read_scratch_buf = new_vacuum_read_scratch_buf(&temp_pager);
                let (batch_pages, read_completion) =
                    start_temp_batch_reads(&temp_pager, 1, batch_end, &read_scratch_buf)?;

                *phase = VacuumInPlacePhase::ReadTempBatch {
                    total_pages: *total_pages,
                    next_page: batch_end,
                    prev_prepared: None,
                    read_scratch_buf,
                    batch_pages,
                    read_completion,
                };
                continue;
            }

            VacuumInPlacePhase::ReadTempBatch {
                total_pages,
                next_page,
                prev_prepared,
                read_scratch_buf,
                batch_pages,
                read_completion,
            } => {
                turso_assert!(
                    *total_pages < u32::MAX,
                    "VACUUM read batch requires a representable exclusive end page",
                    { "total_pages": *total_pages }
                );
                let database_end = *total_pages + 1;
                turso_assert!(
                    !batch_pages.is_empty(),
                    "VACUUM read batch must contain pages"
                );
                turso_assert!(
                    batch_pages.len() as u32 <= VACUUM_COPY_BATCH_SIZE,
                    "VACUUM read batch exceeds configured copy batch size",
                    { "batch_pages": batch_pages.len(), "batch_size": VACUUM_COPY_BATCH_SIZE }
                );
                turso_assert!(
                    *next_page > 1 && *next_page <= database_end,
                    "VACUUM read batch next page is outside database image",
                    { "next_page": *next_page, "total_pages": *total_pages }
                );
                // Wait for every run in this batch to finish.
                if !read_completion.finished() {
                    return Ok(IOResult::IO(IOCompletions(read_completion.clone())));
                }
                if !read_completion.succeeded() {
                    return Err(vacuum_completion_error(read_completion, "temp batch read").into());
                }

                // All pages in this batch are loaded. Prepare WAL frames.
                for page in batch_pages.iter() {
                    turso_assert!(
                        page.is_loaded(),
                        "VACUUM read batch page must be loaded before WAL prepare",
                        { "page_id": page.get().id() }
                    );
                    turso_assert!(
                        !page.is_locked(),
                        "VACUUM read batch page lock leaked before WAL prepare",
                        { "page_id": page.get().id() }
                    );
                }
                let all_read = *next_page > *total_pages;
                let wal = source_pager
                    .wal
                    .as_ref()
                    .ok_or_else(|| LimboError::InternalError("VACUUM requires WAL".into()))?;
                let page_sz = source_pager.get_page_size_unchecked();

                let db_size_on_commit = if all_read { Some(*total_pages) } else { None };

                let prepared = wal.prepare_frames(
                    batch_pages,
                    page_sz,
                    db_size_on_commit,
                    prev_prepared.as_deref(),
                )?;

                // Submit writes via WriteBatch.
                let wal_file = wal.wal_file()?;
                let mut batch = IOWriteBatch::new(wal_file);
                batch.writev(prepared.offset, &prepared.bufs);
                let completions = batch.submit(None)?;

                *phase = VacuumInPlacePhase::WriteWalBatch {
                    total_pages: *total_pages,
                    next_page: *next_page,
                    prev_prepared: Some(Box::new(prepared)),
                    read_scratch_buf: Arc::clone(read_scratch_buf),
                    completions,
                };
                continue;
            }

            VacuumInPlacePhase::WriteWalBatch {
                total_pages,
                next_page,
                prev_prepared,
                read_scratch_buf,
                completions,
            } => {
                let database_end = *total_pages + 1;
                turso_assert!(
                    prev_prepared.is_some(),
                    "VACUUM WAL write batch requires prepared frames"
                );
                turso_assert!(
                    !completions.is_empty(),
                    "VACUUM WAL write batch requires write completions"
                );
                turso_assert!(
                    *next_page > 1 && *next_page <= database_end,
                    "VACUUM WAL write batch next page is outside database image",
                    { "next_page": *next_page, "total_pages": *total_pages }
                );
                // Wait for all writes in this batch. We yield on the first
                // unfinished completion; re-entry will re-check them all.
                let pending = completions.iter().find(|c| !c.finished()).cloned();
                if let Some(pending) = pending {
                    return Ok(IOResult::IO(IOCompletions(pending)));
                }

                // Check for write errors.
                for c in completions.iter() {
                    if !c.succeeded() {
                        return Err(vacuum_completion_error(c, "WAL write").into());
                    }
                }

                // The batch write has finished, so advance the source WAL's
                // connection-local state now. `finish_append_frames_commit()`
                // later publishes `max_frame` and the rolling checksum from
                // the WAL object itself, so each completed batch must first be
                // reflected in the local page->frame cache, max_frame, and
                // checksum state.
                //
                // Skip `finalize_committed_pages()`: these PageRefs came from
                // the temp pager, not the source pager's cache. They carry no
                // source-cache dirty state or WAL tags to update, and the
                // source page cache is invalidated wholesale in Publish.
                let wal = source_pager.wal.as_ref().unwrap();
                let prepared = prev_prepared
                    .as_ref()
                    .expect("VACUUM WriteWalBatch phase requires prepared frames");
                wal.commit_prepared_frames(std::slice::from_ref(prepared.as_ref()));

                // More pages to copy?
                if *next_page <= *total_pages {
                    // Kick off the next read batch in parallel.
                    let temp_db_ref = temp_db
                        .as_ref()
                        .expect("VACUUM WriteWalBatch phase requires temp db");
                    let temp_pager = temp_db_ref.conn.get_pager();
                    let batch_end = next_vacuum_copy_batch_start(*next_page, *total_pages);
                    let (batch_pages, read_completion) = start_temp_batch_reads(
                        &temp_pager,
                        *next_page,
                        batch_end,
                        read_scratch_buf,
                    )?;
                    let prev_prepared = prev_prepared
                        .take()
                        .expect("VACUUM WriteWalBatch phase requires prepared frames");

                    *phase = VacuumInPlacePhase::ReadTempBatch {
                        total_pages: *total_pages,
                        next_page: batch_end,
                        prev_prepared: Some(prev_prepared),
                        read_scratch_buf: Arc::clone(read_scratch_buf),
                        batch_pages,
                        read_completion,
                    };
                    continue;
                }

                // All pages written. Fsync WAL if sync mode requires it.
                // NORMAL mode skips fsync on WAL commit (fsyncs happen on
                // checkpoint and WAL restart instead). This matches the regular
                // pager commit path in Pager::commit_tx / CommitState.
                let sync_mode = connection.get_sync_mode();
                if sync_mode == SyncMode::Full {
                    let sync_c = wal.sync(source_pager.get_sync_type())?;
                    *phase = VacuumInPlacePhase::SyncSourceWal {
                        sync_completion: sync_c,
                    };
                    continue;
                }

                // No sync needed — proceed directly to publish.
                *phase = VacuumInPlacePhase::PublishWalCommit;
                continue;
            }

            VacuumInPlacePhase::SyncSourceWal { sync_completion } => {
                if !sync_completion.finished() {
                    return Ok(IOResult::IO(IOCompletions(sync_completion.clone())));
                }
                if !sync_completion.succeeded() {
                    return Err(vacuum_completion_error(sync_completion, "WAL fsync").into());
                }
                *phase = VacuumInPlacePhase::PublishWalCommit;
                continue;
            }

            VacuumInPlacePhase::PublishWalCommit => {
                turso_assert!(
                    cleanup_state.checkpoint_cleanup == CheckpointLockCleanup::ReleaseRaw,
                    "VACUUM publish phase requires held checkpoint lock"
                );
                turso_assert!(
                    cleanup_state.source_tx_open,
                    "VACUUM publish phase requires source exclusive snapshot"
                );
                let wal = source_pager
                    .wal
                    .as_ref()
                    .ok_or_else(|| LimboError::InternalError("VACUUM requires WAL".into()))?;

                // Publish the copied frames as a committed WAL transaction in the source db.
                // After this point the source write transaction is committed and visible
                // through WAL coordination state, so must no longer treat the source write transaction
                // as abortable via `pager.rollback_tx(connection)`; it may only release remaining
                // locks/state and continue with checkpoint/schema reload handling.
                wal.finish_append_frames_commit()?;

                source_pager.end_write_tx();
                cleanup_state.source_tx_open = false;

                // Restore connection bookkeeping immediately. The commit is
                // durable so there is nothing to roll back; restoring here
                // means the connection is never left poisoned even if the
                // checkpoint or schema reload below fails.
                connection.auto_commit.store(true, Ordering::SeqCst);
                connection.set_tx_state(crate::connection::TransactionState::None);

                // Invalidate page cache and schema cookie so fresh reads see
                // the newly committed WAL frames.
                source_pager.clear_page_cache(false);
                source_pager.set_schema_cookie(None);

                // Drop temp resources before checkpoint.
                let temp_db = temp_db
                    .take()
                    .expect("VACUUM PublishWalCommit phase requires temp db");
                drop(temp_db);

                *phase = VacuumInPlacePhase::Checkpoint;
                continue;
            }

            VacuumInPlacePhase::Checkpoint => {
                // TRUNCATE checkpoint: copy WAL frames into the DB file,
                // force a durable DB sync, then truncate the WAL to zero
                // bytes. It is possible that user might have set sync to normal/off, but we
                // must force fsync on db
                if cleanup_state.checkpoint_cleanup == CheckpointLockCleanup::ReleaseRaw {
                    cleanup_state.checkpoint_cleanup = CheckpointLockCleanup::AbortCheckpoint;
                }
                match source_pager.vacuum_checkpoint_with_held_lock(SyncMode::Full, true) {
                    Ok(crate::IOResult::Done(result)) => {
                        cleanup_state.checkpoint_cleanup = CheckpointLockCleanup::None;
                        turso_assert!(
                            result.should_truncate(),
                            "VACUUM TRUNCATE checkpoint must fully backfill on success",
                            {
                                "wal_max_frame": result.wal_max_frame,
                                "wal_total_backfilled": result.wal_total_backfilled,
                                "wal_checkpoint_backfilled": result.wal_checkpoint_backfilled
                            }
                        );
                        if cleanup_state.vacuum_lock_held {
                            let wal = source_pager.wal.as_ref().ok_or_else(|| {
                                LimboError::InternalError("VACUUM requires WAL".into())
                            })?;
                            wal.release_vacuum_lock();
                            cleanup_state.vacuum_lock_held = false;
                        }
                        *phase = VacuumInPlacePhase::InstallCommittedImage;
                        continue;
                    }
                    Ok(crate::IOResult::IO(io)) => {
                        *phase = VacuumInPlacePhase::Checkpoint;
                        return Ok(IOResult::IO(io));
                    }
                    Err(err) => {
                        tracing::error!("VACUUM post-commit checkpoint failed: {err}");
                        source_pager.cleanup_after_checkpoint_failure();
                        cleanup_state.checkpoint_cleanup = CheckpointLockCleanup::None;
                        let committed_image = committed_image.take().expect(
                            "VACUUM checkpoint failure after publish requires captured metadata",
                        );
                        install_committed_vacuum_image(connection, db, &committed_image);
                        if cleanup_state.vacuum_lock_held {
                            if let Some(wal) = source_pager.wal.as_ref() {
                                wal.release_vacuum_lock();
                                cleanup_state.vacuum_lock_held = false;
                            }
                        }
                        return Err(err);
                    }
                }
            }

            VacuumInPlacePhase::InstallCommittedImage => {
                let committed_image = committed_image
                    .as_ref()
                    .expect("VACUUM metadata install requires captured committed image");
                install_committed_vacuum_image(connection, db, committed_image);
                drop(cleanup_state.mvcc_guard.take());

                *phase = VacuumInPlacePhase::Done;
                return Ok(IOResult::Done(()));
            }

            VacuumInPlacePhase::Done => {
                return Ok(IOResult::Done(()));
            }
        }
    }
}

/// Roll back the source transaction and restore connection state after an
/// in-place VACUUM failure. Uses `cleanup_state` to decide what to undo; this
/// state tracks resources that are not owned by the phase enum.
///
/// `phase` is taken by value so helper statements inside `TargetBuild` are
/// dropped before we attempt `rollback_tx`. This
/// avoids the nestedness suppression bug where live helper statements keep
/// `Connection::nestedness > 0`, making `rollback_tx` a no-op.
fn vacuum_in_place_cleanup(
    connection: &Arc<Connection>,
    db: usize,
    mut phase: VacuumInPlacePhase,
    temp_db: Option<Box<VacuumTempDb>>,
    committed_image: Option<VacuumCommittedImageMeta>,
    mut cleanup_state: VacuumInPlaceCleanupState,
) -> Result<()> {
    if let VacuumInPlacePhase::TargetBuild { context, .. } = &mut phase {
        context.cleanup_after_error()?;
    }

    match cleanup_state.checkpoint_cleanup {
        CheckpointLockCleanup::ReleaseRaw => {
            let pager = connection
                .get_pager_from_database_index(&db)
                .expect("VACUUM cleanup requires source pager");
            if let Some(wal) = pager.wal.as_ref() {
                wal.release_vacuum_checkpoint_lock();
            }
        }
        CheckpointLockCleanup::AbortCheckpoint => {
            let pager = connection
                .get_pager_from_database_index(&db)
                .expect("VACUUM cleanup requires source pager");
            pager.cleanup_after_checkpoint_failure();
        }
        CheckpointLockCleanup::None => {}
    }

    // No source transaction to undo. Any raw locks are still released below.
    if !cleanup_state.source_tx_open {
        if let Some(committed_image) = committed_image.as_ref() {
            install_committed_vacuum_image(connection, db, committed_image);
        }
        if cleanup_state.vacuum_lock_held {
            let pager = connection
                .get_pager_from_database_index(&db)
                .expect("VACUUM cleanup requires source pager");
            if let Some(wal) = pager.wal.as_ref() {
                wal.release_vacuum_lock();
            }
        }
        drop(cleanup_state.mvcc_guard.take());
        drop(temp_db);
        return Ok(());
    }

    // Drop the phase first to release any target-build helper statements.
    // Their Drop impls decrement Connection::nestedness, which must happen
    // before rollback_tx (which is a no-op while nested).
    drop(phase);
    drop(temp_db);

    // Write transaction open but not yet committed — roll back.
    let pager = connection
        .get_pager_from_database_index(&db)
        .expect("VACUUM cleanup requires source pager");
    pager.rollback_tx(connection);

    if cleanup_state.vacuum_lock_held {
        let pager = connection
            .get_pager_from_database_index(&db)
            .expect("VACUUM cleanup requires source pager");
        if let Some(wal) = pager.wal.as_ref() {
            wal.release_vacuum_lock();
        }
    }

    connection.auto_commit.store(true, Ordering::SeqCst);
    connection.set_tx_state(crate::connection::TransactionState::None);
    drop(cleanup_state.mvcc_guard.take());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileId, FileSyncType};
    use crate::mvcc::yield_hooks::YieldPointMarker;
    use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
    use crate::schema::Schema;
    use crate::storage::encryption::{CipherMode, EncryptionKey};
    use crate::storage::pager::Page;
    use crate::util::IOExt;
    use crate::vdbe::execute::TransactionYieldPoint;
    use crate::SqliteDialect;
    use crate::{
        Buffer, Clock, Completion, DatabaseOpts, File, MemoryIO, MonotonicInstant, OpenFlags,
        WallClockInstant, IO,
    };
    use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};

    fn page_ref(id: i64) -> crate::storage::pager::PageRef {
        Arc::new(Page::new(id))
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct SyncCounts {
        db: usize,
        wal: usize,
    }

    struct SyncCountingIo {
        inner: Arc<MemoryIO>,
        tracked_db_path: String,
        tracked_wal_path: String,
        db_syncs: Arc<AtomicUsize>,
        wal_syncs: Arc<AtomicUsize>,
    }

    impl SyncCountingIo {
        fn new(tracked_db_path: &str) -> Self {
            Self {
                inner: Arc::new(MemoryIO::new()),
                tracked_db_path: tracked_db_path.to_string(),
                tracked_wal_path: format!("{tracked_db_path}-wal"),
                db_syncs: Arc::new(AtomicUsize::new(0)),
                wal_syncs: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn counts(&self) -> SyncCounts {
            SyncCounts {
                db: self.db_syncs.load(StdOrdering::SeqCst),
                wal: self.wal_syncs.load(StdOrdering::SeqCst),
            }
        }
    }

    impl Clock for SyncCountingIo {
        fn current_time_monotonic(&self) -> MonotonicInstant {
            self.inner.current_time_monotonic()
        }

        fn current_time_wall_clock(&self) -> WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }

    impl IO for SyncCountingIo {
        fn open_file(&self, path: &str, flags: OpenFlags, direct: bool) -> Result<Arc<dyn File>> {
            let inner = self.inner.open_file(path, flags, direct)?;
            Ok(Arc::new(SyncCountingFile {
                inner,
                count_as_db_sync: path == self.tracked_db_path,
                count_as_wal_sync: path == self.tracked_wal_path,
                db_syncs: self.db_syncs.clone(),
                wal_syncs: self.wal_syncs.clone(),
            }))
        }

        fn remove_file(&self, path: &str) -> Result<()> {
            self.inner.remove_file(path)
        }

        fn step(&self) -> Result<()> {
            self.inner.step()
        }

        fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
            self.inner.drain_completions(completions)
        }

        fn cancel(&self, completions: &[Completion]) -> Result<()> {
            self.inner.cancel(completions)
        }

        fn file_id(&self, path: &str) -> Result<FileId> {
            self.inner.file_id(path)
        }
    }

    struct SyncCountingFile {
        inner: Arc<dyn File>,
        count_as_db_sync: bool,
        count_as_wal_sync: bool,
        db_syncs: Arc<AtomicUsize>,
        wal_syncs: Arc<AtomicUsize>,
    }

    #[derive(Debug)]
    struct OneShotTransactionYieldInjector {
        yielded: AtomicUsize,
    }

    impl OneShotTransactionYieldInjector {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                yielded: AtomicUsize::new(0),
            })
        }

        fn yielded(&self) -> bool {
            self.yielded.load(StdOrdering::SeqCst) != 0
        }
    }

    impl YieldInjector for OneShotTransactionYieldInjector {
        fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
            point == TransactionYieldPoint::BeforeStart.point()
                && self
                    .yielded
                    .compare_exchange(0, 1, StdOrdering::SeqCst, StdOrdering::SeqCst)
                    .is_ok()
        }
    }

    impl File for SyncCountingFile {
        fn lock_file(&self, exclusive: bool) -> Result<()> {
            self.inner.lock_file(exclusive)
        }

        fn unlock_file(&self) -> Result<()> {
            self.inner.unlock_file()
        }

        fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
            self.inner.pread(pos, c)
        }

        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.inner.pwrite(pos, buffer, c)
        }

        fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
            if self.count_as_db_sync {
                self.db_syncs.fetch_add(1, StdOrdering::SeqCst);
            }
            if self.count_as_wal_sync {
                self.wal_syncs.fetch_add(1, StdOrdering::SeqCst);
            }
            self.inner.sync(c, sync_type)
        }

        fn size(&self) -> Result<u64> {
            self.inner.size()
        }

        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    fn vacuum_copy_batch_ranges(total_pages: u32) -> Vec<(u32, u32)> {
        let mut ranges = Vec::new();
        let mut start = 1;
        while start <= total_pages {
            let end = next_vacuum_copy_batch_start(start, total_pages);
            ranges.push((start, end));
            start = end;
        }
        ranges
    }

    #[test]
    fn coalesce_frame_runs_single_page_is_one_run_of_one() {
        let runs = coalesce_frame_runs(vec![(page_ref(1), 42)]);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].0, 42);
        assert_eq!(runs[0].1.len(), 1);
        assert_eq!(runs[0].1[0].get().id(), 1);
    }

    #[test]
    fn coalesce_frame_runs_sorts_by_frame_id_then_coalesces_contiguous() {
        // Target-build writes frames out of page-id order. The input is
        // sorted by page id (1,2,3,4), but the frame ids interleave with a
        // gap at frame 3, producing two runs once sorted by frame id.
        let pairs = vec![
            (page_ref(1), 10),
            (page_ref(2), 11),
            (page_ref(3), 12),
            (page_ref(4), 14),
        ];
        let runs = coalesce_frame_runs(pairs);
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].0, 10);
        assert_eq!(runs[0].1.len(), 3);
        assert_eq!(
            runs[0].1.iter().map(|p| p.get().id()).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(runs[1].0, 14);
        assert_eq!(runs[1].1.len(), 1);
        assert_eq!(runs[1].1[0].get().id(), 4);
    }

    #[test]
    fn coalesce_frame_runs_handles_non_monotonic_page_to_frame_map() {
        // Realistic target-build shape: pages 1..=6 but frames assigned by
        // phase order (schema page goes last, indexes interleave). The
        // coalescer must still produce runs of length > 1 for contiguous
        // frame ranges rather than falling back to length-1 runs.
        let pairs = vec![
            (page_ref(1), 10), // schema written last (gap between 6 and 10)
            (page_ref(2), 1),  // data pages written first
            (page_ref(3), 2),
            (page_ref(4), 3),
            (page_ref(5), 5), // index pages later
            (page_ref(6), 6),
        ];
        let runs = coalesce_frame_runs(pairs);
        // After sort by frame id: frames 1,2,3 contiguous; 5,6 contiguous;
        // 10 isolated.
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0].0, 1);
        assert_eq!(runs[0].1.len(), 3);
        assert_eq!(runs[1].0, 5);
        assert_eq!(runs[1].1.len(), 2);
        assert_eq!(runs[2].0, 10);
        assert_eq!(runs[2].1.len(), 1);

        // Average run length should be non-trivial (>1).
        let total: usize = runs.iter().map(|(_, pages)| pages.len()).sum();
        let avg = total as f64 / runs.len() as f64;
        assert!(avg > 1.0, "expected average run length > 1, got {avg}");
    }

    #[test]
    fn coalesce_frame_runs_all_scattered_is_singleton_runs() {
        let pairs = vec![(page_ref(1), 10), (page_ref(2), 20), (page_ref(3), 30)];
        let runs = coalesce_frame_runs(pairs);
        assert_eq!(runs.len(), 3);
        for (_, pages) in &runs {
            assert_eq!(pages.len(), 1);
        }
    }

    #[test]
    fn coalesce_frame_runs_keeps_run_pages_in_physical_frame_order() {
        let runs = coalesce_frame_runs(vec![
            (page_ref(1), 12),
            (page_ref(2), 10),
            (page_ref(3), 11),
            (page_ref(4), 13),
        ]);

        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].0, 10);
        assert_eq!(
            runs[0].1.iter().map(|p| p.get().id()).collect::<Vec<_>>(),
            vec![2, 3, 1, 4]
        );
    }

    #[test]
    fn vacuum_copy_batch_ranges_cover_boundary_page_counts() {
        let size = VACUUM_COPY_BATCH_SIZE;
        assert!(vacuum_copy_batch_ranges(0).is_empty());
        assert_eq!(vacuum_copy_batch_ranges(1), vec![(1, 2)]);
        assert_eq!(vacuum_copy_batch_ranges(2), vec![(1, 3)]);
        assert_eq!(vacuum_copy_batch_ranges(size - 1), vec![(1, size)]);
        assert_eq!(vacuum_copy_batch_ranges(size), vec![(1, size + 1)]);
        assert_eq!(
            vacuum_copy_batch_ranges(size + 1),
            vec![(1, size + 1), (size + 1, size + 2)]
        );
        assert_eq!(
            vacuum_copy_batch_ranges(size * 2),
            vec![(1, size + 1), (size + 1, size * 2 + 1)]
        );
        assert_eq!(
            vacuum_copy_batch_ranges(size * 2 + 1),
            vec![
                (1, size + 1),
                (size + 1, size * 2 + 1),
                (size * 2 + 1, size * 2 + 2),
            ]
        );
    }

    #[test]
    fn vacuum_db_header_meta_bumps_schema_cookie_and_preserves_sqlite_metadata() {
        let mut source = DatabaseHeader::default();
        source.schema_cookie = u32::MAX.into();
        source.default_page_cache_size = CacheSize::new(123);
        source.text_encoding = TextEncoding::Utf8;
        source.user_version = 7.into();
        source.application_id = 12.into();

        let VacuumDbHeaderMeta {
            read_version,
            write_version,
            schema_cookie,
            default_page_cache_size,
            text_encoding,
            user_version,
            application_id,
        } = VacuumDbHeaderMeta::from_source_header(&source);

        assert_eq!(read_version, source.read_version);
        assert_eq!(write_version, source.write_version);
        assert_eq!(schema_cookie, 0);
        assert_eq!(default_page_cache_size, CacheSize::new(123));
        assert_eq!(text_encoding, TextEncoding::Utf8);
        assert_eq!(user_version, 7);
        assert_eq!(application_id, 12);
    }

    #[test]
    fn replace_shared_schema_after_vacuum_replaces_wrapped_schema_cookie() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;

        db.with_schema_mut(|schema| {
            schema.schema_version = u32::MAX;
            Ok(())
        })?;

        let replacement = Schema {
            generated_columns_enabled: true,
            schema_version: 0,
            ..Default::default()
        };
        replace_shared_schema_after_vacuum(db.as_ref(), Arc::new(replacement));

        let schema = db.clone_schema();
        assert_eq!(schema.schema_version, 0);
        assert!(schema.generated_columns_enabled);

        Ok(())
    }

    #[test]
    fn install_committed_vacuum_image_updates_connection_and_shared_schema() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
        let conn = db.connect()?;

        conn.with_schema_mut(|schema| {
            schema.schema_version = 1;
        })?;
        db.with_schema_mut(|schema| {
            schema.schema_version = 1;
            Ok(())
        })?;

        let mut header = DatabaseHeader::default();
        header.schema_cookie = 42.into();
        let committed = VacuumCommittedImageMeta {
            header,
            schema: Arc::new(Schema {
                generated_columns_enabled: true,
                schema_version: 42,
                ..Default::default()
            }),
        };

        install_committed_vacuum_image(&conn, crate::MAIN_DB_ID, &committed);

        assert_eq!(conn.schema.read().schema_version, 42);
        assert!(conn.schema.read().generated_columns_enabled);
        let shared = db.clone_schema();
        assert_eq!(shared.schema_version, 42);
        assert!(shared.generated_columns_enabled);

        Ok(())
    }

    #[test]
    fn cleanup_after_published_vacuum_installs_committed_image() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))?;
        let conn = db.connect()?;

        conn.with_schema_mut(|schema| {
            schema.schema_version = 1;
        })?;
        db.with_schema_mut(|schema| {
            schema.schema_version = 1;
            Ok(())
        })?;

        let mut header = DatabaseHeader::default();
        header.schema_cookie = 43.into();
        let committed = VacuumCommittedImageMeta {
            header,
            schema: Arc::new(Schema {
                generated_columns_enabled: true,
                schema_version: 43,
                ..Default::default()
            }),
        };

        vacuum_in_place_cleanup(
            &conn,
            crate::MAIN_DB_ID,
            VacuumInPlacePhase::Checkpoint,
            None,
            Some(committed),
            VacuumInPlaceCleanupState::default(),
        )?;

        assert_eq!(conn.schema.read().schema_version, 43);
        assert!(conn.schema.read().generated_columns_enabled);
        let shared = db.clone_schema();
        assert_eq!(shared.schema_version, 43);
        assert!(shared.generated_columns_enabled);

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn capture_target_metadata_uses_final_header_cookie_and_preserves_tvfs() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
        let source_dir = tempfile::tempdir().unwrap();
        let source_path = source_dir.path().join("source.db");
        let source_path = source_path.to_str().unwrap();
        let source_db = Database::open_file_with_flags(
            io,
            source_path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let source_conn = source_db.connect()?;
        // Source is uninitialized so its header reserved_space isn't usable.
        // Derive from the IOContext to keep reserved_space and the auto-
        // installed checksum context consistent under `feature = "checksum"`.
        let reserved_space = source_conn
            .get_pager()
            .io_ctx
            .read()
            .get_reserved_space_bytes();
        let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, reserved_space)?;

        temp.conn.execute("BEGIN IMMEDIATE")?;
        temp.conn
            .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
        temp.conn.execute("CREATE INDEX idx_t_v ON t(v)")?;
        temp.conn.execute("INSERT INTO t VALUES (1, 'x')")?;
        let mvcc_version = RawVersion::from(crate::storage::sqlite3_ondisk::Version::Mvcc);
        let header_meta = VacuumDbHeaderMeta {
            read_version: mvcc_version,
            write_version: mvcc_version,
            schema_cookie: 42,
            default_page_cache_size: CacheSize::new(321),
            text_encoding: TextEncoding::Utf8,
            user_version: 17,
            application_id: 29,
        };
        match finalize_vacuum_target_header(&temp.conn, &header_meta)? {
            crate::IOResult::Done(()) => {}
            crate::IOResult::IO(_) => panic!("fresh temp header should not need async I/O"),
        }
        temp.conn.execute("COMMIT")?;

        let committed = capture_target_metadata(&temp, FxHashMap::default())?;

        assert_eq!(committed.header.schema_cookie.get(), 42);
        assert_eq!(committed.header.read_version, mvcc_version);
        assert_eq!(committed.header.write_version, mvcc_version);
        assert_eq!(committed.schema.schema_version, 42);
        assert!(
            committed.schema.tables.contains_key("generate_series"),
            "captured schema must retain built-in table-valued functions"
        );
        let table_root = match committed.schema.tables.get("t").expect("table t").as_ref() {
            crate::schema::Table::BTree(btree) => btree.root_page,
            _ => panic!("expected btree table"),
        };
        assert!(table_root > 0);
        let index_root = committed
            .schema
            .indexes
            .get("t")
            .and_then(|indexes| indexes.front())
            .map(|index| index.root_page)
            .expect("index idx_t_v");
        assert!(index_root > 0);

        Ok(())
    }

    #[test]
    fn finalize_vacuum_into_output_syncs_destination_db_file() -> Result<()> {
        let io = Arc::new(SyncCountingIo::new("vacuum-into-output.db"));
        let io_dyn: Arc<dyn IO> = io.clone();
        let db = Database::open_file_with_flags(
            io_dyn,
            "vacuum-into-output.db",
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect()?;
        conn.set_sync_mode(crate::SyncMode::Off);
        conn.wal_auto_actions_disable();

        conn.execute("BEGIN IMMEDIATE")?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
        conn.execute("INSERT INTO t VALUES (1, 'x')")?;
        conn.execute("COMMIT")?;

        let before = io.counts();
        assert_eq!(
            before.db, 1,
            "creating the database does exactly one db fsync (page 1 must be durable \
             before the first WAL commit, even with synchronous=OFF); the target \
             build must not add more before finalization"
        );

        finalize_vacuum_into_output(&VacuumTargetBuildContext::new(conn))?;

        let after = io.counts();
        assert_eq!(
            after.db - before.db,
            1,
            "VACUUM INTO finalization must do exactly one destination DB fsync"
        );
        assert_eq!(
            after.wal - before.wal,
            2,
            "VACUUM INTO finalization must do one WAL fsync before backfill (the \
             checkpoint crash-atomicity barrier; finalization forces synchronous=FULL) \
             and one after truncation"
        );

        Ok(())
    }

    #[test]
    fn vacuum_target_build_preserves_nested_statement_explicit_yield() -> Result<()> {
        let io = Arc::new(MemoryIO::new());
        let source_io: Arc<dyn IO> = io.clone();
        let source_db = Database::open_file_with_flags(
            source_io,
            "vacuum-yield-source.db",
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(SqliteDialect),
        )?;
        let source_conn = source_db.connect()?;
        source_conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")?;
        source_conn.execute("CREATE INDEX idx_t_payload ON t(payload)")?;
        for id in 1..=8 {
            source_conn.execute(format!("INSERT INTO t VALUES({id}, 'payload-{id}')"))?;
        }
        source_conn.execute("BEGIN")?;

        let source_pager = source_conn.get_pager();
        let header_meta = source_pager
            .io
            .block(|| source_pager.with_header(VacuumDbHeaderMeta::from_source_header))?;

        let target_io: Arc<dyn IO> = io.clone();
        let target_db = Database::open_file_with_flags(
            target_io,
            "vacuum-yield-target.db",
            OpenFlags::Create,
            vacuum_target_opts_from_source(&source_db),
            None,
            Arc::new(SqliteDialect),
        )?;
        let target_conn = target_db.connect()?;
        mirror_symbols(&source_conn, &target_conn);

        let config = VacuumTargetBuildConfig {
            source_conn: source_conn.clone(),
            escaped_schema_name: "main".to_string(),
            source_db_id: crate::MAIN_DB_ID,
            header_meta,
            source_custom_types: capture_custom_types(&source_conn, crate::MAIN_DB_ID),
            target_mvcc_enabled: source_db.mvcc_enabled(),
            target_auto_vacuum_mode: source_pager.get_auto_vacuum_mode(),
            copy_mvcc_metadata_table: false,
        };

        let injector = OneShotTransactionYieldInjector::new();
        source_conn.set_yield_injector(Some(injector.clone()));

        let mut context = VacuumTargetBuildContext::new(target_conn.clone());
        let mut saw_explicit_yield = false;
        loop {
            match vacuum_target_build_step(&config, &mut context)? {
                crate::IOResult::Done(()) => break,
                crate::IOResult::IO(completions) => {
                    saw_explicit_yield |= completions.is_explicit_yield();
                    if !completions.is_explicit_yield() {
                        completions.wait(io.as_ref())?;
                    }
                }
            }
        }

        source_conn.set_yield_injector(None);
        source_conn.execute("COMMIT")?;

        assert!(injector.yielded(), "test must trigger the injected yield");
        assert!(
            saw_explicit_yield,
            "vacuum target build must return the nested explicit yield instead of panicking"
        );

        let mut copied_rows = None;
        let mut stmt = target_conn.prepare("SELECT COUNT(*) FROM t")?;
        stmt.run_with_row_callback(|row| {
            copied_rows = Some(row.get::<i64>(0)?);
            Ok(())
        })?;
        assert_eq!(copied_rows, Some(8));

        Ok(())
    }

    #[test]
    fn in_place_vacuum_with_sync_off_syncs_source_db_once_and_wal_twice() -> Result<()> {
        let io = Arc::new(SyncCountingIo::new("vacuum-source.db"));
        let io_dyn: Arc<dyn IO> = io.clone();
        let db = Database::open_file_with_flags(
            io_dyn,
            "vacuum-source.db",
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect()?;
        conn.set_sync_mode(crate::SyncMode::Off);

        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
        conn.execute("INSERT INTO t VALUES (1, 'x')")?;
        conn.execute("INSERT INTO t VALUES (2, 'y')")?;

        let before = io.counts();
        assert_eq!(
            before.db, 1,
            "creating the database does exactly one source db fsync (page 1 must be \
             durable before the first WAL commit, even with synchronous=OFF); \
             ordinary writes must not add more"
        );

        conn.execute("VACUUM")?;

        let after = io.counts();
        assert_eq!(
            after.db - before.db,
            1,
            "in-place VACUUM must do exactly one source DB fsync before truncating WAL"
        );
        assert_eq!(
            after.wal - before.wal,
            2,
            "in-place VACUUM with synchronous=OFF must do one WAL fsync before backfill \
             (the checkpoint crash-atomicity barrier; VACUUM forces the checkpoint to \
             synchronous=FULL) and one after truncation"
        );

        Ok(())
    }

    #[test]
    fn in_place_vacuum_with_sync_full_adds_pre_publish_wal_sync() -> Result<()> {
        let io = Arc::new(SyncCountingIo::new("vacuum-source-full.db"));
        let io_dyn: Arc<dyn IO> = io.clone();
        let db = Database::open_file_with_flags(
            io_dyn,
            "vacuum-source-full.db",
            OpenFlags::Create,
            DatabaseOpts::new().with_vacuum(true),
            None,
            Arc::new(SqliteDialect),
        )?;
        let conn = db.connect()?;
        conn.set_sync_mode(crate::SyncMode::Off);
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
        conn.execute("INSERT INTO t VALUES (1, 'x')")?;
        conn.execute("INSERT INTO t VALUES (2, 'y')")?;

        conn.set_sync_mode(crate::SyncMode::Full);
        let before = io.counts();

        conn.execute("VACUUM")?;

        let after = io.counts();
        assert_eq!(
            after.db - before.db,
            1,
            "in-place VACUUM must still do exactly one source DB fsync under synchronous=FULL"
        );
        assert_eq!(
            after.wal - before.wal,
            3,
            "in-place VACUUM with synchronous=FULL must do one WAL fsync before publish, \
             one before backfill (the checkpoint crash-atomicity barrier), and one after \
             truncation"
        );

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn vacuum_db_header_meta_updates_target_header() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
        let source_dir = tempfile::tempdir().unwrap();
        let source_path = source_dir.path().join("source.db");
        let source_path = source_path.to_str().unwrap();
        let source_db = Database::open_file_with_flags(
            io,
            source_path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let source_conn = source_db.connect()?;
        let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, 0)?;

        let mut source_header = DatabaseHeader::default();
        source_header.schema_cookie = 41.into();
        source_header.default_page_cache_size = CacheSize::new(321);
        source_header.text_encoding = TextEncoding::Utf8;
        source_header.user_version = 17.into();
        source_header.application_id = 29.into();
        let header_meta = VacuumDbHeaderMeta::from_source_header(&source_header);

        temp.conn.execute("BEGIN")?;
        match finalize_vacuum_target_header(&temp.conn, &header_meta)? {
            crate::IOResult::Done(()) => {}
            crate::IOResult::IO(_) => panic!("fresh temp header should not need async I/O"),
        }
        temp.conn.execute("COMMIT")?;

        let pager = temp.conn.pager.load();
        let header = pager.io.block(|| {
            pager.with_header(|header| {
                (
                    header.schema_cookie.get(),
                    header.default_page_cache_size,
                    header.text_encoding,
                    header.user_version.get(),
                    header.application_id.get(),
                )
            })
        })?;

        assert_eq!(
            header,
            (42, CacheSize::new(321), TextEncoding::Utf8, 17, 29)
        );

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn internal_vacuum_temp_db_uses_source_runtime_and_disables_auto_checkpoint() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
        let source_dir = tempfile::tempdir().unwrap();
        let source_path = source_dir.path().join("source.db");
        let source_path = source_path.to_str().unwrap();
        let source_db = Database::open_file_with_flags(
            io,
            source_path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let source_conn = source_db.connect()?;

        let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, 0)?;

        assert!(Arc::ptr_eq(&temp._db.io, &source_db.io));
        assert_ne!(temp.path, source_db.path);
        assert_eq!(
            temp.conn.wal_auto_actions(),
            crate::storage::wal::WalAutoActions::empty()
        );
        assert_eq!(temp.conn.get_page_size().get(), 4096);
        assert_eq!(temp.conn.get_reserved_bytes(), Some(0));

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn internal_vacuum_temp_db_preserves_source_encryption() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
        let source_dir = tempfile::tempdir().unwrap();
        let source_path = source_dir.path().join("encrypted-source.db");
        let source_path = source_path.to_str().unwrap();
        let key_hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let key = EncryptionKey::from_hex_string(key_hex)?;
        let source_db = Database::open_file_with_flags(
            io,
            source_path,
            OpenFlags::Create,
            DatabaseOpts::new().with_encryption(true),
            Some(EncryptionOpts {
                cipher: CipherMode::Aes256Gcm.to_string(),
                hexkey: key_hex.to_string(),
            }),
            Arc::new(SqliteDialect),
        )?;
        let source_conn = source_db.connect_with_encryption(Some(key))?;
        let reserved_space = source_conn
            .get_reserved_bytes()
            .expect("encrypted source should have reserved bytes");

        let temp = open_vacuum_temp_db(&source_conn, &source_db, 4096, reserved_space)?;

        assert!(temp._db.experimental_encryption_enabled());
        assert_eq!(
            temp.conn.get_encryption_cipher_mode(),
            source_conn.get_encryption_cipher_mode()
        );
        assert!(temp.conn.encryption_key.read().is_some());
        assert_eq!(temp.conn.get_reserved_bytes(), Some(reserved_space));

        Ok(())
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn vacuum_target_build_cleanup_clears_target_connection_state() -> Result<()> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new()?);
        let target_dir = tempfile::tempdir().unwrap();
        let target_path = target_dir.path().join("target.db");
        let target_path = target_path.to_str().unwrap();
        let target_db = Database::open_file_with_flags(
            io,
            target_path,
            OpenFlags::Create,
            DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )?;
        let target_conn = target_db.connect()?;
        target_conn.execute("CREATE TABLE seed(x)")?;

        let mut context = VacuumTargetBuildContext::new(target_conn.clone());
        let helper_stmt = target_conn.prepare_internal("SELECT 1")?;
        context.phase = VacuumTargetBuildPhase::CollectSchemaRows {
            schema_stmt: Box::new(helper_stmt),
        };

        target_conn.execute("BEGIN IMMEDIATE")?;
        target_conn.execute("INSERT INTO seed VALUES (1)")?;
        let pager = target_conn.pager.load();

        assert!(!target_conn.get_auto_commit());
        assert!(target_conn.is_nested_stmt());

        context.cleanup_after_error()?;

        assert!(target_conn.get_auto_commit());
        assert_eq!(
            target_conn.get_tx_state(),
            crate::connection::TransactionState::None
        );
        assert!(!target_conn.is_nested_stmt());
        assert!(!pager.holds_read_lock());
        assert!(!pager.holds_write_lock());

        let mut stmt = target_conn.prepare("SELECT COUNT(*) FROM seed")?;
        let mut saw_row = false;
        stmt.run_with_row_callback(|_| {
            saw_row = true;
            Ok(())
        })?;
        assert!(
            saw_row,
            "target cleanup should leave the connection queryable"
        );

        Ok(())
    }
}
