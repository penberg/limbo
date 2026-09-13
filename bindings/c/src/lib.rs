#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]

use std::ffi::{self, CStr, CString};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tracing::trace;
use turso_core::dialect::sqlite::{SQLITE_VERSION, SQLITE_VERSION_NUMBER};
use turso_core::SqliteDialect;
use turso_core::{CheckpointMode, DatabaseOpts, LimboError, Value};
use turso_ext::ScalarFunction;
use turso_ext::Value as ExtValue;

/// Global flag: when set, all subsequently opened databases enable experimental features.
static EXPERIMENTAL_ENABLED: AtomicBool = AtomicBool::new(false);
static SQLITE_VERSION_C_STRING: OnceLock<CString> = OnceLock::new();

/// Global B-tree search counter exposed to the TCL test harness as
/// `sqlite_search_count`.
#[no_mangle]
#[allow(non_upper_case_globals)]
pub static mut sqlite3_search_count: ffi::c_int = 0;

/// Test-only export of core's SQLite varint encoder for the TCL harness's
/// btree_varint_test command (upstream test3.c). `buf` must have room for
/// 9 bytes. Returns the number of bytes written. Not part of the sqlite3
/// API.
#[no_mangle]
pub unsafe extern "C" fn turso_test_put_varint(buf: *mut u8, value: u64) -> ffi::c_int {
    let buf = unsafe { std::slice::from_raw_parts_mut(buf, 9) };
    turso_core::storage::sqlite3_ondisk::write_varint(buf, value) as ffi::c_int
}

/// Test-only export of core's SQLite varint decoder, the counterpart of
/// [`turso_test_put_varint`]. `buf` must hold at least 9 readable bytes.
/// Returns the number of bytes consumed and stores the decoded value in
/// `*out`, or returns 0 if the bytes are not a valid varint.
#[no_mangle]
pub unsafe extern "C" fn turso_test_get_varint(buf: *const u8, out: *mut u64) -> ffi::c_int {
    let buf = unsafe { std::slice::from_raw_parts(buf, 9) };
    match turso_core::storage::sqlite3_ondisk::read_varint(buf) {
        Ok((value, n)) => {
            unsafe { *out = value };
            n as ffi::c_int
        }
        Err(_) => 0,
    }
}

/// Enable all experimental features for databases opened after this call.
#[no_mangle]
pub extern "C" fn turso_enable_experimental() {
    EXPERIMENTAL_ENABLED.store(true, Ordering::Release);
}

/// Build `DatabaseOpts` respecting the global experimental flag.
fn default_db_opts() -> DatabaseOpts {
    let mut opts = DatabaseOpts::new();
    if EXPERIMENTAL_ENABLED.load(Ordering::Acquire) {
        opts = opts
            .with_generated_columns(true)
            .with_vacuum(true)
            .with_without_rowid(true)
            .with_attach(true);
    }
    opts
}

macro_rules! stub {
    () => {
        todo!("{} is not implemented", stringify!($fn));
    };
}

/* generic error-codes */
pub const SQLITE_OK: ffi::c_int = 0; /* Successful result */
pub const SQLITE_ERROR: ffi::c_int = 1; /* Generic error */
pub const SQLITE_INTERNAL: ffi::c_int = 2; /* Internal logic error in SQLite */
pub const SQLITE_PERM: ffi::c_int = 3; /* Access permission denied */
pub const SQLITE_ABORT: ffi::c_int = 4; /* Callback routine requested an abort */
pub const SQLITE_BUSY: ffi::c_int = 5; /* The database file is locked */
pub const SQLITE_LOCKED: ffi::c_int = 6; /* A table in the database is locked */
pub const SQLITE_NOMEM: ffi::c_int = 7; /* A malloc() failed */
pub const SQLITE_READONLY: ffi::c_int = 8; /* Attempt to write a readonly database */
pub const SQLITE_INTERRUPT: ffi::c_int = 9; /* Operation terminated by sqlite3_interrupt()*/
pub const SQLITE_IOERR: ffi::c_int = 10; /* Some kind of disk I/O error occurred */
pub const SQLITE_CORRUPT: ffi::c_int = 11; /* The database disk image is malformed */
pub const SQLITE_NOTFOUND: ffi::c_int = 12; /* Unknown opcode in sqlite3_file_control() */
pub const SQLITE_FULL: ffi::c_int = 13; /* Insertion failed because database is full */
pub const SQLITE_CANTOPEN: ffi::c_int = 14; /* Unable to open the database file */
pub const SQLITE_PROTOCOL: ffi::c_int = 15; /* Database lock protocol error */
pub const SQLITE_EMPTY: ffi::c_int = 16; /* Internal use only */
pub const SQLITE_SCHEMA: ffi::c_int = 17; /* The database schema changed */
pub const SQLITE_TOOBIG: ffi::c_int = 18; /* String or BLOB exceeds size limit */
pub const SQLITE_CONSTRAINT: ffi::c_int = 19; /* Abort due to constraint violation */
pub const SQLITE_MISMATCH: ffi::c_int = 20; /* Data type mismatch */
pub const SQLITE_MISUSE: ffi::c_int = 21; /* Library used incorrectly */
pub const SQLITE_NOLFS: ffi::c_int = 22; /* Uses OS features not supported on host */
pub const SQLITE_AUTH: ffi::c_int = 23; /* Authorization denied */
pub const SQLITE_FORMAT: ffi::c_int = 24; /* Not used */
pub const SQLITE_RANGE: ffi::c_int = 25; /* 2nd parameter to sqlite3_bind out of range */
pub const SQLITE_NOTADB: ffi::c_int = 26; /* File opened that is not a database file */
pub const SQLITE_NOTICE: ffi::c_int = 27; /* Notifications from sqlite3_log() */
pub const SQLITE_WARNING: ffi::c_int = 28; /* Warnings from sqlite3_log() */
pub const SQLITE_ROW: ffi::c_int = 100; /* sqlite3_step() has another row ready */
pub const SQLITE_DONE: ffi::c_int = 101; /* sqlite3_step() has finished executing */

/* extended error-codes */
pub const SQLITE_ABORT_ROLLBACK: ffi::c_int = SQLITE_ABORT | (2 << 8);

pub const SQLITE_STATE_OPEN: u8 = 0x76;
pub const SQLITE_STATE_SICK: u8 = 0xba;
pub const SQLITE_STATE_BUSY: u8 = 0x6d;
pub const SQLITE_STATE_ZOMBIE: u8 = 0xa7;

pub const SQLITE_CHECKPOINT_PASSIVE: ffi::c_int = 0;
pub const SQLITE_CHECKPOINT_FULL: ffi::c_int = 1;
pub const SQLITE_CHECKPOINT_RESTART: ffi::c_int = 2;
pub const SQLITE_CHECKPOINT_TRUNCATE: ffi::c_int = 3;

pub const SQLITE_INTEGER: ffi::c_int = 1;
pub const SQLITE_FLOAT: ffi::c_int = 2;
pub const SQLITE_TEXT: ffi::c_int = 3;
pub const SQLITE_BLOB: ffi::c_int = 4;
pub const SQLITE_NULL: ffi::c_int = 5;
pub const SQLITE_STMTSTATUS_FULLSCAN_STEP: ffi::c_int = 1;
pub const SQLITE_STMTSTATUS_SORT: ffi::c_int = 2;
pub const SQLITE_STMTSTATUS_AUTOINDEX: ffi::c_int = 3;
pub const SQLITE_STMTSTATUS_VM_STEP: ffi::c_int = 4;
pub const SQLITE_STMTSTATUS_REPREPARE: ffi::c_int = 5;
pub const SQLITE_STMTSTATUS_RUN: ffi::c_int = 6;
pub const SQLITE_STMTSTATUS_FILTER_MISS: ffi::c_int = 7;
pub const SQLITE_STMTSTATUS_FILTER_HIT: ffi::c_int = 8;
pub const SQLITE_STMTSTATUS_MEMUSED: ffi::c_int = 99;
pub const LIBSQL_STMTSTATUS_BASE: ffi::c_int = 1024;
pub const LIBSQL_STMTSTATUS_ROWS_READ: ffi::c_int = LIBSQL_STMTSTATUS_BASE + 1;
pub const LIBSQL_STMTSTATUS_ROWS_WRITTEN: ffi::c_int = LIBSQL_STMTSTATUS_BASE + 2;

/* authorizer callback return codes */
pub const SQLITE_DENY: ffi::c_int = 1;
pub const SQLITE_IGNORE: ffi::c_int = 2;

/* authorizer action codes */
pub const SQLITE_COPY: ffi::c_int = 0;
pub const SQLITE_CREATE_INDEX: ffi::c_int = 1;
pub const SQLITE_CREATE_TABLE: ffi::c_int = 2;
pub const SQLITE_CREATE_TEMP_INDEX: ffi::c_int = 3;
pub const SQLITE_CREATE_TEMP_TABLE: ffi::c_int = 4;
pub const SQLITE_CREATE_TEMP_TRIGGER: ffi::c_int = 5;
pub const SQLITE_CREATE_TEMP_VIEW: ffi::c_int = 6;
pub const SQLITE_CREATE_TRIGGER: ffi::c_int = 7;
pub const SQLITE_CREATE_VIEW: ffi::c_int = 8;
pub const SQLITE_DELETE: ffi::c_int = 9;
pub const SQLITE_DROP_INDEX: ffi::c_int = 10;
pub const SQLITE_DROP_TABLE: ffi::c_int = 11;
pub const SQLITE_DROP_TEMP_INDEX: ffi::c_int = 12;
pub const SQLITE_DROP_TEMP_TABLE: ffi::c_int = 13;
pub const SQLITE_DROP_TEMP_TRIGGER: ffi::c_int = 14;
pub const SQLITE_DROP_TEMP_VIEW: ffi::c_int = 15;
pub const SQLITE_DROP_TRIGGER: ffi::c_int = 16;
pub const SQLITE_DROP_VIEW: ffi::c_int = 17;
pub const SQLITE_INSERT: ffi::c_int = 18;
pub const SQLITE_PRAGMA: ffi::c_int = 19;
pub const SQLITE_READ: ffi::c_int = 20;
pub const SQLITE_SELECT: ffi::c_int = 21;
pub const SQLITE_TRANSACTION: ffi::c_int = 22;
pub const SQLITE_UPDATE: ffi::c_int = 23;
pub const SQLITE_ATTACH: ffi::c_int = 24;
pub const SQLITE_DETACH: ffi::c_int = 25;
pub const SQLITE_ALTER_TABLE: ffi::c_int = 26;
pub const SQLITE_REINDEX: ffi::c_int = 27;
pub const SQLITE_ANALYZE: ffi::c_int = 28;
pub const SQLITE_CREATE_VTABLE: ffi::c_int = 29;
pub const SQLITE_DROP_VTABLE: ffi::c_int = 30;
pub const SQLITE_FUNCTION: ffi::c_int = 31;
pub const SQLITE_SAVEPOINT: ffi::c_int = 32;
pub const SQLITE_RECURSIVE: ffi::c_int = 33;

/* sqlite3_db_config operations */
pub const SQLITE_DBCONFIG_MAINDBNAME: ffi::c_int = 1000;
pub const SQLITE_DBCONFIG_LOOKASIDE: ffi::c_int = 1001;
pub const SQLITE_DBCONFIG_ENABLE_FKEY: ffi::c_int = 1002;
pub const SQLITE_DBCONFIG_ENABLE_TRIGGER: ffi::c_int = 1003;
pub const SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER: ffi::c_int = 1004;
pub const SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION: ffi::c_int = 1005;
pub const SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE: ffi::c_int = 1006;
pub const SQLITE_DBCONFIG_ENABLE_QPSG: ffi::c_int = 1007;
pub const SQLITE_DBCONFIG_TRIGGER_EQP: ffi::c_int = 1008;
pub const SQLITE_DBCONFIG_RESET_DATABASE: ffi::c_int = 1009;
pub const SQLITE_DBCONFIG_DEFENSIVE: ffi::c_int = 1010;
pub const SQLITE_DBCONFIG_WRITABLE_SCHEMA: ffi::c_int = 1011;
pub const SQLITE_DBCONFIG_LEGACY_ALTER_TABLE: ffi::c_int = 1012;
pub const SQLITE_DBCONFIG_DQS_DML: ffi::c_int = 1013;
pub const SQLITE_DBCONFIG_DQS_DDL: ffi::c_int = 1014;
pub const SQLITE_DBCONFIG_ENABLE_VIEW: ffi::c_int = 1015;
pub const SQLITE_DBCONFIG_LEGACY_FILE_FORMAT: ffi::c_int = 1016;
pub const SQLITE_DBCONFIG_TRUSTED_SCHEMA: ffi::c_int = 1017;

pub struct sqlite3 {
    pub(crate) inner: Mutex<sqlite3Inner>,
}

struct sqlite3Inner {
    pub(crate) _io: Arc<dyn turso_core::IO>,
    pub(crate) _db: Arc<turso_core::Database>,
    pub(crate) conn: Arc<turso_core::Connection>,
    pub(crate) err_code: ffi::c_int,
    pub(crate) err_mask: ffi::c_int,
    pub(crate) malloc_failed: bool,
    pub(crate) e_open_state: u8,
    pub(crate) p_err: *mut ffi::c_void,
    pub(crate) filename: CString,
    pub(crate) stmt_list: *mut sqlite3_stmt,
    /// Set when the caller opened "" and we created a private on-disk
    /// database for it; the file is removed when the handle goes away.
    pub(crate) temp_path: Option<std::path::PathBuf>,
}

impl Drop for sqlite3Inner {
    fn drop(&mut self) {
        // Run the engine's close so the last connection on a database
        // checkpoints its WAL, as SQLite does when a connection closes.
        let _ = self.conn.close();
        if let Some(path) = &self.temp_path {
            let base = path.to_string_lossy().into_owned();
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{base}{suffix}"));
            }
        }
    }
}

impl sqlite3 {
    pub fn new(
        io: Arc<dyn turso_core::IO>,
        db: Arc<turso_core::Database>,
        conn: Arc<turso_core::Connection>,
        filename: CString,
    ) -> Self {
        let inner = sqlite3Inner {
            _io: io,
            _db: db,
            conn,
            err_code: SQLITE_OK,
            // Extended result codes are disabled by default, as in SQLite;
            // sqlite3_extended_result_codes() widens the mask.
            err_mask: 0xff,
            malloc_failed: false,
            e_open_state: SQLITE_STATE_OPEN,
            p_err: std::ptr::null_mut(),
            filename,
            stmt_list: std::ptr::null_mut(),
            temp_path: None,
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    fn into_raw(self) -> *mut sqlite3 {
        #[allow(clippy::arc_with_non_send_sync)]
        let handle = Arc::new(self);
        Arc::into_raw(handle) as *mut sqlite3
    }

    unsafe fn clone_from_raw(db: *mut sqlite3) -> Arc<sqlite3> {
        Arc::increment_strong_count(db);
        Arc::from_raw(db)
    }
}

pub struct sqlite3_stmt {
    pub(crate) db: Arc<sqlite3>,
    pub(crate) stmt: turso_core::Statement,
    /// NUL-terminated copy of the statement's SQL, backing sqlite3_sql():
    /// the returned pointer must stay valid until finalize.
    pub(crate) sql: CString,
    /// Parameter names handed out by sqlite3_bind_parameter_name, which
    /// must stay valid until finalize.
    pub(crate) param_name_cache: Vec<(ffi::c_int, CString)>,
    pub(crate) destructors: Vec<(
        usize,
        Option<unsafe extern "C" fn(*mut ffi::c_void)>,
        *mut ffi::c_void,
    )>,
    pub(crate) next: *mut sqlite3_stmt,
    pub(crate) text_cache: Vec<Vec<u8>>,
    /// Cached ExtValue instances for sqlite3_column_value().
    /// Populated lazily per column; cleared on each step/reset.
    pub(crate) value_cache: Vec<Option<ExtValue>>,
    /// High-water mark of `search_count` already published to the global
    /// counter, so each step contributes only the delta.
    pub(crate) prev_search_count: i64,
}

impl sqlite3_stmt {
    pub fn new(db: Arc<sqlite3>, stmt: turso_core::Statement) -> Self {
        let n_cols = stmt.num_columns();
        let sql = CString::new(stmt.get_sql()).unwrap_or_default();
        Self {
            db,
            stmt,
            sql,
            param_name_cache: Vec::new(),
            destructors: Vec::new(),
            next: std::ptr::null_mut(),
            text_cache: vec![vec![]; n_cols],
            value_cache: (0..n_cols).map(|_| None).collect(),
            prev_search_count: 0,
        }
    }
    #[inline]
    fn clear_text_cache(&mut self) {
        // Drop per-column buffers for the previous row
        for r in &mut self.text_cache {
            r.clear();
        }
        // Drop cached ExtValues for the previous row
        for v in &mut self.value_cache {
            *v = None;
        }
    }
}

// ===== Custom SQL function registration infrastructure =====

/// Context passed to scalar function callbacks registered via sqlite3_create_function_v2.
/// Exposed as an opaque `void*` to C callers.
pub struct SqliteContext {
    pub(crate) result: ExtValue,
    pub(crate) p_app: *mut ffi::c_void,
    pub(crate) db: *mut sqlite3,
}

// SAFETY: SqliteContext is only used single-threaded within a function call.
unsafe impl Send for SqliteContext {}

struct FuncSlot {
    x_func: unsafe extern "C" fn(*mut ffi::c_void, ffi::c_int, *mut *mut ffi::c_void),
    p_app: usize,   // *mut c_void stored as usize for Send
    destroy: usize, // Option<unsafe extern "C" fn(*mut c_void)> stored as usize for Send
    name: String,
    db: usize, // *mut sqlite3 stored as usize for Send
}

// SAFETY: p_app lifetime is the caller's responsibility (same as SQLite C API).
unsafe impl Send for FuncSlot {}

const MAX_CUSTOM_FUNCS: usize = 32;

static FUNC_SLOTS: OnceLock<Mutex<[Option<FuncSlot>; MAX_CUSTOM_FUNCS]>> = OnceLock::new();

fn func_slots() -> &'static Mutex<[Option<FuncSlot>; MAX_CUSTOM_FUNCS]> {
    FUNC_SLOTS.get_or_init(|| Mutex::new(std::array::from_fn(|_| None)))
}

unsafe fn dispatch_func_bridge(slot_id: usize, argc: i32, argv: *const ExtValue) -> ExtValue {
    let (x_func, p_app, db) = {
        let slots = func_slots().lock().unwrap();
        match slots[slot_id].as_ref() {
            Some(s) => (s.x_func, s.p_app, s.db),
            None => return ExtValue::null(),
        }
    };

    // Build array of *mut c_void each pointing into the argv slice.
    // The C callback reads these via sqlite3_value_* functions.
    let mut arg_ptrs: Vec<*mut ffi::c_void> = (0..argc as usize)
        .map(|i| argv.add(i) as *mut ffi::c_void)
        .collect();

    let mut ctx = SqliteContext {
        result: ExtValue::null(),
        p_app: p_app as *mut ffi::c_void,
        db: db as *mut sqlite3,
    };

    x_func(
        &mut ctx as *mut SqliteContext as *mut ffi::c_void,
        argc,
        arg_ptrs.as_mut_ptr(),
    );

    ctx.result
}

// 32 pre-generated bridge functions — one per slot.
// Each bridges turso_core's ScalarFunction ABI to the C sqlite3_create_function_v2 callback.
macro_rules! func_bridge {
    ($id:literal, $name:ident) => {
        unsafe extern "C" fn $name(
            _context: usize,
            argc: i32,
            argv: *const ExtValue,
            _context_destructor: Option<turso_ext::ContextDestructor>,
            _value_destructor: Option<turso_ext::ValueDestructor>,
        ) -> ExtValue {
            dispatch_func_bridge($id, argc, argv)
        }
    };
}

func_bridge!(0, func_bridge_0);
func_bridge!(1, func_bridge_1);
func_bridge!(2, func_bridge_2);
func_bridge!(3, func_bridge_3);
func_bridge!(4, func_bridge_4);
func_bridge!(5, func_bridge_5);
func_bridge!(6, func_bridge_6);
func_bridge!(7, func_bridge_7);
func_bridge!(8, func_bridge_8);
func_bridge!(9, func_bridge_9);
func_bridge!(10, func_bridge_10);
func_bridge!(11, func_bridge_11);
func_bridge!(12, func_bridge_12);
func_bridge!(13, func_bridge_13);
func_bridge!(14, func_bridge_14);
func_bridge!(15, func_bridge_15);
func_bridge!(16, func_bridge_16);
func_bridge!(17, func_bridge_17);
func_bridge!(18, func_bridge_18);
func_bridge!(19, func_bridge_19);
func_bridge!(20, func_bridge_20);
func_bridge!(21, func_bridge_21);
func_bridge!(22, func_bridge_22);
func_bridge!(23, func_bridge_23);
func_bridge!(24, func_bridge_24);
func_bridge!(25, func_bridge_25);
func_bridge!(26, func_bridge_26);
func_bridge!(27, func_bridge_27);
func_bridge!(28, func_bridge_28);
func_bridge!(29, func_bridge_29);
func_bridge!(30, func_bridge_30);
func_bridge!(31, func_bridge_31);

static FUNC_BRIDGES: [ScalarFunction; MAX_CUSTOM_FUNCS] = [
    func_bridge_0,
    func_bridge_1,
    func_bridge_2,
    func_bridge_3,
    func_bridge_4,
    func_bridge_5,
    func_bridge_6,
    func_bridge_7,
    func_bridge_8,
    func_bridge_9,
    func_bridge_10,
    func_bridge_11,
    func_bridge_12,
    func_bridge_13,
    func_bridge_14,
    func_bridge_15,
    func_bridge_16,
    func_bridge_17,
    func_bridge_18,
    func_bridge_19,
    func_bridge_20,
    func_bridge_21,
    func_bridge_22,
    func_bridge_23,
    func_bridge_24,
    func_bridge_25,
    func_bridge_26,
    func_bridge_27,
    func_bridge_28,
    func_bridge_29,
    func_bridge_30,
    func_bridge_31,
];

// ===== End custom function infrastructure =====

static INIT_DONE: std::sync::Once = std::sync::Once::new();

#[no_mangle]
pub unsafe extern "C" fn sqlite3_initialize() -> ffi::c_int {
    INIT_DONE.call_once(|| {
        // Use try_init() instead of init() to avoid panicking if a global
        // subscriber is already installed (e.g., by the embedding application
        // or test harness). A panic here poisons the Once, causing all
        // subsequent sqlite3_initialize calls to abort (panic in extern "C").
        let _ = tracing_subscriber::fmt::try_init();
    });
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_shutdown() -> ffi::c_int {
    SQLITE_OK
}

#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub unsafe extern "C" fn sqlite3_open(
    filename: *const ffi::c_char,
    db_out: *mut *mut sqlite3,
) -> ffi::c_int {
    trace!("sqlite3_open");
    let rc = sqlite3_initialize();
    if rc != SQLITE_OK {
        return rc;
    }
    if filename.is_null() {
        return SQLITE_MISUSE;
    }
    if db_out.is_null() {
        return SQLITE_MISUSE;
    }
    let filename_cstr = CStr::from_ptr(filename);
    let filename_str = match filename_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let io: Arc<dyn turso_core::IO> = match filename_str {
        ":memory:" => Arc::new(turso_core::MemoryIO::new()),
        _ => match turso_core::PlatformIO::new() {
            Ok(io) => Arc::new(io),
            Err(_) => return SQLITE_CANTOPEN,
        },
    };
    match turso_core::Database::open_file_with_flags(
        io.clone(),
        filename_str,
        turso_core::OpenFlags::default(),
        default_db_opts(),
        None,
        Arc::new(SqliteDialect),
    ) {
        Ok(db) => {
            let conn = db.connect().unwrap();
            let filename = match filename_str {
                ":memory:" => CString::new("".to_string()).unwrap(),
                _ => CString::from(filename_cstr),
            };
            *db_out = sqlite3::new(io, db, conn, filename).into_raw();
            SQLITE_OK
        }
        Err(e) => {
            trace!("error opening database {}: {:?}", filename_str, e);
            SQLITE_CANTOPEN
        }
    }
}

/// Flags for sqlite3_open_v2
pub const SQLITE_OPEN_READONLY: ffi::c_int = 0x00000001;
pub const SQLITE_OPEN_READWRITE: ffi::c_int = 0x00000002;
pub const SQLITE_OPEN_CREATE: ffi::c_int = 0x00000004;
pub const SQLITE_OPEN_URI: ffi::c_int = 0x00000040;
pub const SQLITE_OPEN_MEMORY: ffi::c_int = 0x00000080;
pub const SQLITE_OPEN_NOMUTEX: ffi::c_int = 0x00008000;
pub const SQLITE_OPEN_FULLMUTEX: ffi::c_int = 0x00010000;
pub const SQLITE_OPEN_SHAREDCACHE: ffi::c_int = 0x00020000;
pub const SQLITE_OPEN_PRIVATECACHE: ffi::c_int = 0x00040000;
pub const SQLITE_OPEN_NOFOLLOW: ffi::c_int = 0x01000000;

/// Percent-decode a URI component (e.g., `%20` -> ` `, `%2F` -> `/`).
/// Returns None if a percent sequence is malformed or the result is not valid UTF-8.
fn percent_decode(input: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(input.len());
    let mut iter = input.as_bytes().iter();
    while let Some(&b) = iter.next() {
        if b == b'%' {
            let hi = *iter.next()?;
            let lo = *iter.next()?;
            let hex = [hi, lo];
            let s = std::str::from_utf8(&hex).ok()?;
            bytes.push(u8::from_str_radix(s, 16).ok()?);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).ok()
}

/// Parse a URI filename (when SQLITE_OPEN_URI is set).
/// Returns (path, is_memory) where path is the file path extracted from the URI
/// and is_memory indicates whether the database should be in-memory.
///
/// Supported URI forms (per SQLite docs):
///   file::memory:           -> in-memory
///   file:path?mode=memory   -> in-memory (path is just a name)
///   file:path               -> real file at path
///   file:///path             -> real file at /path (authority form)
///   file://localhost/path    -> real file at /path
fn parse_uri_filename(uri: &str) -> Result<(String, bool, bool), ()> {
    // Must start with "file:"
    let after_file = &uri[5..];

    // Extract path and query parts
    let (raw_path, query) = match after_file.find('?') {
        Some(pos) => (&after_file[..pos], Some(&after_file[pos + 1..])),
        None => (after_file, None),
    };

    // Strip fragment if present (after #)
    let raw_path = match raw_path.find('#') {
        Some(pos) => &raw_path[..pos],
        None => raw_path,
    };

    // Handle authority: file:///path or file://localhost/path
    let path = if let Some(after_slashes) = raw_path.strip_prefix("//") {
        // file:///path -> authority is empty, path starts at third /
        // file://localhost/path -> authority is "localhost"
        match after_slashes.find('/') {
            Some(pos) => {
                let authority = &after_slashes[..pos];
                if !authority.is_empty() && authority != "localhost" {
                    return Err(()); // non-local authority not supported
                }
                percent_decode(&after_slashes[pos..]).ok_or(())?
            }
            None => {
                // file:// with no path after authority
                return Err(());
            }
        }
    } else {
        percent_decode(raw_path).ok_or(())?
    };

    let mut is_memory = path == ":memory:";
    let mut cache_shared = false;
    if let Some(query) = query {
        for param in query.split('&') {
            let (key, value) = match param.find('=') {
                Some(pos) => (&param[..pos], &param[pos + 1..]),
                None => (param, ""),
            };
            match key {
                "mode" => match value {
                    "memory" => is_memory = true,
                    "ro" | "rw" | "rwc" => {} // valid modes, no-op (Turso doesn't enforce read-only yet)
                    _ => return Err(()),      // unknown mode -> SQLITE_CANTOPEN
                },
                "cache" => {
                    if value == "shared" {
                        cache_shared = true;
                    }
                    // "private" is also valid but is the default behavior
                }
                _ => {}
            }
        }
    }

    Ok((path, is_memory, cache_shared))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_open_v2(
    filename: *const ffi::c_char,
    db_out: *mut *mut sqlite3,
    flags: ffi::c_int,
    _z_vfs: *const ffi::c_char,
) -> ffi::c_int {
    trace!("sqlite3_open_v2");
    let rc = sqlite3_initialize();
    if rc != SQLITE_OK {
        return rc;
    }
    if filename.is_null() || db_out.is_null() {
        return SQLITE_MISUSE;
    }
    let filename_cstr = CStr::from_ptr(filename);
    let filename_str = match filename_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };

    // Determine the effective filename and whether to use in-memory IO
    let (effective_filename, use_memory, cache_shared) =
        if (flags & SQLITE_OPEN_URI) != 0 && filename_str.starts_with("file:") {
            match parse_uri_filename(filename_str) {
                Ok(result) => result,
                Err(()) => return SQLITE_CANTOPEN,
            }
        } else if (flags & SQLITE_OPEN_MEMORY) != 0 || filename_str == ":memory:" {
            (":memory:".to_string(), true, false)
        } else if filename_str.is_empty() {
            // SQLite opens a private on-disk database for "" and deletes it
            // when the connection closes.
            (
                temp_database_path().to_string_lossy().into_owned(),
                false,
                false,
            )
        } else {
            (filename_str.to_string(), false, false)
        };
    let temp_path = if filename_str.is_empty() && !use_memory {
        Some(std::path::PathBuf::from(&effective_filename))
    } else {
        None
    };

    // Open read-only when asked to, and also when the file exists but is
    // not writable: SQLite falls back to read-only access in that case
    // instead of failing to open.
    let file_is_readonly = !use_memory
        && std::fs::metadata(&effective_filename)
            .map(|m| m.permissions().readonly())
            .unwrap_or(false);
    let file_open_flags = if (flags & SQLITE_OPEN_READONLY) != 0 || file_is_readonly {
        turso_core::OpenFlags::ReadOnly
    } else {
        turso_core::OpenFlags::default()
    };

    let use_shared_memory = use_memory && cache_shared;

    let (io, db) = if use_shared_memory {
        match turso_core::Database::open_shared_memory(&effective_filename, Arc::new(SqliteDialect))
        {
            Ok(db) => (db.io.clone(), db),
            Err(e) => {
                trace!("error opening shared memory database {effective_filename}: {e:?}");
                return SQLITE_CANTOPEN;
            }
        }
    } else if use_memory {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
        match turso_core::Database::open_file_with_flags(
            io.clone(),
            ":memory:",
            turso_core::OpenFlags::default(),
            default_db_opts(),
            None,
            Arc::new(SqliteDialect),
        ) {
            Ok(db) => (io, db),
            Err(e) => {
                trace!("error opening memory database: {e:?}");
                return SQLITE_CANTOPEN;
            }
        }
    } else {
        let io: Arc<dyn turso_core::IO> = match turso_core::PlatformIO::new() {
            Ok(io) => Arc::new(io),
            Err(_) => return SQLITE_CANTOPEN,
        };
        match turso_core::Database::open_file_with_flags(
            io.clone(),
            &effective_filename,
            file_open_flags,
            default_db_opts(),
            None,
            Arc::new(SqliteDialect),
        ) {
            Ok(db) => (io, db),
            Err(e) => {
                trace!("error opening database {effective_filename}: {e:?}");
                return SQLITE_CANTOPEN;
            }
        }
    };

    match db.connect() {
        Ok(conn) => {
            let stored_filename = if use_memory || temp_path.is_some() {
                CString::new("".to_string()).unwrap()
            } else {
                CString::new(effective_filename).unwrap()
            };
            let handle = sqlite3::new(io, db, conn, stored_filename);
            handle.inner.lock().unwrap().temp_path = temp_path;
            *db_out = handle.into_raw();
            SQLITE_OK
        }
        Err(e) => {
            trace!("error connecting to database: {:?}", e);
            SQLITE_CANTOPEN
        }
    }
}

fn temp_database_path() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("turso-temp-{}-{n}.db", std::process::id()))
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close(db: *mut sqlite3) -> ffi::c_int {
    trace!("sqlite3_close");
    if db.is_null() {
        return SQLITE_OK;
    }
    {
        let mut inner = (*db).inner.lock().unwrap();
        if inner.e_open_state == SQLITE_STATE_ZOMBIE {
            return SQLITE_MISUSE;
        }
        if !inner.stmt_list.is_null() {
            set_db_err_msg(
                &mut inner,
                SQLITE_BUSY,
                "unable to close due to unfinalized statements or unfinished backups",
            );
            return SQLITE_BUSY;
        }
    }
    drop(Arc::from_raw(db));
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close_v2(db: *mut sqlite3) -> ffi::c_int {
    trace!("sqlite3_close_v2");
    if db.is_null() {
        return SQLITE_OK;
    }
    {
        let mut inner = (*db).inner.lock().unwrap();
        if inner.e_open_state == SQLITE_STATE_ZOMBIE {
            return SQLITE_MISUSE;
        }
        if !inner.stmt_list.is_null() {
            inner.e_open_state = SQLITE_STATE_ZOMBIE;
        }
    }
    drop(Arc::from_raw(db));
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_filename(
    db: *mut sqlite3,
    db_name: *const ffi::c_char,
) -> *const ffi::c_char {
    if db.is_null() {
        return std::ptr::null();
    }
    if !db_name.is_null() {
        let name = CStr::from_ptr(db_name);
        if name.to_bytes() != b"main" {
            return std::ptr::null();
        }
    }
    let db = &*db;
    let inner = db.inner.lock().unwrap();
    inner.filename.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_trace_v2(
    _db: *mut sqlite3,
    _mask: ffi::c_uint,
    _callback: Option<
        unsafe extern "C" fn(ffi::c_uint, *mut ffi::c_void, *mut ffi::c_void, *mut ffi::c_void),
    >,
    _context: *mut ffi::c_void,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_progress_handler(
    db: *mut sqlite3,
    n: ffi::c_int,
    callback: Option<unsafe extern "C" fn(*mut ffi::c_void) -> ffi::c_int>,
    context: *mut ffi::c_void,
) {
    if db.is_null() {
        return;
    }

    let db_ref = &*db;
    let inner = match db_ref.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    match callback {
        Some(c_callback) if n > 0 => {
            let ctx = context as usize;
            let cb = c_callback;
            inner.conn.set_progress_handler(
                n as u64,
                Some(Box::new(move || unsafe {
                    cb(ctx as *mut ffi::c_void) != 0
                })),
            );
        }
        _ => inner.conn.set_progress_handler(0, None),
    }
}

/// Type for C busy handler callback function.
type BusyHandlerFn = unsafe extern "C" fn(*mut ffi::c_void, ffi::c_int) -> ffi::c_int;

/// Register a callback to handle SQLITE_BUSY errors.
///
/// The sqlite3_busy_handler(D,X,P) routine sets a callback function X that might be invoked
/// with argument P whenever an attempt is made to access a database table associated with
/// database connection D when another thread or process has the table locked.
///
/// If the busy callback is NULL, then SQLITE_BUSY is returned immediately upon encountering
/// the lock. If the busy callback is not NULL, then the callback might be invoked with two
/// arguments: the context pointer P and the number of times the busy handler has been invoked
/// previously for the same locking event.
///
/// If the busy callback returns 0, then no additional attempts are made to access the database
/// and SQLITE_BUSY is returned to the application. If the callback returns non-zero, then
/// another attempt is made to access the database and the cycle repeats.
///
/// There can only be a single busy handler defined for each database connection. Setting a new
/// busy handler clears any previously set handler. Note that calling sqlite3_busy_timeout()
/// will change the busy handler and thus clear any previously set busy handler.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_handler(
    db: *mut sqlite3,
    callback: Option<BusyHandlerFn>,
    context: *mut ffi::c_void,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }

    let db_ref = &*db;
    let inner = match db_ref.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return SQLITE_MISUSE,
    };

    match callback {
        None => {
            // Clear the busy handler
            inner.conn.set_busy_handler(None);
        }
        Some(c_callback) => {
            // We need a Rust wrapper for the C callback
            // The context pointer is captured by value and must remain valid for the
            // lifetime of the handler (caller's responsibility per SQLite spec)
            let ctx = context as usize; // Convert to usize for Send+Sync
            let cb = c_callback;
            inner
                .conn
                .set_busy_handler(Some(Box::new(move |count: i32| {
                    // SAFETY: Caller guarantees context validity for the handler's lifetime
                    unsafe { cb(ctx as *mut ffi::c_void, count as ffi::c_int) }
                })));
        }
    }

    SQLITE_OK
}

/// Set a busy timeout for the database connection.
///
/// This routine sets a busy handler that sleeps for a specified amount of time when a table
/// is locked. The handler will sleep multiple times until at least "ms" milliseconds of
/// sleeping have accumulated. After at least "ms" milliseconds of sleeping, the handler
/// returns 0 which causes sqlite3_step() to return SQLITE_BUSY.
///
/// Calling this routine with an argument less than or equal to zero turns off all busy
/// handlers and returns SQLITE_BUSY immediately upon encountering a lock.
///
/// There can only be a single busy handler for a database connection. Setting a busy timeout
/// clears any previously set busy handler.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_timeout(db: *mut sqlite3, ms: ffi::c_int) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }

    let db_ref = &*db;
    let inner = match db_ref.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return SQLITE_MISUSE,
    };

    let duration = if ms <= 0 {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_millis(ms as u64)
    };

    inner.conn.set_busy_timeout(duration);
    SQLITE_OK
}

/// Callback type for sqlite3_set_authorizer: (userData, actionCode, arg1,
/// arg2, database, trigger-or-view) -> SQLITE_OK / SQLITE_DENY / SQLITE_IGNORE.
pub type sqlite3_authorizer_callback = unsafe extern "C" fn(
    *mut ffi::c_void,
    ffi::c_int,
    *const ffi::c_char,
    *const ffi::c_char,
    *const ffi::c_char,
    *const ffi::c_char,
) -> ffi::c_int;

/// Refused with SQLITE_ERROR: turso_core has no prepare-time authorization
/// hook, so a stored callback would never be invoked and callers (e.g. PHP,
/// which routes its open_basedir checks through the authorizer) would
/// silently lose the enforcement they registered for. PHP ignores this
/// return value when it installs its authorizer at open, so refusing does
/// not break it. Implement for real once core grows an authorization hook.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_set_authorizer(
    db: *mut sqlite3,
    _callback: Option<sqlite3_authorizer_callback>,
    _context: *mut ffi::c_void,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    SQLITE_ERROR
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_context_db_handle(context: *mut ffi::c_void) -> *mut ffi::c_void {
    if context.is_null() {
        return std::ptr::null_mut();
    }
    let ctx = &*(context as *const SqliteContext);
    ctx.db as *mut ffi::c_void
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_prepare_v2(
    raw_db: *mut sqlite3,
    sql: *const ffi::c_char,
    n_byte: ffi::c_int,
    out_stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const ffi::c_char,
) -> ffi::c_int {
    if raw_db.is_null() || sql.is_null() || out_stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let db_handle = sqlite3::clone_from_raw(raw_db);
    let mut db = db_handle.inner.lock().unwrap();
    if db.e_open_state == SQLITE_STATE_ZOMBIE {
        db.err_code = SQLITE_MISUSE;
        return SQLITE_MISUSE;
    }
    // SQLite C-API contract (https://www.sqlite.org/c3ref/prepare.html):
    //   If nByte is negative, zSql is read up to the first zero terminator.
    //   If nByte is positive, it is the number of bytes read from zSql.
    // Without this branch, the function unconditionally walks until NUL — over-reads
    // any non-NUL-terminated Rust `&str` passed by rusqlite/libsqlite3-sys, producing
    // misleading parse errors from neighbouring memory.
    let sql_bytes: &[u8] = if n_byte < 0 {
        CStr::from_ptr(sql).to_bytes()
    } else {
        let bounded = std::slice::from_raw_parts(sql as *const u8, n_byte as usize);
        bounded
            .iter()
            .position(|&b| b == 0)
            .map_or(bounded, |i| &bounded[..i])
    };
    let sql_str = match std::str::from_utf8(sql_bytes) {
        Ok(s) => s,
        Err(_) => {
            db.err_code = SQLITE_MISUSE;
            return SQLITE_MISUSE;
        }
    };
    let stmt = match db.conn.prepare(sql_str) {
        Ok(stmt) => stmt,
        // The C API contract (https://www.sqlite.org/c3ref/prepare.html)
        // treats SQL with nothing to compile (empty string, whitespace,
        // comments) as success with *ppStmt set to NULL, while core
        // reports it as an error so the higher-level bindings can throw.
        Err(LimboError::InvalidArgument(ref msg))
            if msg == "The supplied SQL string contains no statements" =>
        {
            if !tail.is_null() {
                *tail = sql.add(sql_bytes.len());
            }
            *out_stmt = std::ptr::null_mut();
            return SQLITE_OK;
        }
        Err(err) => {
            return set_db_err(&mut db, err);
        }
    };
    if !tail.is_null() {
        // The parser's consumed-bytes position includes whitespace it lexed
        // past, but the C contract points the tail at the first byte past
        // the end of the statement: just after the terminating ';'. When
        // the statement ends without a semicolon, the consumed position is
        // already the answer, so only step back over whitespace that
        // follows a ';'.
        let mut off = stmt.tail_offset();
        let mut p = off;
        while p > 0 && (*(sql.add(p - 1) as *const u8)).is_ascii_whitespace() {
            p -= 1;
        }
        if p > 0 && *(sql.add(p - 1) as *const u8) == b';' {
            off = p;
        }
        *tail = sql.add(off);
    }
    let new_stmt = Box::leak(Box::new(sqlite3_stmt::new(db_handle.clone(), stmt)));

    new_stmt.next = db.stmt_list;
    db.stmt_list = new_stmt;

    *out_stmt = new_stmt;
    SQLITE_OK
}

/// sqlite3_prepare_v3 is identical to sqlite3_prepare_v2 but accepts a
/// `prep_flags` parameter (e.g. SQLITE_PREPARE_PERSISTENT) which is a hint
/// to the query planner. Turso does not use these hints yet, so we delegate
/// directly to sqlite3_prepare_v2.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_prepare_v3(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    n_byte: ffi::c_int,
    _prep_flags: ffi::c_uint,
    out_stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const ffi::c_char,
) -> ffi::c_int {
    sqlite3_prepare_v2(db, sql, n_byte, out_stmt, tail)
}

unsafe fn stmt_run_to_completion(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt_ref = &mut *stmt;
    while stmt_ref.stmt.execution_state().is_running() {
        let result = sqlite3_step(stmt);
        if result != SQLITE_DONE && result != SQLITE_ROW {
            return result;
        }
    }
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = &mut *stmt;

    // first, finalize any execution if it was unfinished
    // (for example, many drivers can consume just one row and finalize statement after that, while there still can be work to do)
    // (this is necessary because queries like INSERT INTO t VALUES (1), (2), (3) RETURNING id return values within a transaction)
    let result = stmt_run_to_completion(stmt);

    {
        let mut db_inner = stmt_ref.db.inner.lock().unwrap();

        if db_inner.stmt_list == stmt {
            db_inner.stmt_list = stmt_ref.next;
        } else {
            let mut current = db_inner.stmt_list;
            while !current.is_null() {
                let current_ref = &mut *current;
                if current_ref.next == stmt {
                    current_ref.next = stmt_ref.next;
                    break;
                }
                current = current_ref.next;
            }
        }
    }

    for (_idx, destructor_opt, ptr) in stmt_ref.destructors.drain(..) {
        if let Some(destructor_fn) = destructor_opt {
            destructor_fn(ptr);
        }
    }
    stmt_ref.clear_text_cache();
    let _ = Box::from_raw(stmt);
    result
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    let db = &stmt.db;
    // Do not hold the handle lock across the step. A user-defined function
    // invoked mid-step may re-enter the C API on the same handle (SQLite
    // allows e.g. a nested prepare/step from inside a scalar callback), and
    // re-locking the non-reentrant mutex on the same thread deadlocks.
    // Nothing here needs the lock while stepping: core's Connection is
    // internally synchronized, and the statement itself was never protected
    // by the handle lock to begin with.
    let res = stmt.stmt.run_one_step_blocking(|| Ok(()), || Ok(()));
    let rc = match res {
        Ok(Some(_)) => {
            stmt.clear_text_cache();
            SQLITE_ROW
        }
        Ok(None) => {
            stmt.clear_text_cache();
            SQLITE_DONE
        }
        Err(LimboError::Busy) => SQLITE_BUSY,
        Err(LimboError::Interrupt) => SQLITE_INTERRUPT,
        Err(err) => {
            let mut db_inner = db.inner.lock().unwrap();
            set_db_err(&mut db_inner, err)
        }
    };
    let current = stmt.stmt.metrics().search_count;
    let delta = current - stmt.prev_search_count;
    stmt.prev_search_count = current;
    sqlite3_search_count = sqlite3_search_count.saturating_add(delta as ffi::c_int);
    rc
}

type exec_callback = Option<
    unsafe extern "C" fn(
        context: *mut ffi::c_void,
        n_column: ffi::c_int,
        argv: *mut *mut ffi::c_char,
        colv: *mut *mut ffi::c_char,
    ) -> ffi::c_int,
>;

#[no_mangle]
pub unsafe extern "C" fn sqlite3_exec(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    callback: exec_callback,
    context: *mut ffi::c_void,
    err: *mut *mut ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }

    let db_ref: &sqlite3 = &*db;
    let sql_cstr = CStr::from_ptr(sql);
    let sql_str = match sql_cstr.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    trace!("sqlite3_exec(sql={})", sql_str);
    if !err.is_null() {
        *err = std::ptr::null_mut();
    }
    let statements = split_sql_statements(sql_str);
    for stmt_sql in statements {
        let trimmed = stmt_sql.trim();
        if trimmed.is_empty() {
            continue;
        }

        let is_dql = is_query_statement(trimmed);
        if !is_dql {
            // For DML/DDL, use normal execute path
            let db_inner = db_ref.inner.lock().unwrap();
            match db_inner.conn.execute(trimmed) {
                Ok(_) => continue,
                Err(e) => {
                    return handle_limbo_err(e, err);
                }
            }
        } else if callback.is_none() {
            // DQL without callback provided, still execute but discard any result rows
            let mut stmt_ptr: *mut sqlite3_stmt = std::ptr::null_mut();
            let rc = sqlite3_prepare_v2(
                db,
                CString::new(trimmed).unwrap().as_ptr(),
                -1,
                &mut stmt_ptr,
                std::ptr::null_mut(),
            );
            if rc != SQLITE_OK {
                if !err.is_null() {
                    let err_msg = format!("Prepare failed: {rc}");
                    *err = CString::new(err_msg).unwrap().into_raw();
                }
                return rc;
            }
            loop {
                let step_rc = sqlite3_step(stmt_ptr);
                match step_rc {
                    SQLITE_ROW => continue,
                    SQLITE_DONE => break,
                    _ => {
                        sqlite3_finalize(stmt_ptr);
                        if !err.is_null() {
                            let err_msg = format!("Step failed: {step_rc}");
                            *err = CString::new(err_msg).unwrap().into_raw();
                        }
                        return step_rc;
                    }
                }
            }
            sqlite3_finalize(stmt_ptr);
        } else {
            // DQL with callback
            let rc = execute_query_with_callback(db, trimmed, callback, context, err);
            if rc != SQLITE_OK {
                return rc;
            }
        }
    }
    SQLITE_OK
}

/// Detect if a SQL statement is DQL
fn is_query_statement(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    let bytes = trimmed.as_bytes();

    let starts_with_ignore_case = |keyword: &[u8]| -> bool {
        if bytes.len() < keyword.len() {
            return false;
        }
        // Check keyword matches
        if !bytes[..keyword.len()].eq_ignore_ascii_case(keyword) {
            return false;
        }
        // Ensure keyword is followed by whitespace or EOF
        bytes.len() == keyword.len() || bytes[keyword.len()].is_ascii_whitespace()
    };

    // Check DQL keywords
    if starts_with_ignore_case(b"SELECT")
        || starts_with_ignore_case(b"VALUES")
        || starts_with_ignore_case(b"WITH")
        || starts_with_ignore_case(b"PRAGMA")
        || starts_with_ignore_case(b"EXPLAIN")
    {
        return true;
    }

    // Look for RETURNING as a whole word, that's not part of another identifier
    let mut i = 0;
    while i < bytes.len() {
        if i + 9 <= bytes.len() && bytes[i..i + 9].eq_ignore_ascii_case(b"RETURNING") {
            // Check it's a word boundary before and after
            let is_word_start =
                i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let is_word_end = i + 9 == bytes.len()
                || !bytes[i + 9].is_ascii_alphanumeric() && bytes[i + 9] != b'_';
            if is_word_start && is_word_end {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Execute a query statement with callback for each row
/// Only called when we know callback is Some
unsafe fn execute_query_with_callback(
    db: *mut sqlite3,
    sql: &str,
    callback: exec_callback,
    context: *mut ffi::c_void,
    err: *mut *mut ffi::c_char,
) -> ffi::c_int {
    let sql_cstring = match CString::new(sql) {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };

    let mut stmt_ptr: *mut sqlite3_stmt = std::ptr::null_mut();
    let rc = sqlite3_prepare_v2(
        db,
        sql_cstring.as_ptr(),
        -1,
        &mut stmt_ptr,
        std::ptr::null_mut(),
    );

    if rc != SQLITE_OK {
        if !err.is_null() {
            let err_msg = format!("Prepare failed: {rc}");
            *err = CString::new(err_msg).unwrap().into_raw();
        }
        return rc;
    }

    let stmt_ref = &*stmt_ptr;
    let n_cols = stmt_ref.stmt.num_columns() as ffi::c_int;
    let mut column_names: Vec<CString> = Vec::with_capacity(n_cols as usize);

    for i in 0..n_cols {
        let name = stmt_ref.stmt.get_column_name(i as usize);
        column_names.push(CString::new(name.as_bytes()).unwrap());
    }

    loop {
        let step_rc = sqlite3_step(stmt_ptr);

        match step_rc {
            SQLITE_ROW => {
                // Safety: checked earlier
                let callback = callback.unwrap();

                let mut values: Vec<Option<CString>> = Vec::with_capacity(n_cols as usize);
                let mut value_ptrs: Vec<*mut ffi::c_char> = Vec::with_capacity(n_cols as usize);
                let mut col_ptrs: Vec<*mut ffi::c_char> = Vec::with_capacity(n_cols as usize);

                for i in 0..n_cols {
                    let val = stmt_ref.stmt.row().unwrap().get_value(i as usize);
                    // SQL NULL is passed to the callback as a NULL pointer,
                    // as in SQLite, not as an empty string.
                    if matches!(val, Value::Null) {
                        values.push(None);
                    } else {
                        values.push(Some(CString::new(val.to_string().as_bytes()).unwrap()));
                    }
                }

                for value in &values {
                    value_ptrs.push(
                        value
                            .as_ref()
                            .map_or(std::ptr::null_mut(), |v| v.as_ptr() as *mut ffi::c_char),
                    );
                }
                for name in &column_names {
                    col_ptrs.push(name.as_ptr() as *mut ffi::c_char);
                }

                let cb_rc = callback(
                    context,
                    n_cols,
                    value_ptrs.as_mut_ptr(),
                    col_ptrs.as_mut_ptr(),
                );

                if cb_rc != 0 {
                    sqlite3_finalize(stmt_ptr);
                    return SQLITE_ABORT;
                }
            }
            SQLITE_DONE => {
                break;
            }
            _ => {
                sqlite3_finalize(stmt_ptr);
                if !err.is_null() {
                    let err_msg = format!("Step failed: {step_rc}");
                    *err = CString::new(err_msg).unwrap().into_raw();
                }
                return step_rc;
            }
        }
    }

    sqlite3_finalize(stmt_ptr)
}

/// Split SQL string into individual statements
/// Handles quoted strings properly and skips comments
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut current_start = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            // Check for escaped quotes first
            b'\'' if !in_double_quote => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = !in_single_quote;
            }
            b'"' if !in_single_quote => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double_quote = !in_double_quote;
            }
            b';' if !in_single_quote && !in_double_quote => {
                // we found the statement boundary
                statements.push(&sql[current_start..i]);
                current_start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }

    if current_start < sql.len() {
        statements.push(&sql[current_start..]);
    }

    statements
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        // sqlite3 returns SQLITE_OK if( pStmt==0 ){
        return SQLITE_OK;
    }
    let stmt = &mut *stmt;
    // first, finalize any execution if it was unfinished
    // (for example, many drivers can consume just one row and finalize statement after that, while there still can be work to do)
    // (this is necessary because queries like INSERT INTO t VALUES (1), (2), (3) RETURNING id return values within a transaction)
    let result = stmt_run_to_completion(stmt);
    if let Err(err) = stmt.stmt.reset() {
        if result == SQLITE_OK {
            return handle_limbo_err(err, std::ptr::null_mut());
        }
    }
    stmt.prev_search_count = 0;
    stmt.clear_text_cache();
    result
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes64(db: *mut sqlite3) -> i64 {
    let db: &sqlite3 = &*db;
    let inner = db.inner.lock().unwrap();
    inner.conn.changes()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes(db: *mut sqlite3) -> ffi::c_int {
    sqlite3_changes64(db) as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_readonly(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return 1;
    }
    if (*stmt).stmt.get_program().is_readonly() {
        1
    } else {
        0
    }
}

/// True if the statement has been stepped but has neither returned
/// SQLITE_DONE nor been reset.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_busy(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return 0;
    }
    (*stmt).stmt.is_busy() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_status(
    stmt: *mut sqlite3_stmt,
    op: ffi::c_int,
    reset_flg: ffi::c_int,
) -> ffi::c_int {
    if stmt.is_null() {
        return 0;
    }

    let stmt = &mut *stmt;
    let counter = match op {
        SQLITE_STMTSTATUS_FULLSCAN_STEP => turso_core::StatementStatusCounter::FullscanStep,
        SQLITE_STMTSTATUS_SORT => turso_core::StatementStatusCounter::Sort,
        SQLITE_STMTSTATUS_VM_STEP => turso_core::StatementStatusCounter::VmStep,
        SQLITE_STMTSTATUS_REPREPARE => turso_core::StatementStatusCounter::Reprepare,
        LIBSQL_STMTSTATUS_ROWS_READ => turso_core::StatementStatusCounter::RowsRead,
        LIBSQL_STMTSTATUS_ROWS_WRITTEN => turso_core::StatementStatusCounter::RowsWritten,
        SQLITE_STMTSTATUS_AUTOINDEX
        | SQLITE_STMTSTATUS_RUN
        | SQLITE_STMTSTATUS_FILTER_MISS
        | SQLITE_STMTSTATUS_FILTER_HIT
        | SQLITE_STMTSTATUS_MEMUSED => return 0,
        _ => return 0,
    };

    let value = stmt.stmt.stmt_status(counter);
    if reset_flg != 0 {
        stmt.stmt.reset_stmt_status(counter);
    }

    value.min(ffi::c_int::MAX as u64) as ffi::c_int
}

/// Iterate over all prepared statements in the database.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_next_stmt(
    db: *mut sqlite3,
    stmt: *mut sqlite3_stmt,
) -> *mut sqlite3_stmt {
    if db.is_null() {
        return std::ptr::null_mut();
    }
    if stmt.is_null() {
        let db = &*db;
        let db = db.inner.lock().unwrap();
        db.stmt_list
    } else {
        let stmt = &mut *stmt;
        stmt.next
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_serialize(
    _db: *mut sqlite3,
    _schema: *const ffi::c_char,
    _out: *mut *mut ffi::c_void,
    _out_bytes: *mut ffi::c_int,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_deserialize(
    _db: *mut sqlite3,
    _schema: *const ffi::c_char,
    _in_: *const ffi::c_void,
    _in_bytes: ffi::c_int,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_get_autocommit(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return 1;
    }
    let db: &sqlite3 = &*db;
    let inner = db.inner.lock().unwrap();
    if inner.conn.get_auto_commit() {
        1
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_total_changes64(db: *mut sqlite3) -> i64 {
    let db: &sqlite3 = &*db;
    let inner = db.inner.lock().unwrap();
    inner.conn.total_changes()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_total_changes(db: *mut sqlite3) -> ffi::c_int {
    sqlite3_total_changes64(db) as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64 {
    let db: &sqlite3 = &*db;
    let inner = db.inner.lock().unwrap();
    inner.conn.last_insert_rowid()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_interrupt(db: *mut sqlite3) {
    if db.is_null() {
        return;
    }
    let db_ref = &*db;
    let inner = match db_ref.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    inner.conn.interrupt();
}

extern "C" {
    /// Variadic implementations in varargs.c; each decodes its va_list and
    /// calls the turso_* backends below. Declared argument-less because they
    /// are only referenced as `sym` jump targets, never called from Rust.
    fn turso_sqlite3_db_config_va();
    fn turso_sqlite3_mprintf_va();
    fn turso_sqlite3_snprintf_va();
}

/// Exported trampolines for the variadic sqlite3_mprintf/sqlite3_snprintf;
/// same mechanism as sqlite3_db_config below.
#[unsafe(naked)]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_mprintf(_fmt: *const ffi::c_char) -> *mut ffi::c_char {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    core::arch::naked_asm!("jmp {}", sym turso_sqlite3_mprintf_va);
    #[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
    core::arch::naked_asm!("b {}", sym turso_sqlite3_mprintf_va);
}

#[unsafe(naked)]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_snprintf(
    _n: ffi::c_int,
    _buf: *mut ffi::c_char,
    _fmt: *const ffi::c_char,
) -> *mut ffi::c_char {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    core::arch::naked_asm!("jmp {}", sym turso_sqlite3_snprintf_va);
    #[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
    core::arch::naked_asm!("b {}", sym turso_sqlite3_snprintf_va);
}

extern "C" {
    // One-line va_arg pumps in varargs.c: only C can read a va_list, and
    // these are the only pieces that need to.
    fn turso_va_i32(ap: *mut ffi::c_void) -> i32;
    fn turso_va_i64(ap: *mut ffi::c_void) -> i64;
    fn turso_va_f64(ap: *mut ffi::c_void) -> f64;
    fn turso_va_ptr(ap: *mut ffi::c_void) -> *mut ffi::c_void;
}

/// Formats a sqlite3_mprintf-style call through core's printf engine — the
/// implementation backing the SQL printf()/format() functions, so the C API
/// and SQL formatting cannot diverge. `ap` is a pointer to the caller's
/// va_list; core's printf_c_arg_plan (derived from the engine's own
/// specifier grammar) dictates the C type pulled for each argument slot.
/// Returns a malloc'd NUL-terminated string for sqlite3_free, or NULL when
/// fmt is NULL or allocation fails.
#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn turso_printf_va(
    fmt: *const ffi::c_char,
    ap: *mut ffi::c_void,
) -> *mut ffi::c_char {
    if fmt.is_null() || ap.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: fmt checked non-null; caller guarantees a NUL-terminated string.
    let fmt_str = unsafe { CStr::from_ptr(fmt) };
    let fmt_str = String::from_utf8_lossy(fmt_str.to_bytes()).into_owned();

    let mut args = Vec::new();
    for slot in turso_core::printf_c_arg_plan(&fmt_str) {
        use turso_core::PrintfCArg;
        // SAFETY: the plan mirrors the engine's specifier grammar, so each
        // pump reads the C type the caller passed for that slot.
        let value = unsafe {
            match slot {
                PrintfCArg::Int32 => turso_core::Value::from_i64(turso_va_i32(ap) as i64),
                PrintfCArg::Uint32 => turso_core::Value::from_i64(turso_va_i32(ap) as u32 as i64),
                PrintfCArg::Int64 => turso_core::Value::from_i64(turso_va_i64(ap)),
                PrintfCArg::Double => turso_core::Value::from_f64(turso_va_f64(ap)),
                PrintfCArg::Text | PrintfCArg::OwnedText => {
                    let s = turso_va_ptr(ap) as *const ffi::c_char;
                    // A NULL string pointer becomes Value::Null: the engine
                    // renders it as (NULL) for %q, NULL for %Q, "" for %s.
                    let value = if s.is_null() {
                        turso_core::Value::Null
                    } else {
                        let text = String::from_utf8_lossy(CStr::from_ptr(s).to_bytes());
                        turso_core::Value::build_text(text.into_owned())
                    };
                    if slot == PrintfCArg::OwnedText {
                        // %z: the caller yields the pointer.
                        libc::free(s as *mut ffi::c_void);
                    }
                    value
                }
                // %c takes a character code in the C API, converted at
                // extraction — as SQLite's C path does before its shared
                // engine runs; the SQL printf('%c', x) takes the first
                // character of x's text form, from the same engine.
                PrintfCArg::CharCode => {
                    let c = u32::try_from(turso_va_i32(ap))
                        .ok()
                        .and_then(char::from_u32)
                        .unwrap_or('\u{fffd}');
                    turso_core::Value::build_text(c.to_string())
                }
            }
        };
        args.push(value);
    }

    let fmt_value = turso_core::Value::build_text(fmt_str);
    let arg_refs: Vec<&turso_core::Value> = args.iter().collect();
    let rendered = match turso_core::exec_printf_values(&fmt_value, &arg_refs) {
        // The SQL function returns NULL for an empty format or an unknown
        // specifier before any output; the C API returns "" there.
        Ok(turso_core::Value::Text(t)) => t.as_str().to_string(),
        Ok(_) => String::new(),
        Err(_) => return std::ptr::null_mut(),
    };
    let n = rendered.len();
    // SAFETY: fresh allocation of n + 1 bytes, copied then NUL-terminated.
    unsafe {
        let buf = libc::malloc(n + 1) as *mut ffi::c_char;
        if buf.is_null() {
            return std::ptr::null_mut();
        }
        std::ptr::copy_nonoverlapping(rendered.as_ptr(), buf as *mut u8, n);
        *buf.add(n) = 0;
        buf
    }
}

/// Exported trampoline for the variadic sqlite3_db_config. rustc exports
/// only Rust items from cdylibs (version script on ELF, exported-symbols
/// list on Apple), so the public symbol must be defined here; the tail jump
/// hands the untouched variadic call frame to the C implementation, whose
/// own prologue then performs va_start as for a direct call. This also
/// pins varargs.c's object into the link. Once rustc can export
/// native-library symbols from cdylibs (rust-lang/rust#155697), this
/// trampoline can be deleted and the C function renamed sqlite3_db_config.
#[unsafe(naked)]
#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_config(_db: *mut ffi::c_void, _op: ffi::c_int) -> ffi::c_int {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    core::arch::naked_asm!("jmp {}", sym turso_sqlite3_db_config_va);
    #[cfg(any(target_arch = "arm", target_arch = "aarch64"))]
    core::arch::naked_asm!("b {}", sym turso_sqlite3_db_config_va);
}

/// Non-variadic backend for sqlite3_db_config. The variadic C entry point in
/// varargs.c decodes the (int value, int *pOut) argument shape shared by all
/// boolean DBCONFIG ops and forwards here. For boolean ops, a negative value
/// queries the current setting without changing it, and the resulting state
/// is written through `p_out` when non-NULL, as in SQLite.
#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn turso_db_config_int(
    db: *mut sqlite3,
    op: ffi::c_int,
    value: ffi::c_int,
    p_out: *mut ffi::c_int,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let _ = (value, p_out);
    match op {
        // Refused rather than accepted-and-ignored, so a caller that checks
        // the return cannot be misled into believing it toggled anything.
        // The protections DEFENSIVE stands for are unconditionally on in
        // turso_core today:
        //   - sqlite_schema DML is rejected with no writable_schema unlock
        //     (core/schema.rs, allow_user_dml)
        //   - PRAGMA schema_version=N is a deliberate no-op
        //     (core/translate/pragma.rs, SchemaVersion write arm)
        //   - PRAGMA journal_mode=OFF is unsupported and silently kept as-is
        //     (core/storage/journal_mode.rs, JournalMode::supported)
        //   - there are no SQLite-style shadow tables
        // Callers that need file-corruption protection therefore already
        // have it; PHP's ext/sqlite3 ignores this return value. Switching to
        // accept + enforce is safe only if every feature above gains a
        // per-connection defensive gate in core first.
        SQLITE_DBCONFIG_DEFENSIVE => SQLITE_ERROR,
        _ => SQLITE_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_handle(stmt: *mut sqlite3_stmt) -> *mut sqlite3 {
    if stmt.is_null() {
        return std::ptr::null_mut();
    }
    Arc::as_ptr(&(*stmt).db) as *mut sqlite3
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_sleep(_ms: ffi::c_int) {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_limit(
    _db: *mut sqlite3,
    _id: ffi::c_int,
    _new_value: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_malloc(n: ffi::c_int) -> *mut ffi::c_void {
    sqlite3_malloc64(n)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_malloc64(n: ffi::c_int) -> *mut ffi::c_void {
    if n <= 0 {
        return std::ptr::null_mut();
    }
    libc::malloc(n as usize)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free(ptr: *mut ffi::c_void) {
    if ptr.is_null() {
        return;
    }
    libc::free(ptr);
}

/// Returns the error code for the most recent failed API call to connection.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_errcode(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    if !sqlite3_safety_check_sick_or_ok(&db) {
        return SQLITE_MISUSE;
    }
    if db.malloc_failed {
        return SQLITE_NOMEM;
    }
    db.err_code & db.err_mask
}

/// Enables or disables extended result codes for the connection. When
/// disabled (the default), sqlite3_errcode and statement results report only
/// the primary result code; sqlite3_extended_errcode always reports the
/// extended code either way.
#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_extended_result_codes(
    db: *mut sqlite3,
    onoff: ffi::c_int,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    // SAFETY: db checked non-null just above; caller guarantees it points to a
    // live sqlite3 for the duration of the call.
    let db: &sqlite3 = unsafe { &*db };
    let mut inner = match db.inner.lock() {
        Ok(guard) => guard,
        Err(_) => return SQLITE_MISUSE,
    };
    inner.err_mask = if onoff != 0 { -1 } else { 0xff };
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errstr(_err: ffi::c_int) -> *const ffi::c_char {
    sqlite3_errstr_impl(_err)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_user_data(context: *mut ffi::c_void) -> *mut ffi::c_void {
    if context.is_null() {
        return std::ptr::null_mut();
    }
    let ctx = &*(context as *const SqliteContext);
    ctx.p_app
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_init(
    _dest_db: *mut sqlite3,
    _dest_name: *const ffi::c_char,
    _source_db: *mut sqlite3,
    _source_name: *const ffi::c_char,
) -> *mut ffi::c_void {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_step(
    _backup: *mut ffi::c_void,
    _n_pages: ffi::c_int,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_remaining(_backup: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_pagecount(_backup: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_finish(_backup: *mut ffi::c_void) -> ffi::c_int {
    stub!();
}

/// Returns the statement's original SQL text. The pointer is owned by the
/// statement and stays valid until sqlite3_finalize.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_sql(stmt: *mut sqlite3_stmt) -> *const ffi::c_char {
    if stmt.is_null() {
        return std::ptr::null();
    }
    (*stmt).sql.as_ptr()
}

/// Returns the SQL text with bound parameters expanded into literals,
/// rendered by Statement::expanded_sql in turso_core. The returned buffer is
/// allocated with sqlite3_malloc and must be released by the caller with
/// sqlite3_free.
#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_expanded_sql(stmt: *mut sqlite3_stmt) -> *mut ffi::c_char {
    if stmt.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: stmt checked non-null just above; caller guarantees it points
    // to a live sqlite3_stmt for the duration of the call.
    let stmt: &sqlite3_stmt = unsafe { &*stmt };
    let expanded = stmt.stmt.expanded_sql();
    let n = expanded.len();
    let buf = unsafe { libc::malloc(n + 1) } as *mut ffi::c_char;
    if buf.is_null() {
        return std::ptr::null_mut();
    }
    // SAFETY: buf is a fresh allocation of n + 1 bytes; expanded is n bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(expanded.as_ptr(), buf as *mut u8, n);
        *buf.add(n) = 0;
    }
    buf
}

/// Number of columns in the current row, or 0 when no row is available
/// (before the first step, after SQLITE_DONE, or after a reset).
#[no_mangle]
pub unsafe extern "C" fn sqlite3_data_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return 0;
    }
    let stmt = &*stmt;
    match stmt.stmt.row() {
        Some(row) => row.len() as ffi::c_int,
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &*stmt;
    stmt.stmt.parameters_count() as ffi::c_int
}

#[inline]
fn sqlite3_bind_index_in_range(stmt: &sqlite3_stmt, idx: ffi::c_int) -> Option<NonZeroUsize> {
    let idx = NonZeroUsize::new(idx as usize)?;
    let max = stmt.stmt.parameters_count();
    if idx.get() > max {
        None
    } else {
        Some(idx)
    }
}

unsafe fn sqlite3_bind_result(
    stmt: &mut sqlite3_stmt,
    result: Result<(), LimboError>,
) -> ffi::c_int {
    match result {
        Ok(()) => SQLITE_OK,
        Err(err) => {
            let db = &stmt.db;
            let mut inner = db.inner.lock().unwrap();
            set_db_err(&mut inner, err)
        }
    }
}

/// The returned pointer is owned by the statement and stays valid until
/// finalize, as documented; names are cached on the statement rather than
/// leaked per call.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_name(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let stmt = &mut *stmt;
    let Some(index) = sqlite3_bind_index_in_range(stmt, idx) else {
        return std::ptr::null();
    };

    if let Some(cached) = stmt.param_name_cache.iter().find(|(i, _)| *i == idx) {
        return cached.1.as_ptr();
    }
    match stmt
        .stmt
        .parameters()
        .name(index)
        .and_then(|n| CString::new(n).ok())
    {
        Some(c_string) => {
            stmt.param_name_cache.push((idx, c_string));
            stmt.param_name_cache.last().unwrap().1.as_ptr()
        }
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_index(
    stmt: *mut sqlite3_stmt,
    name: *const ffi::c_char,
) -> ffi::c_int {
    if stmt.is_null() || name.is_null() {
        return 0;
    }

    let stmt = &*stmt;
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return 0,
    };

    if let Some(index) = stmt.stmt.parameter_index(name_str) {
        index.get() as ffi::c_int
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }

    let stmt = &mut *stmt;
    let Some(index) = sqlite3_bind_index_in_range(stmt, idx) else {
        return SQLITE_RANGE;
    };

    let result = stmt.stmt.bind_at(index, Value::Null);
    sqlite3_bind_result(stmt, result)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    val: i64,
) -> ffi::c_int {
    sqlite3_bind_int64(stmt, idx, val)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int64(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    val: i64,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = &mut *stmt;
    let Some(index) = sqlite3_bind_index_in_range(stmt, idx) else {
        return SQLITE_RANGE;
    };

    let result = stmt.stmt.bind_at(index, Value::from_i64(val));
    sqlite3_bind_result(stmt, result)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_double(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    val: f64,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = &mut *stmt;
    let Some(index) = sqlite3_bind_index_in_range(stmt, idx) else {
        return SQLITE_RANGE;
    };

    let result = stmt.stmt.bind_at(index, Value::from_f64(val));
    sqlite3_bind_result(stmt, result)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_text(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    text: *const ffi::c_char,
    len: ffi::c_int,
    destructor: Option<unsafe extern "C" fn(*mut ffi::c_void)>,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = &mut *stmt;
    let Some(index) = sqlite3_bind_index_in_range(stmt_ref, idx) else {
        return SQLITE_RANGE;
    };
    if text.is_null() {
        let result = stmt_ref.stmt.bind_at(index, Value::Null);
        return sqlite3_bind_result(stmt_ref, result);
    }

    let static_ptr = std::ptr::null();
    let transient_ptr = -1isize as usize as *const ffi::c_void;
    let ptr_val = destructor
        .map(|f| f as *const ffi::c_void)
        .unwrap_or(static_ptr);

    let str_value = if len < 0 {
        match CStr::from_ptr(text).to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => return SQLITE_ERROR,
        }
    } else {
        let slice = std::slice::from_raw_parts(text as *const u8, len as usize);
        match std::str::from_utf8(slice) {
            Ok(s) => s.to_owned(),
            Err(_) => return SQLITE_ERROR,
        }
    };

    if ptr_val == transient_ptr {
        let val = Value::from_text(str_value);
        let result = stmt_ref.stmt.bind_at(index, val);
        let rc = sqlite3_bind_result(stmt_ref, result);
        if rc != SQLITE_OK {
            return rc;
        }
    } else if ptr_val == static_ptr {
        let slice = std::slice::from_raw_parts(text as *const u8, str_value.len());
        let val = Value::from_text(std::str::from_utf8(slice).unwrap());
        let result = stmt_ref.stmt.bind_at(index, val);
        let rc = sqlite3_bind_result(stmt_ref, result);
        if rc != SQLITE_OK {
            return rc;
        }
    } else {
        let slice = std::slice::from_raw_parts(text as *const u8, str_value.len());
        let val = Value::from_text(std::str::from_utf8(slice).unwrap());
        let result = stmt_ref.stmt.bind_at(index, val);
        let rc = sqlite3_bind_result(stmt_ref, result);
        if rc != SQLITE_OK {
            return rc;
        }

        stmt_ref
            .destructors
            .push((idx as usize, destructor, text as *mut ffi::c_void));
    }

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_blob(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
    blob: *const ffi::c_void,
    len: ffi::c_int,
    destructor: Option<unsafe extern "C" fn(*mut ffi::c_void)>,
) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = &mut *stmt;
    let Some(index) = sqlite3_bind_index_in_range(stmt_ref, idx) else {
        return SQLITE_RANGE;
    };
    if blob.is_null() {
        let result = stmt_ref.stmt.bind_at(index, Value::Null);
        return sqlite3_bind_result(stmt_ref, result);
    }

    let slice_blob = std::slice::from_raw_parts(blob as *const u8, len as usize).to_vec();

    let val_blob = Value::from_blob(slice_blob);

    let result = stmt_ref.stmt.bind_at(index, val_blob);
    let rc = sqlite3_bind_result(stmt_ref, result);
    if rc != SQLITE_OK {
        return rc;
    }

    if let Some(destructor_fn) = destructor {
        let ptr_val = destructor_fn as *const ffi::c_void;
        let static_ptr = std::ptr::null();
        let transient_ptr = usize::MAX as *const ffi::c_void;

        if ptr_val != static_ptr && ptr_val != transient_ptr {
            destructor_fn(blob as *mut _);
        }
    }

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_clear_bindings(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }

    let stmt_ref = &mut *stmt;
    stmt_ref.stmt.clear_bindings();

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_type(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> ffi::c_int {
    let stmt = &mut *stmt;
    let row = stmt
        .stmt
        .row()
        .expect("Function should only be called after `SQLITE_ROW`");

    match row.get::<&Value>(idx as usize) {
        Ok(turso_core::Value::Numeric(turso_core::Numeric::Integer(_))) => SQLITE_INTEGER,
        Ok(turso_core::Value::Text(_)) => SQLITE_TEXT,
        Ok(turso_core::Value::Numeric(turso_core::Numeric::Float(_))) => SQLITE_FLOAT,
        Ok(turso_core::Value::Blob(_)) => SQLITE_BLOB,
        _ => SQLITE_NULL,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    stmt.stmt.num_columns() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_decltype(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let stmt = &mut *stmt;

    if let Some(val) = stmt.stmt.get_column_decltype(idx as usize) {
        let c_string = CString::new(val).expect("CString::new failed");
        c_string.into_raw()
    } else {
        std::ptr::null()
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_name(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let idx = idx.try_into().unwrap();
    let stmt = &mut *stmt;

    let binding = stmt.stmt.get_column_name(idx).into_owned();
    let val = binding.as_str();

    if val.is_empty() {
        return std::ptr::null();
    }

    let c_string = CString::new(val).expect("CString::new failed");
    c_string.into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_table_name(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_char {
    let idx = idx.try_into().unwrap();
    let stmt = &mut *stmt;

    let binding = stmt
        .stmt
        .get_column_table_name(idx)
        .map(|cow| cow.into_owned())
        .unwrap_or_default();
    let val = binding.as_str();

    if val.is_empty() {
        return std::ptr::null();
    }

    let c_string = CString::new(val).expect("CString::new failed");
    c_string.into_raw()
}

/// Returns the value at column `idx` of the current row, or None when there
/// is no current row or the index is out of range.
unsafe fn column_value(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> Option<&'static Value> {
    if stmt.is_null() || idx < 0 {
        return None;
    }
    let stmt = &mut *stmt;
    let row = stmt.stmt.row()?;
    row.get::<&Value>(idx as usize).ok()
}

/// The sqlite3_column_* accessors apply SQLite's documented conversions
/// when the requested representation differs from the stored type (a text
/// column read through column_int64 parses like CAST, an integer read
/// through column_text renders as its decimal text, and so on); they never
/// fail on a legal call.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> i64 {
    match column_value(stmt, idx) {
        Some(Value::Numeric(turso_core::Numeric::Integer(i))) => *i,
        Some(v) => match v.exec_cast("INTEGER") {
            Ok(Value::Numeric(turso_core::Numeric::Integer(i))) => i,
            _ => 0,
        },
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_int(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> ffi::c_int {
    sqlite3_column_int64(stmt, idx) as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_double(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> f64 {
    match column_value(stmt, idx) {
        Some(Value::Numeric(turso_core::Numeric::Float(f))) => f64::from(*f),
        Some(v) => match v.exec_cast("REAL") {
            Ok(Value::Numeric(n)) => match n {
                turso_core::Numeric::Float(f) => f64::from(f),
                turso_core::Numeric::Integer(i) => i as f64,
            },
            _ => 0.0,
        },
        None => 0.0,
    }
}

/// Fills (when not yet done for this row) and returns the per-column cache
/// holding the column's text conversion, NUL-terminated: text bytes as-is,
/// raw blob bytes, or the decimal rendering of a number. None for NULL.
unsafe fn column_text_cache(stmt: *mut sqlite3_stmt, idx: ffi::c_int) -> Option<&'static Vec<u8>> {
    let value = column_value(stmt, idx)?;
    let stmt = &mut *stmt;
    let i = idx as usize;
    if i >= stmt.text_cache.len() {
        return None;
    }
    if stmt.text_cache[i].is_empty() {
        let buf = &mut stmt.text_cache[i];
        match value {
            Value::Null => return None,
            Value::Text(t) => buf.extend(t.as_str().as_bytes()),
            Value::Blob(b) => buf.extend(b.iter()),
            numeric => buf.extend(numeric.to_string().as_bytes()),
        }
        buf.push(0);
    }
    Some(&stmt.text_cache[i])
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_blob(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_void {
    if matches!(column_value(stmt, idx), Some(Value::Null) | None) {
        return std::ptr::null();
    }
    match column_text_cache(stmt, idx) {
        // A zero-length result (only the cache's NUL) reports NULL, as
        // sqlite3_column_blob does for zero-length blobs.
        Some(cache) if cache.len() > 1 => cache.as_ptr() as *const ffi::c_void,
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_bytes(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> ffi::c_int {
    match column_text_cache(stmt, idx) {
        Some(cache) => (cache.len() - 1) as ffi::c_int,
        None => 0,
    }
}

// sqlite3_value_* functions interpret the void* as a pointer to turso_ext::Value,
// which is what the function bridge passes for custom scalar function arguments.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_type(value: *mut ffi::c_void) -> ffi::c_int {
    if value.is_null() {
        return SQLITE_NULL;
    }
    let v = &*(value as *const ExtValue);
    match v.value_type() {
        turso_ext::ValueType::Null => SQLITE_NULL,
        turso_ext::ValueType::Integer => SQLITE_INTEGER,
        turso_ext::ValueType::Float => SQLITE_FLOAT,
        turso_ext::ValueType::Text => SQLITE_TEXT,
        turso_ext::ValueType::Blob => SQLITE_BLOB,
        turso_ext::ValueType::Error => SQLITE_NULL,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_int64(value: *mut ffi::c_void) -> i64 {
    if value.is_null() {
        return 0;
    }
    let v = &*(value as *const ExtValue);
    v.to_integer().unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_int(value: *mut ffi::c_void) -> ffi::c_int {
    sqlite3_value_int64(value) as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_double(value: *mut ffi::c_void) -> f64 {
    if value.is_null() {
        return 0.0;
    }
    let v = &*(value as *const ExtValue);
    v.to_float().unwrap_or(0.0)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_text(value: *mut ffi::c_void) -> *const ffi::c_uchar {
    if value.is_null() {
        return std::ptr::null();
    }
    let v = &*(value as *const ExtValue);
    match v.to_text() {
        Some(s) => s.as_ptr() as *const ffi::c_uchar,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_blob(value: *mut ffi::c_void) -> *const ffi::c_void {
    if value.is_null() {
        return std::ptr::null();
    }
    let v = &*(value as *const ExtValue);
    match v.blob_ref() {
        Some(b) => b.as_ptr() as *const ffi::c_void,
        None => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_bytes(value: *mut ffi::c_void) -> ffi::c_int {
    if value.is_null() {
        return 0;
    }
    let v = &*(value as *const ExtValue);
    match v.value_type() {
        turso_ext::ValueType::Text => v.to_text().map(|s| s.len()).unwrap_or(0) as ffi::c_int,
        turso_ext::ValueType::Blob => v.blob_ref().map(|b| b.len()).unwrap_or(0) as ffi::c_int,
        _ => 0,
    }
}

/// Deep-copies an `sqlite3_value`.  Returns a heap-allocated copy that must
/// be freed with `sqlite3_value_free`.  Diesel uses this to create
/// `OwnedSqliteValue` instances.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_dup(value: *mut ffi::c_void) -> *mut ffi::c_void {
    if value.is_null() {
        return std::ptr::null_mut();
    }
    let v = &*(value as *const ExtValue);
    let copy = match v.value_type() {
        turso_ext::ValueType::Integer => ExtValue::from_integer(v.to_integer().unwrap_or(0)),
        turso_ext::ValueType::Float => ExtValue::from_float(v.to_float().unwrap_or(0.0)),
        turso_ext::ValueType::Text => {
            let s = v.to_text().unwrap_or("");
            ExtValue::from_text(s.to_owned())
        }
        turso_ext::ValueType::Blob => {
            let b = v.to_blob().unwrap_or_default();
            ExtValue::from_blob(b.to_vec())
        }
        _ => ExtValue::null(),
    };
    Box::into_raw(Box::new(copy)) as *mut ffi::c_void
}

/// Frees a value previously allocated by `sqlite3_value_dup`.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_free(value: *mut ffi::c_void) {
    if value.is_null() {
        return;
    }
    let _ = Box::from_raw(value as *mut ExtValue);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_text(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *const ffi::c_uchar {
    match column_text_cache(stmt, idx) {
        Some(cache) => cache.as_ptr() as *const ffi::c_uchar,
        None => std::ptr::null(),
    }
}

/// Returns an `sqlite3_value*` (ExtValue pointer) for a column in the current
/// result row.  The pointer remains valid until the next `sqlite3_step()`,
/// `sqlite3_reset()`, or `sqlite3_finalize()`.  Diesel uses this via
/// `sqlite3_column_value` → `sqlite3_value_dup` to read all column types.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_value(
    stmt: *mut sqlite3_stmt,
    idx: ffi::c_int,
) -> *mut ffi::c_void {
    if stmt.is_null() || idx < 0 {
        return std::ptr::null_mut();
    }
    let stmt = &mut *stmt;
    let i = idx as usize;
    if i >= stmt.value_cache.len() {
        return std::ptr::null_mut();
    }
    // Return cached value if we already built one for this column.
    if stmt.value_cache[i].is_some() {
        return stmt.value_cache[i].as_mut().unwrap() as *mut ExtValue as *mut ffi::c_void;
    }
    let binding = stmt.stmt.row();
    let row = match binding.as_ref() {
        Some(row) => row,
        None => return std::ptr::null_mut(),
    };
    let ext = match row.get::<&Value>(i) {
        Ok(turso_core::Value::Numeric(turso_core::Numeric::Integer(n))) => {
            ExtValue::from_integer(*n)
        }
        Ok(turso_core::Value::Numeric(turso_core::Numeric::Float(f))) => {
            ExtValue::from_float(f64::from(*f))
        }
        Ok(turso_core::Value::Text(t)) => ExtValue::from_text(t.value.to_string()),
        Ok(turso_core::Value::Blob(b)) => ExtValue::from_blob(b.clone()),
        _ => ExtValue::null(),
    };
    stmt.value_cache[i] = Some(ext);
    stmt.value_cache[i].as_mut().unwrap() as *mut ExtValue as *mut ffi::c_void
}

pub struct TabResult {
    az_result: Vec<*mut ffi::c_char>,
    n_row: usize,
    n_column: usize,
    z_err_msg: Option<CString>,
    rc: ffi::c_int,
}

impl TabResult {
    fn new(initial_capacity: usize) -> Self {
        Self {
            az_result: Vec::with_capacity(initial_capacity),
            n_row: 0,
            n_column: 0,
            z_err_msg: None,
            rc: SQLITE_OK,
        }
    }

    fn free(&mut self) {
        for &ptr in &self.az_result {
            if !ptr.is_null() {
                unsafe {
                    sqlite3_free(ptr as *mut _);
                }
            }
        }
        self.az_result.clear();
    }
}

#[no_mangle]
unsafe extern "C" fn sqlite_get_table_cb(
    context: *mut ffi::c_void,
    n_column: ffi::c_int,
    argv: *mut *mut ffi::c_char,
    colv: *mut *mut ffi::c_char,
) -> ffi::c_int {
    let res = &mut *(context as *mut TabResult);

    if res.n_row == 0 {
        res.n_column = n_column as usize;
        for i in 0..n_column {
            let col_name = *colv.add(i as usize);
            let col_name_cstring = if !col_name.is_null() {
                CStr::from_ptr(col_name).to_owned()
            } else {
                CString::new("NULL").unwrap()
            };
            res.az_result.push(col_name_cstring.into_raw());
        }
    } else if res.n_column != n_column as usize {
        res.z_err_msg = Some(
            CString::new("sqlite3_get_table() called with two or more incompatible queries")
                .unwrap(),
        );
        res.rc = SQLITE_ERROR;
        return SQLITE_ERROR;
    }

    for i in 0..n_column {
        let value = *argv.add(i as usize);
        let value_cstring = if !value.is_null() {
            let value_cstr = CStr::from_ptr(value).to_bytes();
            let len = value_cstr.len();
            let mut buf = vec![0u8; len + 1];
            buf[0..len].copy_from_slice(value_cstr);
            CString::from_vec_with_nul(buf).unwrap()
        } else {
            CString::new("NULL").unwrap()
        };
        res.az_result.push(value_cstring.into_raw());
    }

    res.n_row += 1;
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_get_table(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    paz_result: *mut *mut *mut ffi::c_char,
    pn_row: *mut ffi::c_int,
    pn_column: *mut ffi::c_int,
    pz_err_msg: *mut *mut ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() || paz_result.is_null() {
        return SQLITE_ERROR;
    }

    let mut res = TabResult::new(20);

    let rc = sqlite3_exec(
        db,
        sql,
        Some(sqlite_get_table_cb),
        &mut res as *mut _ as *mut _,
        pz_err_msg,
    );

    if rc != SQLITE_OK {
        res.free();
        if let Some(err_msg) = res.z_err_msg {
            if !pz_err_msg.is_null() {
                *pz_err_msg = err_msg.into_raw();
            }
        }
        return rc;
    }

    let n_data = res.az_result.len();

    // Allocate a raw C array with an extra slot at position 0 for the entry count,
    // following the SQLite convention. sqlite3_free_table will step back to read it.
    let array = libc::malloc(std::mem::size_of::<*mut ffi::c_char>() * (n_data + 1))
        as *mut *mut ffi::c_char;
    if array.is_null() {
        res.free();
        return SQLITE_NOMEM;
    }

    // Store entry count at position 0 (matching SQLite's SQLITE_INT_TO_PTR convention)
    *array = (n_data + 1) as *mut ffi::c_char;

    // Copy string pointers to positions 1..=n_data
    for (i, &ptr) in res.az_result.iter().enumerate() {
        *array.add(i + 1) = ptr;
    }

    // Return pointer past the count slot
    *paz_result = array.add(1);
    *pn_row = res.n_row as ffi::c_int;
    *pn_column = res.n_column as ffi::c_int;

    // Drop res without freeing the strings -- they are now owned by the raw array.
    // Vec<*mut c_char> drop just deallocates the Vec buffer, not the pointed-to strings.

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free_table(az_result: *mut *mut ffi::c_char) {
    if az_result.is_null() {
        return;
    }
    // Step back one slot to read the entry count stored by sqlite3_get_table
    let array = az_result.sub(1);
    let n = *array as usize;
    for i in 1..n {
        let ptr = *array.add(i);
        if !ptr.is_null() {
            sqlite3_free(ptr as *mut _);
        }
    }
    libc::free(array as *mut _);
}

// sqlite3_result_* functions set the return value of a custom SQL function.
// The context pointer is a *mut SqliteContext cast to void*.

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_null(context: *mut ffi::c_void) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::null();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_int64(context: *mut ffi::c_void, val: i64) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::from_integer(val);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_int(context: *mut ffi::c_void, val: ffi::c_int) {
    sqlite3_result_int64(context, val as i64);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_double(context: *mut ffi::c_void, val: f64) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::from_float(val);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_text(
    context: *mut ffi::c_void,
    text: *const ffi::c_char,
    len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) {
    if context.is_null() || text.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    let s = if len < 0 {
        CStr::from_ptr(text).to_string_lossy().into_owned()
    } else {
        let bytes = std::slice::from_raw_parts(text as *const u8, len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    };
    ctx.result = ExtValue::from_text(s);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_blob(
    context: *mut ffi::c_void,
    blob: *const ffi::c_void,
    len: ffi::c_int,
    _destroy: *mut ffi::c_void,
) {
    if context.is_null() || blob.is_null() || len < 0 {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    let bytes = std::slice::from_raw_parts(blob as *const u8, len as usize).to_vec();
    ctx.result = ExtValue::from_blob(bytes);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_nomem(context: *mut ffi::c_void) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::error(turso_ext::ResultCode::OoM);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_toobig(context: *mut ffi::c_void) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    ctx.result = ExtValue::error(turso_ext::ResultCode::Error);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error(
    context: *mut ffi::c_void,
    err: *const ffi::c_char,
    len: ffi::c_int,
) {
    if context.is_null() {
        return;
    }
    let ctx = &mut *(context as *mut SqliteContext);
    let msg = if err.is_null() {
        String::new()
    } else if len < 0 {
        CStr::from_ptr(err).to_string_lossy().into_owned()
    } else {
        let bytes = std::slice::from_raw_parts(err as *const u8, len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    };
    ctx.result = ExtValue::error_with_message(msg);
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_aggregate_context(
    _context: *mut ffi::c_void,
    _n: ffi::c_int,
) -> *mut ffi::c_void {
    stub!();
}

// `unsafe_op_in_unsafe_fn`: opt this FFI entry point out of the legacy rule that an
// `unsafe fn` body is one big implicit `unsafe` block. Every raw-pointer operation
// must now sit in its own minimal `unsafe {}` with a SAFETY note, and the compiler
// rejects any that escapes — so the pointer-validation logic is provably outside the
// unsafe surface and the unsafe surface stays as small as the hazard.
#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_blob_open(
    db: *mut sqlite3,
    db_name: *const ffi::c_char,
    table_name: *const ffi::c_char,
    column_name: *const ffi::c_char,
    rowid: i64,
    flags: ffi::c_int,
    blob_out: *mut *mut ffi::c_void,
) -> ffi::c_int {
    if blob_out.is_null() {
        return SQLITE_MISUSE;
    }
    // SQLite documents that *ppBlob is NULL after any failure, and callers rely on
    // it to unconditionally call sqlite3_blob_close(*ppBlob). Clear it before any
    // error return so no stack garbage can ever reach Box::from_raw in close.
    // SAFETY: blob_out is non-null (checked); caller owns the out-pointer.
    unsafe { *blob_out = std::ptr::null_mut() };
    if db.is_null() || db_name.is_null() || table_name.is_null() || column_name.is_null() {
        return SQLITE_MISUSE;
    }
    // SAFETY: db checked non-null just above; caller guarantees it points to a live
    // sqlite3 for the duration of the call.
    let db: &sqlite3 = unsafe { &*db };
    let mut db = db.inner.lock().unwrap();
    // SAFETY: db_name checked non-null; caller guarantees a valid NUL-terminated C string.
    let database = match unsafe { CStr::from_ptr(db_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    // Only the main database is supported; resolving "temp" or an attached name
    // against main would silently open the wrong table's bytes for writing.
    if !database.eq_ignore_ascii_case("main") {
        // SAFETY: set_db_err only touches the already-valid &mut inner handle.
        return unsafe {
            set_db_err(
                &mut db,
                turso_core::LimboError::InternalError(format!("no such database: {database}")),
            )
        };
    }
    // SAFETY: table_name checked non-null; caller guarantees a valid NUL-terminated C string.
    let table = match unsafe { CStr::from_ptr(table_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    // SAFETY: column_name checked non-null; caller guarantees a valid NUL-terminated C string.
    let column = match unsafe { CStr::from_ptr(column_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    match db.conn.blob_open(table, column, rowid, flags != 0) {
        Ok(blob) => {
            // SAFETY: blob_out is non-null (checked); we transfer ownership of the box.
            unsafe { *blob_out = Box::into_raw(Box::new(blob)) as *mut ffi::c_void };
            SQLITE_OK
        }
        // SAFETY: set_db_err only touches the already-valid &mut inner handle.
        Err(err) => unsafe { set_db_err(&mut db, err) },
    }
}

#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_blob_read(
    blob: *mut ffi::c_void,
    data: *mut ffi::c_void,
    n: ffi::c_int,
    offset: ffi::c_int,
) -> ffi::c_int {
    if blob.is_null() {
        return SQLITE_MISUSE;
    }
    // Negative sizes and offsets are SQLITE_ERROR per the sqlite3_blob_read docs
    // ("If N or iOffset is less than zero, SQLITE_ERROR is returned"), and like any
    // range error they leave the handle usable.
    if n < 0 || offset < 0 {
        return SQLITE_ERROR;
    }
    // SAFETY: blob checked non-null; caller guarantees it is a live handle from
    // sqlite3_blob_open that has not been closed.
    let blob = unsafe { &mut *(blob as *mut turso_core::Blob) };
    if n == 0 {
        // A zero-length read still range-checks the offset and still fails on an
        // expired handle, but must never manufacture a slice from the data pointer:
        // slice::from_raw_parts is undefined for NULL even at length 0.
        return match blob.read(offset as usize, &mut []) {
            Ok(()) => SQLITE_OK,
            Err(err) => limbo_err_code(&err),
        };
    }
    if data.is_null() {
        return SQLITE_MISUSE;
    }
    // SAFETY: data checked non-null and n > 0 and non-negative; the caller
    // guarantees data points to at least n writable, initialized-or-writable bytes.
    let buf = unsafe { std::slice::from_raw_parts_mut(data as *mut u8, n as usize) };
    match blob.read(offset as usize, buf) {
        Ok(()) => SQLITE_OK,
        Err(err) => limbo_err_code(&err),
    }
}

#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_blob_write(
    blob: *mut ffi::c_void,
    data: *const ffi::c_void,
    n: ffi::c_int,
    offset: ffi::c_int,
) -> ffi::c_int {
    if blob.is_null() {
        return SQLITE_MISUSE;
    }
    // See sqlite3_blob_read: negative arguments are SQLITE_ERROR, not misuse.
    if n < 0 || offset < 0 {
        return SQLITE_ERROR;
    }
    // SAFETY: blob checked non-null; caller guarantees it is a live, open handle.
    let blob = unsafe { &mut *(blob as *mut turso_core::Blob) };
    if n == 0 {
        // See sqlite3_blob_read: keep the range/expiry/read-only checks without
        // touching the possibly-NULL data pointer.
        return match blob.write(offset as usize, &[]) {
            Ok(()) => SQLITE_OK,
            Err(err) => limbo_err_code(&err),
        };
    }
    if data.is_null() {
        return SQLITE_MISUSE;
    }
    // SAFETY: data checked non-null and n > 0 and non-negative; the caller
    // guarantees data points to at least n readable bytes.
    let buf = unsafe { std::slice::from_raw_parts(data as *const u8, n as usize) };
    match blob.write(offset as usize, buf) {
        Ok(()) => SQLITE_OK,
        Err(err) => limbo_err_code(&err),
    }
}

#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_blob_bytes(blob: *mut ffi::c_void) -> ffi::c_int {
    if blob.is_null() {
        return 0;
    }
    // SAFETY: blob checked non-null; caller guarantees a live, open handle. Shared
    // (&) borrow only.
    let blob = unsafe { &*(blob as *const turso_core::Blob) };
    // A value cannot legitimately exceed SQLite's 2^31-1 length limit; if one
    // somehow does, abort rather than hand the C caller a truncated length it
    // would use to size buffers.
    blob.bytes()
        .try_into()
        .expect("blob length exceeds the SQLite length limit")
}

#[no_mangle]
#[deny(unsafe_op_in_unsafe_fn)]
pub unsafe extern "C" fn sqlite3_blob_close(blob: *mut ffi::c_void) -> ffi::c_int {
    if blob.is_null() {
        return SQLITE_OK;
    }
    // The handle is freed unconditionally, like SQLite; the return code only
    // reports whether the final commit of pending writes succeeded.
    // SAFETY: blob checked non-null; caller guarantees it came from a single
    // sqlite3_blob_open and is not double-closed, so reclaiming the box is sound.
    let blob = unsafe { Box::from_raw(blob as *mut turso_core::Blob) };
    match blob.close() {
        Ok(()) => SQLITE_OK,
        Err(err) => limbo_err_code(&err),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stricmp(
    a: *const ffi::c_char,
    b: *const ffi::c_char,
) -> ffi::c_int {
    sqlite3_strnicmp(a, b, ffi::c_int::MAX)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_strnicmp(
    a: *const ffi::c_char,
    b: *const ffi::c_char,
    n: ffi::c_int,
) -> ffi::c_int {
    if a.is_null() {
        return if b.is_null() { 0 } else { -1 };
    } else if b.is_null() {
        return 1;
    }
    if n <= 0 {
        return 0;
    }

    let mut p_a = a as *const u8;
    let mut p_b = b as *const u8;
    let mut rem = n;

    while rem > 0 && *p_a != 0 && p_a.read().eq_ignore_ascii_case(&p_b.read()) {
        p_a = p_a.add(1);
        p_b = p_b.add(1);
        rem -= 1;
    }

    if rem <= 0 {
        0
    } else {
        (p_a.read().to_ascii_lowercase() as ffi::c_int)
            - (p_b.read().to_ascii_lowercase() as ffi::c_int)
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_collation_v2(
    _db: *mut sqlite3,
    _name: *const ffi::c_char,
    _enc: ffi::c_int,
    _context: *mut ffi::c_void,
    _cmp: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    stub!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_function_v2(
    db: *mut sqlite3,
    name: *const ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    context: *mut ffi::c_void,
    func: Option<unsafe extern "C" fn()>,
    _step: Option<unsafe extern "C" fn()>,
    _final_: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    // Only scalar functions (xFunc) are supported for now; skip aggregate registration.
    let x_func_raw = match func {
        Some(f) => f,
        None => return SQLITE_OK,
    };
    // Cast the opaque fn() pointer to the real scalar callback signature.
    let x_func: unsafe extern "C" fn(*mut ffi::c_void, ffi::c_int, *mut *mut ffi::c_void) =
        std::mem::transmute(x_func_raw);

    let func_name = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_owned(),
        Err(_) => return SQLITE_MISUSE,
    };

    // Cast the destroy callback.
    let destroy_fn: Option<unsafe extern "C" fn(*mut ffi::c_void)> =
        _destroy.map(|f| std::mem::transmute(f));

    // Allocate a bridge slot.  Reuse an existing slot with the same name
    // (invoking the old destroy callback) before falling back to a free slot.
    let mut slots = func_slots().lock().unwrap();
    let slot_id = if let Some(id) = slots
        .iter()
        .position(|s| s.as_ref().is_some_and(|s| s.name == func_name))
    {
        // Reuse existing slot — invoke old destroy callback on old user data.
        if let Some(old) = slots[id].take() {
            if old.destroy != 0 {
                let old_destroy: unsafe extern "C" fn(*mut ffi::c_void) =
                    std::mem::transmute(old.destroy);
                old_destroy(old.p_app as *mut ffi::c_void);
            }
        }
        id
    } else {
        match slots.iter().position(|s| s.is_none()) {
            Some(id) => id,
            None => return SQLITE_ERROR, // all 32 slots used
        }
    };
    slots[slot_id] = Some(FuncSlot {
        x_func,
        p_app: context as usize,
        destroy: destroy_fn.map_or(0, |f| f as usize),
        name: func_name.clone(),
        db: db as usize,
    });
    drop(slots);

    let bridge = FUNC_BRIDGES[slot_id];

    let func_name_c = match CString::new(func_name.as_str()) {
        Ok(s) => s,
        Err(_) => {
            func_slots().lock().unwrap()[slot_id] = None;
            return SQLITE_ERROR;
        }
    };

    let db_ref = &*db;
    let inner = db_ref.inner.lock().unwrap();
    let api = inner.conn._build_turso_ext();
    let rc = (api.register_scalar_function)(
        api.ctx,
        func_name_c.as_ptr(),
        _n_args,
        false,
        0,
        bridge,
        None,
        None,
    );
    inner.conn._free_extension_ctx(api);

    if rc != turso_ext::ResultCode::OK {
        func_slots().lock().unwrap()[slot_id] = None;
        return SQLITE_ERROR;
    }

    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_window_function(
    _db: *mut sqlite3,
    _name: *const ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    _context: *mut ffi::c_void,
    _x_step: Option<unsafe extern "C" fn()>,
    _x_final: Option<unsafe extern "C" fn()>,
    _x_value: Option<unsafe extern "C" fn()>,
    _x_inverse: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    stub!();
}

/// Returns the error message for the most recent failed API call to connection.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_errmsg(db: *mut sqlite3) -> *const ffi::c_char {
    if db.is_null() {
        return sqlite3_errstr(SQLITE_NOMEM);
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    if !sqlite3_safety_check_sick_or_ok(&db) {
        return sqlite3_errstr(SQLITE_MISUSE);
    }
    if db.malloc_failed {
        return sqlite3_errstr(SQLITE_NOMEM);
    }
    let err_msg = if db.err_code != SQLITE_OK {
        if !db.p_err.is_null() {
            db.p_err as *const ffi::c_char
        } else {
            std::ptr::null()
        }
    } else {
        std::ptr::null()
    };
    if err_msg.is_null() {
        return sqlite3_errstr(db.err_code);
    }
    err_msg
}

/// Returns the extended error code for the most recent failed API call to connection.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extended_errcode(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    if !sqlite3_safety_check_sick_or_ok(&db) {
        return SQLITE_MISUSE;
    }
    if db.malloc_failed {
        return SQLITE_NOMEM;
    }
    // The extended code is reported regardless of the
    // sqlite3_extended_result_codes() setting; only sqlite3_errcode masks.
    db.err_code
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_complete(sql: *const ffi::c_char) -> ffi::c_int {
    if sql.is_null() {
        return 0;
    }
    let s = CStr::from_ptr(sql).to_bytes();
    s.iter()
        .rev()
        .find(|&&b| !b.is_ascii_whitespace())
        .map_or(0, |&b| (b == b';') as ffi::c_int)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_threadsafe() -> ffi::c_int {
    1
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion() -> *const ffi::c_char {
    SQLITE_VERSION_C_STRING
        .get_or_init(|| {
            CString::new(SQLITE_VERSION).expect("SQLITE_VERSION must not contain a NUL byte")
        })
        .as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion_number() -> ffi::c_int {
    SQLITE_VERSION_NUMBER
}

fn sqlite3_errstr_impl(rc: i32) -> *const ffi::c_char {
    static ERROR_MESSAGES: [&[u8]; 29] = [
        b"not an error\0",                         // SQLITE_OK
        b"SQL logic error\0",                      // SQLITE_ERROR
        b"\0",                                     // SQLITE_INTERNAL
        b"access permission denied\0",             // SQLITE_PERM
        b"query aborted\0",                        // SQLITE_ABORT
        b"database is locked\0",                   // SQLITE_BUSY
        b"database table is locked\0",             // SQLITE_LOCKED
        b"out of memory\0",                        // SQLITE_NOMEM
        b"attempt to write a readonly database\0", // SQLITE_READONLY
        b"interrupted\0",                          // SQLITE_INTERRUPT
        b"disk I/O error\0",                       // SQLITE_IOERR
        b"database disk image is malformed\0",     // SQLITE_CORRUPT
        b"unknown operation\0",                    // SQLITE_NOTFOUND
        b"database or disk is full\0",             // SQLITE_FULL
        b"unable to open database file\0",         // SQLITE_CANTOPEN
        b"locking protocol\0",                     // SQLITE_PROTOCOL
        b"\0",                                     // SQLITE_EMPTY
        b"database schema has changed\0",          // SQLITE_SCHEMA
        b"string or blob too big\0",               // SQLITE_TOOBIG
        b"constraint failed\0",                    // SQLITE_CONSTRAINT
        b"datatype mismatch\0",                    // SQLITE_MISMATCH
        b"bad parameter or other API misuse\0",    // SQLITE_MISUSE
        #[cfg(feature = "lfs")]
        b"\0",      // SQLITE_NOLFS
        #[cfg(not(feature = "lfs"))]
        b"large file support is disabled\0", // SQLITE_NOLFS
        b"authorization denied\0",                 // SQLITE_AUTH
        b"\0",                                     // SQLITE_FORMAT
        b"column index out of range\0",            // SQLITE_RANGE
        b"file is not a database\0",               // SQLITE_NOTADB
        b"notification message\0",                 // SQLITE_NOTICE
        b"warning message\0",                      // SQLITE_WARNING
    ];

    static UNKNOWN_ERROR: &[u8] = b"unknown error\0";
    static ABORT_ROLLBACK: &[u8] = b"abort due to ROLLBACK\0";
    static ANOTHER_ROW_AVAILABLE: &[u8] = b"another row available\0";
    static NO_MORE_ROWS_AVAILABLE: &[u8] = b"no more rows available\0";

    let msg = match rc {
        SQLITE_ABORT_ROLLBACK => ABORT_ROLLBACK,
        SQLITE_ROW => ANOTHER_ROW_AVAILABLE,
        SQLITE_DONE => NO_MORE_ROWS_AVAILABLE,
        _ => {
            let rc = rc & 0xff;
            let idx = rc & 0xff;
            if (idx as usize) < ERROR_MESSAGES.len() && !ERROR_MESSAGES[rc as usize].is_empty() {
                ERROR_MESSAGES[rc as usize]
            } else {
                UNKNOWN_ERROR
            }
        }
    };

    msg.as_ptr() as *const ffi::c_char
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_wal_checkpoint(
    db: *mut sqlite3,
    db_name: *const ffi::c_char,
) -> ffi::c_int {
    sqlite3_wal_checkpoint_v2(
        db,
        db_name,
        SQLITE_CHECKPOINT_PASSIVE,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    )
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_wal_checkpoint_v2(
    db: *mut sqlite3,
    _db_name: *const ffi::c_char,
    mode: ffi::c_int,
    log_size: *mut ffi::c_int,
    checkpoint_count: *mut ffi::c_int,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    let chkptmode = match mode {
        SQLITE_CHECKPOINT_PASSIVE => CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
        SQLITE_CHECKPOINT_RESTART => CheckpointMode::Restart,
        SQLITE_CHECKPOINT_TRUNCATE => CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
        SQLITE_CHECKPOINT_FULL => CheckpointMode::Full,
        _ => return SQLITE_MISUSE, // Unsupported mode
    };
    match db.conn.checkpoint(chkptmode) {
        Ok(res) => {
            if !log_size.is_null() {
                (*log_size) = res.wal_max_frame as ffi::c_int;
            }
            if !checkpoint_count.is_null() {
                (*checkpoint_count) = res.wal_checkpoint_backfilled as ffi::c_int;
            }
            SQLITE_OK
        }
        Err(e) => {
            if matches!(e, turso_core::LimboError::Busy) {
                SQLITE_BUSY
            } else {
                SQLITE_ERROR
            }
        }
    }
}

/// Get the number of frames in the WAL.
///
/// The `libsql_wal_frame_count` function returns the number of frames
/// in the WAL in the `p_frame_count` parameter.
///
/// # Returns
///
/// - `SQLITE_OK` if the number of frames in the WAL file is
///   successfully returned.
/// - `SQLITE_MISUSE` if the `db` is `NULL`.
/// - `SQLITE_ERROR` if an error occurs while getting the number of frames
///   in the WAL file.
///
/// # Safety
///
/// - The `db` must be a valid pointer to a `sqlite3` database connection.
/// - The `p_frame_count` must be a valid pointer to a `u32` that will store
///   the number of frames in the WAL file.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_frame_count(
    db: *mut sqlite3,
    p_frame_count: *mut u32,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    let frame_count = match db.conn.wal_state() {
        Ok(state) => state.max_frame as u32,
        Err(_) => return SQLITE_ERROR,
    };
    *p_frame_count = frame_count;
    SQLITE_OK
}

/// Get a frame from the WAL file
///
/// The `libsql_wal_get_frame` function extracts frame `frame_no` from
/// the WAL for database connection `db` into memory pointed to by `p_frame`
/// of size `frame_len`.
///
/// # Returns
///
/// - `SQLITE_OK` if the frame is successfully returned.
/// - `SQLITE_MISUSE` if the `db` is `NULL`.
/// - `SQLITE_ERROR` if an error occurs while getting the frame.
///
/// # Safety
///
/// - The `db` must be a valid pointer to a `sqlite3` database connection.
/// - The `frame_no` must be a valid frame index.
/// - The `p_frame` must be a valid pointer to a `u8` that will store
///   the frame data.
/// - The `frame_len` must be the size of the frame.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_get_frame(
    db: *mut sqlite3,
    frame_no: u32,
    p_frame: *mut u8,
    frame_len: u32,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    let frame = std::slice::from_raw_parts_mut(p_frame, frame_len as usize);
    match db.conn.wal_get_frame(frame_no as u64, frame) {
        Ok(..) => SQLITE_OK,
        Err(_) => SQLITE_ERROR,
    }
}

/// Insert a frame into the WAL file
///
/// The `libsql_wal_insert_frame` function insert frame at position frame_no
/// with content specified into memory pointed by `p_frame` of size `frame_len`.
///
/// # Returns
///
/// - `SQLITE_OK` if the frame is successfully inserted.
/// - `SQLITE_MISUSE` if the `db` is `NULL`.
/// - `SQLITE_ERROR` if an error occurs while inserting the frame.
///
/// # Safety
///
/// - The `db` must be a valid pointer to a `sqlite3` database connection.
/// - The `frame_no` must be a valid frame index.
/// - The `p_frame` must be a valid pointer to a `u8` that stores the frame data.
/// - The `frame_len` must be the size of the frame.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_insert_frame(
    db: *mut sqlite3,
    frame_no: u32,
    p_frame: *const u8,
    frame_len: u32,
    p_conflict: *mut i32,
) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    let frame = std::slice::from_raw_parts(p_frame, frame_len as usize);
    match db.conn.wal_insert_frame(frame_no as u64, frame) {
        Ok(_) => SQLITE_OK,
        Err(LimboError::Conflict(..)) => {
            if !p_conflict.is_null() {
                *p_conflict = 1;
            }
            SQLITE_ERROR
        }
        Err(_) => SQLITE_ERROR,
    }
}

/// Disable WAL checkpointing.
///
/// Note: This function disables WAL checkpointing entirely for the connection. This is different from
/// sqlite3_wal_autocheckpoint() which only disables automatic checkpoints
/// for the current connection, but still allows checkpointing when the
/// connection is closed.
#[no_mangle]
pub unsafe extern "C" fn libsql_wal_disable_checkpoint(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &sqlite3 = &*db;
    let db = db.inner.lock().unwrap();
    db.conn.wal_auto_actions_disable();
    SQLITE_OK
}

fn sqlite3_safety_check_sick_or_ok(db: &sqlite3Inner) -> bool {
    matches!(
        db.e_open_state,
        SQLITE_STATE_SICK | SQLITE_STATE_OPEN | SQLITE_STATE_BUSY
    )
}

// https://sqlite.org/c3ref/table_column_metadata.html
#[no_mangle]
pub unsafe extern "C" fn sqlite3_table_column_metadata(
    db: *mut sqlite3,
    z_db_name: *const ffi::c_char,
    z_table_name: *const ffi::c_char,
    z_column_name: *const ffi::c_char,
    pz_data_type: *mut *const ffi::c_char,
    pz_coll_seq: *mut *const ffi::c_char,
    p_not_null: *mut ffi::c_int,
    p_primary_key: *mut ffi::c_int,
    p_autoinc: *mut ffi::c_int,
) -> ffi::c_int {
    trace!("sqlite3_table_column_metadata");

    let mut rc = SQLITE_OK;
    let mut z_data_type: *const ffi::c_char = std::ptr::null();
    let mut z_coll_seq: *const ffi::c_char = std::ptr::null();
    let mut not_null = 0;
    let mut primary_key = 0;
    let mut autoinc = 0;

    // Safety checks
    if db.is_null() || z_table_name.is_null() {
        return SQLITE_MISUSE;
    }

    let db_inner = &(*db).inner.lock().unwrap();

    // Convert C strings to Rust strings
    let table_name = match CStr::from_ptr(z_table_name).to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };

    // Handle database name (can be NULL for main database)
    let db_name = if z_db_name.is_null() {
        "main"
    } else {
        match CStr::from_ptr(z_db_name).to_str() {
            Ok(s) => s,
            Err(_) => return SQLITE_MISUSE,
        }
    };

    // For now, we only support the main database
    if db_name != "main" {
        rc = SQLITE_ERROR;
    } else {
        // Handle column name (can be NULL to just check table existence)
        if !z_column_name.is_null() {
            let column_name = match CStr::from_ptr(z_column_name).to_str() {
                Ok(s) => s,
                Err(_) => return SQLITE_MISUSE,
            };

            // Use pragma table_info to get column information
            match db_inner
                .conn
                .pragma_query(&format!("table_info({table_name})"))
            {
                Ok(rows) => {
                    let mut found_column = false;
                    for row in rows {
                        let col_name: &str = match &row[1] {
                            turso_core::Value::Text(text) => text.as_str(),
                            _ => return SQLITE_ERROR,
                        }; // name column
                        if col_name == column_name {
                            // Found the column, extract metadata
                            let col_type: String = match &row[2] {
                                turso_core::Value::Text(text) => text.as_str().to_string(),
                                _ => return SQLITE_ERROR,
                            }; // type column
                            let col_notnull: i64 = row[3].as_int().unwrap(); // notnull column
                            let col_pk: i64 = row[5].as_int().unwrap(); // pk column

                            z_data_type = CString::new(col_type)
                                .expect("CString::new failed")
                                .into_raw();
                            z_coll_seq = CString::new("BINARY")
                                .expect("CString::new failed")
                                .into_raw();
                            not_null = if col_notnull != 0 { 1 } else { 0 };
                            primary_key = if col_pk != 0 { 1 } else { 0 };

                            // For now, we don't support auto-increment detection
                            autoinc = 0;

                            found_column = true;
                            break;
                        }
                    }

                    if !found_column {
                        // Check if it's a rowid reference
                        if column_name == "rowid"
                            || column_name == "oid"
                            || column_name == "_rowid_"
                        {
                            // For rowid columns, return INTEGER type
                            z_data_type = CString::new("INTEGER")
                                .expect("CString::new failed")
                                .into_raw();
                            z_coll_seq = CString::new("BINARY")
                                .expect("CString::new failed")
                                .into_raw();
                            not_null = 0;
                            primary_key = 1;
                            autoinc = 0;
                        } else {
                            rc = SQLITE_ERROR;
                        }
                    }
                }
                Err(_) => {
                    rc = SQLITE_ERROR;
                }
            }
        }
    }

    // Set output parameters
    if !pz_data_type.is_null() {
        *pz_data_type = z_data_type;
    }
    if !pz_coll_seq.is_null() {
        *pz_coll_seq = z_coll_seq;
    }
    if !p_not_null.is_null() {
        *p_not_null = not_null;
    }
    if !p_primary_key.is_null() {
        *p_primary_key = primary_key;
    }
    if !p_autoinc.is_null() {
        *p_autoinc = autoinc;
    }

    rc
}

fn limbo_err_code(err: &LimboError) -> i32 {
    match err {
        LimboError::Corrupt(..) => SQLITE_CORRUPT,
        LimboError::NotADB => SQLITE_NOTADB,
        LimboError::Constraint(_) | LimboError::ForeignKeyConstraint(_) => SQLITE_CONSTRAINT,
        LimboError::DatabaseFull(_) => SQLITE_FULL,
        LimboError::TableLocked => SQLITE_LOCKED,
        LimboError::ReadOnly => SQLITE_READONLY,
        LimboError::Busy => SQLITE_BUSY,
        // SQLite reports operations on an expired blob handle (its row's table was
        // written after sqlite3_blob_open) as SQLITE_ABORT.
        LimboError::BlobHandleExpired => SQLITE_ABORT,
        // SQLite reports its "SQL statements in progress" rejections as
        // error-class SQLITE_BUSY (vdbe.c, OP_AutoCommit / OP_Savepoint).
        LimboError::StatementsInProgress(_) => SQLITE_BUSY,
        LimboError::SchemaUpdated | LimboError::SchemaConflict => SQLITE_SCHEMA,
        _ => SQLITE_ERROR,
    }
}

fn handle_limbo_err(err: LimboError, container: *mut *mut ffi::c_char) -> i32 {
    let code = limbo_err_code(&err);
    if !container.is_null() {
        let err_msg = format!("{err}");
        unsafe { *container = CString::new(err_msg).unwrap().into_raw() };
    }
    code
}

/// Store a LimboError on the database handle, returning the SQLite error code.
unsafe fn set_db_err(db: &mut sqlite3Inner, err: LimboError) -> i32 {
    set_db_err_msg(db, limbo_err_code(&err), &format!("{err}"))
}

unsafe fn set_db_err_msg(db: &mut sqlite3Inner, code: i32, err_msg: &str) -> i32 {
    if !db.p_err.is_null() {
        let _ = CString::from_raw(db.p_err as *mut ffi::c_char);
    }
    db.p_err = CString::new(err_msg).unwrap().into_raw() as *mut ffi::c_void;
    db.err_code = code;
    code
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    /// sqlite3_errmsg must return the bare message SQLite produces — no
    /// "Runtime error:" prefix and no "(19)" suffix; those are sqlite3
    /// shell decoration, not part of the message.
    #[test]
    fn test_sqlite3_errmsg_constraint_failure_matches_sqlite() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE u(a UNIQUE); INSERT INTO u VALUES (1);".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let mut stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO u VALUES (1)".as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(stmt), SQLITE_CONSTRAINT);
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
            assert_eq!(msg, "UNIQUE constraint failed: u.a");
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }
    }

    /// A statement that hit SQLITE_BUSY cannot be run to completion at
    /// finalize time while another connection still holds the write lock.
    /// sqlite3_finalize must free it anyway and report the error, like
    /// SQLite does. Returning early instead leaked the statement and left
    /// it counted as an active root statement on the connection forever,
    /// so every later "no statements active" check failed — an explicit
    /// checkpoint always errored and DETACH reported the database locked.
    #[test]
    fn test_finalize_frees_statement_stuck_on_busy() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let (writer, blocked, stmt) = prepare_statement_stuck_on_busy(&dir);

            assert_eq!(sqlite3_finalize(stmt), SQLITE_BUSY);

            assert_eq!(
                sqlite3_exec(
                    writer,
                    c"COMMIT".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            // The finalized statement must no longer count as active: an
            // explicit checkpoint refuses to run while a statement is in
            // progress on the connection.
            assert_eq!(sqlite3_wal_checkpoint(blocked, ptr::null()), SQLITE_OK);
            assert_eq!(sqlite3_close(blocked), SQLITE_OK);
            assert_eq!(sqlite3_close(writer), SQLITE_OK);
        }
    }

    /// Same contract for sqlite3_reset: it must reset the statement even
    /// when the pending execution cannot be completed, returning the error
    /// of the most recent evaluation. The statement stays usable and stops
    /// counting as active once finalized.
    #[test]
    fn test_reset_resets_statement_stuck_on_busy() {
        unsafe {
            let dir = tempfile::tempdir().unwrap();
            let (writer, blocked, stmt) = prepare_statement_stuck_on_busy(&dir);

            assert_eq!(sqlite3_reset(stmt), SQLITE_BUSY);

            assert_eq!(
                sqlite3_exec(
                    writer,
                    c"COMMIT".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            // The reset must have released the statement's active slot
            // already, before it is stepped again: an explicit checkpoint
            // refuses to run while a statement is in progress.
            assert_eq!(sqlite3_wal_checkpoint(blocked, ptr::null()), SQLITE_OK);
            // The reset statement runs again from the start now that the
            // lock is gone.
            assert_eq!(sqlite3_step(stmt), SQLITE_DONE);
            assert_eq!(sqlite3_finalize(stmt), SQLITE_OK);
            assert_eq!(sqlite3_close(blocked), SQLITE_OK);
            assert_eq!(sqlite3_close(writer), SQLITE_OK);
        }
    }

    /// Opens two connections to the same file, makes `writer` hold the write
    /// lock, and returns a statement on the second connection whose step just
    /// failed with SQLITE_BUSY.
    unsafe fn prepare_statement_stuck_on_busy(
        dir: &tempfile::TempDir,
    ) -> (*mut sqlite3, *mut sqlite3, *mut sqlite3_stmt) {
        let path = CString::new(dir.path().join("busy.db").to_str().unwrap()).unwrap();
        let mut writer = ptr::null_mut();
        let mut blocked = ptr::null_mut();
        assert_eq!(sqlite3_open(path.as_ptr(), &mut writer), SQLITE_OK);
        assert_eq!(sqlite3_open(path.as_ptr(), &mut blocked), SQLITE_OK);
        assert_eq!(
            sqlite3_exec(
                writer,
                c"CREATE TABLE t(x); BEGIN; INSERT INTO t VALUES (1);".as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            SQLITE_OK
        );

        let mut stmt = ptr::null_mut();
        assert_eq!(
            sqlite3_prepare_v2(
                blocked,
                c"INSERT INTO t VALUES (2)".as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            ),
            SQLITE_OK
        );
        assert_eq!(sqlite3_step(stmt), SQLITE_BUSY);
        (writer, blocked, stmt)
    }

    #[test]
    fn test_sqlite3_stmt_status_rows_read_written() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);

            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(x); INSERT INTO t VALUES (1), (2);".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            let mut insert_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"INSERT INTO t VALUES (3)".as_ptr(),
                    -1,
                    &mut insert_stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_step(insert_stmt), SQLITE_DONE);
            assert_eq!(
                sqlite3_stmt_status(insert_stmt, LIBSQL_STMTSTATUS_ROWS_WRITTEN, 0),
                1
            );
            assert_eq!(
                sqlite3_stmt_status(insert_stmt, LIBSQL_STMTSTATUS_ROWS_WRITTEN, 1),
                1
            );
            assert_eq!(
                sqlite3_stmt_status(insert_stmt, LIBSQL_STMTSTATUS_ROWS_WRITTEN, 0),
                0
            );
            assert_eq!(sqlite3_finalize(insert_stmt), SQLITE_OK);

            let mut select_stmt = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT x FROM t ORDER BY x".as_ptr(),
                    -1,
                    &mut select_stmt,
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            while sqlite3_step(select_stmt) == SQLITE_ROW {}

            let rows_read = sqlite3_stmt_status(select_stmt, LIBSQL_STMTSTATUS_ROWS_READ, 0);
            assert!(
                rows_read >= 3,
                "expected at least 3 rows read, got {rows_read}"
            );
            let fullscan_steps =
                sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
            assert!(
                fullscan_steps >= 2,
                "expected fullscan steps for table iteration, got {fullscan_steps}"
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, LIBSQL_STMTSTATUS_ROWS_READ, 1),
                rows_read
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, LIBSQL_STMTSTATUS_ROWS_READ, 0),
                0
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_AUTOINDEX, 0),
                0
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_RUN, 0),
                0
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_FILTER_HIT, 0),
                0
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_FILTER_MISS, 0),
                0
            );
            assert_eq!(
                sqlite3_stmt_status(select_stmt, SQLITE_STMTSTATUS_MEMUSED, 0),
                0
            );
            assert_eq!(sqlite3_stmt_status(select_stmt, 9999, 0), 0);
            assert_eq!(sqlite3_finalize(select_stmt), SQLITE_OK);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// FFI-level transcription of the sqlite3_blob_open conditions exercised by
    /// SQLite's e_blobopen.test: misuse handling, the *ppBlob-is-NULL-on-error
    /// contract, database-name resolution, and error codes/messages.
    #[test]
    fn test_blob_open_argument_and_error_contract() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB, n); \
                      INSERT INTO t VALUES (1, x'00112233', 42);"
                        .as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            // NULL out-pointer is misuse.
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    0,
                    ptr::null_mut()
                ),
                SQLITE_MISUSE
            );

            // Every failure must leave *ppBlob NULL — callers close it unconditionally.
            let garbage = std::ptr::dangling_mut::<ffi::c_void>();

            let mut blob = garbage;
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    ptr::null(),
                    c"data".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_MISUSE
            );
            assert!(blob.is_null());

            // Only "main" resolves; an attached-style or filename-style name errors.
            let mut blob = garbage;
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"aux".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_ERROR
            );
            assert!(blob.is_null());

            // No such table / column / rowid: SQLITE_ERROR, NULL handle, errmsg set.
            let mut blob = garbage;
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"nosuch".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_ERROR
            );
            assert!(blob.is_null());
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
            assert!(msg.contains("no such table"), "errmsg: {msg}");

            let mut blob = garbage;
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    99,
                    0,
                    &mut blob
                ),
                SQLITE_ERROR
            );
            assert!(blob.is_null());
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
            assert!(msg.contains("no such rowid"), "errmsg: {msg}");

            // A non-TEXT/BLOB value cannot be opened (e_blobopen R-11683-62380).
            let mut blob = garbage;
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"n".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_ERROR
            );
            assert!(blob.is_null());
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
            assert!(
                msg.contains("cannot open value of type integer"),
                "errmsg: {msg}"
            );

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// e_blobopen R-50854-53979 / R-03922-41160: flags==0 means read-only
    /// (writes fail with SQLITE_READONLY); any non-zero flags value means
    /// read-write. Also covers R-34146-30782: indexed columns refuse writing.
    #[test]
    fn test_blob_open_flags_and_indexed_columns() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB, ix BLOB); \
                      CREATE INDEX t_ix ON t(ix); \
                      INSERT INTO t VALUES (1, x'0011223344556677', x'aa');"
                        .as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );

            // Read-only handle: reads work, writes are SQLITE_READONLY.
            let mut blob = ptr::null_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_OK
            );
            let mut buf = [0u8; 4];
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
                SQLITE_OK
            );
            assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);
            assert_eq!(
                sqlite3_blob_write(blob, buf.as_ptr().cast(), 4, 0),
                SQLITE_READONLY
            );
            assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);

            // Any non-zero flags value opens read-write.
            for flags in [1, -1, ffi::c_int::MAX, ffi::c_int::MIN] {
                let mut blob = ptr::null_mut();
                assert_eq!(
                    sqlite3_blob_open(
                        db,
                        c"main".as_ptr(),
                        c"t".as_ptr(),
                        c"data".as_ptr(),
                        1,
                        flags,
                        &mut blob
                    ),
                    SQLITE_OK
                );
                let payload = [0xABu8, 0xCD];
                assert_eq!(
                    sqlite3_blob_write(blob, payload.as_ptr().cast(), 2, 6),
                    SQLITE_OK
                );
                assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);
            }

            // Indexed column: read-only open works, read-write open fails.
            let mut blob = ptr::null_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"ix".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);
            let mut blob = ptr::null_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"ix".as_ptr(),
                    1,
                    1,
                    &mut blob
                ),
                SQLITE_ERROR
            );
            assert!(blob.is_null());
            let msg = CStr::from_ptr(sqlite3_errmsg(db)).to_str().unwrap();
            assert!(msg.contains("indexed column"), "errmsg: {msg}");

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// FFI transcription of e_blobwrite/e_blobread range conditions: negative N or
    /// offset and out-of-range spans are SQLITE_ERROR (not misuse), every such error
    /// leaves the handle usable, and zero-length operations never dereference the
    /// data pointer (NULL is legal for them) while still range-checking the offset.
    #[test]
    fn test_blob_read_write_range_contract() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB); \
                      INSERT INTO t VALUES (1, zeroblob(8));"
                        .as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let mut blob = ptr::null_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    1,
                    &mut blob
                ),
                SQLITE_OK
            );
            assert_eq!(sqlite3_blob_bytes(blob), 8);

            let mut buf = [0u8; 8];
            // Out-of-range spans and negative arguments: SQLITE_ERROR.
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 5),
                SQLITE_ERROR
            );
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), -1, 0),
                SQLITE_ERROR
            );
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, -1),
                SQLITE_ERROR
            );
            assert_eq!(
                sqlite3_blob_write(blob, buf.as_ptr().cast(), 4, 5),
                SQLITE_ERROR
            );
            assert_eq!(
                sqlite3_blob_write(blob, buf.as_ptr().cast(), -1, 0),
                SQLITE_ERROR
            );
            assert_eq!(
                sqlite3_blob_write(blob, buf.as_ptr().cast(), 4, -1),
                SQLITE_ERROR
            );

            // Zero-length operations with a NULL data pointer are legal and still
            // range-check the offset.
            assert_eq!(sqlite3_blob_read(blob, ptr::null_mut(), 0, 8), SQLITE_OK);
            assert_eq!(sqlite3_blob_read(blob, ptr::null_mut(), 0, 9), SQLITE_ERROR);
            assert_eq!(sqlite3_blob_write(blob, ptr::null(), 0, 8), SQLITE_OK);
            assert_eq!(sqlite3_blob_write(blob, ptr::null(), 0, 9), SQLITE_ERROR);
            // A non-zero length with a NULL pointer is misuse.
            assert_eq!(
                sqlite3_blob_read(blob, ptr::null_mut(), 4, 0),
                SQLITE_MISUSE
            );
            assert_eq!(sqlite3_blob_write(blob, ptr::null(), 4, 0), SQLITE_MISUSE);

            // All of the above left the handle usable.
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 8, 0),
                SQLITE_OK
            );
            assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);

            // NULL handles: bytes reports 0, close is a no-op, I/O is misuse.
            assert_eq!(sqlite3_blob_bytes(ptr::null_mut()), 0);
            assert_eq!(sqlite3_blob_close(ptr::null_mut()), SQLITE_OK);
            assert_eq!(
                sqlite3_blob_read(ptr::null_mut(), buf.as_mut_ptr().cast(), 1, 0),
                SQLITE_MISUSE
            );
            assert_eq!(
                sqlite3_blob_write(ptr::null_mut(), buf.as_ptr().cast(), 1, 0),
                SQLITE_MISUSE
            );

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }

    /// e_blobopen R-50542-62589 at the FFI: writing the handle's row expires it and
    /// subsequent operations return SQLITE_ABORT; writes to other rows do not.
    #[test]
    fn test_blob_expiry_returns_abort() {
        unsafe {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"CREATE TABLE t(id INTEGER PRIMARY KEY, data BLOB); \
                      INSERT INTO t VALUES (1, x'00112233'); \
                      INSERT INTO t VALUES (2, x'ffeeddcc');"
                        .as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            let mut blob = ptr::null_mut();
            assert_eq!(
                sqlite3_blob_open(
                    db,
                    c"main".as_ptr(),
                    c"t".as_ptr(),
                    c"data".as_ptr(),
                    1,
                    0,
                    &mut blob
                ),
                SQLITE_OK
            );
            let mut buf = [0u8; 4];
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
                SQLITE_OK
            );

            // A write to a DIFFERENT row leaves the handle usable.
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"UPDATE t SET data = x'00000000' WHERE id = 2".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
                SQLITE_OK
            );
            assert_eq!(buf, [0x00, 0x11, 0x22, 0x33]);

            // A write to the handle's own row expires it: SQLITE_ABORT, sticky.
            assert_eq!(
                sqlite3_exec(
                    db,
                    c"UPDATE t SET data = x'99999999' WHERE id = 1".as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
                SQLITE_OK
            );
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
                SQLITE_ABORT
            );
            assert_eq!(
                sqlite3_blob_read(blob, buf.as_mut_ptr().cast(), 4, 0),
                SQLITE_ABORT
            );
            assert_eq!(sqlite3_blob_close(blob), SQLITE_OK);

            assert_eq!(sqlite3_close(db), SQLITE_OK);
        }
    }
}
