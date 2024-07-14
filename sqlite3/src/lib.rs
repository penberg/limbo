#![allow(clippy::missing_safety_doc)]
#![allow(non_camel_case_types)]

use std::cell::RefCell;
use std::ffi;
use std::rc::Rc;

pub const SQLITE_OK: ffi::c_int = 0;
pub const SQLITE_ERROR: ffi::c_int = 1;
pub const SQLITE_BUSY: ffi::c_int = 5;
pub const SQLITE_NOTFOUND: ffi::c_int = 14;
pub const SQLITE_MISUSE: ffi::c_int = 21;
pub const SQLITE_ROW: ffi::c_int = 100;
pub const SQLITE_DONE: ffi::c_int = 101;

pub struct sqlite3 {
    pub(crate) _db: limbo_core::Database,
    pub(crate) conn: limbo_core::Connection,
}

impl sqlite3 {
    pub fn new(db: limbo_core::Database, conn: limbo_core::Connection) -> Self {
        Self { _db: db, conn }
    }
}

pub struct sqlite3_stmt<'a> {
    pub(crate) stmt: limbo_core::Statement,
    pub(crate) row: RefCell<Option<limbo_core::Row<'a>>>,
}

impl<'a> sqlite3_stmt<'a> {
    pub fn new(stmt: limbo_core::Statement) -> Self {
        let row = RefCell::new(None);
        Self { stmt, row }
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_initialize() -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_shutdown() -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_open(
    filename: *const ffi::c_char,
    db_out: *mut *mut sqlite3,
) -> ffi::c_int {
    if filename.is_null() {
        return SQLITE_MISUSE;
    }
    if db_out.is_null() {
        return SQLITE_MISUSE;
    }
    let filename = ffi::CStr::from_ptr(filename);
    let filename = match filename.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let io = match limbo_core::PlatformIO::new() {
        Ok(io) => Rc::new(io),
        Err(_) => return SQLITE_MISUSE,
    };
    match limbo_core::Database::open_file(io, filename) {
        Ok(db) => {
            let conn = db.connect();
            *db_out = Box::leak(Box::new(sqlite3::new(db, conn)));
            SQLITE_OK
        }
        Err(_e) => SQLITE_NOTFOUND,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_open_v2(
    filename: *const ffi::c_char,
    db_out: *mut *mut sqlite3,
    _flags: ffi::c_int,
    _z_vfs: *const ffi::c_char,
) -> ffi::c_int {
    sqlite3_open(filename, db_out)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_OK;
    }
    let _ = Box::from_raw(db);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_close_v2(db: *mut sqlite3) -> ffi::c_int {
    sqlite3_close(db)
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_trace_v2(
    _db: *mut sqlite3,
    _mask: ffi::c_uint,
    _callback: Option<
        unsafe extern "C" fn(
            ffi::c_uint,
            *mut std::ffi::c_void,
            *mut std::ffi::c_void,
            *mut std::ffi::c_void,
        ),
    >,
    _context: *mut std::ffi::c_void,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_progress_handler(
    _db: *mut sqlite3,
    _n: ffi::c_int,
    _callback: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _context: *mut std::ffi::c_void,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_busy_timeout(_db: *mut sqlite3, _ms: ffi::c_int) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_set_authorizer(
    _db: *mut sqlite3,
    _callback: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _context: *mut std::ffi::c_void,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_context_db_handle(
    _context: *mut std::ffi::c_void,
) -> *mut std::ffi::c_void {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_prepare_v2(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    _len: ffi::c_int,
    out_stmt: *mut *mut sqlite3_stmt,
    _tail: *mut *const ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() || out_stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let sql = ffi::CStr::from_ptr(sql);
    let sql = match sql.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let stmt = match db.conn.prepare(sql) {
        Ok(stmt) => stmt,
        Err(_) => return SQLITE_ERROR,
    };
    *out_stmt = Box::leak(Box::new(sqlite3_stmt::new(stmt)));
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let _ = Box::from_raw(stmt);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> std::ffi::c_int {
    let stmt = &mut *stmt;
    if let Ok(result) = stmt.stmt.step() {
        match result {
            limbo_core::RowResult::IO => SQLITE_BUSY,
            limbo_core::RowResult::Done => SQLITE_DONE,
            limbo_core::RowResult::Row(row) => {
                stmt.row.replace(Some(row));
                SQLITE_ROW
            }
        }
    } else {
        SQLITE_ERROR
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_exec(
    db: *mut sqlite3,
    sql: *const ffi::c_char,
    _callback: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _context: *mut std::ffi::c_void,
    _err: *mut *mut std::ffi::c_char,
) -> ffi::c_int {
    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }
    let db: &mut sqlite3 = &mut *db;
    let sql = ffi::CStr::from_ptr(sql);
    let sql = match sql.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    match db.conn.execute(sql) {
        Ok(_) => SQLITE_OK,
        Err(_) => SQLITE_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &mut *stmt;
    stmt.row.replace(None);
    SQLITE_OK
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_changes(_db: *mut sqlite3) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_readonly(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stmt_busy(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_serialize(
    _db: *mut sqlite3,
    _schema: *const std::ffi::c_char,
    _out: *mut *mut std::ffi::c_void,
    _out_bytes: *mut ffi::c_int,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_deserialize(
    _db: *mut sqlite3,
    _schema: *const std::ffi::c_char,
    _in_: *const std::ffi::c_void,
    _in_bytes: ffi::c_int,
    _flags: ffi::c_uint,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_get_autocommit(_db: *mut sqlite3) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_total_changes(_db: *mut sqlite3) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_last_insert_rowid(_db: *mut sqlite3) -> i64 {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_interrupt(_db: *mut sqlite3) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_config(_db: *mut sqlite3, _op: ffi::c_int) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_db_handle(_stmt: *mut sqlite3_stmt) -> *mut sqlite3 {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_sleep(_ms: ffi::c_int) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_limit(
    _db: *mut sqlite3,
    _id: ffi::c_int,
    _new_value: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_malloc64(_n: ffi::c_int) -> *mut std::ffi::c_void {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_free(_ptr: *mut std::ffi::c_void) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errcode(_db: *mut sqlite3) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errstr(_err: ffi::c_int) -> *const std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_user_data(
    _context: *mut std::ffi::c_void,
) -> *mut std::ffi::c_void {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_init(
    _dest_db: *mut sqlite3,
    _dest_name: *const std::ffi::c_char,
    _source_db: *mut sqlite3,
    _source_name: *const std::ffi::c_char,
) -> *mut std::ffi::c_void {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_step(
    _backup: *mut std::ffi::c_void,
    _n_pages: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_remaining(_backup: *mut std::ffi::c_void) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_pagecount(_backup: *mut std::ffi::c_void) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_backup_finish(_backup: *mut std::ffi::c_void) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_expanded_sql(_stmt: *mut sqlite3_stmt) -> *mut std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_data_count(stmt: *mut sqlite3_stmt) -> ffi::c_int {
    let stmt = &*stmt;
    let row = stmt.row.borrow();
    let row = match row.as_ref() {
        Some(row) => row,
        None => return 0,
    };
    row.values.len() as ffi::c_int
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_count(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_parameter_name(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_null(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_int64(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _val: i64,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_double(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _val: f64,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_text(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _text: *const std::ffi::c_char,
    _len: ffi::c_int,
    _destroy: *mut std::ffi::c_void,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_bind_blob(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
    _blob: *const std::ffi::c_void,
    _len: ffi::c_int,
    _destroy: *mut std::ffi::c_void,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_type(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_count(_stmt: *mut sqlite3_stmt) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_decltype(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_name(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_int64(_stmt: *mut sqlite3_stmt, _idx: ffi::c_int) -> i64 {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_double(_stmt: *mut sqlite3_stmt, _idx: ffi::c_int) -> f64 {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_blob(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> *const std::ffi::c_void {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_bytes(
    _stmt: *mut sqlite3_stmt,
    _idx: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_type(value: *mut std::ffi::c_void) -> ffi::c_int {
    let value = value as *mut limbo_core::Value;
    let value = &*value;
    match value {
        limbo_core::Value::Null => 0,
        limbo_core::Value::Integer(_) => 1,
        limbo_core::Value::Float(_) => 2,
        limbo_core::Value::Text(_) => 3,
        limbo_core::Value::Blob(_) => 4,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_int64(value: *mut std::ffi::c_void) -> i64 {
    let value = value as *mut limbo_core::Value;
    let value = &*value;
    match value {
        limbo_core::Value::Integer(i) => *i,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_double(value: *mut std::ffi::c_void) -> f64 {
    let value = value as *mut limbo_core::Value;
    let value = &*value;
    match value {
        limbo_core::Value::Float(f) => *f,
        _ => 0.0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_text(
    value: *mut std::ffi::c_void,
) -> *const std::ffi::c_uchar {
    let value = value as *mut limbo_core::Value;
    let value = &*value;
    match value {
        limbo_core::Value::Text(text) => text.as_bytes().as_ptr(),
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_blob(
    value: *mut std::ffi::c_void,
) -> *const std::ffi::c_void {
    let value = value as *mut limbo_core::Value;
    let value = &*value;
    match value {
        limbo_core::Value::Blob(blob) => blob.as_ptr() as *const std::ffi::c_void,
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_value_bytes(value: *mut std::ffi::c_void) -> ffi::c_int {
    let value = value as *mut limbo_core::Value;
    let value = &*value;
    match value {
        limbo_core::Value::Blob(blob) => blob.len() as ffi::c_int,
        _ => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_column_text(
    stmt: *mut sqlite3_stmt,
    idx: std::ffi::c_int,
) -> *const std::ffi::c_uchar {
    let stmt = &mut *stmt;
    let row = stmt.row.borrow();
    let row = match row.as_ref() {
        Some(row) => row,
        None => return std::ptr::null(),
    };
    match row.values.get(idx as usize) {
        Some(limbo_core::Value::Text(text)) => text.as_bytes().as_ptr(),
        _ => std::ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_null(_context: *mut std::ffi::c_void) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_int64(_context: *mut std::ffi::c_void, _val: i64) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_double(_context: *mut std::ffi::c_void, _val: f64) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_text(
    _context: *mut std::ffi::c_void,
    _text: *const std::ffi::c_char,
    _len: ffi::c_int,
    _destroy: *mut std::ffi::c_void,
) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_blob(
    _context: *mut std::ffi::c_void,
    _blob: *const std::ffi::c_void,
    _len: ffi::c_int,
    _destroy: *mut std::ffi::c_void,
) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_nomem(_context: *mut std::ffi::c_void) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error_toobig(_context: *mut std::ffi::c_void) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_result_error(
    _context: *mut std::ffi::c_void,
    _err: *const std::ffi::c_char,
    _len: ffi::c_int,
) {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_aggregate_context(
    _context: *mut std::ffi::c_void,
    _n: ffi::c_int,
) -> *mut std::ffi::c_void {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_open(
    _db: *mut sqlite3,
    _db_name: *const std::ffi::c_char,
    _table_name: *const std::ffi::c_char,
    _column_name: *const std::ffi::c_char,
    _rowid: i64,
    _flags: ffi::c_int,
    _blob_out: *mut *mut std::ffi::c_void,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_read(
    _blob: *mut std::ffi::c_void,
    _data: *mut std::ffi::c_void,
    _n: ffi::c_int,
    _offset: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_write(
    _blob: *mut std::ffi::c_void,
    _data: *const std::ffi::c_void,
    _n: ffi::c_int,
    _offset: ffi::c_int,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_bytes(_blob: *mut std::ffi::c_void) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_blob_close(_blob: *mut std::ffi::c_void) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_stricmp(
    _a: *const std::ffi::c_char,
    _b: *const std::ffi::c_char,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_collation_v2(
    _db: *mut sqlite3,
    _name: *const std::ffi::c_char,
    _enc: ffi::c_int,
    _context: *mut std::ffi::c_void,
    _cmp: Option<unsafe extern "C" fn() -> ffi::c_int>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_function_v2(
    _db: *mut sqlite3,
    _name: *const std::ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    _context: *mut std::ffi::c_void,
    _func: Option<unsafe extern "C" fn()>,
    _step: Option<unsafe extern "C" fn()>,
    _final_: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_create_window_function(
    _db: *mut sqlite3,
    _name: *const std::ffi::c_char,
    _n_args: ffi::c_int,
    _enc: ffi::c_int,
    _context: *mut std::ffi::c_void,
    _x_step: Option<unsafe extern "C" fn()>,
    _x_final: Option<unsafe extern "C" fn()>,
    _x_value: Option<unsafe extern "C" fn()>,
    _x_inverse: Option<unsafe extern "C" fn()>,
    _destroy: Option<unsafe extern "C" fn()>,
) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_errmsg(_db: *mut sqlite3) -> *const std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_extended_errcode(_db: *mut sqlite3) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_complete(_sql: *const std::ffi::c_char) -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_threadsafe() -> ffi::c_int {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion() -> *const std::ffi::c_char {
    todo!();
}

#[no_mangle]
pub unsafe extern "C" fn sqlite3_libversion_number() -> ffi::c_int {
    todo!();
}
