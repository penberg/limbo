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
    pub(crate) db: limbo_core::Database,
    pub(crate) conn: limbo_core::Connection,
}

impl sqlite3 {
    pub fn new(db: limbo_core::Database, conn: limbo_core::Connection) -> Self {
        Self { db, conn }
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
pub unsafe extern "C" fn sqlite3_close(db: *mut sqlite3) -> ffi::c_int {
    if db.is_null() {
        return SQLITE_OK;
    }
    let _ = Box::from_raw(db);
    SQLITE_OK
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
