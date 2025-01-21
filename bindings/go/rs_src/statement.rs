use crate::types::ResultCode;
use crate::TursoConn;
use limbo_core::{Rows, Statement, StepResult, Value};
use std::ffi::{c_char, c_void};

#[no_mangle]
pub extern "C" fn db_prepare(ctx: *mut c_void, query: *const c_char) -> *mut c_void {
    if ctx.is_null() || query.is_null() {
        return std::ptr::null_mut();
    }
    let query_str = unsafe { std::ffi::CStr::from_ptr(query) }.to_str().unwrap();

    let db = TursoConn::from_ptr(ctx);

    let stmt = db.conn.prepare(query_str.to_string());
    match stmt {
        Ok(stmt) => TursoStatement::new(stmt, db).to_ptr(),
        Err(_) => std::ptr::null_mut(),
    }
}

struct TursoStatement<'a> {
    statement: Statement,
    conn: &'a TursoConn<'a>,
}

impl<'a> TursoStatement<'a> {
    fn new(statement: Statement, conn: &'a TursoConn<'a>) -> Self {
        TursoStatement { statement, conn }
    }
    fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }
    fn from_ptr(ptr: *mut c_void) -> &'static mut TursoStatement<'a> {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut TursoStatement) }
    }
}

#[no_mangle]
pub extern "C" fn db_get_columns(ctx: *mut c_void) -> *const c_void {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let stmt = TursoStatement::from_ptr(ctx);
    let columns = stmt.statement.columns();
    let mut column_names = Vec::new();
    for column in columns {
        column_names.push(column.name().to_string());
    }
    let c_string = std::ffi::CString::new(column_names.join(",")).unwrap();
    c_string.into_raw() as *const c_void
}

struct TursoRows<'a> {
    rows: Rows<'a>,
    conn: &'a mut TursoConn<'a>,
}

impl<'a> TursoRows<'a> {
    fn new(rows: Rows<'a>, conn: &'a mut TursoConn<'a>) -> Self {
        TursoRows { rows, conn }
    }

    fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    fn from_ptr(ptr: *mut c_void) -> &'static mut TursoRows<'a> {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut TursoRows) }
    }
}

#[no_mangle]
pub extern "C" fn rows_next(ctx: *mut c_void, rows_ptr: *mut c_void) -> ResultCode {
    if rows_ptr.is_null() || ctx.is_null() {
        return ResultCode::Error;
    }
    let rows = unsafe { &mut *(rows_ptr as *mut Rows) };
    let conn = TursoConn::from_ptr(ctx);

    match rows.next_row() {
        Ok(StepResult::Row(row)) => {
            conn.cursor = Some(row.values);
            ResultCode::Row
        }
        Ok(StepResult::Done) => {
            // No more rows
            ResultCode::Done
        }
        Ok(StepResult::IO) => {
            let _ = conn.io.run_once();
            ResultCode::Io
        }
        Ok(StepResult::Busy) => ResultCode::Busy,
        Ok(StepResult::Interrupt) => ResultCode::Interrupt,
        Err(_) => ResultCode::Error,
    }
}

#[no_mangle]
pub extern "C" fn rows_get_value(ctx: *mut c_void, col_idx: usize) -> *const c_char {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let conn = TursoConn::from_ptr(ctx);

    if let Some(ref cursor) = conn.cursor {
        if let Some(value) = cursor.get(col_idx) {
            let c_string = std::ffi::CString::new(value.to_string()).unwrap();
            return c_string.into_raw(); // Caller must free this pointer
        }
    }
    std::ptr::null() // No data or invalid index
}

// Free the returned string
#[no_mangle]
pub extern "C" fn free_c_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(std::ffi::CString::from_raw(s)) };
    }
}
#[no_mangle]
pub extern "C" fn rows_get_string(
    ctx: *mut c_void,
    rows_ptr: *mut c_void,
    col_idx: i32,
) -> *const c_char {
    if rows_ptr.is_null() || ctx.is_null() {
        return std::ptr::null();
    }
    let _rows = unsafe { &mut *(rows_ptr as *mut Rows) };
    let conn = TursoConn::from_ptr(ctx);
    if col_idx > conn.cursor_idx as i32 || conn.cursor.is_none() {
        return std::ptr::null();
    }
    if let Some(values) = &conn.cursor {
        let value = &values[col_idx as usize];
        match value {
            Value::Text(s) => {
                return s.as_ptr() as *const i8;
            }
            _ => return std::ptr::null(),
        }
    };
    std::ptr::null()
}

#[no_mangle]
pub extern "C" fn rows_close(rows_ptr: *mut c_void) {
    if !rows_ptr.is_null() {
        let _ = unsafe { Box::from_raw(rows_ptr as *mut Rows) };
    }
}
