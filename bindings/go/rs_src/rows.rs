use crate::{
    types::{LimboValue, ResultCode},
    LimboConn,
};
use limbo_core::{LimboError, Row, Statement, StepResult};
use std::ffi::{c_char, c_void};

pub struct LimboRows<'conn, 'a> {
    stmt: Box<Statement>,
    conn: &'conn mut LimboConn,
    cursor: Option<Row<'a>>,
    err: Option<LimboError>,
}

impl<'conn, 'a> LimboRows<'conn, 'a> {
    pub fn new(stmt: Statement, conn: &'conn mut LimboConn) -> Self {
        LimboRows {
            stmt: Box::new(stmt),
            cursor: None,
            conn,
            err: None,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    pub fn from_ptr(ptr: *mut c_void) -> &'conn mut LimboRows<'conn, 'a> {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut LimboRows) }
    }

    fn get_error(&mut self) -> *const c_char {
        if let Some(err) = &self.err {
            let err = format!("{}", err);
            let c_str = std::ffi::CString::new(err).unwrap();
            self.err = None;
            c_str.into_raw() as *const c_char
        } else {
            std::ptr::null()
        }
    }
}

#[no_mangle]
pub extern "C" fn rows_next(ctx: *mut c_void) -> ResultCode {
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let ctx = LimboRows::from_ptr(ctx);

    match ctx.stmt.step() {
        Ok(StepResult::Row(row)) => {
            ctx.cursor = Some(row);
            ResultCode::Row
        }
        Ok(StepResult::Done) => ResultCode::Done,
        Ok(StepResult::IO) => {
            let _ = ctx.conn.io.run_once();
            ResultCode::Io
        }
        Ok(StepResult::Busy) => ResultCode::Busy,
        Ok(StepResult::Interrupt) => ResultCode::Interrupt,
        Err(err) => {
            ctx.err = Some(err);
            ResultCode::Error
        }
    }
}

#[no_mangle]
pub extern "C" fn rows_get_value(ctx: *mut c_void, col_idx: usize) -> *const c_void {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let ctx = LimboRows::from_ptr(ctx);

    if let Some(ref cursor) = ctx.cursor {
        if let Some(value) = cursor.values.get(col_idx) {
            return LimboValue::from_value(value).to_ptr();
        }
    }
    std::ptr::null()
}

#[no_mangle]
pub extern "C" fn free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(std::ffi::CString::from_raw(s)) };
    }
}

/// Function to get the number of expected ResultColumns in the prepared statement.
/// to avoid the needless complexity of returning an array of strings, this instead
/// works like rows_next/rows_get_value
#[no_mangle]
pub extern "C" fn rows_get_columns(rows_ptr: *mut c_void) -> i32 {
    if rows_ptr.is_null() {
        return -1;
    }
    let rows = LimboRows::from_ptr(rows_ptr);
    rows.stmt.num_columns() as i32
}

/// Returns a pointer to a string with the name of the column at the given index.
/// The caller is responsible for freeing the memory, it should be copied on the Go side
/// immediately and 'free_string' called
#[no_mangle]
pub extern "C" fn rows_get_column_name(rows_ptr: *mut c_void, idx: i32) -> *const c_char {
    if rows_ptr.is_null() {
        return std::ptr::null_mut();
    }
    let rows = LimboRows::from_ptr(rows_ptr);
    if idx < 0 || idx as usize >= rows.stmt.num_columns() {
        return std::ptr::null_mut();
    }
    let name = rows.stmt.get_column_name(idx as usize);
    let cstr = std::ffi::CString::new(
        name.as_ref()
            .unwrap_or(&&format!("column_{}", idx))
            .as_bytes(),
    )
    .expect("Failed to create CString");
    cstr.into_raw() as *const c_char
}

#[no_mangle]
pub extern "C" fn rows_get_error(ctx: *mut c_void) -> *const c_char {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let ctx = LimboRows::from_ptr(ctx);
    ctx.get_error()
}

#[no_mangle]
pub extern "C" fn rows_close(ctx: *mut c_void) {
    if !ctx.is_null() {
        let rows = LimboRows::from_ptr(ctx);
        rows.stmt.reset();
        rows.cursor = None;
        rows.err = None;
    }
    unsafe {
        let _ = Box::from_raw(ctx.cast::<LimboRows>());
    }
}
