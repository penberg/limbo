use crate::{
    statement::LimboStatement,
    types::{LimboValue, ResultCode},
};
use limbo_core::{Statement, StepResult, Value};
use std::ffi::{c_char, c_void};

pub struct LimboRows<'a> {
    rows: Statement,
    cursor: Option<Vec<Value<'a>>>,
    stmt: Box<LimboStatement<'a>>,
}

impl<'a> LimboRows<'a> {
    pub fn new(rows: Statement, stmt: Box<LimboStatement<'a>>) -> Self {
        LimboRows {
            rows,
            stmt,
            cursor: None,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    pub fn from_ptr(ptr: *mut c_void) -> &'static mut LimboRows<'a> {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut LimboRows) }
    }
}

#[no_mangle]
pub extern "C" fn rows_next(ctx: *mut c_void) -> ResultCode {
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let ctx = LimboRows::from_ptr(ctx);

    match ctx.rows.step() {
        Ok(StepResult::Row(row)) => {
            ctx.cursor = Some(row.values);
            ResultCode::Row
        }
        Ok(StepResult::Done) => ResultCode::Done,
        Ok(StepResult::IO) => {
            let _ = ctx.stmt.conn.io.run_once();
            ResultCode::Io
        }
        Ok(StepResult::Busy) => ResultCode::Busy,
        Ok(StepResult::Interrupt) => ResultCode::Interrupt,
        Err(_) => ResultCode::Error,
    }
}

#[no_mangle]
pub extern "C" fn rows_get_value(ctx: *mut c_void, col_idx: usize) -> *const c_void {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let ctx = LimboRows::from_ptr(ctx);

    if let Some(ref cursor) = ctx.cursor {
        if let Some(value) = cursor.get(col_idx) {
            let val = LimboValue::from_value(value);
            return val.to_ptr();
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

#[no_mangle]
pub extern "C" fn rows_get_columns(
    rows_ptr: *mut c_void,
    out_length: *mut usize,
) -> *mut *const c_char {
    if rows_ptr.is_null() || out_length.is_null() {
        return std::ptr::null_mut();
    }
    let rows = LimboRows::from_ptr(rows_ptr);
    let c_strings: Vec<std::ffi::CString> = rows
        .rows
        .columns()
        .iter()
        .map(|name| std::ffi::CString::new(name.as_str()).unwrap())
        .collect();

    let c_ptrs: Vec<*const c_char> = c_strings.iter().map(|s| s.as_ptr()).collect();
    unsafe {
        *out_length = c_ptrs.len();
    }
    let ptr = c_ptrs.as_ptr();
    std::mem::forget(c_strings);
    std::mem::forget(c_ptrs);
    ptr as *mut *const c_char
}

#[no_mangle]
pub extern "C" fn rows_close(rows_ptr: *mut c_void) {
    if !rows_ptr.is_null() {
        let _ = unsafe { Box::from_raw(rows_ptr as *mut LimboRows) };
    }
}

#[no_mangle]
pub extern "C" fn free_columns(columns: *mut *const c_char) {
    if columns.is_null() {
        return;
    }
    unsafe {
        let mut idx = 0;
        while !(*columns.add(idx)).is_null() {
            let _ = std::ffi::CString::from_raw(*columns.add(idx) as *mut c_char);
            idx += 1;
        }
        let _ = Box::from_raw(columns);
    }
}

#[no_mangle]
pub extern "C" fn free_rows(rows: *mut c_void) {
    if rows.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(rows as *mut Statement);
    }
}
