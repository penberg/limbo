use crate::rows::LimboRows;
use crate::types::{AllocPool, LimboValue, ResultCode};
use crate::LimboConn;
use limbo_core::{Statement, StepResult};
use std::ffi::{c_char, c_void};
use std::num::NonZero;

#[no_mangle]
pub extern "C" fn db_prepare(ctx: *mut c_void, query: *const c_char) -> *mut c_void {
    if ctx.is_null() || query.is_null() {
        return std::ptr::null_mut();
    }
    let query_str = unsafe { std::ffi::CStr::from_ptr(query) }.to_str().unwrap();

    let db = LimboConn::from_ptr(ctx);

    let stmt = db.conn.prepare(query_str.to_string());
    match stmt {
        Ok(stmt) => LimboStatement::new(stmt, db).to_ptr(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn stmt_execute(
    ctx: *mut c_void,
    args_ptr: *mut LimboValue,
    arg_count: usize,
    changes: *mut i64,
) -> ResultCode {
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let stmt = LimboStatement::from_ptr(ctx);

    let args = if !args_ptr.is_null() && arg_count > 0 {
        unsafe { std::slice::from_raw_parts(args_ptr, arg_count) }
    } else {
        &[]
    };
    for (i, arg) in args.iter().enumerate() {
        let val = arg.to_value(&mut stmt.pool);
        stmt.statement.bind_at(NonZero::new(i + 1).unwrap(), val);
    }
    loop {
        match stmt.statement.step() {
            Ok(StepResult::Row(_)) => {
                // unexpected row during execution, error out.
                return ResultCode::Error;
            }
            Ok(StepResult::Done) => {
                stmt.conn.conn.total_changes();
                if !changes.is_null() {
                    unsafe {
                        *changes = stmt.conn.conn.total_changes();
                    }
                }
                return ResultCode::Done;
            }
            Ok(StepResult::IO) => {
                let _ = stmt.conn.io.run_once();
            }
            Ok(StepResult::Busy) => {
                return ResultCode::Busy;
            }
            Ok(StepResult::Interrupt) => {
                return ResultCode::Interrupt;
            }
            Err(_) => {
                return ResultCode::Error;
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn stmt_parameter_count(ctx: *mut c_void) -> i32 {
    if ctx.is_null() {
        return -1;
    }
    let stmt = LimboStatement::from_ptr(ctx);
    stmt.statement.parameters_count() as i32
}

#[no_mangle]
pub extern "C" fn stmt_query(
    ctx: *mut c_void,
    args_ptr: *mut LimboValue,
    args_count: usize,
) -> *mut c_void {
    if ctx.is_null() {
        return std::ptr::null_mut();
    }
    let stmt = LimboStatement::from_ptr(ctx);
    let args = if !args_ptr.is_null() && args_count > 0 {
        unsafe { std::slice::from_raw_parts(args_ptr, args_count) }
    } else {
        &[]
    };
    for (i, arg) in args.iter().enumerate() {
        let val = arg.to_value(&mut stmt.pool);
        stmt.statement.bind_at(NonZero::new(i + 1).unwrap(), val);
    }
    match stmt.statement.query() {
        Ok(rows) => {
            let stmt = unsafe { Box::from_raw(stmt) };
            LimboRows::new(rows, stmt).to_ptr()
        }
        Err(_) => std::ptr::null_mut(),
    }
}

pub struct LimboStatement<'conn> {
    pub statement: Statement,
    pub conn: &'conn mut LimboConn,
    pub pool: AllocPool,
}

impl<'conn> LimboStatement<'conn> {
    pub fn new(statement: Statement, conn: &'conn mut LimboConn) -> Self {
        LimboStatement {
            statement,
            conn,
            pool: AllocPool::new(),
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    fn from_ptr(ptr: *mut c_void) -> &'static mut LimboStatement<'conn> {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut LimboStatement) }
    }
}
