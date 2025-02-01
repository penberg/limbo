use crate::rows::LimboRows;
use crate::types::{AllocPool, LimboValue, ResultCode};
use crate::LimboConn;
use limbo_core::{LimboError, Statement, StepResult};
use std::ffi::{c_char, c_void};
use std::num::NonZero;

#[no_mangle]
pub extern "C" fn db_prepare(ctx: *mut c_void, query: *const c_char) -> *mut c_void {
    if ctx.is_null() || query.is_null() {
        return std::ptr::null_mut();
    }
    let query_str = unsafe { std::ffi::CStr::from_ptr(query) }.to_str().unwrap();

    let db = LimboConn::from_ptr(ctx);
    let stmt = db.conn.prepare(query_str);
    match stmt {
        Ok(stmt) => LimboStatement::new(Some(stmt), db).to_ptr(),
        Err(err) => {
            db.err = Some(err);
            std::ptr::null_mut()
        }
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
    let mut pool = AllocPool::new();
    let Some(statement) = stmt.statement.as_mut() else {
        return ResultCode::Error;
    };
    for (i, arg) in args.iter().enumerate() {
        let val = arg.to_value(&mut pool);
        statement.bind_at(NonZero::new(i + 1).unwrap(), val);
    }
    loop {
        match statement.step() {
            Ok(StepResult::Row(_)) => {
                // unexpected row during execution, error out.
                return ResultCode::Error;
            }
            Ok(StepResult::Done) => {
                let total_changes = stmt.conn.conn.total_changes();
                if !changes.is_null() {
                    unsafe {
                        *changes = total_changes;
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
            Err(err) => {
                stmt.conn.err = Some(err);
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
    let Some(statement) = stmt.statement.as_ref() else {
        stmt.err = Some(LimboError::InternalError("Statement is closed".to_string()));
        return -1;
    };
    statement.parameters_count() as i32
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
    let mut pool = AllocPool::new();
    let Some(mut statement) = stmt.statement.take() else {
        return std::ptr::null_mut();
    };
    for (i, arg) in args.iter().enumerate() {
        let val = arg.to_value(&mut pool);
        statement.bind_at(NonZero::new(i + 1).unwrap(), val);
    }
    // ownership of the statement is transfered to the LimboRows object.
    LimboRows::new(statement, stmt.conn).to_ptr()
}

pub struct LimboStatement<'conn> {
    /// If 'query' is ran on the statement, ownership is transfered to the LimboRows object
    pub statement: Option<Statement>,
    pub conn: &'conn mut LimboConn,
    pub err: Option<LimboError>,
}

#[no_mangle]
pub extern "C" fn stmt_close(ctx: *mut c_void) -> ResultCode {
    if !ctx.is_null() {
        let stmt = unsafe { Box::from_raw(ctx as *mut LimboStatement) };
        drop(stmt);
        return ResultCode::Ok;
    }
    ResultCode::Invalid
}

#[no_mangle]
pub extern "C" fn stmt_get_error(ctx: *mut c_void) -> *const c_char {
    if ctx.is_null() {
        return std::ptr::null();
    }
    let stmt = LimboStatement::from_ptr(ctx);
    stmt.get_error()
}

impl<'conn> LimboStatement<'conn> {
    pub fn new(statement: Option<Statement>, conn: &'conn mut LimboConn) -> Self {
        LimboStatement {
            statement,
            conn,
            err: None,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_ptr(self) -> *mut c_void {
        Box::into_raw(Box::new(self)) as *mut c_void
    }

    fn from_ptr(ptr: *mut c_void) -> &'conn mut LimboStatement<'conn> {
        if ptr.is_null() {
            panic!("Null pointer");
        }
        unsafe { &mut *(ptr as *mut LimboStatement) }
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
