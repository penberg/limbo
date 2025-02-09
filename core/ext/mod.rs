use crate::{function::ExternalFunc, util::columns_from_create_table_body, Database, VirtualTable};
use fallible_iterator::FallibleIterator;
use limbo_ext::{ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabModuleImpl};
pub use limbo_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
use sqlite3_parser::{
    ast::{Cmd, Stmt},
    lexer::sql::Parser,
};
use std::{
    ffi::{c_char, c_void, CStr},
    rc::Rc,
};
type ExternAggFunc = (InitAggFunction, StepFunction, FinalizeFunction);

unsafe extern "C" fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_scalar_function_impl(&name_str, func)
}

unsafe extern "C" fn register_aggregate_function(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init_func: InitAggFunction,
    step_func: StepFunction,
    finalize_func: FinalizeFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_aggregate_function_impl(&name_str, args, (init_func, step_func, finalize_func))
}

unsafe extern "C" fn register_module(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &mut *(ctx as *mut Database) };

    db.register_module_impl(&name_str, module)
}

unsafe extern "C" fn declare_vtab(
    ctx: *mut c_void,
    name: *const c_char,
    sql: *const c_char,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };

    let c_str = unsafe { CStr::from_ptr(sql) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };

    if ctx.is_null() {
        return ResultCode::Error;
    }
    let db = unsafe { &mut *(ctx as *mut Database) };
    db.declare_vtab_impl(&name_str, &sql_str)
}

impl Database {
    fn register_scalar_function_impl(&self, name: &str, func: ScalarFunction) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_scalar(name.to_string(), func)),
        );
        ResultCode::OK
    }

    fn register_aggregate_function_impl(
        &self,
        name: &str,
        args: i32,
        func: ExternAggFunc,
    ) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc::new_aggregate(name.to_string(), args, func)),
        );
        ResultCode::OK
    }

    fn register_module_impl(&mut self, name: &str, module: VTabModuleImpl) -> ResultCode {
        self.vtab_modules.insert(name.to_string(), Rc::new(module));
        ResultCode::OK
    }

    fn declare_vtab_impl(&mut self, name: &str, sql: &str) -> ResultCode {
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser.next().unwrap().unwrap();
        let Cmd::Stmt(stmt) = cmd else {
            return ResultCode::Error;
        };
        let Stmt::CreateTable { body, .. } = stmt else {
            return ResultCode::Error;
        };
        let Ok(columns) = columns_from_create_table_body(*body) else {
            return ResultCode::Error;
        };
        let vtab_module = self.vtab_modules.get(name).unwrap().clone();

        let vtab = VirtualTable {
            name: name.to_string(),
            implementation: vtab_module,
            columns,
            args: None,
        };
        self.syms.borrow_mut().vtabs.insert(name.to_string(), vtab);
        ResultCode::OK
    }

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_module,
            declare_vtab,
        }
    }

    pub fn register_builtins(&self) -> Result<(), String> {
        #[allow(unused_variables)]
        let ext_api = self.build_limbo_ext();
        #[cfg(feature = "uuid")]
        if unsafe { !limbo_uuid::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register uuid extension".to_string());
        }
        #[cfg(feature = "percentile")]
        if unsafe { !limbo_percentile::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register percentile extension".to_string());
        }
        #[cfg(feature = "regexp")]
        if unsafe { !limbo_regexp::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register regexp extension".to_string());
        }
        #[cfg(feature = "time")]
        if unsafe { !limbo_time::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register time extension".to_string());
        }
        #[cfg(feature = "crypto")]
        if unsafe { !limbo_crypto::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register crypto extension".to_string());
        }
        #[cfg(feature = "series")]
        if unsafe { !limbo_series::register_extension_static(&ext_api).is_ok() } {
            return Err("Failed to register series extension".to_string());
        }
        Ok(())
    }
}
