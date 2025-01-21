use crate::{function::ExternalFunc, Database};
use limbo_ext::{ExtensionApi, InitAggFunction, ResultCode, ScalarFunction};
pub use limbo_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
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

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
        }
    }

    pub fn register_builtins(&self) -> Result<(), String> {
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
        Ok(())
    }
}
