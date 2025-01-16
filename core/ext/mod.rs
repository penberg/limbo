use crate::{function::ExternalFunc, Database};
use limbo_ext::{ExtensionApi, ResultCode, ScalarFunction, RESULT_ERROR, RESULT_OK};
pub use limbo_ext::{Value as ExtValue, ValueType as ExtValueType};
use std::{
    ffi::{c_char, c_void, CStr},
    rc::Rc,
};

extern "C" fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return RESULT_ERROR,
    };
    if ctx.is_null() {
        return RESULT_ERROR;
    }
    let db = unsafe { &*(ctx as *const Database) };
    db.register_scalar_function_impl(name_str, func)
}

impl Database {
    fn register_scalar_function_impl(&self, name: String, func: ScalarFunction) -> ResultCode {
        self.syms.borrow_mut().functions.insert(
            name.to_string(),
            Rc::new(ExternalFunc {
                name: name.to_string(),
                func,
            }),
        );
        RESULT_OK
    }

    pub fn build_limbo_ext(&self) -> ExtensionApi {
        ExtensionApi {
            ctx: self as *const _ as *mut c_void,
            register_scalar_function,
        }
    }
}
