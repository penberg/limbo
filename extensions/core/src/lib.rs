mod types;
pub use limbo_macros::{register_extension, scalar, AggregateDerive};
use std::os::raw::{c_char, c_void};
pub use types::{ResultCode, Value, ValueType};

#[repr(C)]
pub struct ExtensionApi {
    pub ctx: *mut c_void,

    pub register_scalar_function: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        func: ScalarFunction,
    ) -> ResultCode,

    pub register_aggregate_function: unsafe extern "C" fn(
        ctx: *mut c_void,
        name: *const c_char,
        args: i32,
        init_func: InitAggFunction,
        step_func: StepFunction,
        finalize_func: FinalizeFunction,
    ) -> ResultCode,
}

pub type ExtensionEntryPoint = unsafe extern "C" fn(api: *const ExtensionApi) -> ResultCode;
pub type ScalarFunction = unsafe extern "C" fn(argc: i32, *const Value) -> Value;

pub type InitAggFunction = unsafe extern "C" fn() -> *mut AggCtx;
pub type StepFunction = unsafe extern "C" fn(ctx: *mut AggCtx, argc: i32, argv: *const Value);
pub type FinalizeFunction = unsafe extern "C" fn(ctx: *mut AggCtx) -> Value;

pub trait Scalar {
    fn call(&self, args: &[Value]) -> Value;
}

#[repr(C)]
pub struct AggCtx {
    pub state: *mut c_void,
}

pub trait AggFunc {
    type State: Default;
    const NAME: &'static str;
    const ARGS: i32;

    fn step(state: &mut Self::State, args: &[Value]);
    fn finalize(state: Self::State) -> Value;
}
