use crate::errors::Result;
use crate::errors::{LimboError, LIMBO_ETC};
use crate::utils::set_err_msg_and_throw_exception;
use jni::objects::{JObject, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use limbo_core::{Statement, StepResult};

pub struct LimboStatement {
    pub(crate) stmt: Statement,
}

impl LimboStatement {
    pub fn new(stmt: Statement) -> Self {
        LimboStatement { stmt }
    }

    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    #[allow(dead_code)]
    pub fn drop(ptr: jlong) {
        let _boxed = unsafe { Box::from_raw(ptr as *mut LimboStatement) };
    }
}

pub fn to_limbo_statement(ptr: jlong) -> Result<&'static mut LimboStatement> {
    if ptr == 0 {
        Err(LimboError::InvalidConnectionPointer)
    } else {
        unsafe { Ok(&mut *(ptr as *mut LimboStatement)) }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_step<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    stmt_ptr: jlong,
) -> JObject<'local> {
    println!("statement pointer: {:?}", stmt_ptr);
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            println!("error occurred");
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());

            return JObject::null();
        }
    };

    match stmt.stmt.step() {
        Ok(StepResult::Row(row)) => match row_to_obj_array(&mut env, &row) {
            Ok(row) => row,
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());

                JObject::null()
            }
        },
        Ok(StepResult::IO) => match env.new_object_array(0, "java/lang/Object", JObject::null()) {
            Ok(row) => row.into(),
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());

                JObject::null()
            }
        },
        _ => JObject::null(),
    }
}

#[allow(dead_code)]
fn row_to_obj_array<'local>(
    env: &mut JNIEnv<'local>,
    row: &limbo_core::Row,
) -> Result<JObject<'local>> {
    let obj_array =
        env.new_object_array(row.values.len() as i32, "java/lang/Object", JObject::null())?;

    for (i, value) in row.values.iter().enumerate() {
        let obj = match value {
            limbo_core::Value::Null => JObject::null(),
            limbo_core::Value::Integer(i) => {
                env.new_object("java/lang/Long", "(J)V", &[JValue::Long(*i)])?
            }
            limbo_core::Value::Float(f) => {
                env.new_object("java/lang/Double", "(D)V", &[JValue::Double(*f)])?
            }
            limbo_core::Value::Text(s) => env.new_string(s)?.into(),
            limbo_core::Value::Blob(b) => env.byte_array_from_slice(b)?.into(),
        };
        if let Err(e) = env.set_object_array_element(&obj_array, i as i32, obj) {
            eprintln!("Error on parsing row: {:?}", e);
        }
    }

    Ok(obj_array.into())
}
