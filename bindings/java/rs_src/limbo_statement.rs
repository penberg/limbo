use crate::errors::{LimboError, LIMBO_ETC};
use crate::errors::{Result, SQLITE_ERROR, SQLITE_OK};
use crate::limbo_connection::LimboConnection;
use crate::utils::set_err_msg_and_throw_exception;
use jni::objects::{JByteArray, JObject, JObjectArray, JString, JValue};
use jni::sys::{jdouble, jint, jlong};
use jni::JNIEnv;
use limbo_core::{Statement, StepResult, Value};
use std::num::NonZero;

pub const STEP_RESULT_ID_ROW: i32 = 10;
#[allow(dead_code)]
pub const STEP_RESULT_ID_IO: i32 = 20;
pub const STEP_RESULT_ID_DONE: i32 = 30;
pub const STEP_RESULT_ID_INTERRUPT: i32 = 40;
pub const STEP_RESULT_ID_BUSY: i32 = 50;
pub const STEP_RESULT_ID_ERROR: i32 = 60;

pub struct LimboStatement {
    pub(crate) stmt: Statement,
    pub(crate) connection: LimboConnection,
}

impl LimboStatement {
    pub fn new(stmt: Statement, connection: LimboConnection) -> Self {
        LimboStatement { stmt, connection }
    }

    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

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
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
            return to_limbo_step_result(&mut env, STEP_RESULT_ID_ERROR, None);
        }
    };

    loop {
        let step_result = match stmt.stmt.step() {
            Ok(result) => result,
            Err(_) => return to_limbo_step_result(&mut env, STEP_RESULT_ID_ERROR, None),
        };

        match step_result {
            StepResult::Row => {
                let row = stmt.stmt.row().unwrap();
                return match row_to_obj_array(&mut env, &row) {
                    Ok(row) => to_limbo_step_result(&mut env, STEP_RESULT_ID_ROW, Some(row)),
                    Err(e) => {
                        set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
                        to_limbo_step_result(&mut env, STEP_RESULT_ID_ERROR, None)
                    }
                };
            }
            StepResult::IO => {
                if let Err(e) = stmt.connection.io.run_once() {
                    set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
                    return to_limbo_step_result(&mut env, STEP_RESULT_ID_ERROR, None);
                }
            }
            StepResult::Done => return to_limbo_step_result(&mut env, STEP_RESULT_ID_DONE, None),
            StepResult::Interrupt => {
                return to_limbo_step_result(&mut env, STEP_RESULT_ID_INTERRUPT, None)
            }
            StepResult::Busy => return to_limbo_step_result(&mut env, STEP_RESULT_ID_BUSY, None),
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement__1close<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    stmt_ptr: jlong,
) {
    LimboStatement::drop(stmt_ptr);
}

fn row_to_obj_array<'local>(
    env: &mut JNIEnv<'local>,
    row: &limbo_core::Row,
) -> Result<JObject<'local>> {
    let obj_array = env.new_object_array(row.len() as i32, "java/lang/Object", JObject::null())?;

    for (i, value) in row.get_values().iter().enumerate() {
        let value = value.to_value();
        let obj = match value {
            limbo_core::Value::Null => JObject::null(),
            limbo_core::Value::Integer(i) => {
                env.new_object("java/lang/Long", "(J)V", &[JValue::Long(i)])?
            }
            limbo_core::Value::Float(f) => {
                env.new_object("java/lang/Double", "(D)V", &[JValue::Double(f)])?
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

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_columns<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    stmt_ptr: jlong,
) -> JObject<'local> {
    let stmt = to_limbo_statement(stmt_ptr).unwrap();
    let num_columns = stmt.stmt.num_columns();
    let obj_arr: JObjectArray = env
        .new_object_array(num_columns as i32, "java/lang/String", JObject::null())
        .unwrap();

    for i in 0..num_columns {
        if let Some(column_name) = stmt.stmt.get_column_name(i) {
            let str = env.new_string(column_name).unwrap();
            env.set_object_array_element(&obj_arr, i as i32, str)
                .unwrap();
        }
    }

    obj_arr.into()
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_bindNull<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    stmt_ptr: jlong,
    position: jint,
) -> jint {
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, SQLITE_ERROR, e.to_string());
            return SQLITE_ERROR;
        }
    };

    stmt.stmt
        .bind_at(NonZero::new(position as usize).unwrap(), Value::Null);
    SQLITE_OK
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_bindLong<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    stmt_ptr: jlong,
    position: jint,
    value: jlong,
) -> jint {
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, SQLITE_ERROR, e.to_string());
            return SQLITE_ERROR;
        }
    };

    stmt.stmt.bind_at(
        NonZero::new(position as usize).unwrap(),
        Value::Integer(value),
    );
    SQLITE_OK
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_bindDouble<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    stmt_ptr: jlong,
    position: jint,
    value: jdouble,
) -> jint {
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, SQLITE_ERROR, e.to_string());
            return SQLITE_ERROR;
        }
    };

    stmt.stmt.bind_at(
        NonZero::new(position as usize).unwrap(),
        Value::Float(value),
    );
    SQLITE_OK
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_bindText<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    stmt_ptr: jlong,
    position: jint,
    value: JString<'local>,
) -> jint {
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, SQLITE_ERROR, e.to_string());
            return SQLITE_ERROR;
        }
    };

    let text: String = match env.get_string(&value) {
        Ok(s) => s.into(),
        Err(_) => return SQLITE_ERROR,
    };

    stmt.stmt.bind_at(
        NonZero::new(position as usize).unwrap(),
        Value::Text(text.as_str()),
    );
    SQLITE_OK
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboStatement_bindBlob<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    stmt_ptr: jlong,
    position: jint,
    value: JByteArray<'local>,
) -> jint {
    let stmt = match to_limbo_statement(stmt_ptr) {
        Ok(stmt) => stmt,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, SQLITE_ERROR, e.to_string());
            return SQLITE_ERROR;
        }
    };

    let blob: Vec<u8> = match env.convert_byte_array(value) {
        Ok(b) => b,
        Err(_) => return SQLITE_ERROR,
    };

    stmt.stmt.bind_at(
        NonZero::new(position as usize).unwrap(),
        Value::Blob(blob.as_ref()),
    );
    SQLITE_OK
}

/// Converts an optional `JObject` into Java's `LimboStepResult`.
///
/// This function takes an optional `JObject` and converts it into a Java object
/// of type `LimboStepResult`. The conversion is done by creating a new Java object with the
/// appropriate constructor arguments.
///
/// # Arguments
///
/// * `env` - A mutable reference to the JNI environment.
/// * `id` - An integer representing the type of `StepResult`.
/// * `result` - An optional `JObject` that contains the result data.
///
/// # Returns
///
/// A `JObject` representing the `LimboStepResult` in Java. If the object creation fails,
/// a null `JObject` is returned
fn to_limbo_step_result<'local>(
    env: &mut JNIEnv<'local>,
    id: i32,
    result: Option<JObject<'local>>,
) -> JObject<'local> {
    let mut ctor_args = vec![JValue::Int(id)];
    if let Some(res) = result {
        ctor_args.push(JValue::Object(&res));
        env.new_object(
            "org/github/tursodatabase/core/LimboStepResult",
            "(I[Ljava/lang/Object;)V",
            &ctor_args,
        )
    } else {
        env.new_object(
            "org/github/tursodatabase/core/LimboStepResult",
            "(I)V",
            &ctor_args,
        )
    }
    .unwrap_or_else(|_| JObject::null())
}
