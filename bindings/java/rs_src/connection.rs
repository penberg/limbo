use crate::errors::{
    LimboError, Result, LIMBO_ETC, LIMBO_FAILED_TO_PARSE_BYTE_ARRAY,
    LIMBO_FAILED_TO_PREPARE_STATEMENT,
};
use crate::utils::utf8_byte_arr_to_str;
use jni::objects::{JByteArray, JClass, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use std::rc::Rc;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Rc<limbo_core::Connection>,
    pub(crate) io: Arc<dyn limbo_core::IO>,
}

/// Returns a pointer to a `Cursor` object.
///
/// The Java application will pass this pointer to native functions,
/// which will use it to reference the `Cursor` object.
///
/// # Arguments
///
/// * `_env` - The JNI environment pointer.
/// * `_class` - The Java class calling this function.
/// * `connection_ptr` - A pointer to the `Connection` object.
///
/// # Returns
///
/// A `jlong` representing the pointer to the newly created `Cursor` object.
#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_limbo_LimboConnection_prepareUtf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    connection_ptr: jlong,
    sql_bytes: JByteArray<'local>,
) -> jlong {
    let connection = match to_connection(connection_ptr) {
        Ok(conn) => conn,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
            return 0;
        }
    };

    let sql = match utf8_byte_arr_to_str(&env, sql_bytes) {
        Ok(sql) => sql,
        Err(e) => {
            set_err_msg_and_throw_exception(
                &mut env,
                obj,
                LIMBO_FAILED_TO_PARSE_BYTE_ARRAY,
                e.to_string(),
            );
            return 0;
        }
    };

    match connection.conn.prepare(sql) {
        Ok(stmt) => Box::into_raw(Box::new(stmt)) as jlong,
        Err(e) => {
            set_err_msg_and_throw_exception(
                &mut env,
                obj,
                LIMBO_FAILED_TO_PREPARE_STATEMENT,
                e.to_string(),
            );
            0
        }
    }
}

/// Closes the connection and releases the associated resources.
///
/// This function is called from the Java side to close the connection
/// and free the memory allocated for the `Connection` object.
///
/// # Arguments
///
/// * `_env` - The JNI environment pointer.
/// * `_class` - The Java class calling this function.
/// * `connection_ptr` - A pointer to the `Connection` object to be closed.
#[no_mangle]
pub unsafe extern "system" fn Java_org_github_tursodatabase_limbo_Connection_close<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_ptr: jlong,
) {
    let _boxed_connection = Box::from_raw(connection_ptr as *mut Connection);
}

fn to_connection(connection_ptr: jlong) -> Result<&'static mut Rc<Connection>> {
    if connection_ptr == 0 {
        Err(LimboError::InvalidConnectionPointer)
    } else {
        unsafe { Ok(&mut *(connection_ptr as *mut Rc<Connection>)) }
    }
}

fn set_err_msg_and_throw_exception<'local>(
    env: &mut JNIEnv<'local>,
    obj: JObject<'local>,
    err_code: i32,
    err_msg: String,
) {
    let error_message_bytes = env
        .byte_array_from_slice(err_msg.as_bytes())
        .expect("Failed to convert to byte array");
    match env.call_method(
        obj,
        "throwLimboException",
        "(I[B)V",
        &[err_code.into(), (&error_message_bytes).into()],
    ) {
        Ok(_) => {
            // do nothing because above method will always return Err
        }
        Err(_e) => {
            // do nothing because our java app will handle Err
        }
    }
}
