use crate::errors::{
    LimboError, Result, LIMBO_ETC, LIMBO_FAILED_TO_PARSE_BYTE_ARRAY,
    LIMBO_FAILED_TO_PREPARE_STATEMENT,
};
use crate::limbo_statement::LimboStatement;
use crate::utils::{set_err_msg_and_throw_exception, utf8_byte_arr_to_str};
use jni::objects::{JByteArray, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use limbo_core::Connection;
use std::rc::Rc;

#[derive(Clone)]
pub struct LimboConnection {
    pub(crate) conn: Rc<Connection>,
    pub(crate) io: Rc<dyn limbo_core::IO>,
}

impl LimboConnection {
    pub fn new(conn: Rc<Connection>, io: Rc<dyn limbo_core::IO>) -> Self {
        LimboConnection { conn, io }
    }

    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    #[allow(dead_code)]
    pub fn drop(ptr: jlong) {
        let _boxed = unsafe { Box::from_raw(ptr as *mut LimboConnection) };
    }
}

pub fn to_limbo_connection(ptr: jlong) -> Result<&'static mut LimboConnection> {
    if ptr == 0 {
        Err(LimboError::InvalidConnectionPointer)
    } else {
        unsafe { Ok(&mut *(ptr as *mut LimboConnection)) }
    }
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
pub extern "system" fn Java_org_github_tursodatabase_core_LimboConnection_prepareUtf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    connection_ptr: jlong,
    sql_bytes: JByteArray<'local>,
) -> jlong {
    let connection = match to_limbo_connection(connection_ptr) {
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
        Ok(stmt) => LimboStatement::new(stmt).to_ptr(),
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
