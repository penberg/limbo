use crate::errors::{LimboError, Result, LIMBO_ETC};
use crate::limbo_connection::LimboConnection;
use crate::utils::set_err_msg_and_throw_exception;
use jni::objects::{JByteArray, JObject};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use limbo_core::Database;
use std::sync::Arc;

struct LimboDB {
    db: Arc<Database>,
    io: Arc<dyn limbo_core::IO>,
}

impl LimboDB {
    pub fn new(db: Arc<Database>, io: Arc<dyn limbo_core::IO>) -> Self {
        LimboDB { db, io }
    }

    pub fn to_ptr(self) -> jlong {
        Box::into_raw(Box::new(self)) as jlong
    }

    pub fn drop(ptr: jlong) {
        let _boxed = unsafe { Box::from_raw(ptr as *mut LimboDB) };
    }
}

fn to_limbo_db(ptr: jlong) -> Result<&'static mut LimboDB> {
    if ptr == 0 {
        Err(LimboError::InvalidDatabasePointer)
    } else {
        unsafe { Ok(&mut *(ptr as *mut LimboDB)) }
    }
}

#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub extern "system" fn Java_tech_turso_core_LimboDB_openUtf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    file_path_byte_arr: JByteArray<'local>,
    _open_flags: jint,
) -> jlong {
    let io = match limbo_core::PlatformIO::new() {
        Ok(io) => Arc::new(io),
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
            return -1;
        }
    };

    let path = match env
        .convert_byte_array(file_path_byte_arr)
        .map_err(|e| e.to_string())
    {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
                return -1;
            }
        },
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
            return -1;
        }
    };

    let db = match Database::open_file(io.clone(), &path) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
            return -1;
        }
    };

    LimboDB::new(db, io).to_ptr()
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_LimboDB_connect0<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    db_pointer: jlong,
) -> jlong {
    let db = match to_limbo_db(db_pointer) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, LIMBO_ETC, e.to_string());
            return 0;
        }
    };

    let conn = LimboConnection::new(db.db.connect(), db.io.clone());
    conn.to_ptr()
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_LimboDB_close0<'local>(
    _env: JNIEnv<'local>,
    _obj: JObject<'local>,
    db_pointer: jlong,
) {
    LimboDB::drop(db_pointer);
}

#[no_mangle]
pub extern "system" fn Java_tech_turso_core_LimboDB_throwJavaException<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    error_code: jint,
) {
    set_err_msg_and_throw_exception(
        &mut env,
        obj,
        error_code,
        "throw java exception".to_string(),
    );
}
