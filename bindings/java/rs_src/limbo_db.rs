use jni::objects::{JByteArray, JObject};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use limbo_core::Database;
use std::sync::Arc;

const ERROR_CODE_ETC: i32 = 9999;

#[no_mangle]
#[allow(clippy::arc_with_non_send_sync)]
pub extern "system" fn Java_org_github_tursodatabase_core_LimboDB__1open_1utf8<'local>(
    mut env: JNIEnv<'local>,
    obj: JObject<'local>,
    file_name_byte_arr: JByteArray<'local>,
    _open_flags: jint,
) -> jlong {
    let io = match limbo_core::PlatformIO::new() {
        Ok(io) => Arc::new(io),
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, ERROR_CODE_ETC, e.to_string());
            return -1;
        }
    };

    let path = match env
        .convert_byte_array(file_name_byte_arr)
        .map_err(|e| e.to_string())
    {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                set_err_msg_and_throw_exception(&mut env, obj, ERROR_CODE_ETC, e.to_string());
                return -1;
            }
        },
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, ERROR_CODE_ETC, e.to_string());
            return -1;
        }
    };

    let db = match Database::open_file(io.clone(), &path) {
        Ok(db) => db,
        Err(e) => {
            set_err_msg_and_throw_exception(&mut env, obj, ERROR_CODE_ETC, e.to_string());
            return -1;
        }
    };

    Box::into_raw(Box::new(db)) as jlong
}

fn set_err_msg_and_throw_exception<'local>(
    env: &mut JNIEnv<'local>,
    obj: JObject<'local>,
    err_code: i32,
    err_msg: String,
) {
    let error_message_pointer = Box::into_raw(Box::new(err_msg)) as i64;
    match env.call_method(
        obj,
        "newSQLException",
        "(IJ)Lorg/github/tursodatabase/exceptions/LimboException;",
        &[err_code.into(), error_message_pointer.into()],
    ) {
        Ok(_) => {
            // do nothing because above method will always return Err
        }
        Err(_e) => {
            // do nothing because our java app will handle Err
        }
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_org_github_tursodatabase_core_LimboDB_getErrorMessageUtf8<
    'local,
>(
    env: JNIEnv<'local>,
    _obj: JObject<'local>,
    error_message_ptr: jlong,
) -> JByteArray<'local> {
    let error_message = Box::from_raw(error_message_ptr as *mut String);
    let error_message_bytes = error_message.as_bytes();
    env.byte_array_from_slice(error_message_bytes).unwrap()
}
