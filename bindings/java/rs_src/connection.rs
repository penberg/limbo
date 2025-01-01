use crate::cursor::Cursor;
use jni::objects::JClass;
use jni::sys::jlong;
use jni::JNIEnv;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Connection {
    pub(crate) conn: Arc<Mutex<Rc<limbo_core::Connection>>>,
    pub(crate) io: Arc<limbo_core::PlatformIO>,
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
pub extern "system" fn Java_org_github_tursodatabase_limbo_Connection_cursor<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    connection_ptr: jlong,
) -> jlong {
    let connection = to_connection(connection_ptr);
    let cursor = Cursor {
        array_size: 1,
        conn: connection.clone(),
        description: None,
        rowcount: -1,
        smt: None,
    };
    Box::into_raw(Box::new(cursor)) as jlong
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

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_limbo_Connection_commit<'local>(
    _env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    _connection_id: jlong,
) {
    unimplemented!()
}

#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_limbo_Connection_rollback<'local>(
    _env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    _connection_id: jlong,
) {
    unimplemented!()
}

fn to_connection(connection_ptr: jlong) -> &'static mut Connection {
    unsafe { &mut *(connection_ptr as *mut Connection) }
}
