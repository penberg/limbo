mod connection;
mod cursor;
mod errors;
mod macros;
mod utils;

use crate::connection::Connection;
use crate::errors::ErrorCode;
use jni::errors::JniError;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::sync::{Arc, Mutex};

/// Establishes a connection to the database specified by the given path.
///
/// This function is called from the Java side to create a connection to the database.
/// It returns a pointer to the `Connection` object, which can be used in subsequent
/// native function calls.
///
/// # Arguments
///
/// * `env` - The JNI environment pointer.
/// * `_class` - The Java class calling this function.
/// * `path` - A `JString` representing the path to the database file.
///
/// # Returns
///
/// A `jlong` representing the pointer to the newly created `Connection` object,
/// or [ErrorCode::CONNECTION_FAILURE] if the connection could not be established.
#[no_mangle]
pub extern "system" fn Java_org_github_tursodatabase_limbo_Limbo_connect<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    connect_internal(&mut env, path).unwrap_or_else(|_| ErrorCode::CONNECTION_FAILURE as jlong)
}

#[allow(improper_ctypes_definitions, clippy::arc_with_non_send_sync)] // TODO: remove
fn connect_internal<'local>(
    env: &mut JNIEnv<'local>,
    path: JString<'local>,
) -> Result<jlong, JniError> {
    let io = Arc::new(limbo_core::PlatformIO::new().map_err(|e| {
        println!("IO initialization failed: {:?}", e);
        JniError::Unknown
    })?);

    let path: String = env
        .get_string(&path)
        .expect("Failed to convert JString to Rust String")
        .into();
    let db = limbo_core::Database::open_file(io.clone(), &path).map_err(|e| {
        println!("Failed to open database: {:?}", e);
        JniError::Unknown
    })?;

    let conn = db.connect().clone();
    let connection = Connection {
        conn: Arc::new(Mutex::new(conn)),
        io,
    };

    Ok(Box::into_raw(Box::new(connection)) as jlong)
}
